use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray, UnionArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, build_array, copy_array, estimate_builder_bytes, estimate_builder_bytes_refs, estimate_read_value_bytes, from_field, read_value, struct_field_cell, to_field};
use rtdb_format::envelope::{SourceBody, SourceMessage};
use prost::Message;
use rtdb_types::{Anomaly, EntityId, Iid, LogicalType, Result, ScalarType, TableRef, TimeUnit, Timestamp, Value};

use crate::{ArrowDocuments, Documents, EntityIds, RetainedBytes, SqlArguments, SubmitOptions, TxOp, UserMetadata};
use crate::values::DecodedDocumentLayout;

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    Public(TxOp),
    PutIids { table: TableRef, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PutArrowIids { table: TableRef, documents: ArrowDocuments, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PatchIids { table: TableRef, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    Removed(String),
}

pub struct SourceTransaction {
    pub(crate) operations: Vec<Operation>,
    pub options: SubmitOptions,
    pub(crate) memory: Arc<MemoryBudget>,
    _reservation: Arc<MemoryReservation>,
}

impl SourceTransaction {
    pub fn new(ops: Vec<TxOp>, options: SubmitOptions, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let operations = ops.into_iter().map(Operation::Public).collect();
        Self::from_operations(operations, options, memory)
    }
    fn from_operations(operations: Vec<Operation>, options: SubmitOptions, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut bytes = operations.capacity().checked_mul(std::mem::size_of::<Operation>()).and_then(|size| size.checked_add(options.default_tz.capacity())).and_then(|size| size.checked_add(options.user.as_ref().map_or(0, String::capacity))).ok_or_else(overflow)?;
        for operation in &operations {
            let (text, table, ids) = match operation {
                Operation::Public(TxOp::Sql { sql, .. } | TxOp::SqlBytes { sql, .. }) => (sql.capacity(), None, 0),
                Operation::Public(TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. }) => (0, Some(table), 0),
                Operation::PutIids { table, iids, .. } | Operation::PutArrowIids { table, iids, .. } | Operation::PatchIids { table, iids, .. } => (0, Some(table), iids.capacity().checked_mul(16).ok_or_else(overflow)?),
                Operation::Removed(name) => (name.capacity(), None, 0),
            };
            bytes = bytes.checked_add(text).and_then(|size| size.checked_add(ids)).and_then(|size| size.checked_add(table.map_or(0, |table| table.schema.capacity() + table.table.capacity()))).and_then(|size| size.checked_add(256)).ok_or_else(overflow)?;
        }
        let reservation = memory.reserve(bytes)?;
        Ok(Self { operations, options, memory: Arc::clone(memory), _reservation: reservation })
    }

    /// Decodes current flat tx operations and the legacy one-row envelope without expanding SQL.
    pub fn decode(message: &SourceMessage, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let option_bytes = match message {
            SourceMessage::Protobuf(SourceBody::Tx(tx)) => tx.default_tz.as_ref().map_or(0, String::len)
                .checked_add(tx.user.as_ref().map_or(0, String::len)).ok_or_else(overflow)?,
            _ => 0,
        };
        let _option_scratch = memory.reserve(option_bytes)?;
        let (pages, mut options, legacy) = match message {
            SourceMessage::Protobuf(SourceBody::Tx(tx)) => {
                let bytes = tx.tx_ops.as_deref().ok_or_else(|| invalid("Transaction has no operations stream"))?;
                let options = SubmitOptions { default_tz: tx.default_tz.clone().ok_or_else(|| invalid("Transaction has no default timezone"))?, system_time: tx.system_time_micros.map(Timestamp), user: tx.user.clone(), user_metadata: tx.user_metadata.as_deref().filter(|bytes| !bytes.is_empty()).map(|bytes| decode_metadata(bytes, memory)).transpose()? };
                (crate::ipc::decode(bytes, memory)?, options, false)
            }
            SourceMessage::LegacyTransaction(bytes) => (crate::ipc::decode(bytes, memory)?, SubmitOptions::default(), true),
            _ => return Err(invalid("Source message is not a transaction")),
        };
        if pages.is_empty() && !legacy { return Self::from_operations(Vec::new(), options, memory); }
        let first = pages.first().ok_or_else(|| invalid("Transaction IPC stream has no record batch"))?;
        let mut parsed_fields = memory.reserve(4096)?;
        let parsed_fields = Arc::get_mut(&mut parsed_fields).expect("source decoding owns its reservation");
        if legacy {
            if first.batch().num_rows() != 1 || pages.len() != 1 { return Err(invalid("Legacy transaction must have one envelope row")); }
            let batch = first.batch();
            let option_fields = ["default-tz", "system-time", "user", "user-metadata"];
            let bound = batch.schema().fields().iter().zip(batch.columns()).filter(|(field, _)| option_fields.contains(&field.name().as_str()))
                .try_fold(parsed_fields.bytes(), |bytes, (field, column)| {
                    bytes.checked_add(estimate_read_value_bytes(field, column.as_ref(), 0)?).ok_or_else(overflow)
                })?;
            parsed_fields.grow_to(bound)?;
            options.default_tz = match cell(first.batch(), "default-tz", 0)? { Value::Utf8(value) => value, _ => return Err(invalid("Legacy transaction has no timezone")) };
            options.system_time = optional_timestamp(cell(first.batch(), "system-time", 0)?)?;
            options.user = optional_string(optional_cell(first.batch(), "user", 0)?)?;
            let metadata = optional_cell(first.batch(), "user-metadata", 0)?;
            if !matches!(metadata, Value::Null | Value::Undefined) { options.user_metadata = Some(UserMetadata::new(metadata, memory)?); }
        }
        let mut reader = SourceOperationReader { memory, parsed_fields, layouts: BTreeMap::new() };
        let mut operations = Vec::new();
        for page in &pages {
            let batch = page.batch();
            let column = batch.column(batch.schema().index_of("tx-ops").map_err(|error| invalid(error.to_string()))?);
            let array: ArrayRef = if legacy {
                let list = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Legacy tx-ops is not a list"))?;
                list.value(0)
            } else { Arc::clone(column) };
            let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Transaction operations are not a union"))?;
            for row in 0..union.len() { operations.push(reader.operation(union, row)?); }
        }
        Self::from_operations(operations, options, memory)
    }

    /// Encodes the operations as supplied; submit-time SQL expansion is a separate caller operation.
    /// The retry identity's key, when the transaction's metadata names one.
    pub(crate) fn idempotency_key(&self) -> Option<&str> {
        let Value::Struct(fields) = self.options.user_metadata.as_ref()?.value() else { return None };
        match fields.get("idempotency_key")? { Value::Utf8(key) => Some(key), _ => None }
    }

    /// A digest of everything that determines this transaction's effects, apart from its idempotency key.
    /// Every replica computes it from the same logged record, so the retry decision is deterministic.
    pub(crate) fn body_digest(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        let mut field = |bytes: &[u8]| { hash.update((bytes.len() as u64).to_be_bytes()); hash.update(bytes); };
        let batch = self.operation_batch()?;
        field(crate::ipc::encode(batch.batch().schema().as_ref(), [batch.batch()], &self.memory)?.as_slice());
        field(self.options.default_tz.as_bytes());
        field(&self.options.system_time.map_or([0; 9], |time| { let mut bytes = [1; 9]; bytes[1..].copy_from_slice(&time.0.to_be_bytes()); bytes }));
        if let Some(Value::Struct(fields)) = self.options.user_metadata.as_ref().map(UserMetadata::value) {
            let mut rest = fields.clone();
            rest.remove("idempotency_key");
            field(encode_metadata(&UserMetadata::new(Value::Struct(rest), &self.memory)?, &self.memory)?.as_slice());
        }
        Ok(hash.finalize().iter().map(|byte| format!("{byte:02x}")).collect())
    }

    pub fn encode(&self) -> Result<RetainedBytes> {
        let batch = self.operation_batch()?;
        let bytes = crate::ipc::encode(batch.batch().schema().as_ref(), [batch.batch()], &self.memory)?;
        let metadata = self.options.user_metadata.as_ref().map(|metadata| encode_metadata(metadata, &self.memory)).transpose()?;
        let body_size = bytes.as_slice().len().checked_add(metadata.as_ref().map_or(0, |bytes| bytes.as_slice().len())).and_then(|bytes| bytes.checked_add(self.options.default_tz.capacity())).and_then(|bytes| bytes.checked_add(self.options.user.as_ref().map_or(0, String::capacity))).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let _body_reservation = self.memory.reserve(body_size)?;
        let tx = rtdb_format::generated::xtdb::log::proto::Tx {
            tx_ops: Some(bytes.as_slice().to_vec()), system_time_micros: self.options.system_time.map(|time| time.0),
            default_tz: Some(self.options.default_tz.clone()), user: self.options.user.clone(),
            user_metadata: metadata.as_ref().map(|bytes| bytes.as_slice().to_vec()), external_source_token: None,
        };
        let wire = rtdb_format::generated::xtdb::log::proto::SourceLogMessage { message: Some(SourceBody::Tx(tx)) };
        let reservation = self.memory.reserve(wire.encoded_len().checked_add(128).ok_or_else(overflow)?)?;
        let encoded = SourceMessage::Protobuf(wire.message.ok_or_else(|| invalid("Source envelope has no transaction"))?).encode()?;
        RetainedBytes::with_reservation(encoded, reservation)
    }

    fn operation_batch(&self) -> Result<OwnedBatch> {
        let scratch = self.operations.iter().try_fold(65_536usize, |bytes, operation| {
            let extra = match operation {
                Operation::Public(TxOp::PutDocs { documents, .. } | TxOp::PatchDocs { documents, .. }) | Operation::PutIids { documents, .. } | Operation::PatchIids { documents, .. } => documents.rows().iter().try_fold(0usize, |bytes, document| bytes.checked_add(Value::document_retained_bytes(document)?).ok_or_else(overflow))?,
                Operation::Public(TxOp::PutArrowDocs { documents, .. }) | Operation::PutArrowIids { documents, .. } => {
                    documents.array().len().checked_mul(64).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::Sql { sql, args }) => {
                    let rows = args.as_ref().map(SqlArguments::value_bytes).transpose()?.unwrap_or(0);
                    rows.checked_add(sql.capacity()).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => sql.capacity().checked_add(arg_bytes.as_slice().len()).ok_or_else(overflow)?,
                Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) => ids.ids().len().checked_mul(128).ok_or_else(overflow)?,
                Operation::Removed(name) => name.capacity(),
            };
            bytes.checked_add(extra.checked_mul(8).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow)?).ok_or_else(overflow)
        })?;
        let _scratch_reservation = self.memory.reserve(scratch)?;
        let mut entries = Vec::new();
        for operation in &self.operations {
            check_writable_table(operation)?;
            if matches!(operation, Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) if ids.ids().is_empty()) { continue; }
            entries.push(operation_value(operation, &self.memory)?);
        }
        let metadata_bound = entries.iter().try_fold(65_536usize, |bytes, (_, entry)| {
            let Some((_, doc)) = &entry.documents else { return Ok(bytes); };
            doc.columns().iter().try_fold(bytes, |bytes, name| {
                let size = name.len().checked_mul(8).and_then(|size| size.checked_add(1024)).ok_or_else(overflow)?;
                bytes.checked_add(size).ok_or_else(overflow)
            })
        })?;
        let mut arrays = BatchBuilder::new(metadata_bound, &self.memory)?;
        let union = named_union(entries, |leg| operation_leg(leg, &mut arrays, &self.memory))?;
        let schema = Arc::new(Schema::new(vec![Field::new("tx-ops", union.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![union]).map_err(|error| invalid(error.to_string()))?;
        arrays.finish(batch)
    }
}

struct SourceOperation<'a> {
    body: StructFields<Value>,
    documents: Option<(String, SourceDocuments<'a>)>,
}

enum SourceDocuments<'a> { Rows(&'a Documents), Arrow(&'a ArrowDocuments) }

impl SourceDocuments<'_> {
    fn columns(&self) -> &[String] {
        match self { Self::Rows(documents) => documents.columns(), Self::Arrow(documents) => documents.columns() }
    }
}

fn operation_value<'a>(operation: &'a Operation, memory: &Arc<MemoryBudget>) -> Result<(String, SourceOperation<'a>)> {
    let mut body = StructFields::new();
    let mut docs = None;
    let tag = match operation {
        Operation::Public(TxOp::Sql { sql, args }) => {
            body.insert("query".into(), Value::Utf8(sql.clone()));
            body.insert("args".into(), args.as_ref().map(|args| encode_arguments(args, memory).map(|bytes| Value::Binary(bytes.as_slice().to_vec()))).transpose()?.unwrap_or(Value::Null));
            "sql"
        }
        Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => {
            body.insert("query".into(), Value::Utf8(sql.clone()));
            body.insert("args".into(), if arg_bytes.as_slice().is_empty() { Value::Null } else { Value::Binary(arg_bytes.as_slice().to_vec()) });
            "sql"
        }
        Operation::Public(TxOp::PutDocs { table, documents, valid_from, valid_to } | TxOp::PatchDocs { table, documents, valid_from, valid_to }) => {
            let iids = documents.rows().iter().map(|document| {
                document_id(document, memory).map(|id| Value::FixedSizeBinary(id.iid().0.to_vec()))
            }).collect::<Result<_>>()?;
            body.insert("iids".into(), Value::List(iids));
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), SourceDocuments::Rows(documents)));
            if matches!(operation, Operation::Public(TxOp::PatchDocs { .. })) { "patch-docs" } else { "put-docs" }
        }
        Operation::Public(TxOp::PutArrowDocs { table, documents }) => {
            let reader = documents.id_reader()?;
            let iids = (0..documents.array().len()).map(|row| {
                documents.iid(row, reader.as_ref(), memory).map(|iid| Value::FixedSizeBinary(iid.0.to_vec()))
            }).collect::<Result<_>>()?;
            body.insert("iids".into(), Value::List(iids));
            time_fields(&mut body, None, None);
            docs = Some((table_name(table), SourceDocuments::Arrow(documents)));
            "put-docs"
        }
        Operation::PutArrowIids { table, documents, iids, valid_from, valid_to } => {
            body.insert("iids".into(), Value::List(iids.iter().map(|iid| Value::FixedSizeBinary(iid.0.to_vec())).collect()));
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), SourceDocuments::Arrow(documents)));
            "put-docs"
        }
        Operation::PutIids { table, documents, iids, valid_from, valid_to } | Operation::PatchIids { table, documents, iids, valid_from, valid_to } => {
            body.insert("iids".into(), Value::List(iids.iter().map(|iid| Value::FixedSizeBinary(iid.0.to_vec())).collect()));
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), SourceDocuments::Rows(documents)));
            if matches!(operation, Operation::PatchIids { .. }) { "patch-docs" } else { "put-docs" }
        }
        Operation::Public(TxOp::DeleteDocs { table, ids, valid_from, valid_to }) => {
            body.insert("table".into(), Value::Utf8(table_name(table)));
            body.insert("iids".into(), Value::List(ids.ids().iter().map(|id| Value::FixedSizeBinary(id.iid().0.to_vec())).collect()));
            time_fields(&mut body, *valid_from, *valid_to);
            "delete-docs"
        }
        Operation::Public(TxOp::EraseDocs { table, ids }) => {
            body.insert("table".into(), Value::Utf8(table_name(table)));
            body.insert("iids".into(), Value::List(ids.ids().iter().map(|id| Value::FixedSizeBinary(id.iid().0.to_vec())).collect()));
            "erase-docs"
        }
        Operation::Removed(name) => return Err(Anomaly::unsupported("rtdb.tx/removed-operation", format!("Cannot encode removed operation {name}"))),
    };
    Ok((tag.into(), SourceOperation { body, documents: docs }))
}

pub(crate) fn document_id(document: &StructFields<Value>, memory: &Arc<MemoryBudget>) -> Result<EntityId> {
    if let Some(id) = document.get("_id").filter(|id| !matches!(id, Value::Null | Value::Undefined)) {
        return id.entity_id();
    }
    let bound = document.iter().try_fold(4096usize, |bytes, (key, value)| {
        let key_bytes = key.len().checked_mul(8).ok_or_else(overflow)?;
        let value_bytes = rtdb_expr::text_allocation_bound(value)?;
        bytes.checked_add(key_bytes).and_then(|bytes| bytes.checked_add(value_bytes)).ok_or_else(overflow)
    })?;
    let _scratch = memory.reserve(bound)?;
    let typed = rtdb_format::transit::document_to_transit(document)?;
    let _json = memory.reserve(rtdb_format::transit::json_value_allocation_bound(&typed)?)?;
    Err(Anomaly::incorrect("missing-id", "missing '_id'").with_transit_context("doc", &typed)?)
}

fn named_union<T>(
    values: impl IntoIterator<Item = (String, T)>,
    mut build: impl FnMut(Vec<T>) -> Result<ArrayRef>,
) -> Result<ArrayRef> {
    let mut names = Vec::new();
    let mut legs: Vec<Vec<T>> = Vec::new();
    let mut ids = Vec::new();
    let mut offsets = Vec::new();
    for (name, value) in values {
        let leg = match names.iter().position(|existing| existing == &name) {
            Some(leg) => leg,
            None => { names.push(name); legs.push(Vec::new()); names.len() - 1 }
        };
        ids.push(i8::try_from(leg).map_err(|_| invalid("Too many transaction operation legs"))?);
        offsets.push(i32::try_from(legs[leg].len()).map_err(|_| overflow())?);
        legs[leg].push(value);
    }
    let mut fields = Vec::new();
    let mut children = Vec::new();
    for (name, values) in names.into_iter().zip(legs) {
        let array = build(values)?;
        fields.push(Arc::new(Field::new(name, array.data_type().clone(), false)));
        children.push(array);
    }
    Ok(Arc::new(UnionArray::try_new(UnionFields::new((0..fields.len()).map(|id| id as i8), fields), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(|error| invalid(error.to_string()))?))
}

fn operation_leg(values: Vec<SourceOperation<'_>>, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    let (values, documents): (Vec<_>, Vec<_>) = values.into_iter().map(|value| (value.body, value.documents)).unzip();
    let types = values.iter().map(|value| LogicalType::Struct(value.iter().map(|(name, value)| (name.clone(), value.logical_type())).collect())).collect::<Vec<_>>();
    let LogicalType::Struct(mut types) = LogicalType::join_all(&types) else { return Err(invalid("Transaction operation leg has no struct layout")); };
    for temporal in ["_valid_from", "_valid_to"] {
        if let Some(ty) = types.get_mut(temporal) {
            *ty = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).maybe();
        }
    }
    if let Some(ty) = types.get_mut("iids") { *ty = LogicalType::list(LogicalType::Scalar(ScalarType::FixedSizeBinary(16))); }
    // The reference's argument vector is non-nullable until an operation without arguments writes it a null.
    if let Some(ty) = types.get_mut("args") {
        let binary = LogicalType::Scalar(ScalarType::Binary);
        *ty = if *ty == binary { binary } else { binary.maybe() };
    }
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    // The reference's document writers declare `documents` straight after `iids`, ahead of the valid-time columns.
    let mut documents = documents.iter().any(Option::is_some).then_some(documents);
    for (name, ty) in types {
        let cells = || values.iter().map(|value| struct_field_cell(value, &name, &ty));
        let bound = estimate_builder_bytes_refs(&ty, cells())?;
        columns.push(arrays.column(bound, || copy_array(&ty, &cells().cloned().collect::<Vec<_>>()))?);
        fields.push(Arc::new(to_field(&name, &ty)?));
        if name == "iids" {
            if let Some(documents) = documents.take() {
                let documents = named_union(documents.into_iter().flatten(), |leg| document_leg(leg, arrays, memory))?;
                fields.push(Arc::new(Field::new("documents", documents.data_type().clone(), false)));
                columns.push(documents);
            }
        }
    }
    if documents.is_some() { return Err(invalid("Document operation has no iids")); }
    rtdb_columnar::build_struct_array(fields.into(), &columns, values.len(), None).map_err(|error| invalid(error.to_string()))
}

fn document_leg(lists: Vec<SourceDocuments<'_>>, arrays: &mut BatchBuilder, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    let arrow = lists.iter().map(|list| match list {
        SourceDocuments::Arrow(documents) => Some(documents.array()),
        SourceDocuments::Rows(_) => None,
    }).collect::<Option<Vec<_>>>();
    if let Some(pages) = arrow && let Some(leg) = arrow_document_leg(&pages, arrays)? {
        return Ok(leg);
    }
    let rows = lists.iter().map(|list| match list {
        SourceDocuments::Rows(documents) => Ok((*documents).clone()),
        SourceDocuments::Arrow(documents) => documents.to_rows(false, memory),
    }).collect::<Result<Vec<_>>>()?;
    row_document_leg(rows.iter().collect(), arrays)
}

fn arrow_document_leg(pages: &[&StructArray], arrays: &mut BatchBuilder) -> Result<Option<ArrayRef>> {
    let Some(first) = pages.first() else { return Ok(None); };
    let mut offsets = vec![0i32];
    for page in pages {
        let length = i32::try_from(page.len()).map_err(|_| overflow())?;
        offsets.push(offsets.last().unwrap().checked_add(length).ok_or_else(overflow)?);
    }
    if pages.len() == 1 {
        let bound = first.get_array_memory_size().checked_add(4096).ok_or_else(overflow)?;
        let structs = arrays.column(bound, || Ok(Arc::new((*first).clone())))?;
        return document_list(structs, offsets).map(Some);
    }

    let mut layout = StructFields::<Field>::new();
    for page in pages {
        for field in page.fields() {
            let Some(previous) = layout.get(field.name()) else {
                layout.insert(field.name().clone(), field.as_ref().clone());
                continue;
            };
            let merged = if previous.data_type() == field.data_type() && previous.metadata() == field.metadata() {
                previous.clone().with_nullable(previous.is_nullable() || field.is_nullable())
            } else if previous.data_type() == &DataType::Null && previous.is_nullable() {
                field.as_ref().clone().with_nullable(true)
            } else if field.data_type() == &DataType::Null && field.is_nullable() {
                previous.clone().with_nullable(true)
            } else if let Some(merged) = merge_document_fields(previous, field)? {
                merged
            } else {
                return Ok(None);
            };
            layout.insert(field.name().clone(), merged);
        }
    }
    let mut output_layout = Vec::with_capacity(layout.len());
    for (name, field) in layout {
        let nullable = field.is_nullable() || pages.iter().any(|page| page.column_by_name(&name).is_none());
        let field = if nullable && matches!(field.data_type(), DataType::Union(..)) {
            let Some(logical) = document_relayout_type(&field)? else { return Ok(None) };
            to_field(name, &logical.maybe())?
        } else {
            field.with_nullable(nullable)
        };
        output_layout.push(field);
    }
    let mut fields = Vec::with_capacity(output_layout.len());
    let mut columns = Vec::with_capacity(output_layout.len());
    for field in output_layout {
        let union = matches!(field.data_type(), DataType::Union(..));
        let inputs: Vec<_> = pages.iter().map(|page| {
            page.fields().iter().zip(page.columns()).find(|(source, _)| source.name() == field.name())
                .filter(|(source, _)| union || source.data_type() != &DataType::Null || field.data_type() == &DataType::Null)
        }).collect();
        let logical = from_field(&field)?;
        let bound = pages.iter().zip(&inputs).try_fold(4096usize, |bytes, (page, column)| {
            let size = match column {
                Some((source, column)) => {
                    let size = column.get_array_memory_size();
                    if source.data_type() == field.data_type() { size } else {
                        size.checked_add(rtdb_columnar::arrow_relayout_bound(column.as_ref(), &field)?).ok_or_else(overflow)?
                    }
                }
                None => estimate_builder_bytes_refs(&logical, std::iter::repeat_n(&Value::Null, page.len()))?,
            };
            size.checked_mul(4).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
        })?;
        columns.push(arrays.column(bound, || {
            let inputs = pages.iter().zip(inputs).map(|(page, column)| {
                match column {
                    Some((source, column)) if source.data_type() == field.data_type() => Ok(Arc::clone(column)),
                    Some((source, column)) => rtdb_columnar::relayout_arrow_column(Arc::clone(column), source, &field).map(|(_, column)| column),
                    None if union => {
                        let null = Field::new(field.name(), DataType::Null, true);
                        let array = Arc::new(arrow_array::NullArray::new(page.len()));
                        rtdb_columnar::relayout_arrow_column(array, &null, &field).map(|(_, column)| column)
                    }
                    None => Ok(arrow_array::new_null_array(field.data_type(), page.len())),
                }
            }).collect::<Result<Vec<_>>>()?;
            let inputs = inputs.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
            arrow_select::concat::concat(&inputs).map_err(|error| invalid(error.to_string()))
        })?);
        fields.push(Arc::new(field));
    }
    let structs = rtdb_columnar::build_struct_array(fields.into(), &columns, *offsets.last().unwrap() as usize, None)?;
    document_list(structs, offsets).map(Some)
}

fn merge_document_fields(left: &Field, right: &Field) -> Result<Option<Field>> {
    let (Some(left), Some(right_type)) = (document_relayout_type(left)?, document_relayout_type(right)?) else {
        return Ok(None);
    };
    let joint = left.join(&right_type);
    if !document_relayout_supported(&joint) { return Ok(None); }
    to_field(right.name(), &joint).map(Some)
}

fn document_relayout_type(field: &Field) -> Result<Option<LogicalType>> {
    fn supported_field(field: &Field) -> bool {
        field.metadata().is_empty() && match field.data_type() {
            DataType::Struct(fields) => fields.iter().all(|field| supported_field(field)),
            DataType::List(field) => supported_field(field),
            DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, field)| {
                field.metadata().is_empty() && (!field.is_nullable() || field.data_type() == &DataType::Null)
            }),
            _ => true,
        }
    }
    if !supported_field(field) { return Ok(None); }
    let logical = from_field(field)?;
    Ok(document_relayout_supported(&logical).then_some(logical))
}

fn document_relayout_supported(logical: &LogicalType) -> bool {
    match logical {
        LogicalType::Maybe(value) => document_relayout_supported(value),
        LogicalType::Struct(fields) => fields.values().all(document_relayout_supported),
        LogicalType::List { kind: rtdb_types::ListKind::List, element } => document_relayout_supported(element),
        LogicalType::Union(legs) => legs.iter().all(|leg| matches!(leg, LogicalType::Scalar(_) | LogicalType::Null)),
        LogicalType::Scalar(_) | LogicalType::Null | LogicalType::Nothing => true,
        _ => false,
    }
}

fn row_document_leg(lists: Vec<&Documents>, arrays: &mut BatchBuilder) -> Result<ArrayRef> {
    let mut fields = BTreeMap::<&str, (LogicalType, usize)>::new();
    let mut order = Vec::new();
    let mut offsets = vec![0i32];
    let mut length = 0usize;
    for list in &lists {
        for name in list.columns() {
            let declared = list.declared_types().and_then(|columns| columns.get(name));
            if let std::collections::btree_map::Entry::Vacant(field) = fields.entry(name.as_str()) {
                field.insert((declared.cloned().unwrap_or(LogicalType::Nothing), 0));
                order.push(name.as_str());
            } else if let Some(declared) = declared {
                let (ty, _) = fields.get_mut(name.as_str()).expect("ordered document column has a type");
                if ty != declared { *ty = ty.join(declared); }
            }
        }
        for document in list.rows() {
            for (name, value) in document {
                let (ty, present) = fields.get_mut(name.as_str()).expect("document fields have column names");
                let actual = value.logical_type();
                if *ty != actual { *ty = ty.join(&actual); }
                *present = present.checked_add(1).ok_or_else(overflow)?;
            }
        }
        length = length.checked_add(list.rows().len()).ok_or_else(overflow)?;
        offsets.push(i32::try_from(length).map_err(|_| overflow())?);
    }

    let mut columns = Vec::new();
    let mut schema = Vec::new();
    for name in order {
        let (ty, present) = fields.remove(name).expect("ordered document column has a type");
        let ty = if present < length || present == 0 { ty.maybe() } else { ty };
        let cells = lists.iter().flat_map(|list| list.rows()).map(|document| document.get(name).map_or(WriterCell::Null, WriterCell::of)).collect::<Vec<_>>();
        let ty = backfilled_nullability(ty, &cells);
        let values = || lists.iter().flat_map(|list| list.rows()).map(|document| struct_field_cell(document, name, &ty));
        let estimate = estimate_builder_bytes_refs(&ty, values())?;
        let array = arrays.column(estimate, || copy_array(&ty, &values().cloned().collect::<Vec<_>>()))?;
        columns.push(array);
        schema.push(Arc::new(to_field(name, &ty)?));
    }
    let structs = rtdb_columnar::build_struct_array(schema.into(), &columns, length, None).map_err(|error| invalid(error.to_string()))?;
    document_list(structs, offsets)
}

fn document_list(structs: ArrayRef, offsets: Vec<i32>) -> Result<ArrayRef> {
    let field = Arc::new(Field::new("$data$", structs.data_type().clone(), false));
    Ok(Arc::new(ListArray::try_new(field, OffsetBuffer::new(ScalarBuffer::from(offsets)), structs, None).map_err(|error| invalid(error.to_string()))?))
}

/// One row of a document column as the reference's struct writers see it.
#[derive(Clone, Copy)]
enum WriterCell<'a> { Undefined, Null, Value(&'a Value) }

impl<'a> WriterCell<'a> {
    fn of(value: &'a Value) -> Self { match value { Value::Null => Self::Null, Value::Undefined => Self::Undefined, value => Self::Value(value) } }
    fn fields(&self) -> Option<&'a StructFields<Value>> { match self { Self::Value(Value::Struct(fields)) => Some(fields), _ => None } }
}

/// The reference's writer backfills a struct key first written at row `r` into the struct's earlier rows: undefined where
/// `isNull` holds, null otherwise, and a null makes the field nullable. A non-nullable struct's undefined rows - written
/// because its own parent was null there - do not report `isNull`, so a key first seen after them becomes nullable although
/// its rows only lacked it (xtdb #4988). Union legs start empty and so have nothing to backfill; only single-typed struct
/// chains are adjusted, and only ever towards nullable.
fn backfilled_nullability(ty: LogicalType, cells: &[WriterCell<'_>]) -> LogicalType {
    let (nullable, inner) = match ty { LogicalType::Maybe(inner) => (true, *inner), other => (false, other) };
    let LogicalType::Struct(children) = inner else { return if nullable { inner.maybe() } else { inner }; };
    if !cells.iter().all(|cell| !matches!(cell, WriterCell::Value(value) if !matches!(value, Value::Struct(_)))) {
        let inner = LogicalType::Struct(children);
        return if nullable { inner.maybe() } else { inner };
    }
    let adjusted = children.into_iter().map(|(name, child)| {
        let first = cells.iter().position(|cell| cell.fields().is_some_and(|fields| fields.contains_key(&name)));
        let child_cells = match first {
            None => return (name, child),
            Some(first) => {
                let parent_nullable = cells[..first].iter().any(|cell| matches!(cell, WriterCell::Null));
                cells.iter().enumerate().map(|(row, cell)| match (row < first, cell) {
                    (_, WriterCell::Value(Value::Struct(fields))) => fields.get(&name).map_or(WriterCell::Null, WriterCell::of),
                    (true, WriterCell::Undefined) if !parent_nullable => WriterCell::Null,
                    (_, WriterCell::Undefined | WriterCell::Null) => WriterCell::Undefined,
                    (_, WriterCell::Value(_)) => WriterCell::Undefined,
                }).collect::<Vec<_>>()
            }
        };
        let child = backfilled_nullability(child, &child_cells);
        let child = if child_cells.iter().any(|cell| matches!(cell, WriterCell::Null)) && !matches!(child, LogicalType::Maybe(_)) { child.maybe() } else { child };
        (name, child)
    }).collect();
    let inner = LogicalType::Struct(adjusted);
    if nullable { inner.maybe() } else { inner }
}

struct SourceOperationReader<'a> {
    memory: &'a Arc<MemoryBudget>,
    parsed_fields: &'a mut MemoryReservation,
    layouts: BTreeMap<usize, Arc<DecodedDocumentLayout>>,
}

impl SourceOperationReader<'_> {
    fn operation(&mut self, union: &UnionArray, row: usize) -> Result<Operation> {
        let memory = self.memory;
        let parsed_fields = &mut *self.parsed_fields;
        let (name, array, offset) = union_leg(union, row)?;
        let base = parsed_fields.bytes().checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(overflow)?;
        if name == "xtql" || name == "call" {
            parsed_fields.grow_to(base)?;
            return Ok(Operation::Removed(name));
        }
        let body = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Transaction operation leg is not a struct"))?;
        let value_fields: &[&str] = match name.as_str() {
            "sql" => &["query", "args"],
            "put-docs" | "patch-docs" => &["iids", "_valid_from", "_valid_to"],
            "delete-docs" => &["table", "iids", "_valid_from", "_valid_to"],
            "erase-docs" => &["table", "iids"],
            _ => &[],
        };
        let bound = body.fields().iter().zip(body.columns()).filter(|(field, _)| value_fields.contains(&field.name().as_str()))
            .try_fold(base, |bytes, (field, column)| bytes.checked_add(estimate_read_value_bytes(field, column.as_ref(), offset)?).ok_or_else(overflow))?;
        parsed_fields.grow_to(bound)?;
        let value = |name| struct_cell(body, name, offset);
        let operation = match name.as_str() {
            "sql" => {
                let sql = match value("query")? { Value::Utf8(sql) => sql, _ => return Err(invalid("SQL operation has no query")) };
                let args = match value("args")? { Value::Null | Value::Undefined => None, Value::Binary(bytes) => Some(decode_arguments(&bytes, memory)?), _ => return Err(invalid("SQL arguments are not IPC bytes")) };
                TxOp::Sql { sql, args }
            }
            "put-docs" | "patch-docs" => {
                let docs_array = body.column_by_name("documents").ok_or_else(|| invalid("Document operation has no documents"))?;
                let docs_union = docs_array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Document tables are not a union"))?;
                let (table, docs_array, docs_offset) = union_leg(docs_union, offset)?;
                let table = parse_table(&table)?;
                let lists = docs_array.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Documents are not a list"))?;
                if lists.is_null(docs_offset) { return Err(invalid("Documents are not a list")); }
                let rows = lists.value(docs_offset);
                let structs = rows.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Document is not a struct"))?;
                let valid_from = optional_timestamp(value("_valid_from")?)?;
                let valid_to = optional_timestamp(value("_valid_to")?)?;
                let iids = match value("iids")? { Value::Null | Value::Undefined => None, values => Some(iids(values)?) };
                if iids.as_ref().is_some_and(|iids| iids.len() != structs.len()) { return Err(invalid("Document and IID counts differ")); }
                let identity = structs.fields().as_ptr() as usize;
                let layout = match self.layouts.entry(identity) {
                    std::collections::btree_map::Entry::Occupied(layout) => Arc::clone(layout.get()),
                    std::collections::btree_map::Entry::Vacant(layout) => Arc::clone(layout.insert(DecodedDocumentLayout::new(structs.fields(), memory)?)),
                };
                if name == "put-docs" && iids.is_some()
                    && let Some(documents) = ArrowDocuments::from_source(structs, Arc::clone(&layout)) {
                    return Ok(Operation::PutArrowIids { table, documents, iids: iids.expect("put contains IIDs"), valid_from, valid_to });
                }
                let documents = Documents::read_arrow(structs, layout, name == "put-docs", memory)?;
                if name == "put-docs" {
                    if let Some(iids) = iids { return Ok(Operation::PutIids { table, documents, iids, valid_from, valid_to }); }
                    TxOp::PutDocs { table, documents, valid_from, valid_to }
                } else {
                    if let Some(iids) = iids { return Ok(Operation::PatchIids { table, documents, iids, valid_from, valid_to }); }
                    return Err(invalid("Patch operation has no IIDs"));
                }
            }
            "delete-docs" | "erase-docs" => {
                let table = match value("table")? { Value::Utf8(table) => parse_table(&table)?, _ => return Err(invalid("Delete/erase has no table")) };
                let ids = EntityIds::new(iids(value("iids")?)?.into_iter().map(|iid| EntityId::Uuid(iid.0)).collect(), memory)?;
                if name == "delete-docs" { TxOp::DeleteDocs { table, ids, valid_from: optional_timestamp(value("_valid_from")?)?, valid_to: optional_timestamp(value("_valid_to")?)? } }
                else { TxOp::EraseDocs { table, ids } }
            }
            _ => return Err(invalid(format!("Unknown transaction operation {name}"))),
        };
        Ok(Operation::Public(operation))
    }
}

pub(crate) fn encode_arguments(arguments: &SqlArguments, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    if let Some(pages) = arguments.uniform_arrow_pages() {
        let first = pages[0];
        let _reservation = memory.reserve(pages.iter().try_fold(4096usize, |bytes, page| {
            page.get_array_memory_size().checked_mul(4).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
        })?)?;
        let fields = first.schema_ref().fields().iter().enumerate().map(|(index, field)| {
            field.as_ref().clone().with_name(format!("?_{index}"))
        }).collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let combined;
        let page = if pages.len() == 1 {
            first
        } else {
            combined = arrow_select::concat::concat_batches(first.schema_ref(), pages).map_err(|error| invalid(error.to_string()))?;
            &combined
        };
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(page.num_rows()));
        let page = RecordBatch::try_new_with_options(Arc::clone(&schema), page.columns().to_vec(), &options)
            .map_err(|error| invalid(error.to_string()))?;
        return crate::ipc::encode(&schema, [&page], memory);
    }
    let rows = arguments.rows().collect::<Result<Vec<_>>>()?;
    let bound = arguments.types().iter().enumerate().try_fold(4096usize, |bytes, (index, ty)| {
        bytes.checked_add(rtdb_columnar::estimate_builder_bytes_refs(ty, rows.iter().map(|row| &row[index]))?).ok_or_else(overflow)
    })?;
    let reservation = memory.reserve(bound)?;
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (index, ty) in arguments.types().iter().enumerate() {
        fields.push(to_field(&format!("?_{index}"), ty)?);
        arrays.push(copy_array(ty, &rows.iter().map(|row| row[index].clone()).collect::<Vec<_>>())?);
    }
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(arguments.row_count()));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).map_err(|error| invalid(error.to_string()))?;
    let owned = OwnedBatch::with_reservation(batch, reservation)?;
    crate::ipc::encode(owned.batch().schema().as_ref(), [owned.batch()], memory)
}

pub(crate) fn decode_arguments(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<SqlArguments> {
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let stream = rtdb_format::ipc::OwnedStream::read(bytes, memory)?;
    let schema = Arc::clone(stream.schema());
    for (index, field) in schema.fields().iter().enumerate() {
        if !field.name().is_empty() && field.name() != &format!("${}", index + 1) && field.name() != &format!("?_{}", index) {
            return Err(Anomaly::incorrect("xtdb/invalid-param-names", "SQL argument fields must follow positional order"));
        }
    }
    let mut pages = stream.pages().iter();
    let Some(first) = pages.next() else {
        let types = schema.fields().iter().map(|field| from_field(field)).collect::<Result<Vec<_>>>()?;
        return SqlArguments::new(types, Vec::new(), memory);
    };
    if first.batch().schema() != schema { return Err(invalid("SQL argument stream changes schema")); }
    let mut arguments = SqlArguments::from_arrow(first.clone(), memory)?;
    for page in pages {
        if page.batch().schema() != schema { return Err(invalid("SQL argument stream changes schema")); }
        arguments.append_page(SqlArguments::from_arrow(page.clone(), memory)?)?;
    }
    Ok(arguments)
}

fn encode_metadata(metadata: &UserMetadata, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    let ty = metadata.value().logical_type().maybe();
    let reservation = memory.reserve(estimate_builder_bytes(&ty, std::slice::from_ref(metadata.value()))?)?;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("user-metadata", &ty)?])), vec![build_array(&ty, std::slice::from_ref(metadata.value()))?]).map_err(|error| invalid(error.to_string()))?;
    let owned = OwnedBatch::with_reservation(batch, reservation)?;
    crate::ipc::encode(owned.batch().schema().as_ref(), [owned.batch()], memory)
}
fn decode_metadata(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<UserMetadata> {
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let pages = crate::ipc::decode(bytes, memory)?;
    if pages.len() != 1 || pages[0].batch().num_rows() != 1 { return Err(invalid("User metadata must contain one row")); }
    UserMetadata::new(cell(pages[0].batch(), "user-metadata", 0)?, memory)
}
fn union_leg(union: &UnionArray, row: usize) -> Result<(String, ArrayRef, usize)> {
    let id = union.type_id(row);
    let DataType::Union(fields, UnionMode::Dense) = union.data_type() else { return Err(invalid("Transaction union must be dense")); };
    let name = fields.iter().find_map(|(type_id, field)| (type_id == id).then(|| field.name().clone())).ok_or_else(|| invalid("Union type ID has no field"))?;
    Ok((name, Arc::clone(union.child(id)), union.value_offset(row)))
}
fn struct_cell(body: &StructArray, name: &str, row: usize) -> Result<Value> {
    let index = body.fields().iter().position(|field| field.name() == name).ok_or_else(|| invalid(format!("Operation is missing {name}")))?;
    read_value(&body.fields()[index], body.column(index).as_ref(), row)
}
fn cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Value> {
    let schema = batch.schema();
    let index = schema.index_of(name).map_err(|error| invalid(error.to_string()))?;
    read_value(schema.field(index), batch.column(index).as_ref(), row)
}
fn optional_cell(batch: &RecordBatch, name: &str, row: usize) -> Result<Value> { if batch.schema().index_of(name).is_ok() { cell(batch, name, row) } else { Ok(Value::Null) } }
fn optional_timestamp(value: Value) -> Result<Option<Timestamp>> {
    match value {
        Value::Null | Value::Undefined => Ok(None),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(_) } => Ok(Some(Timestamp(value))),
        _ => Err(invalid("Transaction valid time must be a zoned microsecond timestamp")),
    }
}
fn optional_string(value: Value) -> Result<Option<String>> { match value { Value::Null | Value::Undefined => Ok(None), Value::Utf8(value) => Ok(Some(value)), _ => Err(invalid("Expected optional string")) } }
fn iids(value: Value) -> Result<Vec<Iid>> {
    let Value::List(values) = value else { return Err(invalid("IIDs must be a list")); };
    values.into_iter().map(|value| match value { Value::FixedSizeBinary(bytes) => Iid::from_slice(&bytes), _ => Err(invalid("IID must be fixed-size binary")) }).collect()
}
fn time_fields(body: &mut StructFields<Value>, from: Option<Timestamp>, to: Option<Timestamp>) {
    let time = |time: Option<Timestamp>| time.map(|time| Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).unwrap_or(Value::Null);
    body.insert("_valid_from".into(), time(from)); body.insert("_valid_to".into(), time(to));
}
pub(crate) fn table_name(table: &TableRef) -> String { format!("{}/{}", table.schema, table.table) }
pub(crate) fn is_reserved_write_schema(schema: &str) -> bool {
    matches!(schema, "xt" | "information_schema" | "pg_catalog")
}
fn check_writable_table(operation: &Operation) -> Result<()> {
    let table = match operation {
        Operation::Public(TxOp::PutDocs { table, .. } | TxOp::PutArrowDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. })
        | Operation::PutIids { table, .. } | Operation::PutArrowIids { table, .. } | Operation::PatchIids { table, .. } => table,
        _ => return Ok(()),
    };
    parse_table(&table_name(table))?;
    if is_reserved_write_schema(&table.schema) {
        return Err(Anomaly::incorrect("xtdb/forbidden-table", format!("Cannot write to table: {table}"))
            .with_context("schema", table.schema.clone().into()).with_context("table", table.table.clone().into()));
    }
    Ok(())
}
fn parse_table(name: &str) -> Result<TableRef> {
    let (schema, table) = name.split_once('/').ok_or_else(|| invalid("Source table must use schema/table"))?;
    if schema.is_empty() || table.is_empty() || table.contains('/') { return Err(invalid("Invalid source table name")); }
    Ok(TableRef::new(schema, table))
}
fn overflow() -> Anomaly { invalid("Transaction allocation exceeds address space") }
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.tx/invalid-source-transaction", message) }

#[cfg(test)]
mod tests {
    use super::*;

    fn budget() -> Arc<MemoryBudget> { MemoryBudget::new(128 * 1024 * 1024) }
    fn roundtrip(source: &SourceTransaction) -> SourceTransaction {
        let encoded = source.encode().unwrap();
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        SourceTransaction::decode(&message, &source.memory).unwrap()
    }

    #[test]
    fn arrow_sql_arguments_keep_their_buffers_and_replay_with_positional_fields() {
        let memory = budget();
        let schema = Arc::new(Schema::new(vec![Field::new("client_name", DataType::UInt64, true)]));
        let arguments = |values| {
            let column = Arc::new(arrow_array::UInt64Array::from(values));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column]).unwrap();
            let data = batch.column(0).to_data();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let arguments = SqlArguments::from_arrow(batch, &memory).unwrap();
            let pages = arguments.uniform_arrow_pages().unwrap();
            assert_eq!(pages[0].column(0).to_data().buffers()[0].as_ptr(), data.buffers()[0].as_ptr());
            arguments
        };
        let mut arguments = arguments(vec![Some(u64::MAX), None]);
        let other = RecordBatch::try_new(schema, vec![Arc::new(arrow_array::UInt64Array::from(vec![5]))]).unwrap();
        arguments.append(SqlArguments::from_arrow(OwnedBatch::new(other, &memory).unwrap(), &memory).unwrap()).unwrap();
        let source = SourceTransaction::new(
            vec![TxOp::Sql { sql: "UPDATE docs SET v = ?".into(), args: Some(arguments) }],
            SubmitOptions::default(),
            &memory,
        ).unwrap();

        let replayed = roundtrip(&source);

        let [Operation::Public(TxOp::Sql { args: Some(arguments), .. })] = replayed.operations.as_slice() else {
            panic!("Expected SQL arguments");
        };
        let pages = arguments.uniform_arrow_pages().unwrap();
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].schema_ref().field(0).name(), "?_0");
        assert_eq!(pages[0].schema_ref().field(0).data_type(), &DataType::UInt64);
        let rows: Vec<_> = arguments.indexed_rows().map(|row| {
            let (index, values) = row.unwrap();
            (index, values[0].clone())
        }).collect();
        assert_eq!(rows, [(0, Value::U64(u64::MAX)), (1, Value::Null), (2, Value::U64(5))]);
    }

    #[test]
    fn an_arrow_put_matches_the_source_codec_for_the_same_documents() {
        let memory = budget();
        let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, true), Field::new("v", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
            Arc::new(arrow_array::StringArray::from(vec![Some("text"), None])),
        ]).unwrap();
        let table = TableRef::new("public", "docs");
        let rows = vec![
            StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("text".into()))]),
            StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::Null)]),
        ];
        let row_put = SourceTransaction::new(vec![TxOp::PutDocs {
            table: table.clone(), documents: Documents::new(rows, &memory).unwrap(),
            valid_from: None, valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
        let arrow_put = SourceTransaction::new(vec![TxOp::PutArrowDocs { table, documents }], SubmitOptions::default(), &memory).unwrap();

        assert_eq!(row_put.encode().unwrap().as_slice(), arrow_put.encode().unwrap().as_slice());
        let replayed = roundtrip(&arrow_put);
        let [Operation::PutArrowIids { documents, iids, .. }] = replayed.operations.as_slice() else { panic!("expected columnar replay"); };
        assert_eq!(documents.array().len(), 2);
        assert_eq!(iids, &[EntityId::Integer(1).iid(), EntityId::Integer(2).iid()]);
    }

    #[test]
    fn arrow_document_batches_combine_missing_columns_in_first_seen_order() {
        let memory = budget();
        let first = StructArray::try_new(
            vec![Field::new("a", DataType::Utf8, false), Field::new("_id", DataType::Int64, false)].into(),
            vec![Arc::new(arrow_array::StringArray::from(vec!["left"])), Arc::new(arrow_array::Int64Array::from(vec![1]))],
            None,
        ).unwrap();
        let second = StructArray::try_new(
            vec![Field::new("_id", DataType::Int64, false), Field::new("b", DataType::Boolean, false)].into(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![2])), Arc::new(arrow_array::BooleanArray::from(vec![true]))],
            None,
        ).unwrap();
        let mut arrays = BatchBuilder::new(4096, &memory).unwrap();

        let result = arrow_document_leg(&[&first, &second], &mut arrays).unwrap().unwrap();

        let lists = result.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 1, 2]);
        let values = lists.values().as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(values.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["a", "_id", "b"]);
        assert_eq!(read_value(&values.fields()[0], values.column(0).as_ref(), 0).unwrap(), Value::Utf8("left".into()));
        assert!(values.column(0).is_null(1));
        assert!(values.column(2).is_null(0));
        assert_eq!(read_value(&values.fields()[2], values.column(2).as_ref(), 1).unwrap(), Value::Bool(true));
    }

    #[test]
    fn arrow_puts_with_different_scalar_types_replay_as_dense_unions() {
        let memory = budget();
        let put = |id, value: ArrayRef| {
            let schema = Arc::new(Schema::new(vec![
                Field::new("_id", DataType::Int64, false),
                Field::new("v", value.data_type().clone(), false),
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(arrow_array::Int64Array::from(vec![id])), value]).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: TableRef::new("public", "docs"), documents }
        };
        let source = SourceTransaction::new(vec![
            put(1, Arc::new(arrow_array::Int64Array::from(vec![7]))),
            put(2, Arc::new(arrow_array::StringArray::from(vec!["text"]))),
        ], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        let expected = [Value::I64(7), Value::Utf8("text".into())];
        for (operation, expected) in replay.operations.iter().zip(expected) {
            let Operation::PutArrowIids { documents, .. } = operation else {
                panic!("Expected Arrow documents on replay");
            };
            let array = documents.array().column_by_name("v").unwrap();
            let field = documents.array().fields().iter().find(|field| field.name() == "v").unwrap();
            assert!(array.as_any().is::<UnionArray>());
            assert_eq!(read_value(field, array.as_ref(), 0).unwrap(), expected);
        }
    }

    #[test]
    fn mixed_arrow_scalar_layouts_keep_null_and_missing_values_in_the_union() {
        let memory = budget();
        let table = TableRef::new("public", "docs");
        let put = |ids: Vec<i64>, column: Option<ArrayRef>| {
            let mut fields = vec![Field::new("_id", DataType::Int64, false)];
            let mut columns = vec![Arc::new(arrow_array::Int64Array::from(ids)) as ArrayRef];
            if let Some(column) = column {
                fields.push(Field::new("v", column.data_type().clone(), true));
                columns.push(column);
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: table.clone(), documents }
        };
        let source = SourceTransaction::new(vec![
            put(vec![1, 2, 3], Some(Arc::new(arrow_array::Int64Array::from(vec![Some(7), None, Some(9)])))),
            put(vec![4], Some(Arc::new(arrow_array::StringArray::from(vec!["text"])))),
            put(vec![5], None),
            put(vec![6], Some(Arc::new(arrow_array::NullArray::new(1)))),
        ], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        let expected = [vec![Value::I64(7), Value::Null, Value::I64(9)], vec![Value::Utf8("text".into())], vec![Value::Null], vec![Value::Null]];
        assert_eq!(replay.operations.len(), expected.len());
        for (operation, expected) in replay.operations.iter().zip(expected) {
            let Operation::PutArrowIids { documents, .. } = operation else {
                panic!("Expected Arrow documents on replay");
            };
            let array = documents.array().column_by_name("v").unwrap();
            let field = documents.array().fields().iter().find(|field| field.name() == "v").unwrap();
            assert!(array.as_any().is::<UnionArray>());
            let values = (0..array.len()).map(|row| read_value(field, array.as_ref(), row).unwrap()).collect::<Vec<_>>();
            assert_eq!(values, expected);
        }
    }

    #[test]
    fn evolving_structs_inside_arrow_lists_replay_columnarly() {
        let memory = budget();
        let put = |id, name: &str, number| {
            let list = Value::List(vec![Value::Struct(StructFields::from([
                (name.into(), Value::Bool(true)), ("number".into(), number),
            ]))]);
            let logical = list.logical_type();
            let fields = vec![Field::new("_id", DataType::Int64, false), to_field("items", &logical).unwrap()];
            let columns = vec![
                Arc::new(arrow_array::Int64Array::from(vec![id])) as ArrayRef,
                build_array(&logical, &[list]).unwrap(),
            ];
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
            let batch = OwnedBatch::new(batch, &memory).unwrap();
            let documents = ArrowDocuments::from_batch(&batch, &memory).unwrap().unwrap();
            TxOp::PutArrowDocs { table: TableRef::new("public", "docs"), documents }
        };
        let source = SourceTransaction::new(vec![
            put(1, "first", Value::I64(7)), put(2, "later", Value::Utf8("text".into())),
        ], SubmitOptions::default(), &memory).unwrap();

        let replay = roundtrip(&source);

        let expected = [(true, false, Value::I64(7)), (false, true, Value::Utf8("text".into()))];
        assert_eq!(replay.operations.len(), expected.len());
        for (operation, (first, later, number)) in replay.operations.iter().zip(expected) {
            let Operation::PutArrowIids { documents, .. } = operation else {
                panic!("Expected Arrow documents on replay");
            };
            let list = documents.array().column_by_name("items").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
            let item = list.value(0);
            let item = item.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(item.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["first", "number", "later"]);
            assert_eq!(item.column(0).is_valid(0), first);
            assert_eq!(item.column(2).is_valid(0), later);
            assert!(item.column(1).as_any().is::<UnionArray>());
            assert_eq!(read_value(&item.fields()[1], item.column(1).as_ref(), 0).unwrap(), number);
        }
    }

    #[test]
    fn a_nested_struct_key_first_written_after_its_struct_was_undefined_is_nullable() {
        let memory = budget();
        let put = |row: StructFields<Value>| TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![row], &memory).unwrap(), valid_from: None, valid_to: None };
        let quux = StructFields::from([("_id".to_owned(), Value::I64(2)), ("struct".to_owned(), Value::Struct(StructFields::from([
            ("a".to_owned(), Value::Bool(true)),
            ("b".to_owned(), Value::Struct(StructFields::from([("c".to_owned(), Value::Utf8("c".into())), ("d".to_owned(), Value::Utf8("d".into()))]))),
        ])))]);
        let source = SourceTransaction::new(vec![put(StructFields::from([("_id".to_owned(), Value::I64(1))])), put(quux)], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        let Operation::PutArrowIids { documents, .. } = &decoded.operations[0] else { panic!("put"); };
        let documents = documents.to_rows(true, &memory).unwrap();
        let utf8 = LogicalType::Scalar(ScalarType::Utf8);
        let expected = LogicalType::Struct(StructFields::from([
            ("a".to_owned(), LogicalType::Scalar(ScalarType::Bool)),
            ("b".to_owned(), LogicalType::Struct(StructFields::from([("c".to_owned(), utf8.clone().maybe()), ("d".to_owned(), utf8.maybe())]))),
        ])).maybe();
        assert_eq!(documents.declared_types().unwrap()["struct"], expected);
    }

    #[test]
    fn document_union_keeps_first_seen_column_order_per_table_through_replay() {
        let memory = budget();
        let operation = |table, id, names: &[&str], cells: &[(&str, Value)]| {
            let mut row = StructFields::from([("_id".to_owned(), Value::I64(id))]);
            row.extend(cells.iter().map(|(name, value)| ((*name).to_owned(), value.clone())));
            let columns = names.iter().map(|name| (*name).to_owned()).collect();
            TxOp::PutDocs {
                table: TableRef::new("public", table),
                documents: Documents::with_column_order(vec![row.into()], columns, &memory).unwrap(),
                valid_from: None,
                valid_to: None,
            }
        };
        let source = SourceTransaction::new(vec![
            operation("a", 1, &["z", "_id", "n"], &[("z", Value::Utf8("last".into())), ("n", Value::Null)]),
            operation("b", 2, &["_id", "e"], &[]),
            operation("a", 3, &["b", "_id", "z"], &[("b", Value::Bool(true)), ("z", Value::I64(7))]),
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        let replayed = roundtrip(&decoded);
        for tx in [&decoded, &replayed] {
            let mut operations = tx.operations.iter();
            let Operation::PutArrowIids { documents, .. } = operations.next().unwrap() else { panic!("first put"); };
            let documents = documents.to_rows(true, &memory).unwrap();
            assert_eq!(documents.columns(), ["z", "_id", "n", "b"]);
            assert_eq!(documents.rows()[0]["z"], Value::Utf8("last".into()));
            assert_eq!(documents.declared_types().unwrap()["n"], LogicalType::Null);
            assert!(!documents.rows()[0].contains_key("n"));
            let Operation::PutArrowIids { documents, .. } = operations.next().unwrap() else { panic!("second put"); };
            let documents = documents.to_rows(true, &memory).unwrap();
            assert_eq!(documents.columns(), ["_id", "e"]);
            assert_eq!(documents.declared_types().unwrap()["e"], LogicalType::Null);
            assert!(!documents.rows()[0].contains_key("e"));
            let Operation::PutArrowIids { documents, .. } = operations.next().unwrap() else { panic!("third put"); };
            let documents = documents.to_rows(true, &memory).unwrap();
            assert_eq!(documents.columns(), ["z", "_id", "n", "b"]);
            assert_eq!(documents.rows()[0]["z"], Value::I64(7));
            assert_eq!(documents.rows()[0]["b"], Value::Bool(true));
        }
    }

    #[test]
    fn source_writes_sparse_document_columns_with_one_column_of_builder_scratch() {
        let memory = MemoryBudget::new(1 << 27);
        let rows = (0..100).map(|id| {
            let mut row = StructFields::from([("_id".into(), Value::I64(id))]);
            for index in 0..10 {
                row.insert(format!("field_{id}_{index}"), Value::Utf8("value".into()));
            }
            row
        }).collect();
        let tx = SourceTransaction::new(vec![TxOp::PutDocs {
            table: TableRef::new("public", "wide_docs"),
            documents: Documents::new(rows, &memory).unwrap(),
            valid_from: None,
            valid_to: None,
        }], SubmitOptions::default(), &memory).unwrap();
        let encoded = tx.encode().unwrap();
        assert!(encoded.as_slice().len() < 1 << 20);
        assert!(memory.peak_bytes() < 1 << 27);
    }

    #[test]
    fn source_reads_sparse_documents_with_one_row_of_decode_scratch() {
        let memory = MemoryBudget::new(1 << 25);
        let operations = (0..100).map(|id| {
            let mut row = StructFields::from([("_id".into(), Value::I64(id))]);
            for index in 0..10 {
                row.insert(format!("field_{id}_{index}"), Value::Utf8(format!("value_{id}_{index}")));
            }
            TxOp::PutDocs {
                table: TableRef::new("public", "wide_docs"),
                documents: Documents::new(vec![row.into()], &memory).unwrap(),
                valid_from: None,
                valid_to: None,
            }
        }).collect();
        let source = SourceTransaction::new(operations, SubmitOptions::default(), &memory).unwrap();
        let encoded = source.encode().unwrap();
        drop(source);
        let message = SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let decoded = SourceTransaction::decode(&message, &memory).unwrap();
        assert_eq!(decoded.operations.len(), 100);
        let Operation::PutArrowIids { documents, .. } = &decoded.operations[99] else { panic!("Expected columnar decoded put"); };
        let documents = documents.to_rows(true, &memory).unwrap();
        assert_eq!(documents.columns().len(), 1001);
        assert_eq!(documents.rows()[0]["field_99_9"], Value::Utf8("value_99_9".into()));
        assert_eq!(documents.declared_types().unwrap()["field_0_0"], LogicalType::Scalar(ScalarType::Utf8).maybe());
        assert!(!documents.rows()[0].contains_key("field_0_0"));
    }

    #[test]
    fn ordered_documents_reject_duplicate_columns_and_unrepresented_fields() {
        let memory = budget();
        for names in [vec!["a", "a"], vec!["b"]] {
            let rows = vec![StructFields::from([("a".into(), Value::I64(1))])];
            let columns = names.into_iter().map(str::to_owned).collect();
            let error = Documents::with_column_order(rows, columns, &memory).unwrap_err();
            assert_eq!(error.code, "rtdb.tx/document-schema");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn replay_keeps_explicit_put_and_patch_iids_even_when_the_document_id_hash_differs() {
        let memory = budget();
        let documents = Documents::new(vec![StructFields::from([("_id".into(), Value::Utf8("different-id".into()))])], &memory).unwrap();
        let table = TableRef::new("public", "docs");
        let iid = Iid([42; 16]);
        assert_ne!(iid, EntityId::String("different-id".into()).iid());
        let source = SourceTransaction::from_operations(vec![
            Operation::PutIids { table: table.clone(), documents: documents.clone(), iids: vec![iid], valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)) },
            Operation::PatchIids { table, documents, iids: vec![iid], valid_from: Some(Timestamp(30)), valid_to: None },
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        assert!(matches!(&decoded.operations[0], Operation::PutArrowIids { iids, valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)), .. } if iids == &vec![iid]));
        assert!(matches!(&decoded.operations[1], Operation::PatchIids { iids, valid_from: Some(Timestamp(30)), valid_to: None, .. } if iids == &vec![iid]));
    }

    #[test]
    fn absent_empty_and_single_row_zero_column_sql_arguments_remain_distinct() {
        let memory = budget();
        let source = SourceTransaction::new(vec![
            TxOp::Sql { sql: "SELECT 1".into(), args: None },
            TxOp::Sql { sql: "SELECT 1".into(), args: Some(SqlArguments::new(vec![], vec![], &memory).unwrap()) },
            TxOp::Sql { sql: "SELECT 1".into(), args: Some(SqlArguments::new(vec![], vec![vec![]], &memory).unwrap()) },
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        assert!(matches!(&decoded.operations[0], Operation::Public(TxOp::Sql { args: None, .. })));
        assert!(matches!(&decoded.operations[1], Operation::Public(TxOp::Sql { args: Some(args), .. }) if args.row_count() == 0));
        assert!(matches!(&decoded.operations[2], Operation::Public(TxOp::Sql { args: Some(args), .. }) if args.row_count() == 1 && args.types().is_empty()));
    }

    #[test]
    fn raw_decimal_sql_arguments_retain_metadata_and_coefficients_through_the_source_codec() {
        let memory = budget();
        for precision in [i32::MIN, -1, 0, 1, 256, i32::MAX] {
            let value = Value::Decimal(rtdb_types::DecimalValue::new(18i128.to_be_bytes().to_vec(), precision, 129, 128).unwrap());
            let args = SqlArguments::new(vec![value.logical_type()], vec![vec![value.clone()]], &memory).unwrap();
            let source = SourceTransaction::new(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (2, ?)".into(), args: Some(args) }], SubmitOptions::default(), &memory).unwrap();
            let decoded = roundtrip(&source);
            let Operation::Public(TxOp::Sql { args: Some(args), .. }) = &decoded.operations[0] else { panic!("Expected SQL arguments") };
            assert_eq!(args.types(), &[value.logical_type()]);
            assert_eq!(args.rows().map(|row| row.unwrap().to_vec()).collect::<Vec<_>>(), vec![std::slice::from_ref(&value)]);
        }
    }

    #[test]
    fn empty_delete_and_erase_are_omitted_but_forbidden_tables_are_checked_first() {
        let memory = budget();
        let ids = EntityIds::new(vec![], &memory).unwrap();
        let source = SourceTransaction::new(vec![
            TxOp::DeleteDocs { table: TableRef::new("public", "docs"), ids: ids.clone(), valid_from: None, valid_to: None },
            TxOp::EraseDocs { table: TableRef::new("public", "docs"), ids: ids.clone() },
        ], SubmitOptions::default(), &memory).unwrap();
        assert!(roundtrip(&source).operations.is_empty());
        let source = SourceTransaction::new(vec![TxOp::EraseDocs { table: TableRef::new("xt", "txs"), ids }], SubmitOptions::default(), &memory).unwrap();
        let error = source.encode().unwrap_err();
        assert_eq!(error.code, "xtdb/forbidden-table");
        assert_eq!(error.message.as_deref(), Some("Cannot write to table: xt.txs"));
        assert_eq!(error.context["schema"], "xt");
        assert_eq!(error.context["table"], "txs");
    }

    #[test]
    fn empty_metadata_bytes_are_absent_and_present_metadata_keeps_its_types() {
        let memory = budget();
        let value = Value::Struct(StructFields::from([("source".into(), Value::Utf8("client".into())), ("attempt".into(), Value::I64(2))]));
        let source = SourceTransaction::new(vec![], SubmitOptions { user_metadata: Some(UserMetadata::new(value.clone(), &memory).unwrap()), ..SubmitOptions::default() }, &memory).unwrap();
        assert_eq!(roundtrip(&source).options.user_metadata.unwrap().value(), &value);
        let mut message = SourceMessage::decode(source.encode().unwrap().as_slice()).unwrap().unwrap();
        let SourceMessage::Protobuf(SourceBody::Tx(tx)) = &mut message else { panic!("wrong envelope") };
        tx.user_metadata = Some(vec![]);
        assert!(SourceTransaction::decode(&message, &memory).unwrap().options.user_metadata.is_none());
    }
}
