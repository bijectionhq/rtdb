use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StructArray, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema, UnionFields, UnionMode};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, build_array, estimate_builder_bytes, from_field, read_value, to_field};
use rtdb_format::envelope::{SourceBody, SourceMessage};
use prost::Message;
use rtdb_types::{Anomaly, EntityId, Iid, LogicalType, Result, ScalarType, TableRef, TimeUnit, Timestamp, Value};

use crate::{Documents, EntityIds, RetainedBytes, SqlArguments, SubmitOptions, TxOp, UserMetadata};

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    Public(TxOp),
    PutIids { table: TableRef, documents: Documents, iids: Vec<Iid>, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
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
                Operation::Public(TxOp::PutDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. }) => (0, Some(table), 0),
                Operation::PutIids { table, iids, .. } | Operation::PatchIids { table, iids, .. } => (0, Some(table), iids.capacity().checked_mul(16).ok_or_else(overflow)?),
                Operation::Removed(name) => (name.capacity(), None, 0),
            };
            bytes = bytes.checked_add(text).and_then(|size| size.checked_add(ids)).and_then(|size| size.checked_add(table.map_or(0, |table| table.schema.capacity() + table.table.capacity()))).and_then(|size| size.checked_add(256)).ok_or_else(overflow)?;
        }
        let reservation = memory.reserve(bytes)?;
        Ok(Self { operations, options, memory: Arc::clone(memory), _reservation: reservation })
    }

    /// Decodes current flat tx operations and the legacy one-row envelope without expanding SQL.
    pub fn decode(message: &SourceMessage, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let operation_bytes = match message {
            SourceMessage::Protobuf(SourceBody::Tx(tx)) => tx.tx_ops.as_deref().ok_or_else(|| invalid("Transaction has no operations stream"))?,
            SourceMessage::LegacyTransaction(bytes) => bytes.as_slice(),
            _ => return Err(invalid("Source message is not a transaction")),
        };
        let _value_reservation = memory.reserve(crate::ipc::allocation_bound(operation_bytes)?)?;
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
        if legacy {
            if first.batch().num_rows() != 1 || pages.len() != 1 { return Err(invalid("Legacy transaction must have one envelope row")); }
            options.default_tz = match cell(first.batch(), "default-tz", 0)? { Value::Utf8(value) => value, _ => return Err(invalid("Legacy transaction has no timezone")) };
            options.system_time = optional_timestamp(cell(first.batch(), "system-time", 0)?)?;
            options.user = optional_string(optional_cell(first.batch(), "user", 0)?)?;
            let metadata = optional_cell(first.batch(), "user-metadata", 0)?;
            if !matches!(metadata, Value::Null | Value::Undefined) { options.user_metadata = Some(UserMetadata::new(metadata, memory)?); }
        }
        let mut operations = Vec::new();
        for page in &pages {
            let batch = page.batch();
            let column = batch.column(batch.schema().index_of("tx-ops").map_err(|error| invalid(error.to_string()))?);
            let array: ArrayRef = if legacy {
                let list = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Legacy tx-ops is not a list"))?;
                list.value(0)
            } else { Arc::clone(column) };
            let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Transaction operations are not a union"))?;
            for row in 0..union.len() { operations.push(decode_operation(union, row, memory)?); }
        }
        Self::from_operations(operations, options, memory)
    }

    /// Encodes the operations as supplied; submit-time SQL expansion is a separate caller operation.
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
                Operation::Public(TxOp::Sql { sql, args }) => {
                    let rows = args.as_ref().map(|args| args.rows().flatten().try_fold(0usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))).transpose()?.unwrap_or(0);
                    rows.checked_add(sql.capacity()).ok_or_else(overflow)?
                }
                Operation::Public(TxOp::SqlBytes { sql, arg_bytes }) => sql.capacity().checked_add(arg_bytes.as_slice().len()).ok_or_else(overflow)?,
                Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) => ids.ids().len().checked_mul(128).ok_or_else(overflow)?,
                Operation::Removed(name) => name.capacity(),
            };
            bytes.checked_add(extra.checked_mul(8).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow)?).ok_or_else(overflow)
        })?;
        let _scratch_reservation = self.memory.reserve(scratch)?;
        let mut bodies = Vec::new();
        let mut documents = Vec::new();
        for operation in &self.operations {
            check_writable_table(operation)?;
            if matches!(operation, Operation::Public(TxOp::DeleteDocs { ids, .. } | TxOp::EraseDocs { ids, .. }) if ids.ids().is_empty()) { continue; }
            let (tag, body, docs) = operation_value(operation, &self.memory)?;
            bodies.push((tag, Value::Struct(body)));
            documents.push(docs);
        }
        let mut bound = 65_536usize;
        for (_, value) in &bodies { bound = bound.checked_add(estimate_builder_bytes(&value.logical_type(), std::slice::from_ref(value))?).ok_or_else(overflow)?; }
        for docs in documents.iter().flatten() { bound = bound.checked_add(estimate_builder_bytes(&docs.1.logical_type(), std::slice::from_ref(&docs.1))?).ok_or_else(overflow)?; }
        let reservation = self.memory.reserve(bound)?;
        let union = named_union(&bodies, Some(&documents))?;
        let schema = Arc::new(Schema::new(vec![Field::new("tx-ops", union.data_type().clone(), false)]));
        let batch = RecordBatch::try_new(schema, vec![union]).map_err(|error| invalid(error.to_string()))?;
        OwnedBatch::with_reservation(batch, reservation)
    }
}

type DocumentValue = Option<(String, Value)>;
fn operation_value(operation: &Operation, memory: &Arc<MemoryBudget>) -> Result<(String, BTreeMap<String, Value>, DocumentValue)> {
    let mut body = BTreeMap::new();
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
            let iids = documents.rows().iter().map(|document| document.get("_id").ok_or_else(|| Anomaly::incorrect("missing-id", "missing '_id'"))?.entity_id().map(|id| Value::FixedSizeBinary(id.iid().0.to_vec()))).collect::<Result<_>>()?;
            body.insert("iids".into(), Value::List(iids));
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), Value::List(documents.rows().iter().cloned().map(Value::Struct).collect())));
            if matches!(operation, Operation::Public(TxOp::PatchDocs { .. })) { "patch-docs" } else { "put-docs" }
        }
        Operation::PutIids { table, documents, iids, valid_from, valid_to } | Operation::PatchIids { table, documents, iids, valid_from, valid_to } => {
            body.insert("iids".into(), Value::List(iids.iter().map(|iid| Value::FixedSizeBinary(iid.0.to_vec())).collect()));
            time_fields(&mut body, *valid_from, *valid_to);
            docs = Some((table_name(table), Value::List(documents.rows().iter().cloned().map(Value::Struct).collect())));
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
    Ok((tag.into(), body, docs))
}

fn named_union(values: &[(String, Value)], documents: Option<&[DocumentValue]>) -> Result<ArrayRef> {
    let mut names: Vec<String> = Vec::new();
    let mut legs: Vec<Vec<Value>> = Vec::new();
    let mut leg_docs: Vec<Vec<(String, Value)>> = Vec::new();
    let mut ids = Vec::new();
    let mut offsets = Vec::new();
    for (index, (name, value)) in values.iter().enumerate() {
        let leg = match names.iter().position(|existing| existing == name) {
            Some(leg) => leg,
            None => { names.push(name.clone()); legs.push(Vec::new()); leg_docs.push(Vec::new()); names.len() - 1 }
        };
        ids.push(i8::try_from(leg).map_err(|_| invalid("Too many transaction operation legs"))?);
        offsets.push(i32::try_from(legs[leg].len()).map_err(|_| overflow())?);
        legs[leg].push(value.clone());
        if let Some(Some(doc)) = documents.map(|docs| docs.get(index)).flatten() { leg_docs[leg].push(doc.clone()); }
    }
    let mut fields = Vec::new();
    let mut children = Vec::new();
    for ((name, values), docs) in names.iter().zip(legs).zip(leg_docs) {
        let mut ty = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
        if documents.is_some() {
            if let LogicalType::Struct(fields) = &mut ty {
                for temporal in ["_valid_from", "_valid_to"] {
                    if fields.contains_key(temporal) { fields.insert(temporal.into(), LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).maybe()); }
                }
                if fields.contains_key("iids") { fields.insert("iids".into(), LogicalType::list(LogicalType::Scalar(ScalarType::FixedSizeBinary(16)))); }
                if fields.contains_key("args") { fields.insert("args".into(), LogicalType::Scalar(ScalarType::Binary).maybe()); }
            }
        } else if let LogicalType::List { element, .. } = &mut ty {
            if **element == LogicalType::Nothing { **element = LogicalType::Struct(BTreeMap::new()); }
        }
        let mut array = build_array(&ty, &values)?;
        if !docs.is_empty() {
            let structs = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Document operation is not a struct"))?;
            let docs = named_union(&docs, None)?;
            let mut fields = structs.fields().to_vec();
            let mut columns = structs.columns().to_vec();
            fields.push(Arc::new(Field::new("documents", docs.data_type().clone(), false)));
            columns.push(docs);
            array = Arc::new(StructArray::try_new(fields.into(), columns, structs.nulls().cloned()).map_err(|error| invalid(error.to_string()))?);
        }
        fields.push(Arc::new(Field::new(name, array.data_type().clone(), false)));
        children.push(array);
    }
    Ok(Arc::new(UnionArray::try_new(UnionFields::new((0..fields.len()).map(|id| id as i8), fields), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(|error| invalid(error.to_string()))?))
}

fn decode_operation(union: &UnionArray, row: usize, memory: &Arc<MemoryBudget>) -> Result<Operation> {
    let (name, array, offset) = union_leg(union, row)?;
    if name == "xtql" || name == "call" { return Ok(Operation::Removed(name)); }
    let body = array.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Transaction operation leg is not a struct"))?;
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
            let field = Field::new("documents", docs_array.data_type().clone(), false);
            let documents = match read_value(&field, docs_array.as_ref(), docs_offset)? {
                Value::List(rows) => rows.into_iter().map(|row| match row { Value::Struct(document) => Ok(document), _ => Err(invalid("Document is not a struct")) }).collect::<Result<Vec<_>>>()?,
                _ => return Err(invalid("Documents are not a list")),
            };
            let documents = Documents::new(documents, memory)?;
            let valid_from = optional_timestamp(value("_valid_from")?)?;
            let valid_to = optional_timestamp(value("_valid_to")?)?;
            let iids = match value("iids")? { Value::Null | Value::Undefined => None, values => Some(iids(values)?) };
            if iids.as_ref().is_some_and(|iids| iids.len() != documents.rows().len()) { return Err(invalid("Document and IID counts differ")); }
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

pub(crate) fn encode_arguments(arguments: &SqlArguments, memory: &Arc<MemoryBudget>) -> Result<RetainedBytes> {
    let bound = arguments.types().iter().enumerate().try_fold(4096usize, |bytes, (index, ty)| {
        bytes.checked_add(rtdb_columnar::estimate_builder_bytes_refs(ty, arguments.rows().map(|row| &row[index]))?).ok_or_else(overflow)
    })?;
    let reservation = memory.reserve(bound)?;
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (index, ty) in arguments.types().iter().enumerate() {
        fields.push(to_field(&format!("${}", index + 1), ty)?);
        arrays.push(build_array(ty, &arguments.rows().map(|row| row[index].clone()).collect::<Vec<_>>())?);
    }
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(arguments.row_count()));
    let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).map_err(|error| invalid(error.to_string()))?;
    let owned = OwnedBatch::with_reservation(batch, reservation)?;
    crate::ipc::encode(owned.batch().schema().as_ref(), [owned.batch()], memory)
}

pub(crate) fn decode_arguments(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<SqlArguments> {
    let _value_reservation = memory.reserve(crate::ipc::allocation_bound(bytes)?)?;
    let stream = crate::ipc::decode_stream(bytes, memory)?;
    let schema = Arc::clone(&stream.schema);
    for (index, field) in schema.fields().iter().enumerate() {
        if !field.name().is_empty() && field.name() != &format!("${}", index + 1) && field.name() != &format!("?_{}", index) {
            return Err(Anomaly::incorrect("xtdb/invalid-param-names", "SQL argument fields must follow positional order"));
        }
    }
    let types = schema.fields().iter().map(|field| from_field(field)).collect::<Result<_>>()?;
    let mut arguments = SqlArguments::new(types, Vec::new(), memory)?;
    for page in &stream.pages {
        if page.batch().schema() != schema { return Err(invalid("SQL argument stream changes schema")); }
        let rows = (0..page.batch().num_rows()).map(|row| schema.fields().iter().enumerate().map(|(column, field)| read_value(field, page.batch().column(column).as_ref(), row)).collect::<Result<Vec<_>>>()).collect::<Result<Vec<_>>>()?;
        arguments.append_page(SqlArguments::new(arguments.types().to_vec(), rows, memory)?)?;
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
fn time_fields(body: &mut BTreeMap<String, Value>, from: Option<Timestamp>, to: Option<Timestamp>) {
    let time = |time: Option<Timestamp>| time.map(|time| Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).unwrap_or(Value::Null);
    body.insert("_valid_from".into(), time(from)); body.insert("_valid_to".into(), time(to));
}
fn table_name(table: &TableRef) -> String { format!("{}/{}", table.schema, table.table) }
fn check_writable_table(operation: &Operation) -> Result<()> {
    let table = match operation {
        Operation::Public(TxOp::PutDocs { table, .. } | TxOp::PatchDocs { table, .. } | TxOp::DeleteDocs { table, .. } | TxOp::EraseDocs { table, .. })
        | Operation::PutIids { table, .. } | Operation::PatchIids { table, .. } => table,
        _ => return Ok(()),
    };
    parse_table(&table_name(table))?;
    if matches!(table.schema.as_str(), "xt" | "information_schema" | "pg_catalog") {
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
    fn replay_keeps_explicit_put_and_patch_iids_even_when_the_document_id_hash_differs() {
        let memory = budget();
        let documents = Documents::new(vec![BTreeMap::from([("_id".into(), Value::Utf8("different-id".into()))])], &memory).unwrap();
        let table = TableRef::new("public", "docs");
        let iid = Iid([42; 16]);
        assert_ne!(iid, EntityId::String("different-id".into()).iid());
        let source = SourceTransaction::from_operations(vec![
            Operation::PutIids { table: table.clone(), documents: documents.clone(), iids: vec![iid], valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)) },
            Operation::PatchIids { table, documents, iids: vec![iid], valid_from: Some(Timestamp(30)), valid_to: None },
        ], SubmitOptions::default(), &memory).unwrap();
        let decoded = roundtrip(&source);
        assert!(matches!(&decoded.operations[0], Operation::PutIids { iids, valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)), .. } if iids == &vec![iid]));
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
    fn empty_delete_and_erase_are_omitted_but_forbidden_tables_are_checked_first() {
        let memory = budget();
        let ids = EntityIds::new(vec![], &memory).unwrap();
        let source = SourceTransaction::new(vec![
            TxOp::DeleteDocs { table: TableRef::new("public", "docs"), ids: ids.clone(), valid_from: None, valid_to: None },
            TxOp::EraseDocs { table: TableRef::new("public", "docs"), ids: ids.clone() },
        ], SubmitOptions::default(), &memory).unwrap();
        assert!(roundtrip(&source).operations.is_empty());
        let source = SourceTransaction::new(vec![TxOp::EraseDocs { table: TableRef::new("xt", "txs"), ids }], SubmitOptions::default(), &memory).unwrap();
        assert_eq!(source.encode().unwrap_err().code, "xtdb/forbidden-table");
    }

    #[test]
    fn empty_metadata_bytes_are_absent_and_present_metadata_keeps_its_types() {
        let memory = budget();
        let value = Value::Struct(BTreeMap::from([("source".into(), Value::Utf8("client".into())), ("attempt".into(), Value::I64(2))]));
        let source = SourceTransaction::new(vec![], SubmitOptions { user_metadata: Some(UserMetadata::new(value.clone(), &memory).unwrap()), ..SubmitOptions::default() }, &memory).unwrap();
        assert_eq!(roundtrip(&source).options.user_metadata.unwrap().value(), &value);
        let mut message = SourceMessage::decode(source.encode().unwrap().as_slice()).unwrap().unwrap();
        let SourceMessage::Protobuf(SourceBody::Tx(tx)) = &mut message else { panic!("wrong envelope") };
        tx.user_metadata = Some(vec![]);
        assert!(SourceTransaction::decode(&message, &memory).unwrap().options.user_metadata.is_none());
    }
}
