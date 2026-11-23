use rtdb_types::StructFields;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, FixedSizeListArray, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Fields, Schema};
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, from_field, to_field};
use rtdb_types::{Anomaly, EntityId, Iid, LogicalType, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

#[derive(Clone, Debug)]
pub struct RetainedBytes { data: Arc<Vec<u8>>, _reservation: Arc<MemoryReservation> }
impl RetainedBytes {
    /// Adopts an existing allocation; producers of large payloads must reserve before allocating.
    pub fn new(data: Vec<u8>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(data.capacity().checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self { data: Arc::new(data), _reservation: reservation })
    }
    pub fn as_slice(&self) -> &[u8] { &self.data }
    pub fn into_bytes(self) -> bytes::Bytes { bytes::Bytes::from_owner(self) }
    pub fn with_reservation(data: Vec<u8>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if data.capacity() > reservation.bytes() { return Err(Anomaly::fault("rtdb.tx/byte-reservation", "Encoded payload exceeds its reservation")); }
        Ok(Self { data: Arc::new(data), _reservation: reservation })
    }
}
impl AsRef<[u8]> for RetainedBytes { fn as_ref(&self) -> &[u8] { self.as_slice() } }

#[derive(Clone, Debug)]
pub struct Documents(Arc<DocumentBatch>);

#[derive(Debug)]
struct DocumentBatch {
    rows: Vec<StructFields<Value>>,
    layout: DocumentLayout,
    _reservation: Arc<MemoryReservation>,
}

#[derive(Debug)]
enum DocumentLayout { Submitted(Vec<String>), Decoded(Arc<DecodedDocumentLayout>) }

#[derive(Debug)]
pub(crate) struct DecodedDocumentLayout {
    physical: Fields,
    columns: Vec<String>,
    types: StructFields<LogicalType>,
    _reservation: Arc<MemoryReservation>,
}

impl DecodedDocumentLayout {
    pub(crate) fn new(fields: &Fields, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let schema = Schema::new(fields.clone());
        let reservation = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
        let columns = fields.iter().map(|field| field.name().clone()).collect();
        let types = fields.iter().map(|field| Ok((field.name().clone(), from_field(field)?))).collect::<Result<StructFields<_>>>()?;
        if types.len() != fields.len() {
            return Err(Anomaly::incorrect("rtdb.tx/document-schema", "Document columns must be unique"));
        }
        Ok(Arc::new(Self { physical: fields.clone(), columns, types, _reservation: reservation }))
    }
}

impl Documents {
    /// Columns take the order the rows first name them, as the reference's relation builds its vectors.
    pub fn new(rows: Vec<StructFields<Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut reservations = Vec::new();
        let mut names = BTreeSet::new();
        let mut columns = Vec::new();
        for name in rows.iter().flat_map(|row| row.keys()) {
            if names.insert(name) {
                reservations.push(memory.reserve(name.len().checked_add(128).ok_or_else(overflow)?)?);
                columns.push(name.clone());
            }
        }
        Self::with_column_order(rows, columns, memory)
    }

    /// Adopts row values and their declared column order, including all-null or empty columns.
    pub fn with_column_order(
        mut rows: Vec<StructFields<Value>>,
        columns: Vec<String>,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let row_bytes = rows.iter().try_fold(rows.capacity().checked_mul(std::mem::size_of::<StructFields<Value>>()).ok_or_else(overflow)?, |bytes, row| {
            bytes.checked_add(Value::document_retained_bytes(row)?).ok_or_else(overflow)
        })?;
        let column_bytes = columns.iter().try_fold(columns.capacity().checked_mul(std::mem::size_of::<String>()).ok_or_else(overflow)?, |bytes, name| {
            bytes.checked_add(name.capacity()).ok_or_else(overflow)
        })?;
        let bytes = row_bytes.checked_add(column_bytes).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?;
        let reservation = memory.reserve(bytes)?;
        let _scratch = memory.reserve(columns.len().checked_mul(64).ok_or_else(overflow)?)?;
        let names: BTreeSet<&str> = columns.iter().map(String::as_str).collect();
        if names.len() != columns.len() || rows.iter().any(|row| row.keys().any(|name| !names.contains(name.as_str()))) {
            return Err(Anomaly::incorrect("rtdb.tx/document-schema", "Document columns must be unique and cover every field in every row"));
        }
        for row in &mut rows { row.values_mut().for_each(Value::admit_object_dates); }
        Ok(Self(Arc::new(DocumentBatch { rows, layout: DocumentLayout::Submitted(columns), _reservation: reservation })))
    }

    pub fn rows(&self) -> &[StructFields<Value>] { &self.0.rows }

    /// Returns the source column order for this document batch.
    pub fn columns(&self) -> &[String] {
        match &self.0.layout { DocumentLayout::Submitted(columns) => columns, DocumentLayout::Decoded(layout) => &layout.columns }
    }

    pub(crate) fn declared_types(&self) -> Option<&StructFields<LogicalType>> {
        match &self.0.layout { DocumentLayout::Submitted(_) => None, DocumentLayout::Decoded(layout) => Some(&layout.types) }
    }

    pub(crate) fn read_arrow(array: &StructArray, layout: Arc<DecodedDocumentLayout>, omit_nulls: bool, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if array.fields() != &layout.physical {
            return Err(Anomaly::incorrect("rtdb.tx/document-schema", "Document arrays and their column layout differ"));
        }
        let mut retained = array.len().checked_mul(std::mem::size_of::<StructFields<Value>>())
            .and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?;
        let mut reservation = memory.reserve(retained)?;
        let entry = std::mem::size_of::<(String, Value)>() * 16 + 256;
        let bottom = |field: &arrow_schema::Field| field.data_type() == &DataType::Null && !field.is_nullable();
        let readers = array.fields().iter().map(|field| rtdb_columnar::ColumnReader::new(field)).collect::<Result<Vec<_>>>()?;
        let mut rows = Vec::with_capacity(array.len());
        for row in 0..array.len() {
            if array.is_null(row) { return Err(Anomaly::incorrect("rtdb.tx/invalid-ipc", "Document is not a struct")); }
            let preserve_null = |field: &arrow_schema::Field| !omit_nulls || field.name() == "_id";
            let children = || array.fields().iter().zip(array.columns()).zip(&readers).filter(|((field, column), _)| {
                preserve_null(field) || bottom(field) || (field.data_type() != &DataType::Null && column.is_valid(row))
            });
            let temporary = children().try_fold(4096usize, |bytes, ((field, column), _)| {
                let value = if bottom(field) { 0 } else { rtdb_columnar::estimate_read_cell_bytes(field, column.as_ref(), row)? };
                bytes.checked_add(value).and_then(|bytes| bytes.checked_add(field.name().len()))
                    .and_then(|bytes| bytes.checked_add(entry)).ok_or_else(overflow)
            })?;
            let reservation = Arc::get_mut(&mut reservation).expect("document decoding owns its reservation");
            reservation.grow_to(retained.checked_add(temporary).ok_or_else(overflow)?)?;
            let mut document = StructFields::new();
            for ((field, column), reader) in children() {
                let mut value = if bottom(field) { Value::Undefined } else { reader.read(column.as_ref(), row)? };
                if preserve_null(field) || !matches!(value, Value::Null) {
                    value.admit_object_dates();
                    document.insert(field.name().clone(), value);
                }
            }
            retained = retained.checked_add(Value::document_retained_bytes(&document)?).ok_or_else(overflow)?;
            reservation.shrink_to(retained)?;
            rows.push(document);
        }
        Ok(Self(Arc::new(DocumentBatch { rows, layout: DocumentLayout::Decoded(layout), _reservation: reservation })))
    }
}

#[derive(Clone, Debug)]
pub struct ArrowDocuments {
    owned: OwnedBatch,
    layout: Arc<DecodedDocumentLayout>,
    id: usize,
}

impl ArrowDocuments {
    /// Dense floating-point embeddings with simple scalar metadata can travel in their existing Arrow layout.
    pub fn try_new(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_batch(batch, memory, None, false)
    }

    pub fn try_from_owned(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_batch(batch.batch(), memory, Some(batch), false)
    }

    pub(crate) fn from_batch(batch: &RecordBatch, memory: &Arc<MemoryBudget>, owner: Option<&OwnedBatch>, canonical: bool) -> Result<Option<Self>> {
        let schema = batch.schema();
        let Ok(id) = schema.index_of("_id") else { return Ok(None); };
        let id_type = schema.field(id);
        let supported_extension = |field: &arrow_schema::Field| match field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str) {
            Some("uuid") => field.data_type() == &DataType::FixedSizeBinary(16),
            Some("xt/clj-keyword") => field.data_type() == &DataType::Utf8,
            _ => false,
        };
        if !(id_type.metadata().is_empty()
            && matches!(id_type.data_type(), DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Utf8))
            && !supported_extension(id_type)
        {
            return Ok(None);
        }
        let mut vectors = false;
        if batch.num_rows() == 0 || batch.column(id).null_count() != 0 || !schema.fields().iter().zip(batch.columns()).all(|(field, array)| {
            if field.name().starts_with('_') && field.name() != "_id" { return false; }
            if !field.metadata().is_empty() { return supported_extension(field); }
            match field.data_type() {
                DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) | DataType::Null => true,
                DataType::List(item) if item.metadata().is_empty() && matches!(item.data_type(), DataType::Float32 | DataType::Float64) => {
                    vectors |= canonical || array.null_count() < array.len();
                    array.as_any().is::<ListArray>()
                }
                DataType::FixedSizeList(item, size) if *size > 0 && item.metadata().is_empty() &&
                    matches!(item.data_type(), DataType::Float32 | DataType::Float64) => {
                    vectors |= canonical || array.null_count() < array.len();
                    array.as_any().is::<FixedSizeListArray>()
                }
                _ => false,
            }
        }) || !vectors {
            return Ok(None);
        }
        if schema.fields().iter().map(|field| field.name()).collect::<BTreeSet<_>>().len() != schema.fields().len() {
            return Ok(None);
        }
        let _schema_scratch = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
        let mut fields = Vec::with_capacity(batch.num_columns());
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            if !canonical && column.null_count() == column.len() {
                let field = to_field(field.name(), &LogicalType::Null)?;
                fields.push(Arc::new(field));
                columns.push(Arc::new(arrow_array::NullArray::new(column.len())) as ArrayRef);
                continue;
            }
            let nullable = if canonical { field.is_nullable() } else { column.null_count() != 0 };
            let source = column;
            let invalid_embedding = |reason: &str| Anomaly::incorrect("rtdb.tx/document-schema", reason);
            let logical = match field.data_type() {
                DataType::List(item) => {
                    let list = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid_embedding("Invalid embedding column"))?;
                    let child_nullable = if canonical { item.is_nullable() } else { list.values().null_count() != 0 };
                    let element = from_field(&item.as_ref().clone().with_nullable(child_nullable))?;
                    LogicalType::list(element)
                }
                DataType::FixedSizeList(item, size) => {
                    let list = column.as_any().downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| Anomaly::incorrect("rtdb.tx/document-schema", "Invalid embedding column"))?;
                    let nullable = if canonical { item.is_nullable() } else {
                        list.values().nulls().is_some_and(|items| items.iter().enumerate().any(|(index, valid)| {
                            !valid && list.is_valid(index / *size as usize)
                        }))
                    };
                    let element = from_field(&item.as_ref().clone().with_nullable(nullable))?;
                    LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(*size), element: Box::new(element) }
                }
                _ => from_field(&field.as_ref().clone().with_nullable(false))?,
            };
            let logical = if nullable { logical.maybe() } else { logical };
            let field = to_field(field.name(), &logical)?;
            let column: ArrayRef = match field.data_type() {
                DataType::FixedSizeList(item, size) => {
                    let list = source.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| invalid_embedding("Invalid fixed-width embedding column"))?;
                    Arc::new(FixedSizeListArray::try_new(Arc::clone(item), *size, Arc::clone(list.values()), list.nulls().cloned())
                        .map_err(|error| invalid_embedding(&error.to_string()))?)
                }
                DataType::List(item) => {
                    let list = source.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid_embedding("Invalid variable-width embedding column"))?;
                    Arc::new(ListArray::try_new(Arc::clone(item), list.offsets().clone(), Arc::clone(list.values()), list.nulls().cloned())
                        .map_err(|error| invalid_embedding(&error.to_string()))?)
                }
                _ => Arc::clone(source),
            };
            fields.push(Arc::new(field));
            columns.push(column);
        }
        let canonical_schema = Arc::new(Schema::new(fields));
        if canonical && canonical_schema.fields() != schema.fields() { return Ok(None); }
        let schema = canonical_schema;
        let layout = DecodedDocumentLayout::new(schema.fields(), memory)?;
        let canonical = RecordBatch::try_new(schema, columns)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        let owned = match owner {
            Some(owner) => OwnedBatch::with_reservation(canonical, Arc::clone(owner.reservation()))?,
            None => OwnedBatch::new(canonical, memory)?,
        };
        Ok(Some(Self { owned, layout, id }))
    }

    pub fn copy_buffers(mut self) -> Result<Self> {
        let batch = self.owned.batch();
        let memory = self.owned.reservation().budget();
        let rows = u32::try_from(batch.num_rows()).map_err(|_| overflow())?;
        let overhead = (rows as usize).checked_mul(32).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let _scratch = memory.reserve(overhead)?;
        let indices = arrow_array::UInt32Array::from_iter_values(0..rows);
        let mut builder = rtdb_columnar::BatchBuilder::new(4096, memory)?;
        let columns = batch.columns().iter().map(|array| {
            let bound = array.get_array_memory_size().checked_mul(4).and_then(|bytes| bytes.checked_add(overhead)).ok_or_else(overflow)?;
            builder.column(bound, || arrow_select::take::take(array.as_ref(), &indices, None)
                .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string())))
        }).collect::<Result<Vec<_>>>()?;
        let copied = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        self.owned = builder.finish(copied)?;
        Ok(self)
    }

    pub fn row_count(&self) -> usize { self.owned.batch().num_rows() }
    pub(crate) fn batch(&self) -> &OwnedBatch { &self.owned }
    pub(crate) fn columns(&self) -> &[String] { &self.layout.columns }
    pub(crate) fn types(&self) -> &StructFields<LogicalType> { &self.layout.types }

    pub(crate) fn iid(&self, row: usize) -> Result<Iid> {
        let values = self.owned.batch().column(self.id);
        let field = &self.layout.physical[self.id];
        rtdb_columnar::read_value(field, values.as_ref(), row)?.entity_id().map(|id| id.iid())
    }

    pub(crate) fn materialize(&self, memory: &Arc<MemoryBudget>) -> Result<Documents> {
        let structs = StructArray::from(self.owned.batch().clone());
        Documents::read_arrow(&structs, Arc::clone(&self.layout), true, memory)
    }
}

#[derive(Clone, Debug)]
pub struct EntityIds { ids: Arc<Vec<EntityId>>, _reservation: Arc<MemoryReservation> }
impl EntityIds {
    pub fn new(ids: Vec<EntityId>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = ids.iter().try_fold(ids.capacity().checked_mul(std::mem::size_of::<EntityId>()).ok_or_else(overflow)?, |bytes, id| {
            let extra = match id { EntityId::String(value) => value.capacity(), EntityId::Keyword(value) => value.retained_bytes(), _ => 0 };
            bytes.checked_add(extra).ok_or_else(overflow)
        })?;
        Ok(Self { ids: Arc::new(ids), _reservation: memory.reserve(bytes.checked_add(128).ok_or_else(overflow)?)? })
    }
    pub fn ids(&self) -> &[EntityId] { &self.ids }
}

#[derive(Clone, Debug)]
pub struct UserMetadata { value: Arc<Value>, _reservation: Arc<MemoryReservation> }
impl UserMetadata {
    pub fn new(mut value: Value, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if !matches!(value, Value::Struct(_)) { return Err(Anomaly::incorrect("xtdb/invalid-user-metadata", "Transaction metadata must be a document")); }
        value.admit_object_dates();
        let reservation = memory.reserve(value.retained_bytes()?.checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self { value: Arc::new(value), _reservation: reservation })
    }
    pub fn value(&self) -> &Value { &self.value }

    pub(crate) fn requires_ordered_system_time(&self) -> bool {
        matches!(
            self.value(),
            Value::Struct(fields) if fields.get("rtdb_ordered_system_time") == Some(&Value::Bool(true))
        )
    }
}

#[derive(Clone, Debug)]
struct ArgumentChunk { rows: Vec<Vec<Value>>, _reservation: Arc<MemoryReservation> }

#[derive(Clone, Debug)]
pub struct SqlArguments {
    types: Arc<Vec<LogicalType>>,
    chunks: Arc<Vec<(usize, Arc<ArgumentChunk>)>>,
    memory: Arc<MemoryBudget>,
    schema_reservation: Arc<MemoryReservation>,
    row_count: usize,
}
impl SqlArguments {
    /// Explicit column types preserve typed nulls and the schema of an empty argument batch.
    pub fn new(types: Vec<LogicalType>, mut rows: Vec<Vec<Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        for ty in &types { ty.validate()?; }
        if rows.iter().any(|row| row.len() != types.len()) { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Argument rows have different widths")); }
        for row in &mut rows { row.iter_mut().for_each(Value::admit_object_dates); }
        let mut bytes = rows.capacity().checked_mul(std::mem::size_of::<Vec<Value>>()).ok_or_else(overflow)?;
        for row in &rows {
            bytes = bytes.checked_add(row.capacity().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?).ok_or_else(overflow)?;
            for value in row { bytes = bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow)?; }
        }
        let schema_reservation = memory.reserve(schema_bytes(&types)?)?;
        let row_count = rows.len();
        let chunk = Arc::new(ArgumentChunk { rows, _reservation: memory.reserve(bytes.checked_add(256).ok_or_else(overflow)?)? });
        Ok(Self { types: Arc::new(types), chunks: Arc::new(vec![(0, chunk)]), memory: Arc::clone(memory), schema_reservation, row_count })
    }
    pub fn from_rows(rows: Vec<Vec<Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let width = rows.first().map_or(0, Vec::len);
        let mut types = vec![LogicalType::Nothing; width];
        for row in &rows {
            if row.len() != width { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Argument rows have different widths")); }
            for (ty, value) in types.iter_mut().zip(row) { *ty = ty.join(&value.logical_type()); }
        }
        Self::new(types, rows, memory)
    }
    pub fn types(&self) -> &[LogicalType] { &self.types }
    /// Opened-vector field types for query or DML binding; source payload fields remain available through `types`.
    pub fn execution_types(&self) -> impl Iterator<Item = LogicalType> + '_ { self.types.iter().map(rtdb_columnar::execution_type) }
    pub fn row_count(&self) -> usize { self.row_count }
    pub fn rows(&self) -> impl Iterator<Item = &[Value]> { self.chunks.iter().flat_map(|(_, chunk)| chunk.rows.iter().map(Vec::as_slice)) }
    pub(crate) fn indexed_rows(&self) -> impl Iterator<Item = (usize, &[Value])> { self.chunks.iter().flat_map(|(start, chunk)| chunk.rows.iter().enumerate().map(move |(index, row)| (*start + index, row.as_slice()))) }
    /// Appends argument rows in order and widens each positional column's type.
    pub fn append(&mut self, other: Self) -> Result<()> {
        self.append_rows(other, self.row_count)
    }
    pub(crate) fn append_page(&mut self, other: Self) -> Result<()> { self.append_rows(other, 0) }
    fn append_rows(&mut self, other: Self, start: usize) -> Result<()> {
        if self.types.len() != other.types.len() { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Coalesced SQL arguments have different widths")); }
        let count = self.row_count.checked_add(other.row_count).ok_or_else(overflow)?;
        let types: Vec<_> = self.types.iter().zip(other.types.iter()).map(|(left, right)| if left == right { left.clone() } else { left.join(right) }).collect();
        let chunk_count = self.chunks.len().checked_add(other.chunks.len()).ok_or_else(overflow)?;
        let bytes = schema_bytes(&types)?.checked_add(chunk_count.checked_mul(128).ok_or_else(overflow)?).ok_or_else(overflow)?;
        let reservation = self.memory.reserve(bytes)?;
        let appended = other.chunks.iter().map(|(offset, chunk)| Ok((start.checked_add(*offset).ok_or_else(overflow)?, Arc::clone(chunk)))).collect::<Result<Vec<_>>>()?;
        // An unshared chunk list grows in place, so a statement appended to a buffer of many costs no copy of the rest.
        Arc::make_mut(&mut self.chunks).extend(appended);
        if *self.types != types { self.types = Arc::new(types); }
        self.schema_reservation = reservation;
        self.row_count = count;
        Ok(())
    }
}

fn schema_bytes(types: &[LogicalType]) -> Result<usize> {
    types.iter().try_fold(256usize, |bytes, ty| bytes.checked_add(rtdb_columnar::estimate_builder_bytes(ty, &[])?).ok_or_else(overflow))
}

#[derive(Clone, Debug)]
pub enum TxOp {
    Sql { sql: String, args: Option<SqlArguments> },
    SqlBytes { sql: String, arg_bytes: RetainedBytes },
    PutDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PutArrowDocs { table: TableRef, documents: ArrowDocuments, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PatchDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    DeleteDocs { table: TableRef, ids: EntityIds, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    EraseDocs { table: TableRef, ids: EntityIds },
}

#[derive(Clone, Debug)]
pub struct SubmitOptions {
    pub default_tz: String,
    pub system_time: Option<Timestamp>,
    pub user: Option<String>,
    pub user_metadata: Option<UserMetadata>,
}
impl Default for SubmitOptions {
    fn default() -> Self { Self { default_tz: "UTC".into(), system_time: None, user: None, user_metadata: None } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubmittedTx { pub tx_id: TxId }

#[derive(Debug)]
struct AnomalyAllocation { error: Anomaly, wire: Option<Arc<Vec<u8>>>, reservation: Arc<MemoryReservation> }

#[derive(Clone, Debug)]
pub struct RetainedAnomaly(Arc<AnomalyAllocation>);
impl RetainedAnomaly {
    pub fn new(error: Anomaly, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(anomaly_bytes(&error)?.checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self(Arc::new(AnomalyAllocation { error, wire: None, reservation })))
    }
    /// Retains the exact persisted error representation, including historical Throwable tags.
    pub fn from_transit(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _transient = memory.reserve(bytes.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let error = rtdb_format::transit::decode_anomaly(bytes)?;
        let reservation = memory.reserve(anomaly_bytes(&error)?.checked_add(bytes.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
        Ok(Self(Arc::new(AnomalyAllocation { error, wire: Some(Arc::new(bytes.to_vec())), reservation })))
    }
    pub fn encode(&self) -> Result<RetainedBytes> {
        if let Some(wire) = &self.0.wire { return Ok(RetainedBytes { data: Arc::clone(wire), _reservation: Arc::clone(&self.0.reservation) }); }
        let bound = anomaly_bytes(&self.0.error)?.checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        let reservation = self.0.reservation.budget().reserve(bound)?;
        RetainedBytes::with_reservation(rtdb_format::transit::encode_anomaly(&self.0.error)?, reservation)
    }
}
impl std::ops::Deref for RetainedAnomaly { type Target = Anomaly; fn deref(&self) -> &Anomaly { &self.0.error } }
impl PartialEq for RetainedAnomaly { fn eq(&self, other: &Self) -> bool { self.0.error == other.0.error } }
impl Eq for RetainedAnomaly {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransactionResult { Committed { tx_key: TransactionKey }, Aborted { tx_key: TransactionKey, error: Option<RetainedAnomaly> } }
impl TransactionResult {
    pub fn tx_key(&self) -> TransactionKey { match self { Self::Committed { tx_key } | Self::Aborted { tx_key, .. } => *tx_key } }
    pub fn committed(&self) -> bool { matches!(self, Self::Committed { .. }) }
    pub fn error(&self) -> Option<&Anomaly> { match self { Self::Committed { .. } => None, Self::Aborted { error, .. } => error.as_deref() } }
    pub fn encoded_error(&self) -> Result<Option<RetainedBytes>> { match self { Self::Committed { .. } => Ok(None), Self::Aborted { error, .. } => error.as_ref().map(RetainedAnomaly::encode).transpose() } }
}

pub(crate) fn anomaly_bytes(error: &Anomaly) -> Result<usize> {
    fn json(value: &serde_json::Value) -> Result<usize> {
        match value {
            serde_json::Value::String(value) => value.capacity().checked_add(128).ok_or_else(overflow),
            serde_json::Value::Array(values) => values.iter().try_fold(values.capacity().checked_mul(128).ok_or_else(overflow)?, |bytes, value| bytes.checked_add(json(value)?).ok_or_else(overflow)),
            serde_json::Value::Object(values) => values.iter().try_fold(128usize, |bytes, (key, value)| bytes.checked_add(key.capacity()).and_then(|bytes| bytes.checked_add(256)).and_then(|bytes| bytes.checked_add(json(value).ok()?)).ok_or_else(overflow)),
            _ => Ok(128),
        }
    }
    error.context.iter().try_fold(error.code.capacity().checked_add(error.message.as_ref().map_or(0, String::capacity)).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?, |bytes, (key, value)| bytes.checked_add(key.capacity()).and_then(|bytes| bytes.checked_add(256)).and_then(|bytes| bytes.checked_add(json(value).ok()?)).ok_or_else(overflow))
}

fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tx/allocation-overflow", "Transaction allocation exceeds address space") }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_allocated_messages_retain_their_capacity_and_absent_messages_allocate_none() {
        let absent = Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/error", None);
        let capacity = String::with_capacity(4096);
        let bytes = capacity.capacity();
        let present = Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/error", Some(capacity));
        assert_eq!(anomaly_bytes(&present).unwrap() - anomaly_bytes(&absent).unwrap(), bytes);
        let memory = MemoryBudget::new(anomaly_bytes(&absent).unwrap() + 128);
        assert!(RetainedAnomaly::new(present, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
        let retained = RetainedAnomaly::new(absent, &memory).unwrap();
        assert!(memory.used_bytes() > 0);
        drop(retained); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn decoded_parameter_pages_reset_error_indices_but_coalesced_batches_continue() {
        let memory = MemoryBudget::new(1 << 20);
        let mut args = SqlArguments::from_rows(vec![vec![Value::I64(1)], vec![Value::I64(2)]], &memory).unwrap();
        args.append_page(SqlArguments::from_rows(vec![vec![Value::I64(3)]], &memory).unwrap()).unwrap();
        assert_eq!(args.indexed_rows().map(|(index, _)| index).collect::<Vec<_>>(), [0, 1, 0]);
        let mut coalesced = SqlArguments::from_rows(vec![vec![Value::I64(1)]], &memory).unwrap();
        coalesced.append(SqlArguments::from_rows(vec![vec![Value::I64(2)]], &memory).unwrap()).unwrap();
        assert_eq!(coalesced.indexed_rows().map(|(index, _)| index).collect::<Vec<_>>(), [0, 1]);
    }
}
