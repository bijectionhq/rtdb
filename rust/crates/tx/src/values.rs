use rtdb_types::StructFields;
use std::collections::BTreeSet;
use std::{borrow::Cow, ops::Deref, sync::Arc};

use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, ListArray, PrimitiveArray, RecordBatch, StructArray, cast::AsArray, types::{Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type}};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use bytes::Bytes;
use rtdb_columnar::{ArrowCell, ColumnReader, MemoryBudget, MemoryReservation, OwnedBatch, from_field};
use rtdb_types::{Anomaly, EntityId, ListKind, LogicalType, Result, TableRef, Timestamp, TransactionKey, TxId, Value};

pub(crate) mod arguments;
mod union;

pub(crate) use arguments::valid_argument_union_cell;

#[derive(Clone, Debug)]
pub struct RetainedBytes { data: Bytes, _reservation: Arc<MemoryReservation> }
impl RetainedBytes {
    /// Adopts an existing allocation; producers of large payloads must reserve before allocating.
    pub fn new(data: Vec<u8>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(data.capacity().checked_add(128).ok_or_else(overflow)?)?;
        Self::with_reservation(data, reservation)
    }
    pub fn as_slice(&self) -> &[u8] { &self.data }
    pub(crate) fn reservation_bytes(&self) -> usize { self._reservation.bytes() }
    pub(crate) fn into_parts(self) -> (Bytes, Arc<MemoryReservation>) {
        (self.data, self._reservation)
    }
    pub fn into_bytes(self) -> Bytes { Bytes::from_owner(self) }
    pub fn with_reservation(data: Vec<u8>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        Self::with_reserved_suffix(data, 0, reservation)
    }
    pub(crate) fn with_reserved_suffix(data: Vec<u8>, start: usize, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if data.capacity() > reservation.bytes() { return Err(Anomaly::fault("rtdb.tx/byte-reservation", "Encoded payload exceeds its reservation")); }
        Ok(Self { data: Bytes::from(data).slice(start..), _reservation: reservation })
    }
}
impl AsRef<[u8]> for RetainedBytes { fn as_ref(&self) -> &[u8] { self.as_slice() } }

#[derive(Clone, Debug)]
pub struct Documents(Arc<DocumentBatch>);

#[derive(Debug)]
struct DocumentBatch {
    rows: Vec<StructFields<Value>>,
    layout: DocumentLayout,
    exact_presence: bool,
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
    pub(crate) fn column_type(&self, name: &str) -> Option<&LogicalType> { self.types.get(name) }

    pub(crate) fn new(fields: &Fields, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let schema = Schema::new(fields.clone());
        let mut reservation = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
        let columns = fields.iter().map(|field| field.name().clone()).collect::<Vec<_>>();
        let types = fields.iter().map(|field| Ok((field.name().clone(), from_field(field)?))).collect::<Result<StructFields<_>>>()?;
        if types.len() != fields.len() {
            return Err(Anomaly::incorrect("rtdb.tx/document-schema", "Document columns must be unique"));
        }
        let base = types.table_bytes_bound().and_then(|bytes| bytes.checked_add(fields.size()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() + 64))
            .and_then(|bytes| bytes.checked_add(columns.capacity().checked_mul(std::mem::size_of::<String>())?))
            .ok_or_else(overflow)?;
        let retained = types.iter().zip(&columns).try_fold(base, |bytes, ((name, ty), column)| {
            let type_bytes = ty.retained_bytes()?;
            bytes.checked_add(type_bytes).and_then(|bytes| bytes.checked_add(name.capacity()))
                .and_then(|bytes| bytes.checked_add(column.capacity())).ok_or_else(overflow)
        })?;
        let lease = Arc::get_mut(&mut reservation).expect("Document layout owns its reservation");
        if retained > lease.bytes() { lease.grow_to(retained)?; } else { lease.shrink_to(retained)?; }
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
        for row in &mut rows { row.values_mut().try_for_each(Value::admit_object_dates)?; }
        Ok(Self(Arc::new(DocumentBatch { rows, layout: DocumentLayout::Submitted(columns), exact_presence: true, _reservation: reservation })))
    }

    pub fn rows(&self) -> &[StructFields<Value>] { &self.0.rows }

    pub(crate) fn has_exact_presence(&self) -> bool { self.0.exact_presence }

    /// Returns the source column order for this document batch.
    pub fn columns(&self) -> &[String] {
        match &self.0.layout { DocumentLayout::Submitted(columns) => columns, DocumentLayout::Decoded(layout) => &layout.columns }
    }

    pub(crate) fn declared_types(&self) -> Option<&StructFields<LogicalType>> {
        match &self.0.layout { DocumentLayout::Submitted(_) => None, DocumentLayout::Decoded(layout) => Some(&layout.types) }
    }

    pub(crate) fn read_arrow(array: &StructArray, layout: Arc<DecodedDocumentLayout>, omit_nulls: bool, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::read_arrow_with_presence(array, layout, omit_nulls, None, memory)
    }

    pub(crate) fn read_arrow_with_presence(
        array: &StructArray, layout: Arc<DecodedDocumentLayout>, omit_nulls: bool,
        presence: Option<&[BTreeSet<String>]>, memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        if presence.is_some_and(|rows| rows.len() != array.len()) {
            return Err(Anomaly::incorrect("rtdb.tx/document-presence", "Document presence must cover every row"));
        }
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
            let explicit = presence.map(|rows| &rows[row]);
            let preserve_null = |field: &arrow_schema::Field| explicit.is_some() || !omit_nulls || field.name() == "_id";
            let children = || array.fields().iter().zip(array.columns()).zip(&readers).filter(|((field, column), _)| {
                explicit.map_or_else(
                    || preserve_null(field) || bottom(field) || (field.data_type() != &DataType::Null && column.is_valid(row)),
                    |fields| fields.contains(field.name()),
                )
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
                    value.admit_object_dates()?;
                    document.insert(field.name().clone(), value);
                }
            }
            if explicit.is_some_and(|fields| document.len() != fields.len()) {
                return Err(Anomaly::incorrect("rtdb.tx/document-presence", "Document presence names an unknown column"));
            }
            retained = retained.checked_add(Value::document_retained_bytes(&document)?).ok_or_else(overflow)?;
            reservation.shrink_to(retained)?;
            rows.push(document);
        }
        Ok(Self(Arc::new(DocumentBatch { rows, layout: DocumentLayout::Decoded(layout), exact_presence: presence.is_some(), _reservation: reservation })))
    }
}

#[derive(Clone, Debug)]
pub struct ArrowDocuments {
    array: StructArray,
    layout: Arc<DecodedDocumentLayout>,
    source_bytes: Option<usize>,
    input: DocumentInput,
}

#[derive(Clone, Debug)]
enum DocumentInput {
    Arrow,
    LegacyOwned(LegacyOwnedPresence),
    SubmittedRows(Documents),
}

#[derive(Clone, Debug)]
pub(crate) enum LegacyOwnedPresence {
    Submitted,
    Implicit,
    Exact(Arc<OwnedFieldPresence>),
}

#[derive(Debug)]
pub(crate) struct OwnedFieldPresence {
    pub(crate) fields: Vec<BTreeSet<String>>,
    _reservation: Arc<MemoryReservation>,
}

#[derive(Clone, Copy)]
pub(crate) enum IngestMode { External, Mutation }

impl ArrowDocuments {
    pub fn from_batch(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_record_batch(batch.batch(), memory)
    }

    pub fn from_submitted_rows(documents: &Documents, memory: &Arc<MemoryBudget>) -> Option<Self> {
        // The row writer reports invalid or missing IDs using the original document.
        if documents.rows().is_empty() || documents.rows().iter().any(|row| {
            !row.get("_id").is_some_and(|id| id.entity_id().is_ok())
        }) {
            return None;
        }
        crate::source::owned_row_documents(documents, memory).ok().map(|arrow| {
            Self { input: DocumentInput::SubmittedRows(documents.clone()), ..arrow }
        })
    }

    pub(crate) fn submitted_rows(&self) -> Option<&Documents> {
        match &self.input {
            DocumentInput::SubmittedRows(rows) => Some(rows),
            _ => None,
        }
    }

    /// Validates an upload and prepares its Arrow document columns for source storage.
    pub fn from_ingest_batch(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_external_pages(&[batch.batch()], memory)
    }

    fn from_external_pages(pages: &[&RecordBatch], memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        if pages.is_empty() { return Ok(None); }
        let Some(page) = pages.iter().copied().find(|page| page.num_rows() != 0) else {
            let fields = Fields::from(Vec::<Arc<arrow_schema::Field>>::new());
            let layout = DecodedDocumentLayout::new(&fields, memory)?;
            let array = StructArray::new_empty_fields(0, None);
            return Ok(Some(Self { array, layout, source_bytes: None, input: DocumentInput::Arrow }));
        };
        if let Some(documents) = Self::from_canonical_ingest_pages(pages, IngestMode::External, memory)? {
            return Ok(Some(documents));
        }

        let original = page.schema_ref();
        let _schema = memory.reserve(rtdb_format::ipc::schema_translation_bound(original)?)?;
        let rows = pages.iter().try_fold(0usize, |rows, page| rows.checked_add(page.num_rows()).ok_or_else(overflow))?;
        let mut fields = Vec::with_capacity(original.fields().len());
        let mut columns = Vec::with_capacity(original.fields().len());
        for (index, field) in original.fields().iter().enumerate() {
            let logical = from_field(field)?;
            let canonical = rtdb_columnar::to_field(field.name(), &logical)?;
            let array = canonical_argument_column(pages, index, memory)?;
            fields.push(field.as_ref().clone().with_metadata(canonical.metadata().clone()));
            columns.push(array);
        }
        let schema = Arc::new(Schema::new(fields));
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), columns, &options)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        Self::prepare_columns(&batch, &schema, memory)?
            .ok_or_else(|| Anomaly::incorrect("rtdb.tx/document-schema", "Arrow upload cannot be aligned to a document layout"))
            .map(Some)
    }

    pub(crate) fn from_canonical_ingest(batch: &RecordBatch, mode: IngestMode, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_canonical_ingest_pages(&[batch], mode, memory)
    }

    fn from_canonical_ingest_pages(pages: &[&RecordBatch], mode: IngestMode, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let Some(batch) = pages.iter().copied().find(|page| page.num_rows() != 0) else { return Ok(None); };
        let schema = batch.schema();
        if batch.num_columns() == 0 { return Ok(None); }
        let _schema = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
        let mut fields = Vec::with_capacity(schema.fields().len());
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            let known = if let [page] = pages {
                let Some(ty) = ingest_type(field, page.column(index).as_ref(), mode)? else { return Ok(None); };
                Some(ty)
            } else { None };
            let array = match mode {
                IngestMode::External => canonical_argument_column(pages, index, memory)?,
                IngestMode::Mutation => copy_argument_column(pages, index, memory)?,
            };
            let ty = match known {
                Some(ty) => ty,
                None => match ingest_type(field, array.as_ref(), mode)? {
                    Some(ty) => ty,
                    None => return Ok(None),
                },
            };
            let target = rtdb_columnar::to_field(field.name(), &ty)?;
            let array = if ty == LogicalType::Null {
                Arc::new(arrow_array::NullArray::new(array.len())) as ArrayRef
            } else {
                let Some(array) = normalized_document_array(&array, memory)? else { return Ok(None); };
                let Ok(array) = ingest_layout(array, target.data_type()) else { return Ok(None); };
                array
            };
            fields.push(target);
            columns.push(array);
        }
        let canonical = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        Self::from_record_batch(&canonical, memory)
    }

    pub(crate) fn from_record_batch(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let schema = batch.schema();
        if batch.num_rows() == 0 || schema.fields().iter().any(|field| !Self::compatible(field, true)) {
            return Ok(None);
        }
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            if contains_uuid(field) && (!uuid_field(field)
                || (array.is_null(0) && array.logical_null_count() != array.len())) {
                return Ok(None);
            }
        }
        let _schema = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
        let names = schema.fields().iter().map(|field| field.name()).collect::<BTreeSet<_>>();
        if names.len() != schema.fields().len() { return Ok(None); }
        Self::prepare_columns(batch, &schema, memory)
    }

    pub(crate) fn from_scalar_mutation_page(content: &StructArray, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let batch = RecordBatch::from(content.clone());
        let columns = batch.columns().iter().enumerate().map(|(index, column)| {
            if canonical_mutation_scalar(column.as_ref()) { return Ok(Arc::clone(column)); }
            copy_argument_column(&[&batch], index, memory)
        }).collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|error| Anomaly::fault("rtdb.tx/mutation-schema", error.to_string()))?;
        Self::from_record_batch(&batch, memory)
    }

    fn prepare_columns(batch: &RecordBatch, schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let mut fields = Vec::with_capacity(schema.fields().len());
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (field, array) in schema.fields().iter().zip(batch.columns()) {
            let logical = from_field(field)?;
            rtdb_columnar::validate_readable_column(field, array.as_ref(), &logical, memory)?;
            let nulls = array.logical_null_count();
            let array = if nulls == array.len() {
                Arc::new(arrow_array::NullArray::new(array.len())) as ArrayRef
            } else if uuid_field(field) && nulls != 0 {
                canonical_nullable_uuid(array, memory)?
            } else {
                let Some(array) = normalized_document_array(array, memory)? else { return Ok(None); };
                array
            };
            fields.push(normalized_field(&field.as_ref().clone().with_nullable(nulls != 0), &array));
            columns.push(array);
        }
        let fields = Fields::from(fields);
        let layout = DecodedDocumentLayout::new(&fields, memory)?;
        let array = StructArray::try_new_with_length(fields, columns, None, batch.num_rows())
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        Ok(Some(Self { array, layout, source_bytes: None, input: DocumentInput::Arrow }))
    }

    fn compatible(field: &arrow_schema::Field, normalize: bool) -> bool {
        compatible_document_metadata(field) && match field.data_type() {
            DataType::Struct(fields) => fields.iter().all(|field| Self::compatible(field, normalize)),
            DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _)
            | DataType::RunEndEncoded(_, field) => Self::compatible(field, normalize),
            DataType::Union(fields, arrow_schema::UnionMode::Dense) => fields.iter().all(|(_, field)| {
                Self::compatible(field, normalize) && (normalize || from_field(field).is_ok_and(|ty| rtdb_columnar::execution_type(&ty) == ty))
            }),
            DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) |
            DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) |
            DataType::Timestamp(..) | DataType::Interval(_) | DataType::Decimal32(..) | DataType::Decimal64(..)
            | DataType::Decimal128(..) | DataType::Decimal256(..) => true,
            _ => false,
        }
    }

    pub(crate) fn from_source(array: &StructArray, layout: Arc<DecodedDocumentLayout>, source_bytes: usize, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        if array.null_count() != 0 { return Ok(None); }
        let fixed = || array.fields().iter().zip(array.columns()).filter(|(field, _)| contains_fixed_size_list(field.data_type()));
        if fixed().next().is_some() {
            let bound = fixed().try_fold(4096usize, |bytes, (field, _)| {
                field.size().checked_mul(8).and_then(|size| bytes.checked_add(size)).ok_or_else(overflow)
            })?;
            let _scratch = memory.reserve(bound)?;
            for (field, column) in fixed() {
                let logical = layout.types.get(field.name()).ok_or_else(|| Anomaly::fault("rtdb.tx/document-schema", "Replayed column has no type"))?;
                rtdb_columnar::validate_readable_column(field, column.as_ref(), logical, memory)?;
            }
        }
        Ok(Some(Self { array: array.clone(), layout, source_bytes: Some(source_bytes), input: DocumentInput::Arrow }))
    }

    pub(crate) fn requires_legacy_receipt(&self, patch: bool) -> bool {
        use arrow_schema::TimeUnit as ArrowTime;

        fn physical(field: &arrow_schema::Field) -> bool {
            (field.metadata().is_empty() || uuid_field(field)) && match field.data_type() {
                DataType::Struct(fields) => fields.iter().all(|field| physical(field)),
                DataType::List(field) | DataType::FixedSizeList(field, _) => physical(field),
                DataType::Union(fields, arrow_schema::UnionMode::Dense) => fields.iter().all(|(_, field)| {
                    physical(field) && from_field(field).is_ok_and(|ty| rtdb_columnar::execution_type(&ty) == ty)
                }),
                DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) |
                DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) |
                DataType::Timestamp(ArrowTime::Microsecond, Some(_)) => true,
                _ => false,
            }
        }

        fn patch_kind(ty: &LogicalType) -> bool {
            match ty {
                LogicalType::Struct(fields) => fields.values().all(patch_kind),
                LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_), element } => patch_kind(element),
                LogicalType::Maybe(inner) => patch_kind(inner),
                LogicalType::Null | LogicalType::Scalar(_) => true,
                _ => false,
            }
        }

        self.array.is_empty() || !self.array.fields().iter().all(|field| physical(field) && canonical_uuid_unions(field))
            || patch && !self.layout.types.values().all(|ty| patch_kind(ty) && rtdb_columnar::execution_type(ty) == *ty)
    }

    pub(crate) fn patch_parameter(&self, iids: &[rtdb_types::Iid], memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
        use arrow_schema::Field;
        if iids.len() != self.array.len() {
            return Err(Anomaly::incorrect("xtdb/invalid-iids", "Document and IID counts differ"));
        }
        let bound = self.array.get_array_memory_size().checked_add(iids.len().checked_mul(32).ok_or_else(overflow)?)
            .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(overflow)?;
        let reservation = memory.reserve(bound)?;
        let iid_array = arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(iids.iter().map(|iid| Some(iid.0.as_slice())), 16)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        let fields = vec![
            Field::new("_iid", DataType::FixedSizeBinary(16), false),
            Field::new("doc", self.array.data_type().clone(), false),
        ].into();
        let document = StructArray::new(fields, vec![Arc::new(iid_array), Arc::new(self.array.clone())], None);
        let item = Arc::new(Field::new("item", document.data_type().clone(), false));
        let count = i32::try_from(iids.len()).map_err(|_| overflow())?;
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, count].into());
        let list = ListArray::new(item, offsets, Arc::new(document), None);
        let schema = Arc::new(Schema::new(vec![Field::new("?_0", list.data_type().clone(), false)]));
        let parameter = RecordBatch::try_new(schema, vec![Arc::new(list)])
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        OwnedBatch::with_reservation(parameter, reservation)
    }

    pub fn from_owned_payload(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let batch = batch.batch();
        if batch.num_rows() == 0 || !batch.schema_ref().fields().iter().all(|field| Self::compatible(field, false)) {
            return Ok(None);
        }
        if batch.schema_ref().fields().iter().map(|field| field.name()).collect::<BTreeSet<_>>().len() != batch.num_columns() {
            return Ok(None);
        }
        let layout = DecodedDocumentLayout::new(batch.schema_ref().fields(), memory)?;
        Ok(Some(Self { array: StructArray::from(batch.clone()), layout, source_bytes: None, input: DocumentInput::Arrow }))
    }

    /// Retains compatible legacy document columns with complete entity IDs.
    /// `None` lets the caller use the legacy row reader.
    pub fn from_legacy_owned_payload(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_legacy_payload(batch, memory, legacy_owned_arrow_field)
    }

    /// Retains readable Root upload layouts while keeping the legacy source and receipt encoding.
    pub fn from_legacy_root_payload(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        Self::from_legacy_payload(batch, memory, |field| {
            legacy_root_arrow_field(field) || field.extension_type_name() == Some("uri")
                && field.data_type() == &DataType::Utf8 && !field.is_nullable() && compatible_document_metadata(field)
        })
    }

    fn from_legacy_payload(
        batch: &OwnedBatch, memory: &Arc<MemoryBudget>, compatible: fn(&arrow_schema::Field) -> bool,
    ) -> Result<Option<Self>> {
        let source = batch.batch();
        let fields = source.schema_ref().fields();
        if source.num_rows() == 0 || fields.iter().any(|field| {
            !legacy_owned_field_name(field.name()) || !compatible(field)
        })
            || fields.iter().map(|field| field.name()).collect::<BTreeSet<_>>().len() != fields.len()
        {
            return Ok(None);
        }
        let array = StructArray::from(source.clone());
        let layout = DecodedDocumentLayout::new(fields, memory)?;
        let documents = Self { array, layout, source_bytes: None, input: DocumentInput::Arrow };
        if !(0..documents.row_count()).all(|row| documents.owned_id_ref(row).is_ok()) {
            return Ok(None);
        }
        for (field, column) in fields.iter().zip(source.columns()) {
            let logical = documents.column_type(field.name()).expect("upload layout covers its fields");
            if (0..source.num_rows()).any(|row| !valid_argument_union_cell(field, column.as_ref(), row)) {
                return Ok(None);
            }
            if logical != &LogicalType::Nothing
                && rtdb_columnar::validate_readable_column(field, column.as_ref(), logical, memory).is_err()
            {
                return Ok(None);
            }
        }
        Ok(Some(Self { input: DocumentInput::LegacyOwned(LegacyOwnedPresence::Submitted), ..documents }))
    }

    /// Combines ordered document pages into the payload of one source operation.
    pub fn coalesce_pages(pages: Vec<Self>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        crate::source::coalesce_arrow_documents(pages, memory)
    }

    pub(crate) fn owned_id(&self, row: usize) -> Result<EntityId> {
        self.owned_id_ref(row).map(rtdb_columnar::EntityIdRef::into_owned)
    }

    pub(crate) fn owned_id_ref(&self, row: usize) -> Result<rtdb_columnar::EntityIdRef<'_>> {
        let (field, column) = self.array.fields().iter().zip(self.array.columns())
            .find(|(field, _)| field.name() == "_id")
            .ok_or_else(|| Anomaly::incorrect("rtdb.owned/missing-id", "Owned row has no _id"))?;
        rtdb_columnar::read_owned_entity_id(field, column.as_ref(), row)
    }

    pub(crate) fn select_owned_fields(&self, names: &BTreeSet<String>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let (fields, columns): (Vec<_>, Vec<_>) = self.array.fields().iter().zip(self.array.columns())
            .filter(|(field, _)| names.contains(field.name()))
            .map(|(field, column)| (Arc::clone(field), Arc::clone(column))).unzip();
        if fields.len() != names.len() {
            return Err(Anomaly::incorrect("rtdb.tx/document-presence", "Document presence names an unknown column"));
        }
        let fields = Fields::from(fields);
        let layout = DecodedDocumentLayout::new(&fields, memory)?;
        let array = StructArray::try_new_with_length(fields, columns, None, self.array.len())
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        Ok(Self { array, layout, source_bytes: self.source_bytes, input: DocumentInput::Arrow })
    }

    pub fn row_count(&self) -> usize { self.array.len() }
    pub(crate) fn array(&self) -> &StructArray { &self.array }
    pub(crate) fn columns(&self) -> &[String] { &self.layout.columns }
    pub(crate) fn column_type(&self, name: &str) -> Option<&LogicalType> { self.layout.types.get(name) }
    pub(crate) fn declared_types(&self) -> &StructFields<LogicalType> { &self.layout.types }
    pub(crate) fn source_bytes(&self) -> Option<usize> { self.source_bytes }

    pub(crate) fn legacy_owned_presence(&self) -> Option<&LegacyOwnedPresence> {
        match &self.input {
            DocumentInput::LegacyOwned(presence) => Some(presence),
            _ => None,
        }
    }

    pub(crate) fn with_legacy_owned_presence(mut self, fields: Option<Vec<BTreeSet<String>>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let Some(fields) = fields else {
            self.input = DocumentInput::LegacyOwned(LegacyOwnedPresence::Implicit);
            return Ok(self);
        };
        if fields.len() != self.row_count() {
            return Err(Anomaly::incorrect("rtdb.tx/document-presence", "Document presence must cover every row"));
        }
        let mut bytes = 128usize;
        for names in &fields {
            bytes = bytes.checked_add(128).ok_or_else(overflow)?;
            for name in names {
                if !self.layout.types.contains_key(name) {
                    return Err(Anomaly::incorrect("rtdb.tx/document-presence", "Document presence names an unknown column"));
                }
                bytes = bytes.checked_add(name.capacity() + 128).ok_or_else(overflow)?;
            }
        }
        self.input = DocumentInput::LegacyOwned(LegacyOwnedPresence::Exact(Arc::new(OwnedFieldPresence { fields, _reservation: memory.reserve(bytes)? })));
        Ok(self)
    }

    pub(crate) fn read_legacy_owned_rows(&self, memory: &Arc<MemoryBudget>) -> Result<Documents> {
        match self.legacy_owned_presence().expect("legacy owned documents carry a presence policy") {
            LegacyOwnedPresence::Exact(presence) => Documents::read_arrow_with_presence(&self.array, Arc::clone(&self.layout), true, Some(&presence.fields), memory),
            LegacyOwnedPresence::Implicit => Documents::read_arrow(&self.array, Arc::clone(&self.layout), true, memory),
            LegacyOwnedPresence::Submitted => {
                let rows = Documents::read_arrow(&self.array, Arc::clone(&self.layout), false, memory)?;
                let rows = Arc::into_inner(rows.0).expect("the uploaded row batch owns its rows");
                Documents::with_column_order(rows.rows, self.columns().to_vec(), memory)
            }
        }
    }

    pub(crate) fn legacy_owned_cell_is_present(&self, cell: rtdb_columnar::ArrowCell<'_>) -> Result<bool> {
        match self.legacy_owned_presence().expect("legacy owned documents carry a presence policy") {
            LegacyOwnedPresence::Submitted => Ok(true),
            LegacyOwnedPresence::Exact(presence) => Ok(presence.fields[cell.row].contains(cell.field.name())),
            LegacyOwnedPresence::Implicit => Ok(cell.field.name() == "_id"
                || (cell.field.data_type() == &DataType::Null && !cell.field.is_nullable()) || !cell.is_null()?),
        }
    }

    pub(crate) fn legacy_owned_cell(&self, name: &str, row: usize) -> Result<Option<rtdb_columnar::ArrowCell<'_>>> {
        for (field, array) in self.array.fields().iter().zip(self.array.columns()) {
            if field.name() == name {
                let cell = rtdb_columnar::ArrowCell { field, array: array.as_ref(), row };
                return Ok(self.legacy_owned_cell_is_present(cell)?.then_some(cell));
            }
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn to_rows(&self, omit_nulls: bool, memory: &Arc<MemoryBudget>) -> Result<Documents> {
        Documents::read_arrow(&self.array, Arc::clone(&self.layout), omit_nulls, memory)
    }

    pub(crate) fn iid(&self, row: usize, reader: Option<&(usize, ColumnReader)>, memory: &Arc<MemoryBudget>) -> Result<rtdb_types::Iid> {
        if let Some((index, reader)) = reader {
            let column = self.array.column(*index);
            if column.data_type() != &DataType::Null && column.is_valid(row) {
                if let Some(id) = reader.borrow_entity_id(column.as_ref(), row) { return Ok(id.iid()); }
                let _scratch = memory.reserve(rtdb_columnar::estimate_read_cell_bytes(&self.array.fields()[*index], column.as_ref(), row)?)?;
                if let Some(id) = reader.read_entity_id(column.as_ref(), row)? { return Ok(id.iid()); }
            }
        }
        if let Ok(Some(error)) = self.missing_float_document_id(row, memory) {
            return Err(error);
        }
        let slice = self.array.slice(row, 1);
        let rows = Documents::read_arrow(&slice, Arc::clone(&self.layout), false, memory)?;
        crate::source::document_id(&rows.rows()[0], memory).map(|id| id.iid())
    }

    pub(crate) fn missing_float_document_id(&self, row: usize, memory: &Arc<MemoryBudget>) -> Result<Option<Anomaly>> {
        use arrow_array::types::{Float32Type, Float64Type};
        use rtdb_format::transit::{self, TransitValue as T};

        fn convert(cell: ArrowCell<'_>, depth: usize, allow_bottom: bool, found_float: &mut bool, lease: &mut MemoryReservation) -> Result<Option<T>> {
            if depth > rtdb_types::transit_value::MAX_DEPTH { return Ok(None); }
            let physical = cell.physical()?;
            let bound = lease.bytes().checked_add(cell.field.name().len()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?;
            lease.grow_to(bound)?;
            Ok(Some(if let Some((field, array, range)) = physical.sequence_range()
                && physical.float_list_span().is_some() && arguments::compatible_float_cell(cell, None) {
                if depth == rtdb_types::transit_value::MAX_DEPTH && !range.is_empty() { return Ok(None); }
                *found_float = true;
                let bytes = range.len().checked_mul(std::mem::size_of::<T>()).and_then(|bytes| lease.bytes().checked_add(bytes)).ok_or_else(overflow)?;
                lease.grow_to(bytes)?;
                let items = range.map(|row| {
                    let cell = (ArrowCell { field, array, row }).physical()?;
                    Ok(if cell.is_null()? {
                        T::Null
                    } else if cell.field.data_type() == &DataType::Float32 {
                        T::Float(f64::from(cell.array.as_primitive::<Float32Type>().value(cell.row)))
                    } else {
                        T::Float(cell.array.as_primitive::<Float64Type>().value(cell.row))
                    })
                }).collect::<Result<_>>()?;
                T::Array(items)
            } else if matches!(cell.field.data_type(), DataType::Struct(_)) && let Some(fields) = cell.structure() {
                let mut document = StructFields::new();
                for child in fields {
                    let Some(value) = convert(child, depth + 1, true, found_float, lease)? else { return Ok(None); };
                    document.insert(child.field.name().clone(), value);
                }
                T::Map(document.into_iter().map(|(name, value)| (T::String(name), value)).collect())
            } else if matches!(cell.field.data_type(), DataType::List(_) | DataType::FixedSizeList(..))
                && cell.field.metadata().is_empty() && let Some(items) = cell.sequence() {
                let mut list = Vec::new();
                for child in items {
                    let Some(value) = convert(child, depth + 1, false, found_float, lease)? else { return Ok(None); };
                    list.push(value);
                }
                T::Array(list)
            } else if cell.field.data_type() == &DataType::Null {
                if !cell.field.is_nullable() && !allow_bottom { return Ok(None); }
                T::Null
            } else if cell.array.is_null(cell.row) || physical.is_null()? && arguments::compatible_float_cell(cell, None) {
                T::Null
            } else {
                if !matches!(physical.logical_type()?, LogicalType::Scalar(_)) { return Ok(None); }
                let read_bound = rtdb_columnar::estimate_read_cell_bytes(cell.field, cell.array, cell.row)?;
                lease.grow_to(lease.bytes().checked_add(read_bound).ok_or_else(overflow)?)?;
                let mut value = rtdb_columnar::read_value(cell.field, cell.array, cell.row)?;
                value.admit_object_dates()?;
                let bytes = rtdb_expr::text_allocation_bound(&value)?;
                lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(overflow)?)?;
                transit::value_to_transit(&value)?
            }))
        }

        let cells = self.array.fields().iter().zip(self.array.columns())
            .map(|(field, array)| ArrowCell { field, array: array.as_ref(), row });
        let mut scratch = memory.reserve(4096)?;
        let lease = Arc::get_mut(&mut scratch).expect("Missing ID context owns its reservation");
        let mut document = Vec::new();
        let mut found_float = false;
        for cell in cells {
            let Some(value) = convert(cell, 1, true, &mut found_float, lease)? else { return Ok(None); };
            document.push((T::String(cell.field.name().clone()), value));
        }
        if !found_float { return Ok(None); }
        let document = T::Map(document);
        let _json = memory.reserve(transit::json_value_allocation_bound(&document)?)?;
        Ok(Some(Anomaly::incorrect("missing-id", "missing '_id'").with_transit_context("doc", &document)?))
    }

    pub(crate) fn id_reader(&self) -> Result<Option<(usize, ColumnReader)>> {
        self.array.fields().iter().enumerate().find(|(_, field)| field.name() == "_id")
            .map(|(index, field)| Ok((index, ColumnReader::new(field)?))).transpose()
    }
}

fn ingest_type(field: &arrow_schema::Field, array: &dyn Array, mode: IngestMode) -> Result<Option<LogicalType>> {
    if !field.metadata().is_empty() || array.is_empty()
        || matches!(mode, IngestMode::Mutation) && !array.is_valid(0) {
        return Ok(None);
    }
    if matches!(mode, IngestMode::External) && array.logical_null_count() == array.len() {
        if field.data_type() == &DataType::Null && !field.is_nullable() { return Ok(None); }
        return Ok(Some(LogicalType::Null));
    }
    Ok(Some(match field.data_type() {
        DataType::Struct(fields) if array.null_count() == 0 => {
            let array = array.as_struct();
            let mut types = StructFields::new();
            for (field, column) in fields.iter().zip(array.columns()) {
                let Some(ty) = ingest_type(field, column.as_ref(), mode)? else { return Ok(None); };
                if types.insert(field.name().clone(), ty).is_some() { return Ok(None); }
            }
            LogicalType::Struct(types)
        }
        DataType::List(field) if array.null_count() == 0 => {
            let array = array.as_list::<i32>();
            let offsets = array.value_offsets();
            let start = offsets[0] as usize;
            let end = offsets[array.len()] as usize;
            let values = array.values().slice(start, end - start);
            let Some(element) = ingest_type(field, values.as_ref(), mode)? else { return Ok(None); };
            LogicalType::List { kind: ListKind::List, element: Box::new(element) }
        }
        DataType::FixedSizeList(field, width) if array.null_count() == 0 => {
            let array = array.as_fixed_size_list();
            let Some(element) = ingest_type(field, array.values().as_ref(), mode)? else { return Ok(None); };
            LogicalType::List { kind: ListKind::FixedSizeList(*width), element: Box::new(element) }
        }
        ty if ty != &DataType::Null && (plain_scalar_field(field) || field.metadata().is_empty() && matches!(
            ty, DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_)
        )) && (matches!(mode, IngestMode::External) || !matches!(ty, DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64)) => {
            rtdb_columnar::execution_field_type(&field.clone().with_nullable(array.null_count() != 0))?
        }
        _ => return Ok(None),
    }))
}

fn ingest_layout(array: ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target { return Ok(array); }
    if *target == DataType::Null && array.logical_null_count() == array.len() {
        return Ok(Arc::new(arrow_array::NullArray::new(array.len())));
    }
    let data = array.to_data();
    let layout_error = |message| Anomaly::incorrect("rtdb.tx/document-schema", message);
    let children = match target {
        DataType::Struct(fields) => fields.iter().collect::<Vec<_>>(),
        DataType::List(field) | DataType::FixedSizeList(field, _) => vec![field],
        _ => return Err(layout_error("Ingest leaf disagrees with its canonical layout")),
    };
    if children.len() != data.child_data().len() {
        return Err(layout_error("Ingest children disagree with their canonical layout"));
    }
    let children = children.into_iter().zip(data.child_data()).map(|(child, data)| {
        ingest_layout(arrow_array::make_array(data.clone()), child.data_type()).map(|array| array.to_data())
    }).collect::<Result<Vec<_>>>()?;
    let data = data.into_builder().data_type(target.clone()).child_data(children).build()
        .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
    Ok(arrow_array::make_array(data))
}

pub(crate) fn contains_fixed_size_list(layout: &DataType) -> bool {
    match layout {
        DataType::FixedSizeList(..) => true,
        DataType::Struct(fields) => fields.iter().any(|field| contains_fixed_size_list(field.data_type())),
        DataType::List(field) => contains_fixed_size_list(field.data_type()),
        DataType::Union(fields, arrow_schema::UnionMode::Dense) => fields.iter().any(|(_, field)| contains_fixed_size_list(field.data_type())),
        _ => false,
    }
}

pub(crate) fn float_vector_field(field: &arrow_schema::Field) -> bool {
    if field.extension_type_name().is_some() { return false; }
    match field.data_type() {
        DataType::List(item) | DataType::FixedSizeList(item, _) if item.metadata().is_empty() => {
            matches!(item.data_type(), DataType::Float32 | DataType::Float64)
                || rtdb_types::ListValue::supports_float_union(item.data_type())
        }
        DataType::Union(legs, arrow_schema::UnionMode::Dense) => legs.iter().any(|(_, leg)| float_vector_field(leg))
            && legs.iter().all(|(_, leg)| plain_scalar_field(leg) || float_vector_field(leg)),
        _ => false,
    }
}

pub(crate) fn legacy_owned_field_name(name: &str) -> bool {
    !name.starts_with('_') || matches!(name, "_id" | "_fn" | "_valid_from" | "_valid_to")
}

pub(crate) fn legacy_owned_arrow_field(field: &arrow_schema::Field) -> bool {
    legacy_arrow_field(field, false)
}

pub(crate) fn legacy_root_arrow_field(field: &arrow_schema::Field) -> bool {
    legacy_arrow_field(field, true)
}

fn legacy_arrow_field(field: &arrow_schema::Field, root: bool) -> bool {
    let compatible = |field: &arrow_schema::Field| legacy_arrow_field(field, root);
    if field.extension_type_name().is_some() {
        return uuid_field(field) || root && field.extension_type_name() == Some("xt/clj-keyword")
            && from_field(field).is_ok();
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            let mut names = BTreeSet::new();
            fields.iter().all(|field| names.insert(field.name()) && compatible(field))
        }
        DataType::List(item) | DataType::FixedSizeList(item, _) => compatible(item),
        DataType::Union(legs, arrow_schema::UnionMode::Dense) => {
            let mut names = BTreeSet::new();
            let Ok(LogicalType::Union(types)) = from_field(field) else { return false; };
            legs.iter().all(|(_, leg)| {
                if !compatible(leg) { return false; }
                let Ok(ty) = from_field(leg) else { return false; };
                let Ok(name) = rtdb_columnar::leg_name(&ty) else { return false; };
                names.insert(name) && types.iter().all(|candidate| {
                    rtdb_columnar::leg_name(candidate).is_ok_and(|name| name != *leg.name() || *candidate == ty)
                })
            })
        }
        DataType::Null | DataType::Float16 | DataType::Date32 | DataType::Date64
        | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) | DataType::Timestamp(..) => true,
        DataType::Interval(_) | DataType::Decimal32(..) | DataType::Decimal64(..)
        | DataType::Decimal128(..) | DataType::Decimal256(..) if root => from_field(field).is_ok(),
        _ => plain_scalar_field(&field.clone().with_metadata(Default::default())),
    }
}

pub(crate) fn compatible_document_metadata(field: &arrow_schema::Field) -> bool {
    let metadata = field.metadata();
    metadata.is_empty() || from_field(field).and_then(|ty| rtdb_columnar::to_field(field.name(), &ty))
        .is_ok_and(|canonical| canonical.metadata() == metadata)
}

fn uuid_field(field: &arrow_schema::Field) -> bool {
    let metadata = field.metadata();
    field.data_type() == &DataType::FixedSizeBinary(16)
        && metadata.len() == 2
        && metadata.get(rtdb_columnar::EXTENSION_NAME_KEY).is_some_and(|name| name == "uuid")
        && metadata.get(rtdb_columnar::EXTENSION_METADATA_KEY).is_some_and(String::is_empty)
}

fn contains_uuid(field: &arrow_schema::Field) -> bool {
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().any(|field| contains_uuid(field)),
        DataType::List(field) | DataType::FixedSizeList(field, _) => contains_uuid(field),
        DataType::Union(fields, _) => fields.iter().any(|(_, field)| contains_uuid(field)),
        _ => uuid_field(field),
    }
}

fn canonical_uuid_unions(field: &arrow_schema::Field) -> bool {
    match field.data_type() {
        DataType::Struct(fields) => fields.iter().all(|field| canonical_uuid_unions(field)),
        DataType::List(field) | DataType::FixedSizeList(field, _) => canonical_uuid_unions(field),
        DataType::Union(fields, _) => !contains_uuid(field) || fields.iter().all(|(_, field)| {
            from_field(field).and_then(|logical| rtdb_columnar::leg_name(&logical)).is_ok_and(|name| name == *field.name())
                && canonical_uuid_unions(field)
        }),
        _ => true,
    }
}

fn canonical_nullable_uuid(array: &ArrayRef, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    let bound = array.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let mut reservation = memory.reserve(bound)?;
    let cells = (0..array.len()).map(|index| Some((0, index))).collect::<Vec<_>>();
    let column = rtdb_columnar::copy_nullable_cells(&[array.as_ref()], &cells)?;
    drop(cells);
    Arc::get_mut(&mut reservation).expect("UUID selection owns its reservation").shrink_to(column.get_array_memory_size())?;
    rtdb_columnar::retain_array(column, &reservation)
}

fn canonical_mutation_scalar(array: &dyn Array) -> bool {
    if array.data_type() == &DataType::Null { return true; }
    let nulls = array.nulls();
    if nulls.is_some_and(|nulls| nulls.null_count() == 0 || !canonical_bitmap(nulls.inner())) {
        return false;
    }
    let null_rows = || nulls.into_iter().flat_map(|nulls| nulls.iter().enumerate())
        .filter_map(|(row, valid)| (!valid).then_some(row));
    let data = array.to_data();
    if data.offset() != 0 { return false; }
    let len = array.len();
    match (array.data_type(), data.buffers()) {
        (DataType::Boolean, [_]) => {
            let values = array.as_boolean().values();
            canonical_bitmap(values) && null_rows().all(|row| !values.value(row))
        }
        (DataType::Utf8 | DataType::Binary, [offsets, values]) => {
            let offsets = offsets.typed_data::<i32>();
            offsets.len().checked_sub(1) == Some(len) && offsets.first() == Some(&0)
                && offsets.last().and_then(|&end| usize::try_from(end).ok()) == Some(values.len())
                && null_rows().all(|row| offsets[row] == offsets[row + 1])
        }
        (ty, [values]) => {
            let width = match ty {
                DataType::FixedSizeBinary(width) => usize::try_from(*width).ok(),
                ty => ty.primitive_width(),
            };
            width.is_some_and(|width| len.checked_mul(width) == Some(values.len())
                && null_rows().all(|row| values[row * width..(row + 1) * width].iter().all(|&byte| byte == 0)))
        }
        _ => false,
    }
}

fn canonical_bitmap(bitmap: &arrow_buffer::BooleanBuffer) -> bool {
    let values = bitmap.inner();
    let bits = bitmap.len() % 8;
    bitmap.offset() == 0 && values.len() == bitmap.len().div_ceil(8)
        && (bits == 0 || values.last().is_some_and(|byte| *byte >> bits == 0))
}

pub(crate) fn canonical_argument_column(pages: &[&RecordBatch], index: usize, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    if let [page] = pages {
        let array = page.column(index);
        let nulls = array.logical_null_count();
        if nulls == array.len() || (nulls == 0 && !matches!(array.data_type(), DataType::Boolean | DataType::Struct(_) | DataType::List(_) | DataType::FixedSizeList(..))) {
            return Ok(Arc::clone(array));
        }
    }
    copy_argument_column(pages, index, memory)
}

fn copy_argument_column(pages: &[&RecordBatch], index: usize, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    // The source body digest includes the payload beneath null slots and the padding on a Boolean vector.
    let rows = pages.iter().try_fold(0usize, |rows, page| rows.checked_add(page.num_rows()).ok_or_else(overflow))?;
    let base = rows.checked_mul(64).and_then(|bytes| bytes.checked_add(pages.len().checked_mul(64)?))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let bound = pages.iter().try_fold(base, |bytes, page| {
        let column = page.column(index);
        let field = page.schema_ref().field(index);
        if !matches!(field.data_type(), DataType::Struct(_) | DataType::List(_) | DataType::FixedSizeList(..)) {
            return bytes.checked_add(column.get_array_memory_size().checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow);
        }
        let bytes = bytes.checked_add(field.size().checked_mul(8).ok_or_else(overflow)?).ok_or_else(overflow)?;
        (0..page.num_rows()).try_fold(bytes, |bytes, row| {
            bytes.checked_add(rtdb_columnar::estimate_copy_cell_bytes(field, column.as_ref(), row)?).ok_or_else(overflow)
        })
    })?;
    let mut reservation = memory.reserve(bound)?;
    let mut cells = Vec::with_capacity(rows);
    let sources = pages.iter().enumerate().map(|(source, page)| {
        cells.extend((0..page.num_rows()).map(|row| Some((source, row))));
        page.column(index).as_ref()
    }).collect::<Vec<_>>();
    let column = rtdb_columnar::copy_nullable_cells(&sources, &cells)?;
    drop(sources);
    drop(cells);
    Arc::get_mut(&mut reservation).expect("argument selection owns its reservation").shrink_to(column.get_array_memory_size())?;
    rtdb_columnar::retain_array(column, &reservation)
}

pub(crate) fn plain_scalar_field(field: &arrow_schema::Field) -> bool {
    field.metadata().is_empty() && match field.data_type() {
        DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) => true,
        DataType::Null => field.is_nullable(),
        _ => false,
    }
}

#[test]
fn temporal_ingest_preserves_arrow_layout_and_source_bytes() {
    use rtdb_types::TimeUnit;
    use arrow_schema::Field;

    let memory = MemoryBudget::new(1 << 24);
    let docs = vec![
        StructFields::from([
            ("_id".into(), Value::I64(1)),
            ("day".into(), Value::DateDay(2)),
            ("millis".into(), Value::DateMilli(-86_400_001)),
            ("time".into(), Value::Time { value: 3, unit: TimeUnit::Microsecond, bit_width: 64 }),
            ("duration".into(), Value::Duration { value: 4, unit: TimeUnit::Nanosecond }),
        ]),
        StructFields::from([("_id".into(), Value::I64(2))]),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
        Arc::new(arrow_array::Date32Array::from(vec![Some(2), None])),
        Arc::new(arrow_array::Date64Array::from(vec![Some(-86_400_001), None])),
        Arc::new(arrow_array::Time64MicrosecondArray::from(vec![Some(3), None])),
        Arc::new(arrow_array::DurationNanosecondArray::from(vec![Some(4), None])),
    ];
    let fields = docs[0].keys().zip(&columns).map(|(name, column)| {
        Field::new(name, column.data_type().clone(), name != "_id")
    }).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let batch = OwnedBatch::new(batch, &memory).unwrap();
    let documents = ArrowDocuments::from_ingest_batch(&batch, &memory).unwrap().expect("temporal Arrow admission");
    let table = TableRef::new("public", "docs");
    let arrow = TxOp::PutArrowDocs { table: table.clone(), documents };
    let rows = TxOp::PutDocs { table, documents: Documents::new(docs, &memory).unwrap(), valid_from: None, valid_to: None };
    let encode = |op| crate::SourceTransaction::new(vec![op], SubmitOptions::default(), &memory).unwrap().encode().unwrap();
    let arrow = encode(arrow);
    assert_eq!(arrow.as_slice(), encode(rows).as_slice());
}

#[test]
fn scalar_ingest_batches_match_row_encoded_source_bytes() {
    use arrow_array::{BooleanArray, Int64Array, StringArray, UInt64Array};
    use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
    use arrow_schema::Field;

    let memory = MemoryBudget::new(1 << 24);
    let fields = vec![
        Field::new("_id", DataType::UInt64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("number", DataType::Int64, true),
        Field::new("text", DataType::Utf8, false),
    ];
    let flags = BooleanArray::new(BooleanBuffer::new(Buffer::from_vec(vec![0xff_u8]), 0, 3), None);
    let validity = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(vec![0xfb_u8]), 0, 3));
    let numbers = Int64Array::new(ScalarBuffer::from(vec![8, 17, 999]), Some(validity));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![0, u64::MAX, 2])),
        Arc::new(flags),
        Arc::new(numbers),
        Arc::new(StringArray::from(vec!["prefix", "left", "right"])),
    ];
    let batch = OwnedBatch::new(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap().slice(1, 2), &memory).unwrap();
    let docs = ArrowDocuments::from_ingest_batch(&batch, &memory).unwrap().unwrap();
    let rows = (0..batch.batch().num_rows()).map(|row| {
        batch.batch().schema_ref().fields().iter().zip(batch.batch().columns()).map(|(field, column)| {
            let value = rtdb_columnar::read_execution_value(field, column.as_ref(), row).unwrap();
            (field.name().clone(), value)
        }).collect()
    }).collect();
    let table = TableRef::new("public", "docs");
    let row_operation = TxOp::PutDocs { table: table.clone(), documents: Documents::new(rows, &memory).unwrap(), valid_from: None, valid_to: None };
    let arrow_operation = TxOp::PutArrowDocs { table, documents: docs };
    let encoded = |operation| crate::SourceTransaction::new(vec![operation], SubmitOptions::default(), &memory).unwrap().encode().unwrap();
    assert_eq!(encoded(arrow_operation).as_slice(), encoded(row_operation).as_slice());

    let null_first = OwnedBatch::new(batch.batch().slice(1, 1), &memory).unwrap();
    let null_first = ArrowDocuments::from_ingest_batch(&null_first, &memory).unwrap().unwrap();
    assert_eq!(null_first.array().column_by_name("number").unwrap().data_type(), &DataType::Null);
}

#[test]
fn bulk_argument_pages_keep_source_order_and_canonicalize_leading_nulls() {
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::Field;

    let memory = MemoryBudget::new(1 << 24);
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let page = |id, payload: Option<&str>| {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![id])),
            Arc::new(StringArray::from(vec![payload])),
        ];
        OwnedBatch::new(RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap(), &memory).unwrap()
    };
    let mut args = SqlArguments::from_arrow(page(7, None), &memory).unwrap();
    args.append(SqlArguments::from_arrow(page(3, Some("next")), &memory).unwrap()).unwrap();
    let docs = args.ingest_documents(&memory).unwrap().unwrap();
    let ids = docs.array().column_by_name("_id").unwrap().as_primitive::<Int64Type>();
    assert_eq!(ids.values().as_ref(), &[7, 3]);
    let payload = docs.array().column_by_name("payload").unwrap().as_string::<i32>();
    assert!(payload.is_null(0));
    assert_eq!(payload.value(1), "next");
}

#[test]
fn nested_ingest_reserves_child_positions_before_gathering() {
    use arrow_array::Int64Array;
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::Field;

    let count = 1 << 16;
    let children = StructArray::try_new_with_length(Fields::empty(), Vec::new(), None, count).unwrap();
    let list = ListArray::new(Arc::new(Field::new("item", children.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, count as i32])), Arc::new(children), None);
    let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), Field::new("items", list.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1])), Arc::new(list)]).unwrap();
    let source = MemoryBudget::new(1 << 24);
    let batch = OwnedBatch::new(batch, &source).unwrap();
    let constrained = MemoryBudget::new(1 << 20);
    let error = ArrowDocuments::from_ingest_batch(&batch, &constrained).unwrap_err();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(constrained.used_bytes(), 0);
}

#[test]
fn nested_ingest_does_not_charge_the_unselected_list_prefix() {
    use arrow_array::Int64Array;
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::Field;

    let prefix = 1 << 20;
    let source = MemoryBudget::new(1 << 25);
    let memory = MemoryBudget::new(1 << 20);
    let list = ListArray::new(Arc::new(Field::new("item", DataType::Int64, false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![prefix, prefix + 1])),
        Arc::new(Int64Array::from(vec![7; prefix as usize + 1])), None);
    let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false), Field::new("items", list.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1])), Arc::new(list)]).unwrap();
    let batch = OwnedBatch::new(batch, &source).unwrap();
    let output = ArrowDocuments::from_ingest_batch(&batch, &memory).unwrap().expect("Expected the selected list");
    assert_eq!(output.array().column(1).as_list::<i32>().values().len(), 1);
}

#[test]
fn nested_ingest_compacts_list_offsets_and_uses_the_canonical_source_layout() {
    use arrow_array::{BooleanArray, FixedSizeListArray, Int64Array, UInt64Array};
    use arrow_buffer::{BooleanBuffer, Buffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::Field;

    let memory = MemoryBudget::new(1 << 24);
    let list = ListArray::new(
        Arc::new(Field::new("item", DataType::UInt64, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![1i32, 3, 4])),
        Arc::new(UInt64Array::from(vec![0, 2, u64::MAX, 7, 99])),
        None,
    );
    let bits = BooleanBuffer::new(Buffer::from_vec(vec![0xff_u8]), 0, 4);
    let flags = FixedSizeListArray::new(
        Arc::new(Field::new("flag", DataType::Boolean, true)), 2,
        Arc::new(BooleanArray::new(bits, None)), None,
    );
    let record = StructArray::new(
        Fields::from(vec![Field::new("values", list.data_type().clone(), true), Field::new("flags", flags.data_type().clone(), true)]),
        vec![Arc::new(list), Arc::new(flags)],
        None,
    );
    let fields = vec![Field::new("_id", DataType::Int64, false), Field::new("record", record.data_type().clone(), true)];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(record)]).unwrap();
    let batch = OwnedBatch::new(batch, &memory).unwrap();
    let docs = ArrowDocuments::from_ingest_batch(&batch, &memory).unwrap().expect("Expected compatible nested Arrow documents");
    let rows = (0..batch.batch().num_rows()).map(|row| {
        batch.batch().schema_ref().fields().iter().zip(batch.batch().columns()).map(|(field, column)| {
            (field.name().clone(), rtdb_columnar::read_execution_value(field, column.as_ref(), row).unwrap())
        }).collect()
    }).collect();
    let table = TableRef::new("public", "docs");
    let row_operation = TxOp::PutDocs { table: table.clone(), documents: Documents::new(rows, &memory).unwrap(), valid_from: None, valid_to: None };
    let arrow_operation = TxOp::PutArrowDocs { table, documents: docs };
    let encoded = |operation| crate::SourceTransaction::new(vec![operation], SubmitOptions::default(), &memory).unwrap().encode().unwrap();
    assert_eq!(encoded(arrow_operation).as_slice(), encoded(row_operation).as_slice());
}

fn normalized_document_array(array: &ArrayRef, memory: &Arc<MemoryBudget>) -> Result<Option<ArrayRef>> {
    Ok(Some(match array.data_type() {
        DataType::UInt8 => signed_column::<UInt8Type, Int8Type>(array.as_ref()),
        DataType::UInt16 => signed_column::<UInt16Type, Int16Type>(array.as_ref()),
        DataType::UInt32 => signed_column::<UInt32Type, Int32Type>(array.as_ref()),
        DataType::UInt64 => signed_column::<UInt64Type, Int64Type>(array.as_ref()),
        DataType::Struct(fields) => {
            let source = array.as_struct();
            let children = source.columns().iter().map(|child| normalized_document_array(child, memory)).collect::<Result<Vec<_>>>()?;
            let Some(children) = children.into_iter().collect::<Option<Vec<_>>>() else { return Ok(None); };
            if source.columns().iter().zip(&children).all(|(source, child)| Arc::ptr_eq(source, child)) { return Ok(Some(Arc::clone(array))); }
            let fields = fields.iter().zip(&children).map(|(field, child)| normalized_field(field, child)).collect::<Fields>();
            match StructArray::try_new_with_length(fields, children, source.nulls().cloned(), source.len()) {
                Ok(array) => Arc::new(array),
                Err(_) => return Ok(None),
            }
        }
        DataType::List(field) => {
            let source = array.as_list::<i32>();
            let Some(values) = normalized_document_array(source.values(), memory)? else { return Ok(None); };
            if Arc::ptr_eq(source.values(), &values) { return Ok(Some(Arc::clone(array))); }
            let field = normalized_field(field, &values);
            match ListArray::try_new(Arc::new(field), source.offsets().clone(), values, source.nulls().cloned()) {
                Ok(array) => Arc::new(array),
                Err(_) => return Ok(None),
            }
        }
        DataType::FixedSizeList(field, width) => {
            let source = array.as_fixed_size_list();
            let Some(values) = normalized_document_array(source.values(), memory)? else { return Ok(None); };
            if Arc::ptr_eq(source.values(), &values) { return Ok(Some(Arc::clone(array))); }
            let field = Arc::new(normalized_field(field, &values));
            let result = source.to_data().into_builder().data_type(DataType::FixedSizeList(field, *width))
                .child_data(vec![values.to_data()]).build();
            match result {
                Ok(data) => arrow_array::make_array(data),
                Err(_) => return Ok(None),
            }
        }
        DataType::Map(field, sorted) => {
            let source = array.as_map();
            let entries = Arc::new(source.entries().clone()) as ArrayRef;
            let Some(values) = normalized_document_array(&entries, memory)? else { return Ok(None); };
            if Arc::ptr_eq(&values, &entries) { return Ok(Some(Arc::clone(array))); }
            let field = Arc::new(normalized_field(field, &values));
            let result = source.to_data().into_builder().data_type(DataType::Map(field, *sorted))
                .child_data(vec![values.to_data()]).build();
            match result {
                Ok(data) => arrow_array::make_array(data),
                Err(_) => return Ok(None),
            }
        }
        DataType::RunEndEncoded(ends, values) => {
            let source = arrow_schema::Field::new("", DataType::RunEndEncoded(Arc::clone(ends), Arc::clone(values)), false);
            let mut target = values.as_ref().clone();
            if array.logical_null_count() != 0 { target = target.with_nullable(true); }
            let bytes = rtdb_columnar::arrow_relayout_bound(array.as_ref(), &source, &target)?;
            let _scratch = memory.reserve(bytes)?;
            let (_, expanded) = rtdb_columnar::relayout_arrow_column(Arc::clone(array), &source, &target)?;
            let owned = memory.reserve(expanded.get_array_memory_size())?;
            let expanded = rtdb_columnar::retain_array(expanded, &owned)?;
            return normalized_document_array(&expanded, memory);
        }
        DataType::Union(_, arrow_schema::UnionMode::Dense) => return union::normalize(array, memory),
        _ => Arc::clone(array),
    }))
}

fn normalized_field(field: &arrow_schema::Field, array: &ArrayRef) -> arrow_schema::Field {
    let changed = array.data_type() != field.data_type();
    if changed && array.data_type() == &DataType::Null {
        return arrow_schema::Field::new(field.name(), DataType::Null, true);
    }
    let nullable = field.is_nullable() || (changed && !matches!(array.data_type(), DataType::Union(..)) && (
        array.is_nullable() || (matches!(field.data_type(), DataType::Union(..)) && from_field(field).is_ok_and(|ty| ty.nullable()))
    ));
    field.clone().with_data_type(array.data_type().clone()).with_nullable(nullable)
}

fn signed_column<From: ArrowPrimitiveType, To: ArrowPrimitiveType>(array: &dyn Array) -> ArrayRef {
    let source = array.as_primitive::<From>();
    let values = arrow_buffer::ScalarBuffer::from(source.values().inner().clone());
    Arc::new(PrimitiveArray::<To>::new(values, source.nulls().cloned()))
}

#[derive(Clone, Debug)]
pub struct EntityIds { ids: Arc<Vec<EntityId>>, _reservation: Arc<MemoryReservation> }
impl EntityIds {
    pub fn new(ids: Vec<EntityId>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = ids.iter().try_fold(ids.capacity().checked_mul(std::mem::size_of::<EntityId>()).ok_or_else(overflow)?, |bytes, id| {
            if matches!(id, EntityId::Binary(_)) {
                return Err(Anomaly::invalid_entity_id("An ID must be a string, keyword, signed integer, or UUID"));
            }
            let extra = id.retained_bytes();
            bytes.checked_add(extra).ok_or_else(overflow)
        })?;
        Ok(Self { ids: Arc::new(ids), _reservation: memory.reserve(bytes.checked_add(128).ok_or_else(overflow)?)? })
    }
    pub fn ids(&self) -> &[EntityId] { &self.ids }
}

pub(crate) enum MetadataErasureUnit {
    Table { table: TableRef, id: EntityId },
    Owned { relation: rtdb_types::OwnedRelation, id: EntityId },
}

impl MetadataErasureUnit {
    fn parse(value: &Value) -> Result<Option<Self>> {
        let Value::Struct(metadata) = value else { return Ok(None); };
        let Some(target) = metadata.get("rtdb_erasure_unit_v1") else { return Ok(None); };
        let invalid = || Anomaly::incorrect("rtdb.tx/metadata-erasure-unit",
            "Metadata erasure requires one table or owned relation and an exact entity ID");
        let Value::Struct(fields) = target else { return Err(invalid()); };
        if fields.len() != 2 { return Err(invalid()); }
        let id = fields.get("id").ok_or_else(invalid)?;
        let target = (fields.get("table"), fields.get("owned_relation"));
        let id = if matches!(target, (None, Some(Value::Utf8(_)))) {
            id.owned_entity_id()?
        } else {
            id.entity_id()?
        };
        let unit = match target {
            (Some(Value::Utf8(table)), None) => {
                let table = TableRef::parse(table)?;
                if crate::source::is_reserved_write_schema(&table.schema) { return Err(invalid()); }
                Self::Table { table, id }
            }
            (None, Some(Value::Utf8(relation))) => {
                let relation = rtdb_types::OwnedRelation::new(relation)?;
                if !relation.is_root_data() { return Err(invalid()); }
                Self::Owned { relation, id }
            }
            _ => return Err(invalid()),
        };
        Ok(Some(unit))
    }
}

#[derive(Clone, Debug)]
pub struct UserMetadata { value: Arc<Value>, _reservation: Arc<MemoryReservation> }
impl UserMetadata {
    pub fn new(mut value: Value, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if !matches!(value, Value::Struct(_)) { return Err(Anomaly::incorrect("xtdb/invalid-user-metadata", "Transaction metadata must be a document")); }
        value.admit_object_dates()?;
        MetadataErasureUnit::parse(&value)?;
        let reservation = memory.reserve(value.retained_bytes()?.checked_add(128).ok_or_else(overflow)?)?;
        Ok(Self { value: Arc::new(value), _reservation: reservation })
    }
    pub fn value(&self) -> &Value { &self.value }

    pub(crate) fn erasure_unit(&self) -> Result<Option<MetadataErasureUnit>> { MetadataErasureUnit::parse(self.value()) }
}

#[derive(Clone, Debug)]
enum ArgumentChunk {
    Rows { rows: Vec<Vec<Value>>, _reservation: Arc<MemoryReservation> },
    Arrow { batch: OwnedBatch, types: Arc<Vec<LogicalType>>, source_layout: SourceArgumentLayout, _reservation: Arc<MemoryReservation> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SourceArgumentLayout { Uploaded, Canonical, CanonicalRow, BuiltFromRows }

impl ArgumentChunk {
    fn len(&self) -> usize {
        match self {
            Self::Rows { rows, .. } => rows.len(),
            Self::Arrow { batch, .. } => batch.batch().num_rows(),
        }
    }

    fn row(&self, index: usize, memory: &Arc<MemoryBudget>) -> Result<ArgumentRow<'_>> {
        match self {
            Self::Rows { rows, .. } => Ok(ArgumentRow::borrowed(&rows[index])),
            Self::Arrow { batch, types, .. } => {
                let batch = batch.batch();
                let schema = batch.schema_ref();
                let columns = || schema.fields().iter().zip(batch.columns());
                let base = types.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?;
                let bound = columns().try_fold(base, |bytes, (field, column)| {
                    bytes.checked_add(rtdb_columnar::estimate_read_value_bytes(field, column.as_ref(), index)?)
                        .ok_or_else(overflow)
                })?;
                let reservation = memory.reserve(bound)?;
                let mut values = types.iter().zip(columns()).map(|(ty, (field, column))| {
                    rtdb_columnar::read_value_as(ty, field, column.as_ref(), index)
                }).collect::<Result<Vec<_>>>()?;
                values.iter_mut().try_for_each(Value::admit_object_dates)?;
                Ok(ArgumentRow { values: Cow::Owned(values), _reservation: Some(reservation) })
            }
        }
    }
}

pub struct ArgumentRow<'a> {
    values: Cow<'a, [Value]>,
    _reservation: Option<Arc<MemoryReservation>>,
}

#[derive(Clone, Copy)]
pub(crate) enum BoundArgumentRow<'a> {
    Values { values: &'a [Value], types: Option<&'a [LogicalType]> },
    Arrow { batch: &'a OwnedBatch, row: usize, types: &'a [LogicalType] },
}

impl BoundArgumentRow<'_> {
    pub(crate) fn width(self) -> usize {
        match self { Self::Values { values, .. } => values.len(), Self::Arrow { batch, .. } => batch.batch().num_columns() }
    }
}

impl<'a> ArgumentRow<'a> {
    pub(crate) fn borrowed(values: &'a [Value]) -> Self {
        Self { values: Cow::Borrowed(values), _reservation: None }
    }
}

impl Deref for ArgumentRow<'_> {
    type Target = [Value];

    fn deref(&self) -> &[Value] { &self.values }
}

/// Funded logical types in the field order of one immutable Arrow argument schema.
pub struct ArrowArgumentTypes {
    schema: SchemaRef,
    types: Vec<LogicalType>,
    reservation: Arc<MemoryReservation>,
}

impl ArrowArgumentTypes {
    /// Validates readable values in structurally admitted columns before the caller copies or retains them.
    pub fn for_readable_columns(
        schema: SchemaRef, columns: &[ArrayRef], memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let invalid_layout = || Anomaly::incorrect("rtdb.tx/argument-schema", "Arrow argument columns must match their schema");
        if schema.fields().len() != columns.len() { return Err(invalid_layout()); }
        Self::translate(schema, memory, |index, field, logical| {
            let column = columns[index].as_ref();
            if field.data_type() != column.data_type() { return Err(invalid_layout()); }
            rtdb_columnar::validate_readable_column(field, column, logical, memory)
        })
    }

    fn translate(
        schema: SchemaRef, memory: &Arc<MemoryBudget>,
        mut validate: impl FnMut(usize, &Field, &LogicalType) -> Result<()>,
    ) -> Result<Self> {
        let reservation = memory.reserve(rtdb_format::ipc::schema_translation_bound(&schema)?)?;
        let mut types = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            let logical = from_field(field)?;
            validate(index, field, &logical)?;
            types.push(logical);
        }
        Ok(Self { schema, types, reservation })
    }
}

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
        for row in &mut rows { row.iter_mut().try_for_each(Value::admit_object_dates)?; }
        let mut bytes = rows.capacity().checked_mul(std::mem::size_of::<Vec<Value>>()).ok_or_else(overflow)?;
        for row in &rows {
            bytes = bytes.checked_add(row.capacity().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?).ok_or_else(overflow)?;
            for value in row { bytes = bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow)?; }
        }
        let schema_reservation = memory.reserve(schema_bytes(&types)?)?;
        let row_count = rows.len();
        let chunk = Arc::new(ArgumentChunk::Rows { rows, _reservation: memory.reserve(bytes.checked_add(256).ok_or_else(overflow)?)? });
        Ok(Self { types: Arc::new(types), chunks: Arc::new(vec![(0, chunk)]), memory: Arc::clone(memory), schema_reservation, row_count })
    }
    pub fn from_arrow(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::arrow(batch, SourceArgumentLayout::Uploaded, memory)
    }
    /// Retains the binding in Arrow and preserves the canonical field layout when encoding SQL source arguments.
    pub fn from_arrow_with_canonical_source(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::arrow(batch, SourceArgumentLayout::Canonical, memory)
    }

    /// Reuses the admitted types when the resulting batch keeps the very same schema.
    pub fn from_admitted_arrow_with_canonical_source(batch: OwnedBatch, admitted: ArrowArgumentTypes) -> Result<Self> {
        Self::arrow_with_types(batch, SourceArgumentLayout::Canonical, admitted)
    }

    /// Retains a single argument row built with the source row writer's canonical fields and buffers.
    pub fn from_canonical_arrow_row(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if batch.batch().num_rows() != 1 {
            return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Expected one canonical SQL argument row"));
        }
        Self::arrow(batch, SourceArgumentLayout::CanonicalRow, memory)
    }

    /// The batch must use the canonical field names and physical layouts emitted by the row encoder.
    pub fn from_row_encoded_arrow(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::arrow(batch, SourceArgumentLayout::BuiltFromRows, memory)
    }

    fn arrow(batch: OwnedBatch, source_layout: SourceArgumentLayout, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let admitted = ArrowArgumentTypes::translate(batch.batch().schema(), memory, |_, _, _| Ok(()))?;
        Self::arrow_with_types(batch, source_layout, admitted)
    }

    fn arrow_with_types(batch: OwnedBatch, source_layout: SourceArgumentLayout, admitted: ArrowArgumentTypes) -> Result<Self> {
        if !Arc::ptr_eq(batch.batch().schema_ref(), &admitted.schema) {
            return Err(Anomaly::incorrect("rtdb.tx/argument-schema", "Arrow argument batch does not use the admitted schema"));
        }
        let ArrowArgumentTypes { types, reservation: schema_reservation, .. } = admitted;
        let memory = Arc::clone(schema_reservation.budget());
        let types = Arc::new(types);
        let row_count = batch.batch().num_rows();
        let chunk = Arc::new(ArgumentChunk::Arrow { batch, types: Arc::clone(&types), source_layout, _reservation: Arc::clone(&schema_reservation) });
        Ok(Self { types, chunks: Arc::new(vec![(0, chunk)]), memory, schema_reservation, row_count })
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
    pub fn from_packed_rows(rows: Vec<Vec<Value>>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut arguments = Self::from_rows(rows, memory)?;
        // These columns use the row encoder's exact source layout, including the null representation.
        let batch = arguments.source_columnar_batch(memory)?;
        arguments.chunks = Arc::new(vec![(0, Arc::new(ArgumentChunk::Arrow {
            batch, types: Arc::clone(&arguments.types), source_layout: SourceArgumentLayout::BuiltFromRows,
            _reservation: Arc::clone(&arguments.schema_reservation),
        }))]);
        Ok(arguments)
    }

    pub(crate) fn built_from_rows(&self) -> bool {
        self.chunks.iter().any(|(_, chunk)| matches!(chunk.as_ref(), ArgumentChunk::Arrow {
            source_layout: SourceArgumentLayout::BuiltFromRows, ..
        }))
    }

    pub fn types(&self) -> &[LogicalType] { &self.types }
    /// Opened-vector field types for query or DML binding; source payload fields remain available through `types`.
    pub fn execution_types(&self) -> impl Iterator<Item = LogicalType> + '_ { self.types.iter().map(rtdb_columnar::execution_type) }
    pub fn row_count(&self) -> usize { self.row_count }
    pub fn single_arrow_page(&self) -> Option<&OwnedBatch> {
        match self.chunks.as_slice() {
            [(_, chunk)] => match chunk.as_ref() {
                ArgumentChunk::Arrow { batch, .. } => Some(batch),
                ArgumentChunk::Rows { .. } => None,
            },
            _ => None,
        }
    }
    /// Returns the Arrow page holding the first argument row, if that row was bound as Arrow.
    pub fn first_arrow_page(&self) -> Option<&OwnedBatch> {
        match self.chunks.iter().map(|(_, chunk)| chunk.as_ref()).find(|chunk| chunk.len() != 0)? {
            ArgumentChunk::Arrow { batch, .. } => Some(batch),
            ArgumentChunk::Rows { .. } => None,
        }
    }

    /// Builds one document operation from a stream bound as uniformly typed Arrow pages.
    pub fn ingest_documents(&self, memory: &Arc<MemoryBudget>) -> Result<Option<ArrowDocuments>> {
        let _pages = memory.reserve(self.chunks.len().checked_mul(2 * std::mem::size_of::<&RecordBatch>()).ok_or_else(overflow)?)?;
        let Some(pages) = self.uniform_arrow_pages() else { return Ok(None); };
        ArrowDocuments::from_external_pages(&pages, memory)
    }

    pub fn ingest_named_documents(&self, names: &[String], memory: &Arc<MemoryBudget>) -> Result<Option<ArrowDocuments>> {
        if let Some(documents) = self.ingest_documents(memory)? { return Ok(Some(documents)); }
        if !self.chunks.iter().any(|(_, chunk)| matches!(chunk.as_ref(), ArgumentChunk::Arrow { .. })) { return Ok(None); }
        let batch = self.source_columnar_batch(memory)?;
        let batch = batch.batch();
        if names.len() != batch.num_columns() {
            return Err(Anomaly::incorrect("rtdb.tx/document-schema", "Ingest column names disagree with their Arrow pages"));
        }
        let fields = batch.schema_ref().fields().iter().zip(names).map(|(field, name)| field.as_ref().clone().with_name(name)).collect::<Vec<_>>();
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        let record = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), batch.columns().to_vec(), &options)
            .map_err(|error| Anomaly::incorrect("rtdb.tx/document-schema", error.to_string()))?;
        ArrowDocuments::from_external_pages(&[&record], memory)
    }

    pub fn rows(&self) -> impl Iterator<Item = Result<ArgumentRow<'_>>> {
        self.indexed_rows().map(|row| row.map(|(_, values)| values))
    }

    pub(crate) fn indexed_rows(&self) -> impl Iterator<Item = Result<(usize, ArgumentRow<'_>)>> {
        self.chunks.iter().flat_map(move |(start, chunk)| {
            (0..chunk.len()).map(move |index| chunk.row(index, &self.memory).map(|row| (start + index, row)))
        })
    }

    pub(crate) fn indexed_bound_rows(&self) -> impl Iterator<Item = Result<(usize, BoundArgumentRow<'_>)>> {
        self.indexed_bound_rows_with_source_types().map(|row| row.map(|(index, binding, _)| (index, binding)))
    }

    pub(crate) fn indexed_bound_rows_with_source_types(&self)
        -> impl Iterator<Item = Result<(usize, BoundArgumentRow<'_>, Option<&[LogicalType]>)>> {
        let types = self.types();
        self.chunks.iter().flat_map(move |(start, chunk)| (0..chunk.len()).map(move |row| {
            let (bound, source) = match chunk.as_ref() {
                ArgumentChunk::Rows { rows, .. } => (BoundArgumentRow::Values { values: &rows[row], types: Some(types) }, None),
                ArgumentChunk::Arrow { batch, types: physical_types, .. } => {
                    let page = batch.batch();
                    for ((field, column), ty) in page.schema_ref().fields().iter().zip(page.columns()).zip(physical_types.iter()) {
                        rtdb_columnar::validate_readable_cell(field, column.as_ref(), ty, row, &self.memory)?;
                    }
                    (BoundArgumentRow::Arrow { batch, row, types }, Some(physical_types.as_slice()))
                }
            };
            Ok((start + row, bound, source))
        }))
    }

    pub(crate) fn uniform_arrow_pages(&self) -> Option<Vec<&RecordBatch>> {
        let pages = self.chunks.iter().map(|(_, chunk)| match chunk.as_ref() {
            ArgumentChunk::Arrow { batch, .. } => Some(batch.batch()),
            ArgumentChunk::Rows { .. } => None,
        }).collect::<Option<Vec<_>>>()?;
        let first = pages.first()?.schema_ref();
        pages.iter().all(|page| page.schema_ref() == first).then_some(pages)
    }

    pub(crate) fn has_single_canonical_arrow_row(&self) -> bool {
        matches!(self.chunks.as_slice(), [(_, chunk)] if
            matches!(chunk.as_ref(), ArgumentChunk::Arrow { source_layout: SourceArgumentLayout::CanonicalRow, .. }))
    }

    pub(crate) fn retains_uploaded_source_layout(&self) -> bool {
        self.chunks.iter().all(|(_, chunk)| matches!(chunk.as_ref(), ArgumentChunk::Arrow { source_layout: SourceArgumentLayout::Uploaded, .. }))
    }

    pub(crate) fn value_bytes(&self) -> Result<usize> {
        self.chunks.iter().try_fold(0usize, |bytes, (_, chunk)| match chunk.as_ref() {
            ArgumentChunk::Arrow { batch, .. } => bytes.checked_add(batch.batch().get_array_memory_size()).ok_or_else(overflow),
            ArgumentChunk::Rows { rows, .. } => rows.iter().flatten().try_fold(bytes, |bytes, value| {
                bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow)
            }),
        })
    }
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

pub const SIBLING_PIN_MARGIN_MICROS: i64 = 60_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnedForkRoot {
    None,
    Writes,
    WritesAdvance,
    WritesRenewal,
    WritesSibling,
    Controls,
    ProposalControls,
    ProposalWrites,
    ProposalHold,
    ProposalShorten,
}

/// A finite Root interval containing at least one offset from acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedRelativePeriod {
    start: OwnedPeriodBound,
    end: OwnedPeriodBound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnedPeriodBound {
    Fixed(Timestamp),
    Offset(i64),
}

impl OwnedPeriodBound {
    fn from_fields(fixed: Option<Timestamp>, offset: Option<Timestamp>) -> Option<Self> {
        match (fixed, offset) {
            (Some(fixed), None) => Some(Self::Fixed(fixed)),
            (None, Some(offset)) => Some(Self::Offset(offset.0)),
            _ => None,
        }
    }

    fn fields(self) -> (Option<Timestamp>, Option<i64>) {
        match self {
            Self::Fixed(fixed) => (Some(fixed), None),
            Self::Offset(offset) => (None, Some(offset)),
        }
    }

    fn at(self, ordered: Timestamp) -> Option<Timestamp> {
        match self {
            Self::Fixed(fixed) => Some(fixed),
            Self::Offset(offset) => ordered.0.checked_add(offset).map(Timestamp),
        }
    }
}

impl OwnedRelativePeriod {
    pub fn from_fields(
        from: Option<Timestamp>,
        to: Option<Timestamp>,
        from_offset: Option<Timestamp>,
        to_offset: Option<Timestamp>,
    ) -> Option<Self> {
        let start = OwnedPeriodBound::from_fields(from, from_offset)?;
        let end = OwnedPeriodBound::from_fields(to, to_offset)?;
        match (start, end) {
            (OwnedPeriodBound::Fixed(_), OwnedPeriodBound::Fixed(_)) => return None,
            (OwnedPeriodBound::Offset(from), OwnedPeriodBound::Offset(to)) if from >= to => return None,
            _ => {},
        }
        Some(Self { start, end })
    }

    pub(crate) fn fields(self) -> (Option<Timestamp>, Option<Timestamp>, Option<i64>, Option<i64>) {
        let (from, from_offset) = self.start.fields();
        let (to, to_offset) = self.end.fields();
        (from, to, from_offset, to_offset)
    }

    pub(crate) fn at(self, ordered: Timestamp) -> Option<(Timestamp, Timestamp)> {
        let from = self.start.at(ordered)?;
        let to = self.end.at(ordered)?;
        (from < to).then_some((from, to))
    }
}

#[derive(Clone, Debug)]
pub enum OwnedPeriodEffect {
    Create(Documents),
    Replace(Documents),
    Arrow { documents: ArrowDocuments, create_only: bool },
    Delete(EntityId),
}

/// A temporal target from a held live clock, anchored to an ordered data cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedClockSlot {
    pub through: TxId,
    pub reading: Timestamp,
    pub issued: crate::DecisionObservation,
    pub funded: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum ArrowDocumentWrite { Put, Patch }

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum OwnedFootprintGrantOp {
    DebitOnConflict {
        work_key: String,
        work_age: u64,
        limit: u8,
    },
    AcquireOrWait {
        work_key: String,
        work_age: u64,
        attempt: u64,
        footprint: Vec<String>,
    },
    TransferOnLeaseLoss {
        work_key: String,
        replacement_attempt: u64,
        progress_step: u64,
        additional_footprint: Vec<String>,
    },
    CarryGrant {
        work_key: String,
        work_age: u64,
        holder_attempt: u64,
    },
    Release {
        work_key: String,
        holder_attempt: u64,
    },
}

impl OwnedFootprintGrantOp {
    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::DebitOnConflict { work_key, .. }
            | Self::CarryGrant { work_key, .. }
            | Self::Release { work_key, .. } => work_key.len().saturating_add(64),
            Self::AcquireOrWait { work_key, footprint, .. } => footprint
                .iter()
                .fold(work_key.len().saturating_add(64), |acc, t| {
                    acc.saturating_add(t.len()).saturating_add(32)
                }),
            Self::TransferOnLeaseLoss { work_key, additional_footprint, .. } => additional_footprint
                .iter()
                .fold(work_key.len().saturating_add(64), |acc, t| {
                    acc.saturating_add(t.len()).saturating_add(32)
                }),
        }
    }
}

#[derive(Clone, Debug)]
pub enum TxOp {
    Sql { sql: String, args: Option<SqlArguments> },
    SqlBytes { sql: String, arg_bytes: RetainedBytes },
    OwnedDeclareFixedRange { relation: rtdb_types::OwnedRelation, rule: rtdb_types::FixedRangeRule },
    /// Compare one declared Binary prefix counter at ordered present, after this commit's writes.
    OwnedCheckCapacity { relation: rtdb_types::OwnedRelation, column: String, prefix: Vec<u8>, at_most: i64 },
    OwnedDeclareCapacity { relation: rtdb_types::OwnedRelation, column: String, prefix_bytes: i64, cap: i64, when: Vec<u8> },
    OwnedDeclareUniquePath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, column: String },
    /// A clear-text search path whose files and queries tokenise with `analyzer`.
    OwnedDeclareSearchPath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, column: String, analyzer: rtdb_plan::Analyzer },
    OwnedDeclareRelation { relation: rtdb_types::OwnedRelation, schema: rtdb_types::OwnedSchema },
    OwnedDropSpatialPath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation },
    /// Fence a Root relation against later writes and declarations at this transaction.
    /// Collection reclaims its storage once `retain_micros` has passed and no live world names it.
    OwnedRetireRelation { relation: rtdb_types::OwnedRelation, retain_micros: i64 },
    #[doc(hidden)]
    OwnedAdoptRelation {
        source: rtdb_types::OwnedRelation, destination: rtdb_types::OwnedRelation,
        retain_micros: i64,
    },
    OwnedDeclareSpatialPath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, columns: Vec<String> },
    OwnedDeclareEncodedSpatialPath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, columns: Vec<String>, encoding: rtdb_types::SpatialGeometryEncoding },
    OwnedRequireSpatialPath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, columns: Vec<String>, encoding: rtdb_types::SpatialGeometryEncoding },
    OwnedDeclarePath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, columns: Vec<String> },
    /// An approximate vector path over one clear list column of a Root relation (vector.allium: VectorIndexDefinition).
    OwnedDeclareVectorPath { relation: rtdb_types::OwnedRelation, name: rtdb_types::OwnedRelation, column: String, dimensions: i64, recall: f64 },
    HostedControl(crate::HostedControlWrite),
    HostedConstraintRefusal,
    OwnedDeadline { until: Timestamp, recheck_at_staging: bool, journal_before: Option<rtdb_types::TimedJournalCutoff> },
    OwnedClockSlot { slot: OwnedClockSlot },
    OwnedFootprintGrant(OwnedFootprintGrantOp),
    OwnedCheckWorldScope { scope: rtdb_types::PlacedAttemptScope, clock: Option<rtdb_types::WorldClockLimit> },
    OwnedForkWorld {
        record: StructFields<Value>, previous: Option<rtdb_types::Iid>, sibling: Option<rtdb_types::Iid>,
        overlay_source: Option<(rtdb_types::Iid, rtdb_types::Iid)>,
        attempt: String, fingerprint: [u8; 32], root: OwnedForkRoot,
    },
    OwnedPublishRoot { attempt: String, fingerprint: [u8; 32] },
    OwnedPublishWorld { world: rtdb_types::Iid, parent_pin: rtdb_types::Iid, attempt: String, fingerprint: [u8; 32], expected: Option<Timestamp>, advance_to: Option<Timestamp>, drop_world: bool, settlement_pin: Option<rtdb_types::Iid>, land_root: Option<rtdb_types::Iid>, delete_root: Option<rtdb_types::Iid> },
    OwnedCheckWorldPurge { relation: rtdb_types::OwnedRelation, pin: rtdb_types::Iid, frontier: i64, local_frontier: Option<i64> },
    OwnedCheckRange {
        relation: rtdb_types::OwnedRelation, basis: rtdb_types::TransactionKey,
        scope: rtdb_types::PlacedAttemptScope, frontier: i64, until: Timestamp,
        region: Option<Arc<rtdb_types::OwnedRangeEvidence>>,
        /// After the frontier moved, accept changes that leave what the reader revealed unchanged.
        revealed: Option<rtdb_types::OwnedRevealed>,
    },
    /// Revalidate one point. With `revealed`, the reader's revealed projection
    /// (or absence) must be unchanged and constant over its clock interval;
    /// the version is only the fast path. Without it, the version must match.
    OwnedCheck {
        relation: rtdb_types::OwnedRelation, id: EntityId, valid_at: Timestamp,
        expected: Option<rtdb_types::OwnedPointVersion>, revealed: Option<rtdb_types::OwnedRevealed>,
    },
    OwnedErase { relation: rtdb_types::OwnedRelation, id: EntityId },
    OwnedReplayResidue {
        relation: rtdb_types::OwnedRelation, id: EntityId, origin: rtdb_types::EventOrigin,
        valid: rtdb_types::Interval,
    },
    /// Apply one live nomination and order the same relation's replay floor.
    OwnedSweepExpired {
        relation: rtdb_types::OwnedRelation, id: EntityId, created: Timestamp,
        expiry: Timestamp, scope: rtdb_types::PlacedAttemptScope,
    },
    OwnedDelete { relation: rtdb_types::OwnedRelation, id: EntityId, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    OwnedArrowDocs { relation: rtdb_types::OwnedRelation, documents: ArrowDocuments, create_only: bool, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    OwnedReplaceDocs { relation: rtdb_types::OwnedRelation, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    OwnedInsertDocs { relation: rtdb_types::OwnedRelation, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    /// Resolve the finite bounds at this Root transaction's ordered reading.
    OwnedRelativePeriod {
        relation: rtdb_types::OwnedRelation, effect: OwnedPeriodEffect,
        period: OwnedRelativePeriod,
    },
    InsertDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PutDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PutArrowDocs { table: TableRef, documents: ArrowDocuments },
    WriteArrowDocs { table: TableRef, documents: ArrowDocuments, write: ArrowDocumentWrite, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    PatchDocs { table: TableRef, documents: Documents, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    DeleteDocs { table: TableRef, ids: EntityIds, valid_from: Option<Timestamp>, valid_to: Option<Timestamp> },
    EraseDocs { table: TableRef, ids: EntityIds },
}

impl TxOp {
    pub fn owned_deadline(until: Timestamp) -> Self {
        Self::OwnedDeadline { until, recheck_at_staging: false, journal_before: None }
    }

    pub fn is_single_unbounded_root_document(&self, create_only: bool) -> bool {
        match self {
            Self::OwnedInsertDocs { relation, documents, valid_from: None, valid_to: None } =>
                create_only && relation.is_root_data() && documents.rows().len() == 1,
            Self::OwnedReplaceDocs { relation, documents, valid_from: None, valid_to: None } =>
                !create_only && relation.is_root_data() && documents.rows().len() == 1,
            Self::OwnedArrowDocs { relation, documents, valid_from: None, valid_to: None, create_only: create } =>
                *create == create_only && relation.is_root_data() && documents.row_count() == 1,
            _ => false,
        }
    }

    pub fn is_owned(&self) -> bool {
        matches!(self, Self::HostedConstraintRefusal | Self::HostedControl(_) | Self::OwnedDeclareFixedRange { .. } | Self::OwnedDeclareCapacity { .. } | Self::OwnedCheckCapacity { .. } | Self::OwnedDeclareUniquePath { .. } | Self::OwnedDeclareRelation { .. } | Self::OwnedDeclareSearchPath { .. } | Self::OwnedDropSpatialPath { .. } | Self::OwnedRetireRelation { .. } | Self::OwnedAdoptRelation { .. } | Self::OwnedDeclareSpatialPath { .. } | Self::OwnedDeclareEncodedSpatialPath { .. } | Self::OwnedRequireSpatialPath { .. } | Self::OwnedDeclarePath { .. } | Self::OwnedDeclareVectorPath { .. } | Self::OwnedDeadline { .. } | Self::OwnedClockSlot { .. } | Self::OwnedFootprintGrant(_) | Self::OwnedCheckWorldScope { .. } | Self::OwnedCheckWorldPurge { .. } | Self::OwnedForkWorld { .. } | Self::OwnedPublishRoot { .. } | Self::OwnedPublishWorld { .. } | Self::OwnedCheckRange { .. } | Self::OwnedCheck { .. } | Self::OwnedErase { .. } | Self::OwnedReplayResidue { .. } | Self::OwnedSweepExpired { .. } | Self::OwnedDelete { .. } | Self::OwnedArrowDocs { .. } | Self::OwnedReplaceDocs { .. } | Self::OwnedInsertDocs { .. } | Self::OwnedRelativePeriod { .. })
    }
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmittedTx {
    pub tx_id: TxId,
    pub placed_receipt: Option<String>,
}

#[derive(Debug)]
struct AnomalyAllocation { error: Anomaly, wire: Option<Bytes>, reservation: Arc<MemoryReservation> }

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
        Ok(Self(Arc::new(AnomalyAllocation { error, wire: Some(Bytes::copy_from_slice(bytes)), reservation })))
    }
    pub fn encode(&self) -> Result<RetainedBytes> {
        if let Some(wire) = &self.0.wire { return Ok(RetainedBytes { data: wire.clone(), _reservation: Arc::clone(&self.0.reservation) }); }
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

fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tx/allocation-overflow", "Transaction allocation exceeds address space").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn many_document_layouts_release_translation_scratch_before_the_next_layout() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let fields = Fields::from(vec![arrow_schema::Field::new("_id", DataType::Int64, false)]);
        let layouts = (0..128).map(|_| DecodedDocumentLayout::new(&fields, &memory)).collect::<Result<Vec<_>>>()?;
        assert!(memory.used_bytes() > 0);
        drop(layouts);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

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
    fn packed_rows_under_memory_pressure_refuse_instead_of_keeping_rows() {
        let rows = || (0..64).map(|id| vec![Value::I64(id), Value::Utf8(format!("label {id}"))]).collect::<Vec<_>>();
        let mut refused = false;
        for limit in (1 << 10..=64 << 10).step_by(1 << 10) {
            match SqlArguments::from_packed_rows(rows(), &MemoryBudget::new(limit)) {
                Ok(arguments) => assert!(arguments.built_from_rows(), "a {limit}-byte budget kept the rows"),
                Err(error) => { assert_eq!(error.code, "rtdb/memory-budget-exceeded"); refused = true; }
            }
        }
        assert!(refused, "the smallest budgets refuse the arguments");
        assert!(SqlArguments::from_packed_rows(rows(), &MemoryBudget::new(1 << 24)).unwrap().built_from_rows());
    }

    #[test]
    fn decoded_parameter_pages_reset_error_indices_but_coalesced_batches_continue() {
        let memory = MemoryBudget::new(1 << 20);
        let mut args = SqlArguments::from_rows(vec![vec![Value::I64(1)], vec![Value::I64(2)]], &memory).unwrap();
        args.append_page(SqlArguments::from_rows(vec![vec![Value::I64(3)]], &memory).unwrap()).unwrap();
        assert_eq!(args.indexed_rows().map(|row| row.unwrap().0).collect::<Vec<_>>(), [0, 1, 0]);
        let mut coalesced = SqlArguments::from_rows(vec![vec![Value::I64(1)]], &memory).unwrap();
        coalesced.append(SqlArguments::from_rows(vec![vec![Value::I64(2)]], &memory).unwrap()).unwrap();
        assert_eq!(coalesced.indexed_rows().map(|row| row.unwrap().0).collect::<Vec<_>>(), [0, 1]);
    }
}
