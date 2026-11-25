mod columns;
mod selected;

use rtdb_types::StructFields;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, NullArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, UInt32Array, UnionArray,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder}};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit as ArrowTimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{BatchBuilder, IidView, MemoryBudget, MemoryReservation, OwnedBatch, build_array_refs, build_iids, build_struct_array, copy_array_refs, estimate_builder_bytes_refs, iid_data_type, retain_array, retain_buffer, struct_field_cell, to_field};
use rtdb_types::{Anomaly, EventHeader, EventOp, EventOrigin, EventPosition, Iid, Interval, LogicalType, Result, RunEndWidth, ScalarType, Timestamp, Value};

use arrow_array::cast::AsArray;

#[derive(Clone, Debug, PartialEq)]
pub enum StoredOperation {
    Put(StructFields<Value>),
    Delete,
    Erase,
    Redacted,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub position: Option<EventPosition>,
    pub origin: Option<EventOrigin>,
    /// Canonical complete identity for governed data. IID stays a physical route.
    pub entity_key: Option<Arc<[u8]>>,
    pub iid: Iid,
    pub system_from: Timestamp,
    /// Zero for writes outside an ordered Proposal landing.
    pub commit_position: u32,
    pub valid_time: Interval,
    pub operation: StoredOperation,
    pub recency: Option<Timestamp>,
}

impl StoredEvent {
    pub fn header(&self) -> EventHeader {
        EventHeader { position: self.position, commit_position: self.commit_position, iid: self.iid, system_from: self.system_from, valid_time: self.valid_time, op: match &self.operation {
            StoredOperation::Put(_) => EventOp::Put, StoredOperation::Delete => EventOp::Delete, StoredOperation::Erase => EventOp::Erase,
            StoredOperation::Redacted => EventOp::Redacted,
        } }
    }

    /// Commits the original fact and its coordinates independently of Arrow
    /// field order and compaction's physical recency annotation.
    pub fn history_fingerprint(mut self) -> Result<[u8; 32]> {
        use sha2::{Digest, Sha256};

        struct HashWriter(Sha256);
        impl std::io::Write for HashWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }

        let header = self.header();
        let document = match &mut self.operation {
            StoredOperation::Put(document) => {
                order_document(document);
                Some(document)
            }
            StoredOperation::Delete | StoredOperation::Erase | StoredOperation::Redacted => None,
        };
        let mut writer = HashWriter(Sha256::new());
        writer.0.update(b"rtdb/positioned-fact-event/v1\0");
        serde_json::to_writer(&mut writer, &(header, self.origin.map(|origin| origin.0), self.entity_key.as_deref(), document))
            .map_err(invalid)?;
        Ok(writer.0.finalize().into())
    }
}

fn order_document(document: &mut StructFields<Value>) {
    document.retain(|_, value| !matches!(value, Value::Undefined));
    document.sort_keys();
    document.values_mut().for_each(order_document_fields);
}

fn order_document_fields(value: &mut Value) {
    match value {
        Value::Struct(document) => order_document(document),
        Value::List(items) | Value::FixedSizeList(items) => {
            if let Some(items) = items.values_mut() { items.iter_mut().for_each(order_document_fields); }
        }
        Value::Set(items) => items.iter_mut().for_each(order_document_fields),
        Value::Map { entries, .. } => {
            for (key, value) in entries {
                order_document_fields(key);
                order_document_fields(value);
            }
        }
        _ => {}
    }
}

/// An event an event batch encodes: its header and recency, and the cells of its put document.
pub trait EncodedEvent {
    fn header(&self) -> EventHeader;
    fn recency(&self) -> Option<Timestamp>;
    fn entity_key(&self) -> Option<&[u8]> { None }
    fn origin(&self) -> Option<EventOrigin> { None }
    /// Whether every column the event's put document holds is among `fields`.
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool;
    /// The put document's `name` cell under the column type `ty`, as `struct_field_cell` takes it of the decoded document.
    fn put_cell(&self, name: &str, ty: &LogicalType) -> Result<std::borrow::Cow<'_, Value>>;
    /// Where the put document's `name` column is stored, for an event read in place from an encoded page.
    fn put_column(&self, _name: &str) -> Option<PutColumn<'_>> { None }
}

/// A put document's column as its page stores it.
pub enum PutColumn<'a> {
    /// The column's field and array in the page's put leg, and the document's row in it.
    Stored(&'a Field, &'a dyn Array, usize),
    /// The page's put leg has no such column.
    Absent,
}

impl EncodedEvent for StoredEvent {
    fn header(&self) -> EventHeader { StoredEvent::header(self) }
    fn recency(&self) -> Option<Timestamp> { self.recency }
    fn entity_key(&self) -> Option<&[u8]> { self.entity_key.as_deref() }
    fn origin(&self) -> Option<EventOrigin> { self.origin }
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool {
        match &self.operation { StoredOperation::Put(document) => document.keys().all(|name| fields.contains_key(name)), _ => true }
    }
    fn put_cell(&self, name: &str, ty: &LogicalType) -> Result<std::borrow::Cow<'_, Value>> {
        match &self.operation {
            StoredOperation::Put(document) => Ok(std::borrow::Cow::Borrowed(struct_field_cell(document, name, ty))),
            _ => Err(invalid("Only a put event has document cells")),
        }
    }
}

/// An event read in place from its page: its header and recency, and the offset of its document in the page's put leg,
/// whose cells it reads only as an event batch encodes them.
#[derive(Clone, Debug)]
pub struct PageEvent {
    page: Arc<EventPage>,
    offset: usize,
    row: usize,
    pub iid: Iid,
    pub system_from: Timestamp,
    pub position: Option<EventPosition>,
    pub commit_position: u32,
    pub valid_time: Interval,
    pub op: EventOp,
    pub recency: Option<Timestamp>,
    origin: Option<EventOrigin>,
    /// `document_bytes`, once asked for, since compaction charges an event's document at each stage it passes.
    bytes: std::sync::OnceLock<usize>,
}

#[derive(Debug)]
struct EventPage {
    put: Option<(StructArray, Vec<LogicalType>)>,
    keys: Option<BinaryArray>,
}

#[derive(Clone, Copy)]
struct PutRef<'a> {
    array: &'a StructArray,
    types: PutFieldTypes<'a>,
    offset: usize,
}

#[derive(Clone, Copy)]
enum PutFieldTypes<'a> {
    Ordered(&'a [LogicalType]),
    Named(&'a StructFields<LogicalType>),
}

impl<'a> PutRef<'a> {
    fn field_type(self, index: usize) -> Result<&'a LogicalType> {
        let ty = match self.types {
            PutFieldTypes::Ordered(types) => types.get(index),
            PutFieldTypes::Named(types) => types.get(self.array.fields()[index].name()),
        };
        ty.ok_or_else(|| invalid("Put column has no logical type"))
    }

    fn column(self, name: &str) -> PutColumn<'a> {
        match self.position(name) {
            Some(index) => PutColumn::Stored(&self.array.fields()[index], self.array.column(index).as_ref(), self.offset),
            None => PutColumn::Absent,
        }
    }
    fn value(self, name: &str, ty: &LogicalType) -> Result<Value> {
        match self.position(name) {
            Some(index) => match self.field_type(index)? {
                LogicalType::Nothing => Ok(Value::Undefined),
                stored => rtdb_columnar::read_value_as(stored, &self.array.fields()[index], self.array.column(index).as_ref(), self.offset),
            },
            None => Ok(match ty { LogicalType::Nothing => Value::Undefined, _ => Value::Null }),
        }
    }
    fn position(self, name: &str) -> Option<usize> { self.array.fields().iter().position(|field| field.name() == name) }
    fn within(self, fields: &StructFields<LogicalType>) -> bool {
        self.array.fields().iter().all(|column| fields.contains_key(column.name()))
    }
}

/// Whether selected puts can be copied directly under a compatible persisted layout.
pub fn can_copy_put_cell(field: &Field, array: &dyn Array, row: usize) -> bool {
    columns::valid_cell(field, array, row, true)
}

/// A borrowed event with its document addressed within an Arrow batch.
pub struct EventRef<'a> {
    header: EventHeader,
    recency: Option<Timestamp>,
    put: Option<PutRef<'a>>,
    entity_key: Option<&'a [u8]>,
    origin: Option<EventOrigin>,
}

pub use rtdb_columnar::TextCell as PutText;

pub struct PutTextColumn<'a> {
    batch: &'a EventBatch,
    position: Option<usize>,
    strings: Option<&'a StringArray>,
    owned: bool,
    logical: std::cell::OnceCell<&'a LogicalType>,
}

impl<'a> PutTextColumn<'a> {
    pub fn read(&self, row: usize) -> Result<(EventRef<'a>, PutText<'a>)> {
        let event = self.batch.borrow_event(row)?;
        let text = self.cell(&event)?;
        Ok((event, text))
    }

    fn cell(&self, event: &EventRef<'a>) -> Result<PutText<'a>> {
        let (Some(put), Some(position)) = (event.put, self.position) else { return Ok(PutText::Missing); };
        let logical = match self.logical.get() {
            Some(logical) => *logical,
            None => {
                let logical = put.field_type(position)?;
                self.logical.set(logical).expect("first put field read");
                logical
            }
        };
        let scalar = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
        if let Some(strings) = self.strings && *scalar == LogicalType::Scalar(ScalarType::Utf8) {
            return Ok(if strings.is_null(put.offset) { PutText::Missing } else {
                PutText::Utf8(strings.value(put.offset))
            });
        }
        let field = &put.array.fields()[position];
        let array = put.array.column(position).as_ref();
        if self.owned {
            rtdb_columnar::owned_text_cell(logical, field, array, put.offset)
        } else {
            rtdb_columnar::text_cell(logical, field, array, put.offset)
        }
    }
}

impl<'a> EventRef<'a> {
    pub fn validate_document(&self, memory: &Arc<MemoryBudget>) -> Result<()> {
        let Some(put) = self.put else { return Ok(()); };
        for (index, field) in put.array.fields().iter().enumerate() {
            let logical = put.field_type(index)?;
            if *logical != LogicalType::Nothing {
                rtdb_columnar::validate_readable_cell(field, put.array.column(index).as_ref(), logical, put.offset, memory)?;
            }
        }
        Ok(())
    }

    /// Borrows ordinary UTF-8 cells from a validated put document.
    pub fn put_text(&self, name: &str) -> Result<PutText<'a>> {
        self.put_search_text(name, false)
    }

    pub fn put_search_text(&self, name: &str, owned: bool) -> Result<PutText<'a>> {
        let Some(put) = self.put else { return Ok(PutText::Missing); };
        let Some(position) = put.position(name) else { return Ok(PutText::Missing); };
        let logical = put.field_type(position)?;
        let field = &put.array.fields()[position];
        let array = put.array.column(position).as_ref();
        if owned {
            rtdb_columnar::owned_text_cell(logical, field, array, put.offset)
        } else {
            rtdb_columnar::text_cell(logical, field, array, put.offset)
        }
    }

    pub fn document_allocation_bound(&self) -> Result<usize> {
        let Some(put) = self.put else { return Ok(0); };
        put.array.fields().iter().enumerate().try_fold(4096usize, |bytes, (index, field)| {
            let cell = rtdb_columnar::estimate_read_cell_bytes(field, put.array.column(index).as_ref(), put.offset)?;
            cell.checked_mul(4).and_then(|cell| cell.checked_add(field.name().len()))
                .and_then(|cell| cell.checked_add(512)).and_then(|cell| bytes.checked_add(cell)).ok_or_else(event_size)
        })
    }

    pub fn physical_entity_key(&self) -> Option<&'a [u8]> { self.entity_key }

    pub fn put_identity(&self) -> Result<Option<rtdb_columnar::EntityIdRef<'a>>> {
        match self.put.map(|put| put.column("_id")) {
            Some(PutColumn::Stored(field, array, row)) if self.entity_key().is_some() =>
                rtdb_columnar::read_owned_entity_id(field, array, row).map(Some),
            Some(PutColumn::Stored(field, array, row)) => rtdb_columnar::read_entity_id(field, array, row).map(Some),
            _ => Ok(None),
        }
    }
}

impl EncodedEvent for EventRef<'_> {
    fn header(&self) -> EventHeader { self.header }
    fn recency(&self) -> Option<Timestamp> { self.recency }
    fn entity_key(&self) -> Option<&[u8]> { self.entity_key }
    fn origin(&self) -> Option<EventOrigin> { self.origin }
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool { self.put.is_none_or(|put| put.within(fields)) }
    fn put_cell(&self, name: &str, ty: &LogicalType) -> Result<std::borrow::Cow<'_, Value>> {
        let put = self.put.ok_or_else(|| invalid("Only a put event has document cells"))?;
        put.value(name, ty).map(std::borrow::Cow::Owned)
    }
    fn put_column(&self, name: &str) -> Option<PutColumn<'_>> { self.put.map(|put| put.column(name)) }
}

impl PageEvent {
    pub fn page_ordinal(&self) -> usize { self.row }

    pub fn put_offset(&self) -> Option<usize> { self.put_ref().map(|put| put.offset) }

    /// The bytes the event's document decodes to at most, which encoding its cells may hold at once.
    pub fn document_bytes(&self) -> Result<usize> {
        if let Some(bytes) = self.bytes.get() { return Ok(*bytes); }
        let bytes = match (&self.page.put, self.op) {
            (Some((array, _)), EventOp::Put) => array.fields().iter().enumerate().try_fold(0usize, |bytes, (index, field)| {
                bytes.checked_add(rtdb_columnar::estimate_read_cell_bytes(field, array.column(index).as_ref(), self.offset)?).ok_or_else(event_size)
            })?,
            _ => 0,
        };
        Ok(*self.bytes.get_or_init(|| bytes))
    }
    fn put_ref(&self) -> Option<PutRef<'_>> {
        let (Some((array, types)), EventOp::Put) = (&self.page.put, self.op) else { return None; };
        Some(PutRef { array, types: PutFieldTypes::Ordered(types), offset: self.offset })
    }
}

impl EncodedEvent for PageEvent {
    fn header(&self) -> EventHeader { EventHeader { position: self.position, commit_position: self.commit_position, iid: self.iid, system_from: self.system_from, valid_time: self.valid_time, op: self.op } }
    fn recency(&self) -> Option<Timestamp> { self.recency }
    fn entity_key(&self) -> Option<&[u8]> { self.page.keys.as_ref().map(|keys| keys.value(self.row)) }
    fn origin(&self) -> Option<EventOrigin> { self.origin }
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool {
        self.put_ref().is_none_or(|put| put.within(fields))
    }
    fn put_cell(&self, name: &str, ty: &LogicalType) -> Result<std::borrow::Cow<'_, Value>> {
        let put = self.put_ref().ok_or_else(|| invalid("Only a put event has document cells"))?;
        put.value(name, ty).map(std::borrow::Cow::Owned)
    }
    fn put_column(&self, name: &str) -> Option<PutColumn<'_>> {
        self.put_ref().map(|put| put.column(name))
    }
}

#[derive(Clone, Debug)]
pub struct EventBatch {
    batch: RecordBatch,
    iid: usize,
    system_from: usize,
    valid_from: usize,
    valid_to: usize,
    op: usize,
    recency: Option<usize>,
    entity_keys: Option<usize>,
    commit_positions: Option<usize>,
    origins: Option<usize>,
    event_positions: Option<usize>,
    /// The put leg's logical type, derived from its field on the first put read.
    put_type: std::sync::Arc<std::sync::OnceLock<LogicalType>>,
}

pub(crate) fn event_arrow_error(error: arrow_schema::ArrowError) -> Anomaly {
    invalid(error)
}

impl EventBatch {
    pub fn new(batch: RecordBatch) -> Result<Self> {
        let schema = batch.schema();
        let field_index = |name: &str| schema.index_of(name).map_err(invalid);
        let iid = field_index("_iid")?;
        IidView::new(batch.column(iid).as_ref())?;
        let system_from = field_index("_system_from")?;
        let valid_from = field_index("_valid_from")?;
        let valid_to = field_index("_valid_to")?;
        let op = field_index("op")?;
        let recency = schema.index_of("_recency").ok();
        let entity_keys = schema.index_of(ENTITY_KEY_FIELD).ok();
        let commit_positions = schema.index_of(COMMIT_POSITION_FIELD).ok();
        let origins = schema.index_of(EVENT_ORIGIN_FIELD).ok();
        let event_positions = schema.index_of(EVENT_POSITION_FIELD).ok();
        if let Some(index) = event_positions
            && (schema.field(index).is_nullable()
                || schema.field(index).data_type() != &DataType::FixedSizeBinary(EventPosition::WIDTH)
                || batch.column(index).null_count() != 0)
        {
            return Err(invalid("Event positions require a non-null fixed-width binary column"));
        }
        if let Some(index) = origins && schema.field(index).data_type() != &DataType::FixedSizeBinary(EventOrigin::WIDTH) {
            return Err(invalid("Event origins have an invalid binary width"));
        }
        if entity_keys.is_some() && commit_positions.is_none() {
            return Err(invalid("Governed event pages require commit positions"));
        }
        if let Some(index) = entity_keys
            && (schema.field(index).is_nullable()
                || batch.column(index).as_any().downcast_ref::<BinaryArray>().is_none()
                || batch.column(index).null_count() != 0)
        {
            return Err(invalid("Governed events require non-null binary entity keys"));
        }
        if let Some(index) = commit_positions
            && (schema.field(index).is_nullable()
                || !batch.column(index).as_any().is::<UInt32Array>()
                || batch.column(index).null_count() != 0)
        {
            return Err(invalid("Governed events require non-null UInt32 commit positions"));
        }
        for index in [Some(system_from), Some(valid_from), Some(valid_to), recency].into_iter().flatten() {
            if batch.column(index).as_any().downcast_ref::<TimestampMicrosecondArray>().is_none() || batch.column(index).null_count() != 0 {
                return Err(invalid(format!("{} must contain non-null microsecond timestamps", schema.field(index).name())));
            }
        }
        if !matches!(schema.field(op).data_type(), DataType::Union(_, UnionMode::Dense)) || !batch.column(op).as_any().is::<UnionArray>() {
            return Err(invalid("Event operations must use a dense Arrow union"));
        }
        let DataType::Union(op_fields, _) = schema.field(op).data_type() else { return Err(invalid("Missing operation fields")); };
        let mut seen = std::collections::BTreeSet::new();
        for (_, field) in op_fields.iter() {
            if !seen.insert(field.name()) { return Err(invalid("Event operation names must be unique")); }
            match (field.name().as_str(), field.data_type()) {
                ("put", DataType::Struct(_)) | ("delete" | "erase", DataType::Null) => {}
                ("redacted", DataType::Null) if entity_keys.is_some() && origins.is_some() => {}
                ("redacted", _) => return Err(invalid("Redacted operations require an owned identity and a null payload")),
                ("put", _) => return Err(invalid("Put operations must contain document structs")),
                ("delete" | "erase", _) => return Err(invalid("Delete and erase operations must contain null values")),
                (name, _) => return Err(invalid(format!("Unknown event operation: {name}"))),
            }
        }
        Ok(Self { batch, iid, system_from, valid_from, valid_to, op, recency, entity_keys, commit_positions, origins, event_positions, put_type: Default::default() })
    }

    pub fn put_text_column(&self, name: &str) -> PutTextColumn<'_> {
        self.put_search_text_column(name, false)
    }

    pub fn put_search_text_column(&self, name: &str, owned: bool) -> PutTextColumn<'_> {
        let field = self.operations().ok().and_then(|operations| {
            let DataType::Union(fields, _) = operations.data_type() else { return None; };
            let (id, _) = fields.iter().find(|(_, field)| field.name() == "put")?;
            operations.child(id).as_any().downcast_ref::<StructArray>()
        });
        let position = field.and_then(|array| array.fields().iter().position(|field| field.name() == name));
        let strings = field.zip(position).and_then(|(array, position)| array.column(position).as_any().downcast_ref::<StringArray>());
        PutTextColumn { batch: self, position, strings, owned, logical: Default::default() }
    }

    pub fn arrow(&self) -> &RecordBatch { &self.batch }
    /// The page's events read in place, each reading its document's cells only as an event batch encodes them.
    pub fn page_events(&self) -> Result<Vec<PageEvent>> {
        let mut events = Vec::with_capacity(self.len());
        self.extend_page_events(&mut events)?;
        Ok(events)
    }

    /// Appends event positions in page order and leaves earlier positions intact if decoding fails.
    pub fn extend_page_events(&self, events: &mut Vec<PageEvent>) -> Result<()> {
        let event_at = self.prepare_page_events()?;
        let start = events.len();
        let result = (0..self.len()).try_for_each(|row| {
            events.push(event_at(row)?);
            Ok(())
        });
        if result.is_err() { events.truncate(start); }
        result
    }

    /// Binds a page once so callers can validate or project rows without retaining descriptors.
    pub fn prepare_page_events(&self) -> Result<impl Fn(usize) -> Result<PageEvent> + '_> {
        let union = self.operations()?;
        let DataType::Union(legs, _) = union.data_type() else { return Err(invalid("Missing operation union fields")); };
        let put = match legs.iter().find(|(_, leg)| leg.name() == "put") {
            Some((id, leg)) => {
                let array = union.child(id).as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Put operations must contain document structs"))?.clone();
                let LogicalType::Struct(types) = rtdb_columnar::from_field(leg)? else { return Err(invalid("A put operation must contain a document struct")) };
                Some((array, types.into_iter().map(|(_, ty)| ty).collect()))
            }
            None => None,
        };
        let page = Arc::new(EventPage { put, keys: self.keys().cloned() });
        let mut ops = [None; 128];
        for (id, leg) in legs.iter() {
            let op = match leg.name().as_str() { "put" => EventOp::Put, "delete" => EventOp::Delete, "erase" => EventOp::Erase, "redacted" => EventOp::Redacted, name => return Err(invalid(format!("Unknown event operation: {name}"))) };
            if let Ok(id) = usize::try_from(id) { ops[id] = Some(op); }
        }
        let iids = IidView::new(self.batch.column(self.iid).as_ref())?;
        let times = |index: usize| self.batch.column(index).as_any().downcast_ref::<TimestampMicrosecondArray>().map(|array| array.values()).ok_or_else(|| invalid("Event timestamp array type changed"));
        let (system_from, valid_from, valid_to) = (times(self.system_from)?, times(self.valid_from)?, times(self.valid_to)?);
        let recency = self.recency.map(times).transpose()?;
        let positions = self.positions();
        let fixed = |index: Option<usize>, message| {
            index.map(|index| self.batch.column(index).as_any().downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| invalid(message))).transpose()
        };
        let origins = self.origins()?;
        let event_positions = fixed(self.event_positions, "Event position array type changed")?;
        Ok(move |row| {
            if row >= self.len() { return Err(invalid("Event row exceeds batch length")); }
            let op = usize::try_from(union.type_id(row)).ok().and_then(|id| ops[id]).ok_or_else(|| invalid("Event operation type ID has no leg"))?;
            let origin = origins.filter(|origins| origins.is_valid(row)).map(|origins| {
                EventOrigin(origins.value(row).try_into().expect("validated event origin width"))
            });
            let valid_time = Interval::new(Timestamp(valid_from[row]), Timestamp(valid_to[row]))?;
            Self::require_redacted_position(op, origin, valid_time)?;
            let iid = iids.get(row)?;
            let position = event_positions.map(|positions| {
                positions.value(row).try_into().map(EventPosition::from_bytes).map_err(invalid)
            }).transpose()?;
            Ok(PageEvent { page: Arc::clone(&page), offset: union.value_offset(row), row, iid, system_from: Timestamp(system_from[row]),
                position,
                commit_position: positions.map_or(0, |positions| positions.value(row)),
                valid_time, op, recency: recency.map(|recency| Timestamp(recency[row])), origin, bytes: std::sync::OnceLock::new() })
        })
    }
    /// The dense union of each row's operation, whose put leg holds its document.
    pub fn operations(&self) -> Result<&UnionArray> {
        self.batch.column(self.op).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Event operation array type changed"))
    }

    pub fn put_document(&self) -> Result<Option<&StructArray>> {
        let union = self.operations()?;
        let DataType::Union(legs, _) = union.data_type() else { return Err(invalid("Missing operation union fields")); };
        legs.iter().find(|(_, leg)| leg.name() == "put").map(|(id, _)| {
            union.child(id).as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Put operations must contain document structs"))
        }).transpose()
    }

    /// Validates the page headers and visits erasure rows with one operation and IID view.
    pub fn visit_erasures(&self, mut check: impl FnMut() -> Result<()>, mut visit: impl FnMut(usize, Option<(Iid, Timestamp)>) -> Result<()>) -> Result<()> {
        let union = self.operations()?;
        let DataType::Union(legs, _) = union.data_type() else { return Err(invalid("Missing operation union fields")); };
        let mut erase = None;
        let mut valid_operations = [false; 128];
        for (id, leg) in legs.iter() {
            if let Ok(id) = usize::try_from(id) { valid_operations[id] = true; }
            if leg.name() == "erase" { erase = Some(id); }
        }
        let iids = IidView::new(self.batch.column(self.iid).as_ref())?;
        for (row, &id) in union.type_ids().iter().enumerate() {
            check()?;
            if id < 0 || !valid_operations[id as usize] {
                return Err(invalid("Event operation type ID has no leg"));
            }
            let iid = iids.get(row)?;
            Interval::new(self.timestamp(self.valid_from, row)?, self.timestamp(self.valid_to, row)?)?;
            let erased = if Some(id) == erase { Some((iid, self.timestamp(self.system_from, row)?)) } else { None };
            visit(row, erased)?;
        }
        Ok(())
    }
    pub fn len(&self) -> usize { self.batch.num_rows() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn has_entity_keys(&self) -> bool { self.entity_keys.is_some() }
    pub fn has_commit_positions(&self) -> bool { self.commit_positions.is_some() }
    pub fn has_origin_column(&self) -> bool { self.origins.is_some() }
    pub fn has_position_column(&self) -> bool { self.event_positions.is_some() }

    pub fn origin(&self, row: usize) -> Result<Option<EventOrigin>> {
        if row >= self.len() { return Err(invalid("Event row exceeds batch length")); }
        Ok(self.origins()?.and_then(|origins| origins.is_valid(row)
            .then(|| EventOrigin(origins.value(row).try_into().expect("validated event origin width")))))
    }

    pub(crate) fn origins(&self) -> Result<Option<&FixedSizeBinaryArray>> {
        self.origins.map(|index| self.batch.column(index).as_any().downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("Event origin array type changed"))).transpose()
    }

    fn event_position(&self, row: usize) -> Result<Option<EventPosition>> {
        let Some(index) = self.event_positions else { return Ok(None); };
        let positions = self.batch.column(index).as_any().downcast_ref::<FixedSizeBinaryArray>()
            .ok_or_else(|| invalid("Event position array type changed"))?;
        let bytes = positions.value(row).try_into().map_err(invalid)?;
        Ok(Some(EventPosition::from_bytes(bytes)))
    }

    fn keys(&self) -> Option<&BinaryArray> {
        self.entity_keys.and_then(|index| self.batch.column(index).as_any().downcast_ref::<BinaryArray>())
    }

    fn positions(&self) -> Option<&UInt32Array> {
        self.commit_positions.and_then(|index| self.batch.column(index).as_any().downcast_ref::<UInt32Array>())
    }

    pub fn entity_key(&self, row: usize) -> Result<Option<&[u8]>> {
        if row >= self.len() { return Err(invalid("Event row exceeds batch length")); }
        Ok(self.keys().map(|keys| keys.value(row)))
    }

    pub fn iids(&self) -> Result<IidView<'_>> {
        IidView::new(self.batch.column(self.iid).as_ref())
    }

    pub fn iid(&self, row: usize) -> Result<Iid> {
        self.iids()?.get(row)
    }

    pub fn header(&self, row: usize) -> Result<EventHeader> {
        if row >= self.len() { return Err(invalid("Event row exceeds batch length")); }
        let op = match self.operation(row)?.0.name().as_str() {
            "put" => EventOp::Put, "delete" => EventOp::Delete, "erase" => EventOp::Erase, "redacted" => EventOp::Redacted,
            name => return Err(invalid(format!("Unknown event operation: {name}"))),
        };
        let valid_time = Interval::new(self.timestamp(self.valid_from, row)?, self.timestamp(self.valid_to, row)?)?;
        if op == EventOp::Redacted {
            Self::require_redacted_position(op, self.origin(row)?, valid_time)?;
        }
        Ok(EventHeader {
            position: self.event_position(row)?,
            commit_position: self.positions().map_or(0, |positions| positions.value(row)),
            iid: self.iid(row)?,
            system_from: self.timestamp(self.system_from, row)?,
            valid_time,
            op,
        })
    }

    fn require_redacted_position(op: EventOp, origin: Option<EventOrigin>, valid: Interval) -> Result<()> {
        if op == EventOp::Redacted && (origin.is_none() || valid.is_empty()) {
            return Err(invalid("A redacted assertion requires its original event origin and nonempty extent"));
        }
        Ok(())
    }

    /// Borrows a row and the logical types of its batch's put fields.
    pub fn event_ref<'a>(&'a self, row: usize, put_types: &'a [LogicalType]) -> Result<EventRef<'a>> {
        let header = self.header(row)?;
        self.event_ref_at(row, header, PutFieldTypes::Ordered(put_types))
    }

    /// Borrows an event using the whole put field's validated layout for its cells.
    pub fn borrow_event(&self, row: usize) -> Result<EventRef<'_>> {
        let header = self.header(row)?;
        let types = if header.op == EventOp::Put {
            let (field, _, _) = self.operation(row)?;
            let logical = self.put_layout(field)?;
            let logical = match logical { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
            let LogicalType::Struct(types) = logical else { return Err(invalid("A put operation must contain a document struct")); };
            PutFieldTypes::Named(types)
        } else {
            PutFieldTypes::Ordered(&[])
        };
        self.event_ref_at(row, header, types)
    }

    fn event_ref_at<'a>(&'a self, row: usize, header: EventHeader, put_types: PutFieldTypes<'a>) -> Result<EventRef<'a>> {
        let put = if header.op == EventOp::Put {
            let (_, array, offset) = self.operation(row)?;
            let array = array.as_any().downcast_ref::<StructArray>().filter(|array| array.is_valid(offset))
                .ok_or_else(|| invalid("A put operation must contain a document struct"))?;
            Some(PutRef { array, types: put_types, offset })
        } else { None };
        let recency = self.recency.map(|index| self.timestamp(index, row)).transpose()?;
        Ok(EventRef { header, recency, put, entity_key: self.entity_key(row)?, origin: self.origin(row)? })
    }

    fn put_layout(&self, field: &Field) -> Result<&LogicalType> {
        if let Some(logical) = self.put_type.get() { return Ok(logical); }
        let logical = rtdb_columnar::from_field(field)?;
        Ok(self.put_type.get_or_init(|| logical))
    }

    pub fn read_event(&self, row: usize) -> Result<StoredEvent> {
        let header = self.header(row)?;
        let operation = match header.op {
            EventOp::Put => {
                let (field, array, offset) = self.operation(row)?;
                let logical = self.put_layout(field)?;
                let Value::Struct(document) = rtdb_columnar::read_value_as(logical, field, array.as_ref(), offset)? else {
                    return Err(invalid("A put operation must contain a document struct"));
                };
                StoredOperation::Put(document)
            }
            EventOp::Delete => StoredOperation::Delete,
            EventOp::Erase => StoredOperation::Erase,
            EventOp::Redacted => StoredOperation::Redacted,
        };
        Ok(StoredEvent { position: header.position, origin: self.origin(row)?, commit_position: header.commit_position, entity_key: self.entity_key(row)?.map(Arc::from), iid: header.iid, system_from: header.system_from, valid_time: header.valid_time, operation,
            recency: self.recency.map(|index| self.timestamp(index, row)).transpose()? })
    }

    fn timestamp(&self, index: usize, row: usize) -> Result<Timestamp> {
        self.batch.column(index).as_any().downcast_ref::<TimestampMicrosecondArray>()
            .map(|column| Timestamp(column.value(row))).ok_or_else(|| invalid("Event timestamp array type changed"))
    }

    fn operation(&self, row: usize) -> Result<(&Field, &ArrayRef, usize)> {
        let union = self.batch.column(self.op).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Event operation array type changed"))?;
        let DataType::Union(fields, _) = union.data_type() else { return Err(invalid("Missing operation union fields")); };
        let id = union.type_id(row);
        let field = fields.iter().find_map(|(type_id, field)| (type_id == id).then_some(field)).ok_or_else(|| invalid("Event operation type ID has no leg"))?;
        Ok((field.as_ref(), union.child(id), union.value_offset(row)))
    }
}

/// All rows must either carry recency or omit it; the caller supplies the put schema captured with the rows.
pub fn build_event_batch(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>) -> Result<RecordBatch> {
    event_batch(events, put_fields, iid_encoding, false, None, PutLeg::First, false)
}

#[test]
fn validating_a_borrowed_document_keeps_errors_in_unread_fields() {
    let fields = StructFields::from([
        ("uri".into(), LogicalType::Scalar(ScalarType::Uri)),
        ("body".into(), LogicalType::Scalar(ScalarType::Utf8)),
    ]);
    let event = StoredEvent { position: None,
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid: Iid([0; 16]), system_from: Timestamp(10), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([
            ("uri".into(), Value::Uri("https://example.com".into())),
            ("body".into(), Value::Utf8("indexed text".into())),
        ])),
    };
    let batch = build_event_batch(&[event], Some(fields), None).unwrap();
    let schema = batch.schema();
    let op = schema.index_of("op").unwrap();
    let union = batch.column(op).as_union();
    let DataType::Union(legs, _) = union.data_type() else { panic!("Expected union"); };
    let children = legs.iter().map(|(id, field)| {
        if field.name() != "put" { return Arc::clone(union.child(id)); }
        let put = union.child(id).as_struct();
        let mut columns = put.columns().to_vec();
        let uri = put.fields().iter().position(|field| field.name() == "uri").unwrap();
        columns[uri] = Arc::new(StringArray::from(vec!["%"]));
        Arc::new(StructArray::try_new(put.fields().clone(), columns, None).unwrap()) as ArrayRef
    }).collect();
    let mut columns = batch.columns().to_vec();
    columns[op] = Arc::new(UnionArray::try_new(legs.clone(), union.type_ids().clone(), union.offsets().cloned(), children).unwrap());
    let batch = EventBatch::new(RecordBatch::try_new(schema, columns).unwrap()).unwrap();
    let event = batch.borrow_event(0).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    assert_eq!(event.validate_document(&memory).unwrap_err().code, batch.read_event(0).unwrap_err().code);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn borrowed_events_validate_the_put_layout_when_a_put_is_read() {
    let fields = StructFields::from([("body".into(), LogicalType::Scalar(ScalarType::Utf8))]);
    let events = [StoredOperation::Delete, StoredOperation::Put(StructFields::from([
        ("body".into(), Value::Utf8("indexed text".into())),
    ]))].into_iter().map(|operation| StoredEvent { position: None,
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid: Iid([0; 16]), system_from: Timestamp(10), valid_time: Interval::ALL, recency: None, operation,
    }).collect::<Vec<_>>();
    let batch = build_event_batch(&events, Some(fields), None).unwrap();
    let schema = batch.schema();
    let op = schema.index_of("op").unwrap();
    let union = batch.column(op).as_union();
    let DataType::Union(legs, _) = union.data_type() else { panic!("Expected union"); };
    let children = legs.iter().map(|(id, _)| Arc::clone(union.child(id))).collect();
    let legs = legs.iter().map(|(id, field)| {
        let field = if field.name() == "put" {
            Arc::new(field.as_ref().clone().with_metadata([
                (rtdb_columnar::EXTENSION_NAME_KEY.to_owned(), "unrecognized".into()),
            ].into_iter().collect()))
        } else {
            Arc::clone(field)
        };
        (id, field)
    }).collect();
    let union = UnionArray::try_new(legs, union.type_ids().clone(), union.offsets().cloned(), children).unwrap();
    let mut fields = schema.fields().to_vec();
    fields[op] = Arc::new(fields[op].as_ref().clone().with_data_type(union.data_type().clone()));
    let mut columns = batch.columns().to_vec();
    columns[op] = Arc::new(union);
    let batch = EventBatch::new(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()).unwrap();
    assert_eq!(batch.borrow_event(0).unwrap().header().op, EventOp::Delete);
    assert_eq!(batch.borrow_event(1).err().unwrap().code, batch.read_event(1).unwrap_err().code);
}

/// Reconstructs stored cells under their captured put schema, preserving raw decimal coefficients.
/// Every value must belong to that schema; no decimal parameter or coefficient is converted.
/// The caller admits column construction and event-array overhead before calling.
pub fn copy_event_batch(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>) -> Result<RecordBatch> {
    event_batch(events, put_fields, iid_encoding, true, None, PutLeg::First, false)
}

/// Where the `put` leg sits in the operation union.
/// The reference's live relation starts with `delete` and `erase` and gains `put` with its first document, so live and L0 pages carry it last; compaction declares it first.
#[derive(Clone, Copy)]
enum PutLeg { First, Last }

/// Builds an owned index batch with scratch for one document column at a time, laid out as a live page.
/// `copy` selects the stored-cell writer, which preserves existing decimal coefficients.
pub fn owned_event_batch<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, copy: bool, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let (mut builder, metadata) = event_builder(events.len(), put_fields.as_ref(), memory)?;
    let batch = event_batch(events, put_fields, None, copy, Some(&mut builder), PutLeg::Last, false)?;
    builder.finish_with_metadata(batch, metadata)
}

fn event_builder(rows: usize, put_fields: Option<&StructFields<LogicalType>>, memory: &Arc<MemoryBudget>) -> Result<(BatchBuilder, usize)> {
    let metadata = event_schema_bytes(put_fields)?;
    let headers = rows.checked_mul(256).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(event_size)?;
    let builder = BatchBuilder::new(metadata.checked_add(headers).ok_or_else(event_size)?, memory)?;
    Ok((builder, metadata))
}

fn event_schema_bytes(put_fields: Option<&StructFields<LogicalType>>) -> Result<usize> {
    put_fields.map_or(Ok(4096), |fields| {
        fields.iter().try_fold(4096usize, |bytes, (name, ty)| {
            ty.retained_bytes()?.checked_add(name.len()).and_then(|bytes| bytes.checked_add(256))
                .and_then(|bytes| bytes.checked_mul(8)).and_then(|size| bytes.checked_add(size)).ok_or_else(event_size)
        })
    })
}

pub fn owned_live_event_schema(
    put_fields: &StructFields<LogicalType>, recency: bool, memory: &Arc<MemoryBudget>,
) -> Result<(Schema, Arc<MemoryReservation>)> {
    let put_fields = Some(put_fields);
    let reservation = memory.reserve(event_schema_bytes(put_fields)?)?;
    let schema = event_schema(put_fields, None, recency, PutLeg::Last, false)?;
    Ok((schema, reservation))
}

pub fn owned_tombstone_batch(
    iids: &FixedSizeBinaryArray, system_time: Timestamp,
    delete_intervals: Option<impl ExactSizeIterator<Item = Result<Interval>>>, memory: &Arc<MemoryBudget>,
) -> Result<OwnedBatch> {
    let rows = iids.len();
    if iids.value_length() != 16 || iids.null_count() != 0
        || delete_intervals.as_ref().is_some_and(|intervals| intervals.len() != rows)
    {
        return Err(invalid("Tombstone IIDs and valid-time bounds disagree"));
    }
    build_tombstone_batch(rows, || {
        FixedSizeBinaryArray::try_new(16, iids.value_data().to_vec().into(), None).map_err(invalid)
    }, system_time, delete_intervals, memory)
}

pub fn owned_delete_batch(
    iids: impl ExactSizeIterator<Item = Result<Iid>>, system_time: Timestamp, valid_time: Interval,
    memory: &Arc<MemoryBudget>,
) -> Result<OwnedBatch> {
    let rows = iids.len();
    build_tombstone_batch(rows, || {
        let mut column = FixedSizeBinaryBuilder::with_capacity(rows, 16);
        for iid in iids {
            column.append_value(iid?.0).map_err(invalid)?;
        }
        Ok(column.finish())
    }, system_time, Some(std::iter::repeat_n(valid_time, rows).map(Ok)), memory)
}

fn build_tombstone_batch(
    rows: usize, build_iids: impl FnOnce() -> Result<FixedSizeBinaryArray>, system_time: Timestamp,
    delete_intervals: Option<impl ExactSizeIterator<Item = Result<Interval>>>, memory: &Arc<MemoryBudget>,
) -> Result<OwnedBatch> {
    let (builder, metadata) = event_builder(rows, None, memory)?;
    let (tag, deletes, erases, from, to) = if let Some(intervals) = delete_intervals {
        let mut from = Vec::with_capacity(rows);
        let mut to = Vec::with_capacity(rows);
        for interval in intervals {
            let interval = interval?;
            from.push(interval.lower().0);
            to.push(interval.upper().0);
        }
        (0, rows, 0, from, to)
    } else {
        (1, 0, rows, vec![Interval::ALL.lower().0; rows], vec![Interval::ALL.upper().0; rows])
    };
    let offsets = (0..rows).map(|offset| i32::try_from(offset).map_err(invalid)).collect::<Result<Vec<_>>>()?;
    let op = UnionArray::try_new(
        operation_fields(None, PutLeg::Last, false), ScalarBuffer::from(vec![tag; rows]),
        Some(ScalarBuffer::from(offsets)), vec![Arc::new(NullArray::new(deletes)), Arc::new(NullArray::new(erases))],
    ).map_err(invalid)?;
    let fields = event_fields(DataType::FixedSizeBinary(16), op.data_type().clone(), false);
    let times = |values| Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef;
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![
        Arc::new(build_iids()?), times(vec![system_time.0; rows]), times(from), times(to), Arc::new(op),
    ]).map_err(invalid)?;
    builder.finish_with_metadata(batch, metadata)
}

/// `source_bytes` is the shared source allocation held by the document buffers.
pub fn owned_put_batch(documents: ArrayRef, headers: &[EventHeader], source_bytes: Option<usize>, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    put_batch(documents, headers, None, source_bytes, false, memory)
}

/// Stages puts whose document buffers already carry their allocation grants.
/// A compacted document and the event columns receive their own grants.
pub fn funded_put_batch(documents: ArrayRef, headers: &[EventHeader], memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    put_batch(documents, headers, None, None, true, memory)
}

/// Stages governed puts with their complete entity keys alongside the Arrow documents.
pub fn keyed_put_batch(documents: ArrayRef, headers: &[EventHeader], keys: BinaryArray, source_bytes: Option<usize>, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    if keys.len() != headers.len() || keys.null_count() != 0 { return Err(invalid("Governed puts require one complete key per header")); }
    put_batch(documents, headers, Some(keys), source_bytes, false, memory)
}

fn put_batch(
    documents: ArrayRef, headers: &[EventHeader], keys: Option<BinaryArray>, source_bytes: Option<usize>,
    funded: bool, memory: &Arc<MemoryBudget>,
) -> Result<OwnedBatch> {
    if documents.len() != headers.len() || documents.null_count() != 0
        || !matches!(documents.data_type(), DataType::Struct(_))
        || headers.iter().any(|header| header.op != EventOp::Put || (keys.is_none() && header.commit_position != 0)) {
        return Err(invalid("Put headers and their Arrow documents disagree"));
    }
    let retained = documents.get_array_memory_size().max(source_bytes.unwrap_or_default());
    let field = Field::new("put", documents.data_type().clone(), false);
    let document_layout_bytes = if funded { field.size() } else { 0 };
    let mut selected = documents.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(event_size)?;
    if selected < retained {
        for row in 0..documents.len() {
            selected = selected.checked_add(rtdb_columnar::estimate_event_copy_cell_bytes(&field, documents.as_ref(), row)?).ok_or_else(event_size)?;
            if selected >= retained { break; }
        }
    }
    // A source record can share one large document buffer across thousands of small puts.
    let (documents, copy) = if selected < retained {
        let layout = crate::ipc::schema_translation_bound(&Schema::new(vec![field]))?;
        let scratch = selected.checked_add(layout).ok_or_else(event_size)?;
        match memory.reserve(scratch) {
            Ok(scratch) => {
                let cells = (0..documents.len()).map(|row| Some((0, row))).collect::<Vec<_>>();
                let compact = rtdb_columnar::copy_event_cells(&[documents.as_ref()], &cells)?;
                (compact, Some(scratch))
            }
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => (documents, None),
            Err(error) => return Err(error),
        }
    } else {
        (documents, None)
    };
    let shared = funded && copy.is_none();
    let shared_bytes = if shared { documents.get_buffer_memory_size() } else { 0 };
    let layout_bytes = if shared { document_layout_bytes } else { 0 };
    let document_bytes = documents.get_array_memory_size().checked_sub(shared_bytes).ok_or_else(event_size)?;
    let bound = headers.len().checked_mul(128).and_then(|bytes| bytes.checked_add(document_bytes))
        .and_then(|bytes| bytes.checked_add(keys.as_ref().map_or(0, Array::get_array_memory_size)))
        .and_then(|bytes| bytes.checked_add(layout_bytes))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(event_size)?;
    let mut reservation = if let Some(mut reservation) = copy {
        let lease = Arc::get_mut(&mut reservation).expect("put copy owns its reservation");
        if bound > lease.bytes() { lease.grow_to(bound)?; } else { lease.shrink_to(bound)?; }
        reservation
    } else { memory.reserve(bound)? };
    let fields = operation_fields(Some(documents.data_type().clone()), PutLeg::Last, false);
    let offsets = (0..headers.len()).map(|offset| i32::try_from(offset).map_err(invalid)).collect::<Result<Vec<_>>>()?;
    let operations = UnionArray::try_new(fields, ScalarBuffer::from(vec![2; headers.len()]), Some(ScalarBuffer::from(offsets)),
        vec![Arc::new(NullArray::new(0)), Arc::new(NullArray::new(0)), documents]).map_err(invalid)?;
    let mut batch = event_columns(headers, Arc::new(operations), None, None)?;
    if let Some(keys) = keys {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(entity_key_field()));
        fields.push(Arc::new(commit_position_field()));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(keys));
        columns.push(Arc::new(UInt32Array::from_iter_values(headers.iter().map(|header| header.commit_position))));
        batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)?;
    }
    let batch = with_event_positions(batch, headers.iter().map(|header| header.position))?;
    let retained = batch.get_array_memory_size().checked_sub(shared_bytes)
        .and_then(|bytes| bytes.checked_add(layout_bytes)).ok_or_else(event_size)?;
    Arc::get_mut(&mut reservation).expect("put construction owns its reservation").shrink_to(retained)?;
    if !shared { return OwnedBatch::with_reservation(batch, reservation); }

    let op_index = batch.schema_ref().index_of("op").map_err(invalid)?;
    let columns = batch.columns().iter().enumerate().map(|(index, column)| {
        if index != op_index { return retain_array(Arc::clone(column), &reservation); }
        let (fields, types, offsets, children) = column.as_union().clone().into_parts();
        let types = ScalarBuffer::from(retain_buffer(types.inner(), &reservation));
        let offsets = offsets.map(|values| ScalarBuffer::from(retain_buffer(values.inner(), &reservation)));
        Ok(Arc::new(UnionArray::try_new(fields, types, offsets, children).map_err(invalid)?) as ArrayRef)
    }).collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(batch.schema(), columns).map_err(invalid)?;
    Ok(OwnedBatch::with_funded_columns(batch, reservation))
}

/// A batch has a `put` leg when it holds a put or declares document columns, as the reference's relation gains one.
fn event_batch<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, copy: bool, mut builder: Option<&mut BatchBuilder>, put_leg: PutLeg, redacted_leg: bool) -> Result<RecordBatch> {
    let mut puts = Vec::new();
    let mut deletes = 0usize;
    let mut erases = 0usize;
    let mut redacted = 0usize;
    let mut ids = Vec::with_capacity(events.len());
    let mut offsets = Vec::with_capacity(events.len());
    let put_fields = put_fields.filter(|fields| !fields.is_empty() || events.iter().any(|event| event.header().op == EventOp::Put));
    let has_put = put_fields.is_some();
    let (put_id, delete_id, erase_id) = match (has_put, put_leg) { (true, PutLeg::First) => (0, 1, 2), (true, PutLeg::Last) => (2, 0, 1), (false, _) => (0, 0, 1) };
    let redacted_id = if has_put { 3 } else { 2 };
    let headers: Vec<EventHeader> = events.iter().map(EncodedEvent::header).collect();
    for (event, header) in events.iter().zip(&headers) {
        let (id, offset) = match header.op {
            EventOp::Put => {
                if !has_put { return Err(invalid("Put event has no declared put schema")); }
                let offset = puts.len();
                puts.push(event);
                (put_id, offset)
            }
            EventOp::Delete => { let offset = deletes; deletes += 1; (delete_id, offset) }
            EventOp::Erase => { let offset = erases; erases += 1; (erase_id, offset) }
            EventOp::Redacted => {
                if event.entity_key().is_none() {
                    return Err(invalid("A redacted assertion requires a complete owned identity"));
                }
                EventBatch::require_redacted_position(header.op, event.origin(), header.valid_time)?;
                let offset = redacted;
                redacted += 1;
                (redacted_id, offset)
            }
        };
        ids.push(id);
        offsets.push(i32::try_from(offset).map_err(invalid)?);
    }
    let mut children: Vec<ArrayRef> = vec![Arc::new(NullArray::new(deletes)), Arc::new(NullArray::new(erases))];
    let put_type = if let Some(fields) = put_fields {
        let documents = put_columns(&puts, fields, copy, builder.as_deref_mut())?;
        let data_type = documents.data_type().clone();
        let index = usize::try_from(put_id).map_err(invalid)?;
        children.insert(index, documents);
        Some(data_type)
    } else {
        None
    };
    let include_redacted = redacted > 0 || redacted_leg;
    if include_redacted {
        children.push(Arc::new(NullArray::new(redacted)));
    }
    let op_fields = operation_fields(put_type, put_leg, include_redacted);
    let op: ArrayRef = Arc::new(UnionArray::try_new(op_fields, ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(invalid)?);
    let recencies = events.iter().any(|event| event.recency().is_some()).then(|| {
        events.iter().map(|event| event.recency().map(|time| time.0).ok_or_else(|| invalid("Mixed recency presence in one event batch"))).collect::<Result<Vec<_>>>()
    }).transpose()?;
    let plain = event_columns(&headers, op, iid_encoding, recencies)?;
    if !events.iter().any(|event| event.entity_key().is_some()) {
        let batch = if headers.iter().any(|header| header.commit_position != 0) {
            let mut fields = plain.schema().fields().to_vec();
            fields.push(Arc::new(commit_position_field()));
            let mut columns = plain.columns().to_vec();
            columns.push(Arc::new(UInt32Array::from_iter_values(headers.iter().map(|header| header.commit_position))));
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)?
        } else {
            plain
        };
        return with_event_positions(
            with_event_origins(batch, events)?,
            headers.iter().map(|header| header.position),
        );
    }

    let bytes = events.iter().try_fold(0usize, |bytes, event| {
        let key = event.entity_key().ok_or_else(|| invalid("A governed page mixed keyed and plain events"))?;
        bytes.checked_add(key.len()).ok_or_else(event_size)
    })?;
    i32::try_from(bytes).map_err(invalid)?;
    let _charge = builder.as_deref().map(|builder| builder.reserve_scratch(bytes)).transpose()?;
    let mut keys = BinaryBuilder::with_capacity(events.len(), bytes);
    for event in events {
        keys.append_value(event.entity_key().ok_or_else(|| invalid("A governed page is missing an entity key"))?);
    }
    let mut fields = plain.schema().fields().to_vec();
    fields.push(Arc::new(entity_key_field()));
    fields.push(Arc::new(commit_position_field()));
    let mut columns = plain.columns().to_vec();
    columns.push(Arc::new(keys.finish()));
    columns.push(Arc::new(UInt32Array::from_iter_values(headers.iter().map(|header| header.commit_position))));
    with_event_positions(
        with_event_origins(RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)?, events)?,
        headers.iter().map(|header| header.position),
    )
}

pub const ENTITY_KEY_FIELD: &str = "_entity_key";
pub(crate) const COMMIT_POSITION_FIELD: &str = "_commit_position";
pub const EVENT_ORIGIN_FIELD: &str = "_event_origin";

pub fn event_origin_field() -> Field { Field::new(EVENT_ORIGIN_FIELD, DataType::FixedSizeBinary(EventOrigin::WIDTH), true) }

pub fn origin_array(origins: impl IntoIterator<Item = Option<EventOrigin>>) -> Result<FixedSizeBinaryArray> {
    let origins = origins.into_iter();
    let mut writer = FixedSizeBinaryBuilder::with_capacity(origins.size_hint().0, EventOrigin::WIDTH);
    for origin in origins {
        match origin {
            Some(origin) => writer.append_value(origin.0).map_err(invalid)?,
            None => writer.append_null(),
        }
    }
    Ok(writer.finish())
}

fn with_event_origins<E: EncodedEvent>(batch: RecordBatch, events: &[E]) -> Result<RecordBatch> {
    if !events.iter().any(|event| event.origin().is_some()) { return Ok(batch); }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(event_origin_field()));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(origin_array(events.iter().map(EncodedEvent::origin))?));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)
}

pub fn append_origins(batch: &OwnedBatch, origins: impl IntoIterator<Item = Option<EventOrigin>>, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let bound = batch.batch().num_rows().checked_mul(64).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(event_size)?;
    let mut lease = memory.reserve(bound)?;
    let column = origin_array(origins)?;
    Arc::get_mut(&mut lease).expect("unpublished origin column owns its reservation").shrink_to(column.get_array_memory_size())?;
    let index = batch.batch().schema().index_of(EVENT_POSITION_FIELD).unwrap_or(batch.batch().num_columns());
    batch.with_inserted_column(index, event_origin_field(), Arc::new(column), lease)
}

pub const EVENT_POSITION_FIELD: &str = "_event_position";

pub fn event_position_field() -> Field {
    Field::new(EVENT_POSITION_FIELD, DataType::FixedSizeBinary(EventPosition::WIDTH), false)
}

fn positions_array(positions: impl IntoIterator<Item = EventPosition>) -> Result<FixedSizeBinaryArray> {
    let positions = positions.into_iter();
    let mut column = FixedSizeBinaryBuilder::with_capacity(positions.size_hint().0, EventPosition::WIDTH);
    for position in positions {
        column.append_value(position.to_bytes()).map_err(invalid)?;
    }
    Ok(column.finish())
}

fn with_event_positions(
    batch: RecordBatch, positions: impl Iterator<Item = Option<EventPosition>> + Clone,
) -> Result<RecordBatch> {
    if positions.clone().all(|position| position.is_none()) { return Ok(batch); }
    if positions.clone().any(|position| position.is_none()) {
        return Err(invalid("An event batch mixed assigned positions and pending events"));
    }
    let mut fields = batch.schema().fields().to_vec();
    fields.push(Arc::new(event_position_field()));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(positions_array(positions.flatten())?));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)
}

pub fn append_event_positions(
    batch: &OwnedBatch, positions: impl IntoIterator<Item = EventPosition>, memory: &Arc<MemoryBudget>,
) -> Result<OwnedBatch> {
    let bound = batch.batch().num_rows().checked_mul(32)
        .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(event_size)?;
    let mut lease = memory.reserve(bound)?;
    let column = positions_array(positions)?;
    Arc::get_mut(&mut lease).expect("unpublished event positions own their reservation")
        .shrink_to(column.get_array_memory_size())?;
    let field = event_position_field();
    match batch.batch().schema().index_of(EVENT_POSITION_FIELD) {
        Ok(index) => batch.with_replaced_column(index, Arc::new(column), lease),
        Err(_) => batch.with_appended_column(field, Arc::new(column), lease),
    }
}

pub fn entity_key_field() -> Field {
    Field::new(ENTITY_KEY_FIELD, DataType::Binary, false)
}

/// Keep one topology file schema even for pages containing only readmission rows.
pub fn with_topology_commit_positions(batch: OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let schema = batch.batch().schema_ref();
    if schema.index_of(COMMIT_POSITION_FIELD).is_ok() {
        return Ok(batch);
    }
    let rows = batch.batch().num_rows();
    let reservation = memory.reserve(rows.checked_mul(4).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(event_size)?)?;
    let column = UInt32Array::from(vec![0; rows]);
    let index = schema.index_of(EVENT_ORIGIN_FIELD).or_else(|_| schema.index_of(EVENT_POSITION_FIELD))
        .unwrap_or(batch.batch().num_columns());
    batch.with_inserted_column(index, commit_position_field(), Arc::new(column), reservation)
}

pub fn commit_position_field() -> Field {
    Field::new(COMMIT_POSITION_FIELD, DataType::UInt32, false)
}

fn operation_fields(put: Option<DataType>, put_leg: PutLeg, redacted: bool) -> UnionFields {
    let mut fields = vec![Field::new("delete", DataType::Null, true), Field::new("erase", DataType::Null, true)];
    if let Some(put) = put {
        let index = match put_leg { PutLeg::First => 0, PutLeg::Last => 2 };
        fields.insert(index, Field::new("put", put, false));
    }
    if redacted { fields.push(Field::new("redacted", DataType::Null, true)); }
    UnionFields::new(0..fields.len() as i8, fields)
}

fn event_time_type() -> DataType {
    DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()))
}

fn recency_field() -> Field { Field::new("_recency", event_time_type(), false) }

fn event_fields(iid: DataType, op: DataType, recency: bool) -> Vec<Field> {
    let time_type = event_time_type();
    let mut fields = vec![
        Field::new("_iid", iid, false),
        Field::new("_system_from", time_type.clone(), false),
        Field::new("_valid_from", time_type.clone(), false),
        Field::new("_valid_to", time_type, false),
        Field::new("op", op, false),
    ];
    if recency { fields.push(recency_field()); }
    fields
}

fn put_column_field(name: &str, ty: &LogicalType) -> Result<Arc<Field>> {
    to_field(name, ty).map(Arc::new)
}

fn event_schema(
    put_fields: Option<&StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool,
    put_leg: PutLeg, redacted: bool,
) -> Result<Schema> {
    let put = put_fields.filter(|fields| !fields.is_empty()).map(|fields| {
        fields.iter().map(|(name, ty)| put_column_field(name, ty)).collect::<Result<Vec<_>>>()
            .map(|fields| DataType::Struct(fields.into()))
    }).transpose()?;
    let op = DataType::Union(operation_fields(put, put_leg, redacted), UnionMode::Dense);
    Ok(Schema::new(event_fields(iid_data_type(iid_encoding)?, op, recency)))
}

fn event_columns(headers: &[EventHeader], op: ArrayRef, iid_encoding: Option<RunEndWidth>, recencies: Option<Vec<i64>>) -> Result<RecordBatch> {
    let iids: Vec<_> = headers.iter().map(|header| header.iid).collect();
    let iids = build_iids(&iids, iid_encoding)?;
    let fields = event_fields(iids.data_type().clone(), op.data_type().clone(), recencies.is_some());
    let times = |values: Vec<i64>| Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef;
    let mut columns = vec![iids, times(headers.iter().map(|header| header.system_from.0).collect()),
        times(headers.iter().map(|header| header.valid_time.lower().0).collect()), times(headers.iter().map(|header| header.valid_time.upper().0).collect()), op];
    if let Some(recencies) = recencies {
        columns.push(times(recencies));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid)
}

fn put_columns<E: EncodedEvent>(
    documents: &[&E],
    fields: StructFields<LogicalType>,
    copy: bool,
    mut builder: Option<&mut BatchBuilder>,
) -> Result<ArrayRef> {
    if documents.iter().any(|doc| !doc.put_within(&fields)) {
        return Err(Anomaly::incorrect("rtdb/column-type-mismatch", "Document has a field outside its declared layout"));
    }
    let mut schema = Vec::with_capacity(fields.len());
    let mut columns = Vec::with_capacity(fields.len());
    for (name, ty) in fields {
        let field = put_column_field(&name, &ty)?;
        if copy && let Some(column) = columns::gather_column(documents, &field, &ty, builder.as_deref_mut())? {
            schema.push(field);
            columns.push(column);
            continue;
        }
        let read_size = || {
            documents.iter().try_fold(0usize, |bytes, doc| {
                let cell = match doc.put_column(&name) {
                    Some(PutColumn::Stored(field, array, row)) => rtdb_columnar::estimate_read_cell_bytes(field, array, row)?,
                    Some(PutColumn::Absent) => std::mem::size_of::<Value>(),
                    None => 0,
                };
                bytes.checked_add(cell).ok_or_else(event_size)
            })
        };
        let mut read_bytes = None;
        if copy && let Some(column) = selected::column(documents, &field, &ty, builder.as_deref_mut(), || {
            read_bytes = Some(read_size()?);
            Ok(())
        })? {
            schema.push(field);
            columns.push(column);
            continue;
        }
        let _scratch = builder.as_deref().map(|builder| {
            let bytes = read_bytes.map_or_else(read_size, Ok)?;
            builder.reserve_scratch(bytes)
        }).transpose()?;
        let cells = documents.iter().map(|doc| doc.put_cell(&name, &ty)).collect::<Result<Vec<_>>>()?;
        let build = || {
            let values = cells.iter().map(|cell| cell.as_ref()).collect::<Vec<_>>();
            if copy { copy_array_refs(&ty, &values) } else { build_array_refs(&ty, &values) }
        };
        let column = match builder.as_deref_mut() {
            Some(builder) => builder.column(estimate_builder_bytes_refs(&ty, cells.iter().map(|cell| cell.as_ref()))?, build)?,
            None => build()?,
        };
        schema.push(field);
        columns.push(column);
    }
    build_struct_array(schema.into(), &columns, documents.len(), None)
}

/// Keeps the declared recency column even when a page is empty.
pub fn build_event_batch_with_recency(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool) -> Result<RecordBatch> {
    event_batch_with_recency(events, put_fields, iid_encoding, recency, false, None, false)
}

/// Copies stored cells with the same ownership obligations as `copy_event_batch`, retaining empty-page recency metadata.
pub fn copy_event_batch_with_recency(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool) -> Result<RecordBatch> {
    event_batch_with_recency(events, put_fields, iid_encoding, recency, true, None, false)
}

/// As `copy_event_batch_with_recency`, for events of any kind that encode their own cells.
pub fn copy_encoded_batch_with_recency<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool) -> Result<RecordBatch> {
    event_batch_with_recency(events, put_fields, iid_encoding, recency, true, None, false)
}

/// Describes a compaction output's operation and recency layout before its first page.
pub fn compacted_event_schema(
    put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool, redacted_leg: bool,
) -> Result<SchemaRef> {
    event_schema(put_fields.as_ref(), iid_encoding, recency, PutLeg::First, redacted_leg).map(Arc::new)
}

/// Copies a compaction page under the declared operation layout and memory budget.
pub fn copy_compaction_batch<E: EncodedEvent>(
    events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool,
    redacted_leg: bool, memory: &Arc<MemoryBudget>,
) -> Result<OwnedBatch> {
    let (mut builder, metadata) = event_builder(events.len(), put_fields.as_ref(), memory)?;
    let batch = event_batch_with_recency(events, put_fields, iid_encoding, recency, true, Some(&mut builder), redacted_leg)?;
    builder.finish_with_metadata(batch, metadata)
}

/// Compacts already aligned spill pages into one event batch in range order.
pub fn copy_spill_event_ranges(
    sources: &[(&EventBatch, std::ops::Range<usize>)], memory: &Arc<MemoryBudget>,
) -> Result<Option<OwnedBatch>> {
    let Some((first, _)) = sources.first() else { return Ok(None); };
    let schema = first.arrow().schema_ref();
    if matches!(schema.field(first.iid).data_type(), DataType::RunEndEncoded(..)) {
        return Ok(None);
    }
    let DataType::Union(legs, _) = schema.field(first.op).data_type() else { return Err(invalid("Missing operation union fields")); };
    if let Some((_, put)) = legs.iter().find(|(_, field)| field.name() == "put") {
        let document = rtdb_columnar::from_field(put)?;
        // The event writer merges adjacent equal values in run fields at any depth.
        if document.unencoded() != document { return Ok(None); }
    }
    for (page, rows) in sources {
        if page.arrow().schema_ref() != schema { return Ok(None); }
        for row in rows.clone() {
            if page.header(row)?.op == EventOp::Put {
                let (_, document, offset) = page.operation(row)?;
                let document = document.as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("Put operations must contain document structs"))?;
                if !document.is_valid(offset) || document.fields().iter().zip(document.columns()).any(|(field, array)| {
                    !columns::valid_cell(field, array.as_ref(), offset, true)
                }) { return Ok(None); }
            }
        }
    }
    let count = sources.iter().try_fold(0usize, |count, (_, rows)| {
        count.checked_add(rows.len()).ok_or_else(event_size)
    })?;
    let metadata = crate::ipc::schema_translation_bound(schema)?;
    let base = count.checked_mul(32).and_then(|bytes| bytes.checked_add(metadata)).ok_or_else(event_size)?;
    let mut builder = BatchBuilder::new(base, memory)?;
    let picks = sources.iter().enumerate().flat_map(|(source, (_, rows))| {
        rows.clone().map(move |row| Some((source, row)))
    }).collect::<Vec<_>>();
    let arrays = schema.fields().iter().enumerate().map(|(column, field)| {
        let values = sources.iter().map(|(page, _)| page.arrow().column(column).as_ref()).collect::<Vec<_>>();
        let bound = picks.iter().flatten().try_fold(metadata, |bound, &(source, row)| {
            rtdb_columnar::estimate_event_copy_cell_bytes(field, values[source], row)?
                .checked_mul(4).and_then(|bytes| bound.checked_add(bytes)).ok_or_else(event_size)
        })?;
        builder.column(bound, || rtdb_columnar::copy_event_cells(&values, &picks))
    }).collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::clone(schema), arrays).map_err(invalid)?;
    builder.finish_with_metadata(batch, metadata).map(Some)
}

fn event_batch_with_recency<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool, copy: bool, builder: Option<&mut BatchBuilder>, redacted_leg: bool) -> Result<RecordBatch> {
    if events.iter().any(|event| event.recency().is_some() != recency) { return Err(invalid("Event recency does not match the declared schema")); }
    let batch = event_batch(events, put_fields, iid_encoding, copy, builder, PutLeg::First, redacted_leg)?;
    if recency && events.is_empty() {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(recency_field()));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new()).with_timezone("UTC")));
        return RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(invalid);
    }
    Ok(batch)
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-event-data", error.to_string())
}

fn event_size() -> Anomaly { invalid("Event batch allocation exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{EntityId, ScalarType};

    #[test]
    fn compacted_declaration_matches_the_empty_event_writer() {
        let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 280, scale: -200, bit_width: 128 });
        let variants = LogicalType::from_legs([LogicalType::Scalar(ScalarType::Utf8), LogicalType::Null, decimal.clone()]);
        let document = StructFields::from([
            ("z".into(), LogicalType::Struct(StructFields::from([
                ("nullable_decimal".into(), decimal.maybe()),
                ("variants".into(), variants),
            ])).maybe()),
            ("a".into(), LogicalType::Nothing),
        ]);
        for put in [None, Some(StructFields::new()), Some(document)] {
            for iid in [None, Some(RunEndWidth::I16), Some(RunEndWidth::I32), Some(RunEndWidth::I64)] {
                for (recency, redaction) in [(false, false), (true, false), (false, true), (true, true)] {
                    let expected = event_batch_with_recency::<StoredEvent>(&[], put.clone(), iid, recency, false, None, redaction).unwrap();
                    let declared = compacted_event_schema(put.clone(), iid, recency, redaction).unwrap();
                    assert_eq!(&declared, expected.schema_ref());
                }
            }
        }
    }

    #[test]
    fn compacted_declaration_and_writer_reject_invalid_put_types() {
        let fields = Some(StructFields::from([
            ("invalid".into(), LogicalType::Scalar(ScalarType::FixedSizeBinary(-1))),
        ]));
        let expected = event_batch_with_recency::<StoredEvent>(&[], fields.clone(), None, false, false, None, false).unwrap_err();
        let actual = compacted_event_schema(fields, None, false, false).unwrap_err();
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.message, expected.message);
    }

    #[test]
    fn a_staged_put_charges_only_the_selected_part_of_its_large_source_buffer() {
        use arrow_array::{StringArray, StructArray};

        let other = "x".repeat(1 << 20);
        let strings = Arc::new(StringArray::from(vec!["selected", other.as_str()]));
        let documents = StructArray::try_new(vec![Field::new("v", DataType::Utf8, false)].into(), vec![strings], None).unwrap();
        let selected = Arc::new(documents.slice(0, 1));
        let memory = MemoryBudget::new(1 << 17);
        let header = EventHeader { position: None, commit_position: 0, iid: EntityId::Integer(1).iid(), system_from: Timestamp(1), valid_time: Interval::ALL, op: EventOp::Put };
        assert!(selected.get_array_memory_size() > memory.limit_bytes());

        let batch = owned_put_batch(selected, &[header], None, &memory).unwrap();
        let event = EventBatch::new(batch.batch().clone()).unwrap().read_event(0).unwrap();

        assert_eq!(event.operation, StoredOperation::Put(StructFields::from([("v".into(), Value::Utf8("selected".into()))])));
        assert!(memory.used_bytes() < 4096);
        drop(batch);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn a_small_staged_put_releases_its_shared_source_record() {
        use arrow_array::{StringArray, StructArray};

        let content = StructArray::try_new(vec![Field::new("v", DataType::Utf8, false)].into(), vec![Arc::new(StringArray::from(vec!["kept"]))], None).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("doc", content.data_type().clone(), false), Field::new("other-op", DataType::Utf8, false)]));
        let other = "x".repeat(1 << 20);
        let source = RecordBatch::try_new(schema, vec![Arc::new(content), Arc::new(StringArray::from(vec![other.as_str()]))]).unwrap();
        let source_memory = MemoryBudget::new(1 << 22);
        let source = OwnedBatch::new(source, &source_memory).unwrap();
        let memory = MemoryBudget::new(1 << 17);
        let header = EventHeader { position: None, commit_position: 0, iid: EntityId::Integer(1).iid(), system_from: Timestamp(1), valid_time: Interval::ALL, op: EventOp::Put };

        let batch = owned_put_batch(Arc::clone(source.batch().column(0)), &[header], Some(source.reservation().bytes()), &memory).unwrap();
        drop(source);

        assert_eq!(source_memory.used_bytes(), 0);
        let event = EventBatch::new(batch.batch().clone()).unwrap().read_event(0).unwrap();
        assert_eq!(event.operation, StoredOperation::Put(StructFields::from([("v".into(), Value::Utf8("kept".into()))])));
    }

    fn page(value: Value) -> Vec<PageEvent> {
        let document = StructFields::from([("v".into(), value.clone())]);
        let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(1).iid(), system_from: Timestamp(1), valid_time: Interval::ALL, operation: StoredOperation::Put(document), recency: None };
        let types = StructFields::from([("v".into(), value.logical_type())]);
        EventBatch::new(build_event_batch(&[event], Some(types), None).unwrap()).unwrap().page_events().unwrap()
    }

    #[test]
    fn a_column_every_page_stores_under_its_output_layout_is_copied_from_the_pages() {
        let events = [page(Value::I64(1)), page(Value::I64(2))].concat();
        let ty = LogicalType::Scalar(ScalarType::I64);
        let field = to_field("v", &ty).unwrap();
        let memory = MemoryBudget::new(1 << 20);
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();
        let selected = columns::gather_column(&events.iter().collect::<Vec<_>>(), &field, &ty, Some(&mut builder)).unwrap().unwrap();
        let values = selected.as_any().downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(values.values().as_ref(), &[1, 2]);
    }

    #[test]
    fn a_column_stored_under_two_layouts_joins_their_arrow_vectors() {
        let events = [page(Value::I64(1)), page(Value::Utf8("a".into()))].concat();
        let events: Vec<_> = events.iter().collect();
        let integer = LogicalType::Scalar(ScalarType::I64);
        assert!(columns::gather_column(&events, &to_field("v", &integer).unwrap(), &integer, None).unwrap().is_none());
        let union = integer.join(&LogicalType::Scalar(ScalarType::Utf8));
        let field = to_field("v", &union).unwrap();
        let selected = columns::gather_column(&events, &field, &union, None).unwrap().unwrap();
        assert_eq!(rtdb_columnar::read_value(&field, selected.as_ref(), 0).unwrap(), Value::I64(1));
        assert_eq!(rtdb_columnar::read_value(&field, selected.as_ref(), 1).unwrap(), Value::Utf8("a".into()));
    }
}
