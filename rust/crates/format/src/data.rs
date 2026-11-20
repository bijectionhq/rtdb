use rtdb_types::StructFields;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, NullArray, RecordBatch, TimestampMicrosecondArray, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema, TimeUnit as ArrowTimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{BatchBuilder, IidView, MemoryBudget, OwnedBatch, build_array, build_iids, build_struct_array, copy_array, estimate_builder_bytes_refs, struct_field_cell, to_field};
use rtdb_types::{Anomaly, EventHeader, EventOp, Iid, Interval, LogicalType, Result, RunEndWidth, Timestamp, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum StoredOperation {
    Put(StructFields<Value>),
    Delete,
    Erase,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub iid: Iid,
    pub system_from: Timestamp,
    pub valid_time: Interval,
    pub operation: StoredOperation,
    pub recency: Option<Timestamp>,
}

impl StoredEvent {
    pub fn header(&self) -> EventHeader {
        EventHeader { iid: self.iid, system_from: self.system_from, valid_time: self.valid_time, op: match &self.operation {
            StoredOperation::Put(_) => EventOp::Put, StoredOperation::Delete => EventOp::Delete, StoredOperation::Erase => EventOp::Erase,
        } }
    }
}

/// An event an event batch encodes: its header and recency, and the cells of its put document.
pub trait EncodedEvent {
    fn header(&self) -> EventHeader;
    fn recency(&self) -> Option<Timestamp>;
    /// Whether every column the event's put document holds is among `fields`.
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool;
    /// The put document's `name` cell under the column type `ty`, as `struct_field_cell` takes it of the decoded document.
    fn put_cell(&self, name: &str, ty: &LogicalType) -> Result<std::borrow::Cow<'_, Value>>;
}

impl EncodedEvent for StoredEvent {
    fn header(&self) -> EventHeader { StoredEvent::header(self) }
    fn recency(&self) -> Option<Timestamp> { self.recency }
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
    pub iid: Iid,
    pub system_from: Timestamp,
    pub valid_time: Interval,
    pub op: EventOp,
    pub recency: Option<Timestamp>,
    /// `document_bytes`, once asked for, since compaction charges an event's document at each stage it passes.
    bytes: std::sync::OnceLock<usize>,
}

#[derive(Debug)]
struct EventPage { put: Option<(arrow_array::StructArray, arrow_schema::Fields, StructFields<LogicalType>)> }

impl PageEvent {
    /// The bytes the event's document decodes to at most, which encoding its cells may hold at once.
    pub fn document_bytes(&self) -> Result<usize> {
        if let Some(bytes) = self.bytes.get() { return Ok(*bytes); }
        let bytes = match (&self.page.put, self.op) {
            (Some((array, fields, _)), EventOp::Put) => fields.iter().enumerate().try_fold(0usize, |bytes, (index, field)| {
                bytes.checked_add(rtdb_columnar::estimate_read_cell_bytes(field, array.column(index).as_ref(), self.offset)?).ok_or_else(event_size)
            })?,
            _ => 0,
        };
        Ok(*self.bytes.get_or_init(|| bytes))
    }
}

impl EncodedEvent for PageEvent {
    fn header(&self) -> EventHeader { EventHeader { iid: self.iid, system_from: self.system_from, valid_time: self.valid_time, op: self.op } }
    fn recency(&self) -> Option<Timestamp> { self.recency }
    fn put_within(&self, fields: &StructFields<LogicalType>) -> bool {
        match (&self.page.put, self.op) { (Some((_, columns, _)), EventOp::Put) => columns.iter().all(|column| fields.contains_key(column.name())), _ => true }
    }
    fn put_cell(&self, name: &str, ty: &LogicalType) -> Result<std::borrow::Cow<'_, Value>> {
        let (Some((array, fields, types)), EventOp::Put) = (&self.page.put, self.op) else { return Err(invalid("Only a put event has document cells")) };
        // As reading the document struct does: a column it stores reads under its own type, and one it lacks falls back
        // as `struct_field_cell` falls back for a key the document has no value for.
        Ok(std::borrow::Cow::Owned(match fields.iter().position(|field| field.name() == name) {
            Some(index) => match types.get(name).ok_or_else(|| invalid("Put column has no logical type"))? {
                LogicalType::Nothing => Value::Undefined,
                stored => rtdb_columnar::read_value_as(stored, &fields[index], array.column(index).as_ref(), self.offset)?,
            },
            None => match ty { LogicalType::Nothing => Value::Undefined, _ => Value::Null },
        }))
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
    /// The put leg's logical type, derived from its field on the first put read.
    put_type: std::sync::Arc<std::sync::OnceLock<LogicalType>>,
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
                ("put", _) => return Err(invalid("Put operations must contain document structs")),
                ("delete" | "erase", _) => return Err(invalid("Delete and erase operations must contain null values")),
                (name, _) => return Err(invalid(format!("Unknown event operation: {name}"))),
            }
        }
        Ok(Self { batch, iid, system_from, valid_from, valid_to, op, recency, put_type: Default::default() })
    }

    pub fn arrow(&self) -> &RecordBatch { &self.batch }
    /// The page's events read in place, each reading its document's cells only as an event batch encodes them.
    pub fn page_events(&self) -> Result<Vec<PageEvent>> {
        let union = self.operations()?;
        let DataType::Union(legs, _) = union.data_type() else { return Err(invalid("Missing operation union fields")); };
        let put = match legs.iter().find(|(_, leg)| leg.name() == "put") {
            Some((id, leg)) => {
                let array = union.child(id).as_any().downcast_ref::<arrow_array::StructArray>().ok_or_else(|| invalid("Put operations must contain document structs"))?.clone();
                let DataType::Struct(fields) = leg.data_type() else { return Err(invalid("Put operations must contain document structs")) };
                let LogicalType::Struct(types) = rtdb_columnar::from_field(leg)? else { return Err(invalid("A put operation must contain a document struct")) };
                Some((array, fields.clone(), types))
            }
            None => None,
        };
        let page = Arc::new(EventPage { put });
        (0..self.len()).map(|row| {
            let header = self.header(row)?;
            Ok(PageEvent { page: Arc::clone(&page), offset: union.value_offset(row), iid: header.iid, system_from: header.system_from, valid_time: header.valid_time,
                op: header.op, recency: self.recency.map(|index| self.timestamp(index, row)).transpose()?, bytes: std::sync::OnceLock::new() })
        }).collect()
    }
    /// The dense union of each row's operation, whose put leg holds its document.
    pub fn operations(&self) -> Result<&UnionArray> {
        self.batch.column(self.op).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("Event operation array type changed"))
    }
    pub fn len(&self) -> usize { self.batch.num_rows() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn iid(&self, row: usize) -> Result<Iid> {
        IidView::new(self.batch.column(self.iid).as_ref())?.get(row)
    }

    pub fn header(&self, row: usize) -> Result<EventHeader> {
        if row >= self.len() { return Err(invalid("Event row exceeds batch length")); }
        let op = match self.operation(row)?.0.name().as_str() {
            "put" => EventOp::Put, "delete" => EventOp::Delete, "erase" => EventOp::Erase,
            name => return Err(invalid(format!("Unknown event operation: {name}"))),
        };
        Ok(EventHeader {
            iid: self.iid(row)?,
            system_from: self.timestamp(self.system_from, row)?,
            valid_time: Interval::new(self.timestamp(self.valid_from, row)?, self.timestamp(self.valid_to, row)?)?,
            op,
        })
    }

    pub fn read_event(&self, row: usize) -> Result<StoredEvent> {
        let header = self.header(row)?;
        let operation = match header.op {
            EventOp::Put => {
                let (field, array, offset) = self.operation(row)?;
                let logical = match self.put_type.get() { Some(logical) => logical, None => { let derived = rtdb_columnar::from_field(field)?; self.put_type.get_or_init(|| derived) } };
                let Value::Struct(document) = rtdb_columnar::read_value_as(logical, field, array.as_ref(), offset)? else {
                    return Err(invalid("A put operation must contain a document struct"));
                };
                StoredOperation::Put(document)
            }
            EventOp::Delete => StoredOperation::Delete,
            EventOp::Erase => StoredOperation::Erase,
        };
        Ok(StoredEvent { iid: header.iid, system_from: header.system_from, valid_time: header.valid_time, operation,
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
    event_batch(events, put_fields, iid_encoding, false, None, PutLeg::First)
}

/// Reconstructs stored cells under their captured put schema, preserving raw decimal coefficients.
/// Every value must belong to that schema; no decimal parameter or coefficient is converted.
/// The caller admits column construction and event-array overhead before calling.
pub fn copy_event_batch(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>) -> Result<RecordBatch> {
    event_batch(events, put_fields, iid_encoding, true, None, PutLeg::First)
}

/// Where the `put` leg sits in the operation union.
/// The reference's live relation starts with `delete` and `erase` and gains `put` with its first document, so live and L0 pages carry it last; compaction declares it first.
#[derive(Clone, Copy)]
enum PutLeg { First, Last }

/// Builds an owned index batch with scratch for one document column at a time, laid out as a live page.
/// `copy` selects the stored-cell writer, which preserves existing decimal coefficients.
pub fn owned_event_batch(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, copy: bool, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch> {
    let metadata = put_fields.as_ref().map_or(Ok(4096), |fields| {
        fields.iter().try_fold(4096usize, |bytes, (name, ty)| {
            ty.retained_bytes()?.checked_add(name.len()).and_then(|bytes| bytes.checked_add(256))
                .and_then(|bytes| bytes.checked_mul(8)).and_then(|size| bytes.checked_add(size)).ok_or_else(event_size)
        })
    })?;
    let headers = events.len().checked_mul(256).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(event_size)?;
    let mut builder = BatchBuilder::new(metadata.checked_add(headers).ok_or_else(event_size)?, memory)?;
    let batch = event_batch(events, put_fields, None, copy, Some(&mut builder), PutLeg::Last)?;
    builder.finish_with_metadata(batch, metadata)
}

/// A batch has a `put` leg when it holds a put or declares document columns, as the reference's relation gains one.
fn event_batch<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, copy: bool, builder: Option<&mut BatchBuilder>, put_leg: PutLeg) -> Result<RecordBatch> {
    let mut puts = Vec::new();
    let mut deletes = 0usize;
    let mut erases = 0usize;
    let mut ids = Vec::with_capacity(events.len());
    let mut offsets = Vec::with_capacity(events.len());
    let put_fields = put_fields.filter(|fields| !fields.is_empty() || events.iter().any(|event| event.header().op == EventOp::Put));
    let has_put = put_fields.is_some();
    let (put_id, delete_id, erase_id) = match (has_put, put_leg) { (true, PutLeg::First) => (0, 1, 2), (true, PutLeg::Last) => (2, 0, 1), (false, _) => (0, 0, 1) };
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
        };
        ids.push(id);
        offsets.push(i32::try_from(offset).map_err(invalid)?);
    }
    let mut op_fields = vec![Arc::new(Field::new("delete", DataType::Null, true)), Arc::new(Field::new("erase", DataType::Null, true))];
    let mut children: Vec<ArrayRef> = vec![Arc::new(NullArray::new(deletes)), Arc::new(NullArray::new(erases))];
    if let Some(fields) = put_fields {
        let documents = put_columns(&puts, fields, copy, builder)?;
        let index = usize::try_from(put_id).map_err(invalid)?;
        op_fields.insert(index, Arc::new(Field::new("put", documents.data_type().clone(), false)));
        children.insert(index, documents);
    }
    let op_fields = UnionFields::new(0..op_fields.len() as i8, op_fields);
    let op: ArrayRef = Arc::new(UnionArray::try_new(op_fields.clone(), ScalarBuffer::from(ids), Some(ScalarBuffer::from(offsets)), children).map_err(invalid)?);
    let iids: Vec<_> = headers.iter().map(|header| header.iid).collect();
    let iids = build_iids(&iids, iid_encoding)?;
    let time_type = DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into()));
    let mut fields = vec![
        Field::new("_iid", iids.data_type().clone(), false),
        Field::new("_system_from", time_type.clone(), false),
        Field::new("_valid_from", time_type.clone(), false),
        Field::new("_valid_to", time_type.clone(), false),
        Field::new("op", DataType::Union(op_fields, UnionMode::Dense), false),
    ];
    let times = |values: Vec<i64>| Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC")) as ArrayRef;
    let mut columns = vec![iids, times(headers.iter().map(|header| header.system_from.0).collect()),
        times(headers.iter().map(|header| header.valid_time.lower().0).collect()), times(headers.iter().map(|header| header.valid_time.upper().0).collect()), op];
    if events.iter().any(|event| event.recency().is_some()) {
        let recencies = events.iter().map(|event| event.recency().map(|time| time.0).ok_or_else(|| invalid("Mixed recency presence in one event batch"))).collect::<Result<Vec<_>>>()?;
        fields.push(Field::new("_recency", time_type, false));
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
        let cells = documents.iter().map(|doc| doc.put_cell(&name, &ty)).collect::<Result<Vec<_>>>()?;
        let build = || {
            let values = cells.iter().map(|cell| cell.as_ref().clone()).collect::<Vec<_>>();
            if copy { copy_array(&ty, &values) } else { build_array(&ty, &values) }
        };
        let column = match builder.as_deref_mut() {
            Some(builder) => builder.column(estimate_builder_bytes_refs(&ty, cells.iter().map(|cell| cell.as_ref()))?, build)?,
            None => build()?,
        };
        schema.push(Arc::new(to_field(&name, &ty)?));
        columns.push(column);
    }
    build_struct_array(schema.into(), &columns, documents.len(), None)
}

/// Keeps the declared recency column even when a page is empty.
pub fn build_event_batch_with_recency(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool) -> Result<RecordBatch> {
    event_batch_with_recency(events, put_fields, iid_encoding, recency, false)
}

/// Copies stored cells with the same ownership obligations as `copy_event_batch`, retaining empty-page recency metadata.
pub fn copy_event_batch_with_recency(events: &[StoredEvent], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool) -> Result<RecordBatch> {
    event_batch_with_recency(events, put_fields, iid_encoding, recency, true)
}

/// As `copy_event_batch_with_recency`, for events of any kind that encode their own cells.
pub fn copy_encoded_batch_with_recency<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool) -> Result<RecordBatch> {
    event_batch_with_recency(events, put_fields, iid_encoding, recency, true)
}

fn event_batch_with_recency<E: EncodedEvent>(events: &[E], put_fields: Option<StructFields<LogicalType>>, iid_encoding: Option<RunEndWidth>, recency: bool, copy: bool) -> Result<RecordBatch> {
    if events.iter().any(|event| event.recency().is_some() != recency) { return Err(invalid("Event recency does not match the declared schema")); }
    let batch = event_batch(events, put_fields, iid_encoding, copy, None, PutLeg::First)?;
    if recency && events.is_empty() {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("_recency", DataType::Timestamp(ArrowTimeUnit::Microsecond, Some("UTC".into())), false)));
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
