//! Column-at-a-time pages: a scan gathers the cells its visible rows resolve to into one column per output field,
//! so filters and aggregates can read whole columns, and rows are materialised only for operators that need them.
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StructArray, UnionArray};
use arrow_schema::{DataType, Field as ArrowField};
use rtdb_columnar::{MemoryBudget, MemoryReservation, read_execution_value, read_execution_value_as};
use rtdb_index::IndexBatch;
use rtdb_plan::Field;
use rtdb_temporal::ResolvedRow;
use rtdb_types::{LogicalType, Result, Timestamp, Value};

use crate::{cursor::OwnedRow, invalid, pages::{OwnedPage, PageBuilder}};

/// One output field of a page.
pub(crate) enum Column {
    /// Cells read through the source field and the put child's logical type, exactly as a row read takes them.
    Arrow { array: ArrayRef, field: Arc<ArrowField>, logical: Option<LogicalType> },
    /// Cells whose sources disagree on their physical layout, or that are derived from the event rather than stored.
    Values(Vec<Value>),
}

impl Column {
    pub fn value(&self, row: usize) -> Result<Value> {
        match self {
            Self::Arrow { array, field, logical } => {
                let value = match logical {
                    Some(logical) => read_execution_value_as(logical, field, array.as_ref(), row)?,
                    None => read_execution_value(field, array.as_ref(), row)?,
                };
                Ok(if matches!(value, Value::Undefined) { Value::Null } else { value })
            }
            Self::Values(values) => values.get(row).cloned().ok_or_else(|| invalid("column-row", "Column row is outside its page")),
        }
    }
    pub fn bound(&self, row: usize) -> Result<usize> {
        match self {
            Self::Arrow { array, field, logical } => if logical.is_some() { rtdb_columnar::estimate_read_cell_bytes(field, array.as_ref(), row) }
                else { rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), row) },
            Self::Values(values) => values.get(row).map_or(Ok(0), |value| value.retained_bytes().map(|bytes| bytes.saturating_add(std::mem::size_of::<Value>()))),
        }
    }
    pub(crate) fn retained_bytes(&self) -> Result<usize> {
        match self {
            Self::Arrow { array, .. } => Ok(array.get_array_memory_size()),
            Self::Values(values) => values.iter().try_fold(values.capacity().saturating_mul(std::mem::size_of::<Value>()), |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)),
        }
    }
}

/// A page of columns sharing one row count, charged for every column it retains.
pub(crate) struct ColumnBatch { pub columns: Vec<Column>, pub len: usize, _reservation: Arc<MemoryReservation> }

impl ColumnBatch {
    /// A page of columns allocated under `reservation`, whose charge becomes exactly what they retain.
    pub fn settled(columns: Vec<Column>, len: usize, mut reservation: Arc<MemoryReservation>) -> Result<Self> {
        let bytes = columns.iter().try_fold(256usize, |bytes, column| bytes.checked_add(column.retained_bytes()?).ok_or_else(size_error))?;
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
        if bytes <= lease.bytes() { lease.shrink_to(bytes)?; } else { lease.grow_to(bytes)?; }
        Ok(Self { columns, len, _reservation: reservation })
    }
    fn charged(&self) -> usize { self._reservation.bytes() }
    /// A page of rows as columns of values, charged as each column is built.
    pub fn from_page(page: &OwnedPage, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let width = page.rows().first().map_or(0, |row| row.values.len());
        let mut reservation = memory.reserve(256)?;
        let mut columns = Vec::with_capacity(width);
        for position in 0..width {
            let column = Column::Values(page.rows().iter().map(|row| row.values.get(position).cloned().ok_or_else(|| invalid("column-row", "A row is narrower than its page"))).collect::<Result<_>>()?);
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
            lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
            columns.push(column);
        }
        Self::settled(columns, page.len(), reservation)
    }

    /// Materialises every row, admitting the page's rows together before any is built.
    /// Rows share the page's charge, which ends when the last of them drops.
    pub fn rows(&self, memory: &Arc<MemoryBudget>) -> Result<OwnedPage> { self.rows_admitting(memory, false) }
    pub fn rows_admitting(&self, memory: &Arc<MemoryBudget>, admit_dates: bool) -> Result<OwnedPage> {
        let mut page = PageBuilder::new(self.len, memory)?;
        let mut bound = 0usize;
        for row in 0..self.len {
            let row_bound = self.columns.iter().try_fold(256usize, |bound, column| bound.checked_add(column.bound(row)?).ok_or_else(size_error))?;
            bound = bound.checked_add(row_bound).ok_or_else(size_error)?;
        }
        let reservation = memory.reserve(bound)?;
        for row in 0..self.len {
            let mut values = Vec::with_capacity(self.columns.len());
            for column in &self.columns {
                let mut value = column.value(row)?;
                if admit_dates { value.admit_expression_dates()?; }
                values.push(value);
            }
            page.push(OwnedRow::with_reservation(values, Arc::clone(&reservation))?)?;
        }
        Ok(page.finish())
    }
}

/// The put leg each event batch stores a resolved row's cells in.
struct Source { batch: Arc<IndexBatch>, put: StructArray }

/// Gathers the scan's columns for rows that name their batch by its place in `sources`, in the order given.
pub(crate) fn gather(rows: &[ResolvedRow<(u32, u32)>], sources_in: &[Arc<IndexBatch>], columns: &[Field], memory: &Arc<MemoryBudget>,
    mut derived: impl FnMut(usize) -> Result<Option<Column>>) -> Result<ColumnBatch> {
    let mut sources: Vec<Source> = Vec::new();
    // Each batch's operation union, and the sources its put legs became, found once per batch rather than per row.
    let mut batches: Vec<(UnionArray, Vec<(i8, usize)>)> = Vec::new();
    let mut index = vec![usize::MAX; sources_in.len()];
    let mut cells = Vec::with_capacity(rows.len());
    for row in rows {
        let (handle, position) = (row.source.0 as usize, row.source.1 as usize);
        let row_batch = sources_in.get(handle).ok_or_else(|| invalid("column-gather", "A resolved row names no batch"))?;
        let batch = match index[handle] {
            usize::MAX => {
                batches.push((row_batch.view().operations()?.clone(), Vec::new()));
                index[handle] = batches.len() - 1;
                batches.len() - 1
            }
            batch => batch,
        };
        let (op, legs) = &mut batches[batch];
        let leg = op.type_id(position);
        let source = match legs.iter().find(|(known, _)| *known == leg) {
            Some((_, source)) => *source,
            None => {
                let put = op.child(leg).as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("event-schema", "Visible event is not a put struct"))?.clone();
                sources.push(Source { batch: Arc::clone(row_batch), put });
                legs.push((leg, sources.len() - 1));
                sources.len() - 1
            }
        };
        cells.push((source, op.value_offset(position)));
    }
    // How much a gathered column holds depends on which cells it picks, so each is charged as soon as it is built.
    let mut reservation = memory.reserve(256)?;
    let mut output = Vec::with_capacity(columns.len());
    for (position, field) in columns.iter().enumerate() {
        let column = if let Some(column) = derived(position)? {
            column
        } else {
            match field.name.as_str() {
                "_valid_from" => times("_valid_from", rows.iter().map(|row| row.valid_time.lower()), false),
                "_valid_to" => times("_valid_to", rows.iter().map(|row| row.valid_time.upper()), true),
                "_system_from" => times("_system_from", rows.iter().map(|row| row.system_time.lower()), false),
                "_system_to" => times("_system_to", rows.iter().map(|row| row.system_time.upper()), true),
                "_iid" => {
                    let iids = rows.iter().map(|row| sources_in[row.source.0 as usize].header(row.source.1 as usize).map(|header| header.iid.0)).collect::<Result<Vec<_>>>()?;
                    let array = arrow_array::FixedSizeBinaryArray::try_from_iter(iids.iter()).map_err(|error| invalid("column-gather", error))?;
                    Column::Arrow { array: Arc::new(array), field: Arc::new(ArrowField::new("_iid", DataType::FixedSizeBinary(16), false)),
                        logical: Some(LogicalType::Scalar(rtdb_types::ScalarType::FixedSizeBinary(16))) }
                }
                name => stored(&sources, &cells, name)?,
            }
        };
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
        lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
        output.push(column);
    }
    ColumnBatch::settled(output, rows.len(), reservation)
}

/// A temporal column as microsecond UTC timestamps; an upper bound at the end of time reads as null.
fn times(name: &str, times: impl Iterator<Item = Timestamp>, upper: bool) -> Column {
    let array = arrow_array::TimestampMicrosecondArray::from_iter(times.map(|time| (!(upper && time == Timestamp::MAX)).then_some(time.0))).with_timezone("UTC");
    let logical = LogicalType::Scalar(rtdb_types::ScalarType::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) });
    Column::Arrow { array: Arc::new(array), field: Arc::new(ArrowField::new(name, DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), upper)),
        logical: Some(if upper { logical.maybe() } else { logical }) }
}

/// One stored field across every source; a single array when every source that stores it agrees on its layout.
fn stored(sources: &[Source], cells: &[(usize, usize)], name: &str) -> Result<Column> {
    let children: Vec<Option<(ArrayRef, Arc<ArrowField>, Option<LogicalType>)>> = sources.iter().map(|source| {
        let DataType::Struct(fields) = source.put.data_type() else { return None };
        let index = fields.iter().position(|field| field.name() == name)?;
        Some((Arc::clone(source.put.column(index)), Arc::clone(&fields[index]), source.batch.put_field_type(index).cloned()))
    }).collect();
    let Some((_, field, logical)) = children.iter().flatten().next().cloned() else { return Ok(Column::Values(vec![Value::Null; cells.len()])) };
    let uniform = children.iter().all(|child| child.as_ref().is_some_and(|(_, other, other_logical)| other == &field && other_logical == &logical));
    if uniform && flat(field.data_type()) {
        let arrays: Vec<&dyn Array> = children.iter().flatten().map(|(array, ..)| array.as_ref()).collect();
        let array = interleave(&arrays, cells)?;
        return Ok(Column::Arrow { array, field, logical });
    }
    if !uniform && let Some(column) = joined_union(&children, cells, name)? { return Ok(column); }
    cells.iter().map(|&(source, offset)| match &children[source] {
        None => Ok(Value::Null),
        Some((array, field, logical)) => {
            let value = match logical { Some(logical) => read_execution_value_as(logical, field, array.as_ref(), offset)?, None => read_execution_value(field, array.as_ref(), offset)? };
            Ok(if matches!(value, Value::Undefined) { Value::Null } else { value })
        }
    }).collect::<Result<Vec<_>>>().map(Column::Values)
}

/// Gathers a column whose sources store it in different scalar layouts, as a column written with several integer widths
/// across transactions is, into the dense union of their layouts. `None` where a leg can't be told apart by its layout.
fn joined_union(children: &[Option<(ArrayRef, Arc<ArrowField>, Option<LogicalType>)>], cells: &[(usize, usize)], name: &str) -> Result<Option<Column>> {
    enum Layout<'a> { Scalar(i8, &'a dyn Array), Union(&'a UnionArray, Vec<Option<i8>>) }
    let mut logicals = Vec::with_capacity(children.len());
    for child in children {
        let Some((_, field, Some(logical))) = child else { return Ok(None) };
        if !flat(field.data_type()) { return Ok(None); }
        logicals.push(logical);
    }
    let logical = LogicalType::join_all(logicals.iter().copied());
    let target = rtdb_columnar::to_field(name, &logical)?;
    let DataType::Union(legs, arrow_schema::UnionMode::Dense) = target.data_type() else { return Ok(None) };
    let leg_of = |field: &ArrowField| {
        let mut found = legs.iter().filter(|(_, leg)| leg.data_type() == field.data_type() && leg.metadata() == field.metadata());
        let (id, _) = found.next()?;
        found.next().is_none().then_some(id)
    };
    let null_leg = legs.iter().find(|(_, leg)| leg.data_type() == &DataType::Null).map(|(id, _)| id);
    let mut layouts = Vec::with_capacity(children.len());
    for (array, field, _) in children.iter().flatten() {
        layouts.push(match field.data_type() {
            DataType::Union(source_legs, _) => {
                let union = array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("column-gather", "A union source is not a union"))?;
                let mut map = vec![None; source_legs.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
                for (id, leg) in source_legs.iter() {
                    let Some(target) = leg_of(leg) else { return Ok(None) };
                    map[id as usize] = Some(target);
                }
                Layout::Union(union, map)
            }
            _ => match leg_of(field) { Some(leg) => Layout::Scalar(leg, array.as_ref()), None => return Ok(None) },
        });
    }
    let mut picks: Vec<Vec<(usize, usize)>> = vec![Vec::new(); legs.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
    let mut ids = Vec::with_capacity(cells.len());
    let mut offsets = Vec::with_capacity(cells.len());
    for &(source, row) in cells {
        let (leg, offset) = match &layouts[source] {
            Layout::Scalar(leg, array) if !array.is_null(row) => (*leg, row),
            Layout::Scalar(..) => match null_leg { Some(leg) => (leg, row), None => return Ok(None) },
            Layout::Union(union, map) => match map.get(union.type_id(row) as usize).copied().flatten() {
                Some(leg) => (leg, union.value_offset(row)),
                None => return Err(invalid("column-gather", "A union cell names no leg")),
            },
        };
        let picked = &mut picks[leg as usize];
        offsets.push(i32::try_from(picked.len()).map_err(|_| invalid("column-gather", "A union leg exceeds i32 offsets"))?);
        picked.push((source, offset));
        ids.push(leg);
    }
    let children = legs.iter().map(|(id, leg)| -> Result<ArrayRef> {
        let picked = &picks[id as usize];
        if leg.data_type() == &DataType::Null { return Ok(Arc::new(arrow_array::NullArray::new(picked.len()))); }
        let empty = arrow_array::new_empty_array(leg.data_type());
        let arrays: Vec<&dyn Array> = layouts.iter().map(|layout| match layout {
            Layout::Scalar(source, array) if *source == id => *array,
            Layout::Scalar(..) => empty.as_ref(),
            Layout::Union(union, map) => map.iter().position(|target| *target == Some(id)).map_or(empty.as_ref(), |source| union.child(source as i8).as_ref()),
        }).collect();
        arrow_select::interleave::interleave(&arrays, picked).map_err(|error| invalid("column-gather", error))
    }).collect::<Result<Vec<_>>>()?;
    let array = UnionArray::try_new(legs.clone(), datafusion_common::arrow::buffer::ScalarBuffer::from(ids), Some(datafusion_common::arrow::buffer::ScalarBuffer::from(offsets)), children)
        .map_err(|error| invalid("column-gather", error))?;
    Ok(Some(Column::Arrow { array: Arc::new(array), field: Arc::new(target), logical: Some(logical) }))
}

/// Whether Arrow's interleave copies the layout without validating it: nested layouts are rebuilt and checked, and
/// stored cells such as nulls under a non-nullable list item fail that check by panicking, so they are read as values.
/// Interleaves cells of arrays sharing one layout; a dense union gathers each leg's children in bulk, where Arrow's
/// generic path would extend the union one cell at a time.
pub(crate) fn interleave(arrays: &[&dyn Array], cells: &[(usize, usize)]) -> Result<ArrayRef> {
    let DataType::Union(fields, arrow_schema::UnionMode::Dense) = arrays.first().map(|array| array.data_type()).ok_or_else(|| invalid("column-gather", "Nothing to interleave"))? else {
        return arrow_select::interleave::interleave(arrays, cells).map_err(|error| invalid("column-gather", error));
    };
    let unions = arrays.iter().map(|array| array.as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("column-gather", "A union source is not a union"))).collect::<Result<Vec<_>>>()?;
    let mut picks: Vec<Vec<(usize, usize)>> = vec![Vec::new(); fields.iter().map(|(id, _)| id as usize + 1).max().unwrap_or(0)];
    let mut ids = Vec::with_capacity(cells.len());
    let mut offsets = Vec::with_capacity(cells.len());
    for &(source, row) in cells {
        let union = unions[source];
        let id = union.type_id(row);
        let leg = picks.get_mut(usize::try_from(id).map_err(|_| invalid("column-gather", "A union cell names no leg"))?).ok_or_else(|| invalid("column-gather", "A union cell names no leg"))?;
        offsets.push(i32::try_from(leg.len()).map_err(|_| invalid("column-gather", "A union leg exceeds i32 offsets"))?);
        leg.push((source, union.value_offset(row)));
        ids.push(id);
    }
    let children = fields.iter().map(|(id, _)| {
        let children: Vec<&dyn Array> = unions.iter().map(|union| union.child(id).as_ref()).collect();
        arrow_select::interleave::interleave(&children, &picks[id as usize]).map_err(|error| invalid("column-gather", error))
    }).collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(UnionArray::try_new(fields.clone(), datafusion_common::arrow::buffer::ScalarBuffer::from(ids), Some(datafusion_common::arrow::buffer::ScalarBuffer::from(offsets)), children).map_err(|error| invalid("column-gather", error))?))
}

pub(crate) fn flat(data_type: &DataType) -> bool {
    scalar_layout(data_type) || matches!(data_type, DataType::Union(fields, arrow_schema::UnionMode::Dense) if fields.iter().all(|(_, field)| scalar_layout(field.data_type())))
}

/// Layouts with no child arrays, whose cells interleave without any validation that could reject them.
fn scalar_layout(data_type: &DataType) -> bool {
    data_type.is_primitive() || matches!(data_type, DataType::Boolean | DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) | DataType::Null)
}

fn size_error() -> rtdb_types::Anomaly { invalid("column-size", "Column page exceeds addressable memory") }

/// A batch source: a scan, or an operator that keeps its input's columns.
#[async_trait::async_trait]
pub(crate) trait BatchCursor: Send {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>>;
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { None }
}
pub(crate) type BoxBatchCursor = Box<dyn BatchCursor>;

#[async_trait::async_trait]
impl BatchCursor for crate::scan::ScanCursor {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { crate::scan::ScanCursor::next_batch(self).await }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { crate::pages::PageCursor::trace_attributes(self, span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { crate::pages::PageCursor::scan_counts(self) }
}

/// Materialises a batch source's rows for row operators.
pub(crate) struct BatchRows { input: BoxBatchCursor, memory: Arc<MemoryBudget>, admit_dates: bool }
impl BatchRows {
    pub fn new(input: BoxBatchCursor, memory: Arc<MemoryBudget>) -> Self { Self { input, memory, admit_dates: false } }
    /// Admits each cell's dates as the row projection the batches stand in for would have.
    pub fn admitting_dates(self, admit_dates: bool) -> Self { Self { admit_dates, ..self } }
}
#[async_trait::async_trait]
impl crate::pages::PageCursor for BatchRows {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(batch) = self.input.next_batch().await? else { return Ok(None) };
        batch.rows_admitting(&self.memory, self.admit_dates).map(Some)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

impl ColumnBatch {
    /// Keeps the rows `mask` selects, column by column.
    fn select(self, mask: &[bool], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let selected = mask.iter().filter(|keep| **keep).count();
        if selected == self.len { return Ok(self); }
        // A selection holds a subset of each column's cells, so it retains no more than the page it is taken from.
        let reservation = memory.reserve(self.charged().saturating_add(mask.len()))?;
        let predicate = arrow_array::BooleanArray::from(mask.to_vec());
        let columns = self.columns.into_iter().map(|column| Ok(match column {
            Column::Arrow { array, field, logical } => Column::Arrow { array: arrow_select::filter::filter(array.as_ref(), &predicate).map_err(|error| invalid("column-filter", error))?, field, logical },
            Column::Values(values) => Column::Values(values.into_iter().zip(mask).filter_map(|(value, keep)| keep.then_some(value)).collect()),
        })).collect::<Result<Vec<_>>>()?;
        Self::settled(columns, selected, reservation)
    }
}

/// Filters batches, evaluating the predicate over whole columns where its shape and the columns' types allow,
/// and row by row, exactly as the row filter does, for any batch where they don't.
pub(crate) struct BatchFilter { input: BoxBatchCursor, schema: rtdb_plan::Schema, predicate: rtdb_plan::Expr, execution: crate::context::Execution }
impl BatchFilter {
    pub fn new(input: BoxBatchCursor, schema: rtdb_plan::Schema, predicate: rtdb_plan::Expr, execution: crate::context::Execution) -> Self { Self { input, schema, predicate, execution } }
}
#[async_trait::async_trait]
impl BatchCursor for BatchFilter {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        while let Some(batch) = self.input.next_batch().await? {
            self.execution.context.check_cancelled()?;
            let truth = kernel::truth(&self.predicate, &self.schema, &batch, self.execution.parameters.values())
                .or_else(|| crate::vector::truth(&self.predicate, &self.schema, &batch, self.execution.parameters.values()));
            let mask = match truth {
                Some(truth) => truth.into_iter().map(|value| value == Some(true)).collect::<Vec<_>>(),
                None => {
                    let metadata = self.execution.scope()?;
                    let invocation = self.execution.prepare_expression(&self.predicate, &self.schema, &metadata)?;
                    let scope = self.execution.scope()?;
                    let mut page = batch.rows(&self.execution.context.memory)?;
                    let mut mask = Vec::with_capacity(batch.len);
                    while let Some(row) = page.next() {
                        mask.push(rtdb_expr::is_true(&self.execution.evaluate_invocation(&scope, &invocation, &row.values).await?)?);
                        scope.reset()?;
                    }
                    mask
                }
            };
            if mask.iter().any(|keep| *keep) { return batch.select(&mask, &self.execution.context.memory).map(Some); }
        }
        Ok(None)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

/// Three-valued predicate evaluation over a batch's columns, for the predicate shapes and cell types whose
/// semantics are exactly those of the scalar evaluator; `None` sends the batch to it instead.
mod kernel {
    use arrow_array::{Array, BooleanArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, TimeUnit as ArrowUnit};
    use rtdb_plan::{BinaryOp, Expr, Schema, UnaryOp};
    use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};
    use super::{Column, ColumnBatch};

    type Truth = Vec<Option<bool>>;

    /// A comparable cell; integers and floats compare only where the conversion between them is exact.
    #[derive(Clone, Copy, PartialEq, PartialOrd)]
    enum Cell<'a> { Int(i64), Float(f64), Text(&'a str), Bool(bool), Micros(i64, bool) }

    enum Cells<'a> { Int(&'a Int64Array), Int32(&'a Int32Array), Int16(&'a Int16Array), Int8(&'a Int8Array), Float(&'a Float64Array), Text(&'a StringArray), Bool(&'a BooleanArray), Micros(&'a TimestampMicrosecondArray), Values(&'a [Value]), Owned(Vec<Option<Cell<'static>>>),
        /// A dense union whose legs are each a plain cell type, read through the leg each cell's type id names.
        Union { array: &'a arrow_array::UnionArray, legs: Vec<Option<Cells<'a>>> } }

    impl<'a> Cells<'a> {
        fn of(column: &'a Column) -> Option<Self> {
            match column {
                Column::Values(values) => Some(Self::Values(values)),
                Column::Arrow { array, field, logical } => {
                    let plain = |expected: ScalarType| match logical { Some(LogicalType::Scalar(scalar)) => *scalar == expected, Some(_) => false, None => field.metadata().is_empty() };
                    let any = array.as_any();
                    match array.data_type() {
                        DataType::Int64 if plain(ScalarType::I64) => any.downcast_ref().map(Self::Int),
                        DataType::Int32 if plain(ScalarType::I32) => any.downcast_ref().map(Self::Int32),
                        DataType::Int16 if plain(ScalarType::I16) => any.downcast_ref().map(Self::Int16),
                        DataType::Int8 if plain(ScalarType::I8) => any.downcast_ref().map(Self::Int8),
                        DataType::Float64 if plain(ScalarType::F64) => any.downcast_ref().map(Self::Float),
                        DataType::Utf8 if plain(ScalarType::Utf8) => any.downcast_ref().map(Self::Text),
                        DataType::Boolean if plain(ScalarType::Bool) => any.downcast_ref().map(Self::Bool),
                        DataType::Timestamp(ArrowUnit::Microsecond, _) if matches!(logical, Some(LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, .. })) | None) => any.downcast_ref().map(Self::Micros),
                        DataType::Union(fields, arrow_schema::UnionMode::Dense) => {
                            let array: &arrow_array::UnionArray = any.downcast_ref()?;
                            let mut legs: Vec<Option<Self>> = Vec::new();
                            for (id, field) in fields.iter() {
                                if id < 0 || !field.metadata().is_empty() { return None; }
                                let child = array.child(id);
                                let leg_any = child.as_any();
                                let cells = match child.data_type() {
                                    DataType::Int64 => Self::Int(leg_any.downcast_ref()?),
                                    DataType::Int32 => Self::Int32(leg_any.downcast_ref()?),
                                    DataType::Int16 => Self::Int16(leg_any.downcast_ref()?),
                                    DataType::Int8 => Self::Int8(leg_any.downcast_ref()?),
                                    DataType::Float64 => Self::Float(leg_any.downcast_ref()?),
                                    DataType::Utf8 => Self::Text(leg_any.downcast_ref()?),
                                    DataType::Boolean => Self::Bool(leg_any.downcast_ref()?),
                                    _ => return None,
                                };
                                let id = id as usize;
                                if legs.len() <= id { legs.resize_with(id + 1, || None); }
                                legs[id] = Some(cells);
                            }
                            Some(Self::Union { array, legs })
                        }
                        _ => None,
                    }
                }
            }
        }
        /// `Err` where a cell has a type the kernel doesn't evaluate.
        fn get(&self, row: usize) -> Result<Option<Cell<'a>>, ()> {
            fn valid<A: Array>(array: &A, row: usize) -> bool { array.is_valid(row) }
            Ok(match self {
                Self::Int(array) => valid(*array, row).then(|| Cell::Int(array.value(row))),
                Self::Int32(array) => valid(*array, row).then(|| Cell::Int(i64::from(array.value(row)))),
                Self::Int16(array) => valid(*array, row).then(|| Cell::Int(i64::from(array.value(row)))),
                Self::Int8(array) => valid(*array, row).then(|| Cell::Int(i64::from(array.value(row)))),
                Self::Owned(cells) => cells[row],
                Self::Float(array) => valid(*array, row).then(|| Cell::Float(array.value(row))),
                Self::Text(array) => valid(*array, row).then(|| Cell::Text(array.value(row))),
                Self::Bool(array) => valid(*array, row).then(|| Cell::Bool(array.value(row))),
                Self::Micros(array) => valid(*array, row).then(|| Cell::Micros(array.value(row), matches!(array.data_type(), DataType::Timestamp(_, Some(_))))),
                Self::Values(values) => scalar(&values[row])?,
                Self::Union { array, legs } => {
                    let leg = usize::try_from(array.type_id(row)).ok().and_then(|id| legs.get(id)).and_then(Option::as_ref).ok_or(())?;
                    leg.get(array.value_offset(row))?
                }
            })
        }
    }

    /// A computed numeric cell, which borrows nothing.
    fn owned(cell: Cell<'_>) -> Option<Cell<'static>> {
        match cell { Cell::Int(value) => Some(Cell::Int(value)), Cell::Float(value) => Some(Cell::Float(value)), Cell::Bool(value) => Some(Cell::Bool(value)), Cell::Micros(value, zoned) => Some(Cell::Micros(value, zoned)), Cell::Text(_) => None }
    }
    fn scalar(value: &Value) -> Result<Option<Cell<'_>>, ()> {
        Ok(match value {
            Value::Null => None,
            Value::I8(value) => Some(Cell::Int(i64::from(*value))),
            Value::I16(value) => Some(Cell::Int(i64::from(*value))),
            Value::I32(value) => Some(Cell::Int(i64::from(*value))),
            Value::I64(value) => Some(Cell::Int(*value)),
            Value::F32(value) => Some(Cell::Float(f64::from(*value))),
            Value::F64(value) => Some(Cell::Float(*value)),
            Value::Utf8(value) => Some(Cell::Text(value)),
            Value::Bool(value) => Some(Cell::Bool(*value)),
            Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone } => Some(Cell::Micros(*value, timezone.is_some())),
            _ => return Err(()),
        })
    }

    const EXACT: i64 = 1 << 53;
    /// Orders two cells as the scalar comparison does, or `Err` where the pair is outside the kernel.
    fn order(left: Cell<'_>, right: Cell<'_>, equality_only: bool) -> Result<std::cmp::Ordering, ()> {
        use std::cmp::Ordering;
        let float = |value: f64| if value.is_nan() { Err(()) } else { Ok(value) };
        // Equality is IEEE (`-0.0 = 0.0`), ordering is total (`-0.0 < 0.0`), as in the scalar evaluator.
        let floats = |a: f64, b: f64| if equality_only { a.partial_cmp(&b).ok_or(()) } else { Ok(a.total_cmp(&b)) };
        match (left, right) {
            (Cell::Int(a), Cell::Int(b)) => Ok(a.cmp(&b)),
            (Cell::Micros(a, zoned), Cell::Micros(b, other)) if zoned == other => Ok(a.cmp(&b)),
            (Cell::Float(a), Cell::Float(b)) => floats(float(a)?, float(b)?),
            (Cell::Int(a), Cell::Float(b)) if a.unsigned_abs() < EXACT as u64 => floats(a as f64, float(b)?),
            (Cell::Float(a), Cell::Int(b)) if b.unsigned_abs() < EXACT as u64 => floats(float(a)?, b as f64),
            (Cell::Text(a), Cell::Text(b)) if equality_only || (a.is_ascii() && b.is_ascii()) => Ok(a.cmp(b)),
            (Cell::Bool(a), Cell::Bool(b)) if equality_only => Ok(if a == b { Ordering::Equal } else { Ordering::Less }),
            _ => Err(()),
        }
    }

    enum Operand<'a> { Cells(Cells<'a>), Constant(Option<Cell<'a>>) }
    impl<'a> Operand<'a> {
        fn of(expr: &'a Expr, schema: &Schema, batch: &'a ColumnBatch, parameters: &'a [Value]) -> Option<Self> {
            match expr {
                Expr::Column { id, .. } => {
                    let column = &batch.columns[schema.position(*id)?];
                    match Cells::of(column) {
                        Some(cells) => Some(Self::Cells(cells)),
                        // A column without a direct cell reader, a union of integer legs say, is read once into owned cells.
                        None => (0..batch.len).map(|row| {
                            let value = column.value(row).ok()?;
                            match scalar(&value).ok()? { None => Some(None), Some(cell) => owned(cell).map(Some) }
                        }).collect::<Option<Vec<_>>>().map(|cells| Self::Cells(Cells::Owned(cells))),
                    }
                }
                Expr::Literal(value) => scalar(value).ok().map(Self::Constant),
                Expr::Parameter { index, .. } => scalar(parameters.get(*index)?).ok().map(Self::Constant),
                Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, .. } => {
                    let column = super::projection::column(expr, schema, batch, parameters).ok()??;
                    let cells = Cells::of(&column)?;
                    Some(Self::Cells(Cells::Owned((0..batch.len).map(|row| match cells.get(row).ok()? { None => Some(None), Some(cell) => owned(cell).map(Some) }).collect::<Option<_>>()?)))
                }
                _ => None,
            }
        }
        fn get(&self, row: usize) -> Result<Option<Cell<'a>>, ()> { match self { Self::Cells(cells) => cells.get(row), Self::Constant(cell) => Ok(*cell) } }
    }

    pub(super) fn truth(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: &[Value]) -> Option<Truth> {
        use std::cmp::Ordering::*;
        let rows = batch.len;
        match expr {
            Expr::Binary { op: BinaryOp::And, left, right } => {
                let (left, right) = (truth(left, schema, batch, parameters)?, truth(right, schema, batch, parameters)?);
                Some(left.into_iter().zip(right).map(|pair| match pair { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None }).collect())
            }
            Expr::Binary { op: BinaryOp::Or, left, right } => {
                let (left, right) = (truth(left, schema, batch, parameters)?, truth(right, schema, batch, parameters)?);
                Some(left.into_iter().zip(right).map(|pair| match pair { (Some(true), _) | (_, Some(true)) => Some(true), (Some(false), Some(false)) => Some(false), _ => None }).collect())
            }
            Expr::Unary { op: UnaryOp::Not, expr } => Some(truth(expr, schema, batch, parameters)?.into_iter().map(|value| value.map(|value| !value)).collect()),
            Expr::Unary { op: op @ (UnaryOp::IsNull | UnaryOp::IsNotNull), expr } => {
                let operand = Operand::of(expr, schema, batch, parameters)?;
                (0..rows).map(|row| operand.get(row).ok().map(|cell| Some(cell.is_none() == (*op == UnaryOp::IsNull)))).collect()
            }
            Expr::Column { .. } => {
                let operand = Operand::of(expr, schema, batch, parameters)?;
                (0..rows).map(|row| match operand.get(row).ok()? { None => Some(None), Some(Cell::Bool(value)) => Some(Some(value)), Some(_) => None }).collect()
            }
            Expr::Binary { op, left, right } => {
                let (equality_only, accept): (bool, fn(std::cmp::Ordering) -> bool) = match op {
                    BinaryOp::Eq => (true, |ordering| ordering == Equal), BinaryOp::NotEq => (true, |ordering| ordering != Equal),
                    BinaryOp::Lt => (false, |ordering| ordering == Less), BinaryOp::LtEq => (false, |ordering| ordering != Greater),
                    BinaryOp::Gt => (false, |ordering| ordering == Greater), BinaryOp::GtEq => (false, |ordering| ordering != Less),
                    _ => return None,
                };
                let (left, right) = (Operand::of(left, schema, batch, parameters)?, Operand::of(right, schema, batch, parameters)?);
                (0..rows).map(|row| match (left.get(row).ok()?, right.get(row).ok()?) {
                    (Some(a), Some(b)) => order(a, b, equality_only).ok().map(|ordering| Some(accept(ordering))),
                    _ => Some(None),
                }).collect()
            }
            Expr::Call { name, args, .. } if name == "===" && args.len() == 2 => {
                let (left, right) = (Operand::of(&args[0], schema, batch, parameters)?, Operand::of(&args[1], schema, batch, parameters)?);
                (0..rows).map(|row| Some(Some(match (left.get(row).ok()?, right.get(row).ok()?) {
                    (None, None) => true,
                    (None, Some(_)) | (Some(_), None) => false,
                    (Some(Cell::Int(a)), Some(Cell::Int(b))) => a == b,
                    (Some(Cell::Float(a)), Some(Cell::Float(b))) => if a.is_nan() || b.is_nan() { return None } else { a == b },
                    (Some(Cell::Int(_)), Some(Cell::Float(_))) | (Some(Cell::Float(_)), Some(Cell::Int(_))) => false,
                    (Some(Cell::Text(a)), Some(Cell::Text(b))) => a == b,
                    (Some(Cell::Bool(a)), Some(Cell::Bool(b))) => a == b,
                    _ => return None,
                }))).collect()
            }
            Expr::Between { operand, low, high, symmetric: false } => {
                let (operand, low, high) = (Operand::of(operand, schema, batch, parameters)?, Operand::of(low, schema, batch, parameters)?, Operand::of(high, schema, batch, parameters)?);
                (0..rows).map(|row| {
                    let (value, low, high) = (operand.get(row).ok()?, low.get(row).ok()?, high.get(row).ok()?);
                    let above = match (value, low) { (Some(value), Some(low)) => Some(order(value, low, false).ok()? != Less), _ => None };
                    let below = match (value, high) { (Some(value), Some(high)) => Some(order(value, high, false).ok()? != Greater), _ => None };
                    Some(match (above, below) { (Some(false), _) | (_, Some(false)) => Some(false), (Some(true), Some(true)) => Some(true), _ => None })
                }).collect()
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, Float64Array, Int64Array, StringArray};
    use rtdb_plan::{BinaryOp, ColumnId, Expr, Schema, UnaryOp};
    use rtdb_types::ScalarType;

    struct Random(u64);
    impl Random {
        fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
        fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
    }

    fn field(id: u32, scalar: ScalarType) -> rtdb_plan::Field {
        rtdb_plan::Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(scalar).maybe() }
    }
    fn arrow(array: ArrayRef, scalar: ScalarType) -> Column {
        Column::Arrow { field: Arc::new(ArrowField::new("c", array.data_type().clone(), true)), array, logical: Some(LogicalType::Scalar(scalar)) }
    }

    fn batch(random: &mut Random, rows: usize, memory: &Arc<MemoryBudget>) -> ColumnBatch {
        let null = |random: &mut Random| random.below(5) == 0;
        let ints: Int64Array = (0..rows).map(|_| (!null(random)).then(|| random.below(21) as i64 - 10)).collect();
        let floats: Float64Array = (0..rows).map(|_| (!null(random)).then(|| float_cell(random))).collect();
        let texts: StringArray = (0..rows).map(|_| (!null(random)).then(|| ["a", "b", "ab", "B", ""][random.below(5) as usize])).collect();
        let bools: BooleanArray = (0..rows).map(|_| (!null(random)).then(|| random.below(2) == 0)).collect();
        let values = (0..rows).map(|_| if null(random) { Value::Null } else { Value::I64(random.below(21) as i64 - 10) }).collect();
        ColumnBatch::settled(vec![arrow(Arc::new(ints), ScalarType::I64), arrow(Arc::new(floats), ScalarType::F64), arrow(Arc::new(texts), ScalarType::Utf8),
            arrow(Arc::new(bools), ScalarType::Bool), Column::Values(values)], rows, memory.reserve(0).unwrap()).unwrap()
    }

    fn operand(random: &mut Random, schema: &Schema) -> Expr {
        let numeric = [0usize, 1, 4][random.below(3) as usize];
        match random.below(3) {
            0 => schema.fields()[numeric].expression(),
            1 => Expr::Literal([Value::I64(random.below(21) as i64 - 10), Value::I16(random.below(21) as i16 - 10), Value::I32(random.below(21) as i32 - 10)][random.below(3) as usize].clone()),
            _ => Expr::Literal(Value::F64(float_cell(random))),
        }
    }
    fn predicate(random: &mut Random, schema: &Schema, depth: u32) -> Expr {
        let comparison = [BinaryOp::Eq, BinaryOp::NotEq, BinaryOp::Lt, BinaryOp::LtEq, BinaryOp::Gt, BinaryOp::GtEq];
        match if depth == 0 { random.below(5) } else { random.below(8) } {
            0 => Expr::binary(comparison[random.below(6) as usize], schema.fields()[[0, 1, 4][random.below(3) as usize]].expression(), operand(random, schema)),
            1 => Expr::binary([BinaryOp::Eq, BinaryOp::NotEq][random.below(2) as usize], schema.fields()[2].expression(), Expr::Literal(Value::Utf8(["a", "b", "ab", "zz"][random.below(4) as usize].into()))),
            2 => Expr::Between { operand: Box::new(schema.fields()[[0, 1, 4][random.below(3) as usize]].expression()), low: Box::new(operand(random, schema)), high: Box::new(operand(random, schema)), symmetric: false },
            3 => Expr::Unary { op: [UnaryOp::IsNull, UnaryOp::IsNotNull][random.below(2) as usize], expr: Box::new(schema.fields()[random.below(5) as usize].expression()) },
            4 => schema.fields()[3].expression(),
            5 => Expr::binary(BinaryOp::And, predicate(random, schema, depth - 1), predicate(random, schema, depth - 1)),
            6 => Expr::binary(BinaryOp::Or, predicate(random, schema, depth - 1), predicate(random, schema, depth - 1)),
            7 if random.below(2) == 0 => {
                let arithmetic = Expr::binary(BinaryOp::Multiply, schema.fields()[[0usize, 1, 4][random.below(3) as usize]].expression(), operand(random, schema));
                if random.below(2) == 0 { Expr::Call { name: "===".into(), args: vec![arithmetic, operand(random, schema)], return_type: LogicalType::Scalar(ScalarType::Bool) } }
                else { Expr::binary(comparison[random.below(6) as usize], arithmetic, operand(random, schema)) }
            }
            _ => Expr::Unary { op: UnaryOp::Not, expr: Box::new(predicate(random, schema, depth - 1)) },
        }
    }

    fn arithmetic(random: &mut Random, schema: &Schema, depth: u32) -> Expr {
        let ops = [BinaryOp::Add, BinaryOp::Subtract, BinaryOp::Multiply];
        match if depth == 0 { random.below(3) } else { random.below(5) } {
            0 => schema.fields()[[0usize, 1][random.below(2) as usize]].expression(),
            1 => Expr::Literal([Value::I64(random.below(21) as i64 - 10), Value::I32(random.below(9) as i32 - 4), Value::F64((random.below(41) as f64 - 20.0) / 4.0), Value::I64(i64::MAX - random.below(3) as i64)][random.below(4) as usize].clone()),
            2 => schema.fields()[[0usize, 1, 2][random.below(3) as usize]].expression(),
            _ => Expr::binary(ops[random.below(3) as usize], arithmetic(random, schema, depth - 1), arithmetic(random, schema, depth - 1)),
        }
    }

    #[test]
    fn the_projection_kernel_agrees_with_the_evaluator_on_every_batch_it_computes() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new(vec![field(0, ScalarType::I64), field(1, ScalarType::F64), field(2, ScalarType::Utf8), field(3, ScalarType::Bool), field(4, ScalarType::I64)]).unwrap();
        let mut random = Random(0x2545f4914f6cdd1d);
        let mut computed = 0;
        for _ in 0..400 {
            let batch = batch(&mut random, 64, &memory);
            let expression = if random.below(3) == 0 {
                Expr::Struct(vec![("a".into(), arithmetic(&mut random, &schema, 2)), ("t".into(), schema.fields()[2].expression()), ("b".into(), arithmetic(&mut random, &schema, 2))])
            } else { arithmetic(&mut random, &schema, 3) };
            let Some(column) = projection::column(&expression, &schema, &batch, &[]).unwrap() else { continue };
            computed += 1;
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: &[], parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None, corpora: None };
            for row in 0..batch.len {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                let expected = rtdb_expr::evaluate_projection(&expression, &schema, &values, &context).unwrap();
                assert_eq!(column.value(row).unwrap(), expected, "{expression:?} on {values:?}");
            }
        }
        assert!(computed > 150, "the kernel should compute most generated projections, computed {computed}");
    }

    fn float_cell(random: &mut Random) -> f64 {
        let value = (random.below(41) as f64 - 20.0) / 2.0;
        if value == 0.0 && random.below(2) == 0 { -0.0 } else { value }
    }

    #[test]
    fn the_column_kernel_orders_negative_zero_below_zero_but_equal_to_it() {
        use rtdb_plan::BinaryOp::{Eq, GtEq, Lt};
        let memory = MemoryBudget::new(1 << 24);
        let schema = Schema::new(vec![field(1, ScalarType::F64)]).unwrap();
        let batch = ColumnBatch::settled(vec![arrow(Arc::new(Float64Array::from(vec![-0.0, 0.0])), ScalarType::F64)], 2, memory.reserve(0).unwrap()).unwrap();
        let column = Expr::Column { id: rtdb_plan::ColumnId(1), data_type: LogicalType::Scalar(ScalarType::F64) };
        for (op, literal, expected) in [(Lt, Value::F64(0.0), [true, false]), (Lt, Value::I64(0), [true, false]), (GtEq, Value::F64(0.0), [false, true]), (Eq, Value::F64(0.0), [true, true])] {
            let predicate = Expr::binary(op, column.clone(), Expr::Literal(literal));
            assert_eq!(kernel::truth(&predicate, &schema, &batch, &[]), Some(expected.map(Some).to_vec()), "{predicate:?}");
        }
    }

    #[test]
    fn the_column_kernel_agrees_with_the_scalar_evaluator_on_every_row_it_evaluates() {
        let memory = MemoryBudget::new(1 << 28);
        let schema = Schema::new(vec![field(0, ScalarType::I64), field(1, ScalarType::F64), field(2, ScalarType::Utf8), field(3, ScalarType::Bool), field(4, ScalarType::I64)]).unwrap();
        let mut random = Random(0x9e3779b97f4a7c15);
        let mut evaluated = 0;
        for _ in 0..400 {
            let batch = batch(&mut random, 64, &memory);
            let predicate = predicate(&mut random, &schema, 3);
            let Some(truth) = kernel::truth(&predicate, &schema, &batch, &[]) else { continue };
            evaluated += 1;
            let scope = rtdb_expr::EvaluationScope::new(&memory).unwrap();
            let context = rtdb_expr::EvalContext { parameters: &[], parameter_types: None, current_time: rtdb_types::Instant::from(Timestamp(0)), timezone: "UTC",
                outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None, corpora: None };
            for (row, expected) in truth.iter().enumerate() {
                let values: Vec<Value> = batch.columns.iter().map(|column| column.value(row).unwrap()).collect();
                let actual = match rtdb_expr::evaluate(&predicate, &schema, &values, &context).unwrap() { Value::Bool(value) => Some(value), Value::Null => None, other => panic!("{other:?}") };
                assert_eq!(actual, *expected, "{predicate:?} on {values:?}");
            }
        }
        assert!(evaluated > 200, "the kernel should take most generated predicates, took {evaluated}");
    }
}

/// Reorders, selects or renames a batch's columns, for a projection whose expressions are all input columns.
pub(crate) struct BatchProject { input: BoxBatchCursor, positions: Vec<usize>, memory: Arc<MemoryBudget> }
impl BatchProject { pub fn new(input: BoxBatchCursor, positions: Vec<usize>, memory: Arc<MemoryBudget>) -> Self { Self { input, positions, memory } } }
#[async_trait::async_trait]
impl BatchCursor for BatchProject {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let Some(batch) = self.input.next_batch().await? else { return Ok(None) };
        let len = batch.len;
        let bound = self.positions.iter().try_fold(256usize, |bound, position| bound.checked_add(batch.columns[*position].retained_bytes()?).ok_or_else(size_error))?;
        let reservation = self.memory.reserve(bound)?;
        let mut columns: Vec<Option<Column>> = batch.columns.into_iter().map(Some).collect();
        let mut uses = vec![0usize; columns.len()];
        for &position in &self.positions { uses[position] += 1; }
        let mut output = Vec::with_capacity(self.positions.len());
        for &position in &self.positions {
            uses[position] -= 1;
            output.push(if uses[position] == 0 { columns[position].take().ok_or_else(|| invalid("column-project", "Projected column was already moved"))? }
                else { columns[position].as_ref().map(Column::duplicate).ok_or_else(|| invalid("column-project", "Projected column is absent"))? });
        }
        ColumnBatch::settled(output, len, reservation).map(Some)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}
impl Column {
    fn duplicate(&self) -> Self {
        match self { Self::Arrow { array, field, logical } => Self::Arrow { array: Arc::clone(array), field: Arc::clone(field), logical: logical.clone() }, Self::Values(values) => Self::Values(values.clone()) }
    }
}

/// Projects batches through expressions computed over whole columns where the kernel's arithmetic is exactly the
/// evaluator's, and row by row through the evaluator for any expression or batch where it isn't.
pub(crate) struct BatchExpressions { input: BoxBatchCursor, schema: rtdb_plan::Schema, expressions: Vec<rtdb_plan::Expr>, execution: crate::context::Execution }
impl BatchExpressions {
    pub fn new(input: BoxBatchCursor, schema: rtdb_plan::Schema, expressions: Vec<rtdb_plan::Expr>, execution: crate::context::Execution) -> Self { Self { input, schema, expressions, execution } }
}
#[async_trait::async_trait]
impl BatchCursor for BatchExpressions {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let Some(batch) = self.input.next_batch().await? else { return Ok(None) };
        self.execution.context.check_cancelled()?;
        let parameters = self.execution.parameters.values();
        let mut page = None;
        let mut columns = Vec::with_capacity(self.expressions.len());
        let mut reservation = self.execution.context.memory.reserve(256)?;
        for expression in &self.expressions {
            if let Some(bound) = projection::bound(expression, &self.schema, &batch)? {
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
                lease.grow_to(lease.bytes().checked_add(bound).ok_or_else(size_error)?)?;
                if let Some(column) = projection::column(expression, &self.schema, &batch, parameters)? { columns.push(column); continue; }
            }
            if let Some(column) = crate::vector::column(expression, &self.schema, &batch, parameters) {
                // A vectorised column's size is known once computed; it is charged before the next is built.
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
                continue;
            }
            if page.is_none() { page = Some(batch.rows(&self.execution.context.memory)?); }
            let rows = page.as_ref().expect("rows were materialised for the evaluator");
            let metadata = self.execution.scope()?;
            let invocation = self.execution.prepare_expression(expression, &self.schema, &metadata)?;
            let scope = self.execution.scope()?;
            let mut values = Vec::with_capacity(batch.len);
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("column-size", "A column page's charge is shared"))?;
            lease.grow_to(lease.bytes().checked_add(batch.len.checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?).ok_or_else(size_error)?)?;
            for row in rows.rows() {
                let value = self.execution.evaluate_invocation(&scope, &invocation, &row.values).await?;
                // The value is charged before the evaluator's scope releases what it allocated it under.
                lease.grow_to(lease.bytes().checked_add(value.retained_bytes()?).ok_or_else(size_error)?)?;
                values.push(value);
                scope.reset()?;
            }
            columns.push(Column::Values(values));
        }
        ColumnBatch::settled(columns, batch.len, reservation).map(Some)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

/// Column-at-a-time projection for input columns, scalar constants, `+ - *` over 64-bit integers and floats, and
/// records of those; `Ok(None)` where the evaluator must compute it, including any integer overflow, so its error is raised.
pub(crate) mod projection {
    use std::sync::Arc;
    use arrow_array::{Array, ArrayRef, Float64Array, Int64Array};
    use arrow_schema::{DataType, Field as ArrowField};
    use rtdb_plan::{BinaryOp, Expr, Schema};
    use rtdb_types::{LogicalType, Result, ScalarType, StructFields, Value};
    use super::{Column, ColumnBatch};

    #[derive(Clone, Copy)]
    enum Number { Int(i64), Float(f64) }
    enum Numbers<'a> { Int(&'a Int64Array), Float(&'a Float64Array), Constant(Option<Number>) }
    impl Numbers<'_> {
        fn get(&self, row: usize) -> Option<Number> {
            match self {
                Self::Int(array) => array.is_valid(row).then(|| Number::Int(array.value(row))),
                Self::Float(array) => array.is_valid(row).then(|| Number::Float(array.value(row))),
                Self::Constant(number) => *number,
            }
        }
        fn floating(&self) -> Option<bool> { match self { Self::Int(_) => Some(false), Self::Float(_) => Some(true), Self::Constant(Some(Number::Float(_))) => Some(true), Self::Constant(Some(Number::Int(_))) => Some(false), Self::Constant(None) => None } }
    }
    fn constant(value: &Value) -> Option<Option<Number>> {
        match value {
            Value::Null => Some(None),
            Value::I8(value) => Some(Some(Number::Int(i64::from(*value)))), Value::I16(value) => Some(Some(Number::Int(i64::from(*value)))),
            Value::I32(value) => Some(Some(Number::Int(i64::from(*value)))), Value::I64(value) => Some(Some(Number::Int(*value))),
            Value::F64(value) => Some(Some(Number::Float(*value))),
            _ => None,
        }
    }
    fn numbers<'a>(expr: &Expr, schema: &Schema, batch: &'a ColumnBatch, computed: &'a mut Option<Column>, parameters: &[Value]) -> Result<Option<Numbers<'a>>> {
        let plain = |logical: &Option<LogicalType>, field: &ArrowField, expected: ScalarType| match logical { Some(LogicalType::Scalar(scalar)) => *scalar == expected, Some(_) => false, None => field.metadata().is_empty() };
        let array_numbers = |column: &'a Column| -> Option<Numbers<'a>> {
            let Column::Arrow { array, field, logical } = column else { return None };
            match array.data_type() {
                DataType::Int64 if plain(logical, field, ScalarType::I64) => array.as_any().downcast_ref().map(Numbers::Int),
                DataType::Float64 if plain(logical, field, ScalarType::F64) => array.as_any().downcast_ref().map(Numbers::Float),
                _ => None,
            }
        };
        Ok(match expr {
            Expr::Column { id, .. } => schema.position(*id).and_then(|position| array_numbers(&batch.columns[position])),
            Expr::Literal(value) => constant(value).map(Numbers::Constant),
            Expr::Parameter { index, .. } => parameters.get(*index).and_then(constant).map(Numbers::Constant),
            Expr::Binary { .. } => {
                *computed = arithmetic(expr, schema, batch, parameters)?;
                computed.as_ref().and_then(array_numbers)
            }
            _ => None,
        })
    }
    fn arithmetic(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: &[Value]) -> Result<Option<Column>> {
        let Expr::Binary { op: op @ (BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply), left, right } = expr else { return Ok(None) };
        let (mut left_computed, mut right_computed) = (None, None);
        let Some(left) = numbers(left, schema, batch, &mut left_computed, parameters)? else { return Ok(None) };
        let Some(right) = numbers(right, schema, batch, &mut right_computed, parameters)? else { return Ok(None) };
        if matches!(left, Numbers::Constant(_)) && matches!(right, Numbers::Constant(_)) { return Ok(None); }
        let floating = match (left.floating(), right.floating()) { (Some(a), Some(b)) => a || b, (Some(a), None) | (None, Some(a)) => a, (None, None) => return Ok(None) };
        let float = |number: Number| match number { Number::Int(value) => value as f64, Number::Float(value) => value };
        let (array, scalar): (ArrayRef, ScalarType) = if floating {
            let values: Float64Array = (0..batch.len).map(|row| match (left.get(row), right.get(row)) {
                (Some(a), Some(b)) => Some(match op { BinaryOp::Add => float(a) + float(b), BinaryOp::Subtract => float(a) - float(b), _ => float(a) * float(b) }),
                _ => None,
            }).collect();
            (Arc::new(values), ScalarType::F64)
        } else {
            let mut values = Vec::with_capacity(batch.len);
            for row in 0..batch.len {
                values.push(match (left.get(row), right.get(row)) {
                    (Some(Number::Int(a)), Some(Number::Int(b))) => match match op { BinaryOp::Add => a.checked_add(b), BinaryOp::Subtract => a.checked_sub(b), _ => a.checked_mul(b) } {
                        Some(value) => Some(value),
                        None => return Ok(None),
                    },
                    _ => None,
                });
            }
            (Arc::new(Int64Array::from(values)), ScalarType::I64)
        };
        let field = Arc::new(ArrowField::new("value", array.data_type().clone(), true));
        Ok(Some(Column::Arrow { array, field, logical: Some(LogicalType::Scalar(scalar)) }))
    }

    /// Whether an expression has a shape the kernel may compute; its cells' types decide per batch.
    pub(crate) fn shaped(expr: &Expr) -> bool {
        match expr {
            Expr::Column { .. } | Expr::Parameter { .. } => true,
            Expr::Literal(value) => constant(value).is_some() || matches!(value, Value::Utf8(_) | Value::Bool(_)),
            Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, left, right } => shaped(left) && shaped(right),
            Expr::Struct(fields) => fields.iter().all(|(_, field)| shaped(field)),
            expr => crate::vector::lowerable(expr),
        }
    }
    /// Bounds what `column` allocates for `expr`, or `None` where it would not compute it.
    pub(crate) fn bound(expr: &Expr, schema: &Schema, batch: &ColumnBatch) -> Result<Option<usize>> {
        let rows = batch.len;
        let per_row = |bytes: usize| rows.checked_mul(bytes).and_then(|total| total.checked_add(256)).ok_or_else(super::size_error);
        Ok(Some(match expr {
            Expr::Column { id, .. } => match schema.position(*id) { Some(position) => batch.columns[position].retained_bytes()?, None => return Ok(None) },
            Expr::Literal(value) if constant(value).is_some() || matches!(value, Value::Utf8(_) | Value::Bool(_)) => per_row(std::mem::size_of::<Value>().checked_add(value.retained_bytes()?).ok_or_else(super::size_error)?)?,
            Expr::Parameter { .. } => 0,
            // The checked path builds `Option<i64>` cells before the array, and each operand may be computed too.
            Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, left, right } => {
                let (Some(left), Some(right)) = (bound(left, schema, batch)?, bound(right, schema, batch)?) else { return Ok(None) };
                per_row(32)?.checked_add(left).and_then(|bytes| bytes.checked_add(right)).ok_or_else(super::size_error)?
            }
            Expr::Struct(fields) => {
                let mut total = per_row(std::mem::size_of::<Value>().checked_add(256).ok_or_else(super::size_error)?)?;
                for (name, field) in fields {
                    let Some(child) = bound(field, schema, batch)? else { return Ok(None) };
                    let cells = match field {
                        Expr::Column { id, .. } => match schema.position(*id) { Some(position) => (0..rows).try_fold(0usize, |sum, row| sum.checked_add(batch.columns[position].bound(row)?).ok_or_else(super::size_error))?, None => return Ok(None) },
                        _ => child,
                    };
                    total = per_row(name.len().checked_add(std::mem::size_of::<Value>() + std::mem::size_of::<String>() + 64).ok_or_else(super::size_error)?)?
                        .checked_add(child).and_then(|bytes| bytes.checked_add(cells)).and_then(|bytes| bytes.checked_add(total)).ok_or_else(super::size_error)?;
                }
                total
            }
            _ => return Ok(None),
        }))
    }
    pub(crate) fn column(expr: &Expr, schema: &Schema, batch: &ColumnBatch, parameters: &[Value]) -> Result<Option<Column>> {
        match expr {
            Expr::Column { id, .. } => {
                let Some(position) = schema.position(*id) else { return Ok(None) };
                Ok(Some(match &batch.columns[position] {
                    column @ Column::Arrow { .. } => column.duplicate(),
                    Column::Values(values) => Column::Values(values.iter().map(|value| { let mut value = value.clone(); value.admit_expression_dates()?; Ok(value) }).collect::<Result<_>>()?),
                }))
            }
            Expr::Literal(value) if constant(value).is_some() || matches!(value, Value::Utf8(_) | Value::Bool(_)) => Ok(Some(Column::Values(vec![value.clone(); batch.len]))),
            Expr::Binary { .. } => arithmetic(expr, schema, batch, parameters),
            Expr::Struct(fields) => {
                let mut columns = Vec::with_capacity(fields.len());
                for (_, field) in fields {
                    let Some(column) = column(field, schema, batch, parameters)? else { return Ok(None) };
                    columns.push(column);
                }
                let mut values = Vec::with_capacity(batch.len);
                for row in 0..batch.len {
                    let mut document = StructFields::new();
                    for ((name, _), column) in fields.iter().zip(&columns) {
                        let mut value = column.value(row)?;
                        value.admit_box_date()?;
                        document.insert(name.clone(), value);
                    }
                    let mut value = Value::Struct(document);
                    value.admit_expression_dates()?;
                    rtdb_expr::validate_projection_value(&value)?;
                    values.push(value);
                }
                Ok(Some(Column::Values(values)))
            }
            _ => Ok(None),
        }
    }
}
