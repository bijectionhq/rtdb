use std::{collections::VecDeque, ops::Range, sync::Arc};
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{Binder, ColumnId, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{LogicalType, Result, ScalarType};
use crate::{context::Execution, invalid};
use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_nullable_column, ordinal_column_by, pack_evaluated_column, poll_nested_batch};
use crate::columnar::window::{WindowSpan, lend_window_spans};
use crate::{scalar_key::ScalarKey, set_keys::{BatchKeyIndex, KeyColumns}};
use crate::sort::batches::BatchSort;

mod order;
pub(crate) use order::InputOrder;
mod top_k;
pub(crate) use top_k::{RowNumberLimit, TopKWindow};

pub(crate) struct BatchWindow {
    input: Option<BoxBatchCursor>,
    schema: Schema,
    partition_column: Option<usize>,
    expressions: Vec<WindowExpr>,
    ready: Option<(VecDeque<ColumnBatch>, Arc<MemoryReservation>)>,
    ordered_input: Option<(InputOrder, WindowSort)>,
    reused_input_order: Option<bool>,
    execution: Execution,
}

impl BatchWindow {
    pub fn supports(schema: &Schema, partition_by: &[ColumnId], order_by: &[SortExpr], _expressions: &[WindowExpr]) -> bool {
        partition_by.iter().all(|column| schema.position(*column).is_some())
            && (order_by.is_empty() || BatchSort::supports(schema, order_by))
    }

    pub fn new(input: BoxBatchCursor, schema: Schema, partition_by: &[ColumnId], order_by: Vec<SortExpr>, expressions: &[WindowExpr],
        projected: bool, execution: Execution) -> Result<Self> {
        Self::with_input_order(input, schema, partition_by, order_by, expressions, projected, execution, None)
    }

    pub fn with_input_order(
        input: BoxBatchCursor, schema: Schema, partition_by: &[ColumnId], mut order_by: Vec<SortExpr>, expressions: &[WindowExpr],
        projected: bool, execution: Execution, input_order: Option<InputOrder>,
    ) -> Result<Self> {
        let mut input: BoxBatchCursor = Box::new(WindowInput { input, context: Arc::clone(&execution.context) });
        let mut sort_schema = schema.clone();
        let partition_column = if partition_by.is_empty() { None } else {
            let logical = LogicalType::Scalar(ScalarType::I64);
            let mut binder = Binder::after(&schema)?;
            let field = binder.field("", None, logical.clone())?;
            order_by.insert(0, SortExpr { expr: field.expression(), descending: false, nulls_first: false });
            let arrow = Arc::new(rtdb_columnar::to_field(&field.name, &logical)?);
            let context = Arc::clone(&execution.context);
            let partitions = WindowPartitions::new(&schema, partition_by, &context)?;
            input = Box::new(PartitionBatches { input, context, partitions, arrow, logical, projected });
            sort_schema = schema.concat(&Schema::new(vec![field])?)?;
            Some(schema.len())
        };
        let sort = WindowSort { schema: sort_schema, order_by, projected };
        let (input, ordered_input) = match input_order {
            Some(proof) => (input, Some((proof, sort))),
            None => (sort.open(input, &execution)?, None),
        };
        let reused_input_order = crate::trace::operator_evidence_enabled(&execution.context).then_some(false);
        Ok(Self { input: Some(input), schema, partition_column, expressions: expressions.to_vec(), ready: None,
            ordered_input, reused_input_order, execution })
    }

    async fn evaluate(&mut self, mut input: BoxBatchCursor) -> Result<(VecDeque<ColumnBatch>, Arc<MemoryReservation>)> {
        use std::mem::size_of;
        let context = &self.execution.context;
        let memory = &context.memory;
        let mut batches = Vec::new();
        let mut offsets = Vec::new();
        let mut total = 0usize;
        let mut inventory = memory.reserve(256)?;
        let mut ordered_input = self.ordered_input.take();
        let mut short_page = false;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            if batch.len == 0 { continue; }
            let entry = 3 * size_of::<ColumnBatch>() + 2 * size_of::<usize>();
            let storage = batches.len().checked_add(1usize).and_then(|count| count.checked_mul(entry))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(window_size)?;
            let mut admit = || Arc::get_mut(&mut inventory).expect("Window owns the batch inventory reservation").grow_to(storage);
            if let Some((proof, _)) = &ordered_input {
                let page_rows = context.options.batch_rows;
                // A short page can be the tail; another page would change the ordinary sort's pull boundaries.
                if short_page || batch.len > page_rows || !proof.admits(&batch) || admit().is_err() {
                    let (_, sort) = ordered_input.take().expect("Window has an upstream order candidate");
                    let replay = ReplayWindowInput { prefix: batches.into_iter(), pending: Some(batch), tail: input,
                        inventory: Some(std::mem::replace(&mut inventory, memory.reserve(256)?)) };
                    input = sort.open(Box::new(replay), &self.execution)?;
                    batches = Vec::new();
                    offsets = Vec::new();
                    total = 0;
                    continue;
                }
                short_page = batch.len < page_rows;
            } else {
                admit()?;
            }
            offsets.push(total);
            total = total.checked_add(batch.len).filter(|total| i64::try_from(*total).is_ok())
                .ok_or_else(|| invalid("window-count", "Window row number overflow"))?;
            batches.push(batch);
            if ordered_input.is_some() { tokio::task::consume_budget().await; }
        }
        drop(input);
        let reused_order = ordered_input.take().is_some() && total != 0;
        if let Some(reused) = &mut self.reused_input_order { *reused = reused_order; }

        let mut partitions = Vec::new();
        let mut partition_storage = memory.reserve(0)?;
        if let Some(position) = self.partition_column {
            let mut previous = None;
            for (batch, &start) in batches.iter_mut().zip(&offsets) {
                let (column, _owner) = batch.remove_window_partition(position);
                for row in 0..batch.len {
                    context.check_cancelled_at(row)?;
                    let Some(ScalarKey::Signed(group)) = ScalarKey::read(&column, row) else {
                        return Err(invalid("window-partition", "Window batch lacks its partition identifier"));
                    };
                    if previous == Some(group) { continue; }
                    let bound = partitions.len().checked_add(1).and_then(|len| len.checked_mul(4 * size_of::<usize>()))
                        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(window_size)?;
                    Arc::get_mut(&mut partition_storage).expect("Window owns the partition boundaries").grow_to(bound)?;
                    partitions.push(start + row);
                    previous = Some(group);
                }
            }
        }
        let group = |index: usize| {
            let next = partitions.partition_point(|&start| start <= index);
            let lower = if next == 0 { 0 } else { partitions[next - 1] };
            (lower, partitions.get(next).copied().unwrap_or(total))
        };

        for expression in &self.expressions {
            let _suffixes = memory.reserve(batches.len().checked_mul(size_of::<(crate::columnar::Column, Arc<rtdb_columnar::MemoryReservation>)>())
                .ok_or_else(window_size)?)?;
            let mut suffixes = Vec::with_capacity(batches.len());
            for (batch, &start) in batches.iter().zip(&offsets) {
                let suffix = match expression.function {
                    WindowFunction::RowNumber => ordinal_column_by(batch.len, &expression.field.name, context, |row| {
                        let index = start + row;
                        (index - group(index).0) as i64 + 1
                    })?,
                    WindowFunction::Lead { column, offset } | WindowFunction::Lag { column, offset } => {
                        let position = self.schema.position(column).ok_or_else(|| invalid("window-column", "Window value column is absent"))?;
                        let single_source = shifted_span(expression.function, start, batch.len, offset, group).and_then(|span| {
                            let source = offsets.partition_point(|&start| start <= span.start) - 1;
                            let base = offsets[source];
                            (span.end - base <= batches[source].len)
                                .then(|| (&batches[source], span.start - base..span.end - base))
                        });
                        if let Some((source, rows)) = single_source {
                            for row in 0..batch.len {
                                context.check_cancelled_at(row)?;
                            }
                            let suffix = if let Column::Values(values) = &source.columns[position] {
                                values.get(rows).map(|values| pack_evaluated_column(values, memory)).transpose()?
                            } else {
                                source.lend_window_span(position, rows, memory)?
                            };
                            if let Some(suffix) = suffix {
                                suffixes.push(suffix);
                                continue;
                            }
                        } else if shifted_index(expression.function, (start + batch.len - 1) as u64, offset).is_ok() {
                            let spans = shifted_runs(expression.function, start..start + batch.len, offset, group, &batches, &offsets);
                            if let Some(suffix) = lend_window_spans(spans, position, batch.len, context)? {
                                suffixes.push(suffix);
                                continue;
                            }
                        }
                        let _picks = memory.reserve(batch.len.checked_mul(size_of::<Option<(usize, usize)>>()).ok_or_else(window_size)?)?;
                        let mut cells = Vec::with_capacity(batch.len);
                        for row in 0..batch.len {
                            context.check_cancelled_at(row)?;
                            let index = start + row;
                            let (lower, upper) = group(index);
                            let target = shifted_index(expression.function, index as u64, offset)?;
                            let cell = target.and_then(|target| usize::try_from(target).ok()).filter(|&target| lower <= target && target < upper).map(|target| {
                                let batch = offsets.partition_point(|&start| start <= target) - 1;
                                (batch, target - offsets[batch])
                            });
                            cells.push(cell);
                        }
                        gather_nullable_column(|index| &batches[index], &cells, position, memory)?
                    }
                };
                suffixes.push(suffix);
            }
            for (batch, (suffix, reservation)) in batches.iter_mut().zip(suffixes) {
                batch.append_column(suffix, reservation, memory)?;
            }
        }
        Ok((batches.into(), inventory))
    }
}

struct WindowSort {
    schema: Schema,
    order_by: Vec<SortExpr>,
    projected: bool,
}

impl WindowSort {
    fn open(self, input: BoxBatchCursor, execution: &Execution) -> Result<BoxBatchCursor> {
        if self.order_by.is_empty() { return Ok(input); }
        Ok(Box::new(BatchSort::new(input, self.schema, self.order_by, execution.clone(), self.projected, 0, i64::MAX as u64)?))
    }
}

struct ReplayWindowInput {
    prefix: std::vec::IntoIter<ColumnBatch>,
    pending: Option<ColumnBatch>,
    tail: BoxBatchCursor,
    inventory: Option<Arc<MemoryReservation>>,
}

#[async_trait]
impl BatchCursor for ReplayWindowInput {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        if let Some(batch) = self.prefix.next() { return Ok(Some(batch)); }
        if self.inventory.is_some() {
            self.prefix = std::vec::IntoIter::default();
            self.inventory = None;
        }
        if let Some(batch) = self.pending.take() { return Ok(Some(batch)); }
        poll_nested_batch(self.tail.as_mut()).await
    }
}

struct PartitionBatches {
    input: BoxBatchCursor,
    context: Arc<crate::QueryContext>,
    partitions: WindowPartitions,
    arrow: Arc<arrow_schema::Field>,
    logical: LogicalType,
    projected: bool,
}

struct WindowPartitions {
    positions: Vec<usize>,
    keys: Schema,
    index: BatchKeyIndex,
}

impl WindowPartitions {
    fn new(schema: &Schema, partition_by: &[ColumnId], context: &crate::QueryContext) -> Result<Self> {
        let positions = partition_by.iter().map(|&column| schema.position(column).ok_or_else(|| invalid("window-column", "Window partition column is absent")))
            .collect::<Result<Vec<_>>>()?;
        let keys = Schema::new(positions.iter().enumerate().map(|(index, &position)| {
            let mut field = schema.fields()[position].clone();
            field.id = ColumnId(index as u32);
            field
        }).collect())?;
        Ok(Self { positions, keys, index: BatchKeyIndex::vector_hashing(context)? })
    }

    fn validate(&self) -> Result<()> {
        crate::operators::validate_distinct_types(&self.keys)
    }

    fn identify(&mut self, batch: &ColumnBatch, projected: bool, context: &crate::QueryContext,
        visit: impl FnMut(usize, usize, bool) -> Result<()>,
    ) -> Result<()> {
        self.index.identify(KeyColumns::projection(batch, &self.positions), projected, context, &self.keys, visit)
    }
}

#[async_trait]
impl BatchCursor for PartitionBatches {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let memory = &self.context.memory;
        while let Some(mut batch) = poll_nested_batch(self.input.as_mut()).await? {
            self.partitions.validate()?;
            if batch.len == 0 { continue; }
            let bytes = batch.len.checked_mul(std::mem::size_of::<i64>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(window_size)?;
            let reservation = memory.reserve(bytes)?;
            let mut groups = Vec::with_capacity(batch.len);
            self.partitions.identify(&batch, self.projected, &self.context, |_, group, _| {
                groups.push(i64::try_from(group).map_err(|_| invalid("window-count", "Window partition count overflow"))?);
                Ok(())
            })?;
            let array = Arc::new(arrow_array::Int64Array::from(groups));
            let group = Column::Arrow { array, field: Arc::clone(&self.arrow), logical: Some(self.logical.clone()) };
            batch.append_column(group, reservation, memory)?;
            return Ok(Some(batch));
        }
        Ok(None)
    }
}

struct WindowInput { input: BoxBatchCursor, context: Arc<crate::QueryContext> }

#[async_trait]
impl BatchCursor for WindowInput {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let batch = poll_nested_batch(self.input.as_mut()).await?;
        if let Some(batch) = &batch { batch.validate_readable(&self.context)?; }
        Ok(batch)
    }
}

fn window_size() -> rtdb_types::Anomaly { invalid("window-size", "Window batch inventory exceeds addressable memory") }

fn shifted_span(function: WindowFunction, start: usize, len: usize, offset: u64,
    group: impl Fn(usize) -> (usize, usize),
) -> Option<Range<usize>> {
    if offset == 0 { return Some(start..start + len); }
    let last = start + len - 1;
    let (lower, upper) = group(start);
    if last >= upper { return None; }
    // The row-wise path reports offset overflow in cancellation order.
    let first_target = usize::try_from(shifted_index(function, start as u64, offset).ok().flatten()?).ok()?;
    let last_target = usize::try_from(shifted_index(function, last as u64, offset).ok().flatten()?).ok()?;
    (lower <= first_target && last_target < upper).then(|| first_target..last_target + 1)
}

fn shifted_runs<'a>(
    function: WindowFunction,
    output: Range<usize>,
    offset: u64,
    group: impl Fn(usize) -> (usize, usize) + Clone + 'a,
    batches: &'a [ColumnBatch],
    offsets: &'a [usize],
) -> impl Iterator<Item = WindowSpan<'a>> + Clone + 'a {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    let output_end = output.end;
    let groups = std::iter::successors(Some((output.start, group(output.start))), move |&(_, (_, upper))| {
        (upper < output_end).then(|| (upper, group(upper)))
    });
    groups.flat_map(move |(start, (lower, upper))| {
        let end = upper.min(output_end);
        let (targets, prefix, suffix) = if matches!(function, WindowFunction::Lead { .. }) {
            let present_end = end.min(upper.saturating_sub(offset)).max(start);
            let targets = if start < present_end { start + offset..present_end + offset } else { 0..0 };
            (targets, 0, end - present_end)
        } else {
            let present_start = start.max(lower.saturating_add(offset)).min(end);
            let targets = if present_start < end { present_start - offset..end - offset } else { 0..0 };
            (targets, present_start - start, 0)
        };
        let first = (!targets.is_empty()).then(|| offsets.partition_point(|&base| base <= targets.start) - 1);
        let sources = std::iter::successors(first, move |&page| {
            offsets.get(page + 1).filter(|&&next| next < targets.end).map(|_| page + 1)
        }).map(move |page| {
            let base = offsets[page];
            let batch = &batches[page];
            WindowSpan::Source(batch, targets.start.saturating_sub(base)..(targets.end - base).min(batch.len))
        });
        (prefix > 0).then_some(WindowSpan::Null(prefix)).into_iter()
            .chain(sources)
            .chain((suffix > 0).then_some(WindowSpan::Null(suffix)))
    })
}

fn shifted_index(function: WindowFunction, index: u64, offset: u64) -> Result<Option<u64>> {
    if matches!(function, WindowFunction::Lead { .. }) {
        index.checked_add(offset).filter(|&index| index <= i64::MAX as u64).map(Some)
            .ok_or_else(|| invalid("window-offset", "Window row offset overflow"))
    } else { Ok(index.checked_sub(offset)) }
}

#[async_trait]
impl BatchCursor for BatchWindow {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.execution.context.check_cancelled()?;
        if let Some(input) = self.input.take() {
            self.ready = Some(self.evaluate(input).await?);
        }
        let batch = self.ready.as_mut().and_then(|(ready, _)| ready.pop_front());
        if batch.is_none() { self.ready = None; }
        Ok(batch)
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        if let Some(reused) = self.reused_input_order {
            crate::trace::OperatorCounter::WindowInputOrderReused.record(span, u64::from(reused));
        }
    }
}

#[cfg(test)]
mod page_tests;
