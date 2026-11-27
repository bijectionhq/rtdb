//! Stable sorting of batch positions, with Arrow spill runs.
use std::{mem::size_of, sync::Arc};

use async_trait::async_trait;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_plan::{Expr, Schema, SortExpr};
use rtdb_types::{LogicalType, ObjectSort, Result, ScalarType, Value};

use crate::{
    columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch},
    context::Execution,
    invalid,
    spill::{SpillFile, SpillReader, SpillWriter},
};

use super::{
    BLOCK_ROWS, KeyTypes, MERGE_FAN_IN, SortPlan, legacy, native_keys::{self, ByteKey, BytePrefix, NativeOrder}, orders_consistently, select,
    size_error, timsort,
};

pub(crate) struct BatchSort {
    plan: SortPlan,
    key_columns: Vec<usize>,
    native: Option<NativeOrder>,
    used_native: bool,
    projection: Vec<usize>,
    projected: bool,
    offset: u64,
    input: Option<BoxBatchCursor>,
    output: Option<Sorted>,
    spilled: bool,
    semantic_page_rows: Option<usize>,
    spill_pending: Option<(Arc<ColumnBatch>, usize)>,
    /// Sorts one block in memory however large, never spilling: the in-memory sort of an operator XTDB never spills.
    in_memory: bool,
}

impl BatchSort {
    pub fn supports(schema: &Schema, order: &[SortExpr]) -> bool {
        !order.is_empty() && order.iter().all(|key| {
            matches!(&key.expr, Expr::Column { id, .. } if schema.position(*id).is_some())
        })
    }

    pub fn new(
        input: BoxBatchCursor,
        schema: Schema,
        order: Vec<SortExpr>,
        execution: Execution,
        projected: bool,
        offset: u64,
        limit: u64,
    ) -> Result<Self> {
        let key_columns = order
            .iter()
            .map(|key| match &key.expr {
                Expr::Column { id, .. } => schema.position(*id),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| invalid("sort-state", "Expected sort key columns"))?;
        let projection = (0..schema.len()).collect();
        let native = NativeOrder::bind(&schema, &key_columns);
        Ok(Self {
            plan: SortPlan {
                schema,
                order,
                limit,
                execution,
            },
            key_columns,
            native,
            used_native: false,
            projection,
            projected,
            offset,
            input: Some(input),
            output: None,
            spilled: false,
            semantic_page_rows: None,
            spill_pending: None,
            in_memory: false,
        })
    }

    pub fn preserving_pages(mut self) -> Self {
        self.semantic_page_rows = Some(0);
        self
    }

    /// Holds every row in one block and sorts it there, as XTDB sorts the rows of an ordered `ARRAY_AGG` in memory; a
    /// budget that cannot hold them fails the query.
    pub fn in_memory(mut self) -> Self {
        self.in_memory = true;
        self
    }

    /// Whether the block's comparator lets a limited sort drop rows outside its first `limit` before the input ends.
    fn orders_consistently(&self, block: &Block, key_types: &KeyTypes) -> bool {
        if block.native {
            self.native.as_ref().is_some_and(NativeOrder::is_total)
        } else {
            orders_consistently(&key_types.values)
        }
    }

    async fn sort(&mut self, mut input: BoxBatchCursor) -> Result<Sorted> {
        let context = &self.plan.execution.context;
        let mut block = Block::new(&context.memory, self.native.is_some())?;
        let mut runs = Runs::new(&context.memory)?;
        let mut key_types = KeyTypes::declared(&self.plan)?;
        let mut logical_runs = 0;
        let mut logical_run_rows = 0;
        let mut total_rows = 0usize;
        let limit = usize::try_from(self.plan.limit).unwrap_or(usize::MAX);
        // A small limit keeps only its candidates between batches, so the block stays bounded whatever the input size.
        let retained_rows = (limit != 0 && limit <= context.options.top_k_rows)
            .then(|| limit.saturating_mul(2).max(context.options.batch_rows));
        while let Some(batch) = input.next_batch().await? {
            context.check_cancelled()?;
            if batch.len == 0 {
                continue;
            }
            if self.semantic_page_rows.is_some() {
                total_rows = total_rows.checked_add(batch.len).ok_or_else(size_error)?;
                logical_run_rows += batch.len;
                if logical_run_rows > BLOCK_ROWS {
                    logical_runs += 1;
                    logical_run_rows = 0;
                }
            }
            let payload_bytes = batch.working_set_bytes(&context.memory)?;
            let rows = block.positions.len().saturating_add(batch.len);
            if !self.in_memory && !block.positions.is_empty()
                && (block.charged().saturating_add(payload_bytes).saturating_add(ordering_bytes(rows))
                    > context.options.sort_memory_bytes
                    || (retained_rows.is_none() || !self.orders_consistently(&block, &key_types))
                        && rows > BLOCK_ROWS)
            {
                key_types.validate()?;
                let full = std::mem::replace(
                    &mut block,
                    Block::new(&context.memory, self.native.is_some())?,
                );
                self.used_native |= full.native;
                runs.push(
                    Self::spill(
                        &self.plan,
                        &self.projection,
                        full.order(&self.plan, &self.key_columns, self.native.as_ref(), self.projected)?,
                    )
                    .await?,
                )?;
            }
            block.append(
                batch,
                payload_bytes,
                &self.key_columns,
                self.native.as_ref(),
                self.projected,
                &mut key_types,
                &self.plan,
            )?;
            if let Some(retained_rows) = retained_rows
                && block.positions.len() >= retained_rows
                && self.orders_consistently(&block, &key_types)
            {
                key_types.validate()?;
                let full = std::mem::replace(
                    &mut block,
                    Block::new(&context.memory, self.native.is_some())?,
                );
                self.used_native |= full.native;
                let mut candidates = full.order(&self.plan, &self.key_columns, self.native.as_ref(), self.projected)?;
                while let Some(rows) = candidates.next(&self.plan, &self.projection, limit)? {
                    let payload_bytes = rows.working_set_bytes(&context.memory)?;
                    block.append(
                        rows,
                        payload_bytes,
                        &self.key_columns,
                        self.native.as_ref(),
                        self.projected,
                        &mut key_types,
                        &self.plan,
                    )?;
                }
            }
            tokio::task::consume_budget().await;
        }
        drop(input);
        if let Some(page_rows) = &mut self.semantic_page_rows {
            logical_runs += usize::from(logical_run_rows != 0);
            *page_rows = if logical_runs > 1 { BLOCK_ROWS + 1 } else { total_rows };
        }
        if !block.positions.is_empty() || !runs.files.is_empty() {
            key_types.validate()?;
        }
        self.used_native |= block.native && !block.positions.is_empty();
        let spill_tail = !self.in_memory && !block.positions.is_empty() && block.charged() > context.options.sort_memory_bytes;
        let tail = block.order(&self.plan, &self.key_columns, self.native.as_ref(), self.projected)?;
        if runs.files.is_empty() && !spill_tail {
            return Ok(Sorted::Memory(tail));
        }
        if tail.order.len() != 0 {
            runs.push(Self::spill(&self.plan, &self.projection, tail).await?)?;
        }
        self.spilled = true;
        while runs.files.len() > 1 {
            let Runs {
                files,
                reservation: _inputs,
            } = runs;
            let mut files = files.into_iter();
            let mut merged = Runs::new(&context.memory)?;
            while files.len() > 0 {
                context.check_cancelled()?;
                let count = files.len().min(MERGE_FAN_IN);
                if count == 1 {
                    merged.push(files.next().expect("the last run remains"))?;
                } else {
                    let file = Self::merge(
                        &self.plan,
                        &self.projection,
                        &self.key_columns,
                        self.native.as_ref(),
                        &files.as_slice()[..count],
                    )
                    .await?;
                    for _ in 0..count {
                        drop(files.next());
                    }
                    merged.push(file)?;
                }
                tokio::task::yield_now().await;
            }
            runs = merged;
        }
        let file = runs
            .files
            .pop()
            .ok_or_else(|| invalid("sort-state", "Sort spill has no output run"))?;
        Ok(Sorted::Spill(file.reader(context)?))
    }

    async fn spill(
        plan: &SortPlan,
        projection: &[usize],
        mut run: MemoryRun,
    ) -> Result<Arc<SpillFile>> {
        let mut writer = plan.execution.context.spill.create()?;
        while let Some(batch) = run.next(plan, projection, plan.execution.context.options.batch_rows)? {
            write_batch(&mut writer, batch, plan)?;
            tokio::task::consume_budget().await;
        }
        writer.finish()
    }

    async fn merge(
        plan: &SortPlan,
        projection: &[usize],
        key_columns: &[usize],
        native: Option<&NativeOrder>,
        files: &[Arc<SpillFile>],
    ) -> Result<Arc<SpillFile>> {
        let context = &plan.execution.context;
        let mut writer = context.spill.create()?;
        let _scratch = context.memory.reserve(
            context
                .options
                .batch_rows
                .saturating_mul(64)
                .saturating_add(8192),
        )?;
        let mut runs = files
            .iter()
            .map(|file| {
                Ok(Run {
                    reader: file.reader(context)?,
                    batch: None,
                    row: 0,
                    key: None,
                    prefix: None,
                    native: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut remaining = plan.limit;
        let prefixed = native.filter(|order| order.is_total() && order.has_leading_bytes());
        while remaining != 0 {
            for run in &mut runs {
                run.fill(plan, key_columns, native)?;
            }
            let size = context
                .options
                .batch_rows
                .min(usize::try_from(remaining).unwrap_or(usize::MAX));
            let mut positions = Vec::with_capacity(size);
            // Row offsets identify heads only until the next spill page is loaded.
            let mut comparisons = [[None; MERGE_FAN_IN]; MERGE_FAN_IN];
            while positions.len() < size {
                context.check_cancelled()?;
                let mut winner: Option<usize> = None;
                for lane in 0..runs.len() {
                    let (preceding, candidates) = runs.split_at_mut(lane);
                    let run = &mut candidates[0];
                    if run.batch.is_none() { continue; }
                    if let Some(first) = winner {
                        let other = &mut preceding[first];
                        let rows = (run.row, other.row);
                        let cached = &mut comparisons[lane][first];
                        let comparison = match *cached {
                            Some((previous, comparison)) if previous == rows => comparison,
                            _ => {
                                let comparison = if let Some(order) = prefixed.filter(|_| run.native && other.native) {
                                    order.compare_prefixed(run.head(order), other.head(order), &plan.order, None)?
                                } else {
                                    run.compare(other, plan, key_columns, native)?
                                };
                                *cached = Some((rows, comparison));
                                comparison
                            }
                        };
                        if !comparison.is_lt() {
                            continue;
                        }
                    }
                    winner = Some(lane);
                }
                let Some(lane) = winner else {
                    break;
                };
                let run = &mut runs[lane];
                positions.push((lane, run.row));
                run.row += 1;
                run.prefix = None;
                // Keep every source batch until this selection has been gathered.
                if run.batch.as_ref().is_some_and(|batch| run.row == batch.len) {
                    break;
                }
                run.read_key(plan, key_columns)?;
            }
            if positions.is_empty() {
                break;
            }
            let batch = gather(positions.iter().copied(), projection, &context.memory, |lane| {
                runs[lane]
                    .batch
                    .as_ref()
                    .expect("a selected lane owns its batch")
            })?;
            for run in &mut runs {
                if run.batch.as_ref().is_some_and(|batch| run.row == batch.len) {
                    run.release_page();
                }
            }
            remaining -= batch.len as u64;
            write_batch(&mut writer, batch, plan)?;
            tokio::task::consume_budget().await;
        }
        writer.finish()
    }
}

#[async_trait]
impl BatchCursor for BatchSort {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        if let Some(input) = self.input.take() {
            self.output = Some(self.sort(input).await?);
        }
        let context = &self.plan.execution.context;
        while let Some(output) = &mut self.output {
            context.check_cancelled()?;
            let batch = match output {
                Sorted::Memory(run) => run.next(&self.plan, &self.projection, self.semantic_page_rows.unwrap_or(context.options.batch_rows))?,
                Sorted::Spill(reader) => match self.semantic_page_rows {
                    Some(rows) => read_semantic_page(reader, &mut self.spill_pending, rows, self.projection.len(), context)?,
                    None => reader.read_columns(self.projection.len())?,
                },
            };
            let Some(batch) = batch else {
                self.output = None;
                break;
            };
            if self.offset >= batch.len as u64 {
                self.offset -= batch.len as u64;
                continue;
            }
            let start = std::mem::take(&mut self.offset) as usize;
            if start == 0 {
                return Ok(Some(batch));
            }
            let _mask = context.memory.reserve(batch.len.saturating_add(256))?;
            let mask: Vec<_> = (0..batch.len).map(|row| row >= start).collect();
            return batch.select(&mask, &context.memory).map(Some);
        }
        Ok(None)
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        span.attribute("sort_columnar", rtdb_plan::TraceValue::Boolean(true));
        span.attribute(
            "sort_arrow_keys",
            rtdb_plan::TraceValue::Boolean(self.used_native),
        );
        span.attribute(
            "sort_columnar_spill",
            rtdb_plan::TraceValue::Boolean(self.spilled),
        );
    }
}

enum Sorted {
    Memory(MemoryRun),
    Spill(SpillReader),
}

/// The scratch ordering a block of `rows` takes beside the block itself, which counts against the sort's share so that a
/// block spills before ordering it could outgrow that share.
fn ordering_bytes(rows: usize) -> usize {
    rows.saturating_mul(4 * size_of::<usize>() + size_of::<Option<Key>>())
}

struct Block {
    batches: Vec<Option<Source>>,
    positions: Vec<(usize, usize)>,
    native: bool,
    payload_bytes: usize,
    scratch: Arc<MemoryReservation>,
}

struct Source {
    batch: ColumnBatch,
    remaining: usize,
}

impl Block {
    fn new(memory: &Arc<MemoryBudget>, native: bool) -> Result<Self> {
        Ok(Self {
            batches: vec![],
            positions: vec![],
            native,
            payload_bytes: 0,
            scratch: memory.reserve(256)?,
        })
    }

    fn charged(&self) -> usize {
        self.payload_bytes.saturating_add(self.scratch.bytes())
    }

    fn append(
        &mut self,
        batch: ColumnBatch,
        payload_bytes: usize,
        columns: &[usize],
        native: Option<&NativeOrder>,
        projected: bool,
        types: &mut KeyTypes,
        plan: &SortPlan,
    ) -> Result<()> {
        let base = batch
            .len
            .checked_mul(size_of::<(usize, usize)>() + size_of::<usize>())
            .and_then(|size| size.checked_add(256))
            .ok_or_else(size_error)?;
        let scratch = Arc::get_mut(&mut self.scratch).expect("sort block owns its reservation");
        scratch.grow_to(scratch.bytes().checked_add(base).ok_or_else(size_error)?)?;
        if self.native && !native.is_some_and(|native| native.accepts(&batch)) {
            for earlier in &self.batches {
                observe_scalar_keys(&earlier.as_ref().expect("block holds every input page").batch, columns, projected, types, plan)?;
            }
            self.native = false;
        }
        if !self.native { observe_scalar_keys(&batch, columns, projected, types, plan)?; }
        self.positions.reserve_exact(batch.len);
        let source = self.batches.len();
        self.positions
            .extend((0..batch.len).map(|row| (source, row)));
        self.payload_bytes = self.payload_bytes.saturating_add(payload_bytes);
        self.batches.push(Some(Source { remaining: batch.len, batch }));
        Ok(())
    }

    fn order(
        self,
        plan: &SortPlan,
        key_columns: &[usize],
        native: Option<&NativeOrder>,
        projected: bool,
    ) -> Result<MemoryRun> {
        let context = &plan.execution.context;
        let algorithm = context.options.object_sort;
        let len = self.positions.len();
        let row_at = |index: usize| {
            let (source, row) = self.positions[index];
            (&self.batches[source].as_ref().expect("block holds every input page").batch, row)
        };
        let limit = usize::try_from(plan.limit).unwrap_or(usize::MAX);
        // Under a total order, selecting the first rows yields exactly the stable sort's prefix.
        // The legacy merge sort is kept for its comparison schedule, so it always sorts in full.
        let selects = self.native && algorithm == ObjectSort::TimSort
            && native.is_some_and(NativeOrder::is_total) && limit < len / 8;
        let prefixed = native.filter(|order| selects && order.has_leading_bytes());
        let mut comparisons = 0usize;
        let order = if let Some(order) = prefixed {
            let _temporary = context.memory.reserve(select::scratch_bytes::<ByteKey<'_>>(limit)?)?;
            select::first(len, limit, |index| {
                let (batch, row) = row_at(index);
                order.leading_bytes(batch, row)
            }, |left, a, right, b| {
                context.check_cancelled_at(comparisons)?;
                comparisons += 1;
                let (left, left_row) = row_at(left);
                let (right, right_row) = row_at(right);
                order.compare_prefixed(
                    (left, left_row, a.prefix), (right, right_row, b.prefix), &plan.order,
                    Some((a.tail, b.tail)),
                )
            })?
        } else if let Some(order) = native.filter(|_| selects) {
            let _temporary = context.memory.reserve(select::scratch_bytes::<native_keys::Key<'_>>(limit)?)?;
            select::first(len, limit, |index| {
                let (batch, row) = row_at(index);
                order.leading_key(batch, row)
            }, |left, &a, right, &b| {
                context.check_cancelled_at(comparisons)?;
                comparisons += 1;
                let (left, left_row) = row_at(left);
                let (right, right_row) = row_at(right);
                order.compare_leading((left, left_row, a), (right, right_row, b), &plan.order)
            })?
        } else {
            let bytes = match algorithm {
                ObjectSort::TimSort => timsort::scratch_bytes(len)?,
                ObjectSort::LegacyMergeSort => legacy::scratch_bytes(len)?,
            };
            let _temporary = context.memory.reserve(bytes)?;
            let mut keys = if self.native { None } else { Some(BlockKeys::new(len, &context.memory)?) };
            let compare = |left_index, right_index| {
                context.check_cancelled_at(comparisons)?;
                comparisons += 1;
                let (left, left_row) = row_at(left_index);
                let (right, right_row) = row_at(right_index);
                if let Some(keys) = &mut keys {
                    return keys.compare(left_index, right_index, plan, &|index| {
                        let (batch, row) = row_at(index);
                        comparison_key(batch, row, plan, key_columns, projected)
                    });
                }
                native.expect("only an admitted sort can borrow its keys")
                    .compare(left, left_row, right, right_row, &plan.order)
            };
            let mut order: Vec<_> = (0..len).collect();
            match algorithm {
                ObjectSort::TimSort => timsort::sort(&mut order, compare)?,
                ObjectSort::LegacyMergeSort => legacy::sort(&mut order, compare)?,
            }
            order.truncate(limit);
            order
        };
        let Self {
            mut batches,
            positions,
            mut scratch,
            ..
        } = self;
        if order.len() < len {
            for source in batches.iter_mut().flatten() {
                source.remaining = 0;
            }
            for &index in &order {
                let (source, _) = positions[index];
                batches[source].as_mut().expect("selected page still exists").remaining += 1;
            }
            for source in &mut batches {
                if source.as_ref().is_some_and(|source| source.remaining == 0) {
                    *source = None;
                }
            }
        }
        let bytes = positions
            .capacity()
            .saturating_mul(size_of::<(usize, usize)>())
            .saturating_add(order.capacity().saturating_mul(size_of::<usize>()))
            .saturating_add(batches.capacity().saturating_mul(size_of::<Option<Source>>()))
            .saturating_add(256);
        let scratch_size = Arc::get_mut(&mut scratch).expect("sort block owns its reservation");
        if bytes > scratch_size.bytes() {
            scratch_size.grow_to(bytes)?;
        } else {
            scratch_size.shrink_to(bytes)?;
        }
        Ok(MemoryRun {
            batches,
            positions,
            order: order.into_iter(),
            _scratch: scratch,
        })
    }
}

fn observe_scalar_keys(
    batch: &ColumnBatch, columns: &[usize], projected: bool, types: &mut KeyTypes, plan: &SortPlan,
) -> Result<()> {
    let context = &plan.execution.context;
    if columns.len() == types.values.len() && columns.iter().zip(&types.values).all(|(&column, known)| {
        covered_arrow_key(&batch.columns[column], batch.len, known, &context.memory)
    }) {
        return context.check_cancelled();
    }
    for row in 0..batch.len {
        context.check_cancelled_at(row)?;
        let _scratch = plan.execution.context.memory.reserve(key_bound(batch, columns, row, size_of::<Value>())?)?;
        let values = columns.iter().map(|&column| batch.columns[column].expression_value(row, projected, None))
            .collect::<Result<Vec<_>>>()?;
        types.observe(&values, plan)?;
    }
    Ok(())
}

fn covered_arrow_key(column: &Column, rows: usize, known: &LogicalType, memory: &Arc<MemoryBudget>) -> bool {
    let covered = |array: &dyn arrow_array::Array, field: &arrow_schema::Field, logical: Option<&LogicalType>| {
        covered_arrow_layout(array, field, logical, known, memory)
    };
    match column {
        Column::Arrow { array, field, logical } =>
            array.len() == rows && covered(array.as_ref(), field, logical.as_ref()),
        Column::Selected(selected) => {
            if !selected.source_layouts().all(|(array, field, logical)| covered(array, field, logical)) {
                return false;
            }
            let mut cells = selected.arrow_cells();
            (0..rows).all(|_| cells.next().is_some_and(|cell| cell.is_none_or(|(array, _, _, row)| row < array.len())))
                && cells.next().is_none()
        }
        Column::Values(_) => false,
    }
}

fn covered_arrow_layout(array: &dyn arrow_array::Array, field: &arrow_schema::Field, logical: Option<&LogicalType>,
    known: &LogicalType, memory: &Arc<MemoryBudget>) -> bool {
    if array.data_type() != field.data_type() || field.data_type().is_nested() {
        return false;
    }
    let Ok(_metadata) = memory.reserve(field.size().saturating_mul(4)) else { return false };
    let Ok(physical) = rtdb_columnar::from_field(field) else { return false };
    if logical.is_some_and(|logical| *logical != physical) {
        return false;
    }
    let execution = rtdb_columnar::execution_type(&physical);
    let observed = match &execution {
        LogicalType::Maybe(inner) => inner,
        other => other,
    };
    match observed {
        LogicalType::Null => true,
        // URI decoding validates the reached spelling before the sort checks its comparator.
        LogicalType::Scalar(scalar) if *scalar != ScalarType::Uri => types_cover_scalar(known, scalar),
        _ => false,
    }
}

fn types_cover_scalar(known: &LogicalType, scalar: &ScalarType) -> bool {
    match known {
        LogicalType::Scalar(known) => known == scalar,
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => types_cover_scalar(inner, scalar),
        LogicalType::Union(legs) => legs.iter().any(|leg| types_cover_scalar(leg, scalar)),
        _ => false,
    }
}

struct MemoryRun {
    batches: Vec<Option<Source>>,
    positions: Vec<(usize, usize)>,
    order: std::vec::IntoIter<usize>,
    _scratch: Arc<MemoryReservation>,
}

impl MemoryRun {
    fn next(&mut self, plan: &SortPlan, columns: &[usize], maximum: usize) -> Result<Option<ColumnBatch>> {
        if self.order.len() == 0 {
            return Ok(None);
        }
        let context = &plan.execution.context;
        let count = self.order.len().min(maximum);
        let _scratch = context.memory.reserve(256 + count.saturating_mul(128))?;
        let positions = self.order.as_slice()[..count].iter().map(|&index| self.positions[index]);
        let batch = gather(positions, columns, &context.memory, |source| {
            &self.batches[source].as_ref().expect("selected page still exists").batch
        })?;
        for index in self.order.by_ref().take(count) {
            let (source, _) = self.positions[index];
            let slot = &mut self.batches[source];
            let page = slot.as_mut().expect("selected page still exists");
            page.remaining -= 1;
            if page.remaining == 0 {
                *slot = None;
            }
        }
        Ok(Some(batch))
    }
}

fn gather<'a>(
    positions: impl ExactSizeIterator<Item = (usize, usize)> + Clone,
    columns: &[usize],
    memory: &Arc<MemoryBudget>,
    batch: impl Fn(usize) -> &'a ColumnBatch,
) -> Result<ColumnBatch> {
    if let Some((source, start)) = positions.clone().next()
        && let Some(end) = start.checked_add(positions.len())
        && positions.clone().zip(start..end).all(|(position, row)| position == (source, row))
        && let Some(page) = batch(source).take_sorted_span(start..end, columns, memory)?
    {
        return Ok(page);
    }
    // Exclude unselected sources so their layouts cannot force a gather through decoded values.
    let mut slots = std::collections::BTreeMap::new();
    let mut sources = Vec::new();
    let rows = positions
        .map(|(source, row)| {
            let slot = *slots.entry(source).or_insert_with(|| {
                let slot = sources.len();
                sources.push(batch(source));
                slot
            });
            (slot, row)
        })
        .collect::<Vec<_>>();
    ColumnBatch::take_rows(&sources, &rows, columns, memory)
}

fn key_bound(batch: &ColumnBatch, columns: &[usize], row: usize, slot: usize) -> Result<usize> {
    let base = columns
        .len()
        .checked_mul(slot)
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(size_error)?;
    columns.iter().try_fold(base, |bytes, &column| {
        bytes
            .checked_add(batch.columns[column].bound(row)?)
            .ok_or_else(size_error)
    })
}

struct Runs {
    files: Vec<Arc<SpillFile>>,
    reservation: Arc<MemoryReservation>,
}
impl Runs {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self {
            files: vec![],
            reservation: memory.reserve(256)?,
        })
    }
    fn push(&mut self, file: Arc<SpillFile>) -> Result<()> {
        if self.files.len() == self.files.capacity() {
            let capacity = self
                .files
                .len()
                .checked_mul(2)
                .ok_or_else(size_error)?
                .max(8);
            let bytes = capacity
                .checked_mul(256)
                .and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(size_error)?;
            Arc::get_mut(&mut self.reservation)
                .expect("run list owns its reservation")
                .grow_to(bytes)?;
            self.files.reserve_exact(capacity - self.files.len());
        }
        self.files.push(file);
        Ok(())
    }
}

enum ComparisonCell {
    Decimal(f64),
    Scalar(Value),
}

impl ComparisonCell {
    fn number(&self) -> Option<native_keys::Key<'_>> {
        match self {
            Self::Decimal(value) => Some(native_keys::Key::Float(*value)),
            Self::Scalar(value) => native_keys::Kind::Number.value(value),
        }
    }

    fn as_value(&self) -> std::borrow::Cow<'_, Value> {
        match self {
            Self::Decimal(value) => std::borrow::Cow::Owned(Value::F64(*value)),
            Self::Scalar(value) => std::borrow::Cow::Borrowed(value),
        }
    }
}

struct Key {
    cells: Vec<ComparisonCell>,
    _reservation: Arc<MemoryReservation>,
}

impl Key {
    fn compare(&self, other: &Self, plan: &SortPlan) -> Result<std::cmp::Ordering> {
        for (index, order) in plan.order.iter().enumerate() {
            let left = &self.cells[index];
            let right = &other.cells[index];
            let comparison = if let (Some(left), Some(right)) = (left.number(), right.number()) {
                left.compare(right, order.descending, order.nulls_first)?
            } else {
                plan.compare_cell(&left.as_value(), &right.as_value(), order)?
            };
            if !comparison.is_eq() { return Ok(comparison); }
        }
        Ok(std::cmp::Ordering::Equal)
    }
}

struct BlockKeys {
    keys: Vec<Option<Key>>,
    _reservation: Arc<MemoryReservation>,
}

impl BlockKeys {
    /// The keys of a block of `rows`, charged as part of the sort's memory: a budget that cannot hold them fails the sort.
    fn new(rows: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = rows.checked_mul(size_of::<Option<Key>>()).ok_or_else(size_error)?;
        let reservation = memory.reserve(bytes)?;
        let keys = std::iter::repeat_with(|| None).take(rows).collect();
        Ok(Self { keys, _reservation: reservation })
    }

    fn load(&mut self, index: usize, read: &impl Fn(usize) -> Result<Key>) -> Result<()> {
        if self.keys[index].is_none() {
            self.keys[index] = Some(read(index)?);
        }
        Ok(())
    }

    fn compare(
        &mut self, left: usize, right: usize, plan: &SortPlan, read: &impl Fn(usize) -> Result<Key>,
    ) -> Result<std::cmp::Ordering> {
        self.load(left, read)?;
        self.load(right, read)?;
        self.keys[left].as_ref().expect("loaded sort key")
            .compare(self.keys[right].as_ref().expect("loaded sort key"), plan)
    }
}

struct Run {
    reader: SpillReader,
    batch: Option<ColumnBatch>,
    row: usize,
    key: Option<Key>,
    prefix: Option<BytePrefix>,
    native: bool,
}

impl Run {
    fn fill(
        &mut self,
        plan: &SortPlan,
        columns: &[usize],
        native: Option<&NativeOrder>,
    ) -> Result<()> {
        if self
            .batch
            .as_ref()
            .is_some_and(|batch| self.row < batch.len)
        {
            return Ok(());
        }
        self.release_page();
        self.batch = self.reader.read_columns(plan.schema.len())?;
        self.native = self
            .batch
            .as_ref()
            .is_some_and(|batch| native.is_some_and(|native| native.accepts(batch)));
        self.read_key(plan, columns)
    }

    fn release_page(&mut self) {
        self.key = None;
        self.prefix = None;
        self.batch = None;
        self.row = 0;
    }

    fn head(&mut self, order: &NativeOrder) -> (&ColumnBatch, usize, BytePrefix) {
        let batch = self.batch.as_ref().expect("a selected run owns a batch");
        let row = self.row;
        let prefix = *self.prefix.get_or_insert_with(|| order.leading_bytes(batch, row).prefix);
        (batch, row, prefix)
    }

    fn read_key(&mut self, plan: &SortPlan, columns: &[usize]) -> Result<()> {
        if self.native {
            self.key = None;
        } else if let Some(batch) = &self.batch {
            self.key = Some(comparison_key(batch, self.row, plan, columns, false)?);
        }
        Ok(())
    }

    fn compare(
        &mut self,
        other: &mut Self,
        plan: &SortPlan,
        columns: &[usize],
        native: Option<&NativeOrder>,
    ) -> Result<std::cmp::Ordering> {
        let left = self.batch.as_ref().expect("a selected run owns a batch");
        let right = other.batch.as_ref().expect("a selected run owns a batch");
        if self.native && other.native {
            return native.expect("only admitted runs borrow their keys")
                .compare(left, self.row, right, other.row, &plan.order);
        }
        if self.key.is_none() {
            self.key = Some(comparison_key(left, self.row, plan, columns, false)?);
        }
        if other.key.is_none() {
            other.key = Some(comparison_key(right, other.row, plan, columns, false)?);
        }
        self.key.as_ref().expect("loaded run key")
            .compare(other.key.as_ref().expect("loaded run key"), plan)
    }
}

fn comparison_key(batch: &ColumnBatch, row: usize, plan: &SortPlan, columns: &[usize], projected: bool) -> Result<Key> {
    let reservation = plan.execution.context.memory.reserve(key_bound(batch, columns, row, size_of::<ComparisonCell>())?)?;
    let cells = columns.iter().map(|&column| {
        let column = &batch.columns[column];
        if let Some(key) = decimal_comparison_key(column, row) {
            return Ok(ComparisonCell::Decimal(key));
        }
        Ok(match column.expression_value(row, projected, None)? {
            Value::Decimal(decimal) => ComparisonCell::Decimal(rtdb_expr::decimal_to_f64(&decimal)),
            value => ComparisonCell::Scalar(value),
        })
    }).collect::<Result<Vec<_>>>()?;
    Ok(Key { cells, _reservation: reservation })
}

fn decimal_comparison_key(column: &Column, row: usize) -> Option<f64> {
    let (array, field, logical, row) = column.arrow_cell(row)??;
    let cell = ArrowCell { field, array, row };
    if field.extension_type_name().is_some() { return None; }
    if field.data_type().is_decimal() {
        return decimal_at(cell, logical);
    }
    if !matches!(field.data_type(), arrow_schema::DataType::RunEndEncoded(..) | arrow_schema::DataType::Union(..)) {
        return None;
    }
    let (cell, logical) = cell.logical_position(logical).ok()?;
    decimal_at(cell, Some(&logical))
}

fn decimal_at(cell: ArrowCell<'_>, logical: Option<&LogicalType>) -> Option<f64> {
    if !cell.field.data_type().is_decimal() || cell.field.extension_type_name().is_some() { return None; }
    let (parameters, bytes) = cell.decimal_coefficient().ok()??;
    let scale = match logical {
        None => parameters.scale,
        Some(logical) => {
            let logical = match logical { LogicalType::Maybe(inner) => inner, other => other };
            let LogicalType::Scalar(ScalarType::Decimal { bit_width, scale, .. }) = logical else { return None };
            if *bit_width != parameters.bit_width { return None; }
            *scale
        }
    };
    let width = usize::from(parameters.bit_width / 8);
    Some(rtdb_types::jvm_number::decimal_bytes_to_f64(&bytes[bytes.len() - width..], scale))
}

fn write_batch(writer: &mut SpillWriter, batch: ColumnBatch, plan: &SortPlan) -> Result<()> {
    writer.write_columns(&batch, &plan.schema, &plan.execution.context.memory)
}

fn read_semantic_page(
    reader: &mut SpillReader, pending: &mut Option<(Arc<ColumnBatch>, usize)>, limit: usize, width: usize, context: &crate::QueryContext,
) -> Result<Option<ColumnBatch>> {
    let mut sources = Vec::new();
    let mut spans = Vec::new();
    let mut rows = 0;
    let mut reservation = context.memory.reserve(256)?;
    while rows < limit {
        context.check_cancelled()?;
        let (batch, offset) = match pending.take() {
            Some(next) => next,
            None => {
                let Some(batch) = reader.read_columns(width)? else { break; };
                if rows == 0 && batch.len == limit { return Ok(Some(batch)); }
                (Arc::new(batch), 0)
            }
        };
        let count = (batch.len - offset).min(limit - rows);
        if offset + count < batch.len { *pending = Some((Arc::clone(&batch), offset + count)); }
        let bytes = (sources.len() + 1).checked_mul(256).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        Arc::get_mut(&mut reservation).expect("The sort owns its spill page inventory").grow_to(bytes)?;
        sources.push(batch);
        spans.push(offset..offset + count);
        rows += count;
    }
    ColumnBatch::take_ranges(sources, &spans, rows, &context.memory)
}

#[test]
fn sorted_span_shares_one_source_charge_and_preserves_projection_layout() {
    use crate::columnar::Column;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use rtdb_types::{LogicalType, ScalarType};

    let memory = MemoryBudget::new(1 << 20);
    let logical = LogicalType::Scalar(ScalarType::I64).maybe();
    let key = Int64Array::from_iter_values(0..1024);
    let value = Int64Array::from_iter_values(2000..3024);
    let first = value.values()[256..].as_ptr();
    let second = key.values()[256..].as_ptr();
    let field = |name: &str| Arc::new(Field::new(name, DataType::Int64, true)
        .with_metadata(std::collections::HashMap::from([("origin".into(), name.to_owned())])));
    let columns = [key, value].into_iter().zip(["key", "value"]).map(|(array, name)| {
        Column::Arrow { array: Arc::new(array), field: field(name), logical: Some(logical.clone()) }
    }).collect();
    let source = ColumnBatch::settled(columns, 1024, memory.reserve(1 << 16).unwrap()).unwrap();
    let original = memory.used_bytes();
    let page = gather((256..768).map(|row| (0, row)), &[1, 0, 1], &memory, |_| &source).unwrap();
    assert_eq!(page.len, 512);
    assert!(memory.used_bytes() < 2 * original);
    drop(source);
    assert!(memory.used_bytes() > original);
    assert_eq!(page.working_set_bytes(&memory).unwrap(), memory.used_bytes());
    for (column, (name, pointer)) in page.columns.iter().zip([("value", first), ("key", second), ("value", first)]) {
        let Column::Arrow { array, field: actual, logical: ty } = column else { panic!("Expected a projected Arrow view") };
        assert_eq!(actual, &field(name));
        assert_eq!(ty.as_ref(), Some(&logical));
        let values = array.as_any().downcast_ref::<Int64Array>().unwrap().values();
        assert_eq!(values.as_ptr(), pointer);
    }
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sparse_sorted_span_releases_large_variable_width_backing() {
    use crate::columnar::Column;
    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field};
    use rtdb_types::{LogicalType, ScalarType};

    let memory = MemoryBudget::new(1 << 22);
    let unused = "x".repeat(1 << 18);
    let array = StringArray::from(vec![unused.as_str(), "kept"]);
    let logical = LogicalType::Scalar(ScalarType::Utf8);
    let column = Column::Arrow { array: Arc::new(array), field: Arc::new(Field::new("payload", DataType::Utf8, false)), logical: Some(logical) };
    let source = ColumnBatch::settled(vec![column], 2, memory.reserve(1 << 19).unwrap()).unwrap();
    let page = gather(std::iter::once((0, 1)), &[0], &memory, |_| &source).unwrap();
    drop(source);
    assert!(memory.used_bytes() < 8192);
    assert_eq!(page.columns[0].value(0).unwrap(), Value::Utf8("kept".into()));
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn limited_block_drops_unselected_payload_and_releases_each_consumed_source() {
    use crate::{columnar::Column, context::OuterRows, parameters::BoundParameters};
    use rtdb_plan::{ColumnId, Field};
    use rtdb_types::{LogicalType, ScalarType};

    let context = super::object_sort_tests::context(ObjectSort::TimSort);
    let memory = &context.memory;
    let execution = Execution {
        parameters: Arc::new(BoundParameters::infer(vec![], memory).unwrap()),
        outer: OuterRows::empty(memory).unwrap(),
        context: Arc::clone(&context), bindings: None, analysis: None,
    };
    let key = Field { id: ColumnId(0), name: "key".into(), qualifier: None, qualified_only: false,
        unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let payload = Field { id: ColumnId(1), name: "payload".into(),
        data_type: LogicalType::Scalar(ScalarType::Utf8), ..key.clone() };
    let plan = SortPlan { schema: Schema::new(vec![key.clone(), payload]).unwrap(), limit: 2,
        order: vec![SortExpr { expr: key.expression(), descending: false, nulls_first: false }], execution };
    let native = NativeOrder::bind(&plan.schema, &[0]).unwrap();
    let mut key_types = KeyTypes::declared(&plan).unwrap();
    let mut block = Block::new(memory, true).unwrap();
    for key in [0, 2, 1] {
        let reservation = memory.reserve(1 << 19).unwrap();
        let batch = ColumnBatch::settled(vec![
            Column::Values(vec![Value::I64(key)]),
            Column::Values(vec![Value::Utf8("p".repeat(1 << 18))]),
        ], 1, reservation).unwrap();
        let bytes = batch.working_set_bytes(memory).unwrap();
        block.append(batch, bytes, &[0], Some(&native), false, &mut key_types, &plan).unwrap();
    }
    let before_prune = memory.used_bytes();
    let mut run = block.order(&plan, &[0], Some(&native), false).unwrap();
    assert!(before_prune - memory.used_bytes() >= 1 << 18);
    let before_gather = memory.used_bytes();
    let first = run.next(&plan, &[0, 1], 1).unwrap().unwrap();
    assert_eq!(first.columns[0].value(0).unwrap(), Value::I64(0));
    drop(first);
    assert!(before_gather - memory.used_bytes() >= 1 << 18);
    let last = run.next(&plan, &[0, 1], 1).unwrap().unwrap();
    assert_eq!(last.columns[0].value(0).unwrap(), Value::I64(1));
}

#[test]
fn arrow_wave4_sort_reads_each_cached_key_once() {
    use std::cell::Cell;

    let memory = MemoryBudget::new(1 << 20);
    let mut keys = BlockKeys::new(2, &memory).unwrap();
    let reads = Cell::new(0);
    let read = |index| {
        reads.set(reads.get() + 1);
        Ok(Key { cells: vec![ComparisonCell::Scalar(Value::I64(index as i64))], _reservation: memory.reserve(64)? })
    };
    for index in [1, 0, 0, 1, 0] {
        keys.load(index, &read).unwrap();
    }
    assert_eq!(reads.get(), 2);
    drop(keys);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn declared_arrow_sort_key_types_cover_only_infallible_scalar_reads() {
    use arrow_array::{Int64Array, NullArray, StringArray};
    use arrow_schema::Field;

    let memory = MemoryBudget::new(1 << 20);
    let integer = LogicalType::Scalar(ScalarType::I64);
    let decimal = LogicalType::Scalar(ScalarType::Decimal { precision: 38, scale: 5, bit_width: 128 });
    let mixed = integer.join(&decimal);
    let column = Column::Arrow {
        array: Arc::new(Int64Array::from(vec![Some(10), None])),
        field: Arc::new(Field::new("key", arrow_schema::DataType::Int64, true)),
        logical: Some(integer.maybe()),
    };
    assert!(covered_arrow_key(&column, 2, &mixed, &memory));
    assert!(!covered_arrow_key(&column, 2, &decimal, &memory));
    assert!(!covered_arrow_key(&column, 3, &mixed, &memory));

    let Column::Arrow { array, field, logical } = column else { unreachable!() };
    let source = (array, field, logical, None);
    for (offset, covered) in [(0, true), (2, false)] {
        let selected = Column::Selected(crate::columnar::SelectedColumn::new(
            vec![Some(source.clone()), None], Arc::new(vec![(0, offset), (1, 0)]),
        ));
        assert_eq!(covered_arrow_key(&selected, 2, &mixed, &memory), covered);
        assert!(!covered_arrow_key(&selected, 2, &decimal, &memory));
    }

    let bottom = Column::Arrow {
        array: Arc::new(NullArray::new(1)),
        field: Arc::new(Field::new("key", arrow_schema::DataType::Null, false)),
        logical: Some(LogicalType::Nothing),
    };
    assert!(!covered_arrow_key(&bottom, 1, &mixed, &memory));

    let uri = LogicalType::Scalar(ScalarType::Uri);
    let invalid = Column::Arrow {
        array: Arc::new(StringArray::from(vec!["invalid URI"])),
        field: Arc::new(rtdb_columnar::to_field("key", &uri).unwrap()),
        logical: None,
    };
    assert!(!covered_arrow_key(&invalid, 1, &uri, &memory));
    assert!(invalid.value(0).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn decimal_arrow_sort_keys_use_the_reached_logical_scale_and_signed_zero() {
    use arrow_array::Decimal128Array;

    let scale = 400;
    let logical = LogicalType::Scalar(ScalarType::Decimal { precision: 38, scale, bit_width: 128 });
    let field = Arc::new(rtdb_columnar::to_field("key", &logical).unwrap());
    let array = Arc::new(Decimal128Array::from(vec![Some(-1), Some(1), None])
        .with_precision_and_scale(38, 1).unwrap());
    for logical in [None, Some(logical), Some(LogicalType::Scalar(ScalarType::Decimal {
        precision: 38, scale: 0, bit_width: 128,
    }))] {
        let column = Column::Arrow { array: array.clone(), field: field.clone(), logical };
        for row in 0..2 {
            let Value::Decimal(scalar) = column.value(row).unwrap() else { panic!("expected decimal input") };
            let Some(key) = decimal_comparison_key(&column, row) else { panic!("expected Arrow comparison key") };
            assert_eq!(key.to_bits(), rtdb_expr::decimal_to_f64(&scalar).to_bits());
        }
        assert!(decimal_comparison_key(&column, 2).is_none());
    }
}

#[test]
fn encoded_decimal_sort_keys_use_the_active_scalar_without_decoding_it() {
    use rtdb_types::{DecimalValue, RunEndWidth};

    let decimal = |coefficient: i128| Value::Decimal(DecimalValue {
        unscaled: coefficient.to_be_bytes().into(), precision: 38, scale: 400, bit_width: 128,
    });
    let values = [decimal(-1), decimal(-1), Value::F64(0.0), decimal(1), Value::Null];
    let scalar = LogicalType::join_all(&values.iter().map(Value::logical_type).collect::<Vec<_>>());
    let run = LogicalType::RunEndEncoded { width: RunEndWidth::I32, value: Box::new(scalar.clone()) };
    for logical in [scalar, run] {
        let array = rtdb_columnar::copy_array(&logical, &values).unwrap();
        let field = Arc::new(rtdb_columnar::to_field("key", &logical).unwrap());
        let column = Column::Arrow { array, field, logical: Some(logical) };
        assert_eq!(decimal_comparison_key(&column, 0).unwrap().to_bits(), (-0.0f64).to_bits());
        assert_eq!(decimal_comparison_key(&column, 1).unwrap().to_bits(), (-0.0f64).to_bits());
        assert!(decimal_comparison_key(&column, 2).is_none());
        assert_eq!(decimal_comparison_key(&column, 3).unwrap().to_bits(), 0.0f64.to_bits());
        assert!(decimal_comparison_key(&column, 4).is_none());
    }
}
