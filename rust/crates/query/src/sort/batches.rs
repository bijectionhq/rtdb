//! Stable sorting of batch positions, with Arrow spill runs.
use std::{mem::size_of, sync::Arc};

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{Expr, Schema, SortExpr};
use rtdb_types::{ObjectSort, Result, Value};

use crate::{
    columnar::{BatchCursor, BoxBatchCursor, ColumnBatch},
    context::Execution,
    invalid,
    spill::{SpillFile, SpillReader, SpillWriter},
};

use super::{
    BLOCK_ROWS, KeyTypes, MERGE_FAN_IN, SortPlan, legacy, native_keys::NativeOrder, size_error,
    timsort,
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
        })
    }

    pub fn preserving_pages(mut self) -> Self {
        self.semantic_page_rows = Some(0);
        self
    }

    async fn sort(&mut self, mut input: BoxBatchCursor) -> Result<Sorted> {
        let context = &self.plan.execution.context;
        let mut block = Block::new(&context.memory, self.native.is_some())?;
        let mut runs = Runs::new(&context.memory)?;
        let mut key_types = KeyTypes::declared(&self.plan)?;
        let mut logical_runs = 0;
        let mut logical_run_rows = 0;
        let mut total_rows = 0usize;
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
            if !block.positions.is_empty()
                && (block.charged().saturating_add(payload_bytes)
                    > context.options.sort_memory_bytes
                    || block.positions.len().saturating_add(batch.len) > BLOCK_ROWS)
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
        let spill_tail = !block.positions.is_empty() && block.charged() > context.options.sort_memory_bytes;
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
        Ok(Sorted::Spill(file.reader(context.memory.clone())?))
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
        let mut runs = files
            .iter()
            .map(|file| {
                Ok(Run {
                    reader: file.reader(context.memory.clone())?,
                    batch: None,
                    row: 0,
                    key: None,
                    native: false,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut remaining = plan.limit;
        let _scratch = context.memory.reserve(
            context
                .options
                .batch_rows
                .saturating_mul(64)
                .saturating_add(8192),
        )?;
        while remaining != 0 {
            for run in &mut runs {
                run.fill(plan, key_columns, native)?;
            }
            let size = context
                .options
                .batch_rows
                .min(usize::try_from(remaining).unwrap_or(usize::MAX));
            let mut positions = Vec::with_capacity(size);
            while positions.len() < size {
                context.check_cancelled()?;
                let mut winner: Option<usize> = None;
                for (lane, run) in runs
                    .iter()
                    .enumerate()
                    .filter(|(_, run)| run.batch.is_some())
                {
                    if let Some(first) = winner
                        && !run
                            .compare(&runs[first], plan, key_columns, native)?
                            .is_lt()
                    {
                        continue;
                    }
                    winner = Some(lane);
                }
                let Some(lane) = winner else {
                    break;
                };
                let run = &mut runs[lane];
                positions.push((lane, run.row));
                run.row += 1;
                // Keep every source batch until this selection has been gathered.
                if run.batch.as_ref().is_some_and(|batch| run.row == batch.len) {
                    break;
                }
                run.read_key(plan, key_columns)?;
            }
            if positions.is_empty() {
                break;
            }
            let batch = gather(&positions, projection, &context.memory, |lane| {
                runs[lane]
                    .batch
                    .as_ref()
                    .expect("a selected lane owns its batch")
            })?;
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
        span.attribute("sort_columnar", rtdb_plan::TraceValue::Unsigned(1));
        span.attribute(
            "sort_arrow_keys",
            rtdb_plan::TraceValue::Unsigned(u64::from(self.used_native)),
        );
        span.attribute(
            "sort_columnar_spill",
            rtdb_plan::TraceValue::Unsigned(u64::from(self.spilled)),
        );
    }
}

enum Sorted {
    Memory(MemoryRun),
    Spill(SpillReader),
}

struct Block {
    batches: Vec<ColumnBatch>,
    positions: Vec<(usize, usize)>,
    native: bool,
    payload_bytes: usize,
    scratch: Arc<MemoryReservation>,
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
                observe_scalar_keys(earlier, columns, projected, types, plan)?;
            }
            self.native = false;
        }
        if !self.native { observe_scalar_keys(&batch, columns, projected, types, plan)?; }
        self.positions.reserve_exact(batch.len);
        let source = self.batches.len();
        self.positions
            .extend((0..batch.len).map(|row| (source, row)));
        self.payload_bytes = self.payload_bytes.saturating_add(payload_bytes);
        self.batches.push(batch);
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
        let bytes = match algorithm {
            ObjectSort::TimSort => timsort::scratch_bytes(len)?,
            ObjectSort::LegacyMergeSort => legacy::scratch_bytes(len)?,
        };
        let _temporary = context.memory.reserve(bytes)?;
        let mut order: Vec<_> = (0..len).collect();
        let compare = |left, right| {
            context.check_cancelled()?;
            let (left, left_row) = self.positions[left];
            let (right, right_row) = self.positions[right];
            let (left, right) = (&self.batches[left], &self.batches[right]);
            if !self.native {
                let left = scalar_key(left, left_row, plan, key_columns, projected)?;
                let right = scalar_key(right, right_row, plan, key_columns, projected)?;
                return plan.compare(&left.values, &right.values);
            }
            Ok(native.expect("only an admitted sort can borrow its keys")
                .compare(left, left_row, right, right_row, &plan.order))
        };
        match algorithm {
            ObjectSort::TimSort => timsort::sort(&mut order, compare)?,
            ObjectSort::LegacyMergeSort => legacy::sort(&mut order, compare)?,
        }
        order.truncate(usize::try_from(plan.limit).unwrap_or(usize::MAX));
        let Self {
            batches,
            positions,
            mut scratch,
            ..
        } = self;
        let bytes = positions
            .capacity()
            .saturating_mul(size_of::<(usize, usize)>())
            .saturating_add(order.capacity().saturating_mul(size_of::<usize>()))
            .saturating_add(batches.capacity().saturating_mul(size_of::<ColumnBatch>()))
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
    for row in 0..batch.len {
        plan.execution.context.check_cancelled_at(row)?;
        let key = scalar_key(batch, row, plan, columns, projected)?;
        types.observe(&key.values, plan)?;
    }
    Ok(())
}

struct MemoryRun {
    batches: Vec<ColumnBatch>,
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
        let positions: Vec<_> = self
            .order
            .by_ref()
            .take(count)
            .map(|index| self.positions[index])
            .collect();
        let batch = gather(&positions, columns, &context.memory, |batch| {
            &self.batches[batch]
        })?;
        if self.order.len() == 0 {
            self.batches.clear();
        }
        Ok(Some(batch))
    }
}

/// Compacts the source list for this output batch, so unrelated Arrow layouts
/// do not force the gather through decoded values.
fn gather<'a>(
    positions: &[(usize, usize)],
    columns: &[usize],
    memory: &Arc<MemoryBudget>,
    batch: impl Fn(usize) -> &'a ColumnBatch,
) -> Result<ColumnBatch> {
    let mut slots = std::collections::BTreeMap::new();
    let mut sources = Vec::new();
    let rows = positions
        .iter()
        .map(|&(source, row)| {
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

fn key_bound(batch: &ColumnBatch, columns: &[usize], row: usize) -> Result<usize> {
    let base = columns
        .len()
        .checked_mul(size_of::<Value>())
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

struct Key {
    values: Vec<Value>,
    _reservation: Arc<MemoryReservation>,
}
struct Run {
    reader: SpillReader,
    batch: Option<ColumnBatch>,
    row: usize,
    key: Option<Key>,
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
        self.batch = None;
        self.key = None;
        self.row = 0;
        self.batch = self.reader.read_columns(plan.schema.len())?;
        self.native = self
            .batch
            .as_ref()
            .is_some_and(|batch| native.is_some_and(|native| native.accepts(batch)));
        self.read_key(plan, columns)
    }

    fn read_key(&mut self, plan: &SortPlan, columns: &[usize]) -> Result<()> {
        if !self.native
            && let Some(batch) = &self.batch
        {
            self.key = Some(scalar_key(batch, self.row, plan, columns, false)?);
        }
        Ok(())
    }

    fn compare(
        &self,
        other: &Self,
        plan: &SortPlan,
        columns: &[usize],
        native: Option<&NativeOrder>,
    ) -> Result<std::cmp::Ordering> {
        let left = self.batch.as_ref().expect("a selected run owns a batch");
        let right = other.batch.as_ref().expect("a selected run owns a batch");
        if self.native && other.native {
            return Ok(native
                .expect("only admitted runs borrow their keys")
                .compare(left, self.row, right, other.row, &plan.order));
        }
        let left_key = if self.native {
            Some(scalar_key(left, self.row, plan, columns, false)?)
        } else {
            None
        };
        let right_key = if other.native {
            Some(scalar_key(right, other.row, plan, columns, false)?)
        } else {
            None
        };
        let left = self
            .key
            .as_ref()
            .or(left_key.as_ref())
            .expect("sort run has a key");
        let right = other
            .key
            .as_ref()
            .or(right_key.as_ref())
            .expect("sort run has a key");
        plan.compare(&left.values, &right.values)
    }
}

fn scalar_key(batch: &ColumnBatch, row: usize, plan: &SortPlan, columns: &[usize], projected: bool) -> Result<Key> {
    let reservation = plan
        .execution
        .context
        .memory
        .reserve(256 + key_bound(batch, columns, row)?)?;
    let values = columns
        .iter()
        .map(|&column| batch.columns[column].expression_value(row, projected, None))
        .collect::<Result<Vec<_>>>()?;
    Ok(Key {
        values,
        _reservation: reservation,
    })
}

fn write_batch(writer: &mut SpillWriter, batch: ColumnBatch, plan: &SortPlan) -> Result<()> {
    let mut spans = vec![0..batch.len];
    let context = &plan.execution.context;
    let memory = &context.memory;
    let positions = (0..plan.schema.len()).collect::<Vec<_>>();
    while let Some(span) = spans.pop() {
        let start = writer.position()?;
        let result = if span.len() == batch.len {
            writer.write_columns(&batch, &plan.schema, memory)
        } else {
            let bytes = span.len().checked_mul(size_of::<(usize, usize)>()).ok_or_else(size_error)?;
            let _picks = memory.reserve(bytes)?;
            let picks = span.clone().map(|row| (0, row)).collect::<Vec<_>>();
            let slice = ColumnBatch::take_rows_with_custody(&[&batch], &picks, &positions, true, memory)?;
            writer.write_columns(&slice, &plan.schema, memory)
        };
        if let Err(error) = result {
            if error.code != "rtdb/memory-budget-exceeded" || span.len() < 2 { return Err(error); }
            writer.discard_from(start)?;
            let mid = span.start + span.len() / 2;
            spans.push(mid..span.end);
            spans.push(span.start..mid);
        }
    }
    Ok(())
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
