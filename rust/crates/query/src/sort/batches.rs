//! Stable sorting of batch positions, with Arrow spill runs.
use std::{io::Cursor, mem::size_of, sync::Arc};

use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_plan::{Expr, Schema, SortExpr};
use rtdb_types::{ObjectSort, Result, Value};

use crate::{
    columnar::{BatchCursor, BoxBatchCursor, ColumnBatch},
    context::Execution,
    cursor::column_batch,
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
        })
    }

    async fn sort(&mut self, mut input: BoxBatchCursor) -> Result<Sorted> {
        let context = &self.plan.execution.context;
        let mut block = Block::new(&context.memory, self.native.is_some())?;
        let mut runs = Runs::new(&context.memory)?;
        let mut key_types = KeyTypes::declared(&self.plan)?;
        while let Some(batch) = input.next_batch().await? {
            context.check_cancelled()?;
            if batch.len == 0 {
                continue;
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
                self.used_native |= full.keys.is_none();
                runs.push(
                    Self::spill(
                        &self.plan,
                        &self.projection,
                        full.order(&self.plan, self.key_columns.len(), self.native.as_ref())?,
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
        if !block.positions.is_empty() || !runs.files.is_empty() {
            key_types.validate()?;
        }
        self.used_native |= block.keys.is_none() && !block.positions.is_empty();
        let tail = block.order(&self.plan, self.key_columns.len(), self.native.as_ref())?;
        if runs.files.is_empty() {
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
        while let Some(batch) = run.next(plan, projection)? {
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
                Sorted::Memory(run) => run.next(&self.plan, &self.projection)?,
                Sorted::Spill(reader) => read_batch(reader)?,
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
    keys: Option<Vec<Value>>,
    payload_bytes: usize,
    scratch: Arc<MemoryReservation>,
}

impl Block {
    fn new(memory: &Arc<MemoryBudget>, native: bool) -> Result<Self> {
        Ok(Self {
            batches: vec![],
            positions: vec![],
            keys: (!native).then(Vec::new),
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
        if self.keys.is_none() && !native.is_some_and(|native| native.accepts(&batch)) {
            let mut keys = Vec::new();
            for earlier in &self.batches {
                append_scalar_keys(&mut keys, scratch, earlier, columns, projected, types, plan)?;
            }
            self.keys = Some(keys);
        }
        if let Some(keys) = &mut self.keys {
            append_scalar_keys(keys, scratch, &batch, columns, projected, types, plan)?;
        }
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
        key_width: usize,
        native: Option<&NativeOrder>,
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
            if let Some(keys) = &self.keys {
                let key = |index| &keys[index * key_width..(index + 1) * key_width];
                return plan.compare(key(left), key(right));
            }
            let (left, left_row) = self.positions[left];
            let (right, right_row) = self.positions[right];
            Ok(native
                .expect("only an admitted sort can borrow its keys")
                .compare(
                    &self.batches[left],
                    left_row,
                    &self.batches[right],
                    right_row,
                    &plan.order,
                ))
        };
        match algorithm {
            ObjectSort::TimSort => timsort::sort(&mut order, compare)?,
            ObjectSort::LegacyMergeSort => legacy::sort(&mut order, compare)?,
        }
        order.truncate(usize::try_from(plan.limit).unwrap_or(usize::MAX));
        let Self {
            batches,
            positions,
            keys,
            mut scratch,
            ..
        } = self;
        drop(keys);
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

fn append_scalar_keys(
    keys: &mut Vec<Value>,
    scratch: &mut MemoryReservation,
    batch: &ColumnBatch,
    columns: &[usize],
    projected: bool,
    types: &mut KeyTypes,
    plan: &SortPlan,
) -> Result<()> {
    let bound = (0..batch.len).try_fold(0usize, |bound, row| {
        bound
            .checked_add(key_bound(batch, columns, row)?)
            .ok_or_else(size_error)
    })?;
    scratch.grow_to(scratch.bytes().checked_add(bound).ok_or_else(size_error)?)?;
    keys.reserve_exact(
        batch
            .len
            .checked_mul(columns.len())
            .ok_or_else(size_error)?,
    );
    for row in 0..batch.len {
        plan.execution.context.check_cancelled_at(row)?;
        let start = keys.len();
        for &column in columns {
            let mut value = batch.columns[column].value(row)?;
            if projected {
                value.admit_expression_dates()?;
            }
            keys.push(value);
        }
        types.observe(&keys[start..], plan)?;
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
    fn next(&mut self, plan: &SortPlan, columns: &[usize]) -> Result<Option<ColumnBatch>> {
        if self.order.len() == 0 {
            return Ok(None);
        }
        let context = &plan.execution.context;
        let count = self.order.len().min(context.options.batch_rows);
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
        self.batch = read_batch(&mut self.reader)?;
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
            self.key = Some(scalar_key(batch, self.row, plan, columns)?);
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
            Some(scalar_key(left, self.row, plan, columns)?)
        } else {
            None
        };
        let right_key = if other.native {
            Some(scalar_key(right, other.row, plan, columns)?)
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

fn scalar_key(batch: &ColumnBatch, row: usize, plan: &SortPlan, columns: &[usize]) -> Result<Key> {
    let reservation = plan
        .execution
        .context
        .memory
        .reserve(256 + key_bound(batch, columns, row)?)?;
    let values = columns
        .iter()
        .map(|&column| batch.columns[column].value(row))
        .collect::<Result<Vec<_>>>()?;
    Ok(Key {
        values,
        _reservation: reservation,
    })
}

fn write_batch(writer: &mut SpillWriter, batch: ColumnBatch, plan: &SortPlan) -> Result<()> {
    let memory = &plan.execution.context.memory;
    let owned = column_batch(&plan.schema, memory, batch)?;
    let schema = crate::runtime_schema::schema_bound(&plan.schema)?;
    let bound = owned
        .batch()
        .get_array_memory_size()
        .checked_mul(6)
        .and_then(|bytes| bytes.checked_add(schema.checked_mul(8)?))
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(size_error)?;
    let _scratch = memory.reserve(bound)?;
    let mut ipc =
        StreamWriter::try_new(Vec::new(), owned.batch().schema().as_ref()).map_err(ipc_error)?;
    ipc.write(owned.batch()).map_err(ipc_error)?;
    writer.write_frame(&ipc.into_inner().map_err(ipc_error)?)
}

fn read_batch(reader: &mut SpillReader) -> Result<Option<ColumnBatch>> {
    let Some((bytes, mut reservation)) = reader.read_frame(6)? else {
        return Ok(None);
    };
    let mut ipc = StreamReader::try_new(Cursor::new(bytes), None).map_err(ipc_error)?;
    let batch = ipc
        .next()
        .ok_or_else(|| invalid("invalid-spill", "Sort spill frame contains no batch"))?
        .map_err(ipc_error)?;
    if batch.num_rows() == 0 || ipc.next().transpose().map_err(ipc_error)?.is_some() {
        return Err(invalid(
            "invalid-spill",
            "Sort spill frame must contain one nonempty batch",
        ));
    }
    drop(ipc);
    let bytes = batch
        .get_array_memory_size()
        .checked_add(256)
        .ok_or_else(size_error)?;
    let held = Arc::get_mut(&mut reservation).expect("spill decoder owns its reservation");
    if bytes > held.bytes() {
        held.grow_to(bytes)?;
    } else {
        held.shrink_to(bytes)?;
    }
    ColumnBatch::from_owned(OwnedBatch::with_reservation(batch, reservation)?).map(Some)
}

fn ipc_error(error: arrow_schema::ArrowError) -> rtdb_types::Anomaly {
    invalid("sort-spill-arrow", error)
}
