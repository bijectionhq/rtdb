//! An inner equi-join of two batch inputs on column equalities.
//! It builds its left input in memory and emits pages of columns, matching the row join's output row for row:
//! build rows are hashed in input order into the same index, and each probe row takes its candidates in that order.
use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{Array, UInt32Array};
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{BinaryOp, Expr};
use rtdb_types::{Result, Value};

use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch};
use crate::context::Execution;
use crate::invalid;
use crate::join_key::JoinLayout;
use crate::join_storage::{HashIndex, hash_values};
use crate::operators::DeferredInput;
use crate::pages::{BoxPageCursor, OwnedPage, PageCursor};
use crate::pushdown::{PushdownBuilder, Pushdowns};
use crate::runtime_schema::EmittedRelation;

/// For each equality, whether its first operand is the build side's key; `None` where the join needs the row join.
pub(crate) fn key_order(layout: &JoinLayout) -> Option<Vec<bool>> {
    if !layout.left.computes_no_keys() || !layout.right.computes_no_keys() || layout.comparisons.len() != layout.left.key_count() { return None; }
    layout.comparisons.iter().map(|comparison| match comparison {
        Expr::Binary { op: BinaryOp::Eq, left, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: first, data_type: first_type }, Expr::Column { data_type: second_type, .. })
                if rtdb_plan::validate_equality_types(first_type, second_type).is_ok() => Some(layout.left.original.get(*first).is_some()),
            _ => None,
        },
        _ => None,
    }).collect()
}

pub(crate) struct ProbeInput { pub runtime: Arc<EmittedRelation>, pub execution: Execution, pub pushdowns: Option<Arc<Pushdowns>> }

enum State {
    Build { build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout },
    Probe { table: Table, probe: BoxBatchCursor },
    Rows(BoxPageCursor),
    Done,
}

/// The build side: its batches, the key cells of every row in order, and the index over their hashes.
struct Table { batches: Vec<ColumnBatch>, starts: Vec<usize>, keys: Vec<Value>, index: HashIndex, _keys: Arc<MemoryReservation> }

pub(crate) struct BatchJoin { state: State, spec: Spec }

/// What the join compares and how it reads cells, fixed for its lifetime.
struct Spec { order: Vec<bool>, build_keys: Vec<usize>, probe_keys: Vec<usize>, projected: bool, execution: Execution }

impl BatchJoin {
    pub fn new(build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout, order: Vec<bool>, projected: bool, execution: Execution) -> Self {
        let build_keys = layout.left.key_positions().to_vec();
        let probe_keys = layout.right.key_positions().to_vec();
        Self { state: State::Build { build, probe, layout }, spec: Spec { order, build_keys, probe_keys, projected, execution } }
    }
}

impl Spec {
    fn cell(&self, column: &Column, row: usize) -> Result<Value> {
        let mut value = column.value(row)?;
        if self.projected { value.admit_expression_dates()?; }
        Ok(value)
    }

    async fn build(&mut self, mut build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let memory = Arc::clone(&self.execution.context.memory);
        let threshold = self.execution.context.options.join_build_rows;
        let mut batches = Vec::new();
        let mut rows = 0usize;
        while let Some(batch) = build.next_batch().await? {
            self.execution.context.check_cancelled()?;
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            batches.push(batch);
            if i64::try_from(rows).map_or(true, |rows| rows > threshold) { return self.spill(batches, build, probe, layout); }
        }
        let width = self.build_keys.len();
        let mut reservation = memory.reserve(rows.checked_mul(width).and_then(|cells| cells.checked_mul(std::mem::size_of::<Value>())).and_then(|bytes| bytes.checked_add(rows.checked_mul(std::mem::size_of::<u32>() + std::mem::size_of::<usize>())?)).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?)?;
        let mut keys = Vec::with_capacity(rows * width);
        let mut hashes = Vec::with_capacity(rows);
        let mut starts = Vec::with_capacity(batches.len());
        let mut start = 0;
        for batch in &batches {
            starts.push(start);
            start += batch.len;
            for row in 0..batch.len {
                let first = keys.len();
                for &position in &self.build_keys { keys.push(self.cell(&batch.columns[position], row)?); }
                hashes.push(hash_values(&keys[first..])?);
            }
        }
        let held = keys.iter().try_fold(0usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error))?;
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("join-size", "A join key charge is shared"))?;
        lease.grow_to(lease.bytes().checked_add(held).ok_or_else(size_error)?)?;
        let index = HashIndex::new(&hashes, &memory)?;
        drop(hashes);
        let mut filters = PushdownBuilder::new(layout.condition.as_ref(), &layout.left.schema, &layout.right.original, i64::try_from(rows).map_err(|_| size_error())?, true, &self.execution)?;
        let mut scratch = vec![Value::Null; layout.left.schema.len()];
        for row in 0..rows {
            for (index, &position) in self.build_keys.iter().enumerate() { scratch[position] = keys[row * width + index].clone(); }
            filters.insert(&layout.left.schema, &scratch, &self.execution).await?;
        }
        drop(scratch);
        let filters = filters.finish(&memory)?;
        let pushdowns = Pushdowns::merge(probe.pushdowns.as_ref(), filters, &memory)?;
        let Some((probe_batches, probe_projected)) = crate::operators::build_batches(probe.runtime, probe.execution, pushdowns).await? else {
            return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's probe input did not open as batches"));
        };
        if probe_projected != self.projected { return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's inputs disagree on date admission")); }
        Ok(State::Probe { table: Table { batches, starts, keys, index, _keys: reservation }, probe: probe_batches })
    }

    /// Hands the input read so far, and the rest, to the row join, which spills a build side this large.
    fn spill(&self, held: Vec<ColumnBatch>, rest: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let memory = &self.execution.context.memory;
        let pages = held.iter().map(|batch| batch.rows_admitting(memory, self.projected)).collect::<Result<VecDeque<_>>>()?;
        drop(held);
        let rest: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(rest, Arc::clone(memory)).admitting_dates(self.projected));
        let build: BoxPageCursor = Box::new(HeldThen { pages, rest });
        let probe = DeferredInput::new(probe.runtime, probe.execution, crate::inspection::Constraints::default(), probe.pushdowns);
        Ok(State::Rows(Box::new(crate::join::JoinCursor::new(build, probe, layout, true, rtdb_plan::JoinKind::Inner, self.execution.clone())?)))
    }

    fn probe_batch(&self, table: &Table, probe: &ColumnBatch) -> Result<Option<ColumnBatch>> {
        let equality = self.execution.scope()?;
        let width = self.build_keys.len();
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        let mut probes: Vec<u32> = Vec::new();
        let mut key = Vec::with_capacity(width);
        let mut matches = self.execution.context.memory.reserve(4096)?;
        for row in 0..probe.len {
            self.execution.context.check_cancelled()?;
            key.clear();
            for &position in &self.probe_keys { key.push(self.cell(&probe.columns[position], row)?); }
            'candidates: for build in table.index.candidates(hash_values(&key)?) {
                let mut unknown = false;
                for (index, build_first) in self.order.iter().enumerate() {
                    let (build_cell, probe_cell) = (&table.keys[build * width + index], &key[index]);
                    let (first, second) = if *build_first { (build_cell, probe_cell) } else { (probe_cell, build_cell) };
                    let equal = rtdb_expr::sql_eq_in_timezone(first, second, &self.execution.context.timezone, &equality)?;
                    equality.reset()?;
                    match equal { None => unknown = true, Some(false) => continue 'candidates, Some(true) => {} }
                }
                if unknown { continue; }
                if pairs.len() == pairs.capacity() {
                    let capacity = pairs.capacity().saturating_mul(2).max(1024);
                    let lease = Arc::get_mut(&mut matches).ok_or_else(|| invalid("join-size", "A join match charge is shared"))?;
                    lease.grow_to(capacity.checked_mul(std::mem::size_of::<(usize, usize)>() + std::mem::size_of::<u32>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?)?;
                    pairs.reserve_exact(capacity - pairs.len());
                    probes.reserve_exact(capacity - probes.len());
                }
                let batch = table.starts.partition_point(|start| *start <= build) - 1;
                pairs.push((batch, build - table.starts[batch]));
                probes.push(u32::try_from(row).map_err(|_| size_error())?);
            }
        }
        if pairs.is_empty() { return Ok(None); }
        let mut reservation = self.execution.context.memory.reserve(256)?;
        let mut columns = Vec::with_capacity(table.batches.first().map_or(0, |batch| batch.columns.len()) + probe.columns.len());
        let build_width = table.batches.first().map_or(0, |batch| batch.columns.len());
        for position in 0..build_width { columns.push(gathered(&table.batches, position, &pairs)?); }
        let indices = UInt32Array::from(probes);
        for column in &probe.columns { columns.push(taken(column, &indices)?); }
        for column in &columns {
            let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("join-size", "A join output charge is shared"))?;
            lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
        }
        drop(matches);
        ColumnBatch::settled(columns, pairs.len(), reservation).map(Some)
    }
}

/// A build column across the batches the matches come from, as one array where every batch stores it alike.
fn gathered(batches: &[ColumnBatch], position: usize, pairs: &[(usize, usize)]) -> Result<Column> {
    let arrow = batches.iter().map(|batch| match &batch.columns[position] { Column::Arrow { array, field, logical } => Some((array, field, logical)), Column::Values(_) => None }).collect::<Option<Vec<_>>>();
    if let Some(arrow) = arrow && let Some((_, field, logical)) = arrow.first()
        && crate::columnar::flat(field.data_type()) && arrow.iter().all(|(_, other, other_logical)| other == field && other_logical == logical) {
        let arrays: Vec<&dyn Array> = arrow.iter().map(|(array, ..)| array.as_ref()).collect();
        let array = crate::columnar::interleave(&arrays, pairs)?;
        return Ok(Column::Arrow { array, field: Arc::clone(field), logical: (*logical).clone() });
    }
    pairs.iter().map(|&(batch, row)| batches[batch].columns[position].value(row)).collect::<Result<Vec<_>>>().map(Column::Values)
}

/// A probe column at the probe rows that matched, in match order.
fn taken(column: &Column, indices: &UInt32Array) -> Result<Column> {
    match column {
        Column::Arrow { array, field, logical } if crate::columnar::flat(field.data_type()) =>
            Ok(Column::Arrow { array: arrow_select::take::take(array.as_ref(), indices, None).map_err(|error| invalid("join-gather", error))?, field: Arc::clone(field), logical: logical.clone() }),
        column => indices.values().iter().map(|row| column.value(*row as usize)).collect::<Result<Vec<_>>>().map(Column::Values),
    }
}

#[async_trait::async_trait]
impl BatchCursor for BatchJoin {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.spec.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, State::Done) {
                State::Build { build, probe, layout } => { self.state = self.spec.build(build, probe, layout).await?; }
                State::Probe { table, mut probe } => {
                    let Some(batch) = probe.next_batch().await? else { return Ok(None) };
                    let output = self.spec.probe_batch(&table, &batch)?;
                    self.state = State::Probe { table, probe };
                    if let Some(output) = output { return Ok(Some(output)); }
                }
                State::Rows(mut rows) => {
                    let Some(page) = rows.next_page().await? else { return Ok(None) };
                    let batch = ColumnBatch::from_page(&page, &self.spec.execution.context.memory)?;
                    self.state = State::Rows(rows);
                    if batch.len > 0 { return Ok(Some(batch)); }
                }
                State::Done => return Ok(None),
            }
        }
    }
}

/// Pages already read from the build input, then the rest of it.
struct HeldThen { pages: VecDeque<OwnedPage>, rest: BoxPageCursor }
#[async_trait::async_trait]
impl PageCursor for HeldThen {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        if let Some(page) = self.pages.pop_front() { return Ok(Some(page)); }
        self.rest.next_page().await
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size", "Join output exceeds addressable memory") }

