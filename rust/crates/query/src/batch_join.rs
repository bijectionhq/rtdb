//! Equi-joins of batch inputs on column equalities.
//! Build rows are indexed in input order and the probe gathers its matches from the saved column positions.
use std::{collections::VecDeque, ops::ControlFlow};
use std::sync::Arc;

use arrow_array::Array;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{BinaryOp, Expr, JoinKind};
use rtdb_types::{Result, Value};

use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_column_rows, gather_nullable_column};
use crate::context::Execution;
use crate::invalid;
use crate::join_key::JoinLayout;
use crate::join_spill::{BlockWriter, Partitions, compatible_schema};
use crate::join_storage::{HashIndex, combine_cell_hash, hash_values};
use crate::operators::DeferredInput;
use crate::pages::{BoxPageCursor, OwnedPage, PageCursor};
use crate::pushdown::{PushdownBuilder, Pushdowns};
use crate::runtime_schema::EmittedRelation;
use crate::scalar_key::ScalarKey;

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

#[derive(Clone)]
pub(crate) struct ProbeInput { pub runtime: Arc<EmittedRelation>, pub execution: Execution, pub pushdowns: Option<Arc<Pushdowns>> }

enum State {
    Build { build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout },
    Probe { table: Table, probe: BoxBatchCursor },
    Disk { join: DiskJoin, restart: Option<(ProbeInput, ProbeInput, JoinLayout)> },
    Rows(BoxPageCursor),
    Done,
}

struct Table {
    batches: Vec<ColumnBatch>,
    starts: Vec<usize>,
    keys: BuildKeys,
    index: HashIndex,
    rows: usize,
    unmatched: Option<(Vec<u8>, Arc<MemoryReservation>)>,
    _keys: Arc<MemoryReservation>,
    _inventory: Arc<MemoryReservation>,
}

enum BuildKeys { Arrow, Values { keys: Vec<Value>, kind: Option<KeyKind> } }

impl Table {
    fn position(&self, ordinal: usize) -> (usize, usize) {
        let batch = self.starts.partition_point(|&start| start <= ordinal) - 1;
        (batch, ordinal - self.starts[batch])
    }
}

struct DiskJoin {
    build: Partitions,
    probe: Partitions,
    table: Option<Table>,
    build_partition: usize,
    probe_partition: usize,
    ready: Option<(VecDeque<ColumnBatch>, Arc<MemoryReservation>)>,
}

impl DiskJoin {
    fn next(&mut self, spec: &Spec) -> Result<Option<ColumnBatch>> {
        let context = &spec.execution.context;
        let memory = &context.memory;
        loop {
            context.check_cancelled()?;
            if let Some(batch) = self.ready.as_mut().and_then(|(batches, _)| batches.pop_front()) {
                return Ok(Some(batch));
            }
            self.ready = None;
            let builds = self.build.count();
            if self.build_partition == builds { return Ok(None); }
            if self.probe_partition == self.probe.count() / builds {
                self.table = None;
                self.build_partition += 1;
                self.probe_partition = 0;
                continue;
            }
            let inventory = |frames: usize| frames.checked_mul(2 * std::mem::size_of::<ColumnBatch>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error);
            if self.table.is_none() {
                let count = self.build.frames(self.build_partition);
                let reservation = memory.reserve(inventory(count)?)?;
                let mut batches = Vec::with_capacity(count);
                let mut rows = 0usize;
                for frame in 0..count {
                    context.check_cancelled()?;
                    let batch = self.build.read(self.build_partition, frame)?;
                    rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
                    batches.push(batch);
                }
                self.table = Some(spec.table(batches, rows, reservation)?);
            }
            let table = self.table.as_mut().expect("The build partition is loaded");
            let partition = self.probe_partition * builds + self.build_partition;
            let count = self.probe.frames(partition);
            let reservation = memory.reserve(inventory(count)?)?;
            let mut ready = VecDeque::with_capacity(count);
            let mut rows = 0usize;
            // A probe partition's error must surface before any of its matches.
            for frame in 0..count {
                context.check_cancelled()?;
                let batch = self.probe.read(partition, frame)?;
                if let Some(batch) = spec.probe_batch(table, &batch)? {
                    rows = rows.checked_add(batch.len).filter(|rows| *rows <= i32::MAX as usize).ok_or_else(size_error)?;
                    ready.push_back(batch);
                }
            }
            self.ready = Some((ready, reservation));
            self.probe_partition += 1;
        }
    }
}

pub(crate) struct BatchJoin { state: State, spec: Spec }

/// What the join compares and how it reads cells, fixed for its lifetime.
/// `unswapped` is the join's own layout where the join builds its right input, which a spill hands to the row join.
struct Spec {
    order: Vec<bool>, build_keys: Vec<usize>, probe_keys: Vec<usize>, widths: [usize; 2], kind: JoinKind,
    projected: bool, swapped: bool, unswapped: Option<JoinLayout>, validate_payloads: bool, replay_build: Option<ProbeInput>, execution: Execution,
}

impl BatchJoin {
    pub fn new(build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout, order: Vec<bool>, projected: bool, execution: Execution) -> Self {
        let build_keys = layout.left.key_positions().to_vec();
        let probe_keys = layout.right.key_positions().to_vec();
        let widths = [layout.left.original.len(), layout.right.original.len()];
        Self { state: State::Build { build, probe, layout }, spec: Spec { order, build_keys, probe_keys, widths, kind: JoinKind::Inner,
            projected, swapped: false, unswapped: None, validate_payloads: false, replay_build: None, execution } }
    }
}

impl BatchJoin {
    /// With the join's own layout, builds its right input and emits each match's left columns first, as the join's
    /// schema orders them.
    pub fn swapped(mut self, unswapped: Option<JoinLayout>) -> Self { self.spec.swapped = unswapped.is_some(); self.spec.unswapped = unswapped; self }

    pub fn validating_payloads(mut self, validate: bool) -> Self { self.spec.validate_payloads = validate; self }

    pub fn with_kind(mut self, kind: JoinKind) -> Self { self.spec.kind = kind; self }

    pub fn spilling_arrow(mut self, build: Option<ProbeInput>) -> Self { self.spec.replay_build = build; self }
}

impl Spec {
    fn preserves_probe(&self) -> bool {
        crate::join::preserves_probe(self.kind, !self.swapped)
    }
    fn tracks_build(&self) -> bool {
        crate::join::tracks_build(self.kind, !self.swapped)
    }
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
        let mut inventory = memory.reserve(256)?;
        while let Some(batch) = build.next_batch().await? {
            self.execution.context.check_cancelled()?;
            if self.validate_payloads { batch.validate_readable(&self.execution.context)?; }
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            let bytes = batches.len().checked_add(1).and_then(|count| count.checked_mul(4 * std::mem::size_of::<ColumnBatch>()))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            Arc::get_mut(&mut inventory).expect("The join owns its batch inventory").grow_to(bytes)?;
            batches.push(batch);
            if i64::try_from(rows).map_or(true, |rows| rows > threshold) {
                if let Some(replay_build) = self.replay_build.take()
                    && compatible_schema(&layout.left.original) && compatible_schema(&layout.right.original) {
                    let replay_probe = probe.clone();
                    match self.spill_arrow(batches, build, probe, &layout).await {
                        Ok(join) => return Ok(State::Disk { join, restart: Some((replay_build, replay_probe, layout)) }),
                        Err(error) if retry_row_spill(&error) => {
                            drop(inventory);
                            return self.replay_spill(replay_build, replay_probe, layout).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
                return self.spill(batches, build, probe, layout);
            }
        }
        let mut table = self.table(batches, rows, inventory)?;
        let width = self.build_keys.len();
        let blooms = matches!(self.kind, JoinKind::Inner | JoinKind::Semi) || self.tracks_build() && self.kind != JoinKind::Full;
        let mut filters = PushdownBuilder::new(layout.condition.as_ref(), &layout.left.schema, &layout.right.original, i64::try_from(rows).map_err(|_| size_error())?, blooms, &self.execution)?;
        if blooms {
            match &table.keys {
                BuildKeys::Arrow => for batch in &table.batches {
                    for row in 0..batch.len {
                        self.execution.context.check_cancelled_at(row)?;
                        filters.insert_arrow(&layout.left.schema, batch, row, &memory)?;
                    }
                },
                BuildKeys::Values { keys, .. } => {
                    let _scratch = memory.reserve(layout.left.schema.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?)?;
                    let mut scratch = vec![Value::Null; layout.left.schema.len()];
                    for row in 0..rows {
                        for (index, &position) in self.build_keys.iter().enumerate() { scratch[position] = keys[row * width + index].clone(); }
                        filters.insert(&layout.left.schema, &scratch, &self.execution).await?;
                    }
                }
            }
        }
        let filters = filters.finish(&memory)?;
        let pushdowns = Pushdowns::merge(probe.pushdowns.as_ref(), filters, &memory)?;
        let Some((probe_batches, probe_projected)) = crate::operators::build_batches(probe.runtime, probe.execution, pushdowns).await? else {
            return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's probe input did not open as batches"));
        };
        if probe_projected != self.projected { return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's inputs disagree on date admission")); }
        if self.tracks_build() {
            let charge = memory.reserve(rows)?;
            table.unmatched = Some((vec![1; rows], charge));
        }
        Ok(State::Probe { table, probe: probe_batches })
    }

    fn table(&self, batches: Vec<ColumnBatch>, rows: usize, inventory: Arc<MemoryReservation>) -> Result<Table> {
        let memory = &self.execution.context.memory;
        let width = self.build_keys.len();
        let starts_bytes = batches.len().checked_mul(std::mem::size_of::<usize>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let _hashes = memory.reserve(rows.checked_mul(std::mem::size_of::<u32>()).ok_or_else(size_error)?)?;
        let mut reservation = memory.reserve(starts_bytes)?;
        let mut hashes = Vec::with_capacity(rows);
        let mut starts = Vec::with_capacity(batches.len());
        let mut start = 0;
        for batch in &batches {
            starts.push(start);
            start += batch.len;
        }
        let keys = if self.arrow_build_hashes(&batches, &mut hashes)? {
            BuildKeys::Arrow
        } else {
            hashes.clear();
            let mut keys = Vec::new();
            let bytes = rows.checked_mul(width).and_then(|cells| cells.checked_mul(std::mem::size_of::<Value>()))
                .and_then(|bytes| bytes.checked_add(starts_bytes)).ok_or_else(size_error)?;
            let lease = Arc::get_mut(&mut reservation).expect("The join owns its build-key reservation");
            lease.grow_to(bytes)?;
            keys.reserve_exact(rows * width);
            for batch in &batches {
                for row in 0..batch.len {
                    self.execution.context.check_cancelled_at(row)?;
                    let first = keys.len();
                    let bound = key_bound(batch, row, &self.build_keys)?;
                    let retained = lease.bytes();
                    lease.grow_to(retained.checked_add(bound).ok_or_else(size_error)?)?;
                    for &position in &self.build_keys { keys.push(self.cell(&batch.columns[position], row)?); }
                    let actual = keys[first..].iter().try_fold(retained, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error))?;
                    if actual <= lease.bytes() { lease.shrink_to(actual)?; } else { lease.grow_to(actual)?; }
                    hashes.push(hash_values(&keys[first..])?);
                }
            }
            let kind = if width == 1 { KeyKind::of_build(&keys) } else { None };
            BuildKeys::Values { keys, kind }
        };
        let index = HashIndex::new(&hashes, &memory)?;
        drop(hashes);
        drop(_hashes);
        Ok(Table { batches, starts, keys, index, rows, unmatched: None, _keys: reservation, _inventory: inventory })
    }

    fn arrow_build_hashes(&self, batches: &[ColumnBatch], hashes: &mut Vec<u32>) -> Result<bool> {
        for batch in batches {
            for row in 0..batch.len {
                self.execution.context.check_cancelled_at(row)?;
                let Some(hash) = arrow_hash(batch, row, &self.build_keys) else { return Ok(false); };
                hashes.push(hash);
            }
        }
        Ok(true)
    }

    async fn spill_arrow(
        &self, held: Vec<ColumnBatch>, mut rest: BoxBatchCursor, probe: ProbeInput, layout: &JoinLayout,
    ) -> Result<DiskJoin> {
        let context = &self.execution.context;
        let mut build = BlockWriter::new(&layout.left.original, context)?;
        for batch in held { build.append(batch, context)?; }
        while let Some(batch) = rest.next_batch().await? {
            batch.validate_readable(context)?;
            build.append(batch, context)?;
        }
        drop(rest);
        let build = build.finish(context)?.shuffle(1, context, |batch, row| self.spill_hash(batch, row, &self.build_keys))?;
        let Some((mut input, projected)) = crate::operators::build_batches(probe.runtime, probe.execution, probe.pushdowns).await? else {
            return Err(crate::join_spill::layout_error());
        };
        if projected != self.projected { return Err(crate::join_spill::layout_error()); }
        let mut probe = BlockWriter::new(&layout.right.original, context)?;
        while let Some(batch) = input.next_batch().await? {
            batch.validate_readable(context)?;
            probe.append(batch, context)?;
        }
        let probe = probe.finish(context)?.shuffle(build.count(), context, |batch, row| self.spill_hash(batch, row, &self.probe_keys))?;
        Ok(DiskJoin { build, probe, table: None, build_partition: 0, probe_partition: 0, ready: None })
    }

    fn spill_hash(&self, batch: &ColumnBatch, row: usize, positions: &[usize]) -> Result<u32> {
        if let Some(hash) = arrow_hash(batch, row, positions) { return Ok(hash); }
        let (values, _reservation) = self.value_keys(batch, row, positions)?;
        hash_values(&values)
    }

    async fn replay_spill(&self, build: ProbeInput, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let open = |input: ProbeInput| DeferredInput::new(
            input.runtime, input.execution, crate::inspection::Constraints::default(), input.pushdowns,
        );
        let build = open(build).open_pages(None).await?;
        Ok(State::Rows(Box::new(crate::join::JoinCursor::new(build, open(probe), layout, true, self.kind, self.execution.clone())?)))
    }

    /// Hands the input read so far, and the rest, to the row join, which spills a build side this large.
    fn spill(&mut self, held: Vec<ColumnBatch>, rest: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let memory = &self.execution.context.memory;
        let pages = held.iter().map(|batch| batch.rows_admitting(memory, self.projected)).collect::<Result<VecDeque<_>>>()?;
        drop(held);
        let rest: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(rest, Arc::clone(memory)).admitting_dates(self.projected));
        let build: BoxPageCursor = Box::new(HeldThen { pages, rest });
        let probe = DeferredInput::new(probe.runtime, probe.execution, crate::inspection::Constraints::default(), probe.pushdowns);
        let (layout, build_left) = match self.unswapped.take() { Some(unswapped) => (unswapped, false), None => (layout, true) };
        Ok(State::Rows(Box::new(crate::join::JoinCursor::new(build, probe, layout, build_left, self.kind, self.execution.clone())?)))
    }

    fn probe_batch(&self, table: &mut Table, probe: &ColumnBatch) -> Result<Option<ColumnBatch>> {
        if self.validate_payloads { probe.validate_readable(&self.execution.context)?; }
        match self.kind {
            JoinKind::Semi | JoinKind::Anti => return self.probe_membership(table, probe),
            JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Single => return self.probe_outer(table, probe),
            _ => {}
        }
        let mut matched = Matches { pairs: Vec::new(), probes: Vec::new(), charge: self.execution.context.memory.reserve(4096)? };
        match &table.keys {
            BuildKeys::Arrow => self.probe_arrow_keys(table, probe, &mut matched)?,
            BuildKeys::Values { keys, kind } => self.probe_value_keys(table, probe, keys, *kind, &mut matched)?,
        }
        let Matches { pairs, probes, charge: matches } = matched;
        if pairs.is_empty() { return Ok(None); }
        let memory = &self.execution.context.memory;
        let build_width = table.batches.first().map_or(0, |batch| batch.columns.len());
        let width = build_width.checked_add(probe.columns.len()).ok_or_else(size_error)?;
        let bound = width.checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bound)?;
        let mut columns = Vec::with_capacity(width);
        let mut append = |batches: &[ColumnBatch], rows: &[(usize, usize)], width| -> Result<()> {
            for position in 0..width {
                let (column, _scratch) = gather_column_rows(|batch| &batches[batch], rows, position, memory)?;
                let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("join-size", "A join output charge is shared"))?;
                lease.grow_to(lease.bytes().checked_add(column.retained_bytes()?).ok_or_else(size_error)?)?;
                columns.push(column);
            }
            Ok(())
        };
        if self.swapped { append(std::slice::from_ref(probe), &probes, probe.columns.len())?; }
        append(&table.batches, &pairs, build_width)?;
        if !self.swapped { append(std::slice::from_ref(probe), &probes, probe.columns.len())?; }
        drop(matches);
        ColumnBatch::settled(columns, pairs.len(), reservation).map(Some)
    }

    fn probe_membership(&self, table: &Table, probe: &ColumnBatch) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let memory = &context.memory;
        let bytes = probe.len.checked_mul(std::mem::size_of::<(usize, usize)>())
            .and_then(|bytes| bytes.checked_add(probe.columns.len().checked_mul(std::mem::size_of::<usize>())?))
            .ok_or_else(size_error)?;
        let _selection = memory.reserve(bytes)?;
        let mut selected = Vec::with_capacity(probe.len);
        let equality = self.execution.scope()?;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let matched = self.matching_build_rows(table, probe, row, &equality, |_| Ok(ControlFlow::Break(())))?;
            if matched == (self.kind == JoinKind::Semi) { selected.push((0, row)); }
        }
        if selected.is_empty() { return Ok(None); }
        let columns = (0..probe.columns.len()).collect::<Vec<_>>();
        ColumnBatch::take_rows(&[probe], &selected, &columns, memory).map(Some)
    }

    fn matching_build_rows(
        &self, table: &Table, probe: &ColumnBatch, row: usize, equality: &rtdb_expr::EvaluationScope,
        mut accept: impl FnMut(usize) -> Result<ControlFlow<()>>,
    ) -> Result<bool> {
        let mut probe_values = None;
        let mut hashed = if self.kind != JoinKind::Anti {
            let hash = match arrow_hash(probe, row, &self.probe_keys) {
                Some(hash) => hash,
                None => {
                    let key = self.value_keys(probe, row, &self.probe_keys)?;
                    let hash = hash_values(&key.0)?;
                    probe_values = Some(key);
                    hash
                }
            };
            Some(table.index.candidates(hash))
        } else {
            None
        };
        // Anti joins test the build side in source order, including candidates with different hashes.
        let mut all = 0..table.rows;
        let candidates = std::iter::from_fn(|| match &mut hashed { Some(rows) => rows.next(), None => all.next() });
        let mut matched = false;
        let width = self.build_keys.len();
        for (candidate, ordinal) in candidates.enumerate() {
            self.execution.context.check_cancelled_at(candidate)?;
            let (batch, offset) = table.position(ordinal);
            let build = &table.batches[batch];
            let equal = if let Some(equal) = self.equal_arrow(build, offset, probe, row) {
                equal
            } else {
                if probe_values.is_none() { probe_values = Some(self.value_keys(probe, row, &self.probe_keys)?); }
                let key = &probe_values.as_ref().expect("The probe key was read").0;
                match &table.keys {
                    BuildKeys::Arrow => {
                        let (build, _scratch) = self.value_keys(build, offset, &self.build_keys)?;
                        self.equal_values(&build, key, equality)?
                    }
                    BuildKeys::Values { keys, .. } => self.equal_values(&keys[ordinal * width..(ordinal + 1) * width], key, equality)?,
                }
            };
            if equal {
                matched = true;
                if accept(ordinal)?.is_break() { break; }
            }
        }
        Ok(matched)
    }

    fn probe_outer(&self, table: &mut Table, probe: &ColumnBatch) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let mut matches = OuterPositions::new(&context.memory)?;
        let equality = self.execution.scope()?;
        let mut unmatched = table.unmatched.take();
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let mut matched_once = false;
            let matched = self.matching_build_rows(table, probe, row, &equality, |ordinal| {
                if matched_once && self.kind == JoinKind::Single { return Err(crate::cardinality_violation()); }
                matched_once = true;
                if let Some((unmatched, _)) = &mut unmatched { unmatched[ordinal] = 0; }
                matches.push(Some(table.position(ordinal)), Some((0, row)))?;
                Ok(ControlFlow::Continue(()))
            })?;
            if !matched && self.preserves_probe() { matches.push(None, Some((0, row)))?; }
        }
        table.unmatched = unmatched;
        self.gather_outer(table, Some(probe), matches)
    }

    fn unmatched_build(&self, table: &Table) -> Result<Option<ColumnBatch>> {
        let Some((unmatched, _)) = &table.unmatched else { return Ok(None); };
        let mut matches = OuterPositions::new(&self.execution.context.memory)?;
        for (ordinal, flag) in unmatched.iter().enumerate() {
            self.execution.context.check_cancelled_at(ordinal)?;
            if *flag != 0 { matches.push(Some(table.position(ordinal)), None)?; }
        }
        self.gather_outer(table, None, matches)
    }

    fn gather_outer(&self, table: &Table, probe: Option<&ColumnBatch>, positions: OuterPositions) -> Result<Option<ColumnBatch>> {
        let count = positions.build.len();
        if count == 0 { return Ok(None); }
        let memory = &self.execution.context.memory;
        let mut columns = ColumnBatch::settled(vec![], count, memory.reserve(256)?)?;
        let mut sides = [
            (table.batches.as_slice(), self.widths[0], &positions.build),
            (probe.map_or(&[][..], std::slice::from_ref), self.widths[1], &positions.probe),
        ];
        if self.swapped { sides.swap(0, 1); }
        for (batches, width, rows) in sides {
            for position in 0..width {
                let (column, lease) = gather_nullable_column(|index| &batches[index], rows, position, memory)?;
                columns.append_column(column, lease, memory)?;
            }
        }
        Ok(Some(columns))
    }

    fn probe_value_keys(&self, table: &Table, probe: &ColumnBatch, keys: &[Value], kind: Option<KeyKind>, matches: &mut Matches) -> Result<()> {
        if let Some(key) = kind.and_then(|kind| ProbeKey::of(&probe.columns[self.probe_keys[0]], kind)) {
            for row in 0..probe.len {
                self.execution.context.check_cancelled_at(row)?;
                let Some(hash) = key.hash(row) else { continue };
                for (candidate, build) in table.index.candidates(hash).enumerate() {
                    self.execution.context.check_cancelled_at(candidate)?;
                    if key.equals(row, &keys[build]) { matches.push(table, build, row)?; }
                }
            }
            return Ok(());
        }
        let width = self.build_keys.len();
        let equality = self.execution.scope()?;
        for row in 0..probe.len {
            self.execution.context.check_cancelled()?;
            let (key, _scratch) = self.value_keys(probe, row, &self.probe_keys)?;
            for (candidate, build) in table.index.candidates(hash_values(&key)?).enumerate() {
                self.execution.context.check_cancelled_at(candidate)?;
                if self.equal_values(&keys[build * width..(build + 1) * width], &key, &equality)? {
                    matches.push(table, build, row)?;
                }
            }
        }
        Ok(())
    }

    fn probe_arrow_keys(&self, table: &Table, probe: &ColumnBatch, matches: &mut Matches) -> Result<()> {
        for row in 0..probe.len {
            self.execution.context.check_cancelled_at(row)?;
            let mut probe_values = None;
            let hash = match arrow_hash(probe, row, &self.probe_keys) {
                Some(hash) => hash,
                None => {
                    let key = self.value_keys(probe, row, &self.probe_keys)?;
                    let hash = hash_values(&key.0)?;
                    probe_values = Some(key);
                    hash
                }
            };
            for (candidate, build) in table.index.candidates(hash).enumerate() {
                self.execution.context.check_cancelled_at(candidate)?;
                let (batch_index, build_row) = table.position(build);
                let batch = &table.batches[batch_index];
                let typed = if probe_values.is_none() { self.equal_arrow(batch, build_row, probe, row) } else { None };
                let matched = match typed {
                    Some(matched) => matched,
                    None => {
                        if probe_values.is_none() {
                            probe_values = Some(self.value_keys(probe, row, &self.probe_keys)?);
                        }
                        let (probe_key, _) = probe_values.as_ref().expect("A fallback probe key has been read");
                        let (build_key, _scratch) = self.value_keys(batch, build_row, &self.build_keys)?;
                        self.equal_values(&build_key, probe_key, &self.execution.scope()?)?
                    }
                };
                if matched { matches.push(table, build, row)?; }
            }
        }
        Ok(())
    }

    fn equal_arrow(&self, build: &ColumnBatch, build_row: usize, probe: &ColumnBatch, probe_row: usize) -> Option<bool> {
        let mut matched = true;
        for (&left, &right) in self.build_keys.iter().zip(&self.probe_keys) {
            let left = ScalarKey::read(&build.columns[left], build_row)?;
            let right = ScalarKey::read(&probe.columns[right], probe_row)?;
            matched &= left.equal(right)? && (self.preserves_probe() || left != ScalarKey::Null);
        }
        Some(matched)
    }

    fn equal_values(&self, build: &[Value], probe: &[Value], equality: &rtdb_expr::EvaluationScope) -> Result<bool> {
        let mut unknown = false;
        for ((build, probe), build_first) in build.iter().zip(probe).zip(&self.order) {
            if self.preserves_probe() {
                let build_null = matches!(build, Value::Null | Value::Undefined);
                let probe_null = matches!(probe, Value::Null | Value::Undefined);
                if build_null || probe_null {
                    if build_null != probe_null { return Ok(false); }
                    continue;
                }
            }
            let (left, right) = if *build_first { (build, probe) } else { (probe, build) };
            let equal = rtdb_expr::sql_eq_in_timezone(left, right, &self.execution.context.timezone, equality)?;
            equality.reset()?;
            match equal {
                None => unknown = true,
                Some(false) => return Ok(false),
                Some(true) => {}
            }
        }
        Ok(!unknown)
    }

    fn value_keys(&self, batch: &ColumnBatch, row: usize, positions: &[usize]) -> Result<(Vec<Value>, Arc<MemoryReservation>)> {
        let reservation = self.execution.context.memory.reserve(key_bound(batch, row, positions)?)?;
        let keys = positions.iter().map(|&position| self.cell(&batch.columns[position], row)).collect::<Result<_>>()?;
        Ok((keys, reservation))
    }
}

fn arrow_hash(batch: &ColumnBatch, row: usize, positions: &[usize]) -> Option<u32> {
    positions.iter().try_fold(0, |hash, &position| {
        ScalarKey::read(&batch.columns[position], row).map(|key| combine_cell_hash(hash, key.hash() as i32))
    })
}

fn retry_row_spill(error: &rtdb_types::Anomaly) -> bool {
    matches!(error.code.as_str(), "rtdb.query/join-spill-layout" | "rtdb.query/spill-limit" | "rtdb/memory-budget-exceeded")
}

fn key_bound(batch: &ColumnBatch, row: usize, positions: &[usize]) -> Result<usize> {
    let base = positions.len().checked_mul(std::mem::size_of::<Value>())
        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
    positions.iter().try_fold(base, |bytes, &position| {
        batch.columns[position].bound(row)?.checked_mul(2).and_then(|bound| bytes.checked_add(bound)).ok_or_else(size_error)
    })
}

/// The build and probe rows a probe batch matched, in probe order, charged as they grow.
struct Matches { pairs: Vec<(usize, usize)>, probes: Vec<(usize, usize)>, charge: Arc<MemoryReservation> }
impl Matches {
    fn push(&mut self, table: &Table, build: usize, row: usize) -> Result<()> {
        if self.pairs.len() == self.pairs.capacity() {
            let capacity = self.pairs.capacity().saturating_mul(2).max(1024);
            let lease = Arc::get_mut(&mut self.charge).ok_or_else(|| invalid("join-size", "A join match charge is shared"))?;
            lease.grow_to(capacity.checked_mul(2 * std::mem::size_of::<(usize, usize)>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?)?;
            self.pairs.reserve_exact(capacity - self.pairs.len());
            self.probes.reserve_exact(capacity - self.probes.len());
        }
        self.pairs.push(table.position(build));
        self.probes.push((0, row));
        Ok(())
    }
}

type OptionalPosition = Option<(usize, usize)>;

struct OuterPositions {
    build: Vec<OptionalPosition>,
    probe: Vec<OptionalPosition>,
    reservation: Arc<MemoryReservation>,
}

impl OuterPositions {
    fn new(memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<Self> {
        Ok(Self { build: Vec::new(), probe: Vec::new(), reservation: memory.reserve(256)? })
    }

    fn push(&mut self, build: OptionalPosition, probe: OptionalPosition) -> Result<()> {
        if self.build.len() == i32::MAX as usize { return Err(size_error()); }
        if self.build.len() == self.build.capacity() {
            let capacity = self.build.capacity().saturating_mul(2).clamp(16, i32::MAX as usize);
            let bytes = capacity.checked_mul(2 * std::mem::size_of::<OptionalPosition>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let replacement = self.build.capacity().max(self.probe.capacity())
                .checked_mul(std::mem::size_of::<OptionalPosition>()).ok_or_else(size_error)?;
            let _replacement = self.reservation.budget().reserve(replacement)?;
            Arc::get_mut(&mut self.reservation).expect("Outer join owns its match positions").grow_to(bytes)?;
            self.build.reserve_exact(capacity - self.build.len());
            self.probe.reserve_exact(capacity - self.probe.len());
        }
        self.build.push(build);
        self.probe.push(probe);
        Ok(())
    }
}

/// A single join key whose build cells are all integers of any width, or all strings: SQL equality of two such cells
/// is equality of the integer or of the bytes, and a null matches nothing, so the probe reads its cells straight
/// from its arrays rather than as values.
#[derive(Clone, Copy, PartialEq, Eq)]
enum KeyKind { Int, Utf8 }
impl KeyKind {
    fn of_build(keys: &[Value]) -> Option<Self> {
        let int = |value: &Value| matches!(value, Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_));
        if keys.iter().all(|key| int(key) || matches!(key, Value::Null)) { return Some(Self::Int); }
        if keys.iter().all(|key| matches!(key, Value::Utf8(_) | Value::Null)) { return Some(Self::Utf8); }
        None
    }
}
fn int_value(value: &Value) -> Option<i64> {
    match value { Value::I8(v) => Some(i64::from(*v)), Value::I16(v) => Some(i64::from(*v)), Value::I32(v) => Some(i64::from(*v)), Value::I64(v) => Some(*v), _ => None }
}

enum ProbeKey<'a> { IntArrow(&'a dyn Array), IntValues(&'a [Value]), Utf8(&'a arrow_array::StringArray) }
impl<'a> ProbeKey<'a> {
    fn of(column: &'a Column, kind: KeyKind) -> Option<Self> {
        match (column, kind) {
            (Column::Arrow { array, field, .. }, KeyKind::Utf8) if field.metadata().is_empty() =>
                array.as_any().downcast_ref::<arrow_array::StringArray>().map(Self::Utf8),
            (Column::Arrow { array, field, .. }, KeyKind::Int) if field.metadata().is_empty() && int_layout(array.as_ref()) =>
                Some(Self::IntArrow(array.as_ref())),
            (Column::Values(values), KeyKind::Int) if values.iter().all(|value| matches!(value, Value::Null) || int_value(value).is_some()) =>
                Some(Self::IntValues(values)),
            _ => None,
        }
    }

    fn integer(&self, row: usize) -> Option<i64> {
        match self {
            Self::IntArrow(array) => int_cell(*array, row),
            Self::IntValues(values) => int_value(&values[row]),
            Self::Utf8(_) => None,
        }
    }

    fn hash(&self, row: usize) -> Option<u32> {
        let key = match self {
            Self::Utf8(array) if !array.is_null(row) => ScalarKey::Text(array.value(row)),
            Self::Utf8(_) => return None,
            _ => ScalarKey::Signed(self.integer(row)?),
        };
        Some(combine_cell_hash(0, key.hash() as i32))
    }

    fn equals(&self, row: usize, build: &Value) -> bool {
        match (self, build) {
            (Self::Utf8(array), Value::Utf8(build)) => !array.is_null(row) && array.value(row) == build.as_str(),
            (Self::Utf8(_), _) => false,
            _ => self.integer(row).is_some_and(|probe| int_value(build) == Some(probe)),
        }
    }
}

fn int_layout(array: &dyn Array) -> bool {
    use arrow_schema::DataType;
    match array.data_type() {
        DataType::Union(legs, arrow_schema::UnionMode::Dense) => legs.iter().all(|(_, leg)| {
            leg.metadata().is_empty() && matches!(leg.data_type(), DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Null)
        }),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => true,
        _ => false,
    }
}

fn int_cell(array: &dyn Array, row: usize) -> Option<i64> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type};
    use arrow_schema::DataType;

    if array.is_null(row) { return None; }
    Some(match array.data_type() {
        DataType::Int8 => i64::from(array.as_primitive::<Int8Type>().value(row)),
        DataType::Int16 => i64::from(array.as_primitive::<Int16Type>().value(row)),
        DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row),
        DataType::Union(..) => {
            let union = array.as_union();
            return int_cell(union.child(union.type_id(row)).as_ref(), union.value_offset(row));
        }
        _ => return None,
    })
}

pub(crate) fn int_cells(array: &dyn Array) -> Option<Vec<Option<i64>>> {
    int_layout(array).then(|| (0..array.len()).map(|row| int_cell(array, row)).collect())
}

#[async_trait::async_trait]
impl BatchCursor for BatchJoin {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.spec.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, State::Done) {
                State::Build { build, probe, layout } => { self.state = self.spec.build(build, probe, layout).await?; }
                State::Probe { mut table, mut probe } => {
                    let Some(batch) = probe.next_batch().await? else { return self.spec.unmatched_build(&table) };
                    let output = self.spec.probe_batch(&mut table, &batch)?;
                    self.state = State::Probe { table, probe };
                    if let Some(output) = output { return Ok(Some(output)); }
                }
                State::Rows(mut rows) => {
                    let Some(page) = rows.next_page().await? else { return Ok(None) };
                    let batch = ColumnBatch::from_page(&page, &self.spec.execution.context.memory)?;
                    self.state = State::Rows(rows);
                    if batch.len > 0 { return Ok(Some(batch)); }
                }
                State::Disk { mut join, restart } => match join.next(&self.spec) {
                    Ok(Some(output)) => {
                        self.state = State::Disk { join, restart: None };
                        return Ok(Some(output));
                    }
                    Ok(None) => return Ok(None),
                    Err(error) if retry_row_spill(&error) => {
                        let Some((build, probe, layout)) = restart else { return Err(error); };
                        drop(join);
                        self.state = self.spec.replay_spill(build, probe, layout).await?;
                    }
                    Err(error) => return Err(error),
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

