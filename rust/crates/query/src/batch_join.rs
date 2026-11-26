//! Equi-joins of batch inputs on column equalities.
//! Build rows are indexed in input order and the probe gathers its matches from the saved column positions.
use std::{collections::VecDeque, ops::ControlFlow};
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{BinaryOp, Expr, JoinKind};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_column_rows, gather_join_probe, gather_nullable_column, poll_nested_batch};
use crate::context::Execution;
use crate::invalid;
use crate::join_key::JoinLayout;
use crate::join_spill::{BlockWriter, Partitions};
use crate::join_storage::HashIndex;
use crate::operators::{BatchOutput, build_batch_input};
use crate::pushdown::{PushdownBuilder, Pushdowns};
use crate::runtime_schema::EmittedRelation;
use crate::scalar_key::ScalarKey;
use crate::set_keys::{Key, direct_key_equal};

/// For each equality, whether its first operand is the build side's key.
pub(crate) fn key_order(layout: &JoinLayout) -> Option<Vec<bool>> {
    layout.comparisons.iter().take(layout.left.key_count()).map(|comparison| match comparison {
        Expr::Binary { op: BinaryOp::Eq, left, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: first, .. }, Expr::Column { .. }) => Some(layout.left.schema.get(*first).is_some()),
            _ => None,
        },
        _ => None,
    }).collect()
}

#[derive(Clone, Copy)]
enum ThetaOperand {
    Build(usize),
    Probe(usize),
    Literal(Option<i128>),
}

pub(crate) struct ThetaTest { left: ThetaOperand, right: ThetaOperand, op: BinaryOp }

/// Whether no value of the type can hold a date, which is all that admitting a projected input's dates changes.
fn date_free(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Nothing | LogicalType::Null => true,
        LogicalType::Scalar(ScalarType::DateDay | ScalarType::DateMilli) => false,
        LogicalType::Scalar(_) => true,
        LogicalType::List { element, .. } => date_free(element),
        LogicalType::Struct(fields) => fields.values().all(date_free),
        LogicalType::Maybe(value) | LogicalType::RunEndEncoded { value, .. } => date_free(value),
        LogicalType::Union(legs) => legs.iter().all(date_free),
    }
}

pub(crate) fn theta_tests(layout: &JoinLayout) -> Option<Vec<ThetaTest>> {
    fn integer(ty: &LogicalType) -> bool {
        match ty {
            LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) | LogicalType::Null => true,
            LogicalType::Maybe(ty) => integer(ty),
            _ => false,
        }
    }
    fn operand(layout: &JoinLayout, expr: &Expr) -> Option<ThetaOperand> {
        match expr {
            Expr::Column { id, .. } => {
                if let Some(position) = layout.left.schema.position(*id)
                    && integer(&layout.left.schema.fields()[position].data_type) {
                    Some(ThetaOperand::Build(position))
                } else {
                    let position = layout.right.schema.position(*id)?;
                    integer(&layout.right.schema.fields()[position].data_type).then_some(ThetaOperand::Probe(position))
                }
            }
            Expr::Literal(Value::Null) => Some(ThetaOperand::Literal(None)),
            Expr::Literal(value) if integer(&value.logical_type()) =>
                rtdb_expr::integer(value).map(|integer| ThetaOperand::Literal(Some(integer))),
            _ => None,
        }
    }
    fn tests(layout: &JoinLayout, expr: &Expr, output: &mut Vec<ThetaTest>) -> Option<()> {
        let Expr::Binary { op, left, right } = expr else { return None; };
        if *op == BinaryOp::And {
            tests(layout, left, output)?;
            tests(layout, right, output)?;
            return Some(());
        }
        if !matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) {
            return None;
        }
        output.push(ThetaTest { left: operand(layout, left)?, right: operand(layout, right)?, op: *op });
        Some(())
    }
    let mut output = Vec::new();
    for expr in layout.comparisons.iter().skip(layout.left.key_count()) { tests(layout, expr, &mut output)?; }
    Some(output)
}

pub(crate) struct Residual {
    schema: rtdb_plan::Schema,
    expressions: Vec<Expr>,
    sides: Vec<(bool, usize)>,
    _reservation: Arc<MemoryReservation>,
}

struct PreparedResidual<'a> {
    comparisons: Vec<rtdb_expr::ExpressionInvocation<'a>>,
    sides: &'a [(bool, usize)],
    _metadata: rtdb_expr::EvaluationScope,
}

struct JoinRow<'a> {
    build: &'a ColumnBatch,
    build_row: usize,
    probe: &'a ColumnBatch,
    probe_row: usize,
    sides: &'a [(bool, usize)],
    admit_dates: [bool; 2],
}

impl JoinRow<'_> {
    fn column(&self, index: usize) -> Option<(&Column, usize, bool)> {
        let &(on_build, column) = self.sides.get(index)?;
        if on_build { Some((self.build.columns.get(column)?, self.build_row, self.admit_dates[0])) }
        else { Some((self.probe.columns.get(column)?, self.probe_row, self.admit_dates[1])) }
    }
}

impl rtdb_expr::RowInput for JoinRow<'_> {
    fn arrow_cell(&self, index: usize) -> Option<rtdb_columnar::ArrowCell<'_>> {
        let (column, row, _) = self.column(index)?;
        column.expression_cell(row)
    }

    fn value(&self, index: usize, allocation: Option<&rtdb_expr::EvaluationScope>) -> Result<Option<Value>> {
        self.column(index).map(|(column, row, admit_dates)| column.expression_value(row, admit_dates, allocation)).transpose()
    }
}

impl Residual {
    pub fn new(layout: &JoinLayout, execution: &Execution) -> Result<Self> {
        let expressions = &layout.comparisons[layout.left.key_count()..];
        let bytes = expressions.iter().try_fold(crate::runtime_schema::schema_bound(&layout.theta_schema)?, |bytes, expr| {
            bytes.checked_add(crate::runtime_schema::expression_bound(expr)?).ok_or_else(size_error)
        })?;
        let reservation = execution.context.memory.reserve(bytes.checked_mul(4).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?)?;
        let columns = expressions.iter().flat_map(Expr::referenced_columns).collect::<std::collections::BTreeSet<_>>();
        let mut sides = Vec::with_capacity(columns.len());
        let mut fields = Vec::with_capacity(columns.len());
        for id in columns {
            if let Some(position) = layout.left.schema.position(id) {
                fields.push(layout.left.schema.fields()[position].clone());
                sides.push((true, position));
            } else if let Some(position) = layout.right.schema.position(id) {
                fields.push(layout.right.schema.fields()[position].clone());
                sides.push((false, position));
            } else {
                return Err(invalid("join-key", "A join predicate column is absent"));
            }
        }
        Ok(Self { schema: rtdb_plan::Schema::new(fields)?, expressions: expressions.to_vec(), sides, _reservation: reservation })
    }

    fn prepare(&self, execution: &Execution) -> Result<PreparedResidual<'_>> {
        let metadata = execution.scope()?;
        metadata.reserve(self.expressions.len().checked_mul(std::mem::size_of::<rtdb_expr::ExpressionInvocation<'_>>()).ok_or_else(size_error)?)?;
        let comparisons = self.expressions.iter().map(|expr| execution.prepare_expression(expr, &self.schema, &metadata)).collect::<Result<_>>()?;
        Ok(PreparedResidual { comparisons, sides: &self.sides, _metadata: metadata })
    }
}

#[derive(Clone)]
pub(crate) struct ProbeInput { pub runtime: Arc<EmittedRelation>, pub execution: Execution, pub pushdowns: Option<Arc<Pushdowns>>, pub output: BatchOutput }

enum State {
    Build { build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout },
    Probe { table: Table, probe: BoxBatchCursor },
    Disk(DiskJoin),
    Done,
}

struct Table {
    batches: Vec<ColumnBatch>,
    starts: Vec<usize>,
    index: HashIndex,
    rows: usize,
    unmatched: Option<(Vec<u8>, Arc<MemoryReservation>)>,
    _starts: Arc<MemoryReservation>,
    _inventory: Arc<MemoryReservation>,
}

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
    async fn next(&mut self, spec: &Spec) -> Result<Option<ColumnBatch>> {
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
                let table = self.table.take().expect("The completed partition has a build table");
                self.build_partition += 1;
                self.probe_partition = 0;
                if let Some(unmatched) = spec.unmatched_build(&table)? { return Ok(Some(unmatched)); }
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
                let mut table = spec.table(batches, rows, reservation)?;
                if spec.tracks_build() {
                    let charge = memory.reserve(rows)?;
                    table.unmatched = Some((vec![1; rows], charge));
                }
                self.table = Some(table);
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
                if let Some(batch) = spec.probe_batch(table, batch).await? {
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

struct Spec {
    order: Vec<bool>, build_keys: Vec<usize>, probe_keys: Vec<usize>, widths: [usize; 2], kind: JoinKind,
    projected: [bool; 2], swapped: bool, _original_layout_charge: Option<Arc<MemoryReservation>>, theta: Vec<ThetaTest>, residual: Option<Residual>, execution: Execution,
}

impl BatchJoin {
    pub fn new(build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout, order: Vec<bool>, projected: [bool; 2], execution: Execution) -> Self {
        let build_keys = layout.left.key_positions().to_vec();
        let probe_keys = layout.right.key_positions().to_vec();
        let widths = [layout.left.original.len(), layout.right.original.len()];
        Self { state: State::Build { build, probe, layout }, spec: Spec { order, build_keys, probe_keys, widths, kind: JoinKind::Inner,
            projected, swapped: false, _original_layout_charge: None, theta: Vec::new(), residual: None, execution } }
    }
}

impl BatchJoin {
    pub fn swapped(mut self, original_layout_charge: Option<Arc<MemoryReservation>>) -> Self {
        self.spec.swapped = original_layout_charge.is_some();
        self.spec._original_layout_charge = original_layout_charge;
        self
    }

    pub fn with_kind(mut self, kind: JoinKind) -> Self { self.spec.kind = kind; self }

    pub fn with_theta(mut self, theta: Vec<ThetaTest>) -> Self { self.spec.theta = theta; self }
    pub fn with_residual(mut self, residual: Option<Residual>) -> Self { self.spec.residual = residual; self }

}

impl Spec {
    fn preserves_probe(&self) -> bool {
        crate::join::preserves_probe(self.kind, !self.swapped)
    }
    fn tracks_build(&self) -> bool {
        crate::join::tracks_build(self.kind, !self.swapped)
    }
    fn cell(&self, column: &Column, row: usize, side: usize) -> Result<Value> {
        let mut value = column.value(row)?;
        if self.projected[side] { value.admit_expression_dates()?; }
        Ok(value)
    }

    async fn build(&mut self, build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let mut build = layout.left.project_batches(build, &self.execution);
        let memory = Arc::clone(&self.execution.context.memory);
        let threshold = self.execution.context.options.join_build_rows;
        let mut batches = Vec::new();
        let mut rows = 0usize;
        let mut inventory = memory.reserve(256)?;
        while let Some(batch) = poll_nested_batch(build.as_mut()).await? {
            self.execution.context.check_cancelled()?;
            batch.validate_readable(&self.execution.context)?;
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            let bytes = batches.len().checked_add(1).and_then(|count| count.checked_mul(4 * std::mem::size_of::<ColumnBatch>()))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            Arc::get_mut(&mut inventory).expect("The join owns its batch inventory").grow_to(bytes)?;
            batches.push(batch);
            if i64::try_from(rows).map_or(true, |rows| rows > threshold) {
                drop(inventory);
                return self.spill_arrow(batches, build, probe, &layout).await.map(State::Disk);
            }
        }
        let mut table = self.table(batches, rows, inventory)?;
        let blooms = matches!(self.kind, JoinKind::Inner | JoinKind::Semi) || self.tracks_build() && self.kind != JoinKind::Full;
        let mut filters = PushdownBuilder::new(layout.condition.as_ref(), &layout.left.schema, &layout.right.original, i64::try_from(rows).map_err(|_| size_error())?, blooms, &self.execution)?;
        if blooms {
            for batch in &table.batches {
                for row in 0..batch.len {
                    self.execution.context.check_cancelled_at(row)?;
                    filters.insert_arrow(&layout.left.schema, batch, row, &memory)?;
                }
            }
        }
        let filters = filters.finish(&memory)?;
        let pushdowns = Pushdowns::merge(probe.pushdowns.as_ref(), filters, &memory)?;
        let probe_batches = match build_batch_input(Arc::clone(&probe.runtime), probe.execution.clone(), pushdowns.clone(), probe.output).await? {
            Some((batches, projected)) if projected == self.projected[1] => batches,
            _ => return Err(rtdb_types::Anomaly::fault("rtdb.query/batch-join", "A batch join's probe input did not open as batches")),
        };
        if self.tracks_build() {
            let charge = memory.reserve(rows)?;
            table.unmatched = Some((vec![1; rows], charge));
        }
        Ok(State::Probe { table, probe: layout.right.project_batches(probe_batches, &self.execution) })
    }

    fn table(&self, batches: Vec<ColumnBatch>, rows: usize, inventory: Arc<MemoryReservation>) -> Result<Table> {
        let context = &self.execution.context;
        let starts_bytes = batches.len().checked_mul(std::mem::size_of::<usize>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let _hashes = context.memory.reserve(rows.checked_mul(std::mem::size_of::<u32>()).ok_or_else(size_error)?)?;
        let reservation = context.memory.reserve(starts_bytes)?;
        let mut hashes = Vec::with_capacity(rows);
        let mut starts = Vec::with_capacity(batches.len());
        for batch in &batches {
            starts.push(hashes.len());
            for row in 0..batch.len {
                context.check_cancelled_at(row)?;
                hashes.push(self.key_hash(batch, row, 0)?);
            }
        }
        let index = HashIndex::new(&hashes, &context.memory)?;
        Ok(Table { batches, starts, index, rows, unmatched: None, _starts: reservation, _inventory: inventory })
    }

    async fn spill_arrow(
        &self, held: Vec<ColumnBatch>, mut rest: BoxBatchCursor, probe: ProbeInput, layout: &JoinLayout,
    ) -> Result<DiskJoin> {
        let context = &self.execution.context;
        let mut build = BlockWriter::new(&layout.left.schema, context)?;
        for batch in held { build.append(batch, context)?; }
        while let Some(batch) = poll_nested_batch(rest.as_mut()).await? {
            batch.validate_readable(context)?;
            build.append(batch, context)?;
        }
        drop(rest);
        let build = build.finish(context)?.shuffle(1, context, |batch, row| self.key_hash(batch, row, 0))?;
        let Some((input, projected)) = build_batch_input(probe.runtime, probe.execution, probe.pushdowns, probe.output).await? else {
            return Err(crate::join_spill::layout_error());
        };
        if projected != self.projected[1] { return Err(crate::join_spill::layout_error()); }
        let mut input = layout.right.project_batches(input, &self.execution);
        let mut probe = BlockWriter::new(&layout.right.schema, context)?;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            batch.validate_readable(context)?;
            probe.append(batch, context)?;
        }
        let probe = probe.finish(context)?.shuffle(build.count(), context, |batch, row| self.key_hash(batch, row, 1))?;
        Ok(DiskJoin { build, probe, table: None, build_partition: 0, probe_partition: 0, ready: None })
    }

    fn key_hash(&self, batch: &ColumnBatch, row: usize, side: usize) -> Result<u32> {
        self.key(batch, row, side)?.vector_hash()
    }

    fn key<'a>(&'a self, batch: &'a ColumnBatch, row: usize, side: usize) -> Result<Key<'a>> {
        let positions = if side == 0 { &self.build_keys } else { &self.probe_keys };
        Key::for_columns(batch, row, self.projected[side], positions, &self.execution.context)
    }

    async fn probe_batch(&self, table: &mut Table, probe: ColumnBatch) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        probe.validate_readable(context)?;
        let residual = self.residual.as_ref().map(|predicate| predicate.prepare(&self.execution)).transpose()?;
        match self.kind {
            JoinKind::Semi | JoinKind::Anti => return self.probe_membership(table, probe, residual.as_ref()).await,
            JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Single => return self.probe_outer(table, &probe, residual.as_ref()).await,
            _ => {}
        }
        let mut build = JoinPositions::new();
        let mut runs = ProbeRuns::new();
        if self.theta.is_empty() && residual.is_none() {
            self.probe_arrow_keys(table, &probe, &mut build, &mut runs)?;
        } else if let Some(predicates) = self.residual.as_ref().filter(|predicates| residual.is_some()
            && (self.projected == [false, false] || predicates.schema.fields().iter().all(|field| date_free(&field.data_type))))
            && self.probe_residual_columns(table, &probe, predicates, &mut build, &mut runs).await.is_ok()
        {
        } else {
            // The row path also reports the error a failed column-wise pass met, as it would have met it first.
            build = JoinPositions::new();
            runs = ProbeRuns::new();
            let equality = self.execution.scope()?;
            for row in 0..probe.len {
                context.check_cancelled_at(row)?;
                self.matching_build_rows(table, &probe, row, &equality, residual.as_ref(), |ordinal| {
                    build.push(table.position(ordinal), &context.memory)?;
                    Ok(ControlFlow::Continue(()))
                }).await?;
                runs.end_row(row, build.rows.len(), &context.memory)?;
            }
        }
        self.gather_matches(build.rows.len(), Some((&probe, runs)), |position| {
            gather_column_rows(|batch| &table.batches[batch], &build.rows, position, &context.memory)
        })
    }

    /// An inner join's residual over every key-matched pair of a probe batch at once: each conjunct is evaluated
    /// column-wise over the pairs the earlier ones kept, so no pair meets a conjunct the row-at-a-time path would skip.
    /// An inner join's residual over the key-matched pairs of a probe batch, column-wise, a bounded run of probe rows at a time.
    /// As in the row path, a pair meets each conjunct until one is false; an unknown key or conjunct lets it go on,
    /// for the later conjuncts' errors, but it never matches.
    async fn probe_residual_columns(&self, table: &Table, probe: &ColumnBatch, residual: &Residual, build: &mut JoinPositions<(usize, usize)>, runs: &mut ProbeRuns)
        -> Result<()> {
        const PAIRS_PER_PASS: usize = 1 << 16;
        let context = &self.execution.context;
        let memory = &context.memory;
        let equality = self.execution.scope()?;
        let mut pairs = JoinPositions::new();
        let mut probes = JoinPositions::new();
        let mut known = Vec::new();
        let mut first_row = 0;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let hash = self.key(probe, row, 1)?.vector_hash()?;
            for (candidate, ordinal) in table.index.candidates(hash).enumerate() {
                context.check_cancelled_at(candidate)?;
                let (batch, offset) = table.position(ordinal);
                let Some(keys) = self.residual_keys(&table.batches[batch], offset, probe, row, &equality)? else { continue; };
                pairs.push((batch, offset), memory)?;
                probes.push((0, row), memory)?;
                known.push(keys);
            }
            if pairs.rows.len() >= PAIRS_PER_PASS || row + 1 == probe.len {
                self.residual_pass(table, probe, residual, &pairs.rows, &probes.rows, &mut known, first_row..row + 1, build, runs).await?;
                pairs = JoinPositions::new();
                probes = JoinPositions::new();
                known.clear();
                first_row = row + 1;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn residual_pass(&self, table: &Table, probe: &ColumnBatch, residual: &Residual, pairs: &[(usize, usize)], probes: &[(usize, usize)],
        known: &mut [bool], rows: std::ops::Range<usize>, build: &mut JoinPositions<(usize, usize)>, runs: &mut ProbeRuns) -> Result<()> {
        let memory = &self.execution.context.memory;
        let index_bytes = pairs.len().checked_mul(3 * size_of::<(usize, usize)>()).ok_or_else(size_error)?;
        let _indexes = memory.reserve(index_bytes)?;
        let mut kept: Vec<usize> = (0..pairs.len()).collect();
        for predicate in &residual.expressions {
            if kept.is_empty() { break; }
            let build_rows: Vec<(usize, usize)> = kept.iter().map(|&pair| pairs[pair]).collect();
            let probe_rows: Vec<(usize, usize)> = kept.iter().map(|&pair| probes[pair]).collect();
            let mut columns = Vec::with_capacity(residual.sides.len());
            let mut charges = Vec::with_capacity(residual.sides.len());
            for &(on_build, position) in &residual.sides {
                let (column, charge) = if on_build {
                    gather_column_rows(|batch| &table.batches[batch], &build_rows, position, memory)?
                } else {
                    gather_column_rows(|_| probe, &probe_rows, position, memory)?
                };
                columns.push(column);
                charges.push(charge);
            }
            let pairs_batch = ColumnBatch::settled(columns, kept.len(), memory.reserve(256)?)?;
            let (truths, _truths) = crate::columnar::predicate_truth(&pairs_batch, predicate, &residual.schema, &self.execution).await?;
            kept = kept.into_iter().zip(truths).filter_map(|(pair, truth)| match truth {
                Some(false) => None,
                Some(true) => Some(pair),
                None => { known[pair] = false; Some(pair) }
            }).collect();
        }
        let mut kept = kept.into_iter().filter(|&pair| known[pair]).peekable();
        for row in rows {
            while let Some(&pair) = kept.peek() {
                if probes[pair].1 != row { break; }
                build.push(pairs[pair], memory)?;
                kept.next();
            }
            runs.end_row(row, build.rows.len(), memory)?;
        }
        Ok(())
    }

    async fn probe_membership(&self, table: &Table, probe: ColumnBatch, residual: Option<&PreparedResidual<'_>>) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let memory = &context.memory;
        let mut span = 0..0;
        let mut selected = JoinPositions::new();
        let equality = self.execution.scope()?;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let matched = self.matching_build_rows(table, &probe, row, &equality, residual, |_| Ok(ControlFlow::Break(()))).await?;
            if matched != (self.kind == JoinKind::Semi) { continue; }
            if span.is_empty() {
                span = row..row + 1;
            } else if selected.rows.is_empty() && row == span.end {
                span.end += 1;
            } else {
                if selected.rows.is_empty() {
                    for row in span.clone() {
                        context.check_cancelled_at(row)?;
                        selected.push((0, row), memory)?;
                    }
                }
                selected.push((0, row), memory)?;
            }
        }
        if span.is_empty() { return Ok(None); }
        let width = self.widths[1];
        if span == (0..probe.len) && width == probe.columns.len()
            && probe.columns.iter().all(|column| !matches!(column, Column::Values(_)))
        {
            return Ok(Some(probe));
        }
        let _columns = memory.reserve(width.checked_mul(std::mem::size_of::<usize>()).ok_or_else(size_error)?)?;
        let columns = (0..width).collect::<Vec<_>>();
        if selected.rows.is_empty() {
            match probe.take_sorted_span(span.clone(), &columns, memory) {
                Ok(Some(output)) => return Ok(Some(output)),
                Ok(None) => {}
                Err(error) if error.is(rtdb_types::AnomalyKind::ResourceExhausted) => {}
                Err(error) => return Err(error),
            }
            for row in span {
                context.check_cancelled_at(row)?;
                selected.push((0, row), memory)?;
            }
        }
        ColumnBatch::take_rows(&[&probe], &selected.rows, &columns, memory).map(Some)
    }

    async fn matching_build_rows(
        &self, table: &Table, probe: &ColumnBatch, row: usize, equality: &rtdb_expr::EvaluationScope,
        residual: Option<&PreparedResidual<'_>>,
        mut accept: impl FnMut(usize) -> Result<ControlFlow<()>>,
    ) -> Result<bool> {
        let mut key = None;
        let mut hashed = if self.kind != JoinKind::Anti {
            let probe_key = self.key(probe, row, 1)?;
            let hash = probe_key.vector_hash()?;
            key = Some(probe_key);
            Some(table.index.candidates(hash))
        } else {
            None
        };
        // Anti joins test the build side in source order, including candidates with different hashes.
        let mut all = 0..table.rows;
        let candidates = std::iter::from_fn(|| match &mut hashed { Some(rows) => rows.next(), None => all.next() });
        let mut matched = false;
        for (candidate, ordinal) in candidates.enumerate() {
            self.execution.context.check_cancelled_at(candidate)?;
            let (batch, offset) = table.position(ordinal);
            let build = &table.batches[batch];
            if let Some(residual) = residual {
                if self.matches_residual(residual, build, offset, probe, row, equality).await? {
                    matched = true;
                    if accept(ordinal)?.is_break() { break; }
                }
                continue;
            }
            if key.is_none() { key = Some(self.key(probe, row, 1)?); }
            let probe_key = key.as_ref().expect("The probe key has been read");
            let build_key = self.key(build, offset, 0)?;
            if self.equal_keys(&build_key, probe_key, Some(equality))? && self.matches_theta(build, offset, probe, row)? {
                matched = true;
                if accept(ordinal)?.is_break() { break; }
            }
        }
        Ok(matched)
    }

    async fn matches_residual(&self, residual: &PreparedResidual<'_>, build: &ColumnBatch, build_row: usize,
        probe: &ColumnBatch, probe_row: usize, equality: &rtdb_expr::EvaluationScope) -> Result<bool> {
        let Some(keys) = self.residual_keys(build, build_row, probe, probe_row, equality)? else { return Ok(false); };
        let mut unknown = !keys;
        let input = JoinRow { build, build_row, probe, probe_row, sides: residual.sides, admit_dates: self.projected };
        for comparison in &residual.comparisons {
            let scope = self.execution.scope()?;
            let value = self.execution.evaluate_invocation_row(&scope, comparison, rtdb_expr::ExpressionRow::Borrowed(&input)).await?;
            if matches!(value, Value::Null | Value::Undefined) { unknown = true; }
            else if !rtdb_expr::is_true(&value)? { return Ok(false); }
        }
        Ok(!unknown)
    }

    /// The equality keys of a residual join's pair: `None` when a key is unequal, `Some(false)` when one is unknown.
    fn residual_keys(&self, build: &ColumnBatch, build_row: usize, probe: &ColumnBatch, probe_row: usize, equality: &rtdb_expr::EvaluationScope)
        -> Result<Option<bool>> {
        let memory = &self.execution.context.memory;
        let mut unknown = false;
        for ((&left, &right), &build_first) in self.build_keys.iter().zip(&self.probe_keys).zip(&self.order) {
            let (left, right) = (&build.columns[left], &probe.columns[right]);
            let equal = if let Some(typed) = direct_key_equal(left, build_row, right, probe_row, self.preserves_probe()) {
                typed
            } else {
                let _scratch = memory.reserve(left.bound(build_row)?.checked_add(right.bound(probe_row)?).ok_or_else(size_error)?)?;
                let left = self.cell(left, build_row, 0)?;
                let right = self.cell(right, probe_row, 1)?;
                let (left, right) = if build_first { (&left, &right) } else { (&right, &left) };
                let left_null = matches!(left, Value::Null | Value::Undefined);
                let right_null = matches!(right, Value::Null | Value::Undefined);
                if self.preserves_probe() && (left_null || right_null) {
                    Some(left_null && right_null)
                } else {
                    let equal = rtdb_expr::sql_eq_in_timezone(left, right, &self.execution.context.timezone, equality)?;
                    equality.reset()?;
                    equal
                }
            };
            match equal { Some(false) => return Ok(None), None => unknown = true, Some(true) => {} }
        }
        Ok(Some(!unknown))
    }

    fn matches_theta(&self, build: &ColumnBatch, build_row: usize, probe: &ColumnBatch, probe_row: usize) -> Result<bool> {
        let read = |operand| -> Result<Option<i128>> {
            let (column, row, side) = match operand {
                ThetaOperand::Literal(value) => return Ok(value),
                ThetaOperand::Build(position) => (&build.columns[position], build_row, 0),
                ThetaOperand::Probe(position) => (&probe.columns[position], probe_row, 1),
            };
            match ScalarKey::read(column, row) {
                Some(ScalarKey::Signed(value)) => Ok(Some(i128::from(value))),
                Some(ScalarKey::Null) => Ok(None),
                _ => {
                    let _scratch = self.execution.context.memory.reserve(column.bound(row)?)?;
                    let value = self.cell(column, row, side)?;
                    if matches!(value, Value::Null | Value::Undefined) { return Ok(None); }
                    rtdb_expr::integer(&value).map(Some).ok_or_else(|| invalid("join-key", "Expected an integer comparison operand"))
                }
            }
        };
        for test in &self.theta {
            let (Some(left), Some(right)) = (read(test.left)?, read(test.right)?) else { return Ok(false); };
            let accepted = match test.op {
                BinaryOp::Eq => left == right,
                BinaryOp::NotEq => left != right,
                BinaryOp::Lt => left < right,
                BinaryOp::LtEq => left <= right,
                BinaryOp::Gt => left > right,
                BinaryOp::GtEq => left >= right,
                _ => unreachable!("The comparison was checked"),
            };
            if !accepted { return Ok(false); }
        }
        Ok(true)
    }

    async fn probe_outer(&self, table: &mut Table, probe: &ColumnBatch, residual: Option<&PreparedResidual<'_>>) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let mut build = JoinPositions::new();
        let mut runs = ProbeRuns::new();
        let equality = self.execution.scope()?;
        let mut unmatched = table.unmatched.take();
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let mut matched_once = false;
            let matched = self.matching_build_rows(table, probe, row, &equality, residual, |ordinal| {
                if matched_once && self.kind == JoinKind::Single { return Err(crate::cardinality_violation()); }
                matched_once = true;
                if let Some((unmatched, _)) = &mut unmatched { unmatched[ordinal] = 0; }
                build.push_outer(Some(table.position(ordinal)), &context.memory)?;
                Ok(ControlFlow::Continue(()))
            }).await?;
            if !matched && self.preserves_probe() { build.push_outer(None, &context.memory)?; }
            runs.end_row(row, build.rows.len(), &context.memory)?;
        }
        table.unmatched = unmatched;
        self.gather_matches(build.rows.len(), Some((probe, runs)), |position| {
            gather_nullable_column(|batch| &table.batches[batch], &build.rows, position, &context.memory)
        })
    }

    fn unmatched_build(&self, table: &Table) -> Result<Option<ColumnBatch>> {
        let Some((unmatched, _)) = &table.unmatched else { return Ok(None); };
        let context = &self.execution.context;
        let mut build = JoinPositions::new();
        for (ordinal, flag) in unmatched.iter().enumerate() {
            context.check_cancelled_at(ordinal)?;
            if *flag != 0 { build.push_outer(Some(table.position(ordinal)), &context.memory)?; }
        }
        self.gather_matches(build.rows.len(), None, |position| {
            gather_nullable_column(|batch| &table.batches[batch], &build.rows, position, &context.memory)
        })
    }

    fn gather_matches(
        &self, count: usize, mut probe: Option<(&ColumnBatch, ProbeRuns)>,
        mut gather_build: impl FnMut(usize) -> Result<(Column, Arc<MemoryReservation>)>,
    ) -> Result<Option<ColumnBatch>> {
        if count == 0 { return Ok(None); }
        let context = &self.execution.context;
        context.check_cancelled()?;
        let memory = &context.memory;
        let width = self.widths[0].checked_add(self.widths[1]).ok_or_else(size_error)?;
        let bound = width.checked_mul(std::mem::size_of::<Column>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
        let mut reservation = memory.reserve(bound)?;
        let lease = Arc::get_mut(&mut reservation).expect("The join owns its output reservation");
        let mut columns = Vec::with_capacity(width);
        for side in if self.swapped { [1, 0] } else { [0, 1] } {
            if side == 1 {
                gather_join_probe(probe.take(), count, self.widths[1], &mut columns, lease)?;
            } else {
                for position in 0..self.widths[0] {
                    let (column, mut charge) = gather_build(position)?;
                    let bytes = column.retained_bytes()?;
                    let charge = Arc::get_mut(&mut charge).expect("The gather owns its output reservation");
                    if bytes <= charge.bytes() { charge.shrink_to(bytes)?; } else { charge.grow_to(bytes)?; }
                    lease.absorb(charge)?;
                    columns.push(column);
                }
            }
        }
        ColumnBatch::settled(columns, count, reservation).map(Some)
    }

    fn probe_arrow_keys(&self, table: &Table, probe: &ColumnBatch, build: &mut JoinPositions<(usize, usize)>, runs: &mut ProbeRuns) -> Result<()> {
        let context = &self.execution.context;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let probe_key = self.key(probe, row, 1)?;
            let hash = probe_key.vector_hash()?;
            for (candidate, ordinal) in table.index.candidates(hash).enumerate() {
                context.check_cancelled_at(candidate)?;
                let (batch_index, build_row) = table.position(ordinal);
                let build_key = self.key(&table.batches[batch_index], build_row, 0)?;
                if self.equal_keys(&build_key, &probe_key, None)? {
                    build.push((batch_index, build_row), &context.memory)?;
                }
            }
            runs.end_row(row, build.rows.len(), &context.memory)?;
        }
        Ok(())
    }

    fn equal_keys(&self, build: &Key<'_>, probe: &Key<'_>, equality: Option<&rtdb_expr::EvaluationScope>) -> Result<bool> {
        let mut unknown = false;
        for (index, build_first) in self.order.iter().enumerate() {
            let (left, right) = if *build_first { (build, probe) } else { (probe, build) };
            match left.equal_at(right, index, self.preserves_probe(), &self.execution.context, equality)? {
                None => unknown = true,
                Some(false) => return Ok(false),
                Some(true) => {}
            }
        }
        Ok(!unknown)
    }
}

struct JoinPositions<T> {
    rows: Vec<T>,
    reservation: Option<Arc<MemoryReservation>>,
}

impl<T> JoinPositions<T> {
    fn new() -> Self {
        Self { rows: Vec::new(), reservation: None }
    }

    fn push(&mut self, row: T, memory: &Arc<MemoryBudget>) -> Result<()> {
        self.push_with_limit(row, usize::MAX, memory)
    }

    fn push_with_limit(&mut self, row: T, limit: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.rows.len() == limit { return Err(size_error()); }
        if self.rows.len() == self.rows.capacity() {
            let capacity = self.rows.capacity().saturating_mul(2).max(16).min(limit);
            let bytes = capacity.checked_mul(std::mem::size_of::<T>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let replacement = self.rows.capacity().checked_mul(std::mem::size_of::<T>()).ok_or_else(size_error)?;
            let _replacement = (replacement != 0).then(|| memory.reserve(replacement)).transpose()?;
            if let Some(reservation) = &mut self.reservation {
                Arc::get_mut(reservation).expect("The join owns its positions").grow_to(bytes)?;
            } else {
                self.reservation = Some(memory.reserve(bytes)?);
            }
            self.rows.try_reserve_exact(capacity - self.rows.len()).map_err(|_| size_error())?;
        }
        self.rows.push(row);
        Ok(())
    }
}

impl JoinPositions<Option<(usize, usize)>> {
    fn push_outer(&mut self, row: Option<(usize, usize)>, memory: &Arc<MemoryBudget>) -> Result<()> {
        self.push_with_limit(row, i32::MAX as usize, memory)
    }
}

struct ProbeRun {
    row: usize,
    end: usize,
}

pub(crate) struct ProbeRuns(JoinPositions<ProbeRun>);

impl ProbeRuns {
    fn new() -> Self { Self(JoinPositions::new()) }

    fn end_row(&mut self, row: usize, end: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.0.rows.last().map_or(0, |run| run.end) == end { return Ok(()); }
        self.0.push(ProbeRun { row, end }, memory)
    }

    fn run_at(&self, output_row: usize) -> usize {
        self.0.rows.partition_point(|run| run.end <= output_row)
    }

    pub(crate) fn input_row(&self, output_row: usize) -> usize {
        self.0.rows[self.run_at(output_row)].row
    }

    pub(crate) fn contiguous(&self, rows: &std::ops::Range<usize>) -> bool {
        let first = self.run_at(rows.start);
        let last = self.run_at(rows.end - 1);
        last - first + 1 == rows.len() && self.0.rows[last].row - self.0.rows[first].row + 1 == rows.len()
    }

    pub(crate) fn owner(&self) -> Option<&Arc<MemoryReservation>> {
        self.0.reservation.as_ref()
    }

    pub(crate) fn picks(&self) -> impl Iterator<Item = Option<(usize, usize)>> + Clone + '_ {
        self.0.rows.iter().scan(0, |start, run| {
            let count = run.end - *start;
            *start = run.end;
            Some(std::iter::repeat_n(Some((0, run.row)), count))
        }).flatten()
    }
}

#[async_trait::async_trait]
impl BatchCursor for BatchJoin {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        loop {
            self.spec.execution.context.check_cancelled()?;
            match std::mem::replace(&mut self.state, State::Done) {
                State::Build { build, probe, layout } => { self.state = self.spec.build(build, probe, layout).await?; }
                State::Probe { mut table, mut probe } => {
                    let Some(batch) = poll_nested_batch(probe.as_mut()).await? else { return self.spec.unmatched_build(&table) };
                    let output = self.spec.probe_batch(&mut table, batch).await?;
                    self.state = State::Probe { table, probe };
                    if let Some(output) = output { return Ok(Some(output)); }
                }
                State::Disk(mut join) => {
                    let output = join.next(&self.spec).await?;
                    if output.is_some() { self.state = State::Disk(join); }
                    return Ok(output);
                }
                State::Done => return Ok(None),
            }
        }
    }
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-size", "Join output exceeds addressable memory") }

