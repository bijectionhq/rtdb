//! Equi-joins of batch inputs on column equalities.
//! Build rows are indexed in input order and the probe gathers its matches from the saved column positions.
use std::{collections::VecDeque, ops::ControlFlow};
use std::sync::Arc;

use arrow_array::Array;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{BinaryOp, Expr, JoinKind};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_column_rows, gather_nullable_column, poll_nested_batch};
use crate::context::Execution;
use crate::invalid;
use crate::join_key::JoinLayout;
use crate::join_spill::{BlockWriter, Partitions};
use crate::join_storage::{HashIndex, combine_cell_hash, hash_values};
use crate::operators::{BatchOutput, DeferredInput, build_batch_input};
use crate::pages::{BoxPageCursor, OwnedPage, PageCursor};
use crate::pushdown::{PushdownBuilder, Pushdowns};
use crate::runtime_schema::EmittedRelation;
use crate::scalar_key::ScalarKey;

/// For each equality, whether its first operand is the build side's key; `None` where the join needs the row join.
pub(crate) fn key_order(layout: &JoinLayout, output: BatchOutput) -> Option<Vec<bool>> {
    if output == BatchOutput::Pages && (!layout.left.supports_batch_keys() || !layout.right.supports_batch_keys()) { return None; }
    layout.comparisons.iter().take(layout.left.key_count()).map(|comparison| match comparison {
        Expr::Binary { op: BinaryOp::Eq, left, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: first, data_type: first_type }, Expr::Column { data_type: second_type, .. })
                if output.is_arrow() || rtdb_plan::validate_equality_types(first_type, second_type).is_ok() => Some(layout.left.schema.get(*first).is_some()),
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
    Rows(BoxPageCursor),
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
                if let Some(batch) = spec.probe_batch(table, &batch).await? {
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
    projected: [bool; 2], swapped: bool, unswapped: Option<JoinLayout>, validate_payloads: bool, theta: Vec<ThetaTest>, residual: Option<Residual>, execution: Execution,
}

impl BatchJoin {
    pub fn new(build: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout, order: Vec<bool>, projected: [bool; 2], execution: Execution) -> Self {
        let build_keys = layout.left.key_positions().to_vec();
        let probe_keys = layout.right.key_positions().to_vec();
        let widths = [layout.left.original.len(), layout.right.original.len()];
        Self { state: State::Build { build, probe, layout }, spec: Spec { order, build_keys, probe_keys, widths, kind: JoinKind::Inner,
            projected, swapped: false, unswapped: None, validate_payloads: false, theta: Vec::new(), residual: None, execution } }
    }
}

impl BatchJoin {
    /// With the join's own layout, builds its right input and emits each match's left columns first, as the join's
    /// schema orders them.
    pub fn swapped(mut self, unswapped: Option<JoinLayout>) -> Self { self.spec.swapped = unswapped.is_some(); self.spec.unswapped = unswapped; self }

    pub fn validating_payloads(mut self, validate: bool) -> Self { self.spec.validate_payloads = validate; self }

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
            if self.validate_payloads { batch.validate_readable(&self.execution.context)?; }
            rows = rows.checked_add(batch.len).ok_or_else(size_error)?;
            let bytes = batches.len().checked_add(1).and_then(|count| count.checked_mul(4 * std::mem::size_of::<ColumnBatch>()))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            Arc::get_mut(&mut inventory).expect("The join owns its batch inventory").grow_to(bytes)?;
            batches.push(batch);
            if i64::try_from(rows).map_or(true, |rows| rows > threshold) {
                if probe.output.is_arrow() {
                    drop(inventory);
                    return self.spill_arrow(batches, build, probe, &layout).await.map(State::Disk);
                }
                return self.spill(batches, build, probe, layout);
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
        let positions = if side == 0 { &self.build_keys } else { &self.probe_keys };
        if let Some(hash) = arrow_hash(batch, row, positions) { return Ok(hash); }
        let (values, _reservation) = self.value_keys(batch, row, positions, side)?;
        hash_values(&values)
    }

    /// Hands the input read so far, and the rest, to the row join, which spills a build side this large.
    fn spill(&mut self, held: Vec<ColumnBatch>, rest: BoxBatchCursor, probe: ProbeInput, layout: JoinLayout) -> Result<State> {
        let memory = &self.execution.context.memory;
        let pages = held.iter().map(|batch| batch.rows_admitting(memory, self.projected[0])).collect::<Result<VecDeque<_>>>()?;
        drop(held);
        let rest: BoxPageCursor = Box::new(crate::columnar::BatchRows::new(rest, Arc::clone(memory)).admitting_dates(self.projected[0]));
        let build: BoxPageCursor = Box::new(HeldThen { pages, rest });
        let probe = DeferredInput::new(probe.runtime, probe.execution, crate::inspection::Constraints::default(), probe.pushdowns);
        let projected = !layout.left.computes_no_keys();
        let (layout, build_left) = match self.unswapped.take() { Some(unswapped) => (unswapped, false), None => (layout, true) };
        Ok(State::Rows(Box::new(crate::join::JoinCursor::new(build, probe, layout, build_left, self.kind, self.execution.clone())?
            .with_projected_build(projected))))
    }

    async fn probe_batch(&self, table: &mut Table, probe: &ColumnBatch) -> Result<Option<ColumnBatch>> {
        if self.validate_payloads { probe.validate_readable(&self.execution.context)?; }
        let residual = self.residual.as_ref().map(|predicate| predicate.prepare(&self.execution)).transpose()?;
        match self.kind {
            JoinKind::Semi | JoinKind::Anti => return self.probe_membership(table, probe, residual.as_ref()).await,
            JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Single => return self.probe_outer(table, probe, residual.as_ref()).await,
            _ => {}
        }
        let mut matched = Matches { pairs: Vec::new(), probes: Vec::new(), charge: self.execution.context.memory.reserve(4096)? };
        if self.theta.is_empty() && residual.is_none() {
            self.probe_arrow_keys(table, probe, &mut matched)?;
        } else {
            let equality = self.execution.scope()?;
            for row in 0..probe.len {
                self.execution.context.check_cancelled_at(row)?;
                self.matching_build_rows(table, probe, row, &equality, residual.as_ref(), |build| {
                    matched.push(table, build, row)?;
                    Ok(ControlFlow::Continue(()))
                }).await?;
            }
        }
        let Matches { pairs, probes, charge: matches } = matched;
        if pairs.is_empty() { return Ok(None); }
        let memory = &self.execution.context.memory;
        let [build_width, probe_width] = self.widths;
        let width = build_width.checked_add(probe_width).ok_or_else(size_error)?;
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
        if self.swapped { append(std::slice::from_ref(probe), &probes, probe_width)?; }
        append(&table.batches, &pairs, build_width)?;
        if !self.swapped { append(std::slice::from_ref(probe), &probes, probe_width)?; }
        drop(matches);
        ColumnBatch::settled(columns, pairs.len(), reservation).map(Some)
    }

    async fn probe_membership(&self, table: &Table, probe: &ColumnBatch, residual: Option<&PreparedResidual<'_>>) -> Result<Option<ColumnBatch>> {
        let context = &self.execution.context;
        let memory = &context.memory;
        let bytes = probe.len.checked_mul(std::mem::size_of::<(usize, usize)>())
            .and_then(|bytes| bytes.checked_add(self.widths[1].checked_mul(std::mem::size_of::<usize>())?))
            .ok_or_else(size_error)?;
        let _selection = memory.reserve(bytes)?;
        let mut selected = Vec::with_capacity(probe.len);
        let equality = self.execution.scope()?;
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let matched = self.matching_build_rows(table, probe, row, &equality, residual, |_| Ok(ControlFlow::Break(()))).await?;
            if matched == (self.kind == JoinKind::Semi) { selected.push((0, row)); }
        }
        if selected.is_empty() { return Ok(None); }
        let columns = (0..self.widths[1]).collect::<Vec<_>>();
        ColumnBatch::take_rows(&[probe], &selected, &columns, memory).map(Some)
    }

    async fn matching_build_rows(
        &self, table: &Table, probe: &ColumnBatch, row: usize, equality: &rtdb_expr::EvaluationScope,
        residual: Option<&PreparedResidual<'_>>,
        mut accept: impl FnMut(usize) -> Result<ControlFlow<()>>,
    ) -> Result<bool> {
        let mut probe_values = None;
        let mut hashed = if self.kind != JoinKind::Anti {
            let hash = match arrow_hash(probe, row, &self.probe_keys) {
                Some(hash) => hash,
                None => {
                    let key = self.value_keys(probe, row, &self.probe_keys, 1)?;
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
            let equal = if let Some(equal) = self.equal_arrow(build, offset, probe, row) {
                equal
            } else {
                if probe_values.is_none() { probe_values = Some(self.value_keys(probe, row, &self.probe_keys, 1)?); }
                let key = &probe_values.as_ref().expect("The probe key was read").0;
                let (build, _scratch) = self.value_keys(build, offset, &self.build_keys, 0)?;
                self.equal_values(&build, key, equality)?
            };
            if equal && self.matches_theta(build, offset, probe, row)? {
                matched = true;
                if accept(ordinal)?.is_break() { break; }
            }
        }
        Ok(matched)
    }

    async fn matches_residual(&self, residual: &PreparedResidual<'_>, build: &ColumnBatch, build_row: usize,
        probe: &ColumnBatch, probe_row: usize, equality: &rtdb_expr::EvaluationScope) -> Result<bool> {
        let memory = &self.execution.context.memory;
        let mut unknown = false;
        for ((&left, &right), &build_first) in self.build_keys.iter().zip(&self.probe_keys).zip(&self.order) {
            let (left, right) = (&build.columns[left], &probe.columns[right]);
            let typed = match (ScalarKey::read(left, build_row), ScalarKey::read(right, probe_row)) {
                (Some(left), Some(right)) => left.equal(right).map(|equal| {
                    if left == ScalarKey::Null || right == ScalarKey::Null { self.preserves_probe().then_some(equal) } else { Some(equal) }
                }),
                _ => None,
            };
            let equal = if let Some(typed) = typed {
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
            match equal { Some(false) => return Ok(false), None => unknown = true, Some(true) => {} }
        }
        let input = JoinRow { build, build_row, probe, probe_row, sides: residual.sides, admit_dates: self.projected };
        for comparison in &residual.comparisons {
            let scope = self.execution.scope()?;
            let value = self.execution.evaluate_invocation_row(&scope, comparison, rtdb_expr::ExpressionRow::Borrowed(&input)).await?;
            if matches!(value, Value::Null | Value::Undefined) { unknown = true; }
            else if !rtdb_expr::is_true(&value)? { return Ok(false); }
        }
        Ok(!unknown)
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
        let mut matches = OuterPositions::new(&context.memory)?;
        let equality = self.execution.scope()?;
        let mut unmatched = table.unmatched.take();
        for row in 0..probe.len {
            context.check_cancelled_at(row)?;
            let mut matched_once = false;
            let matched = self.matching_build_rows(table, probe, row, &equality, residual, |ordinal| {
                if matched_once && self.kind == JoinKind::Single { return Err(crate::cardinality_violation()); }
                matched_once = true;
                if let Some((unmatched, _)) = &mut unmatched { unmatched[ordinal] = 0; }
                matches.push(Some(table.position(ordinal)), Some((0, row)))?;
                Ok(ControlFlow::Continue(()))
            }).await?;
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

    fn probe_arrow_keys(&self, table: &Table, probe: &ColumnBatch, matches: &mut Matches) -> Result<()> {
        for row in 0..probe.len {
            self.execution.context.check_cancelled_at(row)?;
            let mut probe_values = None;
            let hash = match arrow_hash(probe, row, &self.probe_keys) {
                Some(hash) => hash,
                None => {
                    let key = self.value_keys(probe, row, &self.probe_keys, 1)?;
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
                            probe_values = Some(self.value_keys(probe, row, &self.probe_keys, 1)?);
                        }
                        let (probe_key, _) = probe_values.as_ref().expect("A fallback probe key has been read");
                        let (build_key, _scratch) = self.value_keys(batch, build_row, &self.build_keys, 0)?;
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

    fn value_keys(&self, batch: &ColumnBatch, row: usize, positions: &[usize], side: usize) -> Result<(Vec<Value>, Arc<MemoryReservation>)> {
        let reservation = self.execution.context.memory.reserve(key_bound(batch, row, positions)?)?;
        let keys = positions.iter().map(|&position| self.cell(&batch.columns[position], row, side)).collect::<Result<_>>()?;
        Ok((keys, reservation))
    }
}

fn arrow_hash(batch: &ColumnBatch, row: usize, positions: &[usize]) -> Option<u32> {
    positions.iter().try_fold(0, |hash, &position| {
        ScalarKey::read(&batch.columns[position], row).map(|key| combine_cell_hash(hash, key.hash() as i32))
    })
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
                    let Some(batch) = poll_nested_batch(probe.as_mut()).await? else { return self.spec.unmatched_build(&table) };
                    let output = self.spec.probe_batch(&mut table, &batch).await?;
                    self.state = State::Probe { table, probe };
                    if let Some(output) = output { return Ok(Some(output)); }
                }
                State::Rows(mut rows) => {
                    let Some(page) = rows.next_page().await? else { return Ok(None) };
                    let batch = ColumnBatch::from_page(&page, &self.spec.execution.context.memory)?;
                    self.state = State::Rows(rows);
                    if batch.len > 0 { return Ok(Some(batch)); }
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

