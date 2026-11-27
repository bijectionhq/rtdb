use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    mem::size_of,
    sync::Arc,
};

use async_trait::async_trait;
use futures::future::BoxFuture;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{BinaryOp, Expr, Relation, RelationNode, Schema, WindowExpr, WindowFunction};
use rtdb_types::{AnomalyKind, LogicalType, ObjectSort, Result, ScalarType, Value};

use super::{WindowPartitions, window_size};
use crate::{
    QueryContext,
    columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, ordinal_column_by, poll_nested_batch},
    invalid,
    runtime_schema::EmittedRelation,
    scalar_key::{ScalarColumn, ScalarKey},
    trace::OperatorCounter,
};

pub(crate) struct RowNumberLimit {
    count: usize,
    key_position: usize,
    float: bool,
    descending: bool,
    nulls_first: bool,
}

impl RowNumberLimit {
    pub(crate) fn bind(window: &EmittedRelation, predicate: &Expr, context: &QueryContext) -> Result<Option<Self>> {
        let RelationNode::Window { partition_by, order_by, expressions, .. } = window.relation().node() else {
            return Ok(None);
        };
        let [WindowExpr { field, function: WindowFunction::RowNumber }] = expressions.as_slice() else { return Ok(None); };
        let [order] = order_by.as_slice() else { return Ok(None); };
        let Expr::Column { id: key, .. } = &order.expr else { return Ok(None); };
        let Expr::Binary { op, left, right } = predicate else { return Ok(None); };
        let bound = match (op, left.as_ref(), right.as_ref()) {
            (BinaryOp::LtEq, Expr::Column { id, .. }, Expr::Literal(Value::I64(count)))
            | (BinaryOp::GtEq, Expr::Literal(Value::I64(count)), Expr::Column { id, .. }) if *id == field.id => *count,
            _ => return Ok(None),
        };
        let Ok(count) = usize::try_from(bound) else { return Ok(None); };
        if partition_by.is_empty() || count == 0 || count > context.options.top_k_rows
            || context.options.object_sort != ObjectSort::TimSort
            || context.options.sort_memory_bytes < 256
        {
            return Ok(None);
        }
        let input = window.child(0)?;
        let Some(key_position) = input.schema().position(*key) else { return Ok(None); };
        let float = match crate::columnar::float_vector::nonnull(&input.schema().fields()[key_position].data_type) {
            LogicalType::Scalar(ScalarType::F32 | ScalarType::F64) => true,
            LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) => false,
            _ => return Ok(None),
        };
        Ok(replayable(input.relation()).then_some(Self {
            count, key_position, float, descending: order.descending, nulls_first: order.nulls_first,
        }))
    }

    fn key(&self, value: ScalarKey<'_>) -> Option<(bool, u64)> {
        const SIGN: u64 = 1 << 63;
        let value = match value {
            ScalarKey::Null => return Some((!self.nulls_first, 0)),
            ScalarKey::Signed(value) if !self.float => (value as u64) ^ SIGN,
            ScalarKey::Float(value) if self.float => {
                let bits = value.to_bits();
                if value.is_nan() { u64::MAX } else if bits & SIGN == 0 { bits ^ SIGN } else { !bits }
            }
            _ => return None,
        };
        Some((self.nulls_first, if self.descending { !value } else { value }))
    }
}

fn replayable(mut input: &Relation) -> bool {
    loop {
        match input.node() {
            RelationNode::Rename { input: child, expressions, .. }
            | RelationNode::Project { input: child, expressions }
            | RelationNode::Extend { input: child, expressions }
                if expressions.iter().all(|expression| matches!(expression.expr, Expr::Column { .. })) => input = child,
            RelationNode::Scan(scan) => return scan.predicates.is_empty() && scan.matches.is_empty()
                && scan.top_k.is_none() && scan.ordered_top_k.is_none()
                && scan.valid_time.expressions().into_iter().chain(scan.system_time.expressions())
                    .all(|expr| matches!(expr, Expr::Literal(_)))
                && !crate::inspection::is_inspection(&scan.table.name.table)
                && crate::system_schema::definition(&scan.table.name.table).is_none(),
            RelationNode::Values { rows } => return literal_rows(rows.iter().flatten()),
            RelationNode::LiteralPages { pages } => return literal_rows(pages.iter().flatten().flatten()),
            RelationNode::ArrowLiteralPages { .. } => return true,
            _ => return false,
        }
    }
}

fn literal_rows<'a>(mut cells: impl Iterator<Item = &'a Expr>) -> bool {
    cells.all(|expr| matches!(expr, Expr::Literal(_)))
}

type ReopenWindow = Box<dyn FnOnce() -> BoxFuture<'static, Result<BoxBatchCursor>> + Send>;

enum State {
    Pending { input: BoxBatchCursor, partitions: WindowPartitions, reopen: ReopenWindow },
    Selected { batches: VecDeque<ColumnBatch>, _inventory: Arc<MemoryReservation> },
    Ordinary(BoxBatchCursor),
    Finished,
}

pub(crate) struct TopKWindow {
    state: State,
    limit: RowNumberLimit,
    positions: Vec<usize>,
    rank_name: String,
    projected: bool,
    context: Arc<QueryContext>,
}

impl TopKWindow {
    pub(crate) fn new(input: BoxBatchCursor, schema: &Schema, partition_by: &[rtdb_plan::ColumnId], rank: &WindowExpr,
        projected: bool, context: Arc<QueryContext>, limit: RowNumberLimit, reopen: ReopenWindow,
    ) -> Result<Self> {
        let partitions = WindowPartitions::new(schema, partition_by, &context)?;
        Ok(Self { state: State::Pending { input, partitions, reopen }, limit, positions: (0..schema.len()).collect(),
            rank_name: rank.field.name.clone(), projected, context })
    }

    async fn select(&mut self, mut input: BoxBatchCursor, mut partitions: WindowPartitions) -> Result<Option<State>> {
        let context = &self.context;
        let mut groups: Vec<BinaryHeap<Candidate>> = Vec::new();
        let mut inventory = context.memory.reserve(256)?;
        let mut seen = 0usize;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            batch.validate_readable(context)?;
            partitions.validate()?;
            if batch.len == 0 { continue; }
            let next = seen.checked_add(batch.len).filter(|&total| i64::try_from(total).is_ok())
                .ok_or_else(|| invalid("window-count", "Window row number overflow"))?;
            let batch = Arc::new(batch);
            let reader = &batch.columns[self.limit.key_position];
            let inferred;
            let flat = match reader {
                Column::Arrow { array, field, logical } => {
                    inferred = logical.is_none().then(|| rtdb_columnar::from_field(field).ok()).flatten();
                    ScalarColumn::bind(array.as_ref(), field, logical.as_ref().or(inferred.as_ref()))
                }
                _ => None,
            };
            let mut compatible = true;
            partitions.identify(&batch, self.projected, context, |row, group, first| {
                if !compatible { return Ok(()); }
                let value = match (flat, reader) {
                    (Some(flat), _) => Some(flat.read(row)),
                    (_, Column::Values(values)) => ScalarKey::from_value(&values[row]),
                    (_, reader) => ScalarKey::read(reader, row),
                };
                let Some(key) = value.and_then(|value| self.limit.key(value)) else {
                    compatible = false;
                    return Ok(());
                };
                if first {
                    debug_assert_eq!(group, groups.len());
                    admit(&mut inventory, 2 * size_of::<BinaryHeap<Candidate>>(), context)?;
                    groups.push(BinaryHeap::new());
                }
                let heap = &mut groups[group];
                let order = (key, seen + row);
                if heap.len() < self.limit.count {
                    if heap.len() == heap.capacity() {
                        let additional = heap.len().max(1).min(self.limit.count - heap.len());
                        let candidate_bytes = size_of::<Candidate>() + size_of::<ColumnBatch>() + 2 * size_of::<usize>();
                        admit(&mut inventory, additional.checked_mul(candidate_bytes).ok_or_else(window_size)?, context)?;
                        heap.reserve_exact(additional);
                    }
                    heap.push(Candidate { order, batch: Arc::clone(&batch), row });
                } else if let Some(mut worst) = heap.peek_mut() {
                    if order < worst.order {
                        *worst = Candidate { order, batch: Arc::clone(&batch), row };
                    }
                }
                Ok(())
            })?;
            if !compatible { return Ok(None); }
            seen = next;
            tokio::task::consume_budget().await;
        }
        drop((input, partitions));

        let page_rows = context.options.batch_rows;
        let selected = groups.iter().map(BinaryHeap::len).sum::<usize>();
        let mut rows = groups.into_iter().flat_map(|heap| heap.into_sorted_vec().into_iter().enumerate());
        let capacity = selected.min(page_rows);
        let _scratch = context.memory.reserve(capacity.checked_mul(128).ok_or_else(window_size)?)?;
        let mut page = Vec::with_capacity(capacity);
        let mut output = VecDeque::new();
        let mut output_inventory = context.memory.reserve(256)?;
        loop {
            context.check_cancelled()?;
            page.extend(rows.by_ref().take(page_rows));
            if page.is_empty() { break; }
            let sources = page.iter().map(|(_, candidate)| candidate.batch.as_ref()).collect::<Vec<_>>();
            let picks = page.iter().enumerate().map(|(index, (_, candidate))| (index, candidate.row)).collect::<Vec<_>>();
            let mut batch = ColumnBatch::take_rows(&sources, &picks, &self.positions, &context.memory)?;
            let (rank, reservation) = ordinal_column_by(page.len(), &self.rank_name, context, |row| page[row].0 as i64 + 1)?;
            batch.append_column(rank, reservation, &context.memory)?;
            admit(&mut output_inventory, 2 * size_of::<ColumnBatch>(), context)?;
            output.push_back(batch);
            page.clear();
            tokio::task::consume_budget().await;
        }
        Ok(Some(State::Selected { batches: output, _inventory: output_inventory }))
    }
}

fn admit(reservation: &mut Arc<MemoryReservation>, bytes: usize, context: &QueryContext) -> Result<()> {
    let total = reservation.bytes().checked_add(bytes).ok_or_else(window_size)?;
    if total > context.options.sort_memory_bytes {
        return Err(rtdb_types::Anomaly::incorrect("rtdb.query/window-top-k-size", "Window top-k exceeds the sort memory limit")
            .with_kind(AnomalyKind::ResourceExhausted));
    }
    Arc::get_mut(reservation).expect("Window top-k owns its inventory").grow_to(total)
}

struct Candidate {
    order: ((bool, u64), usize),
    batch: Arc<ColumnBatch>,
    row: usize,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool { self.order == other.order }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering { self.order.cmp(&other.order) }
}

#[async_trait]
impl BatchCursor for TopKWindow {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        if matches!(self.state, State::Pending { .. }) {
            let State::Pending { input, partitions, reopen } = std::mem::replace(&mut self.state, State::Finished) else { unreachable!() };
            self.state = match self.select(input, partitions).await {
                Ok(Some(output)) => output,
                Ok(None) => State::Ordinary(reopen().await?),
                Err(error) if error.is(AnomalyKind::ResourceExhausted) => State::Ordinary(reopen().await?),
                Err(error) => return Err(error),
            };
        }
        match &mut self.state {
            State::Selected { batches, .. } => Ok(batches.pop_front()),
            State::Ordinary(window) => poll_nested_batch(window.as_mut()).await,
            State::Finished => Ok(None),
            State::Pending { .. } => unreachable!(),
        }
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        match &self.state {
            State::Selected { .. } => {
                OperatorCounter::WindowInputOrderReused.record(span, 0);
                OperatorCounter::WindowRowNumberLimit.record(span, self.limit.count as u64);
            }
            State::Ordinary(window) => window.trace_attributes(span),
            _ => {}
        }
    }
}
