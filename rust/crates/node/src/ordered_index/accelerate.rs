use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
};

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder},
};
use arrow_schema::Schema;
use async_trait::async_trait;
use rtdb_columnar::{BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, to_field};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field};
use rtdb_query::{OrderedBatches, OrderedScan, ScanAccelerator, ScanRequest};
use rtdb_types::{AnomalyCategory, Iid, LogicalType, Result, ScalarType};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::{
    Definition, checkpoint, definitions,
    key::{Atom, AtomRef, Bucket, Family, family},
    maintenance::{cancelled, read_root, source_frontier},
    postings::Tuple,
    tree::{Cursor, Hit, KeyRef, Tree},
};

const BATCH_ROWS: usize = 128;
const MAX_CANDIDATES: usize = 16_384;

pub(crate) struct OrderedIndexAccelerator;

#[async_trait]
impl ScanAccelerator for OrderedIndexAccelerator {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        Ok(match exact(request, None, &[], false).await? {
            Some(OrderedScan::Iids(ids)) => Some(ids.into_iter().collect()),
            _ => None,
        })
    }

    async fn ordered(
        &self,
        request: &ScanRequest<'_>,
        limit: usize,
        projection: &[usize],
        covered_only: bool,
    ) -> Result<Option<OrderedScan>> {
        if limit > MAX_CANDIDATES {
            return Ok(None);
        }
        exact(request, Some(limit), projection, covered_only).await
    }
}

async fn exact(
    request: &ScanRequest<'_>,
    limit: Option<usize>,
    projection: &[usize],
    covered_only: bool,
) -> Result<Option<OrderedScan>> {
    let table = &request.scan.table.name.table;
    if table.schema == super::CATALOG.0
        || request.scan.iids.is_some()
        || !request.bounds.system_time.is_point()
        || !request.bounds.valid_time.is_point()
        || (request.scan.predicates.is_empty() && request.scan.ordered_top_k.is_none())
    {
        return Ok(None);
    }
    match seek(request, limit, projection, covered_only).await {
        Err(error) if error.category == AnomalyCategory::Interrupted => Err(error),
        Err(_) => Ok(None),
        found => found,
    }
}

async fn seek(
    request: &ScanRequest<'_>,
    limit: Option<usize>,
    projection: &[usize],
    covered_only: bool,
) -> Result<Option<OrderedScan>> {
    let definitions = definitions(
        request.database,
        request.snapshot,
        request.storage,
        request.memory,
        request.current_time,
        request.cancellation,
    )
    .await?;
    for definition in definitions
        .iter()
        .filter(|definition| definition.table == request.scan.table.name.table)
    {
        let fields: Vec<_> = definition
            .columns
            .iter()
            .map(|column| {
                request
                    .scan
                    .columns
                    .iter()
                    .find(|field| &field.name == column)
            })
            .collect();
        let range = Range::bind(request, &fields).await?;
        let top = if let Some(limit) = limit {
            let Some((reverse, distinct_slot)) = order(request, &fields, &range) else {
                continue;
            };
            Some(Top {
                limit,
                reverse,
                distinct_slot,
            })
        } else {
            if range.is_open() {
                continue;
            }
            None
        };
        if let Some(found) = use_index(
            request,
            definition,
            &fields,
            range,
            top,
            projection,
            covered_only,
        )
        .await?
        {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

#[derive(Debug, Default)]
struct Range {
    prefix: Vec<Atom>,
    lower: Option<(Atom, bool)>,
    upper: Option<(Atom, bool)>,
    cursors: Vec<CursorBound>,
}

impl Range {
    fn is_open(&self) -> bool {
        self.prefix.is_empty()
            && self.lower.is_none()
            && self.upper.is_none()
            && self.cursors.is_empty()
    }

    async fn bind(request: &ScanRequest<'_>, fields: &[Option<&Field>]) -> Result<Self> {
        let mut conjuncts = Vec::new();
        for predicate in &request.scan.predicates {
            collect_conjuncts(predicate, &mut conjuncts);
        }
        if let Some(top) = &request.scan.ordered_top_k {
            for predicate in &top.residuals {
                collect_conjuncts(predicate, &mut conjuncts);
            }
        }
        let mut range = Self::default();
        for field in fields {
            let Some(field) = field else {
                break;
            };
            let Some(family) = family(&field.data_type) else {
                break;
            };
            let mut equal = None;
            for predicate in &conjuncts {
                let Expr::Binary { op, left, right } = predicate else {
                    continue;
                };
                let Some((op, constant)) = operands(field.id, *op, left, right) else {
                    continue;
                };
                let Some(value) = request.constants.constant(constant).await? else {
                    continue;
                };
                let Some(atom) = Atom::of(&value).filter(|atom| atom.family() == Some(family))
                else {
                    continue;
                };
                match op {
                    BinaryOp::Eq => equal = Some(atom),
                    BinaryOp::Gt | BinaryOp::GtEq => {
                        let bound = (atom, op == BinaryOp::Gt);
                        if range.lower.as_ref().is_none_or(|lower| &bound > lower) {
                            range.lower = Some(bound);
                        }
                    }
                    BinaryOp::Lt | BinaryOp::LtEq => {
                        let strict = op == BinaryOp::Lt;
                        if range.upper.as_ref().is_none_or(|(upper, was_strict)| {
                            atom < *upper || (atom == *upper && strict && !was_strict)
                        }) {
                            range.upper = Some((atom, strict));
                        }
                    }
                    _ => (),
                }
            }
            if let Some(equal) = equal {
                range.prefix.push(equal);
                range.lower = None;
                range.upper = None;
            } else {
                break;
            }
        }
        let suffix = &fields[range.prefix.len()..];
        for predicate in conjuncts {
            if let Some(cursor) = CursorBound::bind(request, predicate, suffix).await? {
                range.cursors.push(cursor);
            }
        }
        Ok(range)
    }

    fn position(&self, key: Tuple<'_>) -> Ordering {
        let prefix = key
            .iter()
            .zip(&self.prefix)
            .map(|(value, bound)| value.cmp(&bound.borrowed()))
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal);
        if prefix != Ordering::Equal {
            return prefix;
        }
        let Some(next) = key.get(self.prefix.len()) else {
            return Ordering::Equal;
        };
        if self.lower.as_ref().is_some_and(|(lower, strict)| {
            next < lower.borrowed() || (*strict && next == lower.borrowed())
        }) || self
            .cursors
            .iter()
            .any(|bound| bound.position(key, self.prefix.len()) == Ordering::Less)
        {
            return Ordering::Less;
        }
        if self.upper.as_ref().is_some_and(|(upper, strict)| {
            next > upper.borrowed() || (*strict && next == upper.borrowed())
        }) || self
            .cursors
            .iter()
            .any(|bound| bound.position(key, self.prefix.len()) == Ordering::Greater)
        {
            return Ordering::Greater;
        }
        Ordering::Equal
    }
}

#[derive(Debug)]
struct CursorBound {
    key: Vec<Atom>,
    lower: bool,
    strict: bool,
}

impl CursorBound {
    async fn bind(
        request: &ScanRequest<'_>,
        predicate: &Expr,
        fields: &[Option<&Field>],
    ) -> Result<Option<Self>> {
        let mut values = Vec::new();
        let Some(operator) = lexicographic(predicate, fields, &mut values) else {
            return Ok(None);
        };
        if values.len() < 2 {
            return Ok(None);
        }
        let mut key = Vec::with_capacity(values.len());
        for (constant, field) in values.into_iter().zip(fields.iter().flatten()) {
            let Some(expected) = family(&field.data_type) else {
                return Ok(None);
            };
            let Some(value) = request.constants.constant(constant).await? else {
                return Ok(None);
            };
            let Some(atom) = Atom::of(&value).filter(|atom| atom.family() == Some(expected)) else {
                return Ok(None);
            };
            key.push(atom);
        }
        Ok(Some(Self {
            key,
            lower: matches!(operator, BinaryOp::Gt | BinaryOp::GtEq),
            strict: matches!(operator, BinaryOp::Gt | BinaryOp::Lt),
        }))
    }

    fn position(&self, tuple: Tuple<'_>, offset: usize) -> Ordering {
        let order = tuple
            .iter()
            .skip(offset)
            .zip(&self.key)
            .map(|(value, bound)| value.cmp(&bound.borrowed()))
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal);
        match (order, self.lower, self.strict) {
            (Ordering::Less, true, _) | (Ordering::Equal, true, true) => Ordering::Less,
            (Ordering::Greater, false, _) | (Ordering::Equal, false, true) => Ordering::Greater,
            _ => Ordering::Equal,
        }
    }
}

fn compare_bound<'a>(predicate: &'a Expr, column: ColumnId) -> Option<(BinaryOp, &'a Expr)> {
    let Expr::Binary { op, left, right } = predicate else {
        return None;
    };
    operands(column, *op, left, right)
}

fn lexicographic<'a>(
    predicate: &'a Expr,
    fields: &[Option<&Field>],
    values: &mut Vec<&'a Expr>,
) -> Option<BinaryOp> {
    let (Some(field), tail) = fields.split_first()? else {
        return None;
    };
    if let Some((op @ (BinaryOp::Gt | BinaryOp::GtEq | BinaryOp::Lt | BinaryOp::LtEq), constant)) =
        compare_bound(predicate, field.id)
    {
        values.push(constant);
        return Some(op);
    }
    let Expr::Binary {
        op: BinaryOp::Or,
        left,
        right,
    } = predicate
    else {
        return None;
    };
    let (op, constant, rest) = lexicographic_parts(left, right, field.id)
        .or_else(|| lexicographic_parts(right, left, field.id))?;
    values.push(constant);
    let end = lexicographic(rest, tail, values)?;
    matches!(
        (op, end),
        (BinaryOp::Gt, BinaryOp::Gt | BinaryOp::GtEq)
            | (BinaryOp::Lt, BinaryOp::Lt | BinaryOp::LtEq)
    )
    .then_some(end)
}

fn lexicographic_parts<'a>(
    outer: &'a Expr,
    inner: &'a Expr,
    column: ColumnId,
) -> Option<(BinaryOp, &'a Expr, &'a Expr)> {
    let (bound @ (BinaryOp::Gt | BinaryOp::Lt), constant) = compare_bound(outer, column)? else {
        return None;
    };
    let Expr::Binary {
        op: BinaryOp::And,
        left,
        right,
    } = inner
    else {
        return None;
    };
    for (equals, rest) in [
        (left.as_ref(), right.as_ref()),
        (right.as_ref(), left.as_ref()),
    ] {
        if compare_bound(equals, column) == Some((BinaryOp::Eq, constant)) {
            return Some((bound, constant, rest));
        }
    }
    None
}

fn collect_conjuncts<'a>(predicate: &'a Expr, output: &mut Vec<&'a Expr>) {
    if let Expr::Binary {
        op: BinaryOp::And,
        left,
        right,
    } = predicate
    {
        collect_conjuncts(left, output);
        collect_conjuncts(right, output);
    } else {
        output.push(predicate);
    }
}

fn operands<'a>(
    id: ColumnId,
    op: BinaryOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(BinaryOp, &'a Expr)> {
    let column =
        |expression: &Expr| matches!(expression, Expr::Column { id: column, .. } if *column == id);
    let constant =
        |expression: &Expr| matches!(expression, Expr::Literal(_) | Expr::Parameter { .. });
    if column(left) && constant(right) {
        return Some((op, right));
    }
    if !column(right) || !constant(left) {
        return None;
    }
    let reversed = match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        BinaryOp::Eq => BinaryOp::Eq,
        _ => return None,
    };
    Some((reversed, left))
}

#[derive(Clone, Copy)]
struct Top {
    limit: usize,
    reverse: bool,
    distinct_slot: Option<usize>,
}

fn order(
    request: &ScanRequest<'_>,
    fields: &[Option<&Field>],
    range: &Range,
) -> Option<(bool, Option<usize>)> {
    let plan = request.scan.ordered_top_k.as_ref()?;
    let order = &plan.order;
    let equal = &fields[..range.prefix.len()];
    let mut suffix = order.iter().filter(|sort| {
        !matches!(&sort.expr, Expr::Column { id, .. } if equal.iter().flatten().any(|field| field.id == *id))
    }).peekable();
    let reverse = suffix.peek()?.descending;
    for (position, sort) in suffix.enumerate() {
        let field = fields.get(range.prefix.len() + position)?.as_ref()?;
        if !matches!(&sort.expr, Expr::Column { id, .. } if *id == field.id)
            || family(&field.data_type).is_none()
            || field.data_type.legs().into_iter().any(|leg| {
                matches!(
                    leg,
                    LogicalType::Scalar(
                        ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64
                    )
                )
            })
            || sort.descending != reverse
            || (sort.nulls_first != reverse && field.data_type.nullable() && field.name != "_id")
        {
            return None;
        }
    }
    let distinct_slot = if plan.distinct {
        let [sort] = order.as_slice() else {
            return None;
        };
        let slot = range.prefix.len();
        let field = fields.get(slot)?.as_ref()?;
        if family(&field.data_type) != Some(Family::Text)
            || !matches!(&sort.expr, Expr::Column { id, .. } if *id == field.id)
        {
            return None;
        }
        Some(slot)
    } else {
        None
    };
    Some((reverse, distinct_slot))
}

#[derive(Debug)]
enum Predicate {
    All(Box<Self>, Box<Self>),
    Either(Box<Self>, Box<Self>),
    Compare {
        slot: usize,
        op: BinaryOp,
        value: Atom,
    },
}

impl Predicate {
    fn bind<'a>(
        request: &'a ScanRequest<'_>,
        fields: &'a [Option<&Field>],
        expression: &'a Expr,
    ) -> futures::future::BoxFuture<'a, Result<Option<Self>>> {
        Box::pin(async move {
            let Expr::Binary { op, left, right } = expression else {
                return Ok(None);
            };
            match op {
                BinaryOp::And | BinaryOp::Or => {
                    let Some(left) = Self::bind(request, fields, left).await? else {
                        return Ok(None);
                    };
                    let Some(right) = Self::bind(request, fields, right).await? else {
                        return Ok(None);
                    };
                    let pair = (Box::new(left), Box::new(right));
                    Ok(Some(if *op == BinaryOp::And {
                        Self::All(pair.0, pair.1)
                    } else {
                        Self::Either(pair.0, pair.1)
                    }))
                }
                BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                    for (slot, field) in fields.iter().enumerate() {
                        let Some(field) = field else {
                            continue;
                        };
                        let Some((op, constant)) = operands(field.id, *op, left, right) else {
                            continue;
                        };
                        let Some(value) = request
                            .constants
                            .constant(constant)
                            .await?
                            .as_ref()
                            .and_then(Atom::of)
                        else {
                            break;
                        };
                        if value == Atom::Null
                            || family(&field.data_type)
                                .is_some_and(|family| value.family() == Some(family))
                        {
                            return Ok(Some(Self::Compare { slot, op, value }));
                        }
                    }
                    Ok(None)
                }
                _ => Ok(None),
            }
        })
    }

    fn matches(&self, tuple: Tuple<'_>) -> bool {
        match self {
            Self::All(left, right) => left.matches(tuple) && right.matches(tuple),
            Self::Either(left, right) => left.matches(tuple) || right.matches(tuple),
            Self::Compare { slot, op, value } => {
                let cell = tuple.cell(*slot);
                let value = value.borrowed();
                if matches!(cell, AtomRef::Null | AtomRef::Unordered) || value == AtomRef::Null {
                    return false;
                }
                match op {
                    BinaryOp::Eq => cell == value,
                    BinaryOp::Lt => cell < value,
                    BinaryOp::LtEq => cell <= value,
                    BinaryOp::Gt => cell > value,
                    BinaryOp::GtEq => cell >= value,
                    _ => unreachable!("only ordering comparisons are bound"),
                }
            }
        }
    }
}

async fn use_index(
    request: &ScanRequest<'_>,
    definition: &Definition,
    fields: &[Option<&Field>],
    range: Range,
    top: Option<Top>,
    projection: &[usize],
    covered_only: bool,
) -> Result<Option<OrderedScan>> {
    let residuals = request
        .scan
        .ordered_top_k
        .as_ref()
        .map_or(&[][..], |plan| plan.residuals.as_slice());
    let mut covered = true;
    let mut predicates = Vec::new();
    if top.is_some() {
        for predicate in request.scan.predicates.iter().chain(residuals) {
            if let Some(predicate) = Predicate::bind(request, fields, predicate).await? {
                predicates.push(predicate);
            } else {
                covered = false;
            }
        }
    }
    let distinct_slot = top.and_then(|top| top.distinct_slot);
    if distinct_slot.is_some() && !covered {
        return Ok(None);
    }
    let projection = if top.is_some() && covered {
        Covered::bind(request, definition, projection)?
    } else {
        None
    };
    if covered_only && projection.is_none() {
        return Ok(None);
    }
    if top.is_some_and(|top| top.limit == 0) {
        return Ok(Some(if projection.is_some() {
            OrderedScan::Covered(Box::new(CoveredSeek::empty()))
        } else {
            OrderedScan::Iids(Vec::new())
        }));
    }

    let directory = definition.directory()?;
    let through = source_frontier(request.snapshot, &definition.table);
    let checkpoint =
        checkpoint::latest(request.storage, &directory, request.memory, through).await?;
    if checkpoint.is_none() && through >= 0 {
        return Ok(None);
    }
    let scope = checkpoint.as_ref().map_or(0, |checkpoint| checkpoint.scope);
    let tree = Tree::new(request.storage, &directory, request.memory, scope).await?;
    let system_at = request.bounds.system_time.lower();
    let Some(root) = read_root(
        &tree,
        definition,
        request.snapshot,
        checkpoint,
        system_at,
        request.cancellation,
    )
    .await?
    else {
        return Ok(None);
    };
    let reverse = top.is_some_and(|top| top.reverse);
    let buckets: Vec<_> = Bucket::at(request.bounds.valid_time.lower()).collect();
    let cursors = buckets
        .iter()
        .map(|_| Cursor::new(root.clone(), reverse))
        .collect();
    let scratch = request.memory.reserve(
        MAX_CANDIDATES
            .min(top.map_or(MAX_CANDIDATES, |top| top.limit))
            .saturating_mul(64)
            .saturating_add(16_384),
    )?;
    let mut scan = PostingScan {
        heap: BinaryHeap::new(),
        cursors,
        buckets,
        advanced: None,
        range,
        reverse,
        width: definition.columns.len(),
        cancellation: request.cancellation.clone(),
        _scratch: scratch,
        tree,
    };
    for slot in 0..scan.cursors.len() {
        scan.advance(slot).await?;
    }

    if let (Some(projection), Some(top)) = (projection, top) {
        let mut seek = CoveredSeek {
            state: Some(CoveredState {
                projection,
                scan,
                predicates,
                distinct_slot,
                last: None,
                remaining: top.limit,
            }),
            first: None,
        };
        seek.first = seek.next_batch().await?;
        return Ok(Some(OrderedScan::Covered(Box::new(seek))));
    }

    let mut selected = Vec::new();
    let mut pending = Vec::new();
    let mut last_distinct = None::<Head>;
    while let Some(head) = scan.next().await? {
        if predicates
            .iter()
            .all(|predicate| predicate.matches(head.tuple()))
        {
            if covered {
                let emit = distinct_slot.is_none_or(|key| {
                    last_distinct
                        .as_ref()
                        .is_none_or(|prior| prior.tuple().cell(key) != head.tuple().cell(key))
                });
                if emit {
                    selected.push(head.iid);
                    if distinct_slot.is_some() {
                        last_distinct = Some(head);
                    }
                }
            } else {
                pending.push(head.iid);
                if pending.len()
                    >= BATCH_ROWS.min(top.map_or(BATCH_ROWS, |top| {
                        top.limit.saturating_sub(selected.len()).max(1)
                    }))
                {
                    filter_payload(request, residuals, &mut pending, &mut selected).await?;
                }
            }
            if top.is_some_and(|top| selected.len() >= top.limit) {
                break;
            }
            if selected.len() > MAX_CANDIDATES {
                return Ok(None);
            }
        }
    }
    filter_payload(request, residuals, &mut pending, &mut selected).await?;
    Ok(Some(OrderedScan::Iids(selected)))
}

struct PostingScan {
    heap: BinaryHeap<Head>,
    cursors: Vec<Cursor>,
    buckets: Vec<Bucket>,
    advanced: Option<usize>,
    range: Range,
    reverse: bool,
    width: usize,
    cancellation: CancellationToken,
    _scratch: Arc<MemoryReservation>,
    tree: Tree,
}

impl PostingScan {
    async fn advance(&mut self, slot: usize) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        if let Some(head) = next(
            &mut self.cursors[slot],
            &self.tree,
            &self.range,
            self.buckets[slot],
            slot,
            self.reverse,
        )
        .await?
        {
            self.heap.push(head);
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Head>> {
        if let Some(slot) = self.advanced.take() {
            self.advance(slot).await?;
        }
        let Some(head) = self.heap.pop() else {
            return Ok(None);
        };
        self.advanced = Some(head.slot);
        if head.tuple().len() != self.width {
            return Err(super::format_error(
                "Posting width differs from its index definition",
            ));
        }
        Ok(Some(head))
    }
}

struct CoveredState {
    projection: Covered,
    predicates: Vec<Predicate>,
    distinct_slot: Option<usize>,
    last: Option<Head>,
    remaining: usize,
    scan: PostingScan,
}

struct CoveredSeek {
    state: Option<CoveredState>,
    first: Option<OwnedBatch>,
}

impl CoveredSeek {
    fn empty() -> Self {
        Self {
            state: None,
            first: None,
        }
    }
}

#[async_trait]
impl OrderedBatches for CoveredSeek {
    async fn next_batch(&mut self) -> Result<Option<OwnedBatch>> {
        if let Some(first) = self.first.take() {
            return Ok(Some(first));
        }
        let Some(state) = &mut self.state else {
            return Ok(None);
        };
        let capacity = BATCH_ROWS.min(state.remaining);
        let mut rows = Vec::with_capacity(capacity);
        while rows.len() < capacity {
            let Some(head) = state.scan.next().await? else {
                break;
            };
            if !state
                .predicates
                .iter()
                .all(|predicate| predicate.matches(head.tuple()))
            {
                continue;
            }
            if let Some(slot) = state.distinct_slot {
                if state
                    .last
                    .as_ref()
                    .is_some_and(|last| last.tuple().cell(slot) == head.tuple().cell(slot))
                {
                    continue;
                }
                state.last = Some(head.clone());
            }
            rows.push(head);
        }
        let batch = if rows.is_empty() {
            None
        } else {
            Some(state.projection.batch(&state.scan.tree.memory, &rows)?)
        };
        state.remaining -= rows.len();
        let done = rows.len() < capacity || state.remaining == 0;
        drop(rows);
        if done {
            self.state = None;
        }
        Ok(batch)
    }
}

async fn next(
    cursor: &mut Cursor,
    tree: &Tree,
    range: &Range,
    bucket: Bucket,
    slot: usize,
    reverse: bool,
) -> Result<Option<Head>> {
    let hit = cursor
        .next(tree, |key| match key {
            KeyRef::Posting(candidate, tuple, _) => {
                candidate.cmp(&bucket).then_with(|| range.position(tuple))
            }
            KeyRef::Row(_) | KeyRef::Revision(_) => Ordering::Greater,
        })
        .await?;
    let Some(hit) = hit else {
        return Ok(None);
    };
    let KeyRef::Posting(_, _, iid) = hit.key() else {
        return Err(super::format_error(
            "Ordered index seek escaped its posting prefix",
        ));
    };
    Ok(Some(Head {
        hit,
        iid,
        slot,
        reverse,
    }))
}

struct Covered {
    columns: Vec<(usize, ScalarType)>,
    schema: Arc<Schema>,
}

impl Covered {
    fn bind(
        request: &ScanRequest<'_>,
        definition: &Definition,
        projection: &[usize],
    ) -> Result<Option<Self>> {
        let fields = projection
            .iter()
            .map(|&position| request.scan.columns.get(position))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| super::format_error("Covered scan position exceeds its schema"))?;
        let mut columns = Vec::new();
        for field in &fields {
            let logical = match &field.data_type {
                LogicalType::Maybe(value) => value.as_ref(),
                logical => logical,
            };
            let LogicalType::Scalar(scalar) = logical else {
                return Ok(None);
            };
            use ScalarType as S;
            if !matches!(
                scalar,
                S::Utf8
                    | S::Binary
                    | S::Bool
                    | S::Uuid
                    | S::DateDay
                    | S::I8
                    | S::I16
                    | S::I32
                    | S::I64
                    | S::U8
                    | S::U16
                    | S::U32
                    | S::U64
            ) {
                return Ok(None);
            }
            let Some(slot) = definition
                .columns
                .iter()
                .position(|column| column == &field.name)
            else {
                return Ok(None);
            };
            columns.push((slot, scalar.clone()));
        }
        let fields = fields
            .iter()
            .map(|field| to_field(&field.name, &field.data_type))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(Self {
            columns,
            schema: Arc::new(Schema::new(fields)),
        }))
    }

    fn batch(&self, memory: &Arc<MemoryBudget>, rows: &[Head]) -> Result<OwnedBatch> {
        let mut builder = BatchBuilder::new(4096 + self.columns.len().saturating_mul(256), memory)?;
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (slot, scalar) in &self.columns {
            let cells = || rows.iter().map(|head| head.tuple().cell(*slot));
            let retained = cells()
                .map(|cell| cell.retained_bytes())
                .fold(0usize, usize::saturating_add);
            if retained > i32::MAX as usize {
                return Err(super::format_error("Covered column exceeds Arrow offsets"));
            }
            let bound = retained
                .saturating_mul(2)
                .saturating_add(rows.len() * 64 + 1024);
            arrays.push(builder.column(bound, || covered_array(scalar, retained, cells()))?);
        }
        let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len()));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &options)
            .map_err(super::format_error)?;
        builder.finish(batch)
    }
}

fn covered_array<'a>(
    scalar: &ScalarType,
    payload_bytes: usize,
    cells: impl ExactSizeIterator<Item = AtomRef<'a>>,
) -> Result<ArrayRef> {
    let bad_type = || super::format_error("Posting does not have the covered field's type");
    macro_rules! cells {
        ($variant:ident, |$value:ident| $convert:expr) => {
            cells.map(|cell| match cell {
                AtomRef::$variant($value) => Ok(Some($convert)),
                AtomRef::Null => Ok(None),
                _ => Err(bad_type()),
            })
        };
    }
    macro_rules! integer {
        ($array:ty, $ty:ty) => {
            Arc::new(
                cells!(Integer, |value| <$ty>::try_from(value)
                    .map_err(super::format_error)?)
                .collect::<Result<$array>>()?,
            )
        };
    }
    use ScalarType as S;
    Ok(match scalar {
        S::Utf8 => {
            let mut builder = StringBuilder::with_capacity(cells.len(), payload_bytes);
            for value in cells!(Text, |value| value) {
                builder.append_option(value?);
            }
            Arc::new(builder.finish())
        }
        S::Binary => {
            let mut builder = BinaryBuilder::with_capacity(cells.len(), payload_bytes);
            for value in cells!(Binary, |value| value) {
                builder.append_option(value?);
            }
            Arc::new(builder.finish())
        }
        S::Bool => Arc::new(cells!(Bool, |value| value).collect::<Result<BooleanArray>>()?),
        S::DateDay => Arc::new(
            cells!(Day, |value| i32::try_from(value)
                .map_err(super::format_error)?)
            .collect::<Result<Date32Array>>()?,
        ),
        S::I8 => integer!(Int8Array, i8),
        S::I16 => integer!(Int16Array, i16),
        S::I32 => integer!(Int32Array, i32),
        S::I64 => integer!(Int64Array, i64),
        S::U8 => integer!(UInt8Array, u8),
        S::U16 => integer!(UInt16Array, u16),
        S::U32 => integer!(UInt32Array, u32),
        S::U64 => integer!(UInt64Array, u64),
        S::Uuid => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(cells.len(), 16);
            for cell in cells!(Uuid, |value| value) {
                match cell? {
                    Some(value) => builder.append_value(value).map_err(super::format_error)?,
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        _ => return Err(bad_type()),
    })
}

async fn filter_payload(
    request: &ScanRequest<'_>,
    residuals: &[Expr],
    pending: &mut Vec<Iid>,
    output: &mut Vec<Iid>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let matching: BTreeSet<_> = request
        .verifier
        .visible_columns(pending.iter().copied().collect(), &[], residuals)
        .await?
        .into_iter()
        .map(|(iid, _)| iid)
        .collect();
    output.extend(pending.drain(..).filter(|iid| matching.contains(iid)));
    Ok(())
}

#[derive(Clone)]
struct Head {
    hit: Hit,
    iid: Iid,
    slot: usize,
    reverse: bool,
}

impl Head {
    fn tuple(&self) -> Tuple<'_> {
        let KeyRef::Posting(_, tuple, _) = self.hit.key() else {
            unreachable!("head is a posting")
        };
        tuple
    }
}

impl PartialEq for Head {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Head {}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self
            .tuple()
            .compare(other.tuple())
            .then_with(|| (self.iid, self.slot).cmp(&(other.iid, other.slot)));
        if self.reverse { order } else { order.reverse() }
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
