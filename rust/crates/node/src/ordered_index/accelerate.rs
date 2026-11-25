use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap, HashMap},
    ops::Range as RowRange,
};

use arrow_array::{
    Array, ArrayAccessor, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    FixedSizeBinaryArray, GenericByteArray, Int8Array, Int16Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    builder::{
        ArrayBuilder, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, GenericByteBuilder,
        PrimitiveBuilder, StringBuilder,
    },
    types::ByteArrayType,
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, OffsetBuffer};
use arrow_schema::Schema;
use async_trait::async_trait;
use rtdb_columnar::{
    BatchBuilder, MemoryBudget, MemoryReservation, OwnedBatch, retain_buffer_with_owner, to_field,
};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, UnaryOp};
use rtdb_query::{ConstantEvaluator, OrderedBatches, OrderedCandidates, OrderedIidBuffer, OrderedScan, ScanAccelerator, ScanRequest, ScanSelection};
use rtdb_types::{AnomalyCategory, Iid, LogicalType, Result, ScalarType, TimeUnit, Timestamp};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::{
    Definition, OwnedProjection, checkpoint, definitions,
    key::{Atom, AtomRef, Bucket, Family, family, owned_family},
    maintenance::{cancelled, read_root, source_frontier, source_rows},
    postings::{Kind, Tuple},
    tree::{Cursor, Hit, KeyRef, Measure, MeasureDomain, Root, Tree, cardinality_overflow},
};

const BATCH_ROWS: usize = 128;
const MAX_CANDIDATES: usize = 16_384;
const MAX_OWNED_REPLAY_ROWS: i64 = 4_096;

pub(crate) struct OrderedIndexAccelerator;

async fn collect_candidates(request: &ScanRequest<'_>) -> Result<Option<OrderedIidBuffer>> {
    let Some(OrderedScan::Payload { mut candidates, .. }) = exact(request, None, &[]).await? else {
        return Ok(None);
    };
    let collected: Result<_> = async {
        let mut selected = OrderedIidBuffer::with_capacity(0, request.memory)?;
        while let Some(chunk) = candidates.next_iids(BATCH_ROWS).await? {
            if !selected.merge_distinct(chunk, MAX_CANDIDATES)? {
                return Ok(None);
            }
        }
        Ok(Some(selected))
    }.await;
    match collected {
        Err(error) if error.category != AnomalyCategory::Interrupted => Ok(None),
        result => result,
    }
}

#[async_trait]
impl ScanAccelerator for OrderedIndexAccelerator {
    fn accepts_arrow_iids(&self) -> bool { true }

    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        let Some(ids) = collect_candidates(request).await? else { return Ok(None); };
        let mut keys = BTreeSet::new();
        for iid in ids.iter() { keys.insert(iid); }
        Ok(Some(keys))
    }

    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        let Some(ids) = collect_candidates(request).await? else { return Ok(None); };
        Ok(Some(ScanSelection { iids: ids.into_sorted_membership()?, scores: None }))
    }

    async fn ordered(
        &self,
        request: &ScanRequest<'_>,
        window: RowRange<usize>,
        projection: &[usize],
    ) -> Result<Option<OrderedScan>> {
        if window.end > MAX_CANDIDATES {
            return Ok(None);
        }
        exact(request, Some(window), projection).await
    }
}

async fn exact(
    request: &ScanRequest<'_>,
    window: Option<RowRange<usize>>,
    projection: &[usize],
) -> Result<Option<OrderedScan>> {
    let table = &request.scan.table.name.table;
    if table.schema == super::CATALOG.0
        || request.iid_scope().is_some()
        || !request.bounds.system_time.is_point()
        || !request.bounds.valid_time.is_point()
        || (request.scan.predicates.is_empty() && request.scan.ordered_top_k.is_none())
    {
        return Ok(None);
    }
    match seek(request, window, projection).await {
        Err(error) if error.category == AnomalyCategory::Interrupted => Err(error),
        Err(_) => Ok(None),
        found => found,
    }
}

async fn seek(
    request: &ScanRequest<'_>,
    window: Option<RowRange<usize>>,
    projection: &[usize],
) -> Result<Option<OrderedScan>> {
    let Some(definitions) = definitions(
        request.database,
        request.snapshot,
        request.storage,
        request.memory,
        request.current_time,
        request.cancellation,
    )
    .await? else {
        return Ok(None);
    };
    let constants = Constants { evaluator: request.constants, atoms: Default::default() };
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
        let range = Range::bind(request, &fields, &constants).await?;
        let top = if let Some(window) = &window {
            let Some((reverse, distinct)) = order(request, &fields, &range) else {
                continue;
            };
            Some(Top {
                limit: window.end,
                offset: window.start,
                reverse,
                distinct,
            })
        } else {
            if range.is_open() {
                continue;
            }
            None
        };
        if let Some(found) = use_index(request, definition, &fields, range, top, projection, &constants).await?
        {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

struct Constants<'a> {
    evaluator: &'a dyn ConstantEvaluator,
    atoms: parking_lot::Mutex<HashMap<usize, Option<Atom>>>,
}

impl Constants<'_> {
    async fn atom(&self, expression: &Expr) -> Result<Option<Atom>> {
        let key = std::ptr::from_ref(expression) as usize;
        if let Some(value) = self.atoms.lock().get(&key) { return Ok(value.clone()); }
        let value = self.evaluator.constant(expression).await?.as_ref().and_then(Atom::of);
        self.atoms.lock().insert(key, value.clone());
        Ok(value)
    }
}

trait BoundAtom {
    fn view(&self) -> AtomRef<'_>;
}

impl BoundAtom for Atom {
    fn view(&self) -> AtomRef<'_> { self.borrowed() }
}

impl BoundAtom for AtomRef<'_> {
    fn view(&self) -> AtomRef<'_> { *self }
}

#[derive(Debug)]
struct Range<T = Atom> {
    prefix: Vec<T>,
    lower: Option<(T, bool)>,
    upper: Option<(T, bool)>,
    cursors: Vec<CursorBound<T>>,
}

impl<T> Default for Range<T> {
    fn default() -> Self {
        Self { prefix: Vec::new(), lower: None, upper: None, cursors: Vec::new() }
    }
}

impl Range<Atom> {
    fn is_open(&self) -> bool {
        self.prefix.is_empty()
            && self.lower.is_none()
            && self.upper.is_none()
            && self.cursors.is_empty()
    }

    async fn bind(request: &ScanRequest<'_>, fields: &[Option<&Field>], constants: &Constants<'_>) -> Result<Self> {
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
                let Some(atom) = constants.atom(constant).await?.filter(|atom| atom.family() == Some(family))
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
            if let Some(cursor) = CursorBound::bind(constants, predicate, suffix).await? {
                range.cursors.push(cursor);
            }
        }
        Ok(range)
    }
}

impl<T: BoundAtom> Range<T> {
    fn position(&self, key: Tuple<'_>) -> Ordering {
        let prefix = key
            .iter()
            .zip(&self.prefix)
            .map(|(value, bound)| value.cmp(&bound.view()))
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal);
        if prefix != Ordering::Equal {
            return prefix;
        }
        let Some(next) = key.get(self.prefix.len()) else {
            return Ordering::Equal;
        };
        if self.lower.as_ref().is_some_and(|(lower, strict)| {
            next < lower.view() || (*strict && next == lower.view())
        }) || self
            .cursors
            .iter()
            .any(|bound| bound.position(key, self.prefix.len()) == Ordering::Less)
        {
            return Ordering::Less;
        }
        if self.upper.as_ref().is_some_and(|(upper, strict)| {
            next > upper.view() || (*strict && next == upper.view())
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
struct CursorBound<T> {
    key: Vec<T>,
    lower: bool,
    strict: bool,
    _allocation: Option<Arc<MemoryReservation>>,
}

impl CursorBound<Atom> {
    async fn bind(
        constants: &Constants<'_>,
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
            let Some(atom) = constants.atom(constant).await?.filter(|atom| atom.family() == Some(expected)) else {
                return Ok(None);
            };
            key.push(atom);
        }
        Ok(Some(Self {
            key,
            lower: matches!(operator, BinaryOp::Gt | BinaryOp::GtEq),
            strict: matches!(operator, BinaryOp::Gt | BinaryOp::Lt),
            _allocation: None,
        }))
    }
}

impl<T: BoundAtom> CursorBound<T> {
    fn position(&self, tuple: Tuple<'_>, offset: usize) -> Ordering {
        let order = tuple
            .iter()
            .skip(offset)
            .zip(&self.key)
            .map(|(value, bound)| value.cmp(&bound.view()))
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
    offset: usize,
    reverse: bool,
    distinct: Option<DistinctPrefix>,
}

#[derive(Clone, Copy)]
struct DistinctPrefix {
    start: usize,
    end: usize,
}

fn order(
    request: &ScanRequest<'_>,
    fields: &[Option<&Field>],
    range: &Range,
) -> Option<(bool, Option<DistinctPrefix>)> {
    let plan = request.scan.ordered_top_k.as_ref()?;
    let order = &plan.order;
    let equal = &fields[..range.prefix.len()];
    let mut suffix = order.iter().filter(|sort| {
        !matches!(&sort.expr, Expr::Column { id, .. } if equal.iter().flatten().any(|field| field.id == *id))
    }).peekable();
    let reverse = suffix.peek()?.descending;
    let start = range.prefix.len();
    let mut end = start;
    for sort in suffix {
        let field = fields.get(end)?.as_ref()?;
        let family = family(&field.data_type)?;
        if !matches!(&sort.expr, Expr::Column { id, .. } if *id == field.id)
            || sort.descending != reverse
            || (sort.nulls_first != reverse && field.data_type.nullable() && field.name != "_id")
            // SQL DISTINCT coalesces signed zeros, which can fall into
            // different compound sort bands. Only adjacent equivalence
            // classes may be collapsed by a prefix seek.
            || (plan.distinct && family == Family::Float)
        {
            return None;
        }
        end += 1;
    }
    Some((
        reverse,
        plan.distinct.then_some(DistinctPrefix { start, end }),
    ))
}

#[derive(Debug)]
enum Predicate {
    All(Box<Self>, Box<Self>),
    Either(Box<Self>, Box<Self>),
    Null {
        slot: usize,
        expected: bool,
    },
    Compare {
        slot: usize,
        op: BinaryOp,
        value: Atom,
    },
}

impl Predicate {
    fn bind<'a>(
        constants: &'a Constants<'_>,
        fields: &'a [Option<&Field>],
        expression: &'a Expr,
    ) -> futures::future::BoxFuture<'a, Result<Option<Self>>> {
        Box::pin(async move {
            if let Expr::Unary {
                op: op @ (UnaryOp::IsNull | UnaryOp::IsNotNull),
                expr,
            } = expression
                && let Expr::Column { id, .. } = expr.as_ref()
            {
                return Ok(fields
                    .iter()
                    .position(|field| field.is_some_and(|field| field.id == *id))
                    .map(|slot| Self::Null {
                        slot,
                        expected: *op == UnaryOp::IsNull,
                    }));
            }
            let Expr::Binary { op, left, right } = expression else {
                return Ok(None);
            };
            match op {
                BinaryOp::And | BinaryOp::Or => {
                    let Some(left) = Self::bind(constants, fields, left).await? else {
                        return Ok(None);
                    };
                    let Some(right) = Self::bind(constants, fields, right).await? else {
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
                        let Some(value) = constants.atom(constant).await?
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
            Self::Null { slot, expected } => (tuple.cell(*slot) == AtomRef::Null) == *expected,
            Self::Compare { slot, op, value } => {
                let cell = tuple.cell(*slot);
                let value = value.borrowed();
                if matches!(
                    cell,
                    AtomRef::Null
                        | AtomRef::Unordered
                        | AtomRef::FloatOrder(super::key::FLOAT_NAN_ORDER)
                ) || value == AtomRef::Null
                {
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
    constants: &Constants<'_>,
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
            if let Some(predicate) = Predicate::bind(constants, fields, predicate).await? {
                predicates.push(predicate);
            } else {
                covered = false;
            }
        }
    }
    let distinct = top.and_then(|top| top.distinct);
    if distinct.is_some() && !covered {
        return Ok(None);
    }
    let projection = if top.is_some() && covered {
        Covered::bind(&request.scan.columns, definition, projection)?
    } else {
        None
    };
    if distinct.is_some() && projection.is_none() {
        return Ok(None);
    }
    if top.is_some_and(|top| top.offset >= top.limit) {
        return Ok(Some(if projection.is_some() {
            OrderedScan::Covered(Box::new(CoveredSeek::empty()))
        } else {
            OrderedScan::Payload {
                candidates: Box::new(PostingCandidates(None)),
                verify: false,
            }
        }));
    }

    let directory = definition.directory(request.storage)?;
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
    let scan = PostingScan::open(
        tree, root, request.bounds.valid_time.lower(), range, reverse, definition.columns.len(),
        request.cancellation,
    ).await?;

    let postings = MatchingPostings {
        scan,
        predicates,
        distinct,
        last: None,
        identity_slot: None,
    };
    if let (Some(projection), Some(top)) = (projection, top) {
        let seek = CoveredSeek {
            state: Some(CoveredState {
                projection,
                postings,
                skip: top.offset,
                remaining: top.limit - top.offset,
            }),
            sliced_rows: 0,
            gathered_rows: 0,
        };
        return Ok(Some(OrderedScan::Covered(Box::new(seek))));
    }

    Ok(Some(OrderedScan::Payload {
        candidates: Box::new(PostingCandidates(Some(postings))),
        verify: !covered,
    }))
}

async fn covered_root(
    context: &rtdb_query::QueryContext,
    snapshot: &rtdb_index::Snapshot,
    storage: &Arc<rtdb_storage::cache::BufferPool>,
    definition: &Definition,
    present: Timestamp,
) -> Result<(Tree, Root)> {
    let through = source_frontier(snapshot, &definition.table);
    let directory = definition.directory(storage)?;
    let checkpoint = checkpoint::latest(storage, &directory, &context.memory, through).await?;
    let durable_rows = source_rows(snapshot, &definition.table);
    let covered_rows = checkpoint
        .as_ref()
        .map_or(0, |checkpoint| checkpoint.source_rows);
    let frontier = snapshot.relation_event_frontier(&definition.table)?;
    if covered_rows > durable_rows || frontier - covered_rows > MAX_OWNED_REPLAY_ROWS {
        return Err(super::uncovered_owned_range());
    }
    let scope = checkpoint.as_ref().map_or(0, |checkpoint| checkpoint.scope);
    let tree = Tree::new(storage, &directory, &context.memory, scope).await?;
    let root = read_root(
        &tree,
        definition,
        snapshot,
        checkpoint,
        present,
        &context.cancellation,
    )
    .await?
    .ok_or_else(super::uncovered_owned_range)?;
    Ok((tree, root))
}

pub(super) async fn owned_page(
    context: &rtdb_query::QueryContext,
    snapshot: &rtdb_index::Snapshot,
    storage: &Arc<rtdb_storage::cache::BufferPool>,
    definition: &Definition,
    projection: OwnedProjection,
    bounds: RowBounds<'_>,
    valid_at: Option<Timestamp>,
    cover_until: Option<Timestamp>,
) -> Result<(super::OwnedRangePage, Option<Timestamp>)> {
    let OwnedProjection { fields, empty } = projection;
    let positions: Vec<_> = (0..fields.len()).collect();
    let covered =
        Covered::bind(&fields, definition, &positions)?.ok_or_else(super::uncovered_owned_range)?;
    let (range, reverse, limit) = bounds.bind(&fields, &context.memory)?;
    if !(1..=BATCH_ROWS).contains(&limit) {
        return Err(rtdb_types::Anomaly::incorrect(
            "rtdb.owned/range-limit",
            "Owned range limit must be between 1 and 128",
        ));
    }

    let present = context.current_time.as_micros()?;
    let valid_at = valid_at.unwrap_or(present);
    valid_at.next()?;
    if cover_until.is_some_and(|until| until <= valid_at) {
        return Err(super::uncovered_owned_range());
    }
    if empty {
        return Ok((super::OwnedRangePage { schema: covered.schema, batch: None }, cover_until));
    }
    let (tree, root) = covered_root(context, snapshot, storage, definition, present).await?;
    let covered_until = match cover_until {
        Some(until) => Some(range_clock_cutoff(
            &tree, &root, &range, valid_at, until, &context.cancellation,
        ).await?),
        None => None,
    };
    let scan = PostingScan::open(
        tree, root, valid_at, range, reverse, definition.columns.len(), &context.cancellation,
    ).await?;
    let schema = Arc::clone(&covered.schema);
    let mut seek = CoveredSeek {
        state: Some(CoveredState {
            projection: covered,
            postings: MatchingPostings {
                scan,
                predicates: Vec::new(),
                distinct: None,
                last: None,
                identity_slot: Some(definition.columns.iter().position(|name| name == "_id")
                    .ok_or_else(super::uncovered_owned_range)?),
            },
            skip: 0,
            remaining: limit,
        }),
        sliced_rows: 0,
        gathered_rows: 0,
    };
    let batch = seek.next_batch().await?;
    Ok((super::OwnedRangePage { schema, batch }, covered_until))
}

async fn range_clock_cutoff(
    tree: &Tree,
    root: &Root,
    range: &Range<impl BoundAtom + Sync>,
    valid_at: Timestamp,
    until: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Timestamp> {
    use super::domain::offset;

    let mut end = offset(until.0);
    for bucket in Bucket::at(valid_at) {
        if cancellation.is_cancelled() { return Err(cancelled()); }
        let next_start = u128::from(bucket.start) + (1u128 << bucket.level);
        if next_start >= end {
            continue;
        }
        let mut current = Cursor::new(root.clone(), false);
        if next(&mut current, tree, range, bucket, 0, false).await?.is_some() {
            end = next_start;
            continue;
        }

        // Jump over keys outside the range within each occupied bucket.
        // Starting with every level's next bucket covers future insertions,
        // including those invisible at the sampled instant.
        let mut start = next_start;
        while start < end {
            if cancellation.is_cancelled() { return Err(cancelled()); }
            let mut cursor = Cursor::new(root.clone(), false);
            let first = cursor.next(tree, |key| match key {
                KeyRef::Posting(found, ..) if found.level < bucket.level
                    || (found.level == bucket.level && u128::from(found.start) < start) => Ordering::Less,
                KeyRef::Posting(found, ..) if found.level == bucket.level
                    && u128::from(found.start) < end => Ordering::Equal,
                _ => Ordering::Greater,
            }).await?;
            let Some(first) = first else { break; };
            let KeyRef::Posting(found, _, _) = first.key() else {
                return Err(super::uncovered_owned_range());
            };
            let mut candidate = Cursor::new(root.clone(), false);
            if next(&mut candidate, tree, range, found, 0, false).await?.is_some() {
                end = u128::from(found.start);
                break;
            }
            start = u128::from(found.start) + (1u128 << found.level);
        }
    }
    Ok(Timestamp((end as i128 + i64::MIN as i128) as i64))
}

pub(super) async fn owned_count(
    context: &rtdb_query::QueryContext,
    snapshot: &rtdb_index::Snapshot,
    storage: &Arc<rtdb_storage::cache::BufferPool>,
    definition: &Definition,
    projection: OwnedProjection,
    request: super::OwnedCountRequest<'_>,
    lifetime: Option<i64>,
    domain: Option<MeasureDomain>,
) -> Result<super::OwnedRangeCount> {
    let OwnedProjection { fields, empty } = projection;
    let measured = domain.is_some();
    let decimal = match domain {
        Some(MeasureDomain::Decimal128(precision, scale)) => Some((precision, scale)),
        _ => None,
    };
    let measure_position = fields.len().checked_sub(2);
    if let Some((precision, scale)) = decimal {
        if measure_position.and_then(|position| owned_family(&fields[position].data_type))
            != Some(Family::Decimal128(precision, scale))
        {
            return Err(super::uncovered_owned_range());
        }
    }
    let positions: Vec<_> = (0..fields.len())
        .filter(|position| decimal.is_none() || Some(*position) != measure_position).collect();
    Covered::bind(&fields, definition, &positions)?.ok_or_else(super::uncovered_owned_range)?;
    let mut range = Range::default();
    compound_bound(&mut range, &fields, request.lower, true, false, &context.memory)?;
    compound_bound(&mut range, &fields, request.upper, false, true, &context.memory)?;
    request.valid_at.next()?;
    let present = context.current_time.as_micros()?;
    if lifetime.is_some() && request.valid_at != present {
        return Err(super::uncovered_owned_range());
    }
    if empty {
        return Ok(super::OwnedRangeCount {
            count: 0,
            sum: (domain == Some(MeasureDomain::Int64)).then_some(0),
            decimal_sum: decimal.map(|(_, scale)| super::OwnedDecimalSum { coefficient: [0; 32], scale }),
            present, valid_at: request.valid_at, expiry: None,
        });
    }
    let (tree, root) = covered_root(context, snapshot, storage, definition, present).await?;
    let (count, measure) = rank_owned_range(
        &tree, &root, &range, request.valid_at, &context.cancellation, measured,
    ).await?;
    if measured && measure.count != count {
        return Err(super::uncovered_owned_range());
    }
    let (sum, decimal_sum) = match domain {
        Some(MeasureDomain::Int64) => (
            Some(measure.coefficient(MeasureDomain::Int64)?.to_i128().ok_or_else(cardinality_overflow)?), None,
        ),
        Some(expected @ MeasureDomain::Decimal128(_, scale)) => (
            None, Some(super::OwnedDecimalSum { coefficient: measure.coefficient(expected)?.to_be_bytes(), scale }),
        ),
        None => (None, None),
        _ => return Err(super::uncovered_owned_range()),
    };
    let expiry = if let Some(duration) = lifetime.filter(|_| count != 0) {
        let mut live = present.0;
        let mut expired = live.checked_add(duration).filter(|until| *until > live && *until < i64::MAX)
            .ok_or_else(super::uncovered_owned_range)?;
        // Every posting in this system root started at its immutable
        // acceptance, no later than present. At present + duration all
        // of them have expired. Probe only this root's valid-time ranks.
        while expired - live > 1 {
            context.check_cancelled()?;
            let middle = live + (expired - live) / 2;
            match rank_owned_range(&tree, &root, &range, Timestamp(middle), &context.cancellation, false).await?.0 {
                current if current == count => live = middle,
                current if current < count => expired = middle,
                _ => return Err(super::uncovered_owned_range()),
            }
        }
        Some(Timestamp(expired))
    } else {
        None
    };
    Ok(super::OwnedRangeCount { count, sum, decimal_sum, present, valid_at: request.valid_at, expiry })
}

async fn rank_owned_range(
    tree: &Tree, root: &Root, range: &Range<AtomRef<'_>>, valid_at: Timestamp,
    cancellation: &CancellationToken, measured: bool,
) -> Result<(u64, Measure)> {
    let mut count = 0u64;
    let mut measure = Measure::default();
    for bucket in Bucket::at(valid_at) {
        let compare = |key: KeyRef<'_>| match key {
            KeyRef::Posting(found, tuple, _) => {
                found.cmp(&bucket).then_with(|| range.position(tuple))
            }
            KeyRef::Row(_) | KeyRef::Revision(_) => Ordering::Greater,
        };
        let (lower, lower_measure) = tree
            .rank_and_measure(
                root,
                |key| compare(key) == Ordering::Less,
                cancellation,
                measured,
            )
            .await?;
        let (upper, upper_measure) = tree
            .rank_and_measure(
                root,
                |key| compare(key) != Ordering::Greater,
                cancellation,
                measured,
            )
            .await?;
        let bucket_count = upper
            .checked_sub(lower)
            .ok_or_else(|| super::format_error("Ordered count ranks are inconsistent"))?;
        count = count
            .checked_add(bucket_count)
            .ok_or_else(cardinality_overflow)?;
        measure = measure.checked_add(upper_measure.checked_sub(lower_measure)?)?;
    }
    Ok((count, measure))
}

pub(crate) enum RowBounds<'a> {
    Leading {
        lower: Option<&'a rtdb_types::Value>,
        upper: Option<&'a rtdb_types::Value>,
        limit: usize,
    },
    Compound(super::OwnedRangeRequest<'a>),
}

pub(super) fn invalid_bound() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::incorrect(
        "rtdb.owned/range-bound",
        "Owned range bound does not match its declared path key",
    )
}

fn bound_atom<'a>(field: &Field, value: &'a rtdb_types::Value) -> Result<AtomRef<'a>> {
    admit_bound_atom(field, AtomRef::of(value).ok_or_else(invalid_bound)?)
}

fn admit_bound_atom<'a>(field: &Field, atom: AtomRef<'a>) -> Result<AtomRef<'a>> {
    let expected = owned_family(&field.data_type).ok_or_else(invalid_bound)?;
    if atom.family() == Some(expected)
        || (matches!(atom, AtomRef::Null) && field.name != "_id" && field.data_type.nullable())
    {
        Ok(atom)
    } else {
        Err(invalid_bound())
    }
}

fn compound_bound<'a>(
    range: &mut Range<AtomRef<'a>>,
    fields: &[Field],
    key: Option<super::OwnedKeyBound<'a>>,
    lower: bool,
    strict: bool,
    memory: &Arc<MemoryBudget>,
) -> Result<()> {
    let Some(key) = key else {
        return Ok(());
    };
    use super::OwnedKeyBound;
    let width = match key {
        OwnedKeyBound::Values(key) => key.len(),
        OwnedKeyBound::Arrow { .. } => key.arrow_cell().and_then(|cell| cell.structure()).ok_or_else(invalid_bound)?.len(),
    };
    if width != fields.len() {
        return Err(invalid_bound());
    }
    let cell = |field: &Field| match key {
        OwnedKeyBound::Values(key) => {
            let value = key.get(&field.name).ok_or_else(invalid_bound)?;
            if !field.data_type.legs().contains(&value.logical_type()) { return Err(invalid_bound()); }
            bound_atom(field, value)
        }
        OwnedKeyBound::Arrow { .. } => {
            let cell = key.arrow_field(&field.name).ok_or_else(invalid_bound)?;
            if !field.data_type.legs().contains(&cell.logical_type()?) { return Err(invalid_bound()); }
            let atom = AtomRef::from_arrow(cell.field, cell.array, cell.row).map_err(|_| invalid_bound())?;
            if !atom.is_bound() { return Err(invalid_bound()); }
            admit_bound_atom(field, atom)
        }
    };
    fields.iter().try_for_each(|field| cell(field).map(drop))?;
    let allocation = memory.reserve(
        width.saturating_mul(size_of::<AtomRef<'_>>())
            .saturating_add(4 * size_of::<CursorBound<AtomRef<'_>>>()),
    )?;
    let mut key = Vec::with_capacity(width);
    for field in fields {
        key.push(cell(field)?);
    }
    range.cursors.push(CursorBound { key, lower, strict, _allocation: Some(allocation) });
    Ok(())
}

impl<'a> RowBounds<'a> {
    pub(super) fn identity_bounds(&self) -> [Option<super::OwnedKeyBound<'a>>; 3] {
        match self {
            Self::Leading { .. } => [None; 3],
            Self::Compound(request) => [request.lower, request.upper, request.after],
        }
    }

    fn bind(self, fields: &[Field], memory: &Arc<MemoryBudget>) -> Result<(Range<AtomRef<'a>>, bool, usize)> {
        let mut range = Range::default();
        match self {
            Self::Leading {
                lower,
                upper,
                limit,
            } => {
                let leading = fields.first().ok_or_else(invalid_bound)?;
                range.lower = lower
                    .map(|value| bound_atom(leading, value))
                    .transpose()?
                    .map(|atom| (atom, false));
                range.upper = upper
                    .map(|value| bound_atom(leading, value))
                    .transpose()?
                    .map(|atom| (atom, true));
                Ok((range, false, limit))
            }
            Self::Compound(request) => {
                for (key, lower, strict) in [
                    (request.lower, true, false),
                    (request.upper, false, true),
                    (request.after, !request.descending, true),
                ] {
                    compound_bound(&mut range, fields, key, lower, strict, memory)?;
                }
                Ok((range, request.descending, request.limit))
            }
        }
    }
}

struct MatchingPostings<T = Atom> {
    scan: PostingScan<T>,
    predicates: Vec<Predicate>,
    distinct: Option<DistinctPrefix>,
    last: Option<Head>,
    identity_slot: Option<usize>,
}

impl<T: BoundAtom + Send + Sync> MatchingPostings<T> {
    async fn next(&mut self) -> Result<Option<Head>> {
        while let Some(head) = self.scan.next().await? {
            if let Some(slot) = self.identity_slot { head.require_identity(slot)?; }
            if !self
                .predicates
                .iter()
                .all(|predicate| predicate.matches(head.tuple()))
            {
                continue;
            }
            if let Some(prefix) = self.distinct {
                let tuple = head.tuple();
                if self.last.as_ref().is_some_and(|last| {
                    let previous = last.tuple();
                    (prefix.start..prefix.end).all(|slot| previous.cell(slot) == tuple.cell(slot))
                }) {
                    continue;
                }
                self.last = Some(head.clone());
            }
            return Ok(Some(head));
        }
        Ok(None)
    }
}

struct PostingCandidates(Option<MatchingPostings>);

#[async_trait]
impl OrderedCandidates for PostingCandidates {
    async fn next_iids(&mut self, maximum: usize) -> Result<Option<OrderedIidBuffer>> {
        let Some(postings) = &mut self.0 else {
            return Ok(None);
        };
        let capacity = maximum.min(BATCH_ROWS);
        if capacity == 0 { return Ok(None); }
        if postings.scan.cancellation.is_cancelled() { return Err(cancelled()); }
        let mut ids = OrderedIidBuffer::with_capacity(capacity, &postings.scan.tree.memory)?;
        while ids.len() < capacity {
            let Some(head) = postings.next().await? else {
                self.0 = None;
                break;
            };
            ids.push(head.address().iid);
        }
        Ok((!ids.is_empty()).then_some(ids))
    }
}

struct PostingScan<T> {
    heap: BinaryHeap<Head>,
    cursors: Vec<Cursor>,
    buckets: Vec<Bucket>,
    advanced: Option<usize>,
    range: Range<T>,
    reverse: bool,
    width: usize,
    cancellation: CancellationToken,
    _scratch: Arc<MemoryReservation>,
    tree: Tree,
}

impl<T: BoundAtom + Send + Sync> PostingScan<T> {
    async fn open(
        tree: Tree,
        root: Root,
        valid_at: Timestamp,
        range: Range<T>,
        reverse: bool,
        width: usize,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let buckets = Bucket::at(valid_at);
        let capacity = buckets.len();
        let scratch = tree.memory.reserve(
            capacity * (size_of::<Bucket>() + size_of::<Cursor>() + size_of::<Head>()),
        )?;
        let mut scan = Self {
            heap: BinaryHeap::with_capacity(capacity),
            cursors: (0..capacity).map(|_| Cursor::new(root.clone(), reverse)).collect(),
            buckets: buckets.collect(),
            advanced: None,
            range,
            reverse,
            width,
            cancellation: cancellation.clone(),
            _scratch: scratch,
            tree,
        };
        for slot in 0..capacity {
            scan.advance(slot).await?;
        }
        Ok(scan)
    }

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

struct CoveredState<T> {
    projection: Covered,
    postings: MatchingPostings<T>,
    skip: usize,
    remaining: usize,
}

struct CoveredSeek<T = Atom> {
    state: Option<CoveredState<T>>,
    sliced_rows: u64,
    gathered_rows: u64,
}

impl CoveredSeek {
    fn empty() -> Self {
        Self {
            state: None,
            sliced_rows: 0,
            gathered_rows: 0,
        }
    }
}

#[async_trait]
impl<T: BoundAtom + Send + Sync> OrderedBatches for CoveredSeek<T> {
    async fn next_batch(&mut self) -> Result<Option<OwnedBatch>> {
        let Some(state) = &mut self.state else {
            return Ok(None);
        };
        let capacity = BATCH_ROWS.min(state.remaining);
        let _staging = state.postings.scan.tree.memory.reserve(capacity * size_of::<Head>())?;
        let mut rows = Vec::with_capacity(capacity);
        while rows.len() < capacity {
            let Some(head) = state.postings.next().await? else {
                break;
            };
            if state.skip != 0 {
                state.skip -= 1;
                continue;
            }
            rows.push(head);
        }
        let batch = if rows.is_empty() {
            None
        } else {
            let (batch, shared) = state.projection.batch(&state.postings.scan.tree, &rows)?;
            if shared {
                self.sliced_rows += rows.len() as u64;
            } else {
                self.gathered_rows += rows.len() as u64;
            }
            Some(batch)
        };
        state.remaining -= rows.len();
        let done = rows.len() < capacity || state.remaining == 0;
        drop(rows);
        if done {
            self.state = None;
        }
        Ok(batch)
    }

    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        span.attribute(
            "scan_ordered_sliced_rows",
            rtdb_plan::TraceValue::Unsigned(self.sliced_rows),
        );
        span.attribute(
            "scan_ordered_gathered_rows",
            rtdb_plan::TraceValue::Unsigned(self.gathered_rows),
        );
    }
}

async fn next(
    cursor: &mut Cursor,
    tree: &Tree,
    range: &Range<impl BoundAtom>,
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
    if !matches!(hit.key(), KeyRef::Posting(..)) {
        return Err(super::format_error(
            "Ordered index seek escaped its posting prefix",
        ));
    }
    Ok(Some(Head {
        hit,
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
        scan_fields: &[Field],
        definition: &Definition,
        projection: &[usize],
    ) -> Result<Option<Self>> {
        let fields = projection
            .iter()
            .map(|&position| scan_fields.get(position))
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
                    | S::Timestamp {
                        unit: TimeUnit::Microsecond,
                        timezone: Some(_)
                    }
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

    fn batch(&self, tree: &Tree, rows: &[Head]) -> Result<(OwnedBatch, bool)> {
        let first = &rows[0].hit;
        let contiguous = tree.shares_result_buffers()
            && rows
                .iter()
                .enumerate()
                .all(|(offset, row)| row.hit.follows(first, offset));
        let mut shared = false;
        let mut builder =
            BatchBuilder::new(4096 + self.columns.len().saturating_mul(256), &tree.memory)?;
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (slot, scalar) in &self.columns {
            if contiguous
                && let Some((array, start, reservation)) = first.plain_column(*slot, rows.len())
            {
                let range = start..start + rows.len();
                if let Some((array, shares)) = covered_slice(&mut builder, scalar, array.as_ref(), range,
                    tree.retain_result_buffers(reservation))? {
                    arrays.push(array);
                    shared |= shares;
                    continue;
                }
            }
            if let Some(array) = covered_gather(&mut builder, scalar, rows, *slot)? {
                arrays.push(array);
                continue;
            }
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
        builder.finish(batch).map(|batch| (batch, shared))
    }
}

fn can_share_bits(bits: &BooleanBuffer, rows: &RowRange<usize>) -> bool {
    (bits.offset() + rows.start).is_multiple_of(8) && rows.len().is_multiple_of(8)
}

fn slice_bits(bits: &BooleanBuffer, rows: RowRange<usize>, owner: Arc<dyn Send + Sync>) -> BooleanBuffer {
    if !can_share_bits(bits, &rows) {
        // At a partial byte the exported bitmap must contain only the requested rows.
        let mut copy = BooleanBufferBuilder::new(rows.len());
        copy.append_buffer(&bits.slice(rows.start, rows.len()));
        return copy.finish();
    }
    let bits = bits.slice(rows.start, rows.len());
    let buffer = bits.inner().slice_with_length(bits.offset() / 8, rows.len() / 8);
    BooleanBuffer::new(retain_buffer_with_owner(&buffer, owner), 0, rows.len())
}

fn slice_nulls(array: &dyn Array, rows: RowRange<usize>, owner: Arc<dyn Send + Sync>) -> Option<NullBuffer> {
    array.nulls().map(|nulls| NullBuffer::new(slice_bits(nulls.inner(), rows, owner)))
}

fn covered_slice(
    builder: &mut BatchBuilder, scalar: &ScalarType, array: &dyn Array, rows: RowRange<usize>,
    owner: Arc<dyn Send + Sync>,
) -> Result<Option<(ArrayRef, bool)>> {
    use arrow_array::{cast::AsArray, types::*};
    let bound = rows.len() * 64 + 1024;
    let nulls = || slice_nulls(array, rows.clone(), Arc::clone(&owner));
    let shares_nulls = array.nulls().is_some_and(|nulls| can_share_bits(nulls.inner(), &rows));
    macro_rules! integer {
        ($ty:ty, $signed:ty, $output:ty) => {
            if let Some(values) = array.as_any().downcast_ref::<Decimal128Array>() {
                return builder.column(bound, || {
                    let result = values.slice(rows.start, rows.len()).try_unary::<_, $ty, _>(|value| {
                        <$signed>::try_from(value).map(|value| value as $output).map_err(super::format_error)
                    })?;
                    Ok(Arc::new(arrow_array::PrimitiveArray::<$ty>::new(result.values().clone(), nulls())))
                }).map(|array| Some((array, shares_nulls)));
            }
        };
    }
    let result = match scalar {
        ScalarType::Utf8 => array.as_any().downcast_ref::<StringArray>()
            .map(|values| slice_bytes(builder, values, rows.clone(), Arc::clone(&owner))).transpose()?,
        ScalarType::Binary => array.as_any().downcast_ref::<BinaryArray>()
            .map(|values| slice_bytes(builder, values, rows.clone(), Arc::clone(&owner))).transpose()?,
        ScalarType::Bool => array.as_any().downcast_ref::<BooleanArray>().map(|values| builder.column(bound, || {
            Ok(Arc::new(BooleanArray::new(slice_bits(values.values(), rows.clone(), Arc::clone(&owner)), nulls())))
        })).transpose()?,
        ScalarType::Uuid => array.as_any().downcast_ref::<FixedSizeBinaryArray>().filter(|values| values.value_length() == 16)
            .map(|values| builder.column(bound, || {
                let values = values.values().slice_with_length(rows.start * 16, rows.len() * 16);
                let values = retain_buffer_with_owner(&values, Arc::clone(&owner));
                Ok(Arc::new(FixedSizeBinaryArray::try_new(16, values, nulls()).map_err(super::format_error)?))
            })).transpose()?,
        ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(timezone) } =>
            array.as_any().downcast_ref::<Int64Array>().map(|values| builder.column(bound, || {
                let values = values.values().slice(rows.start, rows.len());
                let values = retain_buffer_with_owner(values.inner(), Arc::clone(&owner));
                Ok(Arc::new(TimestampMicrosecondArray::new(values.into(), nulls()).with_timezone(timezone.clone())))
            })).transpose()?,
        ScalarType::DateDay if array.data_type() == &arrow_schema::DataType::Int64 => {
            let values = array.as_primitive::<Int64Type>().slice(rows.start, rows.len());
            return builder.column(bound, || {
                let days = values.try_unary::<_, Date32Type, _>(|value| i32::try_from(value).map_err(super::format_error))?;
                Ok(Arc::new(Date32Array::new(days.values().clone(), nulls())))
            }).map(|array| Some((array, shares_nulls)));
        }
        ScalarType::I8 => { integer!(Int8Type, i8, i8); None }
        ScalarType::I16 => { integer!(Int16Type, i16, i16); None }
        ScalarType::I32 => { integer!(Int32Type, i32, i32); None }
        ScalarType::I64 => { integer!(Int64Type, i64, i64); None }
        ScalarType::U8 => { integer!(UInt8Type, i8, u8); None }
        ScalarType::U16 => { integer!(UInt16Type, i16, u16); None }
        ScalarType::U32 => { integer!(UInt32Type, i32, u32); None }
        ScalarType::U64 => { integer!(UInt64Type, i64, u64); None }
        _ => None,
    };
    let shares_values = !matches!(scalar, ScalarType::Bool)
        || array.as_any().downcast_ref::<BooleanArray>().is_some_and(|values| can_share_bits(values.values(), &rows));
    Ok(result.map(|array| (array, shares_values || shares_nulls)))
}

fn slice_bytes<T: ByteArrayType<Offset = i32>>(
    builder: &mut BatchBuilder,
    array: &GenericByteArray<T>,
    rows: std::ops::Range<usize>,
    owner: Arc<dyn Send + Sync>,
) -> Result<ArrayRef> {
    // Expose only the selected byte window, including at the Arrow C data boundary.
    let offsets = &array.value_offsets()[rows.start..=rows.end];
    let base = offsets[0];
    let length = (offsets[offsets.len() - 1] - base) as usize;
    let bound = length
        .saturating_add(offsets.len().saturating_mul(16))
        .saturating_add(1024);
    builder.column(bound, || {
        let values = retain_buffer_with_owner(
            &array.values().slice_with_length(base as usize, length),
            Arc::clone(&owner),
        );
        let offsets = OffsetBuffer::new(
            offsets
                .iter()
                .map(|offset| offset - base)
                .collect::<Vec<_>>()
                .into(),
        );
        let nulls = slice_nulls(array, rows, owner);
        let array =
            GenericByteArray::<T>::try_new(offsets, values, nulls).map_err(super::format_error)?;
        Ok(Arc::new(array))
    })
}

fn covered_gather(
    batch: &mut BatchBuilder,
    scalar: &ScalarType,
    rows: &[Head],
    slot: usize,
) -> Result<Option<ArrayRef>> {
    use arrow_array::types::*;
    use ScalarType as S;

    let len = rows.len();
    let bound = len * 64 + 1024;
    macro_rules! gather {
        ($source:ty, $kind:ident, $column:expr, $append:expr) => {
            gather_covered_column(
                batch,
                bound,
                covered_sources::<$source>(rows, slot, Kind::$kind),
                $column,
                $append,
            )
        };
    }
    macro_rules! convert {
        ($source:ty, $kind:ident, $target:ty, $convert:expr) => {
            gather!(
                $source, $kind,
                || PrimitiveBuilder::<$target>::with_capacity(len),
                |column, value| {
                    let value = value.map($convert).transpose().map_err(super::format_error)?;
                    column.append_option(value);
                    Ok(())
                }
            )
        };
    }
    macro_rules! unsigned_bits {
        ($target:ty, $signed:ty, $unsigned:ty) => {
            convert!(Decimal128Array, Integer, $target, |value| {
                <$signed>::try_from(value).map(|signed| signed as $unsigned)
            })
        };
    }

    match scalar {
        S::Utf8 => gather_covered_bytes::<Utf8Type>(batch, rows, slot, Kind::Text),
        S::Binary => gather_covered_bytes::<BinaryType>(batch, rows, slot, Kind::Binary),
        S::Bool => gather!(
            BooleanArray, Bool,
            || BooleanBuilder::with_capacity(len),
            |column, value| {
                column.append_option(value);
                Ok(())
            }
        ),
        S::Uuid => gather!(
            FixedSizeBinaryArray, Uuid,
            || FixedSizeBinaryBuilder::with_capacity(len, 16),
            |column, value| match value {
                Some(value) => column.append_value(value).map_err(super::format_error),
                None => {
                    column.append_null();
                    Ok(())
                }
            }
        ),
        S::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(timezone) } => gather!(
            Int64Array, InstantMicros,
            || PrimitiveBuilder::<TimestampMicrosecondType>::with_capacity(len).with_timezone(timezone.clone()),
            |column, value| {
                column.append_option(value);
                Ok(())
            }
        ),
        S::DateDay => convert!(Int64Array, Day, Date32Type, i32::try_from),
        S::I8 => convert!(Decimal128Array, Integer, Int8Type, i8::try_from),
        S::I16 => convert!(Decimal128Array, Integer, Int16Type, i16::try_from),
        S::I32 => convert!(Decimal128Array, Integer, Int32Type, i32::try_from),
        S::I64 => convert!(Decimal128Array, Integer, Int64Type, i64::try_from),
        S::U8 => unsigned_bits!(UInt8Type, i8, u8),
        S::U16 => unsigned_bits!(UInt16Type, i16, u16),
        S::U32 => unsigned_bits!(UInt32Type, i32, u32),
        S::U64 => unsigned_bits!(UInt64Type, i64, u64),
        _ => Ok(None),
    }
}

fn covered_sources<'a, A: 'static>(
    rows: &'a [Head],
    slot: usize,
    kind: Kind,
) -> impl Iterator<Item = Option<(&'a A, &'a [Head])>> + 'a {
    rows.chunk_by(|left, right| left.hit.same_leaf(&right.hit))
        .map(move |run| {
            let column = run[0].tuple().plain_column(slot, kind)?;
            Some((column.as_any().downcast_ref::<A>()?, run))
        })
}

fn gather_covered_column<'a, A: 'static, B: ArrayBuilder>(
    batch: &mut BatchBuilder,
    bound: usize,
    sources: impl Iterator<Item = Option<(&'a A, &'a [Head])>>,
    make_column: impl FnOnce() -> B,
    mut append: impl FnMut(&mut B, Option<<&'a A as ArrayAccessor>::Item>) -> Result<()>,
) -> Result<Option<ArrayRef>>
where
    &'a A: ArrayAccessor,
{
    batch.try_column(bound, || {
        let mut column = make_column();
        for source in sources {
            let Some((source, run)) = source else {
                return Ok(None);
            };
            for head in run {
                let row = head.hit.row_in_leaf();
                append(&mut column, source.is_valid(row).then(|| source.value(row)))?;
            }
        }
        Ok(Some(column.finish()))
    })
}

fn gather_covered_bytes<T: ByteArrayType<Offset = i32>>(
    batch: &mut BatchBuilder,
    rows: &[Head],
    slot: usize,
    kind: Kind,
) -> Result<Option<ArrayRef>> {
    let sources = || covered_sources::<GenericByteArray<T>>(rows, slot, kind);
    let Some(payload) = sources().try_fold(0usize, |bytes, source| {
        let (source, run) = source?;
        let offsets = source.value_offsets();
        Some(run.iter()
            .map(|head| head.hit.row_in_leaf())
            .filter(|&row| source.is_valid(row))
            .fold(bytes, |bytes, row| bytes.saturating_add((offsets[row + 1] - offsets[row]) as usize)))
    }) else {
        return Ok(None);
    };
    if payload > i32::MAX as usize {
        return Err(super::format_error("Covered column exceeds Arrow offsets"));
    }
    gather_covered_column(
        batch,
        payload.saturating_add(rows.len() * 64 + 1024),
        sources(),
        || GenericByteBuilder::<T>::with_capacity(rows.len(), payload),
        |column, value| {
            column.append_option(value);
            Ok(())
        },
    )
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
    macro_rules! unsigned_bits {
        ($array:ty, $signed:ty, $unsigned:ty) => {
            Arc::new(
                cells!(Integer, |value| <$signed>::try_from(value)
                    .map(|value| value as $unsigned)
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
        S::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some(timezone),
        } => Arc::new(
            cells!(InstantMicros, |value| value)
                .collect::<Result<TimestampMicrosecondArray>>()?
                .with_timezone(timezone.clone()),
        ),
        S::I8 => integer!(Int8Array, i8),
        S::I16 => integer!(Int16Array, i16),
        S::I32 => integer!(Int32Array, i32),
        S::I64 => integer!(Int64Array, i64),
        S::U8 => unsigned_bits!(UInt8Array, i8, u8),
        S::U16 => unsigned_bits!(UInt16Array, i16, u16),
        S::U32 => unsigned_bits!(UInt32Array, i32, u32),
        S::U64 => unsigned_bits!(UInt64Array, i64, u64),
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

#[derive(Clone)]
struct Head {
    hit: Hit,
    slot: usize,
    reverse: bool,
}

impl Head {
    fn address(&self) -> super::tree::AddressRef<'_> {
        let KeyRef::Posting(_, _, address) = self.hit.key() else {
            unreachable!("head is a posting")
        };
        address
    }

    fn require_identity(&self, slot: usize) -> Result<()> {
        use rtdb_columnar::EntityIdRef;
        let key = self.address().key;
        let matches = match (EntityIdRef::from_handle_bytes(key)?, self.tuple().cell(slot)) {
            (EntityIdRef::String(id), AtomRef::Text(value)) => id == value,
            (EntityIdRef::Binary(id), AtomRef::Binary(value)) => id == value,
            (EntityIdRef::Integer(id), AtomRef::Integer(value)) => i128::from(id) == value,
            (EntityIdRef::Uuid(id), AtomRef::Uuid(value)) => id == value,
            _ => false,
        };
        if !matches { return Err(super::uncovered_owned_range()); }
        Ok(())
    }

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
            .then_with(|| self.address().compare(other.address()))
            .then_with(|| self.slot.cmp(&other.slot));
        if self.reverse { order } else { order.reverse() }
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Date64Array, Float64Array, StructArray};
    use rtdb_columnar::MemoryBudget;
    use rtdb_types::Value;

    #[tokio::test]
    async fn a_seek_reuses_bound_atoms_and_caches_ineligible_literals() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

        struct Evaluator(AtomicUsize);
        #[async_trait]
        impl ConstantEvaluator for Evaluator {
            async fn constant(&self, expression: &Expr) -> Result<Option<rtdb_types::Value>> {
                self.0.fetch_add(1, AtomicOrdering::Relaxed);
                let Expr::Literal(value) = expression else { unreachable!() };
                Ok(Some(value.clone()))
            }
        }
        let evaluator = Evaluator(AtomicUsize::new(0));
        let constants = Constants { evaluator: &evaluator, atoms: Default::default() };
        let negative = Expr::Literal(rtdb_types::Value::F64(-0.0));
        let positive = Expr::Literal(rtdb_types::Value::F64(1.0));
        let first = constants.atom(&negative).await.unwrap();
        assert_eq!(first, None);
        assert_eq!(first, constants.atom(&negative).await.unwrap());
        assert_ne!(first, constants.atom(&positive).await.unwrap());
        assert_eq!(evaluator.0.load(AtomicOrdering::Relaxed), 2);
    }

    #[test]
    fn covered_instant_reuses_only_selected_values_and_validity() {
        let memory = MemoryBudget::new(1 << 20);
        let reservation = memory.reserve(32 << 10).unwrap();
        let source = Int64Array::from((0..24).map(|row| (row != 9).then_some(-10 * row)).collect::<Vec<_>>());
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();
        let (slice, shared) = covered_slice(&mut builder,
            &ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
            &source, 8..16, reservation.clone()).unwrap().unwrap();
        let values = slice.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert!(shared);
        assert_eq!(values.values().as_ptr(), source.values().as_ptr().wrapping_add(8));
        assert_eq!(values.values().len(), 8);
        assert_eq!(values.value(0), -80);
        assert!(values.is_null(1));
        assert_eq!(values.nulls().unwrap().inner().inner().as_ptr(), source.nulls().unwrap().inner().inner().as_ptr().wrapping_add(1));
        let bits = BooleanBuffer::new_set(8);
        let partial = slice_bits(&bits, 2..5, Arc::new(()));
        assert_eq!(partial.values(), &[0b0000_0111]);
        drop((source, reservation, builder));
        assert_eq!(memory.used_bytes(), 32 << 10);
        drop(slice);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn vacant_range_covers_future_matching_activation_and_expiry() -> Result<()> {
        use rtdb_storage::{MemoryStore, ObjectKey, cache::BufferPool};

        use super::super::{postings::KeyRow, tree::{Address, Item, Key}};

        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let memory = MemoryBudget::new(16 << 20);
        let tree = Tree::new(&storage, &ObjectKey::new("range-clock-test")?, &memory, 0).await?;
        let mut edits = Vec::new();
        for (key, from, to) in [(1, 0, 16), (2, 16, 24), (3, 8, 16)] {
            for bucket in Bucket::cover(rtdb_types::Interval::new(Timestamp(from), Timestamp(to))?) {
                edits.push((Key::Posting {
                    bucket,
                    tuple: KeyRow::Fence(Arc::from([Atom::Integer(key)])),
                    address: Address::new(Iid([key as u8; 16]), bytes::Bytes::new()),
                }, Some(Item::Posting)));
            }
        }
        edits.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let root = tree.edit(None, edits).await?;
        let range = Range {
            prefix: vec![Atom::Integer(2)],
            ..Range::default()
        };
        let cancellation = CancellationToken::new();
        for (reading, expected) in [(0, 16), (16, 24), (24, 32)] {
            assert_eq!(range_clock_cutoff(
                &tree, &root, &range, Timestamp(reading), Timestamp(32), &cancellation,
            ).await?, Timestamp(expected));
        }
        Ok(())
    }

    fn path_field(name: &str, data_type: LogicalType) -> Field {
        Field {
            id: ColumnId(0), name: name.into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type,
        }
    }

    #[test]
    fn sliced_arrow_selectors_keep_full_key_types_and_descending_strictness() {
        let array: ArrayRef = Arc::new(StructArray::try_from(vec![
            ("_id", Arc::new(StringArray::from(vec!["outside", "target"])) as ArrayRef),
            ("day", Arc::new(Date64Array::from(vec![0, -1])) as ArrayRef),
        ]).unwrap());
        let array = array.slice(1, 1);
        let field = Arc::new(arrow_schema::Field::new("bound", array.data_type().clone(), true));
        let bound = crate::OwnedKeyBound::Arrow { field: &field, array: &array, row: 0 }.retained();
        let fields = [
            path_field("day", LogicalType::Scalar(ScalarType::DateMilli)),
            path_field("_id", LogicalType::Scalar(ScalarType::Utf8)),
        ];
        let request = crate::OwnedRangeRequest {
            lower: Some(bound.borrowed()), upper: None, after: Some(bound.borrowed()),
            descending: true, limit: 13,
        };
        let (range, reverse, limit) = RowBounds::Compound(request).bind(&fields, &MemoryBudget::new(4096)).unwrap();
        assert!(reverse);
        assert_eq!(limit, 13);
        assert_eq!(range.cursors.len(), 2);
        assert!(range.cursors[0].lower && !range.cursors[0].strict);
        assert!(!range.cursors[1].lower && range.cursors[1].strict);
        let identity = bound.borrowed().arrow_field("_id").unwrap().utf8().unwrap();
        for bound in range.cursors {
            assert_eq!(bound.key, [AtomRef::Day(-1), AtomRef::Text("target")]);
            let AtomRef::Text(text) = bound.key[1] else { unreachable!() };
            assert_eq!(text.as_ptr(), identity.as_ptr());
        }
        let mismatched = [path_field("day", LogicalType::Scalar(ScalarType::DateDay)), fields[1].clone()];
        let mut range = Range::default();
        let failure = compound_bound(&mut range, &mismatched, Some(bound.borrowed()), true, false, &MemoryBudget::new(4096)).unwrap_err();
        assert_eq!(failure.code, "rtdb.owned/range-bound");
    }

    #[test]
    fn arrow_compound_float_bounds_refuse_both_zero_signs_and_nan() {
        let array: ArrayRef = Arc::new(StructArray::try_from(vec![
            ("rating", Arc::new(Float64Array::from(vec![0.0, -0.0, f64::NAN, 1.0])) as ArrayRef),
        ]).unwrap());
        let field = Arc::new(arrow_schema::Field::new("bound", array.data_type().clone(), false));
        let fields = [path_field("rating", LogicalType::Scalar(ScalarType::F64))];
        for row in 0..4 {
            let selector = crate::OwnedKeyBound::Arrow { field: &field, array: &array, row };
            let mut range = Range::default();
            let result = compound_bound(&mut range, &fields, Some(selector), true, false, &MemoryBudget::new(4096));
            assert_eq!(result.is_ok(), row == 3);
        }
    }

    #[test]
    fn unsigned_postings_match_sql_order_and_preserve_covered_bits() {
        macro_rules! check_unsigned {
            ($scalar:ident, $value:ident, $array:ty, $raw:ty) => {{
                let scalar = ScalarType::$scalar;
                let field = to_field("key", &LogicalType::Scalar(scalar.clone())).unwrap();
                let sign_bit = <$raw>::MAX / 2 + 1;
                let input = [sign_bit, <$raw>::MAX, 0, sign_bit - 1];
                let array = <$array>::from(input.to_vec());
                let keys = (0..input.len())
                    .map(|row| AtomRef::from_arrow(&field, &array, row).unwrap())
                    .collect::<Vec<_>>();

                for (&raw, key) in input.iter().zip(&keys) {
                    assert_eq!(Some(key.owned()), Atom::of(&Value::$value(raw)));
                }
                for (row, pair) in keys.windows(2).enumerate() {
                    let sql_order = rtdb_expr::ordered_compare_in_timezone(
                        &Value::$value(input[row]),
                        &Value::$value(input[row + 1]),
                        false,
                        "UTC",
                    )
                    .unwrap();
                    assert_eq!(pair[0].cmp(&pair[1]), sql_order);
                    assert!(sql_order.is_lt());
                }

                let covered = covered_array(&scalar, 0, keys.into_iter()).unwrap();
                let covered = covered.as_any().downcast_ref::<$array>().unwrap();
                assert_eq!(&covered.values()[..], &input);
            }};
        }
        check_unsigned!(U8, U8, UInt8Array, u8);
        check_unsigned!(U16, U16, UInt16Array, u16);
        check_unsigned!(U32, U32, UInt32Array, u32);
        check_unsigned!(U64, U64, UInt64Array, u64);
    }

    #[test]
    fn contiguous_values_share_only_the_selected_byte_window() {
        let memory = MemoryBudget::new(1 << 20);
        let reservation = memory.reserve(64 * 1024).unwrap();
        let permit = Arc::new(());
        let live = Arc::downgrade(&permit);
        let custody = Arc::new((reservation.clone(), permit));
        let source = StringArray::from(vec![
            Some("before"),
            Some("alpha"),
            None,
            Some("omega"),
            Some("after"),
        ]);
        let start = source.values().as_ptr().wrapping_add("before".len());
        let mut builder = BatchBuilder::new(4096, &memory).unwrap();
        let result = slice_bytes(&mut builder, &source, 1..4, custody).unwrap();
        let result_values = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_values.values().as_slice(), b"alphaomega");
        assert_eq!(result_values.values().as_ptr(), start);
        assert_eq!(result_values.value_offsets(), &[0, 5, 5, 10]);
        assert!(result_values.is_null(1));

        drop(source);
        drop(reservation);
        drop(builder);
        assert_eq!(memory.used_bytes(), 64 * 1024);
        assert!(live.upgrade().is_some());
        drop(result);
        assert_eq!(memory.used_bytes(), 0);
        assert!(live.upgrade().is_none());
    }
}
