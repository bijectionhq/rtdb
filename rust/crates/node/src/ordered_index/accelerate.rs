use std::{
    cmp::Ordering,
    collections::{BTreeSet, BinaryHeap},
};

use async_trait::async_trait;
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field};
use rtdb_query::{ScanAccelerator, ScanRequest};
use rtdb_types::{AnomalyCategory, Iid, LogicalType, Result, ScalarType, Value};
use std::sync::Arc;

use super::{
    Definition, definitions,
    key::{Atom, Bucket, Family, family},
    maintenance::{cancelled, read_root},
    tree::{Cursor, Key, Tree},
};

const VERIFY_ROWS: usize = 128;
const MAX_CANDIDATES: usize = 16_384;

pub(crate) struct OrderedIndexAccelerator;

#[async_trait]
impl ScanAccelerator for OrderedIndexAccelerator {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        Ok(exact(request, None)
            .await?
            .map(|ids| ids.into_iter().collect()))
    }

    async fn ordered_iids(
        &self,
        request: &ScanRequest<'_>,
        limit: usize,
    ) -> Result<Option<Vec<Iid>>> {
        if limit > MAX_CANDIDATES {
            return Ok(None);
        }
        exact(request, Some(limit)).await
    }
}

async fn exact(request: &ScanRequest<'_>, limit: Option<usize>) -> Result<Option<Vec<Iid>>> {
    let table = &request.scan.table.name.table;
    if table.schema == super::CATALOG.0
        || request.scan.iids.is_some()
        || !request.bounds.system_time.is_point()
        || !request.bounds.valid_time.is_point()
        || (request.scan.predicates.is_empty() && request.scan.ordered_top_k.is_none())
    {
        return Ok(None);
    }
    match seek(request, limit).await {
        Err(error) if error.category == AnomalyCategory::Interrupted => Err(error),
        Err(_) => Ok(None),
        found => found,
    }
}

async fn seek(request: &ScanRequest<'_>, limit: Option<usize>) -> Result<Option<Vec<Iid>>> {
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
            Some(Top { limit, reverse, distinct_slot })
        } else {
            if range.is_open() {
                continue;
            }
            None
        };
        if let Some(found) = use_index(request, definition, &fields, &range, top).await? {
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

    fn position(&self, key: &[Atom]) -> Ordering {
        let prefix = key
            .iter()
            .zip(&self.prefix)
            .map(|(value, bound)| value.cmp(bound))
            .find(|order| *order != Ordering::Equal)
            .unwrap_or(Ordering::Equal);
        if prefix != Ordering::Equal {
            return prefix;
        }
        let Some(next) = key.get(self.prefix.len()) else {
            return Ordering::Equal;
        };
        let suffix = &key[self.prefix.len()..];
        if self
            .lower
            .as_ref()
            .is_some_and(|(lower, strict)| next < lower || (*strict && next == lower))
            || self
                .cursors
                .iter()
                .any(|bound| bound.position(suffix) == Ordering::Less)
        {
            return Ordering::Less;
        }
        if self
            .upper
            .as_ref()
            .is_some_and(|(upper, strict)| next > upper || (*strict && next == upper))
            || self
                .cursors
                .iter()
                .any(|bound| bound.position(suffix) == Ordering::Greater)
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

    fn position(&self, tuple: &[Atom]) -> Ordering {
        let order = tuple
            .iter()
            .zip(&self.key)
            .map(|(value, bound)| value.cmp(bound))
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
        let [sort] = order.as_slice() else { return None };
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

    fn matches(&self, tuple: &[Atom]) -> bool {
        match self {
            Self::All(left, right) => left.matches(tuple) && right.matches(tuple),
            Self::Either(left, right) => left.matches(tuple) || right.matches(tuple),
            Self::Compare { slot, op, value } => {
                let cell = &tuple[*slot];
                if matches!(cell, Atom::Null | Atom::Unordered) || *value == Atom::Null {
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
    range: &Range,
    top: Option<Top>,
) -> Result<Option<Vec<Iid>>> {
    if top.is_some_and(|top| top.limit == 0) {
        return Ok(Some(Vec::new()));
    }
    let directory = definition.directory()?;
    let tree = Tree {
        storage: request.storage,
        directory: &directory,
        memory: request.memory,
    };
    let system_at = request.bounds.system_time.lower();
    let Some(root) = read_root(
        &tree,
        definition,
        request.snapshot,
        system_at,
        request.cancellation,
    )
    .await?
    else {
        return Ok(None);
    };
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
    let reverse = top.is_some_and(|top| top.reverse);
    let buckets: Vec<_> = Bucket::at(request.bounds.valid_time.lower()).collect();
    let mut cursors: Vec<_> = buckets
        .iter()
        .map(|_| Cursor::new(root.clone(), reverse))
        .collect();
    let _scratch = request.memory.reserve(
        MAX_CANDIDATES
            .min(top.map_or(MAX_CANDIDATES, |top| top.limit))
            .saturating_mul(64)
            .saturating_add(16_384),
    )?;
    let mut heap = BinaryHeap::new();
    for (slot, cursor) in cursors.iter_mut().enumerate() {
        if let Some(head) = next(cursor, &tree, range, buckets[slot], slot, reverse).await? {
            heap.push(head);
        }
    }
    let mut selected = Vec::new();
    let mut pending = Vec::new();
    let mut last_distinct = None::<Arc<[Atom]>>;
    while let Some(head) = heap.pop() {
        if request.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        if predicates
            .iter()
            .all(|predicate| predicate.matches(&head.tuple))
        {
            if covered {
                let emit = distinct_slot.is_none_or(|slot| {
                    last_distinct.as_ref().is_none_or(|prior| prior[slot] != head.tuple[slot])
                });
                if emit {
                    selected.push(head.iid);
                    if distinct_slot.is_some() {
                        last_distinct = Some(Arc::clone(&head.tuple));
                    }
                }
            } else {
                pending.push(head.iid);
                if pending.len()
                    >= VERIFY_ROWS.min(top.map_or(VERIFY_ROWS, |top| {
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
        let slot = head.slot;
        if let Some(head) = next(
            &mut cursors[slot],
            &tree,
            range,
            buckets[slot],
            slot,
            reverse,
        )
        .await?
        {
            heap.push(head);
        }
    }
    filter_payload(request, residuals, &mut pending, &mut selected).await?;
    Ok(Some(selected))
}

async fn next(
    cursor: &mut Cursor,
    tree: &Tree<'_>,
    range: &Range,
    bucket: Bucket,
    slot: usize,
    reverse: bool,
) -> Result<Option<Head>> {
    let entry = cursor
        .next(tree, |key| match key {
            Key::Posting {
                bucket: candidate,
                tuple,
                ..
            } => candidate.cmp(&bucket).then_with(|| range.position(tuple)),
            Key::Row(_) | Key::Revision(_) => Ordering::Greater,
        })
        .await?;
    match entry.map(|entry| entry.key) {
        Some(Key::Posting { tuple, iid, .. }) => Ok(Some(Head {
            tuple,
            iid,
            slot,
            reverse,
        })),
        Some(Key::Row(_) | Key::Revision(_)) => Err(super::format_error(
            "Ordered index seek escaped its posting prefix",
        )),
        None => Ok(None),
    }
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

#[derive(PartialEq, Eq)]
struct Head {
    tuple: Arc<[Atom]>,
    iid: Iid,
    slot: usize,
    reverse: bool,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = (&self.tuple, self.iid, self.slot).cmp(&(&other.tuple, other.iid, other.slot));
        if self.reverse { order } else { order.reverse() }
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
