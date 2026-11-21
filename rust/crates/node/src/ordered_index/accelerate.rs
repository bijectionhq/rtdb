use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
};

use async_trait::async_trait;
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field};
use rtdb_query::{ScanAccelerator, ScanRequest};
use rtdb_types::{Iid, Result, Value};

use super::{
    Definition, definitions,
    key::{Atom, family},
    maintenance::{self, Entries},
    run::{self, Entry, RunReader},
};

const VERIFY_ROWS: usize = 128;
const MAX_CANDIDATES: usize = 16_384;
const MAX_TAIL_FILES: usize = 8;

pub(crate) struct OrderedIndexAccelerator;

#[async_trait]
impl ScanAccelerator for OrderedIndexAccelerator {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        let table = &request.scan.table.name.table;
        if table.schema == super::CATALOG.0
            || request.scan.iids.is_some()
            || (request.scan.predicates.is_empty() && request.scan.ordered_top_k.is_none())
        {
            return Ok(None);
        }
        match candidates(request).await {
            Err(error) if error.category == rtdb_types::AnomalyCategory::Interrupted => Err(error),
            Err(_) => Ok(None),
            found => found,
        }
    }
}

async fn candidates(request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
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
        let top = Top::bind(request, &fields, &range).await?;
        if range.is_open() && top.is_none() {
            continue;
        }
        if let Some(found) = use_index(request, definition, &range, top.as_ref()).await? {
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

struct Top {
    limit: usize,
    reverse: bool,
    columns: Vec<ColumnId>,
}

impl Top {
    async fn bind(
        request: &ScanRequest<'_>,
        fields: &[Option<&Field>],
        range: &Range,
    ) -> Result<Option<Self>> {
        let Some(top) = &request.scan.ordered_top_k else {
            return Ok(None);
        };
        if !request.bounds.system_time.is_point() || !request.bounds.valid_time.is_point() {
            return Ok(None);
        }
        let Some(fields) = fields.iter().copied().collect::<Option<Vec<_>>>() else {
            return Ok(None);
        };
        let columns: Vec<_> = fields.iter().map(|field| field.id).collect();
        let order: Vec<_> = top
            .order
            .iter()
            .filter_map(|key| {
                let Expr::Column { id, .. } = &key.expr else {
                    return None;
                };
                Some((*id, key))
            })
            .collect();
        if order.len() != top.order.len() {
            return Ok(None);
        }
        let suffix: Vec<_> = order
            .into_iter()
            .filter(|(id, _)| !columns[..range.prefix.len()].contains(id))
            .collect();
        if suffix.len() != columns.len() - range.prefix.len() || suffix.is_empty() {
            return Ok(None);
        }
        let reverse = suffix[0].1.descending;
        for ((id, sort), field) in suffix.iter().zip(&fields[range.prefix.len()..]) {
            if *id != field.id
                || family(&field.data_type).is_none()
                || sort.descending != reverse
                || (sort.nulls_first != reverse
                    && field.data_type.nullable()
                    && field.name != "_id")
            {
                return Ok(None);
            }
        }
        let limit = match request.constants.constant(&top.limit).await? {
            Some(Value::I64(n)) => usize::try_from(n).ok(),
            Some(Value::I32(n)) => usize::try_from(n).ok(),
            Some(Value::U64(n)) => usize::try_from(n).ok(),
            _ => None,
        };
        Ok(limit
            .filter(|limit| *limit <= MAX_CANDIDATES)
            .map(|limit| Self {
                limit,
                reverse,
                columns,
            }))
    }
}

async fn use_index(
    request: &ScanRequest<'_>,
    definition: &Definition,
    range: &Range,
    top: Option<&Top>,
) -> Result<Option<BTreeSet<Iid>>> {
    let directory = definition.directory()?;
    let store = request.storage.object_store().as_ref();
    let through = maintenance::source_frontier(request.snapshot, &definition.table);
    let manifest = run::latest(store, &directory, through, request.memory).await?;
    if manifest.through < 0 {
        return Ok(None);
    }
    let files = maintenance::source_files(
        request.snapshot,
        &definition.table,
        store,
        manifest.through,
        through,
    )
    .await?;
    if files.len() > MAX_TAIL_FILES {
        return Ok(None);
    }

    let mut tail = Entries::new();
    for (_, file) in files {
        let mut entries = Entries::from_file(
            store,
            &file,
            &definition.columns,
            request.memory,
            request.cancellation,
        )
        .await?;
        entries
            .rows
            .retain(|entry| range.position(&entry.key) == Ordering::Equal);
        tail.merge(entries);
    }
    for segment in request.snapshot.iter_segments(&definition.table) {
        for batch in segment.batches() {
            tail.add_batch(batch.view(), &definition.columns, request.memory, |key| {
                range.position(key) == Ordering::Equal
            })?;
        }
    }
    tail.rows.sort_unstable();
    tail.rows.dedup();
    let reverse = top.is_some_and(|top| top.reverse);
    if reverse {
        tail.rows.reverse();
    }
    let mut live: VecDeque<_> = std::mem::take(&mut tail.rows).into();

    let mut readers: Vec<_> = manifest
        .runs
        .iter()
        .map(|run| {
            let start = run
                .pages
                .partition_point(|page| range.position(&page.last) == Ordering::Less);
            let end = run
                .pages
                .partition_point(|page| range.position(&page.first) != Ordering::Greater);
            RunReader::new(run, start.min(end), end, reverse)
        })
        .collect();
    let live_slot = readers.len();
    let mut heap = BinaryHeap::new();
    for (slot, reader) in readers.iter_mut().enumerate() {
        if let Some(entry) = next(reader, request, &directory, range).await? {
            heap.push(Head {
                entry,
                slot,
                reverse,
            });
        }
    }
    if let Some(entry) = live.pop_front() {
        heap.push(Head {
            entry,
            slot: live_slot,
            reverse,
        });
    }

    let memory = request.memory;
    let capacity = top.map_or(MAX_CANDIDATES, |top| top.limit.saturating_add(VERIFY_ROWS));
    let _candidate_charge = memory.reserve(capacity * 96)?;
    let mut selected = BTreeSet::new();
    let mut pending = Vec::new();
    while let Some(Head { entry, slot, .. }) = heap.pop() {
        if request.cancellation.is_cancelled() {
            return Err(maintenance::cancelled());
        }
        let following = if slot == live_slot {
            live.pop_front()
        } else {
            next(&mut readers[slot], request, &directory, range).await?
        };
        if let Some(entry) = following {
            heap.push(Head {
                entry,
                slot,
                reverse,
            });
        }
        if selected.contains(&entry.iid) {
            continue;
        }
        if let Some(top) = top {
            if pending.last() != Some(&entry) {
                pending.push(entry);
            }
            if pending.len() >= VERIFY_ROWS.min(top.limit.saturating_sub(selected.len()).max(32)) {
                verify(request, top, &mut pending, &mut selected).await?;
                if selected.len() >= top.limit {
                    break;
                }
            }
        } else {
            selected.insert(entry.iid);
            if selected.len() > MAX_CANDIDATES {
                return Ok(None);
            }
        }
    }
    if let Some(top) = top {
        verify(request, top, &mut pending, &mut selected).await?;
    }
    Ok(Some(selected))
}

async fn next(
    reader: &mut RunReader<'_>,
    request: &ScanRequest<'_>,
    directory: &rtdb_storage::ObjectKey,
    range: &Range,
) -> Result<Option<Entry>> {
    while let Some(entry) = reader
        .next(
            request.storage.object_store().as_ref(),
            directory,
            request.memory,
        )
        .await?
    {
        if range.position(&entry.key) == Ordering::Equal {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}

async fn verify(
    request: &ScanRequest<'_>,
    top: &Top,
    pending: &mut Vec<Entry>,
    selected: &mut BTreeSet<Iid>,
) -> Result<()> {
    if pending.is_empty() || selected.len() >= top.limit {
        return Ok(());
    }
    let ids = pending.iter().map(|entry| entry.iid).collect();
    let residuals = request
        .scan
        .ordered_top_k
        .as_ref()
        .map_or(&[][..], |plan| plan.residuals.as_slice());
    let visible = request
        .verifier
        .visible_columns(ids, &top.columns, residuals)
        .await?;
    let keys: BTreeMap<_, _> = visible
        .iter()
        .filter_map(|(iid, row)| {
            Some((*iid, row.iter().map(Atom::of).collect::<Option<Vec<_>>>()?))
        })
        .collect();
    for entry in pending.drain(..) {
        if keys.get(&entry.iid) == Some(&entry.key) {
            selected.insert(entry.iid);
            if selected.len() == top.limit {
                break;
            }
        }
    }
    Ok(())
}

#[derive(PartialEq, Eq)]
struct Head {
    entry: Entry,
    slot: usize,
    reverse: bool,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        let order = self
            .entry
            .cmp(&other.entry)
            .then_with(|| self.slot.cmp(&other.slot));
        if self.reverse { order } else { order.reverse() }
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
