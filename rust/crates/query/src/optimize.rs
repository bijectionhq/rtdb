use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_plan::{BinaryOp, ColumnId, Expr, JoinBuildReason, JoinKind, JoinPlanEvidence, NamedExpr, QualifiedTable, Relation, RelationNode, Schema, WindowFunction};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::CatalogSnapshot;
use rtdb_columnar::MemoryReservation;

mod decorrelate;
#[cfg(test)]
mod decorrelate_tests;

pub(crate) fn prepare(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    let mut ids = Ids::after(&relation)?;
    let relation = if decorrelates() { decorrelate::decorrelate(relation, &mut ids)? } else { relation };
    prepare_with(relation, &mut ids)
}

/// As `prepare`, then ranking inner join inputs using the plan and captured table counts.
pub(crate) fn prepare_with_statistics(relation: Arc<Relation>, statistics: &mut dyn TableStatistics) -> Result<Arc<Relation>> {
    order_joins(prepare(relation)?, statistics)
}

#[cfg(test)]
thread_local! {
    /// Whether `prepare` rewrites applies into joins, so that a test can run one plan both ways.
    static DECORRELATES: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
}

#[cfg(test)]
fn decorrelates() -> bool { DECORRELATES.with(std::cell::Cell::get) }

#[cfg(not(test))]
fn decorrelates() -> bool { true }

/// The row counts of the tables a plan scans, which order its joins.
pub(crate) trait TableStatistics {
    /// The rows `table` holds, or `None` where nothing is known of it.
    fn row_count(&mut self, table: &QualifiedTable) -> Result<Option<u64>>;
}

#[derive(Default)]
pub(crate) struct JoinStatistics(BTreeMap<QualifiedTable, Option<u64>>);

impl JoinStatistics {
    pub(crate) fn into_recorded(self) -> Option<Arc<Self>> {
        (!self.0.is_empty()).then(|| Arc::new(self))
    }

    pub(crate) fn matches(&self, catalog: &CatalogSnapshot) -> bool {
        self.0.iter().all(|(table, count)| table_rows(catalog, table) == *count)
    }
}

pub(crate) struct RecordStatistics<'a> {
    pub(crate) catalog: &'a CatalogSnapshot,
    pub(crate) counts: &'a mut JoinStatistics,
    pub(crate) reservation: &'a mut MemoryReservation,
}

impl TableStatistics for RecordStatistics<'_> {
    fn row_count(&mut self, table: &QualifiedTable) -> Result<Option<u64>> {
        if let Some(count) = self.counts.0.get(table) {
            return Ok(*count);
        }
        let bytes = self.reservation.bytes().checked_add(1024)
            .and_then(|bytes| bytes.checked_add(table.database.len()))
            .and_then(|bytes| bytes.checked_add(table.table.schema.len()))
            .and_then(|bytes| bytes.checked_add(table.table.table.len()))
            .ok_or_else(|| crate::invalid("plan-size", "Join statistics size exceeds addressable memory"))?;
        self.reservation.grow_to(bytes)?;
        let count = table_rows(self.catalog, table);
        self.counts.0.insert(table.clone(), count);
        Ok(count)
    }
}

/// Each partition's flushed rows, from its table catalog, and its live rows; a partition that holds none of the table's
/// rows counts as none, and only a table no partition knows is unknown.
pub(crate) fn table_rows(catalog: &CatalogSnapshot, table: &QualifiedTable) -> Option<u64> {
    let partitions = catalog.database(&table.database).ok()?.partitions.iter().map(|partition| {
        let flushed = partition.table_catalog().metadata(&table.table).map(|meta| meta.row_count);
        let live = partition.iter_segments(&table.table).map(|segment| i64::from(segment.row_count())).sum::<i64>();
        (flushed, live)
    });
    partition_rows(partitions)
}

/// The rows of a table from each partition's flushed rows, if its catalog knows the table, and its live rows.
fn partition_rows(partitions: impl IntoIterator<Item = (Option<i64>, i64)>) -> Option<u64> {
    let mut known = false;
    let mut rows = 0u64;
    for (flushed, live) in partitions {
        known |= flushed.is_some() || live != 0;
        rows = rows.checked_add(u64::try_from(flushed.unwrap_or(0)).ok()?)?.checked_add(u64::try_from(live).ok()?)?;
    }
    known.then_some(rows)
}

#[test]
fn an_empty_partition_adds_no_rows_rather_than_hiding_the_others() {
    assert_eq!(partition_rows([(Some(40), 2), (None, 0)]), Some(42));
    assert_eq!(partition_rows([(None, 0), (None, 5)]), Some(5));
    assert_eq!(partition_rows([(None, 0), (None, 0)]), None, "no partition knows the table");
    assert_eq!(partition_rows([]), None);
}

fn estimated_rows(relation: &Relation, statistics: &mut dyn TableStatistics) -> Result<Option<u64>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || match relation.node() {
        RelationNode::Scan(scan) => {
            let table = statistics.row_count(&scan.table.name)?;
            let rows = scan.predicates.iter().fold(table, |rows, predicate| {
                smaller_estimate(rows, filtered_rows(table, predicate))
            });
            Ok(if scan.iids.as_ref().is_some_and(BTreeSet::is_empty) { Some(0) } else { rows })
        }
        RelationNode::Values { rows } => Ok(Some(rows.len() as u64)),
        RelationNode::LiteralPages { pages } =>
            Ok(Some(pages.iter().fold(0u64, |total, page| total.saturating_add(page.len() as u64)))),
        RelationNode::ArrowLiteralPages { pages } =>
            Ok(Some(pages.pages().iter().fold(0u64, |total, page| total.saturating_add(page.batch().num_rows() as u64)))),
        RelationNode::Project { input, .. } | RelationNode::Extend { input, .. } | RelationNode::Rename { input, .. }
        | RelationNode::Sort { input, .. } | RelationNode::Ordinality { input, .. } | RelationNode::Window { input, .. }
        | RelationNode::Distinct { input } => estimated_rows(input, statistics),
        RelationNode::Filter { input, predicate } => Ok(filtered_rows(estimated_rows(input, statistics)?, predicate)),
        RelationNode::Limit { input, fetch, .. } | RelationNode::SortLimit { input, fetch, .. } =>
            Ok(smaller_estimate(estimated_rows(input, statistics)?, literal_fetch(fetch.as_ref()))),
        RelationNode::Aggregate { groups, .. } if groups.is_empty() => Ok(Some(1)),
        RelationNode::Aggregate { input, .. } => estimated_rows(input, statistics),
        RelationNode::Join { left, right, kind: JoinKind::Inner | JoinKind::Cross, condition } =>
            Ok(joined_rows(estimated_rows(left, statistics)?, estimated_rows(right, statistics)?,
                condition.as_ref().is_some_and(|expr| direct_equijoin(expr, left.schema(), right.schema())))),
        RelationNode::Let { body, .. } => estimated_rows(body, statistics),
        RelationNode::Reference { binding } => estimated_rows(&binding.relation, statistics),
        _ => Ok(None),
    })
}

fn literal_fetch(fetch: Option<&Expr>) -> Option<u64> {
    let Expr::Literal(value) = fetch? else { return None };
    crate::limit_bounds::checked_row_count(rtdb_expr::integer(value)?)
}

fn smaller_estimate(first: Option<u64>, second: Option<u64>) -> Option<u64> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (Some(rows), None) | (None, Some(rows)) => Some(rows),
        (None, None) => None,
    }
}

fn filtered_rows(rows: Option<u64>, predicate: &Expr) -> Option<u64> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || match predicate {
        Expr::Literal(Value::Bool(false) | Value::Null) => Some(0),
        Expr::Binary { op: BinaryOp::And, left, right } =>
            smaller_estimate(filtered_rows(rows, left), filtered_rows(rows, right)),
        Expr::Binary { op, left, right } if column_constant(left, right) || column_constant(right, left) => {
            // Without column distributions, discount a conjunction by its strongest term.
            let divisor = match op {
                BinaryOp::Eq => 4,
                BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => 2,
                _ => 1,
            };
            rows.map(|rows| rows.div_ceil(divisor))
        }
        _ => rows,
    })
}

fn column_constant(column: &Expr, constant: &Expr) -> bool {
    matches!(column, Expr::Column { .. }) && matches!(constant, Expr::Literal(_) | Expr::Parameter { .. })
}

fn direct_equijoin(condition: &Expr, left: &Schema, right: &Schema) -> bool {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || match condition {
        Expr::Binary { op: BinaryOp::And, left: first, right: second } =>
            direct_equijoin(first, left, right) && direct_equijoin(second, left, right),
        _ => direct_join_columns(condition).is_some_and(|(a, b)| {
            (left.get(a).is_some() && right.get(b).is_some()) || (left.get(b).is_some() && right.get(a).is_some())
        }),
    })
}

fn direct_join_columns(condition: &Expr) -> Option<(ColumnId, ColumnId)> {
    let Expr::Binary { op: BinaryOp::Eq, left, right } = condition else { return None };
    let (Expr::Column { id: a, data_type: a_type }, Expr::Column { id: b, data_type: b_type }) =
        (left.as_ref(), right.as_ref()) else { return None };
    (direct_join_key(a_type) && direct_join_key(b_type)).then_some((*a, *b))
}

fn direct_join_key(data_type: &LogicalType) -> bool {
    let ty = match data_type {
        LogicalType::Maybe(inner) => inner.as_ref(),
        ty => ty,
    };
    matches!(ty, LogicalType::Null | LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64
        | ScalarType::F32 | ScalarType::F64 | ScalarType::Bool | ScalarType::Utf8 | ScalarType::Uuid))
}

fn joined_rows(left: Option<u64>, right: Option<u64>, equijoin: bool) -> Option<u64> {
    match (left, right) {
        (Some(0), _) | (_, Some(0)) => Some(0),
        // Use the larger input as the equality-join estimate when distinct counts are unavailable.
        (Some(left), Some(right)) if equijoin => Some(left.max(right)),
        (Some(left), Some(right)) => Some(left.saturating_mul(right)),
        _ => None,
    }
}

/// Reorders each run of inner joins in the plan, dependent plans included, by its inputs' estimated rows.
fn order_joins(relation: Arc<Relation>, statistics: &mut dyn TableStatistics) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || {
        if reorderable_join(&relation) {
            let ordered = order_within_leaves(&relation, statistics)?;
            return reorder_by(ordered, Some(statistics));
        }
        with_children(&relation, |child| order_joins(Arc::clone(child), statistics))
    })
}

fn order_within_leaves(relation: &Arc<Relation>, statistics: &mut dyn TableStatistics) -> Result<Arc<Relation>> {
    if !reorderable_join(relation) {
        return order_joins(Arc::clone(relation), statistics);
    }
    let RelationNode::Join { left, right, kind, condition } = relation.node() else {
        unreachable!("A reorderable join has two inputs")
    };
    let (ordered_left, ordered_right) = (order_within_leaves(left, statistics)?, order_within_leaves(right, statistics)?);
    if Arc::ptr_eq(&ordered_left, left) && Arc::ptr_eq(&ordered_right, right) { return Ok(Arc::clone(relation)); }
    Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Join { left: ordered_left, right: ordered_right, kind: *kind, condition: condition.clone() })?))
}

/// The relation with each child replaced by `f` of it, or the relation itself where no child changed; `f` keeps each
/// child's schema.
/// A `Let` keeps its binding, which the references in its body share.
fn with_children(relation: &Arc<Relation>, mut f: impl FnMut(&Arc<Relation>) -> Result<Arc<Relation>>) -> Result<Arc<Relation>> {
    let mut changed = false;
    let mut map = |child: &Arc<Relation>| -> Result<Arc<Relation>> {
        let mapped = f(child)?;
        changed |= !Arc::ptr_eq(&mapped, child);
        Ok(mapped)
    };
    let node = match relation.node() {
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::List { .. }
        | RelationNode::ColumnTable { .. } | RelationNode::Scan(_) | RelationNode::Reference { .. } => return Ok(Arc::clone(relation)),
        RelationNode::Ordinality { input, field, scope } => RelationNode::Ordinality { input: map(input)?, field: field.clone(), scope: *scope },
        RelationNode::Project { input, expressions } => RelationNode::Project { input: map(input)?, expressions: expressions.clone() },
        RelationNode::Extend { input, expressions } => RelationNode::Extend { input: map(input)?, expressions: expressions.clone() },
        RelationNode::Rename { input, prefix, expressions } => RelationNode::Rename { input: map(input)?, prefix: prefix.clone(), expressions: expressions.clone() },
        RelationNode::Let { binding, body } => RelationNode::Let { binding: Arc::clone(binding), body: map(body)? },
        RelationNode::Filter { input, predicate } => RelationNode::Filter { input: map(input)?, predicate: predicate.clone() },
        RelationNode::Join { left, right, kind, condition } => RelationNode::Join { left: map(left)?, right: map(right)?, kind: *kind, condition: condition.clone() },
        RelationNode::Apply { input, dependent, arguments, kind, output } => RelationNode::Apply {
            input: map(input)?, dependent: map(dependent)?, arguments: arguments.clone(), kind: *kind, output: output.clone(),
        },
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => RelationNode::Mark {
            input: map(input)?, dependent: map(dependent)?, arguments: arguments.clone(), condition: condition.clone(), comparison: comparison.clone(), output: output.clone(),
        },
        RelationNode::Aggregate { input, groups, aggregates } => RelationNode::Aggregate { input: map(input)?, groups: groups.clone(), aggregates: aggregates.clone() },
        RelationNode::Window { input, partition_by, order_by, expressions } => RelationNode::Window {
            input: map(input)?, partition_by: partition_by.clone(), order_by: order_by.clone(), expressions: expressions.clone(),
        },
        RelationNode::Sort { input, order_by } => RelationNode::Sort { input: map(input)?, order_by: order_by.clone() },
        RelationNode::SortLimit { input, order_by, offset, fetch } => RelationNode::SortLimit {
            input: map(input)?, order_by: order_by.clone(), offset: offset.clone(), fetch: fetch.clone(),
        },
        RelationNode::Limit { input, offset, fetch } => RelationNode::Limit { input: map(input)?, offset: offset.clone(), fetch: fetch.clone() },
        RelationNode::Distinct { input } => RelationNode::Distinct { input: map(input)? },
        RelationNode::Set { left, right, operator, all } => RelationNode::Set { left: map(left)?, right: map(right)?, operator: *operator, all: *all },
        RelationNode::Unnest { input, expr, element, ordinality, outer } => RelationNode::Unnest {
            input: map(input)?, expr: expr.clone(), element: element.clone(), ordinality: ordinality.clone(), outer: *outer,
        },
        RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } => RelationNode::PatchGaps {
            input: map(input)?, iid: *iid, valid_from: *valid_from, valid_to: *valid_to, document: *document, from: from.clone(), to: to.clone(),
        },
    };
    if !changed { return Ok(Arc::clone(relation)); }
    Ok(Arc::new(Relation::new(relation.schema().clone(), node)?))
}

fn prepare_with(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    let required = relation.schema().ids();
    annotate_top_k(prune(relation, &required, ids)?, ids)
}

/// Column ids no field or expression of the plan uses, for the columns the optimizer adds.
struct Ids(u32);

impl Ids {
    fn after(relation: &Relation) -> Result<Self> {
        let mut highest = 0;
        let mut pending = vec![relation];
        while let Some(relation) = pending.pop() {
            let ids = relation.schema().fields().iter().map(|field| field.id.0)
                .chain(relation.expressions().into_iter().flat_map(|expression| expression.referenced_columns()).map(|id| id.0));
            highest = ids.fold(highest, u32::max);
            pending.extend(relation.children().into_iter().map(Arc::as_ref));
        }
        highest.checked_add(1).map(Self).ok_or_else(exhausted)
    }
    fn next(&mut self) -> Result<ColumnId> {
        let id = ColumnId(self.0);
        self.0 = self.0.checked_add(1).ok_or_else(exhausted)?;
        Ok(id)
    }
}

fn exhausted() -> rtdb_types::Anomaly { rtdb_types::Anomaly::incorrect("xtdb/query-too-large", "Column identifier space is exhausted") }

/// Rewrites each `match(column, query)` of the scan's predicates whose query is constant into a column the scan evaluates itself (fts.allium: MatchEvaluatedByTheScan).
/// Inspection and system tables are not read by the scan that evaluates such columns, so their MATCHes stay predicates.
/// Returns whether it added any.
fn evaluate_matches(scan: &mut rtdb_plan::Scan, ids: &mut Ids) -> Result<bool> {
    if crate::inspection::is_inspection(&scan.table.name.table) || crate::system_schema::definition(&scan.table.name.table).is_some() { return Ok(false); }
    let mut added = Vec::new();
    let mut failure = None;
    for predicate in &mut scan.predicates {
        predicate.rewrite(&mut |expression| {
            let Expr::Call { name, args, .. } = expression else { return };
            let ([Expr::Column { id, .. }, query, ..], true) = (args.as_slice(), name.eq_ignore_ascii_case("match")) else { return };
            if !matches!(query, Expr::Literal(_) | Expr::Parameter { .. }) { return; }
            let Ok(analyzer) = rtdb_plan::call_analyzer(name, args) else { return };
            let Some(column) = scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.clone()) else { return };
            let id = match ids.next() { Ok(id) => id, Err(error) => { failure = Some(error); return; } };
            let data_type = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe();
            added.push((rtdb_plan::Field { id, name: format!("match({column})"), qualifier: None, qualified_only: true, unnamed: true, data_type: data_type.clone() },
                rtdb_plan::ScanMatch { id, column, query: query.clone(), analyzer }));
            *expression = Expr::Column { id, data_type };
        });
    }
    if let Some(error) = failure { return Err(error); }
    let any = !added.is_empty();
    for (field, evaluated) in added { scan.columns.push(field); scan.matches.push(evaluated); }
    Ok(any)
}

/// A projection of the scan's columns other than the MATCHes it evaluates for its own predicates, so that nothing above sees more fields than before the rewrite.
fn without_matches(scan: Arc<Relation>) -> Result<Arc<Relation>> {
    let RelationNode::Scan(inner) = scan.node() else { return Ok(scan) };
    let kept = inner.columns.iter().filter(|field| !inner.matches.iter().any(|evaluated| evaluated.id == field.id))
        .map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
    Ok(Arc::new(scan.project(kept)?))
}

/// Prunes the scan a predicate was pushed into, through the nodes `select` pushes through, so that a column only a pushed MATCH read goes.
fn prune_pushed(relation: Arc<Relation>, required: &BTreeSet<ColumnId>, ids: &mut Ids) -> Result<Arc<Relation>> {
    let node = match relation.node() {
        RelationNode::Scan(_) => return prune(relation, required, ids),
        RelationNode::Filter { input, predicate } => {
            let mut columns = required.clone(); columns.extend(predicate.referenced_columns());
            return Ok(Arc::new(prune_pushed(Arc::clone(input), &columns, ids)?.filter(predicate.clone())?));
        }
        RelationNode::Extend { input, expressions } => {
            let mut columns = required.clone(); columns.extend(expressions.iter().flat_map(|named| named.expr.referenced_columns()));
            return Ok(Arc::new(prune_pushed(Arc::clone(input), &columns, ids)?.extend(expressions.clone())?));
        }
        RelationNode::Project { input, expressions } => {
            let columns = expressions.iter().flat_map(|named| named.expr.referenced_columns()).collect();
            RelationNode::Project { input: prune_pushed(Arc::clone(input), &columns, ids)?, expressions: expressions.clone() }
        }
        RelationNode::Rename { input, prefix, expressions } => {
            let columns = expressions.iter().flat_map(|named| named.expr.referenced_columns()).collect();
            RelationNode::Rename { input: prune_pushed(Arc::clone(input), &columns, ids)?, prefix: prefix.clone(), expressions: expressions.clone() }
        }
        _ => return Ok(relation),
    };
    Ok(Arc::new(Relation::new(relation.schema().clone(), node)?))
}

fn has_match(predicate: &Expr) -> bool {
    let mut found = false;
    predicate.visit(&mut |expression| { if let Expr::Call { name, .. } = expression { found |= name.eq_ignore_ascii_case("match"); } });
    found
}

/// Marks the scan under `ORDER BY search_score(column, query) DESC ... LIMIT n` with that ranking, when only projections stand between them and the scan has a MATCH on the same column.
/// The scan may then hand on only the rows that can be among the first n, which is what the sort above would keep.
fn annotate_top_k(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    let rebuilt = match relation.node() {
        RelationNode::Project { input, expressions } => RelationNode::Project { input: annotate_top_k(Arc::clone(input), ids)?, expressions: expressions.clone() },
        RelationNode::Rename { input, prefix, expressions } => RelationNode::Rename { input: annotate_top_k(Arc::clone(input), ids)?, prefix: prefix.clone(), expressions: expressions.clone() },
        RelationNode::SortLimit { input, order_by, offset, fetch: Some(fetch) } => {
            let Some(input) = push_top_k(input, order_by, fetch, offset.as_ref(), &[], false)? else { return Ok(relation) };
            if let Some(first) = order_by.first()
                && let Some((input, field)) = add_ranked_score(&input, &first.expr, ids)?
            {
                let mut order_by = order_by.clone();
                order_by[0].expr = field.expression();
                let sorted = Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit {
                    input, order_by, offset: offset.clone(), fetch: Some(fetch.clone()),
                })?);
                let output = relation.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
                return Ok(Arc::new(sorted.project(output)?));
            }
            RelationNode::SortLimit { input, order_by: order_by.clone(), offset: offset.clone(), fetch: Some(fetch.clone()) }
        }
        _ => return Ok(relation),
    };
    Ok(Arc::new(Relation::new(relation.schema().clone(), rebuilt)?))
}

fn push_top_k(relation: &Arc<Relation>, order: &[rtdb_plan::SortExpr], limit: &Expr, offset: Option<&Expr>, residuals: &[Expr], distinct: bool) -> Result<Option<Arc<Relation>>> {
    let through = |expressions: &[NamedExpr]| {
        let defined: BTreeMap<ColumnId, Expr> = expressions.iter().map(|named| (named.field.id, named.expr.clone())).collect();
        let rewrite = |expression: &mut Expr| if let Expr::Column { id, .. } = expression {
            if let Some(definition) = defined.get(id) { *expression = definition.clone(); }
        };
        let mut order = order.to_vec();
        let mut residuals = residuals.to_vec();
        for key in &mut order { key.expr.rewrite(&mut |expression| rewrite(expression)); }
        for predicate in &mut residuals { predicate.rewrite(&mut |expression| rewrite(expression)); }
        (order, residuals)
    };
    let node = match relation.node() {
        RelationNode::Project { input, expressions } => {
            let (order, residuals) = through(expressions);
            let Some(input) = push_top_k(input, &order, limit, offset, &residuals, distinct)? else { return Ok(None) };
            RelationNode::Project { input, expressions: expressions.clone() }
        }
        RelationNode::Rename { input, prefix, expressions } => {
            let (order, residuals) = through(expressions);
            let Some(input) = push_top_k(input, &order, limit, offset, &residuals, distinct)? else { return Ok(None) };
            RelationNode::Rename { input, prefix: prefix.clone(), expressions: expressions.clone() }
        }
        RelationNode::Extend { input, expressions } => {
            let (order, residuals) = through(expressions);
            let Some(input) = push_top_k(input, &order, limit, offset, &residuals, distinct)? else { return Ok(None) };
            return Ok(Some(Arc::new(input.extend(expressions.clone())?)));
        }
        RelationNode::Filter { input, predicate } if ordered_index_filter(predicate) => {
            let mut residuals = residuals.to_vec();
            residuals.push(predicate.clone());
            let Some(input) = push_top_k(input, order, limit, offset, &residuals, distinct)? else { return Ok(None) };
            RelationNode::Filter { input, predicate: predicate.clone() }
        }
        RelationNode::Distinct { input } if !distinct => {
            let fields = relation.schema().fields();
            if fields.len() != order.len() || fields.iter().any(|field| {
                !order.iter().any(|sort| matches!(&sort.expr, Expr::Column { id, .. } if *id == field.id))
            }) {
                return Ok(None);
            }
            let Some(input) = push_top_k(input, order, limit, offset, residuals, true)? else { return Ok(None) };
            RelationNode::Distinct { input }
        }
        RelationNode::Scan(scan) => {
            let mut scan = scan.clone();
            let Some(first) = order.first() else { return Ok(None) };
            if residuals.iter().all(ordered_index_filter)
                && order.iter().all(|key| matches!(&key.expr, Expr::Column { id, .. } if scan.columns.iter().any(|field| field.id == *id))) {
                scan.ordered_top_k = Some(rtdb_plan::OrderedTopK { order: order.to_vec(), residuals: residuals.to_vec(), distinct });
            } else {
                if distinct || !residuals.is_empty() { return Ok(None); }
                let Expr::Call { name, args, .. } = &first.expr else { return Ok(None) };
                let [Expr::Column { id, .. }, query, ..] = args.as_slice() else { return Ok(None) };
                let Ok(analyzer) = rtdb_plan::call_analyzer(name, args) else { return Ok(None) };
                let arity = rtdb_plan::analyzer_index(name, args.len()).unwrap_or(args.len());
                let corpus = match &args[..arity] {
                    [_, _] => None,
                    [_, _, documents, tokens, frequencies] => {
                        let [Some(documents), Some(tokens), Some(frequencies)] =
                            [documents, tokens, frequencies].map(rank_constant)
                        else { return Ok(None); };
                        Some(Box::new([documents, tokens, frequencies]))
                    }
                    _ => return Ok(None),
                };
                if !first.descending || !name.eq_ignore_ascii_case("search_score")
                    || !matches!(query, Expr::Literal(_) | Expr::Parameter { .. })
                    || !scan.columns.iter().any(|field| field.id == *id) { return Ok(None); }
                let name = scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.as_str());
                let evaluated: Vec<ColumnId> = scan.matches.iter()
                    .filter(|evaluated| Some(evaluated.column.as_str()) == name && evaluated.analyzer == analyzer)
                    .map(|evaluated| evaluated.id).collect();
                if !scan.predicates.iter().any(|predicate| has_match_on(predicate, *id, analyzer, &evaluated)) { return Ok(None); }
                let ascending_id = match order {
                    [_, tie, ..] if !tie.descending => match &tie.expr {
                        Expr::Column { id, .. } if scan.columns.iter().any(|field| field.id == *id && field.name == "_id") => Some(*id),
                        _ => None,
                    },
                    _ => None,
                };
                scan.top_k = Some(rtdb_plan::ScanTopK {
                    column: *id, query: query.clone(), corpus, analyzer, limit: limit.clone(), offset: offset.cloned(), score: None, ascending_id,
                });
            }
            return Ok(Some(Arc::new(Relation::scan(scan)?)));
        }
        _ => return Ok(None),
    };
    Ok(Some(Arc::new(Relation::new(relation.schema().clone(), node)?)))
}

fn rank_constant(expression: &Expr) -> Option<Expr> {
    match expression {
        Expr::Parameter { .. } | Expr::Literal(_) => Some(expression.clone()),
        Expr::List(items) => Some(Expr::Literal(rtdb_types::Value::list(items.iter().map(|item| {
            match item { Expr::Literal(value) => Some(value.clone()), _ => None }
        }).collect::<Option<_>>()?))),
        _ => None,
    }
}

fn add_ranked_score(
    relation: &Arc<Relation>, expression: &Expr, ids: &mut Ids,
) -> Result<Option<(Arc<Relation>, rtdb_plan::Field)>> {
    let mut rebuilt = |input: &Arc<Relation>, expressions: &[NamedExpr]| -> Result<Option<_>> {
        let mut pushed = expression.clone();
        pushed.rewrite(&mut |expression| {
            if let Expr::Column { id, .. } = expression
                && let Some(named) = expressions.iter().find(|named| named.field.id == *id)
            {
                *expression = named.expr.clone();
            }
        });
        let Some((input, score)) = add_ranked_score(input, &pushed, ids)? else { return Ok(None); };
        let expressions = expressions.iter().map(|named| NamedExpr {
            field: named.field.clone(),
            expr: if named.expr == pushed { score.expression() } else { named.expr.clone() },
        }).collect::<Vec<_>>();
        Ok(Some((input, score, expressions)))
    };
    let (node, field) = match relation.node() {
        RelationNode::Scan(scan) => {
            let mut scan = scan.clone();
            let Some(top_k) = &mut scan.top_k else { return Ok(None); };
            let Expr::Call { name, args, return_type } = expression else { return Ok(None); };
            if !name.eq_ignore_ascii_case("search_score")
                || !matches!(args.as_slice(), [Expr::Column { id, .. }, query, ..] if *id == top_k.column && query == &top_k.query)
                || !matches!(args.len(), 2 | 3)
                || rtdb_plan::call_analyzer(name, args).ok() != Some(top_k.analyzer)
            {
                return Ok(None);
            }
            let id = ids.next()?;
            let field = rtdb_plan::Field {
                id, name: format!("search_score_{}", id.0), qualifier: None,
                qualified_only: true, unnamed: true, data_type: return_type.clone().maybe(),
            };
            top_k.score = Some(id);
            scan.columns.push(field.clone());
            return Ok(Some((Arc::new(Relation::scan(scan)?), field)));
        }
        RelationNode::Project { input, expressions } => {
            let Some((input, score, mut expressions)) = rebuilt(input, expressions)? else { return Ok(None); };
            expressions.push(NamedExpr { field: score.clone(), expr: score.expression() });
            (RelationNode::Project { input, expressions }, score)
        }
        RelationNode::Rename { input, expressions, prefix } => {
            let Some((input, score, mut expressions)) = rebuilt(input, expressions)? else { return Ok(None); };
            expressions.push(NamedExpr { field: score.clone(), expr: score.expression() });
            (RelationNode::Rename { input, prefix: prefix.clone(), expressions }, score)
        }
        RelationNode::Extend { input, expressions } => {
            let Some((input, score, expressions)) = rebuilt(input, expressions)? else { return Ok(None); };
            return Ok(Some((Arc::new(input.extend(expressions)?), score)));
        }
        _ => return Ok(None),
    };
    let schema = Schema::new(relation.schema().fields().iter().cloned().chain([field.clone()]).collect())?;
    Ok(Some((Arc::new(Relation::new(schema, node)?), field)))
}

fn ordered_index_filter(expression: &Expr) -> bool {
    if let Expr::Unary { op: rtdb_plan::UnaryOp::IsNull | rtdb_plan::UnaryOp::IsNotNull, expr } = expression {
        return matches!(expr.as_ref(), Expr::Column { .. });
    }
    let Expr::Binary { op, left, right } = expression else { return false; };
    if matches!(op, BinaryOp::And | BinaryOp::Or) {
        return ordered_index_filter(left) && ordered_index_filter(right);
    }
    let operand = |value: &Expr| matches!(value, Expr::Column { .. } | Expr::Literal(_) | Expr::Parameter { .. });
    matches!(op, BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) && operand(left) && operand(right)
}

/// Whether a conjunct of the predicate is a MATCH on `column` under `analyzer`, or one of the scan's own columns evaluating such a MATCH.
fn has_match_on(predicate: &Expr, column: ColumnId, analyzer: rtdb_text::Analyzer, evaluated: &[ColumnId]) -> bool {
    match predicate {
        Expr::Binary { op: BinaryOp::And, left, right } => has_match_on(left, column, analyzer, evaluated) || has_match_on(right, column, analyzer, evaluated),
        Expr::Call { name, args, .. } => name.eq_ignore_ascii_case("match") && matches!(args.first(), Some(Expr::Column { id, .. }) if *id == column)
            && rtdb_plan::call_analyzer(name, args).ok() == Some(analyzer),
        Expr::Column { id, .. } => evaluated.contains(id),
        _ => false,
    }
}

fn prune(relation: Arc<Relation>, required: &BTreeSet<ColumnId>, ids: &mut Ids) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || prune_at(relation, required, ids))
}

fn prune_at(relation: Arc<Relation>, required: &BTreeSet<ColumnId>, ids: &mut Ids) -> Result<Arc<Relation>> {
    match relation.node() {
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            let expressions = expressions.iter().filter(|named| required.contains(&named.field.id)).cloned().collect::<Vec<_>>();
            let columns = expressions.iter().flat_map(|named| named.expr.referenced_columns()).collect();
            let input = prune(Arc::clone(input), &columns, ids)?;
            let schema = Schema::new(expressions.iter().map(|named| named.field.clone()).collect())?;
            let node = match relation.node() {
                RelationNode::Rename { prefix, .. } => RelationNode::Rename { input, prefix: prefix.clone(), expressions },
                _ => match superseded(&expressions, &input) {
                    Some((input, expressions)) => RelationNode::Project { input, expressions },
                    None => RelationNode::Project { input, expressions },
                },
            };
            Ok(Arc::new(Relation::new(schema, node)?))
        }
        RelationNode::Extend { input, expressions } => {
            let mut columns = required.clone();
            columns.extend(expressions.iter().flat_map(|named| named.expr.referenced_columns()));
            let input = prune(Arc::clone(input), &columns, ids)?;
            Ok(Arc::new(input.extend(expressions.clone())?))
        }
        RelationNode::Filter { input, predicate } => {
            let mut columns = required.clone(); columns.extend(predicate.referenced_columns());
            let input = prune(Arc::clone(input), &columns, ids)?;
            let selected = reorder_selected(select(input, predicate.clone())?)?;
            if has_match(predicate) { prune_pushed(selected, required, ids) } else { Ok(selected) }
        }
        RelationNode::Scan(scan) => {
            let mut scan = scan.clone();
            let evaluated = evaluate_matches(&mut scan, ids)?;
            let mut columns = required.clone(); for predicate in &scan.predicates { columns.extend(predicate.referenced_columns()); }
            scan.columns.retain(|field| columns.contains(&field.id));
            scan.matches.retain(|evaluated| columns.contains(&evaluated.id));
            let scan = Arc::new(Relation::scan(scan)?);
            if evaluated { without_matches(scan) } else { Ok(scan) }
        }
        // An opened VALUES table evaluates every cell, including fields discarded by a parent projection.
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } => Ok(relation),
        RelationNode::Limit { input, offset, fetch } => {
            let child = prune(Arc::clone(input), required, ids)?;
            bounds(child, offset.clone(), fetch.clone())
        }
        RelationNode::Sort { input, order_by } => {
            let mut columns = required.clone(); for order in order_by { columns.extend(order.expr.referenced_columns()); }
            let input = prune(Arc::clone(input), &columns, ids)?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by: order_by.clone(), offset: None, fetch: None })?))
        }
        RelationNode::SortLimit { input, order_by, offset, fetch } => {
            let mut columns = required.clone(); for order in order_by { columns.extend(order.expr.referenced_columns()); }
            let input = prune(Arc::clone(input), &columns, ids)?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by: order_by.clone(), offset: offset.clone(), fetch: fetch.clone() })?))
        }
        RelationNode::Distinct { input } => {
            let input = prepare_with(Arc::clone(input), ids)?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input })?))
        }
        RelationNode::Let { binding, body } => {
            let body = prune(Arc::clone(body), required, ids)?;
            Ok(Arc::new(Relation::new(body.schema().clone(), RelationNode::Let { binding: Arc::clone(binding), body })?))
        }
        RelationNode::Join { .. } => join_tree(&relation, required, true, ids),
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            let input = prepare_with(Arc::clone(input), ids)?; let dependent = prepare_with(Arc::clone(dependent), ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Mark { input, dependent, arguments: arguments.clone(), condition: condition.clone(), comparison: comparison.clone(), output: output.clone() })?))
        }
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = prepare_with(Arc::clone(input), ids)?; let dependent = prepare_with(Arc::clone(dependent), ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Apply { input, dependent, arguments: arguments.clone(), kind: *kind, output: output.clone() })?))
        }
        RelationNode::Aggregate { input, groups, aggregates } => {
            let mut columns: BTreeSet<ColumnId> = groups.iter().flat_map(|group| group.expr.referenced_columns()).collect();
            for aggregate in aggregates {
                columns.extend(aggregate.argument.iter().chain(&aggregate.filter).flat_map(Expr::referenced_columns));
                columns.extend(aggregate.order_by.iter().flat_map(|order| order.expr.referenced_columns()));
            }
            let input = annotate_top_k(prune(Arc::clone(input), &columns, ids)?, ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Aggregate { input, groups: groups.clone(), aggregates: aggregates.clone() })?))
        }
        RelationNode::Window { input, partition_by, order_by, expressions } => {
            let mut columns = required.clone();
            columns.extend(partition_by);
            columns.extend(order_by.iter().flat_map(|order| order.expr.referenced_columns()));
            columns.extend(expressions.iter().filter_map(|expression| match expression.function {
                WindowFunction::Lead { column, .. } | WindowFunction::Lag { column, .. } => Some(column),
                WindowFunction::RowNumber => None,
            }));
            let input = prune(Arc::clone(input), &columns, ids)?;
            let schema = Schema::new(input.schema().fields().iter().cloned().chain(expressions.iter().map(|expression| expression.field.clone())).collect())?;
            Ok(Arc::new(Relation::new(schema, RelationNode::Window { input, partition_by: partition_by.clone(), order_by: order_by.clone(), expressions: expressions.clone() })?))
        }
        RelationNode::Set { left, right, operator, all } => {
            let left = prepare_with(Arc::clone(left), ids)?; let right = prepare_with(Arc::clone(right), ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Set { left, right, operator: *operator, all: *all })?))
        }
        RelationNode::Ordinality { input, field, scope } => {
            let input = prepare_with(Arc::clone(input), ids)?;
            Ok(Arc::new(input.with_ordinality(field.clone(), *scope)?))
        }
        RelationNode::Unnest { input, expr, element, ordinality, outer } => {
            let mut columns = required.clone();
            columns.extend(expr.referenced_columns());
            let input = prune(Arc::clone(input), &columns, ids)?;
            let schema = Schema::new(input.schema().fields().iter().cloned().chain([element.clone()]).chain(ordinality.iter().cloned()).collect())?;
            Ok(Arc::new(Relation::new(schema, RelationNode::Unnest { input, expr: expr.clone(), element: element.clone(), ordinality: ordinality.clone(), outer: *outer })?))
        }
        RelationNode::PatchGaps { .. } | RelationNode::Reference { .. } | RelationNode::List { .. } | RelationNode::ColumnTable { .. } => Ok(relation),
    }
}

/// Folds a projection that only selects distinct columns of an inner projection into that inner projection.
fn superseded(outer: &[NamedExpr], input: &Arc<Relation>) -> Option<(Arc<Relation>, Vec<NamedExpr>)> {
    let RelationNode::Project { input: inner_input, expressions: inner } = input.node() else { return None; };
    let mut used = BTreeSet::new();
    let expressions = outer.iter().map(|named| {
        let Expr::Column { id, data_type } = &named.expr else { return None; };
        let source = inner.iter().find(|source| source.field.id == *id && source.field.data_type == *data_type && named.field.data_type == *data_type)?;
        used.insert(*id).then(|| NamedExpr { field: named.field.clone(), expr: source.expr.clone() })
    }).collect::<Option<Vec<_>>>()?;
    Some((Arc::clone(inner_input), expressions))
}

/// Selects over a scan's selection by extending its conjunction, as the reference merges selections around a scan.
/// Elsewhere the selections stay stacked, so the lower one drops its rows before the upper one evaluates them.
fn filter(input: Arc<Relation>, predicate: Expr) -> Result<Arc<Relation>> {
    match input.node() {
        RelationNode::Filter { input: child, predicate: existing } if matches!(child.node(), RelationNode::Scan(_)) =>
            Ok(Arc::new(Arc::clone(child).filter(Expr::binary(BinaryOp::And, existing.clone(), predicate))?)),
        _ => Ok(Arc::new(input.filter(predicate)?)),
    }
}

fn select(input: Arc<Relation>, predicate: Expr) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || select_at(input, predicate))
}

fn select_at(input: Arc<Relation>, predicate: Expr) -> Result<Arc<Relation>> {
    if predicate.is_volatile() {
        return Ok(Arc::new(input.filter(predicate)?));
    }
    if matches!(predicate, Expr::Binary { op: BinaryOp::And, .. }) {
        let mut remaining = vec![predicate];
        let mut conjuncts = Vec::new();
        while let Some(expression) = remaining.pop() {
            match expression {
                Expr::Binary { op: BinaryOp::And, left, right } => { remaining.push(*right); remaining.push(*left); },
                atom => conjuncts.push((atom.referenced_columns().len(), atom)),
            }
        }
        conjuncts.sort_by_key(|(width, _)| *width);
        return conjuncts.into_iter().try_fold(input, |relation, (_, atom)| select(relation, atom));
    }
    let columns = predicate.referenced_columns();
    if columns.is_empty() {
        return filter(input, predicate);
    }
    match input.node() {
        RelationNode::Project { input: child, expressions }
        | RelationNode::Rename { input: child, expressions, .. }
        | RelationNode::Extend { input: child, expressions } => {
            let retains_child_fields = matches!(input.node(), RelationNode::Extend { .. });
            let mut replacements = BTreeMap::new();
            for id in &columns {
                match expressions.iter().find(|named| named.field.id == *id) {
                    Some(rtdb_plan::NamedExpr { expr: Expr::Column { id: source, data_type }, .. }) => {
                        replacements.insert(*id, (*source, data_type.clone()));
                    }
                    None if retains_child_fields && child.schema().get(*id).is_some() => {}
                    _ => return filter(input, predicate),
                }
            }
            let child = select(Arc::clone(child), replace(predicate, &replacements))?;
            let node = match input.node() {
                RelationNode::Project { .. } => RelationNode::Project { input: child, expressions: expressions.clone() },
                RelationNode::Extend { .. } => RelationNode::Extend { input: child, expressions: expressions.clone() },
                RelationNode::Rename { prefix, .. } => RelationNode::Rename { input: child, prefix: prefix.clone(), expressions: expressions.clone() },
                _ => unreachable!(),
            };
            Ok(Arc::new(Relation::new(input.schema().clone(), node)?))
        }
        RelationNode::Filter { predicate: existing, .. } if existing.is_volatile() => Ok(Arc::new(input.filter(predicate)?)),
        RelationNode::Filter { input: child, predicate: existing } => filter(select(Arc::clone(child), predicate)?, existing.clone()),
        RelationNode::Join { left, right, kind, condition } => {
            if condition.as_ref().is_some_and(Expr::is_volatile) {
                return filter(input, predicate);
            }
            let within = |side: &Relation| columns.iter().all(|id| side.schema().get(*id).is_some());
            let (in_left, in_right) = (within(left), within(right));
            if in_left && matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Left | JoinKind::Semi | JoinKind::Anti | JoinKind::Single) {
                let left = select(Arc::clone(left), predicate)?;
                return Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::Join { left, right: Arc::clone(right), kind: *kind, condition: condition.clone() })?));
            }
            if in_right && matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Right) {
                let right = select(Arc::clone(right), predicate)?;
                return Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::Join { left: Arc::clone(left), right, kind: *kind, condition: condition.clone() })?));
            }
            let spans = columns.iter().all(|id| left.schema().get(*id).is_some() || right.schema().get(*id).is_some());
            if spans && matches!(kind, JoinKind::Inner | JoinKind::Cross) {
                let condition = Some(match condition { Some(existing) => Expr::binary(BinaryOp::And, existing.clone(), predicate), None => predicate });
                return join(Arc::clone(left), Arc::clone(right), JoinKind::Inner, condition);
            }
            filter(input, predicate)
        }
        RelationNode::Unnest { input: child, expr, element, ordinality, outer }
            if columns.iter().all(|id| child.schema().get(*id).is_some()) => {
            let child = select(Arc::clone(child), predicate)?;
            let node = RelationNode::Unnest {
                input: child, expr: expr.clone(), element: element.clone(), ordinality: ordinality.clone(), outer: *outer,
            };
            Ok(Arc::new(Relation::new(input.schema().clone(), node)?))
        }
        RelationNode::Scan(scan) if columns.len() == 1 => {
            let mut scan = scan.clone();
            if let Some(existing) = scan.predicates.iter_mut().find(|existing| existing.referenced_columns() == columns) {
                *existing = Expr::binary(BinaryOp::And, predicate, existing.clone());
            } else { scan.predicates.push(predicate); }
            scan.predicates.sort_by_key(|predicate| scan.columns.iter().position(|field| predicate.referenced_columns().contains(&field.id)));
            Ok(Arc::new(Relation::scan(scan)?))
        }
        _ => filter(input, predicate),
    }
}

fn reorderable_join(relation: &Relation) -> bool {
    matches!(relation.node(), RelationNode::Join { kind: JoinKind::Inner | JoinKind::Cross, condition, .. }
        if !condition.as_ref().is_some_and(Expr::is_volatile))
}

/// Prunes a join's inputs to the columns its parent and condition read; the outermost of a run of inner and cross joins
/// then reorders that run, as the reference's mega-join does.
fn join_tree(relation: &Arc<Relation>, required: &BTreeSet<ColumnId>, outermost: bool, ids: &mut Ids) -> Result<Arc<Relation>> {
    let RelationNode::Join { left, right, kind, condition } = relation.node() else { return prune(Arc::clone(relation), required, ids) };
    let mut columns = required.clone();
    if let Some(condition) = condition { columns.extend(condition.referenced_columns()); }
    let mut side = |input: &Arc<Relation>| -> Result<Arc<Relation>> {
        let mut needed: BTreeSet<ColumnId> = input.schema().ids().intersection(&columns).copied().collect();
        if needed.is_empty() { needed.extend(input.schema().fields().first().map(|field| field.id)); }
        if reorderable_join(relation) && reorderable_join(input) { join_tree(input, &needed, false, ids) } else { prune(Arc::clone(input), &needed, ids) }
    };
    let joined = join(side(left)?, side(right)?, *kind, condition.clone())?;
    if outermost && reorderable_join(&joined) { reorder(joined) } else { Ok(joined) }
}

/// Reorders the join run a selection was promoted into, beneath at most one column-preserving wrapper.
fn reorder_selected(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    if reorderable_join(&relation) { return reorder(relation); }
    let node = match relation.node() {
        RelationNode::Project { input, expressions } if reorderable_join(input) => RelationNode::Project { input: reorder(Arc::clone(input))?, expressions: expressions.clone() },
        RelationNode::Extend { input, expressions } if reorderable_join(input) => RelationNode::Extend { input: reorder(Arc::clone(input))?, expressions: expressions.clone() },
        RelationNode::Rename { input, prefix, expressions } if reorderable_join(input) => RelationNode::Rename { input: reorder(Arc::clone(input))?, prefix: prefix.clone(), expressions: expressions.clone() },
        _ => return Ok(relation),
    };
    Ok(Arc::new(Relation::new(relation.schema().clone(), node)?))
}

fn reorder(relation: Arc<Relation>) -> Result<Arc<Relation>> { reorder_by(relation, None) }

struct JoinInput { plan: Arc<Relation>, rows: Option<u64> }

impl JoinInput {
    fn rank(&self) -> (bool, u64) {
        (self.rows.is_none(), self.rows.unwrap_or_default())
    }

    fn join(self, other: Self, kind: JoinKind, condition: Option<Expr>, estimate: bool) -> Result<Self> {
        let direct = estimate && condition.as_ref().is_some_and(|expr| direct_equijoin(expr, self.plan.schema(), other.plan.schema()));
        let build_left = !direct || self.rank() <= other.rank();
        let (build_rows, probe_rows) = if build_left { (self.rows, other.rows) } else { (other.rows, self.rows) };
        let rows = joined_rows(self.rows, other.rows, direct);
        let mut plan = Relation::join(self.plan, other.plan, kind, condition)?;
        if estimate && kind == JoinKind::Inner {
            let reason = match (build_rows, probe_rows) {
                (Some(build), Some(probe)) if build < probe => JoinBuildReason::SmallerInput,
                (Some(build), Some(probe)) if build == probe => JoinBuildReason::StableTie,
                (Some(_), None) => JoinBuildReason::KnownInput,
                (None, None) => JoinBuildReason::UnknownInputs,
                _ => JoinBuildReason::ExistingOrder,
            };
            plan = plan.with_join_evidence(JoinPlanEvidence { build_left, build_rows, probe_rows, reason });
        }
        Ok(Self { plan: Arc::new(plan), rows })
    }
}

fn has_volatile_input(relation: &Relation) -> bool {
    let mut pending = vec![relation];
    let mut seen = std::collections::HashSet::new();
    while let Some(relation) = pending.pop() {
        if !seen.insert(std::ptr::from_ref(relation)) { continue; }
        if relation.expressions().into_iter().any(Expr::is_volatile) { return true; }
        if let RelationNode::Reference { binding } = relation.node() {
            pending.push(&binding.relation);
        }
        pending.extend(relation.children().into_iter().map(Arc::as_ref));
    }
    false
}

/// Greedily joins connected inputs in stable rank order, then crosses the disconnected groups.
fn reorder_by(relation: Arc<Relation>, statistics: Option<&mut dyn TableStatistics>) -> Result<Arc<Relation>> {
    fn flatten(relation: &Arc<Relation>, leaves: &mut Vec<Arc<Relation>>, conditions: &mut Vec<Expr>) {
        stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || match relation.node() {
            RelationNode::Join { left, right, condition, .. } if reorderable_join(relation) => {
                flatten(left, leaves, conditions);
                flatten(right, leaves, conditions);
                let mut remaining: Vec<Expr> = condition.iter().cloned().collect();
                while let Some(expression) = remaining.pop() {
                    if let Expr::Binary { op: BinaryOp::And, left, right } = expression { remaining.push(*right); remaining.push(*left); }
                    else { conditions.push(expression); }
                }
            }
            _ => leaves.push(Arc::clone(relation)),
        })
    }
    let (mut leaves, mut conditions) = (Vec::new(), Vec::new());
    flatten(&relation, &mut leaves, &mut conditions);
    let estimate = statistics.is_some();
    let mut inputs = match statistics {
        None => {
            if leaves.len() < 3 { return Ok(relation); }
            leaves.into_iter().map(|plan| JoinInput { plan, rows: None }).collect::<Vec<_>>()
        }
        Some(statistics) => {
            if leaves.iter().any(|leaf| has_volatile_input(leaf)) { return Ok(relation); }
            let inputs = leaves.into_iter().map(|plan| {
                let rows = estimated_rows(&plan, statistics)?;
                Ok(JoinInput { plan, rows })
            }).collect::<Result<Vec<_>>>()?;
            let consider_builds = !conditions.is_empty() && conditions.iter().all(|condition| direct_join_columns(condition).is_some());
            if inputs.iter().all(|input| input.rows.is_none())
                || !consider_builds && inputs.is_sorted_by_key(JoinInput::rank) {
                return Ok(relation);
            }
            inputs
        }
    };
    if estimate {
        inputs.sort_by_key(JoinInput::rank);
    }
    let ids = |relation: &Relation| relation.schema().ids();
    let mut conditions: Vec<(BTreeSet<ColumnId>, Expr)> = conditions.into_iter().map(|condition| (condition.referenced_columns(), condition)).collect();
    let mut remaining: std::collections::VecDeque<JoinInput> = inputs.into();
    let mut groups = Vec::new();
    while let Some(first) = remaining.pop_front() {
        let mut plan = first;
        loop {
            let planned = ids(&plan.plan);
            let connects = |candidate: &JoinInput, columns: &BTreeSet<ColumnId>| {
                let candidate = ids(&candidate.plan);
                columns.iter().any(|id| planned.contains(id)) && columns.iter().any(|id| candidate.contains(id))
                    && columns.iter().all(|id| planned.contains(id) || candidate.contains(id))
            };
            let Some(index) = remaining.iter().position(|candidate| conditions.iter().any(|(columns, _)| connects(candidate, columns))) else { break };
            let candidate = remaining.remove(index).expect("candidate index is in range");
            let joined = planned.union(&ids(&candidate.plan)).copied().collect::<BTreeSet<_>>();
            let (used, kept): (Vec<_>, Vec<_>) = conditions.into_iter().partition(|(columns, _)| !columns.is_empty() && columns.is_subset(&joined));
            conditions = kept;
            let condition = used.into_iter().map(|(_, condition)| condition).reduce(|first, second| Expr::binary(BinaryOp::And, first, second));
            plan = plan.join(candidate, JoinKind::Inner, condition, estimate)?;
        }
        groups.push(plan);
    }
    let last = groups.pop().expect("a join has relations");
    let mut groups = groups.into_iter();
    let crossed = match groups.next() {
        Some(first) => Some(groups.try_fold(first, |left, right| left.join(right, JoinKind::Cross, None, estimate))?),
        None => None,
    };
    let condition = conditions.into_iter().map(|(_, condition)| condition).reduce(|first, second| Expr::binary(BinaryOp::And, first, second));
    let reordered = match crossed {
        Some(crossed) => crossed.join(last, if condition.is_some() { JoinKind::Inner } else { JoinKind::Cross }, condition, estimate)?.plan,
        None => match condition { Some(condition) => Arc::new(last.plan.filter(condition)?), None => last.plan },
    };
    if reordered.schema().fields().iter().map(|field| field.id).eq(relation.schema().fields().iter().map(|field| field.id)) { return Ok(reordered); }
    let expressions = relation.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
    Ok(Arc::new(reordered.project(expressions)?))
}

/// Rebuilds a join, moving each condition conjunct that reads only one input into that input where the join kind
/// preserves its meaning: a preserved side's rows are emitted whether or not they satisfy the condition.
fn join(left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr>) -> Result<Arc<Relation>> {
    if condition.as_ref().is_some_and(Expr::is_volatile) {
        return Ok(Arc::new(Relation::join(left, right, kind, condition)?));
    }
    let (push_left, push_right) = match kind {
        JoinKind::Inner | JoinKind::Semi => (true, true),
        JoinKind::Left | JoinKind::Anti | JoinKind::Single => (false, true),
        JoinKind::Right => (true, false),
        JoinKind::Full | JoinKind::Cross => (false, false),
    };
    let (mut left, mut right) = (left, right);
    let mut kept = Vec::new();
    let mut remaining: Vec<Expr> = condition.into_iter().collect();
    while let Some(expression) = remaining.pop() {
        if let Expr::Binary { op: BinaryOp::And, left, right } = expression { remaining.push(*right); remaining.push(*left); continue; }
        let columns = expression.referenced_columns();
        let within = |side: &Relation| !columns.is_empty() && columns.iter().all(|id| side.schema().get(*id).is_some());
        if push_left && within(&left) { left = select(left, expression)?; }
        else if push_right && within(&right) { right = select(right, expression)?; }
        else { kept.push(expression); }
    }
    let condition = kept.into_iter().reduce(|first, second| Expr::binary(BinaryOp::And, first, second));
    let kind = if kind == JoinKind::Inner && condition.is_none() { JoinKind::Cross } else { kind };
    Ok(Arc::new(Relation::join(left, right, kind, condition)?))
}

fn fusable(relation: &Relation, offset: &Option<Expr>) -> bool {
    match relation.node() {
        RelationNode::SortLimit { offset: inner, fetch: None, .. } => offset.is_none() || inner.is_none(),
        RelationNode::Project { input, expressions } => {
            let mut numbers_rows = false;
            for named in expressions { named.expr.visit(&mut |expr| { if let Expr::Call { name, .. } = expr { numbers_rows |= matches!(name.as_str(), "row-number" | "row_number" | "local-row-number" | "local_row_number"); } }); }
            !numbers_rows && fusable(input, offset)
        }
        RelationNode::Rename { input, .. } => fusable(input, offset),
        _ => false,
    }
}
fn bounds(input: Arc<Relation>, offset: Option<Expr>, fetch: Option<Expr>) -> Result<Arc<Relation>> {
    if fusable(&input, &offset) {
        let node = match input.node() {
            RelationNode::SortLimit { input, order_by, offset: inner, .. } => RelationNode::SortLimit { input: Arc::clone(input), order_by: order_by.clone(), offset: offset.or_else(|| inner.clone()), fetch },
            RelationNode::Project { input, expressions } => RelationNode::Project { input: bounds(Arc::clone(input), offset, fetch)?, expressions: expressions.clone() },
            RelationNode::Rename { input, prefix, expressions } => RelationNode::Rename { input: bounds(Arc::clone(input), offset, fetch)?, prefix: prefix.clone(), expressions: expressions.clone() },
            _ => unreachable!(),
        };
        return Ok(Arc::new(Relation::new(input.schema().clone(), node)?));
    }
    Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by: Vec::new(), offset, fetch })?))
}

fn replace(mut expression: Expr, columns: &BTreeMap<ColumnId, (ColumnId, rtdb_types::LogicalType)>) -> Expr {
    expression.rewrite(&mut |expression| match expression {
        Expr::Column { id, data_type } => {
            if let Some((replacement, ty)) = columns.get(id) {
                *id = *replacement;
                *data_type = ty.clone();
            }
        }
        Expr::RowCopy(fields) => {
            for field in fields {
                if let Some((id, ty)) = columns.get(&field.id) {
                    field.id = *id;
                    field.data_type = ty.clone();
                }
            }
        }
        _ => {}
    });
    expression
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_plan::{ApplyKind, Field};
    use rtdb_types::{LogicalType, ScalarType, Value};

    fn field(id: u32) -> Field {
        Field {
            id: ColumnId(id), name: format!("column_{id}"), qualifier: None,
            qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64),
        }
    }

    fn base_scan(columns: Vec<Field>) -> Arc<Relation> {
        use rtdb_plan::{QualifiedTable, Scan, TableBinding, TemporalFilter};
        use rtdb_types::TableRef;
        let table = TableBinding {
            name: QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "docs") },
            oid: None,
            columns: columns.iter().map(|field| (field.name.clone(), field.data_type.clone())).collect(),
        };
        Arc::new(Relation::scan(Scan {
            table, columns, valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new(),
        }).unwrap())
    }

    fn equals(field: &Field, value: i64) -> Expr { Expr::binary(BinaryOp::Eq, field.expression(), Expr::Literal(Value::I64(value))) }
    fn conjunction(predicates: Vec<Expr>) -> Expr { predicates.into_iter().reduce(|left, right| Expr::binary(BinaryOp::And, left, right)).unwrap() }
    fn scan_predicates(relation: &Relation) -> &[Expr] {
        match relation.node() { RelationNode::Scan(scan) => &scan.predicates, other => panic!("Expected a scan, found {other:?}") }
    }

    struct TableCounts(BTreeMap<&'static str, u64>);

    impl TableStatistics for TableCounts {
        fn row_count(&mut self, table: &QualifiedTable) -> Result<Option<u64>> {
            Ok(self.0.get(table.table.table.as_str()).copied())
        }
    }

    fn named_scan(name: &str, field: &Field) -> Arc<Relation> {
        let base = base_scan(vec![field.clone()]);
        let RelationNode::Scan(scan) = base.node() else { unreachable!() };
        let mut scan = scan.clone();
        scan.table.name.table.table = name.into();
        Arc::new(Relation::scan(scan).unwrap())
    }

    fn equijoin(left: Arc<Relation>, right: Arc<Relation>, a: &Field, b: &Field) -> Arc<Relation> {
        let on = Expr::binary(BinaryOp::Eq, a.expression(), b.expression());
        Arc::new(Relation::join(left, right, JoinKind::Inner, Some(on)).unwrap())
    }

    fn through_projection(relation: &Relation) -> &Relation {
        match relation.node() {
            RelationNode::Project { input, .. } => through_projection(input),
            _ => relation,
        }
    }

    #[test]
    fn a_pushed_selection_can_make_the_larger_table_the_smaller_build() {
        let [a, b] = [0, 1].map(field);
        let range = conjunction(vec![
            Expr::binary(BinaryOp::GtEq, a.expression(), Expr::Literal(Value::I64(7))),
            Expr::binary(BinaryOp::Lt, a.expression(), Expr::Literal(Value::I64(12))),
        ]);
        for (predicate, estimate) in [(equals(&a, 7), 26), (range, 51)] {
            let selective = Arc::new(named_scan("a", &a).filter(predicate).unwrap());
            let join = equijoin(named_scan("b", &b), selective, &b, &a);
            let mut statistics = TableCounts(BTreeMap::from([("a", 101), ("b", 75)]));
            let planned = prepare_with_statistics(Arc::clone(&join), &mut statistics).unwrap();
            assert_eq!(planned.schema(), join.schema());
            let hash = through_projection(&planned);
            assert!(matches!(hash.node(), RelationNode::Join { left, .. } if left.schema().get(a.id).is_some()));
            assert_eq!(hash.join_evidence(), Some(JoinPlanEvidence {
                build_left: true, build_rows: Some(estimate), probe_rows: Some(75), reason: JoinBuildReason::SmallerInput,
            }));
        }
    }

    #[test]
    fn zero_and_bounded_unknown_inputs_rank_ahead_of_unbounded_unknown_inputs() {
        let [a, b] = [0, 1].map(field);
        for fetch in [0, 3] {
            let bounded = Arc::new(named_scan("b", &b).limit(None, Some(Expr::Literal(Value::I64(fetch)))).unwrap());
            let join = equijoin(named_scan("a", &a), bounded, &a, &b);
            let planned = prepare_with_statistics(Arc::clone(&join), &mut TableCounts(BTreeMap::new())).unwrap();
            assert_eq!(planned.schema(), join.schema());
            let hash = through_projection(&planned);
            assert!(matches!(hash.node(), RelationNode::Join { left, right, .. }
                if left.schema().get(b.id).is_some() && right.schema().get(a.id).is_some()));
            assert_eq!(hash.join_evidence(), Some(JoinPlanEvidence {
                build_left: true, build_rows: Some(fetch as u64), probe_rows: None, reason: JoinBuildReason::KnownInput,
            }));
        }
        assert_eq!(partition_rows([(None, 0), (Some(0), 0)]), Some(0));
        assert_eq!(joined_rows(Some(0), None, true), Some(0));
        assert_eq!(joined_rows(Some(u64::MAX), Some(2), false), Some(u64::MAX));
    }

    #[test]
    fn a_connected_intermediate_keeps_logical_order_and_can_build_on_a_smaller_right_input() {
        let [a, b, c] = [0, 1, 2].map(field);
        let first = equijoin(named_scan("a", &a), named_scan("b", &b), &a, &b);
        let join = equijoin(first, named_scan("c", &c), &b, &c);
        for (b_rows, evidence) in [
            (10, JoinPlanEvidence { build_left: true, build_rows: Some(10), probe_rows: Some(10), reason: JoinBuildReason::StableTie }),
            (10_000, JoinPlanEvidence { build_left: false, build_rows: Some(10), probe_rows: Some(10_000), reason: JoinBuildReason::SmallerInput }),
        ] {
            let mut statistics = TableCounts(BTreeMap::from([("a", 1), ("b", b_rows), ("c", 10)]));
            let planned = prepare_with_statistics(Arc::clone(&join), &mut statistics).unwrap();
            assert_eq!(planned.schema(), join.schema());
            let RelationNode::Join { left, right, .. } = planned.node() else { panic!("Expected the connected join"); };
            assert_eq!(left.schema().ids(), BTreeSet::from([a.id, b.id]));
            assert_eq!(right.schema().ids(), BTreeSet::from([c.id]));
            assert_eq!(planned.join_evidence(), Some(evidence));
        }
    }

    #[test]
    fn statistics_keep_volatile_inputs_and_outer_join_builds_on_their_existing_paths() {
        let [a, b] = [0, 1].map(field);
        let mut statistics = TableCounts(BTreeMap::from([("a", 1000), ("b", 1)]));
        let random = Expr::Call { name: "random".into(), args: vec![], return_type: LogicalType::Scalar(ScalarType::F64) };
        let pred = Expr::binary(BinaryOp::Lt, random, Expr::Literal(Value::F64(1.0)));
        let volatile = Arc::new(named_scan("a", &a).filter(pred).unwrap());
        let inner = equijoin(volatile, named_scan("b", &b), &a, &b);
        let planned = prepare_with_statistics(Arc::clone(&inner), &mut statistics).unwrap();
        assert_eq!(planned, prepare(inner).unwrap());
        assert!(planned.join_evidence().is_none());

        for kind in [JoinKind::Left, JoinKind::Right, JoinKind::Full] {
            let condition = Expr::binary(BinaryOp::Eq, a.expression(), b.expression());
            let outer = Arc::new(Relation::join(named_scan("a", &a), named_scan("b", &b), kind, Some(condition)).unwrap());
            let planned = prepare_with_statistics(Arc::clone(&outer), &mut statistics).unwrap();
            assert_eq!(planned.schema(), outer.schema());
            assert_eq!(planned, prepare(outer).unwrap());
            assert!(planned.join_evidence().is_none());
        }
    }

    #[test]
    fn single_table_conjuncts_over_a_cross_join_reach_each_table() {
        let [a, b, c] = [0, 1, 2].map(field);
        let joined = Relation::join(Arc::new(Relation::join(base_scan(vec![a.clone()]), base_scan(vec![b.clone()]), JoinKind::Cross, None).unwrap()), base_scan(vec![c.clone()]), JoinKind::Cross, None).unwrap();
        let filtered = Arc::new(joined).filter(conjunction(vec![equals(&a, 1), equals(&b, 2), equals(&c, 3)])).unwrap();
        let prepared = prepare(Arc::new(filtered)).unwrap();
        let RelationNode::Join { left, right, kind: JoinKind::Cross, condition: None } = prepared.node() else { panic!("Expected the cross join on top: {prepared:?}") };
        let RelationNode::Join { left: first, right: second, kind: JoinKind::Cross, .. } = left.node() else { panic!("Expected a nested cross join") };
        assert_eq!(scan_predicates(first), [equals(&a, 1)]);
        assert_eq!(scan_predicates(second), [equals(&b, 2)]);
        assert_eq!(scan_predicates(right), [equals(&c, 3)]);
    }

    #[test]
    fn a_conjunct_reading_both_sides_of_a_cross_join_becomes_its_inner_join_condition() {
        let [a, b] = [0, 1].map(field);
        let both = Expr::binary(BinaryOp::Eq, a.expression(), b.expression());
        let joined = Relation::join(base_scan(vec![a.clone()]), base_scan(vec![b.clone()]), JoinKind::Cross, None).unwrap();
        let prepared = prepare(Arc::new(Arc::new(joined).filter(both.clone()).unwrap())).unwrap();
        assert!(matches!(prepared.node(), RelationNode::Join { kind: JoinKind::Inner, condition: Some(condition), .. } if *condition == both), "{prepared:?}");
    }

    #[test]
    fn outer_join_conjuncts_move_only_into_the_side_whose_rows_they_can_drop() {
        let [a, b] = [0, 1].map(field);
        let on = conjunction(vec![Expr::binary(BinaryOp::Eq, a.expression(), b.expression()), equals(&a, 1), equals(&b, 2)]);
        let joined = Arc::new(Relation::join(base_scan(vec![a.clone()]), base_scan(vec![b.clone()]), JoinKind::Left, Some(on)).unwrap());
        let filtered = joined.filter(conjunction(vec![equals(&a, 3), Expr::binary(BinaryOp::IsDistinctFrom, b.expression(), Expr::Literal(Value::I64(4)))])).unwrap();
        let prepared = prepare(Arc::new(filtered)).unwrap();
        let RelationNode::Filter { input, predicate } = prepared.node() else { panic!("A WHERE conjunct on the null-supplying side stays above the join: {prepared:?}") };
        assert_eq!(predicate.referenced_columns(), BTreeSet::from([b.id]));
        let RelationNode::Join { left, right, kind: JoinKind::Left, condition: Some(condition) } = input.node() else { panic!("Expected the left join") };
        assert_eq!(scan_predicates(left), [equals(&a, 3)], "the preserved side takes the WHERE conjunct but not the ON one");
        assert_eq!(scan_predicates(right), [equals(&b, 2)]);
        assert_eq!(condition, &conjunction(vec![Expr::binary(BinaryOp::Eq, a.expression(), b.expression()), equals(&a, 1)]));
    }

    #[test]
    fn a_join_reads_only_the_columns_its_parent_and_condition_need() {
        let [a, unused_a, b, unused_b] = [0, 1, 2, 3].map(field);
        let joined = Arc::new(Relation::join(base_scan(vec![a.clone(), unused_a]), base_scan(vec![b.clone(), unused_b]), JoinKind::Inner,
            Some(Expr::binary(BinaryOp::Eq, a.expression(), b.expression()))).unwrap());
        let output = joined.project(vec![NamedExpr { field: a.clone(), expr: a.expression() }]).unwrap();
        let prepared = prepare(Arc::new(output)).unwrap();
        let RelationNode::Project { input, .. } = prepared.node() else { panic!("Expected the projection") };
        let RelationNode::Join { left, right, .. } = input.node() else { panic!("Expected the join") };
        assert_eq!(left.schema().ids(), BTreeSet::from([a.id]));
        assert_eq!(right.schema().ids(), BTreeSet::from([b.id]));
    }

    #[test]
    fn a_relation_joins_the_plan_once_a_condition_connects_it_and_the_written_column_order_is_kept() {
        let [a, b, c] = [0, 1, 2].map(field);
        let crossed = Relation::join(Arc::new(Relation::join(base_scan(vec![a.clone()]), base_scan(vec![b.clone()]), JoinKind::Cross, None).unwrap()), base_scan(vec![c.clone()]), JoinKind::Cross, None).unwrap();
        let a_c = Expr::binary(BinaryOp::Eq, a.expression(), c.expression());
        let prepared = prepare(Arc::new(Arc::new(crossed).filter(a_c.clone()).unwrap())).unwrap();
        let RelationNode::Project { input, expressions } = prepared.node() else { panic!("Expected the column order restored: {prepared:?}") };
        assert_eq!(expressions.iter().map(|named| named.field.id).collect::<Vec<_>>(), [a.id, b.id, c.id]);
        let RelationNode::Join { left, right, kind: JoinKind::Cross, .. } = input.node() else { panic!("Expected the unconnected table crossed last") };
        assert_eq!(right.schema().ids(), BTreeSet::from([b.id]));
        assert!(matches!(left.node(), RelationNode::Join { kind: JoinKind::Inner, condition: Some(condition), .. } if *condition == a_c));
    }

    #[test]
    fn an_aggregate_reads_only_the_columns_its_groups_and_arguments_need() {
        use rtdb_plan::{AggregateExpr, AggregateFunction};
        let [group, argument, unused, count, total] = [0, 1, 2, 3, 4].map(field);
        let scan = base_scan(vec![group.clone(), argument.clone(), unused]);
        let aggregate = |field: Field, function, argument| AggregateExpr { field, function, argument, distinct: false, filter: None, order_by: Vec::new() };
        let schema = Schema::new(vec![group.clone(), count.clone(), total.clone()]).unwrap();
        let grouped = Relation::new(schema, RelationNode::Aggregate { input: scan, groups: vec![NamedExpr { field: group.clone(), expr: group.expression() }],
            aggregates: vec![aggregate(count, AggregateFunction::CountStar, None), aggregate(total, AggregateFunction::Sum, Some(argument.expression()))] }).unwrap();
        let prepared = prepare(Arc::new(grouped)).unwrap();
        let RelationNode::Aggregate { input, .. } = prepared.node() else { panic!("Expected the aggregate") };
        assert_eq!(input.schema().ids(), BTreeSet::from([group.id, argument.id]));
    }

    #[test]
    fn a_query_prepared_from_a_relation_is_optimised_as_a_prepared_statement_is() {
        let a = field(0);
        let filtered = base_scan(vec![a.clone()]).filter(equals(&a, 1)).unwrap();
        let query = crate::PreparedQuery::new(Arc::new(filtered)).unwrap();
        assert_eq!(scan_predicates(query.relation()), [equals(&a, 1)], "a mutation's input reaches its scan's IID and predicate pruning");
    }

    #[test]
    fn a_match_pushed_into_a_scan_is_evaluated_there_and_its_column_read_only_if_needed() {
        use rtdb_plan::{NamedExpr, UnaryOp};
        let id = field(0);
        let body = Field { name: "body".into(), data_type: LogicalType::Scalar(ScalarType::Utf8).maybe(), ..field(1) };
        let matching = Expr::Call { name: "match".into(), args: vec![body.expression(), Expr::Literal(Value::Utf8("overdue".into()))], return_type: LogicalType::Scalar(ScalarType::Bool).maybe() };
        for (projected, reads_body) in [(vec![id.clone()], false), (vec![id.clone(), body.clone()], true)] {
            let filtered = Arc::new(base_scan(vec![id.clone(), body.clone()]).filter(Expr::unary(UnaryOp::Not, matching.clone())).unwrap());
            let output: Vec<_> = projected.iter().map(|field| NamedExpr { field: Field { id: ColumnId(field.id.0 + 10), ..field.clone() }, expr: field.expression() }).collect();
            let prepared = prepare(Arc::new(filtered.project(output).unwrap())).unwrap();
            let RelationNode::Project { input, .. } = prepared.node() else { panic!("Expected the projection") };
            let RelationNode::Scan(scan) = input.node() else { panic!("Expected the filter in the scan, not {:?}", input.node()) };
            let [evaluated] = scan.matches.as_slice() else { panic!("Expected one evaluated match: {:?}", scan.matches) };
            assert_eq!((evaluated.column.as_str(), &evaluated.query), ("body", &Expr::Literal(Value::Utf8("overdue".into()))));
            assert!(evaluated.id.0 > 1 && projected.iter().all(|field| field.id.0 + 10 != evaluated.id.0), "a fresh column id");
            assert_eq!(scan.predicates, [Expr::unary(UnaryOp::Not, Expr::Column { id: evaluated.id, data_type: LogicalType::Scalar(ScalarType::Bool).maybe() })]);
            assert_eq!(scan.columns.iter().any(|field| field.id == body.id), reads_body, "{projected:?}");
        }
    }

    #[test]
    fn counting_all_rows_needs_no_columns_from_the_scan() {
        let count = rtdb_plan::AggregateExpr { field: field(5), function: rtdb_plan::AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: vec![] };
        let aggregate = Arc::new(Relation::new(Schema::new(vec![field(5)]).unwrap(), RelationNode::Aggregate { input: base_scan(vec![field(0), field(1)]), groups: vec![], aggregates: vec![count] }).unwrap());
        let prepared = prepare(aggregate).unwrap();
        let RelationNode::Aggregate { input, .. } = prepared.node() else { panic!("Expected the aggregate") };
        let RelationNode::Scan(scan) = input.node() else { panic!("Expected the scan") };
        assert!(scan.columns.is_empty());
    }

    #[test]
    fn counting_matching_rows_reads_no_column_but_the_scans_own_match() {
        let body = Field { name: "body".into(), data_type: LogicalType::Scalar(ScalarType::Utf8).maybe(), ..field(1) };
        let matching = Expr::Call { name: "match".into(), args: vec![body.expression(), Expr::Literal(Value::Utf8("overdue".into()))], return_type: LogicalType::Scalar(ScalarType::Bool).maybe() };
        let filtered = Arc::new(base_scan(vec![field(0), body.clone()]).filter(matching).unwrap());
        let count = rtdb_plan::AggregateExpr { field: field(5), function: rtdb_plan::AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: vec![] };
        let aggregate = Arc::new(Relation::new(Schema::new(vec![field(5)]).unwrap(), RelationNode::Aggregate { input: filtered, groups: vec![], aggregates: vec![count] }).unwrap());
        let prepared = prepare(aggregate).unwrap();
        let RelationNode::Aggregate { input, .. } = prepared.node() else { panic!("Expected the aggregate") };
        let RelationNode::Project { input, expressions } = input.node() else { panic!("Expected the scan's projection, not {:?}", input.node()) };
        let RelationNode::Scan(scan) = input.node() else { panic!("Expected the scan") };
        assert!(expressions.is_empty());
        assert_eq!(scan.columns.iter().map(|field| field.name.as_str()).collect::<Vec<_>>(), ["match(body)"]);
    }

    #[test]
    fn a_scans_evaluated_matches_stay_out_of_every_schema_above_it() {
        use rtdb_plan::JoinKind;
        let body = Field { name: "body".into(), data_type: LogicalType::Scalar(ScalarType::Utf8).maybe(), ..field(1) };
        let matching = Expr::Call { name: "match".into(), args: vec![body.expression(), Expr::Literal(Value::Utf8("overdue".into()))], return_type: LogicalType::Scalar(ScalarType::Bool).maybe() };
        let left = Arc::new(base_scan(vec![field(0), body.clone()]).filter(matching).unwrap());
        let right = base_scan(vec![field(2)]);
        let schema = left.schema().concat(right.schema()).unwrap();
        let joined = Arc::new(Relation::new(schema.clone(), RelationNode::Join { left: Arc::clone(&left), right, kind: JoinKind::Cross, condition: None }).unwrap());
        let prepared = prepare(joined).unwrap();
        assert_eq!(prepared.schema(), &schema);
        let RelationNode::Join { left: prepared_left, .. } = prepared.node() else { panic!("Expected the join") };
        assert_eq!(prepared_left.schema(), left.schema(), "the filtered side keeps its fields");
        assert_eq!(prepare(Arc::clone(&left)).unwrap().schema(), left.schema(), "so does a filtered scan at the root");
    }

    #[test]
    fn single_column_conjunct_reaches_the_scan_before_multi_column_selection() {
        let [a, b] = [0, 1].map(field);
        let projection = base_scan(vec![a.clone(), b.clone()]);
        let multi = Expr::binary(BinaryOp::Eq, Expr::binary(BinaryOp::Add, a.expression(), b.expression()), Expr::Literal(Value::I64(12)));
        let single = Expr::binary(BinaryOp::Eq, b.expression(), Expr::Literal(Value::I64(1)));
        let input = Arc::new(projection.filter(Expr::binary(BinaryOp::And, multi.clone(), single.clone())).unwrap());
        let prepared = prepare(input).unwrap();
        let RelationNode::Filter { input, predicate } = prepared.node() else { panic!("Expected a remaining selection"); };
        let RelationNode::Scan(scan) = input.node() else { panic!("Expected a scan"); };
        assert_eq!(predicate, &multi);
        assert_eq!(scan.predicates, [single]);
    }

    #[test]
    fn window_and_unnest_keep_requested_and_expression_input_columns() {
        use rtdb_plan::{NamedExpr, SortExpr, WindowExpr};

        let [selected, partition, ordered, list_value, lagged, unused] = [0, 1, 2, 3, 4, 5].map(field);
        let source = vec![selected.clone(), partition.clone(), ordered.clone(), list_value.clone(), lagged.clone(), unused];
        let scan = base_scan(source);
        let list_expr = Expr::List(vec![list_value.expression()]);
        let list = Field { data_type: list_expr.data_type(), ..field(6) };
        let extended = Arc::new(scan.extend(vec![NamedExpr { field: list.clone(), expr: list_expr }]).unwrap());
        let element = field(7);
        let schema = extended.schema().concat(&Schema::new(vec![element.clone()]).unwrap()).unwrap();
        let values = Arc::new(Relation::new(schema, RelationNode::Unnest { input: extended, expr: list.expression(), element, ordinality: None, outer: false }).unwrap());
        let lag = Field { data_type: lagged.data_type.clone().maybe(), ..field(8) };
        let schema = values.schema().concat(&Schema::new(vec![lag.clone()]).unwrap()).unwrap();
        let window = Arc::new(Relation::new(schema, RelationNode::Window {
            input: values, partition_by: vec![partition.id],
            order_by: vec![SortExpr { expr: ordered.expression(), descending: false, nulls_first: false }],
            expressions: vec![WindowExpr { field: lag.clone(), function: WindowFunction::Lag { column: lagged.id, offset: 1 } }],
        }).unwrap());
        let output = window.project([selected, lag].into_iter().map(|field| NamedExpr { expr: field.expression(), field }).collect()).unwrap();
        let optimized = prepare(Arc::new(output)).unwrap();
        let RelationNode::Project { input: window, .. } = optimized.node() else { panic!("Expected output projection"); };
        let RelationNode::Window { input: unnest, .. } = window.node() else { panic!("Expected window"); };
        let RelationNode::Unnest { input: extended, .. } = unnest.node() else { panic!("Expected UNNEST"); };
        let RelationNode::Extend { input: scan, .. } = extended.node() else { panic!("Expected list projection"); };
        assert_eq!(scan.schema().fields().iter().map(|field| field.id).collect::<Vec<_>>(), (0..5).map(ColumnId).collect::<Vec<_>>());
    }

    #[test]
    fn selection_moves_below_unnest_inside_a_dependent_plan() {
        let source_field = field(0);
        let element = field(1);
        let input = Arc::new(Relation::new(
            Schema::new(vec![source_field.clone()]).unwrap(),
            RelationNode::Values { rows: vec![vec![Expr::Literal(Value::I64(1))]] },
        ).unwrap());
        let unnest = Arc::new(Relation::new(
            Schema::new(vec![source_field.clone(), element.clone()]).unwrap(),
            RelationNode::Unnest {
                input, expr: Expr::List(vec![source_field.expression()]), element, ordinality: None, outer: false,
            },
        ).unwrap());
        let predicate = Expr::binary(BinaryOp::Eq, source_field.expression(), Expr::Parameter {
            index: 0, data_type: source_field.data_type,
        });
        let filtered = Arc::new(unnest.filter(predicate).unwrap());
        let external = prepare(Arc::clone(&filtered)).unwrap();
        assert!(matches!(external.node(), RelationNode::Unnest { input, .. } if matches!(input.node(), RelationNode::Filter { .. })));
        let applied = Arc::new(Relation::new(filtered.schema().clone(), RelationNode::Apply {
            input: Arc::new(Relation::unit()), dependent: filtered,
            arguments: vec![(0, Expr::Literal(Value::I64(1)))], kind: ApplyKind::Inner, output: None,
        }).unwrap());
        let prepared = prepare(Arc::clone(&applied)).unwrap();
        let RelationNode::Join { right, .. } = prepared.node() else { panic!("Expected the application decorrelated into a join: {prepared:?}") };
        assert!(matches!(right.node(), RelationNode::Unnest { input, .. } if matches!(input.node(), RelationNode::Filter { .. })));
        DECORRELATES.with(|enabled| enabled.set(false));
        let captured = prepare(applied);
        DECORRELATES.with(|enabled| enabled.set(true));
        let captured = captured.unwrap();
        let RelationNode::Apply { dependent, .. } = captured.node() else { panic!("Expected captured application") };
        assert!(matches!(dependent.node(), RelationNode::Unnest { input, .. } if matches!(input.node(), RelationNode::Filter { .. })));
    }

    #[test]
    fn column_selection_over_a_projection_folds_into_one_projection() {
        let [a, b, inner_a, inner_b, outer_b, outer_a] = [0, 1, 2, 3, 4, 5].map(field);
        let doubled = Expr::binary(BinaryOp::Add, a.expression(), a.expression());
        let inner = Arc::new(base_scan(vec![a.clone(), b.clone()]).project(vec![
            NamedExpr { field: inner_a.clone(), expr: doubled.clone() }, NamedExpr { field: inner_b.clone(), expr: b.expression() },
        ]).unwrap());
        let outer = inner.project(vec![
            NamedExpr { field: outer_b.clone(), expr: inner_b.expression() }, NamedExpr { field: outer_a.clone(), expr: inner_a.expression() },
        ]).unwrap();
        let prepared = prepare(Arc::new(outer)).unwrap();
        let RelationNode::Project { input, expressions } = prepared.node() else { panic!("Expected a projection"); };
        assert!(matches!(input.node(), RelationNode::Scan(_)));
        assert_eq!(expressions, &[NamedExpr { field: outer_b.clone(), expr: b.expression() }, NamedExpr { field: outer_a.clone(), expr: doubled }]);

        let repeated = Arc::new(base_scan(vec![a.clone()]).project(vec![NamedExpr { field: inner_a.clone(), expr: a.expression() }]).unwrap())
            .project(vec![NamedExpr { field: outer_a, expr: inner_a.expression() }, NamedExpr { field: outer_b, expr: inner_a.expression() }]).unwrap();
        let prepared = prepare(Arc::new(repeated)).unwrap();
        let RelationNode::Project { input, .. } = prepared.node() else { panic!("Expected a projection"); };
        assert!(matches!(input.node(), RelationNode::Project { .. }), "a column selected twice keeps both projections");
    }
}
