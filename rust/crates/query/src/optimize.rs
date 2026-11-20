use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_plan::{BinaryOp, ColumnId, Expr, NamedExpr, Relation, RelationNode, Schema, WindowFunction};
use rtdb_types::Result;

pub(crate) fn prepare(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    let mut ids = Ids::after(&relation)?;
    prepare_with(relation, &mut ids)
}

fn prepare_with(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    let required = relation.schema().ids();
    annotate_top_k(prune(relation, &required, ids)?)
}

/// Column ids no field or expression of the plan uses, for the columns the optimizer adds.
struct Ids(u32);

impl Ids {
    fn after(relation: &Relation) -> Result<Self> {
        fn highest(relation: &Relation) -> u32 {
            let own = relation.schema().fields().iter().map(|field| field.id.0)
                .chain(relation.expressions().into_iter().flat_map(|expression| expression.referenced_columns()).map(|id| id.0)).max().unwrap_or(0);
            relation.children().into_iter().map(|child| highest(child)).fold(own, u32::max)
        }
        highest(relation).checked_add(1).map(Self).ok_or_else(exhausted)
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
            let ([Expr::Column { id, .. }, query], true) = (args.as_slice(), name.eq_ignore_ascii_case("match")) else { return };
            if !matches!(query, Expr::Literal(_) | Expr::Parameter { .. }) { return; }
            let Some(column) = scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.clone()) else { return };
            let id = match ids.next() { Ok(id) => id, Err(error) => { failure = Some(error); return; } };
            let data_type = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Bool).maybe();
            added.push((rtdb_plan::Field { id, name: format!("match({column})"), qualifier: None, qualified_only: true, unnamed: true, data_type: data_type.clone() },
                rtdb_plan::ScanMatch { id, column, query: query.clone() }));
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
fn annotate_top_k(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    let rebuilt = match relation.node() {
        RelationNode::Project { input, expressions } => RelationNode::Project { input: annotate_top_k(Arc::clone(input))?, expressions: expressions.clone() },
        RelationNode::Rename { input, prefix, expressions } => RelationNode::Rename { input: annotate_top_k(Arc::clone(input))?, prefix: prefix.clone(), expressions: expressions.clone() },
        RelationNode::SortLimit { input, order_by, offset, fetch: Some(fetch) } => {
            let Some(first) = order_by.first().filter(|order| order.descending) else { return Ok(relation) };
            let limit = match offset { Some(offset) => Expr::binary(BinaryOp::Add, offset.clone(), fetch.clone()), None => fetch.clone() };
            let Some(input) = push_top_k(input, first.expr.clone(), &limit)? else { return Ok(relation) };
            RelationNode::SortLimit { input, order_by: order_by.clone(), offset: offset.clone(), fetch: Some(fetch.clone()) }
        }
        _ => return Ok(relation),
    };
    Ok(Arc::new(Relation::new(relation.schema().clone(), rebuilt)?))
}

fn push_top_k(relation: &Arc<Relation>, order: Expr, limit: &Expr) -> Result<Option<Arc<Relation>>> {
    let through = |expressions: &[NamedExpr]| {
        let defined: BTreeMap<ColumnId, Expr> = expressions.iter().map(|named| (named.field.id, named.expr.clone())).collect();
        let mut order = order.clone();
        order.rewrite(&mut |expression| if let Expr::Column { id, .. } = expression { if let Some(definition) = defined.get(id) { *expression = definition.clone(); } });
        order
    };
    let node = match relation.node() {
        RelationNode::Project { input, expressions } => {
            let Some(input) = push_top_k(input, through(expressions), limit)? else { return Ok(None) };
            RelationNode::Project { input, expressions: expressions.clone() }
        }
        RelationNode::Rename { input, prefix, expressions } => {
            let Some(input) = push_top_k(input, through(expressions), limit)? else { return Ok(None) };
            RelationNode::Rename { input, prefix: prefix.clone(), expressions: expressions.clone() }
        }
        RelationNode::Extend { input, expressions } => {
            let Some(input) = push_top_k(input, through(expressions), limit)? else { return Ok(None) };
            return Ok(Some(Arc::new(input.extend(expressions.clone())?)));
        }
        RelationNode::Scan(scan) => {
            let Expr::Call { name, args, .. } = &order else { return Ok(None) };
            let ([Expr::Column { id, .. }, query], true) = (args.as_slice(), name.eq_ignore_ascii_case("search_score")) else { return Ok(None) };
            if !matches!(query, Expr::Literal(_) | Expr::Parameter { .. }) || !scan.columns.iter().any(|field| field.id == *id) { return Ok(None); }
            let name = scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.as_str());
            let evaluated: Vec<ColumnId> = scan.matches.iter().filter(|evaluated| Some(evaluated.column.as_str()) == name).map(|evaluated| evaluated.id).collect();
            if !scan.predicates.iter().any(|predicate| has_match_on(predicate, *id, &evaluated)) { return Ok(None); }
            let mut scan = scan.clone();
            scan.top_k = Some(rtdb_plan::ScanTopK { column: *id, query: query.clone(), limit: limit.clone() });
            return Ok(Some(Arc::new(Relation::scan(scan)?)));
        }
        _ => return Ok(None),
    };
    Ok(Some(Arc::new(Relation::new(relation.schema().clone(), node)?)))
}

/// Whether a conjunct of the predicate is a MATCH on `column`, or one of the scan's own columns evaluating such a MATCH.
fn has_match_on(predicate: &Expr, column: ColumnId, evaluated: &[ColumnId]) -> bool {
    match predicate {
        Expr::Binary { op: BinaryOp::And, left, right } => has_match_on(left, column, evaluated) || has_match_on(right, column, evaluated),
        Expr::Call { name, args, .. } => name.eq_ignore_ascii_case("match") && matches!(args.first(), Some(Expr::Column { id, .. }) if *id == column),
        Expr::Column { id, .. } => evaluated.contains(id),
        _ => false,
    }
}

fn prune(relation: Arc<Relation>, required: &BTreeSet<ColumnId>, ids: &mut Ids) -> Result<Arc<Relation>> {
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
            let selected = select(input, predicate.clone())?;
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
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } => Ok(relation),
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
        RelationNode::Join { left, right, kind, condition } => {
            let left = prepare_with(Arc::clone(left), ids)?; let right = prepare_with(Arc::clone(right), ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Join { left, right, kind: *kind, condition: condition.clone() })?))
        }
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            let input = prepare_with(Arc::clone(input), ids)?; let dependent = prepare_with(Arc::clone(dependent), ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Mark { input, dependent, arguments: arguments.clone(), condition: condition.clone(), comparison: comparison.clone(), output: output.clone() })?))
        }
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = prepare_with(Arc::clone(input), ids)?; let dependent = prepare_with(Arc::clone(dependent), ids)?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Apply { input, dependent, arguments: arguments.clone(), kind: *kind, output: output.clone() })?))
        }
        RelationNode::Aggregate { input, groups, aggregates } => {
            let input = prepare_with(Arc::clone(input), ids)?;
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

fn select(input: Arc<Relation>, predicate: Expr) -> Result<Arc<Relation>> {
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
        return Ok(Arc::new(input.filter(predicate)?));
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
                    _ => return Ok(Arc::new(input.filter(predicate)?)),
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
        _ => Ok(Arc::new(input.filter(predicate)?)),
    }
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
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, matches: Vec::new(),
        }).unwrap())
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
        let prepared = prepare(applied).unwrap();
        let RelationNode::Apply { dependent, .. } = prepared.node() else { panic!("Expected captured application") };
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
