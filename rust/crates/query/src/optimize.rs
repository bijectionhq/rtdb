use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_plan::{BinaryOp, ColumnId, Expr, JoinKind, NamedExpr, Relation, RelationNode, Schema, WindowFunction};
use rtdb_types::Result;

pub(crate) fn prepare(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    let required = relation.schema().ids();
    prune(relation, &required)
}

fn prune(relation: Arc<Relation>, required: &BTreeSet<ColumnId>) -> Result<Arc<Relation>> {
    match relation.node() {
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            let expressions = expressions.iter().filter(|named| required.contains(&named.field.id)).cloned().collect::<Vec<_>>();
            let columns = expressions.iter().flat_map(|named| named.expr.referenced_columns()).collect();
            let input = prune(Arc::clone(input), &columns)?;
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
            let input = prune(Arc::clone(input), &columns)?;
            Ok(Arc::new(input.extend(expressions.clone())?))
        }
        RelationNode::Filter { input, predicate } => {
            let mut columns = required.clone(); columns.extend(predicate.referenced_columns());
            let input = prune(Arc::clone(input), &columns)?;
            reorder_selected(select(input, predicate.clone())?)
        }
        RelationNode::Scan(scan) => {
            let mut columns = required.clone(); for predicate in &scan.predicates { columns.extend(predicate.referenced_columns()); }
            let mut scan = scan.clone(); scan.columns.retain(|field| columns.contains(&field.id));
            Ok(Arc::new(Relation::scan(scan)?))
        }
        // An opened VALUES table evaluates every cell, including fields discarded by a parent projection.
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } => Ok(relation),
        RelationNode::Limit { input, offset, fetch } => {
            let child = prune(Arc::clone(input), required)?;
            bounds(child, offset.clone(), fetch.clone())
        }
        RelationNode::Sort { input, order_by } => {
            let mut columns = required.clone(); for order in order_by { columns.extend(order.expr.referenced_columns()); }
            let input = prune(Arc::clone(input), &columns)?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by: order_by.clone(), offset: None, fetch: None })?))
        }
        RelationNode::SortLimit { input, order_by, offset, fetch } => {
            let mut columns = required.clone(); for order in order_by { columns.extend(order.expr.referenced_columns()); }
            let input = prune(Arc::clone(input), &columns)?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit { input, order_by: order_by.clone(), offset: offset.clone(), fetch: fetch.clone() })?))
        }
        RelationNode::Distinct { input } => {
            let input = prepare(Arc::clone(input))?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input })?))
        }
        RelationNode::Let { binding, body } => {
            let body = prune(Arc::clone(body), required)?;
            Ok(Arc::new(Relation::new(body.schema().clone(), RelationNode::Let { binding: Arc::clone(binding), body })?))
        }
        RelationNode::Join { .. } => join_tree(&relation, required, true),
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            let input = prepare(Arc::clone(input))?; let dependent = prepare(Arc::clone(dependent))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Mark { input, dependent, arguments: arguments.clone(), condition: condition.clone(), comparison: comparison.clone(), output: output.clone() })?))
        }
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = prepare(Arc::clone(input))?; let dependent = prepare(Arc::clone(dependent))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Apply { input, dependent, arguments: arguments.clone(), kind: *kind, output: output.clone() })?))
        }
        RelationNode::Aggregate { input, groups, aggregates } => {
            let mut columns: BTreeSet<ColumnId> = groups.iter().flat_map(|group| group.expr.referenced_columns()).collect();
            for aggregate in aggregates {
                columns.extend(aggregate.argument.iter().chain(&aggregate.filter).flat_map(Expr::referenced_columns));
                columns.extend(aggregate.order_by.iter().flat_map(|order| order.expr.referenced_columns()));
            }
            if columns.is_empty() { columns.extend(input.schema().fields().first().map(|field| field.id)); }
            let input = prune(Arc::clone(input), &columns)?;
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
            let input = prune(Arc::clone(input), &columns)?;
            let schema = Schema::new(input.schema().fields().iter().cloned().chain(expressions.iter().map(|expression| expression.field.clone())).collect())?;
            Ok(Arc::new(Relation::new(schema, RelationNode::Window { input, partition_by: partition_by.clone(), order_by: order_by.clone(), expressions: expressions.clone() })?))
        }
        RelationNode::Set { left, right, operator, all } => {
            let left = prepare(Arc::clone(left))?; let right = prepare(Arc::clone(right))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Set { left, right, operator: *operator, all: *all })?))
        }
        RelationNode::Ordinality { input, field, scope } => {
            let input = prepare(Arc::clone(input))?;
            Ok(Arc::new(input.with_ordinality(field.clone(), *scope)?))
        }
        RelationNode::Unnest { input, expr, element, ordinality, outer } => {
            let mut columns = required.clone();
            columns.extend(expr.referenced_columns());
            let input = prune(Arc::clone(input), &columns)?;
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
        RelationNode::Filter { input: child, predicate: existing } => {
            let child = select(Arc::clone(child), predicate)?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::Filter { input: child, predicate: existing.clone() })?))
        }
        RelationNode::Join { left, right, kind, condition } => {
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
            Ok(Arc::new(input.filter(predicate)?))
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

fn inner(relation: &Relation) -> bool { matches!(relation.node(), RelationNode::Join { kind: JoinKind::Inner | JoinKind::Cross, .. }) }

/// Prunes a join's inputs to the columns its parent and condition read; the outermost of a run of inner and cross joins
/// then reorders that run, as the reference's mega-join does.
fn join_tree(relation: &Arc<Relation>, required: &BTreeSet<ColumnId>, outermost: bool) -> Result<Arc<Relation>> {
    let RelationNode::Join { left, right, kind, condition } = relation.node() else { return prune(Arc::clone(relation), required) };
    let mut columns = required.clone();
    if let Some(condition) = condition { columns.extend(condition.referenced_columns()); }
    let side = |input: &Arc<Relation>| -> Result<Arc<Relation>> {
        let mut needed: BTreeSet<ColumnId> = input.schema().ids().intersection(&columns).copied().collect();
        if needed.is_empty() { needed.extend(input.schema().fields().first().map(|field| field.id)); }
        if inner(relation) && inner(input) { join_tree(input, &needed, false) } else { prune(Arc::clone(input), &needed) }
    };
    let joined = join(side(left)?, side(right)?, *kind, condition.clone())?;
    if outermost && inner(&joined) { reorder(joined) } else { Ok(joined) }
}

/// Reorders the join run a selection was promoted into, beneath at most one column-preserving wrapper.
fn reorder_selected(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    if inner(&relation) { return reorder(relation); }
    let node = match relation.node() {
        RelationNode::Project { input, expressions } if inner(input) => RelationNode::Project { input: reorder(Arc::clone(input))?, expressions: expressions.clone() },
        RelationNode::Extend { input, expressions } if inner(input) => RelationNode::Extend { input: reorder(Arc::clone(input))?, expressions: expressions.clone() },
        RelationNode::Rename { input, prefix, expressions } if inner(input) => RelationNode::Rename { input: reorder(Arc::clone(input))?, prefix: prefix.clone(), expressions: expressions.clone() },
        _ => return Ok(relation),
    };
    Ok(Arc::new(Relation::new(relation.schema().clone(), node)?))
}

/// Joins each relation to the plan built so far as soon as a condition connects them, rather than in FROM order.
/// Relations keep their written order otherwise, which is the reference's order when no relation has row-count statistics.
/// Disconnected groups are crossed last, the final join taking the conditions none of them could.
fn reorder(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    fn flatten(relation: &Arc<Relation>, leaves: &mut Vec<Arc<Relation>>, conditions: &mut Vec<Expr>) {
        match relation.node() {
            RelationNode::Join { left, right, kind: JoinKind::Inner | JoinKind::Cross, condition } => {
                flatten(left, leaves, conditions);
                flatten(right, leaves, conditions);
                let mut remaining: Vec<Expr> = condition.iter().cloned().collect();
                while let Some(expression) = remaining.pop() {
                    if let Expr::Binary { op: BinaryOp::And, left, right } = expression { remaining.push(*right); remaining.push(*left); }
                    else { conditions.push(expression); }
                }
            }
            _ => leaves.push(Arc::clone(relation)),
        }
    }
    let (mut leaves, mut conditions) = (Vec::new(), Vec::new());
    flatten(&relation, &mut leaves, &mut conditions);
    if leaves.len() < 3 { return Ok(relation); }
    let ids = |relation: &Relation| relation.schema().ids();
    let mut conditions: Vec<(BTreeSet<ColumnId>, Expr)> = conditions.into_iter().map(|condition| (condition.referenced_columns(), condition)).collect();
    let mut remaining: std::collections::VecDeque<Arc<Relation>> = leaves.into();
    let mut groups = Vec::new();
    while let Some(first) = remaining.pop_front() {
        let mut plan = first;
        loop {
            let planned = ids(&plan);
            let connects = |candidate: &Relation, columns: &BTreeSet<ColumnId>| {
                let candidate = ids(candidate);
                columns.iter().any(|id| planned.contains(id)) && columns.iter().any(|id| candidate.contains(id))
                    && columns.iter().all(|id| planned.contains(id) || candidate.contains(id))
            };
            let Some(index) = remaining.iter().position(|candidate| conditions.iter().any(|(columns, _)| connects(candidate, columns))) else { break };
            let candidate = remaining.remove(index).expect("candidate index is in range");
            let joined = planned.union(&ids(&candidate)).copied().collect::<BTreeSet<_>>();
            let (used, kept): (Vec<_>, Vec<_>) = conditions.into_iter().partition(|(columns, _)| !columns.is_empty() && columns.is_subset(&joined));
            conditions = kept;
            let condition = used.into_iter().map(|(_, condition)| condition).reduce(|first, second| Expr::binary(BinaryOp::And, first, second));
            plan = Arc::new(Relation::join(plan, candidate, JoinKind::Inner, condition)?);
        }
        groups.push(plan);
    }
    let last = groups.pop().expect("a join has relations");
    let mut groups = groups.into_iter();
    let crossed = match groups.next() {
        Some(first) => Some(groups.try_fold(first, |left, right| Relation::join(left, right, JoinKind::Cross, None).map(Arc::new))?),
        None => None,
    };
    let condition = conditions.into_iter().map(|(_, condition)| condition).reduce(|first, second| Expr::binary(BinaryOp::And, first, second));
    let reordered = match crossed {
        Some(crossed) => Arc::new(Relation::join(crossed, last, if condition.is_some() { JoinKind::Inner } else { JoinKind::Cross }, condition)?),
        None => match condition { Some(condition) => Arc::new(last.filter(condition)?), None => last },
    };
    if reordered.schema().fields().iter().map(|field| field.id).eq(relation.schema().fields().iter().map(|field| field.id)) { return Ok(reordered); }
    let expressions = relation.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
    Ok(Arc::new(reordered.project(expressions)?))
}

/// Rebuilds a join, moving each condition conjunct that reads only one input into that input where the join kind
/// preserves its meaning: a preserved side's rows are emitted whether or not they satisfy the condition.
fn join(left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr>) -> Result<Arc<Relation>> {
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
            clamp_valid_time: false, iids: None, predicates: Vec::new(),
        }).unwrap())
    }

    fn equals(field: &Field, value: i64) -> Expr { Expr::binary(BinaryOp::Eq, field.expression(), Expr::Literal(Value::I64(value))) }
    fn conjunction(predicates: Vec<Expr>) -> Expr { predicates.into_iter().reduce(|left, right| Expr::binary(BinaryOp::And, left, right)).unwrap() }
    fn scan_predicates(relation: &Relation) -> &[Expr] {
        match relation.node() { RelationNode::Scan(scan) => &scan.predicates, other => panic!("Expected a scan, found {other:?}") }
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
