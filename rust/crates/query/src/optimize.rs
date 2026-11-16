use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Relation, RelationNode, Schema};
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
            let node = match relation.node() { RelationNode::Rename { prefix, .. } => RelationNode::Rename { input, prefix: prefix.clone(), expressions }, _ => RelationNode::Project { input, expressions } };
            Ok(Arc::new(Relation::new(schema, node)?))
        }
        RelationNode::Filter { input, predicate } => {
            let mut columns = required.clone(); columns.extend(predicate.referenced_columns());
            let input = prune(Arc::clone(input), &columns)?;
            select(input, predicate.clone())
        }
        RelationNode::Scan(scan) => {
            let mut columns = required.clone(); for predicate in &scan.predicates { columns.extend(predicate.referenced_columns()); }
            let mut scan = scan.clone(); scan.columns.retain(|field| columns.contains(&field.id));
            Ok(Arc::new(Relation::scan(scan)?))
        }
        RelationNode::Values { rows } => {
            let indices = relation.schema().fields().iter().enumerate().filter_map(|(index, field)| required.contains(&field.id).then_some(index)).collect::<Vec<_>>();
            let schema = Schema::new(indices.iter().map(|index| relation.schema().fields()[*index].clone()).collect())?;
            let rows = rows.iter().map(|row| indices.iter().map(|index| row[*index].clone()).collect()).collect();
            Ok(Arc::new(Relation::new(schema, RelationNode::Values { rows })?))
        }
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
        RelationNode::Join { left, right, kind, condition } => {
            let left = prepare(Arc::clone(left))?; let right = prepare(Arc::clone(right))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Join { left, right, kind: *kind, condition: condition.clone() })?))
        }
        RelationNode::Mark { input, dependent, arguments, comparison, output } => {
            let input = prepare(Arc::clone(input))?; let dependent = prepare(Arc::clone(dependent))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Mark { input, dependent, arguments: arguments.clone(), comparison: comparison.clone(), output: output.clone() })?))
        }
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = prepare(Arc::clone(input))?; let dependent = prepare(Arc::clone(dependent))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Apply { input, dependent, arguments: arguments.clone(), kind: *kind, output: output.clone() })?))
        }
        RelationNode::Aggregate { input, groups, aggregates } => {
            let input = prepare(Arc::clone(input))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Aggregate { input, groups: groups.clone(), aggregates: aggregates.clone() })?))
        }
        RelationNode::Window { input, partition_by, order_by, expressions } => {
            let input = prepare(Arc::clone(input))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Window { input, partition_by: partition_by.clone(), order_by: order_by.clone(), expressions: expressions.clone() })?))
        }
        RelationNode::Set { left, right, operator, all } => {
            let left = prepare(Arc::clone(left))?; let right = prepare(Arc::clone(right))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Set { left, right, operator: *operator, all: *all })?))
        }
        RelationNode::Unnest { input, expr, element, ordinality, outer } => {
            let input = prepare(Arc::clone(input))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Unnest { input, expr: expr.clone(), element: element.clone(), ordinality: ordinality.clone(), outer: *outer })?))
        }
        RelationNode::PatchGaps { .. } | RelationNode::Reference { .. } => Ok(relation),
    }
}

fn select(input: Arc<Relation>, predicate: Expr) -> Result<Arc<Relation>> {
    if let Expr::Binary { op: BinaryOp::And, left, right } = predicate {
        return select(select(input, *left)?, *right);
    }
    let mut correlated = false;
    predicate.visit(&mut |expr| { correlated |= matches!(expr, Expr::Correlated { .. }); });
    if correlated || predicate.referenced_columns().len() != 1 { return Ok(Arc::new(input.filter(predicate)?)); }
    match input.node() {
        RelationNode::Rename { input: child, prefix, expressions } if expressions.iter().all(|named| matches!(named.expr, Expr::Column { .. })) => {
            let replacements = expressions.iter().filter_map(|named| match &named.expr { Expr::Column { id, data_type } => Some((named.field.id, (*id, data_type.clone()))), _ => None }).collect();
            let child = select(Arc::clone(child), replace(predicate, &replacements))?;
            Ok(Arc::new(Relation::new(input.schema().clone(), RelationNode::Rename { input: child, prefix: prefix.clone(), expressions: expressions.clone() })?))
        }
        RelationNode::Scan(scan) => {
            let mut scan = scan.clone();
            let columns = predicate.referenced_columns();
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
    fn visit(expression: &mut Expr, columns: &BTreeMap<ColumnId, (ColumnId, rtdb_types::LogicalType)>) {
        match expression {
            Expr::Column { id, data_type } => if let Some((replacement, ty)) = columns.get(id) { *id = *replacement; *data_type = ty.clone(); },
            Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::Field { expr, .. } => visit(expr, columns),
            Expr::RegexReplace { source,.. } => visit(source,columns),
            Expr::Binary { left, right, .. } => { visit(left, columns); visit(right, columns); }
            Expr::Call { args, .. } | Expr::Coalesce(args) | Expr::List(args) => for arg in args { visit(arg, columns); },
            Expr::Between { operand,low,high,.. } => { visit(operand,columns);visit(low,columns);visit(high,columns); }
            Expr::SimpleCase { operand, branches, otherwise } => { visit(operand, columns); for (candidate, value) in branches { visit(candidate, columns); visit(value, columns); } visit(otherwise, columns); }
            Expr::Case { branches, otherwise } => { for (condition, value) in branches { visit(condition, columns); visit(value, columns); } visit(otherwise, columns); }
            Expr::Subscript { expr, index, .. } => { visit(expr, columns); visit(index, columns); }
            Expr::Struct(fields) => for (_, expr) in fields { visit(expr, columns); },
            Expr::RowCopy(fields) => for field in fields { if let Some((id, ty)) = columns.get(&field.id) { field.id = *id; field.data_type = ty.clone(); } },
            Expr::In { expr, values, .. } => { visit(expr, columns); for value in values { visit(value, columns); } }
            Expr::Like { expr, pattern, .. } => { visit(expr, columns); visit(pattern, columns); }
            Expr::Literal(_) | Expr::Parameter { .. } | Expr::Correlated { .. } => {}
        }
    }
    visit(&mut expression, columns); expression
}
