use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_plan::{BinaryOp, ColumnId, Expr, NamedExpr, Relation, RelationNode, Schema, WindowFunction};
use rtdb_types::Result;

pub(crate) fn prepare(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    let required = relation.schema().ids();
    annotate_top_k(prune(relation, &required)?)
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
            if !scan.predicates.iter().any(|predicate| has_match_on(predicate, *id)) { return Ok(None); }
            let mut scan = scan.clone();
            scan.top_k = Some(rtdb_plan::ScanTopK { column: *id, query: query.clone(), limit: limit.clone() });
            return Ok(Some(Arc::new(Relation::scan(scan)?)));
        }
        _ => return Ok(None),
    };
    Ok(Some(Arc::new(Relation::new(relation.schema().clone(), node)?)))
}

fn has_match_on(predicate: &Expr, column: ColumnId) -> bool {
    match predicate {
        Expr::Binary { op: BinaryOp::And, left, right } => has_match_on(left, column) || has_match_on(right, column),
        Expr::Call { name, args, .. } => name.eq_ignore_ascii_case("match") && matches!(args.first(), Some(Expr::Column { id, .. }) if *id == column),
        _ => false,
    }
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
            select(input, predicate.clone())
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
        RelationNode::Join { left, right, kind, condition } => {
            let left = prepare(Arc::clone(left))?; let right = prepare(Arc::clone(right))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Join { left, right, kind: *kind, condition: condition.clone() })?))
        }
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            let input = prepare(Arc::clone(input))?; let dependent = prepare(Arc::clone(dependent))?;
            Ok(Arc::new(Relation::new(relation.schema().clone(), RelationNode::Mark { input, dependent, arguments: arguments.clone(), condition: condition.clone(), comparison: comparison.clone(), output: output.clone() })?))
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
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None,
        }).unwrap())
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
