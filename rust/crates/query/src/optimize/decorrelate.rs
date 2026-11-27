//! Rewrites applies into joins where the dependent plan's correlation lifts into a join condition, porting the
//! decorrelation rules of the reference's logical plan (`xtdb.logical-plan`).
//!
//! Every rewrite keeps the schema of the relation it replaces.
//! A rule answers `Ok(None)` where it does not apply, which keeps the apply; an error building a rewrite fails the plan.

use std::sync::Arc;
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, CastTarget, ColumnId, Expr, Field, JoinKind, MaterializedBinding, NamedExpr, OrdinalityScope, PercentileFraction, Relation, RelationNode, Schema, UnaryOp};
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, Value};
use super::{Ids, with_children};

pub(super) fn decorrelate(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || decorrelate_at(relation, ids))
}

fn decorrelate_at(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    match relation.node() {
        // The selection is seen before the apply or mark beneath it, which it may turn into a semi- or anti-join.
        RelationNode::Filter { input, predicate } if selects_through(input) && !predicate.is_volatile() =>
            filter_over(Arc::clone(input), conjuncts(predicate), ids),
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = decorrelate(Arc::clone(input), ids)?;
            let dependent = decorrelate(Arc::clone(dependent), ids)?;
            apply(input, dependent, arguments, *kind, output.as_ref(), relation.schema(), ids)
        }
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            let input = decorrelate(Arc::clone(input), ids)?;
            let dependent = decorrelate(Arc::clone(dependent), ids)?;
            decorrelate_mark(input, dependent, arguments, condition.as_ref(), comparison, output, relation.schema())
        }
        _ => with_children(&relation, |child| decorrelate(Arc::clone(child), ids)),
    }
}

fn selects_through(input: &Relation) -> bool { matches!(input.node(), RelationNode::Apply { .. } | RelationNode::Mark { .. }) }

/// The parameters an apply binds from its input, and what each is bound to.
struct Correlation<'a> { arguments: &'a [(usize, Expr)] }

impl Correlation<'_> {
    fn binds(&self, index: usize) -> bool { self.arguments.iter().any(|(bound, _)| *bound == index) }

    fn in_expression(&self, expression: &Expr) -> bool {
        let mut found = false;
        expression.visit(&mut |expression| if let Expr::Parameter { index, .. } = expression { found |= self.binds(*index); });
        found
    }

    fn references(&self, relation: &Relation) -> bool {
        let mut pending = vec![relation];
        while let Some(relation) = pending.pop() {
            let mut found = false;
            each_expression(relation, &mut |expression| found |= self.in_expression(expression));
            if let RelationNode::Aggregate { aggregates, .. } = relation.node() { found |= self.in_fractions(aggregates); }
            if found { return true; }
            pending.extend(relation.children().into_iter().map(Arc::as_ref));
        }
        false
    }

    /// Whether a percentile reads its fraction from a bound parameter, which no expression can replace.
    fn in_fractions(&self, aggregates: &[AggregateExpr]) -> bool {
        aggregates.iter().any(|aggregate| matches!(&aggregate.function,
            AggregateFunction::Percentile { fraction: PercentileFraction::Parameter { index }, .. } if self.binds(*index)))
    }

    /// Replaces each bound parameter with the input expression it is bound to.
    fn substitute(&self, mut expression: Expr) -> Expr {
        expression.rewrite(&mut |expression| {
            let Expr::Parameter { index, .. } = expression else { return };
            let index = *index;
            if let Some((_, argument)) = self.arguments.iter().find(|(bound, _)| *bound == index) { *expression = argument.clone(); }
        });
        expression
    }
}

/// Every expression a relation evaluates, including those the optimizer has already moved into a scan.
fn each_expression(relation: &Relation, f: &mut impl FnMut(&Expr)) {
    for expression in relation.expressions() { f(expression); }
    let RelationNode::Scan(scan) = relation.node() else { return };
    for evaluated in &scan.matches { f(&evaluated.query); }
    if let Some(top_k) = &scan.top_k {
        f(&top_k.query);
        f(&top_k.limit);
        if let Some(offset) = &top_k.offset { f(offset); }
        if let Some(corpus) = &top_k.corpus { for expression in corpus.iter() { f(expression); } }
    }
    if let Some(ordered) = &scan.ordered_top_k {
        for order in &ordered.order { f(&order.expr); }
        for residual in &ordered.residuals { f(residual); }
    }
}

/// Whether the dependent plan may be evaluated once rather than per input row: a volatile call would give each row its
/// own value, and an outer reference counts the applies around it.
fn movable(relation: &Relation) -> bool {
    let mut pending = vec![relation];
    while let Some(relation) = pending.pop() {
        let mut fixed = false;
        each_expression(relation, &mut |expression| {
            fixed |= expression.is_volatile();
            expression.visit(&mut |expression| fixed |= matches!(expression, Expr::Correlated { .. }));
        });
        if fixed { return false; }
        pending.extend(relation.children().into_iter().map(Arc::as_ref));
    }
    true
}

/// Row numbering counts the rows of the relation it is evaluated over, which a lifted projection would change.
fn numbers_rows(expression: &Expr) -> bool {
    let mut found = false;
    expression.visit(&mut |expression| if let Expr::Call { name, .. } = expression {
        found |= ["row-number", "row_number", "local-row-number", "local_row_number"].iter().any(|numbering| name.eq_ignore_ascii_case(numbering));
    });
    found
}

/// The apply rebuilt over its rewritten children, as a join where one of the reference's rules applies.
fn apply(input: Arc<Relation>, dependent: Arc<Relation>, arguments: &[(usize, Expr)], kind: ApplyKind, output: Option<&Field>, schema: &Schema, ids: &mut Ids) -> Result<Arc<Relation>> {
    if let Some(rewritten) = rewrite(&input, &dependent, arguments, kind, output, schema, ids)? { return Ok(rewritten); }
    Ok(Arc::new(Relation::new(schema.clone(), RelationNode::Apply { input, dependent, arguments: arguments.to_vec(), kind, output: output.cloned() })?))
}

/// The apply as a join, where one of the reference's rules applies.
fn rewrite(input: &Arc<Relation>, dependent: &Arc<Relation>, arguments: &[(usize, Expr)], kind: ApplyKind, output: Option<&Field>, schema: &Schema, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    if !movable(dependent) { return Ok(None); }
    let correlation = Correlation { arguments };
    if !correlation.references(dependent) { return uncorrelated(input, dependent, kind, output, schema, ids); }
    match (kind, output) {
        (ApplyKind::Inner, _) => {
            if let Some(joined) = lateral(input, dependent, &correlation, kind, schema, ids)? { return Ok(Some(joined)); }
            if let Some(unnested) = column_table(input, dependent, &correlation, false, ids)? { return restore(unnested, schema); }
            if let Some(grouped) = grouped_lateral(input, dependent, &correlation, schema, ids)? { return Ok(Some(grouped)); }
            if let Some(grouped) = ungrouped_aggregate(input, dependent, &correlation, kind, None, schema, ids)? { return Ok(Some(grouped)); }
            peel_inner(input, dependent, &correlation, schema, ids)
        }
        (ApplyKind::Left, _) => {
            if let Some(joined) = lateral(input, dependent, &correlation, kind, schema, ids)? { return Ok(Some(joined)); }
            if let Some(unnested) = column_table(input, dependent, &correlation, true, ids)? { return restore(unnested, schema); }
            ungrouped_aggregate(input, dependent, &correlation, kind, None, schema, ids)
        }
        (ApplyKind::Single, _) => {
            if let Some(grouped) = ungrouped_aggregate(input, dependent, &correlation, kind, None, schema, ids)? { return Ok(Some(grouped)); }
            lateral(input, dependent, &correlation, kind, schema, ids)
        }
        (ApplyKind::Semi | ApplyKind::Anti, _) => lateral(input, dependent, &correlation, kind, schema, ids),
        (ApplyKind::Scalar, Some(output)) => match ungrouped_aggregate(input, dependent, &correlation, kind, Some(output), schema, ids)? {
            Some(grouped) => Ok(Some(grouped)),
            None => scalar_single(input, dependent, &correlation, output, ids),
        },
        (ApplyKind::Exists, Some(output)) => exists_mark(input, dependent, &correlation, output, ids),
        (ApplyKind::Mark, Some(output)) => apply_mark(input, dependent, &correlation, output),
        _ => Ok(None),
    }
}

/// Rule 1: `R A⊗ E = R ⊗ E` where E reads no parameter bound from R.
/// A scalar apply becomes the single join the binder plans for an uncorrelated scalar subquery, which keeps its cardinality check.
fn uncorrelated(input: &Arc<Relation>, dependent: &Arc<Relation>, kind: ApplyKind, output: Option<&Field>, schema: &Schema, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    if !matches!(kind, ApplyKind::Scalar) && !input.schema().ids().is_disjoint(&dependent.schema().ids()) { return Ok(None); }
    let joined = match (kind, output) {
        (ApplyKind::Inner, _) => Arc::new(Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Cross, None)?),
        (ApplyKind::Left, _) => Arc::new(Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Left, None)?),
        (ApplyKind::Single, _) => Arc::new(Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Single, None)?),
        (ApplyKind::Semi, _) => Arc::new(Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Semi, None)?),
        (ApplyKind::Anti, _) => Arc::new(Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Anti, None)?),
        (ApplyKind::Scalar, Some(output)) => {
            let [source] = dependent.schema().fields() else { return Ok(None) };
            let field = Field { data_type: source.data_type.clone(), ..output.clone() };
            let right = Arc::new(Arc::clone(dependent).project(vec![NamedExpr { field, expr: source.expression() }])?);
            Arc::new(Relation::join(Arc::clone(input), right, JoinKind::Single, None)?)
        }
        (ApplyKind::Mark, Some(output)) => {
            let [source] = dependent.schema().fields() else { return Ok(None) };
            let comparison = source.expression();
            if !is_boolean_or_null(&comparison.data_type()) { return Ok(None); }
            let mark_schema = input.schema().concat(&Schema::new(vec![output.clone()])?)?;
            Arc::new(Relation::new(mark_schema, RelationNode::Mark {
                input: Arc::clone(input), dependent: Arc::clone(dependent), arguments: Vec::new(),
                condition: None, comparison, output: output.clone(),
            })?)
        }
        (ApplyKind::Exists, Some(output)) => {
            let mark = hidden(ids.next()?, "_mark", LogicalType::Scalar(ScalarType::Bool).maybe());
            let mark_schema = input.schema().concat(&Schema::new(vec![mark.clone()])?)?;
            let marked = Arc::new(Relation::new(mark_schema, RelationNode::Mark {
                input: Arc::clone(input), dependent: Arc::clone(dependent), arguments: Vec::new(),
                condition: None, comparison: Expr::Literal(Value::Bool(true)), output: mark.clone(),
            })?);
            let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                .chain([NamedExpr { field: output.clone(), expr: Expr::unary(UnaryOp::IsTrue, mark.expression()) }]).collect();
            Arc::new(marked.project(expressions)?)
        }
        _ => return Ok(None),
    };
    restore(joined, schema)
}

/// Rule 2: `R A⊗ (σp E) = R ⊗p E` where E reads no parameter bound from R, for the relational applies.
/// Projections the correlated selection was beneath are evaluated over the join, as rule 4 does for a cross apply; for a
/// left or single join, computed columns stay null on rows the join supplies nulls for.
fn lateral(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, kind: ApplyKind, schema: &Schema, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let Some(lifted) = lift(dependent, correlation, matches!(kind, ApplyKind::Semi | ApplyKind::Anti))? else { return Ok(None) };
    if !input.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let mut conditions: Vec<Expr> = lifted.filters.into_iter().flat_map(|filter| conjuncts(&correlation.substitute(filter))).collect();
    // A computed column the dependent does not return was read only by selections, into which it is already inlined, as
    // the reference's `push-decorrelated-predicate-down-past-period-constructor` inlines a period into its predicate.
    let computed: Vec<NamedExpr> = lifted.computed.into_iter().filter(|named| dependent.schema().get(named.field.id).is_some()).collect();
    let join_kind = match kind {
        ApplyKind::Inner if conditions.is_empty() => JoinKind::Cross,
        ApplyKind::Inner => JoinKind::Inner,
        ApplyKind::Left if !conditions.is_empty() => JoinKind::Left,
        ApplyKind::Single if !conditions.is_empty() => JoinKind::Single,
        ApplyKind::Semi if !conditions.is_empty() => JoinKind::Semi,
        ApplyKind::Anti if !conditions.is_empty() => JoinKind::Anti,
        _ => return Ok(None),
    };
    let outer_join = matches!(join_kind, JoinKind::Left | JoinKind::Single);
    let base = if outer_join {
        exclude_null_keys(input.schema(), lifted.base, &mut conditions)?
    } else { lifted.base };
    let substituted: Vec<(Field, Expr)> = computed.into_iter()
        .map(|named| (named.field, correlation.substitute(named.expr))).collect();
    let needs_marker = outer_join && substituted.iter().any(|(_, expr)| {
        !matches!(expr, Expr::Column { id, .. } if base.schema().get(*id).is_some())
    });
    let (right, matched) = if needs_marker {
        let matched = hidden(ids.next()?, "_dep_countable", LogicalType::Scalar(ScalarType::Bool));
        (Arc::new(base.extend(vec![NamedExpr { field: matched.clone(), expr: Expr::Literal(Value::Bool(true)) }])?), Some(matched))
    } else { (base, None) };
    let joined = Arc::new(Relation::join(Arc::clone(input), right, join_kind, conjunction(conditions))?);
    let rows = if matches!(join_kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Left | JoinKind::Single) && !substituted.is_empty() {
        let matched_expr = matched.as_ref().and_then(|field| joined.schema().get(field.id)).map(Field::expression);
        let extended = substituted.into_iter().map(|(field, expr)| {
            let expr = refresh_types(expr, joined.schema());
            let (field, expr) = if outer_join {
                let field = Field { data_type: field.data_type.maybe(), ..field };
                let expr = match &matched_expr {
                    Some(matched_expr) => Expr::Case {
                        branches: vec![(Expr::unary(UnaryOp::IsNull, matched_expr.clone()), Expr::Literal(Value::Null))],
                        otherwise: Box::new(expr),
                    },
                    None => expr,
                };
                (field, expr)
            } else { (field, expr) };
            NamedExpr { field, expr }
        }).collect();
        Arc::new(joined.extend(extended)?)
    } else { joined };
    restore(rows, schema)
}

/// Peels any stack of projections, renames and extensions over an `Aggregate`, inlining them onto the aggregate's columns.
fn peel_projections(relation: &Arc<Relation>) -> Option<(Vec<NamedExpr>, &Arc<Relation>)> {
    match relation.node() {
        RelationNode::Aggregate { .. } => Some((
            relation.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect(),
            relation,
        )),
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            if expressions.iter().any(|named| numbers_rows(&named.expr)) { return None; }
            let (inner, aggregate) = peel_projections(input)?;
            let mut outputs = Vec::with_capacity(expressions.len());
            for named in expressions {
                outputs.push(NamedExpr { field: named.field.clone(), expr: inline(named.expr.clone(), &inner)? });
            }
            Some((outputs, aggregate))
        }
        RelationNode::Extend { input, expressions } => {
            if expressions.iter().any(|named| numbers_rows(&named.expr)) { return None; }
            let (mut inner, aggregate) = peel_projections(input)?;
            let mut added = Vec::with_capacity(expressions.len());
            for named in expressions {
                if inner.iter().any(|existing| existing.field.id == named.field.id) { return None; }
                added.push(NamedExpr { field: named.field.clone(), expr: inline(named.expr.clone(), &inner)? });
            }
            inner.extend(added);
            Some((inner, aggregate))
        }
        _ => None,
    }
}

/// Rule 9: `R A⊗ (G F1 E) = G columns(R),F' (R A⟕ E)`, for a scalar, single, inner or left apply over an aggregate
/// without groups. The ordinality the input is numbered with is the key the rule requires of R, so duplicate input rows
/// stay apart; every aggregate counts only the rows matched for that input row, so unmatched rows aggregate as the empty
/// input the apply would have given them.
fn ungrouped_aggregate(
    input: &Arc<Relation>,
    dependent: &Arc<Relation>,
    correlation: &Correlation<'_>,
    kind: ApplyKind,
    output: Option<&Field>,
    schema: &Schema,
    ids: &mut Ids,
) -> Result<Option<Arc<Relation>>> {
    let Some((outputs, aggregate)) = peel_projections(dependent) else { return Ok(None) };
    let RelationNode::Aggregate { input: source, groups, aggregates } = aggregate.node() else { return Ok(None) };
    if !groups.is_empty() || !aggregates.iter().all(empty_group_safe) || correlation.in_fractions(aggregates) { return Ok(None); }
    // When the aggregate itself reads no bound parameter (only an outer projection does), compute it once and join.
    if !correlation.references(aggregate) {
        if !input.schema().ids().is_disjoint(&aggregate.schema().ids()) { return Ok(None); }
        let join_kind = if kind == ApplyKind::Inner { JoinKind::Cross } else { JoinKind::Single };
        let joined = Arc::new(Relation::join(Arc::clone(input), Arc::clone(aggregate), join_kind, None)?);
        let carried = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() });
        let widen = |expr: Expr, target: &LogicalType| -> Option<Expr> {
            if expr.data_type() == *target { Some(expr) }
            else if expr.data_type().maybe() == *target {
                Some(Expr::Case {
                    branches: vec![(Expr::Literal(Value::Bool(false)), Expr::Literal(Value::Null))],
                    otherwise: Box::new(expr),
                })
            } else { None }
        };
        let expressions: Vec<NamedExpr> = match (kind, output) {
            (ApplyKind::Scalar, Some(output)) => {
                let [only] = outputs.as_slice() else { return Ok(None) };
                let Some(expr) = widen(refresh_types(correlation.substitute(only.expr.clone()), joined.schema()), &output.data_type) else {
                    return Ok(None);
                };
                carried.chain([NamedExpr { field: output.clone(), expr }]).collect()
            }
            (ApplyKind::Inner | ApplyKind::Left | ApplyKind::Single, None) => {
                let mut projected = Vec::with_capacity(input.schema().len() + outputs.len());
                projected.extend(carried);
                for named in outputs {
                    let Some(target) = schema.get(named.field.id) else { return Ok(None) };
                    let Some(expr) = widen(refresh_types(correlation.substitute(named.expr), joined.schema()), &target.data_type) else {
                        return Ok(None);
                    };
                    projected.push(NamedExpr { field: target.clone(), expr });
                }
                projected
            }
            _ => return Ok(None),
        };
        return restore(Arc::new(joined.project(expressions)?), schema);
    }
    if input.schema().fields().iter().any(|field| rtdb_plan::validate_equality_types(&field.data_type, &field.data_type).is_err()) { return Ok(None); }
    let number = hidden(ids.next()?, "_row_number", LogicalType::Scalar(ScalarType::I64));
    let numbered = Arc::new(Arc::clone(input).with_ordinality(number.clone(), OrdinalityScope::Relation)?);
    let direct_groups = input.schema().fields().iter().all(|field| reflexive_key(&field.data_type));
    let (binding, source_input) = if direct_groups {
        (None, numbered)
    } else {
        let binding = Arc::new(MaterializedBinding { name: format!("_decorrelate_{}", number.id.0), relation: numbered });
        let reference = Arc::new(Relation::new(binding.relation.schema().clone(), RelationNode::Reference { binding: Arc::clone(&binding) })?);
        (Some(binding), reference)
    };
    let (rows, present) = if let Some(lifted) = lift(source, correlation, false)? {
        if !source_input.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
        let mut conditions: Vec<Expr> = lifted.filters.into_iter().flat_map(|filter| conjuncts(&correlation.substitute(filter))).collect();
        let base = exclude_null_keys(source_input.schema(), lifted.base, &mut conditions)?;
        let matched = hidden(ids.next()?, "_dep_countable", LogicalType::Scalar(ScalarType::Bool));
        let marked = Arc::new(base.extend(vec![NamedExpr { field: matched.clone(), expr: Expr::Literal(Value::Bool(true)) }])?);
        let joined = Arc::new(Relation::join(source_input, marked, JoinKind::Left, conjunction(conditions))?);
        let matched_expr = joined.schema().get(matched.id).expect("joined schema holds _dep_countable").expression();
        let unmatched = Expr::unary(UnaryOp::IsNull, matched_expr.clone());
        let rows = if lifted.computed.is_empty() { joined } else {
            let computed = lifted.computed.into_iter().map(|named| NamedExpr {
                field: Field { data_type: named.field.data_type.maybe(), ..named.field },
                expr: Expr::Case {
                    branches: vec![(unmatched.clone(), Expr::Literal(Value::Null))],
                    otherwise: Box::new(refresh_types(correlation.substitute(named.expr), joined.schema())),
                },
            }).collect();
            Arc::new(joined.extend(computed)?)
        };
        (rows, Expr::unary(UnaryOp::IsNotNull, matched_expr))
    } else if let Some((rows, present)) = outer_column_table(&source_input, source, correlation, ids)? {
        (rows, present)
    } else {
        return Ok(None);
    };
    let aggregates: Vec<AggregateExpr> = aggregates.iter().map(|aggregate| AggregateExpr {
        argument: aggregate.argument.clone().map(|argument| refresh_types(argument, rows.schema())),
        filter: Some(match &aggregate.filter {
            Some(filter) => Expr::binary(BinaryOp::And, present.clone(), refresh_types(correlation.substitute(filter.clone()), rows.schema())),
            None => present.clone(),
        }),
        ..aggregate.clone()
    }).collect();
    let (grouped, binding) = match binding {
        None => {
            let groups: Vec<NamedExpr> = input.schema().fields().iter().chain([&number]).map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
            let grouped_schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
            (Arc::new(Relation::new(grouped_schema, RelationNode::Aggregate { input: rows, groups, aggregates })?), None)
        }
        Some(binding) => {
            let key = hidden(ids.next()?, "_row_key", LogicalType::Scalar(ScalarType::I64));
            let groups = vec![NamedExpr { field: key.clone(), expr: number.expression() }];
            let grouped_schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
            let grouped = Arc::new(Relation::new(grouped_schema, RelationNode::Aggregate { input: rows, groups, aggregates })?);
            let outer_ref = Arc::new(Relation::new(binding.relation.schema().clone(), RelationNode::Reference { binding: Arc::clone(&binding) })?);
            let rejoined = Arc::new(Relation::join(outer_ref, grouped, JoinKind::Inner, Some(Expr::binary(BinaryOp::Eq, number.expression(), key.expression())))?);
            (rejoined, Some(binding))
        }
    };
    let carried = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() });
    let nullable_expr = |expr: Expr, target: &LogicalType| {
        if expr.data_type() == *target { expr } else {
            // The ordinality is never null; the CASE widens the expression's type to nullable when the apply's output is.
            Expr::Case { branches: vec![(Expr::unary(UnaryOp::IsNull, number.expression()), Expr::Literal(Value::Null))], otherwise: Box::new(expr) }
        }
    };
    let expressions: Vec<NamedExpr> = match (kind, output) {
        (ApplyKind::Scalar, Some(output)) => {
            let [only] = outputs.as_slice() else { return Ok(None) };
            let value = nullable_expr(correlation.substitute(only.expr.clone()), &output.data_type);
            carried.chain([NamedExpr { field: output.clone(), expr: value }]).collect()
        }
        (ApplyKind::Inner | ApplyKind::Left | ApplyKind::Single, None) => {
            let mut projected = Vec::with_capacity(input.schema().len() + outputs.len());
            projected.extend(carried);
            for named in outputs {
                let Some(target) = schema.get(named.field.id) else { return Ok(None) };
                let expr = nullable_expr(correlation.substitute(named.expr), &target.data_type);
                projected.push(NamedExpr { field: target.clone(), expr });
            }
            projected
        }
        _ => return Ok(None),
    };
    let projected = Arc::new(grouped.project(expressions)?);
    let result = match binding {
        Some(binding) => Arc::new(Relation::new(projected.schema().clone(), RelationNode::Let { binding, body: projected })?),
        None => projected,
    };
    restore(result, schema)
}

/// Unnests a correlated `ColumnTable` with `outer: true` for rule 9, returning the rows and the predicate selecting
/// actual elements (excluding empty/null lists and rows rejected by any filters above the table).
fn outer_column_table(input: &Arc<Relation>, source: &Arc<Relation>, correlation: &Correlation<'_>, ids: &mut Ids) -> Result<Option<(Arc<Relation>, Expr)>> {
    let mut filters = Vec::new();
    let mut computed: Vec<NamedExpr> = Vec::new();
    let mut current = source;
    let mut layers = Vec::new();
    loop {
        match current.node() {
            RelationNode::Filter { input: child, predicate } => { layers.push((Some(predicate), None)); current = child; }
            RelationNode::Project { input: child, expressions } | RelationNode::Rename { input: child, expressions, .. } | RelationNode::Extend { input: child, expressions } => {
                if expressions.iter().any(|named| numbers_rows(&named.expr)) { return Ok(None); }
                layers.push((None, Some(expressions)));
                current = child;
            }
            _ => break,
        }
    }
    let (expr, element, ordinality) = match current.node() {
        RelationNode::ColumnTable { expr, element } => (expr, element, None),
        RelationNode::Ordinality { input: table, field, scope: OrdinalityScope::Page } => match table.node() {
            RelationNode::ColumnTable { expr, element } => (expr, element, Some(field)),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    if !input.schema().ids().is_disjoint(&current.schema().ids()) { return Ok(None); }
    let list = correlation.substitute(expr.clone());
    let staged = hidden(ids.next()?, "_unnest", list.data_type());
    let extended = Arc::new(Arc::clone(input).extend(vec![NamedExpr { field: staged.clone(), expr: list }])?);
    let position = hidden(ids.next()?, "_ordinal", LogicalType::Scalar(ScalarType::I32).maybe());
    let nullable_element = Field { data_type: element.data_type.clone().maybe(), ..element.clone() };
    let fields = extended.schema().fields().iter().cloned().chain([nullable_element.clone(), position.clone()]).collect();
    let mut rows = Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Unnest {
        input: extended, expr: staged.expression(), element: nullable_element, ordinality: Some(position.clone()), outer: true,
    })?);
    if let Some(ordinality) = ordinality {
        let field = Field { data_type: ordinality.data_type.clone().maybe(), ..ordinality.clone() };
        let cast = Expr::Cast { expr: Box::new(position.expression()), target: CastTarget::Fixed(field.data_type.clone()) };
        rows = Arc::new(rows.extend(vec![NamedExpr { field, expr: cast }])?);
    }
    let base_ids = rows.schema().ids();
    for (predicate, expressions) in layers.into_iter().rev() {
        if let Some(predicate) = predicate {
            let Some(predicate) = inline(predicate.clone(), &computed) else { return Ok(None) };
            filters.extend(conjuncts(&predicate));
        }
        if let Some(expressions) = expressions {
            let mut added = Vec::new();
            for named in expressions {
                if matches!(&named.expr, Expr::Column { id, .. } if *id == named.field.id) { continue; }
                if base_ids.contains(&named.field.id) || computed.iter().any(|existing| existing.field.id == named.field.id) { return Ok(None); }
                let Some(expr) = inline(named.expr.clone(), &computed) else { return Ok(None) };
                added.push(NamedExpr { field: named.field.clone(), expr });
            }
            computed.extend(added);
        }
    }
    let missing = Expr::unary(UnaryOp::IsNull, position.expression());
    if !computed.is_empty() {
        let extended = computed.into_iter().map(|named| NamedExpr {
            field: Field { data_type: named.field.data_type.maybe(), ..named.field },
            expr: Expr::Case {
                branches: vec![(missing.clone(), Expr::Literal(Value::Null))],
                otherwise: Box::new(refresh_types(correlation.substitute(named.expr), rows.schema())),
            },
        }).collect();
        rows = Arc::new(rows.extend(extended)?);
    }
    let mut present = Expr::unary(UnaryOp::IsNotNull, position.expression());
    if let Some(filter) = conjunction(filters.into_iter().map(|filter| refresh_types(correlation.substitute(filter), rows.schema())).collect()) {
        let guarded = Expr::Case { branches: vec![(missing, Expr::Literal(Value::Bool(false)))], otherwise: Box::new(filter) };
        present = Expr::binary(BinaryOp::And, present, guarded);
    }
    Ok(Some((rows, present)))
}

/// Rule 2 for a scalar subquery: `R A_scalar (π v (σp E)) = π columns(R),v (R ⟕¹p E)` where E reads no parameter bound
/// from R, as the reference's single-join apply becomes a single join.
/// The single join rejects an input row that more than one row of E matches, as the apply's cardinality check does.
fn scalar_single(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let [source] = dependent.schema().fields() else { return Ok(None) };
    let Some(lifted) = lift(dependent, correlation, false)? else { return Ok(None) };
    if !input.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let mut conditions: Vec<Expr> = lifted.filters.into_iter().flat_map(|filter| conjuncts(&correlation.substitute(filter))).collect();
    if conditions.is_empty() { return Ok(None); }
    let base = exclude_null_keys(input.schema(), lifted.base, &mut conditions)?;
    let computed = lifted.computed.into_iter().find(|named| named.field.id == source.id).map(|named| correlation.substitute(named.expr));
    let (right, matched) = match &computed {
        // Over the rows the join supplies nulls for, a value computed from the outer row or an expression must stay null,
        // as the apply's empty result is; only a direct column of the right input is already null on unmatched rows.
        Some(value) if !matches!(value, Expr::Column { id, .. } if base.schema().get(*id).is_some()) => {
            let matched = hidden(ids.next()?, "_dep_countable", LogicalType::Scalar(ScalarType::Bool));
            (Arc::new(base.extend(vec![NamedExpr { field: matched.clone(), expr: Expr::Literal(Value::Bool(true)) }])?), Some(matched))
        }
        _ => (base, None),
    };
    let joined = Arc::new(Relation::join(Arc::clone(input), right, JoinKind::Single, conjunction(conditions))?);
    let matched_expr = matched.as_ref().and_then(|field| joined.schema().get(field.id)).map(Field::expression);
    let value = match (computed, matched_expr) {
        (Some(value), Some(matched_expr)) => Expr::Case {
            branches: vec![(Expr::unary(UnaryOp::IsNull, matched_expr), Expr::Literal(Value::Null))],
            otherwise: Box::new(refresh_types(value, joined.schema())),
        },
        (Some(value), None) => refresh_types(value, joined.schema()),
        (None, _) => match joined.schema().get(source.id) { Some(field) => field.expression(), None => return Ok(None) },
    };
    let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
        .chain([NamedExpr { field: output.clone(), expr: value }]).collect();
    Ok(Some(Arc::new(joined.project(expressions)?)))
}

/// Rule 2 for a standalone `EXISTS` apply (e.g. projected in SELECT or inside a disjunction): turns `R A_exists (σp E)`
/// into a shared `Mark` join whose nullable mark is tested with `IS TRUE` to keep the non-nullable Boolean output.
fn exists_mark(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let Some(lifted) = lift(dependent, correlation, true)? else { return Ok(None) };
    if !input.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let mut conditions: Vec<Expr> = lifted.filters.into_iter().flat_map(|filter| conjuncts(&correlation.substitute(filter))).collect();
    let base = push_right_filters(lifted.base, &mut conditions)?;
    let condition = conjunction(conditions);
    if condition.as_ref().is_some_and(|predicate| !is_boolean_or_null(&predicate.data_type())) { return Ok(None); }
    let mark = hidden(ids.next()?, "_mark", LogicalType::Scalar(ScalarType::Bool).maybe());
    let mark_schema = input.schema().concat(&Schema::new(vec![mark.clone()])?)?;
    let marked = Arc::new(Relation::new(mark_schema, RelationNode::Mark {
        input: Arc::clone(input), dependent: base, arguments: Vec::new(),
        condition, comparison: Expr::Literal(Value::Bool(true)), output: mark.clone(),
    })?);
    let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
        .chain([NamedExpr { field: output.clone(), expr: Expr::unary(UnaryOp::IsTrue, mark.expression()) }]).collect();
    Ok(Some(Arc::new(marked.project(expressions)?)))
}

/// Rule 2 for a standalone `Mark` apply: turns `R A_mark (π c (σp E))` into an uncorrelated `Mark` join.
fn apply_mark(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field) -> Result<Option<Arc<Relation>>> {
    let [source] = dependent.schema().fields() else { return Ok(None) };
    let Some(lifted) = lift(dependent, correlation, false)? else { return Ok(None) };
    if !input.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let comparison = match lifted.computed.iter().find(|named| named.field.id == source.id) {
        Some(named) => correlation.substitute(named.expr.clone()),
        None => match lifted.base.schema().get(source.id) { Some(field) => field.expression(), None => return Ok(None) },
    };
    let mut conditions: Vec<Expr> = lifted.filters.into_iter().flat_map(|filter| conjuncts(&correlation.substitute(filter))).collect();
    let base = push_right_filters(lifted.base, &mut conditions)?;
    let condition = conjunction(conditions);
    if !is_boolean_or_null(&comparison.data_type()) || condition.as_ref().is_some_and(|predicate| !is_boolean_or_null(&predicate.data_type())) {
        return Ok(None);
    }
    let mark_schema = input.schema().concat(&Schema::new(vec![output.clone()])?)?;
    Ok(Some(Arc::new(Relation::new(mark_schema, RelationNode::Mark {
        input: Arc::clone(input), dependent: base, arguments: Vec::new(),
        condition, comparison, output: output.clone(),
    })?)))
}

/// Lifts bound parameters out of a correlated `Mark` relation so it runs as a shared mark join instead of per input row.
fn decorrelate_mark(
    input: Arc<Relation>,
    dependent: Arc<Relation>,
    arguments: &[(usize, Expr)],
    condition: Option<&Expr>,
    comparison: &Expr,
    output: &Field,
    schema: &Schema,
) -> Result<Arc<Relation>> {
    if !arguments.is_empty() && movable(&dependent) {
        let correlation = Correlation { arguments };
        if let Some(lifted) = lift(&dependent, &correlation, false)?
            && input.schema().ids().is_disjoint(&lifted.base.schema().ids())
        {
            let computed: Vec<NamedExpr> = lifted.computed.into_iter()
                .map(|named| NamedExpr { field: named.field, expr: correlation.substitute(named.expr) }).collect();
            if let Some(new_comparison) = inline(comparison.clone(), &computed)
                && let Some(inlined_condition) = match condition {
                    Some(predicate) => inline(predicate.clone(), &computed).map(Some),
                    None => Some(None),
                }
            {
                let mut conditions: Vec<Expr> = lifted.filters.into_iter()
                    .flat_map(|filter| conjuncts(&correlation.substitute(filter)))
                    .chain(inlined_condition.into_iter().flat_map(|predicate| conjuncts(&predicate)))
                    .collect();
                let base = push_right_filters(lifted.base, &mut conditions)?;
                let new_condition = conjunction(conditions);
                if is_boolean_or_null(&new_comparison.data_type()) && new_condition.as_ref().is_none_or(|predicate| is_boolean_or_null(&predicate.data_type())) {
                    return Ok(Arc::new(Relation::new(schema.clone(), RelationNode::Mark {
                        input, dependent: base, arguments: Vec::new(),
                        condition: new_condition, comparison: new_comparison, output: output.clone(),
                    })?));
                }
            }
        }
    }
    Ok(Arc::new(Relation::new(schema.clone(), RelationNode::Mark {
        input, dependent, arguments: arguments.to_vec(),
        condition: condition.cloned(), comparison: comparison.clone(), output: output.clone(),
    })?))
}

/// Rules 3 and 4 for a cross apply: `R A× (σp E) = σp (R A× E)` and `R A× (πv E) = πv ∪ columns(R) (R A× E)`.
/// Only commits when the inner apply decorrelates (e.g. a HAVING filter or projection stack above a grouped aggregate
/// or column table, or an uncorrelated inner plan).
fn peel_inner(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, schema: &Schema, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    match dependent.node() {
        RelationNode::Filter { input: inner, predicate } => {
            if !input.schema().ids().is_disjoint(&inner.schema().ids()) { return Ok(None); }
            let applied = ApplyKind::Inner.result_schema(input.schema(), inner.schema(), None)?;
            let Some(rows) = rewrite(input, inner, correlation.arguments, ApplyKind::Inner, None, &applied, ids)? else { return Ok(None) };
            restore(Arc::new(rows.filter(correlation.substitute(predicate.clone()))?), schema)
        }
        RelationNode::Project { input: inner, expressions } | RelationNode::Rename { input: inner, expressions, .. } => {
            if expressions.iter().any(|named| numbers_rows(&named.expr)) || !input.schema().ids().is_disjoint(&inner.schema().ids()) {
                return Ok(None);
            }
            let applied = ApplyKind::Inner.result_schema(input.schema(), inner.schema(), None)?;
            let Some(rows) = rewrite(input, inner, correlation.arguments, ApplyKind::Inner, None, &applied, ids)? else { return Ok(None) };
            let projected = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                .chain(expressions.iter().map(|named| NamedExpr { field: named.field.clone(), expr: correlation.substitute(named.expr.clone()) }))
                .collect();
            restore(Arc::new(rows.project(projected)?), schema)
        }
        RelationNode::Extend { input: inner, expressions } => {
            if expressions.iter().any(|named| numbers_rows(&named.expr)) || !input.schema().ids().is_disjoint(&inner.schema().ids()) {
                return Ok(None);
            }
            let applied = ApplyKind::Inner.result_schema(input.schema(), inner.schema(), None)?;
            let Some(rows) = rewrite(input, inner, correlation.arguments, ApplyKind::Inner, None, &applied, ids)? else { return Ok(None) };
            let extended = expressions.iter().map(|named| NamedExpr { field: named.field.clone(), expr: correlation.substitute(named.expr.clone()) }).collect();
            restore(Arc::new(rows.extend(extended)?), schema)
        }
        _ => Ok(None),
    }
}

/// Rule 8: `R A× (G A,F E) = G A ∪ columns(R),F (R A× E)`, for a lateral subquery over an aggregate with groups.
/// The ordinality the input is numbered with keeps duplicate input rows in groups of their own.
fn grouped_lateral(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, schema: &Schema, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let Some((outputs, aggregate)) = peel_projections(dependent) else { return Ok(None) };
    let RelationNode::Aggregate { input: source, groups, aggregates } = aggregate.node() else { return Ok(None) };
    if groups.is_empty() || correlation.in_fractions(aggregates) { return Ok(None); }
    if input.schema().fields().iter().any(|field| rtdb_plan::validate_equality_types(&field.data_type, &field.data_type).is_err()) { return Ok(None); }
    let number = hidden(ids.next()?, "_row_number", LogicalType::Scalar(ScalarType::I64));
    let numbered = Arc::new(Arc::clone(input).with_ordinality(number.clone(), OrdinalityScope::Relation)?);
    let direct_groups = input.schema().fields().iter().all(|field| reflexive_key(&field.data_type));
    let (binding, source_input) = if direct_groups {
        (None, numbered)
    } else {
        let binding = Arc::new(MaterializedBinding { name: format!("_decorrelate_{}", number.id.0), relation: numbered });
        let reference = Arc::new(Relation::new(binding.relation.schema().clone(), RelationNode::Reference { binding: Arc::clone(&binding) })?);
        (Some(binding), reference)
    };
    if !source_input.schema().ids().is_disjoint(&source.schema().ids()) { return Ok(None); }
    let applied = ApplyKind::Inner.result_schema(source_input.schema(), source.schema(), None)?;
    let Some(rows) = rewrite(&source_input, source, correlation.arguments, ApplyKind::Inner, None, &applied, ids)? else { return Ok(None) };
    let carried = || input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() });
    let aggregates: Vec<AggregateExpr> = aggregates.iter().map(|aggregate| AggregateExpr {
        filter: aggregate.filter.clone().map(|filter| correlation.substitute(filter)), ..aggregate.clone()
    }).collect();
    let sub_groups = groups.iter().map(|group| NamedExpr { field: group.field.clone(), expr: correlation.substitute(group.expr.clone()) });
    let (grouped, binding) = match binding {
        None => {
            let groups: Vec<NamedExpr> = carried().chain([NamedExpr { field: number.clone(), expr: number.expression() }]).chain(sub_groups).collect();
            let grouped_schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
            (Arc::new(Relation::new(grouped_schema, RelationNode::Aggregate { input: rows, groups, aggregates })?), None)
        }
        Some(binding) => {
            let key = hidden(ids.next()?, "_row_key", LogicalType::Scalar(ScalarType::I64));
            let groups: Vec<NamedExpr> = std::iter::once(NamedExpr { field: key.clone(), expr: number.expression() }).chain(sub_groups).collect();
            let grouped_schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
            let grouped = Arc::new(Relation::new(grouped_schema, RelationNode::Aggregate { input: rows, groups, aggregates })?);
            let outer_ref = Arc::new(Relation::new(binding.relation.schema().clone(), RelationNode::Reference { binding: Arc::clone(&binding) })?);
            let rejoined = Arc::new(Relation::join(outer_ref, grouped, JoinKind::Inner, Some(Expr::binary(BinaryOp::Eq, number.expression(), key.expression())))?);
            (rejoined, Some(binding))
        }
    };
    let expressions: Vec<NamedExpr> = carried().chain(outputs.into_iter().map(|named| NamedExpr { field: named.field, expr: correlation.substitute(named.expr) })).collect();
    let projected = Arc::new(grouped.project(expressions)?);
    let result = match binding {
        Some(binding) => Arc::new(Relation::new(projected.schema().clone(), RelationNode::Let { binding, body: projected })?),
        None => projected,
    };
    restore(result, schema)
}

/// `apply-table-col->unnest`: `R A× T(l)`, the table of the elements of a list `l` read from R, as `μ l (R)`, with the
/// table's row numbers as the unnest's ordinality. An outer unnest supplies the left apply's unmatched row.
fn column_table(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, outer: bool, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    match dependent.node() {
        RelationNode::Filter { input: table, predicate } => {
            if outer { return filtered_left_unnest(input, table, predicate, correlation, ids); }
            let Some(unnested) = column_table(input, table, correlation, false, ids)? else { return Ok(None) };
            Ok(Some(Arc::new(unnested.filter(correlation.substitute(predicate.clone()))?)))
        }
        RelationNode::Project { input: table, expressions } | RelationNode::Rename { input: table, expressions, .. } => {
            if expressions.iter().any(|named| numbers_rows(&named.expr)
                || outer && !matches!(&named.expr, Expr::Column { .. }))
            {
                return Ok(None);
            }
            let Some(unnested) = column_table(input, table, correlation, outer, ids)? else { return Ok(None) };
            let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                .chain(expressions.iter().map(|named| {
                    let mut field = named.field.clone();
                    if outer { field.data_type = field.data_type.maybe(); }
                    let expr = refresh_types(correlation.substitute(named.expr.clone()), unnested.schema());
                    NamedExpr { field, expr }
                })).collect();
            Ok(Some(Arc::new(unnested.project(expressions)?)))
        }
        RelationNode::ColumnTable { expr, element } => unnest(input, expr, element, None, outer, correlation, ids),
        RelationNode::Ordinality { input: table, field, scope: OrdinalityScope::Page } => match table.node() {
            RelationNode::ColumnTable { expr, element } => unnest(input, expr, element, Some(field), outer, correlation, ids),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

fn filtered_left_unnest(input: &Arc<Relation>, table: &Relation, predicate: &Expr, correlation: &Correlation<'_>, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let (column_table, ordinality) = match table.node() {
        RelationNode::Ordinality { input, field, scope: OrdinalityScope::Page } => (input.as_ref(), Some(field)),
        _ => (table, None),
    };
    let RelationNode::ColumnTable { expr: expr @ Expr::Parameter { index, data_type }, element } = column_table.node() else { return Ok(None) };
    if !correlation.binds(*index) || !input.schema().ids().is_disjoint(&table.schema().ids()) { return Ok(None); }
    if !plain_unnest_list(data_type) { return Ok(None); }
    if !correlation.arguments.iter().all(|(slot, argument)| {
        slot.checked_add(1).is_some_and(|width| std::alloc::Layout::array::<LogicalType>(width).is_ok())
            && matches!(argument, Expr::Column { id, data_type: ty } if input.schema().get(*id).is_some_and(|field| field.data_type == *ty)
            && if slot == index { ty == data_type } else { plain_unnest_scalar(ty) })
    }) || !total_outer_predicate(predicate, correlation) || !prunable_unnest_input(input) {
        return Ok(None);
    }

    // Apply tests the predicate only on actual elements, so only a total predicate can become a list gate.
    let list = Expr::Case {
        branches: vec![(correlation.substitute(predicate.clone()), correlation.substitute(expr.clone()))],
        otherwise: Box::new(Expr::Literal(Value::Null)),
    };
    unnest_rows(Arc::clone(input), list, element, ordinality, true, ids).map(Some)
}

fn plain_unnest_scalar(ty: &LogicalType) -> bool {
    let ty = match ty { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
    matches!(ty, LogicalType::Null | LogicalType::Scalar(ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64))
}

fn plain_unnest_list(ty: &LogicalType) -> bool {
    let ty = match ty { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
    matches!(ty, LogicalType::List { kind: ListKind::List, element }
        if **element == LogicalType::Nothing || plain_unnest_scalar(element))
}

fn total_outer_predicate(predicate: &Expr, correlation: &Correlation<'_>) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match predicate {
        Expr::Literal(Value::Bool(_) | Value::Null) => true,
        Expr::Unary { op: op @ (UnaryOp::IsTrue | UnaryOp::IsFalse | UnaryOp::IsNull | UnaryOp::IsNotNull | UnaryOp::IsUnknown), expr } => {
            let Expr::Parameter { index, data_type } = expr.as_ref() else { return false; };
            if matches!(op, UnaryOp::IsTrue | UnaryOp::IsFalse | UnaryOp::IsUnknown) && !is_boolean_or_null(data_type) { return false; }
            correlation.arguments.iter().any(|(slot, argument)| slot == index && argument.data_type() == *data_type)
        }
        Expr::Unary { op: UnaryOp::Not, expr } => total_outer_predicate(expr, correlation),
        Expr::Binary { op: BinaryOp::And | BinaryOp::Or, left, right } =>
            total_outer_predicate(left, correlation) && total_outer_predicate(right, correlation),
        _ => false,
    })
}

fn prunable_unnest_input(mut input: &Relation) -> bool {
    // Apply validates every input cell before binding. Unnest can drop unused projections and skip rejected lists.
    loop {
        match input.node() {
            RelationNode::Project { input: source, expressions } | RelationNode::Rename { input: source, expressions, .. }
                if expressions.iter().all(|named| matches!(&named.expr, Expr::Column { id, data_type }
                    if *data_type == named.field.data_type && source.schema().get(*id).is_some_and(|field| field.data_type == *data_type))) => input = source,
            RelationNode::Values { rows } => return rows.iter().flatten().all(plain_unnest_cell),
            RelationNode::LiteralPages { pages } => return pages.iter().flatten().flatten().all(plain_unnest_cell),
            _ => return false,
        }
    }
}

fn plain_unnest_cell(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Value::List(items)) => i32::try_from(items.len()).is_ok() && plain_unnest_list(&expr.data_type()),
        Expr::Literal(value) => plain_unnest_scalar(&value.logical_type()),
        Expr::List(items) => i32::try_from(items.len()).is_ok() && plain_unnest_list(&expr.data_type())
            && items.iter().all(|item| matches!(item, Expr::Literal(value) if plain_unnest_scalar(&value.logical_type()))),
        Expr::Cast { target: CastTarget::Fixed(ty), .. } => plain_unnest_scalar(ty),
        _ => false,
    }
}

/// The elements of the list `expr` reads from each input row, the input row's columns beside each, and the element's
/// position where `ordinality` names a column for it.
fn unnest(input: &Arc<Relation>, expr: &Expr, element: &Field, ordinality: Option<&Field>, outer: bool, correlation: &Correlation<'_>, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let list = correlation.substitute(expr.clone());
    let staged = hidden(ids.next()?, "_unnest", list.data_type());
    let extended = Arc::new(Arc::clone(input).extend(vec![NamedExpr { field: staged.clone(), expr: list }])?);
    unnest_rows(extended, staged.expression(), element, ordinality, outer, ids).map(Some)
}

fn unnest_rows(input: Arc<Relation>, expr: Expr, element: &Field, ordinality: Option<&Field>, outer: bool, ids: &mut Ids) -> Result<Arc<Relation>> {
    let element = if outer { Field { data_type: element.data_type.clone().maybe(), ..element.clone() } } else { element.clone() };
    // An unnest numbers its elements as 32-bit integers, and a row-numbered table as 64-bit ones.
    let position = match ordinality {
        Some(_) => {
            let ty = LogicalType::Scalar(ScalarType::I32);
            Some(hidden(ids.next()?, "_ordinal", if outer { ty.maybe() } else { ty }))
        }
        None => None,
    };
    let fields = input.schema().fields().iter().cloned().chain([element.clone()]).chain(position.clone()).collect();
    let unnested = Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Unnest {
        input, expr, element, ordinality: position.clone(), outer,
    })?);
    let (Some(ordinality), Some(position)) = (ordinality, position) else { return Ok(unnested) };
    let target = if outer { ordinality.data_type.clone().maybe() } else { ordinality.data_type.clone() };
    let field = Field { data_type: target.clone(), ..ordinality.clone() };
    let cast = Expr::Cast { expr: Box::new(position.expression()), target: CastTarget::Fixed(target) };
    Ok(Arc::new(unnested.extend(vec![NamedExpr { field, expr: cast }])?))
}

/// Whether every value of `data_type` compares equal to itself under the grouping comparator (`null_eq`).
/// Floats (`NaN`), lists, structs and transit values can be nonreflexive (nested nulls or NaNs), which would split a
/// single numbered input row into multiple groups if carried directly in `GROUP BY`.
fn reflexive_key(data_type: &LogicalType) -> bool {
    match data_type {
        LogicalType::Null | LogicalType::Nothing => true,
        LogicalType::Scalar(ScalarType::F32 | ScalarType::F64 | ScalarType::Transit) => false,
        LogicalType::Scalar(_) => true,
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => reflexive_key(inner),
        LogicalType::Union(legs) => legs.iter().all(reflexive_key),
        LogicalType::List { .. } | LogicalType::Struct(_) => false,
    }
}

/// Whether the aggregate over a group whose rows its filter rejects gives what it gives over an empty input.
/// The reference's rule 9 excludes the collecting aggregates for the same reason.
fn empty_group_safe(aggregate: &AggregateExpr) -> bool {
    use AggregateFunction as A;
    aggregate.order_by.is_empty() && matches!(aggregate.function.canonical(), A::Count | A::CountStar | A::Sum | A::Average | A::Min | A::Max | A::Every | A::Any
        | A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample)
}

fn hidden(id: ColumnId, prefix: &str, data_type: LogicalType) -> Field {
    Field { id, name: format!("{prefix}_{}", id.0), qualifier: None, qualified_only: true, unnamed: true, data_type }
}

/// Whether `expression` references at least one column, and all of them belong to `schema`.
fn belongs_to(expression: &Expr, schema: &Schema) -> bool {
    let columns = expression.referenced_columns();
    !columns.is_empty() && columns.iter().all(|id| schema.get(*id).is_some())
}

/// Whether `data_type` is acceptable as a `Mark` condition or comparison predicate.
fn is_boolean_or_null(data_type: &LogicalType) -> bool {
    data_type.legs().iter().all(|ty| matches!(ty, LogicalType::Null | LogicalType::Nothing | LogicalType::Scalar(ScalarType::Bool)))
}

/// Updates every `Expr::Column` type in `expression` to match `schema` (for columns widened to nullable by an outer join).
fn refresh_types(mut expression: Expr, schema: &Schema) -> Expr {
    expression.rewrite(&mut |expression| {
        if let Expr::Column { id, data_type } = expression
            && let Some(field) = schema.get(*id)
        {
            *data_type = field.data_type.clone();
        }
    });
    expression
}

/// Moves conjuncts referencing only `right` from `conditions` into a filter over `right`.
fn push_right_filters(right: Arc<Relation>, conditions: &mut Vec<Expr>) -> Result<Arc<Relation>> {
    let right_schema = right.schema().clone();
    let (right_only, remaining): (Vec<Expr>, Vec<Expr>) = std::mem::take(conditions).into_iter()
        .partition(|condition| belongs_to(condition, &right_schema));
    *conditions = remaining;
    finish(right, right_only)
}

/// Drops right-side rows whose equi-join key is null before a left or single join, and pushes right-only conjuncts below
/// it. Normal left and single hash joins compare top-level null equi-keys as equal (matching XTDB's `with-nil-row?`
/// comparator), whereas the apply they replace evaluated `=` under SQL three-valued equality where NULL never matches.
fn exclude_null_keys(left: &Schema, right: Arc<Relation>, conditions: &mut Vec<Expr>) -> Result<Arc<Relation>> {
    let right_schema = right.schema().clone();
    let mut right = push_right_filters(right, conditions)?;
    let mut non_null = Vec::new();
    for condition in conditions.iter() {
        if let Expr::Binary { op: BinaryOp::Eq, left: first, right: second } = condition {
            let key = if belongs_to(first, left) && belongs_to(second, &right_schema) {
                Some(second.as_ref())
            } else if belongs_to(first, &right_schema) && belongs_to(second, left) {
                Some(first.as_ref())
            } else {
                None
            };
            if let Some(key) = key && !non_null.contains(key) {
                non_null.push(key.clone());
            }
        }
    }
    if !non_null.is_empty() {
        let filters = non_null.into_iter().map(|key| Expr::unary(UnaryOp::IsNotNull, key)).collect();
        right = finish(right, filters)?;
    }
    Ok(right)
}

/// Projects the relation onto `schema` where it holds every field of it with the same type.
fn restore(relation: Arc<Relation>, schema: &Schema) -> Result<Option<Arc<Relation>>> {
    if relation.schema() == schema { return Ok(Some(relation)); }
    let mut expressions = Vec::with_capacity(schema.len());
    for field in schema.fields() {
        let Some(actual) = relation.schema().get(field.id) else { return Ok(None) };
        if actual.data_type != field.data_type { return Ok(None); }
        expressions.push(NamedExpr { field: field.clone(), expr: actual.expression() });
    }
    Ok(Some(Arc::new(relation.project(expressions)?)))
}

/// `conjuncts` selected over `input`, each moved beneath the applies whose input alone it reads, as the reference's
/// `push-selection-down-past-apply` does, and the mark of an apply or mark join it tests turned into a semi- or
/// anti-join, as its `select-mark-join->semi-join` does.
/// A selection stays above an apply that checks its dependent's cardinality, whose error it must not suppress.
fn filter_over(input: Arc<Relation>, conjuncts: Vec<Expr>, ids: &mut Ids) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || filter_over_at(input, conjuncts, ids))
}

fn filter_over_at(input: Arc<Relation>, conjuncts: Vec<Expr>, ids: &mut Ids) -> Result<Arc<Relation>> {
    match input.node() {
        RelationNode::Apply { input: outer, dependent, arguments, kind, output } => {
            let pushable = matches!(kind, ApplyKind::Inner | ApplyKind::Left | ApplyKind::Semi | ApplyKind::Anti | ApplyKind::Exists | ApplyKind::Mark);
            let (pushed, kept) = split_pushable(conjuncts, outer.schema(), pushable);
            let (dep_pushed, mut kept) = split_pushable(kept, dependent.schema(), *kind == ApplyKind::Inner);
            let outer = if pushed.is_empty() { decorrelate(Arc::clone(outer), ids)? } else { filter_over(Arc::clone(outer), pushed, ids)? };
            let dependent = if dep_pushed.is_empty() { decorrelate(Arc::clone(dependent), ids)? } else { filter_over(Arc::clone(dependent), dep_pushed, ids)? };
            if let Some(output) = output && movable(&dependent)
                && let Some(position) = kept.iter().position(|conjunct| tests_mark(conjunct, output.id).is_some())
            {
                let negated = tests_mark(&kept[position], output.id) == Some(true);
                let correlation = Correlation { arguments: arguments.as_slice() };
                let attempt = match kind {
                    ApplyKind::Exists => exists_join(&outer, &dependent, &correlation, output, negated),
                    ApplyKind::Mark => mark_apply_join(&outer, &dependent, &correlation, output, negated),
                    ApplyKind::Scalar => quantified_join(&outer, &dependent, &correlation, output, negated),
                    _ => Ok(None),
                };
                if let Some(joined) = attempt? {
                    kept.remove(position);
                    return finish(joined, kept);
                }
            }
            let applied = apply(outer, dependent, arguments, *kind, output.as_ref(), input.schema(), ids)?;
            finish(applied, kept)
        }
        RelationNode::Mark { input: outer, dependent, arguments, condition, comparison, output } => {
            let (pushed, mut kept) = split_pushable(conjuncts, outer.schema(), true);
            let outer = if pushed.is_empty() { decorrelate(Arc::clone(outer), ids)? } else { filter_over(Arc::clone(outer), pushed, ids)? };
            let dependent = decorrelate(Arc::clone(dependent), ids)?;
            let marked = decorrelate_mark(outer, dependent, arguments, condition.as_ref(), comparison, output, input.schema())?;
            if let RelationNode::Mark { input: outer, dependent, arguments, condition, comparison, output } = marked.node()
                && arguments.is_empty()
                && let Some(position) = kept.iter().position(|conjunct| tests_mark(conjunct, output.id).is_some())
            {
                let negated = tests_mark(&kept[position], output.id) == Some(true);
                if let Some(joined) = select_mark_to_join(outer, dependent, condition.as_ref(), comparison, output, negated)? {
                    kept.remove(position);
                    return finish(joined, kept);
                }
            }
            finish(marked, kept)
        }
        _ => finish(decorrelate(Arc::clone(&input), ids)?, conjuncts),
    }
}

/// `select-mark-join->semi-join` for an uncorrelated `Mark` relation: turns `σ mark (R mark_p,c E)` into `R ⋉_{p ∧ c} E`,
/// and `σ ¬mark (R mark_p,true E)` into `R ▷_p E`.
fn select_mark_to_join(
    outer: &Arc<Relation>,
    dependent: &Arc<Relation>,
    condition: Option<&Expr>,
    comparison: &Expr,
    output: &Field,
    negated: bool,
) -> Result<Option<Arc<Relation>>> {
    let is_true_literal = *comparison == Expr::Literal(Value::Bool(true));
    if negated && !is_true_literal { return Ok(None); }
    let mut conditions = Vec::new();
    if let Some(condition) = condition { conditions.extend(conjuncts(condition)); }
    if !is_true_literal { conditions.push(comparison.clone()); }
    let joined = Arc::new(Relation::join(
        Arc::clone(outer),
        Arc::clone(dependent),
        if negated { JoinKind::Anti } else { JoinKind::Semi },
        conjunction(conditions),
    )?);
    let mark = Expr::Coalesce(vec![Expr::Literal(Value::Bool(!negated)), Expr::Literal(Value::Null)]);
    Ok(Some(Arc::new(joined.extend(vec![NamedExpr { field: output.clone(), expr: mark }])?)))
}

/// The conjuncts that read only `schema`, which may move beneath the operator appending to it, and the rest.
/// A volatile conjunct stays where it is evaluated once per output row.
fn split_pushable(conjuncts: Vec<Expr>, schema: &Schema, pushable: bool) -> (Vec<Expr>, Vec<Expr>) {
    if !pushable { return (Vec::new(), conjuncts); }
    let ids = schema.ids();
    conjuncts.into_iter().partition(|conjunct| {
        let columns = conjunct.referenced_columns();
        !columns.is_empty() && columns.is_subset(&ids) && !conjunct.is_volatile()
    })
}

fn finish(relation: Arc<Relation>, conjuncts: Vec<Expr>) -> Result<Arc<Relation>> {
    Ok(match conjunction(conjuncts) { Some(predicate) => Arc::new(relation.filter(predicate)?), None => relation })
}

/// Whether the conjunct keeps the rows whose mark is true (`Some(false)`) or false (`Some(true)`), and nothing else.
fn tests_mark(conjunct: &Expr, mark: ColumnId) -> Option<bool> {
    let is_mark = |expression: &Expr| matches!(expression, Expr::Column { id, .. } if *id == mark);
    match conjunct {
        expression if is_mark(expression) => Some(false),
        Expr::Unary { op: UnaryOp::IsTrue, expr } if is_mark(expr.as_ref()) => Some(false),
        Expr::Unary { op: UnaryOp::Not | UnaryOp::IsFalse, expr } if is_mark(expr.as_ref()) => Some(true),
        _ => None,
    }
}

/// `σ mark (R A_exists (σp E)) = R ⋉p E`, and `σ ¬mark` the anti-join, where E reads no parameter bound from R.
/// An EXISTS is never null, so the rows each join keeps are exactly those the selection keeps.
fn exists_join(outer: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, negated: bool) -> Result<Option<Arc<Relation>>> {
    let Some(lifted) = lift(dependent, correlation, true)? else { return Ok(None) };
    if !outer.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let conditions: Vec<Expr> = lifted.filters.into_iter().flat_map(|filter| conjuncts(&correlation.substitute(filter))).collect();
    let joined = Arc::new(Relation::join(Arc::clone(outer), lifted.base, if negated { JoinKind::Anti } else { JoinKind::Semi }, conjunction(conditions))?);
    Ok(Some(Arc::new(joined.extend(vec![NamedExpr { field: output.clone(), expr: Expr::Literal(Value::Bool(!negated)) }])?)))
}

/// `σ mark (R A_mark (π c (σp E))) = R ⋉_{p ∧ c} E`, and `σ ¬mark` the anti-join on `p ∧ ¬(c IS FALSE)`.
fn mark_apply_join(outer: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, negated: bool) -> Result<Option<Arc<Relation>>> {
    let [source] = dependent.schema().fields() else { return Ok(None) };
    let Some(lifted) = lift(dependent, correlation, false)? else { return Ok(None) };
    if !outer.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let comparison = match lifted.computed.iter().find(|named| named.field.id == source.id) {
        Some(named) => named.expr.clone(),
        None => match lifted.base.schema().get(source.id) { Some(field) => field.expression(), None => return Ok(None) },
    };
    let is_true_literal = comparison == Expr::Literal(Value::Bool(true));
    let test = if is_true_literal {
        None
    } else if negated {
        Some(Expr::unary(UnaryOp::Not, Expr::unary(UnaryOp::IsFalse, comparison)))
    } else {
        Some(comparison)
    };
    let conditions: Vec<Expr> = lifted.filters.into_iter().chain(test).flat_map(|condition| conjuncts(&correlation.substitute(condition))).collect();
    let joined = Arc::new(Relation::join(Arc::clone(outer), lifted.base, if negated { JoinKind::Anti } else { JoinKind::Semi }, conjunction(conditions))?);
    let mark = Expr::Coalesce(vec![Expr::Literal(Value::Bool(!negated)), Expr::Literal(Value::Null)]);
    Ok(Some(Arc::new(joined.extend(vec![NamedExpr { field: output.clone(), expr: mark }])?)))
}

/// The comparison column and the rows it is computed over where `dependent` is a correlated quantified comparison as the
/// SQL binder plans it: `CASE WHEN count(*) FILTER (c IS TRUE) > 0 THEN true WHEN count(*) FILTER (c IS UNKNOWN) > 0 THEN
/// null ELSE false END` over an aggregate without groups.
fn quantified(dependent: &Relation) -> Option<(ColumnId, &Arc<Relation>)> {
    let RelationNode::Project { input, expressions } = dependent.node() else { return None };
    let [NamedExpr { expr: Expr::Case { branches, otherwise }, .. }] = expressions.as_slice() else { return None };
    let RelationNode::Aggregate { input: source, groups, aggregates } = input.node() else { return None };
    let [(Expr::Binary { op: BinaryOp::Gt, left: truth, right: truth_zero }, Expr::Literal(Value::Bool(true))),
        (Expr::Binary { op: BinaryOp::Gt, left: unknown, right: unknown_zero }, Expr::Literal(Value::Null))] = branches.as_slice() else { return None };
    let zero = Expr::Literal(Value::I64(0));
    if !groups.is_empty() || aggregates.len() != 2 || **otherwise != Expr::Literal(Value::Bool(false)) || **truth_zero != zero || **unknown_zero != zero {
        return None;
    }
    let counted = |count: &Expr, test: UnaryOp| -> Option<ColumnId> {
        let Expr::Column { id, .. } = count else { return None };
        let aggregate = aggregates.iter().find(|aggregate| aggregate.field.id == *id)?;
        if aggregate.function.canonical() != AggregateFunction::CountStar || aggregate.distinct || !aggregate.order_by.is_empty() { return None; }
        match aggregate.filter.as_ref()? {
            Expr::Unary { op, expr } if *op == test => match expr.as_ref() { Expr::Column { id, .. } => Some(*id), _ => None },
            _ => None,
        }
    };
    let comparison = counted(truth.as_ref(), UnaryOp::IsTrue)?;
    (counted(unknown.as_ref(), UnaryOp::IsUnknown)? == comparison).then_some((comparison, source))
}

/// A selection on a correlated quantified comparison as a semi-join on the candidates the comparison holds for, and its
/// negation as an anti-join on those it doesn't hold false for: the comparison is true where some candidate compares
/// true, and false only where every candidate compares false.
fn quantified_join(outer: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, negated: bool) -> Result<Option<Arc<Relation>>> {
    let Some((comparison, source)) = quantified(dependent) else { return Ok(None) };
    let Some(lifted) = lift(source, correlation, false)? else { return Ok(None) };
    if !outer.schema().ids().is_disjoint(&lifted.base.schema().ids()) { return Ok(None); }
    let comparison = match lifted.computed.iter().find(|named| named.field.id == comparison) {
        Some(named) => named.expr.clone(),
        None => match lifted.base.schema().get(comparison) { Some(field) => field.expression(), None => return Ok(None) },
    };
    let test = if negated { Expr::unary(UnaryOp::Not, Expr::unary(UnaryOp::IsFalse, comparison)) } else { comparison };
    let conditions: Vec<Expr> = lifted.filters.into_iter().chain([test]).flat_map(|condition| conjuncts(&correlation.substitute(condition))).collect();
    let joined = Arc::new(Relation::join(Arc::clone(outer), lifted.base, if negated { JoinKind::Anti } else { JoinKind::Semi }, conjunction(conditions))?);
    // The comparison's column is nullable, as the apply's was.
    let mark = Expr::Coalesce(vec![Expr::Literal(Value::Bool(!negated)), Expr::Literal(Value::Null)]);
    Ok(Some(Arc::new(joined.extend(vec![NamedExpr { field: output.clone(), expr: mark }])?)))
}

/// A dependent plan as `π(σ filters (ε computed (base)))`, where `base` reads no bound parameter, and `filters` and the
/// `computed` columns read only `base` and the bound parameters.
struct Lifted { base: Arc<Relation>, filters: Vec<Expr>, computed: Vec<NamedExpr> }

/// Moves the correlated selections of a dependent plan above its projections, inner joins, unnests and groupings, as the
/// reference's `pull-correlated-selection-up-towards-apply` does, leaving its uncorrelated selections beneath an unnest or
/// grouping, as its `push-decorrelated-selection-down-past-unnest` and `-past-group-by` do.
/// Conjuncts are lifted one by one, so a selection need not be ordered before another reading more columns, as the
/// reference's `push-decorrelated-selections-with-fewer-variables-down` orders them.
/// Where only the dependent's existence matters, operators that keep it, such as DISTINCT and sorts, are dropped.
fn lift(relation: &Arc<Relation>, correlation: &Correlation<'_>, existence: bool) -> Result<Option<Lifted>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || lift_at(relation, correlation, existence))
}

fn lift_at(relation: &Arc<Relation>, correlation: &Correlation<'_>, existence: bool) -> Result<Option<Lifted>> {
    if !correlation.references(relation) {
        return Ok(Some(Lifted { base: Arc::clone(relation), filters: Vec::new(), computed: Vec::new() }));
    }
    Ok(match relation.node() {
        RelationNode::Filter { input, predicate } => {
            if correlation.in_expression(predicate) && guards_fallible_unnest(input, predicate) { return Ok(None); }
            let Some(mut lifted) = lift(input, correlation, existence)? else { return Ok(None) };
            let Some(predicate) = inline(predicate.clone(), &lifted.computed) else { return Ok(None) };
            lifted.filters.extend(conjuncts(&predicate));
            Some(lifted)
        }
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } | RelationNode::Extend { input, expressions } => {
            let Some(mut lifted) = lift(input, correlation, existence)? else { return Ok(None) };
            let base = lifted.base.schema().ids();
            let mut added = Vec::new();
            for named in expressions {
                if matches!(&named.expr, Expr::Column { id, .. } if *id == named.field.id) { continue; }
                if numbers_rows(&named.expr) || base.contains(&named.field.id) || lifted.computed.iter().any(|computed| computed.field.id == named.field.id) {
                    return Ok(None);
                }
                let Some(expr) = inline(named.expr.clone(), &lifted.computed) else { return Ok(None) };
                added.push(NamedExpr { field: named.field.clone(), expr });
            }
            lifted.computed.extend(added);
            Some(lifted)
        }
        RelationNode::Distinct { input } | RelationNode::Sort { input, .. } if existence => lift(input, correlation, existence)?,
        RelationNode::Limit { input, offset: None, fetch } | RelationNode::SortLimit { input, offset: None, fetch, .. }
            if existence && fetch.as_ref().is_none_or(positive) => lift(input, correlation, existence)?,
        RelationNode::Join { left, right, kind: JoinKind::Inner | JoinKind::Cross, condition } => {
            let (Some(left), Some(right)) = (lift(left, correlation, false)?, lift(right, correlation, false)?) else { return Ok(None) };
            let mut computed = left.computed;
            computed.extend(right.computed);
            let mut filters = left.filters;
            filters.extend(right.filters);
            let mut kept = Vec::new();
            for conjunct in condition.iter().flat_map(conjuncts) {
                let Some(conjunct) = inline(conjunct, &computed) else { return Ok(None) };
                if correlation.in_expression(&conjunct) { filters.push(conjunct); } else { kept.push(conjunct); }
            }
            let kind = if kept.is_empty() { JoinKind::Cross } else { JoinKind::Inner };
            Some(Lifted { base: Arc::new(Relation::join(left.base, right.base, kind, conjunction(kept))?), filters, computed })
        }
        // A correlated selection on the row-preserving side of a left, semi or anti join keeps the same rows whether it
        // runs before or after the join, as the reference's `pull-correlated-selection-up-towards-apply` lifts it.
        RelationNode::Join { left, right, kind: kind @ (JoinKind::Left | JoinKind::Semi | JoinKind::Anti), condition } => {
            if correlation.references(right) || condition.as_ref().is_some_and(|predicate| correlation.in_expression(predicate)) {
                return Ok(None);
            }
            let Some(lifted) = lift(left, correlation, false)? else { return Ok(None) };
            let Some((base, filters)) = settle(lifted, correlation)? else { return Ok(None) };
            if !base.schema().ids().is_disjoint(&right.schema().ids()) { return Ok(None); }
            let joined = Relation::join(base, Arc::clone(right), *kind, condition.clone())?;
            Some(Lifted { base: Arc::new(joined), filters, computed: Vec::new() })
        }
        RelationNode::Join { left, right, kind: JoinKind::Right, condition } => {
            if correlation.references(left) || condition.as_ref().is_some_and(|predicate| correlation.in_expression(predicate)) {
                return Ok(None);
            }
            let Some(lifted) = lift(right, correlation, false)? else { return Ok(None) };
            let Some((base, filters)) = settle(lifted, correlation)? else { return Ok(None) };
            if !left.schema().ids().is_disjoint(&base.schema().ids()) { return Ok(None); }
            let joined = Relation::join(Arc::clone(left), base, JoinKind::Right, condition.clone())?;
            Some(Lifted { base: Arc::new(joined), filters, computed: Vec::new() })
        }
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            if correlation.references(dependent)
                || arguments.iter().any(|(_, expr)| correlation.in_expression(expr))
                || condition.as_ref().is_some_and(|predicate| correlation.in_expression(predicate))
                || correlation.in_expression(comparison)
            {
                return Ok(None);
            }
            let Some(lifted) = lift(input, correlation, false)? else { return Ok(None) };
            let Some((base, filters)) = settle(lifted, correlation)? else { return Ok(None) };
            if !base.schema().ids().is_disjoint(&dependent.schema().ids()) || base.schema().get(output.id).is_some() {
                return Ok(None);
            }
            let mark_schema = base.schema().concat(&Schema::new(vec![output.clone()])?)?;
            let marked = Relation::new(mark_schema, RelationNode::Mark {
                input: base, dependent: Arc::clone(dependent), arguments: arguments.clone(),
                condition: condition.clone(), comparison: comparison.clone(), output: output.clone(),
            })?;
            Some(Lifted { base: Arc::new(marked), filters, computed: Vec::new() })
        }
        // A selection on the unnested relation's columns keeps the same rows whether it is evaluated before or after
        // each row is repeated for its elements.
        RelationNode::Unnest { input, expr, element, ordinality, outer } => {
            if correlation.in_expression(expr) { return Ok(None); }
            let Some(lifted) = lift(input, correlation, false)? else { return Ok(None) };
            // Staging the list above a correlated filter can evaluate it on rows the filter used to exclude.
            if lifted.filters.iter().any(|filter| correlation.in_expression(filter))
                && (!infallible(expr) || lifted.computed.iter().any(|named| !infallible(&named.expr)))
            {
                return Ok(None);
            }
            let Some((base, filters)) = settle(lifted, correlation)? else { return Ok(None) };
            let fields = base.schema().fields().iter().cloned().chain([element.clone()]).chain(ordinality.clone()).collect();
            let unnested = Relation::new(Schema::new(fields)?, RelationNode::Unnest {
                input: base, expr: expr.clone(), element: element.clone(), ordinality: ordinality.clone(), outer: *outer,
            })?;
            Some(Lifted { base: Arc::new(unnested), filters, computed: Vec::new() })
        }
        // A selection on grouping columns keeps the same groups whether it is evaluated before or after grouping.
        // Without groups, an aggregate gives a row even over the empty input the selection would leave it.
        RelationNode::Aggregate { input, groups, aggregates } if !groups.is_empty() => {
            if relation.expressions().into_iter().any(|expression| correlation.in_expression(expression)) || correlation.in_fractions(aggregates) {
                return Ok(None);
            }
            let Some(lifted) = lift(input, correlation, false)? else { return Ok(None) };
            let keys: Vec<(ColumnId, Expr)> = groups.iter().filter_map(|group| match inline(group.expr.clone(), &lifted.computed)? {
                Expr::Column { id, .. } => Some((id, group.field.expression())),
                _ => None,
            }).collect();
            let Some((base, filters)) = settle(lifted, correlation)? else { return Ok(None) };
            let mut regrouped = Vec::with_capacity(filters.len());
            for mut filter in filters {
                let mut copied = false;
                filter.visit(&mut |expression| copied |= matches!(expression, Expr::RowCopy(_)));
                if copied || !filter.referenced_columns().iter().all(|id| keys.iter().any(|(key, _)| key == id)) { return Ok(None); }
                filter.rewrite(&mut |expression| {
                    let Expr::Column { id, .. } = expression else { return };
                    let id = *id;
                    if let Some((_, group)) = keys.iter().find(|(key, _)| *key == id) { *expression = group.clone(); }
                });
                regrouped.push(filter);
            }
            let grouped = Relation::new(relation.schema().clone(), RelationNode::Aggregate { input: base, groups: groups.clone(), aggregates: aggregates.clone() })?;
            Some(Lifted { base: Arc::new(grouped), filters: regrouped, computed: Vec::new() })
        }
        _ => None,
    })
}

/// Whether `predicate` would push below an uncorrelated `Unnest` in `relation` whose staged list expression can fail
/// (such as `UNNEST(ARRAY[1 / x]) WHERE x = outer.k`). Lifting `predicate` out into a join condition would evaluate that
/// list expression over rows the correlated filter would have excluded first.
fn guards_fallible_unnest(relation: &Relation, predicate: &Expr) -> bool {
    let columns = predicate.referenced_columns();
    if columns.is_empty() { return false; }
    let RelationNode::Unnest { input, expr, .. } = relation.node() else { return false; };
    if !columns.iter().all(|id| input.schema().get(*id).is_some()) { return false; }
    let staged = match (expr, input.node()) {
        (Expr::Column { id, .. }, RelationNode::Extend { input: base, expressions } | RelationNode::Project { input: base, expressions })
            if columns.iter().all(|column| base.schema().get(*column).is_some()) =>
            expressions.iter().find(|named| named.field.id == *id).map(|named| &named.expr).unwrap_or(expr),
        _ => expr,
    };
    !infallible(staged)
}

fn infallible(expression: &Expr) -> bool {
    match expression {
        Expr::Literal(_) | Expr::Column { .. } | Expr::Parameter { .. } | Expr::Correlated { .. } | Expr::DurationLiteral(_) => true,
        Expr::List(items) => items.iter().all(infallible),
        Expr::Struct(fields) => fields.iter().all(|(_, value)| infallible(value)),
        _ => false,
    }
}

/// The lifted plan with its computed columns and uncorrelated selections evaluated over its base, to go beneath an
/// operator only its correlated selections, returned beside it, move above; `None` where a computed column reads a
/// bound parameter.
fn settle(lifted: Lifted, correlation: &Correlation<'_>) -> Result<Option<(Arc<Relation>, Vec<Expr>)>> {
    if lifted.computed.iter().any(|named| correlation.in_expression(&named.expr)) { return Ok(None); }
    let base = if lifted.computed.is_empty() { lifted.base } else { Arc::new(lifted.base.extend(lifted.computed)?) };
    let (correlated, uncorrelated): (Vec<Expr>, Vec<Expr>) = lifted.filters.into_iter().partition(|filter| correlation.in_expression(filter));
    Ok(Some((finish(base, uncorrelated)?, correlated)))
}

fn positive(fetch: &Expr) -> bool {
    match fetch {
        Expr::Literal(Value::I8(rows)) => *rows > 0,
        Expr::Literal(Value::I16(rows)) => *rows > 0,
        Expr::Literal(Value::I32(rows)) => *rows > 0,
        Expr::Literal(Value::I64(rows)) => *rows > 0,
        _ => false,
    }
}

/// Replaces each reference to a computed column with its definition, or `None` where a row copy reads one.
fn inline(mut expression: Expr, computed: &[NamedExpr]) -> Option<Expr> {
    if computed.is_empty() { return Some(expression); }
    let mut copied = false;
    expression.visit(&mut |expression| if let Expr::RowCopy(fields) = expression {
        copied |= fields.iter().any(|field| computed.iter().any(|named| named.field.id == field.id));
    });
    if copied { return None; }
    expression.rewrite(&mut |expression| {
        let Expr::Column { id, .. } = expression else { return };
        let id = *id;
        if let Some(named) = computed.iter().find(|named| named.field.id == id) { *expression = named.expr.clone(); }
    });
    Some(expression)
}

fn conjuncts(predicate: &Expr) -> Vec<Expr> {
    let mut pending = vec![predicate];
    let mut atoms = Vec::new();
    while let Some(expression) = pending.pop() {
        match expression {
            Expr::Binary { op: BinaryOp::And, left, right } => { pending.push(right.as_ref()); pending.push(left.as_ref()); }
            atom => atoms.push(atom.clone()),
        }
    }
    atoms
}

fn conjunction(conjuncts: Vec<Expr>) -> Option<Expr> {
    conjuncts.into_iter().reduce(|left, right| Expr::binary(BinaryOp::And, left, right))
}
