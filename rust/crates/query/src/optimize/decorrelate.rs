//! Rewrites applies into joins where the dependent plan's correlation lifts into a join condition, porting the
//! decorrelation rules of the reference's logical plan (`xtdb.logical-plan`).
//!
//! Every rewrite keeps the schema of the relation it replaces.
//! A rule answers `Ok(None)` where it does not apply, which keeps the apply; an error building a rewrite fails the plan.

use std::sync::Arc;
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, CastTarget, ColumnId, Expr, Field, JoinKind, NamedExpr, OrdinalityScope, PercentileFraction, Relation, RelationNode, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use super::{Ids, with_children};

pub(super) fn decorrelate(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || decorrelate_at(relation, ids))
}

fn decorrelate_at(relation: Arc<Relation>, ids: &mut Ids) -> Result<Arc<Relation>> {
    match relation.node() {
        // The selection is seen before the apply beneath it, which it may turn into a semi- or anti-join.
        RelationNode::Filter { input, predicate } if selects_through(input) => filter_over(Arc::clone(input), conjuncts(predicate), ids),
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = decorrelate(Arc::clone(input), ids)?;
            let dependent = decorrelate(Arc::clone(dependent), ids)?;
            apply(input, dependent, arguments, *kind, output.as_ref(), relation.schema(), ids)
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
    if !correlation.references(dependent) { return uncorrelated(input, dependent, kind, output, schema); }
    match (kind, output) {
        (ApplyKind::Inner, _) => {
            if let Some(joined) = lateral(input, dependent, &correlation, kind, schema)? { return Ok(Some(joined)); }
            if let Some(unnested) = column_table(input, dependent, &correlation, ids)? { return restore(unnested, schema); }
            grouped_lateral(input, dependent, &correlation, schema, ids)
        }
        (ApplyKind::Left | ApplyKind::Single | ApplyKind::Semi | ApplyKind::Anti, _) => lateral(input, dependent, &correlation, kind, schema),
        (ApplyKind::Scalar, Some(output)) => match scalar_aggregate(input, dependent, &correlation, output, ids)? {
            Some(grouped) => Ok(Some(grouped)),
            None => scalar_single(input, dependent, &correlation, output, ids),
        },
        _ => Ok(None),
    }
}

/// Rule 1: `R A⊗ E = R ⊗ E` where E reads no parameter bound from R.
/// A scalar apply becomes the single join the binder plans for an uncorrelated scalar subquery, which keeps its cardinality check.
fn uncorrelated(input: &Arc<Relation>, dependent: &Arc<Relation>, kind: ApplyKind, output: Option<&Field>, schema: &Schema) -> Result<Option<Arc<Relation>>> {
    let joined = match (kind, output) {
        (ApplyKind::Inner, _) => Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Cross, None)?,
        (ApplyKind::Single, _) => Relation::join(Arc::clone(input), Arc::clone(dependent), JoinKind::Single, None)?,
        (ApplyKind::Scalar, Some(output)) => {
            let [source] = dependent.schema().fields() else { return Ok(None) };
            let field = Field { data_type: source.data_type.clone(), ..output.clone() };
            let right = Arc::new(Arc::clone(dependent).project(vec![NamedExpr { field, expr: source.expression() }])?);
            Relation::join(Arc::clone(input), right, JoinKind::Single, None)?
        }
        _ => return Ok(None),
    };
    restore(Arc::new(joined), schema)
}

/// Rule 2: `R A⊗ (σp E) = R ⊗p E` where E reads no parameter bound from R, for the relational applies.
/// Projections the correlated selection was beneath are evaluated over the join, as rule 4 does for a cross apply.
fn lateral(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, kind: ApplyKind, schema: &Schema) -> Result<Option<Arc<Relation>>> {
    let Some(lifted) = lift(dependent, correlation, matches!(kind, ApplyKind::Semi | ApplyKind::Anti))? else { return Ok(None) };
    let conditions: Vec<Expr> = lifted.filters.into_iter().map(|filter| correlation.substitute(filter)).collect();
    // A computed column the dependent does not return was read only by selections, into which it is already inlined, as
    // the reference's `push-decorrelated-predicate-down-past-period-constructor` inlines a period into its predicate.
    let computed: Vec<NamedExpr> = lifted.computed.into_iter().filter(|named| dependent.schema().get(named.field.id).is_some()).collect();
    let spanning = spans(&conditions, input.schema(), lifted.base.schema());
    let join_kind = match kind {
        ApplyKind::Inner if conditions.is_empty() => JoinKind::Cross,
        ApplyKind::Inner => JoinKind::Inner,
        // Over the rows a left join supplies nulls for, a projection could compute a value the apply would not.
        ApplyKind::Left if spanning && computed.is_empty() => JoinKind::Left,
        ApplyKind::Single if spanning && computed.is_empty() => JoinKind::Single,
        ApplyKind::Semi if spanning => JoinKind::Semi,
        ApplyKind::Anti if spanning => JoinKind::Anti,
        _ => return Ok(None),
    };
    let joined = Arc::new(Relation::join(Arc::clone(input), lifted.base, join_kind, conjunction(conditions))?);
    let rows = if matches!(join_kind, JoinKind::Inner | JoinKind::Cross) && !computed.is_empty() {
        let computed = computed.into_iter().map(|named| NamedExpr { field: named.field, expr: correlation.substitute(named.expr) }).collect();
        Arc::new(joined.extend(computed)?)
    } else { joined };
    restore(rows, schema)
}

/// Rule 9: `R A× (G F1 E) = G columns(R),F' (R A⟕ E)`, for a scalar subquery over an aggregate without groups.
/// The ordinality the input is numbered with is the key the rule requires of R, so duplicate input rows stay apart.
/// Every aggregate counts only the rows the join matched, so a row it supplied nulls for aggregates as the empty input the
/// apply would have given it; the dependent always has one row, so there is no cardinality for a check to reject.
fn scalar_aggregate(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let (value, aggregate) = match dependent.node() {
        RelationNode::Project { input: aggregate, expressions } => {
            let [named] = expressions.as_slice() else { return Ok(None) };
            if numbers_rows(&named.expr) { return Ok(None); }
            (named.expr.clone(), aggregate)
        }
        RelationNode::Aggregate { aggregates, .. } => {
            let [only] = aggregates.as_slice() else { return Ok(None) };
            (only.field.expression(), dependent)
        }
        _ => return Ok(None),
    };
    let RelationNode::Aggregate { input: source, groups, aggregates } = aggregate.node() else { return Ok(None) };
    if !groups.is_empty() || !aggregates.iter().all(empty_group_safe) || correlation.in_fractions(aggregates) { return Ok(None); }
    if input.schema().fields().iter().any(|field| rtdb_plan::validate_equality_types(&field.data_type, &field.data_type).is_err()) { return Ok(None); }
    let Some(lifted) = lift(source, correlation, false)? else { return Ok(None) };
    let conditions: Vec<Expr> = lifted.filters.into_iter().map(|filter| correlation.substitute(filter)).collect();
    if !spans(&conditions, input.schema(), lifted.base.schema()) { return Ok(None); }
    let number = hidden(ids.next()?, "_row_number", LogicalType::Scalar(ScalarType::I64));
    let matched = hidden(ids.next()?, "_dep_countable", LogicalType::Scalar(ScalarType::Bool));
    let numbered = Arc::new(Arc::clone(input).with_ordinality(number.clone(), OrdinalityScope::Relation)?);
    let marked = Arc::new(Arc::clone(&lifted.base).extend(vec![NamedExpr { field: matched.clone(), expr: Expr::Literal(Value::Bool(true)) }])?);
    let joined = Arc::new(Relation::join(numbered, marked, JoinKind::Left, conjunction(conditions))?);
    let unmatched = Expr::unary(UnaryOp::IsNull, matched.expression());
    let rows = if lifted.computed.is_empty() { joined } else {
        let computed = lifted.computed.into_iter().map(|named| NamedExpr {
            field: named.field,
            expr: Expr::Case { branches: vec![(unmatched.clone(), Expr::Literal(Value::Null))], otherwise: Box::new(correlation.substitute(named.expr)) },
        }).collect();
        Arc::new(joined.extend(computed)?)
    };
    let present = Expr::unary(UnaryOp::IsNotNull, matched.expression());
    let aggregates: Vec<AggregateExpr> = aggregates.iter().map(|aggregate| AggregateExpr {
        filter: Some(match &aggregate.filter {
            Some(filter) => Expr::binary(BinaryOp::And, present.clone(), correlation.substitute(filter.clone())),
            None => present.clone(),
        }),
        ..aggregate.clone()
    }).collect();
    let groups: Vec<NamedExpr> = input.schema().fields().iter().chain([&number]).map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
    let schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
    let grouped = Arc::new(Relation::new(schema, RelationNode::Aggregate { input: rows, groups, aggregates })?);
    // The apply's value is nullable whatever the aggregate's type; the ordinality is never null.
    let value = Expr::Case { branches: vec![(Expr::unary(UnaryOp::IsNull, number.expression()), Expr::Literal(Value::Null))], otherwise: Box::new(correlation.substitute(value)) };
    let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
        .chain([NamedExpr { field: output.clone(), expr: value }]).collect();
    Ok(Some(Arc::new(grouped.project(expressions)?)))
}

/// Rule 2 for a scalar subquery: `R A_scalar (π v (σp E)) = π columns(R),v (R ⟕¹p E)` where E reads no parameter bound
/// from R, as the reference's single-join apply becomes a single join.
/// The single join rejects an input row that more than one row of E matches, as the apply's cardinality check does.
fn scalar_single(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, output: &Field, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let [source] = dependent.schema().fields() else { return Ok(None) };
    let Some(lifted) = lift(dependent, correlation, false)? else { return Ok(None) };
    let conditions: Vec<Expr> = lifted.filters.into_iter().map(|filter| correlation.substitute(filter)).collect();
    if !spans(&conditions, input.schema(), lifted.base.schema()) { return Ok(None); }
    let computed = lifted.computed.into_iter().find(|named| named.field.id == source.id).map(|named| correlation.substitute(named.expr));
    let (right, matched) = match &computed {
        // Over the rows the join supplies nulls for, a computed value must stay null, as the apply's empty result is.
        Some(value) if !matches!(value, Expr::Column { .. }) => {
            let matched = hidden(ids.next()?, "_dep_countable", LogicalType::Scalar(ScalarType::Bool));
            (Arc::new(Arc::clone(&lifted.base).extend(vec![NamedExpr { field: matched.clone(), expr: Expr::Literal(Value::Bool(true)) }])?), Some(matched))
        }
        _ => (lifted.base, None),
    };
    let joined = Arc::new(Relation::join(Arc::clone(input), right, JoinKind::Single, conjunction(conditions))?);
    let value = match (computed, matched) {
        (Some(value), Some(matched)) => Expr::Case { branches: vec![(Expr::unary(UnaryOp::IsNull, matched.expression()), Expr::Literal(Value::Null))], otherwise: Box::new(value) },
        (Some(value), None) => value,
        (None, _) => match joined.schema().get(source.id) { Some(field) => field.expression(), None => return Ok(None) },
    };
    let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
        .chain([NamedExpr { field: output.clone(), expr: value }]).collect();
    Ok(Some(Arc::new(joined.project(expressions)?)))
}

/// Rule 8: `R A× (G A,F E) = G A ∪ columns(R),F (R A× E)`, for a lateral subquery over an aggregate with groups.
/// The ordinality the input is numbered with keeps duplicate input rows in groups of their own.
/// An aggregate without groups gives a row even for an input row E is empty for, which the grouped form would drop.
fn grouped_lateral(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, schema: &Schema, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let (projection, aggregate) = match dependent.node() {
        RelationNode::Project { input: aggregate, expressions } if matches!(aggregate.node(), RelationNode::Aggregate { .. }) => (Some(expressions), aggregate),
        _ => (None, dependent),
    };
    let RelationNode::Aggregate { input: source, groups, aggregates } = aggregate.node() else { return Ok(None) };
    if groups.is_empty() || correlation.in_fractions(aggregates) { return Ok(None); }
    if projection.is_some_and(|expressions| expressions.iter().any(|named| numbers_rows(&named.expr))) { return Ok(None); }
    if input.schema().fields().iter().any(|field| rtdb_plan::validate_equality_types(&field.data_type, &field.data_type).is_err()) { return Ok(None); }
    let number = hidden(ids.next()?, "_row_number", LogicalType::Scalar(ScalarType::I64));
    let numbered = Arc::new(Arc::clone(input).with_ordinality(number.clone(), OrdinalityScope::Relation)?);
    let applied = ApplyKind::Inner.result_schema(numbered.schema(), source.schema(), None)?;
    let Some(rows) = rewrite(&numbered, source, correlation.arguments, ApplyKind::Inner, None, &applied, ids)? else { return Ok(None) };
    let carried = || input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() });
    let groups: Vec<NamedExpr> = carried().chain([NamedExpr { field: number.clone(), expr: number.expression() }])
        .chain(groups.iter().map(|group| NamedExpr { field: group.field.clone(), expr: correlation.substitute(group.expr.clone()) })).collect();
    let aggregates: Vec<AggregateExpr> = aggregates.iter().map(|aggregate| AggregateExpr {
        filter: aggregate.filter.clone().map(|filter| correlation.substitute(filter)), ..aggregate.clone()
    }).collect();
    let grouped_schema = Schema::new(groups.iter().map(|group| group.field.clone()).chain(aggregates.iter().map(|aggregate| aggregate.field.clone())).collect())?;
    let grouped = Arc::new(Relation::new(grouped_schema, RelationNode::Aggregate { input: rows, groups, aggregates })?);
    let expressions: Vec<NamedExpr> = match projection {
        Some(expressions) => carried().chain(expressions.iter().map(|named| NamedExpr { field: named.field.clone(), expr: correlation.substitute(named.expr.clone()) })).collect(),
        None => carried().chain(aggregate.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })).collect(),
    };
    restore(Arc::new(grouped.project(expressions)?), schema)
}

/// `apply-table-col->unnest`: `R A× T(l)`, the table of the elements of a list `l` read from R, as `μ l (R)`, with the
/// table's row numbers as the unnest's ordinality, and the selections and projections over the table evaluated over the
/// unnest.
fn column_table(input: &Arc<Relation>, dependent: &Arc<Relation>, correlation: &Correlation<'_>, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    match dependent.node() {
        RelationNode::Filter { input: table, predicate } => {
            let Some(unnested) = column_table(input, table, correlation, ids)? else { return Ok(None) };
            Ok(Some(Arc::new(unnested.filter(correlation.substitute(predicate.clone()))?)))
        }
        RelationNode::Project { input: table, expressions } => {
            if expressions.iter().any(|named| numbers_rows(&named.expr)) { return Ok(None); }
            let Some(unnested) = column_table(input, table, correlation, ids)? else { return Ok(None) };
            let expressions = input.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                .chain(expressions.iter().map(|named| NamedExpr { field: named.field.clone(), expr: correlation.substitute(named.expr.clone()) })).collect();
            Ok(Some(Arc::new(unnested.project(expressions)?)))
        }
        RelationNode::ColumnTable { expr, element } => unnest(input, expr, element, None, correlation, ids),
        RelationNode::Ordinality { input: table, field, scope: OrdinalityScope::Page } => match table.node() {
            RelationNode::ColumnTable { expr, element } => unnest(input, expr, element, Some(field), correlation, ids),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// The elements of the list `expr` reads from each input row, the input row's columns beside each, and the element's
/// position where `ordinality` names a column for it.
fn unnest(input: &Arc<Relation>, expr: &Expr, element: &Field, ordinality: Option<&Field>, correlation: &Correlation<'_>, ids: &mut Ids) -> Result<Option<Arc<Relation>>> {
    let list = correlation.substitute(expr.clone());
    let staged = hidden(ids.next()?, "_unnest", list.data_type());
    let extended = Arc::new(Arc::clone(input).extend(vec![NamedExpr { field: staged.clone(), expr: list }])?);
    // An unnest numbers its elements as 32-bit integers, and a row-numbered table as 64-bit ones.
    let position = match ordinality { Some(_) => Some(hidden(ids.next()?, "_ordinal", LogicalType::Scalar(ScalarType::I32))), None => None };
    let fields = extended.schema().fields().iter().cloned().chain([element.clone()]).chain(position.clone()).collect();
    let unnested = Arc::new(Relation::new(Schema::new(fields)?, RelationNode::Unnest {
        input: extended, expr: staged.expression(), element: element.clone(), ordinality: position.clone(), outer: false,
    })?);
    let (Some(ordinality), Some(position)) = (ordinality, position) else { return Ok(Some(unnested)) };
    let cast = Expr::Cast { expr: Box::new(position.expression()), target: CastTarget::Fixed(ordinality.data_type.clone()) };
    Ok(Some(Arc::new(unnested.extend(vec![NamedExpr { field: ordinality.clone(), expr: cast }])?)))
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

/// Whether a condition reads both inputs, so that it stays the join's own rather than moving into one side.
fn spans(conditions: &[Expr], left: &Schema, right: &Schema) -> bool {
    conditions.iter().any(|condition| {
        let columns = condition.referenced_columns();
        columns.iter().any(|id| left.get(*id).is_some()) && columns.iter().any(|id| right.get(*id).is_some())
    })
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
/// `push-selection-down-past-apply` does, and the mark of an apply it tests turned into a semi- or anti-join, as its
/// `select-mark-join->semi-join` does.
/// A selection stays above an apply that checks its dependent's cardinality, whose error it must not suppress.
fn filter_over(input: Arc<Relation>, conjuncts: Vec<Expr>, ids: &mut Ids) -> Result<Arc<Relation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || filter_over_at(input, conjuncts, ids))
}

fn filter_over_at(input: Arc<Relation>, conjuncts: Vec<Expr>, ids: &mut Ids) -> Result<Arc<Relation>> {
    match input.node() {
        RelationNode::Apply { input: outer, dependent, arguments, kind, output } => {
            let pushable = matches!(kind, ApplyKind::Inner | ApplyKind::Left | ApplyKind::Semi | ApplyKind::Anti | ApplyKind::Exists | ApplyKind::Mark);
            let (pushed, mut kept) = split_pushable(conjuncts, outer.schema(), pushable);
            let outer = if pushed.is_empty() { decorrelate(Arc::clone(outer), ids)? } else { filter_over(Arc::clone(outer), pushed, ids)? };
            let dependent = decorrelate(Arc::clone(dependent), ids)?;
            if let Some(output) = output && movable(&dependent)
                && let Some(position) = kept.iter().position(|conjunct| tests_mark(conjunct, output.id).is_some())
            {
                let negated = tests_mark(&kept[position], output.id) == Some(true);
                let correlation = Correlation { arguments: arguments.as_slice() };
                let attempt = match kind {
                    ApplyKind::Exists => exists_join(&outer, &dependent, &correlation, output, negated),
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
            let (pushed, kept) = split_pushable(conjuncts, outer.schema(), true);
            let outer = if pushed.is_empty() { decorrelate(Arc::clone(outer), ids)? } else { filter_over(Arc::clone(outer), pushed, ids)? };
            let dependent = decorrelate(Arc::clone(dependent), ids)?;
            let marked = Arc::new(Relation::new(input.schema().clone(), RelationNode::Mark {
                input: outer, dependent, arguments: arguments.clone(), condition: condition.clone(), comparison: comparison.clone(), output: output.clone(),
            })?);
            finish(marked, kept)
        }
        _ => finish(decorrelate(Arc::clone(&input), ids)?, conjuncts),
    }
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
    let conditions: Vec<Expr> = lifted.filters.into_iter().map(|filter| correlation.substitute(filter)).collect();
    if !spans(&conditions, outer.schema(), lifted.base.schema()) { return Ok(None); }
    let joined = Arc::new(Relation::join(Arc::clone(outer), lifted.base, if negated { JoinKind::Anti } else { JoinKind::Semi }, conjunction(conditions))?);
    Ok(Some(Arc::new(joined.extend(vec![NamedExpr { field: output.clone(), expr: Expr::Literal(Value::Bool(!negated)) }])?)))
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
    let comparison = match lifted.computed.iter().find(|named| named.field.id == comparison) {
        Some(named) => named.expr.clone(),
        None => match lifted.base.schema().get(comparison) { Some(field) => field.expression(), None => return Ok(None) },
    };
    let test = if negated { Expr::unary(UnaryOp::Not, Expr::unary(UnaryOp::IsFalse, comparison)) } else { comparison };
    let conditions: Vec<Expr> = lifted.filters.into_iter().chain([test]).map(|condition| correlation.substitute(condition)).collect();
    if !spans(&conditions, outer.schema(), lifted.base.schema()) { return Ok(None); }
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
        // A selection on the unnested relation's columns keeps the same rows whether it is evaluated before or after
        // each row is repeated for its elements.
        RelationNode::Unnest { input, expr, element, ordinality, outer } => {
            if correlation.in_expression(expr) { return Ok(None); }
            let Some(lifted) = lift(input, correlation, false)? else { return Ok(None) };
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
