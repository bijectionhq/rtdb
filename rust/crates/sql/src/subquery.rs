use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, ColumnId, Expr, Field, Identifier, JoinKind, NamedExpr, Relation, RelationNode, Schema, UnaryOp};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};
use sqlparser::ast;
use crate::bind::{Context, HiddenColumn, aggregate_relation, error};
use crate::expression::Replacements;

pub(crate) struct CorrelationScope { schema: Schema, captures: BTreeMap<ColumnId, (usize, Expr)>, visible: bool }
impl CorrelationScope {
    pub fn new(schema: Schema) -> Self { Self { schema, captures: BTreeMap::new(), visible: true } }
    fn from(schema: Schema) -> Self { Self { schema, captures: BTreeMap::new(), visible: false } }
    pub fn arguments(self) -> Vec<(usize, Expr)> { self.captures.into_values().collect() }
}

pub(crate) enum SubqueryValue {
    Scalar,
    Exists,
    Array,
    NestOne,
    NestMany,
}

#[derive(Clone, Copy)]
pub(crate) enum ExpressionScope<'a> {
    Columns(&'a Schema),
    TablePeriod,
}
impl ExpressionScope<'_> {
    pub(crate) fn resolve(self, context: &mut Context<'_>, names: &[Identifier]) -> Result<Expr> {
        match self {
            Self::Columns(schema) => context.resolve_column(names, schema),
            Self::TablePeriod => {
                let column = names.last().ok_or_else(|| error("Missing column reference"))?.normalized();
                Err(Anomaly::incorrect("xtdb.sql/no-column-refs-allowed", format!("No column reference allowed in table period specification: {column}")))
            }
        }
    }
}

pub(crate) enum ExpressionInput<'a> {
    Scalar(ExpressionScope<'a>),
    Relational(&'a mut Arc<Relation>),
    Deferred(&'a mut DeferredExpressionInput),
}
impl ExpressionInput<'_> {
    pub(crate) fn scope(&self) -> ExpressionScope<'_> {
        match self {
            Self::Scalar(scope) => *scope,
            Self::Relational(input) => ExpressionScope::Columns(input.schema()),
            Self::Deferred(input) => ExpressionScope::Columns(&input.schema),
        }
    }
    pub(crate) fn relational_schema(&self) -> Result<&Schema> {
        match self {
            Self::Scalar(_) => Err(error("Subqueries are not allowed in this context")),
            Self::Relational(input) => Ok(input.schema()),
            Self::Deferred(input) => Ok(&input.schema),
        }
    }

    pub(crate) fn attach(&mut self, attachment: ExpressionAttachment) -> Result<()> {
        match self {
            Self::Scalar(_) => Err(error("Subqueries are not allowed in this context")),
            Self::Relational(input) => {
                **input = attachment.bind(Arc::clone(input))?;
                Ok(())
            }
            Self::Deferred(input) => {
                input.schema = input.schema.concat(&attachment.output_schema()?)?;
                input.attachments.push(attachment);
                Ok(())
            }
        }
    }
}

pub(crate) struct DeferredExpressionInput {
    schema: Schema,
    attachments: Vec<ExpressionAttachment>,
}

impl DeferredExpressionInput {
    pub(crate) fn new(schema: Schema) -> Self {
        Self { schema, attachments: Vec::new() }
    }

    pub(crate) fn bind(self, input: Arc<Relation>) -> Result<Arc<Relation>> {
        self.attachments.into_iter().try_fold(input, |input, attachment| attachment.bind(input))
    }
}

pub(crate) enum ExpressionAttachment {
    Apply {
        dependent: Arc<Relation>,
        arguments: Vec<(usize, Expr)>,
        kind: ApplyKind,
        output: Field,
    },
    Materialize(NamedExpr),
    Mark {
        dependent: Arc<Relation>,
        arguments: Vec<(usize, Expr)>,
        comparison: Expr,
        output: Field,
    },
    Filter(Expr),
    Window(crate::window::BoundWindow),
}

impl ExpressionAttachment {
    fn output_schema(&self) -> Result<Schema> {
        match self {
            Self::Apply { output, .. } | Self::Mark { output, .. }
            | Self::Materialize(NamedExpr { field: output, .. }) => Schema::new(vec![output.clone()]),
            Self::Filter(_) => Ok(Schema::default()),
            Self::Window(window) => window.output_schema(),
        }
    }

    fn bind(self, input: Arc<Relation>) -> Result<Arc<Relation>> {
        let relation = match self {
            Self::Apply { dependent, arguments, kind, output } => {
                let schema = input.schema().concat(&Schema::new(vec![output.clone()])?)?;
                let node = if kind == ApplyKind::Scalar && arguments.is_empty() {
                    let [source] = dependent.schema().fields() else {
                        return Err(error("Scalar subquery requires a single column"));
                    };
                    let field = Field { data_type: source.data_type.clone(), ..output };
                    let expr = source.expression();
                    let right = Arc::new(dependent.project(vec![NamedExpr { field, expr }])?);
                    RelationNode::Join { left: input, right, kind: JoinKind::Single, condition: None }
                } else {
                    RelationNode::Apply { input, dependent, arguments, kind, output: Some(output) }
                };
                Relation::new(schema, node)?
            }
            Self::Materialize(expression) => {
                let mut expressions = input.schema().fields().iter()
                    .map(|field| NamedExpr { field: field.clone(), expr: field.expression() })
                    .collect::<Vec<_>>();
                expressions.push(expression);
                input.project(expressions)?
            }
            Self::Mark { dependent, arguments, comparison, output } => {
                let schema = input.schema().concat(&Schema::new(vec![output.clone()])?)?;
                Relation::new(schema, RelationNode::Mark { input, dependent, arguments, condition: None, comparison, output })?
            }
            Self::Filter(predicate) => input.filter(predicate)?,
            Self::Window(window) => return window.bind(input),
        };
        Ok(Arc::new(relation))
    }
}

impl Context<'_> {
    pub(crate) fn resolve_column(&mut self, names: &[Identifier], schema: &Schema) -> Result<Expr> {
        let schema = Schema::new(schema.fields().iter().filter(|field| self.hidden_columns.get(&field.id) != Some(&HiddenColumn::Internal)).cloned().collect())?;
        let missing = match schema.resolve(names) {
            Ok(field) => {
                self.record_column_demand(field.id);
                return Ok(field.expression());
            }
            Err(error) if error.code != "xtdb/column-not-found" => return Err(error),
            Err(error) => error,
        };
        for scope in (0..self.outer_scopes.len()).rev() {
            if !self.outer_scopes[scope].visible { continue; }
            let schema = Schema::new(self.outer_scopes[scope].schema.fields().iter().filter(|field| self.hidden_columns.get(&field.id) != Some(&HiddenColumn::Internal)).cloned().collect())?;
            match schema.resolve(names) {
                Ok(field) => {
                    return self.capture_outer(scope, field);
                }
                Err(error) if error.code != "xtdb/column-not-found" => return Err(error),
                Err(_) => {},
            }
        }
        Err(missing)
    }

    fn capture_outer(&mut self, scope: usize, field: &Field) -> Result<Expr> {
        self.record_column_demand(field.id);
        let index = match self.outer_scopes[scope].captures.get(&field.id) {
            Some((index, _)) => *index,
            None => {
                let index = self.internal_parameter()?;
                self.allocate_symbol()?;
                self.outer_scopes[scope].captures.insert(field.id, (index, field.expression()));
                index
            }
        };
        Ok(Expr::Parameter { index, data_type: field.data_type.clone() })
    }

    pub(crate) fn wildcard_columns(&mut self, schema: &Schema, qualifier: Option<&str>, excludes: &BTreeSet<String>) -> Result<Vec<(Field, Expr)>> {
        let selected = wildcard_fields(schema, qualifier, excludes, &self.hidden_columns);
        if !selected.is_empty() {
            return Ok(selected.into_iter().map(|field| {
                self.record_column_demand(field.id);
                let expr = field.expression();
                (field, expr)
            }).collect());
        }
        for scope in (0..self.outer_scopes.len()).rev() {
            if !self.outer_scopes[scope].visible { continue; }
            let selected = wildcard_fields(&self.outer_scopes[scope].schema, qualifier, excludes, &self.hidden_columns);
            if !selected.is_empty() { return selected.into_iter().map(|field| { let expr = self.capture_outer(scope, &field)?; Ok((field, expr)) }).collect(); }
        }
        Ok(Vec::new())
    }

    pub(crate) fn wildcard_source_id(&mut self, names: &[Identifier], schema: &Schema) -> Result<Option<ColumnId>> {
        let find = |schema: &Schema| -> Result<Option<ColumnId>> {
            let schema = Schema::new(schema.fields().iter().filter(|field| self.hidden_columns.get(&field.id) != Some(&HiddenColumn::Internal)).cloned().collect())?;
            match schema.resolve(names) { Ok(field) => Ok(Some(field.id)), Err(error) if error.code == "xtdb/column-not-found" => Ok(None), Err(error) => Err(error) }
        };
        let mut id = find(schema)?;
        if id.is_none() {
            for scope in self.outer_scopes.iter().rev().filter(|scope| scope.visible) {
                id = find(&scope.schema)?;
                if id.is_some() { break; }
            }
        }
        if let Some(id) = id {
            self.record_column_demand(id);
        }
        Ok(id)
    }

    fn internal_parameter(&mut self) -> Result<usize> {
        let index = self.next_parameter;
        self.next_parameter = index.checked_add(1).ok_or_else(|| error("Parameter identifier space is exhausted"))?;
        Ok(index)
    }

    pub(crate) fn parameter_capture(&mut self, field: &Field) -> Result<(Expr, (usize, Expr))> {
        let index = self.internal_parameter()?;
        Ok((Expr::Parameter { index, data_type: field.data_type.clone() }, (index, field.expression())))
    }

    pub(crate) fn from_scope<T>(&mut self, schema: Schema, bind: impl FnOnce(&mut Self, usize) -> Result<T>) -> Result<(T, Vec<(usize, Expr)>)> {
        let start = self.outer_scopes.len();
        self.outer_scopes.push(CorrelationScope::from(schema));
        let result = bind(self, start);
        let arguments = self.outer_scopes.pop().ok_or_else(|| error("FROM correlation scope is unavailable"))?.arguments();
        Ok((result?, arguments))
    }

    pub(crate) fn lateral_scope<T>(&mut self, start: usize, bind: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        if start == self.outer_scopes.len() { return bind(self); }
        self.with_visible_outer_scopes(start, bind)
    }

    pub(crate) fn with_projection_scope<T>(&mut self, bind: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.with_visible_outer_scopes(self.outer_scopes.len(), bind)
    }

    fn with_visible_outer_scopes<T>(&mut self, start: usize, bind: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        let previous = self.outer_scopes.iter().map(|scope| scope.visible).collect::<Vec<_>>();
        for (index, scope) in self.outer_scopes.iter_mut().enumerate() { scope.visible = index >= start; }
        let result = bind(self);
        for (scope, visible) in self.outer_scopes.iter_mut().zip(previous) { scope.visible = visible; }
        result
    }

    fn hidden_field(&mut self, data_type: LogicalType) -> Result<Field> {
        let mut field = self.binder.field("", None, data_type)?;
        field.name = format!("_sq_{}", field.id.0);
        self.hidden_columns.insert(field.id,HiddenColumn::Internal);
        Ok(field)
    }

    fn dependent(&mut self, query: &ast::Query, input: &ExpressionInput<'_>) -> Result<(Arc<Relation>, Vec<(usize, Expr)>)> {
        self.outer_scopes.push(CorrelationScope::new(input.relational_schema()?.clone()));
        let relation = self.query(query);
        let arguments = self.outer_scopes.pop().ok_or_else(|| error("Subquery correlation scope is unavailable"))?.arguments();
        Ok((relation?, arguments))
    }

    pub(crate) fn query_value(&mut self, query: &ast::Query, input: &mut ExpressionInput<'_>, value: SubqueryValue) -> Result<Expr> {
        let (dependent, arguments) = self.dependent(query, input)?;
        let nest = match value {
            SubqueryValue::Exists => return self.apply_expression(input, existence_rows(dependent)?, arguments, ApplyKind::Exists),
            SubqueryValue::NestOne => Some(ApplyKind::Pull),
            SubqueryValue::NestMany => Some(ApplyKind::PullMany),
            SubqueryValue::Scalar | SubqueryValue::Array => None,
        };
        if let Some(kind) = nest {
            let document = LogicalType::Struct(dependent.schema().fields().iter().map(|field| (field.name.clone(), field.data_type.clone())).collect());
            let output = self.hidden_field(if kind == ApplyKind::PullMany { LogicalType::list(document).maybe() } else { document.maybe() })?;
            let expression = output.expression();
            input.attach(ExpressionAttachment::Apply { dependent, arguments, kind, output })?;
            return Ok(expression);
        }
        let [field] = dependent.schema().fields() else {
            return Err(error(format!("Subquery arity error: expected a single column, got {}", dependent.schema().len())));
        };
        let dependent = match value {
            SubqueryValue::Array => {
                let aggregate = AggregateExpr {
                    field: self.hidden_field(LogicalType::list(field.data_type.clone()))?,
                    function: AggregateFunction::VecAgg,
                    argument: Some(field.expression()),
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                };
                aggregate_relation(dependent, Vec::new(), vec![aggregate])?
            }
            _ => dependent,
        };
        self.apply_expression(input, dependent, arguments, ApplyKind::Scalar)
    }

    fn apply_expression(&mut self, input: &mut ExpressionInput<'_>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind) -> Result<Expr> {
        let data_type = if kind == ApplyKind::Exists { LogicalType::Scalar(ScalarType::Bool) } else { dependent.schema().fields()[0].data_type.clone().maybe() };
        let output = self.hidden_field(data_type)?;
        let expression = output.expression();
        input.attach(ExpressionAttachment::Apply { dependent, arguments, kind, output })?;
        Ok(expression)
    }

    fn materialize(&mut self, expression: Expr, input: &mut ExpressionInput<'_>) -> Result<Field> {
        let field = self.hidden_field(expression.data_type())?;
        input.attach(ExpressionAttachment::Materialize(NamedExpr { field: field.clone(), expr: expression }))?;
        Ok(field)
    }

    pub(crate) fn in_list(&mut self, needle: Expr, values: &[ast::Expr], input: &mut ExpressionInput<'_>) -> Result<Expr> {
        if values.is_empty() { return Err(error("IN requires at least one candidate")); }
        for value in values { crate::expression::validate_in_list_item(value)?; }
        let constructors = values.iter().map(in_row).collect::<Result<Vec<_>>>()?;
        let scalar = values.len() <= 8 && constructors.iter().all(Option::is_none);
        if scalar {
            let mut scalar = ExpressionInput::Scalar(input.scope());
            let values = values.iter().map(|value| self.expr_bound(value, &mut scalar, &Replacements::new())).collect::<Result<Vec<_>>>()?;
            return Ok(values.into_iter().map(|value| Expr::binary(BinaryOp::Eq, needle.clone(), value))
                .reduce(|left, right| Expr::binary(BinaryOp::Or, left, right)).unwrap());
        }
        if let Some(domain) = self.id_list_domain(&needle, values, input) {
            let mut scalar = ExpressionInput::Scalar(input.scope());
            let values = values.iter().map(|value| self.expr_bound(value, &mut scalar, &Replacements::new())).collect::<Result<Vec<_>>>()?;
            if values.iter().all(|value| id_domain(&value.data_type()) == Some(domain)) {
                let mut equalities = values.into_iter().map(|value| Expr::binary(BinaryOp::Eq, needle.clone(), value)).collect::<Vec<_>>();
                return Ok(disjunction(&mut equalities));
            }
        }
        let needle = self.materialize(needle, input)?;
        self.outer_scopes.push(CorrelationScope::new(input.relational_schema()?.clone()));
        let rows = values.iter().zip(constructors).map(|(value, constructor)| {
            let value = constructor.unwrap_or(value);
            self.expr(value, &Schema::default(), &Replacements::new()).map(|expr| vec![expr])
        }).collect::<Result<Vec<_>>>();
        let arguments = self.outer_scopes.pop().ok_or_else(|| error("IN correlation scope is unavailable"))?.arguments();
        let rows = rows?;
        let candidate = self.hidden_field(LogicalType::join_all(&rows.iter().map(|row| row[0].data_type()).collect::<Vec<_>>()))?;
        let comparison = Expr::binary(BinaryOp::Eq, needle.expression(), candidate.expression());
        let dependent = Arc::new(Relation::new(Schema::new(vec![candidate])?, RelationNode::Values { rows })?);
        let output = self.hidden_field(LogicalType::Scalar(ScalarType::Bool).maybe())?;
        let expression = output.expression();
        input.attach(ExpressionAttachment::Mark { dependent, arguments, comparison, output })?;
        Ok(expression)
    }

    /// The ID kind of `needle` where it is an `_id` column of a single kind and `values` is a list of literals and
    /// parameters short enough to bind as equalities; the values' own kinds are known once they are bound.
    fn id_list_domain(&self, needle: &Expr, values: &[ast::Expr], input: &ExpressionInput<'_>) -> Option<IdDomain> {
        if values.len() > ID_LIST_LIMIT || !values.iter().all(|value| matches!(value, ast::Expr::Value(_) | ast::Expr::TypedString(_))) { return None; }
        let Expr::Column { id, data_type } = needle else { return None; };
        let field = input.relational_schema().ok()?.get(*id)?;
        if field.name != "_id" { return None; }
        id_domain(data_type)
    }

    pub(crate) fn quantified_subquery(&mut self, needle: &ast::Expr, query: &ast::Query, op: BinaryOp, negate: bool, input: &mut ExpressionInput<'_>, replacements: &Replacements) -> Result<Expr> {
        let needle = self.expr_bound(needle, input, replacements)?;
        let existing = match &needle { Expr::Column { id, .. } => input.relational_schema()?.get(*id).cloned(), _ => None };
        let needle = match existing { Some(field) => field, None => self.materialize(needle, input)? };
        let (dependent, mut arguments) = self.dependent(query, input)?;
        if arguments.is_empty() {
            let [candidate] = dependent.schema().fields() else { return Err(error(format!("Subquery arity error: expected a single column, got {}", dependent.schema().len()))); };
            let comparison = Expr::binary(op, needle.expression(), candidate.expression());
            let output = self.hidden_field(LogicalType::Scalar(ScalarType::Bool).maybe())?;
            let result = output.expression();
            input.attach(ExpressionAttachment::Mark { dependent, arguments, comparison, output })?;
            return Ok(if negate { Expr::unary(UnaryOp::Not, result) } else { result });
        }
        let (needle, capture) = self.parameter_capture(&needle)?;
        arguments.push(capture);
        self.quantified_result(needle, dependent, arguments, op, negate, input)
    }

    pub(crate) fn quantified_array(&mut self, needle: &ast::Expr, array: &ast::Expr, op: BinaryOp, all: bool, input: &mut ExpressionInput<'_>, replacements: &Replacements) -> Result<Expr> {
        let needle = self.expr_bound(needle, input, replacements)?;
        if !all && op == BinaryOp::Eq
            && matches!(array, ast::Expr::Value(value) if matches!(value.value, ast::Value::Placeholder(_)))
            && let Expr::Column { id, data_type } = &needle
            && id_domain(data_type).is_some()
            && input.relational_schema().ok().and_then(|schema| schema.get(*id)).is_some_and(|field| field.name == "_id")
        {
            let args = vec![needle, self.expr_bound(array, input, replacements)?];
            let return_type = rtdb_plan::function_type_with_args("_id_eq_any", &args)?;
            return Ok(Expr::Call { name: "_id_eq_any".into(), args, return_type });
        }
        let needle = self.materialize(needle, input)?;
        let array = self.expr_bound(array, input, replacements)?;
        let array = self.materialize(array, input)?;
        let mut elements = Vec::new();
        for leg in array.data_type.legs() {
            match leg {
                LogicalType::List { element, .. } => elements.push(*element),
                LogicalType::Null | LogicalType::Nothing => {},
                _ => return Err(error("Quantified comparison requires an array or subquery")),
            }
        }
        let element = self.hidden_field(LogicalType::join_all(&elements).maybe())?;
        let (array, array_capture) = self.parameter_capture(&array)?;
        let (needle, needle_capture) = self.parameter_capture(&needle)?;
        let expr = Expr::Coalesce(vec![array, Expr::List(vec![Expr::Literal(Value::Null)])]);
        let dependent = Arc::new(Relation::new(Schema::new(vec![element.clone()])?, RelationNode::Unnest { input: Arc::new(Relation::unit()), expr, element, ordinality: None, outer: false })?);
        self.quantified_result(needle, dependent, vec![needle_capture, array_capture], if all { negated_operator(op)? } else { op }, all, input)
    }

    fn quantified_result(&mut self, needle: Expr, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, op: BinaryOp, negate: bool, input: &mut ExpressionInput<'_>) -> Result<Expr> {
        let [candidate] = dependent.schema().fields() else { return Err(error(format!("Subquery arity error: expected a single column, got {}", dependent.schema().len()))); };
        let comparison = Expr::binary(op, needle, candidate.expression());
        let comparison_field = self.hidden_field(comparison.data_type())?;
        let dependent = Arc::new(dependent.project(vec![NamedExpr { field: comparison_field.clone(), expr: comparison }])?);
        let mut aggregates = Vec::new();
        for operation in [UnaryOp::IsTrue, UnaryOp::IsUnknown] {
            aggregates.push(AggregateExpr { field: self.hidden_field(LogicalType::Scalar(ScalarType::I64))?, function: AggregateFunction::CountStar,
                argument: None, distinct: false, filter: Some(Expr::unary(operation, comparison_field.expression())), order_by: Vec::new() });
        }
        let truth = aggregates[0].field.expression();
        let unknown = aggregates[1].field.expression();
        let schema = Schema::new(aggregates.iter().map(|aggregate| aggregate.field.clone()).collect())?;
        let dependent = Arc::new(Relation::new(schema, RelationNode::Aggregate { input: dependent, groups: Vec::new(), aggregates })?);
        let field = self.hidden_field(LogicalType::Scalar(ScalarType::Bool).maybe())?;
        let expression = Expr::Case { branches: vec![
            (Expr::binary(BinaryOp::Gt, truth, Expr::Literal(Value::I64(0))), Expr::Literal(Value::Bool(true))),
            (Expr::binary(BinaryOp::Gt, unknown, Expr::Literal(Value::I64(0))), Expr::Literal(Value::Null)),
        ], otherwise: Box::new(Expr::Literal(Value::Bool(false))) };
        let dependent = Arc::new(dependent.project(vec![NamedExpr { field, expr: expression }])?);
        let result = self.apply_expression(input, dependent, arguments, ApplyKind::Scalar)?;
        Ok(if negate { Expr::unary(UnaryOp::Not, result) } else { result })
    }
}

/// The longest `_id IN (...)` list of constants bound as equalities, which a scan narrows to those IIDs.
/// A longer list is compared through a candidate relation, whose cost per row does not grow with the list.
const ID_LIST_LIMIT: usize = 4096;

/// The kinds of value an entity ID takes. Equality within one kind needs no coercion, so comparing a column of that
/// kind with constants of it gives the same answers as comparing it with a candidate relation of them.
#[derive(Clone, Copy, PartialEq, Eq)]
enum IdDomain { Integer, String, Keyword, Uuid }

fn id_domain(data_type: &LogicalType) -> Option<IdDomain> {
    match data_type {
        LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) => Some(IdDomain::Integer),
        LogicalType::Scalar(ScalarType::Utf8) => Some(IdDomain::String),
        LogicalType::Scalar(ScalarType::Keyword) => Some(IdDomain::Keyword),
        LogicalType::Scalar(ScalarType::Uuid) => Some(IdDomain::Uuid),
        _ => None,
    }
}

/// An OR of `clauses` in their order, nested evenly so that its depth grows with the logarithm of their count.
fn disjunction(clauses: &mut Vec<Expr>) -> Expr {
    fn build(clauses: &mut std::vec::Drain<'_, Expr>, count: usize) -> Expr {
        if count == 1 { return clauses.next().expect("the count was taken from the clauses"); }
        let left = build(clauses, count / 2);
        Expr::binary(BinaryOp::Or, left, build(clauses, count - count / 2))
    }
    let count = clauses.len();
    build(&mut clauses.drain(..), count)
}

fn in_row(value: &ast::Expr) -> Result<Option<&ast::Expr>> {
    let arity = |count| error(format!("Subquery arity error: expected a single column, got {count}"));
    match value {
        ast::Expr::Tuple(values) => match values.as_slice() { [value] => Ok(Some(value)), _ => Err(arity(values.len())) },
        ast::Expr::Function(function) => {
            let names = crate::bind::identifiers(&function.name)?;
            if names.len() != 1 || names[0].quoted || names[0].normalized() != "row" { return Ok(None); }
            if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) { return Err(error("ROW constructor does not accept function modifiers")); }
            let ast::FunctionArguments::List(arguments) = &function.args else { return Err(arity(0)); };
            if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() { return Err(error("ROW constructor does not accept argument modifiers")); }
            if arguments.args.len() != 1 { return Err(arity(arguments.args.len())); }
            match &arguments.args[0] {
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Ok(Some(expr)),
                _ => Err(error("ROW constructor requires an expression")),
            }
        }
        _ => Ok(None),
    }
}

fn wildcard_fields(schema: &Schema, qualifier: Option<&str>, excludes: &BTreeSet<String>, hidden: &BTreeMap<ColumnId, HiddenColumn>) -> Vec<Field> {
    schema.fields().iter().filter(|field| !hidden.contains_key(&field.id) && !excludes.contains(&field.name)
        && qualifier.is_none_or(|qualifier| field.qualifier.as_ref().is_some_and(|scope| scope.alias() == qualifier))).cloned().collect()
}

pub(crate) fn negated_operator(op: BinaryOp) -> Result<BinaryOp> {
    Ok(match op {
        BinaryOp::Eq => BinaryOp::NotEq, BinaryOp::NotEq => BinaryOp::Eq,
        BinaryOp::Lt => BinaryOp::GtEq, BinaryOp::LtEq => BinaryOp::Gt,
        BinaryOp::Gt => BinaryOp::LtEq, BinaryOp::GtEq => BinaryOp::Lt,
        _ => return Err(error("Quantified comparison requires a comparison operator")),
    })
}

fn existence_rows(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    match relation.node() {
        RelationNode::Project { input, .. } | RelationNode::Rename { input, .. } | RelationNode::Sort { input, .. }
        | RelationNode::Mark { input, .. }
        | RelationNode::Join { left: input, kind: JoinKind::Single, condition: None, .. }
        | RelationNode::Apply { input, kind: ApplyKind::Scalar | ApplyKind::Exists | ApplyKind::Pull | ApplyKind::PullMany, .. } => existence_rows(Arc::clone(input)),
        RelationNode::Limit { input, offset, fetch } => Ok(Arc::new(existence_rows(Arc::clone(input))?.limit(offset.clone(), fetch.clone())?)),
        RelationNode::Values { rows } => Ok(Arc::new(Relation::new(Schema::default(), RelationNode::Values { rows: vec![Vec::new(); rows.len()] })?)),
        _ => Ok(relation),
    }
}
