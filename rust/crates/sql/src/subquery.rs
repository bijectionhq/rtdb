use std::{collections::BTreeMap, sync::Arc};
use rtdb_plan::{AggregateExpr, AggregateFunction, ApplyKind, BinaryOp, ColumnId, Expr, Field, Identifier, NamedExpr, Relation, RelationNode, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use sqlparser::ast;
use crate::bind::{Context, HiddenColumn, error};

pub(crate) struct CorrelationScope { schema: Schema, captures: BTreeMap<ColumnId, (usize, Expr)>, visible: bool }
impl CorrelationScope {
    pub fn new(schema: Schema) -> Self { Self { schema, captures: BTreeMap::new(), visible: true } }
    fn from(schema: Schema) -> Self { Self { schema, captures: BTreeMap::new(), visible: false } }
    pub fn arguments(self) -> Vec<(usize, Expr)> { self.captures.into_values().collect() }
}

pub(crate) enum ExpressionInput<'a> {
    Scalar(&'a Schema),
    Relational(&'a mut Arc<Relation>),
}
impl ExpressionInput<'_> {
    pub fn schema(&self) -> &Schema { match self { Self::Scalar(schema) => schema, Self::Relational(input) => input.schema() } }
    fn relation(&self) -> Result<Arc<Relation>> {
        match self { Self::Scalar(_) => Err(error("Subqueries are not allowed in this context")), Self::Relational(input) => Ok(Arc::clone(input)) }
    }
    fn replace(&mut self, relation: Arc<Relation>) -> Result<()> {
        match self { Self::Scalar(_) => Err(error("Subqueries are not allowed in this context")), Self::Relational(input) => { **input = relation; Ok(()) } }
    }
}

impl Context<'_> {
    pub(crate) fn resolve_column(&mut self, names: &[Identifier], schema: &Schema) -> Result<Expr> {
        let schema = Schema::new(schema.fields().iter().filter(|field| self.hidden_columns.get(&field.id) != Some(&HiddenColumn::Internal)).cloned().collect())?;
        let missing = match schema.resolve(names) {
            Ok(field) => return Ok(field.expression()),
            Err(error) if error.code != "xtdb/column-not-found" => return Err(error),
            Err(error) => error,
        };
        for scope in (0..self.outer_scopes.len()).rev() {
            if !self.outer_scopes[scope].visible { continue; }
            let schema = Schema::new(self.outer_scopes[scope].schema.fields().iter().filter(|field| self.hidden_columns.get(&field.id) != Some(&HiddenColumn::Internal)).cloned().collect())?;
            match schema.resolve(names) {
                Ok(field) => {
                    let field = field.clone();
                    let index = match self.outer_scopes[scope].captures.get(&field.id) {
                        Some((index, _)) => *index,
                        None => {
                            let index = self.internal_parameter()?;
                            self.outer_scopes[scope].captures.insert(field.id, (index, field.expression()));
                            index
                        }
                    };
                    return Ok(Expr::Parameter { index, data_type: field.data_type });
                }
                Err(error) if error.code != "xtdb/column-not-found" => return Err(error),
                Err(_) => {},
            }
        }
        Err(missing)
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
        input.relation()?;
        self.outer_scopes.push(CorrelationScope::new(input.schema().clone()));
        let relation = self.query(query);
        let arguments = self.outer_scopes.pop().ok_or_else(|| error("Subquery correlation scope is unavailable"))?.arguments();
        Ok((relation?, arguments))
    }

    pub(crate) fn scalar_subquery(&mut self, query: &ast::Query, input: &mut ExpressionInput<'_>, exists: bool) -> Result<Expr> {
        let (dependent, arguments) = self.dependent(query, input)?;
        if !exists && dependent.schema().len() != 1 { return Err(error(format!("Subquery arity error: expected a single column, got {}", dependent.schema().len()))); }
        let dependent = if exists { existence_rows(dependent)? } else { dependent };
        self.apply_expression(input, dependent, arguments, if exists { ApplyKind::Exists } else { ApplyKind::Scalar })
    }

    fn apply_expression(&mut self, input: &mut ExpressionInput<'_>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind) -> Result<Expr> {
        let data_type = if kind == ApplyKind::Exists { LogicalType::Scalar(ScalarType::Bool) } else { dependent.schema().fields()[0].data_type.clone().maybe() };
        let field = self.hidden_field(data_type)?;
        let relation = input.relation()?;
        let schema = relation.schema().concat(&Schema::new(vec![field.clone()])?)?;
        input.replace(Arc::new(Relation::new(schema, RelationNode::Apply { input: relation, dependent, arguments, kind, output: Some(field.clone()) })?))?;
        Ok(field.expression())
    }

    fn materialize(&mut self, expression: Expr, input: &mut ExpressionInput<'_>) -> Result<Field> {
        let field = self.hidden_field(expression.data_type())?;
        let relation = input.relation()?;
        let mut expressions: Vec<_> = relation.schema().fields().iter().map(|field| NamedExpr { field: field.clone(), expr: field.expression() }).collect();
        expressions.push(NamedExpr { field: field.clone(), expr: expression });
        input.replace(Arc::new(relation.project(expressions)?))?;
        Ok(field)
    }

    pub(crate) fn quantified_subquery(&mut self, needle: &ast::Expr, query: &ast::Query, op: BinaryOp, negate: bool, input: &mut ExpressionInput<'_>, replacements: &BTreeMap<String, Field>) -> Result<Expr> {
        let needle = self.expr_bound(needle, input, replacements)?;
        let needle = self.materialize(needle, input)?;
        let (dependent, mut arguments) = self.dependent(query, input)?;
        let (needle, capture) = self.parameter_capture(&needle)?;
        arguments.push(capture);
        self.quantified_result(needle, dependent, arguments, op, negate, input)
    }

    pub(crate) fn quantified_array(&mut self, needle: &ast::Expr, array: &ast::Expr, op: BinaryOp, all: bool, input: &mut ExpressionInput<'_>, replacements: &BTreeMap<String, Field>) -> Result<Expr> {
        let needle = self.expr_bound(needle, input, replacements)?;
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
        RelationNode::Project { input, .. } | RelationNode::Sort { input, .. }
        | RelationNode::Apply { input, kind: ApplyKind::Scalar | ApplyKind::Exists | ApplyKind::Pull | ApplyKind::PullMany, .. } => existence_rows(Arc::clone(input)),
        RelationNode::Limit { input, offset, fetch } => Ok(Arc::new(existence_rows(Arc::clone(input))?.limit(offset.clone(), fetch.clone())?)),
        RelationNode::Values { rows } => Ok(Arc::new(Relation::new(Schema::default(), RelationNode::Values { rows: vec![Vec::new(); rows.len()] })?)),
        _ => Ok(relation),
    }
}
