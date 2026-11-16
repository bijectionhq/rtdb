use std::collections::BTreeMap;

use rtdb_plan::{AggregateExpr, AggregateFunction, BinaryOp, Expr, Field, Schema, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use sqlparser::ast::{self as ast, Spanned};

use crate::bind::{Context, error, identifier, identifiers, unsupported};
use crate::subquery::{ExpressionInput, negated_operator};
use std::sync::Arc;
use rtdb_plan::Relation;

impl Context<'_> {
    pub(crate) fn expr(&mut self, expression: &ast::Expr, schema: &Schema, replacements: &BTreeMap<String, Field>) -> Result<Expr> {
        self.expr_bound(expression, &mut ExpressionInput::Scalar(schema), replacements)
    }

    pub(crate) fn relational_expr(&mut self, expression: &ast::Expr, input: &mut Arc<Relation>, replacements: &BTreeMap<String, Field>) -> Result<Expr> {
        self.expr_bound(expression, &mut ExpressionInput::Relational(input), replacements)
    }

    pub(crate) fn expr_bound(&mut self, expression: &ast::Expr, input: &mut ExpressionInput<'_>, replacements: &BTreeMap<String, Field>) -> Result<Expr> {
        self.expr_inner(expression, input, replacements).map_err(|anomaly| {
            if anomaly.context.contains_key("span") { anomaly }
            else { let span = crate::parse::source_span(self.source, expression.span()); anomaly.with_context("span", serde_json::json!({"start":span.start,"end":span.end})).with_context("position", serde_json::json!(span.start + 1)) }
        })
    }

    fn expr_inner(&mut self, expression: &ast::Expr, input: &mut ExpressionInput<'_>, replacements: &BTreeMap<String, Field>) -> Result<Expr> {
        if let Some(field) = replacements.get(&expression.to_string()) { return Ok(field.expression()); }
        Ok(match expression {
            ast::Expr::Subquery(query) => self.scalar_subquery(query, input, false)?,
            ast::Expr::Exists { subquery, negated } => {
                let expression = self.scalar_subquery(subquery, input, true)?;
                if *negated { Expr::unary(UnaryOp::Not, expression) } else { expression }
            }
            ast::Expr::InSubquery { expr, subquery, negated } => self.quantified_subquery(expr, subquery, BinaryOp::Eq, *negated, input, replacements)?,
            ast::Expr::AnyOp { left, compare_op, right, .. } | ast::Expr::AllOp { left, compare_op, right } => {
                let all = matches!(expression, ast::Expr::AllOp { .. });
                let op = binary_operator(compare_op)?;
                negated_operator(op)?;
                if let ast::Expr::Subquery(query) = right.as_ref() {
                    self.quantified_subquery(left, query, if all { negated_operator(op)? } else { op }, all, input, replacements)?
                } else { self.quantified_array(left, right, op, all, input, replacements)? }
            }
            ast::Expr::Identifier(name) => {
                let name = identifier(name);
                if !name.quoted && ["current_timestamp", "current_date", "current_time", "localtimestamp", "localtime"].contains(&name.normalized().as_str()) { call(name.normalized(), Vec::new())? }
                else { self.resolve_column(&[name], input.schema())? }
            }
            ast::Expr::CompoundIdentifier(names) => {
                let names: Vec<_> = names.iter().map(identifier).collect();
                let mut found = None;
                for width in (1..=names.len().min(2)).rev() {
                    match self.resolve_column(&names[..width], input.schema()) {
                        Ok(expr) => { found = Some((expr, width)); break; }
                        Err(error) if error.code == "xtdb/ambiguous-column" => return Err(error),
                        Err(_) => {},
                    }
                }
                let (mut expr, consumed) = found.ok_or_else(|| error(format!("Unbound column: {expression}")))?;
                for name in &names[consumed..] { expr = field_access(expr, name.normalized()); }
                expr
            }
            ast::Expr::Value(value) => match &value.value {
                ast::Value::Placeholder(parameter) => {
                    let index = parameter.strip_prefix('$').and_then(|parameter| parameter.parse::<usize>().ok()).and_then(|index| index.checked_sub(1)).ok_or_else(|| error("Invalid parameter index"))?;
                    self.binder.parameter(index, self.options.parameter_types.get(&index).cloned().unwrap_or(LogicalType::Nothing))
                }
                value => Expr::Literal(crate::types::literal(value)?),
            },
            ast::Expr::TypedString(value) => Expr::Cast { expr: Box::new(Expr::Literal(crate::types::literal(&value.value.value)?)), target: crate::types::cast_target(&value.data_type)? },
            ast::Expr::Nested(expr) => self.expr_bound(expr, input, replacements)?,
            ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } if matches!(expr.as_ref(), ast::Expr::Value(value) if matches!(&value.value, ast::Value::Number(value, _) if value == "9223372036854775808")) => Expr::Literal(Value::I64(i64::MIN)),
            ast::Expr::UnaryOp { op, expr } => {
                let expr = self.expr_bound(expr, input, replacements)?;
                match op { ast::UnaryOperator::Plus => expr, ast::UnaryOperator::Minus => Expr::unary(UnaryOp::Negate, expr), ast::UnaryOperator::Not => Expr::unary(UnaryOp::Not, expr), ast::UnaryOperator::BitwiseNot => Expr::unary(UnaryOp::BitNot, expr), _ => return Err(unsupported(format!("Unary operator: {op}"))) }
            }
            ast::Expr::BinaryOp { left, op, right } => {
                let left = self.expr_bound(left, input, replacements)?;
                let right = self.expr_bound(right, input, replacements)?;
                Expr::binary(binary_operator(op)?, left, right)
            }
            ast::Expr::IsNull(expr) => Expr::unary(UnaryOp::IsNull, self.expr_bound(expr, input, replacements)?),
            ast::Expr::IsNotNull(expr) => Expr::unary(UnaryOp::IsNotNull, self.expr_bound(expr, input, replacements)?),
            ast::Expr::IsTrue(expr) => Expr::unary(UnaryOp::IsTrue, self.expr_bound(expr, input, replacements)?),
            ast::Expr::IsFalse(expr) => Expr::unary(UnaryOp::IsFalse, self.expr_bound(expr, input, replacements)?),
            ast::Expr::IsUnknown(expr) => Expr::unary(UnaryOp::IsUnknown, self.expr_bound(expr, input, replacements)?),
            ast::Expr::IsNotTrue(expr) => Expr::unary(UnaryOp::Not, Expr::unary(UnaryOp::IsTrue, self.expr_bound(expr, input, replacements)?)),
            ast::Expr::IsNotFalse(expr) => Expr::unary(UnaryOp::Not, Expr::unary(UnaryOp::IsFalse, self.expr_bound(expr, input, replacements)?)),
            ast::Expr::IsNotUnknown(expr) => Expr::unary(UnaryOp::Not, Expr::unary(UnaryOp::IsUnknown, self.expr_bound(expr, input, replacements)?)),
            ast::Expr::IsDistinctFrom(left, right) => Expr::binary(BinaryOp::IsDistinctFrom, self.expr_bound(left, input, replacements)?, self.expr_bound(right, input, replacements)?),
            ast::Expr::IsNotDistinctFrom(left, right) => Expr::binary(BinaryOp::IsNotDistinctFrom, self.expr_bound(left, input, replacements)?, self.expr_bound(right, input, replacements)?),
            ast::Expr::Between { expr, low, high, negated } => {
                let expr = self.expr_bound(expr, input, replacements)?;
                let predicate = Expr::binary(BinaryOp::And, Expr::binary(BinaryOp::GtEq, expr.clone(), self.expr_bound(low, input, replacements)?), Expr::binary(BinaryOp::LtEq, expr, self.expr_bound(high, input, replacements)?));
                if *negated { Expr::unary(UnaryOp::Not, predicate) } else { predicate }
            }
            ast::Expr::InList { expr, list, negated } => Expr::In { expr: Box::new(self.expr_bound(expr, input, replacements)?), values: list.iter().map(|value| self.expr_bound(value, input, replacements)).collect::<Result<_>>()?, negated: *negated },
            ast::Expr::Like { expr, pattern, escape_char, negated, any } | ast::Expr::ILike { expr, pattern, escape_char, negated, any } => {
                if *any { return Err(unsupported("LIKE ANY")); }
                let escape = escape_char.as_ref().map(|escape| {
                    let Expr::Literal(Value::Utf8(value)) = self.expr_bound(escape, input, replacements)? else { return Err(error("LIKE escape must be a literal character")); };
                    let mut chars = value.chars(); let first = chars.next().ok_or_else(|| error("LIKE escape must contain one character"))?;
                    if chars.next().is_some() { return Err(error("LIKE escape must contain one character")); }
                    Ok(first)
                }).transpose()?;
                Expr::Like { expr: Box::new(self.expr_bound(expr, input, replacements)?), pattern: Box::new(self.expr_bound(pattern, input, replacements)?), escape, case_insensitive: matches!(expression, ast::Expr::ILike { .. }), negated: *negated }
            }
            ast::Expr::Cast { expr, data_type, kind, format } => {
                if format.is_some() || !matches!(kind, ast::CastKind::Cast | ast::CastKind::DoubleColon) { return Err(unsupported("TRY_CAST or cast format")); }
                Expr::Cast { expr: Box::new(self.expr_bound(expr, input, replacements)?), target: crate::types::cast_target(data_type)? }
            }
            ast::Expr::Case { operand, conditions, else_result, .. } => {
                let operand = operand.as_ref().map(|value| self.expr_bound(value, input, replacements)).transpose()?;
                let branches = conditions.iter().map(|when| {
                    let condition = self.expr_bound(&when.condition, input, replacements)?;
                    Ok((match &operand { Some(operand) => Expr::binary(BinaryOp::Eq, operand.clone(), condition), None => condition }, self.expr_bound(&when.result, input, replacements)?))
                }).collect::<Result<_>>()?;
                Expr::Case { branches, otherwise: Box::new(else_result.as_ref().map(|value| self.expr_bound(value, input, replacements)).transpose()?.unwrap_or(Expr::Literal(Value::Null))) }
            }
            ast::Expr::Function(function) => {
                if aggregate_function(&function.name.to_string()).is_some() { return Err(error("Aggregate is not valid in this expression scope")); }
                if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) { return Err(unsupported("Window function or scalar function modifiers")); }
                if let ast::FunctionArguments::List(arguments) = &function.args {
                    if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() { return Err(unsupported("Scalar function argument modifiers")); }
                }
                let args = function_arguments(function)?.iter().map(|arg| match arg { ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => self.expr_bound(expr, input, replacements), _ => Err(unsupported("Named function arguments or scalar wildcard")) }).collect::<Result<Vec<_>>>()?;
                let names = identifiers(&function.name)?;
                let name = names.iter().map(|name| name.normalized()).collect::<Vec<_>>().join(".");
                if name == "coalesce" { Expr::Coalesce(args) } else { call(name, args)? }
            }
            ast::Expr::Array(array) => Expr::List(array.elem.iter().map(|expr| self.expr_bound(expr, input, replacements)).collect::<Result<_>>()?),
            ast::Expr::Dictionary(fields) => Expr::Struct(fields.iter().map(|field| Ok((identifier(&field.key).normalized(), self.expr_bound(&field.value, input, replacements)?))).collect::<Result<_>>()?),
            ast::Expr::CompoundFieldAccess { root, access_chain } => {
                let mut expr = self.expr_bound(root, input, replacements)?;
                for access in access_chain {
                    match access {
                        ast::AccessExpr::Dot(ast::Expr::Identifier(name)) => expr = field_access(expr, identifier(name).normalized()),
                        ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => {
                            let index = Expr::binary(BinaryOp::Subtract, self.expr_bound(index, input, replacements)?, Expr::Literal(Value::I64(1)));
                            let types: Vec<_> = expr.data_type().legs().into_iter().filter_map(|ty| match ty { LogicalType::List { element, .. } => Some(*element), _ => None }).collect();
                            let data_type = LogicalType::join_all(&types).maybe();
                            expr = Expr::Subscript { expr: Box::new(expr), index: Box::new(index), data_type };
                        }
                        _ => return Err(unsupported("Nested field access or array slices")),
                    }
                }
                expr
            }
            ast::Expr::Position { expr, r#in } => call("position".into(), vec![self.expr_bound(expr, input, replacements)?, self.expr_bound(r#in, input, replacements)?])?,
            ast::Expr::Substring { expr, substring_from, substring_for, .. } => {
                let mut args = vec![self.expr_bound(expr, input, replacements)?, substring_from.as_ref().map(|expr| self.expr_bound(expr, input, replacements)).transpose()?.unwrap_or(Expr::Literal(Value::I64(1)))];
                if let Some(length) = substring_for { args.push(self.expr_bound(length, input, replacements)?); }
                call("substring".into(), args)?
            }
            ast::Expr::Trim { expr, trim_where, trim_what, trim_characters } => {
                if trim_characters.is_some() { return Err(unsupported("TRIM character list")); }
                let mut args = vec![self.expr_bound(expr, input, replacements)?];
                if let Some(what) = trim_what { args.push(self.expr_bound(what, input, replacements)?); }
                let name = match trim_where { Some(ast::TrimWhereField::Leading) => "trim_leading", Some(ast::TrimWhereField::Trailing) => "trim_trailing", _ => "trim" };
                call(name.into(), args)?
            }
            _ => return Err(unsupported(format!("SQL expression: {expression}"))),
        })
    }

    pub(crate) fn aggregate(&mut self, function: &ast::Function, input: &mut Arc<Relation>, index: usize) -> Result<AggregateExpr> {
        if function.over.is_some() || function.null_treatment.is_some() { return Err(unsupported("Aggregate window or null treatment")); }
        let mut kind = aggregate_function(&function.name.to_string()).ok_or_else(|| error("Unknown aggregate"))?;
        let args = function_arguments(function)?;
        let argument = match args.as_slice() {
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)] if kind == AggregateFunction::Count => { kind = AggregateFunction::CountStar; None }
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))] => Some(self.relational_expr(expr, input, &BTreeMap::new())?),
            _ => return Err(error("Aggregate requires one expression or COUNT(*)")),
        };
        let mut order_by = function.within_group.iter().map(|order| self.relational_sort_expr(order, input, &BTreeMap::new())).collect::<Result<Vec<_>>>()?;
        let distinct = if let ast::FunctionArguments::List(arguments) = &function.args {
            for clause in &arguments.clauses {
                match clause { ast::FunctionArgumentClause::OrderBy(order) => for expression in order { order_by.push(self.relational_sort_expr(expression, input, &BTreeMap::new())?); }, _ => return Err(unsupported("Aggregate argument modifier")) }
            }
            arguments.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
        } else { false };
        let argument_type = argument.as_ref().map(Expr::data_type).unwrap_or(LogicalType::Nothing);
        let data_type = aggregate_type(kind, &argument_type)?;
        Ok(AggregateExpr { field: self.binder.field(format!("_aggregate_{index}"), None, data_type)?, function: kind, argument, distinct,
            filter: function.filter.as_ref().map(|expr| self.relational_expr(expr, input, &BTreeMap::new())).transpose()?, order_by })
    }
}

fn aggregate_type(function: AggregateFunction, argument: &LogicalType) -> Result<LogicalType> {
    use AggregateFunction as A;
    use LogicalType as T;
    use ScalarType as S;
    let promoted = || T::least_upper_bound(std::slice::from_ref(argument))
        .ok_or_else(|| error(format!("Incomparable types in {function:?} aggregate: {argument:?}")));
    Ok(match function {
        A::Count | A::CountStar => T::Scalar(S::I64),
        A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => T::Scalar(S::F64).maybe(),
        A::Every | A::Any => T::Scalar(S::Bool).maybe(),
        A::ArrayAgg => T::list(argument.clone()).maybe(),
        A::Sum => promoted()?.maybe(),
        A::Average => match promoted()? {
            ty @ T::Scalar(S::Duration(_)) => ty.maybe(),
            T::Nothing | T::Null | T::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F16 | S::F32 | S::F64 | S::Decimal { .. }) => T::Scalar(S::F64).maybe(),
            _ => return Err(error(format!("Cannot compute AVERAGE over type {argument:?}"))),
        },
        A::Min | A::Max => match promoted()? {
            ty @ (T::Nothing | T::Null | T::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F16 | S::F32 | S::F64 | S::Decimal { .. }
                | S::Duration(_) | S::DateDay | S::DateMilli | S::Timestamp { .. } | S::Time { .. } | S::Utf8 | S::Binary | S::Keyword | S::Uri)) => ty.maybe(),
            _ => return Err(rtdb_types::Anomaly::unsupported("xtdb.group-by/unsupported-min-max-type", format!("Unsupported type in min/max aggregate: {argument:?}"))),
        },
    })
}

fn call(name: String, args: Vec<Expr>) -> Result<Expr> {
    let return_type = rtdb_plan::function_type_with_args(&name, &args)?;
    Ok(Expr::Call { name, args, return_type })
}

fn field_access(expr: Expr, name: String) -> Expr {
    let types: Vec<_> = expr.data_type().legs().iter().filter_map(|ty| match ty { LogicalType::Struct(fields) => Some(fields.get(&name).cloned().unwrap_or(LogicalType::Null)), _ => None }).collect();
    Expr::Field { expr: Box::new(expr), name, data_type: LogicalType::join_all(&types).maybe() }
}

fn function_arguments(function: &ast::Function) -> Result<Vec<ast::FunctionArg>> {
    match &function.args { ast::FunctionArguments::None => Ok(Vec::new()), ast::FunctionArguments::List(arguments) => Ok(arguments.args.clone()), _ => Err(unsupported("Function subquery arguments")) }
}

fn binary_operator(operator: &ast::BinaryOperator) -> Result<BinaryOp> {
    use ast::BinaryOperator as A;
    Ok(match operator {
        A::Plus => BinaryOp::Add, A::Minus => BinaryOp::Subtract, A::Multiply => BinaryOp::Multiply, A::Divide => BinaryOp::Divide, A::Modulo => BinaryOp::Modulo,
        A::Eq => BinaryOp::Eq, A::NotEq => BinaryOp::NotEq, A::Lt => BinaryOp::Lt, A::LtEq => BinaryOp::LtEq, A::Gt => BinaryOp::Gt, A::GtEq => BinaryOp::GtEq,
        A::And => BinaryOp::And, A::Or => BinaryOp::Or, A::StringConcat => BinaryOp::Concat,
        A::BitwiseAnd => BinaryOp::BitAnd, A::BitwiseOr => BinaryOp::BitOr, A::PGBitwiseXor | A::BitwiseXor => BinaryOp::BitXor,
        A::PGBitwiseShiftLeft => BinaryOp::ShiftLeft, A::PGBitwiseShiftRight => BinaryOp::ShiftRight,
        _ => return Err(unsupported(format!("Binary operator: {operator}"))),
    })
}

fn aggregate_function(name: &str) -> Option<AggregateFunction> {
    Some(match name.to_lowercase().as_str() {
        "count" => AggregateFunction::Count, "sum" => AggregateFunction::Sum, "avg" => AggregateFunction::Average,
        "min" => AggregateFunction::Min, "max" => AggregateFunction::Max, "array_agg" => AggregateFunction::ArrayAgg,
        "every" | "bool_and" => AggregateFunction::Every, "any" | "some" | "bool_or" => AggregateFunction::Any,
        "var_pop" => AggregateFunction::VariancePopulation, "var_samp" | "variance" => AggregateFunction::VarianceSample,
        "stddev_pop" => AggregateFunction::StddevPopulation, "stddev_samp" | "stddev" => AggregateFunction::StddevSample,
        _ => return None,
    })
}

pub(crate) fn aggregate_calls(expr: &ast::Expr, output: &mut BTreeMap<String, ast::Function>) -> Result<()> {
    if let ast::Expr::Function(function) = expr {
        if function.over.is_none() && aggregate_function(&function.name.to_string()).is_some() { output.insert(expr.to_string(), function.clone()); return Ok(()); }
    }
    for child in children(expr) { aggregate_calls(child, output)?; }
    Ok(())
}

pub(crate) fn window_calls(expr: &ast::Expr, output: &mut Vec<(String, ast::Function)>) {
    if let ast::Expr::Function(function) = expr {
        if function.over.is_some() { output.push((expr.to_string(), function.clone())); return; }
    }
    for child in children(expr) { window_calls(child, output); }
}

pub(crate) fn contains_subquery(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::Subquery(_) | ast::Expr::Exists { .. } | ast::Expr::InSubquery { .. } | ast::Expr::AnyOp { .. } | ast::Expr::AllOp { .. })
        || children(expr).into_iter().any(contains_subquery)
}

pub(crate) fn references_unqualified_names(expr: &ast::Expr, names: &std::collections::BTreeSet<String>) -> bool {
    matches!(expr, ast::Expr::Identifier(name) if names.contains(&identifier(name).normalized()))
        || children(expr).into_iter().any(|expr| references_unqualified_names(expr, names))
}

pub(crate) fn reject_ambiguous_columns(expr: &ast::Expr, schema: &Schema) -> Result<()> {
    let names = match expr { ast::Expr::Identifier(name) => Some(vec![identifier(name)]), ast::Expr::CompoundIdentifier(names) => Some(names.iter().take(2).map(identifier).collect()), _ => None };
    if let Some(names) = names {
        if let Err(error) = schema.resolve(&names) { if error.code == "xtdb/ambiguous-column" { return Err(error); } }
    }
    for child in children(expr) { reject_ambiguous_columns(child, schema)?; }
    Ok(())
}

fn children(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match expr {
        ast::Expr::Nested(expr) | ast::Expr::UnaryOp { expr, .. } | ast::Expr::Cast { expr, .. } | ast::Expr::IsNull(expr) | ast::Expr::IsNotNull(expr) | ast::Expr::IsTrue(expr) | ast::Expr::IsFalse(expr) | ast::Expr::IsUnknown(expr) | ast::Expr::IsNotTrue(expr) | ast::Expr::IsNotFalse(expr) | ast::Expr::IsNotUnknown(expr) => vec![expr],
        ast::Expr::BinaryOp { left, right, .. } | ast::Expr::IsDistinctFrom(left, right) | ast::Expr::IsNotDistinctFrom(left, right) | ast::Expr::AnyOp { left, right, .. } | ast::Expr::AllOp { left, right, .. } => vec![left, right],
        ast::Expr::Between { expr, low, high, .. } => vec![expr, low, high],
        ast::Expr::InList { expr, list, .. } => std::iter::once(expr.as_ref()).chain(list).collect(),
        ast::Expr::InSubquery { expr, .. } => vec![expr],
        ast::Expr::Like { expr, pattern, escape_char, .. } | ast::Expr::ILike { expr, pattern, escape_char, .. } => [expr.as_ref(), pattern.as_ref()].into_iter().chain(escape_char.as_deref()).collect(),
        ast::Expr::Case { operand, conditions, else_result, .. } => operand.as_deref().into_iter().chain(conditions.iter().flat_map(|when| [&when.condition, &when.result])).chain(else_result.as_deref()).collect(),
        ast::Expr::Function(function) => if let ast::FunctionArguments::List(args) = &function.args { args.args.iter().filter_map(|arg| match arg { ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Some(expr), _ => None }).collect() } else { Vec::new() },
        ast::Expr::Array(array) => array.elem.iter().collect(),
        ast::Expr::Dictionary(fields) => fields.iter().map(|field| field.value.as_ref()).collect(),
        ast::Expr::Position { expr, r#in } => vec![expr, r#in],
        ast::Expr::Substring { expr, substring_from, substring_for, .. } => std::iter::once(expr.as_ref()).chain(substring_from.as_deref()).chain(substring_for.as_deref()).collect(),
        ast::Expr::Trim { expr, trim_what, .. } => std::iter::once(expr.as_ref()).chain(trim_what.as_deref()).collect(),
        ast::Expr::CompoundFieldAccess { root, access_chain } => std::iter::once(root.as_ref()).chain(access_chain.iter().filter_map(|access| match access {
            ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => Some(index), _ => None,
        })).collect(),
        _ => Vec::new(),
    }
}
