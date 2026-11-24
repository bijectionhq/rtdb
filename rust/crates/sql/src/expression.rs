use std::collections::BTreeMap;

use rtdb_plan::{AggregateExpr, AggregateFunction, BinaryOp, Expr, Field, PercentileFraction, PercentileKind, Schema, SqlNotice, UnaryOp};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use sqlparser::ast::{self as ast, Spanned};

use crate::bind::{Context, error, grammar_rejected, identifier, identifiers, unsupported};
use crate::subquery::{ExpressionInput, ExpressionScope, SubqueryValue, negated_operator};
use std::sync::Arc;
use rtdb_plan::Relation;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ReplacementKey { Expression(String), Aggregate(sqlparser::tokenizer::Span) }
pub(crate) type Replacements = BTreeMap<ReplacementKey, Field>;

impl Context<'_> {
    pub(crate) fn expr(&mut self, expression: &ast::Expr, schema: &Schema, replacements: &Replacements) -> Result<Expr> {
        self.expr_bound(expression, &mut ExpressionInput::Scalar(ExpressionScope::Columns(schema)), replacements)
    }

    pub(crate) fn relational_expr(&mut self, expression: &ast::Expr, input: &mut Arc<Relation>, replacements: &Replacements) -> Result<Expr> {
        self.expr_bound(expression, &mut ExpressionInput::Relational(input), replacements)
    }

    pub(crate) fn expr_bound(&mut self, expression: &ast::Expr, input: &mut ExpressionInput<'_>, replacements: &Replacements) -> Result<Expr> {
        // Binding recurses once per expression level; the parser admits deeper nesting than a worker thread's stack holds.
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || self.expr_inner(expression, input, replacements)).map_err(|anomaly| {
            if anomaly.context.contains_key("span") { anomaly }
            else { let span = crate::parse::source_span(self.source, expression.span()); anomaly.with_context("span", serde_json::json!({"start":span.start,"end":span.end})).with_context("position", serde_json::json!(span.start + 1)) }
        })
    }

    fn expr_inner(&mut self, expression: &ast::Expr, input: &mut ExpressionInput<'_>, replacements: &Replacements) -> Result<Expr> {
        if matches!(expression, ast::Expr::Function(_)) {
            if let Some(field) = replacements.get(&ReplacementKey::Aggregate(expression.span())) { return Ok(field.expression()); }
        }
        if let Some(field) = replacements.get(&ReplacementKey::Expression(expression.to_string())) { return Ok(field.expression()); }
        let expression = match expression {
            ast::Expr::Subquery(query) => self.query_value(query, input, SubqueryValue::Scalar)?,
            ast::Expr::Exists { subquery, negated } => {
                let expression = self.query_value(subquery, input, SubqueryValue::Exists)?;
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
                if let Some(function) = bare_scalar_name(&name, 1) {
                    call(function, Vec::new())?
                } else if let Some(constant) = catalog_constant(std::slice::from_ref(&name), None)? {
                    constant
                } else {
                    match input.scope().resolve(self, std::slice::from_ref(&name)) {
                        Err(missing) if missing.code == "xtdb/column-not-found" && name.normalized() == "user" => {
                            call("current_user".into(), Vec::new())?
                        }
                        resolved => self.recover_column(resolved, std::slice::from_ref(&name))?,
                    }
                }
            }
            ast::Expr::CompoundIdentifier(names) => {
                let names: Vec<_> = names.iter().map(identifier).collect();
                if let Some(function) = names.last().and_then(|name| bare_scalar_name(name, names.len())) {
                    call(function, Vec::new())?
                } else if let Some(constant) = catalog_constant(&names, None)? {
                    constant
                } else {
                    let resolved = input.scope().resolve(self, &names);
                    self.recover_column(resolved, &names)?
                }
            }
            ast::Expr::Value(value) => match &value.value {
                ast::Value::Placeholder(parameter) => {
                    let index = parameter.strip_prefix('$').and_then(|parameter| parameter.parse::<usize>().ok()).and_then(|index| index.checked_sub(1)).ok_or_else(|| error("Invalid parameter index"))?;
                    self.binder.parameter(index, self.options.parameter_types.get(&index).cloned().unwrap_or(LogicalType::Nothing))
                }
                value => Expr::Literal(crate::types::literal(value)?),
            },
            ast::Expr::TypedString(value) => {
                use crate::dialect::CustomStringLiteral;
                use crate::literal_admission::TemporalLiteralKind as Temporal;
                let literal = crate::types::literal(&value.value.value)?;
                match (&value.data_type, &literal) {
                    (ast::DataType::Date, Value::Utf8(text)) =>
                        Expr::Literal(self.literals.temporal(value.value.span, Temporal::Date, text)),
                    (ast::DataType::Timestamp(..), Value::Utf8(text)) =>
                        Expr::Literal(self.literals.temporal(value.value.span, Temporal::Timestamp, text)),
                    (ast::DataType::Time(..), Value::Utf8(text)) => {
                        let admitted = self.literals.temporal(value.value.span, Temporal::Time, text);
                        Expr::Cast { expr: Box::new(Expr::Literal(admitted)), target: self.cast_target(&value.data_type)? }
                    }
                    (_, Value::Utf8(text)) if CustomStringLiteral::from_type(&value.data_type).is_some() => {
                        match CustomStringLiteral::from_type(&value.data_type).expect("guarded custom literal type") {
                            CustomStringLiteral::Uri => {
                                rtdb_types::UriParts::parse(text).map_err(|failure| error(format!("Cannot parse uri: {}", failure.message.as_deref().unwrap_or("null"))))?;
                                Expr::Literal(Value::Uri(text.clone()))
                            }
                            CustomStringLiteral::Keyword => Expr::Literal(Value::Keyword(rtdb_types::Keyword::from_name(text.strip_prefix(':').unwrap_or(text)))),
                            CustomStringLiteral::Duration => Expr::DurationLiteral(rtdb_types::DurationLiteral::parse(text)
                                .map_err(|message| error(format!("Cannot parse duration: {text} - failed with message {message}")))?),
                        }
                    }
                    _ => Expr::Cast { expr: Box::new(Expr::Literal(literal)), target: self.cast_target(&value.data_type)? },
                }
            },
            ast::Expr::Interval(interval) => self.interval_literal(interval)?,
            ast::Expr::Floor { expr, field } | ast::Expr::Ceil { expr, field } => {
                if !matches!(field, ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime)) { return Err(grammar_rejected("A CEIL/FLOOR precision field")); }
                let name = if matches!(expression, ast::Expr::Floor { .. }) { "floor" } else { "ceil" };
                call(name.into(), vec![self.expr_bound(expr, input, replacements)?])?
            }
            ast::Expr::Nested(expr) => self.expr_bound(expr, input, replacements)?,
            ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } if matches!(expr.as_ref(), ast::Expr::Value(value) if matches!(&value.value, ast::Value::Number(value, _) if value == "9223372036854775808")) => Expr::Literal(Value::I64(i64::MIN)),
            ast::Expr::UnaryOp { op, expr } => {
                let expr = self.expr_bound(expr, input, replacements)?;
                match op { ast::UnaryOperator::Plus => expr, ast::UnaryOperator::Minus => Expr::unary(UnaryOp::Negate, expr), ast::UnaryOperator::Not => Expr::unary(UnaryOp::Not, expr), ast::UnaryOperator::BitwiseNot => Expr::unary(UnaryOp::BitNot, expr), _ => return Err(grammar_rejected(format!("Unary operator {op}"))) }
            }
            ast::Expr::BinaryOp { left, op, right } => {
                if let Some(access) = crate::json_access::JsonAccess::operator(op) {
                    let object = self.expr_bound(left,input,replacements)?;
                    self.expr_bound(right,input,replacements)?;
                    return access.lower(object,right);
                }
                if let ast::BinaryOperator::Custom(name) = op {
                    if matches!(name.as_str(),"rtdb.sql/between/symmetric" | "rtdb.sql/between/not-symmetric") {
                        let ast::Expr::BinaryOp { left: low,op: ast::BinaryOperator::Custom(marker),right: high } = right.as_ref() else { return Err(error("Invalid BETWEEN bounds")); };
                        if marker != "rtdb.sql/between/bounds" { return Err(error("Invalid BETWEEN bounds")); }
                        let expression = Expr::between(self.expr_bound(left,input,replacements)?,self.expr_bound(low,input,replacements)?,self.expr_bound(high,input,replacements)?,true);
                        return Ok(if name == "rtdb.sql/between/not-symmetric" { Expr::unary(UnaryOp::Not,expression) } else { expression });
                    }
                    if matches!(name.as_str(),"rtdb.sql/regex/match" | "rtdb.sql/regex/not") {
                        let ast::Expr::BinaryOp { left: pattern,op: ast::BinaryOperator::Custom(flags_marker),right: flags } = right.as_ref() else { return Err(error("Invalid regex predicate")); };
                        if flags_marker != "rtdb.sql/regex/flags" { return Err(error("Invalid regex predicate flags")); }
                        let expression = call("like_regex".into(),vec![self.expr_bound(left,input,replacements)?,self.expr_bound(pattern,input,replacements)?,self.expr_bound(flags,input,replacements)?])?;
                        return Ok(if name == "rtdb.sql/regex/not" { Expr::unary(UnaryOp::Not,expression) } else { expression });
                    }
                    if let Some(name) = name.strip_prefix("rtdb.sql/string/") {
                        return call(name.into(), vec![self.expr_bound(left, input, replacements)?, self.expr_bound(right, input, replacements)?]);
                    }
                    if let Some(name) = name.strip_prefix("rtdb.sql/period/") {
                        let left = self.expr_bound(left, input, replacements)?;
                        let right = self.expr_bound(right, input, replacements)?;
                        return crate::period::predicate(name, left, right);
                    }
                }
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
                let predicate = Expr::between(expr,self.expr_bound(low,input,replacements)?,self.expr_bound(high,input,replacements)?,false);
                if *negated { Expr::unary(UnaryOp::Not, predicate) } else { predicate }
            }
            ast::Expr::InList { expr, list, negated } => {
                let needle = self.expr_bound(expr, input, replacements)?;
                let predicate = self.in_list(needle, list, input)?;
                if *negated { Expr::unary(UnaryOp::Not, predicate) } else { predicate }
            }
            ast::Expr::Like { expr, pattern, negated, any,.. } | ast::Expr::ILike { expr, pattern, negated, any,.. } => {
                if *any { return Err(grammar_rejected("LIKE ANY")); }
                Expr::Like { expr: Box::new(self.expr_bound(expr, input, replacements)?), pattern: Box::new(self.expr_bound(pattern, input, replacements)?), escape: None, case_insensitive: matches!(expression, ast::Expr::ILike { .. }), negated: *negated }
            }
            ast::Expr::Cast { expr, data_type, kind, format } => {
                if format.is_some() || !matches!(kind, ast::CastKind::Cast | ast::CastKind::DoubleColon) { return Err(grammar_rejected("TRY_CAST or a cast format")); }
                Expr::Cast { expr: Box::new(self.expr_bound(expr, input, replacements)?), target: self.cast_target(data_type)? }
            }
            ast::Expr::Case { operand, conditions, else_result, .. } => {
                let operand = operand.as_ref().map(|value| self.expr_bound(value, input, replacements)).transpose()?;
                let branches = conditions.iter().map(|when| {
                    let candidate = self.expr_bound(&when.condition, input, replacements)?;
                    Ok((if operand.is_some() { candidate } else { Expr::unary(UnaryOp::IsTrue, candidate) }, self.expr_bound(&when.result, input, replacements)?))
                }).collect::<Result<_>>()?;
                let otherwise = Box::new(else_result.as_ref().map(|value| self.expr_bound(value, input, replacements)).transpose()?.unwrap_or(Expr::Literal(Value::Null)));
                match operand { Some(operand) => Expr::SimpleCase { operand: Box::new(operand),branches,otherwise }, None => Expr::Case { branches,otherwise } }
            }
            ast::Expr::Function(function) => {
                if aggregate_function(&function.name.to_string()).is_some() || percentile_kind(&function.name.to_string()).is_some() { return Err(error("Aggregates are not allowed in this context")); }
                if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) { return Err(grammar_rejected("A scalar function modifier")); }
                if let Some((query, value)) = function_query(function) {
                    return self.query_value(query, input, value);
                }
                if let Some(literal) = binary_string_literal(function) { return literal.map(Expr::Literal); }
                if let ast::FunctionArguments::List(arguments) = &function.args
                    && arguments.duplicate_treatment.is_none() && arguments.clauses.is_empty()
                    && let Some(constant) = catalog_constant(&identifiers(&function.name)?, Some(&arguments.args))? {
                    return Ok(constant);
                }
                let units = match &function.args {
                    ast::FunctionArguments::List(arguments) => scalar_argument_units(arguments)?,
                    _ => None,
                };
                let name = scalar_name(function)?;
                if name == "regexp_replace" {
                    let arguments = function_arguments(function)?;
                    crate::settings::regexp_arguments(&arguments)?;
                    let argument = |index| match &arguments[index] { ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Ok(expr), _ => Err(error("REGEXP_REPLACE requires expressions")) };
                    let source = self.expr_bound(argument(0)?, input, replacements)?;
                    let metadata = |index| -> Result<Value> {
                        let expr = argument(index)?;
                        if let ast::Expr::Value(value) = expr {
                            if let Ok(value @ Value::Utf8(_)) = crate::types::literal(&value.value) { return Ok(value); }
                        }
                        let (sign,value) = match expr {
                            ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus,expr } => ("-",expr.as_ref()),
                            ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus,expr } => ("+",expr.as_ref()),
                            expr => ("",expr),
                        };
                        let ast::Expr::Value(value) = value else { return Err(error("REGEXP_REPLACE requires an integer literal")); };
                        let ast::Value::Number(value,_) = &value.value else { return Err(error("REGEXP_REPLACE requires an integer literal")); };
                        let value = format!("{sign}{value}").parse::<i64>().map_err(|_|error("REGEXP_REPLACE integer literal exceeds signed 64-bit range"))?;
                        Ok(Value::I64(value))
                    };
                    let (pattern, replacement) = (metadata(1)?, metadata(2)?);
                    let (mut start, mut occurrence, mut flags) = (None, None, None);
                    for index in 3..arguments.len() {
                        let value = metadata(index)?;
                        if matches!(value, Value::Utf8(_)) { flags = Some(value); }
                        else if index == 3 { start = Some(value); } else { occurrence = Some(value); }
                    }
                    let optional_form = |value: Option<Value>| -> Result<rtdb_plan::RegexForm> {
                        Ok(value.as_ref().map(|value| self.forms.literal(value)).transpose()?.unwrap_or_default())
                    };
                    let recipe = rtdb_plan::RegexReplacement {
                        pattern: self.forms.literal(&pattern)?,
                        replacement: self.forms.literal(&replacement)?,
                        start: optional_form(start)?,
                        occurrence: optional_form(occurrence)?,
                        flags: optional_form(flags)?,
                    };
                    return Ok(Expr::regex_replace(source, recipe));
                }
                if matches!(name.as_str(),"date_bin" | "range_bins") && !matches!(function_arguments(function)?.first(),Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Interval(_))))) {
                    return Err(error("Temporal bin functions require an interval literal stride"));
                }
                let args = function_arguments(function)?.iter().enumerate().map(|(index,arg)| match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        if name == "date_trunc" && index == 0 {
                            Ok(Expr::Literal(Value::Utf8(crate::temporal_syntax::trunc_field(expr)?)))
                        } else { self.expr_bound(expr,input,replacements) }
                    }
                    _ => Err(grammar_rejected("A named or wildcard scalar function argument")),
                }).collect::<Result<Vec<_>>>()?;
                let name = match units {
                    Some(units) => character_length_function(units)?.into(),
                    None => name,
                };
                if matches!(name.as_str(), "pg_sleep" | "pg_sleep_for") {
                    if args.len() != 1 { return Err(error(format!("Function {name} requires one argument"))); }
                    call("sleep".into(), vec![Expr::Cast { expr: Box::new(args.into_iter().next().unwrap()), target: rtdb_plan::CastTarget::Fixed(LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Millisecond))) }])?
                }
                else if name == "concat" {
                    if args.is_empty() { return Err(error("Function concat arity mismatch: expected at least 1 argument")); }
                    let mut parts = args.into_iter().map(|arg| Ok(Expr::Coalesce(vec![call("str".into(), vec![arg])?, Expr::Literal(Value::Utf8(String::new()))]))).collect::<Result<Vec<_>>>()?;
                    if parts.len() == 1 { parts.pop().unwrap() } else { call(name, parts)? }
                }
                else if name == "str" && args.is_empty() { return Err(error("Function str arity mismatch: expected at least 1 argument")); }
                else if name == "coalesce" { Expr::Coalesce(args) }
                else if matches!(name.as_str(), "generate_series" | "range") { self.series(&name, args)? }
                else if name == "overlaps" { crate::period::overlaps(args)? }
                else if name == "range_bins" {
                    if !(2..=3).contains(&args.len()) { return Err(error("RANGE_BINS requires a stride, period and optional origin")); }
                    let mut args = args.into_iter();let stride = args.next().unwrap();let period = args.next().unwrap();
                    let mut lowered = vec![stride,call("lower".into(),vec![period.clone()])?,call("upper".into(),vec![period])?];
                    if let Some(origin) = args.next() { lowered.push(origin); }
                    call(name,lowered)?
                }
                else { call(if name == "tstzrange" { "period".into() } else { name }, args)? }
            }
            ast::Expr::Extract { field,expr,syntax } => call("extract".into(),vec![Expr::Literal(Value::Utf8(crate::temporal_syntax::extract_field(field,syntax)?)),self.expr_bound(expr,input,replacements)?])?,
            ast::Expr::Array(array) => Expr::List(array.elem.iter().map(|expr| self.expr_bound(expr, input, replacements)).collect::<Result<_>>()?),
            ast::Expr::Dictionary(fields) => Expr::Struct(fields.iter().map(|field| Ok((identifier(&field.key).normalized(), self.expr_bound(&field.value, input, replacements)?))).collect::<Result<_>>()?),
            ast::Expr::CompoundFieldAccess { root, access_chain } => {
                let mut expr = self.expr_bound(root, input, replacements)?;
                for access in access_chain {
                    match access {
                        ast::AccessExpr::Dot(ast::Expr::Identifier(name)) => expr = field_access(expr, identifier(name).normalized()),
                        ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => {
                            let index = Expr::binary(BinaryOp::Subtract, self.expr_bound(index, input, replacements)?, Expr::Literal(Value::I64(1)));
                            let data_type = rtdb_plan::function_type("nth", &[expr.data_type(), index.data_type()])?;
                            expr = Expr::Subscript { expr: Box::new(expr), index: Box::new(index), data_type };
                        }
                        _ => return Err(grammar_rejected("Nested field access or an array slice")),
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
            ast::Expr::Overlay { expr, overlay_what, overlay_from, overlay_for } => {
                let target = self.expr_bound(expr, input, replacements)?;
                let placing = self.expr_bound(overlay_what, input, replacements)?;
                let start = self.expr_bound(overlay_from, input, replacements)?;
                let length = match overlay_for { Some(expr) => self.expr_bound(expr, input, replacements)?, None => call("default_overlay_length".into(), vec![placing.clone()])? };
                call("overlay".into(), vec![target, placing, start, length])?
            }
            ast::Expr::Trim { expr, trim_where, trim_what, trim_characters } => {
                if trim_characters.is_some() { return Err(grammar_rejected("A TRIM character list")); }
                let mut args = vec![self.expr_bound(expr, input, replacements)?];
                args.push(match trim_what { Some(what) => self.expr_bound(what, input, replacements)?, None => Expr::Literal(Value::Utf8(" ".into())) });
                let name = match trim_where { Some(ast::TrimWhereField::Leading) => "trim_leading", Some(ast::TrimWhereField::Trailing) => "trim_trailing", _ => "trim" };
                call(name.into(), args)?
            }
            _ => return Err(grammar_rejected(format!("SQL expression {expression}"))),
        };
        rtdb_plan::bind_clock_timezone(expression,self.cast_timezone)
    }

    pub(crate) fn series(&self, name: &str, mut args: Vec<Expr>) -> Result<Expr> {
        if !matches!(name, "generate_series" | "range") { return Err(error(format!("Unknown series function: {name}"))); }
        if !(2..=3).contains(&args.len()) { return Err(error(format!("Function {name} requires two or three arguments"))); }
        if args.len() == 2 { args.push(Expr::Literal(Value::I64(1))); }
        args.push(Expr::Literal(Value::Bool(name == "generate_series" && !self.options.generate_series_end_exclusive)));
        call("generate_series".into(), args)
    }

    pub(crate) fn aggregate(&mut self, function: &ast::Function, input: &mut Arc<Relation>, index: usize) -> Result<AggregateExpr> {
        if function.over.is_some() { return Err(unsupported("Aggregate window functions")); }
        if function.null_treatment.is_some() { return Err(grammar_rejected("Aggregate null treatment")); }
        if let Some(kind) = percentile_kind(&function.name.to_string()) { return self.percentile(function, kind, input, index); }
        let mut kind = aggregate_function(&function.name.to_string()).ok_or_else(|| error("Unknown aggregate"))?;
        if !function.within_group.is_empty() { return Err(grammar_rejected("WITHIN GROUP outside PERCENTILE_CONT and PERCENTILE_DISC")); }
        let args = function_arguments(function)?;
        let argument = match args.as_slice() {
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)] if kind == AggregateFunction::Count => { kind = AggregateFunction::CountStar; None }
            [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))] => Some(self.relational_expr(expr, input, &BTreeMap::new())?),
            _ => return Err(error("Aggregate requires one expression or COUNT(*)")),
        };
        let argument = argument.map(|expr| self.aggregate_input(input, expr, format!("_aggregate_in_{index}"))).transpose()?;
        let mut order_by = function.within_group.iter().map(|order| self.relational_sort_expr(order, input, &BTreeMap::new())).collect::<Result<Vec<_>>>()?;
        let distinct = if let ast::FunctionArguments::List(arguments) = &function.args {
            for clause in &arguments.clauses {
                match clause { ast::FunctionArgumentClause::OrderBy(order) if kind == AggregateFunction::ArrayAgg => for expression in order { order_by.push(self.relational_sort_expr(expression, input, &BTreeMap::new())?); }, _ => return Err(grammar_rejected("An aggregate argument modifier")) }
            }
            if kind == AggregateFunction::ArrayAgg && arguments.duplicate_treatment.is_some() { return Err(grammar_rejected("An ARRAY_AGG set quantifier")); }
            arguments.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
        } else { false };
        if distinct && !matches!(kind.canonical(), AggregateFunction::Count | AggregateFunction::Sum | AggregateFunction::Average | AggregateFunction::Min | AggregateFunction::Max | AggregateFunction::ArrayAgg) {
            return Err(unsupported("The aggregate factory does not support DISTINCT"));
        }
        for (order_index, order) in order_by.iter_mut().enumerate() {
            order.expr = self.aggregate_input(input, order.expr.clone(), format!("_aggregate_sort_{index}_{order_index}"))?;
        }
        let argument_type = argument.as_ref().map(Expr::data_type).unwrap_or(LogicalType::Nothing);
        let data_type = aggregate_type(&kind, &argument_type)?;
        Ok(AggregateExpr { field: self.binder.field(format!("_aggregate_{index}"), None, data_type)?, function: kind, argument, distinct,
            filter: function.filter.as_ref().map(|expr| self.relational_expr(expr, input, &BTreeMap::new())).transpose()?, order_by })
    }

    fn aggregate_input(&mut self, input: &mut Arc<Relation>, expression: Expr, name: String) -> Result<Expr> {
        if matches!(expression, Expr::Column { .. }) { return Ok(expression); }
        let field = self.binder.field(name, None, expression.data_type())?;
        let mut projections = input.schema().fields().iter().map(|field| rtdb_plan::NamedExpr { field: field.clone(), expr: field.expression() }).collect::<Vec<_>>();
        projections.push(rtdb_plan::NamedExpr { field: field.clone(), expr: expression });
        *input = Arc::new(Arc::clone(input).project(projections)?);
        Ok(field.expression())
    }

    fn percentile(&mut self, function: &ast::Function, kind: PercentileKind, input: &mut Arc<Relation>, index: usize) -> Result<AggregateExpr> {
        let ast::FunctionArguments::List(arguments) = &function.args else { return Err(error("Percentile requires one fraction")); };
        if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) {
            return Err(error("Percentile does not accept argument modifiers"));
        }
        let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(fraction))] = arguments.args.as_slice() else { return Err(error("Percentile requires one fraction")); };
        let numeric = |expr: &ast::Expr| matches!(expr, ast::Expr::Value(value) if matches!(value.value, ast::Value::Number(_, _)));
        let allowed = match fraction {
            ast::Expr::Value(value) => matches!(value.value, ast::Value::Number(_, _) | ast::Value::Placeholder(_)),
            ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus | ast::UnaryOperator::Minus, expr } => numeric(expr),
            _ => false,
        };
        if !allowed { return Err(error("Percentile fraction must be a numeric literal or parameter")); }
        let fraction = self.expr(fraction, &Schema::default(), &BTreeMap::new())?;
        let fraction = match &fraction {
            Expr::Parameter { index, .. } => PercentileFraction::Parameter { index: *index },
            Expr::Literal(Value::I64(value)) => PercentileFraction::Integer(*value),
            Expr::Literal(Value::F64(value)) => PercentileFraction::Literal(*value),
            Expr::Unary { op: UnaryOp::Negate, expr } => match expr.as_ref() {
                Expr::Literal(Value::I64(value)) => PercentileFraction::Integer(value.checked_neg().ok_or_else(|| error("Percentile fraction integer overflow"))?),
                Expr::Literal(Value::F64(value)) => PercentileFraction::Literal(-value),
                _ => return Err(error("Percentile fraction must be numeric")),
            },
            _ => return Err(error("Percentile fraction must be numeric")),
        };
        let [order] = function.within_group.as_slice() else { return Err(error("Percentile requires WITHIN GROUP with exactly one ORDER BY expression")); };
        if order.with_fill.is_some() { return Err(error("Percentile ordering does not accept WITH FILL")); }
        let descending = crate::bind::sort_descending(&order.options)?;
        let mut argument = self.relational_expr(&order.expr, input, &BTreeMap::new())?;
        if !matches!(argument, Expr::Column { .. }) {
            let field = self.binder.field(format!("_percentile_sort_{index}"), None, argument.data_type())?;
            let mut projections = input.schema().fields().iter().map(|field| rtdb_plan::NamedExpr { field: field.clone(), expr: field.expression() }).collect::<Vec<_>>();
            projections.push(rtdb_plan::NamedExpr { field: field.clone(), expr: argument });
            *input = Arc::new(Arc::clone(input).project(projections)?);
            argument = field.expression();
        }
        let function_kind = AggregateFunction::Percentile { kind, fraction, descending };
        Ok(AggregateExpr {
            field: self.binder.field(format!("_aggregate_{index}"), None, aggregate_type(&function_kind, &argument.data_type())?)?,
            function: function_kind, argument: Some(argument), distinct: false, order_by: Vec::new(),
            filter: function.filter.as_ref().map(|expr| self.relational_expr(expr, input, &BTreeMap::new())).transpose()?,
        })
    }
}

fn aggregate_type(function: &AggregateFunction, argument: &LogicalType) -> Result<LogicalType> {
    use AggregateFunction as A;
    use LogicalType as T;
    use ScalarType as S;
    let promoted = || T::least_upper_bound(std::slice::from_ref(argument))
        .ok_or_else(|| error(format!("Incomparable types in {function:?} aggregate: {argument:?}")));
    Ok(match function.canonical() {
        A::Alias(_) | A::Named(_) => unreachable!(),
        A::Count | A::CountStar => T::Scalar(S::I64),
        A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => T::Scalar(S::F64).maybe(),
        A::Every | A::Any => T::Scalar(S::Bool).maybe(),
        A::ArrayAgg => T::list(argument.clone()).maybe(),
        A::VecAgg => T::list(argument.clone()),
        A::Percentile { kind: PercentileKind::Continuous, .. } => T::Scalar(S::F64).maybe(),
        A::Percentile { kind: PercentileKind::Discrete, .. } => argument.clone().maybe(),
        A::Sum => promoted()?.maybe(),
        A::Average => match promoted()? {
            ty @ T::Scalar(S::Duration(_)) => ty.maybe(),
            T::Nothing | T::Null | T::Scalar(S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 | S::F16 | S::F32 | S::F64 | S::Decimal { .. }) => T::Scalar(S::F64).maybe(),
            _ => return Err(error(format!("Cannot compute AVERAGE over type {argument:?}"))),
        },
        // Before factory emission this field preserves the possible input types for column binding.
        A::Min | A::Max => T::least_upper_bound(std::slice::from_ref(argument)).unwrap_or_else(|| argument.clone()).maybe(),
    })
}

pub(crate) fn bare_scalar_name(name: &rtdb_plan::Identifier, parts: usize) -> Option<String> {
    if name.quoted { return None; }
    let function = name.text.to_ascii_lowercase();
    match function.as_str() {
        "current_catalog" if parts <= 2 => Some("current_database".into()),
        "current_user" | "current_schema" if parts <= 2 => Some(function),
        "current_timestamp" | "current_date" | "current_time" | "current_timezone"
        | "localtimestamp" | "localtime" | "localdate" | "local_timestamp" | "local_time"
        | "local_date" | "snapshot_token" if parts == 1 => Some(function),
        _ => None,
    }
}

/// pg_catalog introspection stubs: constants whose arguments are never bound, so they need not resolve.
fn catalog_constant(names: &[rtdb_plan::Identifier], args: Option<&[ast::FunctionArg]>) -> Result<Option<Expr>> {
    let [.., last] = names else { return Ok(None); };
    if names.len() > 2 || last.quoted { return Ok(None); }
    let name = last.normalized();
    let expressions = args.map(|args| args.iter().map(|arg| match arg {
        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Some(expr),
        _ => None,
    }).collect::<Option<Vec<_>>>());
    let count = match &expressions { None => None, Some(Some(expressions)) => Some(expressions.len()), Some(None) => return Err(error(format!("Invalid arguments to {name}"))) };
    let column = || matches!(expressions.as_ref().and_then(Option::as_deref), Some([ast::Expr::Identifier(_) | ast::Expr::CompoundIdentifier(_)]));
    let (value, valid) = match name.as_str() {
        "has_table_privilege" | "has_schema_privilege" | "has_any_column_privilege" if count.is_some() => (Value::Bool(true), matches!(count, Some(2 | 3))),
        "pg_table_is_visible" if count.is_some() => (Value::Bool(true), column()),
        "pg_get_userbyid" if count.is_some() => (Value::Utf8("xtdb".into()), column()),
        "pg_get_indexdef" if count.is_some() => (Value::Null, matches!(count, Some(1 | 3))),
        "pg_get_expr" => (Value::Null, matches!(count, None | Some(2 | 3))),
        "_pg_expandarray" => (Value::Null, matches!(count, None | Some(1))),
        _ => return Ok(None),
    };
    if !valid { return Err(error(format!("Invalid arguments to {name}"))); }
    Ok(Some(Expr::Literal(value)))
}

pub(crate) fn scalar_argument_units(arguments: &ast::FunctionArgumentList) -> Result<Option<&ast::CharacterLengthUnits>> {
    if arguments.duplicate_treatment.is_none() {
        match arguments.clauses.as_slice() {
            [] => return Ok(None),
            [ast::FunctionArgumentClause::UsingCharacterLengthUnits(units)] => return Ok(Some(units)),
            _ => {}
        }
    }
    Err(grammar_rejected("A scalar function argument modifier"))
}

pub(crate) fn character_length_function(units: &ast::CharacterLengthUnits) -> Result<&'static str> {
    let (unit, accepted, function) = match units {
        ast::CharacterLengthUnits::Characters(unit) => (unit, "CHARACTERS", "character_length"),
        ast::CharacterLengthUnits::Octets(unit) => (unit, "OCTETS", "octet_length"),
    };
    if unit.value == accepted {
        Ok(function)
    } else {
        Err(rtdb_types::Anomaly::incorrect("xtdb.error/illegal-arg", format!("No matching clause: {}", unit.value)))
    }
}

/// The reference lexes `X('hex')`, with nothing between `X` and the quote, as one binary string token; the SQL parser reads it as a call.
fn binary_string_literal(function: &ast::Function) -> Option<Result<Value>> {
    let [ast::ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else { return None; };
    if name.quote_style.is_some() || !name.value.eq_ignore_ascii_case("x") { return None; }
    let ast::FunctionArguments::List(arguments) = &function.args else { return None; };
    let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(value)))] = arguments.args.as_slice() else { return None; };
    let ast::Value::SingleQuotedString(body) = &value.value else { return None; };
    let contiguous = value.span.start.line == name.span.end.line && value.span.start.column == name.span.end.column + 1
        && value.span.end.line == value.span.start.line && value.span.end.column == value.span.start.column + body.len() as u64 + 2;
    if !contiguous || !body.bytes().all(|byte| byte.is_ascii_hexdigit()) { return None; }
    if body.len() % 2 != 0 {
        return Some(Err(error(format!("Cannot parse binary hex string: {}('{body}') - failed with message Odd number of characters.", name.value))));
    }
    Some(Ok(Value::Binary((0..body.len()).step_by(2).map(|index| u8::from_str_radix(&body[index..index + 2], 16).expect("hex digits")).collect())))
}

pub(crate) fn scalar_name(function: &ast::Function) -> Result<String> {
    let names = identifiers(&function.name)?;
    let unknown = || {
        let name = names.last().map(|name| name.normalized()).unwrap_or_else(|| function.name.to_string());
        error(format!("Unknown function: {name}"))
    };
    if names.is_empty() || names.len() > 2 { return Err(unknown()); }
    let last = names.last().unwrap();
    let name = last.normalized();
    crate::temporal_syntax::function(&name,function)?;
    let count = match &function.args {
        ast::FunctionArguments::None => 0,
        ast::FunctionArguments::List(arguments) => arguments.args.len(),
        _ => return Err(grammar_rejected("An unparenthesised subquery function argument")),
    };
    if let Some(arity) = generic_function_arity(&name) {
        if !arity.contains(&count) { return Err(error(format!("Function {name} arity mismatch: expected {}-{}, got {count}", arity.start(), arity.end()))); }
        return Ok(name);
    }
    if last.quoted { return Err(unknown()); }
    match name.as_str() {
        "version" => return Ok(if names.len() == 2 && names[0].normalized() == "xt" { "xt.version".into() } else { name }),
        "current_schema" | "current_schemas" => return Ok(name),
        "current_catalog" => return Ok("current_database".into()),
        "current_user" if matches!(function.args, ast::FunctionArguments::None) => return Ok(name),
        "generate_series" | "range" => {
            if !(2..=3).contains(&count) { return Err(error(format!("Function {name} requires two or three arguments"))); }
            return Ok(name);
        }
        _ => {},
    }
    if names.len() != 1 { return Err(unknown()); }
    match name.as_str() {
        "char_length" => Ok("character_length".into()),
        "nullif" | "coalesce" | "character_length" | "period" | "tstzrange"
        | "substring" | "trim" | "overlay" | "replace" | "regexp_replace"
        | "uri_scheme" | "uri_user_info" | "uri_host" | "uri_port" | "uri_path" | "uri_query" | "uri_fragment"
        | "pg_sleep" | "pg_sleep_for" | "current_timestamp" | "now" | "current_date"
        | "current_time" | "current_timezone" | "localtime" | "localtimestamp" | "localdate" | "local_date" | "local_time" | "local_timestamp" | "snapshot_token"
        | "current_setting" | "date_trunc" | "date_bin" | "range_bins" | "overlaps" | "trim_array" => Ok(name),
        _ => Err(unknown()),
    }
}

fn generic_function_arity(name: &str) -> Option<std::ops::RangeInclusive<usize>> {
    Some(match name {
        "current_database" => 0..=0,
        "octet_length" | "length" | "cardinality" | "abs" | "log10" | "ln" | "exp" | "sqrt"
        | "floor" | "ceil" | "ceiling" | "sin" | "cos" | "tan" | "sinh" | "cosh" | "tanh"
        | "asin" | "acos" | "atan" | "lower_inf" | "lower" | "upper" | "upper_inf"
        | "namespace" | "local_name" | "reverse" | "quote_ident" | "parse_ident" | "epoch_nanos_be" | "search_length" | "search_terms" => 1..=1,
        "array_upper" | "array_lower" | "array_length" | "mod" | "log" | "power" | "age"
        | "string_to_array" | "col_description" | "pg_indexam_has_property" | "format_type" | "match" | "search_corpus_fingerprint" | "cosine_distance" => 2..=2,
        "search_score" => 2..=5,
        "approximate_nearest" => 3..=3,
        "round" => 1..=2,
        "least" | "greatest" | "str" | "format" | "concat" => 1..=usize::MAX,
        _ => return None,
    })
}

pub(crate) fn call(name: String, args: Vec<Expr>) -> Result<Expr> {
    let return_type = rtdb_plan::function_type_with_args(&name, &args)?;
    Ok(Expr::Call { name, args, return_type })
}

fn field_access(expr: Expr, name: String) -> Expr {
    let data_type = rtdb_plan::field_type(&expr.data_type(), &name);
    Expr::Field { expr: Box::new(expr), name, data_type }
}

fn function_arguments(function: &ast::Function) -> Result<Vec<ast::FunctionArg>> {
    match &function.args { ast::FunctionArguments::None => Ok(Vec::new()), ast::FunctionArguments::List(arguments) => Ok(arguments.args.clone()), _ => Err(grammar_rejected("An unparenthesised subquery function argument")) }
}

fn binary_operator(operator: &ast::BinaryOperator) -> Result<BinaryOp> {
    use ast::BinaryOperator as A;
    Ok(match operator {
        A::Plus => BinaryOp::Add, A::Minus => BinaryOp::Subtract, A::Multiply => BinaryOp::Multiply, A::Divide => BinaryOp::Divide, A::Modulo => BinaryOp::Modulo,
        A::Eq => BinaryOp::Eq, A::NotEq => BinaryOp::NotEq, A::Lt => BinaryOp::Lt, A::LtEq => BinaryOp::LtEq, A::Gt => BinaryOp::Gt, A::GtEq => BinaryOp::GtEq,
        A::And => BinaryOp::And, A::Or => BinaryOp::Or, A::StringConcat => BinaryOp::Concat,
        A::BitwiseAnd => BinaryOp::BitAnd, A::BitwiseOr => BinaryOp::BitOr, A::PGBitwiseXor | A::BitwiseXor => BinaryOp::BitXor,
        A::PGBitwiseShiftLeft => BinaryOp::ShiftLeft, A::PGBitwiseShiftRight => BinaryOp::ShiftRight,
        _ => return Err(grammar_rejected(format!("Binary operator {operator}"))),
    })
}

fn aggregate_function(name: &str) -> Option<AggregateFunction> {
    Some(match name.to_lowercase().as_str() {
        "count" => AggregateFunction::Count, "sum" => AggregateFunction::Sum, "avg" => AggregateFunction::Average,
        "min" => AggregateFunction::Min, "max" => AggregateFunction::Max, "array_agg" => AggregateFunction::ArrayAgg,
        "every" => AggregateFunction::Every, "bool_and" => AggregateFunction::Alias(rtdb_plan::AggregateAlias::BoolAnd), "bool_or" => AggregateFunction::Any,
        "var_pop" => AggregateFunction::VariancePopulation, "var_samp" => AggregateFunction::VarianceSample,
        "stddev_pop" => AggregateFunction::StddevPopulation, "stddev_samp" => AggregateFunction::StddevSample,
        _ => return None,
    })
}

fn percentile_kind(name: &str) -> Option<PercentileKind> {
    match name.to_lowercase().as_str() { "percentile_cont" => Some(PercentileKind::Continuous), "percentile_disc" => Some(PercentileKind::Discrete), _ => None }
}

pub(crate) fn aggregate_notices(expr: &ast::Expr, output: &mut Vec<SqlNotice>) {
    if let ast::Expr::Function(function) = expr {
        if percentile_kind(&function.name.to_string()).is_some() && function.within_group.iter().any(|order| order.options.nulls_first.is_some()) {
            output.push(SqlNotice { message: format!("NULL ordering is ignored for {} (nulls are excluded from ordered-set aggregates)", function.name.to_string().to_lowercase()) });
        }
    }
    for child in children(expr) { aggregate_notices(child, output); }
}

pub(crate) fn aggregate_calls(expr: &ast::Expr, output: &mut Vec<(sqlparser::tokenizer::Span, ast::Function)>) -> Result<()> {
    if let ast::Expr::InList { expr, .. } = expr { return aggregate_calls(expr, output); }
    if let ast::Expr::Function(function) = expr {
        if function.over.is_none() && (aggregate_function(&function.name.to_string()).is_some() || percentile_kind(&function.name.to_string()).is_some()) { output.push((expr.span(), function.clone())); return Ok(()); }
    }
    for child in children(expr) { aggregate_calls(child, output)?; }
    Ok(())
}

pub(crate) fn window_calls(expr: &ast::Expr, output: &mut Vec<(String, ast::Function)>) {
    if let ast::Expr::InList { expr, .. } = expr { window_calls(expr, output); return; }
    if let ast::Expr::Function(function) = expr {
        if function.over.is_some() { output.push((expr.to_string(), function.clone())); return; }
    }
    for child in children(expr) { window_calls(child, output); }
}

pub(crate) fn validate_in_list_item(expr: &ast::Expr) -> Result<()> {
    if contains_subquery(expr) { return Err(error("Subqueries are not allowed in this context")); }
    if let ast::Expr::Function(function) = expr {
        if function.over.is_some() { return Err(error("Window functions are not allowed in this context")); }
        if aggregate_function(&function.name.to_string()).is_some() || percentile_kind(&function.name.to_string()).is_some() { return Err(error("Aggregates are not allowed in this context")); }
    }
    for child in children(expr) { validate_in_list_item(child)?; }
    Ok(())
}

pub(crate) fn contains_subquery(expr: &ast::Expr) -> bool {
    matches!(expr, ast::Expr::Subquery(_) | ast::Expr::Exists { .. } | ast::Expr::InSubquery { .. } | ast::Expr::AnyOp { .. } | ast::Expr::AllOp { .. })
        || matches!(expr, ast::Expr::Function(function) if function_query(function).is_some())
        || matches!(expr, ast::Expr::InList { list, .. } if list.len() > 8 || list.iter().any(row_constructor))
        || children(expr).into_iter().any(contains_subquery)
}

fn function_query(function: &ast::Function) -> Option<(&ast::Query, SubqueryValue)> {
    let [ast::ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else { return None; };
    let ast::FunctionArguments::Subquery(query) = &function.args else { return None; };
    if name.quote_style.is_some() { return None; }
    let value = match name.value.to_ascii_lowercase().as_str() {
        "array" => SubqueryValue::Array,
        "nest_one" => SubqueryValue::NestOne,
        "nest_many" => SubqueryValue::NestMany,
        _ => return None,
    };
    Some((query, value))
}

fn row_constructor(expr: &ast::Expr) -> bool {
    match expr {
        ast::Expr::Tuple(_) => true,
        ast::Expr::Function(function) => identifiers(&function.name).is_ok_and(|names| names.len() == 1 && !names[0].quoted && names[0].normalized() == "row"),
        _ => false,
    }
}

pub(crate) fn references_unqualified_names(expr: &ast::Expr, names: &std::collections::BTreeSet<String>) -> bool {
    matches!(expr, ast::Expr::Identifier(name) if names.contains(&identifier(name).normalized()))
        || children(expr).into_iter().any(|expr| references_unqualified_names(expr, names))
}

pub(crate) fn reject_ambiguous_columns(expr: &ast::Expr, schema: &Schema) -> Result<()> {
    let names = match expr { ast::Expr::Identifier(name) => Some(vec![identifier(name)]), ast::Expr::CompoundIdentifier(names) => Some(names.iter().map(identifier).collect()), _ => None };
    if let Some(names) = names {
        if let Err(error) = schema.resolve(&names) { if error.code == "xtdb/ambiguous-column" { return Err(error); } }
    }
    for child in children(expr) { reject_ambiguous_columns(child, schema)?; }
    Ok(())
}

pub(crate) fn children(expr: &ast::Expr) -> Vec<&ast::Expr> {
    match expr {
        ast::Expr::Floor { expr, .. } | ast::Expr::Ceil { expr, .. } | ast::Expr::Nested(expr) | ast::Expr::UnaryOp { expr, .. } | ast::Expr::Cast { expr, .. } | ast::Expr::IsNull(expr) | ast::Expr::IsNotNull(expr) | ast::Expr::IsTrue(expr) | ast::Expr::IsFalse(expr) | ast::Expr::IsUnknown(expr) | ast::Expr::IsNotTrue(expr) | ast::Expr::IsNotFalse(expr) | ast::Expr::IsNotUnknown(expr) => vec![expr],
        ast::Expr::BinaryOp { left, right, .. } | ast::Expr::IsDistinctFrom(left, right) | ast::Expr::IsNotDistinctFrom(left, right) | ast::Expr::AnyOp { left, right, .. } | ast::Expr::AllOp { left, right, .. } => vec![left, right],
        ast::Expr::Between { expr, low, high, .. } => vec![expr, low, high],
        ast::Expr::InList { expr, list, .. } => std::iter::once(expr.as_ref()).chain(list).collect(),
        ast::Expr::InSubquery { expr, .. } => vec![expr],
        ast::Expr::Like { expr, pattern, .. } | ast::Expr::ILike { expr, pattern, .. } => vec![expr,pattern],
        ast::Expr::Case { operand, conditions, else_result, .. } => operand.as_deref().into_iter().chain(conditions.iter().flat_map(|when| [&when.condition, &when.result])).chain(else_result.as_deref()).collect(),
        ast::Expr::Function(function) => if let ast::FunctionArguments::List(args) = &function.args { args.args.iter().filter_map(|arg| match arg { ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Some(expr), _ => None }).collect() } else { Vec::new() },
        ast::Expr::Array(array) => array.elem.iter().collect(),
        ast::Expr::Dictionary(fields) => fields.iter().map(|field| field.value.as_ref()).collect(),
        ast::Expr::Position { expr, r#in } => vec![expr, r#in],
        ast::Expr::Extract { expr,.. } => vec![expr],
        ast::Expr::Substring { expr, substring_from, substring_for, .. } => std::iter::once(expr.as_ref()).chain(substring_from.as_deref()).chain(substring_for.as_deref()).collect(),
        ast::Expr::Overlay { expr, overlay_what, overlay_from, overlay_for } => [expr.as_ref(), overlay_what.as_ref(), overlay_from.as_ref()].into_iter().chain(overlay_for.as_deref()).collect(),
        ast::Expr::Trim { expr, trim_what, .. } => std::iter::once(expr.as_ref()).chain(trim_what.as_deref()).collect(),
        ast::Expr::CompoundFieldAccess { root, access_chain } => std::iter::once(root.as_ref()).chain(access_chain.iter().filter_map(|access| match access {
            ast::AccessExpr::Subscript(ast::Subscript::Index { index }) => Some(index), _ => None,
        })).collect(),
        _ => Vec::new(),
    }
}
