use std::collections::BTreeMap;

use rtdb_plan::{Expr, QuerySettings, Schema, SettingValue, TemporalFilter};
use rtdb_types::{Result, TimeUnit, Value};
use sqlparser::{ast, tokenizer::{Token, TokenWithSpan}};

use crate::bind::Context;
use crate::parse::{TemporalExtent, expression, sql_error, word};

#[derive(Clone, Debug, Default)]
pub(crate) struct ParsedSettings {
    entries: Vec<ParsedSetting>,
}

#[derive(Clone, Debug)]
enum ParsedSetting { Valid(TemporalExtent), System(TemporalExtent), Clock(ast::Expr), Token(ast::Expr), Time(ast::Expr) }

impl ParsedSettings {
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }
    pub fn bind(self, context: &mut Context<'_>) -> Result<(QuerySettings, Option<TemporalExtent>, Option<TemporalExtent>)> {
        let mut settings = QuerySettings::default();
        let (mut valid, mut system) = (None, None);
        for entry in self.entries {
            match entry {
                ParsedSetting::Valid(extent) => { context.validate_extent(&extent)?; valid = Some(extent); }
                ParsedSetting::System(extent) => { context.validate_extent(&extent)?; system = Some(extent); }
                ParsedSetting::Clock(value) => settings.clock_time = Some(match clock_timestamp_literal(&value)? {
                    Some(text) => SettingValue::TimestampLiteral(text), None => context.setting_value(&value)?,
                }),
                ParsedSetting::Token(value) => settings.snapshot_token = Some(context.setting_value(&value)?),
                ParsedSetting::Time(value) => settings.snapshot_time = Some(context.setting_value(&value)?),
            }
        }
        Ok((settings, valid, system))
    }
}

pub(crate) fn clock_timestamp_literal(expression: &ast::Expr) -> Result<Option<String>> {
    if let ast::Expr::Nested(expression) | ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus,expr: expression } = expression { return clock_timestamp_literal(expression); }
    let ast::Expr::TypedString(value) = expression else { return Ok(None); };
    if !matches!(value.data_type,ast::DataType::Timestamp(..)) { return Ok(None); }
    let Value::Utf8(text) = crate::types::literal(&value.value.value)? else { return Ok(None); };
    rtdb_types::parse_sql_timestamp_instant(&text,"UTC").map_err(|failure| crate::bind::error(format!("Cannot parse timestamp: {text} - failed with message {}",failure.message.as_deref().unwrap_or("null"))))?;
    Ok(Some(text))
}

pub(crate) fn extract(tokens: Vec<TokenWithSpan>, source: &str) -> Result<(Vec<TokenWithSpan>, ParsedSettings)> {
    let start = if word(tokens.first(), "EXPLAIN") { if word(tokens.get(1), "ANALYZE") { 2 } else { 1 } } else { 0 };
    if !word(tokens.get(start), "SETTING") { return Ok((tokens, ParsedSettings::default())); }
    let mut settings = ParsedSettings::default();
    let mut at = start + 1;
    loop {
        let token = tokens.get(at).ok_or_else(|| sql_error("SETTING requires a variable", source, tokens[start].span))?;
        if word(Some(token), "DEFAULT") {
            at += 1;
            let system = word(tokens.get(at), "SYSTEM_TIME");
            if !system && !word(tokens.get(at), "VALID_TIME") { return Err(sql_error("Expected VALID_TIME or SYSTEM_TIME", source, token.span)); }
            at += 1;
            if word(tokens.get(at), "TO") { at += 1; }
            let (extent, used) = parse_extent(&tokens[at..], source, token.span)?;
            at += used;
            settings.entries.push(if system { ParsedSetting::System(extent) } else { ParsedSetting::Valid(extent) });
        } else {
            let constructor = if word(Some(token), "CLOCK_TIME") { ParsedSetting::Clock as fn(ast::Expr) -> ParsedSetting }
                else if word(Some(token), "SNAPSHOT_TOKEN") { ParsedSetting::Token }
                else if word(Some(token), "SNAPSHOT_TIME") { ParsedSetting::Time }
                else { return Err(sql_error("Unknown query setting", source, token.span)); };
            at += 1;
            if word(tokens.get(at), "TO") || tokens.get(at).is_some_and(|token| token.token == Token::Eq) { at += 1; }
            else { return Err(sql_error("Expected TO or = after query setting", source, token.span)); }
            let (value, used) = expression(&tokens[at..], source, 0)?;
            settings.entries.push(constructor(value));
            at += used;
        }
        if tokens.get(at).is_some_and(|token| token.token == Token::Comma) { at += 1; } else { break; }
    }
    let mut output = tokens[..start].to_vec();
    output.extend_from_slice(&tokens[at..]);
    Ok((output, settings))
}

fn parse_extent(tokens: &[TokenWithSpan], source: &str, span: sqlparser::tokenizer::Span) -> Result<(TemporalExtent, usize)> {
    if word(tokens.first(), "ALL") { return Ok((TemporalExtent::All, 1)); }
    if word(tokens.first(), "AS") && word(tokens.get(1), "OF") {
        let (value, used) = expression(&tokens[2..], source, 0)?;
        return Ok((TemporalExtent::At(value), used + 2));
    }
    if word(tokens.first(), "FROM") {
        let (from, used) = expression(&tokens[1..], source, 0)?;
        let mut at = used + 1;
        let to = if word(tokens.get(at), "TO") {
            let (to, used) = expression(&tokens[at + 1..], source, 0)?;
            at += used + 1;
            Some(to)
        } else { None };
        return Ok((TemporalExtent::FromTo(from, to), at));
    }
    if word(tokens.first(), "BETWEEN") {
        let (from, used) = expression(&tokens[1..], source, 20)?;
        let at = used + 1;
        if !word(tokens.get(at), "AND") { return Err(sql_error("BETWEEN requires AND", source, span)); }
        let (to, used) = expression(&tokens[at + 1..], source, 20)?;
        return Ok((TemporalExtent::Between(from, to), at + used + 1));
    }
    Err(sql_error("Invalid temporal default", source, span))
}

impl Context<'_> {
    fn setting_value(&mut self, expression: &ast::Expr) -> Result<SettingValue> {
        use crate::bind::{error, unsupported};
        if let ast::Expr::Nested(expression) | ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus, expr: expression } = expression { return self.setting_value(expression); }
        if let ast::Expr::BinaryOp { left,op,right } = expression {
            if let Some(access) = crate::json_access::JsonAccess::operator(op) {
                let left = self.setting_value(left)?;
                let right_value = self.setting_value(right)?;
                access.validate(right)?;
                return Ok(SettingValue::Unevaluated { parameter_count: left.parameter_count().max(right_value.parameter_count()) });
            }
        }
        if let ast::Expr::Function(function) = expression {
            if function.over.is_some() || function.filter.is_some() || !function.within_group.is_empty() || !matches!(function.parameters, ast::FunctionArguments::None) { return Err(unsupported("Window function or scalar function modifiers")); }
            let arguments: &[ast::FunctionArg] = match &function.args {
                ast::FunctionArguments::None => &[],
                ast::FunctionArguments::List(arguments) if arguments.duplicate_treatment.is_none() && arguments.clauses.is_empty() => &arguments.args,
                _ => return Err(unsupported("Scalar function argument modifiers")),
            };
            let name = crate::expression::scalar_name(function)?;
            setting_function_arity(&name, arguments.len())?;
            if name == "regexp_replace" { regexp_arguments(arguments)?; }
            if matches!(name.as_str(),"date_bin" | "range_bins") && !matches!(arguments.first(),Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Interval(_))))) { return Err(error("Temporal bin functions require an interval literal stride")); }
            let mut parameter_count = 0;
            for (index,argument) in arguments.iter().enumerate() {
                let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(argument)) = argument else { return Err(unsupported("Named function arguments or scalar wildcard")); };
                if name == "date_trunc" && index == 0 {
                    crate::temporal_syntax::trunc_field(argument)?;
                    continue;
                }
                parameter_count = parameter_count.max(self.setting_value(argument)?.parameter_count());
            }
            return Ok(SettingValue::Unevaluated { parameter_count });
        }
        if let ast::Expr::Extract { field,syntax,.. } = expression { crate::temporal_syntax::extract_field(field,syntax)?; }
        if let ast::Expr::Cast { data_type, kind, format, .. } = expression {
            if format.is_some() || !matches!(kind, ast::CastKind::Cast | ast::CastKind::DoubleColon) { return Err(unsupported("TRY_CAST or cast format")); }
            self.cast_target(data_type)?;
        }
        if let ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } = expression {
            if let ast::Expr::Value(value) = expr.as_ref() {
                if let ast::Value::Number(number, _) = &value.value {
                    return Ok(SettingValue::Literal(if number == "9223372036854775808" { Value::I64(i64::MIN) }
                        else { match crate::types::literal(&value.value)? {
                            Value::I64(value) => Value::I64(-value), Value::F64(value) => Value::F64(-value),
                            _ => return Err(error("Expected a numeric literal")),
                        } }));
                }
            }
        }
        let children = crate::expression::children(expression);
        if !children.is_empty() {
            let mut parameter_count = 0;
            for child in children { parameter_count = parameter_count.max(self.setting_value(child)?.parameter_count()); }
            return Ok(SettingValue::Unevaluated { parameter_count });
        }
        let expression = self.expr(expression, &Schema::default(), &BTreeMap::new())?;
        Ok(match expression {
            Expr::Literal(value) => SettingValue::Literal(value),
            Expr::Parameter { index, .. } => SettingValue::Parameter { index },
            expression => SettingValue::Unevaluated { parameter_count: expression.parameter_count() },
        })
    }

    fn validate_extent(&mut self, extent: &TemporalExtent) -> Result<()> {
        match extent {
            TemporalExtent::All => (),
            TemporalExtent::At(at) => { self.setting_value(at)?; }
            TemporalExtent::FromTo(from, to) => { self.setting_value(from)?; if let Some(to) = to { self.setting_value(to)?; } }
            TemporalExtent::Between(from, to) => { self.setting_value(from)?; self.setting_value(to)?; }
        }
        Ok(())
    }

    pub(crate) fn bind_extent(&mut self, extent: TemporalExtent) -> Result<TemporalFilter> {
        Ok(match extent {
            TemporalExtent::All => TemporalFilter::All,
            TemporalExtent::At(at) => TemporalFilter::At(self.expr(&at, &Schema::default(), &BTreeMap::new())?),
            TemporalExtent::FromTo(from, to) => TemporalFilter::FromTo {
                from: self.expr(&from, &Schema::default(), &BTreeMap::new())?,
                to: to.as_ref().map(|to| self.expr(to, &Schema::default(), &BTreeMap::new())).transpose()?.unwrap_or(Expr::Literal(Value::Timestamp { value: i64::MAX, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) })),
            },
            TemporalExtent::Between(from, to) => TemporalFilter::Between { from: self.expr(&from, &Schema::default(), &BTreeMap::new())?, to: self.expr(&to, &Schema::default(), &BTreeMap::new())? },
        })
    }
}

pub(crate) fn regexp_arguments(arguments: &[ast::FunctionArg]) -> Result<()> {
    let expression = |index| match arguments.get(index) { Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expression))) => Some(expression), _ => None };
    let string = |index| matches!(expression(index), Some(ast::Expr::Value(value)) if matches!(crate::types::literal(&value.value), Ok(Value::Utf8(_))));
    let integer = |index| {
        let Some(mut expression) = expression(index) else { return false; };
        if let ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus | ast::UnaryOperator::Minus, expr } = expression { expression = expr; }
        matches!(expression, ast::Expr::Value(value) if matches!(&value.value, ast::Value::Number(number, _) if number.bytes().all(|byte| byte.is_ascii_digit())))
    };
    let valid_tail = match arguments.len() {
        3 => true, 4 => string(3) || integer(3),
        5 => integer(3) && (integer(4) || string(4)),
        6 => integer(3) && integer(4) && string(5), _ => false,
    };
    if string(1) && string(2) && valid_tail { Ok(()) } else { Err(crate::bind::error("REGEXP_REPLACE requires literal pattern, replacement, start, count and flags")) }
}

fn setting_function_arity(name: &str, count: usize) -> Result<()> {
    let arity = match name {
        "str" | "concat" | "format" | "least" | "greatest" | "coalesce" => 1..=usize::MAX,
        "version" | "xt.version" | "current_database" | "current_user" | "current_schema" | "current_catalog" | "current_date" | "current_timezone" | "local_date" | "localdate" | "snapshot_token" | "random" => 0..=0,
        "current_timestamp" | "current_time" | "localtimestamp" | "localtime" | "local_timestamp" | "local_time" | "now" => 0..=1,
        "octet_length" | "character_length" | "char_length" | "length" | "cardinality" | "abs" | "log10" | "ln" | "exp" | "sqrt" | "floor" | "ceil" | "ceiling" | "sin" | "cos" | "tan" | "sinh" | "cosh" | "tanh" | "asin" | "acos" | "atan" | "lower_inf" | "lower" | "upper" | "upper_inf" | "namespace" | "local_name" | "reverse" | "quote_ident" | "parse_ident" | "current_schemas" | "current_setting" | "sleep" | "pg_sleep" | "pg_sleep_for" => 1..=1,
        "array_upper" | "array_lower" | "array_length" | "trim_array" | "mod" | "log" | "power" | "age" | "string_to_array" | "col_description" | "pg_indexam_has_property" | "format_type" | "nullif" | "position" | "octet_position" | "period" | "tstzrange" => 2..=2,
        "round" | "trim" | "trim_leading" | "trim_trailing" => 1..=2,
        "substring" | "generate_series" | "range" | "date_trunc" | "date_bin" | "range_bins" => 2..=3,
        "replace" => 3..=3,
        "uri_scheme" | "uri_user_info" | "uri_host" | "uri_port" | "uri_path" | "uri_query" | "uri_fragment" => 1..=1,
        "regexp_replace" => 3..=6,
        "overlay" => 4..=4,
        _ => return Err(crate::bind::error(format!("Unknown function: {name}"))),
    };
    if arity.contains(&count) { Ok(()) } else { Err(crate::bind::error(format!("Function {name} arity mismatch: expected {}-{}, got {count}", arity.start(), arity.end()))) }
}
