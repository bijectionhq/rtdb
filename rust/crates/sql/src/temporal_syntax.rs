use rtdb_types::{Result,Value};
use sqlparser::ast;
use crate::bind::error;

fn argument(arguments: &[ast::FunctionArg],index: usize) -> Option<&ast::Expr> {
    match arguments.get(index)? { ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => Some(expr),_ => None }
}
fn character_string(expression: &ast::Expr) -> Result<Option<String>> {
    let ast::Expr::Value(value) = expression else { return Ok(None); };
    if !matches!(value.value,ast::Value::SingleQuotedString(_) | ast::Value::EscapedStringLiteral(_) | ast::Value::DollarQuotedString(_)) { return Ok(None); }
    Ok(match crate::types::literal(&value.value)? { Value::Utf8(text) => Some(text),_ => None })
}
fn precision(expression: &ast::Expr) -> bool {
    matches!(expression,ast::Expr::Value(value) if matches!(&value.value,ast::Value::Number(text,_) if !text.is_empty() && text.bytes().all(|byte|byte.is_ascii_digit())))
}
pub(crate) fn trunc_field(expression: &ast::Expr) -> Result<String> {
    if let Some(text) = character_string(expression)? { return Ok(text.to_uppercase()); }
    if let ast::Expr::Identifier(identifier) = expression {
        let field = identifier.value.to_uppercase();
        if identifier.quote_style.is_none() && matches!(field.as_str(),"MILLENNIUM" | "CENTURY" | "DECADE" | "YEAR" | "QUARTER" | "MONTH" | "WEEK" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "MILLISECOND" | "MICROSECOND" | "NANOSECOND") { return Ok(field); }
    }
    Err(error("DATE_TRUNC requires a precision keyword or character string literal"))
}
pub(crate) fn extract_field(field: &ast::DateTimeField,syntax: &ast::ExtractSyntax) -> Result<String> {
    if !matches!(syntax,ast::ExtractSyntax::From) { return Err(error("EXTRACT requires FROM")); }
    let field = field.to_string().to_uppercase();
    if matches!(field.as_str(),"YEAR" | "MONTH" | "DAY" | "HOUR" | "MINUTE" | "SECOND" | "TIMEZONE_HOUR" | "TIMEZONE_MINUTE" | "DOW" | "ISODOW" | "DOY" | "WEEK" | "QUARTER" | "EPOCH") { Ok(field) }
    else { Err(error("Unsupported EXTRACT field syntax")) }
}
pub(crate) fn function(name: &str,function: &ast::Function) -> Result<()> {
    let arguments = match &function.args { ast::FunctionArguments::None => return Ok(()),ast::FunctionArguments::List(arguments) => arguments.args.as_slice(),_ => return Ok(()) };
    match name {
        "current_timestamp" | "now" | "current_time" | "local_time" | "localtime" | "local_timestamp" | "localtimestamp" => {
            let empty = matches!(name,"current_timestamp" | "now");
            if arguments.is_empty() && !empty || arguments.len() > 1 || !arguments.is_empty() && !argument(arguments,0).is_some_and(precision) {
                return Err(error("Temporal precision requires an unsigned integer literal"));
            }
        }
        "date_trunc" => {
            if !(2..=3).contains(&arguments.len()) { return Err(error("DATE_TRUNC requires two or three arguments")); }
            trunc_field(argument(arguments,0).ok_or_else(||error("Invalid DATE_TRUNC field"))?)?;
            if arguments.len() == 3 && character_string(argument(arguments,2).ok_or_else(||error("Invalid DATE_TRUNC timezone"))?)?.is_none() { return Err(error("DATE_TRUNC timezone requires a character string literal")); }
        }
        _ => (),
    }
    Ok(())
}


pub(crate) fn interval_literal(interval: &ast::Interval) -> Result<rtdb_plan::Expr> {
    use rtdb_plan::{Expr, IntervalField};
    let (value, negative) = match interval.value.as_ref() {
        ast::Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => (expr.as_ref(), true),
        ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus, expr } => (expr.as_ref(), false),
        value => (value, false),
    };
    let value = Expr::Literal(Value::Utf8(character_string(value)?.ok_or_else(|| error("INTERVAL requires a character string literal"))?));
    let expression = if let Some(start) = &interval.leading_field {
        let start = IntervalField::parse(&start.to_string())?;
        let fractional = match (start, interval.last_field.as_ref(), interval.leading_precision, interval.fractional_seconds_precision) {
            (IntervalField::Second, None, Some(precision), None) => precision,
            (_, _, None, precision) => precision.unwrap_or(6),
            _ => return Err(error("Unsupported interval leading precision")),
        };
        let fractional = Expr::Literal(Value::I64(i64::try_from(fractional).map_err(error)?));
        let leading = Expr::Literal(Value::I64(2));
        let unit = |field: IntervalField| Expr::Literal(Value::Utf8(field.name().into()));
        let (name, args) = if let Some(end) = &interval.last_field {
            ("multi_field_interval", vec![value, unit(start), leading, unit(IntervalField::parse(&end.to_string())?), fractional])
        } else {
            ("single_field_interval", vec![value, unit(start), leading, fractional])
        };
        let return_type = rtdb_plan::function_type_with_args(name, &args)?;
        Expr::Call { name: name.into(), args, return_type }
    } else {
        return Err(crate::bind::unsupported("Unqualified INTERVAL literals"));
    };
    Ok(if negative { Expr::unary(rtdb_plan::UnaryOp::Negate, expression) } else { expression })
}
