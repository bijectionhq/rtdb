use crate::{EvalContext,admission::Admission,transit_object::{FormatArgument,format_argument}};
use rtdb_types::{Anomaly, Result, Value};
use super::{add, float_text, illegal, scalar_text};

struct Specifier<'a> { end: usize, position: Option<&'a str>, left: bool, width: Option<&'a str>, width_argument: Option<Option<&'a str>>, kind: u8 }

pub(super) fn format(args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let source = match args.first() {
        Some(Value::Utf8(source)) => source.as_str(),
        Some(Value::Null | Value::Undefined) => return Err(Anomaly::fault("xtdb.error/unknown", "Parameter specified as non-null is null: method xtdb.expression.PgFormat.pgFormat, parameter formatStr")),
        _ => return Err(Anomaly::incorrect("xtdb.expression/type-error", "First argument to `format` must be a string")),
    };
    let arguments = args[1..].iter().map(|value| resolve(value, context)).collect::<Result<Vec<_>>>()?;
    let mut pieces = Vec::new();
    let mut last = -1i32;
    let mut end = 0usize;
    let mut cursor = 0usize;
    let mut length = 0usize;
    while cursor < source.len() {
        if source.as_bytes()[cursor] != b'%' { cursor += 1; continue; }
        let Some(specifier) = specifier(source, cursor) else { cursor += 1; continue; };
        length = add(length, cursor - end)?;
        context.reserve((cursor - end).checked_add(256).ok_or_else(crate::numeric::overflow)?)?;
        pieces.push((source[end..cursor].to_owned(), 0usize, false));
        let (formatted, padding, left) = if specifier.kind == b'%' { ("%".to_owned(), 0, false) } else {
            let width = match (specifier.width, specifier.width_argument) {
                (Some(width), _) => Some(parse_long(width)?),
                (_, Some(position)) => {
                    let index = match position { Some(position) => parse_position(position)?, None => last.wrapping_add(1) };
                    let argument = argument(&arguments, index)?;
                    last = index;
                    Some(argument.as_deref().and_then(|text| text.parse::<i64>().ok()).ok_or_else(|| illegal(&format!("width argument at position {} is not a number", index.wrapping_add(1))))?)
                }
                _ => None,
            };
            let left = specifier.left || width.is_some_and(|width| width < 0);
            let width = width.map(|width| width.wrapping_abs() as i32);
            let index = match specifier.position { Some(position) => parse_position(position)?, None => last.wrapping_add(1) };
            let argument = argument(&arguments, index)?;
            last = index;
            context.reserve(argument.as_ref().map_or(256, |value| value.len().saturating_mul(2).saturating_add(256)))?;
            let formatted = match specifier.kind {
                b's' => argument.clone().unwrap_or_default(),
                b'I' => quote_identifier(argument.as_deref().ok_or_else(|| illegal("null values cannot be formatted as an SQL identifier"))?, context)?,
                b'L' => argument.as_ref().map_or_else(|| "NULL".into(), |value| format!("'{}'", value.replace('\'', "''"))),
                _ => unreachable!(),
            };
            let characters = formatted.encode_utf16().count();
            let padding = width.filter(|width| *width > 0).map_or(0, |width| (width as usize).saturating_sub(characters));
            (formatted, padding, left)
        };
        length = add(length, add(formatted.len(), padding)?)?;
        pieces.push((formatted, padding, left));
        end = specifier.end;
        cursor = end;
    }
    length = add(length, source.len() - end)?;
    context.reserve(length)?;
    let mut output = String::with_capacity(length);
    for (text, padding, left) in pieces {
        if !left { output.extend(std::iter::repeat_n(' ', padding)); }
        output.push_str(&text);
        if left { output.extend(std::iter::repeat_n(' ', padding)); }
    }
    output.push_str(&source[end..]);
    Ok(Value::Utf8(output))
}

fn specifier(source: &str, start: usize) -> Option<Specifier<'_>> {
    let bytes = source.as_bytes();
    let mut cursor = start + 1;
    let digits = cursor;
    while bytes.get(cursor).is_some_and(u8::is_ascii_digit) { cursor += 1; }
    let position = if cursor > digits && bytes.get(cursor) == Some(&b'$') { let position = &source[digits..cursor]; cursor += 1; Some(position) } else { cursor = digits; None };
    let left = bytes.get(cursor) == Some(&b'-');
    cursor += usize::from(left);
    let mut width = None;
    let mut width_argument = None;
    if bytes.get(cursor) == Some(&b'*') {
        cursor += 1;
        let digits = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) { cursor += 1; }
        width_argument = Some(if cursor > digits && bytes.get(cursor) == Some(&b'$') { let position = &source[digits..cursor]; cursor += 1; Some(position) } else { cursor = digits; None });
    } else {
        let digits = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_digit) { cursor += 1; }
        if cursor > digits { width = Some(&source[digits..cursor]); }
    }
    let kind = *bytes.get(cursor)?;
    matches!(kind, b's' | b'I' | b'L' | b'%').then_some(Specifier { end: cursor + 1, position, left, width, width_argument, kind })
}
fn argument(arguments: &[Option<String>], index: i32) -> Result<&Option<String>> {
    usize::try_from(index).ok().and_then(|index| arguments.get(index)).ok_or_else(|| illegal(&format!("no argument at position {}", index.wrapping_add(1))))
}
fn parse_position(value: &str) -> Result<i32> { value.parse::<i32>().map(|value| value.wrapping_sub(1)).map_err(|_| number_format(value)) }
fn parse_long(value: &str) -> Result<i64> { value.parse::<i64>().map_err(|_| number_format(value)) }
fn number_format(value: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression/number-format-error", format!("For input string: \"{value}\"")) }
fn quote_identifier(source: &str, context: &EvalContext<'_>) -> Result<String> {
    context.reserve(1_048_576)?;
    let identifier = regex::RegexBuilder::new(r"\A\p{L}[\p{L}\p{Nd}_]*\z").size_limit(262_144).dfa_size_limit(262_144).build().map_err(|error| Anomaly::fault("rtdb.expr/identifier-pattern", error.to_string()))?;
    let simple = source.chars().all(|character| character.len_utf16() == 1) && identifier.is_match(source) && source == source.to_lowercase();
    Ok(if simple { source.into() } else { format!("\"{}\"", source.replace('"', "\"\"")) })
}
fn resolve(value: &Value, context: &EvalContext<'_>) -> Result<Option<String>> {
    Ok(Some(match value {
        Value::Null | Value::Undefined => return Ok(None),
        Value::Utf8(value) => value.clone(),
        Value::Keyword(value) => value.as_str().into(),
        Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => value.to_string(),
        Value::Date(value) => value.date().epoch_day().to_string(),
        Value::DateDay(value) => value.to_string(),
        Value::DateMilli(value) => (value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY)).to_string(),
        Value::Timestamp { value, .. } | Value::Time { value, .. } | Value::Duration { value, .. } => value.to_string(),
        Value::Binary(value) | Value::FixedSizeBinary(value) => String::from_utf8_lossy(value).into_owned(),
        Value::Transit(bytes) => {
            context.reserve(super::render::allocation_bound(value)?)?;
            let decoded = rtdb_format::transit::decode_msgpack(bytes)?;
            match format_argument(decoded,Admission::for_context(context.allocation))? {
                FormatArgument::Semantic(value) => value,
                FormatArgument::Wire(rtdb_format::transit::TransitValue::Null) => return Ok(None),
                FormatArgument::Wire(rtdb_format::transit::TransitValue::String(value)) => value,
                FormatArgument::Wire(_) => match crate::transit_object::str_text(bytes, Admission::for_context(context.allocation))? { Some(value) => value, None => return Ok(None) },
            }
        }
        Value::Uuid(value) => String::from_utf8_lossy(value).into_owned(),
        Value::F32(value) => float_text(f64::from(*value), Some(*value)),
        Value::F64(value) => float_text(*value, None),
        value => scalar_text(value, context)?,
    }))
}
