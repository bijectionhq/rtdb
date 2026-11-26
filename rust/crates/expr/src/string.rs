use crate::EvalContext;
use crate::compare::nullish;
use crate::numeric::{inapplicable, integer, number, overflow};
use rtdb_types::{Anomaly, Result, Value};

mod format;
mod java_unicode;
pub(crate) mod pattern;
pub(crate) mod render;

pub(crate) fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    if matches!(name,"like" | "ilike") { return Some(pattern::like(&args[0],&args[1],None,name == "ilike",false,context)); }
    if !matches!(name, "str" | "format" | "concat" | "trim" | "trim_leading" | "trim_trailing" | "substring" | "overlay" | "position" | "octet_position" | "character_length" | "octet_length" | "default_overlay_length" | "replace" | "reverse" | "string_to_array" | "parse_ident" | "quote_ident") { return None; }
    Some((|| {
        let input_bytes = args.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
        context.reserve(input_bytes.checked_mul(16).ok_or_else(overflow)?)?;
        if name == "format" { return format::format(args, context); }
        if name == "str" {
            let parts = args.iter().map(|value| if nullish(value) { Ok(String::new()) } else { scalar_text(value, context) }).collect::<Result<Vec<_>>>()?;
            return Ok(Value::Utf8(concat_text(&parts)?));
        }
        if name == "string_to_array" {
            if nullish(&args[0]) { return Ok(Value::Null); }
            let source = text(&args[0])?;
            context.reserve(source.len().checked_mul(std::mem::size_of::<Value>() * 2 + 64).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
            let parts = if source.is_empty() { Vec::new() }
                else if nullish(&args[1]) { source.encode_utf16().map(|unit| Value::Utf8(java_utf16([unit]))).collect() }
                else { let delimiter = text(&args[1])?; if delimiter.is_empty() { vec![Value::Utf8(source.into())] } else { source.split(delimiter).map(|part| Value::Utf8(part.into())).collect() } };
            return Ok(Value::list(parts));
        }
        if args.iter().any(nullish) { return Ok(Value::Null); }
        let binary = matches!(args[0], Value::Binary(_));
        Ok(match name {
            "concat" => {
                let length = args.iter().try_fold(0usize, |length, value| add(length, bytes(value)?.len()))?;
                let mut output = Vec::with_capacity(length);
                for value in args { output.extend_from_slice(bytes(value)?); }
                from_bytes(output, binary)?
            }
            "substring" => {
                let position = int(&args[1])?;
                let length = args.get(2).map(int).transpose()?;
                let source = bytes(&args[0])?;
                from_bytes(substring(source, position, length, !binary)?.to_vec(), binary)?
            }
            "overlay" => {
                let source = bytes(&args[0])?;
                let placing = bytes(&args[1])?;
                let start = int(&args[2])?;
                let length = int(&args[3])?;
                let prefix = substring(source, 1, Some(start.wrapping_sub(1)), !binary)?;
                let suffix = substring(source, start.wrapping_add(length), None, !binary)?;
                let mut output = Vec::with_capacity(add(add(prefix.len(), placing.len())?, suffix.len())?);
                output.extend_from_slice(prefix); output.extend_from_slice(placing); output.extend_from_slice(suffix);
                from_bytes(output, binary)?
            }
            "position" | "octet_position" => {
                let needle = bytes(&args[0])?;
                let haystack = bytes(&args[1])?;
                let position = if needle.is_empty() { Some(0) } else { haystack.windows(needle.len()).position(|candidate| candidate == needle) };
                count(position.map_or(0, |index| if name == "position" { utf8_length(&haystack[..index]) + 1 } else { index + 1 }))?
            }
            "character_length" => count(utf8_length(bytes(&args[0])?))?,
            "default_overlay_length" => count(if binary { bytes(&args[0])?.len() } else { utf8_length(bytes(&args[0])?) })?,
            "octet_length" => count(bytes(&args[0])?.len())?,
            "trim" | "trim_leading" | "trim_trailing" => {
                if binary {
                    let source = bytes(&args[0])?;
                    let octet = match args.get(1) {
                        Some(Value::Binary(bytes)) => *bytes.first().ok_or_else(|| illegal("Cannot trim with an empty binary value"))?,
                        Some(value) => numeric_octet(value)?,
                        None => b' ',
                    };
                    let start = if name == "trim_trailing" { 0 } else { source.iter().position(|byte| *byte != octet).unwrap_or(source.len()) };
                    let end = if name == "trim_leading" { source.len() } else { source.iter().rposition(|byte| *byte != octet).map_or(0, |index| index + 1) };
                    Value::Binary(source[start.min(end)..end].to_vec())
                } else {
                    let source = text(&args[0])?;
                    let trim = args.get(1).map(text).transpose()?.unwrap_or(" ");
                    let mut characters = trim.chars();
                    let character = characters.next().filter(|_| characters.next().is_none()).ok_or_else(|| Anomaly::incorrect("xtdb.expression/data-exception", "Data Exception - trim error."))?;
                    let source = if name == "trim_trailing" { source } else { source.trim_start_matches(character) };
                    let source = if name == "trim_leading" || character.len_utf16() == 2 { source } else { source.trim_end_matches(character) };
                    Value::Utf8(source.into())
                }
            }
            "replace" => {
                let source = text(&args[0])?;
                let target = text(&args[1])?;
                let replacement = text(&args[2])?;
                if target.is_empty() {
                    let replacement = replacement.encode_utf16().collect::<Vec<_>>();
                    let length = source.encode_utf16().count();
                    let bound = add(length, length.checked_add(1).and_then(|length| length.checked_mul(replacement.len())).ok_or_else(overflow)?)?;
                    context.reserve(bound.checked_mul(8).ok_or_else(overflow)?)?;
                    let mut output = Vec::with_capacity(bound);
                    output.extend_from_slice(&replacement);
                    for unit in source.encode_utf16() { output.push(unit); output.extend_from_slice(&replacement); }
                    Value::Utf8(java_utf16(output))
                } else {
                    let occurrences = source.matches(target).count();
                    let length = source.len().checked_sub(occurrences.checked_mul(target.len()).ok_or_else(overflow)?).and_then(|length| length.checked_add(occurrences.checked_mul(replacement.len())?)).ok_or_else(overflow)?;
                    check_size(length)?;
                    context.reserve(length)?;
                    Value::Utf8(source.replace(target, replacement))
                }
            }
            "reverse" => Value::Utf8(text(&args[0])?.chars().rev().collect()),
            "parse_ident" => {
                let source = text(&args[0])?;
                context.reserve(source.len().checked_mul(std::mem::size_of::<Value>() * 2 + 64).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
                Value::list(parse_ident(source)?.into_iter().map(Value::Utf8).collect())
            }
            "quote_ident" => Value::Utf8(quote_ident(text(&args[0])?)),
            _ => unreachable!(),
        })
    })())
}

fn scalar_text(value: &Value, context: &EvalContext<'_>) -> Result<String> {
    context.reserve(render::allocation_bound(value)?)?;
    if let Value::RegClass(oid) = value { return crate::pg_catalog::relation_text(*oid, context); }
    if let Value::RegProc(oid) = value { return Ok(crate::pg_catalog::proc_text(*oid)); }
    render::text(value)
}

/// SUBSTRING of text by character position, as the scalar call takes it from its integer arguments.
pub(crate) fn text_substring(source: &str, position: i64, length: Option<i64>) -> Result<&str> {
    let slice = substring(source.as_bytes(), position as i32, length.map(|length| length as i32), true)?;
    std::str::from_utf8(slice).map_err(|_| illegal("String operation produced invalid UTF-8"))
}

fn substring(source: &[u8], position: i32, length: Option<i32>, utf8: bool) -> Result<&[u8]> {
    if length.is_some_and(|length| length < 0) { return Err(illegal("Negative substring length")); }
    let zero = position.wrapping_sub(1);
    let length = length.map(|length| if zero < 0 { length.wrapping_add(zero).max(0) } else { length });
    if length == Some(0) { return Ok(&source[..0]); }
    let start = zero.max(0) as usize;
    if !utf8 {
        if start >= source.len() { return Ok(&source[..0]); }
        let end = length.map(|length| (start as i32).wrapping_add(length).min(source.len() as i32)).unwrap_or(source.len() as i32);
        return source.get(start..usize::try_from(end).map_err(|_| illegal("newLimit < 0"))?).ok_or_else(|| illegal("Invalid substring limit"));
    }
    let Some((start, _)) = source.iter().enumerate().filter(|(_, byte)| (**byte & 0xc0) != 0x80).nth(start) else { return Ok(&source[..0]); };
    let end = length.and_then(|length| source[start..].iter().enumerate().filter(|(_, byte)| (**byte & 0xc0) != 0x80).nth(length as usize).map(|(index, _)| start + index)).unwrap_or(source.len());
    Ok(&source[start..end])
}

fn parse_ident(source: &str) -> Result<Vec<String>> {
    let failure = |code: &str, description: &str| Anomaly::incorrect(format!("xtdb.expression/{code}"), format!("{description} in: {source}")).with_context("input", source.into());
    let mut remaining = source;
    let mut result = Vec::new();
    loop {
        if remaining.is_empty() { return Err(failure("zero-length-identifier", "zero-length identifier")); }
        let (part, rest) = if let Some(quoted) = remaining.strip_prefix('"') {
            let mut part = String::new();
            let mut chars = quoted.char_indices().peekable();
            let rest = loop {
                let Some((index, character)) = chars.next() else { return Err(failure("unterminated-quoted-identifier", "unterminated quoted identifier")); };
                if character == '"' {
                    if chars.peek().is_some_and(|(_, character)| *character == '"') { chars.next(); part.push('"'); }
                    else { break &quoted[index + 1..]; }
                } else { part.push(character); }
            };
            if part.is_empty() { return Err(failure("empty-quoted-identifier", "quoted identifier must not be empty")); }
            if !rest.is_empty() && !rest.starts_with('.') { return Err(failure("unexpected-character-in-identifier", "unexpected character after quoted identifier")); }
            (part, rest)
        } else {
            let end = remaining.find('.').unwrap_or(remaining.len());
            if end == 0 { return Err(failure("zero-length-identifier", "zero-length identifier")); }
            (remaining[..end].to_lowercase(), &remaining[end..])
        };
        result.push(part);
        if rest.is_empty() { return Ok(result); }
        remaining = &rest[1..];
    }
}

fn quote_ident(source: &str) -> String {
    let mut bytes = source.bytes();
    let simple = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_') && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    if simple && !include_str!("string/keywords.txt").lines().any(|keyword| source == keyword) { source.into() }
    else { format!("\"{}\"", source.replace('"', "\"\"")) }
}
fn java_utf16(units: impl IntoIterator<Item = u16>) -> String { char::decode_utf16(units).map(|character| character.unwrap_or('?')).collect() }
fn utf8_length(source: &[u8]) -> usize { source.iter().filter(|byte| (**byte & 0xc0) != 0x80).count() }
fn text(value: &Value) -> Result<&str> { if let Value::Utf8(text) = value { Ok(text) } else { Err(inapplicable("text", &[value])) } }
fn bytes(value: &Value) -> Result<&[u8]> { match value { Value::Utf8(text) => Ok(text.as_bytes()), Value::Binary(bytes) => Ok(bytes), _ => Err(inapplicable("string", &[value])) } }
fn int(value: &Value) -> Result<i32> { integer(value).map(|value| value as i32).ok_or_else(|| inapplicable("string index", &[value])) }
fn numeric_octet(value: &Value) -> Result<u8> {
    if let Some(integer) = integer(value) { return i8::try_from(integer).map(|value| value as u8).map_err(|_| illegal(&format!("Value out of range for byte: {integer}"))); }
    let value = number(value).ok_or_else(|| inapplicable("binary trim octet", &[value]))?;
    if value < -128.0 || value > 127.0 { return Err(illegal(&format!("Value out of range for byte: {value}"))); }
    Ok(value as i8 as u8)
}
fn count(value: usize) -> Result<Value> { Ok(Value::I32(i32::try_from(value).map_err(|_| overflow())?)) }
fn from_bytes(bytes: Vec<u8>, binary: bool) -> Result<Value> { if binary { Ok(Value::Binary(bytes)) } else { String::from_utf8(bytes).map(Value::Utf8).map_err(|_| illegal("String operation produced invalid UTF-8")) } }
fn concat_text(parts: &[String]) -> Result<String> { let size = parts.iter().try_fold(0, |size, part| add(size, part.len()))?; let mut result = String::with_capacity(size); for part in parts { result.push_str(part); } Ok(result) }
fn add(left: usize, right: usize) -> Result<usize> { check_size(left.checked_add(right).ok_or_else(overflow)?) }
fn check_size(size: usize) -> Result<usize> { if size > i32::MAX as usize { Err(overflow()) } else { Ok(size) } }
fn illegal(message: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression/illegal-argument-exception", message) }

pub(crate) fn float_text(value: f64, single: Option<f32>) -> String {
    single.map_or_else(|| rtdb_types::java_f64_text(value), rtdb_types::java_f32_text)
}
