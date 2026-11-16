use crate::{EvalContext, cast, compare, null_eq, sql_eq};
use crate::timezone::zone;
use crate::compare::nullish;
use crate::numeric::{decimal, decimal_value, floating_value, inapplicable, integer, integer_value, number, overflow};
use chrono::{DateTime, Timelike};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Value};
use std::cmp::Ordering;

pub use rtdb_plan::function_type;

fn text(value: &Value) -> Result<&str> { match value { Value::Utf8(text) => Ok(text), _ => Err(inapplicable("text", &[value])) } }
fn count(value: usize) -> Result<Value> { Ok(Value::I32(i32::try_from(value).map_err(|_| overflow())?)) }

pub fn scalar_call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let name = name.to_lowercase().replace('-', "_");
    function_type(&name, &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
    match name.as_str() {
        "format_type" => {
            if nullish(&args[0]) { return Ok(Value::Null); }
            let oid = match &args[0] { Value::Oid(oid) => *oid, value => i32::try_from(integer(value).ok_or_else(|| inapplicable("format_type", &[value]))?).map_err(|_| overflow())? };
            let modifier = if nullish(&args[1]) { -1 } else { i32::try_from(integer(&args[1]).ok_or_else(|| inapplicable("format_type", &[&args[1]]))?).map_err(|_| overflow())? };
            return Ok(Value::Utf8(rtdb_types::PgType::format_oid(oid, modifier)));
        }
        "_patch" => {
            let Value::Struct(right) = &args[1] else { return Err(inapplicable("_patch", &[&args[0], &args[1]])); };
            let mut fields = match &args[0] {
                Value::Struct(left) => left.clone(),
                Value::Null | Value::Undefined => std::collections::BTreeMap::new(),
                _ => return Err(inapplicable("_patch", &[&args[0], &args[1]])),
            };
            for (name, value) in right {
                if !nullish(value) || !fields.contains_key(name) { fields.insert(name.clone(), value.clone()); }
            }
            return Ok(Value::Struct(fields));
        }
        "coalesce" => return Ok(args.iter().find(|value| !nullish(value)).cloned().unwrap_or(Value::Null)),
        "nullif" => return Ok(if sql_eq(&args[0], &args[1])? == Some(true) { Value::Null } else { args[0].clone() }),
        "nil?" | "is_null" => return Ok(Value::Bool(nullish(&args[0]))),
        "true?" => return Ok(Value::Bool(args[0] == Value::Bool(true))),
        "false?" => return Ok(Value::Bool(args[0] == Value::Bool(false))),
        "boolean" => return Ok(Value::Bool(!nullish(&args[0]) && args[0] != Value::Bool(false))),
        "null_eq" => return Ok(Value::Bool(null_eq(&args[0], &args[1])?)),
        "period" => {
            let timestamp = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
            let endpoint = |value: &Value, unbounded| -> Result<i64> {
                if nullish(value) { return Ok(unbounded); }
                let Value::Timestamp { value, .. } = cast(value, &timestamp, context.timezone)? else { return Err(inapplicable("period", &[value])); };
                Ok(value)
            };
            let lower = endpoint(&args[0], i64::MIN)?;
            let upper = endpoint(&args[1], i64::MAX)?;
            if lower >= upper {
                return Err(Anomaly::incorrect("xtdb/invalid-period", "'from' must be earlier than 'to' when constructing a period")
                    .with_context("from", lower.into()).with_context("to", upper.into()));
            }
            return Ok(Value::TimestampTzRange { lower, upper });
        }
        "set" => {
            let mut items = Vec::new();
            for value in args { if !items.iter().any(|old| null_eq(old, value).unwrap_or(false)) { items.push(value.clone()); } }
            return Ok(Value::Set(items));
        }
        "boolean?" | "number?" | "integer?" | "float?" | "string?" | "keyword?" | "uuid?" | "list?" | "set?" | "map?" => {
            return Ok(Value::Bool(match name.as_str() {
                "boolean?" => matches!(args[0], Value::Bool(_)), "number?" => number(&args[0]).is_some(),
                "integer?" => integer(&args[0]).is_some(), "float?" => matches!(args[0], Value::F16(_) | Value::F32(_) | Value::F64(_)),
                "string?" => matches!(args[0], Value::Utf8(_)), "keyword?" => matches!(args[0], Value::Keyword(_)),
                "uuid?" => matches!(args[0], Value::Uuid(_)), "list?" => matches!(args[0], Value::List(_) | Value::FixedSizeList(_)),
                "set?" => matches!(args[0], Value::Set(_)), _ => matches!(args[0], Value::Struct(_) | Value::Map { .. }),
            }));
        }
        "current_timestamp" | "now" | "localtimestamp" | "current_date" | "current_time" | "localtime" => {
            let time = DateTime::from_timestamp_micros(context.current_time.0).ok_or_else(overflow)?.with_timezone(&zone(context.timezone)?);
            return Ok(match name.as_str() {
                "current_timestamp" | "now" => Value::Timestamp { value: context.current_time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
                "localtimestamp" => Value::Timestamp { value: time.naive_local().and_utc().timestamp_micros(), unit: TimeUnit::Microsecond, timezone: None },
                "current_date" => Value::DateDay(i32::try_from(time.date_naive().and_hms_opt(0, 0, 0).ok_or_else(overflow)?.and_utc().timestamp().div_euclid(86400)).map_err(|_| overflow())?),
                _ => Value::Time { value: i64::from(time.num_seconds_from_midnight()) * 1_000_000 + i64::from(time.nanosecond()) / 1_000, unit: TimeUnit::Microsecond, bit_width: 64 },
            });
        }
        _ => (),
    }
    if args.iter().any(nullish) { return Ok(Value::Null); }
    if name == "abs" { if let Some(result) = crate::temporal::absolute(&args[0]) { return result; } }
    if let Some(Value::TimestampTzRange { lower, upper }) = args.first() {
        match name.as_str() {
            "lower" => return Ok(Value::Timestamp { value: *lower, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
            "upper" => return Ok(if *upper == i64::MAX { Value::Null } else { Value::Timestamp { value: *upper, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }),
            "lower_inf" => return Ok(Value::Bool(*lower == i64::MIN)),
            "upper_inf" => return Ok(Value::Bool(*upper == i64::MAX)),
            _ => (),
        }
    }
    match name.as_str() {
        "lower" => Ok(Value::Utf8(text(&args[0])?.to_lowercase())),
        "upper" => Ok(Value::Utf8(text(&args[0])?.to_uppercase())),
        "char_length" | "character_length" | "length" => match &args[0] { Value::Utf8(text) => count(text.chars().count()), Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => count(bytes.len()), Value::List(items) => count(items.len()), value => Err(inapplicable(&name, &[value])) },
        "octet_length" => count(text(&args[0])?.len()),
        "bit_length" => count(text(&args[0])?.len().checked_mul(8).ok_or_else(overflow)?),
        "concat" => Ok(Value::Utf8(args.iter().map(text).collect::<Result<Vec<_>>>()?.concat())),
        "str" => cast(&args[0], &LogicalType::Scalar(ScalarType::Utf8), context.timezone),
        "trim" | "trim_leading" | "trim_trailing" => {
            let value = text(&args[0])?;
            let trim = args.get(1).map(text).transpose()?.unwrap_or(" ");
            let mut chars = trim.chars();
            let trim = chars.next().ok_or_else(|| Anomaly::incorrect("xtdb/trim-error", "Trim character must contain one Unicode character"))?;
            if chars.next().is_some() { return Err(Anomaly::incorrect("xtdb/trim-error", "Trim character must contain one Unicode character")); }
            Ok(Value::Utf8(match name.as_str() { "trim_leading" => value.trim_start_matches(trim), "trim_trailing" => value.trim_end_matches(trim), _ => value.trim_matches(trim) }.into()))
        }
        "substring" | "substr" => {
            let value = text(&args[0])?;
            let from = integer(args.get(1).ok_or_else(|| inapplicable(&name, &[]))?).ok_or_else(|| inapplicable(&name, &[]))?;
            let length = args.get(2).map(|value| integer(value).ok_or_else(|| inapplicable(&name, &[value]))).transpose()?;
            if length.is_some_and(|length| length < 0) { return Err(Anomaly::incorrect("xtdb/substring-error", "Negative substring length")); }
            let first = from.max(1) - 1;
            let end = length.map(|length| from.saturating_add(length).max(1) - 1).unwrap_or(i128::MAX);
            Ok(Value::Utf8(value.chars().skip(usize::try_from(first).unwrap_or(usize::MAX)).take(usize::try_from((end - first).max(0)).unwrap_or(usize::MAX)).collect()))
        }
        "position" => {
            let haystack = text(args.get(1).ok_or_else(|| inapplicable(&name, &[]))?)?;
            let needle = text(&args[0])?;
            count(haystack.find(needle).map(|index| haystack[..index].chars().count() + 1).unwrap_or(0))
        }
        "replace" => {
            if args.len() != 3 { return Err(inapplicable("replace", &[])); }
            Ok(Value::Utf8(text(&args[0])?.replace(text(&args[1])?, text(&args[2])?)))
        }
        "_iid" => Ok(Value::FixedSizeBinary(args[0].entity_id()?.iid().0.to_vec())),
        "keyword" => cast(&args[0], &LogicalType::Scalar(ScalarType::Keyword), context.timezone),
        "uuid" => cast(&args[0], &LogicalType::Scalar(ScalarType::Uuid), context.timezone),
        "uri" => cast(&args[0], &LogicalType::Scalar(ScalarType::Uri), context.timezone),
        "namespace" | "local_name" => {
            let Value::Keyword(keyword) = &args[0] else { return Err(inapplicable(&name, &[&args[0]])); };
            let parts = keyword.as_str().split_once('/');
            Ok(if name == "namespace" { parts.map(|(namespace, _)| Value::Utf8(namespace.into())).unwrap_or(Value::Null) }
                else { Value::Utf8(parts.map_or(keyword.as_str(), |(_, local)| local).into()) })
        }
        "cardinality" => match &args[0] { Value::List(items) | Value::Set(items) | Value::FixedSizeList(items) => count(items.len()), _ => Err(inapplicable(&name, &[&args[0]])) },
        "nth" => {
            let index = integer(&args[1]).ok_or_else(|| inapplicable(&name, &[&args[1]]))?;
            let items = match &args[0] { Value::List(items) | Value::FixedSizeList(items) => items, _ => return Ok(Value::Null) };
            Ok(usize::try_from(index).ok().and_then(|index| items.get(index)).cloned().unwrap_or(Value::Null))
        }
        "greatest" | "least" => {
            let mut best = args[0].clone();
            for value in &args[1..] {
                let ordering = compare(&best, value, false)?;
                if (name == "greatest" && ordering == Ordering::Less) || (name == "least" && ordering == Ordering::Greater) { best = value.clone(); }
            }
            Ok(best)
        }
        "like_regex" => {
            let mut builder = regex::RegexBuilder::new(text(&args[1])?);
            for flag in args.get(2).map(text).transpose()?.unwrap_or("").chars() {
                match flag { 'i' => { builder.case_insensitive(true); }, 's' => { builder.dot_matches_new_line(true); }, 'm' => { builder.multi_line(true); }, 'x' => { builder.ignore_whitespace(true); }, _ => return Err(Anomaly::incorrect("xtdb/invalid-regex-flag", format!("Unknown regex flag: {flag}"))) }
            }
            Ok(Value::Bool(builder.build().map_err(|error| Anomaly::incorrect("xtdb/invalid-regex", error.to_string()))?.is_match(text(&args[0])?)))
        }
        "abs" | "floor" | "ceil" | "ceiling" | "round" | "trunc" => {
            let scale = args.get(1).map(|value| integer(value).ok_or_else(|| inapplicable(&name, &[value]))).transpose()?.unwrap_or(0);
            if matches!(name.as_str(), "floor" | "ceil" | "ceiling") {
                let value = number(&args[0]).ok_or_else(|| inapplicable(&name, &[&args[0]]))?;
                return Ok(Value::F64(if name == "floor" { value.floor() } else { value.ceil() }));
            }
            if let Value::Decimal(value) = &args[0] {
                if name == "abs" { return decimal_value(decimal(value).abs(), value.precision, value.scale, value.bit_width); }
                let rounded = crate::decimal::rounded(value, i32::try_from(scale).map_err(|_| overflow())?, if name == "round" { bigdecimal::RoundingMode::HalfUp } else { bigdecimal::RoundingMode::Down })?;
                return if name == "round" { Ok(Value::Decimal(rounded)) }
                    else { crate::decimal_rescale(&rounded, value.precision, value.scale, value.bit_width).map(Value::Decimal) };
            }
            let LogicalType::Scalar(ty) = args[0].logical_type() else { return Err(inapplicable(&name, &[&args[0]])); };
            if let Some(integer) = integer(&args[0]) {
                if name == "round" && args.len() == 2 {
                    let precision = match ty { ScalarType::I32 => 10, ScalarType::I64 => 19, _ => return Err(inapplicable(&name, &[&args[0]])) };
                    let value = crate::decimal::from_coefficient(integer.into(), precision, 0, 128)?;
                    return crate::decimal::rounded(&value, i32::try_from(scale).map_err(|_| overflow())?, bigdecimal::RoundingMode::HalfUp).map(Value::Decimal);
                }
                return integer_value(if name == "abs" { integer.checked_abs().ok_or_else(overflow)? } else { integer }, &ty);
            }
            let x = number(&args[0]).ok_or_else(|| inapplicable(&name, &[&args[0]]))?;
            if name == "round" {
                if !x.is_finite() { return Ok(Value::F64(x)); }
                let scale = i64::try_from(scale).map_err(|_| overflow())?;
                let factor = 10.0_f64.powf((scale as f64).abs());
                let value = if scale >= 0 { x * factor } else { x / factor };
                if !value.is_finite() { return Err(overflow()); }
                return Ok(Value::F64(if scale >= 0 { value.round() / factor } else { value.round() * factor }));
            }
            let factor = 10.0_f64.powi(i32::try_from(scale).map_err(|_| overflow())?);
            Ok(floating_value(match name.as_str() { "abs" => x.abs(), "floor" => x.floor(), "ceil" | "ceiling" => x.ceil(), "round" => (x * factor).round() / factor, _ => (x * factor).trunc() / factor }, &ty))
        }
        "sqrt" | "exp" | "ln" | "log" | "power" | "sin" | "cos" | "tan" | "asin" | "acos" | "atan" | "atan2" | "degrees" | "radians" => {
            let x = number(&args[0]).ok_or_else(|| inapplicable(&name, &[&args[0]]))?;
            let y = args.get(1).map(|value| number(value).ok_or_else(|| inapplicable(&name, &[value]))).transpose()?;
            let value = match name.as_str() {
                "sqrt" => x.sqrt(), "exp" => x.exp(), "ln" => x.ln(),
                "log" => y.map_or_else(|| x.log10(), |y| y.log(x)), "power" => x.powf(y.ok_or_else(|| inapplicable(&name, &[]))?),
                "sin" => x.sin(), "cos" => x.cos(), "tan" => x.tan(), "asin" => x.asin(), "acos" => x.acos(),
                "atan" => x.atan(), "atan2" => x.atan2(y.ok_or_else(|| inapplicable(&name, &[]))?), "degrees" => x.to_degrees(), _ => x.to_radians(),
            };
            Ok(Value::F64(value))
        }
        _ => Err(Anomaly::unsupported("xtdb/unknown-function", format!("Unknown scalar function: {name}"))),
    }
}
