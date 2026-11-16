use bigdecimal::{BigDecimal, RoundingMode};
use chrono::Timelike;
use num_bigint::BigInt;
use rtdb_types::{Anomaly, Result, TimeUnit, Value};
use std::{cmp::Ordering, str::FromStr};

fn unsupported() -> Anomaly { Anomaly::unsupported("rtdb.conformance/slt-format", "Value has no qualified pinned SLT formatter") }
fn java_order(left: &str, right: &str) -> Ordering { left.encode_utf16().cmp(right.encode_utf16()) }

fn real(value: &Value) -> Result<String> {
    let (decimal, negative) = match value {
        Value::F32(value) => {
            if !value.is_finite() { return Ok(if value.is_nan() { "NaN" } else if value.is_sign_negative() { "-Infinity" } else { "Infinity" }.into()); }
            (BigDecimal::from_str(&f64::from(*value).to_string()).map_err(|_| unsupported())?, value.is_sign_negative())
        }
        Value::F64(value) => {
            if !value.is_finite() { return Ok(if value.is_nan() { "NaN" } else if value.is_sign_negative() { "-Infinity" } else { "Infinity" }.into()); }
            (BigDecimal::from_str(&value.to_string()).map_err(|_| unsupported())?, value.is_sign_negative())
        }
        Value::Decimal(value) => (BigDecimal::new(BigInt::from_signed_bytes_be(&value.unscaled), i64::from(value.scale)), value.unscaled.first().is_some_and(|byte| byte & 0x80 != 0)),
        _ => return Err(Anomaly::incorrect("rtdb.conformance/slt-real-format", "Pinned %f formatter requires a floating-point or decimal value")),
    };
    let rounded = format!("{:.3}", decimal.with_scale_round(3, RoundingMode::HalfUp));
    Ok(if negative && !rounded.starts_with('-') { format!("-{rounded}") } else { rounded })
}

fn integer(value: &Value) -> Result<String> {
    if matches!(value, Value::Utf8(_)) { return Ok("0".into()); }
    if let Some(value) = rtdb_expr::integer(value) { return Ok((value as i64).to_string()); }
    match value {
        Value::F32(value) => Ok((*value as i64).to_string()),
        Value::F64(value) => Ok((*value as i64).to_string()),
        Value::Decimal(value) => {
            let value = BigDecimal::new(BigInt::from_signed_bytes_be(&value.unscaled), i64::from(value.scale)).with_scale(0);
            let coefficient = value.as_bigint_and_exponent().0;
            let bytes = coefficient.to_signed_bytes_le();
            let mut low = [if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; 8];
            for (left, right) in low.iter_mut().zip(bytes) { *left = right; }
            Ok(i64::from_le_bytes(low).to_string())
        }
        _ => Err(Anomaly::incorrect("rtdb.conformance/slt-integer-format", "Pinned long coercion does not accept this value")),
    }
}

fn quoted(value: &str) -> String {
    let mut output = String::from("\"");
    for character in value.chars() { match character {
        '\\' => output.push_str("\\\\"), '"' => output.push_str("\\\""), '\n' => output.push_str("\\n"),
        '\r' => output.push_str("\\r"), '\t' => output.push_str("\\t"), '\u{8}' => output.push_str("\\b"),
        '\u{c}' => output.push_str("\\f"), _ => output.push(character),
    } }
    output.push('"'); output
}

fn text(value: &Value, nested: bool) -> Result<String> {
    if nested && matches!(value, Value::DateDay(_) | Value::DateMilli(_) | Value::Time { .. } | Value::Timestamp { .. } | Value::Duration { .. } | Value::Uuid(_) | Value::Uri(_)) { return Err(unsupported()); }
    let answer = match value {
        Value::Null | Value::Undefined => "nil".into(),
        Value::Utf8(value) => if nested { quoted(value) } else { value.clone() },
        Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) => value.to_string(), Value::I64(value) => value.to_string(),
        Value::U8(value) => value.to_string(), Value::U16(value) => value.to_string(), Value::U32(value) => value.to_string(), Value::U64(value) => value.to_string(),
        Value::F32(value) => java_float(f64::from(*value), value.to_string(), format!("{value:e}")), Value::F64(value) => float_text(*value),
        Value::Decimal(value) => {
            let decimal = BigDecimal::new(BigInt::from_signed_bytes_be(&value.unscaled), i64::from(value.scale));
            let text = decimal_text(&decimal);
            if nested { format!("{text}M") } else { text }
        }
        Value::Keyword(value) => format!(":{}", value.as_str()),
        Value::Uuid(value) => uuid::Uuid::from_bytes(*value).to_string(), Value::Uri(value) => value.clone(),
        Value::DateDay(day) => chrono::DateTime::from_timestamp(i64::from(*day) * 86_400, 0).ok_or_else(unsupported)?.format("%Y-%m-%d").to_string(),
        Value::DateMilli(value) => chrono::DateTime::from_timestamp((*value / 86_400_000) * 86_400, 0).ok_or_else(unsupported)?.format("%Y-%m-%d").to_string(),
        Value::Time { value, unit, .. } => {
            let nanos = nanos(*value, *unit);
            if !(0..86_400_000_000_000).contains(&nanos) { return Err(unsupported()); }
            local_time((nanos / 1_000_000_000) as u32, (nanos % 1_000_000_000) as u32)
        }
        Value::Timestamp { value, unit, timezone } => {
            let nanos = nanos(*value, *unit);
            let instant = chrono::DateTime::from_timestamp(i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| unsupported())?, nanos.rem_euclid(1_000_000_000) as u32).ok_or_else(unsupported)?;
            let local = format!("{}T{}", instant.format("%Y-%m-%d"), local_time(instant.num_seconds_from_midnight(), instant.nanosecond()));
            match timezone.as_deref() {
                Some("UTC") => format!("{local}Z[UTC]"), Some("Z") => format!("{local}Z"),
                None => local,
                _ => return Err(unsupported()),
            }
        }
        Value::List(values) | Value::FixedSizeList(values) => format!("[{}]", values.iter().map(|value| text(value, true)).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Set(values) => format!("#{{{}}}", values.iter().map(|value| text(value, true)).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Struct(values) => format!("{{{}}}", values.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).map(|(key, value)| Ok(format!("{} {}", quoted(key), text(value, true)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Map { entries, .. } => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", text(key, true)?, text(value, true)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        _ => return Err(unsupported()),
    };
    Ok(answer)
}

fn float_text(value: f64) -> String {
    java_float(value, value.to_string(), format!("{value:e}"))
}
fn java_float(value: f64, plain: String, scientific: String) -> String {
    if value.is_nan() { return "NaN".into(); }
    if value.is_infinite() { return if value.is_sign_negative() { "-Infinity" } else { "Infinity" }.into(); }
    let abs = value.abs();
    if abs != 0.0 && !(0.001..10_000_000.0).contains(&abs) {
        let (coefficient, exponent) = scientific.split_once('e').expect("exponential formatting");
        format!("{}{}E{}", coefficient, if coefficient.contains('.') { "" } else { ".0" }, exponent.parse::<i32>().expect("formatted exponent"))
    } else {
        if plain.contains('.') { plain } else { format!("{plain}.0") }
    }
}
fn nanos(value: i64, unit: TimeUnit) -> i128 { i128::from(value) * match unit { TimeUnit::Second => 1_000_000_000, TimeUnit::Millisecond => 1_000_000, TimeUnit::Microsecond => 1000, TimeUnit::Nanosecond => 1 } }
fn local_time(seconds: u32, nanos: u32) -> String {
    let mut value = format!("{:02}:{:02}", seconds / 3600, (seconds / 60) % 60);
    if seconds % 60 != 0 || nanos != 0 { value.push_str(&format!(":{:02}", seconds % 60)); }
    if nanos != 0 {
        if nanos % 1_000_000 == 0 { value.push_str(&format!(".{:03}", nanos / 1_000_000)); }
        else if nanos % 1000 == 0 { value.push_str(&format!(".{:06}", nanos / 1000)); }
        else { value.push_str(&format!(".{nanos:09}")); }
    }
    value
}
fn decimal_text(value: &BigDecimal) -> String {
    let (coefficient, scale) = value.as_bigint_and_exponent();
    let digits = coefficient.to_string();
    let magnitude = digits.trim_start_matches('-');
    let adjusted = magnitude.len() as i64 - scale - 1;
    if scale >= 0 && adjusted >= -6 {
        let sign = if digits.starts_with('-') { "-" } else { "" };
        if scale == 0 { return digits; }
        if magnitude.len() > scale as usize {
            let split = magnitude.len() - scale as usize;
            return format!("{sign}{}.{}", &magnitude[..split], &magnitude[split..]);
        }
        return format!("{sign}0.{}{magnitude}", "0".repeat(scale as usize - magnitude.len()));
    }
    let sign = if digits.starts_with('-') { "-" } else { "" };
    let coefficient = if magnitude.len() == 1 { magnitude.to_owned() } else { format!("{}.{}", &magnitude[..1], &magnitude[1..]) };
    format!("{sign}{coefficient}E{}{adjusted}", if adjusted >= 0 { "+" } else { "" })
}

pub(super) fn cell(kind: u8, value: &Value) -> Result<String> {
    if matches!(value, Value::Null | Value::Undefined) { return Ok("NULL".into()); }
    match kind {
        b'R' => real(value), b'I' => integer(value), b'T' => {
            if matches!(value, Value::Utf8(value) if value.is_empty()) { Ok("(empty)".into()) } else { text(value, false) }
        }
        _ => Err(unsupported()),
    }
}

pub(super) fn finish(mut rows: Vec<Vec<String>>, sort: &str) -> Result<String> {
    if sort == "rowsort" { rows.sort_by(|left, right| java_order(&left.join(" "), &right.join(" "))); }
    let mut values: Vec<_> = rows.into_iter().flatten().collect();
    if sort == "valuesort" { values.sort_by(|left, right| java_order(left, right)); }
    if !matches!(sort, "rowsort" | "valuesort" | "nosort") { return Err(unsupported()); }
    Ok(values.join("\n") + "\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn pinned_integer_string_null_and_empty_rules() {
        assert_eq!(cell(b'I', &Value::Utf8("123".into())).unwrap(), "0");
        assert_eq!(cell(b'R', &Value::Null).unwrap(), "NULL");
        assert_eq!(cell(b'T', &Value::Utf8(String::new())).unwrap(), "(empty)");
        assert!(cell(b'R', &Value::I64(3)).is_err());
    }
    #[test] fn real_rounding_preserves_negative_zero_and_half_up() {
        assert_eq!(cell(b'R', &Value::F64(1.2345)).unwrap(), "1.235");
        assert_eq!(cell(b'R', &Value::F64(-0.0001)).unwrap(), "-0.000");
        assert_eq!(cell(b'R', &Value::F64(f64::NAN)).unwrap(), "NaN");
    }
    #[test] fn row_sort_uses_joined_text_and_java_utf16_order() {
        assert_eq!(finish(vec![vec!["2".into(), "0".into()], vec!["10".into(), "9".into()]], "rowsort").unwrap(), "10\n9\n2\n0\n");
        assert_eq!(finish(vec![vec!["\u{e000}".into()], vec!["\u{10000}".into()]], "valuesort").unwrap(), "\u{10000}\n\u{e000}\n");
        assert_eq!(finish(Vec::new(), "nosort").unwrap(), "\n");
    }
    #[test] fn text_temporals_follow_java_objects_and_float32_keeps_its_own_digits() {
        assert_eq!(cell(b'T', &Value::F32(0.1)).unwrap(), "0.1");
        assert_eq!(cell(b'T', &Value::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).unwrap(), "1970-01-01T00:00Z[UTC]");
        assert_eq!(cell(b'T', &Value::Time { value: 123_000_000, unit: TimeUnit::Nanosecond, bit_width: 64 }).unwrap(), "00:00:00.123");
    }
}
