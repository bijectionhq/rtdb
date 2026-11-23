use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use rtdb_types::{Anomaly, Result, TimeUnit, Value};
use std::{cmp::Ordering, str::FromStr};

fn unsupported() -> Anomaly { Anomaly::unsupported("rtdb.conformance/slt-format", "Value has no qualified pinned SLT formatter") }
fn java_order(left: &str, right: &str) -> Ordering { left.encode_utf16().cmp(right.encode_utf16()) }

fn limit() -> Anomaly { super::engine::gap("SLT formatted value exceeded its retained byte limit") }

fn real_parts(value: &Value) -> Result<(BigInt, i64, bool)> {
    let (decimal, negative) = match value {
        Value::F32(value) => {
            (BigDecimal::from_str(&f64::from(*value).to_string()).map_err(|_| unsupported())?, value.is_sign_negative())
        }
        Value::F64(value) => {
            (BigDecimal::from_str(&value.to_string()).map_err(|_| unsupported())?, value.is_sign_negative())
        }
        Value::Decimal(value) => (BigDecimal::new(BigInt::from_signed_bytes_be(&value.unscaled), i64::from(value.scale)), value.unscaled.first().is_some_and(|byte| byte & 0x80 != 0)),
        _ => return Err(Anomaly::incorrect("rtdb.conformance/slt-real-format", "Pinned %f formatter requires a floating-point or decimal value")),
    };
    let (coefficient, scale) = decimal.into_bigint_and_scale();
    Ok((coefficient, scale, negative))
}

pub(super) fn allocation_bound(kind: u8, value: &Value) -> Result<usize> {
    if kind == b'R' && !matches!(value, Value::Null | Value::Undefined)
        && !matches!(value, Value::F32(value) if !value.is_finite())
        && !matches!(value, Value::F64(value) if !value.is_finite()) {
        let (coefficient, scale, _) = real_parts(value)?;
        let digits = coefficient.to_str_radix(10);
        let length = if digits == "0" { 1 } else { digits.trim_start_matches('-').len() as i64 + (3 - scale).max(0) };
        return usize::try_from(length.max(4)).ok().and_then(|length| length.checked_add(3)).ok_or_else(limit);
    }
    value.retained_bytes()?.checked_mul(8).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(limit)
}

fn real(value: &Value, maximum: usize) -> Result<String> {
    let nonfinite = match value { Value::F32(value) => Some(f64::from(*value)), Value::F64(value) => Some(*value), _ => None };
    if let Some(value) = nonfinite.filter(|value| !value.is_finite()) {
        return Ok(if value.is_nan() { "NaN" } else if value.is_sign_negative() { "-Infinity" } else { "Infinity" }.into());
    }
    if allocation_bound(b'R', value)? > maximum { return Err(limit()); }
    let (coefficient, scale, negative) = real_parts(value)?;
    let coefficient = coefficient.to_str_radix(10);
    let mut digits = coefficient.trim_start_matches('-').as_bytes().to_vec();
    let shift = 3 - scale;
    if shift >= 0 {
        if digits != b"0" { digits.resize(digits.len() + shift as usize, b'0'); }
    } else {
        let removed = (-shift) as u64;
        if removed > digits.len() as u64 { digits = vec![b'0']; }
        else {
            let kept = digits.len() - removed as usize;
            let increment = digits[kept] >= b'5';
            digits.truncate(kept);
            if increment {
                let mut carry = true;
                for digit in digits.iter_mut().rev() {
                    if *digit < b'9' { *digit += 1; carry = false; break; }
                    *digit = b'0';
                }
                if carry { digits.insert(0, b'1'); }
            }
            if digits.is_empty() { digits.push(b'0'); }
        }
    }
    let mut result = String::with_capacity(digits.len().max(4) + 2);
    if negative { result.push('-'); }
    if digits.len() <= 3 {
        result.push_str("0.");
        result.extend(std::iter::repeat_n('0', 3 - digits.len()));
        result.push_str(std::str::from_utf8(&digits).expect("decimal digits"));
    } else {
        let split = digits.len() - 3;
        result.push_str(std::str::from_utf8(&digits[..split]).expect("decimal digits"));
        result.push('.');
        result.push_str(std::str::from_utf8(&digits[split..]).expect("decimal digits"));
    }
    Ok(result)
}

fn integer(value: &Value) -> Result<String> {
    if matches!(value, Value::Utf8(_)) { return Ok("0".into()); }
    if let Some(value) = rtdb_expr::integer(value) { return Ok((value as i64).to_string()); }
    match value {
        Value::F32(_) | Value::F64(_) | Value::Decimal(_) => {
            let number = rtdb_expr::number(value).ok_or_else(unsupported)?;
            if number < i64::MIN as f64 || number > i64::MAX as f64 {
                return Err(Anomaly::incorrect("rtdb.conformance/slt-integer-format", "Pinned long coercion exceeds signed 64 bits"));
            }
            Ok((number as i64).to_string())
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
            let text = rtdb_expr::decimal_text(value);
            if nested { format!("{text}M") } else { text }
        }
        Value::Keyword(value) => format!(":{}", value.as_str()),
        Value::Uuid(value) => uuid::Uuid::from_bytes(*value).to_string(), Value::Uri(value) => value.clone(),
        Value::DateDay(day) => rtdb_types::CivilDate::from_epoch_day(i64::from(*day))?.to_string(),
        Value::DateMilli(value) => rtdb_types::CivilDate::from_epoch_day(value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY))?.to_string(),
        Value::Time { value, unit, .. } => {
            let nanos = nanos(*value, *unit);
            if !(0..86_400_000_000_000).contains(&nanos) { return Err(unsupported()); }
            local_time((nanos / 1_000_000_000) as u32, (nanos % 1_000_000_000) as u32)
        }
        Value::Timestamp { value, unit, timezone } => {
            let nanos = nanos(*value, *unit);
            let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| unsupported())?;
            let zone = timezone.as_deref().map(rtdb_types::ZoneId::parse).transpose()?;
            let offset = zone.map(|zone| zone.offset_at(seconds)).transpose()?;
            let local = rtdb_types::CivilDateTime::from_epoch_nanos(nanos + i128::from(offset.map_or(0, |offset| offset.seconds())) * 1_000_000_000)?;
            let text = format!("{}T{}", local.date(), local_time(local.second_of_day(), local.nano()));
            match (zone, offset) {
                (Some(zone), Some(offset)) => { let name = zone.to_string(); format!("{text}{offset}{}", if name == offset.to_string() { String::new() } else { format!("[{name}]") }) },
                _ => text,
            }
        }
        Value::List(_) | Value::FixedSizeList(_) => format!("[{}]", value.list_view().unwrap().iter().map(|value| text(value.as_ref(), true)).collect::<Result<Vec<_>>>()?.join(" ")),
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
pub(super) fn cell(kind: u8, value: &Value, maximum: usize) -> Result<String> {
    if allocation_bound(kind, value)? > maximum { return Err(limit()); }
    let result = if matches!(value, Value::Null | Value::Undefined) { "NULL".into() } else { match kind {
        b'R' => real(value, maximum)?, b'I' => integer(value)?, b'T' => {
            if matches!(value, Value::Utf8(value) if value.is_empty()) { "(empty)".into() } else { text(value, false)? }
        }
        _ => return Err(unsupported()),
    } };
    if result.len() > maximum { return Err(limit()); }
    Ok(result)
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
    fn cell(kind: u8, value: &Value) -> Result<String> { super::cell(kind, value, 1024 * 1024) }
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
    fn decimal(coefficient: i128, scale: i32) -> Value {
        Value::Decimal(rtdb_types::DecimalValue::new(coefficient.to_be_bytes().to_vec(), 32, scale, 128).unwrap())
    }
    #[test] fn decimal_fixed_format_checks_expansion_before_rounding() {
        for (coefficient, scale, expected) in [(1, i32::MAX, "0.000"), (-1, i32::MAX, "-0.000"), (0, i32::MIN, "0.000"), (99995, 4, "10.000"), (-5, 4, "-0.001"), (4, 4, "0.000")] {
            assert_eq!(cell(b'R', &decimal(coefficient, scale)).unwrap(), expected);
        }
        assert_eq!(cell(b'R', &decimal(1, -129)).unwrap(), format!("1{}.000", "0".repeat(129)));
        assert_eq!(cell(b'T', &decimal(1, i32::MIN)).unwrap(), "1E+2147483648");
        assert_eq!(super::cell(b'R', &decimal(1, i32::MIN), 1024).unwrap_err().code, "rtdb.conformance/slt-harness-gap");
        assert!(allocation_bound(b'R', &decimal(1, i32::MIN)).unwrap() > i32::MAX as usize);
    }
    #[test] fn integer_formatter_uses_clojure_checked_long_coercion() {
        assert_eq!(cell(b'I', &decimal(9_007_199_254_740_993, 0)).unwrap(), "9007199254740992");
        assert_eq!(cell(b'I', &decimal(1, i32::MAX)).unwrap(), "0");
        assert!(cell(b'I', &decimal(1, i32::MIN)).is_err());
        assert!(cell(b'I', &Value::F64(f64::INFINITY)).is_err());
        assert_eq!(cell(b'I', &Value::F64(f64::NAN)).unwrap(), "0");
        assert_eq!(cell(b'I', &Value::F64(i64::MAX as f64)).unwrap(), i64::MAX.to_string());
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
    #[test] fn temporal_formatter_preserves_wide_dates_and_future_region_rules() {
        let day = rtdb_types::CivilDate::from_ymd(300_000,7,1).unwrap().epoch_day();
        assert_eq!(cell(b'T',&Value::DateDay(i32::MAX)).unwrap(),rtdb_types::CivilDate::from_epoch_day(i64::from(i32::MAX)).unwrap().to_string());
        let timestamp = Value::Timestamp { value: day * 86_400 - 7200,unit: TimeUnit::Second,timezone: Some("Europe/Paris".into()) };
        assert_eq!(cell(b'T',&timestamp).unwrap(),"+300000-07-01T00:00+02:00[Europe/Paris]");
        assert!(cell(b'T',&Value::list(vec![timestamp])).is_err());
    }
}
