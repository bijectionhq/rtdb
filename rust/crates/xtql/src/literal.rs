use crate::{Edn, EdnKind, error};
use bigdecimal::BigDecimal;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;
use rtdb_types::{DecimalValue, Keyword, Result, TimeUnit, Value};
use std::str::FromStr;

pub(crate) fn value(form: &Edn) -> Result<Value> {
    Ok(match &form.kind {
        EdnKind::Nil => Value::Null,
        EdnKind::Bool(value) => Value::Bool(*value),
        EdnKind::Integer(value) => Value::I64(*value),
        EdnKind::Float(value) => Value::F64(*value),
        EdnKind::Decimal(value) => decimal(value)?,
        EdnKind::BigInteger(value) => Value::I64(BigInt::from_str(value).ok().and_then(|value| value.to_i64()).ok_or_else(|| error("xtql/invalid-literal", "Integer exceeds signed 64 bits"))?),
        EdnKind::String(value) => Value::Utf8(value.clone()),
        EdnKind::Keyword(value) => Value::Keyword(Keyword::from_name(value)),
        EdnKind::Character(_) => return Err(error("xtql/invalid-literal", "XTDB vectors do not support character objects")),
        EdnKind::Tagged { tag, value: literal } => tagged(tag, literal)?,
        _ => return Err(error("xtql/invalid-literal", "Form is an expression rather than a literal value")),
    })
}

fn decimal(text: &str) -> Result<Value> {
    let spelling = rtdb_types::decimal_spelling(text)?;
    let scale = spelling.scale();
    let decimal = BigDecimal::from_str(spelling.ascii()).map_err(|error_value| error("xtql/invalid-literal", error_value.to_string()))?;
    let (coefficient, _) = decimal.as_bigint_and_exponent();
    let digits = coefficient.to_str_radix(10).trim_start_matches('-').len();
    let (precision, bit_width) = if digits <= 32 { (32, 128) } else if digits <= 64 { (64, 256) } else { return Err(error("xtql/invalid-literal", "Decimal precision exceeds the JVM vector limit of 64")); };
    let coefficient_bytes = coefficient.to_signed_bytes_be();
    let mut bytes = vec![if coefficient.sign() == Sign::Minus { 255 } else { 0 }; usize::from(bit_width / 8)];
    if coefficient_bytes.len() > bytes.len() { return Err(error("xtql/invalid-literal", "Decimal coefficient exceeds physical width")); }
    let offset = bytes.len() - coefficient_bytes.len();
    bytes[offset..].copy_from_slice(&coefficient_bytes);
    Ok(Value::Decimal(DecimalValue::new(bytes, precision, scale, bit_width)?))
}

fn string(form: &Edn) -> Result<&str> {
    if let EdnKind::String(value) = &form.kind { Ok(value) }
    else { Err(error("xtql/invalid-literal", "Tagged literal requires a string payload")) }
}

fn tagged(tag: &str, form: &Edn) -> Result<Value> {
    match tag {
        "uuid" => uuid::Uuid::parse_str(string(form)?).map(|value| Value::Uuid(*value.as_bytes())).map_err(|e| error("xtql/invalid-literal", e.to_string())),
        "bytes" => {
            let value = string(form)?;
            if value.len() % 2 != 0 || !value.is_ascii() { return Err(error("xtql/invalid-literal", "Byte literal must contain pairs of hexadecimal digits")); }
            Ok(Value::Binary((0..value.len()).step_by(2).map(|index| u8::from_str_radix(&value[index..index + 2], 16).map_err(|e| error("xtql/invalid-literal", e.to_string()))).collect::<Result<_>>()?))
        }
        "xt/uri" => Ok(Value::Uri(string(form)?.into())),
        "xt/date" | "xt/local-date" => {
            let date = NaiveDate::parse_from_str(string(form)?, "%Y-%m-%d").map_err(|e| error("xtql/invalid-literal", e.to_string()))?;
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| error("xtql/invalid-literal", "Invalid epoch"))?;
            Ok(Value::DateDay(i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(|_| error("xtql/invalid-literal", "Date exceeds Arrow day representation"))?))
        }
        "xt/date-time" | "xt/local-date-time" | "xt/ldt" => {
            let timestamp = NaiveDateTime::parse_from_str(string(form)?, "%Y-%m-%dT%H:%M:%S%.f").map_err(|e| error("xtql/invalid-literal", e.to_string()))?;
            Ok(Value::Timestamp { value: timestamp.and_utc().timestamp_micros(), unit: TimeUnit::Microsecond, timezone: None })
        }
        "xt/time" => {
            let time = NaiveTime::parse_from_str(string(form)?, "%H:%M:%S%.f").map_err(|e| error("xtql/invalid-literal", e.to_string()))?;
            Ok(Value::Time { value: i64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(time.nanosecond()), unit: TimeUnit::Nanosecond, bit_width: 64 })
        }
        "inst" | "xt/instant" | "xt/zoned-date-time" | "xt/zdt" | "xt/offset-date-time" | "xt/odt" => {
            let text = string(form)?;
            let (timestamp, timezone) = timestamp(text, tag == "inst")?;
            let timestamp = if tag == "inst" { timestamp.div_euclid(1000) * 1000 } else { timestamp };
            Ok(Value::Timestamp { value: timestamp, unit: TimeUnit::Microsecond, timezone: Some(if tag == "inst" || tag == "xt/instant" { "UTC".into() } else { timezone }) })
        }
        "xt/duration" => {
            let (months, days, nanos) = iso_interval(string(form)?)?;
            if months != 0 { return Err(error("xtql/invalid-literal", "Duration cannot contain calendar months")); }
            let micros = i128::from(days) * 86_400_000_000 + nanos.div_euclid(1000);
            Ok(Value::Duration { value: i64::try_from(micros).map_err(|_| error("xtql/invalid-literal", "Duration exceeds microsecond representation"))?, unit: TimeUnit::Microsecond })
        }
        "xt/interval" => {
            let (months, days, nanoseconds) = iso_interval(string(form)?)?;
            Ok(interval(months, days, i64::try_from(nanoseconds).map_err(|_| interval_overflow())?))
        }
        "xt/period-duration" => {
            let values = form.vector().filter(|values| values.len() == 2).ok_or_else(|| error("xtql/invalid-literal", "Period-duration requires two strings"))?;
            let (months, days, _) = iso_interval(string(&values[0])?)?;
            let (duration_months, duration_days, nanos) = iso_interval(string(&values[1])?)?;
            if duration_months != 0 { return Err(error("xtql/invalid-literal", "Duration cannot contain calendar months")); }
            let nanoseconds = i64::try_from(nanos.checked_add(i128::from(duration_days) * 86_400_000_000_000).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?;
            Ok(if days == 0 && nanoseconds == 0 { Value::IntervalYearMonth(months) }
                else if months == 0 && nanoseconds % 1_000_000 == 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds } }
                else { Value::IntervalMonthDayNano { months, days, nanoseconds } })
        }
        "xt/tstz-range" => {
            let values = form.vector().filter(|values| values.len() == 2).ok_or_else(|| error("xtql/invalid-literal", "Timestamp range requires two endpoints"))?;
            let endpoint = |form: &Edn, default| -> Result<i64> {
                if matches!(form.kind, EdnKind::Nil) { return Ok(default); }
                if let EdnKind::String(value) = &form.kind { return timestamp(value, false).map(|value| value.0); }
                match value(form)? { Value::Timestamp { value, .. } => Ok(value), _ => Err(error("xtql/invalid-literal", "Timestamp range endpoint must be temporal")) }
            };
            let lower = endpoint(&values[0], i64::MIN)?;
            let upper = endpoint(&values[1], i64::MAX)?;
            if lower > upper { return Err(error("xtql/invalid-literal", "Timestamp range is reversed")); }
            Ok(Value::TimestampTzRange { lower, upper })
        }
        _ => Err(error("xtql/unknown-tag", format!("No supported XTDB value reader for #{tag}"))),
    }
}

fn timestamp(text: &str, partial: bool) -> Result<(i64, String)> {
    let (text, zone) = if let Some((timestamp, zone)) = text.split_once('[') {
        let zone = zone.strip_suffix(']').ok_or_else(|| error("xtql/invalid-literal", "Invalid zone suffix"))?;
        zone.parse::<chrono_tz::Tz>().map_err(|e| error("xtql/invalid-literal", e.to_string()))?;
        (timestamp, Some(zone.to_owned()))
    } else { (text, None) };
    let text = if partial {
        match text.len() { 4 => format!("{text}-01-01T00:00:00Z"), 7 => format!("{text}-01T00:00:00Z"), 10 => format!("{text}T00:00:00Z"), _ => text.into() }
    } else { text.into() };
    let timestamp = DateTime::parse_from_rfc3339(&text).map_err(|e| error("xtql/invalid-literal", e.to_string()))?;
    let offset = if timestamp.offset().local_minus_utc() == 0 { "Z".into() } else { timestamp.offset().to_string() };
    Ok((timestamp.timestamp_micros(), zone.unwrap_or(offset)))
}

fn interval(months: i32, days: i32, nanoseconds: i64) -> Value {
    if days == 0 && nanoseconds == 0 { Value::IntervalYearMonth(months) }
    else if nanoseconds % 1000 == 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds } }
    else { Value::IntervalMonthDayNano { months, days, nanoseconds } }
}

fn iso_interval(text: &str) -> Result<(i32, i32, i128)> {
    let (sign, text) = if let Some(text) = text.strip_prefix('-') { (-1i128, text) } else { (1, text.strip_prefix('+').unwrap_or(text)) };
    let text = text.strip_prefix('P').ok_or_else(|| error("xtql/invalid-literal", "ISO interval requires P"))?;
    let mut time = false;
    let mut start = 0;
    let mut months = 0i128;
    let mut days = 0i128;
    let mut nanos = 0i128;
    let mut components = 0;
    let mut previous = None;
    for (offset, ch) in text.char_indices() {
        if ch == 'T' {
            if time || start != offset { return Err(error("xtql/invalid-literal", "Invalid ISO interval time separator")); }
            time = true;
            start = offset + 1;
        } else if ch.is_ascii_alphabetic() {
            let number = &text[start..offset];
            let rank = match (time, ch) { (false, 'Y') => 0, (false, 'M') => 1, (false, 'W') => 2, (false, 'D') => 3, (true, 'H') => 4, (true, 'M') => 5, (true, 'S') => 6, _ => return Err(error("xtql/invalid-literal", "Invalid ISO interval unit")) };
            if previous.is_some_and(|previous| previous >= rank) { return Err(error("xtql/invalid-literal", "Repeated or out-of-order interval component")); }
            previous = Some(rank);
            let integer = || number.parse::<i128>().map_err(|_| error("xtql/invalid-literal", "Invalid interval component"));
            match (time, ch) {
                (false, 'Y') => months = months.checked_add(integer()?.checked_mul(12).ok_or_else(interval_overflow)?).ok_or_else(interval_overflow)?,
                (false, 'M') => months = months.checked_add(integer()?).ok_or_else(interval_overflow)?,
                (false, 'W') => days = days.checked_add(integer()?.checked_mul(7).ok_or_else(interval_overflow)?).ok_or_else(interval_overflow)?,
                (false, 'D') => days = days.checked_add(integer()?).ok_or_else(interval_overflow)?,
                (true, 'H' | 'M') => {
                    let unit = if ch == 'H' { 3_600_000_000_000 } else { 60_000_000_000 };
                    nanos = nanos.checked_add(integer()?.checked_mul(unit).ok_or_else(interval_overflow)?).ok_or_else(interval_overflow)?;
                }
                (true, 'S') => {
                    if number.split_once('.').or_else(|| number.split_once(',')).is_some_and(|(_, fraction)| fraction.len() > 9) {
                        return Err(error("xtql/invalid-literal", "Interval seconds allow at most nine fractional digits"));
                    }
                    let number = BigDecimal::from_str(&number.replace(',', ".")).map_err(|e| error("xtql/invalid-literal", e.to_string()))?;
                    let component = (number * BigDecimal::from(1_000_000_000i64)).to_i128().ok_or_else(interval_overflow)?;
                    nanos = nanos.checked_add(component).ok_or_else(interval_overflow)?;
                }
                _ => return Err(error("xtql/invalid-literal", "Invalid ISO interval unit")),
            }
            components += 1;
            start = offset + ch.len_utf8();
        }
    }
    if start != text.len() || components == 0 || text.ends_with('T') { return Err(error("xtql/invalid-literal", "Incomplete ISO interval")); }
    Ok((i32::try_from(months.checked_mul(sign).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?,
        i32::try_from(days.checked_mul(sign).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?,
        nanos.checked_mul(sign).ok_or_else(interval_overflow)?))
}

fn interval_overflow() -> rtdb_types::Anomaly { error("xtql/invalid-literal", "Interval exceeds its physical representation") }
