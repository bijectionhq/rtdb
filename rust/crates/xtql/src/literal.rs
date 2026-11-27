use crate::{Edn, EdnKind, error};
use bigdecimal::BigDecimal;
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
        EdnKind::Ratio(_) => return Err(error("xtql/invalid-literal", "XTDB vectors do not support ratio objects")),
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
        "xt/uri" => { let text = string(form)?; rtdb_types::UriParts::parse(text)?; Ok(Value::Uri(text.into())) },
        "xt/date" | "xt/local-date" => {
            let date = rtdb_types::CivilDate::parse_iso(string(form)?)?;
            Ok(Value::calendar_date(date, rtdb_types::DateUnit::Day))
        }
        "xt/date-time" | "xt/local-date-time" | "xt/ldt" => {
            let timestamp = rtdb_types::parse_sql_local_datetime(string(form)?)?;
            let value = rtdb_types::Instant::new(timestamp.epoch_second(), timestamp.nano())?.as_micros()?.0;
            Ok(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: None })
        }
        "xt/time" => {
            Ok(Value::Time { value: rtdb_types::parse_iso_local_time(string(form)?)? as i64, unit: TimeUnit::Nanosecond, bit_width: 64 })
        }
        "inst" | "xt/instant" | "xt/zoned-date-time" | "xt/zdt" | "xt/offset-date-time" | "xt/odt" => {
            let text = string(form)?;
            let (timestamp, timezone) = if tag == "inst" { (crate::legacy_inst::parse(text)?, "UTC".into()) }
                else {
                    let (instant, zone) = rtdb_types::parse_sql_zoned_datetime(text)?;
                    let timezone = if matches!(tag, "xt/offset-date-time" | "xt/odt") { zone.offset_at(instant.seconds())?.to_string() } else { zone.to_string() };
                    (instant.as_micros()?.0, timezone)
                };
            Ok(Value::Timestamp { value: timestamp, unit: TimeUnit::Microsecond, timezone: Some(if tag == "inst" || tag == "xt/instant" { "UTC".into() } else { timezone }) })
        }
        "xt/duration" => {
            let IsoParts { months, days, nanos, components } = iso_interval(string(form)?)?;
            if components == 0 { return Err(error("xtql/invalid-literal", "Incomplete ISO duration")); }
            if months != 0 { return Err(error("xtql/invalid-literal", "Duration cannot contain calendar months")); }
            let too_long = || error("xtql/invalid-literal", "Duration exceeds microsecond representation");
            let micros = days.checked_mul(86_400_000_000).and_then(|micros| micros.checked_add(nanos.div_euclid(1000))).ok_or_else(too_long)?;
            Ok(Value::Duration { value: i64::try_from(micros).map_err(|_| too_long())?, unit: TimeUnit::Microsecond })
        }
        "xt/interval" => {
            let IsoParts { months, days, nanos, .. } = iso_interval(string(form)?)?;
            let overflow = |_| interval_overflow();
            Ok(interval(i32::try_from(months).map_err(overflow)?, i32::try_from(days).map_err(overflow)?, i64::try_from(nanos).map_err(overflow)?))
        }
        "xt/period-duration" => {
            let values = form.vector().filter(|values| values.len() == 2).ok_or_else(|| error("xtql/invalid-literal", "Period-duration requires two strings"))?;
            let IsoParts { months, days, .. } = iso_interval(string(&values[0])?)?;
            let IsoParts { months: duration_months, days: duration_days, nanos, .. } = iso_interval(string(&values[1])?)?;
            if duration_months != 0 { return Err(error("xtql/invalid-literal", "Duration cannot contain calendar months")); }
            // A PeriodDuration reaches past what an interval vector holds; the reference raises the
            // projection's overflow error when it emits such a literal.
            let overflow = |_| expression_overflow();
            let months = i32::try_from(months).map_err(overflow)?;
            let days = i32::try_from(days).map_err(overflow)?;
            let nanoseconds = duration_days.checked_mul(86_400_000_000_000).and_then(|day_nanos| day_nanos.checked_add(nanos)).ok_or_else(expression_overflow)?;
            let nanoseconds = i64::try_from(nanoseconds).map_err(overflow)?;
            Ok(if days == 0 && nanoseconds == 0 { Value::IntervalYearMonth(months) }
                else if months == 0 && nanoseconds % 1_000_000 == 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds } }
                else { Value::IntervalMonthDayNano { months, days, nanoseconds } })
        }
        "xt/tstz-range" => {
            let values = form.vector().filter(|values| values.len() == 2).ok_or_else(|| error("xtql/invalid-literal", "Timestamp range requires two endpoints"))?;
            let endpoint = |form: &Edn, default| -> Result<i64> {
                if matches!(form.kind, EdnKind::Nil) { return Ok(default); }
                if let EdnKind::String(value) = &form.kind { return rtdb_types::parse_sql_zoned_datetime(value)?.0.as_micros().map(|value| value.0); }
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

fn interval(months: i32, days: i32, nanoseconds: i64) -> Value {
    if days == 0 && nanoseconds == 0 { Value::IntervalYearMonth(months) }
    else if nanoseconds % 1000 == 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds } }
    else { Value::IntervalMonthDayNano { months, days, nanoseconds } }
}

/// Unbounded parts: each caller owns the range its value type admits.
struct IsoParts { months: i128, days: i128, nanos: i128, components: usize }

/// `P` and `PT` read as zero, since `Interval.ZERO` prints as `P`.
fn iso_interval(text: &str) -> Result<IsoParts> {
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
    if start != text.len() { return Err(error("xtql/invalid-literal", "Incomplete ISO interval")); }
    Ok(IsoParts { months: months * sign, days: days * sign, nanos: nanos * sign, components })
}

fn interval_overflow() -> rtdb_types::Anomaly { error("xtql/invalid-literal", "Interval exceeds its physical representation") }
fn expression_overflow() -> rtdb_types::Anomaly { error("xtdb.expression/overflow-error", "data exception - overflow error") }
