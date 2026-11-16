use std::collections::BTreeMap;
use std::sync::Arc;
use chrono::{DateTime, FixedOffset, NaiveDate, Offset, Timelike, Utc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::transit::TransitValue;
use rtdb_types::{Anomaly, PgType, Result, TimeUnit, Value};
use crate::{EncodedValue, binary::{PG_EPOCH_DAYS, PG_EPOCH_MICROS}, invalid, size_error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format { Text, Binary }
impl Format {
    pub fn from_code(code: i16) -> Result<Self> { match code { 0 => Ok(Self::Text), 1 => Ok(Self::Binary), _ => Err(invalid("PostgreSQL format code must be zero or one")) } }
    pub fn code(self) -> i16 { match self { Self::Text => 0, Self::Binary => 1 } }
}

#[derive(Debug, Default)]
pub struct SessionEnvironment { parameters: BTreeMap<String, Option<String>>, _reservation: Option<Arc<MemoryReservation>> }
impl SessionEnvironment {
    pub fn new<'a>(parameters: impl Iterator<Item = (&'a str, Option<&'a str>)> + Clone, timezone: &str, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = parameters.clone().try_fold(timezone.len() + 1024, |bytes, (key, value)| bytes.checked_add(key.len()).and_then(|n| n.checked_add(value.map_or(0, str::len))).and_then(|n| n.checked_add(256)).ok_or_else(size_error))?;
        let reservation = memory.reserve(bytes)?;
        let mut parameters: BTreeMap<_, _> = parameters.map(|(key, value)| (key.to_owned(), value.map(str::to_owned))).collect();
        parameters.insert("timezone".into(), Some(timezone.into()));
        Ok(Self { parameters, _reservation: Some(reservation) })
    }
    pub fn parameter(&self, name: &str) -> Option<&str> { self.parameters.get(name).and_then(Option::as_deref) }
    pub fn fallback(&self) -> PgType { match self.parameter("fallback_output_format").map(str::to_ascii_lowercase).as_deref() { Some("json-ld" | "json_ld") => PgType::JsonLd, Some("transit") => PgType::Transit, _ => PgType::Json } }
}

/// Null and undefined produce no payload; other values retain their serialization reservation in the result.
pub fn encode(ty: PgType, format: Format, value: &Value, environment: &SessionEnvironment, memory: &Arc<MemoryBudget>) -> Result<Option<EncodedValue>> {
    if matches!(value, Value::Null | Value::Undefined) { return Ok(None); }
    let mut bound = value.retained_bytes()?.checked_mul(32).and_then(|n| n.checked_add(8192)).ok_or_else(size_error)?;
    if ty == PgType::Numeric && format == Format::Binary {
        if let Value::Decimal(value) = value { bound = bound.checked_add(crate::binary::numeric_allocation_bound(value)?).ok_or_else(size_error)?; }
    }
    let reservation = memory.reserve(bound)?;
    let bytes = encode_value(ty, format, value, environment)?;
    if bytes.capacity() > reservation.bytes() { return Err(size_error()); }
    Ok(Some(EncodedValue { bytes, _reservation: reservation }))
}

/// Requires caller-owned scratch and output reservations; use `encode` for a retained payload owner.
pub fn encode_value(mut ty: PgType, format: Format, value: &Value, environment: &SessionEnvironment) -> Result<Vec<u8>> {
    if ty == PgType::Default { ty = environment.fallback(); }
    let binary = format == Format::Binary;
    let bytes = match ty {
        PgType::Int2 | PgType::Int4 | PgType::Int8 => {
            let integer = rtdb_expr::integer(value).ok_or_else(|| incompatible(ty, value))?;
            if binary { match ty { PgType::Int2 => (integer as i16).to_be_bytes().to_vec(), PgType::Int4 => (integer as i32).to_be_bytes().to_vec(), _ => (integer as i64).to_be_bytes().to_vec() } }
            else { integer.to_string().into_bytes() }
        }
        PgType::Float4 | PgType::Float8 => {
            let number = rtdb_expr::number(value).ok_or_else(|| incompatible(ty, value))?;
            if binary { if ty == PgType::Float4 { (number as f32).to_be_bytes().to_vec() } else { number.to_be_bytes().to_vec() } }
            else { if ty == PgType::Float4 { float_text(f64::from(number as f32), Some(number as f32)) } else { float_text(number, None) }.into_bytes() }
        }
        PgType::Bool => { let Value::Bool(value) = value else { return Err(incompatible(ty, value)); }; if binary { vec![u8::from(*value)] } else { if *value { b"t" } else { b"f" }.to_vec() } }
        PgType::Null | PgType::Text | PgType::VarChar | PgType::Keyword => match value { Value::Utf8(value) => value.as_bytes().to_vec(), Value::Keyword(value) => value.as_str().as_bytes().to_vec(), _ => return Err(incompatible(ty, value)) },
        PgType::Numeric => { let Value::Decimal(decimal) = value else { return Err(incompatible(ty, value)); }; if binary { crate::binary::numeric_write(decimal)? } else { decimal_text(decimal).into_bytes() } }
        PgType::Timestamp | PgType::TimestampTz => {
            let Value::Timestamp { value, unit, timezone } = value else { return Err(incompatible(ty, value)); };
            if binary {
                let nanos = nanos(*value, *unit);
                let micros = if ty == PgType::Timestamp { nanos / 1000 } else { nanos.div_euclid(1000) };
                i64::try_from(micros).map_err(invalid)?.wrapping_sub(PG_EPOCH_MICROS).to_be_bytes().to_vec()
            } else { timestamp_text(*value, *unit, timezone.as_deref().filter(|_| ty == PgType::TimestampTz), environment.parameter("datestyle") == Some("iso8601"), true)?.into_bytes() }
        }
        PgType::Date => {
            let days = match value {
                Value::DateDay(days) => *days,
                Value::DateMilli(_) if binary => return Err(Anomaly::fault("xtdb.arrow/unsupported-vector-operation", "getInt unsupported on DateMilliVector")),
                Value::DateMilli(value) => i32::try_from(value / 86_400_000).map_err(invalid)?,
                _ => return Err(incompatible(ty, value)),
            };
            if binary { days.wrapping_sub(PG_EPOCH_DAYS).to_be_bytes().to_vec() } else { date_text(days)?.into_bytes() }
        }
        PgType::Time => { let Value::Time { value, unit, .. } = value else { return Err(incompatible(ty, value)); }; let value = nanos(*value, *unit); if binary { i64::try_from(value / 1000).map_err(invalid)?.to_be_bytes().to_vec() } else { time_text(value, true)?.into_bytes() } }
        PgType::Interval => { let (months, days, nanos) = interval_parts(value)?; interval_text(months, days, nanos / 1000 * 1000).into_bytes() }
        PgType::Duration => { if binary { return Err(unsupported(ty, "binary writer")); } let Value::Duration { value, unit } = value else { return Err(incompatible(ty, value)); }; duration_text(nanos(*value, *unit)).into_bytes() }
        PgType::TsTzRange => { let Value::TimestampTzRange { lower, upper } = value else { return Err(incompatible(ty, value)); }; let bound = |value: i64, absent: i64| if value == absent { Ok(String::new()) } else { timestamp_text(value, TimeUnit::Microsecond, Some("UTC"), false, false) }; format!("[{},{})", bound(*lower, i64::MIN)?, bound(*upper, i64::MAX)?).into_bytes() }
        PgType::Bytes => { let bytes = match value { Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => bytes, _ => return Err(incompatible(ty, value)) }; if binary { bytes.clone() } else { let mut output = String::from("\\x"); for byte in bytes { use std::fmt::Write; write!(&mut output, "{byte:02x}").map_err(invalid)?; } output.into_bytes() } }
        PgType::Uuid => { let Value::Uuid(bytes) = value else { return Err(incompatible(ty, value)); }; if binary { bytes.to_vec() } else { uuid::Uuid::from_bytes(*bytes).to_string().into_bytes() } }
        PgType::Oid | PgType::RegClass | PgType::RegProc => { let number = match value { Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) | Value::I32(value) => *value as u32, _ => return Err(incompatible(ty, value)) }; number.to_string().into_bytes() }
        PgType::Int4s | PgType::Int8s | PgType::Texts => array_write(ty, format, value, environment)?,
        PgType::Json | PgType::JsonLd => { let mut output = String::new(); json(value, ty == PgType::JsonLd, &mut output, 0)?; output.into_bytes() }
        PgType::Transit => rtdb_format::transit::encode_json(&to_transit(value, 0)?)?,
        PgType::Jsonb => return Err(unsupported(ty, "writer")),
        PgType::Default => return Err(unsupported(ty, "writer")),
    };
    Ok(bytes)
}

fn array_write(ty: PgType, format: Format, value: &Value, env: &SessionEnvironment) -> Result<Vec<u8>> {
    let values = match value { Value::List(values) | Value::FixedSizeList(values) => values, _ => return Err(incompatible(ty, value)) };
    let element = match ty { PgType::Int4s => PgType::Int4, PgType::Int8s => PgType::Int8, _ => PgType::Text };
    if format == Format::Binary {
        let mut result = Vec::new();
        for value in [1, 0, element.descriptor().oid, i32::try_from(values.len()).map_err(invalid)?, 1] { result.extend_from_slice(&value.to_be_bytes()); }
        for value in values {
            if matches!(value, Value::Null | Value::Undefined) { if ty != PgType::Texts { return Err(invalid("Integer binary arrays do not support null elements")); } result.extend_from_slice(&(-1i32).to_be_bytes()); }
            else { let bytes = encode_value(element, format, value, env)?; result.extend_from_slice(&i32::try_from(bytes.len()).map_err(invalid)?.to_be_bytes()); result.extend_from_slice(&bytes); }
        }
        Ok(result)
    } else {
        let mut output = String::from("{");
        for (index, value) in values.iter().enumerate() {
            if index != 0 { output.push(','); }
            if matches!(value, Value::Null | Value::Undefined) { output.push_str("NULL"); }
            else { let bytes = encode_value(element, format, value, env)?; let text = std::str::from_utf8(&bytes).map_err(invalid)?; if ty == PgType::Texts { output.push('"'); for ch in text.chars() { if matches!(ch, '\\' | '"') { output.push('\\'); } output.push(ch); } output.push('"'); } else { output.push_str(text); } }
        }
        output.push('}'); Ok(output.into_bytes())
    }
}

pub(crate) fn nanos(value: i64, unit: TimeUnit) -> i128 { i128::from(value) * match unit { TimeUnit::Second => 1_000_000_000, TimeUnit::Millisecond => 1_000_000, TimeUnit::Microsecond => 1000, TimeUnit::Nanosecond => 1 } }
fn date_text(days: i32) -> Result<String> { let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| invalid("Invalid date epoch"))?; Ok(epoch.checked_add_signed(chrono::Duration::days(i64::from(days))).ok_or_else(|| invalid("Date exceeds calendar range"))?.to_string()) }
fn fraction(nanos: u32, grouped: bool) -> String {
    if nanos == 0 { return String::new(); }
    let width = if grouped { if nanos % 1_000_000 == 0 { 3 } else if nanos % 1000 == 0 { 6 } else { 9 } } else { 9 };
    let mut value = format!("{nanos:09}"); value.truncate(width); if !grouped { while value.ends_with('0') { value.pop(); } } format!(".{value}")
}
fn time_text(value: i128, force_seconds: bool) -> Result<String> {
    if !(0..86_400_000_000_000).contains(&value) { return Err(invalid("Time is outside a day")); }
    let seconds = value / 1_000_000_000; let nanos = (value % 1_000_000_000) as u32;
    let mut text = format!("{:02}:{:02}", seconds / 3600, seconds / 60 % 60);
    if force_seconds || seconds % 60 != 0 || nanos != 0 { text.push_str(&format!(":{:02}{}", seconds % 60, fraction(nanos, !force_seconds))); } Ok(text)
}
fn timestamp_text(value: i64, unit: TimeUnit, timezone: Option<&str>, iso: bool, include_zone: bool) -> Result<String> {
    let total = nanos(value, unit); let seconds = i64::try_from(total.div_euclid(1_000_000_000)).map_err(invalid)?; let sub = total.rem_euclid(1_000_000_000) as u32;
    let instant = DateTime::<Utc>::from_timestamp(seconds, sub).ok_or_else(|| invalid("Timestamp exceeds calendar range"))?;
    let offset = match timezone { None | Some("UTC" | "Z") => FixedOffset::east_opt(0).ok_or_else(|| invalid("Invalid UTC offset"))?, Some(zone) => {
        if let Ok(zone) = zone.parse::<chrono_tz::Tz>() { instant.with_timezone(&zone).offset().fix() }
        else {
            let normalized = rtdb_expr::normalize_timezone(zone)?;
            let offset = ["UTC", "GMT", "UT"].into_iter().find_map(|prefix| normalized.strip_prefix(prefix)).unwrap_or(&normalized);
            let seconds = if offset.is_empty() || offset == "Z" { 0 } else { let mut parts = offset[1..].split(':').map(|part| part.parse::<i32>().map_err(invalid)); let hours = parts.next().transpose()?.unwrap_or(0); let minutes = parts.next().transpose()?.unwrap_or(0); let seconds = parts.next().transpose()?.unwrap_or(0); (hours * 3600 + minutes * 60 + seconds) * if offset.starts_with('-') { -1 } else { 1 } };
            FixedOffset::east_opt(seconds).ok_or_else(|| invalid("Invalid time zone offset"))?
        }
    } };
    let local = instant.with_timezone(&offset);
    let mut output = if iso { format!("{}T{}", local.date_naive(), time_text(i128::from(local.num_seconds_from_midnight()) * 1_000_000_000 + i128::from(local.nanosecond()), false)?) }
    else { format!("{} {:02}:{:02}:{:02}{}", local.date_naive(), local.hour(), local.minute(), local.second(), fraction(local.nanosecond() / 1000 * 1000, false)) };
    if let Some(zone) = timezone { if iso && offset.local_minus_utc() == 0 { output.push('Z'); } else { output.push_str(&offset.to_string()); } if iso && include_zone && !matches!(zone, "Z") && !zone.starts_with(['+', '-']) { output.push('['); output.push_str(zone); output.push(']'); } }
    Ok(output)
}
fn duration_text(nanos: i128) -> String {
    if nanos == 0 { return "PT0S".into(); }
    let seconds = nanos / 1_000_000_000; let remainder = nanos % 1_000_000_000;
    let hours = seconds / 3600; let minutes = seconds / 60 % 60; let sec = seconds % 60;
    let mut output = String::from("PT"); if hours != 0 { output.push_str(&format!("{hours}H")); } if minutes != 0 { output.push_str(&format!("{minutes}M")); }
    if sec != 0 || remainder != 0 || (hours == 0 && minutes == 0) { if sec == 0 && remainder < 0 { output.push('-'); } output.push_str(&sec.to_string()); output.push_str(&fraction(remainder.unsigned_abs() as u32, false)); output.push('S'); } output
}
fn interval_parts(value: &Value) -> Result<(i32, i32, i64)> { match value { Value::IntervalYearMonth(months) => Ok((*months, 0, 0)), Value::IntervalDayTime { days, milliseconds } => Ok((0, *days, i64::from(*milliseconds) * 1_000_000)), Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => Ok((*months, *days, *nanoseconds)), _ => Err(incompatible(PgType::Interval, value)) } }
fn interval_text(months: i32, days: i32, nanos: i64) -> String { let mut text = String::from("P"); if months / 12 != 0 { text.push_str(&format!("{}Y", months / 12)); } if months % 12 != 0 { text.push_str(&format!("{}M", months % 12)); } if days != 0 { text.push_str(&format!("{days}D")); } if nanos != 0 { text.push_str(&duration_text(i128::from(nanos))[1..]); } text }
fn decimal_text(value: &rtdb_types::DecimalValue) -> String { rtdb_expr::decimal_text(value) }
fn float_text(value: f64, single: Option<f32>) -> String {
    if value.is_nan() { return "NaN".into(); }
    if value == f64::INFINITY { return "Infinity".into(); }
    if value == f64::NEG_INFINITY { return "-Infinity".into(); }
    if value == 0.0 { return if value.is_sign_negative() { "-0.0" } else { "0.0" }.into(); }
    let mut text = single.map_or_else(|| format!("{value:?}"), |value| format!("{value:?}"));
    let mut negative = false;
    if text.starts_with('-') { negative = true; text.remove(0); }
    let (mantissa, power) = text.split_once('e').map_or((text.as_str(), 0), |(mantissa, power)| (mantissa, power.parse::<i32>().unwrap_or(0)));
    let dot = mantissa.find('.').unwrap_or(mantissa.len());
    let mut digits = mantissa.replace('.', "");
    let leading = digits.bytes().take_while(|byte| *byte == b'0').count();
    let exponent = dot as i32 - leading as i32 - 1 + power;
    digits.drain(..leading);
    while digits.len() > 1 && digits.ends_with('0') { digits.pop(); }
    let mut result = if negative { "-".to_owned() } else { String::new() };
    if (-3..7).contains(&exponent) {
        let place = exponent + 1;
        if place <= 0 { result.push_str("0."); result.extend(std::iter::repeat_n('0', (-place) as usize)); result.push_str(&digits); }
        else if place as usize >= digits.len() { result.push_str(&digits); result.extend(std::iter::repeat_n('0', place as usize - digits.len())); result.push_str(".0"); }
        else { result.push_str(&digits[..place as usize]); result.push('.'); result.push_str(&digits[place as usize..]); }
    } else {
        if digits.len() == 1 {
            let precise = single.map_or_else(|| format!("{:.1e}", value.abs()), |value| format!("{:.1e}", value.abs()));
            digits = precise.split('e').next().unwrap_or("0.0").replace('.', "");
        }
        result.push_str(&digits[..1]); result.push('.'); result.push_str(&digits[1..]); result.push('E'); result.push_str(&exponent.to_string());
    }
    result
}
fn unsupported(ty: PgType, operation: &str) -> Anomaly { Anomaly::unsupported("xtdb.pgwire/unsupported-value-operation", format!("{} has no {operation}", ty.descriptor().name)) }
fn incompatible(ty: PgType, value: &Value) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/incompatible-value", format!("Cannot serialize {:?} as {}", value.logical_type(), ty.descriptor().name)) }

fn json(value: &Value, ld: bool, output: &mut String, depth: usize) -> Result<()> {
    if depth > 128 { return Err(size_error()); }
    match value {
        Value::Null | Value::Undefined => output.push_str("null"), Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::I8(value) => output.push_str(&value.to_string()), Value::I16(value) => output.push_str(&value.to_string()), Value::I32(value) => output.push_str(&value.to_string()), Value::I64(value) => output.push_str(&value.to_string()),
        Value::U8(value) => output.push_str(&value.to_string()), Value::U16(value) => output.push_str(&value.to_string()), Value::U32(value) => output.push_str(&value.to_string()), Value::U64(value) => output.push_str(&value.to_string()),
        Value::F32(value) => { if !value.is_finite() { return Err(invalid("JSON cannot encode a non-finite number")); } output.push_str(&float_text(f64::from(*value), Some(*value))); },
        Value::F64(value) => { if !value.is_finite() { return Err(invalid("JSON cannot encode a non-finite number")); } output.push_str(&float_text(*value, None)); },
        Value::Decimal(value) => { let text = decimal_text(value); if ld { tagged_json("xt:decimal", &text, output)?; } else { output.push_str(&text); } },
        Value::Utf8(value) => json_string(value, output)?,
        Value::Struct(fields) => { output.push('{'); for (index, (key, value)) in fields.iter().enumerate() { if index != 0 { output.push(','); } json_string(key, output)?; output.push(':'); json(value, ld, output, depth + 1)?; } output.push('}'); },
        Value::Map { entries, .. } => { output.push('{'); for (index, (key, value)) in entries.iter().enumerate() { if index != 0 { output.push(','); } json_string(&display_key(key)?, output)?; output.push(':'); json(value, ld, output, depth + 1)?; } output.push('}'); },
        Value::List(values) | Value::FixedSizeList(values) | Value::Set(values) => { let set = ld && matches!(value, Value::Set(_)); if set { output.push_str("{\"@type\":\"xt:set\",\"@value\":"); } output.push('['); for (index, value) in values.iter().enumerate() { if index != 0 { output.push(','); } json(value, ld, output, depth + 1)?; } output.push(']'); if set { output.push('}'); } },
        Value::Transit(bytes) => { let error = rtdb_format::transit::decode_anomaly(bytes)?; output.push_str(&String::from_utf8(encode_error(&error, if ld { PgType::JsonLd } else { PgType::Json })?).map_err(invalid)?); },
        value => { let (tag, text) = scalar_text(value)?; if ld { tagged_json(tag, &text, output)?; } else { let text = if matches!(value, Value::Timestamp { timezone: Some(_), .. }) { text.split('[').next().unwrap_or(&text) } else { &text }; json_string(text, output)?; } },
    }
    Ok(())
}
fn json_string(value: &str, output: &mut String) -> Result<()> { output.push_str(&serde_json::to_string(value).map_err(invalid)?); Ok(()) }
fn tagged_json(tag: &str, text: &str, output: &mut String) -> Result<()> { output.push_str("{\"@type\":"); json_string(tag, output)?; output.push_str(",\"@value\":"); json_string(text, output)?; output.push('}'); Ok(()) }
fn scalar_text(value: &Value) -> Result<(&'static str, String)> {
    Ok(match value {
        Value::Keyword(value) => ("xt:keyword", value.as_str().into()), Value::Uuid(value) => ("xt:uuid", uuid::Uuid::from_bytes(*value).to_string()),
        Value::DateDay(days) => ("xt:date", date_text(*days)?), Value::DateMilli(value) => ("xt:date", date_text(i32::try_from(value / 86_400_000).map_err(invalid)?)?),
        Value::Time { value, unit, .. } => ("xt:time", time_text(nanos(*value, *unit), false)?),
        Value::Timestamp { value, unit, timezone } => (if timezone.is_some() { "xt:timestamptz" } else { "xt:timestamp" }, timestamp_text(*value, *unit, timezone.as_deref(), true, true)?),
        Value::Duration { value, unit } => ("xt:duration", duration_text(nanos(*value, *unit))),
        value @ (Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayMicro { .. } | Value::IntervalMonthDayNano { .. }) => { let (months, days, nanos) = interval_parts(value)?; ("xt:interval", interval_text(months, days, nanos)) },
        _ => return Err(Anomaly::incorrect("xtdb/json-encode-error", "Value has no pinned JSON representation")),
    })
}
fn display_key(value: &Value) -> Result<String> { match value { Value::Utf8(value) => Ok(value.clone()), Value::Keyword(value) => Ok(value.as_str().into()), Value::Null => Ok("null".into()), Value::Bool(value) => Ok(value.to_string()), value => rtdb_expr::integer(value).map(|value| value.to_string()).map(Ok).unwrap_or_else(|| scalar_text(value).map(|(_, text)| text)) } }

fn to_transit(value: &Value, depth: usize) -> Result<TransitValue> {
    use TransitValue as T;
    if depth > 128 { return Err(size_error()); }
    Ok(match value {
        Value::Undefined | Value::Null => T::Null, Value::Bool(value) => T::Bool(*value), Value::I8(value) => T::Integer(i64::from(*value)), Value::I16(value) => T::Integer(i64::from(*value)), Value::I32(value) => T::Integer(i64::from(*value)), Value::I64(value) => T::Integer(*value),
        Value::F32(value) => T::Float(f64::from(*value)), Value::F64(value) => T::Float(*value), Value::Utf8(value) => T::String(value.clone()), Value::Keyword(value) => T::Keyword(value.as_str().into()),
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => T::Bytes(bytes.clone()), Value::Transit(bytes) => rtdb_format::transit::decode_msgpack(bytes)?,
        Value::Decimal(value) => tag("f", T::String(decimal_text(value))), Value::Uuid(bytes) => tag("u", T::String(uuid::Uuid::from_bytes(*bytes).to_string())), Value::Uri(value) => tag("r", T::String(value.clone())),
        Value::List(values) | Value::FixedSizeList(values) => T::Array(values.iter().map(|value| to_transit(value, depth + 1)).collect::<Result<_>>()?),
        Value::Set(values) => tag("set", T::Array(values.iter().map(|value| to_transit(value, depth + 1)).collect::<Result<_>>()?)),
        Value::Struct(values) => T::Map(values.iter().map(|(key, value)| Ok((T::String(key.clone()), to_transit(value, depth + 1)?))).collect::<Result<_>>()?),
        Value::Map { entries, .. } => T::Map(entries.iter().map(|(key, value)| Ok((to_transit(key, depth + 1)?, to_transit(value, depth + 1)?))).collect::<Result<_>>()?),
        Value::TimestampTzRange { lower, upper } => { let bound = |value, absent| if value == absent { Ok(T::Null) } else { Ok(tag("time/zoned-date-time", T::String(timestamp_text(value, TimeUnit::Microsecond, Some("UTC"), true, true)?))) }; tag("xtdb/tstz-range", T::Array(vec![bound(*lower, i64::MIN)?, bound(*upper, i64::MAX)?])) },
        value => { let (kind, text) = scalar_text(value)?; let kind = match kind { "xt:date" => "time/date", "xt:time" => "time/time", "xt:timestamp" => "time/date-time", "xt:timestamptz" => "time/zoned-date-time", "xt:duration" => "time/duration", "xt:interval" => "xtdb/interval", _ => return Err(unsupported(PgType::Transit, "value encoder")) }; tag(kind, T::String(text)) },
    })
}
fn tag(tag: &str, representation: TransitValue) -> TransitValue { TransitValue::Tagged { tag: tag.into(), representation: Box::new(representation) } }

pub fn encode_error(error: &Anomaly, format: PgType) -> Result<Vec<u8>> {
    let category = format!("{:?}", error.category).to_ascii_lowercase().replace("notfound", "not-found");
    let value = if format == PgType::JsonLd {
        let mut data = error.context.clone(); if !error.code.is_empty() { data.insert("xtdb.error/code".into(), error.code.clone().into()); }
        serde_json::json!({"@type":format!("xt:{category}"),"@value":{"xtdb.error/message":error.message,"xtdb.error/data":data}})
    } else if format == PgType::Transit { return rtdb_format::transit::encode_json(&rtdb_format::transit::decode_msgpack(&rtdb_format::transit::encode_anomaly(error)?)?); }
    else { let mut object = serde_json::Map::new(); object.insert("category".into(), category.into()); if !error.code.is_empty() { object.insert("code".into(), error.code.clone().into()); } object.insert("message".into(), error.message.clone().into()); if !error.context.is_empty() { object.insert("data".into(), serde_json::to_value(&error.context).map_err(invalid)?); } serde_json::Value::Object(object) };
    serde_json::to_vec(&value).map_err(invalid)
}
