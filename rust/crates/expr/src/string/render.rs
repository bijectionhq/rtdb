use rtdb_format::transit::TransitValue;
use rtdb_types::{Anomaly, Result, TimeUnit, Value};
use crate::{admission::Admission, numeric::overflow, temporal::IntervalParts, transit_object::legacy_date_text};

/// Bounds scratch and output retained while casting a value to text, including Transit cache expansion.
/// Acquire this charge before calling `cast` directly and retain it until the returned text acquires its own owner.
pub fn allocation_bound(value: &Value) -> Result<usize> {
    let base = value.retained_bytes()?.checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let extra = match value {
        Value::Uri(text) => rtdb_types::UriParts::parse(text)?.ascii_allocation_bound()?,
        Value::Transit(bytes) => bytes.len().checked_mul(bytes.len()).and_then(|bytes| bytes.checked_mul(16)).ok_or_else(overflow)?,
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => values.iter().try_fold(0usize, |bytes, value| bytes.checked_add(allocation_bound(value)?).ok_or_else(overflow))?,
        Value::Struct(fields) => fields.values().try_fold(0usize, |bytes, value| bytes.checked_add(allocation_bound(value)?).ok_or_else(overflow))?,
        Value::Map { entries, .. } => entries.iter().try_fold(0usize, |bytes, (key, value)| bytes.checked_add(allocation_bound(key)?).and_then(|bytes| bytes.checked_add(allocation_bound(value).ok()?)).ok_or_else(overflow))?,
        _ => 0,
    };
    base.checked_add(extra).ok_or_else(overflow)
}

pub(crate) fn text(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Undefined | Value::Null => String::new(),
        Value::Utf8(value) | Value::Uri(value) => value.clone(),
        Value::Keyword(value) => value.to_string(),
        Value::RegProc(value) => crate::pg_catalog::proc_text(*value),
        Value::Uuid(value) => uuid::Uuid::from_bytes(*value).to_string(),
        Value::Binary(value) => format!("\\x{}", hex(value)),
        Value::Decimal(value) => crate::decimal_text(value),
        Value::F16(bits) => { let value = half::f16::from_bits(*bits).to_f32(); super::float_text(f64::from(value), Some(value)) }
        Value::F32(value) => super::float_text(f64::from(*value), Some(*value)),
        Value::F64(value) => super::float_text(*value, None),
        Value::Date(value) => value.date().to_string(),
        Value::DateDay(days) => date(i64::from(*days))?,
        Value::DateMilli(millis) => date(millis / 86_400_000)?,
        Value::Time { value, unit, .. } => time(nanos(*value, *unit))?,
        Value::Timestamp { value, unit, timezone } => timestamp(nanos(*value, *unit), timezone.as_deref())?,
        Value::Duration { value, unit } => crate::temporal_text::duration_text(i128::from(i64::try_from(nanos(*value, *unit)).map_err(|_| overflow())?)),
        value if IntervalParts::of(value).is_some() => crate::temporal_text::interval_text(IntervalParts::of(value).ok_or_else(overflow)?),
        Value::Transit(bytes) => {
            let value = rtdb_format::transit::decode_msgpack(bytes)?;
            if let TransitValue::String(value) = value { value } else { transit(&value, 0)? }
        }
        value => reader(value, 0)?,
    })
}

fn reader(value: &Value, depth: usize) -> Result<String> {
    if depth > 128 { return Err(Anomaly::incorrect("xtdb.expression/value-depth", "Printable value exceeds the nesting limit")); }
    Ok(match value {
        Value::Undefined | Value::Null => "nil".into(), Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) => value.to_string(), Value::I64(value) => value.to_string(),
        Value::U8(value) => value.to_string(), Value::U16(value) => value.to_string(), Value::U32(value) => value.to_string(), Value::U64(value) => value.to_string(),
        Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => value.to_string(),
        Value::F16(bits) => { let value = half::f16::from_bits(*bits).to_f32(); printed_float(f64::from(value), Some(value)) }
        Value::F32(value) => printed_float(f64::from(*value), Some(*value)), Value::F64(value) => printed_float(*value, None),
        Value::Utf8(value) => quoted(value), Value::Keyword(value) => quoted(value.as_str()),
        Value::Binary(value) | Value::FixedSizeBinary(value) => format!("#bytes {}", quoted(&hex(value))),
        Value::Uuid(value) => format!("#bytes {}", quoted(&hex(value))),
        Value::Uri(value) => format!("#xt/uri {}", quoted(&rtdb_types::UriParts::parse(value)?.to_ascii_string())),
        Value::Decimal(value) => format!("{}M", crate::decimal_text(value)),
        Value::Date(value) => tagged("time/date", &value.date().to_string()),
        Value::DateDay(days) => tagged("time/date", &date(i64::from(*days))?),
        Value::DateMilli(millis) => tagged("time/date", &date(millis / 86_400_000)?),
        Value::Time { value, unit, .. } => tagged("time/time", &time(nanos(*value, *unit))?),
        Value::Timestamp { value, unit, timezone } => tagged(if timezone.is_some() { "time/zoned-date-time" } else { "time/date-time" }, &timestamp(nanos(*value, *unit), timezone.as_deref())?),
        Value::Duration { value, unit } => tagged("time/duration", &crate::temporal_text::duration_text(nanos(*value, *unit))),
        value if IntervalParts::of(value).is_some() => tagged("xt/interval", &crate::temporal_text::interval_text(IntervalParts::of(value).ok_or_else(overflow)?)),
        Value::TimestampTzRange { lower, upper } => format!("[{} {}]", tagged("time/zoned-date-time", &timestamp(nanos(*lower, TimeUnit::Microsecond), Some("UTC"))?), tagged("time/zoned-date-time", &timestamp(nanos(*upper, TimeUnit::Microsecond), Some("UTC"))?)),
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => format!("[{}]", values.iter().map(|value| reader(value, depth + 1)).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Struct(fields) => format!("{{{}}}", fields.iter().map(|(name, value)| Ok(format!("{} {}", quoted(name), reader(value, depth + 1)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Map { entries, .. } => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", object(key, depth + 1)?, object(value, depth + 1)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Transit(bytes) => transit(&rtdb_format::transit::decode_msgpack(bytes)?, depth + 1)?,
        _ => return Err(crate::numeric::inapplicable("text", &[value])),
    })
}

fn object(value: &Value, depth: usize) -> Result<String> {
    if depth > 128 { return Err(Anomaly::incorrect("xtdb.expression/value-depth", "Printable value exceeds the nesting limit")); }
    Ok(match value {
        Value::Keyword(value) => value.to_string(), Value::Uuid(value) => tagged("uuid", &uuid::Uuid::from_bytes(*value).to_string()),
        Value::Set(values) => format!("#{{{}}}", values.iter().map(|value| object(value, depth + 1)).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::List(values) | Value::FixedSizeList(values) => format!("[{}]", values.iter().map(|value| object(value, depth + 1)).collect::<Result<Vec<_>>>()?.join(" ")),
        value => reader(value, depth)?,
    })
}

pub(super) fn transit(value: &TransitValue, depth: usize) -> Result<String> {
    if depth > 128 { return Err(Anomaly::incorrect("xtdb.expression/value-depth", "Printable Transit value exceeds the nesting limit")); }
    Ok(match value {
        TransitValue::Null => "nil".into(), TransitValue::Bool(value) => value.to_string(), TransitValue::Integer(value) => value.to_string(),
        TransitValue::Float(value) => printed_float(*value, None), TransitValue::String(value) => quoted(value), TransitValue::Keyword(value) => format!(":{value}"), TransitValue::Symbol(value) => value.clone(),
        TransitValue::Float32(value) => printed_float(f64::from(*value),Some(*value)),
        TransitValue::Bytes(value) => tagged("bytes", &hex(value)),
        TransitValue::Array(values) => format!("[{}]", values.iter().map(|value| transit(value, depth + 1)).collect::<Result<Vec<_>>>()?.join(" ")),
        TransitValue::Map(entries) => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", transit(key, depth + 1)?, transit(value, depth + 1)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        TransitValue::Tagged { tag, representation } => match (tag.as_str(), representation.as_ref()) {
            ("i8" | "i16" | "i32" | "i64", TransitValue::Integer(value)) => value.to_string(),
            ("f32", TransitValue::Float(value)) => printed_float(*value, Some(*value as f32)),
            ("f64", TransitValue::Float(value)) => printed_float(*value, None),
            (tag @ ("m" | "t"), representation) if legacy_date_text::HOST_CAPTURE => {
                let millis = legacy_date_text::wire_millis(tag, representation)?;
                legacy_date_text::readable(millis, Admission::Standalone)?
            }
            ("m", TransitValue::String(value)) => instant(value.parse().map_err(|_| Anomaly::incorrect("xtdb.expression/invalid-transit-date", "Invalid millisecond date representation"))?)?,
            ("n", TransitValue::String(value)) => format!("{value}N"), ("f", TransitValue::String(value)) => format!("{value}M"),
            ("u", TransitValue::String(value)) => tagged("uuid", value), ("r", TransitValue::String(value)) => tagged("xt/uri", &rtdb_types::UriParts::parse(value)?.to_ascii_string()),
            ("r", _) => return Err(Anomaly::incorrect("xtdb.expression/invalid-transit-uri", "URI Transit representation must be text")),
            ("set", TransitValue::Array(values)) => format!("#{{{}}}", values.iter().map(|value| transit(value, depth + 1)).collect::<Result<Vec<_>>>()?.join(" ")),
            ("list", TransitValue::Array(values)) => format!("({})", values.iter().map(|value| transit(value, depth + 1)).collect::<Result<Vec<_>>>()?.join(" ")),
            ("xtdb/interval", _) => format!("#xt/interval {}", transit(representation, depth + 1)?),
            ("xtdb/period-duration", _) => format!("#xt/period-duration {}", transit(representation, depth + 1)?),
            ("xtdb/tstz-range", _) => format!("#xt/tstz-range {}", transit(representation, depth + 1)?),
            ("xtdb/path", _) => format!("#xt/path {}", transit(representation, depth + 1)?),
            ("xtdb/tx-key", _) => format!("#xt/tx-key {}", transit(representation, depth + 1)?),
            _ => format!("#{tag} {}", transit(representation, depth + 1)?),
        },
    })
}

fn quoted(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() { match character { '\n' => output.push_str("\\n"), '\r' => output.push_str("\\r"), '\t' => output.push_str("\\t"), '\u{8}' => output.push_str("\\b"), '\u{c}' => output.push_str("\\f"), '\\' => output.push_str("\\\\"), '"' => output.push_str("\\\""), character => output.push(character) } }
    output.push('"'); output
}
fn tagged(tag: &str, value: &str) -> String { format!("#{tag} {}", quoted(value)) }
fn printed_float(value: f64, single: Option<f32>) -> String { if value.is_nan() { "##NaN".into() } else if value == f64::INFINITY { "##Inf".into() } else if value == f64::NEG_INFINITY { "##-Inf".into() } else { super::float_text(value, single) } }
fn hex(value: &[u8]) -> String { const HEX: &[u8] = b"0123456789abcdef"; let mut text = String::with_capacity(value.len() * 2); for byte in value { text.push(HEX[(byte >> 4) as usize] as char); text.push(HEX[(byte & 15) as usize] as char); } text }
fn nanos(value: i64, unit: TimeUnit) -> i128 { i128::from(value) * (1_000_000_000 / i128::from(unit.units_per_second())) }
fn instant(millis: i64) -> Result<String> {
    let seconds = millis.div_euclid(1000).rem_euclid(86_400);
    Ok(tagged("inst", &format!("{}T{:02}:{:02}:{:02}.{:03}-00:00", date(millis.div_euclid(86_400_000))?, seconds / 3600, seconds / 60 % 60, seconds % 60, millis.rem_euclid(1000))))
}

fn timestamp(nanos: i128, zone: Option<&str>) -> Result<String> {
    let (offset, suffix) = if let Some(zone) = zone {
        let normalized = crate::normalize_timezone(zone)?;
        let offset = crate::timezone::zone(&normalized)?.offset_at(i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| overflow())?)?.seconds();
        let absolute = offset.abs();
        let mut suffix = if offset == 0 { "Z".into() } else { format!("{}{:02}:{:02}{}", if offset < 0 { '-' } else { '+' }, absolute / 3600, absolute / 60 % 60, if absolute % 60 == 0 { String::new() } else { format!(":{:02}", absolute % 60) }) };
        if normalized != "Z" && !normalized.starts_with(['+', '-']) { suffix.push('['); suffix.push_str(&normalized); suffix.push(']'); }
        (offset, suffix)
    } else { (0, String::new()) };
    let local = nanos + i128::from(offset) * 1_000_000_000;
    Ok(format!("{}T{}{}", date(i64::try_from(local.div_euclid(86_400_000_000_000)).map_err(|_| overflow())?)?, time(local.rem_euclid(86_400_000_000_000))?, suffix))
}
fn time(nanos: i128) -> Result<String> {
    if !(0..86_400_000_000_000).contains(&nanos) { return Err(overflow()); }
    let seconds = nanos / 1_000_000_000; let fraction = nanos % 1_000_000_000;
    let mut value = format!("{:02}:{:02}", seconds / 3600, seconds / 60 % 60);
    if seconds % 60 != 0 || fraction != 0 { value.push_str(&format!(":{:02}", seconds % 60)); }
    if fraction != 0 { let digits = if fraction % 1_000_000 == 0 { 3 } else if fraction % 1000 == 0 { 6 } else { 9 }; value.push('.'); value.push_str(&format!("{fraction:09}")[..digits]); }
    Ok(value)
}
fn date(days: i64) -> Result<String> {
    Ok(rtdb_types::CivilDate::from_epoch_day(days)?.to_string())
}
