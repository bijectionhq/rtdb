use std::collections::BTreeMap;
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::temporal_text::{TimestampText, date_text, interval_text, nanos};
use rtdb_format::transit::TransitValue;
use rtdb_types::format_iso_duration as duration_text;
use rtdb_types::{Anomaly, PgType, Result, TimeUnit, Value};
use crate::{EncodedValue, binary::{PG_EPOCH_DAYS, PG_EPOCH_MICROS}, invalid, size_error};

mod arrow;
pub use arrow::encode_arrow;

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
        if let Value::Decimal(value) = value { bound = bound.checked_add(crate::binary::numeric_allocation_bound(&value.unscaled, value.scale)?).ok_or_else(size_error)?; }
    }
    let reservation = memory.reserve(bound)?;
    let bytes = encode_value(ty, format, value, environment)?;
    if bytes.capacity() > reservation.bytes() { return Err(size_error()); }
    Ok(Some(EncodedValue { bytes, _reservation: reservation }))
}

/// Requires caller-owned scratch and output reservations; use `encode` for a retained payload owner.
/// Unadmitted calendar objects are rejected; this surface encodes completed vector cells.
pub fn encode_value(mut ty: PgType, format: Format, value: &Value, environment: &SessionEnvironment) -> Result<Vec<u8>> {
    if value.has_calendar_object() { return Err(invalid("PostgreSQL output requires admitted calendar values")); }
    if ty == PgType::Default { ty = environment.fallback(); }
    let binary = format == Format::Binary;
    let bytes = match ty {
        PgType::Int2 | PgType::Int4 | PgType::Int8 => {
            let integer = rtdb_expr::integer(value).ok_or_else(|| incompatible(ty, value))?;
            integer_bytes(ty, format, integer)
        }
        PgType::Float4 | PgType::Float8 => {
            let number = rtdb_expr::number(value).ok_or_else(|| incompatible(ty, value))?;
            if binary { if ty == PgType::Float4 { (number as f32).to_be_bytes().to_vec() } else { number.to_be_bytes().to_vec() } }
            else { if ty == PgType::Float4 { float_text(f64::from(number as f32), Some(number as f32)) } else { float_text(number, None) }.into_bytes() }
        }
        PgType::Bool => { let Value::Bool(value) = value else { return Err(incompatible(ty, value)); }; if binary { vec![u8::from(*value)] } else { if *value { b"t" } else { b"f" }.to_vec() } }
        PgType::Null | PgType::Text | PgType::VarChar | PgType::Keyword => match value { Value::Utf8(value) => value.as_bytes().to_vec(), Value::Keyword(value) => value.as_str().as_bytes().to_vec(), _ => return Err(incompatible(ty, value)) },
        PgType::Numeric => { let Value::Decimal(decimal) = value else { return Err(incompatible(ty, value)); }; if binary { crate::binary::numeric_write(&decimal.unscaled, decimal.scale)? } else { decimal_text(decimal).into_bytes() } }
        PgType::Timestamp | PgType::TimestampTz => {
            let Value::Timestamp { value, unit, timezone } = value else { return Err(incompatible(ty, value)); };
            encode_timestamp(format, *value, *unit, timezone.as_deref(), ty == PgType::TimestampTz, environment)?
        }
        PgType::Date => {
            let days = value.date_days().ok_or_else(|| incompatible(ty, value))?;
            if binary {
                let pg_days = i32::try_from(days - i64::from(PG_EPOCH_DAYS))
                    .map_err(|_| Anomaly::incorrect("xtdb/date-out-of-range", "Date is outside the PostgreSQL binary date range"))?;
                pg_days.to_be_bytes().to_vec()
            } else { date_text(days)?.into_bytes() }
        }
        PgType::Time => { let Value::Time { value, unit, .. } = value else { return Err(incompatible(ty, value)); }; let value = nanos(*value, *unit); if binary { i64::try_from(value / 1000).map_err(invalid)?.to_be_bytes().to_vec() } else { time_text(value, true)?.into_bytes() } }
        PgType::Interval => { let (months, days, nanos) = interval_parts(value)?; interval_text(months, days, nanos / 1000 * 1000).into_bytes() }
        PgType::Duration => { if binary { return Err(unsupported(ty, "binary writer")); } let Value::Duration { value, unit } = value else { return Err(incompatible(ty, value)); }; duration_text(nanos(*value, *unit)).into_bytes() }
        PgType::TsTzRange => { let Value::TimestampTzRange { lower, upper } = value else { return Err(incompatible(ty, value)); }; let bound = |value: i64, absent: i64| if value == absent { Ok(String::new()) } else { timestamp_text(value, TimeUnit::Microsecond, Some("UTC"), TimestampText::Pg) }; format!("[{},{})", bound(*lower, i64::MIN)?, bound(*upper, i64::MAX)?).into_bytes() }
        PgType::Bytes => { let bytes = match value { Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => bytes, _ => return Err(incompatible(ty, value)) }; if binary { bytes.clone() } else { let mut output = String::from("\\x"); for byte in bytes { use std::fmt::Write; write!(&mut output, "{byte:02x}").map_err(invalid)?; } output.into_bytes() } }
        PgType::Uuid => { let Value::Uuid(bytes) = value else { return Err(incompatible(ty, value)); }; if binary { bytes.to_vec() } else { uuid::Uuid::from_bytes(*bytes).to_string().into_bytes() } }
        PgType::Oid | PgType::RegClass | PgType::RegProc => { let number = match value { Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) | Value::I32(value) => *value as u32, _ => return Err(incompatible(ty, value)) }; number.to_string().into_bytes() }
        PgType::Int4s | PgType::Int8s | PgType::Texts => array_write(ty, format, value, environment)?,
        PgType::Json | PgType::JsonLd => { let mut output = String::new(); json(value, ty == PgType::JsonLd, &mut output, 0)?; output.into_bytes() }
        PgType::Transit => rtdb_format::transit::encode_json(&to_transit(value)?)?,
        PgType::Jsonb => return Err(unsupported(ty, "writer")),
        PgType::Default => return Err(unsupported(ty, "writer")),
    };
    Ok(bytes)
}

/// Encodes the physical timestamp cell using the result field's PostgreSQL timestamp type.
pub fn encode_timestamp(format: Format, value: i64, unit: TimeUnit, timezone: Option<&str>, zoned: bool, environment: &SessionEnvironment) -> Result<Vec<u8>> {
    if format == Format::Binary {
        let nanos = nanos(value, unit);
        let micros = if zoned { nanos.div_euclid(1000) } else { nanos / 1000 };
        Ok(i64::try_from(micros).map_err(invalid)?.wrapping_sub(PG_EPOCH_MICROS).to_be_bytes().to_vec())
    } else {
        let format = if environment.parameter("datestyle") == Some("iso8601") { TimestampText::Iso } else { TimestampText::Pg };
        Ok(timestamp_text(value, unit, timezone.filter(|_| zoned), format)?.into_bytes())
    }
}

fn integer_bytes(ty: PgType, format: Format, integer: i128) -> Vec<u8> {
    if format == Format::Text { return integer.to_string().into_bytes(); }
    match ty {
        PgType::Int2 => (integer as i16).to_be_bytes().to_vec(),
        PgType::Int4 => (integer as i32).to_be_bytes().to_vec(),
        _ => (integer as i64).to_be_bytes().to_vec(),
    }
}

fn array_write(ty: PgType, format: Format, value: &Value, env: &SessionEnvironment) -> Result<Vec<u8>> {
    let values = match value { Value::List(_) | Value::FixedSizeList(_) => value.list_view().unwrap(), _ => return Err(incompatible(ty, value)) };
    array_payload(ty, format, values.iter(), |element, value| {
        match value.as_ref() {
            Value::Null | Value::Undefined => Ok(None),
            value => encode_value(element, format, value, env).map(Some),
        }
    })
}

fn array_payload<T>(
    ty: PgType, format: Format, cells: impl ExactSizeIterator<Item = T>,
    mut encode: impl FnMut(PgType, T) -> Result<Option<Vec<u8>>>,
) -> Result<Vec<u8>> {
    let element = match ty { PgType::Int4s => PgType::Int4, PgType::Int8s => PgType::Int8, _ => PgType::Text };
    if format == Format::Binary {
        let mut output = Vec::new();
        for field in [1, 0, element.descriptor().oid, i32::try_from(cells.len()).map_err(invalid)?, 1] {
            output.extend_from_slice(&field.to_be_bytes());
        }
        for cell in cells {
            if let Some(bytes) = encode(element, cell)? {
                output.extend_from_slice(&i32::try_from(bytes.len()).map_err(invalid)?.to_be_bytes());
                output.extend_from_slice(&bytes);
            } else {
                if ty != PgType::Texts { return Err(invalid("Integer binary arrays do not support null elements")); }
                output.extend_from_slice(&(-1i32).to_be_bytes());
            }
        }
        return Ok(output);
    }
    let mut output = String::from("{");
    for (index, cell) in cells.enumerate() {
        if index != 0 { output.push(','); }
        let Some(bytes) = encode(element, cell)? else { output.push_str("NULL"); continue; };
        let text = std::str::from_utf8(&bytes).map_err(invalid)?;
        if ty == PgType::Texts {
            output.push('"');
            for ch in text.chars() {
                if matches!(ch, '\\' | '"') { output.push('\\'); }
                output.push(ch);
            }
            output.push('"');
        } else {
            output.push_str(text);
        }
    }
    output.push('}');
    Ok(output.into_bytes())
}

fn interval_parts(value: &Value) -> Result<(i32, i32, i64)> {
    rtdb_format::temporal_text::interval_parts(value).ok_or_else(|| incompatible(PgType::Interval, value))
}
fn decimal_text(value: &rtdb_types::DecimalValue) -> String { rtdb_expr::decimal_text(value) }
fn float_text(value: f64, single: Option<f32>) -> String {
    single.map_or_else(|| rtdb_types::java_f64_text(value), rtdb_types::java_f32_text)
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
        Value::Struct(fields) => {
            output.push('{');
            for (index, (key, value)) in fields.iter().filter(|(_, value)| !is_absent(value)).enumerate() { if index != 0 { output.push(','); } json_string(key, output)?; output.push(':'); json(value, ld, output, depth + 1)?; }
            output.push('}');
        },
        Value::Map { entries, .. } => { output.push('{'); for (index, (key, value)) in entries.iter().enumerate() { if index != 0 { output.push(','); } json_string(&display_key(key)?, output)?; output.push(':'); json(value, ld, output, depth + 1)?; } output.push('}'); },
        Value::List(_) | Value::FixedSizeList(_) | Value::Set(_) => { let values = value.list_view().unwrap(); let set = ld && matches!(value, Value::Set(_)); if set { output.push_str("{\"@type\":\"xt:set\",\"@value\":"); } output.push('['); for (index, value) in values.iter().enumerate() { if index != 0 { output.push(','); } json(value.as_ref(), ld, output, depth + 1)?; } output.push(']'); if set { output.push('}'); } },
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
        Value::DateDay(days) => ("xt:date", date_text(i64::from(*days))?), Value::DateMilli(value) => ("xt:date", date_text(value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY))?),
        Value::Time { value, unit, .. } => ("xt:time", time_text(nanos(*value, *unit), false)?),
        Value::Timestamp { value, unit, timezone } => (if timezone.is_some() { "xt:timestamptz" } else { "xt:timestamp" }, timestamp_text(*value, *unit, timezone.as_deref(), TimestampText::Iso)?),
        Value::Duration { value, unit } => ("xt:duration", duration_text(nanos(*value, *unit))),
        value @ (Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayMicro { .. } | Value::IntervalMonthDayNano { .. }) => { let (months, days, nanos) = interval_parts(value)?; ("xt:interval", interval_text(months, days, nanos)) },
        _ => return Err(Anomaly::incorrect("xtdb/json-encode-error", "Value has no pinned JSON representation")),
    })
}
fn display_key(value: &Value) -> Result<String> { match value { Value::Utf8(value) => Ok(value.clone()), Value::Keyword(value) => Ok(value.as_str().into()), Value::Null => Ok("null".into()), Value::Bool(value) => Ok(value.to_string()), value => rtdb_expr::integer(value).map(|value| value.to_string()).map(Ok).unwrap_or_else(|| scalar_text(value).map(|(_, text)| text)) } }

/// Converts an admitted cell to Transit using the caller's allocation reservation.
pub fn to_transit(value: &Value) -> Result<TransitValue> {
    if value.has_calendar_object() { return Err(invalid("PostgreSQL output requires admitted calendar values")); }
    rtdb_format::transit::value_to_transit(&without_absent_fields(value)).map_err(translate_output_error)
}

/// The reference reads a struct row as a map of its non-null fields, so a null field is absent from rendered output.
fn is_absent(value: &Value) -> bool { matches!(value, Value::Null | Value::Undefined) }

fn without_absent_fields(value: &Value) -> std::borrow::Cow<'_, Value> {
    use std::borrow::Cow;
    match value {
        Value::Struct(fields) => Cow::Owned(Value::Struct(fields.iter().filter(|(_, value)| !is_absent(value)).map(|(key, value)| (key.clone(), without_absent_fields(value).into_owned())).collect())),
        Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) if contains_struct(value) => {
            let values: Vec<Value> = value.list_view().unwrap().iter().map(|value| without_absent_fields(value.as_ref()).into_owned()).collect();
            Cow::Owned(match value { Value::List(_) => Value::list(values), Value::Set(_) => Value::Set(values), _ => Value::FixedSizeList(values.into()) })
        }
        Value::Map { entries, sorted } if entries.iter().any(|(_, value)| contains_struct(value)) =>
            Cow::Owned(Value::Map { entries: entries.iter().map(|(key, value)| (key.clone(), without_absent_fields(value).into_owned())).collect(), sorted: *sorted }),
        value => Cow::Borrowed(value),
    }
}

fn contains_struct(value: &Value) -> bool {
    match value {
        Value::Struct(_) => true,
        Value::Set(values) => values.iter().any(contains_struct),
        Value::List(values) | Value::FixedSizeList(values) => matches!(values.view(), rtdb_types::ListView::Values(values) if values.iter().any(contains_struct)),
        Value::Map { entries, .. } => entries.iter().any(|(_, value)| contains_struct(value)),
        _ => false,
    }
}

fn time_text(value: i128, force_seconds: bool) -> Result<String> {
    rtdb_format::temporal_text::time_text(value, force_seconds).map_err(translate_output_error)
}

fn timestamp_text(value: i64, unit: TimeUnit, timezone: Option<&str>, format: TimestampText) -> Result<String> {
    rtdb_format::temporal_text::timestamp_text(value, unit, timezone, format).map_err(translate_output_error)
}

fn translate_output_error(error: Anomaly) -> Anomaly {
    match error.code.as_str() {
        "rtdb.format/temporal-text" => invalid(error.message.as_deref().unwrap_or(&error.code)),
        "rtdb.format/transit-value-depth" => size_error(),
        _ => error,
    }
}

pub fn encode_error(error: &Anomaly, format: PgType) -> Result<Vec<u8>> {
    use rtdb_format::{diagnostics, transit};
    match format {
        PgType::JsonLd => serde_json::to_vec(&diagnostics::json_ld(error).map_err(invalid)?).map_err(invalid),
        PgType::Transit => transit::encode_json(&transit::decode_msgpack(&transit::encode_anomaly(error)?)?),
        _ => serde_json::to_vec(&diagnostics::JsonAnomaly::from(error)).map_err(invalid),
    }
}
