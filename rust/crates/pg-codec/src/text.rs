use rtdb_types::StructFields;
use std::{borrow::Cow, collections::BTreeMap};
use rtdb_types::{Anomaly, DecimalValue, IntervalUnit, PgType, Result, ScalarType, TimeUnit, Value};
use crate::interval::parse as interval;

mod column;
pub use column::{read_source_text_column, read_text_column, read_vector_column};
mod transit_input;
pub use transit_input::{decode_transit_msgpack, decode_transit_sequence, decode_transit_transaction};

/// The caller covers decoding allocations and the returned value with its own reservation.
/// Unknown OIDs remain UTF-8 strings; known types without a text reader return an unsupported anomaly.
pub fn read_text(oid: u32, text: &str) -> Result<Value> {
    if rtdb_types::PgType::from_oid(oid as i32).is_none() { return Ok(Value::Utf8(text.to_owned())); }
    Ok(match oid {
        16 => Value::Bool(parse_boolean(text)?),
        21 => Value::I16(text.parse().map_err(invalid)?), 23 => Value::I32(text.parse().map_err(invalid)?), 20 => Value::I64(text.parse().map_err(invalid)?),
        700 => Value::F32(float32(text)?), 701 => Value::F64(float(text)?),
        1700 => decimal(text)?,
        17 => Value::Binary(bytea(text)?.into_owned()),
        2950 => Value::Uuid(parse_uuid(text)?),
        1082 => Value::calendar_date(rtdb_types::CivilDate::parse_iso(text)?, rtdb_types::DateUnit::Day),
        1083 => Value::Time { value: rtdb_types::parse_iso_local_time(text)? as i64, unit: TimeUnit::Nanosecond, bit_width: 64 },
        1114 => Value::Timestamp { value: local_timestamp_micros(text)?, unit: TimeUnit::Microsecond, timezone: None },
        1184 => timestamp(text)?,
        1186 => {
            let (months, days, nanoseconds) = interval(text)?;
            match interval_type(days, nanoseconds) {
                ScalarType::Interval(IntervalUnit::YearMonth) => Value::IntervalYearMonth(months),
                ScalarType::IntervalMonthDayMicro => Value::IntervalMonthDayMicro { months, days, nanoseconds },
                _ => Value::IntervalMonthDayNano { months, days, nanoseconds },
            }
        },
        11112 => Value::Duration { value: crate::interval::duration_micros(text)?, unit: TimeUnit::Microsecond },
        1007 | 1016 | 1009 => { let element = match oid { 1007 => 23, 1016 => 20, _ => 25 }; let mut list = rtdb_types::ListBuilder::with_capacity(0); visit_array(text, |value| { list.push(value.map_or(Ok(Value::Null), |text| read_text(element, &text))?); Ok(()) })?; Value::List(list.finish()) },
        114 | 3802 => read_json(text)?,
        3910 => {
            let Some(inner) = text.strip_prefix('[').and_then(|text| text.strip_suffix(')')) else { return Ok(Value::Null); };
            let Some((lower, upper)) = inner.split_once(',') else { return Ok(Value::Null); };
            let bound = |text: &str, absent| -> Result<i64> { if text.is_empty() { Ok(absent) } else { let value = timestamp(text)?; Ok(rtdb_expr::timestamp_micros(&value, "UTC")?.0) } };
            Value::TimestampTzRange { lower: bound(lower, i64::MIN)?, upper: bound(upper, i64::MAX)? }
        },
        26 | 2205 | 24 => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-text-type", format!("Pinned PgType has no text reader for OID {oid}"))),
        16384 => transit_input::decode_json(text.as_bytes())?,
        _ => Value::Utf8(text.to_owned()),
    })
}
fn parse_boolean(text: &str) -> Result<bool> { Ok(match text.trim().to_ascii_lowercase().as_str() { "tr" | "yes" | "tru" | "true" | "on" | "ye" | "t" | "y" | "1" => true, "f" | "fa" | "fal" | "fals" | "false" | "no" | "n" | "off" | "0" => false, _ => return Err(invalid("Invalid PostgreSQL boolean")) }) }
pub(crate) fn float32(text: &str) -> Result<f32> { Ok(match text { "Infinity" | "+Infinity" => f32::INFINITY, "-Infinity" => f32::NEG_INFINITY, "NaN" => f32::NAN, _ => text.parse().map_err(invalid)? }) }
pub(crate) fn float(text: &str) -> Result<f64> { match text { "Infinity" | "+Infinity" => Ok(f64::INFINITY), "-Infinity" => Ok(f64::NEG_INFINITY), "NaN" => Ok(f64::NAN), _ => text.parse().map_err(invalid) } }
fn decimal(text: &str) -> Result<Value> { crate::numeric::parse(text)?.into_value() }

fn parse_decimal(text: &str) -> Result<DecimalValue> {
    crate::numeric::parse(text)?.into_decimal()
}

fn bytea(text: &str) -> Result<Cow<'_, [u8]>> {
    let Some(hex) = text.strip_prefix("\\x") else { return Ok(Cow::Borrowed(text.as_bytes())); };
    if hex.len() % 2 != 0 { return Err(invalid("Odd-length bytea hexadecimal value")); }
    let bytes = hex.as_bytes().chunks_exact(2).map(|pair| {
        let text = std::str::from_utf8(pair).map_err(invalid)?;
        u8::from_str_radix(text, 16).map_err(invalid)
    }).collect::<Result<_>>()?;
    Ok(Cow::Owned(bytes))
}

fn local_timestamp_micros(text: &str) -> Result<i64> {
    let (value, timezone) = rtdb_types::parse_sql_timestamp_micros(text)?;
    if timezone.is_some() { return Err(invalid("Invalid PostgreSQL local timestamp")); }
    Ok(value)
}

fn zoned_timestamp_micros(text: &str) -> Result<(i64, String)> {
    let (value, zone) = rtdb_types::parse_sql_timestamp_micros(text)?;
    let zone = zone.ok_or_else(|| invalid("Zoned timestamp requires an offset or region"))?;
    Ok((value, rtdb_expr::normalize_timezone(&zone)?))
}

fn timestamp(text: &str) -> Result<Value> {
    let (value, zone) = zoned_timestamp_micros(text)?;
    Ok(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(zone) })
}

fn interval_type(days: i32, nanoseconds: i64) -> ScalarType {
    if nanoseconds % 1000 != 0 { ScalarType::Interval(IntervalUnit::MonthDayNano) }
    else if nanoseconds != 0 || days != 0 { ScalarType::IntervalMonthDayMicro }
    else { ScalarType::Interval(IntervalUnit::YearMonth) }
}

fn visit_array<'a>(text: &'a str, visit: impl FnMut(Option<std::borrow::Cow<'a, str>>) -> Result<()>) -> Result<()> {
    visit_array_elements(text, false, visit)
}

pub(crate) fn read_array_argument(ty: PgType, text: &str) -> Result<arrow_array::ArrayRef> {
    use arrow_array::types::{Int32Type, Int64Type};

    match ty {
        PgType::Int4s => read_integer_array_argument::<Int32Type>(text),
        PgType::Int8s => read_integer_array_argument::<Int64Type>(text),
        PgType::Texts => Ok(read_text_column(1009, 1, |_| Ok(Some(text)))?.expect("text array decoder")),
        _ => unreachable!(),
    }
}

fn read_integer_array_argument<T: arrow_array::ArrowPrimitiveType>(text: &str) -> Result<arrow_array::ArrayRef>
where
    T::Native: std::str::FromStr,
    <T::Native as std::str::FromStr>::Err: std::fmt::Display,
{
    use arrow_array::PrimitiveArray;
    use arrow_buffer::NullBufferBuilder;
    use std::sync::Arc;

    let mut values = Vec::new();
    let mut validity = NullBufferBuilder::new(0);
    let mut first = None;
    visit_array(text, |item| {
        let value = item.map(|item| item.parse::<T::Native>().map_err(invalid)).transpose()?;
        if first.is_none() { first = value; }
        values.push(value.unwrap_or_default());
        validity.append(value.is_some());
        Ok(())
    })?;
    let validity = validity.finish();
    if let (Some(first), Some(validity)) = (first, &validity) {
        // The legacy dense-union collapse gathered index zero for masked slots, whose bytes are included in SQL argument IPC.
        for index in 0..values.len() {
            if validity.is_null(index) { values[index] = first; }
        }
    }
    crate::array_argument(Arc::new(PrimitiveArray::<T>::new(values.into(), validity)))
}

pub(crate) fn visit_array_elements<'a>(text: &'a str, strict_quotes: bool, mut visit: impl FnMut(Option<std::borrow::Cow<'a, str>>) -> Result<()>) -> Result<()> {
    use std::borrow::Cow;

    let text = text.trim();
    if text.is_empty() || text == "{}" { return Ok(()); }
    let inner = text.strip_prefix('{').and_then(|text| text.strip_suffix('}'))
        .ok_or_else(|| invalid("PostgreSQL array must be one-dimensional"))?;
    let mut start = 0;
    let mut copied = 0;
    let mut decoded: Option<String> = None;
    let mut quoted = false;
    let mut escaped = false;
    let mut after_quote = false;
    let finish = |from: usize, end: usize, copied: usize, decoded: Option<String>| {
        if let Some(mut decoded) = decoded {
            decoded.push_str(&inner[copied..end]);
            return Some(Cow::Owned(decoded));
        }
        let raw = inner[from..end].trim();
        (!raw.eq_ignore_ascii_case("NULL")).then_some(Cow::Borrowed(raw))
    };
    for (index, character) in inner.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match character {
            _ if strict_quotes && after_quote && character != ',' && !character.is_whitespace() =>
                return Err(invalid("Invalid PostgreSQL array element after closing quote")),
            '"' if strict_quotes && !quoted && (decoded.is_some() || !inner[start..index].trim().is_empty()) =>
                return Err(invalid("Invalid quote in PostgreSQL array element")),
            '\\' | '"' => {
                let buffer = decoded.get_or_insert_with(String::new);
                buffer.push_str(&inner[copied..index]);
                copied = index + 1;
                if character == '"' { quoted = !quoted; after_quote = !quoted; } else { escaped = true; }
            }
            ',' if !quoted => {
                visit(finish(start, index, copied, decoded.take()))?;
                start = index + 1;
                copied = start;
                after_quote = false;
            }
            '{' | '}' if !quoted => return Err(invalid("Nested PostgreSQL arrays are unsupported by the pinned type reader")),
            _ => {}
        }
    }
    if quoted || escaped { return Err(invalid("Unterminated PostgreSQL array element")); }
    visit(finish(start, inner.len(), copied, decoded))
}
fn read_json(text: &str) -> Result<Value> {
    use serde_json::value::RawValue;
    let text = text.trim();
    Ok(match text.as_bytes().first() {
        Some(b'n') if text == "null" => Value::Null,
        Some(b't') if text == "true" => Value::Bool(true),
        Some(b'f') if text == "false" => Value::Bool(false),
        Some(b'"') => Value::Utf8(serde_json::from_str(text).map_err(invalid)?),
        Some(b'[') => {
            let values: Vec<&RawValue> = serde_json::from_str(text).map_err(invalid)?;
            if let Some(list) = json_float_list(&values)? { Value::List(list) }
            else { Value::list(values.into_iter().map(|value| read_json(value.get())).collect::<Result<_>>()?) }
        },
        Some(b'{') => {
            let mut fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(text).map_err(invalid)?;
            let kind = fields.get("@type").and_then(|kind| serde_json::from_str::<String>(kind.get()).ok());
            if let Some(kind) = kind {
                let value = fields.remove("@value").ok_or_else(|| invalid("JSON-LD value requires @value"))?;
                let string = || serde_json::from_str::<String>(value.get()).map_err(invalid);
                match kind.as_str() {
                    "xt:long" => Value::I64(if let Ok(integer) = value.get().parse::<i64>() { integer } else { string()?.parse().map_err(invalid)? }),
                    "xt:double" => Value::F64(if let Ok(text) = string() { float(&text)? } else { float(value.get())? }),
                    "xt:decimal" => decimal(&string()?)?,
                    "xt:instant" => { let mut timestamp = timestamp(&string()?)?; if let Value::Timestamp { timezone, .. } = &mut timestamp { *timezone = Some("UTC".into()); } timestamp },
                    "xt:timestamptz" => zoned_timestamp(&string()?)?,
                    "xt:timestamp" => read_text(1114, &string()?)?, "xt:date" => read_text(1082, &string()?)?, "xt:time" => read_text(1083, &string()?)?,
                    "xt:duration" => read_text(11112, &string()?)?, "xt:interval" => read_text(1186, &string()?)?,
                    "xt:keyword" => Value::Keyword(rtdb_types::Keyword::from_name(string()?)), "xt:uuid" => read_text(2950, &string()?)?,
                    "xt:set" => { let Value::List(values) = read_json(value.get())? else { return Err(invalid("JSON-LD set requires an array")); }; Value::Set(unique(values.into_values())) },
                    "xt:incorrect" | "xt:unsupported" | "xt:conflict" | "xt:fault" | "xt:interrupted" | "xt:not-found" | "xt:forbidden" | "xt:busy" | "xt:unavailable" | "xt:error" => json_error(&kind, value.get())?,
                    _ => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-json-ld-value", format!("JSON-LD {kind} cannot be imported as a native document value"))),
                }
            } else { Value::Struct(fields.into_iter().map(|(key, value)| Ok((key, read_json(value.get())?))).collect::<Result<_>>()?) }
        }
        _ => {
            let _: Box<RawValue> = serde_json::from_str(text).map_err(invalid)?;
            if let Ok(integer) = text.parse::<i64>() { Value::I64(integer) }
            else if !text.contains(['.', 'e', 'E']) && text.trim_start_matches('-').len() <= 64 { decimal(text)? }
            else { Value::F64(float(text)?) }
        }
    })
}
fn json_float_list(values: &[&serde_json::value::RawValue]) -> Result<Option<rtdb_types::ListValue>> {
    let is_float = |text: &str| {
        matches!(text.as_bytes().first(), Some(b'-' | b'0'..=b'9'))
            && (text.contains(['.', 'e', 'E']) || text.trim_start_matches('-').len() > 64)
    };
    if !values.iter().all(|value| value.get() == "null" || is_float(value.get()))
        || !values.iter().any(|value| value.get() != "null")
    {
        return Ok(None);
    }
    let mut builder = arrow_array::builder::Float64Builder::with_capacity(values.len());
    for value in values {
        let text = value.get();
        builder.append_option((text != "null").then(|| float(text)).transpose()?);
    }
    Ok(Some(rtdb_types::ListValue::from_f64(builder.finish())))
}

fn zoned_timestamp(text: &str) -> Result<Value> {
    timestamp(text)
}
fn unique(values: Vec<Value>) -> Vec<Value> { let mut result = Vec::new(); for value in values { if !result.contains(&value) { result.push(value); } } result }
/// The caller retains a reservation covering both the consumed Transit graph and its native conversion.
pub fn from_transit(value: rtdb_format::transit::TransitValue) -> Result<Value> { transit(value) }
fn transit(value: rtdb_format::transit::TransitValue) -> Result<Value> {
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};
    Ok(match value {
        T::Null => Value::Null, T::Bool(value) => Value::Bool(value), T::Integer(value) => Value::I64(value), T::Float(value) => Value::F64(value), T::String(value) => Value::Utf8(value), T::Keyword(value) => Value::Keyword(rtdb_types::Keyword::from_name(&value)), T::Bytes(value) => Value::Binary(value), T::Array(values) => {
            let mut list = rtdb_types::ListBuilder::with_capacity(values.len());
            for value in values { list.push(transit(value)?); }
            Value::List(list.finish())
        },
        T::Float32(value) => Value::F64(f64::from(value)),
        T::Map(entries) => {
            let mut fields = StructFields::new();
            for (key, value) in entries { let key = match key { T::String(key) => key, T::Keyword(key) => normalize_keyword(&key), _ => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-map-key", "Source maps require string or keyword keys")) }; fields.insert(key, transit(value)?); }
            Value::Struct(fields)
        }
        T::Tagged { tag, representation } => {
            let string = || match representation.as_ref() { T::String(text) => Ok(text.as_str()), _ => Err(invalid("Transit tag requires text")) };
            match tag.as_str() {
                "u" => match representation.as_ref() {
                    T::String(text) => Value::Uuid(parse_uuid(text)?),
                    T::Array(parts) => {
                        let [T::Integer(high), T::Integer(low)] = parts.as_slice() else {
                            return Err(invalid("Transit UUID requires two signed 64-bit words"));
                        };
                        Value::Uuid(uuid_words(*high, *low))
                    }
                    _ => return Err(invalid("Transit UUID requires text or two signed 64-bit words")),
                },
                "f" => decimal(string()?)?,
                "r" => { let text = string()?; rtdb_types::UriParts::parse(text)?; Value::Uri(text.to_owned()) },
                "m" => {
                    let millis = match representation.as_ref() {
                        T::Integer(millis) => *millis,
                        T::String(millis) => millis.parse::<i64>().map_err(invalid)?,
                        _ => return Err(invalid("Transit date requires epoch milliseconds")),
                    };
                    let micros = millis.checked_mul(1000).ok_or_else(|| invalid("Transit date overflow"))?;
                    Value::Timestamp { value: micros, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }
                }
                "time/instant" => { let mut value = timestamp(string()?)?; if let Value::Timestamp { timezone, .. } = &mut value { *timezone = Some("UTC".into()); } value },
                "time/zoned-date-time" => zoned_timestamp(string()?)?, "time/offset-date-time" => timestamp(string()?)?,
                "time/date-time" => read_text(1114, string()?)?, "time/date" => read_text(1082, string()?)?, "time/time" => read_text(1083, string()?)?, "time/duration" => read_text(11112, string()?)?,
                "xtdb/interval" => if crate::interval::matches_interval(string()?) { read_text(1186, string()?)? } else { Value::Null },
                "xt/oid" | "xt/regclass" | "xt/regproc" => {
                    let T::Integer(oid) = *representation else { return Err(invalid("PostgreSQL reference requires an integer OID")); };
                    let oid = i32::try_from(oid).map_err(invalid)?;
                    match tag.as_str() {
                        "xt/oid" => Value::Oid(oid),
                        "xt/regclass" => Value::RegClass(oid),
                        _ => Value::RegProc(oid),
                    }
                }
                "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => cast_transit_number(&tag, transit(*representation)?)?,
                "set" => { let T::Array(values) = *representation else { return Err(invalid("Transit set requires an array")); }; Value::Set(unique(values.into_iter().map(transit).collect::<Result<_>>()?)) },
                "list" => {
                    let T::Array(values) = *representation else { return Err(invalid("Transit list requires an array")); };
                    Value::list(values.into_iter().map(transit).collect::<Result<_>>()?)
                }
                "xtdb/byte-array" => read_text(17, &format!("\\x{}", string()?.get(2..).ok_or_else(|| invalid("Transit byte-array requires a hexadecimal prefix"))?))?,
                "xtdb/tstz-range" => {
                    let T::Array(bounds) = *representation else { return Err(invalid("Transit timestamp range requires an array")); };
                    if bounds.len() != 2 { return Err(invalid("Transit timestamp range requires two bounds")); }
                    let mut bounds = bounds.into_iter();
                    let lower = match bounds.next().ok_or_else(|| invalid("Missing range bound"))? { T::Null => i64::MIN, value => rtdb_expr::timestamp_micros(&transit(value)?, "UTC")?.0 };
                    let upper = match bounds.next().ok_or_else(|| invalid("Missing range bound"))? { T::Null => i64::MAX, value => rtdb_expr::timestamp_micros(&transit(value)?, "UTC")?.0 };
                    Value::TimestampTzRange { lower, upper }
                }
                "xtdb/period-duration" => {
                    let T::Array(parts) = *representation else { return Err(invalid("Transit period-duration requires an array")); };
                    let [T::String(period), T::String(duration), ..] = parts.as_slice() else { return Err(invalid("Transit period-duration requires period and duration strings")); };
                    crate::interval::period_duration(period, duration)?
                }
                "xtdb/clj-form" | "xt/error" | "xtdb/illegal-arg" | "xtdb/runtime-err" | "xtdb/exception-info" => Value::Transit(encode_msgpack(&T::Tagged { tag, representation })?),
                _ => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-transit-value", format!("Transit tag {tag} cannot be imported as a native document value"))),
            }
        }
        T::Symbol(_) => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-symbol", "Source symbols have no Arrow document value type")),
    })
}
fn parse_uuid(text: &str) -> Result<[u8; 16]> {
    Ok(*uuid::Uuid::parse_str(text).map_err(invalid)?.as_bytes())
}

fn uuid_words(high: i64, low: i64) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    bytes
}

fn cast_transit_number(tag: &str, value: Value) -> Result<Value> {
    use rtdb_types::ScalarType;
    let scalar = match tag {
        "i8" => ScalarType::I8,
        "i16" => ScalarType::I16,
        "i32" => ScalarType::I32,
        "i64" => ScalarType::I64,
        "f32" => ScalarType::F32,
        _ => ScalarType::F64,
    };
    rtdb_expr::cast(&value, &rtdb_types::LogicalType::Scalar(scalar), "UTC")
}
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.postgres/invalid-column-value", error.to_string()) }

fn normalize_keyword(name: &str) -> String {
    let name = name.strip_prefix("xt/").map_or_else(|| name.to_owned(), |name| format!("_{name}"));
    name.replace('-', "_").replace(['.', '/', '$'], "$").to_lowercase()
}

fn json_error(kind: &str, text: &str) -> Result<Value> {
    use rtdb_types::AnomalyCategory as C;
    let object: BTreeMap<String, serde_json::Value> = serde_json::from_str(text).map_err(invalid)?;
    let category = match kind { "xt:incorrect" => C::Incorrect, "xt:unsupported" => C::Unsupported, "xt:conflict" => C::Conflict, "xt:interrupted" => C::Interrupted, "xt:not-found" => C::NotFound, "xt:forbidden" => C::Forbidden, "xt:busy" => C::Busy, "xt:unavailable" => C::Unavailable, _ => C::Fault };
    let message = object.get("xtdb.error/message").and_then(serde_json::Value::as_str).map(str::to_owned);
    let data = object.get("xtdb.error/data").and_then(serde_json::Value::as_object);
    let code = data.and_then(|data| data.get("xtdb.error/code")).and_then(serde_json::Value::as_str).unwrap_or("xtdb/error");
    let mut error = Anomaly::new_nullable(category, code, message);
    if let Some(data) = data { error.context = data.clone().into_iter().collect(); }
    Ok(Value::Transit(rtdb_format::transit::encode_anomaly(&error)?))
}
