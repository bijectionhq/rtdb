use rtdb_types::StructFields;
use std::collections::BTreeMap;
use rtdb_types::{Anomaly, DecimalValue, Result, TimeUnit, Value};
use crate::interval::parse as interval;

mod column;
pub use column::read_text_column;

/// The caller covers decoding allocations and the returned value with its own reservation.
/// Unknown OIDs remain UTF-8 strings; known types without a text reader return an unsupported anomaly.
pub fn read_text(oid: u32, text: &str) -> Result<Value> {
    if rtdb_types::PgType::from_oid(oid as i32).is_none() { return Ok(Value::Utf8(text.to_owned())); }
    Ok(match oid {
        16 => Value::Bool(parse_boolean(text)?),
        21 => Value::I16(text.parse().map_err(invalid)?), 23 => Value::I32(text.parse().map_err(invalid)?), 20 => Value::I64(text.parse().map_err(invalid)?),
        700 => Value::F32(float32(text)?), 701 => Value::F64(float(text)?),
        1700 => decimal(text)?,
        17 => Value::Binary(if let Some(hex) = text.strip_prefix("\\x") { if hex.len() % 2 != 0 { return Err(invalid("Odd-length bytea hexadecimal value")); } hex.as_bytes().chunks_exact(2).map(|pair| { let text = std::str::from_utf8(pair).map_err(invalid)?; u8::from_str_radix(text, 16).map_err(invalid) }).collect::<Result<_>>()? } else { text.as_bytes().to_vec() }),
        2950 => Value::Uuid(*uuid::Uuid::parse_str(text).map_err(invalid)?.as_bytes()),
        1082 => Value::calendar_date(rtdb_types::CivilDate::parse_iso(text)?, rtdb_types::DateUnit::Day),
        1083 => Value::Time { value: rtdb_types::parse_iso_local_time(text)? as i64, unit: TimeUnit::Nanosecond, bit_width: 64 },
        1114 => { let value = rtdb_types::parse_sql_timestamp_literal(text)?; if !matches!(&value, Value::Timestamp { timezone: None, .. }) { return Err(invalid("Invalid PostgreSQL local timestamp")); } value },
        1184 => timestamp(text)?,
        1186 => { let (months, days, nanoseconds) = interval(text)?; if nanoseconds % 1000 != 0 { Value::IntervalMonthDayNano { months, days, nanoseconds } } else if nanoseconds != 0 || days != 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds } } else { Value::IntervalYearMonth(months) } },
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
        16384 => transit(rtdb_format::transit::decode_json(text.as_bytes())?)?,
        _ => Value::Utf8(text.to_owned()),
    })
}
fn parse_boolean(text: &str) -> Result<bool> { Ok(match text.trim().to_ascii_lowercase().as_str() { "tr" | "yes" | "tru" | "true" | "on" | "ye" | "t" | "y" | "1" => true, "f" | "fa" | "fal" | "fals" | "false" | "no" | "n" | "off" | "0" => false, _ => return Err(invalid("Invalid PostgreSQL boolean")) }) }
fn float32(text: &str) -> Result<f32> { Ok(match text { "Infinity" | "+Infinity" => f32::INFINITY, "-Infinity" => f32::NEG_INFINITY, "NaN" => f32::NAN, _ => text.parse().map_err(invalid)? }) }
fn float(text: &str) -> Result<f64> { match text { "Infinity" | "+Infinity" => Ok(f64::INFINITY), "-Infinity" => Ok(f64::NEG_INFINITY), "NaN" => Ok(f64::NAN), _ => text.parse().map_err(invalid) } }
pub(crate) fn decimal(text: &str) -> Result<Value> {
    let decimal = rtdb_expr::parse_decimal(text).map_err(invalid)?;
    let (coefficient, scale) = decimal.as_bigint_and_exponent();
    decimal_parts(coefficient, i32::try_from(scale).map_err(invalid)?)
}
pub(crate) fn decimal_parts(coefficient: num_bigint::BigInt, scale: i32) -> Result<Value> {
    let digits = coefficient.to_string().trim_start_matches('-').len().max(1);
    let precision = if digits <= 32 { 32 } else if digits <= 64 { 64 } else { return Err(invalid("Decimal exceeds the pinned value writer precision")); };
    let width = if precision == 32 { 128 } else { 256 };
    let bytes = coefficient.to_signed_bytes_be(); let length = usize::from(width / 8);
    if bytes.len() > length { return Err(invalid("Decimal coefficient exceeds its Arrow width")); }
    let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 0xff } else { 0 }; length - bytes.len()]; unscaled.extend_from_slice(&bytes);
    Ok(Value::Decimal(DecimalValue::new(unscaled, precision, scale, width)?))
}
fn timestamp(text: &str) -> Result<Value> {
    let Value::Timestamp { value, unit, timezone: Some(zone) } = rtdb_types::parse_sql_timestamp_literal(text)? else { return Err(invalid("Zoned timestamp requires an offset or region")); };
    Ok(Value::Timestamp { value, unit, timezone: Some(rtdb_expr::normalize_timezone(&zone)?) })
}
fn visit_array<'a>(text: &'a str, mut visit: impl FnMut(Option<std::borrow::Cow<'a, str>>) -> Result<()>) -> Result<()> {
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
            '\\' | '"' => {
                let buffer = decoded.get_or_insert_with(String::new);
                buffer.push_str(&inner[copied..index]);
                copied = index + 1;
                if character == '"' { quoted = !quoted; } else { escaped = true; }
            }
            ',' if !quoted => {
                visit(finish(start, index, copied, decoded.take()))?;
                start = index + 1;
                copied = start;
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
        Some(b'[') => { let values: Vec<Box<RawValue>> = serde_json::from_str(text).map_err(invalid)?; Value::list(values.into_iter().map(|value| read_json(value.get())).collect::<Result<_>>()?) },
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
                    T::String(text) => read_text(2950, text)?,
                    T::Array(parts) => {
                        let [T::Integer(high), T::Integer(low)] = parts.as_slice() else {
                            return Err(invalid("Transit UUID requires two signed 64-bit words"));
                        };
                        let mut bytes = [0; 16];
                        bytes[..8].copy_from_slice(&high.to_be_bytes());
                        bytes[8..].copy_from_slice(&low.to_be_bytes());
                        Value::Uuid(bytes)
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
                "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
                    let value = transit(*representation)?;
                    let scalar = match tag.as_str() { "i8" => rtdb_types::ScalarType::I8, "i16" => rtdb_types::ScalarType::I16, "i32" => rtdb_types::ScalarType::I32, "i64" => rtdb_types::ScalarType::I64, "f32" => rtdb_types::ScalarType::F32, _ => rtdb_types::ScalarType::F64 };
                    rtdb_expr::cast(&value, &rtdb_types::LogicalType::Scalar(scalar), "UTC")?
                }
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
