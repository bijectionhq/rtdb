use std::collections::BTreeMap;
use std::str::FromStr;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use bigdecimal::BigDecimal;
use rtdb_types::{Anomaly, DecimalValue, Result, TimeUnit, Value};

/// The caller covers decoding allocations and the returned value with its own reservation.
/// Unknown OIDs remain UTF-8 strings; known types without a text reader return an unsupported anomaly.
pub fn read_text(oid: u32, text: &str) -> Result<Value> {
    if rtdb_types::PgType::from_oid(oid as i32).is_none() { return Ok(Value::Utf8(text.to_owned())); }
    Ok(match oid {
        16 => Value::Bool(match text.trim().to_ascii_lowercase().as_str() { "tr" | "yes" | "tru" | "true" | "on" | "ye" | "t" | "y" | "1" => true, "f" | "fa" | "fal" | "fals" | "false" | "no" | "n" | "off" | "0" => false, _ => return Err(invalid("Invalid PostgreSQL boolean")) }),
        21 => Value::I16(text.parse().map_err(invalid)?), 23 => Value::I32(text.parse().map_err(invalid)?), 20 => Value::I64(text.parse().map_err(invalid)?),
        700 => Value::F32(match text { "Infinity" | "+Infinity" => f32::INFINITY, "-Infinity" => f32::NEG_INFINITY, "NaN" => f32::NAN, _ => text.parse().map_err(invalid)? }), 701 => Value::F64(float(text)?),
        1700 => decimal(text)?,
        17 => Value::Binary(if let Some(hex) = text.strip_prefix("\\x") { if hex.len() % 2 != 0 { return Err(invalid("Odd-length bytea hexadecimal value")); } hex.as_bytes().chunks_exact(2).map(|pair| { let text = std::str::from_utf8(pair).map_err(invalid)?; u8::from_str_radix(text, 16).map_err(invalid) }).collect::<Result<_>>()? } else { text.as_bytes().to_vec() }),
        2950 => Value::Uuid(*uuid::Uuid::parse_str(text).map_err(invalid)?.as_bytes()),
        1082 => { let date = NaiveDate::parse_from_str(text, "%Y-%m-%d").map_err(invalid)?; let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| invalid("Missing epoch"))?; Value::DateDay(i32::try_from(date.signed_duration_since(epoch).num_days()).map_err(invalid)?) },
        1083 => { let time = ["%H:%M:%S%.f", "%H:%M"].into_iter().find_map(|format| NaiveTime::parse_from_str(text, format).ok()).filter(|time| time.nanosecond() < 1_000_000_000).ok_or_else(|| invalid("Invalid PostgreSQL local time"))?; Value::Time { value: i64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(time.nanosecond()), unit: TimeUnit::Nanosecond, bit_width: 64 } },
        1114 => { let time = local_timestamp(text)?; Value::Timestamp { value: time.and_utc().timestamp_micros(), unit: TimeUnit::Microsecond, timezone: None } },
        1184 => timestamp(text)?,
        1186 => { let (months, days, nanoseconds) = interval(text)?; if nanoseconds % 1000 != 0 { Value::IntervalMonthDayNano { months, days, nanoseconds } } else if nanoseconds != 0 || days != 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds } } else { Value::IntervalYearMonth(months) } },
        11112 => { let (months, days, nanos) = interval(text)?; if months != 0 { return Err(invalid("Duration cannot contain years or months")); } Value::Duration { value: i64::from(days).checked_mul(86_400_000_000_000).and_then(|value| value.checked_add(nanos)).ok_or_else(|| invalid("Duration overflow"))?.div_euclid(1000), unit: TimeUnit::Microsecond } },
        1007 | 1016 | 1009 => { let element = match oid { 1007 => 23, 1016 => 20, _ => 25 }; Value::List(array(text)?.into_iter().map(|value| value.map_or(Ok(Value::Null), |text| read_text(element, &text))).collect::<Result<_>>()?) },
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
fn float(text: &str) -> Result<f64> { match text { "Infinity" | "+Infinity" => Ok(f64::INFINITY), "-Infinity" => Ok(f64::NEG_INFINITY), "NaN" => Ok(f64::NAN), _ => text.parse().map_err(invalid) } }
pub(crate) fn decimal(text: &str) -> Result<Value> {
    let decimal = BigDecimal::from_str(text).map_err(invalid)?;
    let (coefficient, scale) = decimal.as_bigint_and_exponent();
    let digits = coefficient.to_string().trim_start_matches('-').len().max(1);
    let precision = if digits <= 32 { 32 } else if digits <= 64 { 64 } else { return Err(invalid("Decimal exceeds the pinned value writer precision")); };
    let scale = i8::try_from(scale).map_err(invalid)?;
    let width = if precision == 32 { 128 } else { 256 };
    let bytes = coefficient.to_signed_bytes_be(); let length = usize::from(width / 8);
    if bytes.len() > length { return Err(invalid("Decimal coefficient exceeds its Arrow width")); }
    let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 0xff } else { 0 }; length - bytes.len()]; unscaled.extend_from_slice(&bytes);
    Ok(Value::Decimal(DecimalValue::new(unscaled, precision, scale, width)?))
}
fn local_timestamp(text: &str) -> Result<NaiveDateTime> {
    ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"].into_iter().find_map(|format| NaiveDateTime::parse_from_str(text, format).ok())
        .or_else(|| NaiveDate::parse_from_str(text, "%Y-%m-%d").ok().and_then(|date| date.and_hms_opt(0, 0, 0)))
        .filter(|time| time.nanosecond() < 1_000_000_000).ok_or_else(|| invalid("Invalid PostgreSQL local timestamp"))
}
fn timestamp(text: &str) -> Result<Value> {
    let (text, region) = if text.ends_with(']') { let (text, region) = text.rsplit_once('[').ok_or_else(|| invalid("Invalid zoned timestamp"))?; (text, Some(&region[..region.len() - 1])) } else { (text, None) };
    let offset_at = text.char_indices().rev().find(|(index, character)| *index >= 10 && matches!(character, '+' | '-' | 'Z')).map(|(index, _)| index);
    let (local, offset) = offset_at.map_or((text, None), |index| (&text[..index], Some(&text[index..])));
    let local = Value::Timestamp { value: local_timestamp(local)?.and_utc().timestamp_micros(), unit: TimeUnit::Microsecond, timezone: None };
    let zone = region.or(offset).ok_or_else(|| invalid("Zoned timestamp requires an offset or region"))?;
    let value = rtdb_expr::timestamp_micros(&local, offset.unwrap_or(zone))?.0;
    if region.is_some() { rtdb_expr::timestamp_micros(&local, zone)?; }
    Ok(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(rtdb_expr::normalize_timezone(zone)?) })
}
fn array(text: &str) -> Result<Vec<Option<String>>> {
    let text = text.trim(); if text.is_empty() || text == "{}" { return Ok(Vec::new()); }
    let inner = text.strip_prefix('{').and_then(|text| text.strip_suffix('}')).ok_or_else(|| invalid("PostgreSQL array must be one-dimensional"))?;
    let mut result = Vec::new(); let mut value = String::new(); let mut quoted = false; let mut escaped = false; let mut special = false;
    let finish = |result: &mut Vec<Option<String>>, value: &mut String, special: bool| { let raw = std::mem::take(value); result.push(if special { Some(raw) } else { let raw = raw.trim(); if raw.eq_ignore_ascii_case("NULL") { None } else { Some(raw.to_owned()) } }); };
    for character in inner.chars() {
        if escaped { value.push(character); escaped = false; continue; }
        match character { '\\' => { escaped = true; special = true; }, '"' => { quoted = !quoted; special = true; }, ',' if !quoted => { finish(&mut result, &mut value, special); special = false; }, '{' | '}' if !quoted => return Err(invalid("Nested PostgreSQL arrays are unsupported by the pinned type reader")), value_char => value.push(value_char) }
    }
    if quoted || escaped { return Err(invalid("Unterminated PostgreSQL array element")); }
    finish(&mut result, &mut value, special); Ok(result)
}
fn interval(text: &str) -> Result<(i32, i32, i64)> {
    let (sign, text) = text.strip_prefix('-').map_or((1i128, text), |text| (-1, text));
    let text = text.strip_prefix('P').ok_or_else(|| invalid("Interval requires an ISO-8601 P prefix"))?;
    let mut months = 0i128; let mut days = 0i128; let mut nanos = 0i128; let mut time = false; let mut start = 0;
    for (index, character) in text.char_indices() {
        if character == 'T' { if index != start || time { return Err(invalid("Invalid ISO interval time separator")); } time = true; start = index + 1; continue; }
        if !character.is_ascii_alphabetic() { continue; }
        let number = &text[start..index]; start = index + 1;
        if character == 'S' && time {
            let value = BigDecimal::from_str(number).map_err(invalid)? * BigDecimal::from(1_000_000_000i64);
            let (coefficient, scale) = value.as_bigint_and_exponent();
            if scale.unsigned_abs() > 128 { return Err(invalid("Interval seconds exceed supported precision")); }
            let coefficient = if scale > 0 { coefficient / num_bigint::BigInt::from(10).pow(u32::try_from(scale).map_err(invalid)?) } else { coefficient * num_bigint::BigInt::from(10).pow(u32::try_from(-scale).map_err(invalid)?) };
            nanos = nanos.checked_add(coefficient.to_string().parse::<i128>().map_err(invalid)?).ok_or_else(|| invalid("Interval overflow"))?;
            continue;
        }
        let number = number.parse::<i128>().map_err(invalid)?;
        let (target, factor) = match (character, time) { ('Y', false) => (&mut months, 12), ('M', false) => (&mut months, 1), ('W', false) => (&mut days, 7), ('D', false) => (&mut days, 1), ('H', true) => (&mut nanos, 3_600_000_000_000), ('M', true) => (&mut nanos, 60_000_000_000), _ => return Err(invalid("Unsupported ISO interval component")) };
        *target = target.checked_add(number.checked_mul(factor).ok_or_else(|| invalid("Interval overflow"))?).ok_or_else(|| invalid("Interval overflow"))?;
    }
    if start != text.len() { return Err(invalid("Unterminated ISO interval component")); }
    Ok((i32::try_from(months * sign).map_err(invalid)?, i32::try_from(days * sign).map_err(invalid)?, i64::try_from(nanos * sign).map_err(invalid)?))
}
fn read_json(text: &str) -> Result<Value> {
    use serde_json::value::RawValue;
    let text = text.trim();
    Ok(match text.as_bytes().first() {
        Some(b'n') if text == "null" => Value::Null,
        Some(b't') if text == "true" => Value::Bool(true),
        Some(b'f') if text == "false" => Value::Bool(false),
        Some(b'"') => Value::Utf8(serde_json::from_str(text).map_err(invalid)?),
        Some(b'[') => { let values: Vec<Box<RawValue>> = serde_json::from_str(text).map_err(invalid)?; Value::List(values.into_iter().map(|value| read_json(value.get())).collect::<Result<_>>()?) },
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
                    "xt:set" => { let Value::List(values) = read_json(value.get())? else { return Err(invalid("JSON-LD set requires an array")); }; Value::Set(unique(values)) },
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
        T::Null => Value::Null, T::Bool(value) => Value::Bool(value), T::Integer(value) => Value::I64(value), T::Float(value) => Value::F64(value), T::String(value) => Value::Utf8(value), T::Keyword(value) => Value::Keyword(rtdb_types::Keyword::from_name(&value)), T::Bytes(value) => Value::Binary(value), T::Array(values) => Value::List(values.into_iter().map(transit).collect::<Result<_>>()?),
        T::Map(entries) => {
            let mut fields = BTreeMap::new();
            for (key, value) in entries { let key = match key { T::String(key) => key, T::Keyword(key) => normalize_keyword(&key), _ => return Err(Anomaly::unsupported("xtdb.postgres/unsupported-map-key", "Source maps require string or keyword keys")) }; fields.insert(key, transit(value)?); }
            Value::Struct(fields)
        }
        T::Tagged { tag, representation } => {
            let string = || match representation.as_ref() { T::String(text) => Ok(text.as_str()), _ => Err(invalid("Transit tag requires text")) };
            match tag.as_str() {
                "u" => read_text(2950, string()?)?, "f" => decimal(string()?)?, "r" => Value::Uri(string()?.to_owned()),
                "m" => Value::Timestamp { value: string()?.parse::<i64>().map_err(invalid)?.checked_mul(1000).ok_or_else(|| invalid("Transit date overflow"))?, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) },
                "time/instant" => { let mut value = timestamp(string()?)?; if let Value::Timestamp { timezone, .. } = &mut value { *timezone = Some("UTC".into()); } value },
                "time/zoned-date-time" => zoned_timestamp(string()?)?, "time/offset-date-time" => timestamp(string()?)?,
                "time/date-time" => read_text(1114, string()?)?, "time/date" => read_text(1082, string()?)?, "time/time" => read_text(1083, string()?)?, "time/duration" => read_text(11112, string()?)?, "xtdb/interval" => read_text(1186, string()?)?,
                "i8" | "i16" | "i32" | "i64" | "f32" | "f64" => {
                    let value = transit(*representation)?;
                    let scalar = match tag.as_str() { "i8" => rtdb_types::ScalarType::I8, "i16" => rtdb_types::ScalarType::I16, "i32" => rtdb_types::ScalarType::I32, "i64" => rtdb_types::ScalarType::I64, "f32" => rtdb_types::ScalarType::F32, _ => rtdb_types::ScalarType::F64 };
                    rtdb_expr::cast(&value, &rtdb_types::LogicalType::Scalar(scalar), "UTC")?
                }
                "set" => { let T::Array(values) = *representation else { return Err(invalid("Transit set requires an array")); }; Value::Set(unique(values.into_iter().map(transit).collect::<Result<_>>()?)) },
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
                    let [T::String(period), T::String(duration)] = parts.as_slice() else { return Err(invalid("Transit period-duration requires period and duration strings")); };
                    let (months, days, _) = interval(period)?; let (_, duration_days, nanos) = interval(duration)?;
                    let nanos = i64::from(duration_days).checked_mul(86_400_000_000_000).and_then(|days| days.checked_add(nanos)).ok_or_else(|| invalid("Period duration overflow"))?;
                    if days == 0 && nanos == 0 { Value::IntervalYearMonth(months) }
                    else if months == 0 && nanos % 1_000_000 == 0 { Value::IntervalMonthDayMicro { months, days, nanoseconds: nanos } }
                    else { Value::IntervalMonthDayNano { months, days, nanoseconds: nanos } }
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
    let message = object.get("xtdb.error/message").and_then(serde_json::Value::as_str).unwrap_or_default();
    let data = object.get("xtdb.error/data").and_then(serde_json::Value::as_object);
    let code = data.and_then(|data| data.get("xtdb.error/code")).and_then(serde_json::Value::as_str).unwrap_or("xtdb/error");
    let mut error = Anomaly::new(category, code, message);
    if let Some(data) = data { error.context = data.clone().into_iter().collect(); }
    Ok(Value::Transit(rtdb_format::transit::encode_anomaly(&error)?))
}
