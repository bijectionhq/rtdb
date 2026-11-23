use rtdb_types::StructFields;

use rtdb_format::transit::TransitValue;
use rtdb_types::{Anomaly, DecimalValue, Result, TimeUnit, Value};

pub(crate) fn quoted(text: &str) -> String { serde_json::to_string(text).expect("string serialization") }
pub(crate) fn map(fields: impl IntoIterator<Item = (&'static str, String)>) -> String {
    format!("{{{}}}", fields.into_iter().map(|(name, value)| format!(":{name} {value}")).collect::<Vec<_>>().join(", "))
}
pub(crate) fn vector(values: impl IntoIterator<Item = String>) -> String { format!("[{}]", values.into_iter().collect::<Vec<_>>().join(" ")) }
pub(crate) fn instant(micros: i64) -> String { format!("#time/instant {}", quoted(&datetime(i128::from(micros) * 1000, true))) }
pub(crate) fn document(fields: &StructFields<Value>) -> Result<String> {
    Ok(format!("{{{}}}", fields.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).map(|(key, value)| Ok(format!("{} {}", key_name(key), render(value)?))).collect::<Result<Vec<_>>>()?.join(", ")))
}
pub(crate) fn operation_allocation_bound(value: &rtdb_tx::OperationValue) -> Result<usize> {
    use rtdb_tx::OperationValue as O;
    let add = |left: usize,right: usize| left.checked_add(right).ok_or_else(|| invalid("Operation rendering exceeds addressable memory"));
    match value {
        O::Scalar(value) => rtdb_expr::text_allocation_bound(value),
        O::Tagged { value, .. } => operation_allocation_bound(value),
        O::Struct(fields) => fields.values().try_fold(0, |bytes,value| add(bytes,operation_allocation_bound(value)?)),
        O::List { values, .. } => values.iter().try_fold(0, |bytes,value| add(bytes,operation_allocation_bound(value)?)),
        O::Map(entries) => entries.iter().try_fold(0, |bytes,(key,value)| add(add(bytes,operation_allocation_bound(key)?)?,operation_allocation_bound(value)?)),
    }
}
pub(crate) fn operation(value: &rtdb_tx::OperationValue) -> Result<String> {
    use rtdb_tx::OperationValue as O;
    Ok(match value {
        O::Scalar(value) => render(value)?,
        O::Tagged { tag, value } => format!("#xt/tagged [:{tag} {}]", operation(value)?),
        O::Struct(fields) => format!("{{{}}}", fields.iter().map(|(name, value)| Ok(format!("{} {}", key_name(name), operation(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        O::List { values, set } => { let values = values.iter().map(operation).collect::<Result<Vec<_>>>()?.join(" "); if *set { format!("#{{{values}}}") } else { format!("[{values}]") } }
        O::Map(entries) => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", operation(key)?, operation(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
    })
}
fn key_name(name: &str) -> String {
    let name = name.strip_prefix('_').map_or_else(|| name.to_owned(), |name| format!("xt${name}"));
    let mut parts: Vec<_> = name.split('$').map(|part| part.char_indices().map(|(index, character)| if index > 0 && character == '_' { '-' } else { character }).collect::<String>()).collect();
    let name = parts.pop().unwrap_or_default();
    if parts.is_empty() { format!(":{name}") } else { format!(":{}/{name}", parts.join(".")) }
}
pub(crate) fn render(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Undefined | Value::Null => "nil".into(), Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) => value.to_string(), Value::I64(value) => value.to_string(),
        Value::U8(value) => value.to_string(), Value::U16(value) => value.to_string(), Value::U32(value) => value.to_string(), Value::U64(value) => value.to_string(),
        Value::F16(bits) => {
            let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 }; let exponent = (bits >> 10) & 31; let fraction = bits & 1023;
            float(if exponent == 31 { if fraction == 0 { sign * f64::INFINITY } else { f64::NAN } } else if exponent == 0 { sign * f64::from(fraction) * 2_f64.powi(-24) } else { sign * f64::from(1024 + fraction) * 2_f64.powi(i32::from(exponent) - 25) })
        }
        Value::F32(value) => if value.is_finite() { finite_float(value.to_string()) } else { float(f64::from(*value)) }, Value::F64(value) => float(*value),
        Value::Utf8(value) => quoted(value), Value::Keyword(value) => format!(":{}", value.as_str()),
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => format!("#bytes {}", quoted(&hex(bytes))),
        Value::Uuid(bytes) => { let hex = hex(bytes); format!("#uuid {}", quoted(&format!("{}-{}-{}-{}-{}", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]))) }
        Value::Uri(value) => format!("#xt/uri {}", quoted(&rtdb_types::UriParts::parse(value)?.to_ascii_string())),
        Value::Oid(value) => format!("#xt/oid {value}"), Value::RegClass(value) => format!("#xt/regclass {value}"), Value::RegProc(value) => format!("#xt/regproc {value}"),
        Value::Decimal(value) => decimal(value),
        Value::Date(value) => format!("#time/date {}", quoted(&value.date().to_string())),
        Value::DateDay(days) => format!("#time/date {}", quoted(&date(i64::from(*days)))),
        Value::DateMilli(millis) => format!("#time/date {}", quoted(&date(millis.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY)))),
        Value::Time { value, unit, .. } => format!("#time/time {}", quoted(&time(nanos(*value, *unit), false))),
        Value::Timestamp { value, unit, timezone } => {
            let nanos = nanos(*value, *unit);
            if let Some(zone) = timezone {
                let text = zoned(nanos, zone)?;
                format!("#time/zoned-date-time {}", quoted(&text))
            } else { rtdb_types::CivilDateTime::from_epoch_nanos(nanos)?; format!("#time/date-time {}", quoted(&datetime(nanos, false))) }
        }
        Value::Duration { value, unit } => format!("#time/duration {}", quoted(&duration(nanos(*value, *unit)))),
        Value::IntervalYearMonth(months) => interval(*months, 0, 0),
        Value::IntervalDayTime { days, milliseconds } => interval(0, *days, i128::from(*milliseconds) * 1_000_000),
        Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => interval(*months, *days, i128::from(*nanoseconds)),
        Value::TimestampTzRange { lower, upper } => {
            let endpoint = |value: i64, infinite: i64| if value == infinite { Ok("nil".into()) } else { render(&Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) };
            format!("#xt/tstz-range [{} {}]", endpoint(*lower, i64::MIN)?, endpoint(*upper, i64::MAX)?)
        }
        Value::List(_) | Value::FixedSizeList(_) => vector(value.list_view().unwrap().iter().map(|value| render(value.as_ref())).collect::<Result<Vec<_>>>()?),
        Value::Set(values) => format!("#{{{}}}", values.iter().map(render).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Map { entries, .. } => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", render(key)?, render(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Struct(fields) => document(fields)?,
        Value::Transit(bytes) => transit(&rtdb_format::transit::decode_msgpack(bytes).or_else(|_| rtdb_format::transit::decode_json(bytes))?)?,
    })
}
fn transit(value: &TransitValue) -> Result<String> {
    Ok(match value {
        TransitValue::Null => "nil".into(), TransitValue::Bool(value) => value.to_string(), TransitValue::Integer(value) => value.to_string(), TransitValue::Float(value) => float(*value),
        TransitValue::Float32(value) => if value.is_finite() { rtdb_types::java_f32_text(*value) } else { float(f64::from(*value)) },
        TransitValue::String(value) => quoted(value), TransitValue::Keyword(value) => format!(":{value}"), TransitValue::Symbol(value) => value.clone(),
        TransitValue::Bytes(value) => format!("#bytes {}", quoted(&hex(value))),
        TransitValue::Array(values) => vector(values.iter().map(transit).collect::<Result<Vec<_>>>()?),
        TransitValue::Map(entries) => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", transit(key)?, transit(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        TransitValue::Tagged { tag, representation } if tag == "r" => {
            let TransitValue::String(text) = representation.as_ref() else { return Err(invalid("URI Transit representation must be text")); };
            format!("#xt/uri {}", quoted(&rtdb_types::UriParts::parse(text)?.to_ascii_string()))
        }
        TransitValue::Tagged { tag, representation } => format!("#{tag} {}", transit(representation)?),
    })
}
fn float(value: f64) -> String {
    if value.is_nan() { "##NaN".into() } else if value == f64::INFINITY { "##Inf".into() } else if value == f64::NEG_INFINITY { "##-Inf".into() } else { finite_float(value.to_string()) }
}
fn finite_float(text: String) -> String { if text.contains(['.', 'e', 'E']) { text } else { format!("{text}.0") } }
fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }
fn decimal(value: &DecimalValue) -> String {
    format!("{}M", rtdb_expr::decimal_text(value))
}
fn nanos(value: i64, unit: TimeUnit) -> i128 { i128::from(value) * match unit { TimeUnit::Second => 1_000_000_000, TimeUnit::Millisecond => 1_000_000, TimeUnit::Microsecond => 1000, TimeUnit::Nanosecond => 1 } }
fn zoned(nanos: i128, zone: &str) -> Result<String> {
    let zone = rtdb_expr::normalize_timezone(zone)?;
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| invalid("Timestamp exceeds formatter range"))?;
    let offset = rtdb_types::ZoneId::parse(&zone)?.offset_at(seconds)?;
    let local = nanos + i128::from(offset.seconds()) * 1_000_000_000;
    rtdb_types::CivilDateTime::from_epoch_nanos(local)?;
    let bracket = !(zone == "Z" || zone.starts_with(['+', '-']));
    Ok(format!("{}{}{}", datetime(local, false), offset, if bracket { format!("[{zone}]") } else { String::new() }))
}
fn time(nanos: i128, seconds_required: bool) -> String {
    let seconds = nanos.div_euclid(1_000_000_000);
    let fraction = nanos.rem_euclid(1_000_000_000);
    let mut value = format!("{:02}:{:02}", seconds / 3600, seconds / 60 % 60);
    if seconds_required || seconds % 60 != 0 || fraction != 0 { value.push_str(&format!(":{:02}", seconds % 60)); }
    if fraction != 0 { let digits = if fraction % 1_000_000 == 0 { 3 } else if fraction % 1000 == 0 { 6 } else { 9 }; value.push('.'); value.push_str(&format!("{fraction:09}")[..digits]); }
    value
}
fn datetime(nanos: i128, utc: bool) -> String {
    let days = nanos.div_euclid(86_400_000_000_000);
    format!("{}T{}{}", date(days as i64), time(nanos.rem_euclid(86_400_000_000_000), utc), if utc { "Z" } else { "" })
}
pub(crate) fn date(days: i64) -> String {
    rtdb_types::CivilDate::from_epoch_day(days).expect("validated calendar date or physical Arrow date").to_string()
}
fn duration(nanos: i128) -> String {
    if nanos == 0 { return "PT0S".into(); }
    let sign = if nanos < 0 { "-" } else { "" }; let nanos = nanos.abs();
    let seconds = nanos / 1_000_000_000;
    let mut text = "PT".to_owned();
    if seconds >= 3600 { text.push_str(&format!("{sign}{}H", seconds / 3600)); }
    if seconds / 60 % 60 != 0 { text.push_str(&format!("{sign}{}M", seconds / 60 % 60)); }
    if seconds % 60 != 0 || nanos % 1_000_000_000 != 0 { text.push_str(&format!("{sign}{}", seconds % 60)); if nanos % 1_000_000_000 != 0 { text.push('.'); text.push_str(format!("{:09}", nanos % 1_000_000_000).trim_end_matches('0')); } text.push('S'); }
    text
}
fn interval(months: i32, days: i32, nanos: i128) -> String {
    let mut period = "P".to_owned();
    if months / 12 != 0 { period.push_str(&format!("{}Y", months / 12)); }
    if months % 12 != 0 { period.push_str(&format!("{}M", months % 12)); }
    if days != 0 { period.push_str(&format!("{days}D")); }
    if nanos != 0 { period.push_str(&duration(nanos)[1..]); }
    format!("#xt/interval {}", quoted(&period))
}
fn invalid(message: &str) -> Anomaly { Anomaly::incorrect("rtdb.node/inspection-value", message) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operation_uri_rendering_reserves_nfc_scratch_and_decodes_transit_uri_objects() {
        let uri = Value::Uri("/e\u{301}".into());
        let operation_value = rtdb_tx::OperationValue::Scalar(uri.clone());
        let bound = operation_allocation_bound(&operation_value).unwrap();
        assert!(bound >= rtdb_types::UriParts::parse("/e\u{301}").unwrap().ascii_allocation_bound().unwrap());
        assert_eq!(operation(&operation_value).unwrap(),"#xt/uri \"/%C3%A9\"");
        let tagged = TransitValue::Tagged { tag: "r".into(), representation: Box::new(TransitValue::String("/e\u{301}".into())) };
        let value = Value::Transit(rtdb_format::transit::encode_msgpack(&tagged).unwrap());
        assert_eq!(render(&value).unwrap(),render(&uri).unwrap());
    }

    #[test]
    fn log_tables_edn_preserves_tagged_operations_keyword_names_and_negative_instants() {
        let value = rtdb_tx::OperationValue::Tagged { tag: "put-docs".into(), value: Box::new(rtdb_tx::OperationValue::Struct([
            ("_id".into(), rtdb_tx::OperationValue::Scalar(Value::I64(1))),
            ("foo_bar$baz_qux".into(), rtdb_tx::OperationValue::Scalar(Value::Utf8("v".into()))),
        ].into())) };
        assert_eq!(operation(&value).unwrap(), "#xt/tagged [:put-docs {:xt/id 1, :foo-bar/baz-qux \"v\"}]");
        assert_eq!(instant(-1), "#time/instant \"1969-12-31T23:59:59.999999Z\"");
        assert_eq!(instant(0), "#time/instant \"1970-01-01T00:00:00Z\"");
        assert_eq!(render(&Value::DateMilli(-1)).unwrap(), "#time/date \"1969-12-31\"");
        assert_eq!(render(&Value::DateMilli(-86_400_001)).unwrap(), "#time/date \"1969-12-30\"");
        assert_eq!(render(&Value::Time { value: 0, unit: TimeUnit::Microsecond, bit_width: 64 }).unwrap(), "#time/time \"00:00\"");
        assert_eq!(render(&Value::TimestampTzRange { lower: 0, upper: i64::MAX }).unwrap(), "#xt/tstz-range [#time/zoned-date-time \"1970-01-01T00:00Z[UTC]\" nil]");
        assert_eq!(render(&Value::F32(0.1)).unwrap(), "0.1");
        assert_eq!(render(&Value::Timestamp { value: 1, unit: TimeUnit::Nanosecond, timezone: Some("+01:00:01".into()) }).unwrap(), "#time/zoned-date-time \"1970-01-01T01:00:01.000000001+01:00:01\"");
        assert_eq!(render(&Value::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: Some("Europe/Paris".into()) }).unwrap(), "#time/zoned-date-time \"1970-01-01T01:00+01:00[Europe/Paris]\"");
        assert_eq!(decimal(&DecimalValue { unscaled: vec![123], precision: 3, scale: -2, bit_width: 128 }), "1.23E+4M");
        assert_eq!(decimal(&DecimalValue { unscaled: vec![123], precision: 3, scale: i32::MIN, bit_width: 128 }), "1.23E+2147483650M");
        assert_eq!(decimal(&DecimalValue { unscaled: vec![123], precision: 3, scale: i32::MAX, bit_width: 128 }), "1.23E-2147483645M");
        assert_eq!(render(&Value::IntervalMonthDayMicro { months: 13, days: -2, nanoseconds: 1_500_000_000 }).unwrap(), "#xt/interval \"P1Y1M-2DT1.5S\"");
    }
}
