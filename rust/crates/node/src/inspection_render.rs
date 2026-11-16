use std::collections::BTreeMap;
use chrono::Offset;

use rtdb_format::transit::TransitValue;
use rtdb_types::{Anomaly, DecimalValue, Result, TimeUnit, Value};

pub(crate) fn quoted(text: &str) -> String { serde_json::to_string(text).expect("string serialization") }
pub(crate) fn map(fields: impl IntoIterator<Item = (&'static str, String)>) -> String {
    format!("{{{}}}", fields.into_iter().map(|(name, value)| format!(":{name} {value}")).collect::<Vec<_>>().join(", "))
}
pub(crate) fn vector(values: impl IntoIterator<Item = String>) -> String { format!("[{}]", values.into_iter().collect::<Vec<_>>().join(" ")) }
pub(crate) fn instant(micros: i64) -> String { format!("#time/instant {}", quoted(&datetime(i128::from(micros) * 1000, true))) }
pub(crate) fn document(fields: &BTreeMap<String, Value>) -> Result<String> {
    Ok(format!("{{{}}}", fields.iter().filter(|(_, value)| !matches!(value, Value::Null | Value::Undefined)).map(|(key, value)| Ok(format!("{} {}", key_name(key), render(value)?))).collect::<Result<Vec<_>>>()?.join(", ")))
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
        Value::Uri(value) => format!("#xt/uri {}", quoted(value)),
        Value::Oid(value) => format!("#xt/oid {value}"), Value::RegClass(value) => format!("#xt/regclass {value}"), Value::RegProc(value) => format!("#xt/regproc {value}"),
        Value::Decimal(value) => decimal(value),
        Value::DateDay(days) => format!("#time/date {}", quoted(&date(i64::from(*days)))),
        Value::DateMilli(millis) => format!("#time/date {}", quoted(&date(millis / 86_400_000))),
        Value::Time { value, unit, .. } => format!("#time/time {}", quoted(&time(nanos(*value, *unit), false))),
        Value::Timestamp { value, unit, timezone } => {
            let nanos = nanos(*value, *unit);
            if let Some(zone) = timezone {
                let text = zoned(nanos, zone)?;
                format!("#time/zoned-date-time {}", quoted(&text))
            } else { format!("#time/date-time {}", quoted(&datetime(nanos, false))) }
        }
        Value::Duration { value, unit } => format!("#time/duration {}", quoted(&duration(nanos(*value, *unit)))),
        Value::IntervalYearMonth(months) => interval(*months, 0, 0),
        Value::IntervalDayTime { days, milliseconds } => interval(0, *days, i128::from(*milliseconds) * 1_000_000),
        Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => interval(*months, *days, i128::from(*nanoseconds)),
        Value::TimestampTzRange { lower, upper } => {
            let endpoint = |value: i64, infinite: i64| if value == infinite { Ok("nil".into()) } else { render(&Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) };
            format!("#xt/tstz-range [{} {}]", endpoint(*lower, i64::MIN)?, endpoint(*upper, i64::MAX)?)
        }
        Value::List(values) | Value::FixedSizeList(values) => vector(values.iter().map(render).collect::<Result<Vec<_>>>()?),
        Value::Set(values) => format!("#{{{}}}", values.iter().map(render).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Map { entries, .. } => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", render(key)?, render(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Struct(fields) => document(fields)?,
        Value::Transit(bytes) => transit(&rtdb_format::transit::decode_msgpack(bytes).or_else(|_| rtdb_format::transit::decode_json(bytes))?)?,
    })
}
fn transit(value: &TransitValue) -> Result<String> {
    Ok(match value {
        TransitValue::Null => "nil".into(), TransitValue::Bool(value) => value.to_string(), TransitValue::Integer(value) => value.to_string(), TransitValue::Float(value) => float(*value),
        TransitValue::String(value) => quoted(value), TransitValue::Keyword(value) => format!(":{value}"), TransitValue::Symbol(value) => value.clone(),
        TransitValue::Bytes(value) => format!("#bytes {}", quoted(&hex(value))),
        TransitValue::Array(values) => vector(values.iter().map(transit).collect::<Result<Vec<_>>>()?),
        TransitValue::Map(entries) => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", transit(key)?, transit(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
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
    let prefixed = ["UTC", "GMT", "UT"].into_iter().find_map(|prefix| zone.strip_prefix(prefix).map(|offset| (prefix, offset)));
    let fixed = prefixed.map_or(zone.as_str(), |(_, offset)| offset);
    let offset = if fixed.is_empty() || fixed == "Z" { 0 }
        else if fixed.starts_with(['+', '-']) {
            let parts = fixed[1..].split(':').map(str::parse::<i32>).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| invalid("Invalid fixed timestamp zone"))?;
            (parts.first().copied().unwrap_or(0) * 3600 + parts.get(1).copied().unwrap_or(0) * 60 + parts.get(2).copied().unwrap_or(0)) * if fixed.starts_with('-') { -1 } else { 1 }
        } else {
            let micros = i64::try_from(nanos.div_euclid(1000)).map_err(|_| invalid("Timestamp exceeds formatter range"))?;
            let instant = chrono::DateTime::from_timestamp_micros(micros).ok_or_else(|| invalid("Timestamp zone is outside the inspection formatter domain"))?;
            instant.with_timezone(&zone.parse::<chrono_tz::Tz>().map_err(|_| invalid("Unknown timestamp zone"))?).offset().fix().local_minus_utc()
        };
    let offset_text = if offset == 0 { "Z".into() } else { let absolute = offset.abs(); format!("{}{:02}:{:02}{}", if offset < 0 { '-' } else { '+' }, absolute / 3600, absolute / 60 % 60, if absolute % 60 == 0 { String::new() } else { format!(":{:02}", absolute % 60) }) };
    let bracket = prefixed.is_some() || !(zone == "Z" || zone.starts_with(['+', '-']));
    Ok(format!("{}{}{}", datetime(nanos + i128::from(offset) * 1_000_000_000, false), offset_text, if bracket { format!("[{zone}]") } else { String::new() }))
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
// Howard Hinnant's civil_from_days: https://howardhinnant.github.io/date_algorithms.html#civil_from_days
pub(crate) fn date(days: i64) -> String {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let year = if year > 9999 { format!("+{year}") } else if year < 0 { format!("-{:04}", -year) } else { format!("{year:04}") };
    format!("{year}-{month:02}-{day:02}")
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
    fn log_tables_edn_preserves_tagged_operations_keyword_names_and_negative_instants() {
        let value = rtdb_tx::OperationValue::Tagged { tag: "put-docs".into(), value: Box::new(rtdb_tx::OperationValue::Struct([
            ("_id".into(), rtdb_tx::OperationValue::Scalar(Value::I64(1))),
            ("foo_bar$baz_qux".into(), rtdb_tx::OperationValue::Scalar(Value::Utf8("v".into()))),
        ].into())) };
        assert_eq!(operation(&value).unwrap(), "#xt/tagged [:put-docs {:xt/id 1, :foo-bar/baz-qux \"v\"}]");
        assert_eq!(instant(-1), "#time/instant \"1969-12-31T23:59:59.999999Z\"");
        assert_eq!(instant(0), "#time/instant \"1970-01-01T00:00:00Z\"");
        assert_eq!(render(&Value::DateMilli(-1)).unwrap(), "#time/date \"1970-01-01\"");
        assert_eq!(render(&Value::DateMilli(-86_400_001)).unwrap(), "#time/date \"1969-12-31\"");
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
