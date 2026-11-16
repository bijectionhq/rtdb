use crate::numeric::{decimal, decimal_value, floating_value, inapplicable, integer, integer_value, number, overflow};
use chrono::{DateTime, LocalResult, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone, Utc};
use crate::timezone::{Zone, zone};
use num_bigint::BigInt;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use std::str::FromStr;

fn local_instant(local: NaiveDateTime, timezone: &str) -> Result<DateTime<Zone>> {
    let zone = zone(timezone)?;
    local_instant_in_zone(local, &zone)
}

pub(crate) fn local_instant_in_zone(local: NaiveDateTime, zone: &Zone) -> Result<DateTime<Zone>> {
    match zone.from_local_datetime(&local) {
        LocalResult::Single(value) => Ok(value),
        LocalResult::Ambiguous(first, _) => Ok(first),
        LocalResult::None => {
            let mut before = None;
            let mut after = None;
            for minutes in 1..=2880 {
                let duration = chrono::Duration::minutes(minutes);
                if before.is_none() { before = local.checked_sub_signed(duration).and_then(|time| zone.from_local_datetime(&time).earliest()); }
                if after.is_none() { after = local.checked_add_signed(duration).and_then(|time| zone.from_local_datetime(&time).earliest()); }
                if let (Some(before), Some(after)) = (&before, &after) {
                    let gap = after.offset().fix().local_minus_utc() - before.offset().fix().local_minus_utc();
                    let shifted = local.checked_add_signed(chrono::Duration::seconds(i64::from(gap))).ok_or_else(overflow)?;
                    return zone.from_local_datetime(&shifted).earliest().ok_or_else(|| Anomaly::incorrect("xtdb/invalid-local-time", "Local time cannot be resolved in its time zone"));
                }
            }
            Err(Anomaly::incorrect("xtdb/invalid-local-time", format!("Local time {local} cannot be resolved in {zone:?}")))
        }
    }
}

pub fn parse_timestamp(text: &str, timezone: &str) -> Result<Timestamp> {
    if text == "infinity" { return Ok(Timestamp::MAX); }
    if text == "-infinity" { return Ok(Timestamp::MIN); }
    Ok(Timestamp(parse_instant(text, timezone)?.timestamp_micros()))
}

fn parse_instant(text: &str, timezone: &str) -> Result<DateTime<Utc>> {
    if let Ok(value) = DateTime::parse_from_rfc3339(text) { return Ok(value.with_timezone(&Utc)); }
    for format in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f %:z"] {
        if let Ok(value) = DateTime::parse_from_str(text, format) { return Ok(value.with_timezone(&Utc)); }
    }
    for format in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"] {
        if let Ok(value) = NaiveDateTime::parse_from_str(text, format) { return Ok(local_instant(value, timezone)?.with_timezone(&Utc)); }
    }
    if let Ok(value) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        return Ok(local_instant(value.and_hms_opt(0, 0, 0).ok_or_else(overflow)?, timezone)?.with_timezone(&Utc));
    }
    Err(Anomaly::incorrect("xtdb/invalid-timestamp", format!("Invalid timestamp: {text}")))
}

fn instant_nanos(value: &Value, timezone: &str) -> Result<i128> {
    let timestamp = match value {
        Value::Timestamp { value, unit, timezone: Some(_) } => return Ok(i128::from(*value) * (1_000_000_000 / i128::from(unit.units_per_second()))),
        Value::Timestamp { value, unit, timezone: None } => {
            let local = datetime_from_nanos(i128::from(*value) * (1_000_000_000 / i128::from(unit.units_per_second())))?.naive_utc();
            local_instant(local, timezone)?.with_timezone(&Utc)
        }
        Value::Utf8(text) => parse_instant(text, timezone)?,
        _ => return Ok(i128::from(timestamp_micros(value, timezone)?.0) * 1_000),
    };
    Ok(i128::from(timestamp.timestamp()) * 1_000_000_000 + i128::from(timestamp.timestamp_subsec_nanos()))
}

fn datetime_from_nanos(nanos: i128) -> Result<DateTime<Utc>> {
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| overflow())?;
    DateTime::from_timestamp(seconds, nanos.rem_euclid(1_000_000_000) as u32).ok_or_else(overflow)
}

fn rescale_units(value: i64, source: TimeUnit, target: TimeUnit) -> Result<i64> {
    let scaled = i128::from(value) * i128::from(target.units_per_second()) / i128::from(source.units_per_second());
    i64::try_from(scaled).map_err(|_| overflow())
}

pub fn timestamp_micros(value: &Value, timezone: &str) -> Result<Timestamp> {
    match value {
        Value::Timestamp { value, unit, timezone: Some(_) } => {
            let micros = (i128::from(*value) * 1_000_000).div_euclid(i128::from(unit.units_per_second()));
            Ok(Timestamp(i64::try_from(micros).map_err(|_| overflow())?))
        }
        Value::Timestamp { value, unit, timezone: None } => {
            let seconds = value.div_euclid(unit.units_per_second());
            let nanos = value.rem_euclid(unit.units_per_second()) * (1_000_000_000 / unit.units_per_second());
            let local = DateTime::from_timestamp(seconds, nanos as u32).ok_or_else(overflow)?.naive_utc();
            Ok(Timestamp(local_instant(local, timezone)?.timestamp_micros()))
        }
        Value::DateDay(days) => {
            let local = DateTime::from_timestamp(i64::from(*days) * 86400, 0).ok_or_else(overflow)?.naive_utc();
            Ok(Timestamp(local_instant(local, timezone)?.timestamp_micros()))
        }
        Value::DateMilli(millis) => {
            let local = DateTime::from_timestamp((millis / 86_400_000) * 86_400, 0).ok_or_else(overflow)?.naive_utc();
            Ok(Timestamp(local_instant(local, timezone)?.timestamp_micros()))
        }
        Value::Utf8(text) => parse_timestamp(text, timezone),
        _ => Err(inapplicable("timestamp", &[value])),
    }
}

pub fn cast(value: &Value, data_type: &LogicalType, timezone: &str) -> Result<Value> {
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    let data_type = data_type.unencoded();
    let data_type = match &data_type { LogicalType::Maybe(ty) => ty.as_ref(), ty => ty };
    if let Some(days) = crate::temporal::date_days(value) {
        match data_type {
            LogicalType::Scalar(ScalarType::DateDay) => return Ok(Value::DateDay(i32::try_from(days).map_err(|_| overflow())?)),
            LogicalType::Scalar(ScalarType::DateMilli) => return Ok(Value::DateMilli(days.checked_mul(86_400_000).ok_or_else(overflow)?)),
            _ => {}
        }
    }
    if value.logical_type() == *data_type { return Ok(value.clone()); }
    if let LogicalType::List { kind: rtdb_types::ListKind::List, element } = data_type {
        return cast_target(value, &rtdb_plan::CastTarget::List(Box::new(rtdb_plan::CastTarget::fixed(element.as_ref().clone()))), timezone);
    }
    let LogicalType::Scalar(target) = data_type else {
        return Err(Anomaly::unsupported("xtdb/unsupported-cast", format!("Cast from {:?} to {data_type:?}", value.logical_type())));
    };
    use ScalarType as S;
    match target {
        S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 => {
            let value = match value {
                Value::Utf8(text) => text.parse::<i128>().map_err(|_| inapplicable("integer", &[value]))?,
                Value::Bool(value) => i128::from(*value),
                Value::Decimal(value) => {
                    let number = crate::decimal_to_f64(value);
                    if number < i64::MIN as f64 || number > i64::MAX as f64 { return Err(overflow()); }
                    i128::from(number as i64)
                }
                value => if let Some(value) = integer(value) { value } else {
                    let number = number(value).ok_or_else(|| inapplicable("integer", &[value]))?;
                    if !number.is_finite() || number < i128::MIN as f64 || number >= i128::MAX as f64 { return Err(overflow()); }
                    number.trunc() as i128
                },
            };
            integer_value(value, target)
        }
        S::F16 | S::F32 | S::F64 => {
            let number = match value { Value::Utf8(text) => text.parse().map_err(|_| inapplicable("float", &[value]))?, value => number(value).ok_or_else(|| inapplicable("float", &[value]))? };
            Ok(floating_value(number, target))
        }
        S::Decimal { precision, scale, bit_width } => {
            let decimal = match value {
                Value::Decimal(value) => decimal(value),
                Value::Utf8(text) => crate::parse_decimal(text).map_err(|_| inapplicable("decimal", &[value]))?,
                value => if let Some(integer) = integer(value) { bigdecimal::BigDecimal::new(BigInt::from(integer), 0) }
                    else { bigdecimal::BigDecimal::from_str(&number(value).ok_or_else(|| inapplicable("decimal", &[value]))?.to_string()).map_err(|_| overflow())? },
            };
            decimal_value(decimal, *precision, *scale, *bit_width)
        }
        S::Bool => match value {
            Value::Utf8(text) => match text.to_lowercase().as_str() { "true" | "t" | "yes" | "y" | "on" | "1" => Ok(Value::Bool(true)), "false" | "f" | "no" | "n" | "off" | "0" => Ok(Value::Bool(false)), _ => Err(inapplicable("boolean", &[value])) },
            value if number(value).is_some() => Ok(Value::Bool(number(value) != Some(0.0))),
            _ => Err(inapplicable("boolean", &[value])),
        },
        S::Utf8 => {
            let text = match value {
                Value::Utf8(text) | Value::Uri(text) => text.clone(), Value::Bool(value) => value.to_string(),
                Value::Keyword(value) => value.to_string(), Value::Uuid(value) => uuid::Uuid::from_bytes(*value).to_string(),
                Value::Decimal(value) => crate::decimal_text(value),
                value if integer(value).is_some() => integer(value).ok_or_else(overflow)?.to_string(),
                value if number(value).is_some() => number(value).ok_or_else(overflow)?.to_string(),
                Value::Timestamp { .. } => DateTime::from_timestamp_micros(timestamp_micros(value, timezone)?.0).ok_or_else(overflow)?.to_rfc3339(),
                Value::DateDay(days) => DateTime::from_timestamp(i64::from(*days) * 86400, 0).ok_or_else(overflow)?.date_naive().to_string(),
                Value::Duration { value, unit } => {
                    let nanos = i128::from(*value) * (1_000_000_000 / i128::from(unit.units_per_second()));
                    crate::temporal_text::duration_text(i128::from(i64::try_from(nanos).map_err(|_| overflow())?))
                }
                value if crate::temporal::IntervalParts::of(value).is_some() => crate::temporal_text::interval_text(crate::temporal::IntervalParts::of(value).ok_or_else(overflow)?),
                _ => return Err(inapplicable("text", &[value])),
            };
            Ok(Value::Utf8(text))
        }
        S::Keyword => match value { Value::Utf8(text) => Ok(Value::Keyword(rtdb_types::Keyword::from_name(text))), _ => Err(inapplicable("keyword", &[value])) },
        S::Uuid => match value { Value::Utf8(text) => Ok(Value::Uuid(*uuid::Uuid::parse_str(text).map_err(|_| inapplicable("uuid", &[value]))?.as_bytes())), Value::FixedSizeBinary(bytes) | Value::Binary(bytes) => Ok(Value::Uuid(bytes.as_slice().try_into().map_err(|_| inapplicable("uuid", &[value]))?)), _ => Err(inapplicable("uuid", &[value])) },
        S::Uri => match value { Value::Utf8(text) => Ok(Value::Uri(text.clone())), _ => Err(inapplicable("uri", &[value])) },
        S::Timestamp { unit, timezone: target_zone } => {
            if let Value::Timestamp { value, unit: source_unit, timezone: source_zone } = value {
                if source_zone.is_some() == target_zone.is_some() {
                    return Ok(Value::Timestamp { value: rescale_units(*value, *source_unit, *unit)?, unit: *unit, timezone: target_zone.clone() });
                }
            }
            if let Value::Utf8(text) = value {
                if text == "infinity" || text == "-infinity" { return Ok(Value::Timestamp { value: if text == "infinity" { i64::MAX } else { i64::MIN }, unit: *unit, timezone: target_zone.clone() }); }
            }
            let instant = instant_nanos(value, timezone)?;
            let nanos = if target_zone.is_none() {
                let local = datetime_from_nanos(instant)?.with_timezone(&zone(timezone)?).naive_local().and_utc();
                i128::from(local.timestamp()) * 1_000_000_000 + i128::from(local.timestamp_subsec_nanos())
            } else { instant };
            let value = nanos.div_euclid(1_000_000_000 / i128::from(unit.units_per_second()));
            Ok(Value::Timestamp { value: i64::try_from(value).map_err(|_| overflow())?, unit: *unit, timezone: target_zone.clone() })
        }
        S::DateDay | S::DateMilli => {
            let timestamp = timestamp_micros(value, timezone)?;
            let date = DateTime::from_timestamp_micros(timestamp.0).ok_or_else(overflow)?.with_timezone(&zone(timezone)?).date_naive();
            let days = date.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(overflow)?).num_days();
            if *target == S::DateDay { Ok(Value::DateDay(i32::try_from(days).map_err(|_| overflow())?)) }
            else { Ok(Value::DateMilli(days.checked_mul(86_400_000).ok_or_else(overflow)?)) }
        }
        S::Time { unit, bit_width } => {
            use chrono::Timelike;
            if let Value::Time { value, unit: source_unit, .. } = value {
                return Ok(Value::Time { value: rescale_units(*value, *source_unit, *unit)?, unit: *unit, bit_width: *bit_width });
            }
            let time = match value {
                Value::Utf8(text) => NaiveTime::parse_from_str(text, "%H:%M:%S%.f").map_err(|_| inapplicable("time", &[value]))?,
                _ => datetime_from_nanos(instant_nanos(value, timezone)?)?.with_timezone(&zone(timezone)?).time(),
            };
            let nanos = i64::from(time.num_seconds_from_midnight()) * 1_000_000_000 + i64::from(time.nanosecond());
            Ok(Value::Time { value: nanos / (1_000_000_000 / unit.units_per_second()), unit: *unit, bit_width: *bit_width })
        }
        S::Duration(unit) => match value {
            Value::Duration { value, unit: source } | Value::Time { value, unit: source, .. } => {
                Ok(Value::Duration { value: rescale_units(*value, *source, *unit)?, unit: *unit })
            }
            Value::Utf8(text) => Ok(Value::Duration { value: rescale_units(crate::temporal_text::duration(text)?, TimeUnit::Nanosecond, *unit)?, unit: *unit }),
            Value::F64(seconds) => {
                let value = seconds * unit.units_per_second() as f64;
                if !value.is_finite() || value < i64::MIN as f64 || value >= -(i64::MIN as f64) { return Err(overflow()); }
                Ok(Value::Duration { value: value as i64, unit: *unit })
            }
            Value::IntervalYearMonth(_) => Err(Anomaly::incorrect("xtdb.expression/invalid-cast", "Cannot cast a year-month interval to a duration").with_context("interval-unit", "year-month".into())),
            Value::IntervalDayTime { .. } => Err(Anomaly::incorrect("xtdb.expression/invalid-cast", "Cannot cast a day-time interval to a duration").with_context("interval-unit", "day-time".into())),
            value if crate::temporal::IntervalParts::of(value).is_some() => {
                let parts = crate::temporal::IntervalParts::of(value).ok_or_else(overflow)?;
                if parts.months > 0 { return Err(Anomaly::incorrect("xtdb.expression/cannot-cast-mdn-interval-with-months", "Cannot cast month-day-micro/nano intervals when month component is non-zero.")); }
                let nanos = i128::from(parts.days) * 86_400_000_000_000i128 + i128::from(parts.nanos);
                Ok(Value::Duration { value: rescale_units(i64::try_from(nanos).map_err(|_| overflow())?, TimeUnit::Nanosecond, *unit)?, unit: *unit })
            }
            _ => Err(inapplicable("duration", &[value])),
        },
        S::Interval(_) | S::IntervalMonthDayMicro => {
            if crate::temporal::IntervalParts::of(value).is_some() { return Ok(value.clone()); }
            let (parts, target) = match value {
                Value::Utf8(text) => (crate::temporal_text::interval(text)?, S::IntervalMonthDayMicro),
                Value::Duration { value, unit } => (crate::temporal::IntervalParts { months: 0, days: 0, nanos: rescale_units(*value, *unit, TimeUnit::Nanosecond)? },
                    if *unit == TimeUnit::Nanosecond { S::Interval(rtdb_types::IntervalUnit::MonthDayNano) } else { S::IntervalMonthDayMicro }),
                _ => return Err(inapplicable("interval", &[value])),
            };
            parts.into_value(&target)
        }
        S::Binary | S::FixedSizeBinary(_) => match value {
            Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => {
                if let S::FixedSizeBinary(width) = target { if bytes.len() != *width as usize { return Err(inapplicable("fixed-size binary", &[value])); } }
                Ok(if *target == S::Binary { Value::Binary(bytes.clone()) } else { Value::FixedSizeBinary(bytes.clone()) })
            }
            Value::Utf8(text) if *target == S::Binary => Ok(Value::Binary(text.as_bytes().to_vec())),
            _ => Err(inapplicable("binary", &[value])),
        },
        _ => Err(inapplicable("cast", &[value])),
    }
}

/// Applies expression cast semantics, including source-preserving decimal defaults and recursive lists.
pub fn cast_target(value: &Value, target: &rtdb_plan::CastTarget, timezone: &str) -> Result<Value> {
    use rtdb_plan::CastTarget;
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    match target {
        CastTarget::Fixed(data_type) => cast(value, data_type, timezone),
        CastTarget::DecimalDefault => match value {
            Value::Decimal(_) => Ok(value.clone()),
            value => cast(value, &rtdb_plan::default_decimal_type(), timezone),
        },
        CastTarget::IntervalDefault => cast(value, &LogicalType::Scalar(ScalarType::IntervalMonthDayMicro), timezone),
        CastTarget::List(element) => {
            if let CastTarget::Fixed(target @ (LogicalType::Union(_) | LogicalType::Maybe(_))) = element.as_ref() {
                let Value::List(values) = value else { return Err(inapplicable("list union", &[value])); };
                let legs = target.legs();
                if values.iter().any(|value| !legs.contains(&value.logical_type())) { return Err(inapplicable("list union", &[value])); }
                return Ok(Value::List(values.clone()));
            }
            let parsed;
            let values = match value {
                Value::List(values) | Value::FixedSizeList(values) => values,
                Value::Utf8(text) => { parsed = pg_array_text(text)?; &parsed }
                _ => return Err(inapplicable("list", &[value])),
            };
            Ok(Value::List(values.iter().map(|value| cast_target(value, element, timezone)).collect::<Result<_>>()?))
        }
    }
}

fn pg_array_text(text: &str) -> Result<Vec<Value>> {
    let text = text.trim();
    let inner = text.strip_prefix('{').and_then(|text| text.strip_suffix('}')).ok_or_else(|| Anomaly::incorrect("xtdb.expression/malformed-array-literal", format!("Malformed array literal: '{text}'")))?;
    if inner.trim_start().starts_with('{') { return Err(Anomaly::unsupported("xtdb.expression/multi-dimensional-array-literal", "Multi-dimensional array literals aren't supported")); }
    if inner.is_empty() { return Ok(Vec::new()); }
    let finish = |element: &mut String, quoted: bool| {
        let text = std::mem::take(element);
        if quoted { Value::Utf8(text) } else if text.trim().eq_ignore_ascii_case("NULL") { Value::Null } else { Value::Utf8(text.trim().to_owned()) }
    };
    let mut values = Vec::new(); let mut element = String::new(); let mut quoted = false; let mut in_quotes = false;
    let mut chars = inner.chars();
    while let Some(character) = chars.next() {
        match character {
            '\\' => { element.push(chars.next().unwrap_or('}')); quoted = true; }
            '"' => { in_quotes = !in_quotes; quoted = true; }
            ',' if !in_quotes => { values.push(finish(&mut element, quoted)); quoted = false; }
            other => element.push(other),
        }
    }
    values.push(finish(&mut element, quoted));
    Ok(values)
}
