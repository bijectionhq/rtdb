use crate::numeric::{decimal, decimal_value, floating_value, inapplicable, integer, integer_value, number, number_format, overflow};
use crate::timezone::zone;
use num_bigint::BigInt;
use rtdb_types::{Anomaly, CivilDate, DateUnit, Instant, LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use std::str::FromStr;

pub fn parse_timestamp(text: &str, timezone: &str) -> Result<Timestamp> {
    if text == "infinity" { return Ok(Timestamp::MAX); }
    if text == "-infinity" { return Ok(Timestamp::MIN); }
    rtdb_types::parse_sql_timestamp_instant(text, timezone)?.as_micros()
}

fn rescale_units(value: i64, source: TimeUnit, target: TimeUnit) -> Result<i64> {
    let scaled = i128::from(value) * i128::from(target.units_per_second()) / i128::from(source.units_per_second());
    i64::try_from(scaled).map_err(|_| overflow())
}

fn shift_zone(value: i64, unit: TimeUnit, timezone: &str, to_local: bool) -> Result<i64> {
    let zone = zone(timezone)?;
    let seconds = value.div_euclid(unit.units_per_second());
    let offset = if to_local { zone.offset_at(seconds)? } else { zone.offset_for_local(seconds, None)? };
    let adjustment = i64::from(offset.seconds()).checked_mul(unit.units_per_second()).ok_or_else(overflow)?;
    if to_local { value.checked_add(adjustment) } else { value.checked_sub(adjustment) }.ok_or_else(overflow)
}

/// Floors to seconds before shifting, so an instant within an offset of the unit's limit still has a local reading.
fn local_seconds(value: i64, unit: TimeUnit, timezone: Option<&str>) -> Result<i64> {
    let seconds = value.div_euclid(unit.units_per_second());
    match timezone { None => Ok(seconds), Some(timezone) => shift_zone(seconds, TimeUnit::Second, timezone, true) }
}

pub fn timestamp_micros(value: &Value, timezone: &str) -> Result<Timestamp> {
    match value {
        Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => query_clock_instant(value, timezone)?.as_micros(),
        Value::Timestamp { value, unit, timezone: Some(_) } => {
            let micros = (i128::from(*value) * 1_000_000).div_euclid(i128::from(unit.units_per_second()));
            Ok(Timestamp(i64::try_from(micros).map_err(|_| overflow())?))
        }
        Value::Timestamp { value, unit, timezone: None } => {
            let raw = shift_zone(*value, *unit, timezone, false)?;
            Ok(Timestamp(i64::try_from((i128::from(raw) * 1_000_000).div_euclid(i128::from(unit.units_per_second()))).map_err(|_| overflow())?))
        }
        Value::Utf8(text) => parse_timestamp(text, timezone),
        _ => Err(inapplicable("timestamp", &[value])),
    }
}

/// Resolves a query clock without narrowing its fractional precision.
/// Local temporal values use `timezone`; strings use `Time.asInstant`'s date fallback.
/// Unsupported values and invalid calendar/zone inputs return an anomaly.
pub fn query_clock_instant(value: &Value, timezone: &str) -> Result<Instant> {
    if let Some(days) = value.date_days() {
        return zone(timezone)?.resolve_local(rtdb_types::CivilDateTime::midnight(CivilDate::from_epoch_day(days)?), None);
    }
    match value {
        Value::Timestamp { value, unit, timezone: source_zone } => {
            let hz = unit.units_per_second();
            let seconds = value.div_euclid(hz);
            let nanos = (value.rem_euclid(hz) * (1_000_000_000 / hz)) as u32;
            if source_zone.is_some() { Instant::new(seconds, nanos) }
            else {
                let local = rtdb_types::CivilDateTime::from_epoch_second(seconds, nanos)?;
                zone(timezone)?.resolve_local(local, None)
            }
        }
        Value::Utf8(text) => rtdb_types::parse_query_clock(text, timezone),
        _ => Err(inapplicable("query-clock", &[value])),
    }
}

/// Produces a scalar intermediate, preserving calendar dates until the caller's admission boundary.
/// Expression outputs use `Value::admit_expression_dates`; object inputs use `Value::admit_object_dates`.
/// `Byte/parseByte` through `Long/parseLong`, which the reference's text casts call.
fn parse_java_integer(text: &str, target: &ScalarType) -> Result<Value> {
    let invalid = || number_format(format!("For input string: \"{text}\""));
    Ok(match target {
        ScalarType::I64 => Value::I64(text.parse().map_err(|_| invalid())?),
        ScalarType::I32 => Value::I32(text.parse().map_err(|_| invalid())?),
        _ => {
            let value = text.parse::<i32>().map_err(|_| invalid())?;
            let out_of_range = || number_format(format!("Value out of range. Value:\"{text}\" Radix:10"));
            if *target == ScalarType::I8 { Value::I8(i8::try_from(value).map_err(|_| out_of_range())?) } else { Value::I16(i16::try_from(value).map_err(|_| out_of_range())?) }
        }
    })
}

/// `Double/parseDouble`: surrounding control characters and spaces are trimmed, a trailing `f`/`d` is allowed,
/// and only `NaN` and `Infinity` are spelled out.
fn parse_java_float(text: &str) -> Result<f64> {
    let trimmed = text.trim_matches(|ch: char| ch <= ' ');
    if trimmed.is_empty() { return Err(number_format("empty String".into())); }
    let invalid = || number_format(format!("For input string: \"{trimmed}\""));
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    if matches!(unsigned, "NaN" | "Infinity") { return trimmed.replace("Infinity", "inf").parse().map_err(|_| invalid()); }
    let decimal = trimmed.strip_suffix(['f', 'F', 'd', 'D']).unwrap_or(trimmed);
    let digits = decimal.strip_prefix(['+', '-']).unwrap_or(decimal);
    if !digits.starts_with(|ch: char| ch.is_ascii_digit() || ch == '.') { return Err(invalid()); }
    decimal.parse().map_err(|_| invalid())
}

pub fn cast(value: &Value, data_type: &LogicalType, timezone: &str) -> Result<Value> {
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    let data_type = data_type.unencoded();
    let data_type = match &data_type { LogicalType::Maybe(ty) => ty.as_ref(), ty => ty };
    if value.logical_type() == *data_type { return Ok(value.clone()); }
    if let Some(days) = crate::temporal::date_days(value) {
        match data_type {
            LogicalType::Scalar(ScalarType::DateDay) => return Ok(Value::calendar_date(CivilDate::from_epoch_day(days)?, DateUnit::Day)),
            LogicalType::Scalar(ScalarType::DateMilli) => return Ok(Value::calendar_date(CivilDate::from_epoch_day(days)?, DateUnit::Millisecond)),
            _ => {}
        }
    }
    if let LogicalType::List { kind: rtdb_types::ListKind::List, element } = data_type {
        return cast_target(value, &rtdb_plan::CastTarget::List(Box::new(rtdb_plan::CastTarget::fixed(element.as_ref().clone()))), timezone);
    }
    let LogicalType::Scalar(target) = data_type else {
        return Err(Anomaly::unsupported("xtdb/unsupported-cast", format!("Cast from {:?} to {data_type:?}", value.logical_type())));
    };
    use ScalarType as S;
    match target {
        S::I32 if matches!(value, Value::Oid(_) | Value::RegClass(_) | Value::RegProc(_)) => match value {
            Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => Ok(Value::I32(*value)), _ => unreachable!(),
        },
        S::Oid | S::RegClass | S::RegProc => {
            let oid = match value {
                Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => *value,
                Value::Utf8(name) if *target == S::RegProc => crate::pg_catalog::proc_oid(name)?,
                value => i32::try_from(integer(value).ok_or_else(|| inapplicable("oid", &[value]))?).map_err(|_| overflow())?,
            };
            Ok(match target { S::Oid => Value::Oid(oid), S::RegClass => Value::RegClass(oid), S::RegProc => Value::RegProc(oid), _ => unreachable!() })
        }
        S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64 => {
            let value = match value {
                Value::Utf8(text) => match target {
                    S::I8 | S::I16 | S::I32 | S::I64 => return parse_java_integer(text, target),
                    _ => text.parse::<i128>().map_err(|_| inapplicable("integer", &[value]))?,
                },
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
            let number = match value { Value::Utf8(text) => parse_java_float(text)?, value => number(value).ok_or_else(|| inapplicable("float", &[value]))? };
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
        S::Utf8 => Ok(Value::Utf8(crate::string::render::text(value)?)),
        S::Keyword => match value {
            Value::Utf8(text) => Ok(Value::Keyword(rtdb_types::Keyword::from_name(text.strip_prefix(':').unwrap_or(text)))),
            _ => Err(inapplicable("keyword", &[value])),
        },
        S::Uuid => match value { Value::Utf8(text) => Ok(Value::Uuid(*uuid::Uuid::parse_str(text).map_err(|_| inapplicable("uuid", &[value]))?.as_bytes())), Value::FixedSizeBinary(bytes) | Value::Binary(bytes) => Ok(Value::Uuid(bytes.as_slice().try_into().map_err(|_| inapplicable("uuid", &[value]))?)), _ => Err(inapplicable("uuid", &[value])) },
        S::Uri => match value { Value::Utf8(text) => { rtdb_types::UriParts::parse(text)?; Ok(Value::Uri(text.clone())) }, _ => Err(inapplicable("uri", &[value])) },
        S::Timestamp { unit, timezone: target_zone } => {
            if let Value::Timestamp { value, unit: source_unit, timezone: source_zone } = value {
                if source_zone.is_some() == target_zone.is_some() {
                    return Ok(Value::Timestamp { value: rescale_units(*value, *source_unit, *unit)?, unit: *unit, timezone: target_zone.clone() });
                }
                let shifted = shift_zone(*value, *source_unit, timezone, target_zone.is_none())?;
                let scaled = (i128::from(shifted) * i128::from(unit.units_per_second())).div_euclid(i128::from(source_unit.units_per_second()));
                return Ok(Value::Timestamp { value: i64::try_from(scaled).map_err(|_| overflow())?, unit: *unit, timezone: target_zone.clone() });
            }
            if let Some(days) = crate::temporal::date_days(value) {
                let seconds = if target_zone.is_some() { zone(timezone)?.start_of_day(CivilDate::from_epoch_day(days)?)? }
                    else { days.checked_mul(86_400).ok_or_else(overflow)? };
                return Ok(Value::Timestamp { value: seconds.checked_mul(unit.units_per_second()).ok_or_else(overflow)?, unit: *unit, timezone: target_zone.clone() });
            }
            if let Value::Utf8(text) = value {
                if text == "infinity" || text == "-infinity" { return Ok(Value::Timestamp { value: if text == "infinity" { i64::MAX } else { i64::MIN }, unit: *unit, timezone: target_zone.clone() }); }
                let nanos = rtdb_types::parse_sql_timestamp_cast(text, target_zone.is_some())?;
                let value = nanos.div_euclid(1_000_000_000 / i128::from(unit.units_per_second()));
                return Ok(Value::Timestamp { value: i64::try_from(value).map_err(|_| overflow())?, unit: *unit, timezone: target_zone.clone() });
            }
            Err(inapplicable("timestamp", &[value]))
        }
        S::DateDay | S::DateMilli => {
            let days = match value {
                Value::Utf8(text) => CivilDate::parse_iso(text).map_err(|_| crate::temporal_cast::invalid_temporal_text(text, "date"))?.epoch_day(),
                Value::Timestamp { value, unit, timezone: source_zone } => local_seconds(*value, *unit, source_zone.as_ref().map(|_| timezone))?.div_euclid(86_400),
                _ => return Err(inapplicable("date", &[value])),
            };
            Ok(Value::calendar_date(CivilDate::from_epoch_day(days)?, if *target == S::DateDay { DateUnit::Day } else { DateUnit::Millisecond }))
        }
        S::Time { unit, bit_width } => {
            if let Value::Time { value, unit: source_unit, .. } = value {
                return Ok(Value::Time { value: rescale_units(*value, *source_unit, *unit)?, unit: *unit, bit_width: *bit_width });
            }
            let nanos = match value {
                Value::Utf8(text) => rtdb_types::parse_iso_local_time(text)? as i64,
                Value::Timestamp { value, unit, timezone: source_zone } => {
                    let seconds = local_seconds(*value, *unit, source_zone.as_ref().map(|_| timezone))?;
                    seconds.rem_euclid(86_400) * 1_000_000_000 + value.rem_euclid(unit.units_per_second()) * (1_000_000_000 / unit.units_per_second())
                }
                _ => return Err(inapplicable("time", &[value])),
            };
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
                Value::Duration { value, unit } => (crate::temporal::IntervalParts { months: 0, days: 0, nanos: rescale_units(*value, *unit, TimeUnit::Nanosecond).map_err(|_| crate::temporal::interval_overflow())? },
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
            Value::Utf8(text) if *target == S::Binary => binary_from_hex(text).map(Value::Binary),
            _ => Err(inapplicable("binary", &[value])),
        },
        _ => Err(inapplicable("cast", &[value])),
    }
}

fn binary_from_hex(text: &str) -> Result<Vec<u8>> {
    let length = text.encode_utf16().count();
    if !length.is_multiple_of(2) {
        return Err(Anomaly::fault("xtdb.error/unknown", format!("Odd number of characters {length}.")));
    }
    let digit = |unit: u16, index| {
        char::from_u32(u32::from(unit)).and_then(rtdb_types::hexadecimal_digit).ok_or_else(|| {
            Anomaly::fault("xtdb.error/unknown", format!("Illegal hexadecimal character 0x{:02X} at index {index}.", unit as u8))
        })
    };
    let mut bytes = Vec::with_capacity(length / 2);
    let mut units = text.encode_utf16().enumerate();
    while let Some((index, high)) = units.next() {
        let (next_index, low) = units.next().expect("validated the even UTF-16 length");
        bytes.push((digit(high, index)? << 4) | digit(low, next_index)?);
    }
    Ok(bytes)
}

/// Applies expression cast semantics, including source-preserving decimal defaults and recursive lists.
/// Time-to-timestamp casts require `evaluate` with an `EvalContext` supplying the query date.
pub fn cast_target(value: &Value, target: &rtdb_plan::CastTarget, timezone: &str) -> Result<Value> {
    use rtdb_plan::CastTarget;
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    target.validate_source(&value.logical_type())?;
    match target {
        CastTarget::Checked { target: inner, .. } => cast_target(value, inner, timezone),
        CastTarget::Fixed(data_type) => cast(value, data_type, timezone),
        CastTarget::DecimalDefault => match value {
            Value::Decimal(_) => Ok(value.clone()),
            value => cast(value, &rtdb_plan::default_decimal_type(), timezone),
        },
        CastTarget::IntervalDefault | CastTarget::IntervalEmptyOptions => cast(value, &LogicalType::Scalar(ScalarType::IntervalMonthDayMicro), timezone),
        CastTarget::Temporal(target) => crate::temporal_cast::cast(value, target, timezone),
        CastTarget::Deferred(target) => match target.resolve_value(&value.logical_type())? {
            CastTarget::Temporal(target) => crate::temporal_cast::cast_resolved(value,&target,timezone),
            target => cast_target(value,&target,timezone),
        },
        CastTarget::Interval(target) => crate::interval_cast::cast(value, target),
        CastTarget::List(element) => {
            if let Some(target) = element.union_target() {
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

pub(crate) fn pg_array_text(text: &str) -> Result<Vec<Value>> {
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
