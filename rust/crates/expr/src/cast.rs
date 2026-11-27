use crate::numeric::{decimal, decimal_value, floating_value, inapplicable, integer, integer_value, number, number_format, overflow};
use crate::timezone::zone;
use num_bigint::BigInt;
use rtdb_types::{Anomaly, CivilDate, DateUnit, Instant, LogicalType, NumericCell, Result, ScalarType, TimeUnit, Timestamp, Value, jvm_number::Number};
use std::str::FromStr;

pub fn parse_timestamp(text: &str, timezone: &str) -> Result<Timestamp> {
    if text == "infinity" { return Ok(Timestamp::MAX); }
    if text == "-infinity" { return Ok(Timestamp::MIN); }
    rtdb_types::parse_sql_timestamp_instant(text, timezone)?.as_micros()
}

/// Converts temporal ticks with truncation toward zero and reports overflow if they exceed the signed 64-bit range.
pub fn rescale_units(value: i64, source: TimeUnit, target: TimeUnit) -> Result<i64> {
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
pub(crate) fn local_seconds(value: i64, unit: TimeUnit, timezone: Option<&str>) -> Result<i64> {
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
pub(crate) fn parse_java_integer(text: &str, target: &ScalarType) -> Result<i128> {
    let invalid = || number_format(format!("For input string: \"{text}\""));
    let Number::Long(value) = Number::parse_long(text).map_err(|_| invalid())? else { unreachable!() };
    Ok(match target {
        ScalarType::I64 => i128::from(value),
        ScalarType::I32 => i128::from(i32::try_from(value).map_err(|_| invalid())?),
        ScalarType::I8 | ScalarType::I16 => {
            let value = i32::try_from(value).map_err(|_| invalid())?;
            let out_of_range = || number_format(format!("Value out of range. Value:\"{text}\" Radix:10"));
            if *target == ScalarType::I8 { i128::from(i8::try_from(value).map_err(|_| out_of_range())?) }
            else { i128::from(i16::try_from(value).map_err(|_| out_of_range())?) }
        }
        _ => unreachable!(),
    })
}

pub(crate) fn parse_java_float(text: &str, target: &ScalarType) -> Result<NumericCell> {
    let trimmed = text.trim_matches(|ch: char| ch <= ' ');
    if trimmed.is_empty() { return Err(number_format("empty String".into())); }
    let number = match target {
        ScalarType::F32 => Number::parse_float(trimmed),
        _ => Number::parse_double(trimmed),
    }.map_err(|_| number_format(format!("For input string: \"{trimmed}\"")))?;
    Ok(match number {
        Number::Float(value) => NumericCell::F32(value),
        Number::Double(value) if *target == ScalarType::F16 => NumericCell::F16(half::f16::from_f64(value).to_bits()),
        Number::Double(value) => NumericCell::F64(value),
        _ => unreachable!(),
    })
}

pub(crate) fn decimal_integer(number: f64) -> Result<i128> {
    if number < i64::MIN as f64 || number > i64::MAX as f64 { return Err(overflow()); }
    Ok(i128::from(number as i64))
}

pub(crate) fn float_integer(number: f64) -> Result<i128> {
    if !number.is_finite() || number < i128::MIN as f64 || number >= i128::MAX as f64 { return Err(overflow()); }
    Ok(number.trunc() as i128)
}

pub(crate) fn timestamp_text_raw(text: &str, unit: TimeUnit, zoned: bool) -> Result<i64> {
    match text {
        "infinity" => return Ok(i64::MAX),
        "-infinity" => return Ok(i64::MIN),
        _ => (),
    }
    let nanos = rtdb_types::parse_sql_timestamp_cast(text, zoned)?;
    timestamp_from_nanos(nanos, unit)
}

pub(crate) fn timestamp_from_nanos(nanos: i128, unit: TimeUnit) -> Result<i64> {
    let seconds = i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| overflow())?;
    let hz = unit.units_per_second();
    let fraction = nanos.rem_euclid(1_000_000_000) as i64 / (1_000_000_000 / hz);
    seconds.checked_mul(hz).and_then(|whole| whole.checked_add(fraction)).ok_or_else(overflow)
}

pub(crate) fn duration_seconds(seconds: f64, unit: TimeUnit) -> Result<i64> {
    let value = seconds * unit.units_per_second() as f64;
    if value < i64::MIN as f64 || value > i64::MAX as f64 {
        return Err(Anomaly::incorrect("xtdb.expression/illegal-argument-exception",
            format!("Value out of range for long: {}", rtdb_types::java_f64_text(value))));
    }
    Ok(value as i64)
}

pub(crate) fn timestamp_raw(value: i64, source: TimeUnit, source_zoned: bool, unit: TimeUnit, zoned: bool, timezone: &str) -> Result<i64> {
    if source_zoned == zoned { return rescale_units(value, source, unit); }
    let shifted = shift_zone(value, source, timezone, !zoned)?;
    let scaled = (i128::from(shifted) * i128::from(unit.units_per_second())).div_euclid(i128::from(source.units_per_second()));
    i64::try_from(scaled).map_err(|_| overflow())
}

/// Casts a selected plain text or timestamp cell to the raw storage of a declared timestamp.
/// The caller handles nulls and verifies that the argument type agrees with its Arrow field.
pub fn cast_bound_timestamp(
    cell: rtdb_columnar::ArrowCell<'_>, unit: TimeUnit, zoned: bool, timezone: &str,
) -> Result<Option<i64>> {
    use arrow_array::{cast::AsArray, types::{TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType}};
    use arrow_schema::{DataType, TimeUnit as ArrowUnit};

    if !cell.field.metadata().is_empty() || cell.array.is_null(cell.row) { return Ok(None); }
    let DataType::Timestamp(source_unit, zone) = cell.field.data_type() else {
        return cell.utf8().map(|text| timestamp_text_raw(text, unit, zoned)).transpose();
    };
    let (raw, source) = match source_unit {
        ArrowUnit::Second => (cell.array.as_primitive::<TimestampSecondType>().value(cell.row), TimeUnit::Second),
        ArrowUnit::Millisecond => (cell.array.as_primitive::<TimestampMillisecondType>().value(cell.row), TimeUnit::Millisecond),
        ArrowUnit::Microsecond => (cell.array.as_primitive::<TimestampMicrosecondType>().value(cell.row), TimeUnit::Microsecond),
        ArrowUnit::Nanosecond => (cell.array.as_primitive::<TimestampNanosecondType>().value(cell.row), TimeUnit::Nanosecond),
    };
    if source == unit && zone.is_some() == zoned { return Ok(Some(raw)); }
    timestamp_raw(raw, source, zone.is_some(), unit, zoned, timezone).map(Some)
}

pub(crate) fn date_to_timestamp(days: i64, unit: TimeUnit, zoned: bool, timezone: &str) -> Result<i64> {
    let seconds = if zoned { zone(timezone)?.start_of_day(CivilDate::from_epoch_day(days)?)? }
        else { days.checked_mul(86_400).ok_or_else(overflow)? };
    seconds.checked_mul(unit.units_per_second()).ok_or_else(overflow)
}

pub(crate) fn timestamp_to_time(value: i64, source: TimeUnit, unit: TimeUnit, timezone: Option<&str>) -> Result<i64> {
    let seconds = local_seconds(value, source, timezone)?;
    let nanos = seconds.rem_euclid(86_400) * 1_000_000_000
        + value.rem_euclid(source.units_per_second()) * (1_000_000_000 / source.units_per_second());
    Ok(nanos / (1_000_000_000 / unit.units_per_second()))
}

pub(crate) fn boolean_text(text: &str) -> Result<bool> {
    match text.to_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
        _ => Err(rtdb_types::function_type_mismatch("boolean", &[LogicalType::Scalar(ScalarType::Utf8)])),
    }
}

pub fn cast(value: &Value, data_type: &LogicalType, timezone: &str) -> Result<Value> {
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    cast_from(value, &value.logical_type(), data_type, timezone)
}

fn cast_from(value: &Value, source: &LogicalType, data_type: &LogicalType, timezone: &str) -> Result<Value> {
    let data_type = data_type.unencoded();
    let data_type = match &data_type { LogicalType::Maybe(ty) => ty.as_ref(), ty => ty };
    if source == data_type { return Ok(value.clone()); }
    if let Some(days) = crate::temporal::date_days(value) {
        match data_type {
            LogicalType::Scalar(ScalarType::DateDay) => return Ok(Value::calendar_date(CivilDate::from_epoch_day(days)?, DateUnit::Day)),
            LogicalType::Scalar(ScalarType::DateMilli) => return Ok(Value::calendar_date(CivilDate::from_epoch_day(days)?, DateUnit::Millisecond)),
            _ => {}
        }
    }
    if let LogicalType::List { kind: rtdb_types::ListKind::List, element } = data_type {
        let target = rtdb_plan::CastTarget::List(Box::new(rtdb_plan::CastTarget::fixed(element.as_ref().clone())));
        target.validate_source(source)?;
        return cast_target_from(value, source, &target, timezone);
    }
    let LogicalType::Scalar(target) = data_type else {
        return Err(Anomaly::unsupported("xtdb/unsupported-cast", format!("Cast from {source:?} to {data_type:?}")));
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
                    S::I8 | S::I16 | S::I32 | S::I64 => parse_java_integer(text, target)?,
                    _ => text.parse::<i128>().map_err(|_| inapplicable("integer", &[value]))?,
                },
                Value::Bool(value) => i128::from(*value),
                Value::Decimal(value) => {
                    let number = crate::decimal_to_f64(value);
                    decimal_integer(number)?
                }
                value => if let Some(value) = integer(value) { value } else {
                    let number = number(value).ok_or_else(|| inapplicable("integer", &[value]))?;
                    float_integer(number)?
                },
            };
            integer_value(value, target)
        }
        S::F16 | S::F32 | S::F64 => {
            if let Value::Utf8(text) = value { return parse_java_float(text, target).map(NumericCell::into_value); }
            Ok(floating_value(number(value).ok_or_else(|| inapplicable("float", &[value]))?, target))
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
            Value::Utf8(text) => boolean_text(text).map(Value::Bool),
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
                let value = timestamp_raw(*value, *source_unit, source_zone.is_some(), *unit, target_zone.is_some(), timezone)?;
                return Ok(Value::Timestamp { value, unit: *unit, timezone: target_zone.clone() });
            }
            if let Some(days) = crate::temporal::date_days(value) {
                let value = date_to_timestamp(days, *unit, target_zone.is_some(), timezone)?;
                return Ok(Value::Timestamp { value, unit: *unit, timezone: target_zone.clone() });
            }
            if let Value::Utf8(text) = value {
                let value = timestamp_text_raw(text, *unit, target_zone.is_some())?;
                return Ok(Value::Timestamp { value, unit: *unit, timezone: target_zone.clone() });
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
        S::Time { unit: target_unit, bit_width } => {
            if let Value::Time { value, unit: source_unit, .. } = value {
                return Ok(Value::Time { value: rescale_units(*value, *source_unit, *target_unit)?, unit: *target_unit, bit_width: *bit_width });
            }
            let nanos = match value {
                Value::Utf8(text) => crate::temporal_cast::time_nanos(text)? as i64,
                Value::Timestamp { value, unit, timezone: source_zone } => {
                    let value = timestamp_to_time(*value, *unit, *target_unit, source_zone.as_ref().map(|_| timezone))?;
                    return Ok(Value::Time { value, unit: *target_unit, bit_width: *bit_width });
                }
                _ => return Err(inapplicable("time", &[value])),
            };
            Ok(Value::Time { value: nanos / (1_000_000_000 / target_unit.units_per_second()), unit: *target_unit, bit_width: *bit_width })
        }
        S::Duration(unit) => match value {
            Value::Duration { value, unit: source } | Value::Time { value, unit: source, .. } => {
                Ok(Value::Duration { value: rescale_units(*value, *source, *unit)?, unit: *unit })
            }
            Value::Utf8(text) => Ok(Value::Duration { value: rescale_units(crate::temporal_text::duration(text)?, TimeUnit::Nanosecond, *unit)?, unit: *unit }),
            Value::F64(seconds) => Ok(Value::Duration { value: duration_seconds(*seconds, *unit)?, unit: *unit }),
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
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    let source = value.logical_type();
    target.validate_source(&source)?;
    cast_target_from(value, &source, target, timezone)
}

pub(crate) fn cast_target_from(value: &Value, source: &LogicalType, target: &rtdb_plan::CastTarget, timezone: &str) -> Result<Value> {
    use rtdb_plan::CastTarget;
    match target {
        CastTarget::Checked { target: inner, .. } => cast_target_from(value, source, inner, timezone),
        CastTarget::Fixed(data_type) => cast_from(value, source, data_type, timezone),
        CastTarget::DecimalDefault => match value {
            Value::Decimal(_) => Ok(value.clone()),
            value => cast_from(value, source, &rtdb_plan::default_decimal_type(), timezone),
        },
        CastTarget::IntervalDefault | CastTarget::IntervalEmptyOptions => cast_from(value, source, &LogicalType::Scalar(ScalarType::IntervalMonthDayMicro), timezone),
        CastTarget::Temporal(target) => crate::temporal_cast::cast_resolved(value, target, timezone),
        CastTarget::Deferred(target) => match target.resolve_value(source)? {
            CastTarget::Temporal(target) => crate::temporal_cast::cast_resolved(value,&target,timezone),
            target => {
                target.validate_source(source)?;
                cast_target_from(value, source, &target, timezone)
            },
        },
        CastTarget::Interval(target) => crate::interval_cast::cast(value, target),
        CastTarget::List(element) => {
            if let Some(target) = element.union_target() {
                let Value::List(values) = value else { return Err(inapplicable("list union", &[value])); };
                let legs = target.legs();
                let fits = if values.array().is_some() {
                    let LogicalType::List { element, .. } = source else { unreachable!("list source type"); };
                    element.legs().iter().all(|ty| legs.contains(ty))
                } else {
                    values.iter().all(|value| legs.contains(&value.logical_type()))
                };
                if !fits { return Err(inapplicable("list union", &[value])); }
                return Ok(Value::List(values.clone()));
            }
            if let Value::List(list) | Value::FixedSizeList(list) = value
                && let Some(list) = crate::list_cast::numeric(crate::list_cast::Input::Stored(list.view()), element, false, |_| Ok(()))?
            {
                return Ok(Value::List(list));
            }
            let parsed;
            let values = match value {
                Value::List(_) | Value::FixedSizeList(_) => value.list_view().unwrap(),
                Value::Utf8(text) => { parsed = pg_array_text(text)?; rtdb_types::ListView::Values(&parsed) }
                _ => return Err(inapplicable("list", &[value])),
            };
            if let Some(list) = crate::scalar_list_cast::cast_list(
                crate::scalar_list_cast::Source::Stored(values), element, timezone, None, false, |_| Ok(()),
            )? {
                return Ok(Value::List(list));
            }
            let mut output = rtdb_types::ListBuilder::with_capacity(values.len());
            for value in values.iter() {
                output.push(cast_target(value.as_ref(), element, timezone)?);
            }
            Ok(Value::List(output.finish()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wave4_union_list_cast_checks_selected_numeric_legs() {
        let Value::List(full) = Value::list(vec![Value::F32(4.0), Value::I64(5), Value::Null]) else { unreachable!() };
        let accepted = rtdb_plan::CastTarget::List(Box::new(rtdb_plan::CastTarget::Fixed(LogicalType::Scalar(ScalarType::I64).maybe())));
        let slice = Value::List(full.slice_packed(1, 2).unwrap());
        assert_eq!(cast_target(&slice, &accepted, "UTC").unwrap(), slice);
        assert_eq!(cast_target(&Value::List(full), &accepted, "UTC").unwrap_err().code, "xtdb.expression/function-type-mismatch");
        assert_eq!(cast_target(&Value::list(vec![]), &accepted, "UTC").unwrap(), Value::list(vec![]));
    }

    #[test]
    fn wave4_union_list_cast_rejects_an_incompatible_element_only_when_reached() {
        use rtdb_plan::{CastTarget, Expr, Schema};

        let target = LogicalType::Scalar(ScalarType::I64).maybe();
        let array = Value::list(vec![Value::I64(7), Value::F32(3.0)]);
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal(array)),
            target: CastTarget::List(Box::new(CastTarget::Fixed(target.clone()))),
        };
        let subscript = |index| Expr::Subscript {
            expr: Box::new(expr.clone()), index: Box::new(Expr::Literal(Value::I32(index))), data_type: target.clone(),
        };
        let schema = Schema::default();
        let context = crate::EvalContext::default();
        assert_eq!(crate::evaluate(&subscript(0), &schema, &[], &context).unwrap(), Value::I64(7));
        assert_eq!(crate::evaluate(&subscript(1), &schema, &[], &context).unwrap_err().code, "xtdb/invalid-argument-types");
    }
}
