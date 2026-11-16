use bigdecimal::RoundingMode;
use num_traits::ToPrimitive;
use regex::Regex;
use rtdb_plan::{IntervalField as F, IntervalQualifier};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, Value};
use crate::{numeric::{inapplicable, integer, overflow}, temporal::IntervalParts};

const SECOND: i128 = 1_000_000_000;
const DAY: i128 = 86_400 * SECOND;

pub(crate) fn cast(value: &Value, qualifier: &IntervalQualifier) -> Result<Value> {
    qualifier.validate_source(&value.logical_type())?;
    let LogicalType::Scalar(target) = qualifier.result_type(&value.logical_type()) else { unreachable!() };
    let parts = if let Value::Utf8(text) = value {
        if qualifier.end.is_some() { multi(text, qualifier)? } else { single(text, qualifier.start)? }
    } else if let Some(value) = integer(value) {
        integer_interval(i64::try_from(value).map_err(|_| overflow())?, qualifier.start)?
    } else if let Value::Duration { value, unit } = value {
        let nanos = i128::from(*value) * (SECOND / i128::from(unit.units_per_second()));
        let nanos = if matches!(target, ScalarType::IntervalMonthDayMicro) { nanos / 1000 * 1000 } else { nanos };
        normalize(IntervalParts { months: 0, days: 0, nanos: i64::try_from(nanos).map_err(|_| overflow())? }, qualifier)?
    } else if let Some(parts) = IntervalParts::of(value) { normalize(parts, qualifier)? }
    else { return Err(inapplicable("interval", &[value])); };
    parts.into_value(&target)
}

fn integer_interval(value: i64, field: F) -> Result<IntervalParts> {
    let mut result = IntervalParts { months: 0, days: 0, nanos: 0 };
    match field {
        F::Year => result.months = i32::try_from(value).map_err(|_| overflow())?.checked_mul(12).ok_or_else(overflow)?,
        F::Month => result.months = i32::try_from(value).map_err(|_| overflow())?,
        F::Day => result.days = i32::try_from(value).map_err(|_| overflow())?,
        F::Hour | F::Minute | F::Second => result.nanos = i64::try_from(i128::from(value) * SECOND * match field { F::Hour => 3600, F::Minute => 60, _ => 1 }).map_err(|_| overflow())?,
    }
    Ok(result)
}

fn single(text: &str, field: F) -> Result<IntervalParts> {
    if field != F::Second {
        let invalid = || Anomaly::incorrect("xtdb.expression/invalid-interval", "Parse error. Single field INTERVAL string must contain a positive or negative integer.").with_context("interval", text.into());
        let normalized = rtdb_types::decimal_spelling(text).map_err(|_| invalid())?;
        let value = normalized.ascii().parse::<i32>().map_err(|_| invalid())?;
        return integer_interval(i64::from(value), field);
    }
    let (coefficient, scale) = crate::parse_decimal(text).map_err(|_| Anomaly::incorrect("xtdb.expression/invalid-interval", "Parse error. SECOND INTERVAL string must contain a positive or negative integer or decimal.").with_context("interval", text.into()))?.into_bigint_and_scale();
    let nanos = crate::decimal::rescale_coefficient(coefficient, scale, 9, 29, RoundingMode::Down)?.to_i128().ok_or_else(overflow)?;
    Ok(IntervalParts { months: 0, days: 0, nanos: i64::try_from(nanos).map_err(|_| overflow())? })
}

fn normalize(parts: IntervalParts, qualifier: &IntervalQualifier) -> Result<IntervalParts> {
    if qualifier.year_month() {
        let months = match (qualifier.start, qualifier.end) {
            (F::Year, None) => parts.months / 12 * 12,
            (F::Year, Some(F::Month)) => (parts.months / 12 * 12).checked_add(parts.months.rem_euclid(12)).ok_or_else(overflow)?,
            (F::Month, None) => parts.months,
            _ => return Err(invalid(qualifier)),
        };
        return Ok(IntervalParts { months, days: 0, nanos: 0 });
    }
    if parts.months > 0 { return Err(Anomaly::incorrect("xtdb.expression/cannot-normalize-md*-interval-with-months", "Cannot normalize month-day-micro/nano interval with non-zero month component")); }
    let nanos = i128::from(parts.nanos);
    let whole_days = nanos / DAY;
    let days = i128::from(parts.days);
    let second_precision = || alter_interval_precision(nanos, qualifier.fractional_precision.ok_or_else(overflow)?);
    let (days, nanos) = match (qualifier.start, qualifier.end) {
        (F::Day, None) => (days + whole_days, 0),
        (F::Day, Some(F::Hour)) => (days + whole_days, nanos / (3600 * SECOND) % 24 * 3600 * SECOND),
        (F::Day, Some(F::Minute)) => (days + whole_days, nanos / (60 * SECOND) % 1440 * 60 * SECOND),
        (F::Day, Some(F::Second)) => (days + whole_days, second_precision()? - whole_days * DAY),
        (F::Hour, None) => (0, nanos / (3600 * SECOND) * 3600 * SECOND + days * DAY),
        (F::Hour, Some(F::Minute)) | (F::Minute, None) => (0, nanos / (60 * SECOND) * 60 * SECOND + days * DAY),
        (F::Hour | F::Minute, Some(F::Second)) | (F::Second, None) => (0, second_precision()? + days * DAY),
        _ => return Err(invalid(qualifier)),
    };
    Ok(IntervalParts { months: 0, days: i32::try_from(days).map_err(|_| overflow())?, nanos: i64::try_from(nanos).map_err(|_| overflow())? })
}

fn alter_interval_precision(nanos: i128, precision: i64) -> Result<i128> {
    if (0..=9).contains(&precision) { return crate::temporal_cast::precision(nanos, Some(precision)); }
    let fraction = nanos.rem_euclid(SECOND) as f64;
    let factor = 10f64.powf(9i64.wrapping_sub(precision) as f64);
    let altered = ((fraction / factor).floor() * factor) as i32;
    if !(0..1_000_000_000).contains(&altered) { return Err(Anomaly::incorrect("xtdb/illegal-argument", "Invalid value for NanoOfSecond")); }
    Ok(nanos.div_euclid(SECOND) * SECOND + i128::from(altered))
}

fn invalid(qualifier: &IntervalQualifier) -> Anomaly { Anomaly::incorrect("xtdb.expression/invalid-interval-string", "Cannot parse interval, incorrect format.").with_context("start-unit", qualifier.start.name().into()).with_context("end-unit", qualifier.end.map(|end| end.name()).into()) }

fn multi(text: &str, qualifier: &IntervalQualifier) -> Result<IntervalParts> {
    let pattern = match (qualifier.start, qualifier.end) {
        (F::Year, Some(F::Month)) => r"^([-+]?)([0-9]+)-([0-9]+)",
        (F::Day, Some(F::Hour)) => r"^([-+]?)([0-9]+) ([0-9]+)$",
        (F::Day, Some(F::Minute)) => r"^([-+]?)([0-9]+) ([0-9]+):([0-9]+)$",
        (F::Day, Some(F::Second)) => r"^([-+]?)([0-9]+) ([0-9]+):([0-9]+):([0-9]+)(?:\.([0-9]+))?$",
        (F::Hour, Some(F::Minute)) => r"^([-+]?)([0-9]+):([0-9]+)$",
        (F::Hour, Some(F::Second)) => r"^([-+]?)([0-9]+):([0-9]+):([0-9]+)(?:\.([0-9]+))?$",
        (F::Minute, Some(F::Second)) => r"^([-+]?)([0-9]+):([0-9]+)(?:\.([0-9]+))?$",
        _ => return Err(invalid(qualifier)),
    };
    let regex = Regex::new(pattern).map_err(|error| Anomaly::fault("rtdb.expr/interval-pattern", error.to_string()))?;
    let captures = regex.captures(text).ok_or_else(|| invalid(qualifier))?;
    let part = |index| -> Result<i128> { captures.get(index).ok_or_else(|| invalid(qualifier))?.as_str().parse::<i32>().map(i128::from).map_err(|_| overflow()) };
    let sign = if &captures[1] == "-" { -1i128 } else { 1 };
    let (months, days, seconds, fraction) = match (qualifier.start, qualifier.end) {
        (F::Year, Some(F::Month)) => (part(2)? * 12 + part(3)?, 0, 0, None),
        (F::Day, Some(F::Hour)) => (0, part(2)?, part(3)? * 3600, None),
        (F::Day, Some(F::Minute)) => (0, part(2)?, part(3)? * 3600 + part(4)? * 60, None),
        (F::Day, Some(F::Second)) => (0, part(2)?, part(3)? * 3600 + part(4)? * 60 + part(5)?, captures.get(6)),
        (F::Hour, Some(F::Minute)) => (0, 0, part(2)? * 3600 + part(3)? * 60, None),
        (F::Hour, Some(F::Second)) => (0, 0, part(2)? * 3600 + part(3)? * 60 + part(4)?, captures.get(5)),
        (F::Minute, Some(F::Second)) => (0, 0, part(2)? * 60 + part(3)?, captures.get(4)),
        _ => return Err(invalid(qualifier)),
    };
    let fraction = if let Some(fraction) = fraction {
        let value = fraction.as_str().parse::<i64>().map_err(|_| overflow())?;
        let nanos = 10f64.powf(9.0 - fraction.len() as f64) * value as f64;
        let factor = 10f64.powf(9.0 - qualifier.fractional_precision.ok_or_else(overflow)? as f64);
        ((nanos / factor).floor() * factor) as i128
    } else { 0 };
    let nanos = i64::try_from(seconds * SECOND + fraction).map_err(|_| overflow())?;
    Ok(IntervalParts { months: i32::try_from(sign * months).map_err(|_| overflow())?, days: i32::try_from(sign * days).map_err(|_| overflow())?, nanos: if sign < 0 { nanos.checked_neg().ok_or_else(overflow)? } else { nanos } })
}
