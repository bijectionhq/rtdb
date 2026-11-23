use rtdb_plan::BinaryOp;
use rtdb_types::{Anomaly, IntervalUnit, LogicalType, Result, ScalarType, TimeUnit, Value};
use crate::{cast::cast, numeric::{division_by_zero, inapplicable, integer, number, overflow}, timezone::{Zone, zone}};

pub(crate) fn interval_overflow() -> Anomaly {
    Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error")
}

#[derive(Clone, Copy)]
pub(crate) struct IntervalParts { pub months: i32, pub days: i32, pub nanos: i64 }
impl IntervalParts {
    pub fn of(value: &Value) -> Option<Self> {
        Some(match value {
            Value::IntervalYearMonth(months) => Self { months: *months, days: 0, nanos: 0 },
            Value::IntervalDayTime { days, milliseconds } => Self { months: 0, days: *days, nanos: i64::from(*milliseconds) * 1_000_000 },
            Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => Self { months: *months, days: *days, nanos: *nanoseconds },
            _ => return None,
        })
    }
    pub fn into_value(self, target: &ScalarType) -> Result<Value> {
        Ok(match target {
            ScalarType::Interval(IntervalUnit::YearMonth) => Value::IntervalYearMonth(self.months),
            ScalarType::Interval(IntervalUnit::DayTime) => Value::IntervalDayTime { days: self.days, milliseconds: i32::try_from(self.nanos / 1_000_000).map_err(|_| interval_overflow())? },
            ScalarType::Interval(IntervalUnit::MonthDayNano) => Value::IntervalMonthDayNano { months: self.months, days: self.days, nanoseconds: self.nanos },
            ScalarType::IntervalMonthDayMicro => Value::IntervalMonthDayMicro { months: self.months, days: self.days, nanoseconds: self.nanos },
            _ => return Err(inapplicable("interval", &[])),
        })
    }
    fn mixed(self) -> bool { usize::from(self.months != 0) + usize::from(self.days != 0) + usize::from(self.nanos != 0) > 1 }
    fn scale(self, factor: i64) -> Result<Self> {
        Ok(Self {
            months: i32::try_from(i64::from(self.months).checked_mul(factor).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?,
            days: i32::try_from(i64::from(self.days).checked_mul(factor).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?,
            nanos: self.nanos.checked_mul(factor).ok_or_else(interval_overflow)?,
        })
    }
}

pub(crate) fn negate(value: &Value) -> Option<Result<Value>> {
    let parts = IntervalParts::of(value)?;
    Some(parts.scale(-1).and_then(|parts| {
        let LogicalType::Scalar(target) = value.logical_type() else { return Err(inapplicable("negate", &[value])); };
        parts.into_value(&target)
    }))
}

pub(crate) fn absolute(value: &Value) -> Option<Result<Value>> {
    if let Value::Duration { value, unit } = value { return Some(Ok(Value::Duration { value: value.wrapping_abs(), unit: *unit })); }
    let parts = IntervalParts::of(value)?;
    Some(if parts.mixed() { Err(Anomaly::unsupported("xtdb.expression/cannot-abs-mixed-intervals", "Cannot ABS mixed intervals (month, day, time)")) }
        else {
            let LogicalType::Scalar(target) = value.logical_type() else { return Some(Err(inapplicable("abs", &[value]))); };
            IntervalParts { months: parts.months.wrapping_abs(), days: parts.days.wrapping_abs(), nanos: parts.nanos.wrapping_abs() }.into_value(&target)
        })
}

fn rescale(value: i64, source: TimeUnit, target: TimeUnit) -> Result<i64> {
    i64::try_from(i128::from(value) * i128::from(target.units_per_second()) / i128::from(source.units_per_second())).map_err(|_| overflow())
}
pub(crate) fn date_days(value: &Value) -> Option<i64> { value.date_days() }
/// The wall-clock microseconds a date or timestamp reads as in `timezone`: date-times that compare equal share it.
pub fn local_micros(value: &Value, timezone: &str) -> Option<i64> { timestamp_units(value, TimeUnit::Microsecond, true, timezone).ok() }

pub(crate) fn timestamp_units(value: &Value, unit: TimeUnit, localize: bool, timezone: &str) -> Result<i64> {
    if let Some(days) = date_days(value) { return days.checked_mul(86_400).and_then(|seconds| seconds.checked_mul(unit.units_per_second())).ok_or_else(overflow); }
    match value {
        Value::Timestamp { value, unit: source, timezone: source_zone } => {
            if localize && source_zone.is_some() {
                let offset = offset_at(*value, *source, &zone(timezone)?)?;
                let local = value.checked_add(i64::from(offset).checked_mul(source.units_per_second()).ok_or_else(overflow)?).ok_or_else(overflow)?;
                rescale(local, *source, unit)
            } else { rescale(*value, *source, unit) }
        }
        Value::Duration { value, unit: source } | Value::Time { value, unit: source, .. } => rescale(*value, *source, unit),
        _ => Err(inapplicable("temporal arithmetic", &[value])),
    }
}

fn offset_at(value: i64, unit: TimeUnit, zone: &Zone) -> Result<i32> {
    Ok(zone.offset_at(value.div_euclid(unit.units_per_second()))?.seconds())
}

fn local_near(local_seconds: i64, zone: &Zone, preferred_offset: i32) -> Result<i64> {
    let offset = zone.offset_for_local(local_seconds, Some(rtdb_types::FixedOffset::from_seconds(preferred_offset)?))?;
    local_seconds.checked_sub(i64::from(offset.seconds())).ok_or_else(overflow)
}
fn calendar_days(day: i64, parts: IntervalParts, sign: i64) -> Result<i64> {
    Ok(rtdb_types::CivilDate::from_epoch_day(day)?.plus_months(i64::from(parts.months) * sign)?.plus_days(i64::from(parts.days) * sign)?.epoch_day())
}
fn calendar_units(value: i64, unit: TimeUnit, parts: IntervalParts, sign: i64) -> Result<i64> {
    if parts.months == 0 && parts.days == 0 { return Ok(value); }
    let per_day = unit.units_per_second() * 86_400;
    calendar_days(value.div_euclid(per_day), parts, sign)?.checked_mul(per_day).and_then(|date| date.checked_add(value.rem_euclid(per_day))).ok_or_else(overflow)
}
fn add_interval(value: &Value, parts: IntervalParts, result: &ScalarType, sign: i64) -> Result<Value> {
    if matches!(result, ScalarType::DateDay | ScalarType::DateMilli) {
        let days = calendar_days(date_days(value).ok_or_else(|| inapplicable("interval arithmetic", &[value]))?, parts, sign)?;
        return Ok(Value::calendar_date(rtdb_types::CivilDate::from_epoch_day(days)?, if *result == ScalarType::DateDay { rtdb_types::DateUnit::Day } else { rtdb_types::DateUnit::Millisecond }));
    }
    let ScalarType::Timestamp { unit, timezone } = result else { return Err(inapplicable("interval arithmetic", &[value])); };
    let value = timestamp_units(value, *unit, false, "UTC")?;
    let moved = if let Some(timezone) = timezone {
        if parts.months == 0 && parts.days == 0 { value }
        else {
            let zone = zone(timezone)?;
            let original_seconds = value.div_euclid(unit.units_per_second());
            let offset = offset_at(original_seconds, TimeUnit::Second, &zone)?;
            let local = original_seconds.checked_add(i64::from(offset)).ok_or_else(overflow)?;
            let moved = calendar_units(local, TimeUnit::Second, parts, sign)?;
            local_near(moved, &zone, offset)?.checked_mul(unit.units_per_second()).and_then(|seconds| seconds.checked_add(value.rem_euclid(unit.units_per_second()))).ok_or_else(overflow)?
        }
    } else { calendar_units(value, *unit, parts, sign)? };
    let seconds = parts.nanos.div_euclid(1_000_000_000).checked_mul(unit.units_per_second()).ok_or_else(overflow)?;
    let remainder = parts.nanos.rem_euclid(1_000_000_000);
    let per_unit = 1_000_000_000 / unit.units_per_second();
    let duration = if sign > 0 { seconds.checked_add(remainder / per_unit) }
        else { seconds.checked_neg().and_then(|seconds| seconds.checked_sub((remainder + per_unit - 1) / per_unit)) }.ok_or_else(overflow)?;
    Ok(Value::Timestamp { value: moved.checked_add(duration).ok_or_else(overflow)?, unit: *unit, timezone: timezone.clone() })
}

pub(crate) fn arithmetic(op: BinaryOp, left: &Value, right: &Value, timezone: &str) -> Option<Result<Value>> {
    let (LogicalType::Scalar(left_type), LogicalType::Scalar(right_type)) = (left.logical_type(), right.logical_type()) else { return None; };
    let target = rtdb_plan::temporal_arithmetic_type(op, &left_type, &right_type)?;
    Some(arithmetic_value(op, left, right, &target, timezone))
}
fn arithmetic_value(op: BinaryOp, left: &Value, right: &Value, target: &ScalarType, timezone: &str) -> Result<Value> {
    use BinaryOp as B;
    if let (Some(a), Some(b)) = (IntervalParts::of(left), IntervalParts::of(right)) {
        let parts = match op {
            B::Add => IntervalParts { months: a.months.checked_add(b.months).ok_or_else(interval_overflow)?, days: a.days.checked_add(b.days).ok_or_else(interval_overflow)?, nanos: a.nanos.checked_add(b.nanos).ok_or_else(interval_overflow)? },
            B::Subtract => IntervalParts { months: a.months.checked_sub(b.months).ok_or_else(interval_overflow)?, days: a.days.checked_sub(b.days).ok_or_else(interval_overflow)?, nanos: a.nanos.checked_sub(b.nanos).ok_or_else(interval_overflow)? },
            _ => return Err(inapplicable("interval arithmetic", &[left, right])),
        };
        return parts.into_value(target);
    }
    if matches!(op, B::Multiply | B::Divide) {
        if let Some(parts) = IntervalParts::of(left).or_else(|| (op == B::Multiply).then(|| IntervalParts::of(right)).flatten()) {
            let factor = if IntervalParts::of(left).is_some() { right } else { left };
            let factor = i64::try_from(integer(factor).ok_or_else(|| inapplicable("interval arithmetic", &[left, right]))?).map_err(|_| interval_overflow())?;
            let parts = if op == B::Multiply { parts.scale(factor)? }
            else {
                if parts.mixed() { return Err(Anomaly::unsupported("xtdb.expression/cannot-divide-mixed-intervals", "Cannot divide mixed (month, day, time) intervals")); }
                if factor == 0 { return Err(division_by_zero()); }
                IntervalParts { months: i32::try_from(i64::from(parts.months).checked_div(factor).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?, days: i32::try_from(i64::from(parts.days).checked_div(factor).ok_or_else(interval_overflow)?).map_err(|_| interval_overflow())?, nanos: parts.nanos.checked_div(factor).ok_or_else(interval_overflow)? }
            };
            return parts.into_value(target);
        }
        let (duration, factor) = if matches!(left, Value::Duration { .. }) { (left, right) } else { (right, left) };
        let Value::Duration { value, unit } = duration else { return Err(inapplicable("duration arithmetic", &[left, right])); };
        if let Value::Duration { value: other, unit: other_unit } = factor {
            let common = (*unit).max(*other_unit);
            let (a, b) = (rescale(*value, *unit, common)?, rescale(*other, *other_unit, common)?);
            if b == 0 { return Err(division_by_zero()); }
            return Ok(Value::I64(a.checked_div(b).ok_or_else(overflow)?));
        }
        if IntervalParts::of(factor).is_some() {
            let factor = cast(factor, &LogicalType::Scalar(ScalarType::Duration(*unit)), timezone)?;
            return arithmetic_value(op, duration, &factor, target, timezone);
        }
        let value = if let Some(factor) = integer(factor) {
            let factor = i64::try_from(factor).map_err(|_| overflow())?;
            if op == B::Divide && factor == 0 { return Err(division_by_zero()); }
            if op == B::Multiply { value.checked_mul(factor) } else { value.checked_div(factor) }.ok_or_else(overflow)?
        } else {
            let factor = number(factor).ok_or_else(|| inapplicable("duration arithmetic", &[left, right]))?;
            if op == B::Divide && factor == 0.0 { return Err(division_by_zero()); }
            let value = if op == B::Multiply { *value as f64 * factor } else { *value as f64 / factor };
            if !value.is_finite() || value < i64::MIN as f64 || value >= -(i64::MIN as f64) { return Err(overflow()); }
            value as i64
        };
        return Ok(Value::Duration { value, unit: *unit });
    }
    if let Some(parts) = IntervalParts::of(right) { return add_interval(left, parts, target, if op == B::Add { 1 } else { -1 }); }
    if let Some(parts) = IntervalParts::of(left) { return add_interval(right, parts, target, 1); }
    if *target == ScalarType::I32 {
        let overflow = || rtdb_types::Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error");
        let day = |value| date_days(value).and_then(|day| i32::try_from(day).ok()).ok_or_else(overflow);
        return Ok(Value::I32(day(left)?.checked_sub(day(right)?).ok_or_else(overflow)?));
    }
    let unit = match target { ScalarType::Timestamp { unit, .. } | ScalarType::Duration(unit) => *unit, _ => return Err(inapplicable("temporal arithmetic", &[left, right])) };
    let zoned = |value: &Value| matches!(value, Value::Timestamp { timezone: Some(_), .. });
    let localize = op == B::Subtract && matches!(target, ScalarType::Duration(_)) && zoned(left) != zoned(right);
    let (a, b) = (timestamp_units(left, unit, localize, timezone)?, timestamp_units(right, unit, localize, timezone)?);
    let value = if op == B::Add { a.checked_add(b) } else { a.checked_sub(b) }.ok_or_else(overflow)?;
    Ok(match target {
        ScalarType::Duration(unit) => Value::Duration { value, unit: *unit },
        ScalarType::Timestamp { unit, timezone } => Value::Timestamp { value, unit: *unit, timezone: timezone.clone() },
        _ => return Err(inapplicable("temporal arithmetic", &[left, right])),
    })
}
