use rtdb_plan::{TemporalCast, TemporalKind, fractional_precision};
use rtdb_types::{Anomaly, Instant, LogicalType, Result, TimeUnit, Value};
use crate::{numeric::overflow, temporal::IntervalParts};

pub(crate) fn with_context_resolved(value: &Value, target: &TemporalCast, context: &crate::EvalContext<'_>) -> Result<Value> {
    if let Value::Time { value, unit, .. } = value {
        if matches!(target.kind, TemporalKind::TimestampLocal | TemporalKind::TimestampZone(_)) {
            let LogicalType::Scalar(rtdb_types::ScalarType::Timestamp { timezone, .. }) = target.data_type(context.timezone) else { unreachable!() };
            let raw = time_to_timestamp(*value, *unit, target.unit, timezone.is_some(), context.timezone, context.current_time)?;
            return Ok(Value::Timestamp { value: raw, unit: target.unit, timezone });
        }
    }
    cast_resolved(value,target,context.timezone)
}

pub(crate) fn time_to_timestamp(
    value: i64, source: TimeUnit, target: TimeUnit, zoned: bool, timezone: &str, current_time: Instant,
) -> Result<i64> {
    let zone = crate::timezone::zone(timezone)?;
    let today = zone.local_datetime(current_time)?.date();
    let hz = i128::from(target.units_per_second());
    let local_raw = i128::from(today.epoch_day()) * 86_400 * hz + i128::from(value) * hz / i128::from(source.units_per_second());
    let local_raw = i64::try_from(local_raw).map_err(|_| overflow())?;
    if !zoned { return Ok(local_raw); }
    let seconds = local_raw.div_euclid(target.units_per_second());
    let offset = zone.offset_for_local(seconds, None)?;
    local_raw.checked_sub(i64::from(offset.seconds()).checked_mul(target.units_per_second()).ok_or_else(overflow)?).ok_or_else(overflow)
}

pub(crate) fn precision(nanos: i128, precision: Option<i64>) -> Result<i128> {
    let Some(precision) = precision else { return Ok(nanos); };
    let factor = 10i128.pow(9 - fractional_precision(precision, false)?);
    Ok(nanos.div_euclid(factor) * factor)
}

pub(crate) fn cast_resolved(value: &Value, target: &TemporalCast, timezone: &str) -> Result<Value> {
    let result_type = target.data_type(timezone);
    let LogicalType::Scalar(scalar) = &result_type else { unreachable!() };
    let nanos = match value {
        Value::Utf8(text) => text_nanos(text, target)?,
        Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. } if matches!(target.kind, TemporalKind::Duration) => {
            let parts = IntervalParts::of(value).unwrap();
            if parts.months > 0 { return Err(Anomaly::incorrect("xtdb.expression/cannot-cast-mdn-interval-with-months", "Cannot cast month-day-micro/nano intervals when month component is non-zero.")); }
            i128::from(parts.days) * 86_400_000_000_000 + i128::from(parts.nanos)
        }
        _ => return crate::cast::cast(value, &result_type, timezone),
    };
    let raw = quantize(nanos, target)?;
    Ok(match scalar {
        rtdb_types::ScalarType::Timestamp { unit, timezone } => Value::Timestamp { value: raw, unit: *unit, timezone: timezone.clone() },
        rtdb_types::ScalarType::Time { unit, bit_width } => Value::Time { value: raw, unit: *unit, bit_width: *bit_width },
        rtdb_types::ScalarType::Duration(unit) => Value::Duration { value: raw, unit: *unit },
        _ => unreachable!(),
    })
}

pub(crate) fn text_raw(text: &str, target: &TemporalCast) -> Result<i64> {
    quantize(text_nanos(text, target)?, target)
}

fn text_nanos(text: &str, target: &TemporalCast) -> Result<i128> {
    match &target.kind {
        TemporalKind::TimestampLocal | TemporalKind::TimestampZone(_) => {
                let zoned = matches!(target.kind, TemporalKind::TimestampZone(_));
                rtdb_types::parse_sql_timestamp_cast(text, zoned).map_err(|error| {
                    if error.code == "xtdb/invalid-date-time" { error }
                    else { invalid_temporal_text(text, if zoned { "timestamp with timezone" } else { "timestamp without timezone" }) }
                })
            }
        TemporalKind::Time => time(text),
        TemporalKind::Duration => crate::temporal_text::duration_nanos(text),
    }
}

fn quantize(nanos: i128, target: &TemporalCast) -> Result<i64> {
    let nanos = if let Some(precision) = target.precision {
        let factor = 10f64.powf(9i64.wrapping_sub(precision) as f64);
        let fraction = ((nanos.rem_euclid(1_000_000_000) as f64 / factor).floor() * factor) as i64;
        if !(0..1_000_000_000).contains(&fraction) { return Err(Anomaly::incorrect("xtdb/illegal-argument","Invalid value for NanoOfSecond")); }
        nanos.div_euclid(1_000_000_000) * 1_000_000_000 + i128::from(fraction)
    } else { nanos };
    let divisor = 1_000_000_000 / i128::from(target.unit.units_per_second());
    let raw = if matches!(target.kind, TemporalKind::Duration) {
        i128::from(i64::try_from(nanos).map_err(|_| overflow())?) / divisor
    } else { nanos.div_euclid(divisor) };
    i64::try_from(raw).map_err(|_| overflow())
}

pub(crate) fn invalid_temporal_text(text: &str, kind: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression/invalid-temporal-string", format!("String '{text}' has invalid format for type {kind}")).with_context("s", text.into()).with_context("date-type", kind.into()) }

fn time(text: &str) -> Result<i128> {
    let (clock, fraction) = text.split_once('.').map_or((text, None), |(clock, fraction)| (clock, Some(fraction)));
    let fields = clock.split(':').collect::<Vec<_>>();
    if !(2..=3).contains(&fields.len()) || fields.iter().any(|field| field.len() != 2 || !field.bytes().all(|byte| byte.is_ascii_digit())) { return Err(invalid_temporal_text(text, "time without timezone")); }
    if fraction.is_some_and(|fraction| fields.len() != 3 || fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit())) { return Err(invalid_temporal_text(text, "time without timezone")); }
    let number = |index: usize| fields.get(index).map_or(Ok(0u32), |field| field.parse::<u32>().map_err(|_| invalid_temporal_text(text, "time without timezone")));
    let (hours, minutes, seconds) = (number(0)?, number(1)?, number(2)?);
    if hours > 23 || minutes > 59 || seconds > 59 { return Err(invalid_temporal_text(text, "time without timezone")); }
    let fraction = fraction.filter(|fraction| !fraction.is_empty()).map_or(Ok(0i128), |fraction| fraction.parse::<i128>().map(|nanos| nanos * 10i128.pow(9 - fraction.len() as u32)).map_err(|_| invalid_temporal_text(text, "time without timezone")))?;
    Ok(i128::from(hours * 3600 + minutes * 60 + seconds) * 1_000_000_000 + fraction)
}
