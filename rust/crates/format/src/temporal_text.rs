use rtdb_types::{Anomaly, Result, TimeUnit, Value};

pub fn nanos(value: i64, unit: TimeUnit) -> i128 { i128::from(value) * match unit { TimeUnit::Second => 1_000_000_000, TimeUnit::Millisecond => 1_000_000, TimeUnit::Microsecond => 1000, TimeUnit::Nanosecond => 1 } }
pub fn date_text(days: i64) -> Result<String> { Ok(rtdb_types::CivilDate::from_epoch_day(days)?.to_string()) }
fn fraction(nanos: u32, grouped: bool) -> String {
    if nanos == 0 { return String::new(); }
    let width = if grouped { if nanos % 1_000_000 == 0 { 3 } else if nanos % 1000 == 0 { 6 } else { 9 } } else { 9 };
    let mut value = format!("{nanos:09}"); value.truncate(width); if !grouped { while value.ends_with('0') { value.pop(); } } format!(".{value}")
}
pub fn time_text(value: i128, force_seconds: bool) -> Result<String> {
    if !(0..86_400_000_000_000).contains(&value) { return Err(invalid("Time is outside a day")); }
    let seconds = value / 1_000_000_000; let nanos = (value % 1_000_000_000) as u32;
    let mut text = format!("{:02}:{:02}", seconds / 3600, seconds / 60 % 60);
    if force_seconds || seconds % 60 != 0 || nanos != 0 { text.push_str(&format!(":{:02}{}", seconds % 60, fraction(nanos, !force_seconds))); } Ok(text)
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TimestampText { Iso, Pg }

pub fn timestamp_text(value: i64, unit: TimeUnit, timezone: Option<&str>, format: TimestampText) -> Result<String> {
    let iso = format == TimestampText::Iso;
    let total = nanos(value, unit); let seconds = i64::try_from(total.div_euclid(1_000_000_000)).map_err(invalid)?; let sub = total.rem_euclid(1_000_000_000) as u32;
    let offset = timezone.map(rtdb_types::ZoneId::parse).transpose()?.map_or(Ok(rtdb_types::FixedOffset::UTC), |zone| zone.offset_at(seconds))?;
    let local = rtdb_types::CivilDateTime::from_epoch_second(seconds.checked_add(i64::from(offset.seconds())).ok_or_else(|| invalid("Timestamp offset overflow"))?, sub)?;
    let second = local.second_of_day();
    let mut output = if iso { format!("{}T{}", local.date(), time_text(i128::from(local.nano_of_day()), false)?) }
    else { format!("{} {:02}:{:02}:{:02}{}", local.date(), second / 3600, second / 60 % 60, second % 60, fraction(local.nano() / 1000 * 1000, false)) };
    if let Some(zone) = timezone { if !iso && offset.seconds() == 0 { output.push_str("+00:00"); } else { output.push_str(&offset.to_string()); } if iso && !matches!(zone, "Z") && !zone.starts_with(['+', '-']) { output.push('['); output.push_str(zone); output.push(']'); } }
    Ok(output)
}
pub fn interval_parts(value: &Value) -> Option<(i32, i32, i64)> {
    match value {
        Value::IntervalYearMonth(months) => Some((*months, 0, 0)),
        Value::IntervalDayTime { days, milliseconds } => Some((0, *days, i64::from(*milliseconds) * 1_000_000)),
        Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => Some((*months, *days, *nanoseconds)),
        _ => None,
    }
}
pub fn interval_text(months: i32, days: i32, nanos: i64) -> String { let mut text = String::from("P"); if months / 12 != 0 { text.push_str(&format!("{}Y", months / 12)); } if months % 12 != 0 { text.push_str(&format!("{}M", months % 12)); } if days != 0 { text.push_str(&format!("{days}D")); } if nanos != 0 { text.push_str(&rtdb_types::format_iso_duration(i128::from(nanos))[1..]); } text }

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/temporal-text", error.to_string())
}
