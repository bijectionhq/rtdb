use regex::Regex;
use rtdb_types::{Anomaly, Result};
use crate::{numeric::overflow, temporal::IntervalParts};

fn invalid_duration(text: &str) -> Anomaly {
    Anomaly::incorrect("xtdb.expression/invalid-temporal-string", format!("String '{text}' has invalid format for type duration"))
        .with_context("s", text.into()).with_context("date-type", "duration".into())
}
fn regex(pattern: &str) -> Result<Regex> { Regex::new(pattern).map_err(|error| Anomaly::fault("rtdb.expr/temporal-pattern", error.to_string())) }

pub(crate) fn iso_duration(text: &str) -> Result<i128> {
    let pattern = regex(r"(?i-u)^([+-]?)P(?:([+-]?[0-9]+)D)?(?:T(?:([+-]?[0-9]+)H)?(?:([+-]?[0-9]+)M)?(?:([+-]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?$")?;
    let captures = pattern.captures(text).ok_or_else(|| invalid_duration(text))?;
    if (2..=5).all(|index| captures.get(index).is_none()) || (text.contains('T') && (3..=5).all(|index| captures.get(index).is_none()) { return Err(invalid_duration(text)); }
    let mut components = [0i64; 4];
    for (index, factor) in [(2, 86_400i64), (3, 3600), (4, 60), (5, 1)] {
        if let Some(value) = captures.get(index) {
            components[index - 2] = value.as_str().parse::<i64>().ok().and_then(|value| value.checked_mul(factor)).ok_or_else(|| invalid_duration(text))?;
        }
    }
    let seconds = components.into_iter().rev().try_fold(0i64, |total, value| total.checked_add(value).ok_or_else(|| invalid_duration(text)))?;
    let mut nanos = i128::from(seconds) * 1_000_000_000;
    if let Some(fraction) = captures.get(6).filter(|fraction| !fraction.is_empty()) {
        let value = fraction.as_str().parse::<i128>().map_err(|_| invalid_duration(text))? * 10i128.pow((9 - fraction.len()) as u32);
        nanos += if captures.get(5).is_some_and(|seconds| seconds.as_str().starts_with('-')) { -value } else { value };
    }
    i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| invalid_duration(text))?;
    if captures.get(1).is_some_and(|sign| sign.as_str() == "-") { nanos = -nanos; }
    i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| invalid_duration(text))?;
    Ok(nanos)
}

pub(crate) fn duration(text: &str) -> Result<i64> {
    i64::try_from(duration_nanos(text)?).map_err(|_| overflow())
}

pub(crate) fn duration_nanos(text: &str) -> Result<i128> {
    if let Ok(nanos) = iso_duration(text) { return Ok(nanos); }
    let pattern = regex(r"(?i)^'?([0-9.]+)\s+(microsecond|millisecond|second|minute|hour|day|week|month|year)s?'?$")?;
    let captures = pattern.captures(text).ok_or_else(|| invalid_duration(text))?;
    let amount = captures[1].parse::<f64>().map_err(|_| invalid_duration(text))?;
    let milliseconds = match captures[2].to_ascii_lowercase().as_str() {
        "microsecond" => amount / 1000.0, "millisecond" => amount, "second" => amount * 1000.0, "minute" => amount * 60_000.0,
        "hour" => amount * 3_600_000.0, "day" => amount * 86_400_000.0, "week" => amount * 604_800_000.0,
        "month" => amount * 2_628_000_000.0, "year" => amount * 31_536_000_000.0,
        _ => return Err(invalid_duration(text)),
    };
    if !milliseconds.is_finite() || milliseconds < i64::MIN as f64 || milliseconds >= -(i64::MIN as f64) { return Err(overflow()); }
    Ok(i128::from(milliseconds as i64) * 1_000_000)
}

pub(crate) fn interval(text: &str) -> Result<IntervalParts> {
    // xtdb.expression.temporal/iso8601-string->interval uses an unanchored match and treats absent components as zero.
    let pattern = regex(r"P((?:-?[0-9]+Y)?(?:-?[0-9]+M)?(?:-?[0-9]+W)?(?:-?[0-9]+D)?)?T?((?:-?[0-9]+H)?(?:-?[0-9]+M)?(?:-?[0-9]+\.?[0-9]*?S)?)?")?;
    let captures = pattern.captures(text);
    let period = captures.as_ref().and_then(|captures| captures.get(1)).map_or("", |capture| capture.as_str());
    let duration = captures.as_ref().and_then(|captures| captures.get(2)).map_or("", |capture| capture.as_str());
    let mut parts = IntervalParts { months: 0, days: 0, nanos: 0 };
    let mut months = 0i64;
    for capture in regex(r"(-?[0-9]+)([YMWD])")?.captures_iter(period) {
        let value = capture[1].parse::<i32>().map_err(|_| overflow())?;
        match &capture[2] {
            "Y" => months += i64::from(value) * 12,
            "M" => months += i64::from(value),
            "W" => parts.days = parts.days.checked_add(value.checked_mul(7).ok_or_else(overflow)?).ok_or_else(overflow)?,
            "D" => parts.days = parts.days.checked_add(value).ok_or_else(overflow)?,
            _ => unreachable!(),
        }
    }
    parts.months = i32::try_from(months).map_err(|_| overflow())?;
    if !duration.is_empty() {
        let nanos = iso_duration(&format!("PT{duration}"))?;
        parts.nanos = i64::try_from(nanos.div_euclid(1000) * 1000).map_err(|_| overflow())?;
    }
    Ok(parts)
}

pub(crate) fn duration_text(nanos: i128) -> String {
    if nanos == 0 { return "PT0S".into(); }
    let seconds = nanos / 1_000_000_000;
    let remainder = nanos % 1_000_000_000;
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    let mut output = String::from("PT");
    if hours != 0 { output.push_str(&format!("{hours}H")); }
    if minutes != 0 { output.push_str(&format!("{minutes}M")); }
    if seconds != 0 || remainder != 0 || (hours == 0 && minutes == 0) {
        if seconds == 0 && remainder < 0 { output.push('-'); }
        output.push_str(&seconds.to_string());
        if remainder != 0 {
            let fraction = format!("{:09}", remainder.abs());
            output.push('.'); output.push_str(fraction.trim_end_matches('0'));
        }
        output.push('S');
    }
    output
}
pub(crate) fn interval_text(parts: IntervalParts) -> String {
    let mut output = String::from("P");
    if parts.months / 12 != 0 { output.push_str(&format!("{}Y", parts.months / 12)); }
    if parts.months % 12 != 0 { output.push_str(&format!("{}M", parts.months % 12)); }
    if parts.days != 0 { output.push_str(&format!("{}D", parts.days)); }
    if parts.nanos != 0 { output.push_str(&duration_text(i128::from(parts.nanos))[1..]); }
    output
}
