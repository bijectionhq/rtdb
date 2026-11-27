use rtdb_types::{Result, Value};
use crate::invalid;

pub(crate) fn parse(text: &str) -> Result<(i32, i32, i64)> {
    let (negative, text) = text.strip_prefix('-').map_or((false, text), |text| (true, text));
    let text = text.strip_prefix('P').ok_or_else(|| invalid("Interval requires an ISO-8601 P prefix"))?;
    let (period, time) = text.split_once('T').map_or((text, ""), |parts| parts);
    let (months, days) = period_parts(period, negative, false)?;
    let months = i32::try_from(months).map_err(invalid)?;
    let (seconds, fraction) = duration_parts(time, 0, negative, false)?;
    let nanos = duration_nanos(seconds, fraction)?;
    Ok((months, days, nanos))
}

pub(crate) fn matches_interval(text: &str) -> bool {
    let Some(text) = text.strip_prefix('-').unwrap_or(text).strip_prefix('P') else { return false; };
    let (period, time) = text.split_once('T').map_or((text, ""), |parts| parts);
    period.bytes().all(|byte| byte.is_ascii_digit() || b"-YMWD".contains(&byte))
        && time.bytes().all(|byte| byte.is_ascii_digit() || b"-HMS.".contains(&byte))
}

fn period_parts(period: &str, negative: bool, period_syntax: bool) -> Result<(i64, i32)> {
    let mut fields = [0i32; 4];
    let mut remaining = period;
    let mut previous = None;
    while !remaining.is_empty() {
        let (number, unit) = component(&mut remaining)?;
        let unit = if period_syntax { unit.to_ascii_uppercase() } else { unit };
        let index = match unit { b'Y' => 0, b'M' => 1, b'W' => 2, b'D' => 3, _ => return Err(invalid("Invalid ISO period unit")) };
        ordered(&mut previous, index)?;
        signed_digits(number, period_syntax)?;
        let value = number.parse::<i32>().map_err(invalid)?;
        fields[index] = if negative { value.checked_neg().ok_or_else(|| invalid("Interval period overflow"))? } else { value };
    }
    let months = i64::from(fields[0]) * 12 + i64::from(fields[1]);
    let days = fields[2].checked_mul(7).and_then(|weeks| fields[3].checked_add(weeks)).ok_or_else(|| invalid("Interval days overflow"))?;
    Ok((months, days))
}

pub(crate) fn duration_micros(text: &str) -> Result<i64> {
    let (seconds, nanos) = duration(text)?;
    Ok(seconds.wrapping_mul(1_000_000).wrapping_add(i64::from(nanos / 1000)))
}

pub(crate) fn period_duration(period: &str, text: &str) -> Result<Value> {
    let (negative, period) = period.strip_prefix('-').map_or((false, period.strip_prefix('+').unwrap_or(period)), |period| (true, period));
    let period = period.strip_prefix(['P', 'p']).filter(|period| !period.is_empty()).ok_or_else(|| invalid("Period requires an ISO component"))?;
    let (months, days) = period_parts(period, negative, true)?;
    let (seconds, nanos) = duration(text)?;
    // Parity with XTDB IntervalVector.kt (0b9fa440f2): Long.toInt() truncates months and
    // normalized days when PeriodDuration components exceed i32.
    if days == 0 && seconds == 0 && nanos == 0 { return Ok(Value::IntervalYearMonth(months as i32)); }
    if months == 0 {
        let nanoseconds = duration_nanos(seconds, nanos)?;
        return Ok(if nanoseconds % 1_000_000 == 0 { Value::IntervalMonthDayMicro { months: 0, days, nanoseconds } }
            else { Value::IntervalMonthDayNano { months: 0, days, nanoseconds } });
    }
    let (days, nanoseconds) = match duration_nanos(seconds, nanos) {
        Ok(nanoseconds) => (days, nanoseconds),
        Err(_) => {
            let extra_days = seconds / 86_400;
            ((i64::from(days) + extra_days) as i32, duration_nanos(seconds % 86_400, nanos)?)
        }
    };
    Ok(Value::IntervalMonthDayNano { months: months as i32, days, nanoseconds })
}

fn duration_nanos(seconds: i64, fraction: u32) -> Result<i64> {
    i64::try_from(i128::from(seconds) * 1_000_000_000 + i128::from(fraction)).map_err(invalid)
}

fn duration(text: &str) -> Result<(i64, u32)> {
    let (negative, text) = text.strip_prefix('-').map_or((false, text.strip_prefix('+').unwrap_or(text)), |text| (true, text));
    let text = text.strip_prefix(['P', 'p']).ok_or_else(|| invalid("Duration requires an ISO-8601 P prefix"))?;
    let (date, time) = text.find(['T', 't']).map_or((text, None), |index| (&text[..index], Some(&text[index + 1..])));
    if (time == Some("") && text.ends_with('T')) || (date.is_empty() && time.is_none_or(str::is_empty)) { return Err(invalid("Duration requires a component")); }
    let days = if date.is_empty() { 0 } else {
        let number = date.strip_suffix(['D', 'd']).ok_or_else(|| invalid("Duration date part requires days"))?;
        signed_digits(number, true)?;
        number.parse::<i64>().map_err(invalid)?.checked_mul(86_400).ok_or_else(|| invalid("Duration days overflow"))?
    };
    duration_parts(time.unwrap_or(""), days, negative, true)
}

fn duration_parts(time: &str, days: i64, negative: bool, duration_syntax: bool) -> Result<(i64, u32)> {
    let mut seconds = [0i64; 3];
    let mut fraction = 0i32;
    let mut remaining = time;
    let mut previous = None;
    while !remaining.is_empty() {
        let (number, unit) = component(&mut remaining)?;
        let unit = if duration_syntax { unit.to_ascii_uppercase() } else { unit };
        let (index, multiplier) = match unit { b'H' => (0, 3600), b'M' => (1, 60), b'S' => (2, 1), _ => return Err(invalid("Invalid ISO duration unit")) };
        ordered(&mut previous, index)?;
        let separator = if duration_syntax { number.find(['.', ',']) } else { number.find('.') };
        let (whole, fractional) = if unit == b'S' { separator.map_or((number, None), |index| (&number[..index], Some(&number[index + 1..]))) } else { (number, None) };
        signed_digits(whole, duration_syntax)?;
        seconds[index] = whole.parse::<i64>().map_err(invalid)?.checked_mul(multiplier).ok_or_else(|| invalid("Interval seconds overflow"))?;
        if let Some(digits) = fractional {
            if digits.len() > 9 || !digits.bytes().all(|byte| byte.is_ascii_digit()) { return Err(invalid("Interval seconds allow at most nine fractional digits")); }
            fraction = digits.bytes().fold(0i32, |value, byte| value * 10 + i32::from(byte - b'0')) * 10i32.pow(9 - digits.len() as u32);
            if whole.starts_with('-') { fraction = -fraction; }
        }
    }
    let seconds = seconds[1].checked_add(seconds[2]).and_then(|minutes| seconds[0].checked_add(minutes)).and_then(|seconds| days.checked_add(seconds))
        .and_then(|seconds| seconds.checked_add(i64::from(fraction.div_euclid(1_000_000_000)))).ok_or_else(|| invalid("Duration seconds overflow"))?;
    let fraction = fraction.rem_euclid(1_000_000_000) as u32;
    if !negative { return Ok((seconds, fraction)); }
    let (seconds, fraction) = if fraction == 0 { (-i128::from(seconds), 0) } else { (-i128::from(seconds) - 1, 1_000_000_000 - fraction) };
    Ok((i64::try_from(seconds).map_err(invalid)?, fraction))
}

fn component<'a>(remaining: &mut &'a str) -> Result<(&'a str, u8)> {
    let input = *remaining;
    let index = input.bytes().position(|byte| byte.is_ascii_alphabetic()).ok_or_else(|| invalid("Unterminated ISO interval component"))?;
    let number = &input[..index];
    let unit = input.as_bytes()[index];
    *remaining = &input[index + 1..];
    Ok((number, unit))
}

fn signed_digits(number: &str, plus: bool) -> Result<()> {
    let digits = number.strip_prefix('-').or_else(|| if plus { number.strip_prefix('+') } else { None }).unwrap_or(number);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) { return Err(invalid("Invalid ISO interval number")); }
    Ok(())
}

fn ordered(previous: &mut Option<usize>, index: usize) -> Result<()> {
    if previous.is_some_and(|previous| previous >= index) { return Err(invalid("Repeated or out-of-order interval component")); }
    *previous = Some(index);
    Ok(())
}
