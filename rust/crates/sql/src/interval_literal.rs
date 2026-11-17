use regex::{Captures, Regex};
use rtdb_types::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IntervalLiteralError { NoMatch, Invalid(String) }
impl IntervalLiteralError {
    pub fn message(&self) -> Option<&str> { match self { Self::NoMatch => None, Self::Invalid(message) => Some(message) } }
}

pub(crate) fn parse(text: &str) -> std::result::Result<Value, IntervalLiteralError> {
    let parsed = iso(text).and_then(|value| value.map_or_else(|| postgres(text), |value| Ok(Some(value))));
    match parsed {
        Ok(Some((months, days, nanoseconds))) => Ok(if nanoseconds % 1000 != 0 {
            Value::IntervalMonthDayNano { months, days, nanoseconds }
        } else if days != 0 || nanoseconds != 0 {
            Value::IntervalMonthDayMicro { months, days, nanoseconds }
        } else { Value::IntervalYearMonth(months) }),
        Ok(None) => Err(IntervalLiteralError::NoMatch),
        Err(message) => Err(IntervalLiteralError::Invalid(message)),
    }
}

type Parts = (i32, i32, i64);
type ParseResult<T> = std::result::Result<T, String>;

fn pattern(text: &str) -> Regex { Regex::new(text).expect("interval literal pattern is valid") }
fn failure(message: &str) -> String { message.into() }

fn iso(text: &str) -> ParseResult<Option<Parts>> {
    let outer = pattern(r"\A(-)?P([-0-9YMWD]+)?(?:T([-0-9HMS.]+)?)?\z");
    let Some(parts) = outer.captures(text) else { return Ok(None); };
    let negative = parts.get(1).is_some();
    let (years, months, days) = parts.get(2).map(|part| period(part.as_str(), negative)).transpose()?.unwrap_or((0, 0, 0));
    let nanos = parts.get(3).map(|part| duration(part.as_str(), negative)).transpose()?.unwrap_or(0);
    let months = i32::try_from(i64::from(years) * 12 + i64::from(months)).map_err(|_| failure("integer overflow"))?;
    let nanos = i64::try_from(nanos).map_err(|_| failure("long overflow"))?;
    Ok(Some((months, days, nanos)))
}

fn period(text: &str, negative: bool) -> ParseResult<(i32, i32, i32)> {
    let invalid = || failure("Text cannot be parsed to a Period");
    let components = pattern(r"\A(?:(-?[0-9]+)Y)?(?:(-?[0-9]+)M)?(?:(-?[0-9]+)W)?(?:(-?[0-9]+)D)?\z");
    let parts = components.captures(text).ok_or_else(invalid)?;
    let component = |index| {
        let value = parts.get(index).map(|part| part.as_str().parse::<i32>()).transpose().map_err(|_| invalid())?.unwrap_or(0);
        if negative { value.checked_neg().ok_or_else(invalid) } else { Ok(value) }
    };
    let years = component(1)?;
    let months = component(2)?;
    let weeks = component(3)?;
    let days = component(4)?;
    let days = weeks.checked_mul(7).and_then(|weeks| days.checked_add(weeks)).ok_or_else(|| failure("integer overflow"))?;
    Ok((years, months, days))
}

fn duration(text: &str, negative: bool) -> ParseResult<i128> {
    let invalid = || failure("Text cannot be parsed to a Duration");
    let components = pattern(r"\A(?:(-?[0-9]+)H)?(?:(-?[0-9]+)M)?(?:(-?[0-9]+)(?:\.([0-9]{0,9}))?S)?\z");
    let parts = components.captures(text).ok_or_else(invalid)?;
    let hours = duration_component(&parts, 1, 3600, "hours")?;
    let minutes = duration_component(&parts, 2, 60, "minutes")?;
    let seconds = duration_component(&parts, 3, 1, "seconds")?;
    let seconds = minutes.checked_add(seconds).and_then(|seconds| hours.checked_add(seconds)).ok_or_else(|| failure("Text cannot be parsed to a Duration: overflow"))?;
    let fraction = parts.get(4).filter(|part| !part.is_empty()).map(|part| {
        part.as_str().parse::<i32>().unwrap() * 10i32.pow(9 - part.len() as u32)
    }).unwrap_or(0);
    let fraction = if parts.get(3).is_some_and(|part| part.as_str().starts_with('-')) { -fraction } else { fraction };
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(fraction);
    i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| failure("Text cannot be parsed to a Duration: overflow"))?;
    let nanos = if negative { -nanos } else { nanos };
    i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| failure("Text cannot be parsed to a Duration: overflow"))?;
    Ok(nanos)
}

fn duration_component(parts: &Captures<'_>, index: usize, multiplier: i64, name: &str) -> ParseResult<i64> {
    parts.get(index).map(|part| part.as_str().parse::<i64>().ok().and_then(|value| value.checked_mul(multiplier))
        .ok_or_else(|| format!("Text cannot be parsed to a Duration: {name}"))).transpose().map(|value| value.unwrap_or(0))
}

fn postgres(text: &str) -> ParseResult<Option<Parts>> {
    let lower = text.to_lowercase();
    let components = pattern(r"(-?[0-9]+)[ \t\n\x0B\x0C\r]*(years?|months?|weeks?|days?|hours?|minutes?|seconds?)");
    let mut result = None;
    for part in components.captures_iter(&lower) {
        let value = part[1].parse::<i64>().map_err(|_| format!("For input string: \"{}\"", &part[1]))?;
        let (months, days, nanos) = result.get_or_insert((0i32, 0i32, 0i64));
        match &part[2] {
            "year" | "years" => add_period(months, value, 12)?,
            "month" | "months" => add_period(months, value, 1)?,
            "week" | "weeks" => add_period(days, value, 7)?,
            "day" | "days" => add_period(days, value, 1)?,
            unit => {
                let multiplier = match unit { "hour" | "hours" => 3_600_000_000_000, "minute" | "minutes" => 60_000_000_000, _ => 1_000_000_000 };
                *nanos = value.checked_mul(multiplier).and_then(|value| nanos.checked_add(value)).ok_or_else(|| failure("long overflow"))?;
            }
        }
    }
    Ok(result)
}

fn add_period(target: &mut i32, value: i64, multiplier: i64) -> ParseResult<()> {
    let value = value.checked_mul(multiplier).ok_or_else(|| failure("long overflow"))?;
    let value = i32::try_from(value).map_err(|_| failure("integer overflow"))?;
    *target = target.checked_add(value).ok_or_else(|| failure("integer overflow"))?;
    Ok(())
}
