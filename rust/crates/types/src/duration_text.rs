use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::LazyLock;

static ISO_DURATION: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i-u)\A([+-]?)P(?:([+-]?[0-9]+)D)?(?:T(?:([+-]?[0-9]+)H)?(?:([+-]?[0-9]+)M)?(?:([+-]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?\z")
        .expect("duration pattern is valid")
});

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// A parsed duration in signed seconds with a normalized nanosecond fraction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "(i64, u32)", into = "(i64, u32)")]
pub struct DurationLiteral {
    seconds: i64,
    nanosecond: u32,
}

impl DurationLiteral {
    /// Maximum bytes of ISO text for a duration in the signed-second range.
    pub const MAX_TEXT_LEN: usize = "PT-2562047788015215H-59M-59.999999999S".len();

    pub fn parse(text: &str) -> std::result::Result<Self, &'static str> {
        let nanos = parse_iso_duration_nanos(text)?;
        Ok(Self { seconds: nanos.div_euclid(NANOS_PER_SECOND) as i64, nanosecond: nanos.rem_euclid(NANOS_PER_SECOND) as u32 })
    }

    pub fn nanoseconds(self) -> i128 {
        i128::from(self.seconds) * NANOS_PER_SECOND + i128::from(self.nanosecond)
    }

    pub fn to_iso_string(self) -> String {
        let mut text = String::with_capacity(Self::MAX_TEXT_LEN);
        write_iso_duration(&mut text, self.nanoseconds()).expect("writing a duration to a String cannot fail");
        text
    }
}

impl TryFrom<(i64, u32)> for DurationLiteral {
    type Error = &'static str;

    fn try_from((seconds, nanosecond): (i64, u32)) -> std::result::Result<Self, Self::Error> {
        if i128::from(nanosecond) >= NANOS_PER_SECOND { return Err("Invalid duration nanosecond fraction"); }
        Ok(Self { seconds, nanosecond })
    }
}

impl From<DurationLiteral> for (i64, u32) {
    fn from(duration: DurationLiteral) -> Self { (duration.seconds, duration.nanosecond) }
}

impl fmt::Display for DurationLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_iso_duration(formatter, self.nanoseconds())
    }
}

fn parse_iso_duration_nanos(text: &str) -> std::result::Result<i128, &'static str> {
    const INVALID: &str = "Text cannot be parsed to a Duration";
    const OVERFLOW: &str = "Text cannot be parsed to a Duration: overflow";

    let captures = ISO_DURATION.captures(text).ok_or(INVALID)?;
    let days = captures.get(2);
    let hours = captures.get(3);
    let minutes = captures.get(4);
    let seconds = captures.get(5);
    if hours.is_none() && minutes.is_none() && seconds.is_none() && (days.is_none() || text.ends_with('T')) {
        return Err(INVALID);
    }
    let component = |value: Option<regex::Match<'_>>, multiplier: i64, error| {
        value.map(|value| value.as_str().parse::<i64>().ok().and_then(|value| value.checked_mul(multiplier)).ok_or(error))
            .transpose().map(|value| value.unwrap_or_default())
    };
    let day_seconds = component(days, 86_400, "Text cannot be parsed to a Duration: days")?;
    let hour_seconds = component(hours, 3600, "Text cannot be parsed to a Duration: hours")?;
    let minute_seconds = component(minutes, 60, "Text cannot be parsed to a Duration: minutes")?;
    let whole_seconds = component(seconds, 1, "Text cannot be parsed to a Duration: seconds")?;
    let whole_seconds = whole_seconds.checked_add(minute_seconds)
        .and_then(|sum| sum.checked_add(hour_seconds))
        .and_then(|sum| sum.checked_add(day_seconds)).ok_or(OVERFLOW)?;
    let mut nanos = i128::from(whole_seconds) * NANOS_PER_SECOND;
    if let Some(fraction) = captures.get(6).filter(|fraction| !fraction.is_empty()) {
        let value = fraction.as_str().parse::<i128>().map_err(|_| "Text cannot be parsed to a Duration: fraction")?
            * 10i128.pow((9 - fraction.len()) as u32);
        nanos += if seconds.is_some_and(|seconds| seconds.as_str().starts_with('-')) { -value } else { value };
    }
    i64::try_from(nanos.div_euclid(NANOS_PER_SECOND)).map_err(|_| OVERFLOW)?;
    if &captures[1] == "-" { nanos = -nanos; }
    i64::try_from(nanos.div_euclid(NANOS_PER_SECOND)).map_err(|_| OVERFLOW)?;
    Ok(nanos)
}

pub fn format_iso_duration(nanos: i128) -> String {
    let mut output = String::new();
    write_iso_duration(&mut output, nanos).expect("writing a duration to a String cannot fail");
    output
}

fn write_iso_duration(output: &mut impl fmt::Write, nanos: i128) -> fmt::Result {
    if nanos == 0 { return output.write_str("PT0S"); }
    let whole_seconds = nanos / NANOS_PER_SECOND;
    let remainder = nanos % NANOS_PER_SECOND;
    let (hours, minutes, seconds) = (whole_seconds / 3600, whole_seconds / 60 % 60, whole_seconds % 60);
    output.write_str("PT")?;
    if hours != 0 { write!(output, "{hours}H")?; }
    if minutes != 0 { write!(output, "{minutes}M")?; }
    if seconds != 0 || remainder != 0 || (hours == 0 && minutes == 0) {
        if seconds == 0 && remainder < 0 { output.write_char('-')?; }
        write!(output, "{seconds}")?;
        if remainder != 0 {
            let mut value = remainder.unsigned_abs();
            let mut digits = [b'0'; 9];
            for digit in digits.iter_mut().rev() {
                *digit += (value % 10) as u8;
                value /= 10;
            }
            let end = digits.iter().rposition(|digit| *digit != b'0').expect("nonzero fraction");
            output.write_char('.')?;
            output.write_str(std::str::from_utf8(&digits[..=end]).expect("decimal digits are ASCII"))?;
        }
        output.write_char('S')?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_literals_keep_normalized_seconds_before_vector_narrowing() {
        let duration = DurationLiteral::parse("-PT9223372037.000000001S").unwrap();
        assert_eq!(duration.nanoseconds(), -9_223_372_037_000_000_001);
        assert_eq!(duration.to_string(), "PT-2562047H-47M-17.000000001S");
        let encoded = serde_json::to_value(duration).unwrap();
        assert_eq!(encoded, serde_json::json!([-9_223_372_038i64, 999_999_999]));
        assert_eq!(serde_json::from_value::<DurationLiteral>(encoded).unwrap(), duration);
        assert!(serde_json::from_value::<DurationLiteral>(serde_json::json!([0, 1_000_000_000])).is_err());
    }
}
