use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;
use rtdb_types::{Anomaly, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Zone { Named(Tz), Fixed(FixedOffset) }

#[derive(Clone, Copy, Debug)]
pub(crate) struct ZoneOffset { zone: Zone, offset: FixedOffset }
impl Offset for ZoneOffset { fn fix(&self) -> FixedOffset { self.offset } }
impl std::fmt::Display for ZoneOffset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { std::fmt::Display::fmt(&self.offset, formatter) }
}
impl TimeZone for Zone {
    type Offset = ZoneOffset;
    fn from_offset(offset: &ZoneOffset) -> Self { offset.zone }
    fn offset_from_local_date(&self, date: &NaiveDate) -> LocalResult<ZoneOffset> {
        match self {
            Self::Named(zone) => zone.offset_from_local_date(date).map(|offset| ZoneOffset { zone: *self, offset: offset.fix() }),
            Self::Fixed(offset) => LocalResult::Single(ZoneOffset { zone: *self, offset: *offset }),
        }
    }
    fn offset_from_local_datetime(&self, datetime: &NaiveDateTime) -> LocalResult<ZoneOffset> {
        match self {
            Self::Named(zone) => zone.offset_from_local_datetime(datetime).map(|offset| ZoneOffset { zone: *self, offset: offset.fix() }),
            Self::Fixed(offset) => LocalResult::Single(ZoneOffset { zone: *self, offset: *offset }),
        }
    }
    fn offset_from_utc_date(&self, date: &NaiveDate) -> ZoneOffset {
        let offset = match self { Self::Named(zone) => zone.offset_from_utc_date(date).fix(), Self::Fixed(offset) => *offset };
        ZoneOffset { zone: *self, offset }
    }
    fn offset_from_utc_datetime(&self, datetime: &NaiveDateTime) -> ZoneOffset {
        let offset = match self { Self::Named(zone) => zone.offset_from_utc_datetime(datetime).fix(), Self::Fixed(offset) => *offset };
        ZoneOffset { zone: *self, offset }
    }
}

fn invalid(timezone: &str) -> Anomaly { Anomaly::incorrect("xtdb/invalid-timezone", format!("Unknown time zone: {timezone}")) }

fn fixed_offset(input: &str) -> Option<FixedOffset> {
    if input == "Z" { return FixedOffset::east_opt(0); }
    let bytes = input.as_bytes();
    let sign = match bytes.first()? { b'+' => 1, b'-' => -1, _ => return None };
    let number = |start: usize, end: usize| -> Option<i32> {
        let digits = bytes.get(start..end)?;
        if !digits.iter().all(u8::is_ascii_digit) { return None; }
        Some(digits.iter().fold(0, |value, digit| value * 10 + i32::from(digit - b'0')))
    };
    let (hour, minute, second) = match bytes.len() {
        2 => (number(1, 2)?, 0, 0),
        3 => (number(1, 3)?, 0, 0),
        5 => (number(1, 3)?, number(3, 5)?, 0),
        6 if bytes[3] == b':' => (number(1, 3)?, number(4, 6)?, 0),
        7 => (number(1, 3)?, number(3, 5)?, number(5, 7)?),
        9 if bytes[3] == b':' && bytes[6] == b':' => (number(1, 3)?, number(4, 6)?, number(7, 9)?),
        _ => return None,
    };
    if hour > 18 || minute > 59 || second > 59 || (hour == 18 && (minute != 0 || second != 0)) { return None; }
    FixedOffset::east_opt(sign * (hour * 3600 + minute * 60 + second))
}

fn prefixed_offset(timezone: &str) -> Option<(&str, &str)> {
    for prefix in ["UTC", "GMT", "UT"] {
        if let Some(offset) = timezone.strip_prefix(prefix) {
            if offset.is_empty() || offset.starts_with('+') || offset.starts_with('-') { return Some((prefix, offset)); }
        }
    }
    None
}

pub(crate) fn zone(timezone: &str) -> Result<Zone> {
    if let Some((_, offset)) = prefixed_offset(timezone) {
        return fixed_offset(if offset.is_empty() { "Z" } else { offset }).map(Zone::Fixed).ok_or_else(|| invalid(timezone));
    }
    if timezone == "Z" || timezone.starts_with('+') || timezone.starts_with('-') {
        return fixed_offset(timezone).map(Zone::Fixed).ok_or_else(|| invalid(timezone));
    }
    timezone.parse().map(Zone::Named).map_err(|_| invalid(timezone))
}

/// Validates configured named zones and Java fixed-offset spellings, normalizing the latter for session output.
/// Invalid zones return `xtdb/invalid-timezone`.
pub fn normalize_timezone(timezone: &str) -> Result<String> {
    let parsed = zone(timezone)?;
    let Zone::Fixed(offset) = parsed else { return Ok(timezone.to_owned()); };
    let seconds = offset.local_minus_utc();
    let prefix = prefixed_offset(timezone).map_or("", |(prefix, _)| prefix);
    if seconds == 0 { return Ok(if prefix.is_empty() { "Z".into() } else { prefix.into() }); }
    let absolute = seconds.abs();
    let mut normalized = format!("{prefix}{}{:02}:{:02}", if seconds < 0 { '-' } else { '+' }, absolute / 3600, absolute / 60 % 60);
    if absolute % 60 != 0 { normalized.push_str(&format!(":{:02}", absolute % 60)); }
    Ok(normalized)
}
