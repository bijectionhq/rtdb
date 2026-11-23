use std::fmt;
use crate::{Anomaly, Result};

pub const MIN_CALENDAR_YEAR: i32 = -999_999_999;
pub const MAX_CALENDAR_YEAR: i32 = 999_999_999;
pub const MIN_EPOCH_DAY: i64 = -365_243_219_162;
pub const MAX_EPOCH_DAY: i64 = 365_241_780_471;
pub const NANOS_PER_DAY: u64 = 86_400_000_000_000;

fn invalid() -> Anomaly { Anomaly::incorrect("xtdb/invalid-date", "Date is outside the ISO proleptic Gregorian calendar") }
fn overflow() -> Anomaly { Anomaly::incorrect("xtdb/time-overflow", "Temporal value exceeds its calendar representation") }

/// An ISO proleptic Gregorian date in Java's inclusive year range −999,999,999 through +999,999,999.
/// Arrow storage narrowing belongs to the caller and is not performed by this value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CivilDate { epoch_day: i64 }

impl CivilDate {
    pub const MIN: Self = Self { epoch_day: MIN_EPOCH_DAY };
    pub const MAX: Self = Self { epoch_day: MAX_EPOCH_DAY };
    /// Validates an epoch-day count against the calendar range.
    pub fn from_epoch_day(epoch_day: i64) -> Result<Self> {
        if !(MIN_EPOCH_DAY..=MAX_EPOCH_DAY).contains(&epoch_day) { return Err(invalid()); }
        Ok(Self { epoch_day })
    }

    /// Validates a year, one-based month and one-based day without SMART clamping.
    pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self> {
        if !(MIN_CALENDAR_YEAR..=MAX_CALENDAR_YEAR).contains(&year) || !(1..=12).contains(&month)
            || day == 0 || day > Self::month_length(year, month) { return Err(invalid()); }
        let year = i64::from(year) - i64::from(month <= 2);
        let era = year.div_euclid(400);
        let year_of_era = year.rem_euclid(400);
        let month = i64::from(month) + if month > 2 { -3 } else { 9 };
        let day_of_year = (153 * month + 2) / 5 + i64::from(day) - 1;
        Ok(Self { epoch_day: era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468 })
    }

    /// Parses the strict `ISO_LOCAL_DATE` spelling, including signed extended years.
    pub fn parse_iso(text: &str) -> Result<Self> {
        let mut parts = text.rsplitn(3, '-');
        let day = parts.next().ok_or_else(invalid)?;
        let month = parts.next().ok_or_else(invalid)?;
        let year = parts.next().ok_or_else(invalid)?;
        let digits = year.strip_prefix(['+', '-']).unwrap_or(year);
        if day.len() != 2 || month.len() != 2 || !(4..=10).contains(&digits.len())
            || !day.bytes().chain(month.bytes()).chain(digits.bytes()).all(|byte| byte.is_ascii_digit()) { return Err(invalid()); }
        match year.as_bytes().first() {
            Some(b'+') if digits.len() <= 4 => return Err(invalid()),
            Some(b'-') if digits.bytes().all(|byte| byte == b'0') => return Err(invalid()),
            Some(b'+' | b'-') => (),
            _ if digits.len() != 4 => return Err(invalid()),
            _ => (),
        }
        Self::from_ymd(year.parse().map_err(|_| invalid())?, month.parse().map_err(|_| invalid())?, day.parse().map_err(|_| invalid())?)
    }

    pub fn epoch_day(self) -> i64 { self.epoch_day }

    /// Returns `(year, one-based month, one-based day)` without allocation.
    pub fn ymd(self) -> (i32, u32, u32) {
        // Gregorian civil-day conversion: https://howardhinnant.github.io/date_algorithms.html#civil_from_days.
        let shifted = self.epoch_day + 719_468;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted.rem_euclid(146_097);
        let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month + 2) / 5 + 1;
        let month = month + if month < 10 { 3 } else { -9 };
        ((year + i64::from(month <= 2)) as i32, month as u32, day as u32)
    }
    pub fn year(self) -> i32 { self.ymd().0 }
    pub fn month(self) -> u32 { self.ymd().1 }
    pub fn day(self) -> u32 { self.ymd().2 }
    pub fn is_leap_year(year: i32) -> bool { year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) }

    /// Returns the month's length, or zero for a month outside 1 through 12.
    pub fn month_length(year: i32, month: u32) -> u32 {
        match month { 2 => if Self::is_leap_year(year) { 29 } else { 28 }, 4 | 6 | 9 | 11 => 30, 1 | 3 | 5 | 7 | 8 | 10 | 12 => 31, _ => 0 }
    }

    pub fn day_of_year(self) -> u32 {
        let (year, month, day) = self.ymd();
        (1..month).map(|month| Self::month_length(year, month)).sum::<u32>() + day
    }

    /// Returns Monday=1 through Sunday=7.
    pub fn iso_day_of_week(self) -> u32 { (self.epoch_day + 3).rem_euclid(7) as u32 + 1 }

    /// Adds calendar days and fails when the resulting date leaves the supported year range.
    pub fn plus_days(self, days: i64) -> Result<Self> { Self::from_epoch_day(self.epoch_day.checked_add(days).ok_or_else(overflow)?) }

    /// Adds calendar months, clamping the day to the target month's last day.
    /// Overflow and years outside the supported range return an error.
    pub fn plus_months(self, months: i64) -> Result<Self> {
        let (year, month, day) = self.ymd();
        let target = (i64::from(year) * 12 + i64::from(month) - 1).checked_add(months).ok_or_else(overflow)?;
        let year = i32::try_from(target.div_euclid(12)).map_err(|_| overflow())?;
        let month = target.rem_euclid(12) as u32 + 1;
        Self::from_ymd(year, month, day.min(Self::month_length(year, month)))
    }
}

impl fmt::Display for CivilDate {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = self.ymd();
        if year < 0 { write!(output, "-{:04}", -i64::from(year))?; }
        else if year > 9999 { write!(output, "+{year}")?; }
        else { write!(output, "{year:04}")?; }
        write!(output, "-{month:02}-{day:02}")
    }
}

impl serde::Serialize for CivilDate {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> { serializer.collect_str(self) }
}
impl<'de> serde::Deserialize<'de> for CivilDate {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::parse_iso(&text).map_err(serde::de::Error::custom)
    }
}

/// The Arrow date unit selected for a calendar object's eventual vector admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DateUnit { Day, Millisecond }

/// A validated calendar object retaining its requested date unit before physical vector admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CalendarDate { date: CivilDate, unit: DateUnit }

impl CalendarDate {
    pub fn new(date: CivilDate, unit: DateUnit) -> Self { Self { date, unit } }
    pub fn date(self) -> CivilDate { self.date }
    pub fn unit(self) -> DateUnit { self.unit }

    pub(crate) fn physical_value(self) -> Option<crate::Value> {
        let days = self.date.epoch_day();
        match self.unit {
            DateUnit::Day => i32::try_from(days).ok().map(crate::Value::DateDay),
            DateUnit::Millisecond => days.checked_mul(crate::java_date::MILLIS_PER_DAY).map(crate::Value::DateMilli),
        }
    }

    pub fn object_value(self) -> Result<crate::Value> {
        self.physical_value().ok_or_else(|| Anomaly::incorrect("xtdb/date-out-of-range", format!("Date {} is outside the range of its Arrow date type", self.date)))
    }

    pub fn expression_value(self) -> Result<crate::Value> {
        self.physical_value().ok_or_else(|| Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error"))
    }
}

/// A validated local date and nanosecond clock reading, with no timezone or timeline identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CivilDateTime { date: CivilDate, nano_of_day: u64 }

impl CivilDateTime {
    /// Accepts nanoseconds since midnight in `[0, 86_400_000_000_000)`.
    pub fn new(date: CivilDate, nano_of_day: u64) -> Result<Self> {
        if nano_of_day >= NANOS_PER_DAY { return Err(Anomaly::incorrect("xtdb/invalid-time", "Time is outside a day")); }
        Ok(Self { date, nano_of_day })
    }
    pub fn midnight(date: CivilDate) -> Self { Self { date, nano_of_day: 0 } }

    /// Parses `ISO_LOCAL_DATE_TIME`, requiring a date, `T` separator and local clock without an offset.
    pub fn parse_iso(text: &str) -> Result<Self> {
        let (date, time) = text.split_once(['T', 't']).ok_or_else(invalid)?;
        Self::new(CivilDate::parse_iso(date)?, parse_iso_local_time(time)?)
    }

    /// Interprets seconds from the Unix epoch as a local-clock count, with a nanosecond component below one second.
    /// Calendar-range violations return an error.
    pub fn from_epoch_second(seconds: i64, nanos: u32) -> Result<Self> {
        if nanos >= 1_000_000_000 { return Err(Anomaly::incorrect("xtdb/invalid-time", "Nanoseconds must be within one second")); }
        let date = CivilDate::from_epoch_day(seconds.div_euclid(86_400))?;
        Ok(Self { date, nano_of_day: seconds.rem_euclid(86_400) as u64 * 1_000_000_000 + u64::from(nanos) })
    }
    /// Interprets nanoseconds from the Unix epoch as a local-clock count.
    pub fn from_epoch_nanos(nanos: i128) -> Result<Self> {
        Self::from_epoch_second(i64::try_from(nanos.div_euclid(1_000_000_000)).map_err(|_| overflow())?, nanos.rem_euclid(1_000_000_000) as u32)
    }
    pub fn date(self) -> CivilDate { self.date }
    pub fn nano_of_day(self) -> u64 { self.nano_of_day }
    pub fn nano(self) -> u32 { (self.nano_of_day % 1_000_000_000) as u32 }
    pub fn second_of_day(self) -> u32 { (self.nano_of_day / 1_000_000_000) as u32 }
    pub fn epoch_second(self) -> i64 { self.date.epoch_day * 86_400 + i64::from(self.second_of_day()) }
    pub fn epoch_nanos(self) -> i128 { i128::from(self.epoch_second()) * 1_000_000_000 + i128::from(self.nano()) }
}

/// Parses `ISO_LOCAL_TIME` as nanoseconds since midnight.
/// Hours and minutes have two digits; seconds are optional and may carry up to nine fractional digits.
/// Values outside one day, offsets and leap seconds return an error.
pub fn parse_iso_local_time(text: &str) -> Result<u64> {
    let bytes = text.as_bytes();
    let bad = || Anomaly::incorrect("xtdb/invalid-time", "Invalid ISO local time");
    if bytes.len() < 5 || bytes[2] != b':' || !bytes[..2].iter().chain(&bytes[3..5]).all(u8::is_ascii_digit) { return Err(bad()); }
    if bytes.len() != 5 {
        if bytes.len() < 8 || bytes[5] != b':' || !bytes[6..8].iter().all(u8::is_ascii_digit) { return Err(bad()); }
        if bytes.len() > 8 && (bytes[8] != b'.' || bytes.len() > 18 || !bytes[9..].iter().all(u8::is_ascii_digit)) { return Err(bad()); }
    }
    let pair = |at: usize| u32::from(bytes[at] - b'0') * 10 + u32::from(bytes[at + 1] - b'0');
    let (hour, minute) = (pair(0), pair(3));
    let second = if bytes.len() >= 8 { pair(6) } else { 0 };
    if hour > 23 || minute > 59 || second > 59 { return Err(bad()); }
    let fraction = bytes.get(9..).unwrap_or_default();
    let nanos = fraction.iter().fold(0u64, |value, byte| value * 10 + u64::from(byte - b'0')) * 10u64.pow(9 - fraction.len() as u32);
    Ok(u64::from(hour * 3600 + minute * 60 + second) * 1_000_000_000 + nanos)
}
