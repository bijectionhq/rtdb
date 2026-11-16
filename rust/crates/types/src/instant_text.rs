use crate::{Anomaly, CivilDate, FixedOffset, Instant, Result};

const MIN_SECONDS: i64 = -31_557_014_167_219_200;
const MAX_SECONDS: i64 = 31_556_889_864_403_199;
const DAYS_PER_10000_YEARS: i64 = 3_652_425;
const SECONDS_PER_10000_YEARS: i64 = DAYS_PER_10000_YEARS * 86_400;

fn invalid() -> Anomaly { Anomaly::incorrect("xtdb/invalid-instant", "Invalid ISO instant or instant outside Java's supported range") }

impl Instant {
    /// Parses `ISO_INSTANT`, requiring seconds and an offset, with up to nine fractional digits.
    /// Exact 24:00 carries into the next date and 23:59:60 resolves to 23:59:59, as in Java's instant parser.
    /// The result must be within Java Instant's inclusive second range, −31,557,014,167,219,200 through +31,556,889,864,403,199.
    pub fn parse_iso(text: &str) -> Result<Self> {
        let (date, clock) = text.split_once(['T', 't']).ok_or_else(invalid)?;
        let mut parts = date.rsplitn(3, '-');
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
        let year: i64 = year.parse().map_err(|_| invalid())?;
        let date = CivilDate::from_ymd((year as i32) % 10_000, month.parse().map_err(|_| invalid())?, day.parse().map_err(|_| invalid())?).map_err(|_| invalid())?;
        let offset_at = clock.find(['Z', 'z', '+', '-']).ok_or_else(invalid)?;
        let (clock, offset) = clock.split_at(offset_at);
        let offset = match offset {
            "Z" | "z" => FixedOffset::UTC,
            _ if matches!(offset.len(), 6 | 9) && offset.as_bytes().get(3) == Some(&b':') => FixedOffset::parse(offset).map_err(|_| invalid())?,
            _ => return Err(invalid()),
        };
        let bytes = clock.as_bytes();
        if bytes.len() < 8 || bytes[2] != b':' || bytes[5] != b':'
            || !bytes[..2].iter().chain(&bytes[3..5]).chain(&bytes[6..8]).all(u8::is_ascii_digit)
            || bytes.len() > 8 && (bytes[8] != b'.' || bytes.len() > 18 || !bytes[9..].iter().all(u8::is_ascii_digit)) { return Err(invalid()); }
        let pair = |at: usize| i64::from(bytes[at] - b'0') * 10 + i64::from(bytes[at + 1] - b'0');
        let (hour, minute, mut second) = (pair(0), pair(3), pair(6));
        let fraction = bytes.get(9..).unwrap_or_default();
        let nanos = fraction.iter().fold(0u32, |value, byte| value * 10 + u32::from(byte - b'0')) * 10u32.pow(9 - fraction.len() as u32);
        if hour == 23 && minute == 59 && second == 60 { second = 59; }
        if minute > 59 || second > 59 || hour > 24 || hour == 24 && (minute != 0 || second != 0 || nanos != 0) { return Err(invalid()); }
        // ISO_INSTANT uses 10,000-year cycles independently of LocalDate's year bound (OpenJDK InstantPrinterParser).
        let seconds = date.epoch_day() * 86_400 + hour * 3600 + minute * 60 + second - i64::from(offset.seconds());
        let seconds = seconds.checked_add((year / 10_000).checked_mul(SECONDS_PER_10000_YEARS).ok_or_else(invalid)?).ok_or_else(invalid)?;
        if !(MIN_SECONDS..=MAX_SECONDS).contains(&seconds) { return Err(invalid()); }
        Self::new(seconds, nanos)
    }

    /// Formats `ISO_INSTANT` in UTC with seconds and fractional groups of three, six or nine digits.
    /// Values outside Java Instant's second range return an error; the returned string is caller-owned.
    pub fn to_iso_string(self) -> Result<String> {
        if !(MIN_SECONDS..=MAX_SECONDS).contains(&self.seconds()) { return Err(invalid()); }
        let zero_day = self.seconds().div_euclid(86_400) + 719_528;
        let cycle = zero_day.div_euclid(DAYS_PER_10000_YEARS);
        let date = CivilDate::from_epoch_day(zero_day.rem_euclid(DAYS_PER_10000_YEARS) - 719_528)?;
        let (year, month, day) = date.ymd();
        let year = i64::from(year) + cycle * 10_000;
        let year = if year < 0 { format!("-{:04}", -year) } else if year > 9999 { format!("+{year}") } else { format!("{year:04}") };
        let second = self.seconds().rem_euclid(86_400);
        let mut text = format!("{year}-{month:02}-{day:02}T{:02}:{:02}:{:02}", second / 3600, second / 60 % 60, second % 60);
        if self.nanos() != 0 {
            let digits = if self.nanos() % 1_000_000 == 0 { 3 } else if self.nanos() % 1000 == 0 { 6 } else { 9 };
            text.push('.');
            text.push_str(&format!("{:09}", self.nanos())[..digits]);
        }
        text.push('Z');
        Ok(text)
    }
}
