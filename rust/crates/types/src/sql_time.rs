use crate::{Anomaly, CivilDate, CivilDateTime, FixedOffset, Instant, Result, TimeUnit, Value, ZoneId};

fn invalid() -> Anomaly { Anomaly::incorrect("xtdb/invalid-timestamp", "Invalid SQL temporal literal") }

struct ParsedTimestamp<'a> { local: CivilDateTime, has_clock: bool, zone: Option<&'a str>, offset: Option<FixedOffset> }

fn offset(text: &str) -> Option<FixedOffset> {
    if text == "Z" { return Some(FixedOffset::UTC); }
    let bytes = text.as_bytes();
    let sign = match bytes.first()? { b'+' => 1, b'-' => -1, _ => return None };
    if !matches!(bytes.len(), 3 | 6 | 9) { return None; }
    let pair = |at: usize| -> Option<i32> {
        let digits = bytes.get(at..at + 2)?;
        digits.iter().all(u8::is_ascii_digit).then(|| i32::from(digits[0] - b'0') * 10 + i32::from(digits[1] - b'0'))
    };
    let hours = pair(1)?;
    let minutes = if bytes.len() >= 6 && bytes[3] == b':' { pair(4)? } else if bytes.len() == 3 { 0 } else { return None; };
    let seconds = if bytes.len() == 9 && bytes[6] == b':' { pair(7)? } else if bytes.len() < 9 { 0 } else { return None; };
    if hours > 18 || minutes > 59 || seconds > 59 || hours == 18 && (minutes != 0 || seconds != 0) { return None; }
    FixedOffset::from_seconds(sign * (hours * 3600 + minutes * 60 + seconds)).ok()
}

enum TimestampParseError {
    Expected(usize),
    Trailing(usize),
    InvalidField { field: &'static str, range: &'static str, value: u32 },
    OffsetFields,
    OffsetRange,
    DateOverflow,
}

impl TimestampParseError {
    fn into_anomaly(self, source: &str) -> Anomaly {
        match self.java_message(source) {
            Some(message) => Anomaly::incorrect("xtdb/invalid-date-time", format!("invalid timestamp: {message}")).with_context("timestamp", source.into()),
            None => invalid(),
        }
    }

    /// The anomaly `xtdb.error` derives from the DateTimeParseException that `Time.asInstant` raises.
    fn into_date_time_parse(self, source: &str) -> Anomaly {
        match self.java_message(source) {
            Some(message) => Anomaly::incorrect("xtdb.error/date-time-parse", message).with_context("date-time", source.replace(' ', "T").into()),
            None => invalid(),
        }
    }

    fn java_message(self, source: &str) -> Option<String> {
        let mut text = source.replace(' ', "T");
        let mut utf16 = 0;
        if let Some((index, _)) = text.char_indices().find(|(_, c)| {
            utf16 += c.len_utf16();
            utf16 > 64
        }) {
            text.truncate(index);
            text.push_str("...");
        }
        let description = match self {
            Self::Expected(index) => format!(" at index {index}"),
            Self::Trailing(index) => format!(", unparsed text found at index {index}"),
            Self::InvalidField { field, range, value } =>
                format!(": Invalid value for {field} (valid values {range}): {value}"),
            Self::OffsetFields => ": Value out of range: Hour[0-23], Minute[0-59], Second[0-59]".into(),
            Self::OffsetRange => ": Zone offset not in valid range: -18:00 to +18:00".into(),
            Self::DateOverflow => return None,
        };
        Some(format!("Text '{text}' could not be parsed{description}"))
    }
}

fn digits(bytes: &[u8], start: usize) -> Option<u32> {
    let pair = bytes.get(start..start + 2)?;
    if !pair.iter().all(u8::is_ascii_digit) { return None; }
    Some(u32::from(pair[0] - b'0') * 10 + u32::from(pair[1] - b'0'))
}

fn optional_pair(bytes: &[u8], end: &mut usize) -> Option<u32> {
    if bytes.get(*end) != Some(&b':') { return None; }
    let value = digits(bytes, *end + 1)?;
    *end += 3;
    Some(value)
}

fn clock_prefix(bytes: &[u8], start: usize) -> Option<([u32; 4], usize)> {
    if !matches!(bytes.get(start), Some(b'T' | b' ')) { return None; }
    let hour = digits(bytes, start + 1)?;
    let mut end = start + 3;
    let minute = optional_pair(bytes, &mut end)?;
    let mut nanos = 0;
    let second = optional_pair(bytes, &mut end);
    if second.is_some() && bytes.get(end) == Some(&b'.') {
        end += 1;
        let fraction = bytes[end..].iter().take(9).take_while(|byte| byte.is_ascii_digit());
        let width = fraction.clone().count();
        nanos = fraction.fold(0, |value, byte| value * 10 + u32::from(byte - b'0')) * 10_u32.pow(9 - width as u32);
        end += width;
    }
    Some(([hour, minute, second.unwrap_or(0), nanos], end))
}

fn timestamp_offset(bytes: &[u8], start: usize) -> std::result::Result<(Option<FixedOffset>, usize), TimestampParseError> {
    let sign = match bytes.get(start) {
        Some(b'Z') => return Ok((Some(FixedOffset::UTC), start + 1)),
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return Ok((None, start)),
    };
    let Some(hour) = digits(bytes, start + 1) else { return Ok((None, start)); };
    let mut end = start + 3;
    let minute = optional_pair(bytes, &mut end);
    let second = minute.and_then(|_| optional_pair(bytes, &mut end)).unwrap_or(0);
    let minute = minute.unwrap_or(0);
    if hour > 23 || minute > 59 || second > 59 { return Err(TimestampParseError::OffsetFields); }
    let seconds = sign * (hour * 3600 + minute * 60 + second) as i32;
    let offset = FixedOffset::from_seconds(seconds).map_err(|_| TimestampParseError::OffsetRange)?;
    Ok((Some(offset), end))
}

fn valid_field(value: u32, field: &'static str, range: &'static str, bounds: std::ops::RangeInclusive<u32>) -> std::result::Result<(), TimestampParseError> {
    if bounds.contains(&value) { Ok(()) } else { Err(TimestampParseError::InvalidField { field, range, value }) }
}

fn scan_timestamp(text: &str) -> std::result::Result<ParsedTimestamp<'_>, TimestampParseError> {
    use TimestampParseError::{DateOverflow, Expected, Trailing};
    let bytes = text.as_bytes();
    let signed = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let digits_end = signed + bytes[signed..].iter().take(9).take_while(|byte| byte.is_ascii_digit()).count();
    if digits_end == signed { return Err(Expected(signed)); }
    if bytes.get(digits_end) != Some(&b'-') { return Err(Expected(digits_end)); }
    let year: i32 = text[..digits_end].parse().map_err(|_| Expected(signed))?;
    let month_start = digits_end + 1;
    let month = digits(bytes, month_start).ok_or(Expected(month_start))?;
    let day_start = month_start + 3;
    if bytes.get(day_start - 1) != Some(&b'-') { return Err(Expected(day_start - 1)); }
    let day = digits(bytes, day_start).ok_or(Expected(day_start))?;
    let date_end = day_start + 2;
    let clock = clock_prefix(bytes, date_end);
    let (offset, mut end) = timestamp_offset(bytes, clock.map_or(date_end, |(_, end)| end))?;
    let mut zone = None;
    if bytes.get(end) == Some(&b'[')
        && let Some(relative) = text[end + 1..].find(']')
    {
        let name = &text[end + 1..end + 1 + relative];
        if ZoneId::parse(name).is_ok() {
            zone = Some(name);
            end += relative + 2;
        }
    }
    if end != bytes.len() { return Err(Trailing(text[..end].encode_utf16().count())); }

    valid_field(month, "MonthOfYear", "1 - 12", 1..=12)?;
    valid_field(day, "DayOfMonth", "1 - 28/31", 1..=31)?;
    let mut date = CivilDate::from_ymd(year, month, day.min(CivilDate::month_length(year, month))).map_err(|_| DateOverflow)?;
    let clock_nanos = match clock {
        None => 0,
        Some(([24, 0, 0, 0], _)) => {
            date = date.plus_days(1).map_err(|_| DateOverflow)?;
            0
        }
        Some(([hour, minute, second, nanos], _)) => {
            valid_field(hour, "HourOfDay", "0 - 23", 0..=23)?;
            valid_field(minute, "MinuteOfHour", "0 - 59", 0..=59)?;
            valid_field(second, "SecondOfMinute", "0 - 59", 0..=59)?;
            u64::from(hour * 3600 + minute * 60 + second) * 1_000_000_000 + u64::from(nanos)
        }
    };
    let local = CivilDateTime::new(date, clock_nanos).map_err(|_| DateOverflow)?;
    Ok(ParsedTimestamp { local, has_clock: clock.is_some(), zone, offset })
}

fn parse(text: &str) -> Result<ParsedTimestamp<'_>> { scan_timestamp(text).map_err(|_| invalid()) }

impl ParsedTimestamp<'_> {
    fn instant(&self, default_zone: &str) -> Result<Instant> {
        if self.has_clock || self.zone.is_none() {
            if let Some(offset) = self.offset { return Instant::new(self.local.epoch_second() - i64::from(offset.seconds()), self.local.nano()); }
        }
        ZoneId::parse(self.zone.unwrap_or(default_zone)).map_err(|_| invalid())?.resolve_local(self.local, None)
    }
}

/// Resolves the SQL timestamp grammar to an instant, using `default_zone` only when the text has no offset or region.
/// SMART date/time resolution and nanosecond precision are preserved.
pub fn parse_sql_timestamp_instant(text: &str, default_zone: &str) -> Result<Instant> {
    scan_timestamp(text).map_err(|error| error.into_date_time_parse(text))?.instant(default_zone)
}

/// Extracts local date/time fields from the SQL timestamp grammar, without applying an offset or region.
/// A clock is required, matching `Time.asLocalDateTime`; SMART date/time resolution is retained.
pub fn parse_sql_local_datetime(text: &str) -> Result<CivilDateTime> {
    let parsed = parse(text)?;
    if !parsed.has_clock { return Err(invalid()); }
    Ok(parsed.local)
}

/// Resolves a SQL timestamp carrying an explicit offset or region and returns its instant and normalized zone.
/// A missing clock uses midnight; a missing zone returns an error.
pub fn parse_sql_zoned_datetime(text: &str) -> Result<(Instant, ZoneId)> {
    let parsed = parse(text)?;
    let zone = if let Some(zone) = parsed.zone { ZoneId::parse(zone)? }
        else if let Some(offset) = parsed.offset { ZoneId::parse(&offset.to_string())? }
        else { return Err(invalid()); };
    Ok((parsed.instant("UTC")?, zone))
}

/// Parses the SQL timestamp literal before execution; a missing zone preserves local timestamp semantics.
/// Offset and region suffixes follow the pinned SQL_TIMESTAMP_FORMATTER, with microsecond value precision.
/// SMART resolution clamps day 29–31 to the month's end and carries exact 24:00 into the next date.
pub fn parse_sql_timestamp_literal(text: &str) -> Result<Value> {
    let (value, timezone) = parse_sql_timestamp_micros(text)?;
    Ok(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone })
}

/// Parses a SQL timestamp literal into epoch microseconds and its optional zone.
pub fn parse_sql_timestamp_micros(text: &str) -> Result<(i64, Option<String>)> {
    let parsed = parse(text)?;
    let zoned = parsed.zone.is_some() || parsed.offset.is_some();
    let value = if zoned { parsed.instant("UTC")?.as_micros()?.0 } else { Instant::new(parsed.local.epoch_second(), parsed.local.nano())?.as_micros()?.0 };
    let timezone = if zoned { Some(parsed.zone.map(str::to_owned).unwrap_or_else(|| {
        let offset = parsed.offset.unwrap();
        offset.to_string()
    })) } else { None };
    Ok((value, timezone))
}

/// Parses the SQL timestamp grammar without reducing its fractional precision.
/// Zoned casts require an explicit offset or region; local casts discard the parsed zone after resolving it.
/// Returns epoch nanoseconds for zoned casts and local-clock nanoseconds for local casts.
/// Invalid source text retains the original input and the position or field rejected by the timestamp grammar.
pub fn parse_sql_timestamp_cast(text: &str, zoned: bool) -> Result<i128> {
    let parsed = scan_timestamp(text).map_err(|error| error.into_anomaly(text))?;
    let has_zone = parsed.zone.is_some() || parsed.offset.is_some();
    if zoned && !has_zone { return Err(invalid()); }
    if zoned {
        let instant = parsed.instant("UTC")?;
        return Ok(i128::from(instant.seconds()) * 1_000_000_000 + i128::from(instant.nanos()));
    }
    let local = if let Some(name) = parsed.zone {
        let instant = parsed.instant("UTC")?;
        ZoneId::parse(name).map_err(|_| invalid())?.local_datetime(instant)?
    } else { parsed.local };
    Ok(local.epoch_nanos())
}

/// Parses a SQL DATE literal at preparation, returning days since the Unix epoch.
/// A well-formed spelling with an out-of-range field reports `LocalDate.parse`'s STRICT resolution message.
pub fn parse_sql_date_literal(text: &str) -> Result<Value> {
    CivilDate::parse_iso(text).map(|date| Value::calendar_date(date, crate::DateUnit::Day)).map_err(|_| match date_field_message(text) {
        Some(message) => Anomaly::incorrect("xtdb.error/date-time-parse", format!("Text '{text}' could not be parsed: {message}")),
        None => invalid(),
    })
}

fn date_field_message(text: &str) -> Option<String> {
    const MONTHS: [&str; 12] = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER"];
    let mut parts = text.rsplitn(3, '-');
    let (day, month, year) = (parts.next()?, parts.next()?, parts.next()?);
    if day.len() != 2 || month.len() != 2 { return None; }
    let (year, month, day) = (year.parse::<i32>().ok()?, month.parse::<u32>().ok()?, day.parse::<u32>().ok()?);
    if !(1..=12).contains(&month) { return Some(format!("Invalid value for MonthOfYear (valid values 1 - 12): {month}")); }
    if !(1..=31).contains(&day) { return Some(format!("Invalid value for DayOfMonth (valid values 1 - 28/31): {day}")); }
    if day <= CivilDate::month_length(year, month) { return None; }
    Some(if month == 2 && day == 29 { format!("Invalid date 'February 29' as '{year}' is not a leap year") }
        else { format!("Invalid date '{} {day}'", MONTHS[month as usize - 1]) })
}

/// Validates SQL TIME spelling, including the planner's offset and fractional-second grammar.
/// This does not assign an Arrow representation to OffsetTime values.
pub fn validate_sql_time_literal(text: &str) -> Result<()> {
    let offset_at = text.find(['+', '-']).unwrap_or(text.len());
    if offset_at != text.len() && (text.len() - offset_at != 6 || offset(&text[offset_at..]).is_none()) { return Err(invalid()); }
    let (clock, fraction) = match text[..offset_at].split_once('.') { Some((clock, fraction)) => (clock, Some(fraction)), None => (&text[..offset_at], None) };
    let mut fields = clock.split(':');
    let mut next = || -> Result<u32> {
        let value = fields.next().ok_or_else(invalid)?;
        if value.is_empty() || value.len() > 2 || !value.bytes().all(|byte| byte.is_ascii_digit()) { return Err(invalid()); }
        value.parse().map_err(|_| invalid())
    };
    let (hours, minutes, seconds) = (next()?, next()?, next()?);
    if fields.next().is_some() { return Err(invalid()); }
    let nanos = if let Some(fraction) = fraction {
        if fraction.is_empty() || !fraction.bytes().all(|byte| byte.is_ascii_digit()) { return Err(invalid()); }
        let value = fraction.parse::<i64>().map_err(|_| invalid())?;
        if fraction.len() > 9 { 0 } else { value * 10i64.pow((9 - fraction.len()) as u32) }
    } else { 0 };
    if hours > 23 || minutes > 59 || seconds > 59 || !(0..1_000_000_000).contains(&nanos) { return Err(invalid()); }
    Ok(())
}

/// Resolves a query CLOCK_TIME string with the pinned Time.asInstant date fallback.
/// A timestamp without a zone uses its date at midnight in the session zone.
/// Explicit-zone clocks preserve nanoseconds; invalid timestamp or zone syntax returns an anomaly.
pub fn parse_query_clock(text: &str, default_zone: &str) -> Result<Instant> {
    let parsed = parse(text)?;
    if parsed.zone.is_none() && parsed.offset.is_none() {
        return ZoneId::parse(default_zone)?.resolve_local(CivilDateTime::midnight(parsed.local.date()), None);
    }
    parsed.instant(default_zone)
}
