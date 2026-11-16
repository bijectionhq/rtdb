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

fn date_fields(text: &str) -> Result<(i32, u32, u32)> {
    let mut pieces = text.rsplitn(3, '-');
    let day = pieces.next().ok_or_else(invalid)?;
    let month = pieces.next().ok_or_else(invalid)?;
    let year = pieces.next().ok_or_else(invalid)?;
    let digits = year.strip_prefix(['+', '-']).unwrap_or(year);
    if day.len() != 2 || month.len() != 2 || digits.is_empty() || digits.len() > 10
        || !day.bytes().chain(month.bytes()).chain(digits.bytes()).all(|byte| byte.is_ascii_digit()) { return Err(invalid()); }
    Ok((year.parse().map_err(|_| invalid())?, month.parse().map_err(|_| invalid())?, day.parse().map_err(|_| invalid())?))
}

fn timestamp_date(text: &str) -> Result<CivilDate> {
    let year = text.rsplitn(3, '-').nth(2).ok_or_else(invalid)?;
    if year.strip_prefix(['+', '-']).unwrap_or(year).len() > 9 { return Err(invalid()); }
    let (year, month, day) = date_fields(text)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) { return Err(invalid()); }
    CivilDate::from_ymd(year, month, day.min(CivilDate::month_length(year, month))).map_err(|_| invalid())
}

fn timestamp_time(text: &str) -> Result<u64> {
    let bytes = text.as_bytes();
    if bytes.len() < 5 || bytes[2] != b':' || !bytes[..2].iter().chain(&bytes[3..5]).all(u8::is_ascii_digit) { return Err(invalid()); }
    if bytes.len() != 5 {
        if bytes.len() < 8 || bytes[5] != b':' || !bytes[6..8].iter().all(u8::is_ascii_digit) { return Err(invalid()); }
        if bytes.len() > 8 && (bytes[8] != b'.' || bytes.len() > 18 || !bytes[9..].iter().all(u8::is_ascii_digit)) { return Err(invalid()); }
    }
    let pair = |at: usize| u32::from(bytes[at] - b'0') * 10 + u32::from(bytes[at + 1] - b'0');
    let (hour, minute) = (pair(0), pair(3));
    let second = if bytes.len() >= 8 { pair(6) } else { 0 };
    let nanos = bytes.get(9..).unwrap_or_default().iter().fold(0u32, |nanos, byte| nanos * 10 + u32::from(byte - b'0')) * 10u32.pow(9 - bytes.len().saturating_sub(9) as u32);
    if hour == 24 && minute == 0 && second == 0 && nanos == 0 { return Ok(crate::NANOS_PER_DAY); }
    if hour > 23 || minute > 59 || second > 59 { return Err(invalid()); }
    Ok(u64::from(hour * 3600 + minute * 60 + second) * 1_000_000_000 + u64::from(nanos))
}

fn parse(text: &str) -> Result<ParsedTimestamp<'_>> {
    let (text, zone) = if text.ends_with(']') {
        let start = text.rfind('[').ok_or_else(invalid)?;
        let zone = &text[start + 1..text.len() - 1];
        if zone.is_empty() { return Err(invalid()); }
        (&text[..start], Some(zone))
    } else { (text, None) };
    let date_end = text.char_indices().skip(1).filter(|(_, ch)| *ch == '-').nth(1).map(|(at, _)| at + 3).ok_or_else(invalid)?;
    let day = timestamp_date(text.get(..date_end).ok_or_else(invalid)?)?;
    let rest = text.get(date_end..).ok_or_else(invalid)?;
    let has_clock = rest.starts_with(['T', ' ']);
    let (clock, offset_text) = if let Some(clock) = rest.strip_prefix('T').or_else(|| rest.strip_prefix(' ')) {
        let end = clock.find(['Z', '+', '-']).unwrap_or(clock.len());
        (timestamp_time(&clock[..end])?, &clock[end..])
    } else { (0, rest) };
    let offset = if offset_text.is_empty() { None } else { Some(offset(offset_text).ok_or_else(invalid)?) };
    let local = if clock == crate::NANOS_PER_DAY { CivilDateTime::midnight(day.plus_days(1).map_err(|_| invalid())?) }
        else { CivilDateTime::new(day, clock).map_err(|_| invalid())? };
    if let Some(zone) = zone { ZoneId::parse(zone).map_err(|_| invalid())?; }
    Ok(ParsedTimestamp { local, has_clock, zone, offset })
}

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
pub fn parse_sql_timestamp_instant(text: &str, default_zone: &str) -> Result<Instant> { parse(text)?.instant(default_zone) }

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
    let parsed = parse(text)?;
    let zoned = parsed.zone.is_some() || parsed.offset.is_some();
    let value = if zoned { parsed.instant("UTC")?.as_micros()?.0 } else { Instant::new(parsed.local.epoch_second(), parsed.local.nano())?.as_micros()?.0 };
    let timezone = if zoned { Some(parsed.zone.map(str::to_owned).unwrap_or_else(|| {
        let offset = parsed.offset.unwrap();
        offset.to_string()
    })) } else { None };
    Ok(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone })
}

/// Parses the SQL timestamp grammar without reducing its fractional precision.
/// Zoned casts require an explicit offset or region; local casts discard the parsed zone after resolving it.
/// Returns epoch nanoseconds for zoned casts and local-clock nanoseconds for local casts.
pub fn parse_sql_timestamp_cast(text: &str, zoned: bool) -> Result<i128> {
    let parsed = parse(text)?;
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
pub fn parse_sql_date_literal(text: &str) -> Result<Value> {
    Ok(Value::calendar_date(CivilDate::parse_iso(text).map_err(|_| invalid())?, crate::DateUnit::Day))
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
