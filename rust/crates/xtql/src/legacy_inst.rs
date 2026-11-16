use rtdb_types::{CivilDate, Result};

fn invalid() -> rtdb_types::Anomaly { crate::error("xtql/invalid-literal", "Invalid Clojure inst literal") }

pub(crate) fn parse(text: &str) -> Result<i64> {
    let bytes = text.as_bytes();
    let mut at = 0;
    let digits = |at: &mut usize, count: usize| -> Result<i64> {
        let part = bytes.get(*at..*at + count).ok_or_else(invalid)?;
        if !part.iter().all(u8::is_ascii_digit) { return Err(invalid()); }
        *at += count;
        Ok(part.iter().fold(0, |value, byte| value * 10 + i64::from(byte - b'0')))
    };
    let year = digits(&mut at, 4)? as i32;
    let (mut month, mut day, mut hour, mut minute, mut second, mut millis) = (1, 1, 0, 0, 0, 0);
    if bytes.get(at) == Some(&b'-') && bytes.get(at + 3) != Some(&b':') {
        at += 1; month = digits(&mut at, 2)? as u32;
        if bytes.get(at) == Some(&b'-') && bytes.get(at + 3) != Some(&b':') {
            at += 1; day = digits(&mut at, 2)? as u32;
            if bytes.get(at) == Some(&b'T') {
                at += 1; hour = digits(&mut at, 2)?;
                if bytes.get(at) == Some(&b':') {
                    at += 1; minute = digits(&mut at, 2)?;
                    if bytes.get(at) == Some(&b':') {
                        at += 1; second = digits(&mut at, 2)?;
                        if bytes.get(at) == Some(&b'.') {
                            at += 1;
                            let start = at;
                            while bytes.get(at).is_some_and(u8::is_ascii_digit) { at += 1; }
                            if start == at { return Err(invalid()); }
                            let fraction = &bytes[start..at.min(start + 3)];
                            millis = fraction.iter().fold(0i64, |value, byte| value * 10 + i64::from(byte - b'0')) * 10i64.pow(3 - fraction.len() as u32);
                        }
                    }
                }
            }
        }
    }
    let offset = match bytes.get(at) {
        None => 0,
        Some(b'Z') => { at += 1; 0 },
        Some(b'+' | b'-') => {
            let sign = if bytes[at] == b'-' { -1 } else { 1 };
            at += 1;
            let hours = digits(&mut at, 2)?;
            if bytes.get(at) != Some(&b':') { return Err(invalid()); }
            at += 1;
            let minutes = digits(&mut at, 2)?;
            if hours > 23 || minutes > 59 { return Err(invalid()); }
            sign * (hours * 3600 + minutes * 60)
        }
        _ => return Err(invalid()),
    };
    if at != bytes.len() || hour > 23 || minute > 59 || second > if minute == 59 { 60 } else { 59 } { return Err(invalid()); }
    let date = CivilDate::from_ymd(year, month, day).map_err(|_| invalid())?;
    // clojure.instant/construct-calendar uses GregorianCalendar's default 1582 cutover, after Gregorian field validation.
    let days = if (year, month, day) >= (1582, 10, 15) { date.epoch_day() } else {
        let adjust = (14 - i64::from(month)) / 12;
        let year = i64::from(year) + 4800 - adjust;
        let month = i64::from(month) + 12 * adjust - 3;
        i64::from(day) + (153 * month + 2) / 5 + 365 * year + year / 4 - 32_083 - 2_440_588
    };
    Ok((days * 86_400 + hour * 3600 + minute * 60 + second - offset) * 1_000_000 + millis * 1000)
}
