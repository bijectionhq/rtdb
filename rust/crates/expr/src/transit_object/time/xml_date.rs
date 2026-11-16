use num_bigint::{BigInt,Sign};
use num_traits::{Signed,ToPrimitive,Zero};
use rtdb_types::Result;
use super::{invalid,pattern};

struct XmlDate { year: Option<BigInt>,month: Option<u32>,day: Option<u32>,hour: u32,minute: u32,second: u32,millis: u32 }

pub(super) fn parse(text: &str) -> Result<i64> {
    let text = text.trim_matches([' ','\t','\r','\n']);
    let date = r"(?P<year>-?[0-9]{4,})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})";
    let clock = r"(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):(?P<second>[0-9]{2})(?:\.(?P<fraction>[0-9]+))?";
    let body = if text.contains('T') { format!("{date}T{clock}") }
        else if text.as_bytes().get(2) == Some(&b':') { clock.into() }
        else if text.starts_with("---") { r"---(?P<day>[0-9]{2})".into() }
        else if text.starts_with("--") {
            if matches!(text.len(),4 | 5 | 10) { r"--(?P<month>[0-9]{2})".into() } else { r"--(?P<month>[0-9]{2})-(?P<day>[0-9]{2})".into() }
        } else {
            let length = if text.contains(':') { text.len().checked_sub(6).ok_or_else(invalid)? } else { text.len() };
            let separators = text.get(1..length).ok_or_else(invalid)?.bytes().filter(|byte|*byte == b'-').count();
            match separators { 0 => r"(?P<year>-?[0-9]{4,})".into(),1 => r"(?P<year>-?[0-9]{4,})-(?P<month>[0-9]{2})".into(),_ => date.into() }
        };
    let expression = pattern(&format!(r"^{body}(?:Z|[+-](?P<offset_hour>[0-9]{{2}}):(?P<offset_minute>[0-9]{{2}}))?$"))?;
    let captures = expression.captures(text).ok_or_else(invalid)?;
    let field = |name| -> Result<Option<u32>> { captures.name(name).map(|value|value.as_str().parse().map_err(|_|invalid())).transpose() };
    let offset = field("offset_hour")?.unwrap_or(0) * 60 + field("offset_minute")?.unwrap_or(0);
    if offset > 14 * 60 { return Err(invalid()); }
    let year = captures.name("year").map(|value|value.as_str().parse::<BigInt>().map_err(|_|invalid())).transpose()?;
    let fraction = captures.name("fraction").map_or("",|value|value.as_str());
    let millis = fraction.bytes().take(3).chain(std::iter::repeat(b'0')).take(3).fold(0,|millis,byte|millis * 10 + u32::from(byte - b'0'));
    let mut value = XmlDate { year,month: field("month")?,day: field("day")?,hour: field("hour")?.unwrap_or(0),minute: field("minute")?.unwrap_or(0),second: field("second")?.unwrap_or(0),millis };
    if value.month.is_some_and(|month|!(1..=12).contains(&month)) || value.day.is_some_and(|day|!(1..=31).contains(&day)) || value.hour > 24 || value.minute > 59 || value.second > 60 { return Err(invalid()); }
    if value.hour == 24 {
        if value.minute != 0 || value.second != 0 { return Err(invalid()); }
        value.hour = 0;
        if value.day.is_some() { value.advance_date(); }
    }
    if value.year.as_ref().is_some_and(BigInt::is_zero) { return Err(invalid()); }
    if let (Some(month),Some(day)) = (value.month,value.day) {
        if day > month_length(value.year.as_ref(),month) { return Err(invalid()); }
    }
    Ok(value.millis())
}

impl XmlDate {
    fn advance_date(&mut self) {
        let month = self.month.unwrap_or(1);
        let maximum = month_length(self.year.as_ref(),month);
        let day = self.day.unwrap_or(1).min(maximum) + 1;
        if day <= maximum { self.day = Some(day);return; }
        self.day = Some(1);
        if month < 12 { if self.month.is_some() { self.month = Some(month + 1); } }
        else {
            if self.month.is_some() { self.month = Some(1); }
            if let Some(year) = &mut self.year { *year += 1; }
        }
    }
    fn millis(self) -> i64 {
        let year = self.year.as_ref().map_or(1970,|year| {
            let magnitude = year.abs().to_u32_digits().1.first().copied().unwrap_or(0) as i32;
            if year.sign() == Sign::Minus { 1i32.wrapping_sub(magnitude) } else { magnitude }
        });
        let (month,day) = (self.month.unwrap_or(1),self.day.unwrap_or(1));
        let clock = i64::from(((self.hour * 60 + self.minute) * 60 + self.second) * 1000 + self.millis);
        let days = clock / 86_400_000;
        let gregorian = gregorian_day(year,month,day) + days;
        let epoch_day = if gregorian >= i64::MIN.div_euclid(86_400_000) { gregorian } else { julian_day(year,month,day) + days };
        epoch_day.wrapping_mul(86_400_000).wrapping_add(clock % 86_400_000)
    }
}

fn month_length(year: Option<&BigInt>,month: u32) -> u32 {
    let leap = year.map_or(true,|year| { let cycle = (year % BigInt::from(400)).to_i32().unwrap();cycle % 4 == 0 && (cycle % 100 != 0 || cycle == 0) });
    match month { 2 => if leap { 29 } else { 28 },4 | 6 | 9 | 11 => 30,_ => 31 }
}
fn gregorian_day(year: i32,month: u32,day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);let year = year - era * 400;
    let month = i64::from(month) + if month > 2 { -3 } else { 9 };
    era * 146_097 + year * 365 + year / 4 - year / 100 + (153 * month + 2) / 5 + i64::from(day) - 1 - 719_468
}
fn julian_day(year: i32,month: u32,day: u32) -> i64 {
    let previous = i64::from(year) - 1;
    let days_before_month = [0,31,59,90,120,151,181,212,243,273,304,334][month as usize - 1];
    365 * previous + previous.div_euclid(4) + days_before_month + i64::from(month > 2 && year % 4 == 0) + i64::from(day) - 719_165
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verbose_dates_replace_the_lexical_zone_before_resolving_calendar_fields() {
        for text in ["1970-01-01T00:00:00Z","1970-01-01T00:00:00+12:00","1970-01-01T00:00:00-00:99","1970-01-01T00:00:00"," \t1970-01-01T00:00:00Z\r\n"] { assert_eq!(parse(text).unwrap(),0,"{text}"); }
        assert_eq!(parse("1970-01-01T00:00:00.123999999999Z").unwrap(),123);
        assert_eq!(parse("1970-01-01T00:00:60Z").unwrap(),60_000);
        assert_eq!(parse("1970-01-01T24:00:00.5Z").unwrap(),86_400_500);
        assert_eq!(parse("1970-04-31T24:00:00Z").unwrap(),gregorian_day(1970,5,1) * 86_400_000);
    }

    #[test]
    fn partial_xml_calendars_supply_epoch_defaults_and_keep_undefined_fields_after_hour24() {
        for text in ["1970","1970Z","1970-01","1970-01-01","--01","--01-01","---01","00:00:00","24:00:00"] { assert_eq!(parse(text).unwrap(),0,"{text}"); }
        assert_eq!(parse("--02-29").unwrap(),59 * 86_400_000);
        assert_eq!(parse("---31").unwrap(),30 * 86_400_000);
        assert_eq!(parse("24:00:00.5").unwrap(),500);
        assert_eq!(parse("-0001-01-01").unwrap(),gregorian_day(0,1,1) * 86_400_000);
    }

    #[test]
    fn invalid_xml_calendar_fields_and_lexical_forms_fail_before_date_conversion() {
        for text in ["","0000","-0000-01-01","2023-02-29","--02-30","---00","--13","1970-01-01T24:00:01Z","1970-01-01T25:00:00Z","1970-01-01T00:60:00Z","1970-01-01T00:00:61Z","1970-01-01T00:00:00.Z","1970-01-01T00:00:00+14:01","1970-01-01T00:00:00z","+1970","1970-1-01","00:00","--01--","\u{a0}1970","-0001-12-31T24:00:00Z"] { assert!(parse(text).is_err(),"{text}"); }
    }

    #[test]
    fn arbitrary_xml_years_retain_calendar_signed32_conversion_and_millisecond_wrap() {
        assert_eq!(parse("4294969266-01-01").unwrap(),0);
        assert_eq!(parse("-4294967297-01-01").unwrap(),gregorian_day(0,1,1) * 86_400_000);
        assert_eq!(parse("1000000000-01-01").unwrap(),gregorian_day(1_000_000_000,1,1).wrapping_mul(86_400_000));
        assert_eq!(parse("2147483648-01-01").unwrap(),julian_day(i32::MIN,1,1).wrapping_mul(86_400_000));
    }

    #[test]
    fn leap_second_day_carry_precedes_calendar_cutover_selection() {
        assert_eq!(parse("-292275056-05-15T23:59:60Z").unwrap(),parse("-292275056-05-16T00:00:00Z").unwrap());
    }
}
