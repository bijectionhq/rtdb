use regex::Regex;
use rtdb_format::transit::TransitValue as T;
use rtdb_types::{CivilDate,CivilDateTime,FixedOffset,Instant,Result,ZoneId};
use super::{Object,invalid,tagged};
mod xml_date;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) struct Period { years: i32,months: i32,days: i32 }

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) struct Zoned { instant: Instant,zone: ZoneId }

#[derive(Clone,Debug,PartialEq,Eq)]
pub(crate) enum Time {
    Period(Period),Duration(i128),Date(CivilDate),LocalTime(u64),LocalDateTime(CivilDateTime),
    OffsetTime { local: u64,offset: FixedOffset },OffsetDateTime { local: CivilDateTime,offset: FixedOffset },
    Instant(Instant),ZonedDateTime(Zoned),Zone(ZoneId),Year(i32),YearMonth { year: i32,month: u32 },
    MonthDay { month: u32,day: u32 },DayOfWeek(u8),Month(u8),
    Interval { months: i32,days: i32,nanos: i64 },Range { from: Option<Zoned>,to: Option<Zoned> },LegacyDate(i64),
}

const DAYS: [&str;7] = ["MONDAY","TUESDAY","WEDNESDAY","THURSDAY","FRIDAY","SATURDAY","SUNDAY"];
const MONTHS: [&str;12] = ["JANUARY","FEBRUARY","MARCH","APRIL","MAY","JUNE","JULY","AUGUST","SEPTEMBER","OCTOBER","NOVEMBER","DECEMBER"];

impl Time {
    pub(super) fn recognizes(tag: &str) -> bool {
        matches!(tag,"time/period" | "time/duration" | "time/date" | "time/time" | "time/date-time" | "time/offset-time" |
            "time/offset-date-time" | "time/instant" | "time/zoned-date-time" | "time/zone" | "time/year" |
            "time/year-month" | "time/month-day" | "time/day-of-week" | "time/month" | "xtdb/interval" | "xtdb/tstz-range" | "m" | "t")
    }
    pub(super) fn read(tag: &str,representation: &Object) -> Result<Option<Self>> {
        if tag == "xtdb/tstz-range" {
            if matches!(representation,Object::String(value) if value.is_empty()) || matches!(representation,Object::Bytes(value) if value.is_empty()) {
                return Ok(Some(Self::Range { from: None,to: None }));
            }
            let values = representation.sequence().ok_or_else(invalid)?;
            return Ok(Some(Self::Range { from: Zoned::from_object(values.first())?,to: Zoned::from_object(values.get(1))? }));
        }
        if tag == "m" {
            let millis = match representation {
                Object::Number(value) => match value.as_ref() { super::number::Number::Long(value) => *value,_ => return Err(invalid()) },
                Object::String(text) => decode_long(text)?,_ => return Err(invalid()),
            };
            return Ok(Some(Self::LegacyDate(millis)));
        }
        Self::read_text(tag,representation.text()?)
    }
    pub(super) fn read_wire_range(representation: &T) -> Result<Self> {
        let values = match representation {
            T::Array(values) => values.as_slice(),
            T::Tagged { tag, representation } if tag == "list" => {
                let T::Array(values) = representation.as_ref() else { return Err(invalid()); };
                values.as_slice()
            }
            T::Null => &[],
            T::String(text) if text.is_empty() => &[],
            T::Bytes(bytes) if bytes.is_empty() => &[],
            _ => return Err(invalid()),
        };
        Ok(Self::Range { from: Zoned::from_wire(values.first())?, to: Zoned::from_wire(values.get(1))? })
    }
    pub(super) fn read_text(tag: &str,text: &str) -> Result<Option<Self>> {
        Ok(Some(match tag {
            "t" => Self::LegacyDate(xml_date::parse(text)?),
            "time/period" => Self::Period(Period::parse(text)?),
            "time/duration" => Self::Duration(crate::temporal_text::iso_duration(text)?),
            "time/date" => Self::Date(CivilDate::parse_iso(text)?),
            "time/time" => Self::LocalTime(rtdb_types::parse_iso_local_time(text)?),
            "time/date-time" => Self::LocalDateTime(rtdb_types::parse_sql_local_datetime(text)?),
            "time/offset-time" => {
                let at = text.find(['Z','z','+','-']).ok_or_else(invalid)?;
                let offset = &text[at..];
                if offset != "Z" && offset != "z" && !matches!(offset.len(),6 | 9) { return Err(invalid()); }
                let offset = FixedOffset::parse(if offset == "z" { "Z" } else { offset })?;
                Self::OffsetTime { local: rtdb_types::parse_iso_local_time(&text[..at])?,offset }
            }
            "time/instant" => Self::Instant(Zoned::parse(text)?.instant),
            "time/zoned-date-time" => Self::ZonedDateTime(Zoned::parse(text)?),
            "time/offset-date-time" => {
                let value = Zoned::parse(text)?;
                Self::OffsetDateTime { local: value.zone.local_datetime(value.instant)?,offset: value.zone.offset_at(value.instant.seconds())? }
            }
            "time/zone" => Self::Zone(ZoneId::parse(text)?),
            "time/year" => {
                let digits = text.strip_prefix(['+','-']).unwrap_or(text);
                if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|byte|byte.is_ascii_digit()) { return Err(invalid()); }
                Self::Year(text.parse().map_err(|_|invalid())?)
            }
            "time/year-month" => { let date = CivilDate::parse_iso(&format!("{text}-01"))?;Self::YearMonth { year: date.year(),month: date.month() } }
            "time/month-day" => {
                let rest = text.strip_prefix("--").ok_or_else(invalid)?;
                let date = CivilDate::parse_iso(&format!("2000-{rest}"))?;Self::MonthDay { month: date.month(),day: date.day() }
            }
            "time/day-of-week" => Self::DayOfWeek(DAYS.iter().position(|name|*name == text).ok_or_else(invalid)? as u8),
            "time/month" => Self::Month(MONTHS.iter().position(|name|*name == text).ok_or_else(invalid)? as u8),
            "xtdb/interval" => {
                let pattern = pattern(r"^(-)?P([-0-9YMWD]+)?(?:T([-0-9HMS.]+)?)?$")?;
                let Some(parts) = pattern.captures(text) else { return Ok(None); };
                let sign = parts.get(1).map_or("",|value|value.as_str());
                let period = parts.get(2).map(|value|Period::parse(&format!("{sign}P{}",value.as_str()))).transpose()?.unwrap_or(Period { years: 0,months: 0,days: 0 });
                let nanos = parts.get(3).map(|value|crate::temporal_text::iso_duration(&format!("{sign}PT{}",value.as_str()))).transpose()?.unwrap_or(0);
                Self::Interval { months: i32::try_from(i64::from(period.years) * 12 + i64::from(period.months)).map_err(|_|invalid())?,days: period.days,nanos: i64::try_from(nanos).map_err(|_|invalid())? }
            }
            _ => return Err(invalid()),
        }))
    }
    pub(super) fn wire(&self) -> Result<T> {
        let (tag,text) = match self {
            Self::Period(value) => ("time/period",value.text()),
            Self::Duration(value) => ("time/duration",crate::temporal_text::duration_text(*value)),
            Self::Date(value) => ("time/date",value.to_string()),
            Self::LocalTime(value) => ("time/time",local_time(*value)),
            Self::LocalDateTime(value) => ("time/date-time",local_datetime(*value)),
            Self::OffsetTime { local,offset } => ("time/offset-time",format!("{}{offset}",local_time(*local))),
            Self::OffsetDateTime { local,offset } => ("time/offset-date-time",format!("{}{offset}",local_datetime(*local))),
            Self::Instant(value) => ("time/instant",value.to_iso_string()?),
            Self::ZonedDateTime(value) => ("time/zoned-date-time",value.text()?),
            Self::Zone(value) => ("time/zone",value.to_string()),
            Self::Year(value) => ("time/year",value.to_string()),
            Self::YearMonth { year,month } => ("time/year-month",format!("{}-{month:02}",padded_year(*year))),
            Self::MonthDay { month,day } => ("time/month-day",format!("--{month:02}-{day:02}")),
            Self::DayOfWeek(value) => ("time/day-of-week",DAYS[usize::from(*value)].into()),
            Self::Month(value) => ("time/month",MONTHS[usize::from(*value)].into()),
            Self::Interval { months,days,nanos } => ("xtdb/interval",crate::temporal_text::interval_text(crate::temporal::IntervalParts { months: *months,days: *days,nanos: *nanos })),
            Self::Range { from,to } => return Ok(tagged("xtdb/tstz-range",T::Array([from,to].into_iter().map(|value|value.map(|value|Self::ZonedDateTime(value).wire()).unwrap_or(Ok(T::Null))).collect::<Result<_>>()?))),
            Self::LegacyDate(value) => return Ok(tagged("m",T::Integer(*value))),
        };
        Ok(tagged(tag,T::String(text)))
    }
    pub(super) fn text(&self) -> Result<String> {
        if let Self::Range { from,to } = self {
            let text = |value: &Option<Zoned>|value.map(Zoned::text).transpose().map(|value|value.unwrap_or_else(||"null".into()));
            return Ok(format!("ZonedDateTimeRange(from={}, to={})",text(from)?,text(to)?));
        }
        if let Self::LegacyDate(millis) = self {
            return super::legacy_date_text::plain(*millis,crate::admission::Admission::Standalone);
        }
        let T::Tagged { representation,.. } = self.wire()? else { return Err(invalid()); };
        let T::String(text) = *representation else { return Err(invalid()); };Ok(text)
    }
    pub(super) fn java_hash(&self) -> Result<i32> {
        Ok(match self {
            Self::Period(value) => value.years.wrapping_add(value.months.rotate_left(8)).wrapping_add(value.days.rotate_left(16)),
            Self::Duration(value) => long_hash(value.div_euclid(1_000_000_000) as i64).wrapping_add((value.rem_euclid(1_000_000_000) as i32).wrapping_mul(51)),
            Self::Date(value) => date_hash(*value),Self::LocalTime(value) => long_hash(*value as i64),Self::LocalDateTime(value) => datetime_hash(*value),
            Self::OffsetTime { local,offset } => long_hash(*local as i64) ^ offset.seconds(),
            Self::OffsetDateTime { local,offset } => datetime_hash(*local) ^ offset.seconds(),
            Self::Instant(value) => long_hash(value.seconds()).wrapping_add((value.nanos() as i32).wrapping_mul(51)),
            Self::ZonedDateTime(value) => value.java_hash()?,Self::Zone(value) => zone_hash(*value),Self::Year(value) => *value,
            Self::YearMonth { year,month } => *year ^ ((*month as i32) << 27),Self::MonthDay { month,day } => ((*month as i32) << 6) + *day as i32,
            Self::DayOfWeek(value) => string_hash(DAYS[usize::from(*value)]),Self::Month(value) => string_hash(MONTHS[usize::from(*value)]),
            Self::Interval { months,days,nanos } => months.wrapping_mul(31).wrapping_add(*days).wrapping_mul(31).wrapping_add(long_hash(*nanos)),
            Self::Range { from,to } => from.map(Zoned::java_hash).transpose()?.unwrap_or(0).wrapping_mul(31).wrapping_add(to.map(Zoned::java_hash).transpose()?.unwrap_or(0)),
            Self::LegacyDate(value) => long_hash(*value),
        })
    }
}

impl Zoned {
    fn new(instant: Instant,zone: ZoneId) -> Result<Self> { zone.local_datetime(instant)?;Ok(Self { instant,zone }) }
    fn parse(text: &str) -> Result<Self> { let (instant,zone) = rtdb_types::parse_sql_zoned_datetime(text)?;Self::new(instant,zone) }
    fn from_object(value: Option<&Object>) -> Result<Option<Self>> {
        Ok(match value {
            None | Some(Object::Null) => None,Some(Object::String(text)) => Some(Self::parse(text)?),
            Some(Object::Time(Time::ZonedDateTime(value))) => Some(*value),
            Some(Object::Time(Time::Instant(value))) => Some(Self::new(*value,ZoneId::utc())?),
            Some(Object::Time(Time::LegacyDate(value))) => Some(Self::from_legacy_date(*value)?),
            _ => return Err(invalid()),
        })
    }
    fn from_wire(value: Option<&T>) -> Result<Option<Self>> {
        match value {
            None | Some(T::Null) => Ok(None),
            Some(T::String(text)) => Self::parse(text).map(Some),
            Some(T::Tagged { tag, representation }) if matches!(tag.as_str(), "m" | "t") => {
                Self::from_legacy_date(super::legacy_date_text::wire_millis(tag, representation)?).map(Some)
            }
            Some(T::Tagged { tag, representation }) if matches!(tag.as_str(), "time/instant" | "time/zoned-date-time") => {
                let T::String(text) = representation.as_ref() else { return Err(invalid()); };
                let value = Time::read_text(tag, text)?.map(Object::Time);
                Self::from_object(value.as_ref())
            }
            _ => Err(invalid()),
        }
    }
    fn from_legacy_date(millis: i64) -> Result<Self> {
        Self::new(Instant::new(millis.div_euclid(1000), (millis.rem_euclid(1000) * 1_000_000) as u32)?, ZoneId::utc())
    }
    fn text(self) -> Result<String> {
        let offset = self.zone.offset_at(self.instant.seconds())?;
        let mut text = format!("{}{offset}",local_datetime(self.zone.local_datetime(self.instant)?));
        let zone = self.zone.to_string();
        if !offset_zone(&zone) { text.push('[');text.push_str(&zone);text.push(']'); }
        Ok(text)
    }
    fn java_hash(self) -> Result<i32> { Ok(datetime_hash(self.zone.local_datetime(self.instant)?) ^ self.zone.offset_at(self.instant.seconds())?.seconds() ^ zone_hash(self.zone).rotate_left(3)) }
}

impl Period {
    fn parse(text: &str) -> Result<Self> {
        let pattern = pattern(r"(?i)^([+-]?)P(?:([+-]?[0-9]+)Y)?(?:([+-]?[0-9]+)M)?(?:([+-]?[0-9]+)W)?(?:([+-]?[0-9]+)D)?$")?;
        let parts = pattern.captures(text).ok_or_else(invalid)?;
        if (2..=5).all(|at|parts.get(at).is_none()) { return Err(invalid()); }
        let sign = if &parts[1] == "-" { -1 } else { 1 };
        let component = |at| -> Result<i32> { parts.get(at).map(|part|part.as_str().parse::<i32>().ok().and_then(|value|value.checked_mul(sign)).ok_or_else(invalid)).unwrap_or(Ok(0)) };
        let (years,months,weeks,days) = (component(2)?,component(3)?,component(4)?,component(5)?);
        Ok(Self { years,months,days: weeks.checked_mul(7).and_then(|weeks|days.checked_add(weeks)).ok_or_else(invalid)? })
    }
    fn text(self) -> String {
        let mut text = String::from("P");
        for (value,unit) in [(self.years,'Y'),(self.months,'M'),(self.days,'D')] { if value != 0 { text.push_str(&format!("{value}{unit}")); } }
        if text.len() == 1 { text.push_str("0D"); }text
    }
}

fn pattern(text: &str) -> Result<Regex> { Regex::new(text).map_err(|_|invalid()) }
fn local_time(nanos: u64) -> String {
    let seconds = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let mut text = format!("{:02}:{:02}",seconds / 3600,seconds / 60 % 60);
    if seconds % 60 != 0 || fraction != 0 { text.push_str(&format!(":{:02}",seconds % 60)); }
    if fraction != 0 {
        let digits = if fraction % 1_000_000 == 0 { 3 } else if fraction % 1000 == 0 { 6 } else { 9 };
        let fraction = format!("{fraction:09}");text.push('.');text.push_str(&fraction[..digits]);
    }
    text
}
fn local_datetime(value: CivilDateTime) -> String { format!("{}T{}",value.date(),local_time(value.nano_of_day())) }
fn padded_year(year: i32) -> String { if year < 0 { format!("-{:04}",-year) } else { format!("{year:04}") } }
fn long_hash(value: i64) -> i32 { (value ^ ((value as u64 >> 32) as i64)) as i32 }
fn date_hash(date: CivilDate) -> i32 { let (year,month,day) = date.ymd();(year & !0x7ff) ^ year.wrapping_shl(11).wrapping_add((month as i32) << 6).wrapping_add(day as i32) }
fn datetime_hash(value: CivilDateTime) -> i32 { date_hash(value.date()) ^ long_hash(value.nano_of_day() as i64) }
fn string_hash(text: &str) -> i32 { text.encode_utf16().fold(0i32,|hash,ch|hash.wrapping_mul(31).wrapping_add(i32::from(ch))) }
fn offset_zone(text: &str) -> bool { text == "Z" || text.starts_with(['+','-']) }
fn zone_hash(zone: ZoneId) -> i32 { let text = zone.to_string();if offset_zone(&text) { zone.fixed_offset().unwrap().seconds() } else { string_hash(&text) } }
pub(super) fn decode_long(text: &str) -> Result<i64> {
    let negative = text.starts_with('-');
    let text = text.strip_prefix(['+','-']).unwrap_or(text);
    let (radix,digits) = if let Some(digits) = text.strip_prefix("0x").or_else(||text.strip_prefix("0X")).or_else(||text.strip_prefix('#')) { (16,digits) }
        else if text.starts_with('0') && text.len() > 1 { (8,&text[1..]) } else { (10,text) };
    if digits.is_empty() || digits.starts_with(['+','-']) { return Err(invalid()); }
    let limit = if negative { i64::MIN } else { -i64::MAX };
    let mut value = 0i64;
    for digit in digits.chars() {
        let digit = rtdb_types::hexadecimal_digit(digit).map(u32::from).filter(|digit|*digit < radix).ok_or_else(invalid)?;
        value = value.checked_mul(i64::from(radix)).and_then(|value|value.checked_sub(i64::from(digit))).filter(|value|*value >= limit).ok_or_else(invalid)?;
    }
    Ok(if negative { value } else { -value })
}

#[cfg(test)]
mod tests;
