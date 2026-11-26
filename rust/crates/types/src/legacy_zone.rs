use std::fmt;
use crate::{Anomaly,CivilDate,Result};
use crate::tzdb::legacy::{LegacyRecurring,LegacyRules,LegacyTransition};

#[derive(Clone,Copy,Debug)]
enum Kind {
    Fixed { millis: i32,negative: bool,custom: bool },
    Region { name: &'static str,canonical: &'static str,rules: &'static LegacyRules },
}

/// Immutable OpenJDK `TimeZone` rules for signed64 java.util.Date milliseconds.
/// Uses the zone database's legacy 1900–2100 table and its SimpleTimeZone tail; it is distinct from `ZoneId`.
/// After the zone database is loaded, lookups allocate no memory and perform no I/O; all retained rule data is process-static.
#[derive(Clone,Copy,Debug)]
pub struct LegacyTimeZone { kind: Kind }

impl LegacyTimeZone {
    /// Resolves region names, Java25 short aliases and GMT custom offsets, including seconds up to ±23:59:59.
    /// Unrecognized or malformed names select GMT, matching `TimeZone.getTimeZone` fallback.
    /// - Errors: `xtdb/tzdb-unavailable` when the zone database cannot be loaded.
    pub fn parse(name: &str) -> Result<Self> {
        let alias = SHORT_IDS.iter().find(|(short,_)|*short == name);
        let lookup = alias.map_or(name,|(_,target)|*target);
        if let Some((original,canonical,rules)) = crate::zone::legacy_rules(lookup)? {
            return Ok(Self { kind: Kind::Region { name: alias.map_or(original,|(short,_)|*short),canonical,rules } });
        }
        if let Some((millis,negative)) = custom(name) { return Ok(Self { kind: Kind::Fixed { millis,negative,custom: true } }); }
        Ok(Self::gmt())
    }
    /// Returns the GMT fallback with zero offset and the GMT identifier.
    pub fn gmt() -> Self { Self { kind: Kind::Fixed { millis: 0,negative: false,custom: false } } }

    /// Returns total milliseconds east of UTC at a raw Date instant.
    /// Malformed selected recurrence rules return a timezone anomaly; arithmetic at Date boundaries uses Java wrapping.
    pub fn offset_at_millis(self,millis: i64) -> Result<i32> { self.offsets(millis,false).map(|(raw,saving)|raw + saving) }

    /// Returns standard and daylight-saving offsets, respectively, in milliseconds east of UTC.
    /// Historical savings come from the selected transition, including periods with a different standard offset.
    /// Recurrence errors match `offset_at_millis`; this lookup allocates no memory.
    pub fn offsets_at_millis(self,millis: i64) -> Result<(i32,i32)> { self.offsets(millis,false) }

    /// Returns standard and daylight-saving milliseconds for a lenient local wall-clock reading.
    /// Gaps use the preceding components and overlaps the later components, before any explicit calendar field overrides.
    /// Recurrence errors match `offset_at_millis`; this lookup allocates no memory.
    pub fn offsets_by_wall_millis(self,wall: i64) -> Result<(i32,i32)> { self.offsets(wall,true) }

    /// Resolves lenient local calendar milliseconds, using the later side of an overlap and the preceding side of a gap.
    /// The resulting Date milliseconds wrap at signed64 boundaries; recurrence errors match `offset_at_millis`.
    pub fn resolve_wall_millis(self,wall: i64) -> Result<i64> { let (raw,saving) = self.offsets(wall,true)?;Ok(wall.wrapping_sub(i64::from(raw + saving))) }

    /// Returns the configured final standard offset in milliseconds, independently of the observation date.
    pub fn final_raw_offset_millis(self) -> i32 { match self.kind { Kind::Fixed { millis,.. } => millis,Kind::Region { rules,.. } => rules.raw } }

    /// Returns `ZoneInfo.getRawOffset` using explicit process-initialization and observation clocks.
    /// Both inputs are raw Date milliseconds; no clock is read internally.
    /// A standard transition after initialization selects the observed standard offset, otherwise the final configured offset is returned.
    pub fn raw_offset_at_millis(self,initialized_at: i64,observed_at: i64) -> Result<i32> {
        if let Kind::Region { rules,.. } = self.kind {
            if rules.last_standard_transition.is_some_and(|second|second > initialized_at / 1000) { return self.offsets_at_millis(observed_at).map(|(raw,_)|raw); }
        }
        Ok(self.final_raw_offset_millis())
    }

    /// Returns the last rule's daylight-saving increment in milliseconds, or zero when there is no recurring daylight rule.
    /// This is the configured display/parse increment, not historical savings at an arbitrary date.
    pub fn daylight_savings_millis(self) -> i32 { match self.kind { Kind::Region { rules,.. } => rules.future.as_ref().map_or(0,|future|future.saving),_ => 0 } }

    /// Converts the identifier using Java `TimeZone.toZoneId`, including fixed EST/MST/HST aliases.
    /// A legacy custom offset outside the modern ±18-hour limit returns `xtdb/invalid-timezone`.
    /// Fixed-offset conversion uses a temporary identifier of at most twelve UTF-8 bytes.
    pub fn to_zone_id(self) -> Result<crate::ZoneId> {
        match self.kind {
            Kind::Region { name,.. } => crate::ZoneId::parse(match name {
                "EST" => "-05:00","MST" => "-07:00","HST" => "-10:00",
                _ => SHORT_IDS.iter().find(|(short,_)|*short == name).map_or(name,|(_,target)|*target),
            }),
            Kind::Fixed { millis,.. } => {
                let offset = crate::FixedOffset::from_seconds(millis / 1000)?;
                if millis == 0 { crate::ZoneId::parse("GMT") } else { crate::ZoneId::parse(&format!("GMT{offset}")) }
            },
        }
    }

    fn offsets(self,date: i64,wall: bool) -> Result<(i32,i32)> {
        let Kind::Region { rules,canonical,.. } = self.kind else { return Ok((self.final_raw_offset_millis(),0)); };
        if rules.transitions.is_empty() { return Ok((rules.raw,0)); }
        let index = transition_index(&rules.transitions,date,wall);
        if index < 0 { return Ok((rules.raw,0)); }
        if let Some((_,total,saving)) = rules.transitions.get(index as usize) { return Ok((total - saving,*saving)); }
        let Some(future) = &rules.future else { return Ok(rules.transitions.last().map_or((rules.raw,0),|(_,total,saving)|(total - saving,*saving))); };
        let time = if wall { date.wrapping_sub(i64::from(rules.raw)) } else { date };
        let offset = future.offset(time,rules.raw,canonical == "Africa/Cairo")?;
        let saving = offset - rules.raw;
        if wall && saving > 0 && future.offset(time.wrapping_sub(i64::from(saving)),rules.raw,canonical == "Africa/Cairo")? == rules.raw { return Ok((rules.raw,0)); }
        Ok((rules.raw,saving))
    }
}

impl fmt::Display for LegacyTimeZone {
    fn fmt(&self,out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            Kind::Region { name,.. } => out.write_str(name),
            Kind::Fixed { custom: false,.. } => out.write_str("GMT"),
            Kind::Fixed { millis,negative,.. } => {
                let seconds = millis.abs() / 1000;
                write!(out,"GMT{}{:02}:{:02}",if negative { '-' } else { '+' },seconds / 3600,seconds / 60 % 60)?;
                if seconds % 60 != 0 { write!(out,":{:02}",seconds % 60)?; }
                Ok(())
            }
        }
    }
}

fn transition_index(table: &[(i64,i32,i32)],date: i64,wall: bool) -> isize {
    let mut low = 0isize;let mut high = table.len() as isize - 1;
    while low <= high {
        let mid = (low + high) / 2;
        let (time,total,_) = table[mid as usize];
        let time = if wall { time + i64::from(total) } else { time };
        if time < date { low = mid + 1; } else if time > date { high = mid - 1; } else { return mid; }
    }
    if low >= table.len() as isize { low } else { low - 1 }
}

impl LegacyRecurring {
    fn offset(&self,date: i64,raw: i32,cairo: bool) -> Result<i32> {
        if self.saving <= 0 { return Err(invalid_rule()); }
        self.start.validate()?;self.end.validate()?;
        let year = CivilDate::from_epoch_day(date.wrapping_add(i64::from(raw)).div_euclid(86_400_000))?.year();
        if year < 0 { return Ok(raw); }
        let start = self.start.at(year,raw,0,false)?;
        let end = self.end.at(year,raw,self.saving,cairo)?;
        let daylight = if start <= end { date >= start && date < end }
            else if date < end { date >= self.start.at(year - 1,raw,0,false)? }
            else if date >= start { date < self.end.at(year + 1,raw,self.saving,cairo)? }
            else { false };
        Ok(if daylight { raw + self.saving } else { raw })
    }
}
impl LegacyTransition {
    fn validate(self) -> Result<()> {
        let maximum = match self.month { 2 => 28,4 | 6 | 9 | 11 => 30,_ => 31 };
        if !(1..=12).contains(&self.month) || self.weekday > 7 || self.second > 86_400 || self.definition > 2
            || self.last && self.weekday == 0 || !self.last && !(1..=maximum).contains(&self.day) { return Err(invalid_rule()); }
        Ok(())
    }
    fn at(mut self,year: i32,raw: i32,saving: i32,cairo: bool) -> Result<i64> {
        if cairo && self.last && self.weekday == 5 && self.second == 0 { self.weekday = 4;self.second = 86_400; }
        let mut date = if self.last { CivilDate::from_ymd(year,u32::from(self.month),CivilDate::month_length(year,u32::from(self.month)))? }
            else { CivilDate::from_ymd(year,u32::from(self.month),1)?.plus_days(i64::from(self.day) - 1)? };
        if self.weekday != 0 {
            let difference = i64::from(self.weekday) - i64::from(date.iso_day_of_week());
            date = date.plus_days(if self.last { -(-difference).rem_euclid(7) } else { difference.rem_euclid(7) })?;
        }
        let mut time = i64::from(self.second) * 1000;
        if self.definition != 0 { time -= i64::from(raw); }
        if self.definition == 1 { time -= i64::from(saving); }
        Ok(date.epoch_day().wrapping_mul(86_400_000).wrapping_add(time))
    }
}

fn custom(name: &str) -> Option<(i32,bool)> {
    let bytes = name.strip_prefix("GMT")?.as_bytes();
    if bytes.len() < 2 { return None; }
    let negative = match bytes.first()? { b'+' => false,b'-' => true,_ => return None };
    let mut parts = [0i32;3];let mut part = 0;let mut width = 0;
    for &byte in &bytes[1..] {
        if byte == b':' {
            if part == 2 || width == 0 || width > 2 { return None; }
            part += 1;width = 0;
        } else if byte.is_ascii_digit() { parts[part] = parts[part].wrapping_mul(10).wrapping_add(i32::from(byte - b'0'));width += 1; }
        else { return None; }
    }
    if part == 0 {
        if width > 4 { return None; }
        if width > 2 { parts[1] = parts[0] % 100;parts[0] /= 100; }
    } else if width != 2 { return None; }
    if parts[0] > 23 || parts[1] > 59 || parts[2] > 59 { return None; }
    let millis = (parts[0] * 3600 + parts[1] * 60 + parts[2]) * 1000;
    Some((if negative { -millis } else { millis },negative))
}

fn invalid_rule() -> Anomaly { Anomaly::incorrect("xtdb/invalid-timezone","Selected legacy timezone has an invalid SimpleTimeZone rule") }

const SHORT_IDS: &[(&str,&str)] = &[
    ("ACT","Australia/Darwin"),("AET","Australia/Sydney"),("AGT","America/Argentina/Buenos_Aires"),("ART","Africa/Cairo"),
    ("AST","America/Anchorage"),("BET","America/Sao_Paulo"),("BST","Asia/Dhaka"),("CAT","Africa/Harare"),("CNT","America/St_Johns"),
    ("CST","America/Chicago"),("CTT","Asia/Shanghai"),("EAT","Africa/Addis_Ababa"),("ECT","Europe/Paris"),("IET","America/Indiana/Indianapolis"),
    ("IST","Asia/Kolkata"),("JST","Asia/Tokyo"),("MIT","Pacific/Apia"),("NET","Asia/Yerevan"),("NST","Pacific/Auckland"),
    ("PLT","Asia/Karachi"),("PNT","America/Phoenix"),("PRT","America/Puerto_Rico"),("PST","America/Los_Angeles"),("SST","Pacific/Guadalcanal"),
    ("VST","Asia/Ho_Chi_Minh"),("EST","America/Panama"),("MST","America/Phoenix"),("HST","Pacific/Honolulu"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_timezone_exact_february_rule_rejects_leap_day_at_construction() {
        let rule = LegacyTransition { month: 2, day: 29, weekday: 0, last: false, second: 0, definition: 1 };
        assert!(rule.validate().is_err());
        assert!(LegacyTransition { day: 28, ..rule }.validate().is_ok());
        assert!(LegacyTransition { day: -1, weekday: 7, last: true, ..rule }.validate().is_ok());
    }
}
