use std::fmt;
use crate::{Anomaly, CivilDate, CivilDateTime, Instant, Result};
use crate::legacy_zone::{LegacyRules,LegacyRecurring,LegacyTransition};

const MIN_INSTANT_SECOND: i64 = -31_557_014_167_219_200;
const MAX_INSTANT_SECOND: i64 = 31_556_889_864_403_199;

fn invalid(name: &str) -> Anomaly { Anomaly::incorrect("xtdb/invalid-timezone", format!("Invalid time zone: {name}")) }
fn overflow() -> Anomaly { Anomaly::incorrect("xtdb/time-overflow", "Temporal value exceeds its calendar representation") }

/// An integral UTC offset in the inclusive range −18 through +18 hours.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FixedOffset(i32);

impl FixedOffset {
    pub const UTC: Self = Self(0);
    /// Validates seconds east of UTC.
    pub fn from_seconds(seconds: i32) -> Result<Self> {
        if !(-64_800..=64_800).contains(&seconds) { return Err(invalid(&seconds.to_string())); }
        Ok(Self(seconds))
    }
    pub fn seconds(self) -> i32 { self.0 }
    /// Parses Java `ZoneOffset.of` spellings: `Z`, signed hours, compact hour/minute/second, or colon-separated forms.
    pub fn parse(text: &str) -> Result<Self> {
        if text == "Z" { return Ok(Self::UTC); }
        let bytes = text.as_bytes();
        let sign = match bytes.first() { Some(b'+') => 1, Some(b'-') => -1, _ => return Err(invalid(text)) };
        let pair = |at: usize| -> Option<i32> {
            let digits = bytes.get(at..at + 2)?;
            digits.iter().all(u8::is_ascii_digit).then(|| i32::from(digits[0] - b'0') * 10 + i32::from(digits[1] - b'0'))
        };
        let parts = match bytes.len() {
            2 if bytes[1].is_ascii_digit() => Some((i32::from(bytes[1] - b'0'), 0, 0)),
            3 => pair(1).map(|h| (h, 0, 0)),
            5 => pair(1).zip(pair(3)).map(|(h, m)| (h, m, 0)),
            6 if bytes[3] == b':' => pair(1).zip(pair(4)).map(|(h, m)| (h, m, 0)),
            7 => pair(1).zip(pair(3)).zip(pair(5)).map(|((h, m), s)| (h, m, s)),
            9 if bytes[3] == b':' && bytes[6] == b':' => pair(1).zip(pair(4)).zip(pair(7)).map(|((h, m), s)| (h, m, s)),
            _ => None,
        };
        let (hour, minute, second) = parts.ok_or_else(|| invalid(text))?;
        if hour > 18 || minute > 59 || second > 59 || hour == 18 && (minute != 0 || second != 0) { return Err(invalid(text)); }
        Ok(Self(sign * (hour * 3600 + minute * 60 + second)))
    }
}

impl fmt::Display for FixedOffset {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 { return out.write_str("Z"); }
        let value = self.0.abs();
        write!(out, "{}{:02}:{:02}", if self.0 < 0 { '-' } else { '+' }, value / 3600, value / 60 % 60)?;
        if value % 60 != 0 { write!(out, ":{:02}", value % 60)?; }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Prefix { None, Utc, Gmt, Ut }
impl Prefix { fn text(self) -> &'static str { match self { Self::None => "", Self::Utc => "UTC", Self::Gmt => "GMT", Self::Ut => "UT" } } }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ZoneKind { Fixed { offset: FixedOffset, prefix: Prefix }, Region(usize) }

/// A validated timezone backed by immutable bundled rules or a fixed UTC offset.
/// Successful lookups allocate no memory and perform no I/O; names and rules remain valid for the process lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ZoneId { kind: ZoneKind }

impl ZoneId {
    /// Parses Java `ZoneId.of` names without the optional short-ID alias map.
    /// Unknown names and invalid offset spellings return `xtdb/invalid-timezone`.
    pub fn parse(name: &str) -> Result<Self> {
        for (text, prefix) in [("UTC", Prefix::Utc), ("GMT", Prefix::Gmt), ("UT", Prefix::Ut)] {
            if let Some(suffix) = name.strip_prefix(text) {
                if suffix.is_empty() { return Ok(Self { kind: ZoneKind::Fixed { offset: FixedOffset::UTC, prefix } }); }
                if suffix.starts_with(['+', '-']) { return Ok(Self { kind: ZoneKind::Fixed { offset: FixedOffset::parse(suffix)?, prefix } }); }
            }
        }
        if name == "Z" || name.starts_with(['+', '-']) { return Ok(Self { kind: ZoneKind::Fixed { offset: FixedOffset::parse(name)?, prefix: Prefix::None } }); }
        let index = ZONES.binary_search_by_key(&name, |zone| zone.name).map_err(|_| invalid(name))?;
        Ok(Self { kind: ZoneKind::Region(index) })
    }

    pub fn utc() -> Self { Self { kind: ZoneKind::Fixed { offset: FixedOffset::UTC, prefix: Prefix::Utc } } }

    /// Returns a fixed offset when this zone has no historical or recurring wall-clock changes.
    pub fn fixed_offset(self) -> Option<FixedOffset> {
        match self.kind {
            ZoneKind::Fixed { offset, .. } => Some(offset),
            ZoneKind::Region(index) => { let rules = ZONES[index].rules; rules.fixed.then_some(FixedOffset(rules.initial)) },
        }
    }

    /// Fixed offsets and fixed-rule regions accept every signed64 epoch second.
    /// Regions with transitions require Java Instant bounds; recurring-rule construction also requires a valid calendar date.
    pub fn offset_at(self, epoch_second: i64) -> Result<FixedOffset> {
        match self.kind {
            ZoneKind::Fixed { offset, .. } => Ok(offset),
            ZoneKind::Region(index) => {
                let rules = ZONES[index].rules;
                if self.fixed_offset().is_some() { return Ok(FixedOffset(rules.initial)); }
                if !(MIN_INSTANT_SECOND..=MAX_INSTANT_SECOND).contains(&epoch_second) { return Err(overflow()); }
                rules.offset_at(epoch_second).map(FixedOffset)
            }
        }
    }

    /// Returns the standard offset at a Java Instant epoch second, excluding daylight saving.
    /// Region inputs outside Java Instant's range return `xtdb/time-overflow`; fixed offsets accept every signed64 second.
    pub fn standard_offset_at(self, epoch_second: i64) -> Result<FixedOffset> {
        match self.kind {
            ZoneKind::Fixed { offset,.. } => Ok(offset),
            ZoneKind::Region(index) => {
                if !(MIN_INSTANT_SECOND..=MAX_INSTANT_SECOND).contains(&epoch_second) { return Err(overflow()); }
                Ok(FixedOffset(ZONES[index].rules.standard_at(epoch_second)))
            }
        }
    }

    /// Returns the savings in seconds at the latest historical wall-clock transition at or before the input that has nonzero savings.
    /// Recurring future rules are excluded, matching traversal of Java `ZoneRules.getTransitions`.
    /// Returns zero when no such transition exists and uses the same input-range checks as `standard_offset_at`.
    pub fn historical_daylight_savings_before(self, epoch_second: i64) -> Result<i32> {
        let ZoneKind::Region(index) = self.kind else { return Ok(0); };
        if !(MIN_INSTANT_SECOND..=MAX_INSTANT_SECOND).contains(&epoch_second) { return Err(overflow()); }
        let rules = ZONES[index].rules;
        Ok(rules.transitions.iter().rev().filter(|(second,_)|*second <= epoch_second).find_map(|(second,offset)| {
            let saving = offset - rules.standard_at(*second);
            (saving != 0).then_some(saving)
        }).unwrap_or(0))
    }

    /// Resolves a local-clock second to its offset, preferring a valid supplied offset during overlaps.
    /// Without a preference overlaps use the earlier offset; gaps use the offset before the transition.
    pub fn offset_for_local(self, local_second: i64, preferred: Option<FixedOffset>) -> Result<FixedOffset> {
        if let Some(offset) = self.fixed_offset() { return Ok(offset); }
        let before = self.offset_at(local_second.wrapping_sub(64_800))?;
        let after = self.offset_at(local_second.wrapping_add(64_800))?;
        if before == after { return Ok(before); }
        let before_valid = self.offset_at(local_second.wrapping_sub(i64::from(before.0)))? == before;
        let after_valid = self.offset_at(local_second.wrapping_sub(i64::from(after.0)))? == after;
        Ok(if after_valid && (!before_valid || preferred == Some(after)) { after } else { before })
    }

    /// Resolves a local date-time to an instant, moving readings in a gap forward by the gap length.
    /// Nanoseconds are preserved; an overlap honors a valid preferred offset.
    pub fn resolve_local(self, local: CivilDateTime, preferred: Option<FixedOffset>) -> Result<Instant> {
        let offset = self.offset_for_local(local.epoch_second(), preferred)?;
        Instant::new(local.epoch_second().checked_sub(i64::from(offset.0)).ok_or_else(overflow)?, local.nano())
    }

    /// Converts an instant to the zone's local date-time, failing beyond the calendar range.
    pub fn local_datetime(self, instant: Instant) -> Result<CivilDateTime> {
        let offset = self.offset_at(instant.seconds())?;
        CivilDateTime::from_epoch_second(instant.seconds().checked_add(i64::from(offset.0)).ok_or_else(overflow)?, instant.nanos())
    }

    /// Resolves the first valid instant of a calendar day.
    /// A midnight gap starts at the transition instant, including transitions that skip an entire date.
    pub fn start_of_day(self, date: CivilDate) -> Result<i64> {
        let midnight = date.epoch_day() * 86_400;
        let offset = self.offset_for_local(midnight, None)?;
        let resolved = midnight - i64::from(offset.0);
        if self.offset_at(resolved)? == offset { return Ok(resolved); }
        let ZoneKind::Region(index) = self.kind else { return Ok(resolved); };
        ZONES[index].rules.gap_transition(midnight, date.year())?.ok_or_else(overflow)
    }
}

impl fmt::Display for ZoneId {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ZoneKind::Region(index) => out.write_str(ZONES[index].name),
            ZoneKind::Fixed { offset, prefix } => {
                out.write_str(prefix.text())?;
                if offset != FixedOffset::UTC || prefix == Prefix::None { write!(out, "{offset}")?; }
                Ok(())
            }
        }
    }
}

/// Iterates the selected TZDB version's region identifiers, including its retained aliases, in lexical order.
pub fn timezone_names() -> impl ExactSizeIterator<Item = &'static str> { ZONES.iter().map(|zone| zone.name) }

pub(crate) fn legacy_rules(name: &str) -> Option<(&'static str,&'static str,&'static LegacyRules)> {
    let zone = &ZONES[ZONES.binary_search_by_key(&name,|zone|zone.name).ok()?];
    Some((zone.name,zone.legacy_name,&zone.rules.legacy))
}

struct ZoneRecord { name: &'static str, legacy_name: &'static str, rules: &'static RuleData }
struct RuleData { initial: i32, fixed: bool, standard_initial: i32, standards: &'static [(i64,i32)], transitions: &'static [(i64, i32)], recurring: &'static [TransitionRule], legacy: LegacyRules }

impl RuleData {
    fn standard_at(&self, second: i64) -> i32 {
        let index = self.standards.partition_point(|(transition,_)|*transition <= second);
        if index == 0 { self.standard_initial } else { self.standards[index - 1].1 }
    }
    fn offset_at(&self, second: i64) -> Result<i32> {
        let Some(&(last, last_offset)) = self.transitions.last() else { return Ok(self.initial); };
        if second > last && !self.recurring.is_empty() {
            let year = CivilDate::from_epoch_day((second + i64::from(last_offset)).div_euclid(86_400))?.year();
            let mut offset = last_offset;
            for rule in self.recurring {
                if second < rule.epoch_second(year)? { return Ok(rule.before); }
                offset = rule.after;
            }
            return Ok(offset);
        }
        let index = self.transitions.partition_point(|(transition, _)| *transition <= second);
        Ok(if index == 0 { self.initial } else { self.transitions[index - 1].1 })
    }
    fn gap_transition(&self, local: i64, year: i32) -> Result<Option<i64>> {
        let mut before = self.initial;
        for &(second, after) in self.transitions {
            if after > before && (second + i64::from(before)..second + i64::from(after)).contains(&local) { return Ok(Some(second)); }
            before = after;
        }
        for rule in self.recurring {
            let second = rule.epoch_second(year)?;
            if rule.after > rule.before && (second + i64::from(rule.before)..second + i64::from(rule.after)).contains(&local) { return Ok(Some(second)); }
        }
        Ok(None)
    }
}

struct TransitionRule { month: u8, day: i8, weekday: u8, second: u32, end_of_day: bool, definition: u8, standard: i32, before: i32, after: i32 }
impl TransitionRule {
    fn epoch_second(&self, year: i32) -> Result<i64> {
        let day = if self.day < 0 { CivilDate::month_length(year, u32::from(self.month)) as i32 + 1 + i32::from(self.day) } else { i32::from(self.day) };
        let mut date = CivilDate::from_ymd(year, u32::from(self.month), day as u32)?;
        if self.weekday != 0 {
            let difference = i64::from(self.weekday) - i64::from(date.iso_day_of_week());
            date = date.plus_days(if self.day < 0 { -(-difference).rem_euclid(7) } else { difference.rem_euclid(7) })?;
        }
        if self.end_of_day { date = date.plus_days(1)?; }
        let offset = match self.definition { 0 => 0, 1 => self.before, _ => self.standard };
        Ok(date.epoch_day() * 86_400 + i64::from(self.second) - i64::from(offset))
    }
}

include!(concat!(env!("OUT_DIR"), "/tzdb.rs"));
