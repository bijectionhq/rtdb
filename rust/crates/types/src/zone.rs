use std::{fmt, io::Read, path::Path, sync::OnceLock};
use crate::{Anomaly, CivilDate, CivilDateTime, Instant, Result};
use crate::tzdb::{self, Database, Rule, Rules, legacy::LegacyRules};

const MIN_INSTANT_SECOND: i64 = -31_557_014_167_219_200;
const MAX_INSTANT_SECOND: i64 = 31_556_889_864_403_199;

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb/invalid-timezone", message) }
fn overflow() -> Anomaly { Anomaly::incorrect("xtdb/time-overflow", "Temporal value exceeds its calendar representation") }

/// An integral UTC offset in the inclusive range −18 through +18 hours.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FixedOffset(i32);

impl FixedOffset {
    pub const UTC: Self = Self(0);
    /// Validates seconds east of UTC.
    pub fn from_seconds(seconds: i32) -> Result<Self> {
        if !(-64_800..=64_800).contains(&seconds) { return Err(invalid("Zone offset not in valid range: -18:00 to +18:00")); }
        Ok(Self(seconds))
    }
    pub fn seconds(self) -> i32 { self.0 }
    /// Parses Java `ZoneOffset.of` spellings: `Z`, signed hours, compact hour/minute/second, or colon-separated forms.
    pub fn parse(text: &str) -> Result<Self> {
        if text == "Z" { return Ok(Self::UTC); }
        let invalid_format = || invalid(format!("Invalid ID for ZoneOffset, invalid format: {text}"));
        let mut units = [0u16; 9];
        let mut len = 0;
        for unit in text.encode_utf16() {
            *units.get_mut(len).ok_or_else(invalid_format)? = unit;
            len += 1;
        }
        if len == 2 {
            units[2] = units[1];
            units[1] = u16::from(b'0');
            len = 3;
        }
        let fail = |reason| invalid(format!("Invalid ID for ZoneOffset, {reason}: {}", String::from_utf16_lossy(&units[..len])));
        let pair = |start: usize, after_colon: bool| {
            if after_colon && units[start - 1] != u16::from(b':') {
                return Err(fail("colon not found when expected"));
            }
            let [first, second] = [units[start], units[start + 1]];
            let digit = |value| (u16::from(b'0')..=u16::from(b'9')).contains(&value);
            if !digit(first) || !digit(second) { return Err(fail("non numeric characters found")); }
            Ok(i32::from(first - u16::from(b'0')) * 10 + i32::from(second - u16::from(b'0')))
        };
        let (hours, minutes, seconds) = match len {
            3 => (pair(1, false)?, 0, 0),
            5 => (pair(1, false)?, pair(3, false)?, 0),
            6 => (pair(1, false)?, pair(4, true)?, 0),
            7 => (pair(1, false)?, pair(3, false)?, pair(5, false)?),
            9 => (pair(1, false)?, pair(4, true)?, pair(7, true)?),
            _ => return Err(invalid_format()),
        };
        let sign = match units[0] {
            value if value == u16::from(b'+') => 1,
            value if value == u16::from(b'-') => -1,
            _ => return Err(fail("plus/minus not found when expected")),
        };
        for (name, offset, limit) in [("hours", hours, 18), ("minutes", minutes, 59), ("seconds", seconds, 59)] {
            if offset > limit {
                return Err(invalid(format!("Zone offset {name} not in valid range: value {} is not in the range -{limit} to {limit}", sign * offset)));
            }
        }
        Self::from_seconds(sign * (hours * 3600 + minutes * 60 + seconds))
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
enum ZoneKind { Fixed { offset: FixedOffset, prefix: Prefix }, Region(Region) }

/// A region of the process's zone database, identified by its name, which is unique within that one database.
#[derive(Clone, Copy)]
struct Region { name: &'static str, rules: &'static Rules }
impl PartialEq for Region { fn eq(&self, other: &Self) -> bool { self.name == other.name } }
impl Eq for Region {}
impl std::hash::Hash for Region { fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.name.hash(state); } }
impl fmt::Debug for Region { fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result { out.debug_tuple("Region").field(&self.name).finish() } }

/// A validated timezone backed by the process's zone database or a fixed UTC offset.
/// After the first region lookup loads the database, lookups allocate no memory and perform no I/O; names and rules remain valid for the process lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ZoneId { kind: ZoneKind }

impl ZoneId {
    /// Parses Java `ZoneId.of` names without the optional short-ID alias map.
    /// Unknown names and invalid offset spellings return `xtdb/invalid-timezone`; a region name, when the zone database cannot be loaded, `xtdb/tzdb-unavailable`.
    pub fn parse(name: &str) -> Result<Self> {
        for (text, prefix) in [("UTC", Prefix::Utc), ("GMT", Prefix::Gmt), ("UT", Prefix::Ut)] {
            if let Some(suffix) = name.strip_prefix(text) {
                if suffix.is_empty() { return Ok(Self { kind: ZoneKind::Fixed { offset: FixedOffset::UTC, prefix } }); }
                if suffix.starts_with(['+', '-']) {
                    let offset = FixedOffset::parse(suffix).map_err(|_| invalid(format!("Invalid ID for offset-based ZoneId: {name}")))?;
                    return Ok(Self { kind: ZoneKind::Fixed { offset, prefix } });
                }
            }
        }
        if name.encode_utf16().nth(1).is_none() || name.starts_with(['+', '-']) {
            return Ok(Self { kind: ZoneKind::Fixed { offset: FixedOffset::parse(name)?, prefix: Prefix::None } });
        }
        let database = database()?;
        let index = database.zones.binary_search_by(|zone| zone.name.as_str().cmp(name)).map_err(|_| {
            let mut bytes = name.bytes();
            if bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
                && bytes.all(|byte| byte.is_ascii_alphanumeric() || b"/~._+-".contains(&byte)) {
                invalid(format!("Unknown time-zone ID: {name}"))
            } else {
                invalid(format!("Invalid ID for region-based ZoneId, invalid format: {name}"))
            }
        })?;
        let zone = &database.zones[index];
        Ok(Self { kind: ZoneKind::Region(Region { name: zone.name.as_str(), rules: &database.rules[zone.rules] }) })
    }

    pub fn utc() -> Self { Self { kind: ZoneKind::Fixed { offset: FixedOffset::UTC, prefix: Prefix::Utc } } }

    /// Returns a fixed offset when this zone has no historical or recurring wall-clock changes.
    pub fn fixed_offset(self) -> Option<FixedOffset> {
        match self.kind {
            ZoneKind::Fixed { offset, .. } => Some(offset),
            ZoneKind::Region(Region { rules, .. }) => rules.fixed.then_some(FixedOffset(rules.initial)),
        }
    }

    /// Fixed offsets and fixed-rule regions accept every signed64 epoch second.
    /// Regions with transitions require Java Instant bounds; recurring-rule construction also requires a valid calendar date.
    pub fn offset_at(self, epoch_second: i64) -> Result<FixedOffset> {
        match self.kind {
            ZoneKind::Fixed { offset, .. } => Ok(offset),
            ZoneKind::Region(Region { rules, .. }) => {
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
            ZoneKind::Region(Region { rules, .. }) => {
                if !(MIN_INSTANT_SECOND..=MAX_INSTANT_SECOND).contains(&epoch_second) { return Err(overflow()); }
                Ok(FixedOffset(rules.standard_at(epoch_second)))
            }
        }
    }

    /// Returns the savings in seconds at the latest historical wall-clock transition at or before the input that has nonzero savings.
    /// Recurring future rules are excluded, matching traversal of Java `ZoneRules.getTransitions`.
    /// Returns zero when no such transition exists and uses the same input-range checks as `standard_offset_at`.
    pub fn historical_daylight_savings_before(self, epoch_second: i64) -> Result<i32> {
        let ZoneKind::Region(Region { rules, .. }) = self.kind else { return Ok(0); };
        if !(MIN_INSTANT_SECOND..=MAX_INSTANT_SECOND).contains(&epoch_second) { return Err(overflow()); }
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
        let ZoneKind::Region(Region { rules, .. }) = self.kind else { return Ok(resolved); };
        rules.gap_transition(midnight, date.year())?.ok_or_else(overflow)
    }
}

impl fmt::Display for ZoneId {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ZoneKind::Region(Region { name, .. }) => out.write_str(name),
            ZoneKind::Fixed { offset, prefix } => {
                out.write_str(prefix.text())?;
                if offset != FixedOffset::UTC || prefix == Prefix::None { write!(out, "{offset}")?; }
                Ok(())
            }
        }
    }
}

/// Iterates the zone database's region identifiers, including its retained aliases, in lexical order.
/// - Errors: `xtdb/tzdb-unavailable` when the zone database cannot be loaded.
pub fn timezone_names() -> Result<impl ExactSizeIterator<Item = &'static str>> { Ok(database()?.zones.iter().map(|zone| zone.name.as_str())) }

/// The version of the zone database this process uses: `TZDB_VERSION` unless `RTDB_TZDB_FILE` names a replacement.
/// - Errors: `xtdb/tzdb-unavailable` when the zone database cannot be loaded.
pub fn tzdb_version() -> Result<&'static str> { Ok(database()?.version.as_str()) }

/// A region's name, the name `TimeZone` resolves it to, and its `TimeZone` rules; `None` when the database has no such region.
pub(crate) fn legacy_rules(name: &str) -> Result<Option<(&'static str,&'static str,&'static LegacyRules)>> {
    let database = database()?;
    let Ok(index) = database.zones.binary_search_by(|zone| zone.name.as_str().cmp(name)) else { return Ok(None); };
    let zone = &database.zones[index];
    Ok(Some((zone.name.as_str(),zone.legacy_name.as_str(),&database.legacy[zone.rules])))
}

/// The OpenJDK `tzdb.dat` the build selected and validated, which the zone database uses unless `RTDB_TZDB_FILE` replaces it.
static BUNDLED_TZDB: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/tzdb.dat"));

/// The process's zone database, loaded on first use as the JVM loads its `lib/tzdb.dat`, so that, as there, the rules
/// can be updated without a new build: from the OpenJDK `tzdb.dat` file a non-empty `RTDB_TZDB_FILE` names, otherwise
/// from the bundled one. A failed load fails every later region lookup the same way.
fn database() -> Result<&'static Database> {
    static DATABASE: OnceLock<Result<Database>> = OnceLock::new();
    DATABASE.get_or_init(|| load(std::env::var_os("RTDB_TZDB_FILE").filter(|path| !path.is_empty()).as_deref().map(Path::new)))
        .as_ref().map_err(Clone::clone)
}

/// Decodes the `tzdb.dat` at `path`, or the bundled one, reading at most the format's one-MiB bound.
fn load(path: Option<&Path>) -> Result<Database> {
    let unavailable = |source: &dyn fmt::Display, message: &dyn fmt::Display| Anomaly::fault("xtdb/tzdb-unavailable", format!("Unable to load TZDB time-zone rules from {source}: {message}"));
    let Some(path) = path else { return tzdb::decode(BUNDLED_TZDB).map_err(|message| unavailable(&"the bundled tzdb.dat", &message)); };
    let mut bytes = Vec::new();
    std::fs::File::open(path).and_then(|file| file.take(tzdb::MAX_BYTES as u64 + 1).read_to_end(&mut bytes))
        .map_err(|error| unavailable(&path.display(), &error))?;
    tzdb::decode(&bytes).map_err(|message| unavailable(&path.display(), &message))
}

impl Rules {
    fn standard_at(&self, second: i64) -> i32 {
        let index = self.standards.partition_point(|(transition,_)|*transition <= second);
        if index == 0 { self.standard_initial } else { self.standards[index - 1].1 }
    }
    fn offset_at(&self, second: i64) -> Result<i32> {
        let Some(&(last, last_offset)) = self.transitions.last() else { return Ok(self.initial); };
        if second > last && !self.recurring.is_empty() {
            let year = CivilDate::from_epoch_day((second + i64::from(last_offset)).div_euclid(86_400))?.year();
            let mut offset = last_offset;
            for rule in &self.recurring {
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
        for &(second, after) in &self.transitions {
            if after > before && (second + i64::from(before)..second + i64::from(after)).contains(&local) { return Ok(Some(second)); }
            before = after;
        }
        for rule in &self.recurring {
            let second = rule.epoch_second(year)?;
            if rule.after > rule.before && (second + i64::from(rule.before)..second + i64::from(rule.after)).contains(&local) { return Ok(Some(second)); }
        }
        Ok(None)
    }
}

impl Rule {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_replacement_tzdb_file_is_decoded_in_place_of_the_bundled_one() {
        let directory = std::env::temp_dir().join(format!("rtdb-tzdb-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("tzdb.dat");
        std::fs::write(&path, BUNDLED_TZDB).unwrap();
        let database = load(Some(path.as_path())).unwrap();
        assert_eq!(database.version, TZDB_VERSION);
        assert!(database.zones.iter().any(|zone| zone.name == "Europe/Paris"));
        std::fs::write(&path, &BUNDLED_TZDB[..BUNDLED_TZDB.len() / 2]).unwrap();
        let truncated = load(Some(path.as_path())).unwrap_err();
        assert!(truncated.to_string().contains("Unable to load TZDB time-zone rules"), "{truncated}");
        std::fs::remove_dir_all(&directory).unwrap();
        assert!(load(Some(path.as_path())).is_err());
    }

    #[test]
    fn the_bundled_tzdb_is_the_one_the_build_selected() {
        assert_eq!(load(None).unwrap().version, TZDB_VERSION);
        if std::env::var_os("RTDB_TZDB_FILE").is_none() { assert_eq!(tzdb_version().unwrap(), TZDB_VERSION); }
    }
}
