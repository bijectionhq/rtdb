use std::{collections::BTreeMap, fmt::Write};

#[path = "legacy_tzdb.rs"]
mod legacy;

pub const MAX_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub struct Database {
    pub version: String,
    zones: BTreeMap<String, usize>,
    aliases: BTreeMap<String,String>,
    rules: Vec<Rules>,
}

#[derive(Debug)]
struct Rules { initial: i32, fixed: bool, standard_initial: i32, standards: Vec<(i64,i32)>, transitions: Vec<(i64, i32)>, recurring: Vec<Rule> }

#[derive(Debug)]
struct Rule { month: u8, day: i8, weekday: u8, second: u32, end_of_day: bool, definition: u8, standard: i32, before: i32, after: i32 }

struct Reader<'a> { bytes: &'a [u8], position: usize }

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self { Self { bytes, position: 0 } }
    fn take(&mut self, count: usize) -> Result<&'a [u8], String> {
        let end = self.position.checked_add(count).ok_or("TZDB length overflow")?;
        let bytes = self.bytes.get(self.position..end).ok_or("Truncated TZDB data")?;
        self.position = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, String> { Ok(self.take(1)?[0]) }
    fn short(&mut self) -> Result<usize, String> { Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as usize) }
    fn int(&mut self) -> Result<i32, String> { Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap())) }
    fn long(&mut self) -> Result<i64, String> { Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap())) }
    fn count(&mut self, maximum: usize) -> Result<usize, String> {
        let count = self.short()?;
        if count > maximum { return Err("TZDB count exceeds format bound".into()); }
        Ok(count)
    }
    fn text(&mut self) -> Result<String, String> {
        let size = self.count(255)?;
        let bytes = self.take(size)?;
        if bytes.is_empty() || !bytes.iter().all(|byte| byte.is_ascii_graphic()) { return Err("TZDB identifiers must be nonempty printable ASCII".into()); }
        Ok(std::str::from_utf8(bytes).unwrap().into())
    }
    fn epoch_second(&mut self) -> Result<i64, String> {
        let first = self.byte()?;
        if first == 255 { return self.long(); }
        let compressed = (i64::from(first) << 16) | (i64::from(self.byte()?) << 8) | i64::from(self.byte()?);
        Ok(compressed * 900 - 4_575_744_000)
    }
    fn offset(&mut self) -> Result<i32, String> {
        let byte = self.byte()? as i8;
        let offset = if byte == 127 { self.int()? } else { i32::from(byte) * 900 };
        checked_offset(offset)
    }
    fn transitions(&mut self) -> Result<(Vec<i64>, Vec<i32>), String> {
        let count = usize::try_from(self.int()?).map_err(|_| "Negative transition count")?;
        if count > 1024 { return Err("Too many TZDB transitions".into()); }
        let times = (0..count).map(|_| self.epoch_second()).collect::<Result<Vec<_>, _>>()?;
        if times.windows(2).any(|pair| pair[0] >= pair[1]) { return Err("Unordered TZDB transitions".into()); }
        let offsets = (0..=count).map(|_| self.offset()).collect::<Result<Vec<_>, _>>()?;
        Ok((times, offsets))
    }
    fn rule(&mut self) -> Result<Rule, String> {
        let bits = self.int()? as u32;
        let month = (bits >> 28) as u8;
        let day = ((bits >> 22) & 63) as i8 - 32;
        let weekday = ((bits >> 19) & 7) as u8;
        let time = ((bits >> 14) & 31) as u8;
        let definition = ((bits >> 12) & 3) as u8;
        let standard = ((bits >> 4) & 255) as i32;
        let before = ((bits >> 2) & 3) as i32;
        let after = (bits & 3) as i32;
        if !(1..=12).contains(&month) || !(-28..=31).contains(&day) || day == 0 || definition > 2 || (time > 24 && time != 31) { return Err("Invalid TZDB recurring rule".into()); }
        let second = if time == 31 { u32::try_from(self.int()?).map_err(|_| "Negative transition clock")? } else { u32::from(time % 24) * 3600 };
        if second >= 86_400 { return Err("TZDB transition clock exceeds a day".into()); }
        let standard = checked_offset(if standard == 255 { self.int()? } else { (standard - 128) * 900 })?;
        let before = checked_offset(if before == 3 { self.int()? } else { standard + before * 1800 })?;
        let after = checked_offset(if after == 3 { self.int()? } else { standard + after * 1800 })?;
        if before == after { return Err("TZDB transition must change offset".into()); }
        Ok(Rule { month, day, weekday, second, end_of_day: time == 24, definition, standard, before, after })
    }
    fn rules(&mut self) -> Result<Rules, String> {
        if self.byte()? != 1 { return Err("TZDB rule is not a ZoneRules record".into()); }
        let (standard_times, standard_offsets) = self.transitions()?;
        let (times, offsets) = self.transitions()?;
        for (index, second) in times.iter().enumerate() {
            checked_local_transition(*second, offsets[index])?;
            checked_local_transition(*second, offsets[index + 1])?;
        }
        let initial = offsets[0];
        let transitions: Vec<_> = times.into_iter().zip(offsets.into_iter().skip(1)).collect();
        let count = self.byte()?;
        if count > 16 { return Err("Too many recurring TZDB rules".into()); }
        let recurring: Vec<_> = (0..count).map(|_| self.rule()).collect::<Result<_, _>>()?;
        if self.position != self.bytes.len() { return Err("Trailing ZoneRules bytes".into()); }
        let fixed = standard_times.is_empty() && transitions.is_empty() && recurring.is_empty() && standard_offsets[0] == initial;
        let standard_initial = standard_offsets[0];
        let standards = standard_times.into_iter().zip(standard_offsets.into_iter().skip(1)).collect();
        Ok(Rules { initial, fixed, standard_initial, standards, transitions, recurring })
    }
}

fn checked_offset(offset: i32) -> Result<i32, String> {
    if !(-64_800..=64_800).contains(&offset) { return Err("TZDB offset exceeds eighteen hours".into()); }
    Ok(offset)
}

fn checked_local_transition(second: i64, offset: i32) -> Result<(), String> {
    const MIN_LOCAL_SECOND: i64 = -365_243_219_162 * 86_400;
    const MAX_LOCAL_SECOND: i64 = 365_241_780_471 * 86_400 + 86_399;
    match second.checked_add(i64::from(offset)) {
        Some(local) if (MIN_LOCAL_SECOND..=MAX_LOCAL_SECOND).contains(&local) => Ok(()),
        _ => Err("TZDB wall transition exceeds the LocalDateTime calendar range".into()),
    }
}

/// Decodes OpenJDK's TZDB format version 1 from at most one MiB.
/// Returns the last version's complete region mapping and validated transition records.
/// Malformed lengths, identifiers, offsets and transition encodings return a diagnostic string.
/// Both local endpoints of each wall transition must fit Java LocalDateTime's calendar range.
pub fn decode(bytes: &[u8]) -> Result<Database, String> {
    if bytes.len() > MAX_BYTES { return Err("TZDB input exceeds one MiB".into()); }
    let mut reader = Reader::new(bytes);
    if reader.byte()? != 1 || reader.text()? != "TZDB" { return Err("Unsupported TZDB file format".into()); }
    let versions = reader.count(16)?;
    if versions == 0 { return Err("TZDB file has no versions".into()); }
    let mut version = String::new();
    for _ in 0..versions { version = reader.text()?; }
    let count = reader.count(4096)?;
    let regions = (0..count).map(|_| reader.text()).collect::<Result<Vec<_>, _>>()?;
    let count = reader.count(4096)?;
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        let size = reader.short()?;
        rules.push(Reader::new(reader.take(size)?).rules()?);
    }
    let mut zones = BTreeMap::new();
    for _ in 0..versions {
        zones.clear();
        let count = reader.count(4096)?;
        for _ in 0..count {
            let region = regions.get(reader.short()?).ok_or("Unknown TZDB region index")?.clone();
            let rule = reader.short()?;
            if rule >= rules.len() || zones.insert(region, rule).is_some() { return Err("Invalid or duplicate TZDB region mapping".into()); }
        }
    }
    if zones.is_empty() { return Err("TZDB version contains no zones".into()); }
    let mut aliases = BTreeMap::new();
    while reader.position < bytes.len() {
        let count = reader.count(4096)?;
        aliases.clear();
        for _ in 0..count {
            let alias = reader.short()?;let target = reader.short()?;
            if alias >= regions.len() || target >= regions.len() { return Err("Unknown TZDB alias index".into()); }
            aliases.insert(regions[alias].clone(),regions[target].clone());
        }
    }
    let legacy_entries = rules.iter().try_fold(0usize,|count,rules|count.checked_add(legacy::allocation_entries(rules)?).ok_or_else(||"Legacy TZDB table bound overflow".to_owned()))?;
    if legacy_entries > 1_048_576 { return Err("Derived legacy TZDB transitions exceed the one-million-entry resource limit".into()); }
    Ok(Database { version, zones, aliases, rules })
}

impl Database {
    pub fn rust_source(&self, sha256: &str) -> String {
        let mut out = format!("pub const TZDB_VERSION: &str = {:?};\npub const TZDB_SHA256: &str = {sha256:?};\n", self.version);
        for (index, rules) in self.rules.iter().enumerate() {
            writeln!(out, "static RULE_{index}: RuleData = RuleData {{ initial: {}, fixed: {}, standard_initial: {}, standards: &{:?}, transitions: &{:?}, recurring: &[", rules.initial, rules.fixed, rules.standard_initial, rules.standards, rules.transitions).unwrap();
            for rule in &rules.recurring {
                writeln!(out, "TransitionRule {{ month: {}, day: {}, weekday: {}, second: {}, end_of_day: {}, definition: {}, standard: {}, before: {}, after: {} }},", rule.month, rule.day, rule.weekday, rule.second, rule.end_of_day, rule.definition, rule.standard, rule.before, rule.after).unwrap();
            }
            out.push_str("], legacy: ");
            legacy::write(&mut out,rules);
            out.push_str(" };\n");
        }
        out.push_str("static ZONES: &[ZoneRecord] = &[\n");
        for (name, index) in &self.zones {
            let canonical = self.aliases.get(name).unwrap_or(name);
            writeln!(out, "ZoneRecord {{ name: {name:?}, legacy_name: {canonical:?}, rules: &RULE_{index} }},").unwrap();
        }
        out.push_str("];\n");
        out
    }
}
