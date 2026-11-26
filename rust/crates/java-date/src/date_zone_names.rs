use std::{collections::BTreeSet,sync::Arc};
use rtdb_types::{LegacyTimeZone,MemoryBudget,MemoryReservation,Result,timezone_names};
use super::{date_locale::LocaleData,date_names::insensitive_prefix,locale_data::{self,Family,LocaleValue},error};

pub(super) struct ZoneName { pub id: &'static str,pub zone: LegacyTimeZone,pub names: [String;6] }
pub(super) struct ZoneDisplay { id: String,names: [String;6],_charge: Arc<MemoryReservation> }
pub(super) struct ZoneNames { entries: Vec<ZoneName>,_charge: Arc<MemoryReservation> }
pub(super) struct MatchedZone { pub zone: LegacyTimeZone,pub saving: Option<i32>,pub consumed: usize }
impl ZoneDisplay {
    fn allocation_bound(locale: &LocaleData) -> Result<usize> {
        let largest = locale.zone_sources().iter().flat_map(|entries|entries.iter()).map(|(_,value)|match value {
            LocaleValue::Text(value) => value.len(),LocaleValue::Array(values) => values.iter().map(|value|value.len()).max().unwrap_or(0),
        }).max().unwrap_or(0).max(128);
        let largest = ["timezone.regionFormat.standard","timezone.regionFormat.daylight","timezone.regionFormat","timezone.gmtFormat","timezone.gmtZeroFormat","timezone.hourFormat"]
            .into_iter().filter_map(|key|locale.text(key)).try_fold(largest,|largest,value|largest.checked_add(value.len()).ok_or_else(overflow))?;
        largest.checked_add(256).and_then(|bytes|bytes.checked_mul(12)).ok_or_else(overflow)
    }
    pub fn new(locale: &LocaleData,zone: LegacyTimeZone,observe: &dyn Fn() -> i64,memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charge = memory.reserve(Self::allocation_bound(locale)?)?;
        let id = zone.to_string();
        let names = if id.starts_with("GMT+") || id.starts_with("GMT-") { std::array::from_fn(|_|id.clone()) } else { derive(locale,&id,observe,0)? };
        Ok(Self { id,names,_charge: charge })
    }
    pub fn matches(&self,zone: LegacyTimeZone) -> bool { self.id == zone.to_string() }
    pub fn display(&self,daylight: bool,long: bool) -> &str { &self.names[usize::from(daylight) * 2 + usize::from(!long)] }
}
impl ZoneNames {
    pub fn allocation_bound(locale: &LocaleData) -> Result<usize> {
        let entries = locale.zone_sources().iter().try_fold(timezone_names()?.len() + SHORT_IDS.len(),|total,entries|total.checked_add(entries.len()).ok_or_else(overflow))?;
        let per_entry = ZoneDisplay::allocation_bound(locale)?;
        entries.checked_add(32).and_then(|entries|entries.checked_mul(per_entry)).ok_or_else(overflow)
    }
    pub fn new(locale: &LocaleData,observe: &dyn Fn() -> i64,memory: &Arc<MemoryBudget>) -> Result<Self> {
        let charge = memory.reserve(Self::allocation_bound(locale)?)?;
        let mut seen = BTreeSet::new();
        let mut ids = Vec::new();
        for entries in locale.zone_sources() {
            for (id,value) in *entries {
                if matches!(value,LocaleValue::Array(_)) && seen.insert(*id) { ids.push(*id); }
            }
        }
        let mut remaining = timezone_names()?.chain(SHORT_IDS.iter().copied()).filter(|id| !seen.contains(id)
            && !id.starts_with("Etc/GMT") && !id.starts_with("GMT") && !id.starts_with("SystemV")).collect::<Vec<_>>();
        let capacity = ((timezone_names()?.len() + SHORT_IDS.len()) * 4 / 3 + 1).next_power_of_two();
        remaining.sort_by_key(|id|java_hash(id) as usize & (capacity - 1));
        ids.extend(remaining);
        let entries = ids.into_iter().map(|id|Ok(ZoneName { id,zone: LegacyTimeZone::parse(id)?,names: derive(locale,id,observe,0)? })).collect::<Result<Vec<_>>>()?;
        Ok(Self { entries,_charge: charge })
    }
    pub fn match_prefix(&self,text: &str,current: LegacyTimeZone,default: LegacyTimeZone) -> Option<MatchedZone> {
        let current = current.to_string();let default = default.to_string();
        let preferred = [current.as_str(),default.as_str()].into_iter().filter_map(|id|self.entries.iter().find(|entry|entry.id == id));
        for entry in preferred.chain(&self.entries) {
            for (index,name) in entry.names[..4].iter().enumerate() {
                if name.is_empty() { continue; }
                let Some(consumed) = insensitive_prefix(text,name) else { continue; };
                let zone = entry.zone;
                let saving = if index >= 2 { zone.daylight_savings_millis() } else { 0 };
                let same = index < 2 && insensitive_prefix(&entry.names[index + 2],name) == Some(entry.names[index + 2].len());
                return Some(MatchedZone { zone,saving: (!(same || index >= 2 && saving == 0)).then_some(saving),consumed });
            }
        }
        None
    }
}

fn derive(locale: &LocaleData,id: &str,observe: &dyn Fn() -> i64,depth: usize) -> Result<[String;6]> {
    if depth > 16 { return Err(error("invalid-date-locale","Timezone-name alias chain exceeds its bound")); }
    let canonical = locale_data::canonical_zone(id).filter(|canonical|*canonical != id);
    let initial = array(locale.zone(id)).or_else(||canonical.and_then(|canonical|array(locale.zone(canonical))));
    let mut names: [String;6] = std::array::from_fn(|index|initial.and_then(|names|names.get(index)).unwrap_or(&"").to_string());
    let modern = LegacyTimeZone::parse(id)?.to_zone_id()?;
    let fixed = modern.fixed_offset().is_some();
    for index in 0..6 {
        if !names[index].is_empty() {
            if names[index] == "∅∅∅" { names[index] = gmt(locale,id,index / 2 == 1,observe)?; }
            continue;
        }
        let root = Family::Zones.resource("",true);
        for entries in locale.zone_sources().iter().skip(1) {
            if index % 2 == 0 && root.is_some_and(|root|std::ptr::eq(root,*entries)) { continue; }
            if let Some(value) = entries.iter().rev().find(|(key,_)|*key == id).and_then(|(_,value)|array(Some(*value))).and_then(|names|names.get(index)).filter(|name|!name.is_empty()) {
                names[index] = (*value).into();break;
            }
        }
        if !names[index].is_empty() { continue; }
        if let Some(canonical) = canonical { names[index] = derive(locale,canonical,observe,depth + 1)?[index].clone();continue; }
        if fixed {
            for from in [4 + index % 2,index % 2] {
                if !names[from].is_empty() && !names[from].starts_with("GMT") { names[index] = names[from].clone();break; }
            }
        }
        if !names[index].is_empty() { continue; }
        if index % 2 == 0 {
            let canonical = canonical.unwrap_or(id);
            let city = match locale.zone(&format!("timezone.excity.{canonical}")) { Some(LocaleValue::Text(city)) => Some(city.to_owned()),_ => None }
                .or_else(|| (!canonical.starts_with("Etc") && !canonical.starts_with("SystemV")).then(||canonical.rsplit_once('/').map(|(_,city)|city.replace('_'," "))).flatten());
            if let Some(city) = city {
                let key = match index { 0 => "timezone.regionFormat.standard",2 => "timezone.regionFormat.daylight",_ => "timezone.regionFormat" };
                if let Some(pattern) = locale.text(key).filter(|pattern|!pattern.is_empty()) { names[index] = message(pattern,&city)?; }
            }
        }
        if !names[index].is_empty() { continue; }
        names[index] = gmt(locale,id,index / 2 == 1,observe)?;
        if matches!(id,"Etc/GMT" | "Etc/GMT-0" | "Etc/GMT+0" | "Etc/GMT0" | "GMT+0" | "GMT-0" | "GMT0" | "Greenwich") && !names[0].is_empty() { names[2] = names[0].clone();names[4] = names[0].clone(); }
    }
    Ok(names)
}
fn array(value: Option<LocaleValue>) -> Option<&'static [&'static str]> { match value { Some(LocaleValue::Array(values)) => Some(values),_ => None } }
fn gmt(locale: &LocaleData,id: &str,daylight: bool,observe: &dyn Fn() -> i64) -> Result<String> {
    let zone = LegacyTimeZone::parse(id)?.to_zone_id()?;
    let second = observe().div_euclid(1000);
    let offset = (zone.standard_offset_at(second)?.seconds() + if daylight { zone.historical_daylight_savings_before(second)? } else { 0 }) / 60;
    if offset == 0 { return Ok(locale.text("timezone.gmtZeroFormat").ok_or_else(missing)?.into()); }
    let format = locale.text("timezone.hourFormat").ok_or_else(missing)?;
    let (positive,negative) = format.split_once(';').ok_or_else(missing)?;
    let format = if offset > 0 { positive } else { negative };
    let mut result = String::new();let mut chars = format.chars().peekable();
    let zero = locale.numbers("NumberElements").and_then(|numbers|numbers.get(4)).and_then(|zero|zero.chars().next()).ok_or_else(missing)?;
    while let Some(character) = chars.next() {
        if matches!(character,'H' | 'm') {
            while chars.peek() == Some(&character) { chars.next(); }
            let value = if character == 'H' { offset.abs() / 60 } else { offset.abs() % 60 };
            for digit in [value / 10,value % 10] { result.push(char::from_u32(zero as u32 + digit as u32).ok_or_else(missing)?); }
        } else { result.push(character); }
    }
    message(locale.text("timezone.gmtFormat").ok_or_else(missing)?,&result)
}
fn message(pattern: &str,value: &str) -> Result<String> {
    let mut result = String::new();let mut chars = pattern.chars().peekable();let mut quote = false;
    while let Some(character) = chars.next() {
        if character == '\'' { if chars.peek() == Some(&'\'') { chars.next();result.push('\''); } else { quote = !quote; } }
        else if !quote && character == '{' {
            if chars.next() != Some('0') || chars.next() != Some('}') { return Err(missing()); }
            result.push_str(value);
        } else { result.push(character); }
    }
    Ok(result)
}
fn java_hash(value: &str) -> u32 { let hash = value.encode_utf16().fold(0u32,|hash,value|hash.wrapping_mul(31).wrapping_add(u32::from(value)));hash ^ (hash >> 16) }
fn missing() -> rtdb_types::Anomaly { error("invalid-date-locale","Selected locale lacks required timezone-name resources") }
fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow","Timezone-name allocation exceeds range") }
const SHORT_IDS: &[&str] = &["ACT","AET","AGT","ART","AST","BET","BST","CAT","CNT","CST","CTT","EAT","ECT","IET","IST","JST","MIT","NET","NST","PLT","PNT","PRT","PST","SST","VST","EST","MST","HST"];

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::date_locale::LocaleId;
    use std::collections::BTreeMap;
    #[test]
    fn localized_zone_names_preserve_dst_and_default_zone_parse_precedence() {
        let locale = LocaleData::new(LocaleId::from_parts("en","","US","",BTreeMap::new()).unwrap()).unwrap();
        let names = ZoneNames::new(&locale,&||1_600_000_000_000,&MemoryBudget::new(256 << 20)).unwrap();
        let chicago = LegacyTimeZone::parse("America/Chicago").unwrap();
        assert_eq!(ZoneDisplay::new(&locale,chicago,&||1_600_000_000_000,&MemoryBudget::new(256 << 20)).unwrap().display(false,false),"CST");
        let matched = names.match_prefix("CST remaining",chicago,LegacyTimeZone::gmt()).unwrap();
        assert_eq!(matched.zone.to_string(),"America/Chicago");assert_eq!(matched.saving,Some(0));assert_eq!(matched.consumed,3);
        let matched = names.match_prefix("CDT",chicago,LegacyTimeZone::gmt()).unwrap();
        assert_eq!(matched.saving,Some(3_600_000));
        assert_eq!(ZoneDisplay::new(&locale,LegacyTimeZone::parse("GMT-00:00").unwrap(),&||0,&MemoryBudget::new(256 << 20)).unwrap().display(false,true),"GMT-00:00");
    }
    #[test]
    fn message_substitution_keeps_quoted_literals_and_doubled_apostrophes() {
        assert_eq!(message("heure d''été : {0}","Paris").unwrap(),"heure d'été : Paris");
        assert_eq!(message("'{0}' {0}","Paris").unwrap(),"{0} Paris");
    }
}
