#[derive(Clone, Copy)]
pub(super) enum LocaleValue { Text(&'static str), Array(&'static [&'static str]) }
struct LocaleBundle {
    name: &'static str,
    entries: &'static [(&'static str, LocaleValue)],
    maps: &'static [(&'static str, &'static [(&'static str, LocaleValue)])],
    available: Option<&'static str>,
    nonlikely_script: Option<bool>,
}

include!(concat!(env!("OUT_DIR"), "/legacy_locale_data.rs"));

const BASE: &str = "sun/util/cldr/CLDRBaseLocaleDataMetaInfo";
const NON_BASE: &str = "sun/util/resources/cldr/provider/CLDRLocaleDataMetaInfo";
const MAPS: &str = "sun/util/cldr/CLDRBaseLocaleDataMetaInfo$CLDRMapHolder";

fn bundle(name: &str) -> Option<&'static LocaleBundle> {
    LOCALE_BUNDLES.binary_search_by_key(&name, |bundle| bundle.name).ok().map(|index| &LOCALE_BUNDLES[index])
}
fn map(owner: &str, name: &str) -> &'static [(&'static str, LocaleValue)] {
    bundle(owner).and_then(|bundle| bundle.maps.iter().find(|(key, _)| *key == name).map(|(_, entries)| *entries)).unwrap_or(&[])
}
fn lookup(entries: &'static [(&'static str, LocaleValue)], key: &str) -> Option<LocaleValue> {
    entries.iter().rev().find(|(name, _)| *name == key).map(|(_, value)| *value)
}

pub(super) fn alias(tag: &str) -> Option<&'static str> {
    match lookup(map(BASE, "languageAliasMap"), tag)? { LocaleValue::Text(value) => Some(value), _ => None }
}
pub(super) fn parent(tag: &str) -> Option<&'static str> {
    for (parent, children) in map(BASE, "parentLocalesMap") {
        if let LocaleValue::Array(children) = children {
            if children.binary_search(&tag).is_ok() { return Some(parent); }
        }
    }
    None
}
pub(super) fn nonlikely_script_is_root() -> bool { bundle(BASE).and_then(|bundle| bundle.nonlikely_script).unwrap_or(false) }
pub(super) fn likely_script(language: &str) -> Option<&'static str> {
    map(MAPS, "likelyScriptMap").iter().find_map(|(script, languages)| match languages {
        LocaleValue::Text(languages) if languages.split_ascii_whitespace().any(|candidate| candidate == language) => Some(*script), _ => None,
    })
}
pub(super) fn canonical_zone(zone: &str) -> Option<&'static str> {
    match lookup(map(MAPS, "tzCanonicalIDMap"), zone)? { LocaleValue::Text(value) => Some(value), _ => None }
}
pub(super) fn available(tag: &str) -> bool {
    tag.is_empty() || [BASE, NON_BASE].into_iter().filter_map(bundle).filter_map(|bundle| bundle.available)
        .any(|tags| tags.split_ascii_whitespace().any(|candidate| candidate == tag))
}

#[derive(Clone, Copy)]
pub(super) enum Family { Format, Zones, Calendar }
impl Family {
    pub fn resource(self, suffix: &str, cldr: bool) -> Option<&'static [(&'static str, LocaleValue)]> {
        let (package, family) = match self { Self::Format => ("text", "FormatData"), Self::Zones => ("util", "TimeZoneNames"), Self::Calendar => ("util", "CalendarData") };
        let base = if cldr { "cldr/" } else { "" };
        let suffix = if suffix.is_empty() { String::new() } else { format!("_{suffix}") };
        let name = format!("sun/{package}/resources/{base}{family}{suffix}");
        bundle(&name).or_else(|| bundle(&format!("sun/{package}/resources/{base}ext/{family}{suffix}"))).map(|bundle| bundle.entries)
    }
    pub fn value(self, suffix: &str, key: &str, cldr: bool) -> Option<LocaleValue> { lookup(self.resource(suffix, cldr)?, key) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pinned_locale_maps_supply_irregular_parents_and_canonical_zone_names() {
        assert_eq!(parent("en-GB"), Some("en-001"));
        assert_eq!(likely_script("en"), Some("Latn"));
        assert!(available("en-GB"));
        assert!(!available("zz-ZZ"));
        assert!(matches!(Family::Format.value("fr", "MonthNames", true), Some(LocaleValue::Array(names)) if names[0] == "janvier"));
        assert!(Family::Calendar.value("", "firstDayOfWeek", true).is_some());
        assert_eq!(canonical_zone("Asia/Kolkata"), Some("Asia/Calcutta"));
    }
}
