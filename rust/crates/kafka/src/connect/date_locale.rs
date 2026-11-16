use std::collections::BTreeMap;
use rtdb_types::Result;
use super::{error, locale_data::{self, Family, LocaleValue}};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LocaleId {
    pub language: String,
    pub script: String,
    pub region: String,
    pub variant: String,
    pub extensions: BTreeMap<String, String>,
}
impl LocaleId {
    pub fn from_parts(language: &str, script: &str, region: &str, variant: &str, extensions: BTreeMap<String, String>) -> Result<Self> {
        let bytes = [language, script, region, variant].into_iter().chain(extensions.iter().flat_map(|(key, value)| [key.as_str(), value.as_str()])).try_fold(0usize, |total, value| total.checked_add(value.len()).ok_or_else(|| error("invalid-date-locale", "Locale identifier length overflow")))?;
        if bytes > 1024 || ![language, script, region, variant].into_iter().all(str::is_ascii)
            || extensions.iter().any(|(key, value)| key.len() != 2 || !key.bytes().all(|byte| byte.is_ascii_alphanumeric()) || !value.is_ascii()) {
            return Err(error("invalid-date-locale", "Locale identifier exceeds its bound or contains invalid fields"));
        }
        let mut normalized = BTreeMap::new();
        for (key,value) in extensions {
            if !value.is_empty() && value.split('-').any(|part|!(3..=8).contains(&part.len()) || !part.bytes().all(|byte|byte.is_ascii_alphanumeric())) {
                return Err(error("invalid-date-locale","Unicode locale type is not a sequence of three-to-eight character subtags"));
            }
            if normalized.insert(key.to_ascii_lowercase(),value.to_ascii_lowercase()).is_some() { return Err(error("invalid-date-locale","Unicode locale key is duplicated after case normalization")); }
        }
        let language = match language.to_ascii_lowercase().as_str() { "iw" => "he".into(), "ji" => "yi".into(), "in" => "id".into(), other => other.to_owned() };
        let mut script = script.to_ascii_lowercase();
        if let Some(first) = script.get_mut(..1) { first.make_ascii_uppercase(); }
        Ok(Self { language, script, region: region.to_ascii_uppercase(), variant: variant.into(), extensions: normalized })
    }
    fn root() -> Self { Self { language: String::new(), script: String::new(), region: String::new(), variant: String::new(), extensions: BTreeMap::new() } }
    fn base(&self) -> Self { Self { extensions: BTreeMap::new(), ..self.clone() } }
    fn resource_suffix(&self) -> String {
        let mut value = self.language.clone();
        if !self.script.is_empty() { value.push('_'); value.push_str(&self.script); }
        if !self.region.is_empty() || !self.variant.is_empty() { value.push('_'); value.push_str(&self.region); }
        if !self.variant.is_empty() { value.push('_'); value.push_str(&self.variant); }
        value
    }
    fn tag(&self) -> String {
        let mut value = self.language.clone();
        for part in [&self.script, &self.region, &self.variant] {
            if !part.is_empty() { value.push('-'); value.push_str(&part.replace('_', "-")); }
        }
        if !self.extensions.is_empty() {
            value.push_str("-u");
            for (key, kind) in &self.extensions { value.push('-'); value.push_str(key); if !kind.is_empty() { value.push('-'); value.push_str(kind); } }
        }
        value
    }
    fn canonical_tag(tag: &str) -> Result<Self> {
        if tag.is_empty() || tag == "und" { return Ok(Self::root()); }
        let mut parts = tag.split('-').peekable();
        let language = parts.next().unwrap();
        let script = if parts.peek().is_some_and(|part| part.len() == 4 && part.bytes().all(|byte| byte.is_ascii_alphabetic())) { parts.next().unwrap() } else { "" };
        let region = if parts.peek().is_some_and(|part| part.len() == 2 || part.len() == 3 && part.bytes().all(|byte| byte.is_ascii_digit())) { parts.next().unwrap() } else { "" };
        let variant = parts.collect::<Vec<_>>().join("_");
        Self::from_parts(language, script, region, &variant, BTreeMap::new())
    }
    fn default_candidates(&self) -> Vec<Self> {
        let mut source = self.base();
        if source.language == "zh" && source.script.is_empty() {
            source.script = match source.region.as_str() { "TW" | "HK" | "MO" => "Hant", "CN" | "SG" => "Hans", _ => "" }.into();
        }
        let mut variants = Vec::new();
        let mut variant = source.variant.as_str();
        while !variant.is_empty() { variants.push(variant.to_owned()); variant = variant.rsplit_once('_').map_or("", |(prefix, _)| prefix); }
        let mut result = Vec::new();
        for variant in &variants { result.push(Self { variant: variant.clone(), ..source.clone() }); }
        if !source.region.is_empty() { result.push(Self { variant: String::new(), ..source.clone() }); }
        if !source.script.is_empty() {
            result.push(Self { region: String::new(), variant: String::new(), ..source.clone() });
            if source.language == "zh" && source.region.is_empty() { source.region = match source.script.as_str() { "Hans" => "CN", "Hant" => "TW", _ => "" }.into(); }
            source.script.clear();
            for variant in variants { result.push(Self { variant, ..source.clone() }); }
            if !source.region.is_empty() { result.push(Self { variant: String::new(), ..source.clone() }); }
        }
        if !source.language.is_empty() { result.push(Self { language: source.language, ..Self::root() }); }
        result.push(Self::root());
        result
    }
    fn candidates(&self) -> Vec<Self> {
        let mut source = self.base();
        let nynorsk = source.language == "no" && source.region == "NO" && source.variant == "NY";
        if source.language == "no" && !nynorsk || source.language == "nb" {
            let no_first = source.language == "no";
            source.language = "nb".into();
            return source.default_candidates().into_iter().flat_map(|nb| {
                if nb.language.is_empty() { return vec![nb]; }
                let no = Self { language: "no".into(), ..nb.clone() };
                if no_first { vec![no, nb] } else { vec![nb, no] }
            }).collect();
        }
        if nynorsk || source.language == "nn" {
            source.language = "nn".into();
            if nynorsk { source.variant.clear(); }
            let mut result = source.default_candidates();
            result.pop();
            result.push(Self { language: "no".into(), region: "NO".into(), variant: "NY".into(), ..Self::root() });
            result.push(Self { language: "no".into(), region: "NO".into(), ..Self::root() });
            result.push(Self { language: "no".into(), ..Self::root() });
            result.push(Self::root());
            return result;
        }
        source.default_candidates()
    }
    fn cldr_candidates(&self, depth: usize) -> Result<Vec<Self>> {
        if depth > 16 { return Err(error("invalid-date-locale", "Locale parent chain exceeds its bound")); }
        let mut result = self.candidates();
        for index in 0..result.len().saturating_sub(1) {
            let locale = &result[index];
            let tag = locale.tag();
            let parent = if matches!(tag.as_str(), "" | "en" | "en-US") { None }
                else { locale_data::parent(&tag).or_else(|| {
                    if locale_data::nonlikely_script_is_root() && locale.region.is_empty() && !locale.script.is_empty()
                        && locale_data::likely_script(&locale.language).is_some_and(|likely| likely != locale.script) { Some("") } else { None }
                }) };
            if let Some(parent) = parent {
                let parent = Self::canonical_tag(parent)?;
                if parent != result[index + 1] {
                    result.truncate(index + 1);
                    if matches!(parent.language.as_str(), "nb" | "no") && parent.region.is_empty() && parent.script.is_empty() && parent.variant.is_empty() {
                        result.push(Self { language: "no".into(), ..Self::root() }); result.push(Self::root());
                    } else { result.extend(parent.cldr_candidates(depth + 1)?); }
                    break;
                }
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CalendarKind { Gregorian, Buddhist, Japanese }
type Entries = &'static [(&'static str, LocaleValue)];
pub(super) struct LocaleData {
    pub locale: LocaleId,
    pub calendar: CalendarKind,
    pub first_day: u8,
    pub minimal_days: u8,
    formats: Vec<Entries>,
    zones: Vec<Entries>,
}
impl LocaleData {
    pub fn new(locale: LocaleId) -> Result<Self> {
        let calendar = match locale.extensions.get("ca").map(String::as_str) {
            Some("buddhist") => CalendarKind::Buddhist, Some("japanese") => CalendarKind::Japanese, Some("gregory") => CalendarKind::Gregorian,
            _ if locale.language == "th" && locale.region == "TH" => CalendarKind::Buddhist,
            _ if locale.language == "ja" && locale.region == "JP" && locale.variant == "JP" => CalendarKind::Japanese,
            _ => CalendarKind::Gregorian,
        };
        let alias = locale_data::alias(&locale.tag()).map(LocaleId::canonical_tag).transpose()?;
        let candidates = alias.as_ref().unwrap_or(&locale).cldr_candidates(0)?;
        let mut formats = Vec::new(); let mut zones = Vec::new();
        for candidate in candidates {
            if !locale_data::available(&candidate.tag()) { continue; }
            let suffix = candidate.resource_suffix();
            if let Some(resource) = Family::Format.resource(&suffix, true) { formats.push(resource); }
            if let Some(resource) = Family::Zones.resource(&suffix, true) { zones.push(resource); }
        }
        if formats.is_empty() || zones.is_empty() { return Err(error("invalid-date-locale", "Selected locale bundle lacks its root resources")); }
        let region = locale.extensions.get("rg").filter(|value| value.len() == 6 && value[..2].bytes().all(|byte| byte.is_ascii_alphabetic()) && value[2..].eq_ignore_ascii_case("zzzz")).map(|value| value[..2].to_ascii_uppercase());
        let region = region.as_deref().unwrap_or(if locale.region.is_empty() { "US" } else { &locale.region });
        let first_day = locale.extensions.get("fw").and_then(|value| match value.as_str() { "sun" => Some(1), "mon" => Some(2), "tue" => Some(3), "wed" => Some(4), "thu" => Some(5), "fri" => Some(6), "sat" => Some(7), _ => None }).unwrap_or_else(|| week_value("firstDayOfWeek", region, 1));
        let minimal_days = week_value("minimalDaysInFirstWeek", region, 1);
        Ok(Self { locale, calendar, first_day, minimal_days, formats, zones })
    }
    pub fn format(&self, key: &str) -> Option<LocaleValue> { self.formats.iter().find_map(|entries| lookup(entries, key)) }
    pub fn array(&self, key: &str) -> Option<&'static [&'static str]> { match self.format(key)? { LocaleValue::Array(value) => Some(value), _ => None } }
    pub fn text(&self, key: &str) -> Option<&'static str> { match self.format(key)? { LocaleValue::Text(value) => Some(value), _ => None } }
    pub fn numbers(&self, kind: &str) -> Option<&'static [&'static str]> {
        self.locale.extensions.get("nu").and_then(|system| self.array(&format!("{system}.{kind}")))
            .or_else(|| self.text("DefaultNumberingSystem").and_then(|system| self.array(&format!("{system}.{kind}"))))
            .or_else(|| self.array(kind))
    }
    pub fn zone(&self, key: &str) -> Option<LocaleValue> { self.zones.iter().find_map(|entries| lookup(entries, key)) }
    pub fn zone_sources(&self) -> &[Entries] { &self.zones }
    pub fn maximum_text_bytes(&self) -> usize {
        self.formats.iter().chain(&self.zones).flat_map(|entries|entries.iter()).map(|(_,value)|match value {
            LocaleValue::Text(value) => value.len(),LocaleValue::Array(values) => values.iter().map(|value|value.len()).max().unwrap_or(0),
        }).max().unwrap_or(0)
    }
}

fn lookup(entries: Entries, key: &str) -> Option<LocaleValue> { entries.iter().rev().find(|(candidate, _)| *candidate == key).map(|(_, value)| *value) }
fn week_value(key: &str, region: &str, fallback: u8) -> u8 {
    let Some(LocaleValue::Text(value)) = Family::Calendar.value("", key, true) else { return fallback; };
    let value_for = |region: &str| {
        let at = value.find(region)?;
        let start = value[..at].rfind(';').map_or(0, |index| index + 1);
        let colon = value[start..].find(':')? + start;
        value[start..colon].parse::<u8>().ok().filter(|value| (1..=7).contains(value))
    };
    value_for(region).or_else(|| value_for("001")).unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(language: &str, script: &str, region: &str) -> LocaleId { LocaleId::from_parts(language, script, region, "", BTreeMap::new()).unwrap() }
    #[test]
    fn candidate_lookup_preserves_chinese_scripts_and_irregular_english_parents() {
        let english = id("en", "", "GB").cldr_candidates(0).unwrap().into_iter().map(|locale| locale.tag()).collect::<Vec<_>>();
        assert_eq!(english, ["en-GB", "en-001", "en", ""]);
        let chinese = id("zh", "", "HK").candidates().into_iter().map(|locale| locale.resource_suffix()).collect::<Vec<_>>();
        assert_eq!(chinese, ["zh_Hant_HK", "zh_Hant", "zh_HK", "zh", ""]);
        assert!(LocaleData::new(id("zz", "", "ZZ")).is_ok());
    }
    #[test]
    fn locale_calendar_numbering_and_week_overrides_remain_independent() {
        let locale = LocaleData::new(id("th", "", "TH")).unwrap();
        assert_eq!(locale.calendar, CalendarKind::Buddhist);
        let mut extensions = BTreeMap::new();
        extensions.insert("ca".into(), "japanese".into()); extensions.insert("fw".into(), "wed".into()); extensions.insert("rg".into(), "gbzzzz".into()); extensions.insert("nu".into(), "arab".into());
        let locale = LocaleData::new(LocaleId::from_parts("en", "", "US", "", extensions).unwrap()).unwrap();
        assert_eq!(locale.calendar, CalendarKind::Japanese); assert_eq!(locale.first_day, 4); assert_eq!(locale.minimal_days, 4);
        assert!(locale.numbers("NumberElements").is_some());
        assert_eq!(LocaleData::new(id("fr", "", "FR")).unwrap().array("MonthNames").unwrap()[0], "janvier");
    }
}
