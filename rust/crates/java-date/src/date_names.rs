use super::date_locale::{CalendarKind, LocaleData};

#[path = "../../../vendor/fancy-regex/src/unicode_case.rs"]
mod unicode_case;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum NameField { Era, Year, Month, Weekday, AmPm }
#[derive(Clone, Copy)]
pub(super) struct NameStyle { pub long: bool, pub standalone: bool }
impl NameStyle {
    pub const SHORT: Self = Self { long: false, standalone: false };
    pub const LONG: Self = Self { long: true, standalone: false };
    pub const SHORT_STANDALONE: Self = Self { long: false, standalone: true };
    pub const LONG_STANDALONE: Self = Self { long: true, standalone: true };
    fn index(self) -> usize { usize::from(self.long) * 2 + usize::from(self.standalone) }
}

pub(super) struct FieldNames { field: NameField, arrays: [Option<&'static [&'static str]>; 4] }
impl FieldNames {
    pub fn new(locale: &LocaleData, field: NameField) -> Self {
        let arrays = std::array::from_fn(|index| {
            let style = NameStyle { long: index >= 2, standalone: index % 2 == 1 };
            let key = resource_key(locale.calendar,field,style);
            locale.array(&key).or_else(|| style.standalone.then(|| locale.array(&key.replacen("standalone.","",1))).flatten())
        });
        Self { field,arrays }
    }
    pub fn display(&self,value: i32,style: NameStyle) -> Option<&'static str> {
        let index = value.checked_sub(i32::from(matches!(self.field,NameField::Year | NameField::Weekday)))?;
        let value = self.arrays[style.index()]?.get(usize::try_from(index).ok()?).copied()?;
        if value.is_empty() && style.standalone { self.display(index,NameStyle { standalone: false,..style }) }
        else { Some(value) }
    }
    pub fn maximum_name_bytes(&self) -> usize { self.arrays.iter().flatten().flat_map(|names|names.iter()).map(|name|name.len()).max().unwrap_or(0) }
    pub fn match_prefix(&self,text: &str,styles: &[NameStyle]) -> Option<(i32,usize)> {
        let mut selected: Option<(&str,i32,usize)> = None;
        for style in styles {
            let Some(names) = self.arrays[style.index()] else { continue; };
            if self.field != NameField::AmPm && names.iter().enumerate().any(|(index,name)| !name.is_empty() && names[index + 1..].contains(name)) { continue; }
            let names = if self.field == NameField::Year { &names[..names.len().min(1)] }
                else if self.field == NameField::AmPm { &names[..names.len().min(2)] } else { names };
            for (index,name) in names.iter().enumerate().rev() {
                if name.is_empty() { continue; }
                let Some(consumed) = insensitive_prefix(text,name) else { continue; };
                let value = index as i32 + i32::from(matches!(self.field,NameField::Weekday | NameField::Year));
                let replace = selected.is_none_or(|(previous,_,_)| {
                    let length = name.encode_utf16().count().cmp(&previous.encode_utf16().count());
                    length.is_gt() || length.is_eq() && name.encode_utf16().cmp(previous.encode_utf16()).is_le()
                });
                if replace { selected = Some((name,value,consumed)); }
            }
        }
        selected.map(|(_,value,consumed)|(value,consumed))
    }
}

fn resource_key(calendar: CalendarKind,field: NameField,style: NameStyle) -> String {
    let calendar = match calendar { CalendarKind::Gregorian => "",CalendarKind::Buddhist => "buddhist.",CalendarKind::Japanese => "japanese." };
    let standalone = if style.standalone { "standalone." } else { "" };
    match field {
        NameField::Era => format!("{calendar}{}Eras",if style.long { "long." } else { "" }),
        NameField::Year => format!("{}FirstYear",if calendar.is_empty() { "null." } else { calendar }),
        NameField::Month => format!("{standalone}Month{}",if style.long { "Names" } else { "Abbreviations" }),
        NameField::Weekday => format!("{standalone}Day{}",if style.long { "Names" } else { "Abbreviations" }),
        NameField::AmPm => "AmPmMarkers".into(),
    }
}

pub(super) fn insensitive_prefix(text: &str,prefix: &str) -> Option<usize> {
    let mut text_chars = text.char_indices();
    let mut end = 0;
    for expected in prefix.chars() {
        let (at,actual) = text_chars.next()?;
        if actual.len_utf16() != expected.len_utf16() || !unicode_case::equivalent(actual,expected) { return None; }
        end = at + actual.len_utf8();
    }
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::date_locale::LocaleId;
    use std::collections::BTreeMap;

    #[test]
    fn contextual_and_standalone_month_names_keep_distinct_forms() {
        let locale = LocaleData::new(LocaleId::from_parts("ru","","RU","",BTreeMap::new()).unwrap()).unwrap();
        let months = FieldNames::new(&locale,NameField::Month);
        assert_eq!(months.display(0,NameStyle::LONG),Some("января"));
        assert_eq!(months.display(0,NameStyle::LONG_STANDALONE),Some("январь"));
        assert_eq!(months.match_prefix("ЯНВАРЯ остаток",&[NameStyle::SHORT,NameStyle::LONG]),Some((0,"ЯНВАРЯ".len())));
    }
    #[test]
    fn name_matching_uses_longest_names_and_discards_ambiguous_provider_arrays() {
        let names = FieldNames { field: NameField::Month,arrays: [Some(&["a","ab"]),None,Some(&["same","same"]),None] };
        assert_eq!(names.match_prefix("abc",&[NameStyle::SHORT]),Some((1,2)));
        assert_eq!(names.match_prefix("same",&[NameStyle::LONG]),None);
        let ampm = FieldNames { field: NameField::AmPm,arrays: [Some(&["same","same","evening"]),None,None,None] };
        assert_eq!(ampm.match_prefix("same",&[NameStyle::SHORT]),Some((0,4)));
        assert_eq!(ampm.match_prefix("evening",&[NameStyle::SHORT]),None);
    }
    #[test]
    fn simple_case_prefixes_preserve_utf16_width_without_full_case_expansion() {
        assert_eq!(insensitive_prefix("İstanbul","istanbul"),Some("İstanbul".len()));
        assert_eq!(insensitive_prefix("ẞ","ß"),Some("ẞ".len()));
        assert_eq!(insensitive_prefix("SS","ß"),None);
    }
    #[test]
    fn empty_standalone_weekday_name_uses_provider_recursive_value_adjustment() {
        let names = FieldNames { field: NameField::Weekday,arrays: [Some(&["Sunday","Monday"]),Some(&["",""]),None,None] };
        assert_eq!(names.display(1,NameStyle::SHORT_STANDALONE),None);
        assert_eq!(names.display(2,NameStyle::SHORT_STANDALONE),Some("Sunday"));
    }
}
