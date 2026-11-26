use std::sync::Arc;
use rtdb_types::{LegacyTimeZone,Result,decimal_digit};
use super::{date_calendar::{CalendarField,CalendarFields,CalendarSystem},date_environment::DateEnvironment,date_names::{NameField,NameStyle},date_zone_names::{ZoneDisplay,ZoneNames},error};
use CalendarField::*;

enum Part { Literal(String),Field(char,usize) }
/// Compiled legacy date pattern with a retained locale profile and caller-owned memory budget.
/// Calls that parse or format require exclusive access to its reusable timezone caches.
pub struct DatePattern {
    parts: Vec<Part>,
    environment: Arc<DateEnvironment>,
    calendar: CalendarSystem,
    zone: LegacyTimeZone,
    zone_names: Option<ZoneNames>,
    zone_display: Option<ZoneDisplay>,
    pivot: i64,
    pivot_year: i32,
    standalone_month: bool,
    maximum_output: usize,
    _charge: Arc<rtdb_types::MemoryReservation>,
}
impl DatePattern {
    /// Bounds a UTF-8 result before compiling pattern text against the supplied locale.
    /// Returns an allocation overflow error if the bound cannot fit in `usize`.
    pub fn uncompiled_output_bound(text: &str,environment: &DateEnvironment) -> Result<usize> {
        environment.locale.maximum_text_bytes().checked_mul(8).and_then(|width|width.checked_add(64))
            .and_then(|width|text.len().checked_mul(width)).ok_or_else(||error("allocation-overflow","Date pattern output exceeds range"))
    }
    /// Compiles SimpleDateFormat fields and literals against the captured date environment.
    /// Selects UTC when `utc` is true and the captured calendar zone otherwise.
    /// Invalid patterns, unavailable calendar rules and admission failures return an error.
    pub fn compile(text: &str,environment: Arc<DateEnvironment>,utc: bool) -> Result<Self> {
        if text.len() > 1 << 20 { return Err(error("invalid-plugin-config","Date pattern exceeds its byte bound")); }
        let bound = text.len().checked_mul(64).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(||error("allocation-overflow","Date pattern allocation exceeds range"))?;
        let charge = environment.memory().reserve(bound)?;
        let mut parts = Vec::new();let mut literal = String::new();let mut quote = false;let mut chars = text.chars().peekable();
        while let Some(character) = chars.next() {
            if character == '\'' {
                if chars.peek() == Some(&'\'') { chars.next();literal.push('\''); } else { quote = !quote; }continue;
            }
            if !quote && character.is_ascii_alphabetic() {
                if !literal.is_empty() { parts.push(Part::Literal(std::mem::take(&mut literal))); }
                let mut count = 1;while chars.peek() == Some(&character) { chars.next();count += 1; }
                if !"GyMdkHmsSEDFwWahKzZYuXL".contains(character) || character == 'X' && count > 3 {
                    return Err(error("invalid-plugin-config",format!("Invalid date pattern field '{character}' with width {count}")));
                }
                parts.push(Part::Field(character,count));
            } else { literal.push(character); }
        }
        if quote { return Err(error("invalid-plugin-config","Unterminated quoted date pattern")); }
        if !literal.is_empty() { parts.push(Part::Literal(literal)); }
        let standalone_month = parts.iter().filter(|part|matches!(part,Part::Field(..))).count() == 1 && parts.iter().any(|part|matches!(part,Part::Field('M',_)));
        let calendar = CalendarSystem::new(environment.locale.calendar,environment.locale.first_day,environment.locale.minimal_days)?;
        let pivot = calendar.subtract_years(environment.constructed_at(),environment.calendar_zone,80)?;
        let pivot_year = calendar.view(pivot,environment.calendar_zone)?.get(Year);
        let zone_bound = if parts.iter().any(|part|matches!(part,Part::Field('z',_))) { Self::uncompiled_output_bound("z",&environment)? } else { 0 };
        let maximum_output = parts.iter().try_fold(0usize,|size,part| {
            let length = match part { Part::Literal(value) => value.len(),Part::Field(_,width) => environment.number.bound(*width).max(zone_bound).max(environment.names.iter().map(super::date_names::FieldNames::maximum_name_bytes).max().unwrap_or(0)).max(16) };
            size.checked_add(length).ok_or_else(||error("allocation-overflow","Date pattern output exceeds range"))
        })?;
        let zone = if utc { LegacyTimeZone::parse("UTC")? } else { environment.calendar_zone };
        Ok(Self { parts,environment,calendar,zone,zone_names: None,zone_display: None,pivot,pivot_year,standalone_month,maximum_output,_charge: charge })
    }
    /// Returns the UTF-8 result bound to admit before formatting.
    pub fn maximum_output_bytes(&self) -> usize { self.maximum_output }
    fn ensure_zone_names(&mut self) -> Result<()> {
        if self.zone_names.is_none() { self.zone_names = Some(ZoneNames::new(&self.environment.locale,&||self.environment.observed_at(),self.environment.memory())?); }
        Ok(())
    }
    /// Formats signed Date milliseconds, returning timezone and cache admission failures.
    /// The caller admits the transient UTF-8 output using `maximum_output_bytes`.
    pub fn format(&mut self,millis: i64) -> Result<String> {
        let view = self.calendar.view(millis,self.zone)?;
        let mut output = String::with_capacity(self.maximum_output);
        for index in 0..self.parts.len() {
            let part = &self.parts[index];
            let Part::Field(mut field,count) = *part else { if let Part::Literal(literal) = part { output.push_str(literal); }continue; };
            if field == 'Y' && !self.calendar.week_date_supported() { field = 'y'; }
            let style = NameStyle { long: count >= 4,standalone: field == 'L' || field == 'M' && self.standalone_month };
            let name_field = name_field(field);
            let value = if field == 'Y' { view.week_year() } else if field == 'u' { let day = view.get(DayOfWeek);if day == 1 { 7 } else { day - 1 } }
                else { view.get(calendar_field(field)) };
            let name = name_field.and_then(|name| {
                if matches!(field,'M' | 'L') && count < 3 || matches!(field,'y' | 'Y') && self.calendar.gregorian_compatible() { None }
                else { self.environment.names(name).display(value,style) }
            });
            if let Some(name) = name { output.push_str(name);continue; }
            match field {
                'G' => (),
                'z' => {
                    if !self.zone_display.as_ref().is_some_and(|display|display.matches(self.zone)) {
                        self.zone_display = Some(ZoneDisplay::new(&self.environment.locale,self.zone,&||self.environment.observed_at(),self.environment.memory())?);
                    }
                    output.push_str(self.zone_display.as_ref().unwrap().display(view.get(DstOffset) != 0,count >= 4));
                }
                'Z' | 'X' => zone_number(&mut output,view.get(ZoneOffset).wrapping_add(view.get(DstOffset)),field,count),
                'y' | 'Y' if self.calendar.gregorian_compatible() => self.environment.number.append(value,count,(count == 2).then_some(2),&mut output),
                'y' | 'Y' => self.environment.number.append(value,if count >= 4 { 1 } else { count },None,&mut output),
                'M' | 'L' => self.environment.number.append(value.wrapping_add(1),count,None,&mut output),
                'k' => self.environment.number.append(if value == 0 { 24 } else { value },count,None,&mut output),
                'h' => self.environment.number.append(if value == 0 { 12 } else { value },count,None,&mut output),
                _ => self.environment.number.append(value,count,None,&mut output),
            }
        }
        Ok(output)
    }
    /// Parses a nonempty prefix into signed Date milliseconds using the lenient calendar.
    pub fn read(&mut self,text: &str) -> Result<i64> { self.read_prefix(text).and_then(|(millis,consumed)|if consumed == 0 { Err(mismatch()) } else { Ok(millis) }) }
    /// Parses a prefix and returns signed Date milliseconds and the consumed UTF-8 byte count.
    /// Invalid input and temporary allocation failures return an error.
    pub fn read_prefix(&mut self,text: &str) -> Result<(i64,usize)> {
        let bound = text.len().checked_mul(4).and_then(|size|size.checked_add(4096)).ok_or_else(||error("allocation-overflow","Date parse scratch exceeds range"))?;
        let _scratch = self.environment.memory().reserve(bound)?;
        let mut fields = CalendarFields::new();let mut position = 0usize;let mut ambiguous = false;
        for index in 0..self.parts.len() {
            let part = &self.parts[index];
            let Part::Field(mut field,count) = *part else {
                if let Part::Literal(literal) = part {
                    for expected in literal.chars() {
                        let actual = text[position..].chars().next().ok_or_else(mismatch)?;
                        if actual != expected && !(space_separator(actual) && space_separator(expected)) { return Err(mismatch()); }
                        position += actual.len_utf8();
                    }
                }
                continue;
            };
            if field == 'Y' && !self.calendar.week_date_supported() { field = 'y'; }
            let start = position;
            while text.as_bytes().get(position).is_some_and(|byte|matches!(*byte,b' ' | b'\t')) { position += 1; }
            if position == text.len() { return Err(mismatch()); }
            let actual_start = position;
            if matches!(field,'z' | 'Z' | 'X') {
                if field == 'X' && text.as_bytes()[position] == b'Z' { fields.set(ZoneOffset,0);fields.set(DstOffset,0);position += 1;continue; }
                let mut zone_start = position;
                let gmt = field != 'X' && super::date_names::insensitive_prefix(&text[start..],"GMT") == Some(3);
                if gmt { zone_start = start + 3; }
                let sign = match text.as_bytes().get(zone_start) { Some(b'+') => Some(1),Some(b'-') => Some(-1),_ => None };
                if let Some(sign) = sign {
                    let (offset,end) = numeric_zone(text,zone_start + 1,sign,if field == 'X' { count } else { 0 },gmt || field == 'X' && count == 3)?;
                    fields.set(ZoneOffset,offset);fields.set(DstOffset,0);position = end;
                } else if gmt { fields.set(ZoneOffset,0);fields.set(DstOffset,0);position = zone_start; }
                else if field == 'X' { return Err(mismatch()); }
                else {
                    self.ensure_zone_names()?;
                    let matched = self.zone_names.as_ref().unwrap().match_prefix(&text[position..],self.zone,self.environment.default_zone).ok_or_else(mismatch)?;
                    self.zone = matched.zone;
                    if let Some(saving) = matched.saving { fields.clear(ZoneOffset);fields.set(DstOffset,saving); }
                    position += matched.consumed;
                }
                continue;
            }
            let textual = matches!(field,'G' | 'E' | 'a') || matches!(field,'M' | 'L') && count >= 3;
            if textual {
                let name = name_field(field).unwrap();
                let styles: &[NameStyle] = match field {
                    'M' if self.standalone_month => &[NameStyle::SHORT_STANDALONE,NameStyle::LONG_STANDALONE],
                    'M' => &[NameStyle::SHORT,NameStyle::LONG],
                    'E' => &[NameStyle::LONG,NameStyle::SHORT],
                    _ => &[NameStyle::SHORT,NameStyle::SHORT_STANDALONE,NameStyle::LONG,NameStyle::LONG_STANDALONE],
                };
                let names = self.environment.names(name);
                let matched = if field == 'E' { styles.iter().find_map(|style|names.match_prefix(&text[start..],std::slice::from_ref(style))) }
                    else { names.match_prefix(&text[start..],styles) };
                let (value,consumed) = matched.ok_or_else(mismatch)?;
                fields.set(calendar_field(field),value);position = start + consumed;continue;
            }
            let obey = self.parts.get(index + 1).is_some_and(|part|matches!(part,Part::Field(next,width) if numeric_field(*next,*width)));
            let limit = if obey { utf16_limit(&text[start..],count).map(|length|start + length).ok_or_else(mismatch)? } else { text.len() };
            let number = (actual_start <= limit).then(||self.environment.number.parse(&text[actual_start..limit])).flatten();
            if matches!(field,'y' | 'Y') && !self.calendar.gregorian_compatible() {
                if let Some((value,consumed)) = self.environment.names(NameField::Year).match_prefix(&text[start..],&[if count >= 4 { NameStyle::LONG } else { NameStyle::SHORT }]) {
                    fields.set(Year,value);position = start + consumed;continue;
                }
                if number.is_none() { fields.set(Year,0);position = actual_start;continue; }
            }
            let (mut value,consumed) = number.ok_or_else(mismatch)?;
            position = actual_start + consumed;
            if let Some(minus) = self.environment.number.following_minus() {
                let delimiter = self.parts.get(index + 1).is_some_and(|part|matches!(part,Part::Literal(value) if value.starts_with(minus)));
                if delimiter && value < 0 && (position < text.len() && !text[position..].starts_with(minus) || position == text.len() && text[..position].ends_with(minus)) {
                    value = value.wrapping_neg();position -= minus.len_utf8();
                }
            }
            if matches!(field,'y' | 'Y') && self.calendar.gregorian_compatible() && count <= 2
                && text[actual_start..position].encode_utf16().count() == 2 && text[actual_start..position].chars().all(|character|decimal_digit(character).is_some()) {
                let two_digit = self.pivot_year % 100;
                ambiguous = value == two_digit;
                value = value.wrapping_add((self.pivot_year / 100).wrapping_mul(100)).wrapping_add(if value < two_digit { 100 } else { 0 });
            }
            value = match field { 'M' | 'L' => value.wrapping_sub(1),'k' if value == 24 => 0,'h' if value == 12 => 0,'u' if (1..=7).contains(&value) => if value == 7 { 1 } else { value + 1 },_ => value };
            if field == 'Y' { fields.set_week_year(value); } else { fields.set(calendar_field(field),value); }
        }
        let mut millis = self.calendar.resolve(&fields,self.zone)?;
        if ambiguous && millis < self.pivot { fields.add_year(100);millis = self.calendar.resolve(&fields,self.zone)?; }
        Ok((millis,position))
    }
}

fn calendar_field(field: char) -> CalendarField {
    match field { 'G' => Era,'y' | 'Y' => Year,'M' | 'L' => Month,'d' => DayOfMonth,'k' | 'H' => HourOfDay,'m' => Minute,'s' => Second,'S' => Millisecond,
        'E' | 'u' => DayOfWeek,'D' => DayOfYear,'F' => DayOfWeekInMonth,'w' => WeekOfYear,'W' => WeekOfMonth,'a' => AmPm,'h' | 'K' => Hour,_ => ZoneOffset }
}
fn name_field(field: char) -> Option<NameField> { match field { 'G' => Some(NameField::Era),'y' | 'Y' => Some(NameField::Year),'M' | 'L' => Some(NameField::Month),'E' => Some(NameField::Weekday),'a' => Some(NameField::AmPm),_ => None } }
fn numeric_field(field: char,count: usize) -> bool { match field { 'M' | 'L' => count <= 2,'y' | 'Y' | 'd' | 'k' | 'H' | 'm' | 's' | 'S' | 'D' | 'F' | 'w' | 'W' | 'h' | 'K' | 'u' => true,_ => false } }
fn utf16_limit(text: &str,count: usize) -> Option<usize> {
    let mut units = 0;
    for (at,character) in text.char_indices() { units += character.len_utf16();if units >= count { return Some(if units == count { at + character.len_utf8() } else { at }); } }
    None
}
fn space_separator(character: char) -> bool { matches!(character,' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}') }
fn numeric_zone(text: &str,start: usize,sign: i32,count: usize,colon: bool) -> Result<(i32,usize)> {
    let mut position = start;
    let digit = |position: &mut usize| { let value = text.as_bytes().get(*position).filter(|byte|byte.is_ascii_digit()).map(|byte|i32::from(*byte - b'0')).ok_or_else(mismatch)?;*position += 1;Ok::<_,rtdb_types::Anomaly>(value) };
    let mut hours = digit(&mut position)?;
    if text.as_bytes().get(position).is_some_and(u8::is_ascii_digit) { hours = hours * 10 + digit(&mut position)?; }
    else if count > 0 || !colon { return Err(mismatch()); }
    let mut minutes = 0;
    if count != 1 {
        if colon { if text.as_bytes().get(position) != Some(&b':') { return Err(mismatch()); }position += 1; }
        minutes = digit(&mut position)? * 10 + digit(&mut position)?;
    }
    if hours > 23 || minutes > 59 { return Err(mismatch()); }
    Ok(((hours * 60 + minutes) * 60_000 * sign,position))
}
fn zone_number(output: &mut String,offset: i32,field: char,count: usize) {
    use std::fmt::Write;
    if field == 'X' && offset == 0 { output.push('Z');return; }
    let minutes = offset / 60_000;output.push(if minutes < 0 { '-' } else { '+' });
    write!(output,"{:02}",minutes.abs() / 60).unwrap();
    if field != 'X' || count != 1 { if field == 'X' && count == 3 { output.push(':'); }write!(output,"{:02}",minutes.abs() % 60).unwrap(); }
}
fn mismatch() -> rtdb_types::Anomaly { error("transform-error","Timestamp does not match its date pattern") }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::CivilDate;
    use rtdb_types::MemoryBudget;
    use super::super::date_environment::DateEnvironmentInput;
    use std::collections::BTreeMap;
    fn environment(language: &str,region: &str,zone: &str,clock: i64) -> Arc<DateEnvironment> {
        DateEnvironment::from_inputs(DateEnvironmentInput { language: language.into(),script: String::new(),region: region.into(),variant: String::new(),unicode: BTreeMap::new(),timezone: zone.into(),initialized_at: clock,observed_at: clock,constructed_at: clock },&MemoryBudget::new(256 << 20)).unwrap()
    }
    fn midnight(year: i32,month: u32,day: u32) -> i64 { CivilDate::from_ymd(year,month,day).unwrap().epoch_day() * 86_400_000 }
    #[test]
    fn two_digit_pivot_uses_construction_zone_before_timestamp_transform_utc_override() {
        let clock = midnight(2020,6,18) + 12 * 3_600_000;
        let mut pattern = DatePattern::compile("yy-MM-dd HH:mm",environment("en","US","Europe/Paris",clock),true).unwrap();
        assert_eq!(pattern.read("40-06-18 11:59").unwrap(),midnight(2040,6,18) + 11 * 3_600_000 + 59 * 60_000);
        assert_eq!(pattern.read("40-06-18 12:00 trailing").unwrap(),midnight(1940,6,18) + 12 * 3_600_000);
    }
    #[test]
    fn localized_names_calendar_fields_and_lenient_space_literals_are_read() {
        let mut pattern = DatePattern::compile("d MMMM yyyy HH:mm",environment("fr","FR","UTC",0),true).unwrap();
        let date = midnight(2020,2,3) + 4 * 3_600_000 + 5 * 60_000;
        assert_eq!(pattern.format(date).unwrap(),"3 février 2020 04:05");
        assert_eq!(pattern.read("3\u{a0}FÉVRIER 2020 04:05").unwrap(),date);
        let mut compact = DatePattern::compile("yyyyMMdd",environment("en","US","UTC",0),true).unwrap();
        assert_eq!(compact.read("20200203").unwrap(),midnight(2020,2,3));
    }
    #[test]
    fn named_zone_parsing_updates_the_passive_formatter_even_when_later_fields_fail() {
        let mut pattern = DatePattern::compile("z yyyy",environment("en","US","UTC",0),true).unwrap();
        assert!(pattern.read("PST wrong").is_err());
        assert_eq!(pattern.zone.to_string(),"America/Los_Angeles");
        assert!(pattern.format(0).unwrap().starts_with("PST "));
    }
    #[test]
    fn zone_display_and_numeric_zone_parsing_leave_the_full_parse_catalog_uninitialized() {
        let mut pattern = DatePattern::compile("z",environment("en","US","America/Chicago",0),false).unwrap();
        assert!(pattern.zone_names.is_none());assert!(pattern.zone_display.is_none());
        assert_eq!(pattern.format(0).unwrap(),"CST");
        assert!(pattern.zone_names.is_none());assert!(pattern.zone_display.is_some());
        assert_eq!(pattern.read("GMT+02:00").unwrap(),-7_200_000);
        assert!(pattern.zone_names.is_none());
        assert!(pattern.read("CST").is_ok());
        assert!(pattern.zone_names.is_some());
    }
    #[test]
    fn legacy_pattern_offset_forms_remain_ascii_and_keep_subminute_sign_rules() {
        let environment = environment("ar","EG","GMT-00:00:30",0);
        let mut pattern = DatePattern::compile("Z X XX XXX",environment,false).unwrap();
        assert_eq!(pattern.format(0).unwrap(),"+0000 +00 +0000 +00:00");
    }
    #[test]
    fn empty_pattern_does_not_turn_a_zero_consumption_parse_into_a_date() {
        let mut pattern = DatePattern::compile("",environment("en","US","UTC",0),false).unwrap();
        assert!(pattern.read("text").is_err());
        assert_eq!(pattern.format(0).unwrap(),"");
    }
    #[test]
    fn wide_numeric_fields_keep_decimal_format_long_digit_limit() {
        let mut pattern = DatePattern::compile(&"S".repeat(400),environment("en","US","UTC",0),false).unwrap();
        assert_eq!(pattern.format(7).unwrap(),format!("{}7","0".repeat(308)));
        let mut year = DatePattern::compile(&"y".repeat(400),environment("en","US","UTC",0),false).unwrap();
        assert_eq!(year.format(0).unwrap(),format!("{}1970","0".repeat(305)));
    }
    #[test]
    fn japanese_year_text_fallback_keeps_zero_value_and_parse_position_on_numeric_failure() {
        let memory = MemoryBudget::new(256 << 20);
        let environment = DateEnvironment::from_inputs(DateEnvironmentInput { language: "ja".into(),script: String::new(),region: "JP".into(),variant: "JP".into(),unicode: BTreeMap::new(),timezone: "UTC".into(),initialized_at: 0,observed_at: 0,constructed_at: 0 },&memory).unwrap();
        let mut pattern = DatePattern::compile("y'!'-MM-dd",environment,false).unwrap();
        assert_eq!(pattern.read("!-01-01").unwrap(),midnight(2018,1,1));
    }
    #[test]
    fn format_and_parse_reservations_remain_owned_until_their_operation_finishes() {
        let memory = MemoryBudget::new(16 << 20);
        let environment = DateEnvironment::from_inputs(DateEnvironmentInput { language: "en".into(),script: String::new(),region: "US".into(),variant: String::new(),unicode: BTreeMap::new(),timezone: "UTC".into(),initialized_at: 0,observed_at: 0,constructed_at: 0 },&memory).unwrap();
        let before = memory.used_bytes();
        let mut pattern = DatePattern::compile("yyyy-MM-dd",Arc::clone(&environment),false).unwrap();
        let retained = memory.used_bytes();assert!(retained > before);
        assert_eq!(pattern.read("1970-01-01").unwrap(),0);
        assert_eq!(memory.used_bytes(),retained);
        drop(pattern);assert_eq!(memory.used_bytes(),before);
        drop(environment);assert_eq!(memory.used_bytes(),0);
    }
}
