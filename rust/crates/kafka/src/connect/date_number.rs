use rtdb_types::{Result,decimal_digit};
use super::{date_locale::LocaleData,error};

const LONG_INTEGER_DIGITS: usize = 309;

pub(super) struct DateNumber {
    zero: char,
    decimal: char,
    minus: &'static str,
    exponent: &'static str,
    infinity: &'static str,
    nan: &'static str,
    positive: (String,String),
    negative: (String,String),
    following_minus: Option<char>,
}
impl DateNumber {
    pub fn new(locale: &LocaleData) -> Result<Self> {
        let elements = locale.numbers("NumberElements").ok_or_else(missing)?;
        let element = |index: usize| elements.get(index).copied().ok_or_else(missing);
        let zero = element(4)?.chars().next().filter(|value|value.len_utf16() == 1).ok_or_else(missing)?;
        let decimal = element(0)?.chars().next().ok_or_else(missing)?;
        let minus = element(6)?;
        let pattern = locale.numbers("NumberPatterns").and_then(|patterns|patterns.first()).copied().ok_or_else(missing)?;
        let (positive,negative) = affixes(pattern,minus)?;
        let following_minus = pattern.split_once(';').and_then(|(_,negative)|negative.find('-').filter(|at|Some(*at) > negative.rfind(['0','#'])))
            .map(|_|minus.chars().find(|character|!matches!(*character,'\u{061c}' | '\u{200e}' | '\u{200f}' | '\u{2066}'..='\u{2069}')).unwrap_or('-'));
        Ok(Self { zero,decimal,minus,exponent: element(7)?,infinity: element(9)?,nan: element(10)?,positive,negative,following_minus })
    }
    pub fn append(&self,value: i32,minimum: usize,maximum: Option<usize>,out: &mut String) {
        let maximum = maximum.map(|maximum|maximum.min(LONG_INTEGER_DIGITS));
        let minimum = minimum.min(maximum.unwrap_or(LONG_INTEGER_DIGITS));
        let direct = (0..100).contains(&value) && (1..=2).contains(&minimum)
            || (1000..10000).contains(&value) && (minimum == 4 || minimum == 2 && maximum == Some(2));
        let (prefix,suffix) = if value < 0 { &self.negative } else { &self.positive };
        if !direct { out.push_str(prefix); }
        let digits = value.unsigned_abs().to_string();
        let digits = maximum.map_or(digits.as_str(),|maximum|&digits[digits.len().saturating_sub(maximum)..]);
        for _ in digits.len()..minimum { out.push(self.zero); }
        for byte in digits.bytes() { out.push(char::from_u32(self.zero as u32 + u32::from(byte - b'0')).unwrap()); }
        if !direct { out.push_str(suffix); }
    }
    pub fn parse(&self,text: &str) -> Option<(i32,usize)> {
        if !self.nan.is_empty() && text.starts_with(self.nan) { return Some((0,self.nan.len())); }
        let positive = text.starts_with(&self.positive.0);
        let negative = text.starts_with(&self.negative.0);
        let selected = match (positive,negative) {
            (false,false) => return None,
            (true,true) if self.positive.0.len() != self.negative.0.len() => Some(self.negative.0.len() > self.positive.0.len()),
            (true,true) => None,
            (_,negative) => Some(negative),
        };
        let prefix_length = selected.map_or(self.positive.0.len(),|negative|if negative { self.negative.0.len() } else { self.positive.0.len() });
        let rest = &text[prefix_length..];
        let (magnitude,body_end,integer_end) = if !self.infinity.is_empty() && rest.starts_with(self.infinity) { (Magnitude::Overflow,self.infinity.len(),None) }
            else { self.integer(rest)? };
        let tail = &rest[body_end..];
        let positive = positive && selected != Some(true) && tail.starts_with(&self.positive.1);
        let negative = negative && selected != Some(false) && tail.starts_with(&self.negative.1);
        let negative = match (positive,negative) {
            (true,false) => false,(false,true) => true,
            (true,true) if self.positive.1.len() != self.negative.1.len() => self.negative.1.len() > self.positive.1.len(),
            _ => return None,
        };
        let suffix = if negative { &self.negative.1 } else { &self.positive.1 };
        let value = match magnitude {
            Magnitude::Overflow => if negative { i32::MIN } else { i32::MAX },
            Magnitude::Floating(value) => (if negative { -value } else { value }) as i32,
            Magnitude::Integer(value) if value > i64::MAX as u64 + u64::from(negative) => if negative { i32::MIN } else { i32::MAX },
            Magnitude::Integer(value) => if negative { (value as i64).wrapping_neg() as i32 } else { value as i32 },
        };
        Some((value,prefix_length + integer_end.unwrap_or(body_end + suffix.len())))
    }
    fn integer(&self,text: &str) -> Option<(Magnitude,usize,Option<usize>)> {
        let mut first = None;
        let mut count = 0i64;
        let mut end = 0;
        let mut significant = [0u8;19];
        let mut last_nonzero = 0i64;
        for (index,character) in text.char_indices() {
            let Some(digit) = self.digit(character) else { break; };
            if first.is_some() || digit != 0 {
                first.get_or_insert(index);
                if count < significant.len() as i64 { significant[count as usize] = digit; }
                if digit != 0 { last_nonzero = count; }
                count += 1;
            }
            end = index + character.len_utf8();
        }
        if end == 0 { return None; }
        let digits_end = end;
        let integer_end = text[end..].starts_with(self.decimal).then_some(end);
        if integer_end.is_some() {
            end += self.decimal.len_utf8();
            for (index,character) in text[end..].char_indices() {
                if self.digit(character).is_none() { break; }
                end = digits_end + self.decimal.len_utf8() + index + character.len_utf8();
            }
        }
        let mut exponent = 0i64;
        if !self.exponent.is_empty() && text[end..].starts_with(self.exponent) {
            let start = end + self.exponent.len();
            let negative = !self.minus.is_empty() && text[start..].starts_with(self.minus);
            let start = start + if negative { self.minus.len() } else { 0 };
            let mut exponent_end = start;
            for (index,character) in text[start..].char_indices() {
                let Some(digit) = self.digit(character) else { break; };
                exponent = exponent.saturating_mul(10).saturating_add(i64::from(digit));
                exponent_end = start + index + character.len_utf8();
            }
            if exponent_end > start { end = exponent_end;if negative { exponent = exponent.saturating_neg(); } }
        }
        if integer_end.is_some() { exponent = 0; }
        let length = count.saturating_add(exponent);
        if first.is_none() || length < 0 { return Some((Magnitude::Integer(0),end,integer_end)); }
        if length > 19 { return Some((Magnitude::Overflow,end,integer_end)); }
        if length <= last_nonzero {
            let mut normalized: String = text[..digits_end].chars().map(|character|char::from(b'0' + self.digit(character).unwrap())).collect();
            normalized.push('e');normalized.push_str(&exponent.to_string());
            return Some((Magnitude::Floating(normalized.parse().ok()?),end,integer_end));
        }
        let mut magnitude = 0u64;
        for index in 0..length as usize { magnitude = magnitude * 10 + u64::from(if index < count as usize { significant[index] } else { 0 }); }
        Some((Magnitude::Integer(magnitude),end,integer_end))
    }
    fn digit(&self,character: char) -> Option<u8> {
        let localized = (character as u32).wrapping_sub(self.zero as u32);
        if localized <= 9 { Some(localized as u8) } else { decimal_digit(character) }
    }
    pub fn bound(&self,minimum: usize) -> usize { minimum.max(11).saturating_mul(4).saturating_add(self.positive.0.len() + self.positive.1.len() + self.negative.0.len() + self.negative.1.len()) }
    pub fn following_minus(&self) -> Option<char> { self.following_minus }
}
enum Magnitude { Integer(u64),Floating(f64),Overflow }

fn affixes(pattern: &str,minus: &str) -> Result<((String,String),(String,String))> {
    let mut patterns = vec![(String::new(),String::new())];
    let mut number = false;
    let mut suffix = false;
    let mut quoted = false;
    let mut chars = pattern.chars().peekable();
    while let Some(character) = chars.next() {
        if character == '\'' {
            if chars.peek() == Some(&'\'') { chars.next(); }
            else { quoted = !quoted;continue; }
        } else if !quoted && character == ';' {
            if patterns.len() == 2 { return Err(missing()); }
            patterns.push((String::new(),String::new()));number = false;suffix = false;continue;
        } else if !quoted && matches!(character,'#' | '0' | ',' | '.' | 'E') && !suffix {
            number = true;continue;
        }
        if number { suffix = true; }
        let part = patterns.last_mut().unwrap();
        let target = if suffix { &mut part.1 } else { &mut part.0 };
        if !quoted && character == '-' { target.push_str(minus); } else { target.push(character); }
    }
    if quoted || !number { return Err(missing()); }
    let positive = patterns.remove(0);
    let negative = patterns.pop().filter(|negative|*negative != positive).unwrap_or_else(||(format!("{minus}{}",positive.0),positive.1.clone()));
    Ok((positive,negative))
}
fn missing() -> rtdb_types::Anomaly { error("invalid-date-locale","Selected locale has invalid integer-number resources") }

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::date_locale::LocaleId;
    use std::collections::BTreeMap;
    fn english() -> DateNumber { DateNumber::new(&LocaleData::new(LocaleId::from_parts("en","","US","",BTreeMap::new()).unwrap()).unwrap()).unwrap() }
    #[test]
    fn integer_parser_preserves_number_intvalue_wrap_then_double_saturation() {
        let number = english();
        assert_eq!(number.parse("2147483648"),Some((i32::MIN,10)));
        assert_eq!(number.parse("9223372036854775807"),Some((-1,19)));
        assert_eq!(number.parse("9223372036854775808"),Some((i32::MAX,19)));
        assert_eq!(number.parse("-9223372036854775808"),Some((0,20)));
        assert_eq!(number.parse("1E3tail"),Some((1000,3)));
        assert_eq!(number.parse("123E-2"),Some((1,6)));
        assert_eq!(number.parse("123.456"),Some((123,3)));
        assert_eq!(number.parse("NaN"),Some((0,3)));
        assert_eq!(number.parse("+1"),None);
        assert_eq!(number.parse("9223372036854775807E-1"),Some((i32::MAX,22)));
        let rounded = "-214748364799999999999E-11";
        assert_eq!(number.parse(rounded),Some((i32::MIN,rounded.len())));
        assert_eq!(number.parse("21474836480E-1"),Some((i32::MIN,14)));
        assert_eq!(number.parse("123.456E3tail"),Some((123,3)));
    }
    #[test]
    fn integer_only_parser_checks_suffix_after_fraction_but_returns_decimal_position() {
        let mut number = english();number.positive.1 = "!".into();number.negative.1 = "!".into();
        assert_eq!(number.parse("12.5!rest"),Some((12,2)));
        assert_eq!(number.parse("12.5rest"),None);
        assert_eq!(number.parse("12!rest"),Some((12,3)));
    }
    #[test]
    fn date_number_short_positive_fields_bypass_decimal_format_affixes() {
        let mut number = english();number.positive = ("prefix".into(),"suffix".into());
        let mut output = String::new();number.append(3,2,None,&mut output);assert_eq!(output,"03");
        output.clear();number.append(2020,4,None,&mut output);assert_eq!(output,"2020");
        output.clear();number.append(2020,2,Some(2),&mut output);assert_eq!(output,"20");
        output.clear();number.append(123,3,None,&mut output);assert_eq!(output,"prefix123suffix");
    }
    #[test]
    fn date_number_long_formatting_clamps_integer_padding_to_309_digits() {
        let number = english();
        for minimum in [308,309,310,1024] {
            for value in [7,-7] {
                let mut output = String::new();number.append(value,minimum,None,&mut output);
                let digits = output.trim_start_matches('-');
                assert_eq!(digits.len(),minimum.min(309));
                assert_eq!(digits.trim_start_matches('0'),"7");
                assert_eq!(output.starts_with('-'),value < 0);
            }
        }
    }
    #[test]
    fn localized_digits_and_bidi_negative_affixes_are_captured_from_the_bundle() {
        let locale = LocaleData::new(LocaleId::from_parts("ar","","EG","",BTreeMap::new()).unwrap()).unwrap();
        let number = DateNumber::new(&locale).unwrap();
        let mut output = String::new();number.append(-12,4,None,&mut output);
        assert_eq!(number.parse(&output),Some((-12,output.len())));
        assert!(output.contains('١'));
        assert_eq!(english().parse("١٢"),Some((12,"١٢".len())));
    }
}
