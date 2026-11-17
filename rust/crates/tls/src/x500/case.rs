const UPPER: &[u8] = include_bytes!("case/unicode16-upper.bin");
const LOWER: &[u8] = include_bytes!("case/unicode16-lower.bin");
const CASED: &[u8] = include_bytes!("case/java25-cased.bin");
const WORDS: &[u8] = include_bytes!("case/WordBreakIteratorData");
const SIMPLE_FOLD: &[u8] = include_bytes!("case/unicode16-simple-fold.bin");

pub(super) fn fold(value: &str) -> String {
    if value.is_ascii() { return value.to_ascii_lowercase(); }
    let mut upper = Vec::with_capacity(value.len());
    for character in value.chars() { map(character, UPPER, |mapped| upper.push(mapped)); }
    let mut result = String::with_capacity(value.len());
    lowercase(&upper, |code| result.push(char::from_u32(code).expect("case mapping preserves Unicode scalars")));
    result
}

pub(crate) fn lowercase_utf16(units: &[u16]) -> Vec<u16> {
    let codepoints = char::decode_utf16(units.iter().copied()).map(|value| match value {
        Ok(character) => character as u32, Err(error) => u32::from(error.unpaired_surrogate()),
    }).collect::<Vec<_>>();
    let mut result = Vec::with_capacity(units.len());
    lowercase(&codepoints, |code| {
        if let Some(character) = char::from_u32(code) { result.extend_from_slice(character.encode_utf16(&mut [0; 2])); }
        else { result.push(code as u16); }
    });
    result
}

pub(crate) fn equals_ignore_case_utf16(left: &[u16], right: &[u16]) -> bool {
    if left.len() != right.len() { return false; }
    let folded = |value: Result<char, std::char::DecodeUtf16Error>| {
        let code = match value { Ok(character) => character as u32, Err(error) => u32::from(error.unpaired_surrogate()) };
        let mut low = 0;
        let mut high = SIMPLE_FOLD.len() / 8;
        while low < high {
            let middle = low + (high - low) / 2;
            let row = &SIMPLE_FOLD[middle * 8..middle * 8 + 8];
            match number(row, 0).cmp(&code) {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => return number(row, 4),
            }
        }
        code
    };
    char::decode_utf16(left.iter().copied()).map(&folded).eq(char::decode_utf16(right.iter().copied()).map(&folded))
}

fn lowercase<T: Copy + Into<u32>>(text: &[T], mut push: impl FnMut(u32)) {
    let lower = |code: u32, push: &mut dyn FnMut(u32)| {
        if let Some(character) = char::from_u32(code) { map(character, LOWER, |mapped| push(mapped as u32)); }
        else { push(code); }
    };
    if !text.iter().any(|character| (*character).into() == 'Σ' as u32) {
        for character in text { lower((*character).into(), &mut push); }
        return;
    }
    let words = WordRules::new();
    let mut start = 0;
    while start < text.len() {
        let end = words.next(text, start);
        let word = &text[start..end];
        let mut cased = word.iter().enumerate().filter_map(|(index, character)| is_cased(*character).then_some(index));
        let first = cased.next();
        let last = cased.next_back().or(first);
        for (index, character) in word.iter().copied().enumerate() {
            if character.into() == 'Σ' as u32 && last == Some(index) && first.is_some_and(|first| first < index) {
                push('ς' as u32);
            } else {
                lower(character.into(), &mut push);
            }
        }
        start = end;
    }
}

fn map(character: char, table: &[u8], mut push: impl FnMut(char)) {
    let key = character as u32;
    let mut low = 0;
    let mut high = table.len() / 16;
    while low < high {
        let middle = low + (high - low) / 2;
        let row = &table[middle * 16..middle * 16 + 16];
        match number(row, 0).cmp(&key) {
            std::cmp::Ordering::Less => low = middle + 1,
            std::cmp::Ordering::Greater => high = middle,
            std::cmp::Ordering::Equal => {
                for index in [4, 8, 12] {
                    let mapped = number(row, index);
                    if mapped != 0 { push(char::from_u32(mapped).expect("pinned case mapping is a Unicode scalar")); }
                }
                return;
            }
        }
    }
    push(character);
}

fn is_cased(character: impl Into<u32>) -> bool {
    let key = character.into();
    let mut low = 0;
    let mut high = CASED.len() / 8;
    while low < high {
        let middle = low + (high - low) / 2;
        let row = &CASED[middle * 8..middle * 8 + 8];
        if key < number(row, 0) { high = middle; }
        else if key > number(row, 4) { low = middle + 1; }
        else { return true; }
    }
    false
}

fn number(bytes: &[u8], offset: usize) -> u32 { u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("pinned table integer")) }
fn short(bytes: &[u8], offset: usize) -> usize { usize::from(u16::from_be_bytes(bytes[offset..offset + 2].try_into().expect("pinned table short"))) }

struct WordRules {
    states: &'static [u8],
    ends: &'static [u8],
    lookahead: &'static [u8],
    bmp_indices: &'static [u8],
    bmp: &'static [u8],
    supplementary: &'static [u8],
    categories: usize,
}

impl WordRules {
    fn new() -> Self {
        let state_length = number(WORDS, 12) as usize * 2;
        let backward_length = number(WORDS, 16) as usize * 2;
        let end_length = number(WORDS, 20) as usize;
        let lookahead_length = number(WORDS, 24) as usize;
        let bmp_length = number(WORDS, 28) as usize;
        let supplementary_length = number(WORDS, 32) as usize * 4;
        let states = &WORDS[48..48 + state_length];
        let offset = 48 + state_length + backward_length;
        let ends = &WORDS[offset..offset + end_length];
        let offset = offset + end_length;
        let lookahead = &WORDS[offset..offset + lookahead_length];
        let offset = offset + lookahead_length;
        let bmp_indices = &WORDS[offset..offset + 1024];
        let offset = offset + 1024;
        let bmp = &WORDS[offset..offset + bmp_length];
        let offset = offset + bmp_length;
        let supplementary = &WORDS[offset..offset + supplementary_length];
        Self { states, ends, lookahead, bmp_indices, bmp, supplementary, categories: state_length / 2 / end_length }
    }

    fn category(&self, character: impl Into<u32>) -> u8 {
        let code = character.into();
        if code < 0x10000 {
            return self.bmp[short(self.bmp_indices, (code as usize >> 7) * 2) + (code as usize & 127)];
        }
        let mut low = 0;
        let mut high = self.supplementary.len() / 4 - 1;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            if code < number(self.supplementary, middle * 4) >> 8 { high = middle; }
            else { low = middle; }
        }
        number(self.supplementary, low * 4) as u8
    }

    // OpenJDK 25.0.1 RuleBasedBreakIterator.handleNext, with code point indices in place of UTF-16 offsets.
    fn next<T: Copy + Into<u32>>(&self, text: &[T], start: usize) -> usize {
        let mut result = start + 1;
        let mut lookahead = 0;
        let mut state = 1;
        let mut index = start;
        while index < text.len() && text[index].into() != 0xffff && state != 0 {
            let category = self.category(text[index]);
            if category != 255 { state = short(self.states, (state * self.categories + usize::from(category)) * 2); }
            if self.lookahead[state] != 0 {
                if self.ends[state] != 0 { result = lookahead; }
                else { lookahead = index + 1; }
            } else if self.ends[state] != 0 { result = index + 1; }
            index += 1;
        }
        if (index == text.len() || text[index].into() == 0xffff) && lookahead == text.len() { result = lookahead; }
        result
    }
}

#[cfg(test)]
mod tests;
