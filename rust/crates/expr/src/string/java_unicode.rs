#[path = "java_unicode_data.rs"]
mod data;

use std::fmt::Write;
use rtdb_types::Result;

fn uppercase_ascii(value: &str) -> Option<String> {
    case_ascii(value, true)
}

fn case_ascii(value: &str, upper: bool) -> Option<String> {
    let mut result = String::with_capacity(value.len());
    let mappings = if upper { data::ASCII_UPPER } else { data::ASCII_LOWER };
    for character in value.chars() {
        if character.is_ascii() {
            result.push(if upper { character.to_ascii_uppercase() } else { character.to_ascii_lowercase() });
        } else {
            let index = mappings.binary_search_by_key(&(character as u32), |entry| entry.0).ok()?;
            result.push_str(mappings[index].1);
        }
    }
    Some(result)
}

fn contains(ranges: &[(u32, u32)], value: u32) -> bool {
    let index = ranges.partition_point(|range| range.1 < value);
    ranges.get(index).is_some_and(|range| range.0 <= value)
}

pub(super) fn named_character(name: &str) -> Option<u32> {
    let name = uppercase_ascii(name.trim_matches(|character| character <= '\u{20}'))?;
    if let Ok(index) = data::NAMES.binary_search_by(|entry| entry.0.cmp(name.as_str())) {
        return Some(data::NAMES[index].1);
    }
    let (block, hex) = name.rsplit_once(' ')?;
    let value = u32::from_str_radix(hex, 16).ok()?;
    if !contains(data::SETS[data::ASSIGNED], value) || data::NAMED_CODEPOINTS.binary_search(&value).is_ok() {
        return None;
    }
    let index = data::BLOCK_NAMES.partition_point(|entry| entry.1 < value);
    let (start, _, expected) = data::BLOCK_NAMES.get(index)?;
    (*start <= value && *expected == block && hex == format!("{value:X}")).then_some(value)
}

fn property(values: &[(&str, usize, usize)], name: &str, insensitive: bool) -> Option<usize> {
    let index = values.binary_search_by(|entry| entry.0.cmp(name)).ok()?;
    Some(if insensitive { values[index].2 } else { values[index].1 })
}

fn alias(values: &[(&str, usize)], name: &str) -> Option<usize> {
    let name = uppercase_ascii(name)?;
    let index = values.binary_search_by(|entry| entry.0.cmp(name.as_str())).ok()?;
    Some(values[index].1)
}

pub(super) fn property_class(name: &str, unicode: bool, insensitive: bool) -> Result<String> {
    property_class_utf16(name, unicode, insensitive, false)
}

pub(super) fn property_class_utf16(name: &str, unicode: bool, insensitive: bool, utf16: bool) -> Result<String> {
    let selected = if let Some((property_name, value)) = name.split_once('=') {
        match case_ascii(property_name, false).as_deref() {
            Some("sc" | "script") => alias(data::SCRIPT_ALIASES, value),
            Some("blk" | "block") => alias(data::BLOCK_ALIASES, value),
            Some("gc" | "general_category") => property(data::PROPERTIES, value, insensitive),
            _ => None,
        }
    } else if let Some(block) = name.strip_prefix("In") {
        alias(data::BLOCK_ALIASES, block)
    } else if let Some(value) = name.strip_prefix("Is") {
        uppercase_ascii(value).and_then(|name| property(data::UNICODE, &name, insensitive))
            .or_else(|| property(data::PROPERTIES, value, insensitive))
            .or_else(|| alias(data::SCRIPT_ALIASES, value))
    } else {
        unicode.then(|| uppercase_ascii(name).and_then(|name| property(data::POSIX, &name, insensitive))).flatten()
            .or_else(|| property(data::PROPERTIES, name, insensitive))
    };
    let index = selected.ok_or_else(|| super::illegal(&format!("Unknown character property name {{{name}}}")))?;
    if utf16 {
        let mut result = String::new();
        for &(start, end) in data::SETS[index] { append_range(&mut result, start, end); }
        if result.is_empty() { result.push_str("\\x00&&\\x01"); }
        Ok(result)
    } else { Ok(render(data::SETS[index])) }
}

fn render(ranges: &[(u32, u32)]) -> String {
    let mut output = String::new();
    for &(start, end) in ranges {
        if start < 0xd800 { append_range(&mut output, start, end.min(0xd7ff)); }
        if end > 0xdfff { append_range(&mut output, start.max(0xe000), end); }
    }
    if output.is_empty() { output.push_str("\\x00&&\\x01"); }
    output
}

fn append_range(output: &mut String, start: u32, end: u32) {
    write!(output, "\\x{{{start:x}}}").expect("writing to a String");
    if start != end { write!(output, "-\\x{{{end:x}}}").expect("writing to a String"); }
}

fn mappings(value: u32) -> (u32, u32, u32) {
    data::CASE_MAPPINGS.binary_search_by_key(&value, |entry| entry.0)
        .map(|index| { let (_, upper, lower, fold) = data::CASE_MAPPINGS[index]; (upper, lower, fold) })
        .unwrap_or((value, value, value))
}

pub(super) fn single_is_bmp(value: u32, insensitive: bool, unicode_case: bool) -> bool {
    let (upper, _, fold) = mappings(value);
    value < 0x10000 && !(0xd800..=0xdfff).contains(&value) && !(insensitive && unicode_case && upper != fold)
}

pub(super) fn property_is_bmp(name: &str, unicode: bool) -> bool {
    let property_name = if let Some((key, value)) = name.split_once('=') {
        if !matches!(case_ascii(key, false).as_deref(), Some("gc" | "general_category")) { return false; }
        value
    } else if let Some(value) = name.strip_prefix("Is") {
        if uppercase_ascii(value).is_some_and(|name| property(data::UNICODE, &name, false).is_some()) { return false; }
        value
    } else {
        if unicode && uppercase_ascii(name).is_some_and(|name| property(data::POSIX, &name, false).is_some()) { return false; }
        name
    };
    matches!(property_name, "L1" | "ASCII" | "Alnum" | "Alpha" | "Blank" | "Cntrl" | "Digit" | "Graph" | "Lower" | "Print" | "Punct" | "Space" | "Upper" | "XDigit")
}

fn render_points(mut values: Vec<u32>) -> String {
    values.sort_unstable();
    values.dedup();
    let mut ranges: Vec<(u32, u32)> = Vec::new();
    for value in values {
        if let Some(previous) = ranges.last_mut().filter(|range| range.1 + 1 == value) { previous.1 = value; }
        else { ranges.push((value, value)); }
    }
    render(&ranges)
}

pub(super) fn literal_single(value: char) -> String {
    let value = value as u32;
    let (upper, _, fold) = mappings(value);
    if upper == fold { return render(&[(value, value)]); }
    folded_literal(value)
}

pub(super) fn folded_literal(value: u32) -> String {
    let (_, _, fold) = mappings(value);
    let mut values = vec![fold];
    values.extend(data::CASE_MAPPINGS.iter().filter_map(|&(candidate, _, _, candidate_fold)| (candidate_fold == fold).then_some(candidate)));
    render_points(values)
}

pub(super) fn class_single(value: char) -> String {
    let cp = value as u32;
    if cp < 256 && !matches!(cp, 0xff | 0xb5 | 0x49 | 0x69 | 0x53 | 0x73 | 0x4b | 0x6b | 0xc5 | 0xe5) {
        let (upper, lower, _) = mappings(cp);
        render_points(vec![cp, upper, lower])
    } else {
        literal_single(value)
    }
}

pub(super) fn class_range(start: char, end: char) -> String {
    let (start, end) = (start as u32, end as u32);
    let mut ranges = vec![(start, end)];
    ranges.extend(data::CASE_MAPPINGS.iter().filter_map(|&(candidate, upper, _, fold)| {
        (!(start..=end).contains(&candidate) && ((start..=end).contains(&upper) || (start..=end).contains(&fold))).then_some((candidate, candidate))
    }));
    ranges.sort_unstable();
    let mut merged: Vec<(u32, u32)> = Vec::new();
    for (start, end) in ranges {
        if let Some(previous) = merged.last_mut().filter(|range| start <= range.1 + 1) { previous.1 = previous.1.max(end); }
        else { merged.push((start, end)); }
    }
    render(&merged)
}

pub(super) fn class_range_utf16(start: u32, end: u32, insensitive: bool, unicode_case: bool) -> String {
    let mut ranges = vec![(start, end)];
    if insensitive {
        ranges.extend(data::CASE_MAPPINGS.iter().filter_map(|&(candidate, upper, _, fold)| {
            ((unicode_case || candidate < 128) && !(start..=end).contains(&candidate)
                && ((start..=end).contains(&upper) || (start..=end).contains(&fold))).then_some((candidate, candidate))
        }));
    }
    ranges.sort_unstable();
    let mut merged: Vec<(u32, u32)> = Vec::new();
    for (start, end) in ranges {
        if let Some(previous) = merged.last_mut().filter(|range| start <= range.1 + 1) { previous.1 = previous.1.max(end); }
        else { merged.push((start, end)); }
    }
    let mut result = String::new();
    for (start, end) in merged { append_range(&mut result, start, end); }
    result
}

#[cfg(test)]
#[path = "java_unicode_tests.rs"]
mod tests;
