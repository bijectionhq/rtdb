#[path = "grapheme_data.rs"]
mod data;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Class { Other, Cr, Lf, Control, Extend, Zwj, Regional, Prepend, Spacing, L, V, T, Lv, Lvt, Pictographic }

fn class(character: char) -> Class {
    class_codepoint(character as u32)
}

fn class_codepoint(value: u32) -> Class {
    if (0xac00..0xac00 + 11172).contains(&value) {
        return if (value - 0xac00) % 28 == 0 { Class::Lv } else { Class::Lvt };
    }
    let index = data::CLASSES.partition_point(|entry| entry.0 <= value);
    if index > 0 && value <= data::CLASSES[index - 1].1 { data::CLASSES[index - 1].2 } else { Class::Other }
}

fn contains(ranges: &[(u32, u32)], character: char) -> bool {
    contains_codepoint(ranges, character as u32)
}

fn contains_codepoint(ranges: &[(u32, u32)], value: u32) -> bool {
    let index = ranges.partition_point(|entry| entry.0 <= value);
    index > 0 && value <= ranges[index - 1].1
}

fn conjunct_end(text: &str, position: usize) -> Option<usize> {
    let mut linked = false;
    for (offset, character) in text[position..].char_indices() {
        if contains(data::LINKER, character) { linked = true; }
        else if contains(data::CONSONANT, character) {
            return linked.then_some(position + offset + character.len_utf8());
        } else if !contains(data::EXTEND, character) { return None; }
    }
    None
}

fn separates(left: Class, right: Class) -> bool {
    use Class::*;
    match (left, right) {
        (Cr, Lf) => false,
        (Cr | Lf | Control, _) | (_, Cr | Lf | Control) => true,
        (L, L | V | Lv | Lvt) | (Lv | V, V | T) | (Lvt | T, T) => false,
        (_, Extend | Zwj | Spacing) | (Prepend, _) => false,
        _ => true,
    }
}

pub(crate) fn next_boundary(text: &str, start: usize) -> Option<usize> {
    let mut previous = text[start..].chars().next()?;
    let mut left = class(previous);
    let pictographic = left == Class::Pictographic;
    let mut regional_count = usize::from(left == Class::Regional);
    let mut position = start + previous.len_utf8();
    while let Some(character) = text[position..].chars().next() {
        if contains(data::CONSONANT, previous) {
            if let Some(end) = conjunct_end(text, position) { position = end; continue; }
        }
        let right = class(character);
        let joined = pictographic && left == Class::Zwj && right == Class::Pictographic
            || regional_count % 2 == 1 && left == Class::Regional && right == Class::Regional;
        if !joined && separates(left, right) { break; }
        regional_count += usize::from(right == Class::Regional);
        previous = character;
        left = right;
        position += character.len_utf8();
    }
    Some(position)
}

pub(crate) fn next_boundary_utf16(text: &[u16], start: usize) -> Option<usize> {
    let (mut previous, mut position) = crate::java::point(text, start)?;
    let mut left = class_codepoint(previous);
    let pictographic = left == Class::Pictographic;
    let mut regional_count = usize::from(left == Class::Regional);
    while let Some((character, end)) = crate::java::point(text, position) {
        if contains_codepoint(data::CONSONANT, previous) {
            let mut cursor = position; let mut linked = false; let mut conjunct = None;
            while let Some((character, end)) = crate::java::point(text, cursor) {
                if contains_codepoint(data::LINKER, character) { linked = true; }
                else if contains_codepoint(data::CONSONANT, character) { if linked { conjunct = Some(end); } break; }
                else if !contains_codepoint(data::EXTEND, character) { break; }
                cursor = end;
            }
            if let Some(end) = conjunct { position = end; continue; }
        }
        let right = class_codepoint(character);
        let joined = pictographic && left == Class::Zwj && right == Class::Pictographic
            || regional_count % 2 == 1 && left == Class::Regional && right == Class::Regional;
        if !joined && separates(left, right) { break; }
        regional_count += usize::from(right == Class::Regional);
        previous = character; left = right; position = end;
    }
    Some(position)
}
