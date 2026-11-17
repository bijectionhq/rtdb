use alloc::{boxed::Box, format, vec::Vec};
use crate::{compile::compile_inner, RegexOptions, Result};
use regex_automata::meta::Regex;

#[derive(Clone, Debug)]
pub(super) enum CharacterClass { Set(Set), Regex(Regex) }

#[derive(Clone, Debug)]
pub(super) enum Set { Range(u32, u32), Ranges(Vec<(u32, u32)>), Union(Vec<Set>), Intersection(Box<Set>, Box<Set>), Negate(Box<Set>) }

impl CharacterClass {
    pub(super) fn compile(pattern: &str, casei: bool, options: &RegexOptions) -> Result<Self> {
        if !casei {
            let mut parser = Parser { pattern, position: 0 };
            if let Some(set) = parser.class() {
                if parser.position == pattern.len() { return Ok(Self::Set(set)); }
            }
        }
        let pattern = if casei { format!("\\A(?i:{pattern})\\z") } else { format!("\\A(?:{pattern})\\z") };
        Ok(Self::Regex(compile_inner(&pattern, options)?))
    }

    pub(super) fn contains(&self, value: u32) -> bool {
        match self {
            Self::Set(set) => set.contains(value),
            Self::Regex(regex) => char::from_u32(value).is_some_and(|character| {
                let mut bytes = [0; 4]; regex.is_match(&*character.encode_utf8(&mut bytes))
            }),
        }
    }
}

impl Set {
    fn contains(&self, value: u32) -> bool {
        match self {
            Self::Range(start, end) => *start <= value && value <= *end,
            Self::Ranges(ranges) => {
                let index = ranges.partition_point(|range| range.1 < value);
                ranges.get(index).is_some_and(|range| range.0 <= value)
            }
            Self::Union(sets) => sets.iter().any(|set| set.contains(value)),
            Self::Intersection(left, right) => left.contains(value) && right.contains(value),
            Self::Negate(set) => !set.contains(value),
        }
    }
}

struct Parser<'a> { pattern: &'a str, position: usize }
impl Parser<'_> {
    fn take(&mut self, token: &str) -> bool {
        if self.pattern[self.position..].starts_with(token) { self.position += token.len(); true } else { false }
    }
    fn class(&mut self) -> Option<Set> {
        if !self.take("[") { return None; }
        let negated = self.take("^");
        let mut result = self.union()?;
        while self.take("&&") { result = Set::Intersection(Box::new(result), Box::new(self.union()?)); }
        if !self.take("]") { return None; }
        Some(if negated { Set::Negate(Box::new(result)) } else { result })
    }
    fn union(&mut self) -> Option<Set> {
        let mut sets = Vec::new();
        while self.position < self.pattern.len() && !self.pattern[self.position..].starts_with(']') && !self.pattern[self.position..].starts_with("&&") {
            if self.pattern[self.position..].starts_with("--") || self.pattern[self.position..].starts_with("~~") { return None; }
            if self.pattern[self.position..].starts_with('[') { sets.push(self.class()?); continue; }
            let start = self.point()?;
            let end = if self.take("-") { self.point()? } else { start };
            if start > end { return None; }
            sets.push(Set::Range(start, end));
        }
        if sets.iter().all(|set| matches!(set, Set::Range(_, _))) {
            let mut ranges: Vec<(u32, u32)> = sets.into_iter().map(|set| { let Set::Range(start, end) = set else { unreachable!() }; (start, end) }).collect();
            ranges.sort_unstable();
            let mut merged: Vec<(u32, u32)> = Vec::with_capacity(ranges.len());
            for range in ranges {
                if let Some(previous) = merged.last_mut().filter(|previous| previous.1.saturating_add(1) >= range.0) { previous.1 = previous.1.max(range.1); }
                else { merged.push(range); }
            }
            Some(Set::Ranges(merged))
        } else { Some(Set::Union(sets)) }
    }
    fn point(&mut self) -> Option<u32> {
        let character = self.pattern[self.position..].chars().next()?;
        self.position += character.len_utf8();
        if character != '\\' { return Some(character as u32); }
        let escape = self.pattern[self.position..].chars().next()?;
        self.position += escape.len_utf8();
        Some(match escape {
            'n' => 10, 'r' => 13, 't' => 9, 'f' => 12, 'a' => 7, 'e' => 27, 'v' => 11,
            'x' | 'u' | 'U' => {
                if self.take("{") {
                    let start = self.position;
                    while self.pattern.as_bytes().get(self.position).is_some_and(u8::is_ascii_hexdigit) { self.position += 1; }
                    let value = u32::from_str_radix(&self.pattern[start..self.position], 16).ok()?;
                    if !self.take("}") { return None; } value
                } else {
                    let count = match escape { 'x' => 2, 'u' => 4, _ => 8 };
                    let end = self.position.checked_add(count)?;
                    let value = u32::from_str_radix(self.pattern.get(self.position..end)?, 16).ok()?;
                    self.position = end; value
                }
            }
            character if !character.is_ascii_alphanumeric() => character as u32,
            _ => return None,
        })
    }
}
