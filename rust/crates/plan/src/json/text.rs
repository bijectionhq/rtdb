use std::{alloc::Layout, str::Chars};
use rtdb_types::{AllocationScope, Anomaly, java_text::{JavaString, JavaText}};
use super::{JsonSink, JsonWriteFailure, WriteResult};

#[cfg(test)]
#[path = "text_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JsonStringIssue { ExpectedQuote, InvalidEscape, InvalidHex, RawControl, MissingQuote }
#[derive(Debug)]
enum JsonReadFailure { Syntax { byte_offset: usize, issue: JsonStringIssue }, Admission(Anomaly) }
impl From<Anomaly> for JsonReadFailure {
    fn from(error: Anomaly) -> Self { Self::Admission(error) }
}
type ReadResult<T> = std::result::Result<T, JsonReadFailure>;

struct JsonStringToken<'source> { body: &'source str, units: usize, escaped: bool }
impl<'source> JsonStringToken<'source> {
    fn parse(source: &'source str, start: usize) -> ReadResult<(Self, usize)> {
        let bytes = source.as_bytes();
        if bytes.get(start) != Some(&b'"') || !source.is_char_boundary(start) {
            return Err(syntax(start, JsonStringIssue::ExpectedQuote));
        }
        let mut cursor = start + 1;
        let mut units = 0usize;
        let mut escaped = false;
        loop {
            let byte = *bytes.get(cursor).ok_or_else(|| syntax(cursor, JsonStringIssue::MissingQuote))?;
            match byte {
                b'"' => return Ok((Self { body: &source[start + 1..cursor], units, escaped }, cursor + 1)),
                0..=31 => return Err(syntax(cursor, JsonStringIssue::RawControl)),
                b'\\' => {
                    escaped = true;
                    cursor += 1;
                    match bytes.get(cursor).copied() {
                        Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => cursor += 1,
                        Some(b'u') => {
                            cursor += 1;
                            for _ in 0..4 {
                                if bytes.get(cursor).copied().and_then(hex).is_none() { return Err(syntax(cursor, JsonStringIssue::InvalidHex)); }
                                cursor += 1;
                            }
                        }
                        _ => return Err(syntax(cursor, JsonStringIssue::InvalidEscape)),
                    }
                    units = units.checked_add(1).ok_or_else(size_error)?;
                }
                _ => {
                    let character = source[cursor..].chars().next().expect("present UTF8 character");
                    cursor += character.len_utf8();
                    units = units.checked_add(character.len_utf16()).ok_or_else(size_error)?;
                }
            }
        }
    }

    fn decode(&self, allocation: &AllocationScope) -> ReadResult<JavaString> {
        if !self.escaped { return Ok(JavaString::copy_utf8(self.body, allocation.memory_budget())?); }
        let mut output = DecodedText::new(self.units, allocation)?;
        for unit in (TokenUnits { characters: self.body.chars(), pending: None }) { output.push(unit)?; }
        Ok(JavaString::copy_utf16(&output.units, allocation.memory_budget())?)
    }
}

struct TokenUnits<'source> { characters: Chars<'source>, pending: Option<u16> }
impl Iterator for TokenUnits<'_> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        if let Some(unit) = self.pending.take() { return Some(unit); }
        let character = self.characters.next()?;
        if character == '\\' {
            return Some(match self.characters.next().expect("validated escape") {
                '"' => u16::from(b'"'), '\\' => u16::from(b'\\'), '/' => u16::from(b'/'),
                'b' => 8, 'f' => 12, 'n' => 10, 'r' => 13, 't' => 9,
                'u' => (0..4).fold(0u16, |value, _| value * 16 + hex(self.characters.next().expect("validated hex") as u8).expect("validated hex")),
                _ => unreachable!("JSON token admits only its escape set"),
            });
        }
        let mut encoded = [0; 2];
        let units = character.encode_utf16(&mut encoded);
        if units.len() == 2 { self.pending = Some(units[1]); }
        Some(units[0])
    }
}

struct DecodedText<'scope> { units: Vec<u16>, limit: usize, allocation: &'scope AllocationScope }
impl<'scope> DecodedText<'scope> {
    fn new(limit: usize, allocation: &'scope AllocationScope) -> ReadResult<Self> {
        if limit > i32::MAX as usize { return Err(size_error().into()); }
        let bytes = Layout::array::<u16>(limit).map_err(|_| size_error())?.size();
        allocation.reserve(bytes)?;
        Ok(Self { units: Vec::with_capacity(limit), limit, allocation })
    }
    fn push(&mut self, unit: u16) -> ReadResult<()> {
        if self.units.len() == self.limit { return Err(size_error().into()); }
        self.units.push(unit);
        Ok(())
    }
}
// The scope owns the scratch charge through destruction of the decoded buffer.
impl Drop for DecodedText<'_> { fn drop(&mut self) { let _ = self.allocation; } }

fn syntax(byte_offset: usize, issue: JsonStringIssue) -> JsonReadFailure { JsonReadFailure::Syntax { byte_offset, issue } }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.plan/json-text-size", "JSON text exceeds addressable storage") }
fn hex(byte: u8) -> Option<u16> {
    match byte { b'0'..=b'9' => Some(u16::from(byte - b'0')), b'a'..=b'f' => Some(u16::from(byte - b'a' + 10)), b'A'..=b'F' => Some(u16::from(byte - b'A' + 10)), _ => None }
}

pub(super) fn string(text: JavaText<'_>, sink: &mut impl JsonSink) -> WriteResult<()> {
    sink.append(b"\"")?;
    for point in text.code_points() {
        match point {
            8 => sink.append(b"\\b")?, 9 => sink.append(b"\\t")?, 10 => sink.append(b"\\n")?,
            12 => sink.append(b"\\f")?, 13 => sink.append(b"\\r")?,
            34 => sink.append(b"\\\"")?, 92 => sink.append(b"\\\\")?,
            0..=31 | 0xd800..=0xdfff => {
                let mut escape = *b"\\u0000";
                for (index, byte) in escape[2..].iter_mut().enumerate() { *byte = b"0123456789abcdef"[((point >> ((3 - index) * 4)) & 15) as usize]; }
                sink.append(&escape)?;
            }
            _ => {
                let character = char::from_u32(point).ok_or(JsonWriteFailure::Internal)?;
                let mut encoded = [0; 4];
                sink.append(character.encode_utf8(&mut encoded).as_bytes())?;
            }
        }
    }
    sink.append(b"\"")
}
