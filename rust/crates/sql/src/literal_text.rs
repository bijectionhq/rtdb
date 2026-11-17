use std::{alloc::Layout, iter::Peekable};
use rtdb_types::{AllocationScope, Anomaly, java_text::{JavaString, JavaText, Utf16Units}};
use crate::lexer::{Kind, Lexeme};

#[cfg(test)]
mod tests;

type DecodeResult<T> = std::result::Result<T, LiteralDecodeFailure>;
const BACKSLASH: u16 = b'\\' as u16;

pub(crate) struct Literal<'sql, 'tokens> { kind: LiteralKind<'sql, 'tokens> }
enum LiteralKind<'sql, 'tokens> {
    Standard(Lexeme<'sql>),
    Escaped(Lexeme<'sql>),
    Dollar(&'tokens [Lexeme<'sql>]),
}

impl<'sql, 'tokens> Literal<'sql, 'tokens> {
    pub(crate) fn single(token: Lexeme<'sql>) -> Option<Self> {
        let kind = match token.kind() {
            Kind::CharacterString => LiteralKind::Standard(token),
            Kind::EscapedString => LiteralKind::Escaped(token),
            _ => return None,
        };
        Some(Self { kind })
    }

    pub(crate) fn dollar(tokens: &'tokens [Lexeme<'sql>]) -> Option<Self> {
        if tokens.len() < 2 { return None; }
        let first = tokens.first()?;
        let last = tokens.last()?;
        if first.kind() != Kind::DollarStart || last.kind() != Kind::DollarEnd || first.tag() != last.tag()
            || tokens[1..tokens.len() - 1].iter().any(|token| token.kind() != Kind::DollarText) { return None; }
        Some(Self { kind: LiteralKind::Dollar(tokens) })
    }

    pub(crate) fn decode(&self, allocation: &AllocationScope) -> DecodeResult<JavaString> {
        match &self.kind {
            LiteralKind::Standard(token) => {
                let body = token.body().expect("standard literal body");
                if !body.contains("''") { return Ok(JavaString::copy_utf8(body, allocation.memory_budget())?); }
                let source = JavaText::from_utf8(body);
                let mut output = DecodedUnits::new(source.len_units(), allocation)?;
                let mut units = source.units().peekable();
                while let Some(unit) = units.next() {
                    output.push(unit)?;
                    if unit == u16::from(b'\'') && units.peek() == Some(&unit) { units.next(); }
                }
                output.finish()
            }
            LiteralKind::Escaped(token) => decode_c_body(JavaText::from_utf8(token.body().expect("escaped literal body")), allocation),
            LiteralKind::Dollar(tokens) => {
                let bodies = &tokens[1..tokens.len() - 1];
                if bodies.is_empty() { return Ok(JavaString::copy_utf8("", allocation.memory_budget())?); }
                if let [body] = bodies { return Ok(JavaString::copy_utf8(body.raw(), allocation.memory_budget())?); }
                let length = bodies.iter().try_fold(0usize, |length, body| {
                    length.checked_add(body.raw().encode_utf16().count()).ok_or_else(size_error)
                })?;
                let mut output = DecodedUnits::new(length, allocation)?;
                for body in bodies { output.extend(body.raw().encode_utf16())?; }
                output.finish()
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum LiteralDecodeFailure { Java(LiteralJavaFailure), Resource(Anomaly) }

impl From<Anomaly> for LiteralDecodeFailure {
    fn from(error: Anomaly) -> Self { Self::Resource(error) }
}
impl From<LiteralJavaFailure> for LiteralDecodeFailure {
    fn from(error: LiteralJavaFailure) -> Self { Self::Java(error) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LiteralJavaFailure { InvalidEscape(u16), InvalidCodePoint(i32) }

impl LiteralJavaFailure {
    pub(crate) fn exception_class(self) -> &'static str { "java.lang.IllegalArgumentException" }

    pub(crate) fn message(self, allocation: &AllocationScope) -> rtdb_types::Result<JavaString> {
        let mut units = [0u16; 64];
        let mut length = 0;
        let mut append = |unit| { units[length] = unit; length += 1; };
        let (value, width) = match self {
            Self::InvalidEscape(unit) => {
                for byte in b"Invalid escape sequence: \\" { append(u16::from(*byte)); }
                append(unit);
                for byte in b" \\\\u" { append(u16::from(*byte)); }
                (u32::from(unit), 4)
            }
            Self::InvalidCodePoint(value) => {
                for byte in b"Not a valid Unicode code point: 0x" { append(u16::from(*byte)); }
                let value = value as u32;
                (value, (32 - value.leading_zeros()).div_ceil(4).max(1))
            }
        };
        for index in (0..width).rev() { append(u16::from(b"0123456789ABCDEF"[((value >> (index * 4)) & 15) as usize])); }
        JavaString::copy_utf16(&units[..length], allocation.memory_budget())
    }
}

struct DecodedUnits<'scope> {
    units: Vec<u16>,
    limit: usize,
    allocation: &'scope AllocationScope,
}

impl<'scope> DecodedUnits<'scope> {
    fn new(limit: usize, allocation: &'scope AllocationScope) -> DecodeResult<Self> {
        allocation.reserve(scratch_bytes(limit)?)?;
        Ok(Self { units: Vec::with_capacity(limit), limit, allocation })
    }

    fn push(&mut self, unit: u16) -> DecodeResult<()> {
        if self.units.len() == self.limit { return Err(size_error().into()); }
        self.units.push(unit);
        Ok(())
    }

    fn extend(&mut self, units: impl IntoIterator<Item = u16>) -> DecodeResult<()> {
        for unit in units { self.push(unit)?; }
        Ok(())
    }

    fn finish(self) -> DecodeResult<JavaString> {
        Ok(JavaString::copy_utf16(&self.units, self.allocation.memory_budget())?)
    }
}

// Keep the scratch scope borrowed through the buffer's destruction.
impl Drop for DecodedUnits<'_> { fn drop(&mut self) {} }

fn scratch_bytes(units: usize) -> rtdb_types::Result<usize> {
    if units > i32::MAX as usize { return Err(size_error()); }
    Layout::array::<u16>(units).map(|layout| layout.size()).map_err(|_| size_error())
}

fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.sql/literal-text-size", "SQL literal text exceeds addressable storage") }

fn decode_c_body(source: JavaText<'_>, allocation: &AllocationScope) -> DecodeResult<JavaString> {
    scratch_bytes(source.len_units())?;
    let mut scanner = CScanner::new(source);
    let first = loop {
        match scanner.next_fragment() {
            Some(Fragment::Escape(escape)) => break Some(escape),
            Some(Fragment::Unit(_)) => (),
            None => break None,
        }
    };
    let Some(first) = first else {
        if let Some(source) = source.utf8_source() { return Ok(JavaString::copy_utf8(source, allocation.memory_budget())?); }
        let mut output = DecodedUnits::new(source.len_units(), allocation)?;
        output.extend(source.units())?;
        return output.finish();
    };
    let mut output = DecodedUnits::new(source.len_units(), allocation)?;
    output.extend(source.units().take(first.start))?;
    first.translate(&mut output)?;
    while let Some(fragment) = scanner.next_fragment() {
        match fragment { Fragment::Unit(unit) => output.push(unit)?, Fragment::Escape(escape) => escape.translate(&mut output)? }
    }
    output.finish()
}

struct CScanner<'a> { units: Peekable<Utf16Units<'a>>, offset: usize }
enum Fragment { Unit(u16), Escape(Escape) }
struct Escape { start: usize, units: [u16; 10], length: usize }

impl<'a> CScanner<'a> {
    fn new(source: JavaText<'a>) -> Self { Self { units: source.units().peekable(), offset: 0 } }

    fn take(&mut self) -> Option<u16> {
        let unit = self.units.next()?;
        self.offset += 1;
        Some(unit)
    }

    fn next_fragment(&mut self) -> Option<Fragment> {
        let start = self.offset;
        let unit = self.take()?;
        if unit != BACKSLASH { return Some(Fragment::Unit(unit)); }
        let Some(next) = self.units.peek().copied() else { return Some(Fragment::Unit(unit)); };
        let hex_digits = match next { 0x78 => 2, 0x75 => 4, 0x55 => 8, _ => 0 };
        if hex_digits != 0 && self.units.clone().nth(1).and_then(hex).is_none() { return Some(Fragment::Unit(unit)); }
        let mut escape = Escape { start, units: [0; 10], length: 1 };
        escape.units[0] = BACKSLASH;
        escape.push(self.take().expect("peeked escape unit"));
        if octal(next) {
            for _ in 0..2 {
                if self.units.peek().copied().is_none_or(|unit| !octal(unit)) { break; }
                escape.push(self.take().expect("peeked octal unit"));
            }
        } else if hex_digits != 0 {
            for _ in 0..hex_digits {
                if self.units.peek().copied().and_then(hex).is_none() { break; }
                escape.push(self.take().expect("peeked hexadecimal unit"));
            }
        } else if (0xd800..=0xdbff).contains(&next)
            && self.units.peek().is_some_and(|unit| (0xdc00..=0xdfff).contains(unit)) {
            escape.push(self.take().expect("peeked low surrogate"));
        }
        Some(Fragment::Escape(escape))
    }
}

impl Escape {
    fn push(&mut self, unit: u16) { self.units[self.length] = unit; self.length += 1; }

    fn translate(&self, output: &mut DecodedUnits<'_>) -> DecodeResult<()> {
        let first = self.units[1];
        if matches!(first, 0x78 | 0x75 | 0x55) {
            let point = self.units[2..self.length].iter().fold(0u32, |value, unit| value * 16 + hex(*unit).expect("matched hexadecimal unit"));
            if point <= 0xffff { return output.push(point as u16); }
            if point > 0x10ffff { return Err(LiteralJavaFailure::InvalidCodePoint(point as i32).into()); }
            output.push(0xd800 + ((point - 0x10000) >> 10) as u16)?;
            return output.push(0xdc00 + ((point - 0x10000) & 0x3ff) as u16);
        }
        if octal(first) {
            let digits = (if first <= u16::from(b'3') { 3usize } else { 2 }).min(self.length - 1);
            let value = self.units[1..=digits].iter().fold(0u16, |value, unit| value * 8 + *unit - u16::from(b'0'));
            output.push(value)?;
            return output.extend(self.units[digits + 1..self.length].iter().copied());
        }
        let unit = match char::from_u32(u32::from(first)) {
            Some('b') => 8, Some('f') => 12, Some('n') => 10, Some('r') => 13, Some('s') => 32, Some('t') => 9,
            Some('\'' | '"' | '\\') => first,
            Some('\n' | '\r') => return Ok(()),
            _ => return Err(LiteralJavaFailure::InvalidEscape(first).into()),
        };
        output.push(unit)
    }
}

fn octal(unit: u16) -> bool { (u16::from(b'0')..=u16::from(b'7')).contains(&unit) }
fn hex(unit: u16) -> Option<u32> {
    match unit {
        0x30..=0x39 => Some(u32::from(unit - 0x30)),
        0x41..=0x46 => Some(u32::from(unit - 0x41) + 10),
        0x61..=0x66 => Some(u32::from(unit - 0x61) + 10),
        _ => None,
    }
}
