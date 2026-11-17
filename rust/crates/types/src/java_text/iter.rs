use std::{iter::{FusedIterator, Peekable}, slice, str};
use super::Source;

/// Sequential exact UTF16 units from a borrowed text range, with an exact remaining length.
/// Clones copy cursor state and allocate nothing.
#[derive(Clone)]
pub struct Utf16Units<'a> { source: Units<'a>, remaining: usize }

#[derive(Clone)]
enum Units<'a> { Utf8(str::EncodeUtf16<'a>), Utf16(slice::Iter<'a, u16>) }

impl<'a> Utf16Units<'a> {
    pub(super) fn new(source: Source<'a>, start: usize, end: usize) -> Self {
        let source = match source {
            Source::Utf8(value) => {
                let mut units = value.encode_utf16();
                if start != 0 { units.nth(start - 1); }
                Units::Utf8(units)
            }
            Source::Utf16(value) => Units::Utf16(value[start..end].iter()),
        };
        Self { source, remaining: end - start }
    }
}

impl Iterator for Utf16Units<'_> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        if self.remaining == 0 { return None; }
        self.remaining -= 1;
        match &mut self.source { Units::Utf8(value) => value.next(), Units::Utf16(value) => value.next().copied() }
    }
    fn size_hint(&self) -> (usize, Option<usize>) { (self.remaining, Some(self.remaining)) }
}
impl ExactSizeIterator for Utf16Units<'_> {}
impl FusedIterator for Utf16Units<'_> {}

/// Java code points, including unmatched surrogate unit values, from one borrowed range.
/// Only an adjacent high/low pair within that range is combined; traversal allocates nothing.
#[derive(Clone)]
pub struct JavaCodePoints<'a> { units: Peekable<Utf16Units<'a>> }

impl<'a> JavaCodePoints<'a> {
    pub(super) fn new(units: Utf16Units<'a>) -> Self { Self { units: units.peekable() } }
}

impl Iterator for JavaCodePoints<'_> {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        let high = self.units.next()?;
        if (0xd800..=0xdbff).contains(&high) {
            if let Some(low) = self.units.peek().copied().filter(|low| (0xdc00..=0xdfff).contains(low)) {
                self.units.next();
                return Some(0x10000 + ((u32::from(high) - 0xd800) << 10) + u32::from(low) - 0xdc00);
            }
        }
        Some(u32::from(high))
    }
    fn size_hint(&self) -> (usize, Option<usize>) { let units = self.units.len(); (units.div_ceil(2), Some(units)) }
}
impl FusedIterator for JavaCodePoints<'_> {}

/// Bytes from explicit Java String UTF8 encoding, with fixed pending-byte storage.
/// Valid pairs encode one supplementary point and each unmatched unit encodes byte `?`.
#[derive(Clone)]
pub struct JavaUtf8Bytes<'a> { points: JavaCodePoints<'a>, pending: [u8; 4], start: usize, end: usize }

impl<'a> JavaUtf8Bytes<'a> {
    pub(super) fn new(points: JavaCodePoints<'a>) -> Self { Self { points, pending: [0; 4], start: 0, end: 0 } }
}

impl Iterator for JavaUtf8Bytes<'_> {
    type Item = u8;
    fn next(&mut self) -> Option<u8> {
        if self.start == self.end {
            let point = self.points.next()?;
            let character = char::from_u32(point).unwrap_or('?');
            self.end = character.encode_utf8(&mut self.pending).len();
            self.start = 0;
        }
        let byte = self.pending[self.start];
        self.start += 1;
        Some(byte)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let pending = self.end - self.start;
        let (lower, upper) = self.points.size_hint();
        (lower.saturating_add(pending), upper.and_then(|points| points.checked_mul(4)).and_then(|bytes| bytes.checked_add(pending)))
    }
}
impl FusedIterator for JavaUtf8Bytes<'_> {}
