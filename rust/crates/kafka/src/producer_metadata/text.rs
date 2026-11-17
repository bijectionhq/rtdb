use std::cmp::Ordering;

/// Borrowed bytes with OpenJDK String's UTF8 replacement grouping and UTF16 comparison semantics.
/// Decoding, measurement, comparison and hashing allocate no output storage.
#[derive(Clone, Copy, Debug)]
pub struct JavaText<'a>(&'a [u8]);

/// A text-size failure before any output bytes are written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextError {
    /// The measured decoded size exceeds usize.
    LengthOverflow,
    /// The caller's output has fewer bytes than required.
    OutputTooSmall { required: usize },
}

impl<'a> JavaText<'a> {
    /// Borrows original bytes; malformed sequences are replaced only while decoding.
    pub const fn new(bytes: &'a [u8]) -> Self { Self(bytes) }
    /// Returns the original bytes, before replacement expansion.
    pub fn raw(self) -> &'a [u8] { self.0 }
    /// Iterates Unicode scalars with Java's malformed-input consumption boundaries.
    pub fn chars(self) -> Scalars<'a> { Scalars(self.0) }
    /// Iterates Java UTF16 units, including pairs for supplementary scalars.
    pub fn utf16(self) -> Units<'a> { Units { scalars: self.chars(), pending: None } }
    /// Measures the exact replacement-decoded UTF8 bytes required.
    pub fn utf8_len(self) -> Result<usize, TextError> {
        self.chars().try_fold(0usize, |length, character| length.checked_add(character.len_utf8()).ok_or(TextError::LengthOverflow))
    }
    /// Measures Java UTF16 code units, counting supplementary scalars twice.
    /// Returns LengthOverflow if the count exceeds usize.
    pub fn utf16_len(self) -> Result<usize, TextError> {
        self.chars().try_fold(0usize, |length, character| length.checked_add(character.len_utf16()).ok_or(TextError::LengthOverflow))
    }
    /// Returns String.hashCode over the replacement-decoded UTF16 units with signed wrapping arithmetic.
    pub fn hash_code(self) -> i32 {
        self.utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit)))
    }
    /// Compares decoded Java string units against an existing valid UTF8 string.
    pub fn equals_str(self, text: &str) -> bool { self.utf16().eq(text.encode_utf16()) }
    /// Writes decoded UTF8 into caller-owned storage and returns its byte length.
    /// A short output or size overflow leaves the entire destination unchanged; an extra suffix is untouched.
    pub fn write_utf8(self, output: &mut [u8]) -> Result<usize, TextError> {
        let required = self.utf8_len()?;
        if output.len() < required { return Err(TextError::OutputTooSmall { required }); }
        let mut at = 0;
        for character in self.chars() {
            let mut bytes = [0; 4];
            let text = character.encode_utf8(&mut bytes);
            output[at..at+text.len()].copy_from_slice(text.as_bytes());
            at += text.len();
        }
        Ok(at)
    }
}
impl PartialEq for JavaText<'_> { fn eq(&self, other: &Self) -> bool { self.utf16().eq(other.utf16()) } }
impl Eq for JavaText<'_> {}
impl PartialOrd for JavaText<'_> { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl Ord for JavaText<'_> { fn cmp(&self, other: &Self) -> Ordering { self.utf16().cmp(other.utf16()) } }

/// A borrowed scalar iterator; malformed groups each yield one replacement character.
#[derive(Clone, Debug)]
pub struct Scalars<'a>(&'a [u8]);
impl Iterator for Scalars<'_> {
    type Item = char;
    fn next(&mut self) -> Option<char> {
        let first = *self.0.first()?;
        let width = match first { 0..=127 => 1, 0xc2..=0xdf => 2, 0xe0..=0xef => 3, 0xf0..=0xf4 => 4, _ => 0 };
        if width <= 1 {
            self.0 = &self.0[1..];
            return Some(if width == 1 { char::from(first) } else { '\u{fffd}' });
        }
        let mut scalar = u32::from(first & (0x7f >> width));
        for index in 1..width {
            let Some(&byte) = self.0.get(index) else { self.0 = &[]; return Some('\u{fffd}'); };
            let valid_second = index != 1 || match first { 0xe0 => byte >= 0xa0, 0xf0 => byte >= 0x90, 0xf4 => byte <= 0x8f, _ => true };
            if byte & 0xc0 != 0x80 || !valid_second {
                self.0 = &self.0[index..];
                return Some('\u{fffd}');
            }
            scalar = (scalar << 6) | u32::from(byte & 0x3f);
        }
        self.0 = &self.0[width..];
        Some(char::from_u32(scalar).unwrap_or('\u{fffd}'))
    }
}
impl std::iter::FusedIterator for Scalars<'_> {}

/// A borrowed UTF16 iterator over the Java replacement-decoded string.
#[derive(Clone, Debug)]
pub struct Units<'a> { scalars: Scalars<'a>, pending: Option<u16> }
impl Iterator for Units<'_> {
    type Item = u16;
    fn next(&mut self) -> Option<u16> {
        if let Some(unit) = self.pending.take() { return Some(unit); }
        let character = self.scalars.next()?;
        let mut buffer = [0; 2];
        let units = character.encode_utf16(&mut buffer);
        self.pending = units.get(1).copied();
        Some(units[0])
    }
}
impl std::iter::FusedIterator for Units<'_> {}

#[cfg(test)]
#[path = "text/tests.rs"]
mod tests;
