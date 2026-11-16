use crate::{Anomaly, Result};
use std::borrow::Cow;

/// A validated Java decimal spelling with ASCII digits and its signed 32-bit scale.
/// ASCII input is borrowed; normalization retains at most the input's UTF-8 byte length.
#[derive(Debug, PartialEq, Eq)]
pub struct DecimalSpelling<'a> {
    ascii: Cow<'a, str>,
    scale: i32,
}

impl DecimalSpelling<'_> {
    /// Borrows the normalized spelling, including the original ASCII signs, point, and exponent marker.
    pub fn ascii(&self) -> &str { &self.ascii }

    /// Returns fractional digit count minus the signed exponent.
    pub fn scale(&self) -> i32 { self.scale }
}

/// Returns OpenJDK 21's `Character.digit(char, 10)` value.
/// Only Unicode 15.0 BMP decimal digits qualify; supplementary-plane digits return `None`.
pub fn decimal_digit(character: char) -> Option<u8> {
    let code = u32::from(character);
    if character.is_ascii_digit() { return Some(code as u8 - b'0'); }
    // OpenJDK jdk-21+35 src/java.base/share/data/unicodedata/UnicodeData.txt, category Nd.
    const ZEROES: &[u32] = &[
        0x0660, 0x06f0, 0x07c0, 0x0966, 0x09e6, 0x0a66, 0x0ae6, 0x0b66,
        0x0be6, 0x0c66, 0x0ce6, 0x0d66, 0x0de6, 0x0e50, 0x0ed0, 0x0f20,
        0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90,
        0x1b50, 0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0,
        0xa9f0, 0xaa50, 0xabf0, 0xff10,
    ];
    let index = ZEROES.partition_point(|zero| *zero <= code).checked_sub(1)?;
    let digit = code - ZEROES[index];
    (digit < 10).then_some(digit as u8)
}

/// Normalizes Java decimal digits and validates the coefficient, exponent, and final scale.
/// - ASCII input allocates nothing; other input allocates at most `text.len()` bytes before validation.
///   The caller must include that scratch/retained allocation in its input reservation.
/// - Invalid syntax or a final scale outside signed 32 bits returns `xtdb/invalid-decimal`.
pub fn decimal_spelling(text: &str) -> Result<DecimalSpelling<'_>> {
    let ascii = if text.is_ascii() { Cow::Borrowed(text) } else {
        let mut normalized = String::with_capacity(text.len());
        for character in text.chars() {
            if character.is_ascii() { normalized.push(character); }
            else { normalized.push(char::from(b'0' + decimal_digit(character).ok_or_else(invalid)?)); }
        }
        Cow::Owned(normalized)
    };
    let scale = ascii_scale(&ascii)?;
    Ok(DecimalSpelling { ascii, scale })
}

/// Validates a Java decimal spelling and returns its signed 32-bit scale.
/// Unicode normalization has the same allocation contract as `decimal_spelling`.
pub fn decimal_text_scale(text: &str) -> Result<i32> { Ok(decimal_spelling(text)?.scale()) }

fn invalid() -> Anomaly { Anomaly::incorrect("xtdb/invalid-decimal", "Invalid decimal text or scale") }

fn ascii_scale(text: &str) -> Result<i32> {
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (mantissa, exponent) = unsigned.find(['e', 'E']).map_or((unsigned, None), |index| (&unsigned[..index], Some(&unsigned[index + 1..])));
    let (mut point, mut count, mut fraction) = (false, 0usize, 0i64);
    for byte in mantissa.bytes() {
        if byte.is_ascii_digit() { count += 1; if point { fraction = fraction.checked_add(1).ok_or_else(invalid)?; } }
        else if byte == b'.' && !point { point = true; }
        else { return Err(invalid()); }
    }
    if count == 0 { return Err(invalid()); }
    let exponent = if let Some(exponent) = exponent {
        let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) || digits.trim_start_matches('0').len() > 10 { return Err(invalid()); }
        exponent.parse::<i64>().map_err(|_| invalid())?
    } else { 0 };
    i32::try_from(fraction.checked_sub(exponent).ok_or_else(invalid)?).map_err(|_| invalid())
}
