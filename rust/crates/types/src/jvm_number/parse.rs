use num_bigint::BigUint;
use num_traits::{One,ToPrimitive,Zero};
use super::{Number,Result,invalid};

pub(super) fn long(text: &str) -> Result<Number> {
    let (negative,digits) = match text.as_bytes().first() { Some(b'-') => (true,&text[1..]),Some(b'+') => (false,&text[1..]),_ => (false,text) };
    if digits.is_empty() { return Err(invalid()); }
    let limit = if negative { i64::MIN } else { -i64::MAX };
    let mut value = 0i64;
    for ch in digits.chars() {
        let digit = i64::from(crate::decimal_digit(ch).ok_or_else(invalid)?);
        value = value.checked_mul(10).and_then(|value|value.checked_sub(digit)).filter(|value|*value >= limit).ok_or_else(invalid)?;
    }
    Ok(Number::Long(if negative { value } else { -value }))
}

pub(super) fn float(text: &str, single: bool) -> Result<Number> {
    let text = text.trim_matches(|ch: char|ch <= '\u{20}');
    let (negative,unsigned) = match text.as_bytes().first() { Some(b'-') => (true,&text[1..]),Some(b'+') => (false,&text[1..]),_ => (false,text) };
    if unsigned == "NaN" { return Ok(if single { Number::Float(f32::NAN) } else { Number::Double(f64::NAN) }); }
    let infinity = unsigned == "Infinity";
    let unsigned = unsigned.strip_suffix(['f','F','d','D']).unwrap_or(unsigned);
    let hex = unsigned.strip_prefix("0x").or_else(|| unsigned.strip_prefix("0X"));
    let decimal = unsigned.starts_with(|ch: char| ch.is_ascii_digit() || ch == '.');
    if single {
        let value = if infinity { f32::INFINITY }
            else if let Some(hex) = hex { f32::from_bits(hexadecimal(hex, 23, 127)? as u32) }
            else if decimal { unsigned.parse::<f32>().map_err(|_| invalid())? }
            else { return Err(invalid()); };
        Ok(Number::Float(if negative { -value } else { value }))
    } else {
        let value = if infinity { f64::INFINITY }
            else if let Some(hex) = hex { f64::from_bits(hexadecimal(hex, 52, 1023)?) }
            else if decimal { unsigned.parse::<f64>().map_err(|_| invalid())? }
            else { return Err(invalid()); };
        Ok(Number::Double(if negative { -value } else { value }))
    }
}

fn hex_digits(text: &str) -> Result<(String,usize)> {
    let mut digits = String::new();let mut fraction = 0;let mut dot = false;
    for byte in text.bytes() {
        if byte == b'.' && !dot { dot = true;continue; }
        if !byte.is_ascii_hexdigit() { return Err(invalid()); }
        digits.push(char::from(byte));
        if dot { fraction += 1; }
    }
    if digits.is_empty() { return Err(invalid()); }Ok((digits,fraction))
}

fn signed_exponent(text: &str) -> Result<i64> {
    let (negative,text) = match text.as_bytes().first() { Some(b'-') => (true,&text[1..]),Some(b'+') => (false,&text[1..]),_ => (false,text) };
    if text.is_empty() || !text.bytes().all(|byte|byte.is_ascii_digit()) { return Err(invalid()); }
    let exponent = text.bytes().fold(0i64,|value,byte|value.saturating_mul(10).saturating_add(i64::from(byte - b'0')));
    Ok(if negative { -exponent } else { exponent })
}

fn hexadecimal(text: &str, fraction_bits: i64, bias: i64) -> Result<u64> {
    let (mantissa,exponent) = text.split_once(['p','P']).ok_or_else(invalid)?;
    let exponent = signed_exponent(exponent)?;
    let (digits,fraction) = hex_digits(mantissa)?;
    let significand = BigUint::parse_bytes(digits.as_bytes(),16).ok_or_else(invalid)?;
    if significand.is_zero() { return Ok(0); }
    let exponent = exponent.saturating_sub(i64::try_from(fraction).map_err(|_|invalid())?.saturating_mul(4));
    let significand_top = i64::try_from(significand.bits()).map_err(|_|invalid())? - 1;
    let top = exponent.saturating_add(significand_top);
    let infinity = ((2 * bias + 1) as u64) << fraction_bits;
    let min_normal = 1 - bias;
    let min_subnormal = min_normal - fraction_bits;
    if top > bias { return Ok(infinity); }
    if top < min_subnormal - 1 { return Ok(0); }
    let shift = if top >= min_normal { significand_top - fraction_bits } else { min_subnormal - exponent };
    let mut rounded = (if shift > 0 {
        let shift = usize::try_from(shift).map_err(|_|invalid())?;
        let mut quotient = &significand >> shift;
        let remainder = &significand - (&quotient << shift);let half = BigUint::one() << (shift - 1);
        if remainder > half || remainder == half && quotient.bit(0) { quotient += 1u8; }quotient
    } else { significand << usize::try_from(-shift).map_err(|_|invalid())? }).to_u64().ok_or_else(invalid)?;
    if top < min_normal { return Ok(rounded); }
    let mut top = top;
    if rounded == 2u64 << fraction_bits { rounded >>= 1;top += 1; }
    if top > bias { return Ok(infinity); }
    Ok(((top + bias) as u64) << fraction_bits | (rounded & ((1u64 << fraction_bits) - 1)))
}
