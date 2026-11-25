use std::fmt::{self, Write as _};
use arrow_buffer::i256;
use num_bigint::{BigInt,Sign};

/// Formats a Java BigDecimal coefficient and signed scale without exponent-sized padding.
/// The caller admits coefficient scratch and the returned text; storage is proportional to coefficient digits.
pub fn decimal_parts_text(coefficient: &BigInt,scale: i32) -> String {
    let signed = coefficient.to_str_radix(10);
    let mut result = String::new();
    write_decimal_digits(&mut result, signed.trim_start_matches('-'), coefficient.sign() == Sign::Minus, scale)
        .expect("writing to String");
    result
}

pub fn decimal_coefficient_display(coefficient: [u8; 32], scale: i32) -> impl fmt::Display {
    DecimalCoefficient { coefficient, scale }
}

pub fn write_decimal_coefficient(out: &mut String, coefficient: [u8; 32], scale: i32) {
    write!(out, "{}", decimal_coefficient_display(coefficient, scale)).expect("writing to String");
}

struct DecimalCoefficient {
    coefficient: [u8; 32],
    scale: i32,
}

impl fmt::Display for DecimalCoefficient {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut coefficient = i256::from_be_bytes(self.coefficient);
        let negative = coefficient < i256::ZERO;
        let mut digits = [0; 77];
        let mut start = digits.len();
        let divisor = i256::from(1_000_000_000_000_000_000i64);
        loop {
            let quotient = coefficient / divisor;
            let mut chunk = (coefficient - quotient * divisor).as_i128().unsigned_abs();
            let end = start;
            loop {
                start -= 1;
                digits[start] = b'0' + (chunk % 10) as u8;
                chunk /= 10;
                if chunk == 0 && (quotient == i256::ZERO || end - start == 18) { break; }
            }
            if quotient == i256::ZERO { break; }
            coefficient = quotient;
        }
        let digits = str::from_utf8(&digits[start..]).expect("coefficient digits are ASCII");
        write_decimal_digits(out, digits, negative, self.scale)
    }
}

fn write_decimal_digits(result: &mut impl fmt::Write, digits: &str, negative: bool, scale: i32) -> fmt::Result {
    let adjusted = digits.len() as i64 - 1 - i64::from(scale);
    if negative { result.write_char('-')?; }
    if scale == 0 { return result.write_str(digits); }
    if scale >= 0 && adjusted >= -6 {
        let point = digits.len() as i64 - i64::from(scale);
        if point > 0 {
            let point = point as usize;
            result.write_str(&digits[..point])?;
            result.write_char('.')?;
            result.write_str(&digits[point..])
        } else {
            result.write_str("0.")?;
            for _ in point..0 { result.write_char('0')?; }
            result.write_str(digits)
        }
    } else {
        result.write_str(&digits[..1])?;
        if digits.len() > 1 {
            result.write_char('.')?;
            result.write_str(&digits[1..])?;
        }
        result.write_char('E')?;
        if adjusted >= 0 { result.write_char('+')?; }
        write!(result, "{adjusted}")
    }
}
