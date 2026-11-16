use bigdecimal::{BigDecimal, RoundingMode};
use num_bigint::{BigInt, Sign};
use num_traits::{Signed, Zero};
use rtdb_plan::BinaryOp;
use rtdb_types::{Anomaly, DecimalValue, Result};
use std::cmp::Ordering;
use std::str::FromStr;
use crate::numeric::{division_by_zero, inapplicable, overflow};

fn coefficient(value: &DecimalValue) -> BigInt { BigInt::from_signed_bytes_be(&value.unscaled) }
fn digits(value: &BigInt) -> usize { value.to_str_radix(10).trim_start_matches('-').len() }
fn power(exponent: i64) -> Result<BigInt> { Ok(BigInt::from(10u8).pow(u32::try_from(exponent).map_err(|_| overflow())?)) }

/// Parses OpenJDK 21 decimal syntax with a signed 32-bit result scale.
/// BMP decimal digits are normalized; supplementary-plane digits and non-ASCII signs are rejected.
/// Normalization and coefficient allocation are proportional to input bytes and must be reserved by the caller.
pub fn parse_decimal(text: &str) -> Result<BigDecimal> {
    let invalid = || Anomaly::incorrect("xtdb/invalid-decimal", "Invalid decimal text or scale");
    let spelling = rtdb_types::decimal_spelling(text)?;
    let value = BigDecimal::from_str(spelling.ascii()).map_err(|_| invalid())?;
    Ok(value)
}

pub(crate) fn from_coefficient(value: BigInt, precision: u8, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    if digits(&value) > usize::from(precision) { return Err(overflow()); }
    let bytes = value.to_signed_bytes_be();
    let width = usize::from(bit_width / 8);
    if bytes.len() > width { return Err(overflow()); }
    let mut padded = vec![if value.sign() == Sign::Minus { 0xff } else { 0 }; width - bytes.len()];
    padded.extend(bytes);
    DecimalValue::new(padded, precision, scale, bit_width)
}

fn round_quotient(quotient: BigInt, remainder: &BigInt, divisor: &BigInt, sign: Sign, mode: RoundingMode) -> BigInt {
    if remainder.is_zero() { return quotient; }
    let half = (remainder.abs() * 2u8).cmp(&divisor.abs());
    let increment = match mode {
        RoundingMode::Up => true,
        RoundingMode::Down => false,
        RoundingMode::Ceiling => sign != Sign::Minus,
        RoundingMode::Floor => sign == Sign::Minus,
        RoundingMode::HalfUp => half != Ordering::Less,
        RoundingMode::HalfDown => half == Ordering::Greater,
        RoundingMode::HalfEven => half == Ordering::Greater || (half == Ordering::Equal && (&quotient % 2u8) != BigInt::zero()),
    };
    if increment { quotient + if sign == Sign::Minus { -1 } else { 1 } } else { quotient }
}

pub(crate) fn rescale_coefficient(value: BigInt, source_scale: i64, target_scale: i32, precision: u8, mode: RoundingMode) -> Result<BigInt> {
    if value.is_zero() { return Ok(value); }
    let shift = i64::from(target_scale).checked_sub(source_scale).ok_or_else(overflow)?;
    if shift.unsigned_abs() > i32::MAX as u64 { return Err(overflow()); }
    let count = i64::try_from(digits(&value)).map_err(|_| overflow())?;
    if shift >= 0 {
        if count.checked_add(shift).ok_or_else(overflow)? > i64::from(precision) { return Err(overflow()); }
        return Ok(value * power(shift)?);
    }
    if shift < -count {
        return Ok(match mode {
            RoundingMode::Up => BigInt::from(if value.sign() == Sign::Minus { -1 } else { 1 }),
            RoundingMode::Floor if value.sign() == Sign::Minus => (-1).into(),
            RoundingMode::Ceiling if value.sign() != Sign::Minus => 1.into(),
            _ => BigInt::zero(),
        });
    }
    let divisor = power(-shift)?;
    Ok(round_quotient(&value / &divisor, &(&value % &divisor), &divisor, value.sign(), mode))
}

pub(crate) fn from_bigdecimal(value: BigDecimal, precision: u8, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    let (value, source_scale) = value.into_bigint_and_scale();
    let source_scale = i32::try_from(source_scale).map_err(|_| overflow())?;
    from_coefficient(rescale_coefficient(value, i64::from(source_scale), scale, precision, RoundingMode::HalfEven)?, precision, scale, bit_width)
}

/// Applies Java HALF_EVEN scaling and checks the requested vector precision and coefficient width.
/// Work is bounded by coefficient digits; distant scales return zero or overflow without allocating their exponent.
pub fn decimal_rescale(value: &DecimalValue, precision: u8, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    from_coefficient(rescale_coefficient(coefficient(value), i64::from(value.scale), scale, precision, RoundingMode::HalfEven)?, precision, scale, bit_width)
}

pub(crate) fn rounded(value: &DecimalValue, scale: i32, mode: RoundingMode) -> Result<DecimalValue> {
    from_coefficient(rescale_coefficient(coefficient(value), i64::from(value.scale), scale, value.precision, mode)?, value.precision, scale, value.bit_width)
}

/// Formats the canonical Java BigDecimal representation, including its scale and scientific exponent.
/// The output length is bounded by coefficient digits plus exponent digits, including at i32 scale limits.
pub fn decimal_text(value: &DecimalValue) -> String {
    let coefficient = coefficient(value);
    let signed = coefficient.to_str_radix(10);
    let digits = signed.trim_start_matches('-');
    let adjusted = digits.len() as i64 - 1 - i64::from(value.scale);
    if value.scale == 0 { return signed; }
    let mut result = String::new();
    if coefficient.sign() == Sign::Minus { result.push('-'); }
    if value.scale >= 0 && adjusted >= -6 {
        let point = digits.len() as i64 - i64::from(value.scale);
        if point > 0 {
            let point = point as usize;
            result.push_str(&digits[..point]); result.push('.'); result.push_str(&digits[point..]);
        } else {
            result.push_str("0."); result.extend(std::iter::repeat_n('0', (-point) as usize)); result.push_str(digits);
        }
    } else {
        result.push_str(&digits[..1]);
        if digits.len() > 1 { result.push('.'); result.push_str(&digits[1..]); }
        result.push('E');
        if adjusted >= 0 { result.push('+'); }
        result.push_str(&adjusted.to_string());
    }
    result
}

/// Truncates toward zero and returns the low signed 64 bits, as Java BigDecimal.longValue does.
pub fn decimal_long_value(value: &DecimalValue) -> i64 {
    let mut coefficient = coefficient(value);
    if value.scale > 0 {
        if i64::from(value.scale) >= digits(&coefficient) as i64 { return 0; }
        coefficient /= BigInt::from(10u8).pow(value.scale as u32);
    } else if value.scale < 0 {
        if value.scale <= -64 { return 0; }
        coefficient *= BigInt::from(10u8).pow((-value.scale) as u32);
    }
    let bytes = coefficient.to_signed_bytes_be();
    let mut low = [if coefficient.sign() == Sign::Minus { 0xff } else { 0 }; 8];
    let len = bytes.len().min(8);
    low[8 - len..].copy_from_slice(&bytes[bytes.len() - len..]);
    i64::from_be_bytes(low)
}

/// Converts through a bounded decimal representation, retaining signed underflow and IEEE infinities.
pub fn decimal_to_f64(value: &DecimalValue) -> f64 {
    decimal_text(value).parse().unwrap_or_else(|_| if coefficient(value).sign() == Sign::Minus { f64::NEG_INFINITY } else { f64::INFINITY })
}

pub(crate) fn compare_parts(left: &BigInt, left_scale: i64, right: &BigInt, right_scale: i64) -> Ordering {
    let sign = left.sign().cmp(&right.sign());
    if sign != Ordering::Equal { return sign; }
    if left.is_zero() { return Ordering::Equal; }
    let (a, b) = (left.abs().to_str_radix(10), right.abs().to_str_radix(10));
    let order = (a.len() as i128 - i128::from(left_scale)).cmp(&(b.len() as i128 - i128::from(right_scale)));
    let order = if order != Ordering::Equal { order } else {
        a.bytes().chain(std::iter::repeat(b'0')).zip(b.bytes().chain(std::iter::repeat(b'0'))).take(a.len().max(b.len())).map(|(a, b)| a.cmp(&b)).find(|order| *order != Ordering::Equal).unwrap_or(Ordering::Equal)
    };
    if left.sign() == Sign::Minus { order.reverse() } else { order }
}

fn align(value: BigInt, shift: i64, maximum: usize) -> Result<BigInt> {
    if value.is_zero() { return Ok(value); }
    if digits(&value) as i64 + shift > maximum as i64 { return Err(overflow()); }
    Ok(value * power(shift)?)
}

fn factors(mut value: BigInt, factor: u8) -> i64 {
    let mut count = 0;
    while !value.is_zero() && (&value % factor).is_zero() { value /= factor; count += 1; }
    count
}

fn modulo(a: BigInt, ls: i64, b: BigInt, rs: i64, target: i64, precision: u8) -> Result<BigInt> {
    if b.is_zero() { return Err(division_by_zero()); }
    let (left, right) = (a.abs(), b.abs());
    let (mut remainder, mut result_scale) = if compare_parts(&left, ls, &right, rs) == Ordering::Less {
        (a.clone(), ls)
    } else if ls >= rs {
        let denominator = align(b.clone(), ls - rs, digits(&a).max(digits(&b)) + 1)?;
        (a.clone() % denominator, ls)
    } else {
        let shift = rs - ls;
        let remainder = (&left % &right) * BigInt::from(10u8).modpow(&BigInt::from(shift), &right) % &right;
        let quotient_zeros = if shift <= (left.bits() + right.bits() + 1) as i64 {
            let quotient = left.clone() * power(shift)? / &right;
            quotient.to_str_radix(10).bytes().rev().take_while(|byte| *byte == b'0').count() as i64
        } else if remainder.is_zero() {
            (shift + factors(left.clone(), 2) - factors(right.clone(), 2)).min(shift + factors(left, 5) - factors(right, 5))
        } else {
            (factors(remainder.clone(), 2) - factors(right.clone(), 2)).min(factors(remainder.clone(), 5) - factors(right, 5))
        };
        let result_scale = rs - shift.min(quotient_zeros);
        let remainder = if remainder.is_zero() { remainder } else { remainder / power(rs - result_scale)? };
        (if a.sign() == Sign::Minus { -remainder } else { remainder }, result_scale)
    };
    if !remainder.is_zero() && a.sign() != b.sign() {
        let scale = result_scale.max(rs);
        let maximum = digits(&remainder).max(digits(&b)).max(usize::from(precision)) + 1;
        remainder = align(remainder, scale - result_scale, maximum)? + align(b, scale - rs, maximum)?;
        result_scale = scale;
    }
    if result_scale != target { return Err(overflow()); }
    Ok(remainder)
}

pub(crate) fn arithmetic(op: BinaryOp, left: &DecimalValue, right: &DecimalValue, precision: u8, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    let (a, b) = (coefficient(left), coefficient(right));
    let (ls, rs, target) = (i64::from(left.scale), i64::from(right.scale), i64::from(scale));
    let result = match op {
        BinaryOp::Add | BinaryOp::Subtract => {
            let maximum = digits(&a).max(digits(&b)).max(usize::from(precision)) + 1;
            let (a, b) = (align(a, target - ls, maximum)?, align(b, target - rs, maximum)?);
            if op == BinaryOp::Add { a + b } else { a - b }
        }
        BinaryOp::Multiply => a * b,
        BinaryOp::Divide => {
            if b.is_zero() { return Err(division_by_zero()); }
            if a.is_zero() { BigInt::zero() } else {
                let raised_scale = i32::try_from(target + rs).map_err(|_| overflow())?;
                if i64::from(raised_scale) <= ls { i32::try_from(ls - target).map_err(|_| overflow())?; }
                let shift = target + rs - ls;
                if shift.unsigned_abs() > i32::MAX as u64 { return Err(overflow()); }
                let magnitude = digits(&a) as i64 - digits(&b) as i64 + shift;
                if magnitude > i64::from(precision) { return Err(overflow()); }
                if magnitude < -1 { BigInt::zero() } else {
                    let (a, b) = if shift >= 0 { (a * power(shift)?, b) } else { (a, b * power(-shift)?) };
                    let sign = if a.sign() == b.sign() { Sign::Plus } else { Sign::Minus };
                    round_quotient(&a / &b, &(&a % &b), &b, sign, RoundingMode::HalfEven)
                }
            }
        }
        BinaryOp::Modulo => {
            modulo(a, ls, b, rs, target, precision)?
        }
        _ => return Err(inapplicable("decimal arithmetic", &[])),
    };
    from_coefficient(result, precision, scale, bit_width)
}
