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

fn physical_digits(bit_width: u16) -> Result<usize> {
    match bit_width { 32 => Ok(10), 64 => Ok(19), 128 => Ok(39), 256 => Ok(77), _ => Err(overflow()) }
}

fn write_digits(precision: i32, bit_width: u16) -> Result<usize> {
    if precision <= 0 { return Err(overflow()); }
    Ok((precision as usize).min(physical_digits(bit_width)?))
}

pub(crate) fn from_coefficient(value: BigInt, precision: i32, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    if digits(&value) > write_digits(precision, bit_width)? { return Err(overflow()); }
    store_coefficient(value, precision, scale, bit_width)
}

pub(crate) fn validate_write(value: &DecimalValue) -> Result<()> {
    validate_coefficient(&value.unscaled, value.precision, value.bit_width)
}

pub(crate) fn validate_coefficient(bytes: &[u8], precision: i32, bit_width: u16) -> Result<()> {
    physical_digits(bit_width)?;
    if bytes.len() != usize::from(bit_width / 8) { return Err(overflow()); }
    if precision <= 0 || digits(&BigInt::from_signed_bytes_be(bytes)) > precision as usize {
        return Err(Anomaly::incorrect("rtdb/decimal-precision", "Decimal coefficient exceeds its declared precision"));
    }
    Ok(())
}

fn store_coefficient(value: BigInt, precision: i32, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    physical_digits(bit_width)?;
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

pub(crate) fn rescale_coefficient(value: BigInt, source_scale: i64, target_scale: i32, maximum_digits: usize, mode: RoundingMode) -> Result<BigInt> {
    if value.is_zero() { return Ok(value); }
    let shift = i64::from(target_scale).checked_sub(source_scale).ok_or_else(overflow)?;
    if shift.unsigned_abs() > i32::MAX as u64 { return Err(overflow()); }
    let count = i64::try_from(digits(&value)).map_err(|_| overflow())?;
    if shift >= 0 {
        if count.checked_add(shift).ok_or_else(overflow)? > maximum_digits as i64 { return Err(overflow()); }
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

pub(crate) fn from_bigdecimal(value: BigDecimal, precision: i32, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    let params = rtdb_types::decimal_arrow::DecimalParameters { precision, scale, bit_width };
    let bytes = cast_coefficient(value, params)?;
    DecimalValue::new(bytes[32 - usize::from(bit_width / 8)..].to_vec(), precision, scale, bit_width)
}

pub(crate) fn cast_coefficient(value: BigDecimal, params: rtdb_types::decimal_arrow::DecimalParameters) -> Result<[u8; 32]> {
    let (value, source_scale) = value.into_bigint_and_scale();
    let source_scale = i32::try_from(source_scale).map_err(|_| overflow())?;
    let digits_limit = write_digits(params.precision, params.bit_width)?;
    let value = rescale_coefficient(value, i64::from(source_scale), params.scale, digits_limit, RoundingMode::HalfEven)?;
    if digits(&value) > digits_limit { return Err(overflow()); }
    let bytes = value.to_signed_bytes_be();
    if bytes.len() > usize::from(params.bit_width / 8) { return Err(overflow()); }
    let mut extended = [if value.sign() == Sign::Minus { 255 } else { 0 }; 32];
    extended[32 - bytes.len()..].copy_from_slice(&bytes);
    Ok(extended)
}

/// Applies Java HALF_EVEN scaling and checks the requested vector precision and coefficient width.
/// Work is bounded by coefficient digits; distant scales return zero or overflow without allocating their exponent.
pub fn decimal_rescale(value: &DecimalValue, precision: i32, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    from_coefficient(rescale_coefficient(coefficient(value), i64::from(value.scale), scale, write_digits(precision, bit_width)?, RoundingMode::HalfEven)?, precision, scale, bit_width)
}

/// Infers the reference object writer's 32/128 or 64/256 precision/width pair from coefficient digits.
/// The scale is retained; invalid source widths and coefficients above 64 digits fail.
/// The caller reserves 4096 bytes for coefficient scratch and the returned allocation before calling.
pub fn infer_decimal(value: &DecimalValue) -> Result<DecimalValue> {
    physical_digits(value.bit_width)?;
    if value.unscaled.len() != usize::from(value.bit_width / 8) { return Err(overflow()); }
    let coefficient = coefficient(value);
    let count = digits(&coefficient);
    let (precision, bit_width) = match count {
        0..=32 => (32, 128),
        33..=64 => (64, 256),
        _ => return Err(Anomaly::unsupported("", format!("Unsupported precision: {count}"))),
    };
    store_coefficient(coefficient, precision, value.scale, bit_width)
}

/// Accumulates one DecimalVector cell, retaining the output vector's metadata without precision validation.
/// An absent previous cell starts at coefficient zero at the output scale; an absent input contributes scale-zero zero.
/// The coefficient of the addition is stored without rescaling to the output scale, matching vector SUM.
/// Input coefficients must fill their declared widths; signed output-width overflow returns an error.
/// A previous cell must have exactly the requested precision, scale, and bit width.
pub fn decimal_sum(previous: Option<&DecimalValue>, input: Option<&DecimalValue>, precision: i32, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    let maximum = physical_digits(bit_width)?;
    for value in [previous, input].into_iter().flatten() {
        physical_digits(value.bit_width)?;
        if value.unscaled.len() != usize::from(value.bit_width / 8) { return Err(overflow()); }
    }
    if previous.is_some_and(|value| value.precision != precision || value.scale != scale || value.bit_width != bit_width) { return Err(overflow()); }
    let left = previous.map(coefficient).unwrap_or_default();
    let right = input.map(coefficient).unwrap_or_default();
    let input_scale = input.map_or(0, |value| value.scale);
    let result_scale = scale.max(input_scale);
    let maximum = maximum.max(digits(&left)).max(digits(&right)) + 1;
    let result = align(left, i64::from(result_scale) - i64::from(scale), maximum)?
        + align(right, i64::from(result_scale) - i64::from(input_scale), maximum)?;
    store_coefficient(result, precision, scale, bit_width)
}

pub(crate) fn rounded(value: &DecimalValue, scale: i32, mode: RoundingMode) -> Result<DecimalValue> {
    from_coefficient(rescale_coefficient(coefficient(value), i64::from(value.scale), scale, write_digits(value.precision, value.bit_width)?, mode)?, value.precision, scale, value.bit_width)
}

/// Formats the canonical Java BigDecimal representation, including its scale and scientific exponent.
/// The output length is bounded by coefficient digits plus exponent digits, including at i32 scale limits.
pub fn decimal_text(value: &DecimalValue) -> String {
    decimal_parts_text(&coefficient(value),value.scale)
}

pub(crate) use rtdb_types::jvm_number::decimal_parts_text;

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

/// Rounds a decimal to binary64, retaining signed underflow and IEEE infinities.
pub fn decimal_to_f64(value: &DecimalValue) -> f64 {
    rtdb_types::jvm_number::decimal_bytes_to_f64(&value.unscaled, value.scale)
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

fn modulo(a: BigInt, ls: i64, b: BigInt, rs: i64, target: i64, output_digits: usize) -> Result<BigInt> {
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
        if !remainder.is_zero() && shift > i64::from(i32::MAX) { return Err(overflow()); }
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
        let maximum = digits(&remainder).max(digits(&b)).max(output_digits) + 1;
        remainder = align(remainder, scale - result_scale, maximum)? + align(b, scale - rs, maximum)?;
        result_scale = scale;
    }
    if result_scale != target { return Err(overflow()); }
    Ok(remainder)
}

pub(crate) fn arithmetic(op: BinaryOp, left: &DecimalValue, right: &DecimalValue, precision: i32, scale: i32, bit_width: u16) -> Result<DecimalValue> {
    let output_digits = physical_digits(bit_width)?;
    let (a, b) = (coefficient(left), coefficient(right));
    let (ls, rs, target) = (i64::from(left.scale), i64::from(right.scale), i64::from(scale));
    let result = match op {
        BinaryOp::Add | BinaryOp::Subtract => {
            let maximum = digits(&a).max(digits(&b)).max(output_digits) + 1;
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
                if magnitude > i64::from(precision).min(output_digits as i64) { return Err(overflow()); }
                if magnitude < -1 { BigInt::zero() } else {
                    let (a, b) = if shift >= 0 { (a * power(shift)?, b) } else { (a, b * power(-shift)?) };
                    let sign = if a.sign() == b.sign() { Sign::Plus } else { Sign::Minus };
                    round_quotient(&a / &b, &(&a % &b), &b, sign, RoundingMode::HalfEven)
                }
            }
        }
        BinaryOp::Modulo => {
            modulo(a, ls, b, rs, target, output_digits)?
        }
        _ => return Err(inapplicable("decimal arithmetic", &[])),
    };
    from_coefficient(result, precision, scale, bit_width)
}
