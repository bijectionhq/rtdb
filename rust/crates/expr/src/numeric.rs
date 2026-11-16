use bigdecimal::{BigDecimal, RoundingMode};
use num_bigint::BigInt;
use num_traits::{ToPrimitive, Zero};
use rtdb_plan::BinaryOp;
use rtdb_types::{Anomaly, DecimalValue, LogicalType, Result, ScalarType, Value};

pub fn integer(value: &Value) -> Option<i128> {
    match value {
        Value::I8(v) => Some((*v).into()), Value::I16(v) => Some((*v).into()),
        Value::I32(v) => Some((*v).into()), Value::I64(v) => Some((*v).into()),
        Value::U8(v) => Some((*v).into()), Value::U16(v) => Some((*v).into()),
        Value::U32(v) => Some((*v).into()), Value::U64(v) => Some((*v).into()),
        _ => None,
    }
}

pub fn number(value: &Value) -> Option<f64> {
    if let Some(value) = integer(value) { return Some(value as f64); }
    match value {
        Value::F16(bits) => Some(half::f16::from_bits(*bits).to_f64()),
        Value::F32(v) => Some(f64::from(*v)), Value::F64(v) => Some(*v),
        Value::Decimal(value) => decimal(value).to_f64(),
        _ => None,
    }
}

pub(crate) fn decimal(value: &DecimalValue) -> BigDecimal {
    BigDecimal::new(BigInt::from_signed_bytes_be(&value.unscaled), i64::from(value.scale))
}

pub(crate) fn exact_decimal(value: &Value) -> Option<BigDecimal> {
    if let Value::Decimal(value) = value { Some(decimal(value)) }
    else { integer(value).map(|value| BigDecimal::new(BigInt::from(value), 0)) }
}

pub(crate) fn decimal_value(value: BigDecimal, precision: u8, scale: i8, bit_width: u16) -> Result<Value> {
    let value = value.with_scale_round(i64::from(scale), RoundingMode::HalfEven);
    let (coefficient, _) = value.as_bigint_and_exponent();
    let digit_count = coefficient.to_str_radix(10).trim_start_matches('-').len();
    if digit_count > usize::from(precision) { return Err(overflow()); }
    let bytes = coefficient.to_signed_bytes_be();
    let width = usize::from(bit_width / 8);
    if bytes.len() > width { return Err(overflow()); }
    let mut padded = vec![if coefficient < BigInt::zero() { 0xff } else { 0 }; width - bytes.len()];
    padded.extend(bytes);
    Ok(Value::Decimal(DecimalValue::new(padded, precision, scale, bit_width)?))
}

pub(crate) fn integer_value(value: i128, ty: &ScalarType) -> Result<Value> {
    macro_rules! convert { ($ty:ty, $variant:ident) => { <$ty>::try_from(value).map(Value::$variant).map_err(|_| overflow()) }; }
    match ty {
        ScalarType::I8 => convert!(i8, I8), ScalarType::I16 => convert!(i16, I16),
        ScalarType::I32 => convert!(i32, I32), ScalarType::I64 => convert!(i64, I64),
        ScalarType::U8 => convert!(u8, U8), ScalarType::U16 => convert!(u16, U16),
        ScalarType::U32 => convert!(u32, U32), ScalarType::U64 => convert!(u64, U64),
        _ => Err(inapplicable("integer conversion", &[])),
    }
}

pub(crate) fn floating_value(value: f64, ty: &ScalarType) -> Value {
    match ty { ScalarType::F16 => Value::F16(half::f16::from_f64(value).to_bits()), ScalarType::F32 => Value::F32(value as f32), _ => Value::F64(value) }
}

pub(crate) fn overflow() -> Anomaly { Anomaly::incorrect("xtdb/numeric-value-out-of-range", "Numeric overflow") }
pub(crate) fn division_by_zero() -> Anomaly { Anomaly::incorrect("xtdb.expression/division-by-zero", "data exception - division by zero") }
pub(crate) fn inapplicable(name: &str, values: &[&Value]) -> Anomaly {
    Anomaly::incorrect("xtdb/invalid-argument-types", format!("{name} not applicable to types {:?}", values.iter().map(|value| value.logical_type()).collect::<Vec<_>>()))
}

pub fn arithmetic(op: BinaryOp, left: &Value, right: &Value) -> Result<Value> {
    arithmetic_in_timezone(op, left, right, "UTC")
}

/// Temporal operations interpret mixed local/zoned operands using the supplied session time zone.
pub fn arithmetic_in_timezone(op: BinaryOp, left: &Value, right: &Value, timezone: &str) -> Result<Value> {
    use BinaryOp as B;
    if matches!(left, Value::Null | Value::Undefined) || matches!(right, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    if let Some(result) = crate::temporal::arithmetic(op, left, right, timezone) { return result; }
    if matches!(op, B::ShiftLeft | B::ShiftRight) {
        let a = integer(left).ok_or_else(|| inapplicable("bit shift", &[left, right]))? as i64;
        let b = integer(right).ok_or_else(|| inapplicable("bit shift", &[left, right]))? as u32 & 63;
        let shifted = if op == B::ShiftLeft { a.wrapping_shl(b) } else { a >> b };
        return Ok(match left {
            Value::I8(_) => Value::I8(shifted as i8), Value::I16(_) => Value::I16(shifted as i16),
            Value::I32(_) => Value::I32(shifted as i32), Value::I64(_) => Value::I64(shifted),
            Value::U8(_) => Value::U8(shifted as u8), Value::U16(_) => Value::U16(shifted as u16),
            Value::U32(_) => Value::U32(shifted as u32), Value::U64(_) => Value::U64(shifted as u64),
            _ => return Err(inapplicable("bit shift", &[left, right])),
        });
    }
    if matches!(left, Value::Decimal(_)) && integer(right).is_some() {
        let right = decimal_value(exact_decimal(right).ok_or_else(overflow)?, 32, 0, 128)?;
        return arithmetic(op, left, &right);
    }
    if matches!(right, Value::Decimal(_)) && integer(left).is_some() {
        let left = decimal_value(exact_decimal(left).ok_or_else(overflow)?, 32, 0, 128)?;
        return arithmetic(op, &left, right);
    }
    if let (Value::Decimal(left), Value::Decimal(right)) = (left, right) {
        let precision = if left.precision <= 32 && right.precision <= 32 { 32 } else { 64 };
        let scale = match op {
            B::Multiply => i16::from(left.scale) + i16::from(right.scale),
            B::Divide => 6.max(i16::from(left.scale) + i16::from(right.scale) + 1),
            _ => i16::from(left.scale.max(right.scale)),
        };
        let scale = i8::try_from(scale).map_err(|_| overflow())?;
        let (a, b) = (decimal(left), decimal(right));
        if matches!(op, B::Divide | B::Modulo) && b.is_zero() { return Err(division_by_zero()); }
        let value = match op {
            B::Add => a + b, B::Subtract => a - b, B::Multiply => a * b,
            B::Divide => a / b, B::Modulo => { let remainder = &a % &b; if !remainder.is_zero() && (remainder < BigDecimal::zero()) != (b < BigDecimal::zero()) { remainder + b } else { remainder } },
            _ => return Err(inapplicable("arithmetic", &[])),
        };
        return decimal_value(value, precision, scale, if precision <= 38 { 128 } else { 256 });
    }
    let ty = LogicalType::least_upper_bound(&[left.logical_type(), right.logical_type()]);
    let Some(LogicalType::Scalar(ty)) = ty else { return Err(inapplicable("arithmetic", &[left, right])); };
    if let (Some(a), Some(b)) = (integer(left), integer(right)) {
        if matches!(op, B::Divide | B::Modulo) && b == 0 { return Err(division_by_zero()); }
        let result = match op {
            B::Add => a.checked_add(b), B::Subtract => a.checked_sub(b), B::Multiply => a.checked_mul(b),
            B::Divide => a.checked_div(b), B::Modulo => { let r = a % b; Some(if r != 0 && (r < 0) != (b < 0) { r + b } else { r }) },
            B::BitAnd => Some(a & b), B::BitOr => Some(a | b), B::BitXor => Some(a ^ b),
            _ => None,
        }.ok_or_else(overflow)?;
        return integer_value(result, &ty);
    }
    if let (Some(a), Some(b)) = (number(left), number(right)) {
        if matches!(op, B::Divide | B::Modulo) && b == 0.0 { return Err(division_by_zero()); }
        let value = match op {
            B::Add => a + b, B::Subtract => a - b, B::Multiply => a * b, B::Divide => a / b,
            B::Modulo => { let r = a % b; if r != 0.0 && r.is_sign_negative() != b.is_sign_negative() { r + b } else { r } },
            _ => return Err(inapplicable("arithmetic", &[left, right])),
        };
        return Ok(floating_value(value, &ty));
    }
    Err(inapplicable("arithmetic", &[left, right]))
}
