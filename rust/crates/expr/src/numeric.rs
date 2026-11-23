use bigdecimal::BigDecimal;
use num_bigint::BigInt;
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
        Value::Decimal(value) => Some(crate::decimal_to_f64(value)),
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

pub(crate) fn decimal_value(value: BigDecimal, precision: i32, scale: i32, bit_width: u16) -> Result<Value> {
    crate::decimal::from_bigdecimal(value, precision, scale, bit_width).map(Value::Decimal)
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

pub fn signed_modulo(left: i64, right: i64) -> Result<i64> {
    if right == 0 { return Err(division_by_zero()); }
    let remainder = left.wrapping_rem(right);
    Ok(if remainder != 0 && (remainder < 0) != (right < 0) { remainder + right } else { remainder })
}

pub fn float_modulo(left: f64, right: f64) -> Result<f64> {
    if right == 0.0 {
        return Err(Anomaly::incorrect("xtdb.expression/unknown-arithmetic-error", "Divide by zero"));
    }
    let quotient = left / right;
    if !quotient.is_finite() { return Err(number_format("Infinite or NaN".into())); }
    // The reference truncates through a Java long in this range, including the saturated positive endpoint.
    let quotient = if quotient.abs() <= -(i64::MIN as f64) { (quotient as i64) as f64 } else { quotient };
    let remainder = left - quotient * right;
    Ok(if remainder == 0.0 || (left > 0.0) == (right > 0.0) { remainder } else { remainder + right })
}
pub(crate) fn inapplicable(name: &str, values: &[&Value]) -> Anomaly {
    rtdb_types::function_type_mismatch(name, &values.iter().map(|value| value.logical_type()).collect::<Vec<_>>())
}
/// `Long/parseLong` and friends' `NumberFormatException`, which the reference's projection maps to this code.
pub(crate) fn number_format(message: String) -> Anomaly { Anomaly::incorrect("xtdb.expression/number-format-error", message) }
fn arithmetic_name(op: BinaryOp) -> &'static str {
    match op { BinaryOp::Add => "+", BinaryOp::Subtract => "-", BinaryOp::Multiply => "*", BinaryOp::Divide => "/", BinaryOp::Modulo => "mod", _ => "arithmetic" }
}

/// Computes a scalar intermediate using UTC; calendar results await explicit vector admission.
pub fn arithmetic(op: BinaryOp, left: &Value, right: &Value) -> Result<Value> {
    arithmetic_in_timezone(op, left, right, "UTC")
}

/// Temporal operations interpret mixed local/zoned operands using the supplied session time zone.
/// Calendar results remain scalar intermediates until the caller admits their output.
pub fn arithmetic_in_timezone(op: BinaryOp, left: &Value, right: &Value, timezone: &str) -> Result<Value> {
    use BinaryOp as B;
    if matches!(left, Value::Null | Value::Undefined) || matches!(right, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    if let Some(result) = crate::bitwise::binary(op, left, right) { return result; }
    if op == B::Multiply { if let Some(result) = crate::period::intersection(left, right) { return Ok(result); } }
    if let Some(result) = crate::temporal::arithmetic(op, left, right, timezone) { return result; }
    if op == B::Modulo && (matches!(left, Value::Decimal(_)) || matches!(right, Value::Decimal(_))) {
        if let (Some(a), Some(b)) = (exact_decimal(left), exact_decimal(right)) {
            let Some(LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width })) = LogicalType::least_upper_bound(&[left.logical_type(), right.logical_type()]) else { return Err(overflow()); };
            let a = if let Value::Decimal(value) = left { value.clone() } else { crate::decimal::from_bigdecimal(a, 32, 0, 128)? };
            let b = if let Value::Decimal(value) = right { value.clone() } else { crate::decimal::from_bigdecimal(b, 32, 0, 128)? };
            return crate::decimal::arithmetic(op, &a, &b, precision, scale, bit_width).map(Value::Decimal);
        }
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
        let scale = rtdb_plan::decimal_arithmetic_scale(op, left.scale, right.scale)?;
        return crate::decimal::arithmetic(op, left, right, precision, scale, if precision <= 32 { 128 } else { 256 }).map(Value::Decimal);
    }
    let ty = LogicalType::least_upper_bound(&[left.logical_type(), right.logical_type()]);
    let Some(LogicalType::Scalar(ty)) = ty else { return Err(inapplicable(arithmetic_name(op), &[left, right])); };
    if let (Some(a), Some(b)) = (integer(left), integer(right)) {
        if op == B::Modulo && let (Ok(a), Ok(b)) = (i64::try_from(a), i64::try_from(b)) {
            return integer_value(i128::from(signed_modulo(a, b)?), &ty);
        }
        if matches!(op, B::Divide | B::Modulo) && b == 0 { return Err(division_by_zero()); }
        let result = match op {
            B::Add => a.checked_add(b), B::Subtract => a.checked_sub(b), B::Multiply => a.checked_mul(b),
            B::Divide => a.checked_div(b), B::Modulo => { let r = a % b; Some(if r != 0 && (r < 0) != (b < 0) { r + b } else { r }) },
            _ => None,
        }.ok_or_else(overflow)?;
        return integer_value(result, &ty);
    }
    if let (Some(a), Some(b)) = (number(left), number(right)) {
        if op == B::Divide && b == 0.0 { return Err(division_by_zero()); }
        let value = match op {
            B::Add => a + b, B::Subtract => a - b, B::Multiply => a * b, B::Divide => a / b,
            B::Modulo => float_modulo(a, b)?,
            _ => return Err(inapplicable(arithmetic_name(op), &[left, right])),
        };
        return Ok(floating_value(value, &ty));
    }
    Err(inapplicable(arithmetic_name(op), &[left, right]))
}
