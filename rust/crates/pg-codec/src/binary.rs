use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};
use rtdb_types::{Anomaly, DecimalValue, PgType, Result, TimeUnit, Value};
use crate::invalid;

pub(crate) const PG_EPOCH_MICROS: i64 = 946_684_800_000_000;
pub(crate) const PG_EPOCH_DAYS: i32 = 10_957;

pub(crate) fn read(ty: PgType, bytes: &[u8]) -> Result<Value> {
    let mut input = Input(bytes);
    let value = match ty {
        PgType::Default | PgType::Null | PgType::Text | PgType::VarChar | PgType::Keyword => Value::Utf8(std::str::from_utf8(bytes).map_err(invalid)?.to_owned()),
        PgType::Int2 => Value::I16(input.i16()?), PgType::Int4 => Value::I32(input.i32()?), PgType::Int8 => Value::I64(input.i64()?),
        PgType::Float4 => Value::F32(f32::from_bits(input.i32()? as u32)), PgType::Float8 => Value::F64(f64::from_bits(input.i64()? as u64)),
        PgType::Bool => Value::Bool(match input.take(1)?[0] { 0 => false, 1 => true, _ => return Err(invalid("Invalid binary boolean value")) }),
        PgType::Numeric => numeric_read(&mut input)?,
        PgType::Timestamp | PgType::TimestampTz => Value::Timestamp { value: input.i64()?.wrapping_add(PG_EPOCH_MICROS), unit: TimeUnit::Microsecond, timezone: (ty == PgType::TimestampTz).then(|| "UTC".into()) },
        PgType::Date => Value::DateDay(input.i32()?.wrapping_add(PG_EPOCH_DAYS)),
        PgType::Time => { let micros = input.i64()?; if !(0..86_400_000_000).contains(&micros) { return Err(invalid("Time is outside a day")); } Value::Time { value: micros * 1000, unit: TimeUnit::Nanosecond, bit_width: 64 } },
        PgType::Interval => { let nanoseconds = input.i64()?.checked_mul(1000).ok_or_else(|| invalid("Interval overflow"))?; let days = input.i32()?; let months = input.i32()?; if nanoseconds == 0 && days == 0 { Value::IntervalYearMonth(months) } else { Value::IntervalMonthDayMicro { months, days, nanoseconds } } },
        PgType::Bytes => Value::Binary(bytes.to_vec()),
        PgType::Uuid => Value::Uuid(input.take(16)?.try_into().map_err(invalid)?),
        PgType::Int4s | PgType::Int8s | PgType::Texts => array_read(ty, &mut input)?,
        PgType::Jsonb => { if input.take(1)?[0] != 1 { return Err(Anomaly::incorrect("xtdb.pgwire/unsupported-jsonb-version", "Unsupported binary jsonb version")); } return crate::text::read_text(114, std::str::from_utf8(input.0).map_err(invalid)?); },
        PgType::Json | PgType::JsonLd | PgType::Transit | PgType::TsTzRange => return crate::text::read_text(ty.descriptor().oid as u32, std::str::from_utf8(bytes).map_err(invalid)?),
        PgType::Duration | PgType::Oid | PgType::RegClass | PgType::RegProc => return Err(Anomaly::unsupported("xtdb.pgwire/unsupported-binary-type", "This PostgreSQL type has no binary reader")),
    };
    if !matches!(ty, PgType::Default | PgType::Null | PgType::Text | PgType::VarChar | PgType::Keyword | PgType::Bytes) && !input.0.is_empty() { return Err(invalid("Trailing bytes in PostgreSQL binary value")); }
    Ok(value)
}

fn array_read(ty: PgType, input: &mut Input<'_>) -> Result<Value> {
    let dimensions = input.i32()?;
    let flags = input.i32()?;
    let oid = input.i32()?;
    if dimensions == 0 { return Ok(Value::List(Vec::new())); }
    if dimensions != 1 || flags != 0 { return Err(invalid("Binary array requires one dimension and non-null elements")); }
    let count = usize::try_from(input.i32()?).map_err(invalid)?;
    let _lower = input.i32()?;
    if count > input.0.len() / 4 { return Err(invalid("Binary array count exceeds remaining bytes")); }
    let element = match (ty, oid) { (PgType::Texts, 25) => PgType::Text, (PgType::Int4s | PgType::Int8s, 23) => PgType::Int4, (PgType::Int4s | PgType::Int8s, 20) => PgType::Int8, _ => return Err(invalid("Unsupported binary array element OID")) };
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = usize::try_from(input.i32()?).map_err(|_| invalid("Binary array reader does not support null elements"))?;
        let value = read(element, input.take(length)?)?;
        values.push(match (ty, value) { (PgType::Int4s, Value::I64(value)) => Value::I32(value as i32), (PgType::Int8s, Value::I32(value)) => Value::I64(i64::from(value)), (_, value) => value });
    }
    Ok(Value::List(values))
}

fn numeric_read(input: &mut Input<'_>) -> Result<Value> {
    let count = usize::try_from(input.i16()?).map_err(invalid)?;
    let weight = i32::from(input.i16()?);
    let sign = input.i16()?;
    let scale = input.i16()?;
    if count > input.0.len() / 2 || count > 1024 || !matches!(sign, 0 | 0x4000) || !(0..=127).contains(&scale) { return Err(invalid("Unsupported PostgreSQL numeric header")); }
    let mut coefficient = BigInt::zero();
    for _ in 0..count { let digit = input.i16()?; if !(0..10000).contains(&digit) { return Err(invalid("Invalid PostgreSQL numeric digit")); } coefficient = coefficient * 10000 + digit; }
    if count == 0 { return crate::text::decimal("0"); }
    let exponent = (weight - i32::try_from(count).map_err(invalid)? + 1).checked_mul(4).and_then(|n| n.checked_add(i32::from(scale))).ok_or_else(|| invalid("Numeric exponent overflow"))?;
    if exponent.unsigned_abs() > 4096 { return Err(invalid("Numeric exponent exceeds Arrow decimal range")); }
    let factor = BigInt::from(10).pow(exponent.unsigned_abs());
    coefficient = if exponent >= 0 { coefficient * factor } else { coefficient / factor };
    if sign != 0 { coefficient = -coefficient; }
    crate::text::decimal(&BigDecimal::new(coefficient, i64::from(scale)).to_string())
}

pub(crate) fn numeric_write(value: &DecimalValue) -> Result<Vec<u8>> {
    let signed = BigInt::from_signed_bytes_be(&value.unscaled);
    let negative = signed <= BigInt::zero();
    let mut coefficient = signed.abs();
    let scale = i32::from(value.scale);
    let fraction_groups = if scale > 0 { (scale + 3) / 4 } else { 0 };
    let mut digits = Vec::<u16>::new();
    if scale > 0 && scale % 4 != 0 {
        let factor = BigInt::from(10).pow((scale % 4) as u32);
        digits.push(((&coefficient % &factor) * BigInt::from(10).pow((4 - scale % 4) as u32)).to_u16().ok_or_else(|| invalid("Numeric digit overflow"))?);
        coefficient /= factor;
    } else if scale < 0 { coefficient *= BigInt::from(10).pow(scale.unsigned_abs()); }
    while !coefficient.is_zero() { digits.push((&coefficient % 10000u32).to_u16().ok_or_else(|| invalid("Numeric digit overflow"))?); coefficient /= 10000u32; }
    let mut bytes = Vec::with_capacity(8 + digits.len() * 2);
    bytes.extend_from_slice(&i16::try_from(digits.len()).map_err(invalid)?.to_be_bytes());
    bytes.extend_from_slice(&i16::try_from(digits.len() as i32 - fraction_groups - 1).map_err(invalid)?.to_be_bytes());
    bytes.extend_from_slice(&(if negative { 0x4000i16 } else { 0 }).to_be_bytes());
    bytes.extend_from_slice(&(scale.max(0) as i16).to_be_bytes());
    for digit in digits.into_iter().rev() { bytes.extend_from_slice(&digit.to_be_bytes()); }
    Ok(bytes)
}

struct Input<'a>(&'a [u8]);
impl<'a> Input<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> { let (head, tail) = self.0.split_at_checked(length).ok_or_else(|| invalid("Truncated PostgreSQL binary value"))?; self.0 = tail; Ok(head) }
    fn i16(&mut self) -> Result<i16> { Ok(i16::from_be_bytes(self.take(2)?.try_into().map_err(invalid)?)) }
    fn i32(&mut self) -> Result<i32> { Ok(i32::from_be_bytes(self.take(4)?.try_into().map_err(invalid)?)) }
    fn i64(&mut self) -> Result<i64> { Ok(i64::from_be_bytes(self.take(8)?.try_into().map_err(invalid)?)) }
}
