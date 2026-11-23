use num_bigint::BigInt;
use rtdb_types::{Result, Value};
use crate::compare::emitted_byte_buffer;
use super::{Object, number::Number};

impl Object {
    pub(crate) fn equivalent_emitted(&self, other: &Value,allocation: &crate::EvaluationScope) -> Result<bool> {
        Ok(match (self, other) {
            (Self::Null, Value::Null | Value::Undefined) => true,
            (Self::Bool(left), Value::Bool(right)) => left == right,
            (Self::Number(left), right) => emitted_number(right,allocation)?.is_some_and(|right| left.equivalent(&right)),
            (Self::ByteBuffer(left), right) => emitted_byte_buffer(right).is_some_and(|right| left.as_slice() == right),
            (Self::Reg(left), Value::Oid(right)) => left.kind == super::RegKind::Oid && left.oid == *right,
            (Self::Reg(left), Value::RegClass(right)) => left.kind == super::RegKind::RegClass && left.oid == *right,
            (Self::Reg(left), Value::RegProc(right)) => left.kind == super::RegKind::RegProc && left.oid == *right,
            (Self::Uri(left), Value::Uri(right)) => rtdb_types::UriParts::parse(left)?.equivalent(&rtdb_types::UriParts::parse(right)?),
            (Self::Time(super::Time::Interval { months, days, nanos }), right) => crate::temporal::IntervalParts::of(right).is_some_and(|right| *months == right.months && *days == right.days && *nanos == right.nanos),
            _ => false,
        })
    }
}

fn emitted_number(value: &Value,allocation: &crate::EvaluationScope) -> Result<Option<Number>> {
    Ok(Some(match value {
        Value::I8(value) => Number::Byte(*value), Value::I16(value) => Number::Short(*value),
        Value::I32(value) | Value::DateDay(value) => Number::Int(*value),
        Value::I64(value) => Number::Long(*value),
        Value::DateMilli(value) => Number::Long(value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY)),
        Value::Time { value, .. } | Value::Timestamp { value, .. } | Value::Duration { value, .. } => Number::Long(*value),
        Value::F32(value) => Number::Float(*value), Value::F64(value) => Number::Double(*value),
        Value::Decimal(value) => {
            allocation.reserve(value.unscaled.len().checked_mul(32).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(crate::numeric::overflow)?)?;
            Number::Decimal { coefficient: BigInt::from_signed_bytes_be(&value.unscaled), scale: value.scale }
        },
        Value::Date(value) => return emitted_number(&value.expression_value()?,allocation),
        _ => return Ok(None),
    }))
}
