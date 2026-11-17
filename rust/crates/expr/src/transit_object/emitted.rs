use num_bigint::BigInt;
use rtdb_types::{Result, Value};
use super::{Object, number::Number};

impl Object {
    pub(crate) fn equivalent_emitted(&self, other: &Value) -> Result<bool> {
        Ok(match (self, other) {
            (Self::Null, Value::Null | Value::Undefined) => true,
            (Self::Bool(left), Value::Bool(right)) => left == right,
            (Self::Number(left), right) => emitted_number(right)?.is_some_and(|right| left.equivalent(&right)),
            (Self::ByteBuffer(left), Value::Utf8(right)) => left.as_slice() == right.as_bytes(),
            (Self::ByteBuffer(left), Value::Keyword(right)) => left.as_slice() == right.as_str().as_bytes(),
            (Self::ByteBuffer(left), Value::Binary(right) | Value::FixedSizeBinary(right)) => left.as_slice() == right.as_slice(),
            (Self::ByteBuffer(left), Value::Uuid(right)) => left.as_slice() == right.as_slice(),
            (Self::Uri(left), Value::Uri(right)) => rtdb_types::UriParts::parse(left)?.equivalent(&rtdb_types::UriParts::parse(right)?),
            (Self::Time(super::Time::Interval { months, days, nanos }), right) => crate::temporal::IntervalParts::of(right).is_some_and(|right| *months == right.months && *days == right.days && *nanos == right.nanos),
            _ => false,
        })
    }
}

fn emitted_number(value: &Value) -> Result<Option<Number>> {
    Ok(Some(match value {
        Value::I8(value) => Number::Byte(*value), Value::I16(value) => Number::Short(*value),
        Value::I32(value) | Value::DateDay(value) => Number::Int(*value),
        Value::I64(value) => Number::Long(*value),
        Value::DateMilli(value) => Number::Long(*value / 86_400_000),
        Value::Time { value, .. } | Value::Timestamp { value, .. } | Value::Duration { value, .. } => Number::Long(*value),
        Value::F32(value) => Number::Float(*value), Value::F64(value) => Number::Double(*value),
        Value::Decimal(value) => Number::Decimal { coefficient: BigInt::from_signed_bytes_be(&value.unscaled), scale: value.scale },
        Value::Date(value) => return emitted_number(&value.expression_value()?),
        _ => return Ok(None),
    }))
}
