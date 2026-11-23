use arrow_array::Array;
use arrow_schema::Field;
use rtdb_types::{LogicalType, Result, ScalarType, Value};

/// Returns the opened-vector type, reinterpreting unsigned integer storage at its signed width.
/// Child layouts and nullability are retained; union legs that become equal are joined.
pub fn execution_type(physical: &LogicalType) -> LogicalType {
    use LogicalType as T;
    use ScalarType as S;
    match physical {
        T::Scalar(scalar) => T::Scalar(match scalar { S::U8 => S::I8, S::U16 => S::I16, S::U32 => S::I32, S::U64 => S::I64, scalar => scalar.clone() }),
        T::Maybe(value) => execution_type(value).maybe(),
        T::RunEndEncoded { width, value } => T::RunEndEncoded { width: *width, value: Box::new(execution_type(value)) },
        T::List { kind, element } => T::List { kind: kind.clone(), element: Box::new(execution_type(element)) },
        T::Struct(fields) => T::Struct(fields.iter().map(|(name, ty)| (name.clone(), execution_type(ty))).collect()),
        T::Union(legs) => T::join_all(&legs.iter().map(execution_type).collect::<Vec<_>>()),
        T::Nothing | T::Null => physical.clone(),
    }
}

/// Reads an execution type from a validated native Arrow field without changing its physical metadata.
pub fn execution_field_type(field: &Field) -> Result<LogicalType> { Ok(execution_type(&crate::from_field(field)?)) }

/// Admits a domain value to execution in place, preserving allocations and integer bits.
/// Apply at external-vector or parameter boundaries; execution-owned unsigned identities are not domain values.
/// Calendar objects are first narrowed by the vector object writer's storage rules.
pub fn admit_execution_value(value: &mut Value) -> Result<()> {
    match value {
        Value::Date(date) => *value = date.object_value()?,
        Value::U8(raw) => *value = Value::I8(*raw as i8),
        Value::U16(raw) => *value = Value::I16(*raw as i16),
        Value::U32(raw) => *value = Value::I32(*raw as i32),
        Value::U64(raw) => *value = Value::I64(*raw as i64),
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => values.iter_mut().try_for_each(admit_execution_value)?,
        Value::Struct(fields) => fields.values_mut().try_for_each(admit_execution_value)?,
        Value::Map { entries, .. } => for (key, value) in entries { admit_execution_value(key)?; admit_execution_value(value)?; },
        _ => (),
    }
    Ok(())
}

/// Decodes one domain cell with opened-vector semantics; raw `read_value` remains lossless.
/// The caller reserves the same decoding bound required by `read_value` before calling.
/// `read_execution_value` for a caller that has derived `logical` from `field` once, for many cells of the column.
pub fn read_execution_value_as(logical: &LogicalType, field: &Field, array: &dyn Array, index: usize) -> Result<Value> {
    let mut value = crate::read_value_as(logical, field, array, index)?;
    admit_execution_value(&mut value)?;
    Ok(value)
}

pub fn read_execution_value(field: &Field, array: &dyn Array, index: usize) -> Result<Value> {
    let mut value = crate::read_value(field, array, index)?;
    admit_execution_value(&mut value)?;
    Ok(value)
}
