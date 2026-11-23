use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, StringArray,
    cast::AsArray,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_schema::{DataType, Field, TimeUnit as Unit};

use crate::{LogicalType, ScalarType, TimeUnit, Value};

#[derive(Clone, Debug)]
pub struct ScalarList {
    field: Arc<Field>,
    array: ArrayRef,
    scalar: ScalarType,
}

impl ScalarList {
    pub(crate) fn new(field: Arc<Field>, array: ArrayRef) -> Option<Self> {
        if field.data_type() != array.data_type() || !field.metadata().is_empty() {
            return None;
        }
        let scalar = match field.data_type() {
            DataType::Utf8 => ScalarType::Utf8,
            DataType::Date32 => ScalarType::DateDay,
            DataType::Date64 => ScalarType::DateMilli,
            DataType::Time32(unit @ (Unit::Second | Unit::Millisecond)) => {
                ScalarType::Time { unit: logical_unit(unit), bit_width: 32 }
            }
            DataType::Time64(unit @ (Unit::Microsecond | Unit::Nanosecond)) => {
                ScalarType::Time { unit: logical_unit(unit), bit_width: 64 }
            }
            DataType::Timestamp(unit, zone) => ScalarType::Timestamp {
                unit: logical_unit(unit),
                timezone: zone.as_ref().map(ToString::to_string),
            },
            DataType::Duration(unit) => ScalarType::Duration(logical_unit(unit)),
            _ => return None,
        };
        Some(Self { field, array, scalar })
    }

    pub fn field(&self) -> &Arc<Field> {
        &self.field
    }
    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    pub(crate) fn slice(&self, offset: usize, len: usize) -> Self {
        Self { field: self.field.clone(), array: self.array.slice(offset, len), scalar: self.scalar.clone() }
    }

    pub(crate) fn observed_type(&self) -> LogicalType {
        if self.array.is_empty() {
            LogicalType::Nothing
        } else if self.array.null_count() == self.array.len() {
            LogicalType::Null
        } else {
            let ty = LogicalType::Scalar(self.scalar.clone());
            if self.array.null_count() == 0 { ty } else { ty.maybe() }
        }
    }

    pub(crate) fn value(&self, index: usize) -> Value {
        if self.array.is_null(index) {
            return Value::Null;
        }
        if self.scalar == ScalarType::Utf8 {
            return Value::Utf8(
                self.array.as_any().downcast_ref::<StringArray>().unwrap().value(index).to_owned(),
            );
        }
        let value = Self::temporal_raw(self.array.as_ref(), index).expect("validated temporal storage");
        match &self.scalar {
            ScalarType::DateDay => Value::DateDay(value as i32),
            ScalarType::DateMilli => Value::DateMilli(value),
            ScalarType::Time { unit, bit_width } => Value::Time { value, unit: *unit, bit_width: *bit_width },
            ScalarType::Timestamp { unit, timezone } => {
                Value::Timestamp { value, unit: *unit, timezone: timezone.clone() }
            }
            ScalarType::Duration(unit) => Value::Duration { value, unit: *unit },
            _ => unreachable!("validated scalar storage"),
        }
    }
    pub fn temporal_raw(array: &dyn Array, index: usize) -> Option<i64> {
        macro_rules! raw {
            ($ty:ty) => {
                i64::from(array.as_primitive::<$ty>().value(index))
            };
        }
        Some(match array.data_type() {
            DataType::Date32 => raw!(Date32Type),
            DataType::Date64 => raw!(Date64Type),
            DataType::Time32(Unit::Second) => raw!(Time32SecondType),
            DataType::Time32(Unit::Millisecond) => raw!(Time32MillisecondType),
            DataType::Time64(Unit::Microsecond) => raw!(Time64MicrosecondType),
            DataType::Time64(Unit::Nanosecond) => raw!(Time64NanosecondType),
            DataType::Timestamp(Unit::Second, _) => raw!(TimestampSecondType),
            DataType::Timestamp(Unit::Millisecond, _) => raw!(TimestampMillisecondType),
            DataType::Timestamp(Unit::Microsecond, _) => raw!(TimestampMicrosecondType),
            DataType::Timestamp(Unit::Nanosecond, _) => raw!(TimestampNanosecondType),
            DataType::Duration(Unit::Second) => raw!(DurationSecondType),
            DataType::Duration(Unit::Millisecond) => raw!(DurationMillisecondType),
            DataType::Duration(Unit::Microsecond) => raw!(DurationMicrosecondType),
            DataType::Duration(Unit::Nanosecond) => raw!(DurationNanosecondType),
            _ => return None,
        })
    }
}

fn logical_unit(unit: &Unit) -> TimeUnit {
    match unit {
        Unit::Second => TimeUnit::Second,
        Unit::Millisecond => TimeUnit::Millisecond,
        Unit::Microsecond => TimeUnit::Microsecond,
        Unit::Nanosecond => TimeUnit::Nanosecond,
    }
}
