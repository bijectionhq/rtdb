use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef,
    cast::AsArray,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_schema::{DataType, Field, TimeUnit as Unit, UnionMode};

use crate::{Keyword, LogicalType, NumericSpan, ScalarType, TimeUnit, Value, numeric_list};

#[derive(Clone, Debug)]
pub struct ScalarList {
    field: Arc<Field>,
    array: ArrayRef,
    scalar: Option<ScalarType>,
}

impl ScalarList {
    pub(crate) fn new(field: Arc<Field>, array: ArrayRef) -> Option<Self> {
        if field.data_type() != array.data_type() {
            return None;
        }
        let scalar = Self::supported_type(&field);
        if scalar.is_none() && !matches!(field.data_type(), DataType::Union(fields, UnionMode::Dense)
            if !field.metadata().contains_key("ARROW:extension:name")
                && fields.iter().all(|(_, leg)| Self::leaf_type(leg).is_some()))
        {
            return None;
        }
        Some(Self { field, array, scalar })
    }

    pub fn supported_type(field: &Field) -> Option<ScalarType> {
        crate::decimal_arrow::decimal_field_parameters(field).ok()?;
        let extension = field.metadata().get("ARROW:extension:name").map(String::as_str);
        Some(match (field.data_type(), extension) {
            (DataType::Boolean, None) => ScalarType::Bool,
            (DataType::Utf8, None) => ScalarType::Utf8,
            (DataType::Utf8, Some("xt/clj-keyword")) => ScalarType::Keyword,
            (DataType::Binary, None) => ScalarType::Binary,
            (DataType::Binary, Some("xt/transit+msgpack")) => ScalarType::Transit,
            (DataType::FixedSizeBinary(width), None) => ScalarType::FixedSizeBinary(*width),
            (DataType::FixedSizeBinary(16), Some("uuid")) => ScalarType::Uuid,
            (DataType::Int32, Some("xt/oid")) => ScalarType::Oid,
            (DataType::Int32, Some("xt/regclass")) => ScalarType::RegClass,
            (DataType::Int32, Some("xt/regproc")) => ScalarType::RegProc,
            (_, Some(_)) => return None,
            (ty, None) => match ty {
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
            },
        })
    }

    pub fn leaf_type(field: &Field) -> Option<LogicalType> {
        if let Some(scalar) = Self::supported_type(field) {
            return Some(LogicalType::Scalar(scalar));
        }
        (!matches!(field.data_type(), DataType::Union(..) | DataType::RunEndEncoded(..))
            && numeric_list::numeric_input(field))
        .then(|| numeric_list::scalar_type(field))
    }

    pub(crate) fn admit_signed(&mut self) {
        if let Some((field, array)) = numeric_list::signed_layout(&self.field, &self.array) {
            self.field = Arc::new(field);
            self.array = array;
        }
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
        if let DataType::Union(fields, UnionMode::Dense) = self.field.data_type() {
            return numeric_list::observed_union_type(fields, self.array.as_union(), |field| {
                Self::leaf_type(field).expect("validated mixed list leaf")
            });
        }
        if self.array.is_empty() {
            LogicalType::Nothing
        } else if self.array.null_count() == self.array.len() {
            LogicalType::Null
        } else {
            let ty = LogicalType::Scalar(self.scalar.clone().expect("plain scalar list"));
            if self.array.null_count() == 0 { ty } else { ty.maybe() }
        }
    }

    fn active_cell(&self, index: usize) -> (&Field, &dyn Array, usize) {
        let array = self.array.as_ref();
        let DataType::Union(fields, _) = self.field.data_type() else {
            return (&self.field, array, index);
        };
        let union = array.as_union();
        let tag = union.type_id(index);
        let offset = union.value_offset(index);
        let (_, field) = fields.iter().find(|(id, _)| *id == tag).expect("validated union tag");
        (field.as_ref(), union.child(tag).as_ref(), offset)
    }

    pub(crate) fn utf8(&self, index: usize) -> Option<&str> {
        let (field, array, row) = self.active_cell(index);
        (field.data_type() == &DataType::Utf8 && !field.metadata().contains_key("ARROW:extension:name")
            && array.is_valid(row)).then(|| array.as_string::<i32>().value(row))
    }

    pub(crate) fn value(&self, index: usize) -> Value {
        if let Some(scalar) = &self.scalar {
            return Self::scalar_value(scalar, self.array.as_ref(), index);
        }
        let (field, array, row) = self.active_cell(index);
        if let Some(scalar) = Self::supported_type(field) {
            Self::scalar_value(&scalar, array, row)
        } else {
            NumericSpan::new(field, array, row..row + 1).expect("validated mixed list leaf")
                .get(0).unwrap().into_value()
        }
    }

    fn scalar_value(scalar: &ScalarType, array: &dyn Array, index: usize) -> Value {
        if array.is_null(index) { return Value::Null; }
        let temporal = || Self::temporal_raw(array, index).expect("validated temporal storage");
        match scalar {
            ScalarType::Bool => Value::Bool(array.as_boolean().value(index)),
            ScalarType::Utf8 => Value::Utf8(array.as_string::<i32>().value(index).to_owned()),
            ScalarType::Keyword => Value::Keyword(Keyword::from_name(array.as_string::<i32>().value(index))),
            ScalarType::Binary => Value::Binary(array.as_binary::<i32>().value(index).to_vec()),
            ScalarType::Transit => Value::Transit(array.as_binary::<i32>().value(index).to_vec()),
            ScalarType::FixedSizeBinary(_) => Value::FixedSizeBinary(array.as_fixed_size_binary().value(index).to_vec()),
            ScalarType::Uuid => Value::Uuid(array.as_fixed_size_binary().value(index).try_into().expect("uuid width")),
            ScalarType::Oid => Value::Oid(array.as_primitive::<arrow_array::types::Int32Type>().value(index)),
            ScalarType::RegClass => Value::RegClass(array.as_primitive::<arrow_array::types::Int32Type>().value(index)),
            ScalarType::RegProc => Value::RegProc(array.as_primitive::<arrow_array::types::Int32Type>().value(index)),
            ScalarType::DateDay => Value::DateDay(temporal() as i32),
            ScalarType::DateMilli => Value::DateMilli(temporal()),
            ScalarType::Time { unit, bit_width } => Value::Time { value: temporal(), unit: *unit, bit_width: *bit_width },
            ScalarType::Timestamp { unit, timezone } => {
                Value::Timestamp { value: temporal(), unit: *unit, timezone: timezone.clone() }
            }
            ScalarType::Duration(unit) => Value::Duration { value: temporal(), unit: *unit },
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
