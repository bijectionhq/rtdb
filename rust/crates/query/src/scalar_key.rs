use std::cmp::Ordering;
use arrow_array::{
    cast::AsArray,
    types::{Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type},
};
use arrow_schema::DataType;
use rtdb_columnar::ArrowCell;
use rtdb_types::{LogicalType, ScalarType, UriParts, Value};

use crate::columnar::Column;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ScalarKey<'a> {
    Null,
    Bool(bool),
    Signed(i64),
    Float(f64),
    Text(&'a str),
    Keyword(&'a str),
    Bytes(&'a [u8]),
    FixedBytes(&'a [u8]),
    Uuid(&'a [u8; 16]),
    Reference(i32),
    Uri(&'a str),
}

impl<'a> ScalarKey<'a> {
    pub fn read(column: &'a Column, row: usize) -> Option<Self> {
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
            return Some(Self::Null);
        };
        if !field.metadata().is_empty() {
            return None;
        }
        match (array.data_type(), crate::columnar::float_vector::nonnull(logical?)) {
            (DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32
            | DataType::Int64 | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary,
            LogicalType::Null | LogicalType::Scalar(ScalarType::Bool | ScalarType::I8 | ScalarType::I16
            | ScalarType::I32 | ScalarType::I64 | ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Binary)) =>
                Self::read_resolved(ArrowCell { array, field, row }, logical?),
            _ => None,
        }
    }

    pub fn read_resolved(cell: ArrowCell<'a>, logical: &LogicalType) -> Option<Self> {
        let ArrowCell { array, field, row } = cell;
        let logical = crate::columnar::float_vector::nonnull(logical);
        let null = array.is_null(row).then_some(Self::Null);
        Some(match (array.data_type(), logical) {
            (DataType::Null, LogicalType::Null) if field.is_nullable() => Self::Null,
            (DataType::Boolean, LogicalType::Scalar(ScalarType::Bool)) => {
                null.unwrap_or_else(|| Self::Bool(array.as_boolean().value(row)))
            }
            (DataType::Int8, LogicalType::Scalar(ScalarType::I8)) => null.unwrap_or_else(|| {
                Self::Signed(i64::from(array.as_primitive::<Int8Type>().value(row)))
            }),
            (DataType::Int16, LogicalType::Scalar(ScalarType::I16)) => null.unwrap_or_else(|| {
                Self::Signed(i64::from(array.as_primitive::<Int16Type>().value(row)))
            }),
            (DataType::Int32, LogicalType::Scalar(ScalarType::I32)) => null.unwrap_or_else(|| {
                Self::Signed(i64::from(array.as_primitive::<Int32Type>().value(row)))
            }),
            (DataType::Int64, LogicalType::Scalar(ScalarType::I64)) => {
                null.unwrap_or_else(|| Self::Signed(array.as_primitive::<Int64Type>().value(row)))
            }
            (DataType::Float32, LogicalType::Scalar(ScalarType::F32)) => {
                null.unwrap_or_else(|| {
                    Self::Float(f64::from(array.as_primitive::<Float32Type>().value(row)))
                })
            }
            (DataType::Float64, LogicalType::Scalar(ScalarType::F64)) => {
                null.unwrap_or_else(|| Self::Float(array.as_primitive::<Float64Type>().value(row)))
            }
            (DataType::UInt8, LogicalType::Scalar(ScalarType::U8)) =>
                null.unwrap_or_else(|| Self::Signed(i64::from(array.as_primitive::<UInt8Type>().value(row) as i8))),
            (DataType::UInt16, LogicalType::Scalar(ScalarType::U16)) =>
                null.unwrap_or_else(|| Self::Signed(i64::from(array.as_primitive::<UInt16Type>().value(row) as i16))),
            (DataType::UInt32, LogicalType::Scalar(ScalarType::U32)) =>
                null.unwrap_or_else(|| Self::Signed(i64::from(array.as_primitive::<UInt32Type>().value(row) as i32))),
            (DataType::UInt64, LogicalType::Scalar(ScalarType::U64)) =>
                null.unwrap_or_else(|| Self::Signed(array.as_primitive::<UInt64Type>().value(row) as i64)),
            (DataType::Int32, LogicalType::Scalar(ScalarType::Oid | ScalarType::RegClass | ScalarType::RegProc)) =>
                null.unwrap_or_else(|| Self::Reference(array.as_primitive::<Int32Type>().value(row))),
            (DataType::Utf8, LogicalType::Scalar(ScalarType::Utf8)) => {
                null.unwrap_or_else(|| Self::Text(array.as_string::<i32>().value(row)))
            }
            (DataType::Utf8, LogicalType::Scalar(ScalarType::Keyword)) =>
                null.unwrap_or_else(|| Self::Keyword(array.as_string::<i32>().value(row))),
            (DataType::Utf8, LogicalType::Scalar(ScalarType::Uri)) => {
                if let Some(null) = null { return Some(null); }
                let value = array.as_string::<i32>().value(row);
                UriParts::parse(value).ok()?;
                Self::Uri(value)
            }
            (DataType::Binary, LogicalType::Scalar(ScalarType::Binary)) => {
                null.unwrap_or_else(|| Self::Bytes(array.as_binary::<i32>().value(row)))
            }
            (DataType::FixedSizeBinary(_), LogicalType::Scalar(ScalarType::FixedSizeBinary(_))) =>
                null.unwrap_or_else(|| Self::FixedBytes(array.as_fixed_size_binary().value(row))),
            (DataType::FixedSizeBinary(16), LogicalType::Scalar(ScalarType::Uuid)) => {
                if let Some(null) = null { return Some(null); }
                Self::Uuid(array.as_fixed_size_binary().value(row).try_into().ok()?)
            }
            _ => return None,
        })
    }

    pub fn from_value(value: &'a Value) -> Option<Self> {
        Some(match value {
            Value::Null | Value::Undefined => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::I8(value) => Self::Signed(i64::from(*value)),
            Value::I16(value) => Self::Signed(i64::from(*value)),
            Value::I32(value) => Self::Signed(i64::from(*value)),
            Value::I64(value) => Self::Signed(*value),
            Value::F32(value) => Self::Float(f64::from(*value)),
            Value::F64(value) => Self::Float(*value),
            Value::Utf8(value) => Self::Text(value),
            Value::Keyword(value) => Self::Keyword(value.as_str()),
            Value::Binary(value) => Self::Bytes(value),
            Value::FixedSizeBinary(value) => Self::FixedBytes(value),
            Value::Uuid(value) => Self::Uuid(value),
            Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => Self::Reference(*value),
            Value::Uri(value) if UriParts::parse(value).is_ok() => Self::Uri(value),
            _ => return None,
        })
    }

    pub fn hash(self) -> u32 {
        self.hash_with(rtdb_columnar::xxhash32)
    }

    pub fn murmur3(self) -> u32 {
        self.hash_with(rtdb_columnar::murmur3_32)
    }

    pub fn equal(self, other: Self) -> Option<bool> {
        match (self, other) {
            (Self::Null, _)
            | (_, Self::Null)
            | (Self::Bool(_), Self::Bool(_))
            | (Self::Signed(_), Self::Signed(_))
            | (Self::Reference(_), Self::Reference(_))
            | (Self::Float(_), Self::Float(_))
            | (Self::Text(_), Self::Text(_))
            | (Self::Bytes(_), Self::Bytes(_)) => Some(self == other),
            (Self::Signed(left), Self::Float(right)) | (Self::Float(right), Self::Signed(left)) => Some(left as f64 == right),
            (Self::Signed(left), Self::Reference(right)) | (Self::Reference(right), Self::Signed(left)) => Some(left == i64::from(right)),
            (Self::Uri(left), Self::Uri(right)) => Some(UriParts::parse(left).ok()?.equivalent(&UriParts::parse(right).ok()?)),
            _ => Some(self.byte_buffer()? == other.byte_buffer()?),
        }
    }

    fn byte_buffer(self) -> Option<&'a [u8]> {
        match self {
            Self::Text(value) | Self::Keyword(value) => Some(value.as_bytes()),
            Self::Bytes(value) | Self::FixedBytes(value) => Some(value),
            Self::Uuid(value) => Some(value),
            _ => None,
        }
    }

    pub fn ordered_compare(self, other: Self) -> Option<Ordering> {
        fn floats(left: f64, right: f64) -> Ordering {
            match (left.is_nan(), right.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                (false, false) => left.total_cmp(&right),
            }
        }
        Some(match (self, other) {
            (Self::Signed(left), Self::Signed(right)) => left.cmp(&right),
            (Self::Signed(left), Self::Float(right)) => floats(left as f64, right),
            (Self::Float(left), Self::Signed(right)) => floats(left, right as f64),
            (Self::Float(left), Self::Float(right)) => floats(left, right),
            (Self::Bool(left), Self::Bool(right)) => left.cmp(&right),
            (Self::Text(left), Self::Text(right)) => left.as_bytes().cmp(right.as_bytes()),
            (Self::Bytes(left), Self::Bytes(right)) => left.cmp(right),
            _ => return None,
        })
    }

    fn hash_with(self, hash: fn(&[u8], u32) -> u32) -> u32 {
        match self {
            Self::Null => 0,
            Self::Bool(value) => {
                if value {
                    17
                } else {
                    19
                }
            }
            Self::Signed(value) => hash(&(value as f64).to_bits().to_be_bytes(), 0),
            Self::Float(value) => hash(&value.to_bits().to_be_bytes(), 0),
            Self::Text(text) | Self::Keyword(text) | Self::Uri(text) => hash(text.as_bytes(), 0),
            Self::Bytes(bytes) | Self::FixedBytes(bytes) => hash(bytes, 0),
            Self::Uuid(bytes) => hash(bytes, 0),
            Self::Reference(value) => hash(&f64::from(value).to_bits().to_be_bytes(), 0),
        }
    }
}
