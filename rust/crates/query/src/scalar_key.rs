use std::cmp::Ordering;
use arrow_array::{
    cast::AsArray,
    types::{Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type},
};
use arrow_schema::DataType;
use rtdb_types::{LogicalType, ScalarType};

use crate::columnar::Column;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum ScalarKey<'a> {
    Null,
    Bool(bool),
    Signed(i64),
    Float(f64),
    Text(&'a str),
    Bytes(&'a [u8]),
}

impl<'a> ScalarKey<'a> {
    pub fn read(column: &'a Column, row: usize) -> Option<Self> {
        let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
            return Some(Self::Null);
        };
        if !field.metadata().is_empty() {
            return None;
        }
        let logical = match logical? {
            LogicalType::Maybe(inner) => inner.as_ref(),
            ty => ty,
        };
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
            (DataType::Utf8, LogicalType::Scalar(ScalarType::Utf8)) => {
                null.unwrap_or_else(|| Self::Text(array.as_string::<i32>().value(row)))
            }
            (DataType::Binary, LogicalType::Scalar(ScalarType::Binary)) => {
                null.unwrap_or_else(|| Self::Bytes(array.as_binary::<i32>().value(row)))
            }
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
            | (Self::Float(_), Self::Float(_))
            | (Self::Text(_), Self::Text(_))
            | (Self::Bytes(_), Self::Bytes(_)) => Some(self == other),
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
            Self::Text(text) => hash(text.as_bytes(), 0),
            Self::Bytes(bytes) => hash(bytes, 0),
        }
    }
}
