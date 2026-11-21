use rtdb_types::{Interval, LogicalType, ScalarType, Timestamp, Value};
use serde::{Deserialize, Serialize};

use super::domain::{bucket_start, offset, upper_bound};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Atom {
    Bool(bool),
    Integer(i128),
    Text(String),
    Binary(Vec<u8>),
    Uuid([u8; 16]),
    Day(i64),
    Unordered,
    Null,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Family {
    Bool,
    Integer,
    Text,
    Binary,
    Uuid,
    Day,
}

impl Atom {
    pub(super) fn of(value: &Value) -> Option<Self> {
        Some(match value {
            Value::Null | Value::Undefined => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::I8(value) => Self::Integer((*value).into()),
            Value::I16(value) => Self::Integer((*value).into()),
            Value::I32(value) => Self::Integer((*value).into()),
            Value::I64(value) => Self::Integer((*value).into()),
            Value::U8(value) => Self::Integer((*value).into()),
            Value::U16(value) => Self::Integer((*value).into()),
            Value::U32(value) => Self::Integer((*value).into()),
            Value::U64(value) => Self::Integer((*value).into()),
            Value::Utf8(value) => Self::Text(value.clone()),
            Value::Binary(value) => Self::Binary(value.clone()),
            Value::Uuid(value) => Self::Uuid(*value),
            Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => {
                Self::Day(value.date_days()?)
            }
            _ => return None,
        })
    }

    pub(super) fn family(&self) -> Option<Family> {
        Some(match self {
            Self::Null | Self::Unordered => return None,
            Self::Bool(_) => Family::Bool,
            Self::Integer(_) => Family::Integer,
            Self::Text(_) => Family::Text,
            Self::Binary(_) => Family::Binary,
            Self::Uuid(_) => Family::Uuid,
            Self::Day(_) => Family::Day,
        })
    }

    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Text(value) => value.len(),
            Self::Binary(value) => value.len(),
            _ => 0,
        }
    }
}

pub(super) fn family(logical: &LogicalType) -> Option<Family> {
    use ScalarType as S;
    let mut family = None;
    for leg in logical.legs() {
        let next = match leg {
            LogicalType::Null => continue,
            LogicalType::Scalar(S::Bool) => Family::Bool,
            LogicalType::Scalar(
                S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64,
            ) => Family::Integer,
            LogicalType::Scalar(S::Utf8) => Family::Text,
            LogicalType::Scalar(S::Binary) => Family::Binary,
            LogicalType::Scalar(S::Uuid) => Family::Uuid,
            LogicalType::Scalar(S::DateDay | S::DateMilli) => Family::Day,
            _ => return None,
        };
        if family.is_some_and(|prior| prior != next) {
            return None;
        }
        family = Some(next);
    }
    family
}

/// An aligned half-open range in the unsigned valid-time domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct Bucket {
    level: u32,
    start: u64,
}

impl Bucket {
    pub fn at(time: Timestamp) -> impl Iterator<Item = Self> {
        let point = offset(time.0);
        (0..=64).map(move |level| Self {
            level,
            start: bucket_start(point, level),
        })
    }

    pub fn cover(interval: Interval) -> impl Iterator<Item = Self> {
        let mut start = offset(interval.lower().0);
        let end = upper_bound(interval.upper().0);
        let empty = interval.is_empty();
        std::iter::from_fn(move || {
            if empty || start >= end {
                return None;
            }
            let level = start.trailing_zeros().min((end - start).ilog2());
            let bucket = Self {
                level,
                start: start as u64,
            };
            start += 1_u128 << level;
            Some(bucket)
        })
    }
}
