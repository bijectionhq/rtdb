use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, RunArray, StringArray,
    TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, UnionArray,
    types::{Int16Type, Int32Type, Int64Type},
};
use arrow_schema::{DataType, Field, TimeUnit as ArrowTimeUnit, UnionMode};
use rtdb_types::{Interval, LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
use serde::{Deserialize, Serialize};

use super::{
    domain::{bucket_start, cover_step, offset, upper_bound},
    format_error,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Scalar<Text, Binary> {
    Bool(bool),
    Integer(i128),
    Text(Text),
    Binary(Binary),
    Uuid([u8; 16]),
    Day(i64),
    InstantMicros(i64),
    FloatOrder(u64),
    Decimal128(u8, i8, i128),
    Unordered,
    Null,
}

pub(super) type Atom = Scalar<String, Vec<u8>>;
pub(super) type AtomRef<'a> = Scalar<&'a str, &'a [u8]>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Family {
    Bool,
    Integer,
    Text,
    Binary,
    Uuid,
    Day,
    InstantMicros,
    Float,
    Decimal128(u8, i8),
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
            Value::U8(value) => Self::Integer(i128::from(*value as i8)),
            Value::U16(value) => Self::Integer(i128::from(*value as i16)),
            Value::U32(value) => Self::Integer(i128::from(*value as i32)),
            Value::U64(value) => Self::Integer(i128::from(*value as i64)),
            // SQL equality groups signed zeros and refuses to equate NaNs.
            // A single ordered key cannot witness those equality bounds.
            Value::F32(value) if *value != 0.0 && !value.is_nan() => {
                Self::FloatOrder(float_order(f64::from(*value)))
            }
            Value::F64(value) if *value != 0.0 && !value.is_nan() => {
                Self::FloatOrder(float_order(*value))
            }
            Value::Utf8(value) => Self::Text(value.clone()),
            Value::Binary(value) => Self::Binary(value.clone()),
            Value::Uuid(value) => Self::Uuid(*value),
            Value::Decimal(value) if value.bit_width == 128 => {
                let precision = value.precision.try_into().ok()?;
                let scale = value.scale.try_into().ok()?;
                let coefficient = i128::from_be_bytes(value.unscaled.as_slice().try_into().ok()?);
                if !decimal_coefficient_fits(precision, coefficient) { return None; }
                Self::Decimal128(precision, scale, coefficient)
            },
            Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => {
                Self::Day(value.date_days()?)
            }
            Value::Timestamp {
                value,
                unit: TimeUnit::Microsecond,
                timezone: Some(_),
            } => Self::InstantMicros(*value),
            _ => return None,
        })
    }

    pub(super) fn borrowed(&self) -> AtomRef<'_> {
        match self {
            Self::Bool(value) => Scalar::Bool(*value),
            Self::Integer(value) => Scalar::Integer(*value),
            Self::Text(value) => Scalar::Text(value),
            Self::Binary(value) => Scalar::Binary(value),
            Self::Uuid(value) => Scalar::Uuid(*value),
            Self::Day(value) => Scalar::Day(*value),
            Self::InstantMicros(value) => Scalar::InstantMicros(*value),
            Self::FloatOrder(value) => Scalar::FloatOrder(*value),
            Self::Decimal128(precision, scale, value) => Scalar::Decimal128(*precision, *scale, *value),
            Self::Unordered => Scalar::Unordered,
            Self::Null => Scalar::Null,
        }
    }
}

impl<'a> AtomRef<'a> {
    pub(super) fn is_bound(self) -> bool {
        !matches!(self, Self::FloatOrder(bits)
            if bits == FLOAT_NAN_ORDER || bits == float_order(0.0) || bits == float_order(-0.0))
    }

    pub(super) fn owned(self) -> Atom {
        match self {
            Self::Bool(value) => Scalar::Bool(value),
            Self::Integer(value) => Scalar::Integer(value),
            Self::Text(value) => Scalar::Text(value.to_owned()),
            Self::Binary(value) => Scalar::Binary(value.to_vec()),
            Self::Uuid(value) => Scalar::Uuid(value),
            Self::Day(value) => Scalar::Day(value),
            Self::InstantMicros(value) => Scalar::InstantMicros(value),
            Self::FloatOrder(value) => Scalar::FloatOrder(value),
            Self::Decimal128(precision, scale, value) => Scalar::Decimal128(precision, scale, value),
            Self::Unordered => Scalar::Unordered,
            Self::Null => Scalar::Null,
        }
    }

    pub(super) fn from_arrow(field: &Field, array: &'a dyn Array, row: usize) -> Result<Self> {
        if field.data_type() != array.data_type() || row >= array.len() {
            return Err(format_error(
                "Indexed column disagrees with its Arrow layout",
            ));
        }
        if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
            let union = arrow::<UnionArray>(array)?;
            let id = union.type_id(row);
            let child = fields
                .iter()
                .find_map(|(tag, field)| (tag == id).then_some(field))
                .ok_or_else(|| format_error("Indexed union has an unknown leg"))?;
            return Self::from_arrow(child, union.child(id).as_ref(), union.value_offset(row));
        }
        if let DataType::RunEndEncoded(ends, values) = field.data_type() {
            macro_rules! run_value {
                ($ty:ty) => {{
                    let runs = arrow::<RunArray<$ty>>(array)?;
                    Self::from_arrow(values, runs.values().as_ref(), runs.get_physical_index(row))
                }};
            }
            return match ends.data_type() {
                DataType::Int16 => run_value!(Int16Type),
                DataType::Int32 => run_value!(Int32Type),
                DataType::Int64 => run_value!(Int64Type),
                _ => Err(format_error("Indexed column has an invalid run-end width")),
            };
        }
        if array.is_null(row) || field.data_type() == &DataType::Null {
            return Ok(Self::Null);
        }
        macro_rules! value {
            ($ty:ty) => {
                arrow::<$ty>(array)?.value(row)
            };
        }
        // EventBatch has already validated the field's logical layout.
        if let Some(extension) = field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY) {
            return Ok(match extension.as_str() {
                "uuid" => Self::Uuid(
                    value!(FixedSizeBinaryArray)
                        .try_into()
                        .map_err(format_error)?,
                ),
                _ => Self::Unordered,
            });
        }
        Ok(match field.data_type() {
            DataType::Boolean => Self::Bool(value!(BooleanArray)),
            DataType::Int8 => Self::Integer(value!(Int8Array).into()),
            DataType::Int16 => Self::Integer(value!(Int16Array).into()),
            DataType::Int32 => Self::Integer(value!(Int32Array).into()),
            DataType::Int64 => Self::Integer(value!(Int64Array).into()),
            DataType::Decimal128(precision, scale) => {
                let coefficient = value!(Decimal128Array);
                if !decimal_coefficient_fits(*precision, coefficient) {
                    return Err(format_error("Decimal128 coefficient exceeds its path declaration"));
                }
                Self::Decimal128(*precision, *scale, coefficient)
            },
            DataType::UInt8 => Self::Integer(i128::from(value!(UInt8Array) as i8)),
            DataType::UInt16 => Self::Integer(i128::from(value!(UInt16Array) as i16)),
            DataType::UInt32 => Self::Integer(i128::from(value!(UInt32Array) as i32)),
            DataType::UInt64 => Self::Integer(i128::from(value!(UInt64Array) as i64)),
            DataType::Float32 => Self::FloatOrder(float_order(f64::from(value!(Float32Array)))),
            DataType::Float64 => Self::FloatOrder(float_order(value!(Float64Array))),
            DataType::Utf8 => Self::Text(value!(StringArray)),
            DataType::Binary => Self::Binary(value!(BinaryArray)),
            DataType::Date32 => Self::Day(value!(Date32Array).into()),
            DataType::Date64 => Self::Day(value!(Date64Array).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY)),
            DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(_)) => {
                Self::InstantMicros(value!(TimestampMicrosecondArray))
            }
            _ => Self::Unordered,
        })
    }
}

pub(super) fn decimal_coefficient_fits(precision: u8, coefficient: i128) -> bool {
    (1..=38).contains(&precision) && coefficient.unsigned_abs() < 10_u128.pow(u32::from(precision))
}

pub(super) const FLOAT_NAN_ORDER: u64 = u64::MAX;

// SQL sorting distinguishes signed zeros and places every NaN in one last
// equivalence class. Float payloads still come from the original row.
pub(super) fn float_order(value: f64) -> u64 {
    const SIGN: u64 = 1 << 63;
    if value.is_nan() {
        return FLOAT_NAN_ORDER;
    }
    let bits = value.to_bits();
    if bits & SIGN == 0 { bits ^ SIGN } else { !bits }
}

pub(super) fn arrow<T: 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format_error("Unexpected ordered index Arrow array"))
}

impl<Text, Binary> Scalar<Text, Binary> {
    pub(super) fn family(&self) -> Option<Family> {
        Some(match self {
            Self::Null | Self::Unordered => return None,
            Self::Bool(_) => Family::Bool,
            Self::Integer(_) => Family::Integer,
            Self::Text(_) => Family::Text,
            Self::Binary(_) => Family::Binary,
            Self::Uuid(_) => Family::Uuid,
            Self::Day(_) => Family::Day,
            Self::InstantMicros(_) => Family::InstantMicros,
            Self::FloatOrder(_) => Family::Float,
            Self::Decimal128(precision, scale, _) => Family::Decimal128(*precision, *scale),
        })
    }
}

impl<Text: AsRef<str>, Binary: AsRef<[u8]>> Scalar<Text, Binary> {
    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Text(value) => value.as_ref().len(),
            Self::Binary(value) => value.as_ref().len(),
            _ => 0,
        }
    }
}

pub(super) fn family(logical: &LogicalType) -> Option<Family> {
    scalar_family(logical, false)
}

pub(super) fn owned_family(logical: &LogicalType) -> Option<Family> {
    scalar_family(logical, true)
}

fn scalar_family(logical: &LogicalType, owned: bool) -> Option<Family> {
    use ScalarType as S;
    let mut family = None;
    for leg in logical.legs() {
        let next = match leg {
            LogicalType::Null => continue,
            LogicalType::Scalar(S::Bool) => Family::Bool,
            LogicalType::Scalar(
                S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64,
            ) => Family::Integer,
            LogicalType::Scalar(S::F32 | S::F64) => Family::Float,
            LogicalType::Scalar(S::Decimal { bit_width: 128, precision, scale }) if owned =>
                Family::Decimal128(precision.try_into().ok()?, scale.try_into().ok()?),
            LogicalType::Scalar(S::Utf8) => Family::Text,
            LogicalType::Scalar(S::Binary) => Family::Binary,
            LogicalType::Scalar(S::Uuid) => Family::Uuid,
            LogicalType::Scalar(S::DateDay | S::DateMilli) => Family::Day,
            LogicalType::Scalar(S::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some(_),
            }) => Family::InstantMicros,
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
    pub level: u32,
    pub start: u64,
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
            let (level, next) = cover_step(start, end);
            let bucket = Self {
                level,
                start: start as u64,
            };
            start = next;
            Some(bucket)
        })
    }
}
