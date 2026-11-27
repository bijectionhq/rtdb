//! The cells a predicate compares, read from any plain layout a batch column holds, and their order, as the row
//! evaluator orders scalar values.

use std::{cmp::Ordering, sync::Arc};

use arrow_array::{Array, BooleanArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, PrimitiveArray, StringArray, TimestampMicrosecondArray, UnionArray, types::ArrowPrimitiveType};
use arrow_schema::{DataType, Field as ArrowField, TimeUnit as ArrowUnit, UnionMode};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{LogicalType, ScalarType, TimeUnit, Value};

use super::super::{Column, SelectedRows, plain_scalar};
use super::{Charged, Vector};

/// A comparable cell; integers and floats compare only where the conversion between them is exact.
#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub(super) enum Cell<'a> {
    Int(i64), Float(f64), Text(&'a str), Bool(bool), Micros(i64, Option<&'a str>),
    /// A UUID as the big-endian integer of its bytes, which orders as the bytes do.
    Uuid(u128),
}

impl Cell<'_> {
    pub(super) fn is_nan(&self) -> bool { matches!(self, Self::Float(value) if value.is_nan()) }
}

/// A batch column's cells, read in place.
pub(super) enum Cells<'a> {
    Null, Uuid(&'a FixedSizeBinaryArray), Int(&'a Int64Array), Int32(&'a Int32Array), Int16(&'a Int16Array), Int8(&'a Int8Array),
    Float(&'a Float64Array), Float32(&'a Float32Array), Text(&'a StringArray), Bool(&'a BooleanArray),
    Micros(&'a TimestampMicrosecondArray, Option<&'a str>), Values(&'a [Value]),
    /// A dense union whose legs are each a plain cell type, read through the leg each cell's type id names.
    Union { array: &'a UnionArray, legs: Vec<Option<Cells<'a>>> },
    Selected { rows: &'a SelectedRows, sources: Vec<Option<Cells<'a>>> },
}

impl<'a> Cells<'a> {
    pub(super) fn of(column: &'a Column) -> Option<Self> {
        match column {
            Column::Values(values) => Some(Self::Values(values)),
            Column::Arrow { array, field, logical } => Self::arrow(array.as_ref(), field, logical.as_ref()),
            Column::Selected(selected) => {
                let sources = selected.sources.iter().map(|source| match source {
                    Some((array, field, logical, _)) => Self::arrow(array.as_ref(), field, logical.as_ref()).map(Some),
                    None => Some(None),
                }).collect::<Option<Vec<_>>>()?;
                Some(Self::Selected { rows: &selected.cells, sources })
            }
        }
    }

    fn arrow(array: &'a dyn Array, field: &'a ArrowField, logical: Option<&'a LogicalType>) -> Option<Self> {
        let scalar = logical.and_then(plain_scalar);
        let plain = |expected: ScalarType| match logical { Some(_) => scalar == Some(&expected), None => field.metadata().is_empty() };
        let any = array.as_any();
        match array.data_type() {
            DataType::Null if field.is_nullable() && logical.is_none_or(|ty| *ty == LogicalType::Null) => Some(Self::Null),
            DataType::Int64 if plain(ScalarType::I64) => any.downcast_ref().map(Self::Int),
            DataType::Int32 if plain(ScalarType::I32) => any.downcast_ref().map(Self::Int32),
            DataType::Int16 if plain(ScalarType::I16) => any.downcast_ref().map(Self::Int16),
            DataType::Int8 if plain(ScalarType::I8) => any.downcast_ref().map(Self::Int8),
            DataType::Float32 if plain(ScalarType::F32) => any.downcast_ref().map(Self::Float32),
            DataType::Float64 if plain(ScalarType::F64) => any.downcast_ref().map(Self::Float),
            DataType::Utf8 if plain(ScalarType::Utf8) => any.downcast_ref().map(Self::Text),
            DataType::Boolean if plain(ScalarType::Bool) => any.downcast_ref().map(Self::Bool),
            // Sixteen bytes are a UUID only where the column's logical type says so.
            DataType::FixedSizeBinary(16) if scalar == Some(&ScalarType::Uuid) => any.downcast_ref().map(Self::Uuid),
            // A cell reads its zone from the column's logical type, or from the layout where it has none.
            DataType::Timestamp(ArrowUnit::Microsecond, zone) => {
                let zone = match (logical, scalar) {
                    (Some(_), Some(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone })) => timezone.as_deref(),
                    (None, _) if field.metadata().is_empty() => zone.as_deref(),
                    _ => return None,
                };
                any.downcast_ref().map(|array| Self::Micros(array, zone))
            }
            DataType::Union(fields, UnionMode::Dense) => {
                if logical.is_some_and(|logical| rtdb_columnar::from_field(field).ok().as_ref() != Some(logical)) {
                    return None;
                }
                let array: &UnionArray = any.downcast_ref()?;
                let mut legs: Vec<Option<Self>> = Vec::new();
                for (id, field) in fields.iter() {
                    if id < 0 || !field.metadata().is_empty() { return None; }
                    let child = array.child(id);
                    let leg = child.as_any();
                    let cells = match child.data_type() {
                        DataType::Int64 => Self::Int(leg.downcast_ref()?),
                        DataType::Int32 => Self::Int32(leg.downcast_ref()?),
                        DataType::Int16 => Self::Int16(leg.downcast_ref()?),
                        DataType::Int8 => Self::Int8(leg.downcast_ref()?),
                        DataType::Float32 => Self::Float32(leg.downcast_ref()?),
                        DataType::Float64 => Self::Float(leg.downcast_ref()?),
                        DataType::Utf8 => Self::Text(leg.downcast_ref()?),
                        DataType::Boolean => Self::Bool(leg.downcast_ref()?),
                        _ => return None,
                    };
                    let id = id as usize;
                    if legs.len() <= id { legs.resize_with(id + 1, || None); }
                    legs[id] = Some(cells);
                }
                Some(Self::Union { array, legs })
            }
            _ => None,
        }
    }

    /// Whether the layout alone shows that no cell is null.
    pub(super) fn complete(&self) -> bool {
        match self {
            Self::Int(array) => array.null_count() == 0,
            Self::Int32(array) => array.null_count() == 0,
            Self::Int16(array) => array.null_count() == 0,
            Self::Int8(array) => array.null_count() == 0,
            Self::Float(array) => array.null_count() == 0,
            Self::Float32(array) => array.null_count() == 0,
            Self::Text(array) => array.null_count() == 0,
            Self::Bool(array) => array.null_count() == 0,
            Self::Micros(array, _) => array.null_count() == 0,
            Self::Uuid(array) => array.null_count() == 0,
            Self::Null | Self::Values(_) | Self::Union { .. } | Self::Selected { .. } => false,
        }
    }

    /// `Err` where a cell has a type no comparison here reads.
    fn get(&self, row: usize) -> Result<Option<Cell<'a>>, ()> {
        fn valid<A: Array>(array: &A, row: usize) -> bool { array.is_valid(row) }
        Ok(match self {
            Self::Null => None,
            Self::Selected { rows, sources } => {
                let (source, position) = rows.get(row).ok_or(())?;
                match &sources[source] {
                    Some(cells) => cells.get(position)?,
                    None => None,
                }
            }
            Self::Int(array) => integral(*array, row),
            Self::Int32(array) => integral(*array, row),
            Self::Int16(array) => integral(*array, row),
            Self::Int8(array) => integral(*array, row),
            Self::Float(array) => valid(*array, row).then(|| Cell::Float(array.value(row))),
            Self::Float32(array) => valid(*array, row).then(|| Cell::Float(f64::from(array.value(row)))),
            Self::Text(array) => valid(*array, row).then(|| Cell::Text(array.value(row))),
            Self::Bool(array) => valid(*array, row).then(|| Cell::Bool(array.value(row))),
            Self::Micros(array, zone) => valid(*array, row).then(|| Cell::Micros(array.value(row), *zone)),
            Self::Uuid(array) => match valid(*array, row).then(|| array.value(row).try_into()) {
                Some(bytes) => Some(Cell::Uuid(u128::from_be_bytes(bytes.map_err(|_| ())?))),
                None => None,
            },
            Self::Values(values) => scalar(&values[row])?,
            Self::Union { array, legs } => {
                let leg = usize::try_from(array.type_id(row)).ok().and_then(|id| legs.get(id)).and_then(Option::as_ref).ok_or(())?;
                leg.get(array.value_offset(row))?
            }
        })
    }
}

fn integral<T: ArrowPrimitiveType>(array: &PrimitiveArray<T>, row: usize) -> Option<Cell<'static>>
where i64: From<T::Native> {
    array.is_valid(row).then(|| Cell::Int(i64::from(array.value(row))))
}

/// The cell of a scalar value, `Err` for a type no comparison here reads.
pub(super) fn scalar(value: &Value) -> Result<Option<Cell<'_>>, ()> {
    Ok(match value {
        Value::Null => None,
        Value::I8(value) => Some(Cell::Int(i64::from(*value))),
        Value::I16(value) => Some(Cell::Int(i64::from(*value))),
        Value::I32(value) => Some(Cell::Int(i64::from(*value))),
        Value::I64(value) => Some(Cell::Int(*value)),
        Value::F32(value) => Some(Cell::Float(f64::from(*value))),
        Value::F64(value) => Some(Cell::Float(*value)),
        Value::Utf8(value) => Some(Cell::Text(value)),
        Value::Bool(value) => Some(Cell::Bool(*value)),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone } => Some(Cell::Micros(*value, timezone.as_deref())),
        Value::Uuid(bytes) => Some(Cell::Uuid(u128::from_be_bytes(*bytes))),
        _ => return Err(()),
    })
}

/// Whether a stored layout without a direct cell reader can be read once into its values for comparison.
pub(super) fn readable_values(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(fields, UnionMode::Dense) => fields.iter().all(|(_, field)| readable_values(field.data_type())),
        DataType::RunEndEncoded(_, field) => readable_values(field.data_type()),
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float32 | DataType::Float64
        | DataType::Timestamp(ArrowUnit::Microsecond, _) => true,
        _ => false,
    }
}

const EXACT: i64 = 1 << 53;

/// Orders two non-NaN cells as the scalar comparison does, or `Err` where the pair is outside these comparisons.
pub(super) fn order(left: Cell<'_>, right: Cell<'_>, equality_only: bool) -> Result<Ordering, ()> {
    let float = |value: f64| if value.is_nan() { Err(()) } else { Ok(value) };
    // Equality is IEEE (`-0.0 = 0.0`), ordering is total (`-0.0 < 0.0`), as in the scalar evaluator.
    let floats = |a: f64, b: f64| if equality_only { a.partial_cmp(&b).ok_or(()) } else { Ok(a.total_cmp(&b)) };
    match (left, right) {
        (Cell::Int(a), Cell::Int(b)) => Ok(a.cmp(&b)),
        (Cell::Micros(a, zoned), Cell::Micros(b, other)) if zoned.is_some() == other.is_some() => Ok(a.cmp(&b)),
        (Cell::Float(a), Cell::Float(b)) => floats(float(a)?, float(b)?),
        (Cell::Int(a), Cell::Float(b)) if a.unsigned_abs() < EXACT as u64 => floats(a as f64, float(b)?),
        (Cell::Float(a), Cell::Int(b)) if b.unsigned_abs() < EXACT as u64 => floats(float(a)?, b as f64),
        (Cell::Text(a), Cell::Text(b)) if equality_only || (a.is_ascii() && b.is_ascii()) => Ok(a.cmp(b)),
        (Cell::Bool(a), Cell::Bool(b)) if equality_only => Ok(if a == b { Ordering::Equal } else { Ordering::Less }),
        (Cell::Uuid(a), Cell::Uuid(b)) => Ok(a.cmp(&b)),
        _ => Err(()),
    }
}

/// `===`: nulls are identical to each other, and two values are identical where they are of one category and equal
/// under SQL equality; `Err` where the pair is outside these comparisons.
pub(super) fn identical(left: Option<Cell<'_>>, right: Option<Cell<'_>>) -> Result<bool, ()> {
    Ok(match (left, right) {
        (None, None) => true,
        (None, Some(_)) | (Some(_), None) => false,
        (Some(Cell::Int(a)), Some(Cell::Int(b))) => a == b,
        // Floats keep their category but compare by SQL equality, so NaN differs from itself.
        (Some(Cell::Float(a)), Some(Cell::Float(b))) => a == b,
        (Some(Cell::Int(_)), Some(Cell::Float(_))) | (Some(Cell::Float(_)), Some(Cell::Int(_))) => false,
        (Some(Cell::Text(a)), Some(Cell::Text(b))) => a == b,
        (Some(Cell::Bool(a)), Some(Cell::Bool(b))) => a == b,
        (Some(Cell::Uuid(a)), Some(Cell::Uuid(b))) => a == b,
        // Timestamps are equal only under the same zone label.
        (Some(Cell::Micros(a, zone)), Some(Cell::Micros(b, other))) => zone == other && a == b,
        _ => return Err(()),
    })
}

/// An operand of a comparison: cells read in place from a batch column or a bound parameter, a constant, the values
/// of a column without a direct cell reader, or a vector the evaluator computed.
pub(super) enum Operand<'a> {
    Cells(Cells<'a>),
    Constant(Option<Cell<'a>>),
    /// Values read once, with the charge for holding them; unread rows are null.
    Values { values: Vec<Value>, _charge: Arc<MemoryReservation> },
    Computed(Charged),
}

impl Operand<'_> {
    /// Each row's cell, `Err` where its type is outside these comparisons.
    pub(super) fn get(&self, row: usize) -> Result<Option<Cell<'_>>, ()> {
        match self {
            Self::Cells(cells) => cells.get(row),
            Self::Constant(cell) => Ok(*cell),
            Self::Values { values, .. } => scalar(&values[row]),
            Self::Computed(charged) => Ok(match &charged.vector {
                Vector::Null => None,
                Vector::I32(array) => integral(array, row),
                Vector::I64(array) => integral(array, row),
                Vector::F64(array) => array.is_valid(row).then(|| Cell::Float(array.value(row))),
                Vector::Text(array) => array.is_valid(row).then(|| Cell::Text(array.value(row))),
                Vector::Bool(array) => array.is_valid(row).then(|| Cell::Bool(array.value(row))),
                Vector::Micros(array, zone) => array.is_valid(row).then(|| Cell::Micros(array.value(row), zone.as_deref())),
            }),
        }
    }
}
