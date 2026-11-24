use rtdb_types::StructFields;
use std::{borrow::{Borrow, Cow}, sync::Arc};

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, ListArray, MapArray, NullArray, RunArray, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array, UnionArray, builder::FixedSizeBinaryBuilder, make_array, types::{Int16Type, Int32Type, Int64Type}};
use arrow_buffer::{BooleanBufferBuilder, IntervalDayTime, IntervalMonthDayNano, NullBuffer, OffsetBuffer, ScalarBuffer, i256};
use arrow_data::transform::MutableArrayData;
use arrow_schema::{DataType, Field, Fields, UnionMode};
use rtdb_types::{Anomaly, ListBuilder, DecimalValue, IntervalUnit, ListKind, ListValue, ListView, LogicalType, Result, RunEndWidth, ScalarType, TimeUnit, Value};

use crate::{EntityIdRef, entity_id::borrow_id_scalar, from_field, leg_name, to_field};

/// Builds arrays with exact logical value types; numeric coercions belong to expressions.
/// Undefined cells are accepted only for bottom-typed fields.
/// Calendar objects use the vector object writer's truncating day and wrapping millisecond conversions.
pub fn build_array(logical: &LogicalType, values: &[Value]) -> Result<ArrayRef> {
    build_values(logical, values, true)
}

pub fn build_array_refs(logical: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    build_values(logical, values, true)
}

/// Builds a column from scalar evaluation results using bounded temporary cell storage.
pub fn copy_evaluated_cells(
    ty: &LogicalType, rows: usize, memory: &Arc<crate::MemoryBudget>,
    mut bound: impl FnMut(usize) -> Result<usize>, mut read: impl FnMut(usize) -> Result<Value>,
) -> Result<(ArrayRef, Arc<crate::MemoryReservation>)> {
    const CELLS: usize = 128;
    let overflow = || Anomaly::incorrect("rtdb/column-size", "Evaluated column exceeds addressable memory");
    let _inventory = memory.reserve(rows.div_ceil(CELLS).checked_mul(128)
        .and_then(|size| size.checked_add(256)).ok_or_else(overflow)?)?;
    let mut arrays = Vec::with_capacity(rows.div_ceil(CELLS));
    for start in (0..rows).step_by(CELLS) {
        let end = start.saturating_add(CELLS).min(rows);
        let bytes = (start..end).try_fold((end - start) * std::mem::size_of::<Value>(), |bytes, row| {
            bytes.checked_add(bound(row)?).ok_or_else(overflow)
        })?;
        let _cells = memory.reserve(bytes)?;
        let values = (start..end).map(&mut read).collect::<Result<Vec<_>>>()?;
        let mut scratch = memory.reserve(crate::estimate_builder_bytes(ty, &values)?)?;
        let array = copy_array(ty, &values)?;
        Arc::get_mut(&mut scratch).expect("Cell writer owns its chunk").shrink_to(array.get_array_memory_size())?;
        arrays.push((array, scratch));
    }
    match arrays.len() {
        0 => {
            let mut scratch = memory.reserve(crate::estimate_builder_bytes(ty, &[])?)?;
            let array = copy_array(ty, &[])?;
            Arc::get_mut(&mut scratch).expect("Cell writer owns its empty vector").shrink_to(array.get_array_memory_size())?;
            Ok((array, scratch))
        }
        1 => Ok(arrays.pop().expect("The evaluated column has one chunk")),
        _ => {
            let field = crate::to_field("value", ty)?;
            let bytes = arrays.iter().try_fold(rows.checked_mul(64).ok_or_else(overflow)?, |bytes, (array, _)| {
                (0..array.len()).try_fold(bytes, |bytes, row| {
                    let cell = crate::estimate_copy_cell_bytes(&field, array.as_ref(), row)?;
                    bytes.checked_add(cell.checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow)
                })
            })?;
            let mut scratch = memory.reserve(bytes)?;
            let inputs = arrays.iter().map(|(array, _)| array.as_ref()).collect::<Vec<_>>();
            let picks = arrays.iter().enumerate().flat_map(|(source, (array, _))| (0..array.len()).map(move |row| (source, row))).collect::<Vec<_>>();
            let array = crate::copy_selected_cells(&inputs, &picks)?;
            Arc::get_mut(&mut scratch).expect("Cell writer owns its result").shrink_to(array.get_array_memory_size())?;
            Ok((array, scratch))
        }
    }
}

/// Reconstructs typed cells without revalidating decimal precision against their stored coefficients.
/// Every nonnull value must belong to its target logical type, including exact decimal parameters.
/// Coefficient width is checked; values are neither rescaled nor relabeled.
/// Unadmitted calendar objects are rejected, including nested cells.
/// The caller reserves `estimate_builder_bytes` before invoking this function.
pub fn copy_array(logical: &LogicalType, values: &[Value]) -> Result<ArrayRef> {
    copy_values(logical, values)
}

/// Copies existing logical cells from borrowed values without cloning them.
pub fn copy_array_refs(logical: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    copy_values(logical, values)
}

fn copy_values(logical: &LogicalType, values: &[impl Borrow<Value>]) -> Result<ArrayRef> {
    if values.iter().map(Borrow::borrow).any(Value::has_calendar_object) {
        return Err(Anomaly::incorrect("rtdb/unadmitted-calendar-value", "Stored-cell copies require admitted calendar values"));
    }
    build_values(logical, values, false)
}

fn build_values(logical: &LogicalType, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    logical.validate()?;
    let admission = Admission::new(logical);
    for (row, value) in values.iter().map(Borrow::borrow).enumerate() {
        if !admission.accepts(value) {
            return Err(Anomaly::incorrect("rtdb/column-type-mismatch", format!("Value at row {row} does not belong to {logical:?}")));
        }
    }
    build(logical, values, check_precision)
}

pub(crate) fn accepts(logical: &LogicalType, value: &Value) -> bool { Admission::new(logical).accepts(value) }

/// Whether values belong to a column type: joining a value's type into the column's leaves it unchanged.
/// Built once per column so that each value is checked against the type without re-deriving it.
pub(crate) struct Admission { plain: LogicalType, join_normal: bool }

impl Admission {
    pub(crate) fn new(logical: &LogicalType) -> Self {
        let plain = logical.unencoded();
        let join_normal = join_normal(&plain);
        Self { plain, join_normal }
    }

    pub(crate) fn accepts(&self, value: &Value) -> bool {
        let plain = &self.plain;
        if matches!(value, Value::Undefined) { return matches!(plain, LogicalType::Nothing); }
        if self.join_normal { return covers(plain, value); }
        let actual = value.logical_type();
        *plain == actual
            || matches!(plain, LogicalType::Maybe(inner) if **inner == actual || actual == LogicalType::Null)
            || plain.join(&actual) == *plain
    }
}

/// Whether `ty` is its own join: the canonical leg shape, one struct leg and one list leg per kind at every depth.
fn join_normal(ty: &LogicalType) -> bool {
    match ty {
        LogicalType::Nothing | LogicalType::Null | LogicalType::Scalar(_) => true,
        LogicalType::Maybe(inner) => matches!(inner.as_ref(), LogicalType::Scalar(_) | LogicalType::List { .. } | LogicalType::Struct(_)) && join_normal(inner),
        LogicalType::List { element, .. } => join_normal(element),
        LogicalType::Struct(fields) => fields.values().all(join_normal),
        LogicalType::Union(legs) => {
            let mut structs = 0;
            let mut kinds = std::collections::BTreeSet::new();
            legs.iter().filter(|leg| **leg != LogicalType::Null).count() >= 2 && legs.iter().all(|leg| match leg {
                LogicalType::Null | LogicalType::Scalar(_) => true,
                LogicalType::List { kind, element } => kinds.insert(kind.clone()) && join_normal(element),
                LogicalType::Struct(fields) => { structs += 1; structs == 1 && fields.values().all(join_normal) }
                _ => false,
            })
        }
        LogicalType::RunEndEncoded { .. } => false,
    }
}

/// `plain.join(value.logical_type()) == plain` for a join-normal `plain`, without building the value's type.
fn covers(plain: &LogicalType, value: &Value) -> bool {
    let legs: Vec<&LogicalType> = match (plain, value) {
        (_, Value::Undefined) => return true,
        (LogicalType::Null | LogicalType::Maybe(_), Value::Null) => return true,
        (LogicalType::Union(legs), Value::Null) => return legs.contains(&LogicalType::Null),
        (_, Value::Null) => return false,
        (LogicalType::Nothing, _) => return false,
        (LogicalType::Maybe(inner), _) => vec![inner.as_ref()],
        (LogicalType::Union(legs), _) => legs.iter().collect(),
        (leg, _) => vec![leg],
    };
    let nullable = |ty: &LogicalType| match ty {
        LogicalType::Null | LogicalType::Maybe(_) => true,
        LogicalType::Union(legs) => legs.contains(&LogicalType::Null),
        _ => false,
    };
    match value {
        Value::List(values) | Value::FixedSizeList(values) => {
            let kind = if matches!(value, Value::FixedSizeList(_)) {
                ListKind::FixedSizeList(i32::try_from(values.len()).unwrap_or(i32::MAX))
            } else { ListKind::List };
            legs.iter().any(|leg| match leg {
                LogicalType::List { kind: expected, element } if *expected == kind => {
                    match values.view() {
                        rtdb_types::ListView::Values(values) => values.iter().all(|value| covers(element, value)),
                        _ => match value.logical_type() {
                            LogicalType::List { element: actual, .. } => element.join(&actual) == **element,
                            _ => false,
                        },
                    }
                }
                _ => false,
            })
        }
        Value::Set(values) => {
            legs.iter().any(|leg| matches!(leg, LogicalType::List { kind, element } if *kind == ListKind::Set && values.iter().all(|value| covers(element, value))))
        }
        Value::Map { entries, sorted } => legs.iter().any(|leg| match leg {
            LogicalType::List { kind: ListKind::Map { sorted: expected }, element } => match element.as_ref() {
                LogicalType::Struct(fields) => expected == sorted
                    && fields.iter().all(|(name, ty)| name == "key" || name == "value" || nullable(ty))
                    && match (fields.get("key"), fields.get("value")) {
                        (Some(key), Some(item)) => entries.iter().all(|(k, v)| covers(key, k) && covers(item, v)),
                        _ => false,
                    },
                _ => false,
            },
            _ => false,
        }),
        Value::Struct(values) => legs.iter().any(|leg| match leg {
            LogicalType::Struct(fields) => values.iter().all(|(name, value)| fields.get(name).is_some_and(|ty| covers(ty, value)))
                && fields.iter().all(|(name, ty)| values.contains_key(name) || nullable(ty)),
            _ => false,
        }),
        value => match value.logical_type() {
            LogicalType::Scalar(actual) => legs.iter().any(|leg| matches!(leg, LogicalType::Scalar(scalar) if *scalar == actual)),
            actual => plain.join(&actual) == *plain,
        },
    }
}

/// Resolves a struct field's cell in its declared layout.
/// A missing bottom-typed field returns `Value::Undefined`.
/// Other missing fields return `Value::Null`.
pub fn struct_field_cell<'a>(fields: &'a StructFields<Value>, name: &str, logical: &LogicalType) -> &'a Value {
    fields.get(name).unwrap_or(match logical {
        LogicalType::Nothing => &Value::Undefined,
        _ => &Value::Null,
    })
}

pub fn build_struct_array(fields: Fields, arrays: &[ArrayRef], length: usize, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
    let data = arrow_data::ArrayData::builder(DataType::Struct(fields)).len(length).nulls(nulls)
        .child_data(arrays.iter().map(|array| array.to_data()).collect()).build().map_err(super::arrow_error)?;
    Ok(make_array(data))
}

/// Constructs a list whose child layout may express nulls through a dense union.
pub fn build_list_array(item: Arc<Field>, offsets: ScalarBuffer<i32>, values: &ArrayRef, nulls: Option<NullBuffer>) -> Result<ArrayRef> {
    let length = offsets.len().checked_sub(1).ok_or_else(layout_error)?;
    let data = arrow_data::ArrayData::builder(DataType::List(item)).len(length).nulls(nulls)
        .add_buffer(offsets.into_inner()).add_child_data(values.to_data()).build().map_err(super::arrow_error)?;
    Ok(make_array(data))
}

fn validity(values: &[impl Borrow<Value>]) -> Option<NullBuffer> {
    let valid: Vec<_> = values.iter().map(Borrow::borrow).map(|v| !matches!(v, Value::Null | Value::Undefined)).collect();
    valid.iter().any(|v| !v).then(|| NullBuffer::from(valid))
}

fn build(logical: &LogicalType, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    match logical {
        LogicalType::Nothing | LogicalType::Null => Ok(Arc::new(NullArray::new(values.len()))),
        LogicalType::Maybe(inner) => build(inner, values, check_precision),
        LogicalType::RunEndEncoded { width, value } => build_runs(logical, *width, value, values, check_precision),
        LogicalType::Scalar(scalar) => build_scalar(scalar, values, check_precision),
        LogicalType::Struct(children) => {
            let field = to_field("", logical)?;
            let DataType::Struct(fields) = field.data_type() else { return Err(layout_error()); };
            let arrays = children.iter().map(|(name, child_type)| {
                let child_values = values.iter().map(Borrow::borrow).map(|v| match v {
                    Value::Struct(fields) => struct_field_cell(fields, name, child_type),
                    _ => &Value::Undefined,
                }).collect::<Vec<_>>();
                build(child_type, &child_values, check_precision)
            }).collect::<Result<Vec<_>>>()?;
            build_struct_array(fields.clone(), &arrays, values.len(), validity(values))
        }
        LogicalType::List { kind, element } => build_list(logical, kind, element, values, check_precision),
        LogicalType::Union(_) => build_union(logical, values, check_precision),
    }
}

macro_rules! primitive {
    ($values:expr, $array:ty, $pattern:pat => $value:expr) => {{
        let values = $values.iter().map(Borrow::borrow).map(|value| match value { $pattern => Some($value), _ => None }).collect::<Vec<_>>();
        Ok(Arc::new(<$array>::from(values)) as ArrayRef)
    }};
}

fn build_scalar(scalar: &ScalarType, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    use ScalarType::*;
    match scalar {
        Bool => primitive!(values, BooleanArray, Value::Bool(v) => *v),
        I8 => primitive!(values, Int8Array, Value::I8(v) => *v),
        I16 => primitive!(values, Int16Array, Value::I16(v) => *v),
        I32 => primitive!(values, Int32Array, Value::I32(v) => *v),
        I64 => primitive!(values, Int64Array, Value::I64(v) => *v),
        U8 => primitive!(values, UInt8Array, Value::U8(v) => *v),
        U16 => primitive!(values, UInt16Array, Value::U16(v) => *v),
        U32 => primitive!(values, UInt32Array, Value::U32(v) => *v),
        U64 => primitive!(values, UInt64Array, Value::U64(v) => *v),
        F16 => primitive!(values, Float16Array, Value::F16(v) => half::f16::from_bits(*v)),
        F32 => primitive!(values, Float32Array, Value::F32(v) => *v),
        F64 => primitive!(values, Float64Array, Value::F64(v) => *v),
        DateDay => Ok(Arc::new(Date32Array::from(values.iter().map(Borrow::borrow).map(|value| match value {
            Value::DateDay(value) => Ok(Some(*value)),
            Value::Date(value) => match rtdb_types::CalendarDate::new(value.date(), rtdb_types::DateUnit::Day).object_value()? {
                Value::DateDay(days) => Ok(Some(days)),
                _ => Err(layout_error()),
            },
            _ => Ok(None),
        }).collect::<Result<Vec<_>>>()?))),
        DateMilli => Ok(Arc::new(Date64Array::from(values.iter().map(Borrow::borrow).map(|value| match value {
            Value::DateMilli(value) => Ok(Some(*value)),
            Value::Date(value) => match rtdb_types::CalendarDate::new(value.date(), rtdb_types::DateUnit::Millisecond).object_value()? {
                Value::DateMilli(millis) => Ok(Some(millis)),
                _ => Err(layout_error()),
            },
            _ => Ok(None),
        }).collect::<Result<Vec<_>>>()?))),
        Oid => primitive!(values, Int32Array, Value::Oid(v) => *v),
        RegClass => primitive!(values, Int32Array, Value::RegClass(v) => *v),
        RegProc => primitive!(values, Int32Array, Value::RegProc(v) => *v),
        Utf8 | Uri | Keyword => {
            if *scalar == Uri { for value in values.iter().map(Borrow::borrow) { if let Value::Uri(text) = value { rtdb_types::UriParts::parse(text)?; } } }
            let strings = values.iter().map(Borrow::borrow).map(|value| match value {
                Value::Utf8(value) | Value::Uri(value) => Some(value.as_str()),
                Value::Keyword(value) => Some(value.as_str()),
                _ => None,
            }).collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(strings)))
        }
        Binary | Transit => {
            let values = values.iter().map(Borrow::borrow).map(|value| match value {
                Value::Binary(value) | Value::Transit(value) => Some(value.as_slice()),
                _ => None,
            }).collect::<Vec<_>>();
            Ok(Arc::new(BinaryArray::from(values)))
        }
        FixedSizeBinary(width) => build_binary(*width, values),
        Uuid => build_binary(16, values),
        Decimal { precision, scale, bit_width } => build_decimal(*precision, *scale, *bit_width, values, check_precision),
        Time { unit, bit_width } => {
            if *bit_width == 32 {
                let converted = values.iter().map(Borrow::borrow).map(|value| match value {
                    Value::Time { value, .. } => i32::try_from(*value).map(Some).map_err(|_| Anomaly::incorrect("rtdb/time-overflow", "Time exceeds i32 representation")),
                    _ => Ok(None),
                }).collect::<Result<Vec<_>>>()?;
                match unit {
                    TimeUnit::Second => Ok(Arc::new(Time32SecondArray::from(converted))),
                    TimeUnit::Millisecond => Ok(Arc::new(Time32MillisecondArray::from(converted))),
                    _ => Err(layout_error()),
                }
            } else {
                match unit {
                    TimeUnit::Microsecond => primitive!(values, Time64MicrosecondArray, Value::Time { value, .. } => *value),
                    TimeUnit::Nanosecond => primitive!(values, Time64NanosecondArray, Value::Time { value, .. } => *value),
                    _ => Err(layout_error()),
                }
            }
        }
        Timestamp { unit, timezone } => {
            let values = values.iter().map(Borrow::borrow).map(|v| match v { Value::Timestamp { value, .. } => Some(*value), _ => None }).collect::<Vec<_>>();
            match unit {
                TimeUnit::Second => Ok(Arc::new(TimestampSecondArray::from(values).with_timezone_opt(timezone.as_deref()))),
                TimeUnit::Millisecond => Ok(Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(timezone.as_deref()))),
                TimeUnit::Microsecond => Ok(Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(timezone.as_deref()))),
                TimeUnit::Nanosecond => Ok(Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(timezone.as_deref()))),
            }
        }
        Duration(unit) => match unit {
            TimeUnit::Second => primitive!(values, DurationSecondArray, Value::Duration { value, .. } => *value),
            TimeUnit::Millisecond => primitive!(values, DurationMillisecondArray, Value::Duration { value, .. } => *value),
            TimeUnit::Microsecond => primitive!(values, DurationMicrosecondArray, Value::Duration { value, .. } => *value),
            TimeUnit::Nanosecond => primitive!(values, DurationNanosecondArray, Value::Duration { value, .. } => *value),
        },
        Interval(IntervalUnit::YearMonth) => primitive!(values, IntervalYearMonthArray, Value::IntervalYearMonth(v) => *v),
        Interval(IntervalUnit::DayTime) => primitive!(values, IntervalDayTimeArray, Value::IntervalDayTime { days, milliseconds } => IntervalDayTime::new(*days, *milliseconds)),
        Interval(IntervalUnit::MonthDayNano) => primitive!(values, IntervalMonthDayNanoArray, Value::IntervalMonthDayNano { months, days, nanoseconds } => IntervalMonthDayNano::new(*months, *days, *nanoseconds)),
        IntervalMonthDayMicro => primitive!(values, IntervalMonthDayNanoArray, Value::IntervalMonthDayMicro { months, days, nanoseconds } => IntervalMonthDayNano::new(*months, *days, *nanoseconds)),
    }
}

fn build_binary(width: i32, values: &[impl Borrow<Value>]) -> Result<ArrayRef> {
    let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), width);
    for value in values.iter().map(Borrow::borrow) {
        match value {
            Value::FixedSizeBinary(bytes) => builder.append_value(bytes).map_err(super::arrow_error)?,
            Value::Uuid(bytes) => builder.append_value(bytes).map_err(super::arrow_error)?,
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_decimal(precision: i32, scale: i32, bit_width: u16, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    match bit_width {
        32 => {
            let values = values.iter().map(Borrow::borrow).map(|value| match value {
                Value::Decimal(value) => {
                    let bytes: [u8; 4] = value.unscaled.as_slice().try_into().map_err(|_| decimal_width())?;
                    Ok(Some(i32::from_be_bytes(bytes)))
                }
                _ => Ok(None),
            }).collect::<Result<Vec<_>>>()?;
            finish_decimal(Decimal32Array::from(values), precision, scale, check_precision, i256::from)
        }
        64 => {
            let values = values.iter().map(Borrow::borrow).map(|value| match value {
                Value::Decimal(value) => {
                    let bytes: [u8; 8] = value.unscaled.as_slice().try_into().map_err(|_| decimal_width())?;
                    Ok(Some(i64::from_be_bytes(bytes)))
                }
                _ => Ok(None),
            }).collect::<Result<Vec<_>>>()?;
            finish_decimal(Decimal64Array::from(values), precision, scale, check_precision, i256::from)
        }
        128 => {
            let values = values.iter().map(Borrow::borrow).map(|value| match value {
                Value::Decimal(value) => {
                    let bytes: [u8; 16] = value.unscaled.as_slice().try_into().map_err(|_| decimal_width())?;
                    Ok(Some(i128::from_be_bytes(bytes)))
                }
                _ => Ok(None),
            }).collect::<Result<Vec<_>>>()?;
            finish_decimal(Decimal128Array::from(values), precision, scale, check_precision, i256::from_i128)
        }
        256 => {
            let values = values.iter().map(Borrow::borrow).map(|value| match value {
                Value::Decimal(value) => {
                    let bytes: [u8; 32] = value.unscaled.as_slice().try_into().map_err(|_| decimal_width())?;
                    Ok(Some(i256::from_be_bytes(bytes)))
                }
                _ => Ok(None),
            }).collect::<Result<Vec<_>>>()?;
            finish_decimal(Decimal256Array::from(values), precision, scale, check_precision, std::convert::identity)
        }
        _ => Err(decimal_width()),
    }
}

fn finish_decimal<T: arrow_array::types::DecimalType>(
    array: arrow_array::PrimitiveArray<T>, precision: i32, scale: i32, check_precision: bool,
    coefficient: impl Fn(T::Native) -> i256,
) -> Result<ArrayRef> {
    if check_precision && let Some(bound) = crate::decimal::precision_bound(precision) {
        if array.iter().flatten().any(|value| !crate::decimal::fits_precision(coefficient(value), Some(bound))) {
            return Err(crate::decimal::precision_error());
        }
    }
    Ok(Arc::new(array.with_data_type(T::TYPE_CONSTRUCTOR(crate::decimal_storage_precision(precision), crate::decimal_storage_scale(scale)))))
}

fn build_list(logical: &LogicalType, kind: &ListKind, element: &LogicalType, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    let field = to_field("", logical)?;
    let packed_schema = match (kind, field.data_type()) {
        (ListKind::FixedSizeList(_), DataType::FixedSizeList(item, width)) => Some((item, Some(*width))),
        (ListKind::List, DataType::List(item)) => Some((item, None)),
        _ => None,
    };
    if let Some((item, width)) = packed_schema {
        if item.metadata().is_empty() && let Some(array) = build_float_lists(item, width, values)? {
            return Ok(array);
        }
        let packed_scalars = values.iter().any(|value| matches!(value.borrow().list_view(), Some(ListView::Scalars(_))));
        if (ListValue::supports_numeric_arrow(item) || packed_scalars)
            && let Some(array) = build_packed_lists(item, width, values, check_precision)?
        {
            return Ok(array);
        }
    }
    let mut flat = Vec::new();
    let mut offsets = vec![0i32];
    let fixed = match kind { ListKind::FixedSizeList(size) => Some(*size as usize), ListKind::TimestampTzRange => Some(2), _ => None };
    for value in values.iter().map(Borrow::borrow) {
        match value {
            Value::Set(children) => flat.extend(children.iter().map(Cow::Borrowed)),
            Value::List(children) | Value::FixedSizeList(children) => flat.extend(children.iter()),
            Value::TimestampTzRange { lower, upper } => {
                flat.push(Cow::Owned(Value::Timestamp { value: *lower, unit: TimeUnit::Microsecond, timezone: Some("UTC".to_owned()) }));
                flat.push(Cow::Owned(Value::Timestamp { value: *upper, unit: TimeUnit::Microsecond, timezone: Some("UTC".to_owned()) }));
            }
            Value::Map { entries, .. } => for (key, value) in entries {
                if matches!(key, Value::Null | Value::Undefined) { return Err(Anomaly::incorrect("rtdb/null-map-key", "Arrow map keys cannot be null")); }
                flat.push(Cow::Owned(Value::Struct(StructFields::from([("key".to_owned(), key.clone()), ("value".to_owned(), value.clone())]))));
            },
            _ => if let Some(size) = fixed { flat.extend((0..size).map(|_| Cow::Borrowed(&Value::Undefined))); },
        }
        offsets.push(i32::try_from(flat.len()).map_err(|_| Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets"))?);
    }
    let array = build(element, &flat, check_precision)?;
    match field.data_type() {
        DataType::List(item) => build_list_array(Arc::clone(item), offsets.into(), &array, validity(values)),
        DataType::FixedSizeList(_, _) => {
            let data = arrow_data::ArrayData::builder(field.data_type().clone()).len(values.len()).nulls(validity(values))
                .add_child_data(array.to_data()).build().map_err(super::arrow_error)?;
            Ok(make_array(data))
        }
        DataType::Map(child, sorted) => {
            let entries = array.as_any().downcast_ref::<StructArray>().ok_or_else(layout_error)?.clone();
            Ok(Arc::new(MapArray::try_new(Arc::clone(child), OffsetBuffer::new(ScalarBuffer::from(offsets)), entries, validity(values), *sorted).map_err(super::arrow_error)?))
        }
        _ => Err(layout_error()),
    }
}

fn build_float_lists(item: &Arc<Field>, width: Option<i32>, values: &[impl Borrow<Value>]) -> Result<Option<ArrayRef>> {
    if width.is_some_and(|width| width <= 0) { return Ok(None); }
    if !matches!(item.data_type(), DataType::Float32 | DataType::Float64) { return Ok(None); }
    let total = values.iter().map(Borrow::borrow).try_fold(0usize, |length, value| {
        let add = match value {
            Value::List(list) | Value::FixedSizeList(list) => list.len(),
            _ => width.unwrap_or(0) as usize,
        };
        length.checked_add(add).ok_or_else(layout_error)
    })?;
    macro_rules! build_float {
        ($ty:ty, $array:ty) => {{
            let mut builder = arrow_array::builder::PrimitiveBuilder::<$ty>::with_capacity(total);
            let mut offsets = Vec::new();
            if width.is_none() { offsets.push(0); }
            for value in values.iter().map(Borrow::borrow) {
                match value {
                    Value::Null | Value::Undefined => builder.append_nulls(width.unwrap_or(0) as usize),
                    Value::List(list) | Value::FixedSizeList(list) => {
                        let Some(array) = list.array().and_then(|array| array.as_any().downcast_ref::<$array>()) else {
                            return Ok(None);
                        };
                        if width.is_some_and(|width| array.len() != width as usize) { return Ok(None); }
                        if array.null_count() == 0 {
                            builder.append_slice(array.values().as_ref());
                        } else {
                            builder.extend(array.iter());
                        }
                    }
                    _ => return Ok(None),
                }
                if width.is_none() {
                    offsets.push(i32::try_from(arrow_array::builder::ArrayBuilder::len(&builder))
                        .map_err(|_| Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets"))?);
                }
            }
            let child = Arc::new(builder.finish());
            let array: ArrayRef = match width {
                Some(width) => Arc::new(FixedSizeListArray::try_new(Arc::clone(item), width, child, validity(values)).map_err(super::arrow_error)?),
                None => Arc::new(ListArray::try_new(Arc::clone(item), OffsetBuffer::new(ScalarBuffer::from(offsets)), child, validity(values)).map_err(super::arrow_error)?),
            };
            Ok(Some(array))
        }};
    }
    match item.data_type() {
        DataType::Float32 => build_float!(arrow_array::types::Float32Type, Float32Array),
        DataType::Float64 => build_float!(arrow_array::types::Float64Type, Float64Array),
        _ => Ok(None),
    }
}

fn build_packed_lists(item: &Arc<Field>, width: Option<i32>, values: &[impl Borrow<Value>], check_precision: bool) -> Result<Option<ArrayRef>> {
    if width.is_some_and(|width| width <= 0) { return Ok(None); }
    let padding = width.map(|width| arrow_array::new_null_array(item.data_type(), width as usize));
    let mut children = Vec::with_capacity(values.len());
    let mut offsets = vec![0];
    let mut length = 0usize;
    for value in values.iter().map(Borrow::borrow) {
        let array = match value {
            Value::Null | Value::Undefined => padding.as_ref().cloned(),
            Value::List(list) | Value::FixedSizeList(list) => {
                if width.is_some_and(|width| list.len() != width as usize) { return Ok(None); }
                let (source_field, source): (Field, ArrayRef) = match list.view() {
                    ListView::Numeric(values) => (values.field().as_ref().clone(), Arc::clone(values.array())),
                    ListView::F32(array) => (Field::new(item.name(), DataType::Float32, array.null_count() != 0), Arc::new(array.clone())),
                    ListView::F64(array) => (Field::new(item.name(), DataType::Float64, array.null_count() != 0), Arc::new(array.clone())),
                    ListView::Union(array) => (Field::new(item.name(), array.data_type().clone(), false), Arc::new(array.clone())),
                    ListView::Scalars(values) => (values.field().as_ref().clone(), Arc::clone(values.array())),
                    ListView::Values(_) if list.is_empty() => (item.as_ref().clone(), arrow_array::new_empty_array(item.data_type())),
                    ListView::Values(_) => return Ok(None),
                };
                let array = if source.is_empty() { arrow_array::new_empty_array(item.data_type()) } else {
                    let Ok((_, array)) = crate::relayout_arrow_column(source, &source_field, item) else { return Ok(None); };
                    if array.data_type() != item.data_type() { return Ok(None); }
                    array
                };
                if check_precision {
                    crate::validate_projected_decimals(item, array.as_ref())?;
                }
                Some(array)
            }
            _ => return Ok(None),
        };
        if let Some(array) = array {
            length = length.checked_add(array.len()).ok_or_else(layout_error)?;
            children.push(array);
        }
        if width.is_none() {
            offsets.push(i32::try_from(length).map_err(|_| Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets"))?);
        }
    }
    let child = match children.as_slice() {
        [] => arrow_array::new_empty_array(item.data_type()),
        [array] => Arc::clone(array),
        arrays => {
            let arrays = arrays.iter().map(|array| array.as_ref()).collect::<Vec<_>>();
            arrow_select::concat::concat(&arrays).map_err(super::arrow_error)?
        }
    };
    let data_type = match width {
        Some(width) => DataType::FixedSizeList(Arc::clone(item), width),
        None => DataType::List(Arc::clone(item)),
    };
    let mut data = arrow_data::ArrayData::builder(data_type).len(values.len())
        .nulls(validity(values)).add_child_data(child.to_data());
    if width.is_none() { data = data.add_buffer(ScalarBuffer::from(offsets).into_inner()); }
    Ok(Some(make_array(data.build().map_err(super::arrow_error)?)))
}

fn build_union(logical: &LogicalType, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    let field = to_field("", logical)?;
    let DataType::Union(fields, UnionMode::Dense) = field.data_type() else { return build(&from_field(&field)?, values, check_precision); };
    let legs = fields.iter().map(|(_, field)| from_field(field)).collect::<Result<Vec<_>>>()?;
    let ids = fields.iter().map(|(id, _)| id).collect::<Vec<_>>();
    let admissions = legs.iter().map(Admission::new).collect::<Vec<_>>();
    let mut children: Vec<Vec<&Value>> = legs.iter().map(|_| Vec::new()).collect();
    let mut type_ids = Vec::with_capacity(values.len());
    let mut offsets = Vec::with_capacity(values.len());
    for value in values.iter().map(Borrow::borrow) {
        let leg = if matches!(value, Value::Undefined) { 0 } else {
            admissions.iter().position(|leg| leg.accepts(value)).ok_or_else(|| Anomaly::incorrect("rtdb/union-value", "Value has no matching union leg"))?
        };
        type_ids.push(ids[leg]);
        offsets.push(i32::try_from(children[leg].len()).map_err(|_| Anomaly::incorrect("rtdb/union-offset-overflow", "Union child exceeds i32 offsets"))?);
        children[leg].push(value);
    }
    let children = legs.iter().zip(children).map(|(leg, values)| build(leg, &values, check_precision)).collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(UnionArray::try_new(fields.clone(), ScalarBuffer::from(type_ids), Some(ScalarBuffer::from(offsets)), children).map_err(super::arrow_error)?))
}

fn build_runs(logical: &LogicalType, width: RunEndWidth, value_type: &LogicalType, values: &[impl Borrow<Value>], check_precision: bool) -> Result<ArrayRef> {
    let mut distinct = Vec::new();
    let mut ends = Vec::new();
    for (index, value) in values.iter().map(Borrow::borrow).enumerate() {
        if values.get(index + 1).is_none_or(|next| !same_representation(value, next.borrow())) {
            distinct.push(value);
            ends.push(index + 1);
        }
    }
    let array = build(value_type, &distinct, check_precision)?;
    pack_runs(to_field("", logical)?.data_type(), width, ends, array.as_ref())
}

/// Encodes the selected values of consecutive runs with the event writer's run-end layout.
pub fn pack_runs(data_type: &DataType, width: RunEndWidth, ends: Vec<usize>, values: &dyn Array) -> Result<ArrayRef> {
    let runs: ArrayRef = match width {
        RunEndWidth::I16 => {
            let ends = ends.into_iter().map(i16::try_from).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| run_overflow())?;
            Arc::new(RunArray::<Int16Type>::try_new(&Int16Array::from(ends), values).map_err(super::arrow_error)?)
        }
        RunEndWidth::I32 => {
            let ends = ends.into_iter().map(i32::try_from).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| run_overflow())?;
            Arc::new(RunArray::<Int32Type>::try_new(&Int32Array::from(ends), values).map_err(super::arrow_error)?)
        }
        RunEndWidth::I64 => {
            let ends = ends.into_iter().map(i64::try_from).collect::<std::result::Result<Vec<_>, _>>().map_err(|_| run_overflow())?;
            Arc::new(RunArray::<Int64Type>::try_new(&Int64Array::from(ends), values).map_err(super::arrow_error)?)
        }
    };
    let data = runs.to_data().into_builder().data_type(data_type.clone()).build().map_err(super::arrow_error)?;
    Ok(make_array(data))
}

fn same_representation(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::F32(a), Value::F32(b)) => a.to_bits() == b.to_bits(),
        (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
        (Value::Set(a), Value::Set(b)) => a.len() == b.len() && a.iter().zip(b).all(|(a, b)| same_representation(a, b)),
        (Value::List(a), Value::List(b)) | (Value::FixedSizeList(a), Value::FixedSizeList(b)) => same_list_representation(a.view(), b.view()),
        (Value::Struct(a), Value::Struct(b)) => a.len() == b.len() && a.iter().zip(b).all(|((ak, av), (bk, bv))| ak == bk && same_representation(av, bv)),
        (Value::Map { entries: a, sorted: sa }, Value::Map { entries: b, sorted: sb }) => sa == sb && a.len() == b.len() && a.iter().zip(b).all(|((ak, av), (bk, bv))| same_representation(ak, bk) && same_representation(av, bv)),
        _ => left == right,
    }
}

fn same_list_representation(left: ListView<'_>, right: ListView<'_>) -> bool {
    if left.len() != right.len() { return false; }
    match (left, right) {
        (ListView::F32(left), ListView::F32(right)) => left.to_data() == right.to_data(),
        (ListView::F64(left), ListView::F64(right)) => left.to_data() == right.to_data(),
        (ListView::Numeric(left), ListView::Numeric(right))
            if !matches!(left.array().data_type(), DataType::Union(..))
                && left.array().data_type() == right.array().data_type()
                && left.field().metadata() == right.field().metadata() =>
            left.array().to_data() == right.array().to_data(),
        _ if left.is_packed_numeric() && right.is_packed_numeric() =>
            (0..left.len()).all(|index| match (left.numeric_component(index), right.numeric_component(index)) {
                (rtdb_types::NumericCell::F32(left), rtdb_types::NumericCell::F32(right)) => left.to_bits() == right.to_bits(),
                (rtdb_types::NumericCell::F64(left), rtdb_types::NumericCell::F64(right)) => left.to_bits() == right.to_bits(),
                (left, right) => left == right,
            }),
        _ => left.iter().zip(right.iter()).all(|(left, right)| same_representation(left.as_ref(), right.as_ref())),
    }
}

/// Reads a logical cell, expanding unions and run-end layouts by the declared field.
/// Reading bottom is a fault; slices retain their physical offset through Arrow's accessors.
pub fn read_value(field: &Field, array: &dyn Array, row: usize) -> Result<Value> {
    read_typed(&from_field(field)?, field, array, row)
}

/// Reads the cells of one column, deriving its logical type once rather than per cell as `read_value` does.
pub struct ColumnReader<'a> { field: &'a Field, logical: LogicalType }

impl<'a> ColumnReader<'a> {
    pub fn new(field: &'a Field) -> Result<Self> { Ok(Self { logical: from_field(field)?, field }) }
    pub fn read(&self, array: &dyn Array, row: usize) -> Result<Value> { read_typed(&self.logical, self.field, array, row) }
    pub fn logical(&self) -> &LogicalType { &self.logical }

    pub fn read_owned_entity_id<'b>(&self, array: &'b dyn Array, row: usize) -> Result<Option<EntityIdRef<'b>>> {
        read_selected(&self.logical, self.field, array, row, read_id::<false, true>)
    }

    /// Reads a raw entity ID, returning absence for a null cell.
    pub fn read_entity_id<'b>(&self, array: &'b dyn Array, row: usize) -> Result<Option<EntityIdRef<'b>>> {
        read_selected(&self.logical, self.field, array, row, read_id::<false, false>)
    }
}

/// Borrows an entity ID with the same signed-integer interpretation as an execution value.
pub fn read_execution_entity_id_as<'a>(
    logical: Option<&LogicalType>, field: &Field, array: &'a dyn Array, row: usize,
) -> Result<Option<EntityIdRef<'a>>> {
    let derived;
    let logical = match logical {
        Some(logical) => logical,
        None => { derived = from_field(field)?; &derived }
    };
    read_selected(logical, field, array, row, read_id::<true, false>)
}

pub fn read_execution_owned_entity_id<'a>(field: &Field, array: &'a dyn Array, row: usize) -> Result<Option<EntityIdRef<'a>>> {
    read_selected(&from_field(field)?, field, array, row, read_id::<true, true>)
}

/// `read_value` for a caller that has derived `logical` from `field` once, for many cells of the column.
pub fn read_value_as(logical: &LogicalType, field: &Field, array: &dyn Array, row: usize) -> Result<Value> { read_typed(logical, field, array, row) }

pub(crate) fn union_read_type<'a>(logical: &'a LogicalType, child: &Field) -> Result<Cow<'a, LogicalType>> {
    if let LogicalType::Union(legs) = logical
        && let Some(leg) = legs.iter().find(|leg| crate::schema::leg_name(leg).is_ok_and(|name| name == *child.name())) {
        return Ok(Cow::Borrowed(leg));
    }
    from_field(child).map(Cow::Owned)
}

/// `read_value` with the field's logical type already derived, so nested cells reuse their parent's types.
fn read_typed(logical: &LogicalType, field: &Field, array: &dyn Array, row: usize) -> Result<Value> {
    read_selected(logical, field, array, row, read)
}

fn read_selected<'a, T>(
    logical: &LogicalType, field: &Field, array: &'a dyn Array, row: usize,
    read_leaf: impl Fn(&LogicalType, &Field, &'a dyn Array, usize) -> Result<T> + Copy,
) -> Result<T> {
    if row >= array.len() { return Err(Anomaly::incorrect("rtdb/row-index", "Cell index exceeds array length")); }
    if field.data_type() != array.data_type() { return Err(layout_error()); }
    if matches!(logical, LogicalType::Nothing) { return Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read")); }
    if let DataType::RunEndEncoded(ends, values) = field.data_type() {
        let derived;
        let value = match logical { LogicalType::RunEndEncoded { value, .. } => value.as_ref(), _ => { derived = from_field(values)?; &derived } };
        fn read_run<'a, R: arrow_array::types::RunEndIndexType, T>(
            logical: &LogicalType, field: &Field, array: &'a dyn Array, row: usize,
            read_leaf: impl Fn(&LogicalType, &Field, &'a dyn Array, usize) -> Result<T> + Copy,
        ) -> Result<T> {
            let runs = downcast::<RunArray<R>>(array)?;
            read_selected(logical, field, runs.values().as_ref(), runs.get_physical_index(row), read_leaf)
        }
        return match ends.data_type() {
            DataType::Int16 => read_run::<Int16Type, _>(value, values, array, row, read_leaf),
            DataType::Int32 => read_run::<Int32Type, _>(value, values, array, row, read_leaf),
            DataType::Int64 => read_run::<Int64Type, _>(value, values, array, row, read_leaf),
            _ => Err(layout_error()),
        };
    }
    if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
        let union = downcast::<UnionArray>(array)?;
        let id = union.type_id(row);
        let child = fields.iter().find_map(|(child_id, field)| (child_id == id).then_some(field)).ok_or_else(layout_error)?;
        let leg = union_read_type(logical, child)?;
        return read_selected(&leg, child, union.child(id).as_ref(), union.value_offset(row), read_leaf);
    }
    if let LogicalType::Maybe(inner) = logical
        && array.is_valid(row)
    {
        return read_selected(inner, field, array, row, read_leaf);
    }
    read_leaf(logical, field, array, row)
}

fn read_id<'a, const EXECUTION: bool, const OWNED: bool>(
    logical: &LogicalType, field: &Field, array: &'a dyn Array, row: usize,
) -> Result<Option<EntityIdRef<'a>>> {
    if array.is_null(row) || *logical == LogicalType::Null { return Ok(None); }
    if let Some(id) = borrow_id_scalar(logical, array, row, EXECUTION, OWNED) { return Ok(Some(id)); }
    match logical {
        LogicalType::Scalar(ScalarType::Binary) => { downcast::<BinaryArray>(array)?; }
        LogicalType::Scalar(ScalarType::FixedSizeBinary(_)) => { downcast::<FixedSizeBinaryArray>(array)?; }
        // A non-ID cell can still fail decoding before ID validation (for example, a malformed URI).
        _ => {
            let mut value = read(logical, field, array, row)?;
            if EXECUTION { crate::admit_execution_value(&mut value)?; }
            value.entity_id()?;
            return Err(layout_error());
        }
    }
    Err(crate::entity_id::invalid())
}

fn read(logical: &LogicalType, field: &Field, array: &dyn Array, row: usize) -> Result<Value> {
    if array.is_null(row) { return Ok(Value::Null); }
    match logical {
        LogicalType::Null => Ok(Value::Null),
        LogicalType::Scalar(scalar) => read_scalar(scalar, array, row),
        LogicalType::Struct(children) => {
            let array = downcast::<StructArray>(array)?;
            let DataType::Struct(fields) = field.data_type() else { return Err(layout_error()); };
            let mut values = StructFields::new();
            for (i, child) in fields.iter().enumerate() {
                let child_type = children.get(child.name()).ok_or_else(layout_error)?;
                let value = if matches!(child_type, LogicalType::Nothing) { Value::Undefined }
                    else { read_typed(child_type, child, array.column(i).as_ref(), row)? };
                values.insert(child.name().clone(), value);
            }
            Ok(Value::Struct(values))
        }
        LogicalType::List { kind, element } => read_list(kind, element, field, array, row),
        _ => Err(layout_error()),
    }
}

macro_rules! cell {
    ($array:expr, $row:expr, $array_type:ty, $value:ident) => { Ok(Value::$value(downcast::<$array_type>($array)?.value($row))) };
}

fn read_scalar(scalar: &ScalarType, array: &dyn Array, row: usize) -> Result<Value> {
    use ScalarType::*;
    match scalar {
        Bool => cell!(array, row, BooleanArray, Bool),
        I8 => cell!(array, row, Int8Array, I8), I16 => cell!(array, row, Int16Array, I16),
        I32 => cell!(array, row, Int32Array, I32), I64 => cell!(array, row, Int64Array, I64),
        U8 => cell!(array, row, UInt8Array, U8), U16 => cell!(array, row, UInt16Array, U16),
        U32 => cell!(array, row, UInt32Array, U32), U64 => cell!(array, row, UInt64Array, U64),
        F16 => Ok(Value::F16(downcast::<Float16Array>(array)?.value(row).to_bits())),
        F32 => cell!(array, row, Float32Array, F32), F64 => cell!(array, row, Float64Array, F64),
        DateDay => cell!(array, row, Date32Array, DateDay), DateMilli => cell!(array, row, Date64Array, DateMilli),
        Oid => cell!(array, row, Int32Array, Oid), RegClass => cell!(array, row, Int32Array, RegClass), RegProc => cell!(array, row, Int32Array, RegProc),
        Utf8 => Ok(Value::Utf8(downcast::<StringArray>(array)?.value(row).to_owned())),
        Keyword => Ok(Value::Keyword(rtdb_types::Keyword::from_name(downcast::<StringArray>(array)?.value(row)))),
        Uri => { let text = downcast::<StringArray>(array)?.value(row); rtdb_types::UriParts::parse(text)?; Ok(Value::Uri(text.to_owned())) },
        Binary => Ok(Value::Binary(downcast::<BinaryArray>(array)?.value(row).to_vec())),
        Transit => Ok(Value::Transit(downcast::<BinaryArray>(array)?.value(row).to_vec())),
        FixedSizeBinary(_) => Ok(Value::FixedSizeBinary(downcast::<FixedSizeBinaryArray>(array)?.value(row).to_vec())),
        Uuid => {
            let bytes = downcast::<FixedSizeBinaryArray>(array)?.value(row);
            Ok(Value::Uuid(bytes.try_into().map_err(|_| layout_error())?))
        }
        Decimal { precision, scale, bit_width } => {
            let unscaled = match bit_width {
                32 => downcast::<Decimal32Array>(array)?.value(row).to_be_bytes().to_vec(),
                64 => downcast::<Decimal64Array>(array)?.value(row).to_be_bytes().to_vec(),
                128 => downcast::<Decimal128Array>(array)?.value(row).to_be_bytes().to_vec(),
                256 => downcast::<Decimal256Array>(array)?.value(row).to_be_bytes().to_vec(),
                _ => return Err(decimal_width()),
            };
            Ok(Value::Decimal(DecimalValue { unscaled, precision: *precision, scale: *scale, bit_width: *bit_width }))
        }
        Time { unit, bit_width } => {
            let value = match unit {
                TimeUnit::Second => i64::from(downcast::<Time32SecondArray>(array)?.value(row)),
                TimeUnit::Millisecond => i64::from(downcast::<Time32MillisecondArray>(array)?.value(row)),
                TimeUnit::Microsecond => downcast::<Time64MicrosecondArray>(array)?.value(row),
                TimeUnit::Nanosecond => downcast::<Time64NanosecondArray>(array)?.value(row),
            };
            Ok(Value::Time { value, unit: *unit, bit_width: *bit_width })
        }
        Timestamp { unit, timezone } => {
            let value = match unit {
                TimeUnit::Second => downcast::<TimestampSecondArray>(array)?.value(row),
                TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?.value(row),
                TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?.value(row),
                TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array)?.value(row),
            };
            Ok(Value::Timestamp { value, unit: *unit, timezone: timezone.clone() })
        }
        Duration(unit) => {
            let value = match unit {
                TimeUnit::Second => downcast::<DurationSecondArray>(array)?.value(row),
                TimeUnit::Millisecond => downcast::<DurationMillisecondArray>(array)?.value(row),
                TimeUnit::Microsecond => downcast::<DurationMicrosecondArray>(array)?.value(row),
                TimeUnit::Nanosecond => downcast::<DurationNanosecondArray>(array)?.value(row),
            };
            Ok(Value::Duration { value, unit: *unit })
        }
        Interval(IntervalUnit::YearMonth) => cell!(array, row, IntervalYearMonthArray, IntervalYearMonth),
        Interval(IntervalUnit::DayTime) => {
            let value = downcast::<IntervalDayTimeArray>(array)?.value(row);
            Ok(Value::IntervalDayTime { days: value.days, milliseconds: value.milliseconds })
        }
        Interval(IntervalUnit::MonthDayNano) => {
            let value = downcast::<IntervalMonthDayNanoArray>(array)?.value(row);
            Ok(Value::IntervalMonthDayNano { months: value.months, days: value.days, nanoseconds: value.nanoseconds })
        }
        IntervalMonthDayMicro => {
            let value = downcast::<IntervalMonthDayNanoArray>(array)?.value(row);
            Ok(Value::IntervalMonthDayMicro { months: value.months, days: value.days, nanoseconds: value.nanoseconds })
        }
    }
}

fn read_list(kind: &ListKind, element: &LogicalType, field: &Field, array: &dyn Array, row: usize) -> Result<Value> {
    let (child_field, values) = match field.data_type() {
        DataType::List(child) => (child, downcast::<ListArray>(array)?.value(row)),
        DataType::FixedSizeList(child, _) => (child, downcast::<FixedSizeListArray>(array)?.value(row)),
        DataType::Map(child, _) => (child, Arc::new(downcast::<MapArray>(array)?.value(row)) as ArrayRef),
        _ => return Err(layout_error()),
    };
    if matches!(kind, ListKind::List | ListKind::FixedSizeList(_)) && child_field.metadata().is_empty() {
        let ty = match element { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
        let admit = |list| match kind { ListKind::List => Value::List(list), _ => Value::FixedSizeList(list) };
        if matches!(ty, LogicalType::Scalar(ScalarType::F32)) && let Some(array) = values.as_any().downcast_ref::<Float32Array>() {
            return Ok(admit(ListValue::from_f32(copy_float_components(array))));
        }
        if matches!(ty, LogicalType::Scalar(ScalarType::F64)) && let Some(array) = values.as_any().downcast_ref::<Float64Array>() {
            return Ok(admit(ListValue::from_f64(copy_float_components(array))));
        }
    }
    if matches!(kind, ListKind::List | ListKind::FixedSizeList(_))
        && ListValue::supports_numeric_arrow(child_field)
        && crate::schema::numeric_field_matches(child_field, element)?
    {
        let copied = copy_numeric_components(values.as_ref())?;
        let packed = ListValue::from_numeric_arrow(Arc::clone(child_field), copied).ok_or_else(layout_error)?;
        return Ok(if matches!(kind, ListKind::List) { Value::List(packed) } else { Value::FixedSizeList(packed) });
    }
    if matches!(kind, ListKind::List | ListKind::FixedSizeList(_))
        && let Some(span) = numeric_list_span(child_field, values.as_ref(), 0..values.len(), element)
        && let Some(list) = repack_numeric_list(&span, false)
    {
        return Ok(if matches!(kind, ListKind::List) { Value::List(list) } else { Value::FixedSizeList(list) });
    }
    if matches!(kind, ListKind::List | ListKind::FixedSizeList(_))
        && from_field(child_field).ok().as_ref() == Some(element)
        && numeric_aliases_agree(child_field, values.as_ref(), 0..values.len(), element) == Some(true)
        && let Some(list) = repack_scalar_list(child_field, values.as_ref(), 0..values.len(), false, || Ok(()))?
    {
        return Ok(if matches!(kind, ListKind::List) { Value::List(list) } else { Value::FixedSizeList(list) });
    }
    let values = (0..values.len()).map(|index| read_typed(element, child_field, values.as_ref(), index)).collect::<Result<Vec<_>>>()?;
    match kind {
        ListKind::List => Ok(Value::list(values)),
        ListKind::Set => Ok(Value::Set(values)),
        ListKind::FixedSizeList(_) => Ok(Value::FixedSizeList(values.into())),
        ListKind::TimestampTzRange => {
            if let [Value::Timestamp { value: lower, .. }, Value::Timestamp { value: upper, .. }] = values.as_slice() {
                Ok(Value::TimestampTzRange { lower: *lower, upper: *upper })
            } else { Err(Anomaly::incorrect("rtdb/timestamp-range", "Timestamp range requires two non-null timestamps")) }
        }
        ListKind::Map { sorted } => {
            let entries = values.into_iter().map(|entry| match entry {
                Value::Struct(mut fields) => {
                    let key = fields.remove("key").ok_or_else(layout_error)?;
                    let value = fields.remove("value").ok_or_else(layout_error)?;
                    Ok((key, value))
                }
                _ => Err(layout_error()),
            }).collect::<Result<Vec<_>>>()?;
            Ok(Value::Map { entries, sorted: *sorted })
        }
    }
}

pub(crate) fn numeric_list_span<'a>(
    field: &'a Field, array: &'a dyn Array, range: std::ops::Range<usize>, element: &LogicalType,
) -> Option<rtdb_types::NumericSpan<'a>> {
    if &from_field(field).ok()? != element { return None; }
    if !numeric_aliases_agree(field, array, range.clone(), element)? { return None; }
    rtdb_types::NumericSpan::selected(field, array, range)
}

fn numeric_aliases_agree(
    field: &Field, array: &dyn Array, range: std::ops::Range<usize>, element: &LogicalType,
) -> Option<bool> {
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_any().downcast_ref::<UnionArray>()?;
            for (tag, child) in fields.iter() {
                let actual = from_field(child).ok()?;
                let reinterpreted = matches!(element, LogicalType::Union(legs) if legs.iter().any(|leg|
                    leg_name(leg).is_ok_and(|name| &name == child.name()) && !actual.legs().contains(leg)));
                let nested = matches!(child.data_type(), DataType::Union(..));
                if !reinterpreted && !nested { continue; }
                let values = union.child(tag).as_ref();
                for i in range.clone().filter(|&i| union.type_id(i) == tag) {
                    let offset = union.value_offset(i);
                    if nested {
                        if reinterpreted || !numeric_aliases_agree(child, values, offset..offset + 1, &actual)? { return Some(false); }
                    } else if !values.is_null(offset) {
                        return Some(false);
                    }
                }
            }
            Some(true)
        }
        DataType::RunEndEncoded(ends, child) if matches!(child.data_type(), DataType::Union(..)) => {
            let LogicalType::RunEndEncoded { value, .. } = element else { return None; };
            macro_rules! run {
                ($ty:ty) => {{
                    let runs = array.as_any().downcast_ref::<RunArray<$ty>>()?;
                    if range.is_empty() { return Some(true); }
                    let start = runs.get_physical_index(range.start);
                    let end = runs.get_physical_index(range.end - 1);
                    numeric_aliases_agree(child, runs.values().as_ref(), start..end + 1, value)
                }};
            }
            match ends.data_type() {
                DataType::Int16 => run!(Int16Type),
                DataType::Int32 => run!(Int32Type),
                DataType::Int64 => run!(Int64Type),
                _ => None,
            }
        }
        _ => Some(true),
    }
}

pub fn repack_numeric_list(span: &rtdb_types::NumericSpan<'_>, signed: bool) -> Option<ListValue> {
    let mut builder = ListBuilder::with_capacity(span.len());
    for value in span.iter() {
        if !builder.try_push_numeric(if signed { value.signed() } else { value }) { return None; }
    }
    Some(builder.finish())
}

pub fn repack_scalar_list(
    field: &Field, array: &dyn Array, range: std::ops::Range<usize>, signed: bool, reserve: impl FnOnce() -> Result<()>,
) -> Result<Option<ListValue>> {
    use rtdb_types::ScalarList;
    if field.data_type() != array.data_type() || range.start > range.end || range.end > array.len()
        || (ScalarList::supported_type(field).is_none()
            && !matches!(field.data_type(), DataType::Union(_, UnionMode::Dense) | DataType::RunEndEncoded(..)))
    {
        return Ok(None);
    }
    let mut representative: Option<&Field> = None;
    for row in range.clone() {
        let cell = (crate::ArrowCell { field, array, row }).physical()?;
        if cell.is_undefined()? { return Ok(None); }
        if cell.is_null()? { continue; }
        if let Some(first) = representative {
            if first.data_type() != cell.field.data_type()
                || first.metadata() != cell.field.metadata() && ScalarList::supported_type(first) != ScalarList::supported_type(cell.field)
            {
                return repack_mixed_scalar_list(field, array, range, signed, reserve);
            }
        } else {
            if ScalarList::supported_type(cell.field).is_none() {
                return repack_mixed_scalar_list(field, array, range, signed, reserve);
            }
            representative = Some(cell.field);
        }
    }
    let Some(representative) = representative else { return Ok(None); };
    reserve()?;
    let mut inputs = Vec::<&dyn Array>::new();
    let mut cells = Vec::with_capacity(range.len());
    for row in range {
        let cell = (crate::ArrowCell { field, array, row }).physical()?;
        let slot = if cell.is_null()? { None } else {
            let source = inputs.iter().position(|&source| std::ptr::eq(source, cell.array)).unwrap_or_else(|| {
                inputs.push(cell.array);
                inputs.len() - 1
            });
            Some((source, cell.row))
        };
        cells.push(slot);
    }
    let copied = crate::copy_nullable_cells(&inputs, &cells)?;
    let output = to_field("value", &LogicalType::Scalar(ScalarList::supported_type(representative).unwrap()))?;
    Ok(ListValue::from_scalar_arrow(Arc::new(output.with_nullable(copied.null_count() != 0)), copied))
}

struct ScalarListLeg<'a> {
    ty: LogicalType,
    inputs: Vec<(&'a dyn Array, Option<ArrayRef>)>,
    picks: Vec<(usize, usize)>,
}

fn repack_mixed_scalar_list(
    field: &Field, array: &dyn Array, range: std::ops::Range<usize>, signed: bool, reserve: impl FnOnce() -> Result<()>,
) -> Result<Option<ListValue>> {
    use rtdb_types::ScalarList;
    for row in range.clone() {
        let cell = (crate::ArrowCell { field, array, row }).physical()?;
        if cell.is_undefined()? || !cell.is_null()? && ScalarList::leaf_type(cell.field).is_none() { return Ok(None); }
    }
    reserve()?;
    let mut legs = Vec::<ScalarListLeg<'_>>::new();
    let mut rows = Vec::with_capacity(range.len());
    let mut nulls = 0;
    for row in range {
        let cell = (crate::ArrowCell { field, array, row }).physical()?;
        let raw_type = if cell.is_null()? { LogicalType::Null }
            else { ScalarList::leaf_type(cell.field).expect("validated selected scalar") };
        let ty = if signed { crate::execution_type(&raw_type) } else { raw_type.clone() };
        let tag = legs.iter().position(|leg| leg.ty == ty).unwrap_or_else(|| {
            legs.push(ScalarListLeg { ty: ty.clone(), inputs: Vec::new(), picks: Vec::new() });
            legs.len() - 1
        });
        let leg = &mut legs[tag];
        if ty == LogicalType::Null {
            rows.push((tag, nulls));
            nulls += 1;
            continue;
        }
        let source = if let Some(source) = leg.inputs.iter().position(|(input, _)| std::ptr::eq(*input, cell.array)) {
            source
        } else {
            let converted = if ty != raw_type {
                let source = Arc::new(to_field("value", &raw_type)?);
                let mut list = ListValue::from_numeric_arrow(source, make_array(cell.array.to_data())).ok_or_else(layout_error)?;
                list.admit_signed();
                Some(list.packed_column().expect("signed numeric child").1)
            } else { None };
            leg.inputs.push((cell.array, converted));
            leg.inputs.len() - 1
        };
        rows.push((tag, leg.picks.len()));
        leg.picks.push((source, cell.row));
    }
    let logical = LogicalType::join_all(legs.iter().map(|leg| &leg.ty));
    let output = Arc::new(to_field("value", &logical)?);
    let DataType::Union(fields, _) = output.data_type() else {
        let Some(leg) = legs.iter().find(|leg| leg.ty != LogicalType::Null) else { return Ok(None); };
        let inputs = leg.inputs.iter().map(|(input, converted)| converted.as_deref().unwrap_or(*input)).collect::<Vec<_>>();
        let picks = rows.into_iter().map(|(tag, offset)| (legs[tag].ty != LogicalType::Null).then(|| leg.picks[offset]))
            .collect::<Vec<_>>();
        let array = crate::copy_nullable_cells(&inputs, &picks)?;
        return Ok(if ListValue::supports_numeric_arrow(&output) {
            ListValue::from_numeric_arrow(output, array)
        } else {
            ListValue::from_scalar_arrow(output, array)
        });
    };
    let tags = rows.iter().map(|(tag, _)| i8::try_from(*tag).map_err(|_| layout_error())).collect::<Result<Vec<_>>>()?;
    let offsets = rows.iter().map(|(_, offset)| i32::try_from(*offset).map_err(|_| layout_error())).collect::<Result<Vec<_>>>()?;
    let children = legs.iter().map(|leg| {
        if leg.ty == LogicalType::Null { return Ok(Arc::new(NullArray::new(nulls)) as ArrayRef); }
        let inputs = leg.inputs.iter().map(|(input, converted)| converted.as_deref().unwrap_or(*input)).collect::<Vec<_>>();
        crate::copy_selected_cells(&inputs, &leg.picks)
    }).collect::<Result<Vec<_>>>()?;
    let array = UnionArray::try_new(fields.clone(), tags.into(), Some(offsets.into()), children).map_err(super::arrow_error)?;
    Ok(ListValue::from_scalar_arrow(output, Arc::new(array)))
}

fn copy_float_components<T: arrow_array::types::ArrowPrimitiveType>(
    array: &arrow_array::PrimitiveArray<T>,
) -> arrow_array::PrimitiveArray<T> {
    // Owned reads detach from the batch reservation and preserve the zero payload of null float slots.
    let mut values = array.values().to_vec();
    let nulls = array.nulls().filter(|nulls| nulls.null_count() != 0).map(|nulls| {
        let mut end = 0;
        for (start, next_end) in nulls.valid_slices() {
            values[end..start].fill(T::Native::default());
            end = next_end;
        }
        values[end..].fill(T::Native::default());
        let mut bitmap = BooleanBufferBuilder::new(array.len());
        bitmap.append_buffer(nulls.inner());
        NullBuffer::new(bitmap.finish())
    });
    arrow_array::PrimitiveArray::new(values.into(), nulls)
}

fn copy_numeric_components(values: &dyn Array) -> Result<ArrayRef> {
    let length = values.len();
    i32::try_from(length).map_err(|_| layout_error())?;
    let data = values.to_data();
    let DataType::Union(fields, UnionMode::Dense) = values.data_type() else {
        let mut copy = MutableArrayData::new(vec![&data], false, length);
        copy.extend(0, 0, length);
        return Ok(make_array(copy.freeze()));
    };

    let union = downcast::<UnionArray>(values)?;
    let mut lengths = [0_i32; 128];
    let mut offsets = Vec::with_capacity(length);
    for &tag in union.type_ids().iter() {
        let count = &mut lengths[tag as usize];
        offsets.push(*count);
        *count += 1;
    }
    let mut positions = [0; 128];
    let mut children = fields.iter().zip(data.child_data()).enumerate().map(|(position, ((tag, _), child))| {
        positions[tag as usize] = position;
        MutableArrayData::new(vec![child], false, lengths[tag as usize] as usize)
    }).collect::<Vec<_>>();
    let mut pending = [(0, 0); 128];
    for (&tag, &offset) in union.type_ids().iter().zip(union.offsets().ok_or_else(layout_error)?.iter()) {
        let (start, end) = &mut pending[tag as usize];
        let offset = offset as usize;
        if offset != *end {
            if start != end { children[positions[tag as usize]].extend(0, *start, *end); }
            *start = offset;
        }
        *end = offset + 1;
    }
    let children = fields.iter().zip(children).map(|((tag, _), mut child)| {
        let (start, end) = pending[tag as usize];
        if start != end { child.extend(0, start, end); }
        make_array(child.freeze())
    }).collect();
    let tags = ScalarBuffer::from(union.type_ids().to_vec());
    let copied = UnionArray::try_new(fields.clone(), tags, Some(offsets.into()), children).map_err(super::arrow_error)?;
    Ok(Arc::new(copied))
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(layout_error) }
fn layout_error() -> Anomaly { Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree") }
fn decimal_width() -> Anomaly { Anomaly::incorrect("rtdb/decimal-width", "Decimal coefficient must fill its declared signed width") }
fn run_overflow() -> Anomaly { Anomaly::incorrect("rtdb/run-end-overflow", "Array length exceeds its run-end index width") }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_expression_owned_float_lists_copy_sliced_values_and_validity() {
        let nan = f32::from_bits(0x7f80_0100);
        let values = Float32Array::new(
            vec![13.0, -0.0, 42.0, nan, 8.0].into(),
            Some(NullBuffer::from(vec![true, true, false, true, true])),
        );
        let item = Arc::new(Field::new("item", DataType::Float32, true));
        let lists = ListArray::try_new(item, OffsetBuffer::new(vec![0, 1, 4, 5].into()), Arc::new(values.clone()), None).unwrap();
        let field = Field::new("lists", lists.data_type().clone(), false);
        let result = ColumnReader::new(&field).unwrap().read(&lists.slice(1, 1), 0).unwrap();
        let Some(ListView::F32(result)) = result.list_view() else { panic!("expected float buffer"); };

        assert_ne!(values.values().inner().data_ptr(), result.values().inner().data_ptr());
        assert_ne!(values.nulls().unwrap().buffer().data_ptr(), result.nulls().unwrap().buffer().data_ptr());
        drop(lists);
        drop(values);
        assert_eq!(result.values().iter().map(|value| value.to_bits()).collect::<Vec<_>>(), [(-0.0_f32).to_bits(), 0, nan.to_bits()]);
        assert_eq!(result.nulls().unwrap().iter().collect::<Vec<_>>(), [true, false, true]);
    }

    #[test]
    fn hot_expression_owned_numeric_union_keeps_compact_layout_and_raw_bits() {
        let fields = arrow_schema::UnionFields::try_new(
            [2, 9],
            [Field::new("u64", DataType::UInt64, true), Field::new("i16", DataType::Int16, false)],
        ).unwrap();
        let unsigned = UInt64Array::new(
            vec![99, u64::MAX, 123, 55].into(),
            Some(NullBuffer::from(vec![true, true, false, true])),
        );
        let union = UnionArray::try_new(
            fields, vec![2, 2, 9, 2, 2, 9].into(), Some(vec![0, 2, 1, 1, 1, 0].into()),
            vec![Arc::new(unsigned.clone()), Arc::new(Int16Array::from(vec![10, 20]))],
        ).unwrap();
        let item = Arc::new(Field::new("item", union.data_type().clone(), true));
        let lists = FixedSizeListArray::try_new(item, 4, Arc::new(union.slice(1, 4)), None).unwrap();
        let field = Field::new("vectors", lists.data_type().clone(), false);
        let decoded = read_value(&field, &lists, 0).unwrap();
        let Some(ListView::Numeric(packed)) = decoded.list_view() else { panic!("expected numeric buffer"); };
        let copied = downcast::<UnionArray>(packed.array().as_ref()).unwrap();
        assert_eq!(copied.type_ids().as_ref(), &[2, 9, 2, 2]);
        assert_eq!(copied.offsets().unwrap().as_ref(), &[0, 0, 1, 2]);
        let child = downcast::<UInt64Array>(copied.child(2).as_ref()).unwrap();
        assert_ne!(unsigned.values().inner().data_ptr(), child.values().inner().data_ptr());
        assert_eq!(child.values().as_ref(), &[123, u64::MAX, u64::MAX]);
        assert!(child.is_null(0));
        assert_eq!(downcast::<Int16Array>(copied.child(9).as_ref()).unwrap().values().as_ref(), &[20]);
        let execution = crate::read_execution_value(&field, &lists, 0).unwrap();
        assert_eq!(execution.list_view().unwrap().get(2).unwrap().as_ref(), &Value::I64(-1));
    }

    fn joined(plain: &LogicalType, value: &Value) -> bool {
        if matches!(value, Value::Undefined) { return matches!(plain, LogicalType::Nothing); }
        let actual = value.logical_type();
        *plain == actual
            || matches!(plain, LogicalType::Maybe(inner) if **inner == actual || actual == LogicalType::Null)
            || plain.join(&actual) == *plain
    }

    #[test]
    fn wave4_run_list_buffers_keep_zero_and_nan_bits() {
        let list_type = LogicalType::list(LogicalType::Scalar(ScalarType::F64));
        let ty = list_type.run_end_encoded(RunEndWidth::I32).unwrap();
        let bits = [0, 0, 1 << 63, 0x7ff8_0000_0000_0001, 0x7ff8_0000_0000_0001, 0x7ff8_0000_0000_0002];
        let values = bits.map(|bits| Value::list(vec![Value::F64(f64::from_bits(bits))]));
        let array = build_array(&ty, &values).unwrap();
        let data = array.to_data();
        assert_eq!(data.child_data()[0].buffers()[0].as_slice(), [2i32, 3, 5, 6].map(i32::to_le_bytes).concat());
        assert_eq!(data.child_data()[1].child_data()[0].buffers()[0].as_slice(),
            [bits[0], bits[2], bits[3], bits[5]].map(u64::to_le_bytes).concat());
    }

    #[test]
    fn wave4_decimal_precision_is_strict_at_signed_coefficient_bounds() {
        for (width, coefficient) in [(32, i32::MIN as i128), (64, i64::MIN as i128), (128, i128::MIN)] {
            let decimal = |coefficient: i128, precision| Value::Decimal(DecimalValue::new(
                coefficient.to_be_bytes()[16 - width / 8..].to_vec(), precision, 0, width as u16,
            ).unwrap());
            for value in [decimal(-99, 2), decimal(99, 2), decimal(coefficient, 77)] {
                let field = to_field("value", &value.logical_type()).unwrap();
                let array = build_array(&value.logical_type(), &[value.clone()]).unwrap();
                assert_eq!(read_value(&field, array.as_ref(), 0).unwrap(), value);
            }
            for value in [decimal(-100, 2), decimal(100, 2), decimal(0, 0)] {
                assert_eq!(build_array(&value.logical_type(), &[value]).unwrap_err().code, "rtdb/decimal-precision");
            }
        }
        let min = Value::Decimal(DecimalValue::new(i256::MIN.to_be_bytes().to_vec(), 77, 0, 256).unwrap());
        let actual = build_array(&min.logical_type(), &[min.clone()]).unwrap().to_data();
        assert_eq!(actual.buffers()[0].as_slice(), i256::MIN.to_le_bytes());
        let too_wide = Value::Decimal(DecimalValue::new(i256::MIN.to_be_bytes().to_vec(), 76, 0, 256).unwrap());
        assert_eq!(build_array(&too_wide.logical_type(), &[too_wide]).unwrap_err().code, "rtdb/decimal-precision");
    }

    #[test]
    fn floating_list_cells_keep_arrow_through_row_storage_and_reassembly() {
        use rtdb_types::{ListValue, ListView};
        let nan = f32::from_bits(0x7fc00001);
        let float = ListValue::from_f32(Float32Array::from(vec![Some(-0.0), None, Some(nan)]));
        for input in [Value::List(float.clone()), Value::FixedSizeList(float.clone())] {
            let logical = input.logical_type().maybe();
            let field = to_field("embedding", &logical).unwrap();
            let column = copy_array(&logical, &[Value::Null, input]).unwrap();
            assert_eq!(read_value(&field, column.as_ref(), 0).unwrap(), Value::Null);
            let decoded = read_value(&field, column.as_ref(), 1).unwrap();
            let Some(ListView::F32(array)) = decoded.list_view() else { panic!("expected Arrow components"); };
            assert_eq!(array.value(0).to_bits(), (-0.0_f32).to_bits());
            assert!(array.is_null(1));
            assert_eq!(array.value(2).to_bits(), nan.to_bits());
            assert_eq!(copy_array(&logical, &[Value::Null, decoded]).unwrap().to_data(), column.to_data());
        }
    }

    #[test]
    fn mixed_float_widths_use_an_arrow_union_through_result_reassembly() {
        let a = Value::FixedSizeList(ListValue::from_f32(Float32Array::from(vec![-0.0_f32, 2.0])));
        let b = Value::FixedSizeList(ListValue::from_f64(Float64Array::from(vec![4.0, 8.0])));
        let logical = a.logical_type().join(&b.logical_type()).maybe();
        let field = to_field("embedding", &logical).unwrap();
        let column = build_array(&logical, &[a.clone(), Value::Null, b.clone()]).unwrap();
        let decoded = (0..3).map(|row| read_value(&field, column.as_ref(), row).unwrap()).collect::<Vec<_>>();
        assert_eq!(decoded, [a, Value::Null, b]);
        assert!(matches!(decoded[0].list_view(), Some(ListView::Union(_))));
        assert!(matches!(decoded[2].list_view(), Some(ListView::Union(_))));
        let copied = copy_array(&logical, &decoded).unwrap();
        assert_eq!(copied.to_data(), column.to_data());
    }

    #[test]
    fn annotated_numeric_children_repack_their_selected_legs_in_observed_order() {
        let label = std::collections::HashMap::from([("label".into(), "sample".into())]);
        let decimal = rtdb_types::decimal_arrow::with_decimal_parameters(
            Field::new("decimal-12--400-128", DataType::Decimal128(12, 1), false).with_metadata(label.clone()),
            rtdb_types::decimal_arrow::DecimalParameters { precision: 12, scale: -400, bit_width: 128 },
        ).unwrap();
        let tags = arrow_schema::UnionFields::try_new([7, 2, 4], [
            decimal,
            Field::new("u32", DataType::UInt32, true).with_metadata(label.clone()),
            Field::new("f32", DataType::Float32, false),
        ]).unwrap();
        let nan = f32::from_bits(0x7fc00071);
        let child = UnionArray::try_new(tags, vec![7, 4, 2, 2, 7].into(), Some(vec![0, 0, 1, 0, 0].into()), vec![
            Arc::new(Decimal128Array::from(vec![123]).with_precision_and_scale(12, 1).unwrap()),
            Arc::new(UInt32Array::from(vec![None, Some(u32::MAX)])),
            Arc::new(Float32Array::from(vec![nan])),
        ]).unwrap();
        let item = Arc::new(Field::new("$data$", child.data_type().clone(), true).with_metadata(label));
        let list = ListArray::try_new(item, OffsetBuffer::new(vec![0, 1, 5].into()), Arc::new(child), None).unwrap();
        let field = Field::new("v", list.data_type().clone(), false);
        let span = crate::ArrowCell { field: &field, array: &list, row: 1 }.numeric_list_span().unwrap();
        assert!(matches!(span.get(1), Some(rtdb_types::NumericCell::U32(u32::MAX))));
        let expected = Value::list(vec![
            Value::F32(nan), Value::U32(u32::MAX), Value::Null,
            Value::Decimal(DecimalValue::new(123_i128.to_be_bytes().to_vec(), 12, -400, 128).unwrap()),
        ]);
        let Value::List(expected) = expected else { unreachable!() };
        let Value::List(actual) = read_value(&field, &list, 1).unwrap() else { panic!("expected list"); };
        assert_eq!(actual.element_type(), expected.element_type());
        assert_eq!(actual.array().unwrap().to_data(), expected.array().unwrap().to_data());
    }

    #[test]
    fn selected_numeric_runs_unpack_without_reading_other_union_legs() {
        let fields = arrow_schema::UnionFields::try_new([3, 6, 2], [
            Field::new("utf8", DataType::Utf8, false),
            Field::new("u32", DataType::UInt32, true),
            Field::new("f64", DataType::Float64, true),
        ]).unwrap();
        let child = UnionArray::try_new(fields, vec![3, 6, 2, 6].into(), Some(vec![0, 0, 0, 1].into()), vec![
            Arc::new(StringArray::from(vec!["unused"])),
            Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])),
            Arc::new(Float64Array::from(vec![-0.0])),
        ]).unwrap();
        let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![2, 5, 7, 8]), &child).unwrap();
        let item = Arc::new(Field::new("item", runs.data_type().clone(), false));
        let data = arrow_data::ArrayData::builder(DataType::List(item)).len(2)
            .add_buffer(ScalarBuffer::from(vec![0_i32, 3, 8]).into_inner())
            .add_child_data(runs.to_data()).build().unwrap();
        let list = ListArray::from(data);
        let field = Field::new("v", list.data_type().clone(), false);
        let first = crate::ArrowCell { field: &field, array: &list, row: 0 };
        assert!(first.numeric_list_span().is_none());
        assert_eq!(first.numeric_list_span_range(2..3).unwrap().get(0), Some(rtdb_types::NumericCell::U32(u32::MAX)));

        let expected = Value::list(vec![Value::U32(u32::MAX), Value::U32(u32::MAX), Value::F64(-0.0), Value::F64(-0.0), Value::Null]);
        let decoded = read_value(&field, &list, 1).unwrap();
        assert_eq!(decoded, expected);
        let span = crate::ArrowCell { field: &field, array: &list, row: 1 }.numeric_list_span().unwrap();
        assert!(matches!(span.get(2), Some(rtdb_types::NumericCell::F64(v)) if v.to_bits() == (-0.0_f64).to_bits()));
        assert_eq!(span.get(4), Some(rtdb_types::NumericCell::Null));
    }

    #[test]
    fn selected_keyword_and_null_legs_repack_to_the_logical_extension() {
        let keyword = to_field("named-keyword", &LogicalType::Scalar(ScalarType::Keyword)).unwrap();
        let fields = arrow_schema::UnionFields::try_new([2, 4, 7], [
            keyword, Field::new("null", DataType::Null, true), Field::new("i32", DataType::Int32, false),
        ]).unwrap();
        let child = UnionArray::try_new(fields, vec![7, 2, 4, 2].into(), Some(vec![0, 1, 0, 0].into()), vec![
            Arc::new(StringArray::from(vec!["a", "b"])), Arc::new(NullArray::new(1)), Arc::new(Int32Array::from(vec![3])),
        ]).unwrap();
        let item = Arc::new(Field::new("item", child.data_type().clone(), true));
        let list = ListArray::try_new(item, OffsetBuffer::new(vec![0, 1, 4].into()), Arc::new(child), None).unwrap();
        let field = Field::new("v", list.data_type().clone(), false);
        let value = read_value(&field, &list, 1).unwrap();
        let expected = Value::list(vec![Value::Keyword(rtdb_types::Keyword::from_name("b")), Value::Null,
            Value::Keyword(rtdb_types::Keyword::from_name("a"))]);
        assert_eq!(value, expected);
        let Value::List(packed) = &value else { panic!("expected list"); };
        let (child, _) = packed.packed_column().expect("expected Arrow child");
        assert_eq!(child.metadata()[crate::EXTENSION_NAME_KEY], "xt/clj-keyword");
        assert_eq!(copy_array(&expected.logical_type(), &[value]).unwrap().to_data(),
            copy_array(&expected.logical_type(), &[expected]).unwrap().to_data());
    }

    #[test]
    fn mixed_scalar_children_repack_to_the_observed_union_and_keep_execution_bits() {
        let keyword = to_field("keyword", &LogicalType::Scalar(ScalarType::Keyword)).unwrap();
        let fields = arrow_schema::UnionFields::try_new([2, 4, 7], [
            keyword, Field::new("null", DataType::Null, true), Field::new("u32", DataType::UInt32, false),
        ]).unwrap();
        let child = UnionArray::try_new(fields, vec![7, 4, 2, 7].into(), Some(vec![0, 0, 0, 1].into()), vec![
            Arc::new(StringArray::from(vec!["tag"])), Arc::new(NullArray::new(1)), Arc::new(UInt32Array::from(vec![u32::MAX, 4])),
        ]).unwrap();
        let item = Arc::new(Field::new("item", child.data_type().clone(), true));
        let list = ListArray::try_new(item, OffsetBuffer::new(vec![0, 4].into()), Arc::new(child), None).unwrap();
        let field = Field::new("v", list.data_type().clone(), false);
        let keyword = Value::Keyword(rtdb_types::Keyword::from_name("tag"));
        let expected = Value::list(vec![Value::U32(u32::MAX), Value::Null, keyword.clone(), Value::U32(4)]);
        let logical = expected.logical_type();
        let raw = read_value(&field, &list, 0).unwrap();
        assert_eq!(raw, expected);
        let Value::List(packed) = &raw else { panic!("expected list"); };
        let (child, _) = packed.packed_column().expect("expected typed mixed children");
        assert_eq!(from_field(&child).unwrap(), packed.element_type());
        assert_eq!(copy_array(&logical, &[raw]).unwrap().to_data(), copy_array(&logical, &[expected]).unwrap().to_data());

        let (field, array, range) = (crate::ArrowCell { field: &field, array: &list, row: 0 }).sequence_range().unwrap();
        let executed = repack_scalar_list(field, array, range, true, || Ok(())).unwrap().unwrap();
        assert_eq!(Value::List(executed), Value::list(vec![Value::I32(-1), Value::Null, keyword, Value::I32(4)]));
    }

    #[test]
    fn admission_of_join_normal_types_agrees_with_the_lattice_join() {
        let s = |fields: &[(&str, Value)]| Value::Struct(fields.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect());
        let values = vec![
            Value::Null, Value::Undefined, Value::I64(1), Value::Utf8("a".into()), Value::Bool(true),
            Value::list(vec![]), Value::list(vec![Value::I64(1)]), Value::list(vec![Value::I64(1), Value::Utf8("b".into())]),
            Value::list(vec![Value::Null]), Value::Set(vec![Value::I64(2)]), Value::FixedSizeList(vec![Value::I64(1), Value::I64(2)].into()),
            s(&[("a", Value::I64(1))]), s(&[("a", Value::Null)]), s(&[("b", Value::Utf8("x".into()))]), s(&[("a", Value::Undefined)]),
            s(&[("a", Value::list(vec![s(&[("c", Value::Bool(false))])]))]), s(&[]),
            Value::Map { entries: vec![(Value::Utf8("k".into()), Value::I64(1))], sorted: false },
            Value::Map { entries: vec![], sorted: false },
            Value::TimestampTzRange { lower: 0, upper: 1 },
        ];
        let mut types = vec![LogicalType::Nothing];
        for a in &values { for b in &values { for c in &values {
            let ty = LogicalType::join_all([&a.logical_type(), &b.logical_type(), &c.logical_type()]);
            types.push(ty.clone().maybe());
            types.push(ty);
        } } }
        for ty in &types {
            let admission = Admission::new(ty);
            assert!(admission.join_normal, "{ty:?}");
            for value in &values { assert_eq!(admission.accepts(value), joined(&admission.plain, value), "{ty:?} admits {value:?}"); }
        }
    }
}
