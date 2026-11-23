use arrow_array::{Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, ListArray, MapArray, PrimitiveArray, RunArray, StringArray, StructArray, UnionArray, types::*};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
use rtdb_types::{Anomaly, DecimalValue, Result, UriParts, Value};

use crate::{EXTENSION_NAME_KEY, read_value};

pub use rtdb_types::murmur3_32;

/// XXHash32 with the same unsigned wraparound and seed as `xtdb.util.Hasher.Xx`.
pub fn xxhash32(bytes: &[u8], seed: u32) -> u32 {
    const P1: u32 = 0x9e37_79b1;
    const P2: u32 = 0x85eb_ca77;
    const P3: u32 = 0xc2b2_ae3d;
    const P4: u32 = 0x27d4_eb2f;
    const P5: u32 = 0x1656_67b1;
    let lane = |index: usize| u32::from_le_bytes([bytes[index], bytes[index + 1], bytes[index + 2], bytes[index + 3]]);
    let step = |acc: u32, value: u32| acc.wrapping_add(value.wrapping_mul(P2)).rotate_left(13).wrapping_mul(P1);
    let mut index = 0;
    let mut hash = if bytes.len() >= 16 {
        let mut v1 = seed.wrapping_add(P1).wrapping_add(P2);
        let mut v2 = seed.wrapping_add(P2);
        let mut v3 = seed;
        let mut v4 = seed.wrapping_sub(P1);
        while index <= bytes.len() - 16 {
            v1 = step(v1, lane(index));
            v2 = step(v2, lane(index + 4));
            v3 = step(v3, lane(index + 8));
            v4 = step(v4, lane(index + 12));
            index += 16;
        }
        v1.rotate_left(1).wrapping_add(v2.rotate_left(7)).wrapping_add(v3.rotate_left(12)).wrapping_add(v4.rotate_left(18))
    } else { seed.wrapping_add(P5) };
    hash = hash.wrapping_add(bytes.len() as u32);
    while bytes.len() - index >= 4 {
        hash = hash.wrapping_add(lane(index).wrapping_mul(P3)).rotate_left(17).wrapping_mul(P4);
        index += 4;
    }
    while index < bytes.len() {
        hash = hash.wrapping_add(u32::from(bytes[index]).wrapping_mul(P5)).rotate_left(11).wrapping_mul(P1);
        index += 1;
    }
    hash ^= hash >> 15;
    hash = hash.wrapping_mul(P2);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(P3);
    hash ^ (hash >> 16)
}

fn combine(current: i32, next: i32) -> i32 { current.wrapping_mul(31).wrapping_add(next) }

pub fn hash_float_components(values: impl Iterator<Item = Option<f64>>, seed: u32, hash_bytes: fn(&[u8], u32) -> u32) -> i32 {
    values.fold(0, |hash, value| {
        let value = value.map_or(0, |value| hash_bytes(&value.to_bits().to_be_bytes(), seed) as i32);
        combine(hash, value)
    })
}

/// Hashes the JVM vector interpretation of one semantic value.
/// Struct children follow this value's key order; use `hash_cell` for an existing physical schema.
/// Unadmitted calendar objects are rejected.
pub fn hash_value(value: &Value, seed: u32) -> Result<i32> {
    hash_value_with(value, seed, xxhash32)
}

/// Uses the JVM Murmur3 vector hash; struct fields follow this value's key order.
/// Unadmitted calendar objects are rejected.
pub fn hash_value_murmur3(value: &Value, seed: u32) -> Result<i32> {
    hash_value_with(value, seed, murmur3_32)
}

fn hash_value_with(value: &Value, seed: u32, hash_bytes: fn(&[u8], u32) -> u32) -> Result<i32> {
    let float_hash = |value: f64, seed| hash_bytes(&value.to_bits().to_be_bytes(), seed) as i32;
    let nested = |value: &Value, seed| hash_value_with(value, seed, hash_bytes);
    Ok(match value {
        Value::Undefined | Value::Null => 0,
        Value::Bool(value) => if *value { 17 } else { 19 },
        Value::I8(value) => float_hash(f64::from(*value), seed),
        Value::I16(value) => float_hash(f64::from(*value), seed),
        Value::I32(value) | Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => float_hash(f64::from(*value), seed),
        Value::I64(value) => float_hash(*value as f64, seed),
        Value::U8(value) => float_hash(f64::from(*value as i8), seed),
        Value::U16(value) => float_hash(f64::from(*value as i16), seed),
        Value::U32(value) => float_hash(f64::from(*value as i32), seed),
        Value::U64(value) => float_hash((*value as i64) as f64, seed),
        Value::F16(_) => return Err(Anomaly::unsupported("rtdb/jvm-half-float", "The pinned JVM vector factory rejects half-precision floats")),
        Value::F32(value) => float_hash(f64::from(*value), seed),
        Value::F64(value) => float_hash(*value, seed),
        Value::Decimal(value) => float_hash(decimal_double(value)?, seed),
        Value::Utf8(value) | Value::Uri(value) => hash_bytes(value.as_bytes(), seed) as i32,
        Value::Keyword(value) => hash_bytes(value.as_str().as_bytes(), seed) as i32,
        Value::Binary(value) | Value::FixedSizeBinary(value) | Value::Transit(value) => hash_bytes(value, seed) as i32,
        Value::Uuid(value) => hash_bytes(value, seed) as i32,
        Value::Date(_) => return Err(Anomaly::incorrect("rtdb/unadmitted-calendar-value", "Vector hashing requires admitted calendar values")),
        Value::DateDay(value) => float_hash(i64::from(*value).wrapping_mul(86_400) as f64, seed),
        Value::DateMilli(value) => float_hash((value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY) * 86_400) as f64, seed),
        Value::Time { value, bit_width: 32, .. } => hash_bytes(&(*value as i32).to_le_bytes(), seed) as i32,
        Value::Time { value, bit_width: 64, .. } => hash_bytes(&value.to_le_bytes(), seed) as i32,
        Value::Time { .. } => return Err(Anomaly::incorrect("rtdb/time-width", "Time vectors require 32 or 64 bits")),
        Value::Timestamp { value, unit, .. } | Value::Duration { value, unit } => float_hash(*value as f64 / unit.units_per_second() as f64, seed),
        Value::IntervalYearMonth(value) => hash_bytes(&value.to_le_bytes(), seed) as i32,
        Value::IntervalDayTime { days, milliseconds } => {
            let mut bytes = [0; 8];
            bytes[..4].copy_from_slice(&days.to_le_bytes());
            bytes[4..].copy_from_slice(&milliseconds.to_le_bytes());
            hash_bytes(&bytes, seed) as i32
        }
        Value::IntervalMonthDayNano { months, days, nanoseconds } | Value::IntervalMonthDayMicro { months, days, nanoseconds } => {
            let mut bytes = [0; 16];
            bytes[..4].copy_from_slice(&months.to_le_bytes());
            bytes[4..8].copy_from_slice(&days.to_le_bytes());
            bytes[8..].copy_from_slice(&nanoseconds.to_le_bytes());
            hash_bytes(&bytes, seed) as i32
        }
        Value::List(_) | Value::FixedSizeList(_) => {
            let values = value.list_view().unwrap();
            if values.is_float() {
                hash_float_components((0..values.len()).map(|index| values.float_component(index)), seed, hash_bytes)
            } else if let rtdb_types::ListView::Numeric(values) = values {
                let array = values.array().as_ref();
                (0..array.len()).try_fold(0, |hash, row| {
                    let cell = crate::ArrowCell { field: values.field(), array, row }.physical()?;
                    let next = if cell.array.is_null(cell.row) { 0 } else {
                        scalar_hash_with(cell.field, cell.array, cell.row, seed, hash_bytes)?
                            .ok_or_else(layout_error)?
                    };
                    Ok::<i32, Anomaly>(combine(hash, next))
                })?
            } else {
                values.iter().try_fold(0, |hash, value| Ok::<i32, Anomaly>(combine(hash, nested(value.as_ref(), seed)?)))?
            }
        },
        Value::Set(values) => values.iter().try_fold(0i32, |hash, value| Ok::<i32, Anomaly>(hash.wrapping_add(nested(value, seed)?)))?,
        Value::Struct(fields) => fields.values().try_fold(0, |hash, value| Ok::<i32, Anomaly>(combine(hash, nested(value, seed)?)))?,
        Value::Map { entries, .. } => entries.iter().try_fold(0, |hash, (key, value)| {
            Ok::<i32, Anomaly>(combine(hash, combine(nested(key, seed)?, nested(value, seed)?)))
        })?,
        Value::TimestampTzRange { lower, upper } => combine(float_hash(*lower as f64 / 1_000_000.0, seed), float_hash(*upper as f64 / 1_000_000.0, seed)),
    })
}

fn decimal_double(value: &DecimalValue) -> Result<f64> {
    let width = match value.bit_width {
        32 | 64 | 128 | 256 => usize::from(value.bit_width / 8),
        _ => return Err(decimal_width()),
    };
    if value.unscaled.len() != width { return Err(decimal_width()); }
    Ok(rtdb_types::jvm_number::decimal_bytes_to_f64(&value.unscaled, value.scale))
}

pub fn decimal_cell_as_f64(field: &Field, array: &dyn Array, row: usize) -> Result<f64> {
    if row >= array.len() || array.data_type() != field.data_type() { return Err(layout_error()); }
    let (parameters, bytes) = (crate::ArrowCell { field, array, row }).decimal_coefficient()?.ok_or_else(decimal_width)?;
    let width = usize::from(parameters.bit_width / 8);
    Ok(rtdb_types::jvm_number::decimal_bytes_to_f64(&bytes[bytes.len() - width..], parameters.scale))
}

fn scalar_hash_with(
    field: &Field, array: &dyn Array, row: usize, seed: u32, hash_bytes: fn(&[u8], u32) -> u32,
) -> Result<Option<i32>> {
    let extension = field.metadata().get(EXTENSION_NAME_KEY).map(String::as_str);
    if !field.metadata().is_empty() {
        crate::decimal_field_parameters(field)?;
        match (field.data_type(), extension) {
            (_, None)
            | (DataType::Int32, Some("xt/oid" | "xt/regclass" | "xt/regproc"))
            | (DataType::Utf8, Some("xt/clj-keyword" | "uri"))
            | (DataType::Binary, Some("xt/transit+msgpack"))
            | (DataType::FixedSizeBinary(16), Some("uuid"))
            | (DataType::Interval(IntervalUnit::MonthDayNano), Some("xt/intervalDMMType")) => {}
            _ => return Ok(None),
        }
    }
    let bytes = |bytes: &[u8]| hash_bytes(bytes, seed) as i32;
    let double = |value: f64| bytes(&value.to_bits().to_be_bytes());
    macro_rules! cell {
        ($ty:ty) => { downcast::<PrimitiveArray<$ty>>(array)?.value(row) };
    }
    Ok(Some(match field.data_type() {
        DataType::Null => 0,
        DataType::Float16 => return Err(Anomaly::unsupported("rtdb/jvm-half-float", "The pinned JVM vector factory rejects half-precision floats")),
        DataType::Boolean => if downcast::<BooleanArray>(array)?.value(row) { 17 } else { 19 },
        DataType::Int8 => double(f64::from(cell!(Int8Type))),
        DataType::Int16 => double(f64::from(cell!(Int16Type))),
        DataType::Int32 => double(f64::from(cell!(Int32Type))),
        DataType::Int64 => double(cell!(Int64Type) as f64),
        DataType::UInt8 => double(f64::from(cell!(UInt8Type) as i8)),
        DataType::UInt16 => double(f64::from(cell!(UInt16Type) as i16)),
        DataType::UInt32 => double(f64::from(cell!(UInt32Type) as i32)),
        DataType::UInt64 => double((cell!(UInt64Type) as i64) as f64),
        DataType::Float32 => double(f64::from(cell!(Float32Type))),
        DataType::Float64 => double(cell!(Float64Type)),
        DataType::Date32 => double(i64::from(cell!(Date32Type)).wrapping_mul(86_400) as f64),
        DataType::Date64 => double((cell!(Date64Type).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY) * 86_400) as f64),
        DataType::Time32(TimeUnit::Second) => bytes(&cell!(Time32SecondType).to_le_bytes()),
        DataType::Time32(TimeUnit::Millisecond) => bytes(&cell!(Time32MillisecondType).to_le_bytes()),
        DataType::Time64(TimeUnit::Microsecond) => bytes(&cell!(Time64MicrosecondType).to_le_bytes()),
        DataType::Time64(TimeUnit::Nanosecond) => bytes(&cell!(Time64NanosecondType).to_le_bytes()),
        DataType::Duration(unit) => double(match unit {
            TimeUnit::Second => cell!(DurationSecondType) as f64,
            TimeUnit::Millisecond => cell!(DurationMillisecondType) as f64 / 1e3,
            TimeUnit::Microsecond => cell!(DurationMicrosecondType) as f64 / 1e6,
            TimeUnit::Nanosecond => cell!(DurationNanosecondType) as f64 / 1e9,
        }),
        DataType::Timestamp(unit, _) => double(match unit {
            TimeUnit::Second => cell!(TimestampSecondType) as f64,
            TimeUnit::Millisecond => cell!(TimestampMillisecondType) as f64 / 1e3,
            TimeUnit::Microsecond => cell!(TimestampMicrosecondType) as f64 / 1e6,
            TimeUnit::Nanosecond => cell!(TimestampNanosecondType) as f64 / 1e9,
        }),
        DataType::Interval(IntervalUnit::YearMonth) => bytes(&cell!(IntervalYearMonthType).to_le_bytes()),
        DataType::Interval(IntervalUnit::DayTime) => {
            let value = cell!(IntervalDayTimeType);
            let mut buffer = [0; 8];
            buffer[..4].copy_from_slice(&value.days.to_le_bytes());
            buffer[4..].copy_from_slice(&value.milliseconds.to_le_bytes());
            bytes(&buffer)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let value = cell!(IntervalMonthDayNanoType);
            let mut buffer = [0; 16];
            buffer[..4].copy_from_slice(&value.months.to_le_bytes());
            buffer[4..8].copy_from_slice(&value.days.to_le_bytes());
            buffer[8..].copy_from_slice(&value.nanoseconds.to_le_bytes());
            bytes(&buffer)
        }
        DataType::Utf8 => {
            let value = downcast::<StringArray>(array)?.value(row);
            if extension == Some("uri") { UriParts::parse(value)?; }
            bytes(value.as_bytes())
        }
        DataType::Binary => bytes(downcast::<BinaryArray>(array)?.value(row)),
        DataType::FixedSizeBinary(_) => bytes(downcast::<FixedSizeBinaryArray>(array)?.value(row)),
        DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..) =>
            double(decimal_cell_as_f64(field, array, row)?),
        _ => return Ok(None),
    }))
}

/// Hashes in physical schema order, matching vector hashes used in JVM HLL metadata.
pub fn hash_cell(field: &Field, array: &dyn Array, row: usize, seed: u32) -> Result<i32> {
    if row >= array.len() || array.data_type() != field.data_type() { return Err(layout_error()); }
    match field.data_type() {
        DataType::RunEndEncoded(ends, values) => return match ends.data_type() {
            DataType::Int16 => { let runs = downcast::<RunArray<Int16Type>>(array)?; hash_cell(values, runs.values().as_ref(), runs.get_physical_index(row), seed) }
            DataType::Int32 => { let runs = downcast::<RunArray<Int32Type>>(array)?; hash_cell(values, runs.values().as_ref(), runs.get_physical_index(row), seed) }
            DataType::Int64 => { let runs = downcast::<RunArray<Int64Type>>(array)?; hash_cell(values, runs.values().as_ref(), runs.get_physical_index(row), seed) }
            _ => Err(layout_error()),
        },
        DataType::Union(fields, UnionMode::Dense) => {
            let union = downcast::<UnionArray>(array)?;
            let id = union.type_id(row);
            let field = fields.iter().find_map(|(child_id, field)| (id == child_id).then_some(field)).ok_or_else(layout_error)?;
            return hash_cell(field, union.child(id).as_ref(), union.value_offset(row), seed);
        }
        _ => {}
    }
    if array.is_null(row) { return Ok(0); }
    match field.data_type() {
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array)?;
            fields.iter().enumerate().try_fold(0, |hash, (index, child)| Ok::<i32, Anomaly>(combine(hash, hash_cell(child, values.column(index).as_ref(), row, seed)?)))
        }
        DataType::List(child) => {
            let values = downcast::<ListArray>(array)?.value(row);
            let set = field.metadata().get(EXTENSION_NAME_KEY).is_some_and(|name| name == "set");
            (0..values.len()).try_fold(0i32, |hash, index| {
                let next = hash_cell(child, values.as_ref(), index, seed)?;
                Ok(if set { hash.wrapping_add(next) } else { combine(hash, next) })
            })
        }
        DataType::FixedSizeList(child, _) => {
            let values = downcast::<FixedSizeListArray>(array)?.value(row);
            (0..values.len()).try_fold(0, |hash, index| Ok::<i32, Anomaly>(combine(hash, hash_cell(child, values.as_ref(), index, seed)?)))
        }
        DataType::Map(child, _) => {
            let values = downcast::<MapArray>(array)?.value(row);
            (0..values.len()).try_fold(0, |hash, index| Ok::<i32, Anomaly>(combine(hash, hash_cell(child, &values, index, seed)?)))
        }
        _ => match scalar_hash_with(field, array, row, seed, xxhash32)? {
            Some(hash) => Ok(hash),
            None => hash_value(&read_value(field, array, row)?, seed),
        },
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(layout_error) }
fn layout_error() -> Anomaly { Anomaly::incorrect("rtdb/hash-array-layout", "Hash cell index, field, and array must agree") }
fn decimal_width() -> Anomaly { Anomaly::incorrect("rtdb/decimal-width", "Decimal coefficient must fill its declared signed width") }
