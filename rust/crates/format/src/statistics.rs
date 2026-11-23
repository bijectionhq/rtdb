use rtdb_types::StructFields;
use std::collections::BTreeMap;

use arrow_array::{Array, FixedSizeListArray, ListArray, RecordBatch, RunArray, StructArray, UnionArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, LogicalType, Result, Value};

use crate::bloom::BloomFilter;

#[derive(Clone)]
enum Mask { All, Selected(Vec<bool>) }

impl Mask {
    fn includes(&self, row: usize) -> bool { match self { Self::All => true, Self::Selected(rows) => rows[row] } }
}

#[derive(Default)]
struct Statistics {
    fields: StructFields<Value>,
    numeric: BTreeMap<&'static str, (f64, f64)>,
    bytes_seen: bool,
    bloom: Option<BloomFilter>,
}

pub fn columns_for_event_page(batch: &RecordBatch, calculate_blooms: bool) -> Result<Vec<Value>> {
    let schema = batch.schema();
    let mut output = Vec::new();
    for name in ["_iid", "_valid_from", "_valid_to", "_system_from", "_recency"] {
        match schema.index_of(name) {
            Ok(index) => { column_metadata(schema.field(index), batch.column(index).as_ref(), true, calculate_blooms, &mut output)?; }
            Err(_) if name == "_recency" => {}
            Err(error) => return Err(invalid(error)),
        }
    }
    let op_index = schema.index_of("op").map_err(invalid)?;
    let op = downcast::<UnionArray>(batch.column(op_index).as_ref())?;
    let DataType::Union(fields, _) = op.data_type() else { return Err(invalid("Missing event operation fields")); };
    if let Some((id, field)) = fields.iter().find(|(_, field)| field.name() == "put") {
        let put = downcast::<StructArray>(op.child(id).as_ref())?;
        let DataType::Struct(fields) = field.data_type() else { return Err(invalid("Put operation must be a struct")); };
        for (index, field) in fields.iter().enumerate() { column_metadata(field, put.column(index).as_ref(), true, calculate_blooms, &mut output)?; }
    }
    Ok(output)
}

pub fn column_metadata(field: &Field, array: &dyn Array, root: bool, calculate_blooms: bool, output: &mut Vec<Value>) -> Result<usize> {
    write_column(field, array, root, calculate_blooms, &Mask::All, output, 0)
}

fn write_column(field: &Field, array: &dyn Array, root: bool, calculate_blooms: bool, mask: &Mask, output: &mut Vec<Value>, depth: usize) -> Result<usize> {
    if depth > 128 { return Err(invalid("Column metadata nesting exceeds 128 levels")); }
    let mut statistics = Statistics::default();
    let with_bloom = calculate_blooms || (root && field.name() == "_iid");
    fold(field, array, mask, with_bloom, calculate_blooms, output, &mut statistics, depth)?;
    let count = match present_cells(field, array, mask) {
        Some(count) => i64::try_from(count).map_err(invalid)?,
        None => {
            let reader = rtdb_columnar::ColumnPresence::new(field)?;
            let mut count = 0i64;
            for row in 0..array.len() {
                if mask.includes(row) && reader.is_present(array, row)? { count += 1; }
            }
            count
        }
    };
    statistics.fields.insert("col-name".into(), Value::Utf8(field.name().clone()));
    statistics.fields.insert("root-col?".into(), Value::Bool(root));
    statistics.fields.insert("count".into(), Value::I64(count));
    for (name, (min, max)) in statistics.numeric {
        let fields = StructFields::from([
            ("min".into(), if min == f64::INFINITY { Value::Null } else { Value::F64(min) }),
            ("max".into(), if max == f64::NEG_INFINITY { Value::Null } else { Value::F64(max) }),
        ]);
        statistics.fields.insert(name.into(), Value::Struct(fields));
    }
    if statistics.bytes_seen {
        let bloom = match statistics.bloom { Some(bloom) => Value::Binary(bloom.to_bytes()?), None => Value::Null };
        statistics.fields.insert("bytes".into(), Value::Struct(StructFields::from([("bloom".into(), bloom)])));
    }
    let index = output.len();
    i32::try_from(index).map_err(invalid)?;
    output.push(Value::Struct(statistics.fields));
    Ok(index)
}

#[allow(clippy::too_many_arguments)]
fn fold(field: &Field, array: &dyn Array, mask: &Mask, with_bloom: bool, calculate_blooms: bool, output: &mut Vec<Value>, statistics: &mut Statistics, depth: usize) -> Result<()> {
    if depth > 128 { return Err(invalid("Column metadata nesting exceeds 128 levels")); }
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = downcast::<UnionArray>(array)?;
            for (id, field) in fields.iter() {
                let child = union.child(id);
                let child_mask = match mask {
                    Mask::All => Mask::All,
                    Mask::Selected(_) => {
                        let mut selected = vec![false; child.len()];
                        for row in 0..array.len() { if mask.includes(row) && union.type_id(row) == id { selected[union.value_offset(row)] = true; } }
                        Mask::Selected(selected)
                    }
                };
                fold(field, child.as_ref(), &child_mask, with_bloom, calculate_blooms, output, statistics, depth + 1)?;
            }
            return Ok(());
        }
        DataType::RunEndEncoded(ends, value) => {
            macro_rules! runs {
                ($ty:ty) => {{
                    let runs = downcast::<RunArray<$ty>>(array)?;
                    let mut selected = vec![false; runs.values().len()];
                    for row in 0..array.len() { if mask.includes(row) { selected[runs.get_physical_index(row)] = true; } }
                    fold(value, runs.values().as_ref(), &Mask::Selected(selected), with_bloom, calculate_blooms, output, statistics, depth + 1)
                }};
            }
            return match ends.data_type() { DataType::Int16 => runs!(Int16Type), DataType::Int32 => runs!(Int32Type), DataType::Int64 => runs!(Int64Type), _ => Err(invalid("Invalid run-end index width")) };
        }
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array)?;
            let child_mask = if field.is_nullable() { Mask::Selected((0..array.len()).map(|row| mask.includes(row) && !array.is_null(row)).collect()) } else { mask.clone() };
            let children = fields.iter().enumerate().map(|(index, field)| {
                write_column(field, values.column(index).as_ref(), false, calculate_blooms, &child_mask, output, depth + 1).and_then(|index| i32::try_from(index).map(Value::I32).map_err(invalid))
            }).collect::<Result<Vec<_>>>()?;
            statistics.fields.insert("struct".into(), Value::List(children));
            return Ok(());
        }
        DataType::List(element) | DataType::FixedSizeList(element, _) => {
            let values = match field.data_type() {
                DataType::List(_) => downcast::<ListArray>(array)?.values(),
                _ => downcast::<FixedSizeListArray>(array)?.values(),
            };
            let child = write_column(element, values.as_ref(), false, calculate_blooms, &Mask::All, output, depth + 1)?;
            let name = if field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).is_some_and(|name| name == "set") { "set" } else { "list" };
            statistics.fields.insert(name.into(), Value::I32(i32::try_from(child).map_err(invalid)?));
            return Ok(());
        }
        DataType::Null => return Ok(()),
        _ => {}
    }
    let ty = rtdb_columnar::from_field(field)?;
    if ty == LogicalType::Nothing { return Ok(()); }
    let extension = field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).map(String::as_str);
    let presence = matches!(field.data_type(), DataType::Boolean | DataType::Interval(_)) || matches!(extension, Some("xt/oid" | "xt/regclass" | "xt/regproc" | "xt/interval-month-day-micro" | "xt/transit+msgpack"));
    if presence {
        let mono = match &ty { LogicalType::Maybe(value) => value.as_ref(), value => value };
        statistics.fields.insert(rtdb_columnar::leg_name(mono)?, Value::Bool(true));
        return Ok(());
    }
    let byte_backed = matches!(field.data_type(), DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_));
    if byte_backed {
        statistics.bytes_seen = true;
        if with_bloom {
            let bloom = statistics.bloom.get_or_insert_with(BloomFilter::default);
            if !bloom.insert_byte_cells(array, (0..array.len()).filter(|row| mask.includes(*row))) {
                for row in 0..array.len() { if mask.includes(row) { bloom.insert_cell(field, array, row)?; } }
            }
        }
        return Ok(());
    }
    let group = match field.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => "date-times",
        DataType::Time32(_) | DataType::Time64(_) => "times-of-day",
        DataType::Duration(_) => "durations",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float32 | DataType::Float64 | DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "numbers",
        _ => return Err(Anomaly::unsupported("rtdb.format/metadata-flavour", format!("Unsupported column metadata type: {:?}", field.data_type()))),
    };
    let bounds = statistics.numeric.entry(group).or_insert((f64::INFINITY, f64::NEG_INFINITY));
    let cells = numeric_cells(array);
    for row in 0..array.len() {
        if !mask.includes(row) || array.is_null(row) { continue; }
        let value = match &cells {
            Some(cells) => cells(row),
            None => rtdb_columnar::decimal_cell_as_f64(field, array, row)?,
        };
        if value.is_nan() || bounds.0.is_nan() { bounds.0 = f64::NAN; } else { bounds.0 = bounds.0.min(value); }
        if value.is_nan() || bounds.1.is_nan() { bounds.1 = f64::NAN; } else { bounds.1 = bounds.1.max(value); }
    }
    Ok(())
}

/// How many of a flat array's selected cells read as neither null nor undefined, counted from its validity.
fn present_cells(field: &Field, array: &dyn Array, mask: &Mask) -> Option<usize> {
    let data_type = array.data_type();
    let flat = data_type.is_primitive() || matches!(data_type, DataType::Boolean | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_));
    if !flat || field.data_type() != data_type || !field.metadata().is_empty() { return None; }
    Some(match mask {
        Mask::All => array.len() - array.null_count(),
        Mask::Selected(rows) => (0..array.len()).filter(|row| rows[*row] && array.is_valid(*row)).count(),
    })
}

/// Reads numeric statistics in place, using signed execution widths and seconds for time values.
fn numeric_cells(array: &dyn Array) -> Option<Box<dyn Fn(usize) -> f64 + '_>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::*;
    use arrow_schema::TimeUnit as Unit;
    let per_second = |unit: &Unit| match unit { Unit::Second => 1.0, Unit::Millisecond => 1e3, Unit::Microsecond => 1e6, Unit::Nanosecond => 1e9 };
    Some(match array.data_type() {
        DataType::Int8 => { let array = array.as_primitive::<Int8Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Int16 => { let array = array.as_primitive::<Int16Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Int32 => { let array = array.as_primitive::<Int32Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Int64 => { let array = array.as_primitive::<Int64Type>(); Box::new(move |row| array.value(row) as f64) }
        DataType::UInt8 => { let array = array.as_primitive::<UInt8Type>(); Box::new(move |row| f64::from(array.value(row) as i8)) }
        DataType::UInt16 => { let array = array.as_primitive::<UInt16Type>(); Box::new(move |row| f64::from(array.value(row) as i16)) }
        DataType::UInt32 => { let array = array.as_primitive::<UInt32Type>(); Box::new(move |row| f64::from(array.value(row) as i32)) }
        DataType::UInt64 => { let array = array.as_primitive::<UInt64Type>(); Box::new(move |row| (array.value(row) as i64) as f64) }
        DataType::Float32 => { let array = array.as_primitive::<Float32Type>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Float64 => { let array = array.as_primitive::<Float64Type>(); Box::new(move |row| array.value(row)) }
        DataType::Date32 => { let array = array.as_primitive::<Date32Type>(); Box::new(move |row| i64::from(array.value(row)).wrapping_mul(86_400) as f64) }
        DataType::Date64 => { let array = array.as_primitive::<Date64Type>(); Box::new(move |row| (array.value(row).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY) * 86_400) as f64) }
        DataType::Time32(Unit::Second) => { let array = array.as_primitive::<Time32SecondType>(); Box::new(move |row| f64::from(array.value(row))) }
        DataType::Time32(Unit::Millisecond) => { let array = array.as_primitive::<Time32MillisecondType>(); Box::new(move |row| f64::from(array.value(row)) / 1e3) }
        DataType::Time64(Unit::Microsecond) => { let array = array.as_primitive::<Time64MicrosecondType>(); Box::new(move |row| array.value(row) as f64 / 1e6) }
        DataType::Time64(Unit::Nanosecond) => { let array = array.as_primitive::<Time64NanosecondType>(); Box::new(move |row| array.value(row) as f64 / 1e9) }
        DataType::Timestamp(unit, _) => {
            let scale = per_second(unit);
            let values: Box<dyn Fn(usize) -> i64 + '_> = match unit {
                Unit::Second => { let array = array.as_primitive::<TimestampSecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Millisecond => { let array = array.as_primitive::<TimestampMillisecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Microsecond => { let array = array.as_primitive::<TimestampMicrosecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Nanosecond => { let array = array.as_primitive::<TimestampNanosecondType>(); Box::new(move |row| array.value(row)) }
            };
            Box::new(move |row| values(row) as f64 / scale)
        }
        DataType::Duration(unit) => {
            let scale = per_second(unit);
            let values: Box<dyn Fn(usize) -> i64 + '_> = match unit {
                Unit::Second => { let array = array.as_primitive::<DurationSecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Millisecond => { let array = array.as_primitive::<DurationMillisecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Microsecond => { let array = array.as_primitive::<DurationMicrosecondType>(); Box::new(move |row| array.value(row)) }
                Unit::Nanosecond => { let array = array.as_primitive::<DurationNanosecondType>(); Box::new(move |row| array.value(row)) }
            };
            Box::new(move |row| values(row) as f64 / scale)
        }
        _ => return None,
    })
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(|| invalid("Metadata array and field disagree")) }
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.format/invalid-column-metadata", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_statistics_handle_full_scale_without_exponent_sized_allocation() {
        for (scale, expected) in [(i32::MIN, f64::NEG_INFINITY), (i32::MAX, -0.0)] {
            let value = Value::Decimal(rtdb_types::DecimalValue::new((-123i128).to_be_bytes().to_vec(), 3, scale, 128).unwrap());
            let field = rtdb_columnar::to_field("d", &value.logical_type()).unwrap();
            let array = rtdb_columnar::copy_array(&value.logical_type(), &[value]).unwrap();
            let mut columns = Vec::new();
            column_metadata(&field, array.as_ref(), true, false, &mut columns).unwrap();
            let Value::Struct(column) = &columns[0] else { panic!("Expected column metadata"); };
            let Value::Struct(numbers) = &column["numbers"] else { panic!("Expected numeric statistics"); };
            let Value::F64(min) = numbers["min"] else { panic!("Expected the minimum"); };
            assert_eq!(min.to_bits(), expected.to_bits());
        }
    }
    #[test]
    fn date_millisecond_statistics_floor_to_the_containing_day() {
        let millis = arrow_array::Date64Array::from(vec![-86_400_001, -86_400_000, -1, 0, 86_400_001]);
        let days = arrow_array::Date32Array::from(vec![-2, -1, -1, 0, 1]);
        let millis = numeric_cells(&millis).unwrap();
        let days = numeric_cells(&days).unwrap();
        for row in 0..5 { assert_eq!(millis(row), days(row)); }
    }
}
