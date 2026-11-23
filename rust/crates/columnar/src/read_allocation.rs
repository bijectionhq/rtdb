use arrow_array::{Array, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, ListArray, MapArray, RunArray, StringArray, StructArray, UnionArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, Result, Value};

/// Bounds `read_value`'s temporary schema construction and owned result without allocating.
/// Only the selected cell, active union leg, and selected list offsets contribute value bytes.
/// Existing Arrow buffers retain their own reservations; sum this bound before reading multiple cells.
pub fn estimate_read_value_bytes(field: &Field, array: &dyn Array, row: usize) -> Result<usize> {
    if field.data_type() != array.data_type() { return Err(layout()); }
    let (schema, depth) = schema_bytes(field, false)?;
    add(mul(mul(schema, depth)?, 4)?, estimate_read_cell_bytes(field, array, row)?)
}

/// Bounds the owned result of reading one cell through a `ColumnReader`, whose schema is derived once per column.
pub fn estimate_read_cell_bytes(field: &Field, array: &dyn Array, row: usize) -> Result<usize> {
    if row >= array.len() { return Err(Anomaly::incorrect("rtdb/row-index", "Cell index exceeds array length")); }
    if field.data_type() != array.data_type() { return Err(layout()); }
    let base = add(mul(std::mem::size_of::<Value>(), 2)?, 256)?;
    if field.data_type() == &DataType::Null && !field.is_nullable() { return Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read")); }
    if let DataType::RunEndEncoded(ends, values) = field.data_type() {
        let child = match ends.data_type() {
            DataType::Int16 => { let runs = cast::<RunArray<Int16Type>>(array)?; estimate_read_cell_bytes(values, runs.values().as_ref(), runs.get_physical_index(row))? },
            DataType::Int32 => { let runs = cast::<RunArray<Int32Type>>(array)?; estimate_read_cell_bytes(values, runs.values().as_ref(), runs.get_physical_index(row))? },
            DataType::Int64 => { let runs = cast::<RunArray<Int64Type>>(array)?; estimate_read_cell_bytes(values, runs.values().as_ref(), runs.get_physical_index(row))? },
            _ => return Err(layout()),
        };
        return add(base, child);
    }
    if let DataType::Union(fields, UnionMode::Dense) = field.data_type() {
        let union = cast::<UnionArray>(array)?;
        let id = union.type_id(row);
        let field = fields.iter().find_map(|(candidate, field)| (candidate == id).then_some(field)).ok_or_else(layout)?;
        return add(base, estimate_read_cell_bytes(field, union.child(id).as_ref(), union.value_offset(row))?);
    }
    if array.is_null(row) { return Ok(base); }
    let value = match field.data_type() {
        DataType::Utf8 => mul(cast::<StringArray>(array)?.value(row).len(), 2)?,
        DataType::Binary => mul(cast::<BinaryArray>(array)?.value(row).len(), 2)?,
        DataType::FixedSizeBinary(_) => mul(cast::<FixedSizeBinaryArray>(array)?.value(row).len(), 2)?,
        DataType::Timestamp(_, timezone) => timezone.as_ref().map_or(0, |timezone| timezone.len()),
        DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => 64,
        DataType::Struct(fields) => {
            let values = cast::<StructArray>(array)?;
            let mut bytes = 0;
            for (index, child) in fields.iter().enumerate() {
                bytes = add(bytes, map_node()?)?;
                bytes = add(bytes, mul(child.name().len(), 2)?)?;
                bytes = add(bytes, if child.data_type() == &DataType::Null && !child.is_nullable() { std::mem::size_of::<Value>() } else { estimate_read_cell_bytes(child, values.column(index).as_ref(), row)? })?;
            }
            bytes
        }
        DataType::List(child) => {
            let values = cast::<ListArray>(array)?;
            let start = usize::try_from(values.value_offsets()[row]).map_err(|_| layout())?;
            let end = usize::try_from(values.value_offsets()[row + 1]).map_err(|_| layout())?;
            if field.metadata().is_empty() && child.metadata().is_empty()
                && matches!(child.data_type(), DataType::Float32 | DataType::Float64) {
                if start > end || end > values.values().len() { return Err(layout()); }
                add(256, mul(end - start, 16)?)?
            } else {
                range(child, values.values().as_ref(), start, end)?
            }
        }
        DataType::FixedSizeList(child, length) => {
            let values = cast::<FixedSizeListArray>(array)?;
            let length = usize::try_from(*length).map_err(|_| layout())?;
            let start = mul(row, length)?;
            let end = add(start, length)?;
            if field.metadata().is_empty() && child.metadata().is_empty()
                && matches!(child.data_type(), DataType::Float32 | DataType::Float64)
            {
                if end > values.values().len() { return Err(layout()); }
                add(256, mul(length, 16)?)?
            } else {
                range(child, values.values().as_ref(), start, end)?
            }
        }
        DataType::Map(child, _) => {
            let values = cast::<MapArray>(array)?;
            let start = usize::try_from(values.value_offsets()[row]).map_err(|_| layout())?;
            let end = usize::try_from(values.value_offsets()[row + 1]).map_err(|_| layout())?;
            add(range(child, values.entries(), start, end)?, mul(end.checked_sub(start).ok_or_else(layout)?, mul(std::mem::size_of::<Value>(), 4)?)?)?
        }
        _ => 0,
    };
    add(base, value)
}

fn range(field: &Field, values: &dyn Array, start: usize, end: usize) -> Result<usize> {
    if start > end || end > values.len() { return Err(layout()); }
    let mut bytes = add(256, mul(end - start, mul(std::mem::size_of::<Value>(), 2)?)?)?;
    for row in start..end { bytes = add(bytes, estimate_read_cell_bytes(field, values, row)?)?; }
    Ok(bytes)
}

fn schema_bytes(field: &Field, in_union: bool) -> Result<(usize, usize)> {
    if let Some(extension) = field.metadata().get(crate::EXTENSION_NAME_KEY) {
        let valid = match extension.as_str() {
            "xt/clj-keyword" | "uri" => field.data_type() == &DataType::Utf8,
            "xt/transit+msgpack" => field.data_type() == &DataType::Binary,
            "uuid" => field.data_type() == &DataType::FixedSizeBinary(16),
            "xt/oid" | "xt/regclass" | "xt/regproc" => field.data_type() == &DataType::Int32,
            "xt/intervalDMMType" => field.data_type() == &DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            "set" => matches!(field.data_type(), DataType::List(_)),
            "xt/tstz-range" => matches!(field.data_type(), DataType::FixedSizeList(_, 2)),
            _ => return Err(Anomaly::unsupported("rtdb/unknown-arrow-extension", "Unknown Arrow extension")),
        };
        if !valid { return Err(Anomaly::incorrect("rtdb/extension-storage-type", "Arrow storage type disagrees with extension")); }
    }
    let mut bytes = add(256, mul(field.name().len(), 4)?)?;
    let mut depth = 1;
    let mut child = |field: &Field, in_union| -> Result<()> {
        let (size, nested) = schema_bytes(field, in_union)?;
        bytes = add(bytes, size)?;
        depth = depth.max(add(nested, 1)?);
        Ok(())
    };
    match field.data_type() {
        DataType::Struct(fields) => {
            for (index, field) in fields.iter().enumerate() {
                if fields[..index].iter().any(|previous| previous.name() == field.name()) { return Err(Anomaly::incorrect("rtdb/duplicate-struct-field", "Repeated struct field")); }
                child(field, in_union)?;
            }
        }
        DataType::List(field) | DataType::FixedSizeList(field, _) => child(field, in_union)?,
        DataType::Map(field, _) => {
            let DataType::Struct(fields) = field.data_type() else { return Err(layout()); };
            if field.is_nullable() || fields.len() != 2 || fields[0].name() != "key" || fields[0].is_nullable() || fields[1].name() != "value" { return Err(layout()); }
            child(field, in_union)?;
        }
        DataType::Union(fields, UnionMode::Dense) => for (_, field) in fields.iter() { child(field, true)?; },
        DataType::RunEndEncoded(ends, values) => {
            if in_union { return Err(Anomaly::unsupported("rtdb/encoded-union-leg", "Union legs must be plain at every depth")); }
            if field.is_nullable() || ends.is_nullable() || ends.name() != "run_ends" || values.name() != "values" || !matches!(ends.data_type(), DataType::Int16 | DataType::Int32 | DataType::Int64) || matches!(values.data_type(), DataType::Null | DataType::RunEndEncoded(_, _)) { return Err(layout()); }
            child(ends, false)?; child(values, false)?;
        }
        DataType::Timestamp(_, timezone) => bytes = add(bytes, mul(timezone.as_ref().map_or(0, |timezone| timezone.len()), 4)?)?,
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float16 | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) | DataType::Decimal32(_, _) | DataType::Decimal64(_, _) | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) | DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) | DataType::Duration(_) | DataType::Interval(_) => {}
        _ => return Err(Anomaly::unsupported("rtdb/arrow-type", "Unsupported XTDB Arrow type")),
    }
    Ok((bytes, depth))
}
fn cast<T: 'static>(array: &dyn Array) -> Result<&T> { array.as_any().downcast_ref().ok_or_else(layout) }
fn map_node() -> Result<usize> { add(mul(add(std::mem::size_of::<Value>(), std::mem::size_of::<String>())?, 16)?, 512) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb/allocation-bound", "Arrow cell allocation bound exceeds address space") }
fn layout() -> Anomaly { Anomaly::incorrect("rtdb/array-layout", "Arrow array and logical field layout disagree") }
