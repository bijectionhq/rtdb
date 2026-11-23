use rtdb_types::{Anomaly, ListKind, LogicalType, Result, ScalarType, Value};

/// Bounds allocations made by `build_array` and `copy_array`, including temporary value clones and Arrow container capacities.
/// Sum this bound for each column before building a batch; existing input values require their own reservation.
/// The bound includes batch overhead and rejects incompatible input types or arithmetic overflow.
pub fn estimate_builder_bytes(logical: &LogicalType, values: &[Value]) -> Result<usize> {
    estimate_builder_bytes_refs(logical, values)
}

/// Estimates the same reservation without materializing a column of cloned input values.
pub fn estimate_builder_bytes_refs<'a>(logical: &LogicalType, values: impl IntoIterator<Item = &'a Value>) -> Result<usize> {
    logical.validate()?;
    let (schema, depth) = schema_cost(logical)?;
    let mut bytes = schema;
    let admission = super::values::Admission::new(logical);
    for value in values {
        if !admission.accepts(value) {
            return Err(Anomaly::incorrect("rtdb/column-type-mismatch", "Allocation estimate requires values belonging to the declared logical type"));
        }
        bytes = add(bytes, cell_cost(logical, value)?)?;
        bytes = add(bytes, multiply(clone_cost(value)?, value_depth(value).min(depth))?)?;
    }
    multiply(bytes, 4)
}

fn schema_cost(logical: &LogicalType) -> Result<(usize, usize)> {
    let mut bytes = 1024;
    let mut depth = 1;
    let mut child = |logical: &LogicalType| -> Result<()> {
        let (child_bytes, child_depth) = schema_cost(logical)?;
        bytes = add(bytes, child_bytes)?;
        depth = depth.max(add(child_depth, 1)?);
        Ok(())
    };
    match logical {
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => child(inner)?,
        LogicalType::List { element, .. } => child(element)?,
        LogicalType::Struct(fields) => {
            for field in fields.values() { child(field)?; }
            for name in fields.keys() { bytes = add(bytes, multiply(name.len(), 4)?)?; }
        }
        LogicalType::Union(legs) => for leg in legs { child(leg)?; },
        LogicalType::Scalar(ScalarType::Timestamp { timezone: Some(timezone), .. }) => bytes = add(bytes, multiply(timezone.len(), 4)?)?,
        _ => {}
    }
    Ok((bytes, depth))
}

fn cell_cost(logical: &LogicalType, value: &Value) -> Result<usize> {
    if matches!(value, Value::Undefined | Value::Null) { return absent_cost(logical); }
    let mut bytes = add(multiply(std::mem::size_of::<Value>(), 2)?, 128)?;
    match logical {
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => bytes = add(bytes, cell_cost(inner, value)?)?,
        LogicalType::Nothing | LogicalType::Null => {}
        LogicalType::Scalar(scalar) => {
            let width = match scalar {
                ScalarType::FixedSizeBinary(width) => *width as usize,
                ScalarType::Utf8 | ScalarType::Uri | ScalarType::Keyword | ScalarType::Binary | ScalarType::Transit => match value {
                    Value::Utf8(value) | Value::Uri(value) => value.len(),
                    Value::Keyword(value) => value.as_str().len(),
                    Value::Binary(value) | Value::Transit(value) => value.len(),
                    _ => 0,
                },
                _ => 32,
            };
            bytes = add(bytes, width)?;
        }
        LogicalType::Struct(fields) => for (name, logical) in fields {
            let value = match value { Value::Struct(values) => super::values::struct_field_cell(values, name, logical), _ => &Value::Undefined };
            bytes = add(bytes, cell_cost(logical, value)?)?;
        },
        LogicalType::Union(legs) => {
            let leg = if matches!(value, Value::Undefined) { legs.first() }
                else { legs.iter().find(|leg| super::values::accepts(leg, value)) }.ok_or_else(overflow)?;
            bytes = add(bytes, cell_cost(leg, value)?)?;
        }
        LogicalType::List { kind, element } => match value {
            Value::Set(values) => {
                for value in values { bytes = add(bytes, cell_cost(element, value)?)?; }
            }
            Value::List(values) | Value::FixedSizeList(values) => match values.view() {
                rtdb_types::ListView::Values(values) => {
                    for value in values { bytes = add(bytes, cell_cost(element, value)?)?; }
                }
                _ => {
                    let plain = match element.as_ref() { LogicalType::Maybe(inner) => inner.as_ref(), element => element };
                    if matches!(plain, LogicalType::Scalar(ScalarType::F32 | ScalarType::F64)) {
                        bytes = add(bytes, multiply(values.len(), 8)?)?;
                    } else {
                        for value in values.iter() { bytes = add(bytes, cell_cost(element, value.as_ref())?)?; }
                    }
                }
            },
            Value::Map { entries, .. } => {
                let LogicalType::Struct(fields) = element.as_ref() else { return Err(overflow()); };
                let key_type = fields.get("key").ok_or_else(overflow)?;
                let value_type = fields.get("value").ok_or_else(overflow)?;
                for (key, value) in entries {
                    bytes = add(bytes, map_node_cost()?)?;
                    bytes = add(bytes, cell_cost(key_type, key)?)?;
                    bytes = add(bytes, cell_cost(value_type, value)?)?;
                }
            }
            Value::TimestampTzRange { .. } => bytes = add(bytes, multiply(cell_cost(element, &Value::Undefined)?, 2)?)?,
            _ => {
                let count = match kind { ListKind::FixedSizeList(width) => *width as usize, ListKind::TimestampTzRange => 2, _ => 0 };
                bytes = add(bytes, multiply(cell_cost(element, &Value::Undefined)?, count)?)?;
            }
        },
    }
    Ok(bytes)
}

/// An absent or null cell occupies one null slot in every descendant array and clones no value.
/// A slot is its value width (at most a 16-byte scalar), a validity bit, and an offset or type ID where the layout has one;
/// a wide struct type pays this for every field a document lacks.
fn absent_cost(logical: &LogicalType) -> Result<usize> {
    Ok(match logical {
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } => add(2, absent_cost(inner)?)?,
        LogicalType::Nothing | LogicalType::Null => 2,
        LogicalType::Scalar(ScalarType::FixedSizeBinary(width)) => add(2, *width as usize)?,
        LogicalType::Scalar(_) => 16,
        LogicalType::Struct(fields) => fields.values().try_fold(4, |bytes, field| add(bytes, absent_cost(field)?))?,
        LogicalType::Union(legs) => add(8, legs.first().map(absent_cost).transpose()?.unwrap_or(0))?,
        LogicalType::List { kind, element } => {
            let count = match kind { ListKind::FixedSizeList(width) => *width as usize, ListKind::TimestampTzRange => 2, _ => 0 };
            add(8, multiply(absent_cost(element)?, count)?)?
        }
    })
}

/// Copies clone a value once per nesting level it occupies.
fn value_depth(value: &Value) -> usize {
    1 + match value {
        Value::Struct(fields) => fields.values().map(value_depth).max().unwrap_or(0),
        Value::Set(values) => values.iter().map(value_depth).max().unwrap_or(0),
        Value::List(values) | Value::FixedSizeList(values) => match values.view() {
            rtdb_types::ListView::Values(values) => values.iter().map(value_depth).max().unwrap_or(0),
            _ => usize::from(!values.is_empty()),
        },
        Value::Map { entries, .. } => entries.iter().map(|(key, value)| value_depth(key).max(value_depth(value))).max().unwrap_or(0) + 1,
        _ => 0,
    }
}

fn clone_cost(value: &Value) -> Result<usize> {
    let mut bytes = std::mem::size_of::<Value>();
    match value {
        Value::Utf8(value) | Value::Uri(value) => bytes = add(bytes, value.len())?,
        Value::Keyword(value) => bytes = add(bytes, value.as_str().len())?,
        Value::Binary(value) | Value::Transit(value) | Value::FixedSizeBinary(value) => bytes = add(bytes, value.len())?,
        Value::Decimal(value) => bytes = add(bytes, value.unscaled.len())?,
        Value::Timestamp { timezone: Some(timezone), .. } => bytes = add(bytes, timezone.len())?,
        Value::Set(values) => {
            for value in values { bytes = add(bytes, clone_cost(value)?)?; }
        }
        Value::List(values) | Value::FixedSizeList(values) => if let rtdb_types::ListView::Values(values) = values.view() {
            for value in values { bytes = add(bytes, clone_cost(value)?)?; }
        },
        Value::Map { entries, .. } => for (key, value) in entries {
            bytes = add(bytes, map_node_cost()?)?;
            bytes = add(bytes, add(clone_cost(key)?, clone_cost(value)?)?)?;
        },
        Value::Struct(fields) => for (key, value) in fields {
            bytes = add(bytes, map_node_cost()?)?;
            bytes = add(bytes, add(key.len(), clone_cost(value)?)?)?;
        },
        _ => {}
    }
    Ok(bytes)
}

/// One map or struct entry: its key and value, doubled for B-tree node slack, plus node overhead.
fn map_node_cost() -> Result<usize> { add(multiply(add(std::mem::size_of::<Value>(), std::mem::size_of::<String>())?, 2)?, 64) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn multiply(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb/allocation-bound", "Array builder allocation bound cannot be represented") }
