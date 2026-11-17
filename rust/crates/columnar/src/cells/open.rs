use arrow_array::{StructArray, make_array};
use arrow_data::ArrayData;
use arrow_schema::UnionMode;

use super::*;

pub(super) fn open(field: Arc<Field>, column: OwnedColumn) -> Result<ExecutionVector> {
    let (array, reservation) = column.into_parts();
    open_from_parts(field, array, reservation)
}

pub(super) fn open_from_parts(field: Arc<Field>, array: ArrayRef, reservation: Arc<MemoryReservation>) -> Result<ExecutionVector> {
    let (bytes, depth) = schema_bound(&field, 1)?;
    let bytes = bytes.checked_mul(depth).and_then(|n| n.checked_mul(8)).ok_or_else(allocation)?;
    vector_length(array.len())?;
    if field.data_type() != array.data_type() { return Err(layout()); }
    if array.get_array_memory_size() > reservation.bytes() {
        return Err(Anomaly::incorrect("rtdb/undersized-reservation", "Execution vector input allocation exceeds its reservation"));
    }
    let metadata = reservation.budget().reserve(bytes)?;
    crate::from_field(&field)?;
    let data = normalize(&field, array.to_data(), reservation.budget())?;
    let normalized = make_array(data);
    let owner = Arc::new(OpenedOwner { _array: array, reservation, _metadata: metadata });
    let node = node(field, normalized, &owner)?;
    Ok(ExecutionVector::direct(node))
}

fn schema_bound(field: &Field, depth: usize) -> Result<(usize, usize)> {
    if depth > 128 { return Err(Anomaly::unsupported("rtdb/vector-schema-depth", "Execution vector schema exceeds 128 levels")); }
    let mut bytes = 4096usize.checked_add(field.name().capacity()).ok_or_else(allocation)?;
    bytes = bytes.checked_add(field.metadata().capacity().checked_mul(128).ok_or_else(allocation)?).ok_or_else(allocation)?;
    for (key, value) in field.metadata() {
        bytes = bytes.checked_add(key.capacity()).and_then(|n| n.checked_add(value.capacity())).ok_or_else(allocation)?;
    }
    let mut maximum = depth;
    let mut child = |field: &Field| -> Result<()> {
        let (size, nested) = schema_bound(field, depth + 1)?;
        bytes = bytes.checked_add(size).ok_or_else(allocation)?;
        maximum = maximum.max(nested);
        Ok(())
    };
    match field.data_type() {
        DataType::Struct(fields) => for field in fields { child(field)?; },
        DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _) => child(field)?,
        DataType::Union(fields, UnionMode::Dense) => {
            let mut ids = [false; 128];
            for (id, field) in fields.iter() {
                if id < 0 || ids[id as usize] { return Err(layout()); }
                ids[id as usize] = true;
                child(field)?;
            }
        }
        DataType::RunEndEncoded(ends, values) => { child(ends)?; child(values)?; }
        DataType::Timestamp(_, Some(timezone)) => bytes = bytes.checked_add(timezone.len()).ok_or_else(allocation)?,
        DataType::Float16 => return Err(Anomaly::unsupported("rtdb/half-vector", "HALF vectors cannot be opened for execution")),
        DataType::Null | DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::Float32 | DataType::Float64
        | DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) | DataType::Date32 | DataType::Date64
        | DataType::Time32(_) | DataType::Time64(_) | DataType::Timestamp(_, None) | DataType::Duration(_)
        | DataType::Interval(_) | DataType::Decimal32(_, _) | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {},
        _ => return Err(Anomaly::unsupported("rtdb/arrow-type", "Unsupported execution vector layout")),
    }
    Ok((bytes, maximum))
}

fn normalize(field: &Field, data: ArrayData, budget: &Arc<crate::MemoryBudget>) -> Result<ArrayData> {
    if field.data_type() != data.data_type() { return Err(layout()); }
    vector_length(data.len())?;
    if let DataType::Timestamp(_, Some(timezone)) = field.data_type() { rtdb_types::ZoneId::parse(timezone)?; }
    let (data_type, len, nulls, offset, buffers, children) = data.into_parts();
    let mut children = children.into_iter();
    let mut normalized = Vec::with_capacity(children.len());
    let mut child = |field: &Field| -> Result<()> {
        normalized.push(normalize(field, children.next().ok_or_else(layout)?, budget)?);
        Ok(())
    };
    match &data_type {
        DataType::Struct(fields) => for field in fields { child(field)?; },
        DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::Map(field, _) => child(field)?,
        DataType::Union(fields, _) => for (_, field) in fields.iter() { child(field)?; },
        DataType::RunEndEncoded(ends, values) => { child(ends)?; child(values)?; }
        _ => {},
    }
    if children.next().is_some() { return Err(layout()); }
    let nulls = field.is_nullable().then_some(nulls).flatten();
    let validation_bytes = match (&data_type, &nulls) {
        (DataType::FixedSizeList(field, size), Some(nulls)) if !field.is_nullable() => {
            let size = usize::try_from(*size).map_err(|_| layout())?;
            nulls.len().checked_mul(size).and_then(|n| n.checked_add(7)).map(|n| n / 8).and_then(|n| n.checked_add(256)).ok_or_else(allocation)?
        }
        _ => 0,
    };
    let _validation = budget.reserve(validation_bytes)?;
    ArrayData::builder(data_type).len(len).offset(offset).nulls(nulls).buffers(buffers).child_data(normalized).build().map_err(crate::arrow_error)
}

fn node(field: Arc<Field>, array: ArrayRef, owner: &Arc<OpenedOwner>) -> Result<Arc<OpenedNode>> {
    vector_length(array.len())?;
    let logical = crate::execution_field_type(&field)?;
    let mut children = Vec::new();
    match field.data_type() {
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array.as_ref())?;
            children.reserve_exact(fields.len());
            for (field, value) in fields.iter().zip(values.columns()) { children.push(node(Arc::clone(field), Arc::clone(value), owner)?); }
        }
        DataType::List(field) => children.push(node(Arc::clone(field), Arc::clone(downcast::<ListArray>(array.as_ref())?.values()), owner)?),
        DataType::FixedSizeList(field, _) => children.push(node(Arc::clone(field), Arc::clone(downcast::<FixedSizeListArray>(array.as_ref())?.values()), owner)?),
        DataType::Map(field, _) => children.push(node(Arc::clone(field), Arc::new(downcast::<MapArray>(array.as_ref())?.entries().clone()), owner)?),
        DataType::Union(fields, _) => {
            let values = downcast::<UnionArray>(array.as_ref())?;
            children.reserve_exact(fields.len());
            for (id, field) in fields.iter() { children.push(node(Arc::clone(field), Arc::clone(values.child(id)), owner)?); }
        }
        DataType::RunEndEncoded(ends, field) => {
            let values = match ends.data_type() {
                DataType::Int16 => downcast::<RunArray<Int16Type>>(array.as_ref())?.values(),
                DataType::Int32 => downcast::<RunArray<Int32Type>>(array.as_ref())?.values(),
                DataType::Int64 => downcast::<RunArray<Int64Type>>(array.as_ref())?.values(),
                _ => return Err(layout()),
            };
            children.push(node(Arc::clone(field), Arc::clone(values), owner)?);
        }
        _ => {},
    }
    Ok(Arc::new(OpenedNode { field, logical, array, children, owner: Arc::clone(owner) }))
}
