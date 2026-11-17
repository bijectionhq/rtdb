use arrow_array::{FixedSizeListArray, ListArray, MapArray, StructArray, UnionArray, make_array};
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
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Arrow { _array: array, reservation }, metadata });
    let node = node(field, normalized, &owner)?;
    Ok(ExecutionVector::direct(node))
}

pub(super) fn schema_bound(field: &Field, depth: usize) -> Result<(usize, usize)> {
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

fn node(field: Arc<Field>, array: ArrayRef, owner: &Arc<NodeOwner>) -> Result<Arc<PhysicalNode>> {
    vector_length(array.len())?;
    let BackingOwner::Arrow { reservation, .. } = &owner.backing else { return Err(layout()); };
    let length = array.len();
    let storage = match field.data_type() {
        DataType::Struct(fields) => {
            let values = downcast::<StructArray>(array.as_ref())?;
            let mut children = Vec::with_capacity(fields.len());
            for (field, value) in fields.iter().zip(values.columns()) { children.push(node(Arc::clone(field), Arc::clone(value), owner)?); }
            NodeLayout::Struct { validity: values.nulls().cloned(), children }
        }
        DataType::List(field) => {
            let list = downcast::<ListArray>(array.as_ref())?;
            NodeLayout::List {
                validity: list.nulls().cloned(), offsets: RetainedBuffer::arrow(list.offsets().inner().inner().clone(), reservation),
                elements: node(Arc::clone(field), Arc::clone(list.values()), owner)?,
            }
        }
        DataType::FixedSizeList(field, size) => {
            let list = downcast::<FixedSizeListArray>(array.as_ref())?;
            NodeLayout::FixedList { validity: list.nulls().cloned(), size: usize::try_from(*size).map_err(|_| layout())?, elements: node(Arc::clone(field), Arc::clone(list.values()), owner)? }
        }
        DataType::Map(field, _) => {
            let map = downcast::<MapArray>(array.as_ref())?;
            NodeLayout::List {
                validity: map.nulls().cloned(), offsets: RetainedBuffer::arrow(map.offsets().inner().inner().clone(), reservation),
                elements: node(Arc::clone(field), Arc::new(map.entries().clone()), owner)?,
            }
        }
        DataType::Union(fields, _) => {
            let values = downcast::<UnionArray>(array.as_ref())?;
            let mut children = Vec::with_capacity(fields.len());
            for (id, field) in fields.iter() { children.push(node(Arc::clone(field), Arc::clone(values.child(id)), owner)?); }
            NodeLayout::Union {
                type_ids: RetainedBuffer::arrow(values.type_ids().inner().clone(), reservation),
                offsets: RetainedBuffer::arrow(values.offsets().ok_or_else(layout)?.inner().clone(), reservation), children,
            }
        }
        DataType::RunEndEncoded(ends, field) => {
            let data = array.to_data();
            NodeLayout::RunEnd(RunEndReader::new(RunStorage {
                logical_offset: data.offset(),
                ends: node(Arc::clone(ends), make_array(data.child_data()[0].clone()), owner)?,
                values: node(Arc::clone(field), make_array(data.child_data()[1].clone()), owner)?,
            }))
        }
        _ => {
            let data = array.to_data();
            NodeLayout::Leaf { payload: data.buffers().first().cloned(), payload_offset: data.offset(), array }
        },
    };
    finish_node(field, length, storage, Arc::clone(owner))
}

pub(super) fn finish_node(ingress: Arc<Field>, length: usize, storage: NodeLayout, owner: Arc<NodeOwner>) -> Result<Arc<PhysicalNode>> {
    let count = SourceCount::from_len(length)?;
    if let (NodeLayout::RunEnd(reader), DataType::RunEndEncoded(_, _)) = (&storage, ingress.data_type()) { storage::check_runs(reader.ends(), reader.values(), length)?; }
    let descriptor = VectorDescriptor::opened(&ingress, &storage, Arc::clone(&owner.metadata))?;
    Ok(Arc::new(PhysicalNode { declaration: Declaration::Arrow(ingress), descriptor, count, storage, owner }))
}

pub(super) fn scalar_data_type(data_type: &DataType) -> Result<DataType> {
    Ok(match data_type {
        DataType::UInt8 => DataType::Int8, DataType::UInt16 => DataType::Int16,
        DataType::UInt32 => DataType::Int32, DataType::UInt64 => DataType::Int64,
        DataType::Timestamp(unit, Some(zone)) => DataType::Timestamp(*unit, Some(rtdb_types::ZoneId::parse(zone)?.to_string().into())),
        other => other.clone(),
    })
}
