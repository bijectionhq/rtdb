use std::collections::BTreeMap;
use super::*;

pub(super) fn object(cell: &CellRef<'_>) -> Result<OwnedValue> {
    let cell = cell.resolve()?;
    let reservation = cell.node.owner.metadata.budget().reserve(bound(cell.node, cell.row)?)?;
    let value = read(cell.node, cell.row)?;
    Ok(OwnedValue { value, _reservation: reservation })
}

fn leaf(node: &PhysicalNode, row: PhysicalIndex) -> Result<ResolvedCell<'_>> {
    match resolve_node(node, row)? {
        ResolvedAccess::Leaf(cell) if cell.node.descriptor.is_bottom_scalar() => Err(Anomaly::fault("xtdb/zero-leg-read", "The bottom type has no values to read")),
        ResolvedAccess::Leaf(cell) => Ok(cell),
        ResolvedAccess::AbsentUnion { type_id, row } => Err(absent(type_id, row)),
    }
}

fn bound(node: &PhysicalNode, row: PhysicalIndex) -> Result<usize> {
    let cell = leaf(node, row)?;
    let (node, row) = (cell.node, cell.row);
    let mut bytes = add(1024, mul(std::mem::size_of::<Value>(), 4)?)?;
    if node.null(row)? { return Ok(bytes); }
    if let NodeLayout::Leaf { array, .. } = &node.storage { return crate::estimate_read_value_bytes(node.ingress_arrow()?, array.as_ref(), node.checked_position(row)?); }
    if matches!(&node.storage, NodeLayout::Scalar(_)) { return super::native_scalar::value_bound(node, node.checked_position(row)?); }
    match node.container() {
        Some(ContainerStorageRef::Struct { children, .. }) => {
            for (key, child) in children.iter() {
                bytes = add(bytes, add(1024, mul(add(std::mem::size_of::<Value>(), std::mem::size_of::<String>())?, 16)?)?)?;
                bytes = add(bytes, mul(key.len(), 2)?)?;
                if !child.descriptor.is_bottom_scalar() { bytes = add(bytes, bound(child, row)?)?; }
            }
        }
        Some(ContainerStorageRef::List { .. } | ContainerStorageRef::FixedList { .. }) => {
            let (child, start, end) = node.elements(node.checked_position(row)?)?;
            bytes = add(bytes, mul(end - start, mul(std::mem::size_of::<Value>(), 6)?)?)?;
            for row in start..end { bytes = add(bytes, bound(child, PhysicalIndex::from_position(row)?)?)?; }
        }
        _ => return Err(layout()),
    }
    Ok(bytes)
}

fn read(node: &PhysicalNode, row: PhysicalIndex) -> Result<Value> {
    let cell = leaf(node, row)?;
    let (node, row) = (cell.node, cell.row);
    if node.null(row)? { return Ok(Value::Null); }
    if let NodeLayout::Leaf { array, .. } = &node.storage {
        let mut value = crate::read_execution_value(node.ingress_arrow()?, array.as_ref(), node.checked_position(row)?)?;
        if let (Value::Timestamp { timezone, .. }, DataType::Timestamp(_, zone)) = (&mut value, node.scalar_field()?.data_type()) {
            *timezone = zone.as_ref().map(|zone| zone.to_string());
        }
        return Ok(value);
    }
    if matches!(&node.storage, NodeLayout::Scalar(_)) { return super::native_scalar::value(node, node.checked_position(row)?); }
    match node.container() {
        Some(ContainerStorageRef::Struct { children, .. }) => {
            let mut fields = BTreeMap::new();
            for (key, child) in children.iter() {
                let value = if child.descriptor.is_bottom_scalar() { Value::Undefined } else { read(child, row)? };
                fields.insert(key.to_owned(), value);
            }
            Ok(Value::Struct(fields))
        }
        Some(ContainerStorageRef::List { .. } | ContainerStorageRef::FixedList { .. }) => {
            let (child, start, end) = node.elements(node.checked_position(row)?)?;
            let mut values = Vec::with_capacity(end - start);
            for row in start..end { values.push(read(child, PhysicalIndex::from_position(row)?)?); }
            match node.descriptor.container() {
                Some(ContainerKind::List(_)) => Ok(Value::List(values)),
                Some(ContainerKind::Set(_)) => Ok(Value::Set(values)),
                Some(ContainerKind::FixedList { .. }) => Ok(Value::FixedSizeList(values)),
                Some(ContainerKind::TimestampRange(_)) => match values.as_slice() {
                    [Value::Timestamp { value: lower, .. }, Value::Timestamp { value: upper, .. }] => Ok(Value::TimestampTzRange { lower: *lower, upper: *upper }),
                    _ => Err(Anomaly::incorrect("rtdb/timestamp-range", "Timestamp range requires two non-null timestamps")),
                },
                Some(ContainerKind::Map { sorted, .. }) => {
                    let entries = values.into_iter().map(|value| match value {
                        Value::Struct(mut fields) => Ok((fields.remove("key").ok_or_else(layout)?, fields.remove("value").ok_or_else(layout)?)),
                        _ => Err(layout()),
                    }).collect::<Result<Vec<_>>>()?;
                    Ok(Value::Map { entries, sorted: *sorted })
                }
                _ => Err(layout()),
            }
        }
        _ => Err(layout()),
    }
}

fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(allocation) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(allocation) }
