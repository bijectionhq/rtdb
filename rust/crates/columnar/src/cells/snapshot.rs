use super::native_scalar::{NativeScalarData, ScalarStorageRef};
use super::*;

impl ExecutionVector {
    pub(super) fn snapshot_reader(&self, memory: &Arc<crate::MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(view_bound(self, 1)?)?;
        snapshot_view(self, &reservation)
    }
}

fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(allocation) }
fn depth_limit(depth: usize) -> Result<()> {
    if depth > 128 { Err(Anomaly::unsupported("rtdb/vector-depth", "Execution reader snapshot exceeds 128 levels")) } else { Ok(()) }
}
fn view_bound(vector: &ExecutionVector, depth: usize) -> Result<usize> {
    depth_limit(depth)?;
    let base = std::mem::size_of::<ExecutionVector>().checked_add(256).ok_or_else(allocation)?;
    add(base, match &vector.view {
        ReaderView::Direct(node) => node_bound(node, depth)?,
        ReaderView::UnionLeg { inner, .. } => view_bound(inner, depth + 1)?,
    })
}
fn node_bound(node: &PhysicalNode, depth: usize) -> Result<usize> {
    depth_limit(depth)?;
    let mut bytes = std::mem::size_of::<PhysicalNode>().checked_add(std::mem::size_of::<NodeOwner>()).and_then(|n| n.checked_add(256)).ok_or_else(allocation)?;
    match &node.storage {
        NodeLayout::Struct { children, .. } | NodeLayout::Union { children, .. } => {
            bytes = add(bytes, children.len().checked_mul(std::mem::size_of::<Arc<PhysicalNode>>()).ok_or_else(allocation)?)?;
            for child in children { bytes = add(bytes, node_bound(child, depth + 1)?)?; }
        },
        NodeLayout::List { elements, .. } | NodeLayout::FixedList { elements, .. } => { bytes = add(bytes, node_bound(elements, depth + 1)?)?; },
        NodeLayout::RunEnd(reader) => {
            bytes = add(bytes, node_bound(reader.ends(), depth + 1)?)?;
            bytes = add(bytes, node_bound(reader.values(), depth + 1)?)?;
        },
        NodeLayout::Leaf { .. } | NodeLayout::Scalar(_) => {},
    }
    Ok(bytes)
}

fn snapshot_view(vector: &ExecutionVector, reservation: &Arc<MemoryReservation>) -> Result<ExecutionVector> {
    let view = match &vector.view {
        ReaderView::Direct(node) => ReaderView::Direct(snapshot_node(node, reservation)?),
        ReaderView::UnionLeg { union, type_id, inner, .. } => {
            let union = Arc::clone(union);
            let inner = Arc::new(snapshot_view(inner, reservation)?);
            ReaderView::UnionLeg { union, type_id: *type_id, inner, _reservation: Arc::clone(reservation) }
        },
    };
    Ok(ExecutionVector { view, address: vector.address.clone() })
}

fn snapshot_node(node: &PhysicalNode, reservation: &Arc<MemoryReservation>) -> Result<Arc<PhysicalNode>> {
    let storage = match &node.storage {
        NodeLayout::Scalar(storage) => {
            let mut storage = storage.clone();
            if let NativeScalarData::Variable { offsets, last_offset, .. } = &mut storage.data {
                *last_offset = if node.length == 0 { 0 } else { super::native_scalar::ScalarOffsets::Native(offsets.bytes.as_slice()).get(node.length)? };
            }
            NodeLayout::Scalar(storage)
        },
        NodeLayout::Leaf { array, payload, payload_offset } => {
            let array = Arc::clone(array);
            let payload = payload.clone();
            if node.length != 0 {
                if let ScalarStorageRef::Variable { offsets, .. } = ScalarStorageRef::node(node)? { offsets.get(node.length)?; }
            }
            NodeLayout::Leaf { array, payload, payload_offset: *payload_offset }
        },
        NodeLayout::Struct { validity, children } => {
            let validity = validity.clone();
            let mut copied = Vec::with_capacity(children.len());
            for child in children { copied.push(snapshot_node(child, reservation)?); }
            NodeLayout::Struct { validity, children: copied }
        },
        NodeLayout::List { validity, offsets, elements } => {
            let elements = snapshot_node(elements, reservation)?;
            let validity = validity.clone();
            let offsets = offsets.clone();
            if node.length != 0 { node.list_offset(PhysicalIndex::from_position(node.length)?, false)?; }
            NodeLayout::List { validity, offsets, elements }
        },
        NodeLayout::FixedList { validity, size, elements } => {
            let validity = validity.clone();
            let elements = snapshot_node(elements, reservation)?;
            NodeLayout::FixedList { validity, size: *size, elements }
        },
        NodeLayout::Union { type_ids, offsets, children } => {
            let mut copied = Vec::with_capacity(children.len());
            for child in children { copied.push(snapshot_node(child, reservation)?); }
            let type_ids = type_ids.clone();
            let offsets = offsets.clone();
            NodeLayout::Union { type_ids, offsets, children: copied }
        },
        NodeLayout::RunEnd(reader) => {
            let ends = snapshot_node(reader.ends(), reservation)?;
            let values = snapshot_node(reader.values(), reservation)?;
            NodeLayout::RunEnd(RunEndReader::new(RunStorage { logical_offset: reader.logical_offset(), ends, values }))
        },
    };
    let declaration = match &node.declaration { Declaration::Arrow(field) => Declaration::Arrow(Arc::clone(field)), Declaration::Native => Declaration::Native };
    let owner = Arc::new(NodeOwner { backing: node.owner.backing.clone(), metadata: Arc::clone(reservation) });
    Ok(Arc::new(PhysicalNode { declaration, descriptor: Arc::clone(&node.descriptor), length: node.length, storage, owner }))
}
