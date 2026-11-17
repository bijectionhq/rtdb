use super::descriptor::ContainerShapeRef;
use super::native_scalar::{StoredBits, StoredBytes};
use super::*;

#[derive(Clone, Copy)]
pub(super) enum NativeListKind { List, Set, Map { sorted: bool } }
#[derive(Clone, Copy)]
pub(super) enum NativeFixedKind { List(i32), TimestampRange }

pub(super) enum NativeContainerParts {
    Struct { validity: Option<StoredBits>, children: Vec<ExecutionVector> },
    List { kind: NativeListKind, validity: Option<StoredBits>, offsets: StoredBytes, last_offset: i32, elements: ExecutionVector },
    FixedList { kind: NativeFixedKind, validity: Option<StoredBits>, elements: ExecutionVector },
    Union { type_ids: StoredBytes, offsets: StoredBytes, children: Vec<ExecutionVector> },
    RunEnd { ends: ExecutionVector, values: ExecutionVector },
}

#[derive(Debug)]
pub(super) enum NativeContainerStorage {
    Struct { validity: Option<StoredBits>, children: Vec<Arc<PhysicalNode>> },
    List { validity: Option<StoredBits>, offsets: StoredBytes, last_offset: i32, elements: Arc<PhysicalNode> },
    FixedList { validity: Option<StoredBits>, elements: Arc<PhysicalNode> },
    Union { type_ids: StoredBytes, offsets: StoredBytes, children: Vec<Arc<PhysicalNode>> },
}

#[derive(Clone, Copy)]
pub(super) enum ContainerValidity<'a> {
    Absent,
    Sdk { bits: Option<&'a arrow_buffer::NullBuffer>, length: usize },
    Native(&'a StoredBits),
}
impl ContainerValidity<'_> {
    pub(super) fn is_null(self, row: PhysicalIndex) -> Result<bool> {
        match self {
            Self::Absent => Ok(false),
            Self::Sdk { bits, length } => {
                let row = row.position()?;
                if row >= length { return Err(index_error()); }
                Ok(bits.is_some_and(|bits| bits.is_null(row)))
            },
            Self::Native(bits) => Ok(!super::native_scalar::bit(bits.bytes.as_slice(), row.position()?)?),
        }
    }
}
fn native_validity(bits: Option<&StoredBits>) -> ContainerValidity<'_> { bits.map_or(ContainerValidity::Absent, ContainerValidity::Native) }

#[derive(Clone, Copy)]
pub(super) enum ContainerOffsets<'a> { Sdk(&'a RetainedBuffer), Native(&'a StoredBytes) }
impl ContainerOffsets<'_> {
    pub(super) fn get(self, row: PhysicalIndex) -> Result<i32> {
        match self {
            Self::Sdk(offsets) => offsets.i32(row.position()?),
            Self::Native(offsets) => {
                let start = usize::try_from(row.0.wrapping_mul(4)).map_err(|_| index_error())?;
                Ok(i32::from_le_bytes(super::native_scalar::span(offsets.bytes.as_slice(), start, 4)?.try_into().map_err(|_| layout())?))
            },
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ContainerStorageRef<'a> {
    Struct { validity: ContainerValidity<'a>, children: &'a [Arc<PhysicalNode>] },
    List { validity: ContainerValidity<'a>, offsets: ContainerOffsets<'a>, elements: &'a Arc<PhysicalNode> },
    FixedList { validity: ContainerValidity<'a>, size: i32, elements: &'a Arc<PhysicalNode> },
    Union { type_ids: &'a [u8], offsets: ContainerOffsets<'a>, children: &'a [Arc<PhysicalNode>] },
}

impl PhysicalNode {
    fn sdk_validity<'a>(&'a self, bits: Option<&'a arrow_buffer::NullBuffer>) -> ContainerValidity<'a> {
        if self.descriptor.is_nullable() { ContainerValidity::Sdk { bits, length: self.length } } else { ContainerValidity::Absent }
    }
    pub(super) fn container(&self) -> Option<ContainerStorageRef<'_>> {
        Some(match &self.storage {
            NodeLayout::Struct { validity, children } => ContainerStorageRef::Struct { validity: self.sdk_validity(validity.as_ref()), children },
            NodeLayout::List { validity, offsets, elements } => ContainerStorageRef::List { validity: self.sdk_validity(validity.as_ref()), offsets: ContainerOffsets::Sdk(offsets), elements },
            NodeLayout::FixedList { validity, size, elements } => ContainerStorageRef::FixedList { validity: self.sdk_validity(validity.as_ref()), size: *size as i32, elements },
            NodeLayout::Union { type_ids, offsets, children } => ContainerStorageRef::Union { type_ids: type_ids.as_slice(), offsets: ContainerOffsets::Sdk(offsets), children },
            NodeLayout::NativeContainer(storage) => {
                match storage {
                    NativeContainerStorage::Struct { validity: bits, children } => ContainerStorageRef::Struct { validity: native_validity(bits.as_ref()), children },
                    NativeContainerStorage::List { validity: bits, offsets, elements, .. } => ContainerStorageRef::List { validity: native_validity(bits.as_ref()), offsets: ContainerOffsets::Native(offsets), elements },
                    NativeContainerStorage::FixedList { validity: bits, elements } => {
                        let size = match self.descriptor.container()? { ContainerKind::FixedList { size, .. } => *size, ContainerKind::TimestampRange(_) => 2, _ => return None };
                        ContainerStorageRef::FixedList { validity: native_validity(bits.as_ref()), size, elements }
                    },
                    NativeContainerStorage::Union { type_ids, offsets, children } => ContainerStorageRef::Union { type_ids: type_ids.bytes.as_slice(), offsets: ContainerOffsets::Native(offsets), children },
                }
            },
            _ => return None,
        })
    }
}

fn check_bytes(bytes: &StoredBytes) -> Result<()> { if bytes.append <= bytes.bytes.len() { Ok(()) } else { Err(layout()) } }
fn check_bits(bits: Option<&StoredBits>) -> Result<()> {
    if bits.is_some_and(|bits| bits.byte_append > bits.bytes.len()) { Err(layout()) } else { Ok(()) }
}

/// The caller must admit the input child-vector capacity before constructing `parts` and retain that lease through this call.
/// Publication reserves the overlapping input, new node and descriptor-reference slots; it does not admit an earlier allocation retroactively.
pub(super) fn publish_native(name: &str, length: usize, parts: NativeContainerParts, memory: &Arc<crate::MemoryBudget>) -> Result<ExecutionVector> {
    vector_length(length)?;
    let slots = match &parts {
        NativeContainerParts::Struct { children, .. } | NativeContainerParts::Union { children, .. } => { vector_length(children.len())?; children.capacity() },
        NativeContainerParts::RunEnd { .. } => 2,
        _ => 1,
    };
    let bytes = slots.checked_mul(std::mem::size_of::<ExecutionVector>() + 3 * std::mem::size_of::<Arc<PhysicalNode>>()).and_then(|n| n.checked_add(2048)).ok_or_else(allocation)?;
    let metadata = memory.reserve(bytes)?;
    let children = |children: Vec<ExecutionVector>| -> Result<Vec<Arc<PhysicalNode>>> {
        let mut nodes = Vec::with_capacity(children.len());
        for child in children { nodes.push(child.into_direct()?); }
        Ok(nodes)
    };
    let (descriptor, storage) = match parts {
        NativeContainerParts::Struct { validity, children: inputs } => {
            check_bits(validity.as_ref())?;
            let children = children(inputs)?;
            let descriptors = children.iter().map(|child| Arc::clone(&child.descriptor)).collect::<Vec<_>>();
            let descriptor = VectorDescriptor::native_container(name, validity.is_some(), ContainerShapeRef::Struct(&descriptors), memory)?;
            (descriptor, NodeLayout::NativeContainer(NativeContainerStorage::Struct { validity, children }))
        },
        NativeContainerParts::List { kind, validity, offsets, last_offset, elements } => {
            check_bits(validity.as_ref())?; check_bytes(&offsets)?;
            let elements = elements.into_direct()?;
            let shape = match kind {
                NativeListKind::List => ContainerShapeRef::List(&elements.descriptor),
                NativeListKind::Set => ContainerShapeRef::Set(&elements.descriptor),
                NativeListKind::Map { sorted } => ContainerShapeRef::Map { sorted, entries: &elements.descriptor },
            };
            let descriptor = VectorDescriptor::native_container(name, validity.is_some(), shape, memory)?;
            (descriptor, NodeLayout::NativeContainer(NativeContainerStorage::List { validity, offsets, last_offset, elements }))
        },
        NativeContainerParts::FixedList { kind, validity, elements } => {
            check_bits(validity.as_ref())?;
            let elements = elements.into_direct()?;
            let shape = match kind { NativeFixedKind::List(size) => ContainerShapeRef::FixedList { size, elements: &elements.descriptor }, NativeFixedKind::TimestampRange => ContainerShapeRef::TimestampRange(&elements.descriptor) };
            let descriptor = VectorDescriptor::native_container(name, validity.is_some(), shape, memory)?;
            (descriptor, NodeLayout::NativeContainer(NativeContainerStorage::FixedList { validity, elements }))
        },
        NativeContainerParts::Union { type_ids, offsets, children: inputs } => {
            check_bytes(&type_ids)?; check_bytes(&offsets)?;
            let children = children(inputs)?;
            let descriptors = children.iter().map(|child| Arc::clone(&child.descriptor)).collect::<Vec<_>>();
            let descriptor = VectorDescriptor::native_container(name, false, ContainerShapeRef::OrdinalUnion(&descriptors), memory)?;
            (descriptor, NodeLayout::NativeContainer(NativeContainerStorage::Union { type_ids, offsets, children }))
        },
        NativeContainerParts::RunEnd { ends, values } => {
            let ends = ends.into_direct()?;
            let values = values.into_direct()?;
            let descriptor = VectorDescriptor::native_container(name, false, ContainerShapeRef::RunEnd { ends: &ends.descriptor, values: &values.descriptor }, memory)?;
            (descriptor, NodeLayout::RunEnd(RunEndReader::new(RunStorage { logical_offset: 0, ends, values })))
        },
    };
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata });
    Ok(ExecutionVector::direct(Arc::new(PhysicalNode { declaration: Declaration::Native, descriptor, length, storage, owner })))
}

#[cfg(test)]
mod tests;
