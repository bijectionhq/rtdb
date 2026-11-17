use arrow_buffer::{Buffer, NullBuffer};

use super::*;

/// Immutable bytes paired with the reservation covering their backing allocation.
/// Slices and clones retain that reservation; no uncharged Buffer is returned.
#[derive(Clone, Debug)]
pub struct RetainedBuffer { buffer: Buffer, _reservation: Arc<MemoryReservation> }

impl RetainedBuffer {
    /// Adopts bytes already allocated under this reservation.
    /// The reservation must cover the Buffer's complete allocation capacity, including a sliced buffer's backing allocation.
    /// A lease shared across different allocations must cover their combined capacity before those allocations are made.
    /// Independent Buffer clones held by the caller require their own retained reservation.
    pub fn with_reservation(buffer: Buffer, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if buffer.capacity() > reservation.bytes() { return Err(Anomaly::incorrect("rtdb/undersized-reservation", "Physical buffer exceeds its reservation")); }
        Ok(Self { buffer, _reservation: reservation })
    }
    pub fn as_slice(&self) -> &[u8] { self.buffer.as_slice() }
    pub fn len(&self) -> usize { self.buffer.len() }
    pub fn is_empty(&self) -> bool { self.buffer.is_empty() }
    /// Shares a checked byte range without allocating or releasing the backing charge.
    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        if offset.checked_add(length).is_none_or(|end| end > self.len()) { return Err(index_error()); }
        Ok(Self { buffer: self.buffer.slice_with_length(offset, length), _reservation: Arc::clone(&self._reservation) })
    }
    pub(super) fn arrow(buffer: Buffer, reservation: &Arc<MemoryReservation>) -> Self {
        Self { buffer, _reservation: Arc::clone(reservation) }
    }
    pub(super) fn i32(&self, index: usize) -> Result<i32> {
        let start = index.checked_mul(4).ok_or_else(allocation)?;
        let bytes = self.as_slice().get(start..start.checked_add(4).ok_or_else(allocation)?).ok_or_else(index_error)?;
        Ok(i32::from_le_bytes(bytes.try_into().map_err(|_| layout())?))
    }
}

/// Native container input; children must be complete direct vectors, not selected reader views.
/// Offsets are little-endian i32 values; validity bytes use Arrow's least-significant-bit-first convention.
/// Dense union type bytes retain negative absent-leg discriminators and nonnegative IDs from the declaration.
/// `ExecutionVector::from_physical` validates every layout before publication and adopts the supplied owners.
pub enum PhysicalLayout {
    Struct { length: usize, validity: Option<RetainedBuffer>, children: Vec<ExecutionVector> },
    List { length: usize, validity: Option<RetainedBuffer>, offsets: RetainedBuffer, elements: ExecutionVector },
    FixedSizeList { length: usize, validity: Option<RetainedBuffer>, elements: ExecutionVector },
    DenseUnion { type_ids: RetainedBuffer, offsets: RetainedBuffer, children: Vec<ExecutionVector> },
    RunEndEncoded { length: usize, run_ends: ExecutionVector, values: ExecutionVector },
}

#[derive(Debug)]
pub(super) enum NodeLayout {
    Scalar(super::native_scalar::NativeScalarStorage),
    NativeContainer(NativeContainerStorage),
    Leaf { array: ArrayRef, payload: Option<Buffer>, payload_offset: usize },
    Struct { validity: Option<NullBuffer>, children: Vec<Arc<PhysicalNode>> },
    List { validity: Option<NullBuffer>, offsets: RetainedBuffer, elements: Arc<PhysicalNode> },
    FixedList { validity: Option<NullBuffer>, size: usize, elements: Arc<PhysicalNode> },
    Union { type_ids: RetainedBuffer, offsets: RetainedBuffer, children: Vec<Arc<PhysicalNode>> },
    RunEnd(RunEndReader),
}

impl PhysicalNode {
    pub(super) fn array(&self) -> Result<&dyn Array> {
        match &self.storage { NodeLayout::Leaf { array, .. } => Ok(array.as_ref()), _ => Err(layout()) }
    }
    pub(super) fn null(&self, row: PhysicalIndex) -> Result<bool> {
        if let Some(container) = self.container() {
            return match container {
                ContainerStorageRef::Struct { validity, .. } | ContainerStorageRef::List { validity, .. } | ContainerStorageRef::FixedList { validity, .. } => validity.is_null(row),
                ContainerStorageRef::Union { .. } => match self.union_child(row)? { Some((child, offset)) => child.null(offset), None => Ok(true) },
            };
        }
        Ok(match &self.storage {
            NodeLayout::Scalar(storage) => {
                if self.scalar_field()?.data_type() == &DataType::Null { true }
                else if let Some(bits) = &storage.validity { !super::native_scalar::bit(bits.bytes.as_slice(), self.checked_position(row)?)? }
                else { false }
            },
            NodeLayout::Leaf { .. } if self.descriptor.scalar_field().is_some_and(|field| field.data_type() == &DataType::Null) => true,
            NodeLayout::Leaf { array, .. } => self.descriptor.is_nullable() && array.is_null(self.checked_position(row)?),
            NodeLayout::RunEnd(reader) => reader.values().null(reader.index(row)?)?,
            _ => return Err(layout()),
        })
    }
    pub(super) fn check(&self, row: usize) -> Result<()> { if row < self.length { Ok(()) } else { Err(index_error()) } }
    pub(super) fn checked_position(&self, row: PhysicalIndex) -> Result<usize> { let row = row.position()?; self.check(row)?; Ok(row) }
    pub(super) fn union_id(&self, row: PhysicalIndex) -> Result<i8> {
        let Some(ContainerStorageRef::Union { type_ids, .. }) = self.container() else { return Err(unsupported("getTypeId")); };
        Ok(*type_ids.get(row.position()?).ok_or_else(index_error)? as i8)
    }
    pub(super) fn union_offset_index(&self, row: PhysicalIndex) -> Result<PhysicalIndex> {
        let Some(ContainerStorageRef::Union { offsets, .. }) = self.container() else { return Err(layout()); };
        Ok(PhysicalIndex(offsets.get(row)?))
    }
    pub(super) fn union_leg(&self, row: PhysicalIndex) -> Result<Option<&Arc<Self>>> {
        let id = self.union_id(row)?;
        if id < 0 { return Ok(None); }
        let Some(ContainerStorageRef::Union { children, .. }) = self.container() else { return Err(layout()); };
        let index = self.descriptor.union_index(id)?;
        Ok(Some(children.get(index).ok_or_else(index_error)?))
    }
    pub(super) fn union_child(&self, row: PhysicalIndex) -> Result<Option<(&Arc<Self>, PhysicalIndex)>> {
        self.union_leg(row)?.map(|child| Ok((child, self.union_offset_index(row)?))).transpose()
    }
    pub(super) fn list_offset(&self, row: PhysicalIndex, count: bool) -> Result<i32> {
        match self.container() {
            Some(ContainerStorageRef::FixedList { size, .. }) => Ok(if count { size } else { row.0.wrapping_mul(size) }),
            Some(ContainerStorageRef::List { offsets, .. }) => {
                if count {
                    let end = offsets.get(PhysicalIndex(row.0.wrapping_add(1)))?;
                    Ok(end.wrapping_sub(offsets.get(row)?))
                } else { offsets.get(row) }
            },
            _ => Err(unsupported(if count { "getListCount" } else { "getListStartIndex" })),
        }
    }
    pub(super) fn elements(&self, row: usize) -> Result<(&Arc<Self>, usize, usize)> {
        self.check(row)?;
        let row = PhysicalIndex::from_position(row)?;
        let (child, start, end) = match self.container() {
            Some(ContainerStorageRef::List { offsets, elements, .. }) => (elements, usize::try_from(offsets.get(row)?).map_err(|_| layout())?, usize::try_from(offsets.get(PhysicalIndex(row.0.wrapping_add(1)))?).map_err(|_| layout())?),
            Some(ContainerStorageRef::FixedList { size, elements, .. }) => {
                let start = usize::try_from(row.0.wrapping_mul(size)).map_err(|_| layout())?;
                let end = usize::try_from(row.0.wrapping_add(1).wrapping_mul(size)).map_err(|_| layout())?;
                if size < 0 { return Err(layout()); }
                (elements, start, end)
            }
            _ => return Err(unsupported("getListStartIndex")),
        };
        if start > end || end > child.length { return Err(layout()); }
        Ok((child, start, end))
    }
}

pub(super) fn run_end(ends: &PhysicalNode, row: usize) -> Result<usize> {
    ends.check(row)?;
    if let NodeLayout::Scalar(storage) = &ends.storage {
        let data = storage.borrow();
        let value = match ends.scalar_field()?.data_type() {
            DataType::Int16 => i64::from(i16::from_le_bytes(data.lane(row)?)),
            DataType::Int32 => i64::from(i32::from_le_bytes(data.lane(row)?)),
            DataType::Int64 => i64::from_le_bytes(data.lane(row)?),
            _ => return Err(layout()),
        };
        return usize::try_from(value).map_err(|_| layout());
    }
    let array = ends.array()?;
    let value = match array.data_type() {
        DataType::Int16 => i64::from(downcast::<arrow_array::Int16Array>(array)?.value(row)),
        DataType::Int32 => i64::from(downcast::<arrow_array::Int32Array>(array)?.value(row)),
        DataType::Int64 => downcast::<arrow_array::Int64Array>(array)?.value(row),
        _ => return Err(layout()),
    };
    usize::try_from(value).map_err(|_| layout())
}

pub(super) fn check_runs(ends: &PhysicalNode, values: &PhysicalNode, length: usize) -> Result<()> {
    if ends.descriptor.is_nullable() || !matches!(ends.scalar_field()?.data_type(), DataType::Int16 | DataType::Int32 | DataType::Int64) || ends.length != values.length { return Err(layout()); }
    let mut previous = 0;
    for row in 0..ends.length {
        let next = run_end(ends, row)?;
        vector_length(next)?;
        if next <= previous { return Err(layout()); }
        previous = next;
    }
    if previous < length { return Err(layout()); }
    Ok(())
}

pub(super) fn native(field: Arc<Field>, input: PhysicalLayout, memory: &Arc<crate::MemoryBudget>) -> Result<ExecutionVector> {
    let length = match &input {
        PhysicalLayout::Struct { length, .. } | PhysicalLayout::List { length, .. } | PhysicalLayout::FixedSizeList { length, .. } | PhysicalLayout::RunEndEncoded { length, .. } => *length,
        PhysicalLayout::DenseUnion { type_ids, .. } => type_ids.len(),
    };
    vector_length(length)?;
    let (bytes, depth) = super::open::schema_bound(&field, 1)?;
    let container = match &input { PhysicalLayout::Struct { children, .. } | PhysicalLayout::DenseUnion { children, .. } => children.capacity().checked_mul(std::mem::size_of::<ExecutionVector>()).ok_or_else(allocation)?, _ => 0 };
    let metadata = memory.reserve(bytes.checked_mul(depth).and_then(|n| n.checked_mul(8)).and_then(|n| n.checked_add(container)).ok_or_else(allocation)?)?;
    crate::from_field(&field)?;
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata });
    let direct = |vector: ExecutionVector, declaration: &Field| -> Result<Arc<PhysicalNode>> {
        let node = vector.into_direct()?;
        if node.ingress_arrow()? != declaration { return Err(layout()); }
        Ok(node)
    };
    let validity = |length: usize, bits: Option<RetainedBuffer>| -> Result<Option<NullBuffer>> {
        if !field.is_nullable() { return Ok(None); }
        let Some(bits) = bits else { return Ok(None); };
        if length.checked_add(7).ok_or_else(allocation)? / 8 > bits.len() { return Err(layout()); }
        Ok(Some(NullBuffer::new(arrow_buffer::BooleanBuffer::new(owned_buffer(bits), 0, length))))
    };
    let (length, storage) = match (field.data_type(), input) {
        (DataType::Struct(fields), PhysicalLayout::Struct { length, validity: bits, children }) => {
            if fields.len() != children.len() { return Err(layout()); }
            let children = children.into_iter().zip(fields).map(|(child, field)| direct(child, field)).collect::<Result<Vec<_>>>()?;
            if children.iter().any(|child| child.length != length) { return Err(layout()); }
            (length, NodeLayout::Struct { validity: validity(length, bits)?, children })
        }
        (DataType::List(child) | DataType::Map(child, _), PhysicalLayout::List { length, validity: bits, offsets, elements }) => {
            let elements = direct(elements, child)?;
            if length.checked_add(1).and_then(|n| n.checked_mul(4)).is_none_or(|size| size != offsets.len()) { return Err(layout()); }
            let mut previous = 0;
            for row in 0..=length {
                let next = usize::try_from(offsets.i32(row)?).map_err(|_| layout())?;
                if next < previous || next > elements.length { return Err(layout()); }
                previous = next;
            }
            (length, NodeLayout::List { validity: validity(length, bits)?, offsets, elements })
        }
        (DataType::FixedSizeList(child, size), PhysicalLayout::FixedSizeList { length, validity: bits, elements }) => {
            let elements = direct(elements, child)?;
            let size = usize::try_from(*size).map_err(|_| layout())?;
            if length.checked_mul(size) != Some(elements.length) { return Err(layout()); }
            (length, NodeLayout::FixedList { validity: validity(length, bits)?, size, elements })
        }
        (DataType::Union(fields, arrow_schema::UnionMode::Dense), PhysicalLayout::DenseUnion { type_ids, offsets, children }) => {
            let length = type_ids.len();
            if fields.len() != children.len() || length.checked_mul(4) != Some(offsets.len()) { return Err(layout()); }
            let children = children.into_iter().zip(fields.iter()).map(|(child, (_, field))| direct(child, field)).collect::<Result<Vec<_>>>()?;
            for row in 0..length {
                let id = type_ids.as_slice()[row] as i8;
                if id >= 0 {
                    let index = fields.iter().position(|(candidate, _)| candidate == id).ok_or_else(layout)?;
                    children[index].check(usize::try_from(offsets.i32(row)?).map_err(|_| layout())?)?;
                }
            }
            (length, NodeLayout::Union { type_ids, offsets, children })
        }
        (DataType::RunEndEncoded(ends_field, values_field), PhysicalLayout::RunEndEncoded { length, run_ends, values }) => {
            let ends = direct(run_ends, ends_field)?;
            let values = direct(values, values_field)?;
            (length, NodeLayout::RunEnd(RunEndReader::new(RunStorage { logical_offset: 0, ends, values })))
        }
        _ => return Err(layout()),
    };
    vector_length(length)?;
    let node = super::open::finish_node(field, length, storage, owner)?;
    Ok(ExecutionVector::direct(node))
}

struct BufferLease(RetainedBuffer);
impl AsRef<[u8]> for BufferLease { fn as_ref(&self) -> &[u8] { self.0.as_slice() } }

fn owned_buffer(bytes: RetainedBuffer) -> Buffer {
    let pointer = std::ptr::NonNull::new(bytes.as_slice().as_ptr() as *mut u8).expect("Arrow buffers have a nonnull address");
    let length = bytes.len();
    // SAFETY: the retained owner keeps this immutable pointer valid for the supplied slice length.
    unsafe { Buffer::from_custom_allocation(pointer, length, Arc::new(BufferLease(bytes))) }
}
