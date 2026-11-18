use std::cmp::Ordering;
use super::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceScalarReadFailure {
    Unsupported { declaration: SourceScalarDeclaration, method: &'static str },
    ValidityByteBounds { offset: i32, capacity: usize },
    SelectionIndexBounds { index: i32, length: i32 },
    LongLongLaneBounds { offset: i32, capacity: usize },
    LongLongReceiverClass { declaration: SourceScalarDeclaration },
    PointerComparisonNegativeLength { operand: PointerComparisonOperand, length: i64 },
    PointerComparisonBounds { operand: PointerComparisonOperand, offset: i64, length: i64, capacity: usize },
    PointerHashNegativeLength { length: i64 },
    PointerHashBounds { offset: i64, length: i64, capacity: usize },
    NegativeViewLength(i64),
    UuidFirstLongUnderflow,
    UuidSecondLongUnderflow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PointerComparisonOperand { Left, Right }

#[derive(Debug)]
pub(crate) enum SourceScalarReadError { Reference(SourceScalarReadFailure), Native(Anomaly) }
pub(crate) type SourceScalarReadResult<T> = std::result::Result<T, SourceScalarReadError>;

#[derive(Clone, Copy)]
pub(super) enum LongLongLane { High, Low }
impl LongLongLane {
    pub(super) fn offset(self, row: i32) -> i32 {
        let lane = match self { Self::High => 0, Self::Low => 1 };
        row.wrapping_mul(2).wrapping_add(lane).wrapping_mul(8)
    }
}

pub(crate) struct SourceScalarReader<'a> {
    declaration: SourceScalarDeclaration,
    backing: &'a [u8],
    validity: Option<&'a [u8]>,
}
impl<'a> SourceScalarReader<'a> {
    pub(crate) fn new(vector: &'a ExecutionVector) -> Result<Option<Self>> {
        let (ReaderView::Direct(node), Address::Identity { .. }) = (&vector.view, &vector.address) else { return Ok(None); };
        let Some(declaration) = node.descriptor.scalar().and_then(|scalar| scalar.source_declaration()) else { return Ok(None); };
        let NodeLayout::Scalar(storage) = &node.storage else { return Ok(None); };
        let native_scalar::NativeScalarData::Fixed(bytes) = &storage.data else { return Err(layout()); };
        let validity = storage.validity.as_ref().map(|bits| bits.bytes.as_slice());
        Ok(Some(Self::fixed(declaration, bytes.bytes.as_slice(), validity)))
    }
    pub(super) fn fixed(declaration: SourceScalarDeclaration, backing: &'a [u8], validity: Option<&'a [u8]>) -> Self {
        Self { declaration, backing, validity }
    }
    pub(crate) fn slice(&self, start: i32, length: i32) -> SourceScalarSlice<'a> {
        SourceScalarSlice { inner: Self::fixed(self.declaration, self.backing, self.validity), start, length }
    }
    pub(crate) fn select<'indices>(&self, indices: &'indices [i32]) -> Result<SourceScalarSelection<'a, 'indices>> {
        vector_length(indices.len())?;
        Ok(SourceScalarSelection { inner: Self::fixed(self.declaration, self.backing, self.validity), indices })
    }
    pub(crate) fn get_pointer(&self, row: i32) -> SourceBytePointer<'a> {
        let width = self.declaration.fixed_width();
        SourceBytePointer { backing: self.backing, offset: i64::from(row.wrapping_mul(width)), length: i64::from(width), cached_hash: None }
    }
    pub(crate) fn get_pointer_into<'slot, 'region>(
        &self, row: i32, reuse: &'slot mut SourceBytePointer<'region>,
    ) -> &'slot mut SourceBytePointer<'region>
    where
        'a: 'region,
    {
        *reuse = self.get_pointer(row);
        reuse
    }
    pub(crate) fn get_bytes(&self, row: i32) -> SourceScalarReadResult<SourceByteView<'a>> {
        if !self.declaration.bytes_method() {
            return Err(SourceScalarReadError::Reference(SourceScalarReadFailure::Unsupported { declaration: self.declaration, method: "getBytes" }));
        }
        let pointer = self.get_pointer(row);
        let length = usize::try_from(pointer.length).map_err(|_| SourceScalarReadError::Reference(SourceScalarReadFailure::NegativeViewLength(pointer.length)))?;
        Ok(SourceByteView { backing: pointer.backing, offset: pointer.offset, length })
    }
    pub(crate) fn get_long_long_high(&self, row: i32) -> Option<SourceScalarReadResult<i64>> {
        self.supports_long_long().then(|| self.read_long_long(row, LongLongLane::High))
    }
    pub(crate) fn get_long_long_low(&self, row: i32) -> Option<SourceScalarReadResult<i64>> {
        self.supports_long_long().then(|| self.read_long_long(row, LongLongLane::Low))
    }
    fn supports_long_long(&self) -> bool { matches!(self.declaration, SourceScalarDeclaration::FixedBinary(_)) }
    fn require_long_long(&self) -> SourceScalarReadResult<()> {
        if self.supports_long_long() { return Ok(()); }
        Err(SourceScalarReadError::Reference(SourceScalarReadFailure::LongLongReceiverClass {
            declaration: self.declaration,
        }))
    }
    fn read_long_long(&self, row: i32, lane: LongLongLane) -> SourceScalarReadResult<i64> {
        let offset = lane.offset(row);
        let bytes = usize::try_from(offset).ok()
            .and_then(|start| self.backing.get(start..))
            .and_then(|tail| tail.first_chunk::<8>())
            .ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::LongLongLaneBounds {
                offset, capacity: self.backing.len(),
            }))?;
        Ok(i64::from_be_bytes(*bytes))
    }
    pub(crate) fn binary_object0(&self, row: i32, memory: &Arc<crate::MemoryBudget>) -> Option<SourceScalarReadResult<OwnedValue>> {
        self.declaration.bytes_method().then(|| self.read_binary_object(row, memory))
    }

    pub(crate) fn binary_object(&self, row: i32, memory: &Arc<crate::MemoryBudget>) -> Option<SourceScalarReadResult<Option<OwnedValue>>> {
        self.declaration.bytes_method().then(|| {
            if let Some(validity) = self.validity {
                let offset = row >> 3;
                let byte = usize::try_from(offset).ok().and_then(|index| validity.get(index))
                    .ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::ValidityByteBounds {
                        offset, capacity: validity.len(),
                    }))?;
                if byte & (1 << (row & 7)) == 0 { return Ok(None); }
            }
            self.read_binary_object(row, memory).map(Some)
        })
    }

    fn read_binary_object(&self, row: i32, memory: &Arc<crate::MemoryBudget>) -> SourceScalarReadResult<OwnedValue> {
        let view = self.get_bytes(row)?;
        let source = view.demand_native()?;
        let bound = source.len().checked_mul(4).and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(|| SourceScalarReadError::Native(allocation()))?;
        let copy_reservation = memory.reserve(bound).map_err(SourceScalarReadError::Native)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(source.len()).map_err(|cause| SourceScalarReadError::Native(Anomaly::incorrect("rtdb/allocation-failed", cause.to_string())))?;
        bytes.extend_from_slice(source);
        if matches!(self.declaration, SourceScalarDeclaration::FixedBinary(_)) {
            return Ok(OwnedValue { value: Value::FixedSizeBinary(bytes), _reservation: copy_reservation });
        }
        let high = bytes.get(..8).ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::UuidFirstLongUnderflow))?;
        let low = bytes.get(8..16).ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::UuidSecondLongUnderflow))?;
        let mut uuid = [0; 16];
        uuid[..8].copy_from_slice(high);
        uuid[8..].copy_from_slice(low);
        let reservation = memory.reserve(4096).map_err(SourceScalarReadError::Native)?;
        let result = OwnedValue { value: Value::Uuid(uuid), _reservation: reservation };
        drop(bytes);
        drop(copy_reservation);
        Ok(result)
    }
}

pub(crate) struct SourceScalarSlice<'a> {
    inner: SourceScalarReader<'a>,
    start: i32,
    length: i32,
}
impl<'a> SourceScalarSlice<'a> {
    pub(crate) fn value_count(&self) -> i32 { self.length }
    pub(crate) fn get_pointer(&self, row: i32) -> SourceBytePointer<'a> {
        self.inner.get_pointer(self.start.wrapping_add(row))
    }
    pub(crate) fn get_pointer_into<'slot, 'region>(
        &self, row: i32, reuse: &'slot mut SourceBytePointer<'region>,
    ) -> &'slot mut SourceBytePointer<'region>
    where
        'a: 'region,
    {
        self.inner.get_pointer_into(self.start.wrapping_add(row), reuse)
    }
    pub(crate) fn get_bytes(&self, row: i32) -> SourceScalarReadResult<SourceByteView<'a>> {
        self.inner.get_bytes(self.start.wrapping_add(row))
    }
    pub(crate) fn get_long_long_high(&self, row: i32) -> SourceScalarReadResult<i64> {
        self.read_long_long(row, LongLongLane::High)
    }
    pub(crate) fn get_long_long_low(&self, row: i32) -> SourceScalarReadResult<i64> {
        self.read_long_long(row, LongLongLane::Low)
    }
    fn read_long_long(&self, row: i32, lane: LongLongLane) -> SourceScalarReadResult<i64> {
        self.inner.require_long_long()?;
        self.inner.read_long_long(self.start.wrapping_add(row), lane)
    }
    pub(crate) fn binary_object(&self, row: i32, memory: &Arc<crate::MemoryBudget>) -> Option<SourceScalarReadResult<Option<OwnedValue>>> {
        self.inner.binary_object(self.start.wrapping_add(row), memory)
    }
}

pub(crate) struct SourceScalarSelection<'a, 'indices> {
    inner: SourceScalarReader<'a>,
    indices: &'indices [i32],
}
impl<'a> SourceScalarSelection<'a, '_> {
    pub(crate) fn value_count(&self) -> i32 { self.indices.len() as i32 }

    fn index(&self, row: i32) -> SourceScalarReadResult<i32> {
        usize::try_from(row).ok().and_then(|index| self.indices.get(index)).copied()
            .ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::SelectionIndexBounds {
                index: row, length: self.value_count(),
            }))
    }

    pub(crate) fn get_bytes(&self, row: i32) -> SourceScalarReadResult<SourceByteView<'a>> {
        self.inner.get_bytes(self.index(row)?)
    }

    pub(crate) fn get_pointer(&self, row: i32) -> SourceScalarReadResult<SourceBytePointer<'a>> {
        Ok(self.inner.get_pointer(self.index(row)?))
    }

    pub(crate) fn get_pointer_into<'slot, 'region>(
        &self, row: i32, reuse: &'slot mut SourceBytePointer<'region>,
    ) -> SourceScalarReadResult<&'slot mut SourceBytePointer<'region>>
    where
        'a: 'region,
    {
        Ok(self.inner.get_pointer_into(self.index(row)?, reuse))
    }

    pub(crate) fn get_long_long_high(&self, row: i32) -> SourceScalarReadResult<i64> {
        self.read_long_long(row, LongLongLane::High)
    }

    pub(crate) fn get_long_long_low(&self, row: i32) -> SourceScalarReadResult<i64> {
        self.read_long_long(row, LongLongLane::Low)
    }

    fn read_long_long(&self, row: i32, lane: LongLongLane) -> SourceScalarReadResult<i64> {
        self.inner.require_long_long()?;
        self.inner.read_long_long(self.index(row)?, lane)
    }
}

pub(crate) struct SourceBytePointer<'a> {
    backing: &'a [u8],
    offset: i64,
    length: i64,
    cached_hash: Option<i32>,
}
impl<'a> SourceBytePointer<'a> {
    pub(crate) fn from_int_arrow19_default(vector: &'a ExecutionVector, row: i32) -> Result<Option<Self>> {
        let (ReaderView::Direct(node), Address::Identity { .. }) = (&vector.view, &vector.address) else { return Ok(None); };
        if node.descriptor.scalar().and_then(|scalar| scalar.count_class()) != Some(CountScalarClass::IntArrow19Default) { return Ok(None); }
        let NodeLayout::Scalar(storage) = &node.storage else { return Err(layout()); };
        let native_scalar::NativeScalarData::Fixed(bytes) = &storage.data else { return Err(layout()); };
        Ok(Some(Self::int(bytes.bytes.as_slice(), row)))
    }
    pub(super) fn int(backing: &'a [u8], row: i32) -> Self { Self { backing, offset: i64::from(row.wrapping_mul(4)), length: 4, cached_hash: None } }
    pub(crate) fn offset(&self) -> i64 { self.offset }
    pub(crate) fn length(&self) -> i64 { self.length }
    pub(crate) fn compare_to(&self, right: &SourceBytePointer<'_>) -> SourceScalarReadResult<Ordering> {
        let (left, right) = self.comparison_spans(right)?;
        Ok(left.cmp(right))
    }
    pub(crate) fn equals(&self, right: &SourceBytePointer<'_>) -> SourceScalarReadResult<bool> {
        if std::ptr::eq(self, right) { return Ok(true); }
        let (left, right) = self.comparison_spans(right)?;
        Ok(left == right)
    }
    pub(crate) fn hash_code(&mut self) -> SourceScalarReadResult<i32> {
        if let Some(hash) = self.cached_hash { return Ok(hash); }
        let length = usize::try_from(self.length)
            .map_err(|_| SourceScalarReadError::Reference(SourceScalarReadFailure::PointerHashNegativeLength {
                length: self.length,
            }))?;
        let bytes = retained_span(self.backing, self.offset, length)
            .ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::PointerHashBounds {
                offset: self.offset, length: self.length, capacity: self.backing.len(),
            }))?;
        admit_simple_hasher_cursor(length)?;
        let hash = simple_hash(bytes);
        self.cached_hash = Some(hash);
        Ok(hash)
    }
    fn comparison_spans<'right>(
        &self, right: &SourceBytePointer<'right>,
    ) -> SourceScalarReadResult<(&'a [u8], &'right [u8])> {
        let left = self.comparison_span(PointerComparisonOperand::Left)?;
        let right = right.comparison_span(PointerComparisonOperand::Right)?;
        Ok((left, right))
    }
    fn comparison_span(&self, operand: PointerComparisonOperand) -> SourceScalarReadResult<&'a [u8]> {
        let length = usize::try_from(self.length)
            .map_err(|_| SourceScalarReadError::Reference(SourceScalarReadFailure::PointerComparisonNegativeLength {
                operand, length: self.length,
            }))?;
        retained_span(self.backing, self.offset, length)
            .ok_or(SourceScalarReadError::Reference(SourceScalarReadFailure::PointerComparisonBounds {
                operand, offset: self.offset, length: self.length, capacity: self.backing.len(),
            }))
    }
    pub(crate) fn demand_native(&self) -> SourceScalarReadResult<&'a [u8]> {
        let length = usize::try_from(self.length).map_err(|_| unretained())?;
        demand(self.backing, self.offset, length)
    }
}

fn admit_simple_hasher_cursor(length: usize) -> SourceScalarReadResult<()> {
    // The last positive multiple of eight makes Arrow's signed `index + 8` guard wrap.
    if length >= i32::MAX as usize - 7 {
        return Err(SourceScalarReadError::Native(Anomaly::incorrect(
            "rtdb/pointer-hash-cursor-overflow", "Source pointer hash length exceeds Arrow's signed cursor range",
        )));
    }
    Ok(())
}

fn simple_hash(bytes: &[u8]) -> i32 {
    let (lanes, tail) = bytes.as_chunks::<8>();
    let mut hash = 0i32;
    for lane in lanes {
        let word = u64::from_le_bytes(*lane);
        hash = hash.wrapping_mul(37).wrapping_add((word ^ (word >> 32)) as i32);
    }
    let tail = if let Some((lane, tail)) = tail.split_first_chunk::<4>() {
        hash = hash.wrapping_mul(37).wrapping_add(i32::from_le_bytes(*lane));
        tail
    } else { tail };
    for &byte in tail {
        hash = hash.wrapping_mul(37).wrapping_add(i32::from(byte as i8));
    }
    hash
}

pub(crate) struct SourceByteView<'a> { backing: &'a [u8], offset: i64, length: usize }
impl<'a> SourceByteView<'a> {
    pub(crate) fn offset(&self) -> i64 { self.offset }
    pub(crate) fn len(&self) -> usize { self.length }
    pub(crate) fn is_empty(&self) -> bool { self.length == 0 }
    pub(crate) fn demand_native(&self) -> SourceScalarReadResult<&'a [u8]> { demand(self.backing, self.offset, self.length) }
}

fn demand(backing: &[u8], offset: i64, length: usize) -> SourceScalarReadResult<&[u8]> {
    retained_span(backing, offset, length).ok_or_else(unretained)
}

fn retained_span(backing: &[u8], offset: i64, length: usize) -> Option<&[u8]> {
    let offset = usize::try_from(offset).ok()?;
    let end = offset.checked_add(length)?;
    backing.get(offset..end)
}
fn unretained() -> SourceScalarReadError { SourceScalarReadError::Native(Anomaly::incorrect("rtdb/unretained-byte-view", "Source byte view exceeds retained native storage")) }
