use super::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceScalarReadFailure {
    Unsupported { declaration: SourceScalarDeclaration, method: &'static str },
    NegativeViewLength(i64),
    UuidFirstLongUnderflow,
    UuidSecondLongUnderflow,
}

#[derive(Debug)]
pub(crate) enum SourceScalarReadError { Reference(SourceScalarReadFailure), Native(Anomaly) }
pub(crate) type SourceScalarReadResult<T> = std::result::Result<T, SourceScalarReadError>;

pub(crate) struct SourceScalarReader<'a> { declaration: SourceScalarDeclaration, backing: &'a [u8] }
impl<'a> SourceScalarReader<'a> {
    pub(crate) fn new(vector: &'a ExecutionVector) -> Result<Option<Self>> {
        let (ReaderView::Direct(node), Address::Identity { .. }) = (&vector.view, &vector.address) else { return Ok(None); };
        let Some(declaration) = node.descriptor.scalar().and_then(|scalar| scalar.source_declaration()) else { return Ok(None); };
        let NodeLayout::Scalar(storage) = &node.storage else { return Ok(None); };
        let native_scalar::NativeScalarData::Fixed(bytes) = &storage.data else { return Err(layout()); };
        Ok(Some(Self::fixed(declaration, bytes.bytes.as_slice())))
    }
    pub(super) fn fixed(declaration: SourceScalarDeclaration, backing: &'a [u8]) -> Self { Self { declaration, backing } }
    pub(crate) fn slice(&self, start: i32, length: i32) -> SourceScalarSlice<'a> {
        SourceScalarSlice { inner: Self::fixed(self.declaration, self.backing), start, length }
    }
    pub(crate) fn get_pointer(&self, row: i32) -> SourceBytePointer<'a> {
        let width = self.declaration.fixed_width();
        SourceBytePointer { backing: self.backing, offset: i64::from(row.wrapping_mul(width)), length: i64::from(width) }
    }
    pub(crate) fn get_bytes(&self, row: i32) -> SourceScalarReadResult<SourceByteView<'a>> {
        if !self.declaration.bytes_method() {
            return Err(SourceScalarReadError::Reference(SourceScalarReadFailure::Unsupported { declaration: self.declaration, method: "getBytes" }));
        }
        let pointer = self.get_pointer(row);
        let length = usize::try_from(pointer.length).map_err(|_| SourceScalarReadError::Reference(SourceScalarReadFailure::NegativeViewLength(pointer.length)))?;
        Ok(SourceByteView { backing: pointer.backing, offset: pointer.offset, length })
    }
    pub(crate) fn binary_object0(&self, row: i32, memory: &Arc<crate::MemoryBudget>) -> Option<SourceScalarReadResult<OwnedValue>> {
        if !self.declaration.bytes_method() { return None; }
        Some((|| {
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
        })())
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
    pub(crate) fn get_bytes(&self, row: i32) -> SourceScalarReadResult<SourceByteView<'a>> {
        self.inner.get_bytes(self.start.wrapping_add(row))
    }
}

pub(crate) struct SourceBytePointer<'a> { backing: &'a [u8], offset: i64, length: i64 }
impl<'a> SourceBytePointer<'a> {
    pub(crate) fn from_int_arrow19_default(vector: &'a ExecutionVector, row: i32) -> Result<Option<Self>> {
        let (ReaderView::Direct(node), Address::Identity { .. }) = (&vector.view, &vector.address) else { return Ok(None); };
        if node.descriptor.scalar().and_then(|scalar| scalar.count_class()) != Some(CountScalarClass::IntArrow19Default) { return Ok(None); }
        let NodeLayout::Scalar(storage) = &node.storage else { return Err(layout()); };
        let native_scalar::NativeScalarData::Fixed(bytes) = &storage.data else { return Err(layout()); };
        Ok(Some(Self::int(bytes.bytes.as_slice(), row)))
    }
    pub(super) fn int(backing: &'a [u8], row: i32) -> Self { Self { backing, offset: i64::from(row.wrapping_mul(4)), length: 4 } }
    pub(crate) fn offset(&self) -> i64 { self.offset }
    pub(crate) fn length(&self) -> i64 { self.length }
    pub(crate) fn demand_native(&self) -> SourceScalarReadResult<&'a [u8]> {
        let length = usize::try_from(self.length).map_err(|_| unretained())?;
        demand(self.backing, self.offset, length)
    }
}

pub(crate) struct SourceByteView<'a> { backing: &'a [u8], offset: i64, length: usize }
impl<'a> SourceByteView<'a> {
    pub(crate) fn offset(&self) -> i64 { self.offset }
    pub(crate) fn len(&self) -> usize { self.length }
    pub(crate) fn is_empty(&self) -> bool { self.length == 0 }
    pub(crate) fn demand_native(&self) -> SourceScalarReadResult<&'a [u8]> { demand(self.backing, self.offset, self.length) }
}

fn demand(backing: &[u8], offset: i64, length: usize) -> SourceScalarReadResult<&[u8]> {
    let offset = usize::try_from(offset).map_err(|_| unretained())?;
    let end = offset.checked_add(length).ok_or_else(unretained)?;
    backing.get(offset..end).ok_or_else(unretained)
}
fn unretained() -> SourceScalarReadError { SourceScalarReadError::Native(Anomaly::incorrect("rtdb/unretained-byte-view", "Source byte view exceeds retained native storage")) }
