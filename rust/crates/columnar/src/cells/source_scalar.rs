use super::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceScalarReadFailure {
    Unsupported { declaration: SourceScalarDeclaration, method: &'static str },
    NegativeViewLength(i64),
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
}

pub(crate) struct SourceBytePointer<'a> { backing: &'a [u8], offset: i64, length: i64 }
impl<'a> SourceBytePointer<'a> {
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
