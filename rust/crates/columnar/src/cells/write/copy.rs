use super::*;
use super::scalar::{MutableScalarData, ScalarState};

#[derive(Clone, Copy)]
pub(crate) struct ScalarSource<'a> { vector: &'a ExecutionVector, node: &'a PhysicalNode, descriptor: super::super::descriptor::ScalarDescriptorRef<'a> }
impl<'a> ScalarSource<'a> {
    pub(crate) fn new(vector: &'a ExecutionVector) -> WriteResult<Self> {
        let ReaderView::Direct(node) = &vector.view else { return Err(unsupported_write("ScalarSource")); };
        if !matches!(&node.storage, NodeLayout::Leaf { .. } | NodeLayout::Scalar(_)) { return Err(unsupported_write("ScalarSource")); }
        let descriptor = node.descriptor.scalar().ok_or_else(|| unsupported_write("ScalarSource"))?;
        Ok(Self { vector, node, descriptor })
    }
    fn scalar_type(&self) -> &LogicalType { self.descriptor.value_type() }
    fn translate(&self, index: i32) -> WriteResult<i32> { Ok(self.vector.address.translate(PhysicalIndex(index))?.0) }
    fn position(&self, row: i32) -> WriteResult<usize> { Ok(self.node.checked_position(PhysicalIndex(row))?) }
    fn storage(&self) -> WriteResult<ScalarStorageRef<'a>> { Ok(ScalarStorageRef::node(self.node)?) }
    fn payload<T>(&self, result: Result<T>) -> WriteResult<T> {
        result.map_err(|error| if matches!(&self.node.storage, NodeLayout::Scalar(_)) { bounds() } else { WriteError::Native(error) })
    }
    fn valid(&self, row: i32) -> WriteResult<bool> {
        if !self.node.descriptor.is_nullable() { return Ok(true); }
        let position = self.position(row)?;
        match &self.node.storage {
            NodeLayout::Scalar(storage) => match &storage.validity {
                Some(bits) => self.payload(native_scalar::bit(bits.bytes.as_slice(), position)),
                None => Ok(self.node.scalar_field()?.data_type() != &DataType::Null),
            },
            _ => Ok(!self.node.null(PhysicalIndex(row))?),
        }
    }
    fn span(&self, start: i32, count: usize, width: usize) -> WriteResult<&'a [u8]> {
        let row = if count == 0 { position(start)? } else { self.position(start)? };
        if count != 0 { self.node.check(row.checked_add(count - 1).ok_or_else(native_size)?)?; }
        let start = self.payload(native_scalar::wrapped_stride(row, width))?;
        let length = self.payload(native_scalar::wrapped_stride(count, width))?;
        self.payload(self.storage()?.fixed(start, length))
    }
    fn variable_offsets(&self) -> WriteResult<(ScalarOffsets<'a>, &'a [u8])> {
        let ScalarStorageRef::Variable { offsets, data } = self.storage()? else { return Err(WriteError::Native(layout())); };
        Ok((offsets, data))
    }
    fn variable_range_length(&self, start: i32, count: usize) -> WriteResult<i32> {
        let row = self.position(start)?;
        if count != 0 { self.node.check(row.checked_add(count - 1).ok_or_else(native_size)?)?; }
        let (offsets, _) = self.variable_offsets()?;
        let end = self.payload(offsets.get(row.checked_add(count).ok_or_else(native_size)?))?;
        let start = self.payload(offsets.get(row))?;
        Ok(end.wrapping_sub(start))
    }
    fn variable_range(&self, start: i32, count: usize) -> WriteResult<(i32, i32)> {
        let row = self.position(start)?;
        if count != 0 { self.node.check(row.checked_add(count - 1).ok_or_else(native_size)?)?; }
        let (offsets, _) = self.variable_offsets()?;
        Ok((self.payload(offsets.get(row))?, self.payload(offsets.get(row.checked_add(count).ok_or_else(native_size)?))?))
    }
}

#[derive(Clone, Copy)]
enum Rows<'a> { One(i32), Selected(&'a [i32]), Range { start: i32, length: usize } }
impl Rows<'_> {
    fn len(self) -> usize { match self { Self::One(_) => 1, Self::Selected(rows) => rows.len(), Self::Range { length, .. } => length } }
    fn at(self, index: usize) -> i32 { match self { Self::One(row) => row, Self::Selected(rows) => rows[index], Self::Range { start, .. } => start.wrapping_add(index as i32) } }
}
#[derive(Clone, Copy)]
enum CopyKind { Null, Bits, Fixed(usize), Variable }
pub(crate) struct ScalarCopier<'a, 'b> { source: ScalarSource<'a>, destination: &'b mut ScalarWriter, kind: CopyKind }
impl<'a, 'b> ScalarCopier<'a, 'b> {
    pub(crate) fn new(source: ScalarSource<'a>, destination: &'b mut ScalarWriter) -> WriteResult<Self> {
        if source.node.scalar_field()?.data_type() == &DataType::Null {
            destination.ready()?;
            return Ok(Self { source, destination, kind: CopyKind::Null });
        }
        let target = match destination.logical() { LogicalType::Maybe(inner) => inner, logical => logical };
        if source.scalar_type() != target { return Err(WriteError::Reference(ReferenceFailure::InvalidCopySource)); }
        Self::storage_factory(source, destination)
    }
    pub(super) fn storage_factory(source: ScalarSource<'a>, destination: &'b mut ScalarWriter) -> WriteResult<Self> {
        let kind = destination.operation(|writer, mutation| {
            let source_type = source.node.scalar_field()?.data_type();
            let source_extension = source.node.descriptor.metadata().contains_key(crate::EXTENSION_NAME_KEY);
            if writer.extension() && !source_extension { return Err(WriteError::Reference(ReferenceFailure::RequireFailed)); }
            let same_wrapper = source_extension == writer.extension();
            match &writer.data {
                MutableScalarData::Null => Ok(CopyKind::Null),
                MutableScalarData::Bits(_) => {
                    if !same_wrapper || source_type != &DataType::Boolean { return Err(WriteError::Reference(ReferenceFailure::CheckFailed)); }
                    Ok(CopyKind::Bits)
                },
                MutableScalarData::Fixed { width, .. } => {
                    let width = *width;
                    if source.node.descriptor.is_nullable() { writer.promote(mutation)?; }
                    if !same_wrapper || super::super::scalar::fixed_width(source_type) != Some(width) { return Err(WriteError::Reference(ReferenceFailure::CheckFailed)); }
                    Ok(CopyKind::Fixed(width))
                },
                MutableScalarData::Variable { .. } => {
                    if !same_wrapper || !matches!(source_type, DataType::Utf8 | DataType::Binary) { return Err(WriteError::Reference(ReferenceFailure::CheckFailed)); }
                    if source.node.descriptor.is_nullable() { writer.promote(mutation)?; }
                    Ok(CopyKind::Variable)
                },
            }
        })?;
        Ok(Self { source, destination, kind })
    }
    pub(crate) fn copy_row(&mut self, row: i32) -> WriteResult<()> {
        self.destination.ready()?;
        let row = self.source.translate(row)?;
        self.copy(Rows::One(row))
    }
    pub(crate) fn copy_rows(&mut self, rows: &[i32]) -> WriteResult<()> { self.dispatch(Rows::Selected(rows)) }
    pub(crate) fn copy_range(&mut self, start: i32, length: i32) -> WriteResult<()> {
        self.destination.ready()?;
        if matches!(self.kind, CopyKind::Null | CopyKind::Bits | CopyKind::Variable) && matches!(&self.source.vector.address, Address::Identity { .. }) && length <= 0 { return Ok(()); }
        let length = usize::try_from(length).map_err(|_| bounds())?;
        self.dispatch(Rows::Range { start, length })
    }
    fn dispatch(&mut self, rows: Rows<'_>) -> WriteResult<()> {
        self.destination.ready()?;
        checked_count(rows.len())?;
        if matches!(&self.source.vector.address, Address::Identity { .. }) { return self.copy(rows); }
        let _reservation = self.source.vector.budget().reserve(rows.len().checked_mul(std::mem::size_of::<i32>()).and_then(|n| n.checked_add(128)).ok_or_else(native_size)?)?;
        let mut translated = Vec::with_capacity(rows.len());
        for row in 0..rows.len() { translated.push(self.source.translate(rows.at(row))?); }
        self.copy(Rows::Selected(&translated))
    }
    fn copy(&mut self, rows: Rows<'_>) -> WriteResult<()> {
        let source = self.source;
        match self.kind {
            CopyKind::Null => self.destination.operation(|writer, mutation| { for _ in 0..rows.len() { writer.promote(mutation)?; writer.undefined(mutation)?; } Ok(()) }),
            CopyKind::Bits => self.destination.operation(|writer, mutation| {
                for index in 0..rows.len() {
                    if source.node.descriptor.is_nullable() { writer.promote(mutation)?; }
                    let row = rows.at(index);
                    if !source.valid(row)? { writer.promote(mutation)?; writer.undefined(mutation)?; }
                    else { writer.boolean(source.payload(source.storage()?.bit(source.position(row)?))?, mutation)?; }
                }
                Ok(())
            }),
            CopyKind::Fixed(width) => self.destination.operation(|writer, mutation| {
                checked_count(writer.count.checked_add(rows.len()).ok_or_else(native_size)?)?;
                let MutableScalarData::Fixed { data, .. } = &mut writer.data else { return Err(native_size()); };
                data.ensure_writable_i32((rows.len() as i32).wrapping_mul(width as i32), mutation)?;
                if let Some(validity) = &mut writer.validity { copy_validity(validity, source, rows, mutation)?; }
                let MutableScalarData::Fixed { data, .. } = &mut writer.data else { return Err(native_size()); };
                match rows {
                    Rows::Range { start, length } => data.append_reserved(source.span(start, length, width)?, mutation)?,
                    _ => for index in 0..rows.len() { data.append_reserved(source.span(rows.at(index), 1, width)?, mutation)?; },
                }
                writer.increment_count(rows.len(), mutation)
            }),
            CopyKind::Variable => self.destination.operation(|writer, mutation| {
                if matches!(rows, Rows::Range { length: 0, .. }) { return Ok(()); }
                checked_count(writer.count.checked_add(rows.len()).ok_or_else(native_size)?)?;
                let mut length = 0i32;
                match rows {
                    Rows::One(row) => length = source.variable_range_length(row, 1)?,
                    Rows::Range { start, length: count } => length = source.variable_range_length(start, count)?,
                    _ => for row in 0..rows.len() { let (start, end) = source.variable_range(rows.at(row), 1)?; length = length.wrapping_add(end.wrapping_sub(start)); },
                }
                if let Some(validity) = &mut writer.validity { validity.ensure_writable(rows.len(), mutation)?; }
                let MutableScalarData::Variable { offsets, data, .. } = &mut writer.data else { return Err(native_size()); };
                offsets.ensure_writable_i32((rows.len() as i32).wrapping_add(1).wrapping_mul(4), mutation)?;
                data.ensure_writable_i32(length, mutation)?;
                if let Some(validity) = &mut writer.validity { copy_validity(validity, source, rows, mutation)?; }
                match rows {
                    Rows::Range { start, length } => copy_variable_range(writer, source, start, length, mutation),
                    _ => { for index in 0..rows.len() { copy_variable_range(writer, source, rows.at(index), 1, mutation)?; } Ok(()) },
                }
            }),
        }
    }
}

fn copy_validity(destination: &mut MutableBits, source: ScalarSource<'_>, rows: Rows<'_>, mutation: &mut Mutation) -> WriteResult<()> {
    match rows {
        Rows::One(row) if matches!(source.node.scalar_field()?.data_type(), DataType::Utf8 | DataType::Binary) => {
            if source.node.descriptor.is_nullable() { destination.selected(1, |_| source.valid(row), mutation) }
            else { destination.ones(1, mutation) }
        },
        Rows::One(row) => destination.write(source.valid(row)?, mutation),
        _ if !source.node.descriptor.is_nullable() => destination.ones(rows.len(), mutation),
        _ => destination.selected(rows.len(), |index| source.valid(rows.at(index)), mutation),
    }
}
fn copy_variable_range(writer: &mut ScalarState, source: ScalarSource<'_>, start: i32, length: usize, mutation: &mut Mutation) -> WriteResult<()> {
    let MutableScalarData::Variable { offsets, data, last_offset } = &mut writer.data else { return Err(native_size()); };
    if writer.count == 0 { offsets.append_reserved(&0i32.to_le_bytes(), mutation)?; }
    let (src_start, src_end) = source.variable_range(start, length)?;
    let (src_offsets, src_data) = source.variable_offsets()?;
    let delta = last_offset.wrapping_sub(src_start);
    let row = source.position(start)?;
    for index in 0..length { offsets.append_reserved(&source.payload(src_offsets.get(row.checked_add(index + 1).ok_or_else(native_size)?))?.wrapping_add(delta).to_le_bytes(), mutation)?; }
    let data_start = source.payload(usize::try_from(src_start).map_err(|_| index_error()))?;
    let data_length = source.payload(usize::try_from(src_end.wrapping_sub(src_start)).map_err(|_| index_error()))?;
    let bytes = source.payload(native_scalar::span(src_data, data_start, data_length))?;
    data.append_reserved(bytes, mutation)?;
    *last_offset = last_offset.wrapping_add(data_length as i32);
    mutation.mark();
    writer.increment_count(length, mutation)
}
