use super::*;
use super::owner::{MutableScalarData, ScalarState};

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
    fn storage(&self) -> WriteResult<ScalarStorageRef<'a>> { Ok(ScalarStorageRef::node(self.node)?) }
    fn sdk_range(&self, start: i32, count: i32) -> WriteResult<()> {
        if matches!(&self.node.storage, NodeLayout::Leaf { .. }) {
            let count = usize::try_from(count).map_err(|_| index_error())?;
            let row = if count == 0 { usize::try_from(start).map_err(|_| native_size())? } else { self.node.checked_position(PhysicalIndex(start))? };
            if count != 0 { self.node.check(row.checked_add(count - 1).ok_or_else(native_size)?)?; }
        }
        Ok(())
    }
    fn payload<T>(&self, result: Result<T>) -> WriteResult<T> {
        result.map_err(|error| if matches!(&self.node.storage, NodeLayout::Scalar(_)) { bounds() } else { WriteError::Native(error) })
    }
    fn validity(&self) -> WriteResult<CopyValidity<'a>> {
        if !self.node.descriptor.is_nullable() { return Ok(CopyValidity::Absent); }
        Ok(match &self.node.storage {
            NodeLayout::Scalar(storage) => storage.validity.as_ref().map_or(CopyValidity::Absent, |bits| CopyValidity::Bits(BitSource { bytes: bits.bytes.as_slice(), base: 0 })),
            NodeLayout::Leaf { array, .. } => array.nulls().map_or(CopyValidity::SdkAllValid(self.node.count), |bits| CopyValidity::Bits(BitSource { bytes: bits.inner().values(), base: bits.offset() })),
            _ => return Err(WriteError::Native(layout())),
        })
    }
    fn valid(&self, row: i32) -> WriteResult<bool> {
        match self.validity()? {
            CopyValidity::Absent => Ok(true),
            CopyValidity::Bits(bits) => { self.sdk_range(row, 1)?; self.payload(bits.direct(row)) },
            CopyValidity::SdkAllValid(count) => sdk_all_valid(count, row),
        }
    }
    fn boolean(&self, row: i32) -> WriteResult<bool> {
        self.sdk_range(row, 1)?;
        let ScalarStorageRef::Bits { data, offset } = self.storage()? else { return Err(WriteError::Native(layout())); };
        self.payload(BitSource { bytes: data, base: offset }.direct(row))
    }
    fn span(&self, start: i32, count: i32, width: usize) -> WriteResult<&'a [u8]> {
        self.sdk_range(start, count)?;
        let start = self.payload(usize::try_from(start.wrapping_mul(width as i32)).map_err(|_| index_error()))?;
        let length = self.payload(usize::try_from(count.wrapping_mul(width as i32)).map_err(|_| index_error()))?;
        self.payload(self.storage()?.fixed(start, length))
    }
    fn variable_offsets(&self) -> WriteResult<(ScalarOffsets<'a>, &'a [u8])> {
        let ScalarStorageRef::Variable { offsets, data } = self.storage()? else { return Err(WriteError::Native(layout())); };
        Ok((offsets, data))
    }
    fn variable_range_length(&self, start: i32, count: i32) -> WriteResult<i32> {
        self.sdk_range(start, count)?;
        let (offsets, _) = self.variable_offsets()?;
        let end = self.payload(offsets.get_index(PhysicalIndex(start.wrapping_add(count))))?;
        let start = self.payload(offsets.get_index(PhysicalIndex(start)))?;
        Ok(end.wrapping_sub(start))
    }
    fn variable_range(&self, start: i32, count: i32) -> WriteResult<(i32, i32)> {
        self.sdk_range(start, count)?;
        let (offsets, _) = self.variable_offsets()?;
        Ok((self.payload(offsets.get_index(PhysicalIndex(start)))?, self.payload(offsets.get_index(PhysicalIndex(start.wrapping_add(count))))?))
    }
}

#[derive(Clone, Copy)]
struct BitSource<'a> { bytes: &'a [u8], base: usize }
enum CopyValidity<'a> { Absent, Bits(BitSource<'a>), SdkAllValid(SourceCount) }
fn sdk_all_valid(count: SourceCount, row: i32) -> WriteResult<bool> {
    let row = usize::try_from(row).map_err(|_| index_error())?;
    if count.contains(row) { Ok(true) } else { Err(index_error().into()) }
}
impl BitSource<'_> {
    fn position(self, row: i32) -> Result<i64> { i64::try_from(self.base).ok().and_then(|base| base.checked_add(i64::from(row))).ok_or_else(index_error) }
    fn byte(self, index: i64) -> Result<i32> {
        let index = usize::try_from(index).map_err(|_| index_error())?;
        Ok(i32::from(*self.bytes.get(index).ok_or_else(index_error)? as i8))
    }
    fn direct(self, row: i32) -> Result<bool> {
        let position = self.position(row)?;
        Ok((self.byte(position >> 3)? >> (position & 7)) & 1 == 1)
    }
}
struct ValidityReader<'a> { bits: BitSource<'a>, byte: i32, index: i64 }
impl<'a> ValidityReader<'a> {
    fn new(bits: BitSource<'a>) -> Self { Self { bits, byte: -1, index: -1 } }
    fn read(&mut self, row: i32) -> Result<bool> {
        let position = self.bits.position(row)?;
        let index = position / 8;
        if index != self.index { self.index = index; self.byte = self.bits.byte(index)?; }
        Ok((self.byte.wrapping_shr((position % 8) as u32)) & 1 == 1)
    }
}

#[derive(Clone, Copy)]
enum Rows<'a> { One(i32), Selected(&'a [i32]), Range { start: i32, length: i32 } }
impl Rows<'_> {
    fn count(self) -> i32 { match self { Self::One(_) => 1, Self::Selected(rows) => rows.len() as i32, Self::Range { length, .. } => length } }
    fn at(self, index: i32) -> i32 { match self { Self::One(row) => row, Self::Selected(rows) => rows[index as usize], Self::Range { start, .. } => start.wrapping_add(index) } }
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
    pub(crate) fn copy_rows(&mut self, rows: &[i32]) -> WriteResult<()> {
        self.destination.ready()?;
        array_count(rows.len())?;
        self.dispatch(Rows::Selected(rows))
    }
    pub(crate) fn copy_range(&mut self, start: i32, length: i32) -> WriteResult<()> {
        self.dispatch(Rows::Range { start, length })
    }
    fn dispatch(&mut self, rows: Rows<'_>) -> WriteResult<()> {
        self.destination.ready()?;
        if matches!(&self.source.vector.address, Address::Identity { .. }) { return self.copy(rows); }
        let count = rows.count();
        let length = usize::try_from(count).map_err(|_| WriteError::Reference(ReferenceFailure::NegativeArraySize(count)))?;
        let _reservation = self.source.vector.budget().reserve(length.checked_mul(std::mem::size_of::<i32>()).and_then(|n| n.checked_add(128)).ok_or_else(native_size)?)?;
        let mut translated = Vec::with_capacity(length);
        for row in 0..count { translated.push(self.source.translate(rows.at(row))?); }
        self.copy(Rows::Selected(&translated))
    }
    fn copy(&mut self, rows: Rows<'_>) -> WriteResult<()> {
        let source = self.source;
        match self.kind {
            CopyKind::Null => self.destination.operation(|writer, mutation| { for _ in 0..rows.count() { writer.promote(mutation)?; writer.undefined(mutation)?; } Ok(()) }),
            CopyKind::Bits => self.destination.operation(|writer, mutation| {
                for index in 0..rows.count() {
                    if source.node.descriptor.is_nullable() { writer.promote(mutation)?; }
                    let row = rows.at(index);
                    if !source.valid(row)? { writer.promote(mutation)?; writer.undefined(mutation)?; }
                    else { writer.boolean(source.boolean(row)?, mutation)?; }
                }
                Ok(())
            }),
            CopyKind::Fixed(width) => self.destination.operation(|writer, mutation| {
                let MutableScalarData::Fixed { data, .. } = &mut writer.data else { return Err(native_size()); };
                data.ensure_writable_i32(rows.count().wrapping_mul(width as i32), mutation)?;
                if let Some(validity) = &mut writer.validity { copy_validity(validity, source, rows, mutation)?; }
                let MutableScalarData::Fixed { data, .. } = &mut writer.data else { return Err(native_size()); };
                match rows {
                    Rows::Range { start, length } => data.copy_from_reserved(i64::from(length.wrapping_mul(width as i32)), || source.span(start, length, width), mutation)?,
                    _ => for index in 0..rows.count() { data.copy_from_reserved(width as i64, || source.span(rows.at(index), 1, width), mutation)?; },
                }
                writer.increment_count(rows.count(), mutation)
            }),
            CopyKind::Variable => self.destination.operation(|writer, mutation| {
                if matches!(rows, Rows::Range { length, .. } if length <= 0) { return Ok(()); }
                let mut length = 0i32;
                match rows {
                    Rows::One(row) => length = source.variable_range_length(row, 1)?,
                    Rows::Range { start, length: count } => length = source.variable_range_length(start, count)?,
                    _ => for row in 0..rows.count() { let (start, end) = source.variable_range(rows.at(row), 1)?; length = length.wrapping_add(end.wrapping_sub(start)); },
                }
                if let Some(validity) = &mut writer.validity { validity.ensure_writable(rows.count(), mutation)?; }
                let MutableScalarData::Variable { offsets, data, .. } = &mut writer.data else { return Err(native_size()); };
                offsets.ensure_writable_i32(rows.count().wrapping_add(1).wrapping_mul(4), mutation)?;
                data.ensure_writable_i32(length, mutation)?;
                if let Some(validity) = &mut writer.validity { copy_validity(validity, source, rows, mutation)?; }
                match rows {
                    Rows::Range { start, length } => copy_variable_range(writer, source, start, length, mutation),
                    _ => { for index in 0..rows.count() { copy_variable_range(writer, source, rows.at(index), 1, mutation)?; } Ok(()) },
                }
            }),
        }
    }
}

fn copy_validity(destination: &mut MutableBits, source: ScalarSource<'_>, rows: Rows<'_>, mutation: &mut Mutation) -> WriteResult<()> {
    match rows {
        Rows::One(row) if !matches!(source.node.scalar_field()?.data_type(), DataType::Utf8 | DataType::Binary) => destination.write(source.valid(row)?, mutation),
        _ => match source.validity()? {
            CopyValidity::Absent => destination.ones(rows.count(), mutation),
            CopyValidity::Bits(bits) => {
                let mut reader = ValidityReader::new(bits);
                destination.selected(rows.count(), |index| { let row = rows.at(index); source.sdk_range(row, 1)?; source.payload(reader.read(row)) }, mutation)
            },
            CopyValidity::SdkAllValid(count) => destination.selected(rows.count(), |index| sdk_all_valid(count, rows.at(index)), mutation),
        },
    }
}
fn copy_variable_range(writer: &mut ScalarState, source: ScalarSource<'_>, start: i32, length: i32, mutation: &mut Mutation) -> WriteResult<()> {
    let MutableScalarData::Variable { offsets, data, last_offset } = &mut writer.data else { return Err(native_size()); };
    if writer.count.is_zero() { offsets.append_primitive_reserved(&0i32.to_le_bytes(), mutation)?; }
    let (src_start, src_end) = source.variable_range(start, length)?;
    let (src_offsets, src_data) = source.variable_offsets()?;
    let delta = last_offset.wrapping_sub(src_start);
    for index in 0..length {
        let offset = source.payload(src_offsets.get_index(PhysicalIndex(start.wrapping_add(index).wrapping_add(1))))?.wrapping_add(delta);
        offsets.append_primitive_reserved(&offset.to_le_bytes(), mutation)?;
    }
    let data_length = src_end.wrapping_sub(src_start);
    data.copy_from_reserved(i64::from(data_length), || {
        let data_start = source.payload(usize::try_from(src_start).map_err(|_| index_error()))?;
        let data_length = source.payload(usize::try_from(data_length).map_err(|_| index_error()))?;
        source.payload(native_scalar::span(src_data, data_start, data_length))
    }, mutation)?;
    *last_offset = last_offset.wrapping_add(data_length);
    mutation.mark();
    writer.increment_count(length, mutation)
}
