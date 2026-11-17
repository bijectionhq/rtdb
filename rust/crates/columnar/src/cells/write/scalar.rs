use super::*;
use rtdb_types::{IntervalUnit, ScalarType};
use super::super::getters::Getter;

pub(super) enum MutableScalarData {
    Null,
    Bits(MutableBits),
    Fixed { width: usize, data: MutableBytes },
    Variable { offsets: MutableBytes, data: MutableBytes, last_offset: i32 },
}
struct ScalarDescriptor { ingress: Arc<Field>, field: Arc<Field>, logical: LogicalType }
pub(super) struct ScalarState {
    descriptor: ScalarDescriptor,
    pub(super) count: usize,
    pub(super) validity: Option<MutableBits>,
    pub(super) data: MutableScalarData,
    metadata: Arc<MemoryReservation>,
}
impl ScalarState {
    fn new(ingress: Arc<Field>, memory: Arc<MemoryBudget>) -> Result<Self> {
        let (bound, depth) = super::super::open::schema_bound(&ingress, 1)?;
        let metadata = memory.reserve(bound.checked_mul(depth).and_then(|n| n.checked_mul(16)).ok_or_else(allocation)?)?;
        crate::from_field(&ingress)?;
        let field = Arc::new(ingress.as_ref().clone().with_data_type(super::super::open::scalar_data_type(ingress.data_type())?));
        let logical = crate::execution_field_type(&field)?;
        let data = match field.data_type() {
            DataType::Null => MutableScalarData::Null,
            DataType::Boolean => MutableScalarData::Bits(MutableBits::new(&memory)),
            DataType::Utf8 | DataType::Binary => MutableScalarData::Variable { offsets: MutableBytes::new(&memory), data: MutableBytes::new(&memory), last_offset: 0 },
            data_type => MutableScalarData::Fixed { width: super::super::scalar::fixed_width(data_type).ok_or_else(|| unsupported("ScalarWriter"))?, data: MutableBytes::new(&memory) },
        };
        let validity = (field.is_nullable() && field.data_type() != &DataType::Null).then(|| MutableBits::new(&memory));
        Ok(Self { descriptor: ScalarDescriptor { ingress, field, logical }, count: 0, validity, data, metadata })
    }
    pub(super) fn len(&self) -> usize { self.count }
    pub(super) fn field(&self) -> &Field { &self.descriptor.field }
    pub(super) fn logical(&self) -> &LogicalType { &self.descriptor.logical }
    fn scalar_type(&self) -> Option<&ScalarType> {
        match self.logical() { LogicalType::Scalar(s) => Some(s), LogicalType::Maybe(inner) => match inner.as_ref() { LogicalType::Scalar(s) => Some(s), _ => None }, _ => None }
    }
    pub(super) fn extension(&self) -> bool { self.field().metadata().contains_key(crate::EXTENSION_NAME_KEY) }
    pub(super) fn promote(&mut self, mutation: &mut Mutation) -> WriteResult<()> {
        if self.field().is_nullable() { return Ok(()); }
        let ingress = Arc::new(self.descriptor.ingress.as_ref().clone().with_nullable(true));
        let field = Arc::new(self.descriptor.field.as_ref().clone().with_nullable(true));
        let logical = crate::execution_field_type(&field)?;
        self.descriptor = ScalarDescriptor { ingress, field, logical };
        mutation.mark();
        if !matches!(&self.data, MutableScalarData::Null) {
            self.validity = Some(MutableBits::new(self.metadata.budget()));
            self.validity.as_mut().expect("validity installed").ones(self.count, mutation)?;
        }
        Ok(())
    }
    pub(super) fn increment_count(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        self.count = checked_count(self.count.checked_add(count).ok_or_else(native_size)?)?;
        if count != 0 { mutation.mark(); }
        Ok(())
    }
    fn next_count(&self) -> WriteResult<()> { checked_count(self.count.checked_add(1).ok_or_else(native_size)?)?; Ok(()) }
    pub(super) fn not_null(&mut self, mutation: &mut Mutation) -> WriteResult<()> {
        if let Some(validity) = &mut self.validity { validity.write_at(self.count, true, mutation)?; }
        self.increment_count(1, mutation)
    }
    fn offset(&mut self, new_offset: i32, mutation: &mut Mutation) -> WriteResult<()> {
        let MutableScalarData::Variable { offsets, last_offset, .. } = &mut self.data else { return Err(unsupported_write("writeOffset")); };
        if self.count == 0 { offsets.append(&0i32.to_le_bytes(), mutation)?; }
        offsets.append(&new_offset.to_le_bytes(), mutation)?;
        *last_offset = new_offset;
        mutation.mark();
        Ok(())
    }
    pub(super) fn undefined(&mut self, mutation: &mut Mutation) -> WriteResult<()> {
        self.next_count()?;
        match &self.data {
            MutableScalarData::Variable { last_offset, .. } => self.offset(*last_offset, mutation)?,
            _ => (),
        }
        if let Some(validity) = &mut self.validity { validity.write_at(self.count, false, mutation)?; }
        match &mut self.data {
            MutableScalarData::Null | MutableScalarData::Variable { .. } => self.increment_count(1, mutation),
            MutableScalarData::Fixed { width, data } => { data.zero(*width, mutation)?; self.increment_count(1, mutation) },
            MutableScalarData::Bits(bits) => {
                let row = self.count;
                self.count += 1;
                mutation.mark();
                bits.write_at(row, false, mutation)
            },
        }
    }
    fn capacity(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        checked_count(count)?;
        match &mut self.data {
            MutableScalarData::Null => { if count > self.count { self.count = count; mutation.mark(); } },
            MutableScalarData::Bits(bits) => {
                if count > self.count { self.count = count; mutation.mark(); }
                if let Some(validity) = &mut self.validity { validity.ensure_capacity(count, mutation)?; }
                bits.ensure_capacity(count, mutation)?;
            },
            MutableScalarData::Fixed { width, data } => {
                if count > self.count {
                    self.count = count;
                    mutation.mark();
                    if let Some(validity) = &mut self.validity { validity.ensure_capacity(count, mutation)?; }
                    let extent = (count as i32).wrapping_mul(*width as i32);
                    if extent > 0 { data.ensure_capacity(extent as usize, mutation)?; }
                }
            },
            MutableScalarData::Variable { .. } => return Err(unsupported_write("ensureCapacity")),
        }
        Ok(())
    }
    fn capacity_after_index(&mut self, index: i32, mutation: &mut Mutation) -> WriteResult<()> {
        let count = index.wrapping_add(1);
        if count > 0 { self.capacity(count as usize, mutation)?; }
        else if let MutableScalarData::Bits(bits) = &mut self.data {
            if let Some(validity) = &mut self.validity { validity.ensure_capacity_signed(count, mutation)?; }
            bits.ensure_capacity_signed(count, mutation)?;
        }
        Ok(())
    }
    pub(super) fn ensure_capacity(&mut self, count: usize, mutation: &mut Mutation) -> WriteResult<()> {
        if self.extension() { return Err(unsupported_write("ensureCapacity")); }
        self.capacity(count, mutation)
    }
    pub(super) fn set_null(&mut self, index: i32, mutation: &mut Mutation) -> WriteResult<()> {
        if self.extension() || matches!(&self.data, MutableScalarData::Variable { .. }) { return Err(unsupported_write("setNull")); }
        self.capacity_after_index(index, mutation)?;
        if let Some(validity) = &mut self.validity { validity.set(usize::try_from(index).map_err(|_| bounds())?, false, mutation)?; }
        Ok(())
    }
    pub(super) fn boolean(&mut self, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        if !matches!(self.scalar_type(), Some(ScalarType::Bool)) { return Err(unsupported_write("writeBoolean")); }
        self.next_count()?;
        if let Some(validity) = &mut self.validity { validity.write_at(self.count, true, mutation)?; }
        let row = self.count;
        self.count += 1;
        mutation.mark();
        let MutableScalarData::Bits(bits) = &mut self.data else { return Err(native_size()); };
        bits.write_at(row, value, mutation)
    }
    pub(super) fn primitive(&mut self, method: &'static str, bytes: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        use ScalarType::*;
        let admitted = match (method, self.scalar_type()) {
            ("writeByte", Some(I8)) | ("writeShort", Some(I16)) | ("writeInt", Some(I32 | DateDay | Time { bit_width: 32, .. } | Interval(IntervalUnit::YearMonth) | Oid | RegClass | RegProc))
            | ("writeLong", Some(I64 | DateMilli | Time { .. } | Timestamp { .. } | Duration(_))) | ("writeFloat", Some(F32)) | ("writeDouble", Some(F64)) => true,
            _ => false,
        };
        if !admitted { return Err(unsupported_write(method)); }
        self.next_count()?;
        let mut converted = [0; 8];
        let bytes = if method == "writeLong" {
            let value = i64::from_le_bytes(bytes.try_into().map_err(|_| native_size())?);
            match self.scalar_type() {
                Some(DateMilli) => { converted.copy_from_slice(&value.wrapping_mul(86_400_000).to_le_bytes()); &converted[..] },
                Some(Time { bit_width: 32, .. }) => { converted[..4].copy_from_slice(&(value as i32).to_le_bytes()); &converted[..4] },
                _ => bytes,
            }
        } else { bytes };
        if method == "writeFloat" { self.not_null(mutation)?; }
        let MutableScalarData::Fixed { data, .. } = &mut self.data else { return Err(native_size()); };
        data.append(bytes, mutation)?;
        if method != "writeFloat" { self.not_null(mutation)?; }
        Ok(())
    }
    pub(super) fn bytes(&mut self, bytes: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        match &self.data {
            MutableScalarData::Fixed { .. } | MutableScalarData::Variable { .. } => (),
            _ => return Err(unsupported_write("writeBytes")),
        }
        self.next_count()?;
        if let MutableScalarData::Variable { last_offset, .. } = &self.data {
            let length = i32::try_from(bytes.len()).map_err(|_| native_size())?;
            self.offset(last_offset.wrapping_add(length), mutation)?;
            self.not_null(mutation)?;
            let MutableScalarData::Variable { data, .. } = &mut self.data else { return Err(native_size()); };
            data.append(bytes, mutation)
        } else {
            let MutableScalarData::Fixed { data, .. } = &mut self.data else { return Err(native_size()); };
            data.append(bytes, mutation)?;
            self.not_null(mutation)
        }
    }
    pub(super) fn long_long(&mut self, high: i64, low: i64, mutation: &mut Mutation) -> WriteResult<()> {
        if self.extension() || !matches!(self.field().data_type(), DataType::FixedSizeBinary(_)) { return Err(unsupported_write("writeLongLong")); }
        self.next_count()?;
        let MutableScalarData::Fixed { data, .. } = &mut self.data else { return Err(native_size()); };
        data.append(&high.to_be_bytes(), mutation)?;
        data.append(&low.to_be_bytes(), mutation)?;
        self.not_null(mutation)
    }
    pub(super) fn setter(&mut self, method: &'static str, index: i32, bytes: &[u8], mutation: &mut Mutation) -> WriteResult<()> {
        if self.extension() || !matches!(&self.data, MutableScalarData::Fixed { .. }) { return Err(unsupported_write(method)); }
        self.capacity_after_index(index, mutation)?;
        let row = usize::try_from(index).map_err(|_| bounds())?;
        if let Some(validity) = &mut self.validity { validity.set(row, true, mutation)?; }
        let MutableScalarData::Fixed { data, .. } = &mut self.data else { return Err(native_size()); };
        data.set(row.checked_mul(bytes.len()).ok_or_else(native_size)?, bytes, mutation)
    }
    pub(super) fn set_boolean(&mut self, index: i32, value: bool, mutation: &mut Mutation) -> WriteResult<()> {
        if !matches!(&self.data, MutableScalarData::Bits(_)) { return Err(unsupported_write("setBoolean")); }
        self.capacity_after_index(index, mutation)?;
        let row = usize::try_from(index).map_err(|_| bounds())?;
        if let Some(validity) = &mut self.validity { validity.set(row, true, mutation)?; }
        let MutableScalarData::Bits(bits) = &mut self.data else { return Err(native_size()); };
        bits.set(row, value, mutation)
    }
    fn clear(&mut self) {
        if let Some(validity) = &mut self.validity { validity.clear(); }
        match &mut self.data {
            MutableScalarData::Null => (), MutableScalarData::Bits(bits) => bits.clear(), MutableScalarData::Fixed { data, .. } => data.clear(),
            MutableScalarData::Variable { offsets, data, last_offset } => { offsets.clear(); data.clear(); *last_offset = 0; },
        }
        self.count = 0;
    }
    fn finish(self) -> Result<ExecutionVector> {
        let validity = self.validity.map(MutableBits::finish).transpose()?;
        let data = match self.data {
            MutableScalarData::Null => NativeScalarData::Null,
            MutableScalarData::Bits(bits) => NativeScalarData::Bits(bits.finish()?),
            MutableScalarData::Fixed { data, .. } => NativeScalarData::Fixed(data.finish()?),
            MutableScalarData::Variable { offsets, data, last_offset } => NativeScalarData::Variable { offsets: offsets.finish()?, data: data.finish()?, last_offset },
        };
        let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: self.metadata });
        let node = super::super::open::finish_node(self.descriptor.ingress, self.count, NodeLayout::Scalar(NativeScalarStorage { validity, data }), owner)?;
        Ok(ExecutionVector::direct(node))
    }
}

pub(crate) struct ScalarWriter {
    scalar: ScalarState,
    state: PublicationState,
}
impl ScalarWriter {
    pub(crate) fn new(ingress: Arc<Field>, memory: Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { scalar: ScalarState::new(ingress, memory)?, state: PublicationState::Ready })
    }
    pub(crate) fn len(&self) -> usize { self.scalar.len() }
    pub(crate) fn field(&self) -> &Field { self.scalar.field() }
    pub(super) fn logical(&self) -> &LogicalType { self.scalar.logical() }
    #[cfg(test)]
    pub(super) fn fixture_state(&mut self) -> &mut ScalarState { &mut self.scalar }
    pub(super) fn ready(&self) -> WriteResult<()> {
        if self.state == PublicationState::NativeAborted { Err(WriteError::Native(aborted())) } else { Ok(()) }
    }
    pub(super) fn operation<T>(&mut self, action: impl FnOnce(&mut ScalarState, &mut Mutation) -> WriteResult<T>) -> WriteResult<T> {
        self.ready()?;
        let mut mutation = Mutation::default();
        let result = action(&mut self.scalar, &mut mutation);
        if matches!(&result, Err(WriteError::Native(_))) && mutation.changed { self.state = PublicationState::NativeAborted; }
        result
    }
    pub(crate) fn write_undefined(&mut self) -> WriteResult<()> { self.operation(|writer, mutation| writer.undefined(mutation)) }
    pub(crate) fn write_null(&mut self) -> WriteResult<()> { self.operation(|writer, mutation| { writer.promote(mutation)?; writer.undefined(mutation) }) }
    pub(crate) fn ensure_capacity(&mut self, count: usize) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.ensure_capacity(count, mutation)) }
    pub(crate) fn set_null(&mut self, index: i32) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.set_null(index, mutation)) }
    pub(crate) fn write_boolean(&mut self, value: bool) -> WriteResult<()> { self.operation(|writer, mutation| writer.boolean(value, mutation)) }
    fn primitive(&mut self, method: &'static str, bytes: &[u8]) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.primitive(method, bytes, mutation)) }
    pub(crate) fn write_byte(&mut self, value: i8) -> WriteResult<()> { self.primitive("writeByte", &value.to_le_bytes()) }
    pub(crate) fn write_short(&mut self, value: i16) -> WriteResult<()> { self.primitive("writeShort", &value.to_le_bytes()) }
    pub(crate) fn write_int(&mut self, value: i32) -> WriteResult<()> { self.primitive("writeInt", &value.to_le_bytes()) }
    pub(crate) fn write_long(&mut self, value: i64) -> WriteResult<()> { self.primitive("writeLong", &value.to_le_bytes()) }
    pub(crate) fn write_float(&mut self, value: f32) -> WriteResult<()> { self.primitive("writeFloat", &value.to_le_bytes()) }
    pub(crate) fn write_double(&mut self, value: f64) -> WriteResult<()> { self.primitive("writeDouble", &value.to_le_bytes()) }
    pub(crate) fn write_bytes(&mut self, bytes: &[u8]) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.bytes(bytes, mutation)) }
    pub(crate) fn write_long_long(&mut self, high: i64, low: i64) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.long_long(high, low, mutation)) }
    fn setter(&mut self, method: &'static str, index: i32, bytes: &[u8]) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.setter(method, index, bytes, mutation)) }
    pub(crate) fn set_int(&mut self, index: i32, value: i32) -> WriteResult<()> { self.setter("setInt", index, &value.to_le_bytes()) }
    pub(crate) fn set_long(&mut self, index: i32, value: i64) -> WriteResult<()> { self.setter("setLong", index, &value.to_le_bytes()) }
    pub(crate) fn set_float(&mut self, index: i32, value: f32) -> WriteResult<()> { self.setter("setFloat", index, &value.to_le_bytes()) }
    pub(crate) fn set_double(&mut self, index: i32, value: f64) -> WriteResult<()> { self.setter("setDouble", index, &value.to_le_bytes()) }
    pub(crate) fn set_boolean(&mut self, index: i32, value: bool) -> WriteResult<()> { self.operation(|scalar, mutation| scalar.set_boolean(index, value, mutation)) }
    pub(crate) fn clear(&mut self) {
        self.scalar.clear();
        self.state = PublicationState::Ready;
    }
    pub(crate) fn reader(&self) -> ScalarReader<'_> { ScalarReader { scalar: &self.scalar } }
    pub(crate) fn finish(self) -> Result<ExecutionVector> {
        if self.state == PublicationState::NativeAborted { return Err(aborted()); }
        self.scalar.finish()
    }
}

pub(crate) struct ScalarReader<'a> { scalar: &'a ScalarState }
impl ScalarReader<'_> {
    fn storage(&self) -> ScalarStorageRef<'_> {
        match &self.scalar.data {
            MutableScalarData::Null => ScalarStorageRef::Null,
            MutableScalarData::Bits(bits) => ScalarStorageRef::Bits { data: bits.bytes(), offset: 0 },
            MutableScalarData::Fixed { data, .. } => ScalarStorageRef::Fixed { data: data.bytes(), offset: 0 },
            MutableScalarData::Variable { offsets, data, .. } => ScalarStorageRef::Variable { offsets: ScalarOffsets::Native(offsets.bytes()), data: data.bytes() },
        }
    }
    fn row(&self, row: i32) -> Result<usize> { let row = usize::try_from(row).map_err(|_| index_error())?; if row >= self.scalar.count { Err(index_error()) } else { Ok(row) } }
    pub(crate) fn is_null(&self, row: i32) -> Result<bool> {
        if matches!(&self.scalar.data, MutableScalarData::Null) { return Ok(true); }
        match &self.scalar.validity { None => Ok(false), Some(bits) => Ok(!native_scalar::bit(bits.bytes(), self.row(row)?)?) }
    }
    fn primitive(&self, getter: Getter, row: i32, checks: NullChecks) -> VectorReadResult<usize> {
        if !getter.admits(self.scalar.logical()) { return Err(unsupported(getter.name()).into()); }
        if checks == NullChecks::Enabled && self.is_null(row)? { return Err(Anomaly::fault("xtdb.error/unknown", format!("null at index {row}")).into()); }
        Ok(self.row(row)?)
    }
    pub(crate) fn get_boolean(&self, row: i32, checks: NullChecks) -> VectorReadResult<bool> { Ok(self.storage().bit(self.primitive(Getter::Boolean, row, checks)?)?) }
    pub(crate) fn get_byte(&self, row: i32, checks: NullChecks) -> VectorReadResult<i8> { Ok(self.storage().lane::<1>(self.primitive(Getter::Byte, row, checks)?)?[0] as i8) }
    pub(crate) fn get_short(&self, row: i32, checks: NullChecks) -> VectorReadResult<i16> { Ok(i16::from_le_bytes(self.storage().lane(self.primitive(Getter::Short, row, checks)?)?)) }
    pub(crate) fn get_int(&self, row: i32, checks: NullChecks) -> VectorReadResult<i32> { Ok(super::super::getters::integer(self.storage(), self.scalar.field().data_type(), self.primitive(Getter::Int, row, checks)?)?) }
    pub(crate) fn get_long(&self, row: i32, checks: NullChecks) -> VectorReadResult<i64> { Ok(super::super::getters::long(self.storage(), self.scalar.field().data_type(), self.primitive(Getter::Long, row, checks)?)?) }
    pub(crate) fn get_float(&self, row: i32, checks: NullChecks) -> VectorReadResult<f32> { Ok(f32::from_le_bytes(self.storage().lane(self.primitive(Getter::Float, row, checks)?)?)) }
    pub(crate) fn get_double(&self, row: i32, checks: NullChecks) -> VectorReadResult<f64> { Ok(super::super::getters::double(self.storage(), self.scalar.field().data_type(), self.primitive(Getter::Double, row, checks)?)?) }
    pub(crate) fn get_bytes(&self, row: i32) -> VectorReadResult<&[u8]> {
        if !matches!(self.scalar.field().data_type(), DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)) { return Err(unsupported("getBytes").into()); }
        Ok(self.storage().bytes(self.row(row)?, self.scalar.field().data_type())?)
    }
}
