use arrow_schema::IntervalUnit;
use rtdb_types::{DecimalValue, ScalarType};
use super::*;

#[derive(Clone, Copy)]
enum RunGetter { Pointer, LongLong }
impl RunGetter {
    fn name(self) -> &'static str { match self { Self::Pointer => "getPointer", Self::LongLong => "LongLongVectorReader" } }
}

fn long_long_reader(node: &PhysicalNode) -> bool {
    matches!(&node.storage, NodeLayout::RunEnd { .. })
        || matches!(node.descriptor.scalar_field().map(Field::data_type), Some(DataType::FixedSizeBinary(_))) && !node.descriptor.metadata().contains_key(crate::EXTENSION_NAME_KEY)
}

impl<'a> CellRef<'a> {
    /// Borrows the getPointer byte span for fixed-width or variable-width storage without a validity check.
    /// Fixed-width pointer strides wrap as i32 before retained-span checks.
    /// Indirect/run-end and extension readers delegate; Boolean, Null, containers and union/named-leg readers reject it.
    pub fn byte_span(&self) -> Result<&'a [u8]> {
        let cell = self.run_delegate(RunGetter::Pointer)?;
        let data_type = cell.node.descriptor.scalar_field().ok_or_else(|| unsupported("getPointer"))?.data_type();
        if matches!(data_type, DataType::Utf8 | DataType::Binary) {
            return bytes(&cell);
        }
        let width = fixed_width(data_type).ok_or_else(|| unsupported("getPointer"))?;
        let cell = cell.payload()?;
        fixed_bytes(&cell, width, super::native_scalar::wrapped_stride(cell.row, width)?, width)
    }

    /// Reads the first big-endian long at the fixed binary reader's 16-byte row stride with i32 wrapping, without a validity check.
    /// Plain/indirect fixed binary and run-end wrappers expose it; extension and union readers do not.
    /// A read beyond the retained byte span returns a bounds error.
    pub fn get_long_long_high(&self) -> Result<i64> { self.long_long(0) }

    /// Reads the second big-endian long under the same admission and bounds as get_long_long_high.
    pub fn get_long_long_low(&self) -> Result<i64> { self.long_long(8) }

    fn long_long(&self, part: usize) -> Result<i64> {
        let cell = self.run_delegate(RunGetter::LongLong)?;
        if cell.node.descriptor.metadata().contains_key(crate::EXTENSION_NAME_KEY) { return Err(unsupported("LongLongVectorReader")); }
        let Some(DataType::FixedSizeBinary(width)) = cell.node.descriptor.scalar_field().map(Field::data_type) else { return Err(unsupported("LongLongVectorReader")); };
        let width = usize::try_from(*width).map_err(|_| layout())?;
        let cell = cell.payload()?;
        let lane = (cell.row as i32).wrapping_mul(2).wrapping_add((part / 8) as i32);
        let offset = usize::try_from(lane.wrapping_mul(8)).map_err(|_| index_error())?;
        Ok(i64::from_be_bytes(fixed_bytes(&cell, width, offset, 8)?.try_into().map_err(|_| layout())?))
    }

    fn run_delegate(&self, getter: RunGetter) -> Result<ResolvedCell<'a>> {
        let ReaderView::Direct(node) = &self.vector.view else { return Err(unsupported(getter.name())); };
        let mut node = node.as_ref();
        let mut row = self.vector.address.row(self.index)?;
        loop {
            match &node.storage {
                NodeLayout::RunEnd { values, .. } => {
                    if matches!(getter, RunGetter::LongLong) && !long_long_reader(values) { return Err(unsupported(getter.name())); }
                    row = node.run_index(row)?;
                    node = values;
                },
                NodeLayout::Union { .. } => return Err(unsupported(getter.name())),
                _ => return Ok(ResolvedCell { node, row }),
            }
        }
    }

    fn numeric(&self) -> Result<ResolvedCell<'a>> {
        let (ReaderView::Direct(node), Address::Identity { .. }) = (&self.vector.view, &self.vector.address) else { return Err(unsupported("NumericVector")); };
        let scalar = match node.descriptor.logical_type() { LogicalType::Maybe(inner) => inner.as_ref(), other => other };
        if !matches!(scalar, LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::F32 | ScalarType::F64 | ScalarType::Decimal { .. })) { return Err(unsupported("NumericVector")); }
        Ok(ResolvedCell { node, row: PhysicalIndex::from_position(self.index)? })
    }

    /// Reads a concrete IntegerVector as i32, narrowing Long with Java wrapping.
    /// Indirect, run-end, union and extension classes do not expose the numeric-class methods.
    pub fn get_as_int(&self, checks: NullChecks) -> VectorReadResult<i32> { Ok(self.get_as_long(checks)? as i32) }

    /// Reads a concrete IntegerVector as i64, applying its primitive null-check policy.
    /// Numeric-class admission is separate from the forwarding VectorReader primitive methods.
    pub fn get_as_long(&self, checks: NullChecks) -> VectorReadResult<i64> {
        let cell = self.numeric()?;
        match cell.node.scalar_field()?.data_type() {
            DataType::Int8 => Ok(i64::from(self.get_byte(checks)?)),
            DataType::Int16 => Ok(i64::from(self.get_short(checks)?)),
            DataType::Int32 => Ok(i64::from(self.get_int(checks)?)),
            DataType::Int64 => self.get_long(checks),
            _ => Err(unsupported("IntegerVector").into()),
        }
    }

    /// Converts a concrete NumericVector to f32; Decimal reads its coefficient even on null rows.
    /// Primitive classes apply the requested null check; temporary decimal conversion storage is budgeted.
    pub fn get_as_float(&self, checks: NullChecks) -> VectorReadResult<f32> {
        let cell = self.numeric()?;
        match cell.node.scalar_field()?.data_type() {
            DataType::Float32 => self.get_float(checks),
            DataType::Float64 => Ok(self.get_double(checks)? as f32),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(self.get_as_long(checks)? as f32),
            _ => {
                let _scratch = self.vector.budget().reserve(4096)?;
                decimal_text(&cell)?.parse().map_err(|_| layout().into())
            }
        }
    }

    /// Converts a concrete NumericVector to f64 with the same admission and null behavior as get_as_float.
    pub fn get_as_double(&self, checks: NullChecks) -> VectorReadResult<f64> {
        let cell = self.numeric()?;
        match cell.node.scalar_field()?.data_type() {
            DataType::Float32 | DataType::Float64 => self.get_double(checks),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(self.get_as_long(checks)? as f64),
            _ => {
                let _scratch = self.vector.budget().reserve(4096)?;
                decimal_text(&cell)?.parse().map_err(|_| layout().into())
            }
        }
    }

    /// Materializes the concrete DecimalVector coefficient and full logical parameters without a null guard.
    /// Coefficient offsets use wide index multiplication.
    /// Only direct Decimal readers expose getDecimal; the returned logical value retains its allocation lease.
    pub fn get_decimal(&self) -> Result<OwnedValue> {
        let cell = self.numeric()?;
        if crate::decimal_parameters(cell.node.scalar_field()?.data_type()).is_none() { return Err(unsupported("getDecimal")); }
        let reservation = self.vector.budget().reserve(4096)?;
        let parameters = crate::decimal_field_parameters(cell.node.scalar_field()?)?.ok_or_else(|| unsupported("getDecimal"))?;
        let width = usize::from(parameters.bit_width / 8);
        let cell = cell.payload()?;
        let raw = fixed_bytes(&cell, width, cell.row.checked_mul(width).ok_or_else(allocation)?, width)?;
        let value = Value::Decimal(DecimalValue { unscaled: raw.iter().rev().copied().collect(), precision: parameters.precision, scale: parameters.scale, bit_width: parameters.bit_width });
        Ok(OwnedValue { value, _reservation: reservation })
    }
}

fn bytes<'a>(cell: &ResolvedCell<'a>) -> Result<&'a [u8]> {
    super::native_scalar::ScalarStorageRef::node(cell.node)?.bytes(cell.payload()?.row, cell.node.scalar_field()?.data_type())
}

fn fixed_bytes<'a>(cell: &PayloadCell<'a>, _width: usize, relative: usize, length: usize) -> Result<&'a [u8]> {
    super::native_scalar::ScalarStorageRef::node(cell.node)?.fixed(relative, length)
}

pub(super) fn fixed_width(data_type: &DataType) -> Option<usize> {
    Some(match data_type {
        DataType::Int8 | DataType::UInt8 => 1,
        DataType::Int16 | DataType::UInt16 => 2,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 | DataType::Time32(_) | DataType::Decimal32(_, _) | DataType::Interval(IntervalUnit::YearMonth) => 4,
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) | DataType::Duration(_) | DataType::Decimal64(_, _) | DataType::Interval(IntervalUnit::DayTime) => 8,
        DataType::Decimal128(_, _) | DataType::Interval(IntervalUnit::MonthDayNano) => 16,
        DataType::Decimal256(_, _) => 32,
        DataType::FixedSizeBinary(size) => usize::try_from(*size).ok()?,
        _ => return None,
    })
}

fn decimal_text(cell: &ResolvedCell<'_>) -> Result<String> {
    let parameters = crate::decimal_field_parameters(cell.node.scalar_field()?)?.ok_or_else(|| unsupported("NumericVector"))?;
    let width = usize::from(parameters.bit_width / 8);
    let cell = cell.payload()?;
    let raw = fixed_bytes(&cell, width, cell.row.checked_mul(width).ok_or_else(allocation)?, width)?;
    let coefficient = match width {
        4 => i32::from_le_bytes(raw.try_into().map_err(|_| layout())?).to_string(),
        8 => i64::from_le_bytes(raw.try_into().map_err(|_| layout())?).to_string(),
        16 => i128::from_le_bytes(raw.try_into().map_err(|_| layout())?).to_string(),
        32 => arrow_buffer::i256::from_le_bytes(raw.try_into().map_err(|_| layout())?).to_string(),
        _ => return Err(layout()),
    };
    Ok(format!("{coefficient}e{}", -i64::from(parameters.scale)))
}
