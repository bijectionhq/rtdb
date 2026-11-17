use super::native_scalar::ScalarStorageRef;
use rtdb_types::{IntervalUnit, ScalarType};

use super::*;

#[derive(Clone, Copy)]
pub(super) enum Getter { Boolean, Byte, Short, Int, Long, Float, Double }

impl Getter {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Boolean => "getBoolean", Self::Byte => "getByte", Self::Short => "getShort", Self::Int => "getInt",
            Self::Long => "getLong", Self::Float => "getFloat", Self::Double => "getDouble",
        }
    }

    pub(super) fn admits(self, logical: &LogicalType) -> bool {
        let logical = if let LogicalType::Maybe(inner) = logical { inner.as_ref() } else { logical };
        let LogicalType::Scalar(scalar) = logical else { return false; };
        use ScalarType::*;
        match self {
            Self::Boolean => matches!(scalar, Bool),
            Self::Byte => matches!(scalar, I8),
            Self::Short => matches!(scalar, I16),
            Self::Int => matches!(scalar, I16 | I32 | DateDay | Time { bit_width: 32, .. } | Interval(IntervalUnit::YearMonth) | Oid | RegClass | RegProc),
            Self::Long => matches!(scalar, I16 | I32 | I64 | DateMilli | Time { bit_width: 64, .. } | Timestamp { .. } | Duration(_)),
            Self::Float => matches!(scalar, F32),
            Self::Double => matches!(scalar, F32 | F64),
        }
    }
}

impl<'a> CellRef<'a> {
    /// Borrows raw Utf8/Binary/FixedSizeBinary payload bytes, including extension storage and null-row payloads.
    /// Union, run-end and named-leg readers delegate; other leaf classes reject getBytes without a null check.
    pub fn get_bytes(&self) -> VectorReadResult<&'a [u8]> {
        let cell = self.resolve_primitive()?;
        if !matches!(cell.node.descriptor.scalar_field().map(Field::data_type), Some(DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_))) { return Err(unsupported("getBytes").into()); }
        let cell = cell.payload()?;
        Ok(ScalarStorageRef::node(cell.node)?.bytes(cell.row, cell.node.descriptor.scalar().ok_or_else(layout)?.fixed_width()?)?)
    }

    fn primitive(&self, getter: Getter, checks: NullChecks) -> VectorReadResult<PayloadCell<'a>> {
        let cell = self.resolve_primitive()?;
        if !cell.node.descriptor.scalar().is_some_and(|scalar| getter.admits(scalar.logical_type())) { return Err(unsupported(getter.name()).into()); }
        if checks == NullChecks::Enabled && cell.node.null(cell.row)? {
            return Err(Anomaly::fault("xtdb.error/unknown", format!("null at index {}", cell.row.0)).into());
        }
        Ok(cell.payload()?)
    }

    /// Reads Boolean payload bits after method admission and the requested validity check.
    pub fn get_boolean(&self, checks: NullChecks) -> VectorReadResult<bool> {
        let cell = self.primitive(Getter::Boolean, checks)?;
        Ok(ScalarStorageRef::node(cell.node)?.bit(cell.row)?)
    }

    /// Reads signed byte bits from I8/U8 storage; other vector classes reject the method before checking nullness.
    pub fn get_byte(&self, checks: NullChecks) -> VectorReadResult<i8> {
        let cell = self.primitive(Getter::Byte, checks)?;
        Ok(ScalarStorageRef::node(cell.node)?.lane::<1>(cell.row)?[0] as i8)
    }

    /// Reads signed short bits from I16/U16 storage with the requested validity check.
    pub fn get_short(&self, checks: NullChecks) -> VectorReadResult<i16> {
        let cell = self.primitive(Getter::Short, checks)?;
        Ok(short(&cell)?)
    }

    /// Reads the concrete vector's Int getter, widening Short and preserving signed storage bits.
    /// DateDay uses epoch days, Time32 its declared unit, and YearMonth intervals total months.
    pub fn get_int(&self, checks: NullChecks) -> VectorReadResult<i32> {
        let cell = self.primitive(Getter::Int, checks)?;
        Ok(int(&cell)?)
    }

    /// Reads the concrete vector's Long getter, widening Short/Int and preserving signed storage bits.
    /// DateMilli returns epoch days by truncating raw milliseconds toward zero; other temporal getters retain their declared unit.
    /// DateDay, Time32, OID extensions, intervals and Byte do not expose this getter.
    pub fn get_long(&self, checks: NullChecks) -> VectorReadResult<i64> {
        let cell = self.primitive(Getter::Long, checks)?;
        Ok(long(ScalarStorageRef::node(cell.node)?, cell.node.scalar_field()?.data_type(), cell.row)?)
    }

    /// Reads F32 payload bits with the requested validity check; HALF and Double are not Float getters.
    pub fn get_float(&self, checks: NullChecks) -> VectorReadResult<f32> {
        let cell = self.primitive(Getter::Float, checks)?;
        Ok(f32::from_le_bytes(ScalarStorageRef::node(cell.node)?.lane(cell.row)?))
    }

    /// Reads F64 or widens F32 after the requested validity check.
    pub fn get_double(&self, checks: NullChecks) -> VectorReadResult<f64> {
        let cell = self.primitive(Getter::Double, checks)?;
        Ok(double(ScalarStorageRef::node(cell.node)?, cell.node.scalar_field()?.data_type(), cell.row)?)
    }
}

fn short(cell: &PayloadCell<'_>) -> Result<i16> {
    Ok(i16::from_le_bytes(ScalarStorageRef::node(cell.node)?.lane(cell.row)?))
}
fn int(cell: &PayloadCell<'_>) -> Result<i32> { integer(ScalarStorageRef::node(cell.node)?, cell.node.scalar_field()?.data_type(), cell.row) }
pub(super) fn integer(data: ScalarStorageRef<'_>, data_type: &DataType, row: usize) -> Result<i32> {
    Ok(match data_type {
        DataType::Int16 | DataType::UInt16 => i32::from(i16::from_le_bytes(data.lane(row)?)),
        _ => i32::from_le_bytes(data.lane(row)?),
    })
}
pub(super) fn long(data: ScalarStorageRef<'_>, data_type: &DataType, row: usize) -> Result<i64> {
    Ok(match data_type {
        DataType::Int16 | DataType::UInt16 | DataType::Int32 | DataType::UInt32 => i64::from(integer(data, data_type, row)?),
        DataType::Date64 => i64::from_le_bytes(data.lane(row)?) / 86_400_000,
        _ => i64::from_le_bytes(data.lane(row)?),
    })
}
pub(super) fn double(data: ScalarStorageRef<'_>, data_type: &DataType, row: usize) -> Result<f64> {
    Ok(match data_type {
        DataType::Float32 => f64::from(f32::from_le_bytes(data.lane(row)?)),
        _ => f64::from_le_bytes(data.lane(row)?),
    })
}
