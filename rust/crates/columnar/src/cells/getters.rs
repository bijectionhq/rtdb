use arrow_array::*;
use arrow_schema::{IntervalUnit as ArrowIntervalUnit, TimeUnit as ArrowTimeUnit};
use rtdb_types::{IntervalUnit, ScalarType};

use super::*;

#[derive(Clone, Copy)]
enum Getter { Boolean, Byte, Short, Int, Long, Float, Double }

impl Getter {
    fn name(self) -> &'static str {
        match self {
            Self::Boolean => "getBoolean", Self::Byte => "getByte", Self::Short => "getShort", Self::Int => "getInt",
            Self::Long => "getLong", Self::Float => "getFloat", Self::Double => "getDouble",
        }
    }

    fn admits(self, logical: &LogicalType) -> bool {
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
        let array = cell.node.array().map_err(|_| unsupported("getBytes"))?;
        if !matches!(array.data_type(), DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_)) { return Err(unsupported("getBytes").into()); }
        let cell = cell.payload()?;
        match array.data_type() {
            DataType::Utf8 => Ok(downcast::<StringArray>(array)?.value(cell.row).as_bytes()),
            DataType::Binary => Ok(downcast::<BinaryArray>(array)?.value(cell.row)),
            DataType::FixedSizeBinary(_) => Ok(downcast::<FixedSizeBinaryArray>(array)?.value(cell.row)),
            _ => Err(unsupported("getBytes").into()),
        }
    }

    fn primitive(&self, getter: Getter, checks: NullChecks) -> VectorReadResult<PayloadCell<'a>> {
        let cell = self.resolve_primitive()?;
        if !getter.admits(&cell.node.logical) { return Err(unsupported(getter.name()).into()); }
        if checks == NullChecks::Enabled && cell.node.null(cell.row)? {
            return Err(Anomaly::fault("xtdb.error/unknown", format!("null at index {}", cell.row.0)).into());
        }
        Ok(cell.payload()?)
    }

    /// Reads Boolean payload bits after method admission and the requested validity check.
    pub fn get_boolean(&self, checks: NullChecks) -> VectorReadResult<bool> {
        let cell = self.primitive(Getter::Boolean, checks)?;
        Ok(downcast::<BooleanArray>(cell.node.array()?)?.value(cell.row))
    }

    /// Reads signed byte bits from I8/U8 storage; other vector classes reject the method before checking nullness.
    pub fn get_byte(&self, checks: NullChecks) -> VectorReadResult<i8> {
        let cell = self.primitive(Getter::Byte, checks)?;
        Ok(match cell.node.array()?.data_type() {
            DataType::Int8 => downcast::<Int8Array>(cell.node.array()?)?.value(cell.row),
            DataType::UInt8 => downcast::<UInt8Array>(cell.node.array()?)?.value(cell.row) as i8,
            _ => return Err(layout().into()),
        })
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
        let array = cell.node.array()?;
        let row = cell.row;
        Ok(match cell.node.array()?.data_type() {
            DataType::Int16 | DataType::UInt16 | DataType::Int32 | DataType::UInt32 => i64::from(int(&cell)?),
            DataType::Int64 => downcast::<Int64Array>(array)?.value(row),
            DataType::UInt64 => downcast::<UInt64Array>(array)?.value(row) as i64,
            DataType::Date64 => downcast::<Date64Array>(array)?.value(row) / 86_400_000,
            DataType::Time64(ArrowTimeUnit::Microsecond) => downcast::<Time64MicrosecondArray>(array)?.value(row),
            DataType::Time64(ArrowTimeUnit::Nanosecond) => downcast::<Time64NanosecondArray>(array)?.value(row),
            DataType::Timestamp(unit, _) => match unit {
                ArrowTimeUnit::Second => downcast::<TimestampSecondArray>(array)?.value(row),
                ArrowTimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(array)?.value(row),
                ArrowTimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(array)?.value(row),
                ArrowTimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(array)?.value(row),
            },
            DataType::Duration(unit) => match unit {
                ArrowTimeUnit::Second => downcast::<DurationSecondArray>(array)?.value(row),
                ArrowTimeUnit::Millisecond => downcast::<DurationMillisecondArray>(array)?.value(row),
                ArrowTimeUnit::Microsecond => downcast::<DurationMicrosecondArray>(array)?.value(row),
                ArrowTimeUnit::Nanosecond => downcast::<DurationNanosecondArray>(array)?.value(row),
            },
            _ => return Err(layout().into()),
        })
    }

    /// Reads F32 payload bits with the requested validity check; HALF and Double are not Float getters.
    pub fn get_float(&self, checks: NullChecks) -> VectorReadResult<f32> {
        let cell = self.primitive(Getter::Float, checks)?;
        Ok(downcast::<Float32Array>(cell.node.array()?)?.value(cell.row))
    }

    /// Reads F64 or widens F32 after the requested validity check.
    pub fn get_double(&self, checks: NullChecks) -> VectorReadResult<f64> {
        let cell = self.primitive(Getter::Double, checks)?;
        Ok(match cell.node.array()?.data_type() {
            DataType::Float32 => f64::from(downcast::<Float32Array>(cell.node.array()?)?.value(cell.row)),
            DataType::Float64 => downcast::<Float64Array>(cell.node.array()?)?.value(cell.row),
            _ => return Err(layout().into()),
        })
    }
}

fn short(cell: &PayloadCell<'_>) -> Result<i16> {
    Ok(match cell.node.array()?.data_type() {
        DataType::Int16 => downcast::<Int16Array>(cell.node.array()?)?.value(cell.row),
        DataType::UInt16 => downcast::<UInt16Array>(cell.node.array()?)?.value(cell.row) as i16,
        _ => return Err(layout()),
    })
}

fn int(cell: &PayloadCell<'_>) -> Result<i32> {
    let array = cell.node.array()?;
    let row = cell.row;
    Ok(match cell.node.array()?.data_type() {
        DataType::Int16 | DataType::UInt16 => i32::from(short(cell)?),
        DataType::Int32 => downcast::<Int32Array>(array)?.value(row),
        DataType::UInt32 => downcast::<UInt32Array>(array)?.value(row) as i32,
        DataType::Date32 => downcast::<Date32Array>(array)?.value(row),
        DataType::Time32(ArrowTimeUnit::Second) => downcast::<Time32SecondArray>(array)?.value(row),
        DataType::Time32(ArrowTimeUnit::Millisecond) => downcast::<Time32MillisecondArray>(array)?.value(row),
        DataType::Interval(ArrowIntervalUnit::YearMonth) => downcast::<IntervalYearMonthArray>(array)?.value(row),
        _ => return Err(layout()),
    })
}
