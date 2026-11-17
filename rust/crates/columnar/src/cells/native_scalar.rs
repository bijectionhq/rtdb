use arrow_array::{BinaryArray, BooleanArray, StringArray};
use rtdb_types::{DecimalValue, ScalarType};
use super::*;

#[derive(Clone, Debug)]
pub(super) struct StoredBytes { pub bytes: RetainedBuffer, pub append: usize }
#[derive(Clone, Debug)]
pub(super) struct StoredBits { pub bytes: RetainedBuffer, pub append: i32, pub byte_append: usize }
#[derive(Clone, Debug)]
pub(super) struct NativeScalarStorage {
    pub validity: Option<StoredBits>,
    pub data: NativeScalarData,
}
#[derive(Clone, Debug)]
pub(super) enum NativeScalarData {
    Null,
    Bits(StoredBits),
    Fixed(StoredBytes),
    Variable { offsets: StoredBytes, data: StoredBytes, last_offset: i32 },
}

#[derive(Clone, Copy)]
pub(super) enum ScalarOffsets<'a> { Native(&'a [u8]), Sdk(&'a [i32]) }
impl ScalarOffsets<'_> {
    pub fn get(self, row: usize) -> Result<i32> {
        match self {
            Self::Native(bytes) => Ok(i32::from_le_bytes(span(bytes, wrapped_stride(row, 4)?, 4)?.try_into().map_err(|_| layout())?)),
            Self::Sdk(offsets) => offsets.get(wrapped_stride(row, 4)? / 4).copied().ok_or_else(index_error),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ScalarStorageRef<'a> {
    Null,
    Bits { data: &'a [u8], offset: usize },
    Fixed { data: &'a [u8], offset: usize },
    Variable { offsets: ScalarOffsets<'a>, data: &'a [u8] },
}
impl<'a> ScalarStorageRef<'a> {
    pub fn node(node: &'a PhysicalNode) -> Result<Self> {
        match &node.storage {
            NodeLayout::Scalar(storage) => Ok(storage.borrow()),
            NodeLayout::Leaf { array, payload, payload_offset } => match array.data_type() {
                DataType::Null => Ok(Self::Null),
                DataType::Boolean => { let bits = downcast::<BooleanArray>(array.as_ref())?.values(); Ok(Self::Bits { data: bits.values(), offset: bits.offset() }) },
                DataType::Utf8 => { let array = downcast::<StringArray>(array.as_ref())?; Ok(Self::Variable { offsets: ScalarOffsets::Sdk(array.value_offsets()), data: array.value_data() }) },
                DataType::Binary => { let array = downcast::<BinaryArray>(array.as_ref())?; Ok(Self::Variable { offsets: ScalarOffsets::Sdk(array.value_offsets()), data: array.value_data() }) },
                data_type => {
                    let width = super::scalar::fixed_width(data_type).ok_or_else(layout)?;
                    Ok(Self::Fixed { data: payload.as_ref().ok_or_else(layout)?.as_slice(), offset: payload_offset.checked_mul(width).ok_or_else(allocation)? })
                },
            },
            _ => Err(layout()),
        }
    }
    pub fn bit(self, row: usize) -> Result<bool> {
        let Self::Bits { data, offset } = self else { return Err(layout()); };
        bit(data, offset.checked_add(row).ok_or_else(allocation)?)
    }
    pub fn fixed(self, offset: usize, length: usize) -> Result<&'a [u8]> {
        let Self::Fixed { data, offset: base } = self else { return Err(layout()); };
        span(data, base.checked_add(offset).ok_or_else(allocation)?, length)
    }
    pub fn bytes(self, row: usize, data_type: &DataType) -> Result<&'a [u8]> {
        match self {
            Self::Variable { offsets, data } => {
                let start = offsets.get(row)?;
                let end = offsets.get(row.checked_add(1).ok_or_else(allocation)?)?;
                let length = usize::try_from(end.wrapping_sub(start)).map_err(|_| index_error())?;
                span(data, usize::try_from(start).map_err(|_| index_error())?, length)
            },
            Self::Fixed { .. } => { let width = super::scalar::fixed_width(data_type).ok_or_else(layout)?; self.fixed(wrapped_stride(row, width)?, width) },
            _ => Err(layout()),
        }
    }
    pub fn lane<const N: usize>(self, row: usize) -> Result<[u8; N]> {
        self.fixed(wrapped_stride(row, N)?, N)?.try_into().map_err(|_| layout())
    }
}
impl NativeScalarStorage {
    pub fn borrow(&self) -> ScalarStorageRef<'_> {
        match &self.data {
            NativeScalarData::Null => ScalarStorageRef::Null,
            NativeScalarData::Bits(bits) => ScalarStorageRef::Bits { data: bits.bytes.as_slice(), offset: 0 },
            NativeScalarData::Fixed(bytes) => ScalarStorageRef::Fixed { data: bytes.bytes.as_slice(), offset: 0 },
            NativeScalarData::Variable { offsets, data, .. } => ScalarStorageRef::Variable { offsets: ScalarOffsets::Native(offsets.bytes.as_slice()), data: data.bytes.as_slice() },
        }
    }
}

pub(super) fn wrapped_stride(row: usize, width: usize) -> Result<usize> {
    let row = i32::try_from(row).map_err(|_| index_error())?;
    let width = i32::try_from(width).map_err(|_| index_error())?;
    usize::try_from(row.wrapping_mul(width)).map_err(|_| index_error())
}
pub(super) fn bit(data: &[u8], row: usize) -> Result<bool> { Ok(data.get(row / 8).ok_or_else(index_error)? & (1 << (row % 8)) != 0) }
pub(super) fn span(data: &[u8], start: usize, length: usize) -> Result<&[u8]> { data.get(start..start.checked_add(length).ok_or_else(allocation)?).ok_or_else(index_error) }

pub(super) fn value_bound(node: &PhysicalNode, row: usize) -> Result<usize> {
    let data = ScalarStorageRef::node(node)?;
    let payload = match node.scalar_field()?.data_type() {
        DataType::Utf8 | DataType::Binary | DataType::FixedSizeBinary(_) => data.bytes(row, node.scalar_field()?.data_type())?.len(),
        DataType::Timestamp(_, Some(zone)) => zone.len().checked_add(64).ok_or_else(allocation)?,
        _ => 64,
    };
    payload.checked_mul(4).and_then(|n| n.checked_add(4096)).ok_or_else(allocation)
}

pub(super) fn value(node: &PhysicalNode, row: usize) -> Result<Value> {
    let scalar = node.descriptor.scalar().ok_or_else(layout)?.value_type();
    let LogicalType::Scalar(scalar) = scalar else { return Err(layout()); };
    let data = ScalarStorageRef::node(node)?;
    let int = || Ok::<_, Anomaly>(i32::from_le_bytes(data.lane(row)?));
    let long = || Ok::<_, Anomaly>(i64::from_le_bytes(data.lane(row)?));
    let bytes = || data.bytes(row, node.scalar_field()?.data_type());
    use ScalarType::*;
    Ok(match scalar {
        Bool => Value::Bool(data.bit(row)?),
        I8 => Value::I8(data.lane::<1>(row)?[0] as i8),
        I16 => Value::I16(i16::from_le_bytes(data.lane(row)?)),
        I32 => Value::I32(int()?), I64 => Value::I64(long()?),
        F32 => Value::F32(f32::from_le_bytes(data.lane(row)?)), F64 => Value::F64(f64::from_le_bytes(data.lane(row)?)),
        Utf8 => Value::Utf8(rtdb_types::decode_java_utf8(bytes()?)),
        Binary => Value::Binary(bytes()?.to_vec()), FixedSizeBinary(_) => Value::FixedSizeBinary(bytes()?.to_vec()),
        Uri => { let text = rtdb_types::decode_java_utf8(bytes()?); rtdb_types::UriParts::parse(&text)?; Value::Uri(text) },
        Keyword => Value::Keyword(rtdb_types::Keyword::from_name(rtdb_types::decode_java_utf8(bytes()?))),
        Transit => Value::Transit(bytes()?.to_vec()),
        Uuid => Value::Uuid(bytes()?.try_into().map_err(|_| layout())?),
        Oid => Value::Oid(int()?), RegClass => Value::RegClass(int()?), RegProc => Value::RegProc(int()?),
        DateDay => Value::DateDay(int()?), DateMilli => Value::DateMilli(long()?),
        Time { unit, bit_width } => Value::Time { value: if *bit_width == 32 { i64::from(int()?) } else { long()? }, unit: *unit, bit_width: *bit_width },
        Timestamp { unit, timezone } => Value::Timestamp { value: long()?, unit: *unit, timezone: timezone.clone() },
        Duration(unit) => Value::Duration { value: long()?, unit: *unit },
        Interval(rtdb_types::IntervalUnit::YearMonth) => Value::IntervalYearMonth(int()?),
        Interval(rtdb_types::IntervalUnit::DayTime) => { let b = data.lane::<8>(row)?; Value::IntervalDayTime { days: i32::from_le_bytes(b[..4].try_into().map_err(|_| layout())?), milliseconds: i32::from_le_bytes(b[4..].try_into().map_err(|_| layout())?) } },
        Interval(rtdb_types::IntervalUnit::MonthDayNano) | IntervalMonthDayMicro => {
            let b = data.lane::<16>(row)?;
            let months = i32::from_le_bytes(b[..4].try_into().map_err(|_| layout())?);
            let days = i32::from_le_bytes(b[4..8].try_into().map_err(|_| layout())?);
            let nanoseconds = i64::from_le_bytes(b[8..].try_into().map_err(|_| layout())?);
            if matches!(scalar, IntervalMonthDayMicro) { Value::IntervalMonthDayMicro { months, days, nanoseconds } } else { Value::IntervalMonthDayNano { months, days, nanoseconds } }
        },
        Decimal { precision, scale, bit_width } => {
            let width = usize::from(*bit_width / 8);
            Value::Decimal(DecimalValue { unscaled: data.fixed(row.checked_mul(width).ok_or_else(allocation)?, width)?.iter().rev().copied().collect(), precision: *precision, scale: *scale, bit_width: *bit_width })
        },
        _ => return Err(layout()),
    })
}
