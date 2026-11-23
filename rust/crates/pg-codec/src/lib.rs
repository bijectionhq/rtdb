mod binary;
mod float_array;
mod interval;
mod output;
mod text;

pub use output::{Format, SessionEnvironment, encode, encode_arrow, encode_error, encode_timestamp, encode_value, to_transit};
pub use text::{from_transit, read_text, read_text_column};

use std::{mem::size_of, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::transit::{TransitValue, decode_msgpack_in};
use rtdb_types::{Anomaly, PgType, Result, Value};

pub struct DecodedValue { value: Value, _reservation: Arc<MemoryReservation> }
impl DecodedValue {
    pub fn value(&self) -> &Value { &self.value }
    /// Keep the returned reservation alive until the value is dropped or adopted by another charged owner.
    pub fn into_parts(self) -> (Value, Arc<MemoryReservation>) { (self.value, self._reservation) }
}

pub struct EncodedValue { bytes: Vec<u8>, _reservation: Arc<MemoryReservation> }
impl EncodedValue {
    pub fn as_slice(&self) -> &[u8] { &self.bytes }
}

/// Reserves before decoding; the returned owner covers the value, excluding the borrowed input bytes.
pub fn decode(ty: PgType, format: Format, bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<DecodedValue> {
    let reservation = memory.reserve(decode_bound(bytes)?)?;
    let value = match (format, ty) {
        (Format::Text, PgType::Float4s | PgType::Float8s) => float_array::read_value(ty, format, bytes)?,
        (Format::Text, _) => text::read_text(ty.descriptor().oid as u32, std::str::from_utf8(bytes).map_err(invalid)?)?,
        (Format::Binary, _) => binary::read(ty, bytes)?,
    };
    if value.retained_bytes()? > reservation.bytes() { return Err(size_error()); }
    Ok(DecodedValue { value, _reservation: reservation })
}

fn decode_bound(bytes: &[u8]) -> Result<usize> {
    let structural = bytes.iter().filter(|byte| matches!(byte, b'[' | b'{' | b',' | b':')).count();
    bytes.len().checked_mul(64).and_then(|n| n.checked_add(structural.checked_mul(4096)?)).and_then(|n| n.checked_add(8192)).ok_or_else(size_error)
}

/// Decodes a single Bind argument and keeps its array covered until the portal adopts it.
pub fn decode_argument(
    name: &str, ty: PgType, format: Format, bytes: &[u8], memory: &Arc<MemoryBudget>,
) -> Result<(arrow_schema::Field, arrow_array::ArrayRef, Arc<MemoryReservation>)> {
    use arrow_array::{ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::DataType;

    let mut charge = memory.reserve(decode_bound(bytes)?)?;
    let fixed_type = match (format, ty) {
        (_, PgType::Default | PgType::Null | PgType::Text | PgType::VarChar | PgType::Keyword) => PgType::Text.logical_type(),
        (_, PgType::Int2 | PgType::Int4 | PgType::Int8 | PgType::Float4 | PgType::Float8 | PgType::Bool
            | PgType::Float4s | PgType::Float8s) | (Format::Binary, PgType::Bytes) => ty.logical_type(),
        _ => None,
    };
    let (field, array) = if let Some(logical) = fixed_type {
        let field = rtdb_columnar::to_field(name, &logical)?;
        let array: ArrayRef = match (format, ty) {
            (_, PgType::Float4s | PgType::Float8s) => {
                let DataType::List(item) = field.data_type() else { unreachable!() };
                Arc::new(float_array::read(ty, format, bytes, Arc::clone(item))?)
            }
            (Format::Text, _) => {
                let text = std::str::from_utf8(bytes).map_err(invalid)?;
                text::read_text_column(if logical == PgType::Text.logical_type().unwrap() { 25 } else { ty.descriptor().oid as u32 }, 1, |_| Ok(Some(text)))?
                    .expect("Bind selects fixed text decoders")
            }
            (Format::Binary, PgType::Int2) => Arc::new(Int16Array::from(vec![i16::from_be_bytes(fixed_binary(bytes)?)])),
            (Format::Binary, PgType::Int4) => Arc::new(Int32Array::from(vec![i32::from_be_bytes(fixed_binary(bytes)?)])),
            (Format::Binary, PgType::Int8) => Arc::new(Int64Array::from(vec![i64::from_be_bytes(fixed_binary(bytes)?)])),
            (Format::Binary, PgType::Float4) => Arc::new(Float32Array::from(vec![f32::from_be_bytes(fixed_binary(bytes)?)])),
            (Format::Binary, PgType::Float8) => Arc::new(Float64Array::from(vec![f64::from_be_bytes(fixed_binary(bytes)?)])),
            (Format::Binary, PgType::Bool) => {
                let value = match bytes.first() {
                    Some(0) => false,
                    Some(1) => true,
                    Some(_) => return Err(invalid("Invalid binary boolean value")),
                    None => return Err(invalid("Truncated PostgreSQL binary value")),
                };
                fixed_binary::<1>(bytes)?;
                Arc::new(BooleanArray::from(vec![value]))
            }
            (Format::Binary, PgType::Bytes) => Arc::new(BinaryArray::from_vec(vec![bytes])),
            (Format::Binary, _) => Arc::new(StringArray::from(vec![std::str::from_utf8(bytes).map_err(invalid)?])),
        };
        (field, array)
    } else {
        let value = match format {
            Format::Text => text::read_text(ty.descriptor().oid as u32, std::str::from_utf8(bytes).map_err(invalid)?)?,
            Format::Binary => binary::read(ty, bytes)?,
        };
        if value.retained_bytes()? > charge.bytes() { return Err(size_error()); }
        let logical = value.logical_type();
        let values = std::slice::from_ref(&value);
        let bound = charge.bytes().checked_add(rtdb_columnar::estimate_builder_bytes(&logical, values)?).ok_or_else(size_error)?;
        Arc::get_mut(&mut charge).expect("Bind column owns its reservation").grow_to(bound)?;
        (rtdb_columnar::to_field(name, &logical)?, rtdb_columnar::build_array(&logical, values)?)
    };
    Ok((field, array, charge))
}

fn fixed_binary<const N: usize>(bytes: &[u8]) -> Result<[u8; N]> {
    if bytes.len() < N { return Err(invalid("Truncated PostgreSQL binary value")); }
    bytes.try_into().map_err(|_| invalid("Trailing bytes in PostgreSQL binary value"))
}

/// Admits the wire graph and its native conversion separately, then retains the native value's storage.
pub fn decode_transit_msgpack(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<DecodedValue> {
    let (wire, wire_reservation) = decode_msgpack_in(bytes, memory)?;
    let bound = transit_conversion_bound(&wire)?.checked_add(8192).ok_or_else(size_error)?;
    let mut reservation = memory.reserve(bound)?;
    let value = from_transit(wire)?;
    drop(wire_reservation);
    let retained = value.retained_bytes()?.checked_add(8192).ok_or_else(size_error)?;
    Arc::get_mut(&mut reservation).expect("Transit conversion owns its reservation").shrink_to(retained)?;
    Ok(DecodedValue { value, _reservation: reservation })
}

fn transit_conversion_bound(wire: &TransitValue) -> Result<usize> {
    use TransitValue as T;
    let add = |left: usize, right: usize| left.checked_add(right).ok_or_else(size_error);
    let text = |length: usize| length.checked_mul(8).ok_or_else(size_error);
    let inline = size_of::<Value>() * 8 + 256;
    let map_entry = size_of::<(String, Value)>() * 16 + 256;
    match wire {
        T::String(value) | T::Keyword(value) | T::Symbol(value) => add(inline, text(value.len())?),
        T::Bytes(value) => add(inline, text(value.len())?),
        T::Array(values) => values.iter().try_fold(inline, |bytes, value| add(bytes, transit_conversion_bound(value)?)),
        T::Map(entries) => entries.iter().try_fold(inline, |bytes, (key, value)| {
            add(add(add(bytes, map_entry)?, transit_conversion_bound(key)?)?, transit_conversion_bound(value)?)
        }),
        T::Tagged { tag, representation } => add(add(inline, text(tag.len())?)?, transit_conversion_bound(representation)?),
        _ => Ok(inline),
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/invalid-arg-representation", error.to_string()) }
fn size_error() -> Anomaly { Anomaly::incorrect("xtdb.pgwire/value-size", "PostgreSQL value exceeds the supported allocation size") }
