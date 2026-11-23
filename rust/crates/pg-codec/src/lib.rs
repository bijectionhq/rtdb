mod binary;
mod interval;
mod output;
mod text;

pub use output::{Format, SessionEnvironment, encode, encode_arrow, encode_error, encode_timestamp, encode_value, to_transit};
pub use text::{from_transit, read_text};

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
    let structural = bytes.iter().filter(|byte| matches!(byte, b'[' | b'{' | b',' | b':')).count();
    let bound = bytes.len().checked_mul(64).and_then(|n| n.checked_add(structural.checked_mul(4096)?)).and_then(|n| n.checked_add(8192)).ok_or_else(size_error)?;
    let reservation = memory.reserve(bound)?;
    let value = match format {
        Format::Text => text::read_text(ty.descriptor().oid as u32, std::str::from_utf8(bytes).map_err(invalid)?)?,
        Format::Binary => binary::read(ty, bytes)?,
    };
    if value.retained_bytes()? > reservation.bytes() { return Err(size_error()); }
    Ok(DecodedValue { value, _reservation: reservation })
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
