mod binary;
mod output;
mod text;

pub use output::{Format, SessionEnvironment, encode, encode_error, encode_value};
pub use text::{from_transit, read_text};

use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
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

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.pgwire/invalid-arg-representation", error.to_string()) }
fn size_error() -> Anomaly { Anomaly::incorrect("xtdb.pgwire/value-size", "PostgreSQL value exceeds the supported allocation size") }
