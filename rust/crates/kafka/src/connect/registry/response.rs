use std::{fmt, sync::Arc};
use rtdb_types::{Anomaly, MemoryReservation};
use super::{ConversionContext, error};

mod owned;
mod cursor;
mod beans;
mod scalars;
mod collections;
mod decode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Profile { Jackson216, Jackson221 }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TextBufferObservation {
    pub(super) value_start: usize,
    pub(super) capacity_units: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailureKind { Syntax, Coercion, Constructor, MissingSetter, Constraint }

#[derive(Debug)]
pub(super) enum Failure {
    Native(Anomaly),
    Decode { kind: FailureKind, offset: usize, detail: &'static str },
}

impl From<Anomaly> for Failure { fn from(cause: Anomaly) -> Self { Self::Native(cause) } }
impl Failure {
    fn at(kind: FailureKind, offset: usize, detail: &'static str) -> Self { Self::Decode { kind, offset, detail } }
    pub(super) fn into_anomaly(self) -> Anomaly {
        match self { Self::Native(cause) => cause, cause => error("schema-registry", cause) }
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(cause) => fmt::Display::fmt(cause, f),
            Self::Decode { kind, offset, detail } => write!(f, "Registry response {kind:?} at byte {offset}: {detail}"),
        }
    }
}
pub(super) type Result<T> = std::result::Result<T, Failure>;

pub(super) enum RawResponse { NoContent, Body(RawBody) }
pub(super) struct RawBody { bytes: Vec<u8>, _charge: Arc<MemoryReservation> }
impl RawResponse {
    pub(super) fn body(bytes: Vec<u8>, charge: Arc<MemoryReservation>) -> Self { Self::Body(RawBody { bytes, _charge: charge }) }
    pub(super) fn bytes(&self) -> Option<&[u8]> { match self { Self::NoContent => None, Self::Body(body) => Some(&body.bytes) } }
}

fn overflow() -> Anomaly { error("allocation-overflow", "Registry response allocation exceeds addressable memory") }
fn allocation_failed() -> Anomaly { error("allocation-failed", "Registry response allocation failed") }

pub(super) fn schema_string(bytes: &[u8], profile: Profile, context: &mut ConversionContext) -> Result<Option<beans::SchemaString>> {
    decode::schema_string(&mut cursor::Cursor::new(bytes, profile)?, context)
}

pub(super) fn schema(bytes: &[u8], profile: Profile, context: &mut ConversionContext) -> Result<Option<beans::Schema>> {
    decode::schema(&mut cursor::Cursor::new(bytes, profile)?, context)
}

pub(super) fn schema_string_with_text_buffers(bytes: &[u8], profile: Profile, observations: &[TextBufferObservation],
    context: &mut ConversionContext) -> Result<Option<beans::SchemaString>> {
    decode::schema_string(&mut cursor::Cursor::with_text_buffers(bytes, profile, observations)?, context)
}

pub(super) fn schema_with_text_buffers(bytes: &[u8], profile: Profile, observations: &[TextBufferObservation],
    context: &mut ConversionContext) -> Result<Option<beans::Schema>> {
    decode::schema(&mut cursor::Cursor::with_text_buffers(bytes, profile, observations)?, context)
}

#[cfg(test)]
mod tests;
