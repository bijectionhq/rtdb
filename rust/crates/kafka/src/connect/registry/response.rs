use std::fmt;
use rtdb_types::Anomaly;
use super::{ConversionContext, error};

mod owned;
mod cursor;
mod beans;
mod scalars;
mod collections;
mod decode;
#[cfg(test)]
mod window;
pub(in crate::connect) mod symbols;

#[cfg(test)]
pub(super) use beans::{Metadata, Properties, Rule, RuleKind, RuleMode, RuleSet};
#[cfg(test)]
pub(super) use window::{LexicalObservations, NameObservation, ObservedDecode, WindowInput, schema_from_window, schema_string_from_window};
#[cfg(test)]
pub(super) use window::{ReaderInput, schema_from_reader, schema_string_from_reader};
#[cfg(test)]
pub(super) use window::{schema_from_utf8, schema_string_from_utf8};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::connect) enum Profile {
    Jackson216,
    Jackson221,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TextBufferObservation {
    pub(super) value_start: usize,
    pub(super) capacity_units: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailureKind {
    Syntax,
    Coercion,
    Constructor,
    #[cfg(test)]
    MissingSetter,
    Constraint,
    #[cfg(test)]
    Unchecked,
}

#[derive(Debug)]
pub(super) enum Failure {
    Native(Anomaly),
    Decode { kind: FailureKind, offset: usize, detail: &'static str },
}

impl From<Anomaly> for Failure { fn from(cause: Anomaly) -> Self { Self::Native(cause) } }
impl Failure {
    fn at(kind: FailureKind, offset: usize, detail: &'static str) -> Self { Self::Decode { kind, offset, detail } }
    #[cfg(test)]
    pub(super) fn into_anomaly(self) -> Anomaly {
        match self { Self::Native(cause) => cause, cause => error("schema-registry", cause) }
    }
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(cause) => fmt::Display::fmt(cause, f),
            Self::Decode { kind, offset, detail } => write!(f, "Registry response {kind:?} at offset {offset}: {detail}"),
        }
    }
}
pub(super) type Result<T> = std::result::Result<T, Failure>;

fn overflow() -> Anomaly { error("allocation-overflow", "Registry response allocation exceeds addressable memory") }
fn allocation_failed() -> Anomaly { error("allocation-failed", "Registry response allocation failed") }

pub(super) fn schema_string(bytes: &[u8], profile: Profile, context: &mut ConversionContext) -> Result<Option<beans::SchemaString>> {
    decode::schema_string(&mut cursor::Cursor::new(bytes, profile)?, context)
}

#[cfg(test)]
pub(super) fn schema(bytes: &[u8], profile: Profile, context: &mut ConversionContext) -> Result<Option<beans::Schema>> {
    decode::schema(&mut cursor::Cursor::new(bytes, profile)?, context)
}

#[cfg(test)]
pub(super) fn schema_string_with_text_buffers(bytes: &[u8], profile: Profile, observations: &[TextBufferObservation],
    context: &mut ConversionContext) -> Result<Option<beans::SchemaString>> {
    decode::schema_string(&mut cursor::Cursor::with_text_buffers(bytes, profile, observations)?, context)
}

#[cfg(test)]
pub(super) fn schema_with_text_buffers(bytes: &[u8], profile: Profile, observations: &[TextBufferObservation],
    context: &mut ConversionContext) -> Result<Option<beans::Schema>> {
    decode::schema(&mut cursor::Cursor::with_text_buffers(bytes, profile, observations)?, context)
}

#[cfg(test)]
mod tests;
