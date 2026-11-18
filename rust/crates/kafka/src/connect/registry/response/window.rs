use super::{ConversionContext, Failure, Profile, Result, TextBufferObservation, beans, cursor::Cursor, decode};
use super::super::{error, input_window};

pub(in crate::connect::registry) enum WindowInput<'a> {
    Utf8(input_window::Utf8Window<'a>),
    Utf16(input_window::Utf16Window<'a>),
    Utf32(input_window::ReaderWindow<'a>),
}
impl WindowInput<'_> {
    pub(super) fn is_utf8(&self) -> bool { matches!(self, Self::Utf8(_)) }
    pub(super) fn processed(&self) -> i64 {
        match self { Self::Utf8(window) => window.processed(), Self::Utf16(window) => window.processed(), Self::Utf32(window) => window.processed() }
    }
    pub(super) fn validate_profile(&self, profile: Profile) -> Result<()> {
        if let Self::Utf32(window) = self {
            let expected = match profile { Profile::Jackson216 => input_window::Profile::Jackson216, Profile::Jackson221 => input_window::Profile::Jackson221 };
            if window.profile() != expected { return Err(unavailable("UTF32 window and lexical Jackson profiles differ")); }
        }
        Ok(())
    }
    pub(super) fn position(&self) -> Result<usize> {
        let (processed, position) = match self {
            Self::Utf8(window) => (window.processed(), window.position()),
            Self::Utf16(window) => (window.processed(), window.position()),
            Self::Utf32(window) => (window.processed(), window.position()),
        };
        processed.checked_add(i64::try_from(position).map_err(|_| super::overflow())?)
            .and_then(|value| usize::try_from(value).ok()).ok_or_else(|| unavailable("Observed parser position is outside the native address range"))
    }
    pub(super) fn next(&mut self) -> Result<Option<u16>> {
        match self {
            Self::Utf8(window) => window.next().map(|value| value.map(u16::from)).map_err(source_failure),
            Self::Utf16(window) => window.next().map_err(source_failure),
            Self::Utf32(window) => window.next().map_err(source_failure),
        }
    }
    pub(super) fn close(&mut self) -> Result<()> {
        match self {
            Self::Utf8(window) => window.close().map_err(source_failure),
            Self::Utf16(window) => window.close().map_err(source_failure),
            Self::Utf32(window) => window.close().map_err(source_failure),
        }
    }
    pub(super) fn observations_consumed(&self) -> usize {
        match self {
            Self::Utf8(window) => window.observations_consumed(),
            Self::Utf16(window) => window.observations_consumed(),
            Self::Utf32(window) => window.observations_consumed(),
        }
    }
}

#[derive(Clone, Copy)]
pub(in crate::connect::registry) struct NameObservation<'a> { pub(in crate::connect::registry) start: usize, pub(in crate::connect::registry) units: &'a [u16] }

#[derive(Clone, Copy)]
pub(in crate::connect::registry) struct LexicalObservations<'a> {
    pub(in crate::connect::registry) names: &'a [NameObservation<'a>],
    pub(in crate::connect::registry) text_buffers: &'a [TextBufferObservation],
}

pub(in crate::connect::registry) struct ObservedDecode<T> {
    pub(in crate::connect::registry) result: Result<T>,
    pub(in crate::connect::registry) suppressed_close: Option<Failure>,
    pub(in crate::connect::registry) observations_consumed: usize,
}

pub(in crate::connect::registry) fn schema_string_from_window<'a>(input: WindowInput<'a>, profile: Profile,
    observations: LexicalObservations<'a>, context: &mut ConversionContext) -> ObservedDecode<Option<beans::SchemaString>> {
    decode_window(input, profile, observations, context, decode::schema_string)
}

pub(in crate::connect::registry) fn schema_from_window<'a>(input: WindowInput<'a>, profile: Profile,
    observations: LexicalObservations<'a>, context: &mut ConversionContext) -> ObservedDecode<Option<beans::Schema>> {
    decode_window(input, profile, observations, context, decode::schema)
}

fn decode_window<'a, T>(input: WindowInput<'a>, profile: Profile, observations: LexicalObservations<'a>,
    context: &mut ConversionContext, decode: impl FnOnce(&mut Cursor<'a>, &mut ConversionContext) -> Result<T>) -> ObservedDecode<T> {
    let mut cursor = Cursor::observed(input, profile, observations);
    let result = cursor.start().and_then(|()| decode(&mut cursor, context));
    let close = cursor.close();
    let observations_consumed = cursor.observations_consumed();
    match (result, close) {
        (Ok(value), Ok(())) => ObservedDecode { result: Ok(value), suppressed_close: None, observations_consumed },
        (Ok(_), Err(cause)) => ObservedDecode { result: Err(cause), suppressed_close: None, observations_consumed },
        (Err(cause), close) => ObservedDecode { result: Err(cause), suppressed_close: close.err(), observations_consumed },
    }
}

pub(super) fn unavailable(detail: &'static str) -> Failure {
    rtdb_types::Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-source-profile", detail).into()
}

fn source_failure(cause: input_window::Failure<'_>) -> Failure {
    match cause {
        input_window::Failure::Native(cause) => Failure::Native(cause),
        cause @ input_window::Failure::Observation { .. } => error("registry-input-observation", format_args!("{cause:?}")).into(),
        cause @ input_window::Failure::UnsupportedOrder(_) => error("registry-input-encoding", format_args!("{cause:?}")).into(),
        cause @ (input_window::Failure::TruncatedUtf32 { .. } | input_window::Failure::InvalidUtf32 { .. }) =>
            error("registry-input-encoding", format_args!("{cause:?}")).into(),
        cause @ (input_window::Failure::Io { .. } | input_window::Failure::ZeroRead(_)) =>
            error("registry-input-io", format_args!("{cause:?}")).into(),
    }
}

#[cfg(test)]
mod tests;
