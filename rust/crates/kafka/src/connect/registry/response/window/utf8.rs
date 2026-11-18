use std::sync::Arc;
use super::{ConversionContext, ObservedDecode, Result, TextBufferObservation, WindowInput, beans, byte_symbol_failure, decode, input_window};
use super::super::{cursor::Cursor, symbols::{InternAuthority, bytes::ByteSymbols}};

pub(in crate::connect::registry) fn schema_string_from_utf8<'a>(input: input_window::Utf8Window<'a>, root: &Arc<ByteSymbols>,
    authority: &'a mut dyn InternAuthority, text_buffers: &'a [TextBufferObservation], context: &mut ConversionContext) -> ObservedDecode<Option<beans::SchemaString>> {
    decode_utf8(input, root, authority, text_buffers, context, decode::schema_string)
}
pub(in crate::connect::registry) fn schema_from_utf8<'a>(input: input_window::Utf8Window<'a>, root: &Arc<ByteSymbols>,
    authority: &'a mut dyn InternAuthority, text_buffers: &'a [TextBufferObservation], context: &mut ConversionContext) -> ObservedDecode<Option<beans::Schema>> {
    decode_utf8(input, root, authority, text_buffers, context, decode::schema)
}
fn decode_utf8<'a, T>(input: input_window::Utf8Window<'a>, root: &Arc<ByteSymbols>, authority: &'a mut dyn InternAuthority,
    text_buffers: &'a [TextBufferObservation], context: &mut ConversionContext,
    decode: impl FnOnce(&mut Cursor<'a>, &mut ConversionContext) -> Result<T>) -> ObservedDecode<T> {
    let prepared_count = input.observations_consumed();
    let child = match root.child(context) {
        Ok(child) => child,
        Err(cause) => return ObservedDecode { result: Err(byte_symbol_failure(cause, 0)), suppressed_close: None, observations_consumed: prepared_count },
    };
    let mut cursor = match Cursor::utf8(WindowInput::Utf8(input), child, authority, text_buffers, context) {
        Ok(cursor) => cursor,
        Err(cause) => return ObservedDecode { result: Err(cause), suppressed_close: None, observations_consumed: prepared_count },
    };
    let result = cursor.start().and_then(|()| decode(&mut cursor, context));
    let close = cursor.close(context);
    let observations_consumed = cursor.observations_consumed();
    match (result, close) {
        (Ok(value), Ok(())) => ObservedDecode { result: Ok(value), suppressed_close: None, observations_consumed },
        (Ok(_), Err(cause)) => ObservedDecode { result: Err(cause), suppressed_close: None, observations_consumed },
        (Err(cause), close) => ObservedDecode { result: Err(cause), suppressed_close: close.err(), observations_consumed },
    }
}

#[cfg(test)]
mod tests;
