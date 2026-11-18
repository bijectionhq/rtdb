use std::sync::Arc;
use super::{ConversionContext, ObservedDecode, Result, WindowInput, beans, decode, input_window, source_failure, symbol_failure};
use super::super::{cursor::Cursor, symbols::{InternAuthority, ReaderSymbols}};

/// Transfer a detected decoder before any read, read_one or ready call.
pub(in crate::connect::registry) enum ReaderInput<'a> {
    Utf16(input_window::Utf16Reader<'a>),
    Utf32(input_window::Utf32Reader<'a>),
}
impl<'a> ReaderInput<'a> {
    fn observations_consumed(&self) -> usize {
        match self { Self::Utf16(reader) => reader.observations_consumed(), Self::Utf32(reader) => reader.observations_consumed() }
    }
    fn into_window(self, capacity: usize, context: &mut ConversionContext) -> Result<WindowInput<'a>> {
        match self {
            Self::Utf16(reader) => input_window::Utf16Window::new(reader, capacity, context).map(WindowInput::Utf16).map_err(source_failure),
            Self::Utf32(reader) => input_window::ReaderWindow::new(reader, capacity, context).map(WindowInput::Utf32).map_err(source_failure),
        }
    }
}

pub(in crate::connect::registry) fn schema_string_from_reader<'a>(input: ReaderInput<'a>, parser_capacity: usize,
    root: &Arc<ReaderSymbols>, authority: &'a mut dyn InternAuthority, context: &mut ConversionContext) -> ObservedDecode<Option<beans::SchemaString>> {
    decode_reader(input, parser_capacity, root, authority, context, decode::schema_string)
}
pub(in crate::connect::registry) fn schema_from_reader<'a>(input: ReaderInput<'a>, parser_capacity: usize,
    root: &Arc<ReaderSymbols>, authority: &'a mut dyn InternAuthority, context: &mut ConversionContext) -> ObservedDecode<Option<beans::Schema>> {
    decode_reader(input, parser_capacity, root, authority, context, decode::schema)
}

fn decode_reader<'a, T>(input: ReaderInput<'a>, parser_capacity: usize, root: &Arc<ReaderSymbols>,
    authority: &'a mut dyn InternAuthority, context: &mut ConversionContext,
    decode: impl FnOnce(&mut Cursor<'a>, &mut ConversionContext) -> Result<T>) -> ObservedDecode<T> {
    let prepared_count = input.observations_consumed();
    let child = match root.child(context) {
        Ok(child) => child,
        Err(cause) => return ObservedDecode { result: Err(symbol_failure(cause, 0)), suppressed_close: None, observations_consumed: prepared_count },
    };
    let input = match input.into_window(parser_capacity, context) {
        Ok(input) => input,
        Err(cause) => return ObservedDecode { result: Err(cause), suppressed_close: None, observations_consumed: prepared_count },
    };
    let mut cursor = Cursor::reader(input, child, authority);
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
