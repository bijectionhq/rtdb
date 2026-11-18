use super::*;
use serde_json::value::RawValue;
use crate::connect::json_value::RawObject;
use rtdb_types::jvm_number::{Error as NumberError, Number};
pub(super) const MAX_DOCUMENT_BYTES: usize = 256 * 1024;
const MAX_TEXT_UNITS: usize = 50_000;
const MAX_NUMBER_BYTES: usize = 1000;

pub(super) fn profile_limit() -> Anomaly { Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-avro-input-profile", "Input exceeds the bounded standard-JSON schema document profile") }

pub(super) fn parse(text: &str, context: &mut ConversionContext) -> Result<JsonNode> {
    if text.len() > MAX_DOCUMENT_BYTES { return Err(profile_limit()); }
    context.reserve(bytes(text.len(), size_of::<usize>())?)?;
    let raw = json::<&RawValue>(text)?;
    value(raw, context, 0)
}
fn json<'a, T: serde::Deserialize<'a>>(text: &'a str) -> Result<T> {
    serde_json::from_str(text).map_err(|cause| error("avro-native-json-input", cause))
}
fn bytes(count: usize, item: usize) -> Result<usize> { add(mul(mul(count, item)?, 16)?, 4096) }
fn value(raw: &RawValue, context: &mut ConversionContext, depth: usize) -> Result<JsonNode> {
    guard_depth(depth, "Schema document exceeds the native traversal depth limit of 128")?;
    match raw.get().as_bytes().first() {
        Some(b'n') => JsonNode::with(0, context, |_| Ok(JsonValue::Null)),
        Some(b't' | b'f') => JsonNode::with(0, context, |_| Ok(JsonValue::Bool(json(raw.get())?))),
        Some(b'"') => {
            context.reserve(bytes(raw.get().len(), size_of::<u16>())?)?;
            let decoded = json::<String>(raw.get())?;
            if decoded.encode_utf16().count() > MAX_TEXT_UNITS { return Err(profile_limit()); }
            JsonNode::with(0, context, |context| Ok(JsonValue::Text(Text::copy(JavaText::from_utf8(&decoded), context)?)))
        }
        Some(b'[') => {
            context.reserve(bytes(raw.get().len(), size_of::<&RawValue>())?)?;
            let elements = json::<Vec<&RawValue>>(raw.get())?;
            JsonNode::with(bytes(elements.len(), size_of::<JsonNode>())?, context, |context| {
                let mut values = Vec::new();
                values.try_reserve_exact(elements.len()).map_err(allocation)?;
                for raw in elements { values.push(value(raw, context, depth + 1)?); }
                Ok(JsonValue::Array(values))
            })
        }
        Some(b'{') => {
            context.reserve(bytes(raw.get().len(), size_of::<(String, &RawValue)>())?)?;
            let RawObject(fields) = json::<RawObject<'_>>(raw.get())?;
            JsonNode::with(0, context, |context| {
                let members = Members::with_context(bytes(fields.len(), size_of::<(Text, JsonNode)>())?, context, |context| {
                    let mut values = Vec::new();
                    values.try_reserve_exact(fields.len()).map_err(allocation)?;
                    for (key, raw) in fields {
                        if key.encode_utf16().count() > MAX_TEXT_UNITS { return Err(profile_limit()); }
                        let key = Text::copy(JavaText::from_utf8(&key), context)?;
                        values.push((key, value(raw, context, depth + 1)?));
                    }
                    Ok(values)
                })?;
                Ok(JsonValue::Object(members))
            })
        }
        Some(b'-' | b'0'..=b'9') => {
            let text = raw.get();
            if text.len() > MAX_NUMBER_BYTES { return Err(profile_limit()); }
            if text.contains(['.', 'e', 'E']) {
                let number = double(JavaText::from_utf8(text), context)?;
                JsonNode::with(0, context, |_| Ok(JsonValue::F64(number)))
            } else if let Ok(number) = text.parse::<i64>() {
                JsonNode::with(0, context, |_| Ok(match i32::try_from(number) { Ok(value) => JsonValue::I32(value), Err(_) => JsonValue::I64(number) }))
            } else {
                JsonNode::integer_decimal(JavaText::from_utf8(text), context)
            }
        }
        _ => Err(error("avro-native-json-input", "Schema document contains an unsupported JSON token")),
    }
}
pub(super) fn double(text: JavaText<'_>, context: &mut ConversionContext) -> Result<f64> {
    if text.units().any(|unit| unit > 0x7f) { return Err(error("avro-schema-unchecked", "Java Double text contains a non-ASCII unit")); }
    context.reserve(add(mul(text.len_units(), 128)?, 4096)?)?;
    let mut ascii = String::new();
    ascii.try_reserve_exact(text.len_units()).map_err(allocation)?;
    ascii.extend(text.units().map(|unit| char::from(unit as u8)));
    match Number::parse_double(&ascii) {
        Ok(Number::Double(value)) => Ok(value),
        Ok(_) => unreachable!(),
        Err(NumberError::External(cause)) => Err(cause),
        Err(NumberError::Invalid) => Err(error("avro-schema-unchecked", "Invalid Java Double default text")),
    }
}

#[cfg(test)]
mod tests;
