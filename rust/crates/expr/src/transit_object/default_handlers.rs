use std::sync::Arc;
use base64::{Engine,alphabet,engine::{GeneralPurpose,GeneralPurposeConfig,DecodePaddingMode}};
use rtdb_types::Result;
use super::{Object,invalid};

pub(super) fn binary(text: &str) -> Result<Object> {
    let engine = GeneralPurpose::new(&alphabet::STANDARD,GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true).with_decode_padding_mode(DecodePaddingMode::Indifferent));
    let bytes = engine.decode(text).map_err(|_|invalid())?;
    let unpadded = text.trim_end_matches('=');
    if text.len() != unpadded.len() && text.len() % 4 != 0 { return Err(invalid()); }
    Ok(Object::Bytes(Arc::new(bytes)))
}

pub(super) fn with_meta(representation: Object) -> Result<Object> {
    let values = representation.list().ok_or_else(invalid)?;
    let (Some(value),Some(metadata)) = (values.first(),values.get(1)) else { return Err(invalid()); };
    if !matches!(metadata,Object::Null | Object::Map(_) | Object::TxKey(_)) { return Err(invalid()); }
    if !matches!(value,Object::Vector(_) | Object::List(_) | Object::Set(_) | Object::Map(_) | Object::TxKey(_) | Object::Symbol(_)) { return Err(invalid()); }
    Ok(value.clone())
}
