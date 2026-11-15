use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

const CACHE_LIMIT: usize = 44 * 44;
const MAX_DEPTH: usize = 128;

#[derive(Clone, Debug, PartialEq)]
pub enum TransitValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Keyword(String),
    Symbol(String),
    Bytes(Vec<u8>),
    Array(Vec<TransitValue>),
    Map(Vec<(TransitValue, TransitValue)>),
    Tagged { tag: String, representation: Box<TransitValue> },
}

#[derive(Clone, Debug, PartialEq)]
enum Decoded {
    Value(TransitValue),
    Tag(String),
}

#[derive(Clone, Debug)]
enum Packed {
    Null, Bool(bool), Integer(i64), Float(f64), String(String),
    Array(Vec<Packed>), Map(Vec<(Packed, Packed)>),
}

pub fn decode_msgpack(bytes: &[u8]) -> Result<TransitValue> {
    let mut cursor = Cursor { bytes, offset: 0 };
    let packed = cursor.value(0)?;
    if cursor.offset != bytes.len() { return Err(invalid("Trailing bytes after Transit value")); }
    unpack_value(packed, false, &mut Vec::new(), 0)
}

pub fn encode_msgpack(value: &TransitValue) -> Result<Vec<u8>> {
    let mut encoder = Encoder { bytes: Vec::new(), cache: BTreeMap::new() };
    match value {
        TransitValue::Array(_) | TransitValue::Map(_) => encoder.value(value, false, 0)?,
        TransitValue::Tagged { tag, .. } if tag.len() > 1 => encoder.value(value, false, 0)?,
        _ => {
            encoder.array_len(2)?;
            encoder.string("~#'", false)?;
            encoder.value(value, false, 0)?;
        }
    }
    Ok(encoder.bytes)
}

fn cacheable(value: &str, key: bool) -> bool {
    value.encode_utf16().count() >= 4
        && (key || value.starts_with("~:") || value.starts_with("~$") || value.starts_with("~#"))
}

fn string_value(value: String, key: bool, cache: &mut Vec<Decoded>) -> Result<Decoded> {
    if value.starts_with('^') && value != "^ " {
        let code = &value.as_bytes()[1..];
        let index = match code {
            [low] if (48..92).contains(low) => usize::from(*low - 48),
            [high, low] if (48..92).contains(high) && (48..92).contains(low) => usize::from(*high - 48) * 44 + usize::from(*low - 48),
            _ => return Err(invalid("Invalid Transit cache code")),
        };
        return cache.get(index).cloned().ok_or_else(|| invalid("Transit cache code references an unwritten entry"));
    }
    let decoded = if let Some(tag) = value.strip_prefix("~#") {
        Decoded::Tag(tag.into())
    } else if value.starts_with("~~") || value.starts_with("~^") || value.starts_with("~`") {
        Decoded::Value(TransitValue::String(value[1..].into()))
    } else if let Some(keyword) = value.strip_prefix("~:") {
        Decoded::Value(TransitValue::Keyword(keyword.into()))
    } else if let Some(symbol) = value.strip_prefix("~$") {
        Decoded::Value(TransitValue::Symbol(symbol.into()))
    } else if let Some(binary) = value.strip_prefix("~b") {
        Decoded::Value(TransitValue::Bytes(STANDARD.decode(binary).map_err(invalid)?))
    } else if value.starts_with('~') && value.len() >= 2 {
        let mut chars = value[1..].chars();
        let tag = chars.next().ok_or_else(|| invalid("Missing Transit string tag"))?.to_string();
        Decoded::Value(tagged(tag, TransitValue::String(chars.collect()))?)
    } else {
        Decoded::Value(TransitValue::String(value.clone()))
    };
    if cacheable(&value, key) {
        if cache.len() == CACHE_LIMIT { cache.clear(); }
        cache.push(decoded.clone());
    }
    Ok(decoded)
}

fn unpack_value(packed: Packed, key: bool, cache: &mut Vec<Decoded>, depth: usize) -> Result<TransitValue> {
    match unpack(packed, key, cache, depth)? {
        Decoded::Value(value) => Ok(value),
        Decoded::Tag(_) => Err(invalid("Transit tag has no representation")),
    }
}

fn unpack(packed: Packed, key: bool, cache: &mut Vec<Decoded>, depth: usize) -> Result<Decoded> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    let value = match packed {
        Packed::Null => TransitValue::Null,
        Packed::Bool(value) => TransitValue::Bool(value),
        Packed::Integer(value) => TransitValue::Integer(value),
        Packed::Float(value) => TransitValue::Float(value),
        Packed::String(value) => return string_value(value, key, cache),
        Packed::Array(values) => {
            let mut values = values.into_iter();
            if let Some(first) = values.next() {
                match unpack(first, false, cache, depth + 1)? {
                    Decoded::Tag(tag) => {
                        let rep = values.next().ok_or_else(|| invalid("Transit tag has no representation"))?;
                        if values.next().is_some() { return Err(invalid("Tagged Transit array has more than two entries")); }
                        tagged(tag, unpack_value(rep, false, cache, depth + 1)?)?
                    }
                    Decoded::Value(first) => {
                        let mut array = vec![first];
                        for value in values { array.push(unpack_value(value, false, cache, depth + 1)?); }
                        TransitValue::Array(array)
                    }
                }
            } else { TransitValue::Array(Vec::new()) }
        }
        Packed::Map(entries) => {
            let count = entries.len();
            let mut map = Vec::with_capacity(count);
            for (key, value) in entries {
                match unpack(key, true, cache, depth + 1)? {
                    Decoded::Tag(tag) => {
                        if count != 1 { return Err(invalid("Tagged Transit map has more than one entry")); }
                        return Ok(Decoded::Value(tagged(tag, unpack_value(value, false, cache, depth + 1)?)?));
                    }
                    Decoded::Value(key) => map.push((key, unpack_value(value, false, cache, depth + 1)?)),
                }
            }
            TransitValue::Map(map)
        }
    };
    Ok(Decoded::Value(value))
}

fn tagged(tag: String, representation: TransitValue) -> Result<TransitValue> {
    if tag == "'" { return Ok(representation); }
    if let TransitValue::String(text) = &representation {
        match tag.as_str() {
            "_" => return Ok(TransitValue::Null),
            "?" if text == "t" || text == "f" => return Ok(TransitValue::Bool(text == "t")),
            "i" => return Ok(TransitValue::Integer(text.parse::<i64>().map_err(invalid)?)),
            "d" => return Ok(TransitValue::Float(text.parse::<f64>().map_err(invalid)?)),
            "z" => return Ok(TransitValue::Float(match text.as_str() {
                "NaN" => f64::NAN, "INF" => f64::INFINITY, "-INF" => f64::NEG_INFINITY,
                _ => return Err(invalid("Unknown special float representation")),
            })),
            _ => {}
        }
    }
    Ok(TransitValue::Tagged { tag, representation: Box::new(representation) })
}

struct Cursor<'a> { bytes: &'a [u8], offset: usize }

impl<'a> Cursor<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self.offset.checked_add(count).ok_or_else(|| invalid("MessagePack length overflow"))?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| invalid("Truncated MessagePack value"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn unsigned(&mut self, count: usize) -> Result<u64> {
        Ok(self.take(count)?.iter().fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
    }

    fn text(&mut self, len: usize) -> Result<Packed> {
        Ok(Packed::String(std::str::from_utf8(self.take(len)?).map_err(invalid)?.into()))
    }

    fn array(&mut self, count: usize, depth: usize) -> Result<Packed> {
        if count > self.bytes.len().saturating_sub(self.offset) { return Err(invalid("MessagePack array exceeds its input")); }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count { values.push(self.value(depth + 1)?); }
        Ok(Packed::Array(values))
    }

    fn map(&mut self, count: usize, depth: usize) -> Result<Packed> {
        if count > self.bytes.len().saturating_sub(self.offset) / 2 { return Err(invalid("MessagePack map exceeds its input")); }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count { entries.push((self.value(depth + 1)?, self.value(depth + 1)?)); }
        Ok(Packed::Map(entries))
    }

    fn value(&mut self, depth: usize) -> Result<Packed> {
        if depth > MAX_DEPTH { return Err(invalid("MessagePack nesting limit exceeded")); }
        let tag = self.take(1)?[0];
        Ok(match tag {
            0x00..=0x7f => Packed::Integer(i64::from(tag)),
            0x80..=0x8f => return self.map(usize::from(tag & 0x0f), depth),
            0x90..=0x9f => return self.array(usize::from(tag & 0x0f), depth),
            0xa0..=0xbf => return self.text(usize::from(tag & 0x1f)),
            0xc0 => Packed::Null,
            0xc2 => Packed::Bool(false), 0xc3 => Packed::Bool(true),
            0xca => Packed::Float(f32::from_bits(self.unsigned(4)? as u32) as f64),
            0xcb => Packed::Float(f64::from_bits(self.unsigned(8)?)),
            0xcc => Packed::Integer(self.unsigned(1)? as i64),
            0xcd => Packed::Integer(self.unsigned(2)? as i64),
            0xce => Packed::Integer(self.unsigned(4)? as i64),
            0xcf => Packed::Integer(i64::try_from(self.unsigned(8)?).map_err(invalid)?),
            0xd0 => Packed::Integer(self.unsigned(1)? as i8 as i64),
            0xd1 => Packed::Integer(self.unsigned(2)? as i16 as i64),
            0xd2 => Packed::Integer(self.unsigned(4)? as i32 as i64),
            0xd3 => Packed::Integer(self.unsigned(8)? as i64),
            0xd9 => { let len = self.unsigned(1)? as usize; return self.text(len); }
            0xda => { let len = self.unsigned(2)? as usize; return self.text(len); }
            0xdb => { let len = self.unsigned(4)? as usize; return self.text(len); }
            0xdc => { let len = self.unsigned(2)? as usize; return self.array(len, depth); }
            0xdd => { let len = self.unsigned(4)? as usize; return self.array(len, depth); }
            0xde => { let len = self.unsigned(2)? as usize; return self.map(len, depth); }
            0xdf => { let len = self.unsigned(4)? as usize; return self.map(len, depth); }
            0xe0..=0xff => Packed::Integer(tag as i8 as i64),
            _ => return Err(invalid(format!("MessagePack tag {tag:#x} is not a Transit value"))),
        })
    }
}

struct Encoder { bytes: Vec<u8>, cache: BTreeMap<String, usize> }

impl Encoder {
    fn length(&mut self, len: usize, fix_limit: usize, fix_tag: u8, tag16: u8, tag32: u8) -> Result<()> {
        if len < fix_limit { self.bytes.push(fix_tag | len as u8); }
        else if let Ok(len) = u16::try_from(len) { self.bytes.push(tag16); self.bytes.extend_from_slice(&len.to_be_bytes()); }
        else { self.bytes.push(tag32); self.bytes.extend_from_slice(&u32::try_from(len).map_err(invalid)?.to_be_bytes()); }
        Ok(())
    }

    fn array_len(&mut self, len: usize) -> Result<()> { self.length(len, 16, 0x90, 0xdc, 0xdd) }

    fn string(&mut self, value: &str, key: bool) -> Result<()> {
        let mut encoded = value.to_owned();
        if cacheable(value, key) {
            if let Some(index) = self.cache.get(value).copied() {
                encoded = if index < 44 { format!("^{}", char::from(index as u8 + 48)) }
                    else { format!("^{}{}", char::from((index / 44) as u8 + 48), char::from((index % 44) as u8 + 48)) };
            } else {
                if self.cache.len() == CACHE_LIMIT { self.cache.clear(); }
                self.cache.insert(value.into(), self.cache.len());
            }
        }
        let bytes = encoded.as_bytes();
        if bytes.len() <= 31 { self.bytes.push(0xa0 | bytes.len() as u8); }
        else if let Ok(len) = u8::try_from(bytes.len()) { self.bytes.extend_from_slice(&[0xd9, len]); }
        else if let Ok(len) = u16::try_from(bytes.len()) { self.bytes.push(0xda); self.bytes.extend_from_slice(&len.to_be_bytes()); }
        else { self.bytes.push(0xdb); self.bytes.extend_from_slice(&u32::try_from(bytes.len()).map_err(invalid)?.to_be_bytes()); }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn integer(&mut self, value: i64) {
        if (-32..=127).contains(&value) { self.bytes.push(value as u8); }
        else if value >= 0 {
            if let Ok(value) = u8::try_from(value) { self.bytes.extend_from_slice(&[0xcc, value]); }
            else if let Ok(value) = u16::try_from(value) { self.bytes.push(0xcd); self.bytes.extend_from_slice(&value.to_be_bytes()); }
            else if let Ok(value) = u32::try_from(value) { self.bytes.push(0xce); self.bytes.extend_from_slice(&value.to_be_bytes()); }
            else { self.bytes.push(0xcf); self.bytes.extend_from_slice(&(value as u64).to_be_bytes()); }
        } else if let Ok(value) = i8::try_from(value) { self.bytes.extend_from_slice(&[0xd0, value as u8]); }
        else if let Ok(value) = i16::try_from(value) { self.bytes.push(0xd1); self.bytes.extend_from_slice(&value.to_be_bytes()); }
        else if let Ok(value) = i32::try_from(value) { self.bytes.push(0xd2); self.bytes.extend_from_slice(&value.to_be_bytes()); }
        else { self.bytes.push(0xd3); self.bytes.extend_from_slice(&value.to_be_bytes()); }
    }

    fn value(&mut self, value: &TransitValue, key: bool, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
        match value {
            TransitValue::Null => self.bytes.push(0xc0),
            TransitValue::Bool(value) => self.bytes.push(if *value { 0xc3 } else { 0xc2 }),
            TransitValue::Integer(value) => self.integer(*value),
            TransitValue::Float(value) => { self.bytes.push(0xcb); self.bytes.extend_from_slice(&value.to_bits().to_be_bytes()); }
            TransitValue::String(value) => {
                if value.starts_with(['~', '^', '`']) { self.string(&format!("~{value}"), key)?; }
                else { self.string(value, key)?; }
            }
            TransitValue::Keyword(value) => self.string(&format!("~:{value}"), key)?,
            TransitValue::Symbol(value) => self.string(&format!("~${value}"), key)?,
            TransitValue::Bytes(value) => self.string(&format!("~b{}", STANDARD.encode(value)), key)?,
            TransitValue::Array(values) => {
                self.array_len(values.len())?;
                for value in values { self.value(value, false, depth + 1)?; }
            }
            TransitValue::Map(entries) => {
                self.length(entries.len(), 16, 0x80, 0xde, 0xdf)?;
                for (key, value) in entries { self.value(key, true, depth + 1)?; self.value(value, false, depth + 1)?; }
            }
            TransitValue::Tagged { tag, representation } => {
                if tag.chars().count() == 1 {
                    if let TransitValue::String(text) = representation.as_ref() {
                        self.string(&format!("~{tag}{text}"), key)?;
                        return Ok(());
                    }
                }
                if key { return Err(invalid("Compound Transit tags cannot be map keys")); }
                self.array_len(2)?;
                self.string(&format!("~#{tag}"), false)?;
                self.value(representation, false, depth + 1)?;
            }
        }
        Ok(())
    }
}

pub fn encode_anomaly(error: &Anomaly) -> Result<Vec<u8>> {
    let category = match error.category {
        AnomalyCategory::Incorrect => "incorrect", AnomalyCategory::Unsupported => "unsupported",
        AnomalyCategory::NotFound => "not-found", AnomalyCategory::Forbidden => "forbidden",
        AnomalyCategory::Conflict => "conflict", AnomalyCategory::Busy => "busy",
        AnomalyCategory::Unavailable => "unavailable", AnomalyCategory::Interrupted => "interrupted",
        AnomalyCategory::Fault => "fault",
    };
    let context = error.context.iter().map(|(key, value)| (TransitValue::Keyword(key.clone()), from_json(value))).collect();
    encode_msgpack(&TransitValue::Tagged { tag: "xt/error".into(), representation: Box::new(TransitValue::Array(vec![
        TransitValue::Keyword(category.into()), TransitValue::Keyword(error.code.clone()),
        TransitValue::String(error.message.clone()), TransitValue::Map(context),
    ])) })
}

fn from_json(value: &serde_json::Value) -> TransitValue {
    match value {
        serde_json::Value::Null => TransitValue::Null,
        serde_json::Value::Bool(value) => TransitValue::Bool(*value),
        serde_json::Value::Number(value) => value.as_i64().map(TransitValue::Integer).unwrap_or_else(|| TransitValue::Float(value.as_f64().unwrap_or_default())),
        serde_json::Value::String(value) => TransitValue::String(value.clone()),
        serde_json::Value::Array(values) => TransitValue::Array(values.iter().map(from_json).collect()),
        serde_json::Value::Object(values) => TransitValue::Map(values.iter().map(|(key, value)| (TransitValue::String(key.clone()), from_json(value))).collect()),
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-transit", error.to_string())
}
