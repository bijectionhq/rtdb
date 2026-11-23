use std::{collections::BTreeMap, mem::size_of, sync::Arc};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, MemoryReservation, Result};

mod arrow_json;
pub use arrow_json::{write_arrow_json, write_arrow_row_json};

const CACHE_LIMIT: usize = 44 * 44;

pub use rtdb_types::transit_value::{TransitValue, from_json_value, json_value_allocation_bound, to_json_value};
use rtdb_types::transit_value::MAX_DEPTH;

pub use crate::transit_cell::{document_to_transit, value_to_transit};

/// Constructs decoded values before they enter the Transit cache; cache hits clone `Value`.
/// Array and map states remain caller-owned until completion; item insertion runs in wire order and may fail.
/// Sizes are allocation hints for raw containers, whose tags or JSON map markers can select another constructor.
pub trait TransitReadBuilder {
    type Value: Clone;
    type Array;
    type Map;
    fn null(&mut self) -> Result<Self::Value>;
    fn boolean(&mut self, value: bool) -> Result<Self::Value>;
    fn integer(&mut self, value: i64) -> Result<Self::Value>;
    fn float(&mut self, value: f64) -> Result<Self::Value>;
    fn string(&mut self, value: String) -> Result<Self::Value>;
    fn keyword(&mut self, value: String) -> Result<Self::Value>;
    fn symbol(&mut self, value: String) -> Result<Self::Value>;
    fn bytes(&mut self, value: Vec<u8>) -> Result<Self::Value>;
    /// Receives Base64 text; the default requires canonical padding and zero unused bits.
    fn binary(&mut self, representation: String) -> Result<Self::Value> { self.bytes(STANDARD.decode(representation).map_err(invalid)?) }
    /// Selects direct array construction for this tag; nested tags and JSON map markers bypass it.
    fn array_handler(&self, _tag: &str) -> bool { false }
    fn start_array(&mut self, tag: Option<&str>, size: usize) -> Result<Self::Array>;
    fn array_item(&mut self, array: &mut Self::Array, value: Self::Value) -> Result<()>;
    fn array_float(&mut self, array: &mut Self::Array, value: f64) -> Result<()> {
        let value = self.float(value)?;
        self.array_item(array, value)
    }
    fn array_floats(&mut self, array: &mut Self::Array, values: Vec<f64>) -> Result<()> {
        for value in values { self.array_float(array, value)?; }
        Ok(())
    }
    fn finish_array(&mut self, array: Self::Array) -> Result<Self::Value>;
    fn start_map(&mut self, size: usize) -> Result<Self::Map>;
    fn map_entry(&mut self, map: &mut Self::Map, key: Self::Value, value: Self::Value) -> Result<()>;
    fn finish_map(&mut self, map: Self::Map) -> Result<Self::Value>;
    fn tagged(&mut self, tag: String, representation: Self::Value) -> Result<Self::Value>;
    fn tagged_float(&mut self, tag: String, value: f64) -> Result<Self::Value> {
        let representation = self.float(value)?;
        self.tagged(tag, representation)
    }
}

struct WireValues;
impl TransitReadBuilder for WireValues {
    type Value = TransitValue;
    type Array = Vec<TransitValue>;
    type Map = Vec<(TransitValue,TransitValue)>;
    fn null(&mut self) -> Result<TransitValue> { Ok(TransitValue::Null) }
    fn boolean(&mut self, value: bool) -> Result<TransitValue> { Ok(TransitValue::Bool(value)) }
    fn integer(&mut self, value: i64) -> Result<TransitValue> { Ok(TransitValue::Integer(value)) }
    fn float(&mut self, value: f64) -> Result<TransitValue> { Ok(TransitValue::Float(value)) }
    fn string(&mut self, value: String) -> Result<TransitValue> { Ok(TransitValue::String(value)) }
    fn keyword(&mut self, value: String) -> Result<TransitValue> { Ok(TransitValue::Keyword(value)) }
    fn symbol(&mut self, value: String) -> Result<TransitValue> { Ok(TransitValue::Symbol(value)) }
    fn bytes(&mut self, value: Vec<u8>) -> Result<TransitValue> { Ok(TransitValue::Bytes(value)) }
    fn start_array(&mut self, _tag: Option<&str>, size: usize) -> Result<Self::Array> { Ok(Vec::with_capacity(size)) }
    fn array_item(&mut self, array: &mut Self::Array, value: TransitValue) -> Result<()> { array.push(value);Ok(()) }
    fn finish_array(&mut self, array: Self::Array) -> Result<TransitValue> { Ok(TransitValue::Array(array)) }
    fn start_map(&mut self, size: usize) -> Result<Self::Map> { Ok(Vec::with_capacity(size)) }
    fn map_entry(&mut self, map: &mut Self::Map, key: TransitValue, value: TransitValue) -> Result<()> { map.push((key,value));Ok(()) }
    fn finish_map(&mut self, map: Self::Map) -> Result<TransitValue> { Ok(TransitValue::Map(map)) }
    fn tagged(&mut self, tag: String, representation: TransitValue) -> Result<TransitValue> { resolve_tag(tag, representation) }
}

#[derive(Clone, Debug, PartialEq)]
enum Decoded<V> {
    Value(V),
    Tag(String),
}

#[derive(Default)]
struct ReadAdmission(Option<Arc<MemoryReservation>>);
impl ReadAdmission {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> { Ok(Self(Some(memory.reserve(4096)?))) }
    fn items<T>(&mut self, count: usize) -> Result<()> {
        self.bytes(count.checked_mul(size_of::<T>()).ok_or_else(read_overflow)?)
    }
    fn bytes(&mut self, bytes: usize) -> Result<()> {
        if let Some(reservation) = &mut self.0 {
            let total = reservation.bytes().checked_add(bytes).ok_or_else(read_overflow)?;
            Arc::get_mut(reservation).expect("Transit decode owns its reservation").grow_to(total)?;
        }
        Ok(())
    }
}
fn read_overflow() -> Anomaly { invalid("Transit decode allocation exceeds addressable memory") }

struct CacheEntry<V> { value: Decoded<V>, clone_bytes: usize }
struct ReadCache<V> { slots: Vec<CacheEntry<V>>, next: usize, admission: ReadAdmission }
impl<V: Clone> ReadCache<V> {
    fn new() -> Self { Self { slots: Vec::new(), next: 0, admission: ReadAdmission::default() } }
    fn get(&mut self, index: usize) -> Result<Decoded<V>> {
        let entry = self.slots.get(index).ok_or_else(|| invalid("Transit cache code references an unwritten entry"))?;
        self.admission.bytes(entry.clone_bytes)?;
        Ok(entry.value.clone())
    }
    fn insert(&mut self, value: &Decoded<V>, clone_bytes: usize) -> Result<()> {
        if self.slots.len() == self.slots.capacity() && self.slots.len() < CACHE_LIMIT && self.admission.0.is_some() {
            let capacity = (self.slots.len() * 2).clamp(8, CACHE_LIMIT);
            self.admission.items::<CacheEntry<V>>(capacity)?;
            self.slots.try_reserve_exact(capacity - self.slots.len()).map_err(invalid)?;
        }
        self.admission.bytes(clone_bytes)?;
        let entry = CacheEntry { value: value.clone(), clone_bytes };
        if self.next == CACHE_LIMIT { self.next = 0; }
        if self.next < self.slots.len() { self.slots[self.next] = entry; } else { self.slots.push(entry); }
        self.next += 1;
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum Packed {
    Null, Bool(bool), Integer(i64), Float(f64), String(String),
    Array(Vec<Packed>), Map(Vec<(Packed, Packed)>),
    Floats(Vec<f64>), TaggedFloats(Box<(String, Vec<f64>)>),
}

pub fn decode_msgpack(bytes: &[u8]) -> Result<TransitValue> {
    unpack_value(packed_msgpack(bytes)?, false, &mut ReadCache::new(), 0, &mut WireValues)
}

/// Decodes one MessagePack value, admitting wire containers and cache expansion before allocation.
/// Retain the returned lease until the value is dropped or adopted by another charged owner.
pub fn decode_msgpack_in(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<(TransitValue, Arc<MemoryReservation>)> {
    decode_msgpack_with_in(bytes, memory, &mut WireValues)
}

pub fn decode_msgpack_with_in<B: TransitReadBuilder>(
    bytes: &[u8], memory: &Arc<MemoryBudget>, builder: &mut B,
) -> Result<(B::Value, Arc<MemoryReservation>)> {
    let mut cursor = Cursor { bytes, offset: 0, admission: ReadAdmission::new(memory)? };
    let packed = cursor.complete_value()?;
    let mut cache = ReadCache { admission: ReadAdmission::new(memory)?, ..ReadCache::new() };
    let value = unpack_value(packed, false, &mut cache, 0, builder)?;
    Ok((value, cache.admission.0.expect("tracked Transit decode has a reservation")))
}

fn packed_msgpack(bytes: &[u8]) -> Result<Packed> {
    Cursor { bytes, offset: 0, admission: ReadAdmission::default() }.complete_value()
}

/// Decodes Transit JSON, retaining object entry order for Transit cache references.
pub fn decode_json(bytes: &[u8]) -> Result<TransitValue> {
    decode_json_with(bytes, &mut WireValues)
}

pub fn decode_json_with<B: TransitReadBuilder>(bytes: &[u8], builder: &mut B) -> Result<B::Value> {
    unpack_value(packed_json(bytes)?, false, &mut ReadCache::new(), 0, builder)
}

fn packed_json(bytes: &[u8]) -> Result<Packed> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let packed = <JsonPacked as serde::Deserialize>::deserialize(&mut decoder).map_err(invalid)?;
    decoder.end().map_err(invalid)?;
    Ok(packed.0)
}

/// Chooses MessagePack or JSON syntax before constructing values, preserving builder failures.
pub fn decode_with<B: TransitReadBuilder>(bytes: &[u8], builder: &mut B) -> Result<B::Value> {
    let packed = packed_msgpack(bytes).or_else(|_| packed_json(bytes))?;
    unpack_value(packed, false, &mut ReadCache::new(), 0, builder)
}

/// Reads consecutive top-level values, resetting Transit cache indices for each value.
pub fn decode_json_sequence(bytes: &[u8]) -> Result<Vec<TransitValue>> {
    decode_json_sequence_with(bytes, &mut WireValues)
}

pub fn decode_json_sequence_with<B: TransitReadBuilder>(bytes: &[u8], builder: &mut B) -> Result<Vec<B::Value>> {
    serde_json::Deserializer::from_slice(bytes).into_iter::<JsonPacked>()
        .map(|packed| unpack_value(packed.map_err(|error| json_parse(bytes, error))?.0, false, &mut ReadCache::new(), 0, builder)).collect()
}

/// Malformed JSON is the reference's `xtdb.error/json-parse`; a stray close marker carries Jackson's wording.
fn json_parse(bytes: &[u8], error: serde_json::Error) -> Anomaly {
    if !matches!(error.classify(), serde_json::error::Category::Syntax | serde_json::error::Category::Eof) { return invalid(error); }
    let offending = bytes.split(|byte| *byte == b'\n').nth(error.line().saturating_sub(1)).and_then(|line| line.get(error.column().saturating_sub(1))).copied();
    let message = match offending {
        Some(marker @ (b'}' | b']')) => format!("Unexpected close marker '{}'", marker as char),
        _ => error.to_string(),
    };
    Anomaly::incorrect("xtdb.error/json-parse", message)
}

/// Reads consecutive top-level values; a truncated trailing value is an error.
pub fn decode_msgpack_sequence(bytes: &[u8]) -> Result<Vec<TransitValue>> {
    decode_msgpack_sequence_with(bytes, &mut WireValues)
}

pub fn decode_msgpack_sequence_with<B: TransitReadBuilder>(bytes: &[u8], builder: &mut B) -> Result<Vec<B::Value>> {
    let mut cursor = Cursor { bytes, offset: 0, admission: ReadAdmission::default() };
    let mut values = Vec::new();
    while cursor.offset < bytes.len() { values.push(unpack_value(cursor.value(0)?, false, &mut ReadCache::new(), 0, builder)?); }
    Ok(values)
}

/// Encodes Transit JSON with array maps and top-level quoting; cache substitution is optional on the wire.
pub fn encode_json(value: &TransitValue) -> Result<Vec<u8>> {
    let encoded = json_wire(value, false, 0)?;
    let encoded = match value {
        TransitValue::Array(_) | TransitValue::Map(_) => encoded,
        TransitValue::Tagged { tag, .. } if tag.chars().count() > 1 => encoded,
        _ => serde_json::Value::Array(vec![serde_json::Value::String("~#'".into()), encoded]),
    };
    serde_json::to_vec(&encoded).map_err(invalid)
}

fn json_wire(value: &TransitValue, key: bool, depth: usize) -> Result<serde_json::Value> {
    use serde_json::Value as Json;
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    Ok(match value {
        TransitValue::Null if key => Json::String("~_".into()),
        TransitValue::Null => Json::Null,
        TransitValue::Bool(value) if key => Json::String(if *value { "~?t" } else { "~?f" }.into()),
        TransitValue::Bool(value) => Json::Bool(*value),
        TransitValue::Integer(value) if key || !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(value) => Json::String(format!("~i{value}")),
        TransitValue::Integer(value) => Json::Number((*value).into()),
        TransitValue::Float(value) if !value.is_finite() => Json::String(if value.is_nan() { "~zNaN" } else if value.is_sign_positive() { "~zINF" } else { "~z-INF" }.into()),
        TransitValue::Float(value) if key => Json::String(format!("~d{value}")),
        TransitValue::Float(value) => Json::Number(serde_json::Number::from_f64(*value).ok_or_else(|| invalid("Invalid JSON number"))?),
        TransitValue::Float32(value) => {
            if !value.is_finite() { Json::String(if value.is_nan() { "~zNaN" } else if value.is_sign_positive() { "~zINF" } else { "~z-INF" }.into()) }
            else if key { Json::String(format!("~d{}",rtdb_types::java_f32_text(*value))) }
            else { serde_json::from_str(&rtdb_types::java_f32_text(*value)).map_err(invalid)? }
        }
        TransitValue::String(value) => Json::String(if value.starts_with(['~', '^', '`']) { format!("~{value}") } else { value.clone() }),
        TransitValue::Keyword(value) => Json::String(format!("~:{value}")),
        TransitValue::Symbol(value) => Json::String(format!("~${value}")),
        TransitValue::Bytes(value) => Json::String(format!("~b{}", STANDARD.encode(value))),
        TransitValue::Array(values) => Json::Array(values.iter().map(|value| json_wire(value, false, depth + 1)).collect::<Result<_>>()?),
        TransitValue::Map(entries) => {
            let simple = entries.iter().all(|(key, _)| stringable(key));
            let mut values = Vec::with_capacity(entries.len().checked_mul(2).and_then(|size| size.checked_add(1)).ok_or_else(|| invalid("Transit map length overflow"))?);
            if simple { values.push(Json::String("^ ".into())); }
            for (key, value) in entries { values.push(json_wire(key, simple, depth + 1)?); values.push(json_wire(value, false, depth + 1)?); }
            if simple { Json::Array(values) } else { Json::Array(vec![Json::String("~#cmap".into()), Json::Array(values)]) }
        }
        TransitValue::Tagged { tag, representation } => {
            if tag.is_empty() { return Err(invalid("Transit tag is empty")); }
            if tag.chars().count() == 1 && matches!(representation.as_ref(), TransitValue::String(_)) {
                let TransitValue::String(text) = representation.as_ref() else { unreachable!() };
                Json::String(format!("~{tag}{text}"))
            } else {
                if key { return Err(invalid("Compound Transit tags cannot be map keys")); }
                Json::Array(vec![Json::String(format!("~#{tag}")), json_wire(representation, false, depth + 1)?])
            }
        }
    })
}

struct JsonPacked(Packed);

impl<'de> serde::Deserialize<'de> for JsonPacked {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> std::result::Result<Self, D::Error> {
        decoder.deserialize_any(JsonVisitor(None)).map(|packed| Self(packed.expect("root JSON value")))
    }
}

struct JsonVisitor<'a>(Option<&'a mut Vec<Packed>>);

impl<'de> serde::de::DeserializeSeed<'de> for JsonVisitor<'_> {
    type Value = Option<Packed>;

    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> std::result::Result<Self::Value, D::Error> {
        decoder.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for JsonVisitor<'_> {
    type Value = Option<Packed>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("a Transit JSON value") }
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> { Ok(Some(Packed::Null)) }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> { Ok(Some(Packed::Bool(value))) }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> { Ok(Some(Packed::Integer(value))) }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(Some(Packed::Integer(i64::try_from(value).map_err(E::custom)?)))
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
        if let Some(array) = self.0 {
            push_float(array, value);
            Ok(None)
        } else {
            Ok(Some(Packed::Float(value)))
        }
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> { Ok(Some(Packed::String(value.into()))) }
    fn visit_string<E: serde::de::Error>(self, value: String) -> std::result::Result<Self::Value, E> { Ok(Some(Packed::String(value))) }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(JsonVisitor(Some(&mut values)))? {
            if let Some(value) = value { values.push(value); }
        }
        if let Some(array) = self.0 {
            push_item(array, values);
            Ok(None)
        } else {
            Ok(Some(Packed::Array(values)))
        }
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some((key, value)) = map.next_entry::<String, JsonPacked>()? {
            values.push((Packed::String(key), value.0));
        }
        Ok(Some(Packed::Map(values)))
    }
}

fn push_float(array: &mut Vec<Packed>, value: f64) {
    match array.last_mut() {
        Some(item @ Packed::Float(_)) => {
            let Packed::Float(first) = *item else { unreachable!() };
            *item = Packed::Floats(vec![first, value]);
        }
        Some(Packed::Floats(values)) => values.push(value),
        _ => array.push(Packed::Float(value)),
    }
}

fn push_item(array: &mut Vec<Packed>, values: Vec<Packed>) {
    if let [Packed::String(marker), Packed::Float(float)] = values.as_slice()
        && (marker == "~#f32" || marker.starts_with('^') && marker != "^ ")
    {
        match array.last_mut() {
            Some(Packed::TaggedFloats(run)) if &run.0 == marker => run.1.push(*float),
            _ => array.push(Packed::TaggedFloats(Box::new((marker.clone(), vec![*float])))),
        }
    } else {
        array.push(Packed::Array(values));
    }
}

fn packed_array_len(values: &[Packed]) -> usize {
    values.iter().map(|value| match value { Packed::Floats(values) => values.len(), Packed::TaggedFloats(run) => run.1.len(), _ => 1 }).sum()
}

fn packed_items(values: impl Iterator<Item = Packed>) -> impl Iterator<Item = Packed> {
    values.flat_map(|value| {
        let (mut single, marker, mut floats) = match value {
            Packed::Floats(values) => (None, None, values.into_iter()),
            Packed::TaggedFloats(run) => (None, Some(run.0), run.1.into_iter()),
            value => (Some(value), None, Vec::new().into_iter()),
        };
        std::iter::from_fn(move || {
            if let Some(value) = single.take() { return Some(value); }
            let value = Packed::Float(floats.next()?);
            Some(match &marker {
                Some(marker) => Packed::Array(vec![Packed::String(marker.clone()), value]),
                None => value,
            })
        })
    })
}

/// Bounds MessagePack output capacity, temporary text and the encoder's string cache for this value.
/// Reserve before encoding and retain the charge while the returned bytes live.
/// Size overflow and nesting beyond the encoder's depth limit return errors.
pub fn msgpack_allocation_bound(value: &TransitValue) -> Result<usize> {
    fn bound(value: &TransitValue,depth: usize) -> Result<usize> {
        if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
        let add = |left: usize,right: usize|left.checked_add(right).ok_or_else(||invalid("Transit encoding allocation exceeds addressable memory"));
        let text = |size: usize|size.checked_mul(16).ok_or_else(||invalid("Transit encoding allocation exceeds addressable memory"));
        let payload = match value {
            TransitValue::String(value) | TransitValue::Keyword(value) | TransitValue::Symbol(value) => text(value.len())?,
            TransitValue::Bytes(value) => text(value.len())?,
            TransitValue::Array(values) => values.iter().try_fold(0,|bytes,value|add(bytes,bound(value,depth + 1)?))?,
            TransitValue::Map(values) => values.iter().try_fold(0,|bytes,(key,value)|add(add(bytes,bound(key,depth + 1)?)?,bound(value,depth + 1)?))?,
            TransitValue::Tagged { tag,representation } => {
                let representation = if tag.chars().count() == 1 {
                    match representation.as_ref() { TransitValue::String(value) => add(512,text(value.len())?)?,value => bound(value,depth + 1)? }
                } else { bound(representation,depth + 1)? };
                add(text(tag.len())?,representation)?
            },
            _ => 0,
        };
        add(512,payload)
    }
    bound(value,0)?.checked_add(4096).ok_or_else(||invalid("Transit encoding allocation exceeds addressable memory"))
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

fn string_value<B: TransitReadBuilder>(value: String, key: bool, cache: &mut ReadCache<B::Value>, builder: &mut B) -> Result<Decoded<B::Value>> {
    if value.starts_with('^') && value != "^ " {
        let code = &value.as_bytes()[1..];
        let index = match code {
            [low] if (48..92).contains(low) => usize::from(*low - 48),
            [high, low] if (48..92).contains(high) && (48..92).contains(low) => usize::from(*high - 48) * 44 + usize::from(*low - 48),
            _ => return Err(invalid("Invalid Transit cache code")),
        };
        return cache.get(index);
    }
    let clone_bytes = value.len().checked_mul(2).and_then(|bytes| bytes.checked_add(size_of::<B::Value>())).ok_or_else(read_overflow)?;
    cache.admission.bytes(clone_bytes)?;
    let decoded = if let Some(tag) = value.strip_prefix("~#") {
        Decoded::Tag(tag.into())
    } else if value.starts_with("~~") || value.starts_with("~^") || value.starts_with("~`") {
        Decoded::Value(builder.string(value[1..].into())?)
    } else if let Some(keyword) = value.strip_prefix("~:") {
        Decoded::Value(builder.keyword(keyword.into())?)
    } else if let Some(symbol) = value.strip_prefix("~$") {
        Decoded::Value(builder.symbol(symbol.into())?)
    } else if let Some(binary) = value.strip_prefix("~b") {
        Decoded::Value(builder.binary(binary.into())?)
    } else if value.starts_with('~') && value.len() >= 2 {
        let mut chars = value[1..].chars();
        let tag = chars.next().ok_or_else(|| invalid("Missing Transit string tag"))?.to_string();
        let representation = builder.string(chars.collect())?;
        Decoded::Value(builder.tagged(tag, representation)?)
    } else {
        Decoded::Value(builder.string(value.clone())?)
    };
    if cacheable(&value, key) {
        cache.insert(&decoded, clone_bytes)?;
    }
    Ok(decoded)
}

fn unpack_value<B: TransitReadBuilder>(packed: Packed, key: bool, cache: &mut ReadCache<B::Value>, depth: usize, builder: &mut B) -> Result<B::Value> {
    match unpack(packed, key, cache, depth, builder)? {
        Decoded::Value(value) => Ok(value),
        Decoded::Tag(_) => Err(invalid("Transit tag has no representation")),
    }
}

fn unpack<B: TransitReadBuilder>(packed: Packed, key: bool, cache: &mut ReadCache<B::Value>, depth: usize, builder: &mut B) -> Result<Decoded<B::Value>> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    let value = match packed {
        Packed::Null => builder.null()?,
        Packed::Bool(value) => builder.boolean(value)?,
        Packed::Integer(value) => builder.integer(value)?,
        Packed::Float(value) => builder.float(value)?,
        Packed::Floats(_) | Packed::TaggedFloats(_) => unreachable!("float runs are unpacked by their parent array"),
        Packed::String(value) => return string_value(value, key, cache, builder),
        Packed::Array(values) => return unpack_array(values,None,cache,depth,builder).map(Decoded::Value),
        Packed::Map(entries) => {
            let count = entries.len();
            cache.admission.items::<(B::Value, B::Value)>(count)?;
            let mut map = builder.start_map(count)?;
            for (key, value) in entries {
                match unpack(key, true, cache, depth + 1, builder)? {
                    Decoded::Tag(tag) => {
                        if count != 1 { return Err(invalid("Tagged Transit map has more than one entry")); }
                        return unpack_tagged(tag,value,cache,depth + 1,builder).map(Decoded::Value);
                    }
                    Decoded::Value(key) => { let value = unpack_value(value,false,cache,depth + 1,builder)?;builder.map_entry(&mut map,key,value)?; }
                }
            }
            builder.finish_map(map)?
        }
    };
    Ok(Decoded::Value(value))
}

fn unpack_tagged<B: TransitReadBuilder>(tag: String,representation: Packed,cache: &mut ReadCache<B::Value>,depth: usize,builder: &mut B) -> Result<B::Value> {
    if builder.array_handler(&tag) {
        if let Packed::Array(values) = representation { return unpack_array(values,Some(&tag),cache,depth,builder); }
    }
    cache.admission.items::<B::Value>(1)?;
    if let ("cmap", Packed::Array(values)) = (tag.as_str(), &representation) {
        cache.admission.items::<(B::Value, B::Value)>(packed_array_len(values) / 2)?;
    }
    if let Packed::Float(value) = representation {
        if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
        return builder.tagged_float(tag, value);
    }
    let representation = unpack_value(representation,false,cache,depth,builder)?;
    builder.tagged(tag,representation)
}

fn unpack_array<B: TransitReadBuilder>(values: Vec<Packed>,tag: Option<&str>,cache: &mut ReadCache<B::Value>,depth: usize,builder: &mut B) -> Result<B::Value> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    let size = packed_array_len(&values);
    let mut values = values.into_iter().peekable();
    cache.admission.items::<B::Value>(size)?;
    let mut array = builder.start_array(tag, size)?;
    if matches!(values.peek(), Some(Packed::String(_))) {
        let first = values.next().expect("first string");
        if matches!(&first, Packed::String(marker) if marker == "^ ") {
            cache.admission.items::<(B::Value, B::Value)>(size / 2)?;
            let mut map = builder.start_map(size / 2)?;
            let mut values = packed_items(values);
            while let Some(key) = values.next() {
                let value = values.next().ok_or_else(|| invalid("Transit map has an unmatched key"))?;
                let key = unpack_value(key, true, cache, depth + 1, builder)?;
                let value = unpack_value(value, false, cache, depth + 1, builder)?;
                builder.map_entry(&mut map, key, value)?;
            }
            return builder.finish_map(map);
        }
        match unpack(first, false, cache, depth + 1, builder)? {
            Decoded::Tag(tag) => {
                if size == 1 { return Err(invalid("Transit tag has no representation")); }
                if size > 2 { return Err(invalid("Tagged Transit array has more than two entries")); }
                let representation = packed_items(values).next().expect("tag representation");
                return unpack_tagged(tag, representation, cache, depth + 1, builder);
            }
            Decoded::Value(first) => builder.array_item(&mut array, first)?,
        }
    }
    for value in values {
        match value {
            Packed::Float(value) => {
                if depth == MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
                builder.array_float(&mut array, value)?;
            }
            Packed::Floats(values) => {
                if depth == MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
                builder.array_floats(&mut array, values)?;
            }
            Packed::TaggedFloats(run) => {
                if depth == MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
                for value in run.1 {
                    cache.admission.items::<B::Value>(2)?;
                    let mut pair = builder.start_array(None, 2)?;
                    if depth + 1 == MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
                    let value = match string_value(run.0.clone(), false, cache, builder)? {
                        Decoded::Tag(tag) => {
                            cache.admission.items::<B::Value>(1)?;
                            builder.tagged_float(tag, value)?
                        }
                        Decoded::Value(first) => {
                            builder.array_item(&mut pair, first)?;
                            builder.array_float(&mut pair, value)?;
                            builder.finish_array(pair)?
                        }
                    };
                    builder.array_item(&mut array, value)?;
                }
            }
            value => {
                let value = unpack_value(value, false, cache, depth + 1, builder)?;
                builder.array_item(&mut array, value)?;
            }
        }
    }
    builder.finish_array(array)
}

pub fn resolve_tag(tag: String, representation: TransitValue) -> Result<TransitValue> {
    if tag == "'" { return Ok(representation); }
    if tag == "cmap" {
        let TransitValue::Array(values) = representation else { return Err(invalid("Composite map representation is not an array")); };
        if values.len() % 2 != 0 { return Err(invalid("Composite map has an unmatched key")); }
        let mut values = values.into_iter();
        let mut entries = Vec::with_capacity(values.len() / 2);
        while let Some(key) = values.next() { entries.push((key, values.next().ok_or_else(|| invalid("Composite map has no value"))?)); }
        return Ok(TransitValue::Map(entries));
    }
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

struct Cursor<'a> { bytes: &'a [u8], offset: usize, admission: ReadAdmission }

impl<'a> Cursor<'a> {
    fn complete_value(&mut self) -> Result<Packed> {
        let value = self.value(0)?;
        if self.offset != self.bytes.len() { return Err(invalid("Trailing bytes after Transit value")); }
        Ok(value)
    }

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
        let text = std::str::from_utf8(self.take(len)?).map_err(invalid)?;
        self.admission.bytes(len)?;
        Ok(Packed::String(text.into()))
    }

    fn array(&mut self, count: usize, depth: usize) -> Result<Packed> {
        if count > self.bytes.len().saturating_sub(self.offset) { return Err(invalid("MessagePack array exceeds its input")); }
        self.admission.items::<Packed>(count)?;
        let mut values = Vec::new();
        for _ in 0..count {
            if values.len() == values.capacity() {
                let next = (values.len() * 2).clamp(1, count);
                values.reserve_exact(next - values.len());
            }
            if depth < MAX_DEPTH && let Some(tag @ (0xca | 0xcb)) = self.bytes.get(self.offset).copied() {
                self.offset += 1;
                let value = self.float(tag)?;
                push_float(&mut values, value);
            } else {
                match self.value(depth + 1)? {
                    Packed::Array(items) => push_item(&mut values, items),
                    value => values.push(value),
                }
            }
        }
        Ok(Packed::Array(values))
    }

    fn map(&mut self, count: usize, depth: usize) -> Result<Packed> {
        if count > self.bytes.len().saturating_sub(self.offset) / 2 { return Err(invalid("MessagePack map exceeds its input")); }
        self.admission.items::<(Packed, Packed)>(count)?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count { entries.push((self.value(depth + 1)?, self.value(depth + 1)?)); }
        Ok(Packed::Map(entries))
    }

    fn float(&mut self, tag: u8) -> Result<f64> {
        if tag == 0xca { Ok(f32::from_bits(self.unsigned(4)? as u32) as f64) }
        else { Ok(f64::from_bits(self.unsigned(8)?)) }
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
            0xca | 0xcb => Packed::Float(self.float(tag)?),
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
            TransitValue::Float(value) => {
                if value.is_finite() { self.bytes.push(0xcb); self.bytes.extend_from_slice(&value.to_bits().to_be_bytes()); }
                else { self.string(if value.is_nan() { "~zNaN" } else if value.is_sign_positive() { "~zINF" } else { "~z-INF" }, key)?; }
            }
            TransitValue::Float32(value) => {
                if value.is_finite() { self.bytes.push(0xca); self.bytes.extend_from_slice(&value.to_bits().to_be_bytes()); }
                else { self.string(if value.is_nan() { "~zNaN" } else if value.is_sign_positive() { "~zINF" } else { "~z-INF" }, key)?; }
            }
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
                if entries.iter().all(|(key, _)| stringable(key)) {
                    self.length(entries.len(), 16, 0x80, 0xde, 0xdf)?;
                    for (key, value) in entries { self.value(key, true, depth + 1)?; self.value(value, false, depth + 1)?; }
                } else {
                    self.array_len(2)?;
                    self.string("~#cmap", false)?;
                    self.array_len(entries.len().checked_mul(2).ok_or_else(|| invalid("Composite map length overflow"))?)?;
                    for (key, value) in entries { self.value(key, false, depth + 1)?; self.value(value, false, depth + 1)?; }
                }
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

fn stringable(value: &TransitValue) -> bool {
    match value {
        TransitValue::Array(_) | TransitValue::Map(_) => false,
        TransitValue::Tagged { tag, representation } => tag.chars().count() == 1 && matches!(representation.as_ref(), TransitValue::String(_)),
        _ => true,
    }
}

/// Where a keyword key falls in a Clojure `PersistentHashMap`'s iteration: its `hasheq` read in 5-bit digits from the least
/// significant, the trie's branching order. The reference's anomalies hold their data in such a map (`Anomaly.dataFromMap`),
/// so that order is the order their Transit form writes.
fn hash_map_position(key: &str) -> [u32; 7] {
    let hash = clojure_keyword_hash(key) as u32;
    std::array::from_fn(|digit| (hash >> (5 * digit)) & 31)
}

/// Clojure's `Keyword.hasheq`: the symbol's `hashCombine` of Murmur3 over the name's UTF-16 units and the namespace's
/// `String.hashCode`, plus the golden-ratio constant.
fn clojure_keyword_hash(key: &str) -> i32 {
    let (namespace, name) = match key.split_once('/') { Some((namespace, name)) if !namespace.is_empty() && !name.is_empty() => (Some(namespace), name), _ => (None, key) };
    let namespace_hash = namespace.map_or(0i32, |namespace| namespace.encode_utf16().fold(0i32, |hash, unit| hash.wrapping_mul(31).wrapping_add(i32::from(unit))));
    let seed = murmur3_unencoded_chars(name);
    let symbol = seed ^ namespace_hash.wrapping_add(0x9e37_79b9_u32 as i32).wrapping_add(seed << 6).wrapping_add(seed >> 2);
    symbol.wrapping_add(0x9e37_79b9_u32 as i32)
}

fn murmur3_unencoded_chars(text: &str) -> i32 {
    let mix_k1 = |k: u32| k.wrapping_mul(0xcc9e_2d51).rotate_left(15).wrapping_mul(0x1b87_3593);
    let units: Vec<u16> = text.encode_utf16().collect();
    let mut h1 = 0u32;
    for pair in units.chunks_exact(2) {
        h1 ^= mix_k1(u32::from(pair[0]) | (u32::from(pair[1]) << 16));
        h1 = h1.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if let [.., last] = units.as_slice() { if units.len() % 2 == 1 { h1 ^= mix_k1(u32::from(*last)); } }
    h1 ^= (2 * units.len()) as u32;
    h1 ^= h1 >> 16; h1 = h1.wrapping_mul(0x85eb_ca6b); h1 ^= h1 >> 13; h1 = h1.wrapping_mul(0xc2b2_ae35); h1 ^= h1 >> 16;
    h1 as i32
}

pub fn encode_anomaly(error: &Anomaly) -> Result<Vec<u8>> {
    let category = match error.category {
        AnomalyCategory::Incorrect => "incorrect", AnomalyCategory::Unsupported => "unsupported",
        AnomalyCategory::NotFound => "not-found", AnomalyCategory::Forbidden => "forbidden",
        AnomalyCategory::Conflict => "conflict", AnomalyCategory::Busy => "busy",
        AnomalyCategory::Unavailable => "unavailable", AnomalyCategory::Interrupted => "interrupted",
        AnomalyCategory::Fault => "fault",
    };
    let mut keys: Vec<&String> = error.context.keys().collect();
    keys.sort_by_key(|key| hash_map_position(key));
    let context = keys.into_iter().map(|key| Ok((TransitValue::Keyword(key.clone()), from_json_value(&error.context[key])?))).collect::<Result<_>>()?;
    encode_msgpack(&TransitValue::Tagged { tag: "xt/error".into(), representation: Box::new(TransitValue::Array(vec![
        TransitValue::Keyword(category.into()), if error.code.is_empty() { TransitValue::Null } else { TransitValue::Keyword(error.code.clone()) },
        error.message.as_ref().map_or(TransitValue::Null, |message| TransitValue::String(message.clone())), TransitValue::Map(context),
    ])) })
}

pub fn decode_anomaly(bytes: &[u8]) -> Result<Anomaly> {
    let TransitValue::Tagged { tag, representation } = decode_msgpack(bytes)? else { return Err(invalid("Error is not a tagged Transit value")); };
    let TransitValue::Array(values) = representation.as_ref() else { return Err(invalid("Error representation is not an array")); };
    if tag == "xtdb/exception-info" {
        let [message, context] = values.as_slice() else { return Err(invalid("Legacy exception-info representation must have two fields")); };
        let message = match message { TransitValue::Null => None, TransitValue::String(value) => Some(value.clone()), _ => return Err(invalid("Error message is not text")) };
        if !matches!(context, TransitValue::Map(_) | TransitValue::Null) { return Err(invalid("Legacy exception-info data is not a map")); }
        return Ok(Anomaly::new_nullable(AnomalyCategory::Fault, "xtdb.error/unknown", message));
    }
    let (category, code, message, context) = match (tag.as_str(), values.as_slice()) {
        ("xt/error", [TransitValue::Keyword(category), code, message, context]) => (anomaly_category(category)?, code, message, context),
        ("xtdb/illegal-arg" | "xtdb/runtime-err", [code, message, context, ..]) => (AnomalyCategory::Incorrect, code, message, context),
        _ => return Err(invalid(format!("Unsupported error representation {tag}"))),
    };
    let code = match code { TransitValue::Null => String::new(), TransitValue::Keyword(value) | TransitValue::String(value) => value.clone(), _ => return Err(invalid("Error code is not a keyword")) };
    let message = match message { TransitValue::Null => None, TransitValue::String(value) => Some(value.clone()), _ => return Err(invalid("Error message is not text")) };
    let context = match context {
        TransitValue::Null => BTreeMap::new(),
        TransitValue::Map(entries) => entries.iter().map(|(key, value)| {
            let key = match key { TransitValue::Keyword(key) | TransitValue::String(key) => key.clone(), _ => return Err(invalid("Error context key is not text")) };
            Ok((key, to_json_value(value)?))
        }).collect::<Result<_>>()?,
        _ => return Err(invalid("Error context is not a map")),
    };
    Ok(Anomaly { category, code, message, context })
}

fn anomaly_category(category: &str) -> Result<AnomalyCategory> {
    Ok(match category {
        "incorrect" => AnomalyCategory::Incorrect, "unsupported" => AnomalyCategory::Unsupported, "not-found" => AnomalyCategory::NotFound,
        "forbidden" => AnomalyCategory::Forbidden, "conflict" => AnomalyCategory::Conflict, "busy" => AnomalyCategory::Busy,
        "unavailable" => AnomalyCategory::Unavailable, "interrupted" => AnomalyCategory::Interrupted, "fault" => AnomalyCategory::Fault,
        _ => return Err(invalid("Unknown anomaly category")),
    })
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::incorrect("rtdb.format/invalid-transit", error.to_string())
}

#[cfg(test)]
mod compact_array_tests {
    use super::*;

    #[test]
    fn both_syntax_readers_group_plain_and_tagged_float_components() {
        let tagged = |value| TransitValue::Tagged { tag: "f32".into(), representation: Box::new(TransitValue::Float(value)) };
        let value = TransitValue::Array(vec![TransitValue::Float(-0.0), TransitValue::Float(1.0), tagged(2.0), tagged(3.0), tagged(4.0)]);
        let json = packed_json(br#"[-0.0,1.0,["~#f32",2.0],["^0",3.0],["^0",4.0]]"#).unwrap();
        let msgpack = packed_msgpack(&encode_msgpack(&value).unwrap()).unwrap();
        for packed in [json, msgpack] {
            let Packed::Array(items) = packed else { panic!("expected array"); };
            let [Packed::Floats(floats), Packed::TaggedFloats(first), Packed::TaggedFloats(cached)] = items.as_slice() else {
                panic!("expected compact float runs, got {items:?}");
            };
            assert_eq!(floats[0].to_bits(), (-0.0f64).to_bits());
            assert_eq!(first.as_ref(), &("~#f32".into(), vec![2.0]));
            assert_eq!(cached.as_ref(), &("^0".into(), vec![3.0, 4.0]));
            assert_eq!(unpack_array(items, None, &mut ReadCache::new(), 0, &mut WireValues).unwrap(), value);
        }
    }
}
