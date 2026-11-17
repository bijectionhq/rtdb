use std::{fmt, mem::size_of};
use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use super::{ConversionContext, Json, Result, error};

const RAW_VALUE: &str = "$serde_json::private::RawValue";

struct Measure<'a> { context: &'a mut ConversionContext, bytes: usize, failure: Option<rtdb_types::Anomaly> }
struct Node<'a, 'b>(&'a mut Measure<'b>);
struct Key<'a, 'b>(&'a mut Measure<'b>, bool);
struct Raw<'a, 'b>(&'a mut Measure<'b>);

fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow", "Registry JSON allocation exceeds range") }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn scratch(bytes: usize) -> Result<usize> { add(mul(bytes, 4)?, 4096) }
fn node_bytes() -> Result<usize> { add(mul(size_of::<Json>(), 16)?, 512) }
fn key_bytes() -> Result<usize> { add(mul(add(size_of::<(String, Json)>(), mul(size_of::<usize>(), 2)?)?, 16)?, 512) }
fn text_bytes(bytes: usize) -> Result<usize> { add(mul(bytes, 2)?, 256) }

/// The returned tree must remain in the region whose caller retains `context`'s reservations.
pub(super) fn parse(bytes: &[u8], context: &mut ConversionContext) -> Result<Json> {
    let mut measure = Measure { context, bytes: 0, failure: None };
    if let Err(cause) = measure.scan(bytes) {
        return Err(measure.failure.take().unwrap_or_else(|| error("schema-registry", cause)));
    }
    measure.context.reserve(measure.bytes)?;
    serde_json::from_slice(bytes).map_err(|cause| error("schema-registry", cause))
}

impl Measure<'_> {
    fn reject<E: serde::de::Error>(&mut self, cause: rtdb_types::Anomaly) -> E {
        if self.failure.is_none() { self.failure = Some(cause); }
        E::custom("Registry JSON allocation was not admitted")
    }
    fn admit<E: serde::de::Error>(&mut self, bytes: Result<usize>) -> std::result::Result<(), E> {
        let bytes = bytes.and_then(|bytes| add(self.bytes, bytes));
        let bytes = match bytes { Ok(bytes) => bytes, Err(cause) => return Err(self.reject(cause)) };
        if bytes > self.context.allocation_remaining() {
            return Err(self.reject(error("batch-too-large", "Registry JSON tree exceeds the remaining converter allocation")));
        }
        self.bytes = bytes; Ok(())
    }
    fn scan(&mut self, bytes: &[u8]) -> std::result::Result<(), serde_json::Error> {
        let charge = scratch(bytes.len()).and_then(|bytes| self.context.reserve(bytes));
        if let Err(cause) = charge { return Err(self.reject(cause)); }
        if self.bytes > self.context.allocation_remaining() {
            return Err(self.reject(error("batch-too-large", "Registry JSON scratch leaves insufficient tree allocation")));
        }
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        Node(self).deserialize(&mut deserializer)?;
        deserializer.end()
    }
}

impl<'de> DeserializeSeed<'de> for Node<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> std::result::Result<(), D::Error> {
        self.0.admit(node_bytes())?;
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Node<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("any valid JSON value") }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> std::result::Result<(), E> { Ok(()) }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> std::result::Result<(), E> { Ok(()) }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> std::result::Result<(), E> { Ok(()) }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> std::result::Result<(), E> { Ok(()) }
    fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<(), E> { Ok(()) }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<(), E> { self.0.admit(text_bytes(value.len())) }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> std::result::Result<(), A::Error> {
        while sequence.next_element_seed(Node(&mut *self.0))?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<(), A::Error> {
        match map.next_key_seed(Key(&mut *self.0, true))? {
            Some(true) => map.next_value_seed(Raw(self.0)),
            Some(false) => {
                map.next_value_seed(Node(&mut *self.0))?;
                while map.next_key_seed(Key(&mut *self.0, false))?.is_some() { map.next_value_seed(Node(&mut *self.0))?; }
                Ok(())
            }
            None => Ok(()),
        }
    }
}

impl<'de> DeserializeSeed<'de> for Key<'_, '_> {
    type Value = bool;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> std::result::Result<bool, D::Error> { deserializer.deserialize_str(self) }
}
impl<'de> Visitor<'de> for Key<'_, '_> {
    type Value = bool;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("a string key") }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<bool, E> {
        self.0.admit(key_bytes().and_then(|bytes| add(bytes, text_bytes(value.len())?)))?;
        Ok(self.1 && value == RAW_VALUE)
    }
}
impl<'de> DeserializeSeed<'de> for Raw<'_, '_> {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> std::result::Result<(), D::Error> { deserializer.deserialize_str(self) }
}
impl<'de> Visitor<'de> for Raw<'_, '_> {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str("raw value") }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> std::result::Result<(), E> {
        self.0.admit(text_bytes(value.len()))?;
        self.0.scan(value.as_bytes()).map_err(E::custom)
    }
}

#[cfg(test)] mod tests;
