use std::collections::BTreeMap;
use rtdb_types::{Anomaly, Result};
use serde_json::Value as Json;
use super::AvroDatumPlan;

#[derive(Clone, Copy)]
pub(super) struct ReaderMode { kind: Kind, logical_types: bool }
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind { Generic, Specific, Reflection }

impl ReaderMode {
    pub(super) fn parse(options: &BTreeMap<String, String>) -> Result<Self> {
        let flag = super::super::converter::option_bool;
        let specific = flag(options, "specific.avro.reader", false)?;
        let reflection = flag(options, "schema.reflection", false)?;
        let logical_types = flag(options, "avro.use.logical.type.converters", false)?;
        Ok(Self { kind: if reflection { Kind::Reflection } else if specific { Kind::Specific } else { Kind::Generic }, logical_types })
    }

    pub(super) fn validate(self, writer: &AvroDatumPlan, reader: Option<&AvroDatumPlan>) -> Result<()> {
        if self.kind == Kind::Generic { return Ok(()); }
        let raw = writer.metadata.root();
        let selected = reader.unwrap_or(writer).metadata.root();
        if bare_primitive(raw) {
            if self.logical_types && !anonymous_schema(selected, true, 0) { return Err(unsupported()); }
            return Ok(());
        }
        if self.kind == Kind::Specific && (reader.is_some() || container(raw))
            && anonymous_schema(raw, self.logical_types, 0) && anonymous_schema(selected, self.logical_types, 0) {
            return Ok(());
        }
        Err(unsupported())
    }
}

fn primitive(name: &str) -> bool { matches!(name, "null" | "boolean" | "int" | "long" | "float" | "double" | "string" | "bytes") }

fn bare_primitive(raw: &Json) -> bool {
    if let Some(name) = raw.as_str() { return primitive(name); }
    let Some(object) = raw.as_object() else { return false; };
    // Avro1.11.4 Schema.parse omits SCHEMA_RESERVED keys from the properties compared by Schema.equals.
    object.get("type").and_then(Json::as_str).is_some_and(primitive) && object.keys().all(|name| matches!(name.as_str(),
        "doc" | "fields" | "items" | "name" | "namespace" | "size" | "symbols" | "values" | "type" | "aliases"))
}

fn container(raw: &Json) -> bool {
    raw.is_array() || raw.get("type").is_some_and(|value| value.is_array() || matches!(value.as_str(), Some("array" | "map")))
}

fn anonymous_schema(raw: &Json, reject_logical: bool, depth: usize) -> bool {
    if depth > 128 { return false; }
    if let Some(name) = raw.as_str() { return primitive(name); }
    if let Some(branches) = raw.as_array() { return branches.iter().all(|branch| anonymous_schema(branch, reject_logical, depth + 1)); }
    let Some(object) = raw.as_object() else { return false; };
    if ["java-class", "java-key-class", "java-element-class"].iter().any(|key| object.contains_key(*key))
        || (reject_logical && object.contains_key("logicalType")) { return false; }
    match object.get("type") {
        Some(Json::String(name)) if primitive(name) => true,
        Some(Json::String(name)) if name == "array" => object.get("items").is_some_and(|item| anonymous_schema(item, reject_logical, depth + 1)),
        Some(Json::String(name)) if name == "map" => object.get("values").is_some_and(|value| anonymous_schema(value, reject_logical, depth + 1)),
        Some(Json::Array(branches)) => branches.iter().all(|branch| anonymous_schema(branch, reject_logical, depth + 1)),
        _ => false,
    }
}

fn unsupported() -> Anomaly {
    Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-avro-reader", "The selected Avro specific/reflection reader schema requires a native class or logical-type adapter")
}

#[cfg(test)]
#[path = "reader_tests.rs"]
mod tests;
