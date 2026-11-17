use std::{collections::BTreeSet, mem::size_of};
use serde_json::Value as Json;
use rtdb_types::Result;
use super::{NameParts, error, simple_name};

struct CanonicalNames<'a, A> { definitions: BTreeSet<NameParts<'a>>, admit: A }

pub(in crate::connect::avro) fn validate_ordered<'a>(documents: impl IntoIterator<Item = &'a Json>, admit: impl FnMut(usize) -> Result<()>) -> Result<()> {
    let mut names = CanonicalNames { definitions: BTreeSet::new(), admit };
    for document in documents { names.schema(document, "", 0)?; }
    Ok(())
}
fn primitive(name: &str) -> bool { matches!(name, "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string") }
fn required<'a>(object: &'a Json, name: &str) -> Result<&'a Json> { object.get(name).ok_or_else(|| error("avro-schema", "Required schema member is absent")) }
fn text<'a>(object: &'a Json, name: &str) -> Result<&'a str> { required(object, name)?.as_str().ok_or_else(|| error("avro-schema", "Required schema member is not textual")) }
fn parts<'a>(name: &'a str, namespace: &'a str) -> Result<NameParts<'a>> { NameParts::parse(name, Some(namespace)).map_err(|cause| error("avro-schema", cause)) }
fn aliases(object: &Json, namespace: Option<&str>) -> Result<()> {
    if let Some(aliases) = object.get("aliases") {
        for alias in aliases.as_array().ok_or_else(|| error("avro-schema", "Aliases are not an array"))? {
            let alias = alias.as_str().ok_or_else(|| error("avro-schema", "Alias is not textual"))?;
            if let Some(namespace) = namespace { parts(alias, namespace)?; }
        }
    }
    Ok(())
}
impl<'a, A: FnMut(usize) -> Result<()>> CanonicalNames<'a, A> {
    fn reference(&self, name: &'a str, namespace: &'a str, fallback: bool) -> Result<()> {
        if primitive(name) { return Ok(()); }
        let primary = parts(name, namespace)?;
        if self.definitions.contains(&primary) || (fallback && self.definitions.contains(&parts(name, "")?)) { Ok(()) }
        else { Err(error("avro-schema", "Undefined canonical schema name")) }
    }
    fn register(&mut self, name: NameParts<'a>) -> Result<()> {
        if self.definitions.contains(&name) { return Err(error("avro-schema", "Canonical schema name is redefined")); }
        let bytes = size_of::<NameParts<'a>>().checked_mul(16).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| error("allocation-overflow", "Avro canonical-name allocation overflow"))?;
        (self.admit)(bytes)?;
        self.definitions.insert(name);
        Ok(())
    }
    fn schema(&mut self, raw: &'a Json, namespace: &'a str, depth: usize) -> Result<()> {
        if depth > 128 { return Err(error("avro-schema", "Schema nesting limit exceeded")); }
        match raw {
            Json::String(name) => self.reference(name, namespace, true),
            Json::Array(members) => { for member in members { self.schema(member, namespace, depth + 1)?; } Ok(()) }
            Json::Object(_) => {
                let kind = text(raw, "type")?;
                if primitive(kind) { return Ok(()); }
                match kind {
                    "record" | "error" | "enum" | "fixed" => {
                        let name = parts(text(raw, "name")?, raw.get("namespace").and_then(Json::as_str).unwrap_or(namespace))?;
                        if name.namespace.is_empty() && primitive(name.simple) { return Err(error("avro-schema", "Schema is named after a primitive")); }
                        match kind {
                            "record" | "error" => {
                                self.register(name)?;
                                let fields = required(raw, "fields")?.as_array().ok_or_else(|| error("avro-schema", "Record fields are not an array"))?;
                                for field in fields {
                                    let field_name = text(field, "name")?;
                                    self.schema(required(field, "type")?, name.namespace, depth + 1)?;
                                    if !simple_name(field_name) { return Err(error("avro-schema", "Record field name is invalid")); }
                                    aliases(field, None)?;
                                }
                                for (index, field) in fields.iter().enumerate() {
                                    if fields[..index].iter().any(|previous| previous.get("name") == field.get("name")) { return Err(error("avro-schema", "Record field name is duplicated")); }
                                }
                            }
                            "enum" => {
                                let symbols = required(raw, "symbols")?.as_array().ok_or_else(|| error("avro-schema", "Enum symbols are not an array"))?;
                                for (index, symbol) in symbols.iter().enumerate() {
                                    let symbol = symbol.as_str().filter(|symbol| simple_name(symbol)).ok_or_else(|| error("avro-schema", "Enum symbol is invalid"))?;
                                    if symbols[..index].iter().any(|previous| previous.as_str() == Some(symbol)) { return Err(error("avro-schema", "Enum symbol is duplicated")); }
                                }
                                if let Some(default) = raw.get("default").and_then(Json::as_str) {
                                    if !symbols.iter().any(|symbol| symbol.as_str() == Some(default)) { return Err(error("avro-schema", "Enum default is not a symbol")); }
                                }
                                self.register(name)?;
                            }
                            "fixed" => {
                                required(raw, "size")?.as_i64().filter(|size| i32::try_from(*size).is_ok() && *size >= 0).ok_or_else(|| error("avro-schema", "Fixed size is not a nonnegative signed 32-bit integer"))?;
                                self.register(name)?;
                            }
                            _ => unreachable!(),
                        }
                        aliases(raw, Some(name.namespace))
                    }
                    "array" => self.schema(required(raw, "items")?, namespace, depth + 1),
                    "map" => self.schema(required(raw, "values")?, namespace, depth + 1),
                    name => self.reference(name, namespace, false),
                }
            }
            _ => Err(error("avro-schema", "Schema is neither textual, an object nor an array")),
        }
    }
}

#[cfg(test)] mod tests;
