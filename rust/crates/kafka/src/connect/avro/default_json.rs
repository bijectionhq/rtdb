use std::collections::BTreeMap;
use serde_json::value::RawValue;
use rtdb_types::Result;
use super::error;

pub(super) use super::super::json_value::JsonValue as DefaultJson;
use super::super::json_value::RawObject;

fn default_metadata(raw: &RawValue, depth: usize) -> Result<serde_json::Value> {
    use serde_json::Value;
    if depth > 128 { return Err(error("avro-schema", "Default nesting limit exceeded")); }
    Ok(match raw.get().as_bytes().first() {
        Some(b'{') => {
            let RawObject(fields) = json(raw.get())?;
            Value::Object(fields.into_iter().map(|(key, value)| {
                Ok((key, default_metadata(value, depth + 1)?))
            }).collect::<Result<_>>()?)
        }
        Some(b'[') => Value::Array(json::<Vec<&RawValue>>(raw.get())?.into_iter()
            .map(|value| default_metadata(value, depth + 1)).collect::<Result<_>>()?),
        Some(b'-' | b'0'..=b'9') => serde_json::from_str(raw.get()).unwrap_or(Value::Null),
        _ => json(raw.get())?,
    })
}
pub(super) fn field_default_class(value: &DefaultJson, schema: &apache_avro::Schema, names: &BTreeMap<String, apache_avro::Schema>) -> Result<Option<bool>> {
    use apache_avro::Schema;
    let schema = super::resolve(schema, names)?;
    let schema = if let Schema::Decimal(decimal) = schema { decimal.inner.as_ref() } else { schema };
    if let Schema::Union(union) = schema { return field_default_class(value, union.variants().first().ok_or_else(|| error("avro-schema", "Empty default union"))?, names); }
    Ok(match value {
        DefaultJson::Number(_) => {
            if !value.is_integral() { if matches!(schema, Schema::Float | Schema::Double) { Some(true) } else { None } }
            else if let Some(value) = value.as_i64() {
                match schema {
                    Schema::Int | Schema::Date | Schema::TimeMillis => Some(i32::try_from(value).is_ok()),
                    Schema::Long | Schema::TimestampMillis | Schema::Float | Schema::Double => Some(true),
                    _ => None,
                }
            } else { None }
        }
        DefaultJson::Decimal(_) => None,
        DefaultJson::Boolean(_) => Some(matches!(schema, Schema::Boolean)),
        DefaultJson::String(_) => if matches!(schema, Schema::String | Schema::Enum(_) | Schema::Bytes | Schema::Fixed(_)) { Some(true) } else { None },
        DefaultJson::Array(_) => if matches!(schema, Schema::Array(_)) { Some(true) } else { None },
        DefaultJson::Object(fields) => match schema { Schema::Record(record) if fields.iter().any(|(key, _)| !record.lookup.get(key).and_then(|index| record.fields.get(*index)).is_some_and(|field| field.name == *key) && !record.fields.iter().any(|field| field.name == *key)) => None, Schema::Map(_) | Schema::Record(_) => Some(true), _ => None },
        DefaultJson::Null => Some(true),
    })
}

#[derive(PartialEq)]
pub(super) struct Defaults { records: BTreeMap<String, BTreeMap<String, DefaultJson>> }
impl Defaults {
    pub fn parse(root: &str, references: &[String]) -> Result<Self> {
        let mut defaults = Self { records: BTreeMap::new() };
        defaults.collect(json(root)?, "", 0)?;
        for reference in references { defaults.collect(json(reference)?, "", 0)?; }
        Ok(defaults)
    }
    pub fn get(&self, record: &str, field: &str) -> Option<&DefaultJson> { self.records.get(record)?.get(field) }
    fn collect(&mut self, raw: &RawValue, namespace: &str, depth: usize) -> Result<()> {
        if depth > 128 { return Err(error("avro-schema", "Schema nesting limit exceeded")); }
        match raw.get().as_bytes().first() {
            Some(b'[') => for schema in json::<Vec<&RawValue>>(raw.get())? { self.collect(schema, namespace, depth + 1)?; },
            Some(b'{') => {
                let object = json::<BTreeMap<String, &RawValue>>(raw.get())?;
                let Some(kind) = object.get("type") else { return Ok(()); };
                if kind.get().starts_with('[') || kind.get().starts_with('{') { return self.collect(kind, namespace, depth + 1); }
                let kind: String = json(kind.get())?;
                match kind.as_str() {
                    "record" => {
                        let name: String = json(object.get("name").ok_or_else(|| error("avro-schema", "Record name is absent"))?.get())?;
                        let namespace: String = match object.get("namespace") { Some(value) => json(value.get())?, None => namespace.into() };
                        let name = if name.contains('.') || namespace.is_empty() { name } else { format!("{namespace}.{name}") };
                        let namespace = name.rsplit_once('.').map_or("", |(namespace, _)| namespace);
                        if let Some(fields) = object.get("fields") {
                            for field in json::<Vec<BTreeMap<String, &RawValue>>>(fields.get())? {
                                let field_name: String = json(field.get("name").ok_or_else(|| error("avro-schema", "Field name is absent"))?.get())?;
                                if let Some(value) = field.get("default") { self.records.entry(name.clone()).or_default().insert(field_name, DefaultJson::parse(value, depth + 1)?); }
                                if let Some(kind) = field.get("type") { self.collect(kind, namespace, depth + 1)?; }
                            }
                        }
                    }
                    "array" => if let Some(items) = object.get("items") { self.collect(items, namespace, depth + 1)?; },
                    "map" => if let Some(values) = object.get("values") { self.collect(values, namespace, depth + 1)?; },
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(())
    }
}
pub(super) fn metadata_text(text: &str) -> Result<String> {
    fn value(raw: &RawValue, depth: usize) -> Result<serde_json::Value> {
        if depth > 128 { return Err(error("avro-schema", "Schema nesting limit exceeded")); }
        match raw.get().as_bytes().first() {
            Some(b'{') => {
                let fields: RawObject<'_> = json(raw.get())?;
                Ok(serde_json::Value::Object(fields.0.into_iter().map(|(key, raw)| {
                    let value = if key == "default" { default_metadata(raw, depth + 1)? } else { value(raw, depth + 1)? };
                    Ok((key, value))
                }).collect::<Result<_>>()?))
            }
            Some(b'[') => Ok(serde_json::Value::Array(json::<Vec<&RawValue>>(raw.get())?.into_iter().map(|raw| value(raw, depth + 1)).collect::<Result<_>>()?)),
            _ => json(raw.get()),
        }
    }
    serde_json::to_string(&value(json(text)?, 0)?).map_err(|cause| error("avro-schema", cause))
}
fn json<'a, T: serde::Deserialize<'a>>(text: &'a str) -> Result<T> { serde_json::from_str(text).map_err(|cause| error("avro-schema", cause)) }


#[cfg(test)] mod tests;
