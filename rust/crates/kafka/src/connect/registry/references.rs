use std::collections::{BTreeMap, BTreeSet};
use serde_json::Value as Json;
use super::{ConversionContext, RegistryConverter, RegistrySchema, Result, error};

#[derive(Debug, Default, PartialEq)]
pub(super) struct ResolvedReferences { by_name: BTreeMap<String, String>, order: Vec<String> }

pub(super) struct ResolvedSchema { pub(super) root: RegistrySchema, pub(super) references: ResolvedReferences }

struct ReferenceSchema {
    body: RegistrySchema,
    subject: Option<String>,
    version: Option<i32>,
}

#[derive(serde::Deserialize)]
struct ReferenceResponse {
    schema: String,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    #[serde(default, deserialize_with = "deserialize_reference_list")]
    references: Vec<Option<super::Reference>>,
    #[serde(rename = "ruleSet")]
    rule_set: Option<Json>,
    #[serde(default)]
    metadata: Option<Json>,
    #[serde(default)]
    subject: Option<String>,
    #[serde(default)]
    version: Option<i32>,
}

struct Frame { schema: ReferenceSchema, next: usize, name: Option<String> }

impl ResolvedReferences {
    pub(super) fn by_name(&self) -> &BTreeMap<String, String> { &self.by_name }
    pub(super) fn texts(&self) -> impl Iterator<Item = &str> {
        self.order.iter().map(|name| self.by_name.get(name).expect("completed reference name").as_str())
    }
    pub(super) fn retained_bound(&self) -> Result<usize> {
        let bytes = self.order.capacity().checked_mul(std::mem::size_of::<String>()).ok_or_else(overflow)?;
        let bytes = self.order.iter().try_fold(bytes, |bytes, name| add(bytes, name.capacity()))?;
        self.by_name.iter().try_fold(bytes, |bytes, (name, text)| add(add(add(bytes, 512)?, name.capacity())?, text.capacity()))
    }
    fn insert(&mut self, name: String, text: String) {
        assert!(!self.by_name.contains_key(&name), "reference inserted after first visit");
        self.order.push(name.clone());
        self.by_name.insert(name, text);
    }
}

impl RegistryConverter {
    pub(super) async fn resolve_references(&mut self, root: RegistrySchema, context: &mut ConversionContext) -> Result<ResolvedSchema> {
        context.reserve(std::mem::size_of::<Frame>().checked_mul(3).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let mut stack = vec![Frame { schema: ReferenceSchema { body: root, subject: None, version: None }, next: 0, name: None }];
        let mut visited = BTreeSet::new();
        let mut references = ResolvedReferences::default();
        loop {
            if self.format == super::RegistryFormat::Protobuf && stack.len() > super::protobuf::MAX_DEPTH {
                return Err(error("schema-registry", "Protobuf reference nesting limit exceeded"));
            }
            let frame = stack.last_mut().expect("root reference frame");
            if frame.next == frame.schema.body.references.len() {
                let frame = stack.pop().expect("completed reference frame");
                if let Some(name) = frame.name { references.insert(name, frame.schema.body.schema); }
                else { return Ok(ResolvedSchema { root: frame.schema.body, references }); }
                continue;
            }
            let reference = frame.schema.body.references[frame.next].as_ref().ok_or_else(|| error("schema-registry", "Schema reference must not be null"))?;
            let (Some(name), Some(subject), Some(version)) = (&reference.name, &reference.subject, reference.version) else {
                return Err(error("schema-registry", "Schema reference requires a name, subject and version"));
            };
            context.reserve(step_bound(name, subject, frame.schema.subject.as_deref())?)?;
            let name = name.clone();
            let subject = qualify_subject(frame.schema.subject.as_deref(), subject);
            let index = frame.next;
            frame.next += 1;
            let value = self.request_query(&["subjects".into(), subject, "versions".into(), version.to_string()], &[("deleted".into(), "true".into())], None, context).await?;
            admit_typed_references(&value, context)?;
            let response: ReferenceResponse = serde_json::from_value(value).map_err(|cause| error("schema-registry", cause))?;
            let fetched = ReferenceSchema {
                body: RegistrySchema { schema: response.schema, schema_type: response.schema_type, references: response.references, rule_set: response.rule_set, metadata: response.metadata },
                subject: response.subject, version: response.version,
            };
            if self.format == super::RegistryFormat::Protobuf {
                self.validate_format(&fetched.body)?;
            }
            if version == -1 {
                stack.last_mut().expect("requesting reference frame").schema.body.references[index].as_mut().expect("requesting reference was validated").version = fetched.version;
            }
            if !visited.insert(name.clone()) { continue; }
            stack.push(Frame { schema: fetched, next: 0, name: Some(name) });
        }
    }
}

fn step_bound(name: &str, subject: &str, parent: Option<&str>) -> Result<usize> {
    let text = add(add(name.len(), subject.len())?, parent.map_or(0, str::len))?;
    let slots = add(std::mem::size_of::<Frame>(), std::mem::size_of::<String>())?.checked_mul(3).ok_or_else(overflow)?;
    add(add(text.checked_mul(8).ok_or_else(overflow)?, slots)?, 2048)
}

fn qualified_parts(text: &str) -> (&str, &str) {
    if let Some(rest) = text.strip_prefix(":.") {
        match rest.find(':') {
            Some(index) => (&text[1..index + 2], &text[index + 3..]),
            None => (&text[1..], ""),
        }
    } else if let Some(rest) = text.strip_prefix(":*:") { ("*", rest) }
    else { (".", text) }
}

fn qualify_subject(parent: Option<&str>, child: &str) -> String {
    let (context, subject) = qualified_parts(child);
    if context != "." { return format!(":{context}:{subject}"); }
    let (parent_context, _) = qualified_parts(parent.unwrap_or_default());
    if parent_context != "." { format!(":{parent_context}:{child}") } else { subject.into() }
}

pub(super) fn deserialize_reference_list<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Vec<Option<super::Reference>>, D::Error> {
    <Option<Vec<Option<super::Reference>>> as serde::Deserialize<'de>>::deserialize(deserializer).map(Option::unwrap_or_default)
}

pub(super) fn empty_text(value: &Option<String>) -> bool { value.as_ref().is_none_or(String::is_empty) }

pub(super) fn admit_typed_references(value: &Json, context: &mut ConversionContext) -> Result<()> {
    if let Some(values) = value.get("references").and_then(Json::as_array) {
        context.reserve(values.len().checked_mul(std::mem::size_of::<Option<super::Reference>>()).and_then(|bytes| bytes.checked_mul(3)).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
    }
    Ok(())
}

pub(super) fn root_retained_bound(root: &RegistrySchema) -> Result<usize> {
    let mut bytes = add(root.schema.capacity(), root.schema_type.as_ref().map_or(0, String::capacity))?;
    bytes = add(bytes, root.references.capacity().checked_mul(std::mem::size_of::<Option<super::Reference>>()).ok_or_else(overflow)?)?;
    for reference in root.references.iter().flatten() {
        bytes = add(add(bytes, reference.name.as_ref().map_or(0, String::capacity))?, reference.subject.as_ref().map_or(0, String::capacity))?;
    }
    for value in [&root.metadata, &root.rule_set].into_iter().flatten() { bytes = add(bytes, json_retained_bound(value)?)?; }
    Ok(bytes)
}

fn json_retained_bound(value: &Json) -> Result<usize> {
    match value {
        Json::Null | Json::Bool(_) => Ok(0),
        Json::Number(number) => add(serialized_size(number)?.checked_mul(2).ok_or_else(overflow)?, 64),
        Json::String(text) => Ok(text.capacity()),
        Json::Array(values) => values.iter().try_fold(values.capacity().checked_mul(std::mem::size_of::<Json>()).ok_or_else(overflow)?, |bytes, value| add(bytes, json_retained_bound(value)?)),
        Json::Object(values) => values.iter().try_fold(1024, |bytes, (key, value)| add(add(add(bytes, 512)?, key.capacity())?, json_retained_bound(value)?)),
    }
}

struct SerializedSize(usize);
impl std::io::Write for SerializedSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| std::io::Error::other("Schema serialized size exceeds range"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
pub(super) fn serialized_size(value: &impl serde::Serialize) -> Result<usize> {
    let mut size = SerializedSize(0);
    serde_json::to_writer(&mut size, value).map_err(|cause| error("allocation-overflow", cause))?;
    Ok(size.0)
}

fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow", "Schema reference allocation exceeds range") }

#[cfg(test)]
mod tests;
