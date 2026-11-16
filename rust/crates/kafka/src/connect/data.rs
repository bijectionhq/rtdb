use std::sync::Arc;
use std::collections::BTreeMap;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, Result, Value};

/// Graph-backed children use `Reference`; inspect their concrete kind through `ConnectSchema::resolved`.
#[derive(Clone, Debug, PartialEq)]
pub enum SchemaKind { Bool, I8, I16, I32, I64, F32, F64, String, Bytes, Array(Arc<ConnectSchema>), ArrayOfNull, Map(Arc<ConnectSchema>, Arc<ConnectSchema>), MapWithNullValues(Arc<ConnectSchema>), MapOfNull, Struct(Vec<ConnectField>), Reference(super::SchemaReference) }
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectField { pub name: String, pub schema: Arc<ConnectSchema> }
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectSchema {
    pub kind: SchemaKind,
    pub optional: bool,
    pub name: Option<String>,
    pub version: Option<i32>,
    pub doc: Option<String>,
    pub parameters: BTreeMap<String, String>,
    pub default: Option<Value>,
}
impl ConnectSchema {
    pub fn new(kind: SchemaKind) -> Self { Self { kind, optional: false, name: None, version: None, doc: None, parameters: BTreeMap::new(), default: None } }
    /// Expands one graph node; the returned view retains every referenced node's immutable graph.
    pub fn resolved(&self) -> std::borrow::Cow<'_, Self> {
        match &self.kind { SchemaKind::Reference(reference) => std::borrow::Cow::Owned(super::schema_graph::Metadata::from_schema(self).schema(reference.resolve().kind)), _ => std::borrow::Cow::Borrowed(self) }
    }
    pub fn field(&self, name: &str) -> Option<ConnectField> { match &self.resolved().kind { SchemaKind::Struct(fields) => fields.iter().find(|field| field.name == name).cloned(), _ => None } }
    pub(crate) fn view_allocation_bound(&self) -> usize {
        match &self.kind {
            SchemaKind::Reference(reference) => reference.view_allocation_bound(),
            SchemaKind::Struct(fields) => fields.iter().map(|field| field.schema.view_allocation_bound()).max().unwrap_or(0),
            SchemaKind::Array(element) | SchemaKind::MapWithNullValues(element) => element.view_allocation_bound(),
            SchemaKind::Map(key, value) => key.view_allocation_bound().max(value.view_allocation_bound()),
            _ => 0,
        }
    }
    pub fn retained_bytes(&self) -> Result<usize> { self.retained_bytes_inner(&mut None) }
    fn retained_bytes_inner(&self, first_graph: &mut Option<usize>) -> Result<usize> {
        let mut bytes = 4096usize.saturating_add(self.name.as_ref().map_or(0, String::capacity)).saturating_add(self.doc.as_ref().map_or(0, String::capacity));
        for (key, value) in &self.parameters { bytes = bytes.saturating_add(1024).saturating_add(key.capacity()).saturating_add(value.capacity()); }
        if let Some(default) = &self.default { bytes = bytes.saturating_add(default.retained_bytes()?); }
        match &self.kind {
            SchemaKind::Array(element) => bytes = bytes.saturating_add(element.retained_bytes_inner(first_graph)?),
            SchemaKind::Map(key, value) => bytes = bytes.saturating_add(key.retained_bytes_inner(first_graph)?).saturating_add(value.retained_bytes_inner(first_graph)?),
            SchemaKind::MapWithNullValues(key) => bytes = bytes.saturating_add(key.retained_bytes_inner(first_graph)?),
            SchemaKind::Struct(fields) => for field in fields { bytes = bytes.saturating_add(field.name.capacity()).saturating_add(field.schema.retained_bytes_inner(first_graph)?); },
            SchemaKind::Reference(reference) => {
                let owner = reference.owner_id();
                if *first_graph != Some(owner) { bytes = bytes.saturating_add(reference.retained_bytes()); }
                first_graph.get_or_insert(owner);
            }
            _ => (),
        }
        Ok(bytes)
    }
}
pub(crate) fn validate_value(schema: &ConnectSchema, value: &Value, depth: usize) -> Result<()> {
    if depth > 128 { return Err(super::error("conversion-error", "Connect value validation nesting limit exceeded")); }
    let schema = schema.resolved();
    let invalid = || super::error("conversion-error", "Value does not match its Connect schema");
    if *value == Value::Null { return if schema.optional { Ok(()) } else { Err(invalid()) }; }
    let logical = match schema.name.as_deref() {
        Some("org.apache.kafka.connect.data.Decimal") => Some(matches!(value, Value::Decimal(_))),
        Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp") => Some(matches!(value, Value::DateDay(_) | Value::Time { .. } | Value::Timestamp { .. })),
        _ => None,
    };
    if let Some(valid) = logical { return if valid { Ok(()) } else { Err(invalid()) }; }
    match (&schema.kind, value) {
        (SchemaKind::Bool, Value::Bool(_)) | (SchemaKind::I8, Value::I8(_)) | (SchemaKind::I16, Value::I16(_)) | (SchemaKind::I32, Value::I32(_)) | (SchemaKind::I64, Value::I64(_)) | (SchemaKind::F32, Value::F32(_)) | (SchemaKind::F64, Value::F64(_)) | (SchemaKind::String, Value::Utf8(_)) | (SchemaKind::Bytes, Value::Binary(_)) => Ok(()),
        (SchemaKind::Array(element), Value::List(values)) => values.iter().try_for_each(|value| validate_value(element, value, depth + 1)),
        (SchemaKind::Map(key, element), Value::Map { entries, .. }) => entries.iter().try_for_each(|(k, v)| { validate_value(key, k, depth + 1)?; validate_value(element, v, depth + 1) }),
        (SchemaKind::Struct(fields), Value::Struct(values)) => fields.iter().try_for_each(|field| {
            let value = values.get(&field.name).unwrap_or(&Value::Null);
            if *value == Value::Null && (field.schema.optional || field.schema.default.is_some()) { return Ok(()); }
            validate_value(&field.schema, value, depth + 1)
        }),
        _ => Err(invalid()),
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectData { pub schema: Option<Arc<ConnectSchema>>, pub value: Value }
impl ConnectData {
    pub fn null() -> Self { Self { schema: None, value: Value::Null } }
    pub fn schemaless(value: Value) -> Self { Self { schema: None, value } }
}

pub struct SinkRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_millis: Option<i64>,
    pub key: ConnectData,
    pub value: ConnectData,
    pub headers: Vec<(String, ConnectData)>,
    pub(crate) reservations: Vec<Arc<MemoryReservation>>,
}
impl SinkRecord {
    pub fn new(topic: String, partition: i32, offset: i64, key: ConnectData, value: ConnectData) -> Self {
        Self { topic, partition, offset, timestamp_millis: None, key, value, headers: Vec::new(), reservations: Vec::new() }
    }
    pub fn context(&self, error: Anomaly) -> Anomaly {
        error.with_context("topic", self.topic.clone().into()).with_context("partition", self.partition.into()).with_context("offset", self.offset.into())
    }
    pub fn retained_bytes(&self) -> Result<usize> {
        let mut bytes = self.topic.capacity().saturating_add(1024).saturating_add(self.key.value.retained_bytes()?).saturating_add(self.value.value.retained_bytes()?);
        for data in [&self.key, &self.value] { if let Some(schema) = &data.schema { bytes = bytes.saturating_add(schema.retained_bytes()?); } }
        for (name, header) in &self.headers { bytes = bytes.saturating_add(name.capacity()).saturating_add(header.value.retained_bytes()?); if let Some(schema) = &header.schema { bytes = bytes.saturating_add(schema.retained_bytes()?); } }
        Ok(bytes)
    }
}
