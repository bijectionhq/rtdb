use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::{Result, Value};
use super::{ConnectData, ConnectField, ConnectSchema, SchemaKind, SinkRecord, Transform, error, string_map};

pub(super) struct Flatten { delimiter: String, key: bool }
impl Flatten { pub fn new(delimiter: String, key: bool) -> Self { Self { delimiter, key } } }
impl Transform for Flatten {
    fn allocation_bound(&self, record: &SinkRecord) -> Result<usize> {
        let data = if self.key { &record.key } else { &record.value };
        let extra = match &data.schema { Some(schema) => schema_names(schema, 0, self.delimiter.len(), 0)?, None => value_names(&data.value, 0, self.delimiter.len(), 0)? };
        let view = data.schema.as_ref().map_or(0, |schema| schema.view_allocation_bound());
        record.retained_bytes()?.checked_mul(4).and_then(|bytes| bytes.checked_add(extra.checked_add(129)?.checked_mul(view.saturating_add(4))?)).ok_or_else(|| error("allocation-overflow", "Flatten allocation overflow"))
    }
    fn apply(&mut self, mut record: SinkRecord) -> Result<Option<SinkRecord>> {
        let data = if self.key { &mut record.key } else { &mut record.value };
        super::resolve_record_schema(data);
        if data.value == Value::Null { return Ok(Some(record)); }
        let mut output = BTreeMap::new();
        if let Some(schema) = &data.schema {
            let mut fields = Vec::new();
            flatten_schema(schema, "", &self.delimiter, schema.optional, schema.default.as_ref(), &mut fields, 0)?;
            flatten_struct(schema, &data.value, "", &self.delimiter, &mut output, 0)?;
            for field in &fields { output.entry(field.name.clone()).or_insert(Value::Null); }
            let mut schema = schema.as_ref().clone(); schema.kind = SchemaKind::Struct(fields); schema.default = None; schema.optional = false;
            *data = ConnectData { schema: Some(Arc::new(schema)), value: Value::Struct(output) };
        } else {
            flatten_map(&data.value, "", &self.delimiter, &mut output, 0)?;
            data.value = string_map(output);
        }
        Ok(Some(record))
    }
}
fn name(prefix: &str, field: &str, delimiter: &str) -> String { if prefix.is_empty() { field.into() } else { format!("{prefix}{delimiter}{field}") } }
fn check_depth(depth: usize) -> Result<()> { if depth > 128 { Err(error("transform-error", "Flatten nesting limit exceeded")) } else { Ok(()) } }
fn flatten_map(value: &Value, prefix: &str, delimiter: &str, output: &mut BTreeMap<String, Value>, depth: usize) -> Result<()> {
    check_depth(depth)?;
    let Value::Map { entries, .. } = value else { return Err(error("transform-error", "Schemaless Flatten requires a Map")); };
    for (key, value) in entries {
        let Value::Utf8(key) = key else { return Err(error("transform-error", "Flatten requires string map keys")); };
        let name = name(prefix, key, delimiter);
        match value {
            Value::Map { .. } => flatten_map(value, &name, delimiter, output, depth + 1)?,
            Value::Null | Value::Bool(_) | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::F32(_) | Value::F64(_) | Value::Utf8(_) | Value::Binary(_) | Value::List(_) => { output.insert(name, value.clone()); }
            _ => return Err(error("transform-error", "Unsupported schemaless Flatten input type")),
        }
    }
    Ok(())
}
fn flatten_schema(schema: &ConnectSchema, prefix: &str, delimiter: &str, optional: bool, default: Option<&Value>, fields: &mut Vec<ConnectField>, depth: usize) -> Result<()> {
    check_depth(depth)?;
    let schema = schema.resolved();
    let SchemaKind::Struct(children) = &schema.kind else { return Err(error("transform-error", "Schema-bearing Flatten requires Structs")); };
    let default = match default { Some(Value::Struct(values)) => Some(values), Some(Value::Null) | None => None, _ => return Err(error("transform-error", "Flatten Struct default is not a Struct")) };
    for field in children {
        let field_schema = field.schema.resolved();
        let name = name(prefix, &field.name, delimiter);
        let optional = optional || field.schema.optional;
        let default = field.schema.default.as_ref().filter(|value| **value != Value::Null).or_else(|| default.and_then(|values| values.get(&field.name)).filter(|value| **value != Value::Null));
        match &field_schema.kind {
            SchemaKind::Struct(_) => flatten_schema(&field.schema, &name, delimiter, optional, default, fields, depth + 1)?,
            SchemaKind::Map(_, _) | SchemaKind::MapOfNull | SchemaKind::MapWithNullValues(_) => return Err(error("transform-error", "Schema-bearing Flatten does not support Map fields")),
            _ => {
                if fields.iter().any(|field| field.name == name) { return Err(error("transform-error", "Flatten generates duplicate schema field names")); }
                let mut schema = field_schema.into_owned(); schema.optional = optional; schema.default = default.cloned();
                fields.push(ConnectField { name, schema: Arc::new(schema) });
            }
        }
    }
    Ok(())
}
fn flatten_struct(schema: &ConnectSchema, value: &Value, prefix: &str, delimiter: &str, output: &mut BTreeMap<String, Value>, depth: usize) -> Result<()> {
    check_depth(depth)?;
    if *value == Value::Null { return Ok(()); }
    let schema = schema.resolved();
    let (SchemaKind::Struct(fields), Value::Struct(values)) = (&schema.kind, value) else { return Err(error("transform-error", "Flatten requires a Struct value")); };
    for field in fields {
        let name = name(prefix, &field.name, delimiter);
        let value = values.get(&field.name).filter(|value| **value != Value::Null).or(field.schema.default.as_ref()).unwrap_or(&Value::Null);
        match &field.schema.resolved().kind {
            SchemaKind::Struct(_) => flatten_struct(&field.schema, value, &name, delimiter, output, depth + 1)?,
            SchemaKind::Map(_, _) | SchemaKind::MapOfNull | SchemaKind::MapWithNullValues(_) => return Err(error("transform-error", "Schema-bearing Flatten does not support Map fields")),
            _ => { output.insert(name, value.clone()); }
        }
    }
    Ok(())
}
fn length(prefix: usize, field: usize, delimiter: usize) -> Result<usize> { prefix.checked_add(field).and_then(|bytes| bytes.checked_add(if prefix == 0 { 0 } else { delimiter })).ok_or_else(|| error("allocation-overflow", "Flatten field length overflow")) }
fn schema_names(schema: &ConnectSchema, prefix: usize, delimiter: usize, depth: usize) -> Result<usize> {
    check_depth(depth)?;
    if let SchemaKind::Reference(reference) = &schema.kind { return reference.flattened_name_bound(prefix, delimiter, depth); }
    let SchemaKind::Struct(fields) = &schema.kind else { return Ok(0); };
    fields.iter().try_fold(0usize, |bytes, field| {
        let name = length(prefix, field.name.len(), delimiter)?;
        let nested = schema_names(&field.schema, name, delimiter, depth + 1)?;
        bytes.checked_add(name).and_then(|bytes| bytes.checked_add(nested)).ok_or_else(|| error("allocation-overflow", "Flatten schema length overflow"))
    })
}
fn value_names(value: &Value, prefix: usize, delimiter: usize, depth: usize) -> Result<usize> {
    check_depth(depth)?;
    let Value::Map { entries, .. } = value else { return Ok(0); };
    entries.iter().try_fold(0usize, |bytes, (key, value)| {
        let Value::Utf8(key) = key else { return Err(error("transform-error", "Flatten requires string map keys")); };
        let name = length(prefix, key.len(), delimiter)?;
        let nested = value_names(value, name, delimiter, depth + 1)?;
        bytes.checked_add(name).and_then(|bytes| bytes.checked_add(nested)).ok_or_else(|| error("allocation-overflow", "Flatten value length overflow"))
    })
}
