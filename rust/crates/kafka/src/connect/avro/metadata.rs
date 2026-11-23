use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::{Result, Value};
use serde_json::Value as Json;
use super::{ConnectSchema, SchemaKind, error};

pub(super) struct Metadata { root: Json, names: BTreeMap<String, Json>, connect: bool, discard_docs: bool, generalized: bool }
impl Metadata {
    pub fn new(text: &str, references: &[String], options: &BTreeMap<String, String>) -> Result<Self> {
        let root = serde_json::from_str(text).map_err(|cause| error("avro-schema", cause))?;
        let mut names = BTreeMap::new(); collect(&root, "", &mut names);
        for reference in references { collect(&serde_json::from_str(reference).map_err(|cause| error("avro-schema", cause))?, "", &mut names); }
        Ok(Self { root, names, connect: super::super::converter::option_bool(options, "connect.meta.data", true)?, discard_docs: super::super::converter::option_bool(options, "discard.type.doc.default", false)?, generalized: super::super::converter::option_bool(options, "generalized.sum.type.support", false)? })
    }
    pub fn root(&self) -> &Json { &self.root }
    pub fn named(&self, name: &str) -> Option<&Json> { self.names.get(name) }
    pub fn decorate_node_at_version(&self, schema: &ConnectSchema, raw: &Json, default: Option<&Json>, field_doc: Option<&str>, root: bool, version: Option<i32>) -> Result<super::super::schema_graph::Metadata> {
        let mut schema = self.schema(Arc::new(schema.clone()), raw, default, field_doc, 0)?;
        if root && raw.get("connect.version").is_none() && !raw.is_array() && !raw.get("type").is_some_and(Json::is_array) {
            if let Some(version) = version.filter(|version| *version >= 0) {
                if schema.version.is_some_and(|current| current != version) { return Err(error("avro-schema", "Registry version disagrees with logical type version")); }
                Arc::make_mut(&mut schema).version = Some(version);
            }
        }
        Ok(super::super::schema_graph::Metadata::from_schema(&schema))
    }
    fn schema(&self, schema: Arc<ConnectSchema>, raw: &Json, default: Option<&Json>, field_doc: Option<&str>, depth: usize) -> Result<Arc<ConnectSchema>> {
        if depth > 128 { return Err(error("avro-schema", "Avro metadata nesting limit exceeded")); }
        if let Some(name) = raw.as_str() {
            if let Some(definition) = schema.name.as_ref().and_then(|name| self.names.get(name)).or_else(|| self.names.get(name)) {
                return self.schema(schema, definition, default, field_doc, depth + 1);
            }
        }
        if raw.is_array() || raw.get("type").is_some_and(Json::is_array) { return Ok(schema); }
        let mut result = before_declared_logical_name(schema.as_ref().clone(), raw);
        let kind = raw.as_str().or_else(|| raw.get("type").and_then(Json::as_str)).unwrap_or_default();
        if kind == "record" && self.connect && !self.discard_docs {
            if let Some(fields) = raw.get("fields").and_then(Json::as_array) { for field in fields {
                if let (Some(name), Some(doc)) = (field.get("name").and_then(Json::as_str), field.get("doc").and_then(Json::as_str)) { result.parameters.insert(format!("io.confluent.connect.avro.field.doc.{name}"), doc.into()); }
            } }
        }
        if kind == "int" && raw.get("logicalType").is_none() {
            if let Some(annotation) = raw.get("connect.type").and_then(Json::as_str) { result.kind = match annotation { "int8" => SchemaKind::I8, "int16" => SchemaKind::I16, _ => return Err(error("avro-schema", "Unknown int connect.type annotation")) }; }
        }
        if kind == "fixed" { if let Some(size) = raw.get("size").and_then(Json::as_u64) { result.parameters.insert("connect.fixed.size".into(), size.to_string()); } }
        if raw.get("logicalType").and_then(Json::as_str).is_some_and(|name| name.eq_ignore_ascii_case("decimal")) {
            if let Some(precision) = raw.get("precision").and_then(Json::as_u64) { if precision != 64 { result.parameters.insert("connect.decimal.precision".into(), precision.to_string()); } }
        }
        if kind == "enum" {
            let fullname = schema.name.clone().unwrap_or_default(); let name = fullname.rsplit('.').next().unwrap_or(&fullname);
            let parameter = if self.generalized { "org.apache.kafka.connect.data.Enum" } else { "io.confluent.connect.avro.Enum" };
            result.parameters.insert(parameter.into(), fullname.clone());
            if let Some(symbols) = raw.get("symbols").and_then(Json::as_array) { for (index, symbol) in symbols.iter().enumerate() { let symbol = symbol.as_str().ok_or_else(|| error("avro-schema", "Enum symbol is not a string"))?; result.parameters.insert(format!("{parameter}.{symbol}"), if self.generalized { index.to_string() } else { symbol.into() }); } }
            if self.connect {
                if let Some(doc) = raw.get("doc").and_then(Json::as_str) { result.parameters.insert(if self.discard_docs { "connect.enum.doc".into() } else { format!("io.confluent.connect.avro.enum.doc.{name}") }, doc.into()); }
                if !self.discard_docs { if let Some(value) = raw.get("default").and_then(Json::as_str) { result.parameters.insert(format!("io.confluent.connect.avro.enum.default.{name}"), value.into()); } }
            }
        }
        let doc = raw.get("doc").and_then(Json::as_str);
        if self.discard_docs {
            result.doc = field_doc.or(doc).or_else(|| raw.get("connect.doc").and_then(Json::as_str)).map(str::to_owned);
            if self.connect { if let Some(doc) = doc { result.parameters.insert("connect.record.doc".into(), doc.into()); } }
        } else if self.connect {
            result.doc = raw.get("connect.doc").and_then(Json::as_str).map(str::to_owned);
            if kind == "record" { if let Some(doc) = doc { result.parameters.insert("io.confluent.connect.avro.record.doc".into(), doc.into()); } }
        }
        if let Some(version) = raw.get("connect.version") {
            let version = version.as_i64().ok_or_else(|| error("avro-schema", "Connect version must be numeric"))? as i32;
            if version >= 0 { if result.version.is_some_and(|previous| previous != version) { return Err(error("avro-schema", "Connect version disagrees with logical type version")); } result.version = Some(version); }
        }
        if self.connect { if let Some(parameters) = raw.get("connect.parameters") {
            for (name, value) in parameters.as_object().ok_or_else(|| error("avro-schema", "Connect parameters must be an object"))? { result.parameters.insert(name.clone(), value.as_str().ok_or_else(|| error("avro-schema", "Connect parameter must be a string"))?.into()); }
        } }
        if let Some(properties) = raw.as_object() { for (name, value) in properties { if name.starts_with("avro") { result.parameters.insert(name.clone(), value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())); } } }
        let connect_default = raw.get("connect.default");
        if default.is_some() && connect_default.is_none() && self.connect && !self.discard_docs { result.parameters.insert("io.confluent.connect.avro.field.default".into(), "true".into()); }
        let field_default = default.is_some();
        if let Some(default) = default.or(connect_default) {
            if let Ok(value) = default_value(default, &result, field_default) {
                if value != Value::Null && super::super::data::validate_value(&result, &value, 0).is_ok() { result.default = Some(value); }
            }
        }
        let declared_name = raw.get("connect.name").map(|name| name.as_str().map(str::to_owned).ok_or_else(|| error("avro-schema", "Connect name must be a string"))).transpose()?
            .or_else(|| if matches!(kind, "record" | "enum" | "fixed") { self.names.iter().find(|(_, definition)| *definition == raw).map(|(name, _)| name.clone()) } else { None });
        if let Some(name) = declared_name {
            if !name.starts_with("io.confluent.connect.avro.ConnectDefault") {
                if result.name.as_ref().is_some_and(|current| current.starts_with("org.apache.kafka.connect.data.") && current != &name) { return Err(error("avro-schema", "Connect name disagrees with logical type name")); }
                result.name = Some(name);
            }
        }
        if result.name.as_ref().is_some_and(|name| name.starts_with("io.confluent.connect.avro.ConnectDefault")) { result.name = None; }
        Ok(Arc::new(result))
    }
}
pub(super) fn before_declared_logical_name(mut schema: ConnectSchema, raw: &Json) -> ConnectSchema {
    let raw = raw.as_array().or_else(|| raw.get("type").and_then(Json::as_array)).and_then(|variants| variants.first()).unwrap_or(raw);
    let logical_name = raw.get("connect.name").and_then(Json::as_str).filter(|name| matches!(*name, "org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp" | "org.apache.kafka.connect.data.Decimal"));
    let physical_logical = raw.get("logicalType").and_then(Json::as_str).is_some_and(|name| ["date", "time-millis", "timestamp-millis", "decimal"].iter().any(|logical| name.eq_ignore_ascii_case(logical)));
    if logical_name.is_some() && schema.name.as_deref() == logical_name && !physical_logical { schema.name = None; }
    schema
}
fn collect(raw: &Json, namespace: &str, names: &mut BTreeMap<String, Json>) {
    if let Some(variants) = raw.as_array() { for variant in variants { collect(variant, namespace, names); } return; }
    let namespace = raw.get("namespace").and_then(Json::as_str).unwrap_or(namespace);
    match raw.get("type").and_then(Json::as_str) {
        Some("record" | "enum" | "fixed") => {
            let Some(name) = raw.get("name").and_then(Json::as_str) else { return; };
            let fullname = if name.contains('.') || namespace.is_empty() { name.into() } else { format!("{namespace}.{name}") };
            names.insert(fullname.clone(), raw.clone());
            let namespace = fullname.rsplit_once('.').map_or("", |(namespace, _)| namespace);
            if let Some(fields) = raw.get("fields").and_then(Json::as_array) { for field in fields { collect(&field["type"], namespace, names); } }
        }
        Some("array") => collect(&raw["items"], namespace, names), Some("map") => collect(&raw["values"], namespace, names),
        _ => if let Some(kind) = raw.get("type") { if kind.is_array() || kind.is_object() { collect(kind, namespace, names); } },
    }
}
pub(super) fn is_map_entry(raw: &Json) -> bool {
    raw.get("connect.internal.type").and_then(Json::as_str) == Some("MapEntry") || raw.get("name").and_then(Json::as_str) == Some("io.confluent.connect.avro.MapEntry") || (raw.get("name").and_then(Json::as_str) == Some("MapEntry") && raw.get("namespace").and_then(Json::as_str) == Some("io.confluent.connect.avro"))
}
pub(super) fn coerce(value: Value, schema: &ConnectSchema) -> Result<Value> {
    let schema = schema.resolved();
    Ok(match (value, &schema.kind) {
        (Value::I32(value), SchemaKind::I8) => Value::I8(value as i8), (Value::I32(value), SchemaKind::I16) => Value::I16(value as i16),
        (Value::Struct(mut values), SchemaKind::Struct(fields)) => { for field in fields { if let Some(value) = values.remove(&field.name) { values.insert(field.name.clone(), coerce(value, &field.schema)?); } } Value::Struct(values) }
        (Value::List(values), SchemaKind::Array(element)) => Value::list(values.into_values().into_iter().map(|value| coerce(value, element)).collect::<Result<_>>()?),
        (Value::Map { entries, sorted }, SchemaKind::Map(key, value)) => Value::Map { entries: entries.into_iter().map(|(k, v)| Ok((coerce(k, key)?, coerce(v, value)?))).collect::<Result<_>>()?, sorted },
        (Value::List(values), SchemaKind::Map(key, value)) => Value::Map { entries: values.into_values().into_iter().map(|entry| {
            let Value::Struct(mut fields) = entry else { return Err(error("avro-decode", "MapEntry value is not a record")); };
            Ok((coerce(fields.remove("key").ok_or_else(|| error("avro-decode", "MapEntry key is absent"))?, key)?, coerce(fields.remove("value").ok_or_else(|| error("avro-decode", "MapEntry value is absent"))?, value)?))
        }).collect::<Result<_>>()?, sorted: false },
        (value, _) => value,
    })
}
fn logical_date_default(schema: &ConnectSchema, int: i32, long: i64) -> Result<Option<Value>> {
    let millis = match schema.name.as_deref() {
        Some("org.apache.kafka.connect.data.Date") if matches!(schema.kind, SchemaKind::I32) => i64::from(int) * 86_400_000,
        Some("org.apache.kafka.connect.data.Time") if matches!(schema.kind, SchemaKind::I32) => {
            if !(0..=86_400_000).contains(&int) { return Err(error("avro-schema", "Time default is outside one day")); }
            i64::from(int)
        }
        Some("org.apache.kafka.connect.data.Timestamp") if matches!(schema.kind, SchemaKind::I64) => long,
        Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp") => return Err(error("avro-schema", "Logical date default has an incompatible primitive schema")),
        _ => return Ok(None),
    };
    Ok(Some(super::super::data::date_value(millis)))
}

fn default_value(value: &Json, schema: &ConnectSchema, field_default: bool) -> Result<Value> {
    let schema = schema.resolved();
    if value.is_null() { return Ok(Value::Null); }
    let integer = || value.as_i64().or_else(|| value.as_u64().map(|value| value as i64)).unwrap_or(0);
    if let Some(value) = logical_date_default(&schema, integer() as i32, integer())? { return Ok(value); }
    let bytes = || value.as_str().map(|value| value.chars().map(|character| u8::try_from(u32::from(character)).unwrap_or(b'?')).collect::<Vec<_>>()).ok_or_else(|| error("avro-schema", "Binary default must be a string"));
    match schema.name.as_deref() {
        Some("org.apache.kafka.connect.data.Decimal") => {
            return super::decimal_value(bytes()?, &schema);
        }
        _ => (),
    }
    Ok(match &schema.kind {
        SchemaKind::Bool => Value::Bool(value.as_bool().unwrap_or(false)), SchemaKind::I8 => Value::I8(integer() as i8), SchemaKind::I16 => Value::I16(integer() as i16), SchemaKind::I32 => Value::I32(integer() as i32), SchemaKind::I64 => Value::I64(integer()), SchemaKind::F32 => Value::F32(value.as_f64().unwrap_or(0.0) as f32), SchemaKind::F64 => Value::F64(value.as_f64().unwrap_or(0.0)),
        SchemaKind::String => Value::Utf8(value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string())), SchemaKind::Bytes => Value::Binary(bytes()?),
        SchemaKind::Array(element) => Value::list(value.as_array().ok_or_else(|| error("avro-schema", "Array default is not an array"))?.iter().map(|value| default_value(value, element, field_default)).collect::<Result<_>>()?),
        SchemaKind::Map(_, element) => Value::Map { entries: value.as_object().ok_or_else(|| error("avro-schema", "Map default is not an object"))?.iter().map(|(key, value)| Ok((Value::Utf8(key.clone()), default_value(value, element, field_default)?))).collect::<Result<_>>()?, sorted: false },
        SchemaKind::Struct(fields) => {
            let object = value.as_object().ok_or_else(|| error("avro-schema", "Record default is not an object"))?;
            Value::Struct(fields.iter().map(|field| {
                let value = match object.get(&field.name) {
                    Some(value) => default_value(value, &field.schema, field_default)?,
                    None if field_default => field.schema.default.clone().unwrap_or(Value::Null),
                    None => Value::Null,
                };
                super::super::data::validate_value(&field.schema, &value, 0)?;
                Ok((field.name.clone(), value))
            }).collect::<Result<_>>()?)
        }
        SchemaKind::ArrayOfNull | SchemaKind::MapOfNull | SchemaKind::MapWithNullValues(_) => return Err(error("avro-schema", "Avro default requires an element schema")),
        SchemaKind::Reference(_) => unreachable!("schema references resolve before defaults"),
    })
}

pub(super) fn raw_default_value(value: &super::default_json::DefaultJson, schema: &ConnectSchema, field_default: bool) -> Result<Value> {
    let schema = schema.resolved();
    if value.is_null() { return Ok(Value::Null); }
    let integer = || value.long_value().unwrap_or(0);
    if let Some(value) = logical_date_default(&schema, value.int_value().unwrap_or(0), integer())? { return Ok(value); }
    let bytes = || value.as_str().map(|value| value.chars().map(|character| u8::try_from(u32::from(character)).unwrap_or(b'?')).collect::<Vec<_>>()).ok_or_else(|| error("avro-schema", "Binary default must be a string"));
    match schema.name.as_deref() {
        Some("org.apache.kafka.connect.data.Decimal") => return super::decimal_value(bytes()?, &schema),
        _ => {}
    }
    Ok(match &schema.kind {
        SchemaKind::Bool => Value::Bool(value.as_bool().unwrap_or(false)),
        SchemaKind::I8 => Value::I8(value.int_value().unwrap_or(0) as i8), SchemaKind::I16 => Value::I16(value.int_value().unwrap_or(0) as i16), SchemaKind::I32 => Value::I32(value.int_value().unwrap_or(0)), SchemaKind::I64 => Value::I64(integer()),
        SchemaKind::F32 => Value::F32(value.double_value().unwrap_or(0.0) as f32), SchemaKind::F64 => Value::F64(value.double_value().unwrap_or(0.0)),
        SchemaKind::String => Value::Utf8(value.as_str().map(str::to_owned).ok_or_else(|| error("avro-schema", "String default is not textual"))?), SchemaKind::Bytes => Value::Binary(bytes()?),
        SchemaKind::Array(element) => Value::list(value.as_array().ok_or_else(|| error("avro-schema", "Array default is not an array"))?.iter().map(|value| raw_default_value(value, element, field_default)).collect::<Result<_>>()?),
        SchemaKind::Map(_, element) => Value::Map { entries: value.as_object().ok_or_else(|| error("avro-schema", "Map default is not an object"))?.iter().map(|(key, value)| Ok((Value::Utf8(key.clone()), raw_default_value(value, element, field_default)?))).collect::<Result<_>>()?, sorted: false },
        SchemaKind::Struct(fields) => {
            value.as_object().ok_or_else(|| error("avro-schema", "Record default is not an object"))?;
            Value::Struct(fields.iter().map(|field| {
                let value = match value.get(&field.name) { Some(value) => raw_default_value(value, &field.schema, field_default)?, None if field_default => field.schema.default.clone().unwrap_or(Value::Null), None => Value::Null };
                super::super::data::validate_value(&field.schema, &value, 0)?; Ok((field.name.clone(), value))
            }).collect::<Result<_>>()?)
        }
        _ => return Err(error("avro-schema", "Default requires a concrete Connect schema")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn logical_date_defaults_use_raw_dates_and_the_primitive_number_accessor() {
        use super::super::default_json::DefaultJson;
        let mut date = ConnectSchema::new(SchemaKind::I32);
        date.name = Some("org.apache.kafka.connect.data.Date".into());
        assert_eq!(raw_default_value(&DefaultJson::Number("1e50".into()), &date, false).unwrap(), super::super::super::data::date_value(i64::from(i32::MAX) * 86_400_000));
        assert_eq!(raw_default_value(&DefaultJson::Number("-1".into()), &date, false).unwrap(), super::super::super::data::date_value(-86_400_000));
        let mut time = ConnectSchema::new(SchemaKind::I32);
        time.name = Some("org.apache.kafka.connect.data.Time".into());
        assert_eq!(raw_default_value(&DefaultJson::Number("86400000".into()), &time, false).unwrap(), super::super::super::data::date_value(86_400_000));
        assert!(raw_default_value(&DefaultJson::Number("86400001".into()), &time, false).is_err());
        assert!(raw_default_value(&DefaultJson::Number("-1".into()), &time, false).is_err());
        time.kind = SchemaKind::I8;
        assert!(raw_default_value(&DefaultJson::Number("0".into()), &time, false).is_err());
    }
    #[test]
    fn decimal_defaults_preserve_full_scale_and_reject_invalid_schema_scales() {
        let mut schema = ConnectSchema::new(SchemaKind::Bytes);
        schema.name = Some("org.apache.kafka.connect.data.Decimal".into());
        for scale in [i32::MIN, -129, 129, i32::MAX] {
            schema.parameters.insert("scale".into(), scale.to_string());
            let Value::Decimal(value) = default_value(&Json::String("\u{1}".into()), &schema, false).unwrap() else { panic!("expected Decimal") };
            assert_eq!(value.scale, scale);
            assert_eq!(num_bigint::BigInt::from_signed_bytes_be(&value.unscaled), 1.into());
        }
        for scale in ["2147483648", "invalid"] {
            schema.parameters.insert("scale".into(), scale.into());
            assert!(default_value(&Json::String("\u{1}".into()), &schema, false).is_err());
        }
        schema.parameters.remove("scale");
        assert!(default_value(&Json::String("\u{1}".into()), &schema, false).is_err());
        schema.parameters.insert("scale".into(), "0".into());
        assert!(default_value(&Json::String(String::new()), &schema, false).is_err());
    }
    #[test]
    fn field_float_defaults_round_through_the_jackson_double_accessor() {
        for (literal,bits) in [("1152921573326323713",0x5d800000),("-1152921573326323713",0xdd800000)] {
            let schema=format!(r#"{{"type":"record","name":"R","fields":[{{"name":"scalar","type":"float","default":{literal}}},{{"name":"array","type":{{"type":"array","items":"float"}},"default":[{literal}]}},{{"name":"map","type":{{"type":"map","values":"float"}},"default":{{"a":{literal}}}}},{{"name":"node","type":{{"type":"float","connect.default":{literal}}}}}]}}"#);
            let plan=super::super::AvroPlan::parse(&schema,&[]).unwrap();
            let scalar=plan.connect.field("scalar").unwrap().schema;
            let Value::F32(value)=scalar.default.as_ref().unwrap() else { panic!("expected Float default") };
            assert_eq!(value.to_bits(),bits);
            let array=plan.connect.field("array").unwrap().schema;
            let Value::List(values)=array.default.as_ref().unwrap() else { panic!("expected Array default") };
            let item=values.get(0).unwrap();
            let Value::F32(value)=item.as_ref() else { panic!("expected Float element") };
            assert_eq!(value.to_bits(),bits);
            let map=plan.connect.field("map").unwrap().schema;
            let Value::Map {entries,..}=map.default.as_ref().unwrap() else { panic!("expected Map default") };
            let Value::F32(value)=&entries[0].1 else { panic!("expected Float map value") };
            assert_eq!(value.to_bits(),bits);
            let node=plan.connect.field("node").unwrap().schema;
            let Value::F32(value)=node.default.as_ref().unwrap() else { panic!("expected JsonNode Float default") };
            assert_eq!(value.to_bits(),bits);
        }
    }
}
