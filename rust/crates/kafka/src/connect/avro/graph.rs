use std::{collections::BTreeMap, sync::Arc};
use apache_avro::Schema;
use rtdb_types::Result;
use serde_json::Value as Json;
use super::{ConnectSchema, error, metadata};
use super::super::schema_graph::{GraphBuilder, Metadata, Node, NodeId, NodeKind, Scalar};

#[derive(Clone, Copy)]
pub(super) struct UnionOptions { pub enhanced: bool, pub generalized: bool, pub flatten_singleton: bool }
impl UnionOptions {
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self> {
        let flag = super::super::converter::option_bool;
        Ok(Self { enhanced: flag(options, "enhanced.avro.schema.support", false)?, generalized: flag(options, "generalized.sum.type.support", false)?, flatten_singleton: flag(options, "flatten.singleton.unions", true)? })
    }
    pub fn field_name(self, schema: &Schema, index: usize) -> String {
        if self.generalized { return format!("connect_union_field_{index}"); }
        if self.enhanced { match schema {
            Schema::Record(value) => return value.name.fullname(None), Schema::Enum(value) => return value.name.fullname(None), Schema::Fixed(value) => return value.name.fullname(None),
            Schema::Decimal(value) => return self.field_name(&value.inner, index), _ => (),
        } }
        super::member_name(schema)
    }
}
struct Decoration { raw: Json, default: Option<Json>, default_source: Option<(String, String)>, doc: Option<String>, root: bool }
struct Builder<'a> {
    graph: GraphBuilder, names: BTreeMap<String, NodeId>, decorations: BTreeMap<NodeId, Decoration>,
    definitions: &'a BTreeMap<String, Schema>, metadata: &'a metadata::Metadata, unions: UnionOptions, next_union: &'a mut i32,
}
pub(super) fn build_at(schema: &Schema, raw: &Json, definitions: &BTreeMap<String, Schema>, metadata: &metadata::Metadata, defaults: &super::default_json::Defaults, unions: UnionOptions, version: Option<i32>, next_union: &mut i32) -> Result<Arc<ConnectSchema>> {
    let mut builder = Builder { graph: GraphBuilder::new(), names: BTreeMap::new(), decorations: BTreeMap::new(), definitions, metadata, unions, next_union };
    let root = builder.schema(schema, raw, 0)?;
    if let Some(decoration) = builder.decorations.get_mut(&root) { decoration.root = true; }
    builder.graph.finish_with(root, |id, schema| match builder.decorations.get(&id) {
        Some(decoration) => {
            let default = decoration.default_source.as_ref().and_then(|(record, field)| defaults.get(record, field));
            let class = match (&decoration.default_source, default) {
                (Some((record, field)), Some(default)) => {
                    let source = definitions.get(record).and_then(|schema| if let Schema::Record(record) = schema { record.lookup.get(field).and_then(|index| record.fields.get(*index)).map(|field| &field.schema) } else { None }).ok_or_else(|| error("avro-schema", "Default field schema is absent"))?;
                    super::default_json::field_default_class(default, source, definitions)?
                }
                _ => None,
            };
            let mut metadata = metadata.decorate_node_at_version(schema, &decoration.raw, if default.is_none() || class.is_some() { decoration.default.as_ref() } else { None }, decoration.doc.as_deref(), decoration.root, version)?;
            if let (Some(default), Some(valid)) = (default, class) {
                let view = metadata::before_declared_logical_name(metadata.clone().schema(schema.kind.clone()), &decoration.raw);
                metadata.default = if valid { metadata::raw_default_value(default, &view, true).ok().filter(|value| *value != rtdb_types::Value::Null && super::super::data::validate_value(&view, value, 0).is_ok()) } else { None };
            }
            Ok(metadata)
        }
        None => Ok(Metadata::from_schema(schema)),
    })
}
impl Builder<'_> {
    fn alias(&mut self, target: NodeId, optional: bool, raw: &Json, default: Option<&Json>, default_source: Option<(String, String)>, doc: Option<&str>) -> NodeId {
        let id = self.graph.insert(Node { kind: NodeKind::Alias(target), metadata: Metadata { optional, ..Default::default() } });
        self.decorations.insert(id, Decoration { raw: raw.clone(), default: default.cloned(), default_source, doc: doc.map(str::to_owned), root: false }); id
    }
    fn schema(&mut self, schema: &Schema, raw: &Json, depth: usize) -> Result<NodeId> {
        if depth > 128 { return Err(error("avro-schema", "Avro schema nesting limit exceeded")); }
        if let Schema::Ref { name } = schema {
            let name = name.fullname(None);
            if let Some(id) = self.names.get(&name) { return Ok(*id); }
            let definition = self.definitions.get(&name).ok_or_else(|| error("avro-schema", "Unknown Avro schema reference"))?;
            let raw = self.metadata.named(&name).ok_or_else(|| error("avro-schema", "Unknown Avro metadata reference"))?;
            return self.schema(definition, raw, depth + 1);
        }
        let name = match schema {
            Schema::Record(value) => Some(value.name.fullname(None)), Schema::Enum(value) => Some(value.name.fullname(None)), Schema::Fixed(value) => Some(value.name.fullname(None)),
            Schema::Decimal(value) => if let Schema::Fixed(fixed) = value.inner.as_ref() { Some(fixed.name.fullname(None)) } else { None }, _ => None,
        };
        if let Some(id) = name.as_ref().and_then(|name| self.names.get(name)) { return Ok(*id); }
        let id = self.graph.reserve();
        if let Some(name) = &name { self.names.insert(name.clone(), id); }
        let raw = name.as_deref().and_then(|name| self.metadata.named(name)).unwrap_or(raw);
        let mut metadata = Metadata { name, ..Default::default() };
        let kind = match schema {
            Schema::Null => return Err(error("avro-schema", "Standalone null schemas are not supported by AvroConverter")),
            Schema::Boolean => NodeKind::Scalar(Scalar::Bool),
            Schema::Int => NodeKind::Scalar(if raw.get("logicalType").is_none() { match raw.get("connect.type").and_then(Json::as_str) { Some("int8") => Scalar::I8, Some("int16") => Scalar::I16, None => Scalar::I32, _ => return Err(error("avro-schema", "Unknown int connect.type annotation")) } } else { Scalar::I32 }),
            Schema::Date => { metadata.name = Some("org.apache.kafka.connect.data.Date".into()); metadata.version = Some(1); NodeKind::Scalar(Scalar::I32) }
            Schema::TimeMillis => { metadata.name = Some("org.apache.kafka.connect.data.Time".into()); metadata.version = Some(1); NodeKind::Scalar(Scalar::I32) }
            Schema::TimestampMillis => { metadata.name = Some("org.apache.kafka.connect.data.Timestamp".into()); metadata.version = Some(1); NodeKind::Scalar(Scalar::I64) }
            Schema::Long | Schema::TimeMicros | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => NodeKind::Scalar(Scalar::I64),
            Schema::Float => NodeKind::Scalar(Scalar::F32), Schema::Double => NodeKind::Scalar(Scalar::F64),
            Schema::String | Schema::Uuid | Schema::Enum(_) => NodeKind::Scalar(Scalar::String),
            Schema::Bytes | Schema::Fixed(_) | Schema::BigDecimal | Schema::Duration => NodeKind::Scalar(Scalar::Bytes),
            Schema::Decimal(decimal) => { metadata.name = Some("org.apache.kafka.connect.data.Decimal".into()); metadata.version = Some(1); metadata.parameters.insert("scale".into(), decimal.scale.to_string()); NodeKind::Scalar(Scalar::Bytes) }
            Schema::Array(array) => {
                let items = super::resolve(&array.items, self.definitions)?;
                let raw_items = if let Schema::Record(record) = items { self.metadata.named(&record.name.fullname(None)).unwrap_or(&raw["items"]) } else { &raw["items"] };
                if metadata::is_map_entry(raw_items) {
                    let Schema::Record(record) = items else { return Err(error("avro-schema", "MapEntry must be a record")); };
                    if record.fields.len() != 2 { return Err(error("avro-schema", "MapEntry requires key and value fields")); }
                    let mut entries = Vec::new();
                    for name in ["key", "value"] {
                        let field = record.fields.iter().find(|field| field.name == name).ok_or_else(|| error("avro-schema", "MapEntry field is absent"))?;
                        let raw_field = raw_items["fields"].as_array().and_then(|fields| fields.iter().find(|field| field["name"].as_str() == Some(name))).ok_or_else(|| error("avro-schema", "MapEntry metadata is absent"))?;
                        entries.push(self.schema(&field.schema, &raw_field["type"], depth + 1)?);
                    }
                    NodeKind::Map(entries[0], entries[1])
                } else { NodeKind::Array(self.schema(&array.items, &raw["items"], depth + 1)?) }
            }
            Schema::Map(map) => {
                let key = self.graph.insert(Node { kind: NodeKind::Scalar(Scalar::String), metadata: Metadata::default() });
                NodeKind::Map(key, self.schema(&map.types, &raw["values"], depth + 1)?)
            }
            Schema::Record(record) => {
                let mut fields = Vec::new();
                for field in &record.fields {
                    let raw_field = raw["fields"].as_array().and_then(|fields| fields.iter().find(|raw| raw["name"].as_str() == Some(&field.name))).ok_or_else(|| error("avro-schema", "Record field metadata is absent"))?;
                    let target = self.schema(&field.schema, &raw_field["type"], depth + 1)?;
                    let field = (field.name.clone(), self.alias(target, false, &raw_field["type"], raw_field.get("default"), Some((record.name.fullname(None), field.name.clone())), raw_field.get("doc").and_then(Json::as_str)));
                    fields.push(field);
                }
                NodeKind::Struct(fields)
            }
            Schema::Union(union) => {
                let variants = union.variants();
                let raw_variants = raw.as_array().or_else(|| raw.get("type").and_then(Json::as_array)).ok_or_else(|| error("avro-schema", "Union metadata is absent"))?;
                let nullable = variants.iter().any(|schema| matches!(schema, Schema::Null));
                if (variants.len() == 1 && self.unions.flatten_singleton) || (variants.len() == 2 && nullable) {
                    let index = variants.iter().position(|schema| !matches!(schema, Schema::Null)).ok_or_else(|| error("avro-schema", "Union has no value member"))?;
                    metadata.optional = nullable;
                    NodeKind::Alias(self.schema(&variants[index], &raw_variants[index], depth + 1)?)
                } else {
                    let name = if self.unions.generalized { let name = format!("connect_union_{}", self.next_union); *self.next_union = self.next_union.wrapping_add(1); name } else { "io.confluent.connect.avro.Union".into() };
                    if self.unions.generalized { metadata.parameters.insert("org.apache.kafka.connect.data.Union".into(), name.clone()); }
                    metadata.name = Some(name); metadata.optional = nullable;
                    let mut fields = Vec::new();
                    for (index, member) in variants.iter().enumerate().filter(|(_, member)| !matches!(member, Schema::Null)) {
                        let target = self.schema(member, &raw_variants[index], depth + 1)?;
                        fields.push((self.unions.field_name(super::resolve(member, self.definitions)?, index), self.alias(target, true, &raw_variants[index], None, None, None)));
                    }
                    NodeKind::Struct(fields)
                }
            }
            Schema::Ref { .. } => unreachable!("references resolve before node construction"),
        };
        self.graph.define(id, Node { kind, metadata })?;
        self.decorations.insert(id, Decoration { raw: raw.clone(), default: None, default_source: None, doc: None, root: false }); Ok(id)
    }
}
