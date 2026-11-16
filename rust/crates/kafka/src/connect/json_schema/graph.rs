use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_types::Result;
use super::super::json_value::JsonValue as Json;
use super::super::{ConnectSchema, error, schema_graph::{GraphBuilder, Metadata, Node, NodeId, NodeKind, Scalar}};

struct Builder<'a> {
    graph: GraphBuilder,
    nodes: BTreeMap<(usize, Option<String>), NodeId>,
    defaults: BTreeMap<NodeId, &'a Json>,
    objects: BTreeSet<NodeId>,
    reference_ids: BTreeMap<NodeId, i32>,
    references: &'a BTreeMap<String, Json>,
    optional_fields: bool, generalized: bool, flatten_singleton: bool,
    union_index: i32,
    reference_index: i32,
}
pub(super) fn build(root: &Json, references: &BTreeMap<String, Json>, options: &BTreeMap<String, String>, version: Option<i32>) -> Result<Arc<ConnectSchema>> {
    let flag = super::super::converter::option_bool;
    let mut builder = Builder { graph: GraphBuilder::new(), nodes: BTreeMap::new(), defaults: BTreeMap::new(), objects: BTreeSet::new(), reference_ids: BTreeMap::new(), references, optional_fields: flag(options, "use.optional.for.nonrequired", false)?, generalized: flag(options, "generalized.sum.type.support", false)?, flatten_singleton: flag(options, "flatten.singleton.unions", true)?, union_index: 0, reference_index: 0 };
    let root_id = builder.schema(root, root, None, version, 0)?;
    let defaults = builder.defaults;
    let reference_ids = builder.reference_ids;
    builder.graph.finish_with(root_id, |id, schema| {
        let mut metadata = Metadata::from_schema(schema);
        if let Some(index) = reference_ids.get(&id) { metadata.parameters.insert("io.confluent.connect.json.Id".into(), format!("#id{index}")); }
        if let Some(default) = defaults.get(&id) {
            let limit = default.encoded_bound()?.saturating_add(1).saturating_mul(512usize.saturating_add(schema.view_allocation_bound())).saturating_add(schema.retained_bytes()?.saturating_mul(4)).saturating_add(65_536);
            let value = super::super::converter::convert_registry(&Arc::new(schema.clone()), default, limit)?;
            super::super::data::validate_value(schema, &value, 0)?;
            metadata.default = Some(value);
        }
        Ok(metadata)
    })
}
impl<'a> Builder<'a> {
    fn alias(&mut self, target: NodeId, optional: bool) -> NodeId {
        self.graph.insert(Node { kind: NodeKind::Alias(target), metadata: Metadata { optional, ..Default::default() } })
    }
    fn reference(&self, raw: &'a Json, document: &'a Json) -> Result<(&'a Json, &'a Json)> {
        let reference = raw.get("$ref").and_then(Json::as_str).ok_or_else(|| error("json-schema", "JSON Schema reference must be a string"))?;
        let (name, pointer) = reference.split_once('#').unwrap_or((reference, ""));
        let document = if name.is_empty() { document } else { self.references.get(name).ok_or_else(|| error("json-schema", format!("Unresolved JSON Schema reference '{name}'")))? };
        let target = document.pointer(pointer).ok_or_else(|| error("json-schema", "JSON Schema reference pointer is absent"))?;
        Ok((target, document))
    }
    fn schema(&mut self, raw: &'a Json, document: &'a Json, kind_override: Option<&str>, version: Option<i32>, depth: usize) -> Result<NodeId> {
        if depth > 128 { return Err(error("json-schema", "JSON Schema nesting limit exceeded")); }
        let identity = (raw as *const Json as usize, kind_override.map(str::to_owned));
        if let Some(id) = self.nodes.get(&identity) { return Ok(*id); }
        let id = self.graph.reserve(); self.nodes.insert(identity, id);
        if raw.get("$ref").is_some() {
            let (target, document) = self.reference(raw, document)?;
            if let Some(target) = self.nodes.get(&(target as *const Json as usize, None)).filter(|target| self.objects.contains(*target)) {
                self.reference_index = self.reference_index.wrapping_add(1);
                self.reference_ids.insert(*target, self.reference_index);
            }
            let target = self.schema(target, document, None, version, depth + 1)?;
            self.graph.define(id, Node { kind: NodeKind::Alias(target), metadata: Metadata::default() })?; return Ok(id);
        }
        let mut metadata = annotations(raw, version)?;
        let kind = if let Some(variants) = raw.get("type").and_then(Json::as_array).filter(|_| kind_override.is_none()) {
            let variants = variants.iter().map(|kind| kind.as_str().map(|kind| (raw, Some(kind))).ok_or_else(|| error("json-schema", "JSON Schema type member must be a string"))).collect::<Result<Vec<_>>>()?;
            self.union(&variants, document, &mut metadata, version, depth + 1)?
        } else if let Some(variants) = raw.get("oneOf").or_else(|| raw.get("anyOf")).and_then(Json::as_array) {
            self.union(&variants.iter().map(|schema| (schema, None)).collect::<Vec<_>>(), document, &mut metadata, version, depth + 1)?
        } else if let Some(variants) = raw.get("allOf").and_then(Json::as_array) {
            self.all_of(variants, document, version, depth + 1)?
        } else {
            let primitive = kind_override.or_else(|| raw.get("type").and_then(Json::as_str)).or_else(|| if raw.get("properties").is_some() { Some("object") } else if raw.get("enum").is_some() || raw.get("const").is_some() { Some("string") } else { None }).ok_or_else(|| error("json-schema", "JSON Schema has no supported Connect type"))?;
            let annotation = raw.get("connect.type").and_then(Json::as_str);
            match primitive {
                "boolean" => NodeKind::Scalar(Scalar::Bool),
                "integer" | "number" => NodeKind::Scalar(match annotation {
                    Some("int8") => Scalar::I8, Some("int16") => Scalar::I16, Some("int32") => Scalar::I32, Some("int64") => Scalar::I64, Some("float32") => Scalar::F32, Some("float64") => Scalar::F64, Some("bytes") => Scalar::Bytes,
                    None => if primitive == "integer" { Scalar::I64 } else { Scalar::F64 }, _ => return Err(error("json-schema", "Unknown numeric connect.type")),
                }),
                "string" => {
                    if let Some(symbols) = raw.get("enum").and_then(Json::as_array).map(|symbols| symbols.iter().collect::<Vec<_>>()).or_else(|| raw.get("const").map(|value| vec![value])) {
                        let parameter = if self.generalized { "org.apache.kafka.connect.data.Enum" } else { "io.confluent.connect.json.Enum" };
                        metadata.parameters.insert(parameter.into(), String::new());
                        for (index, symbol) in symbols.into_iter().enumerate() {
                            let symbol = if symbol.is_null() { "<NULL>".into() } else { if let Some(text) = symbol.as_str() { text.to_owned() } else { String::from_utf8(symbol.to_vec()?).expect("JSON is UTF-8") } };
                            metadata.parameters.insert(format!("{parameter}.{symbol}"), if self.generalized { index.to_string() } else { symbol });
                        }
                    }
                    NodeKind::Scalar(if annotation == Some("bytes") { Scalar::Bytes } else { Scalar::String })
                }
                "array" if annotation == Some("map") => {
                    let properties = raw.get("items").and_then(|items| items.get("properties")).ok_or_else(|| error("json-schema", "Map entries require key and value properties"))?;
                    NodeKind::Map(self.schema(properties.get("key").ok_or_else(|| error("json-schema", "Map entry key schema is absent"))?, document, None, None, depth + 1)?, self.schema(properties.get("value").ok_or_else(|| error("json-schema", "Map entry value schema is absent"))?, document, None, None, depth + 1)?)
                }
                "array" => NodeKind::Array(self.schema(raw.get("items").ok_or_else(|| error("json-schema", "Array schema lacks items"))?, document, None, None, depth + 1)?),
                "object" if annotation == Some("map") => {
                    let key = self.graph.insert(Node { kind: NodeKind::Scalar(Scalar::String), metadata: Metadata::default() });
                    NodeKind::Map(key, self.schema(raw.get("additionalProperties").ok_or_else(|| error("json-schema", "Map schema lacks additionalProperties"))?, document, None, None, depth + 1)?)
                }
                "object" => {
                    self.objects.insert(id);
                    let required = raw.get("required").and_then(Json::as_array); let mut fields = BTreeMap::new();
                    if let Some(properties) = raw.get("properties").and_then(Json::as_object) {
                        for (name, property) in properties {
                            let target = self.schema(property, document, None, None, depth + 1)?;
                            let optional = self.optional_fields && !required.is_some_and(|required| required.iter().any(|value| value.as_str() == Some(name)));
                            let index = integer_annotation(property, "connect.index")?.map(i64::from).unwrap_or(fields.len() as i64);
                            fields.insert(index, (name.clone(), if optional { self.alias(target, true) } else { target }));
                        }
                    }
                    NodeKind::Struct(fields.into_values().collect())
                }
                _ => return Err(error("json-schema", format!("Unsupported standalone JSON Schema type '{primitive}'"))),
            }
        };
        self.graph.define(id, Node { kind, metadata })?;
        if let Some(default) = raw.get("default") { self.defaults.insert(id, default); }
        Ok(id)
    }
    fn union(&mut self, variants: &[(&'a Json, Option<&str>)], document: &'a Json, metadata: &mut Metadata, version: Option<i32>, depth: usize) -> Result<NodeKind> {
        if variants.len() == 1 && self.flatten_singleton { return Ok(NodeKind::Alias(self.schema(variants[0].0, document, variants[0].1, None, depth)?)); }
        let name = if self.generalized { let name = format!("connect_union_{}", self.union_index); self.union_index = self.union_index.wrapping_add(1); name } else { "io.confluent.connect.json.OneOf".into() };
        let nonnull = variants.iter().filter(|(schema, kind)| kind.or_else(|| schema.get("type").and_then(Json::as_str)) != Some("null")).collect::<Vec<_>>();
        if variants.len() == 2 && nonnull.len() == 1 { metadata.optional = true; let (schema, kind) = nonnull[0]; return Ok(NodeKind::Alias(self.schema(schema, document, *kind, version, depth)?)); }
        metadata.name = Some(name.clone()); metadata.optional |= nonnull.len() < variants.len();
        if self.generalized { metadata.parameters.insert("org.apache.kafka.connect.data.Union".into(), name.clone()); }
        let mut fields = Vec::new();
        for (index, (schema, kind)) in nonnull.into_iter().enumerate() {
            let target = self.schema(schema, document, *kind, None, depth)?;
            fields.push((if self.generalized { format!("connect_union_field_{index}") } else { format!("{name}.field.{index}") }, self.alias(target, true)));
        }
        Ok(NodeKind::Struct(fields))
    }
    fn all_of(&mut self, variants: &'a [Json], document: &'a Json, version: Option<i32>, depth: usize) -> Result<NodeKind> {
        if variants.len() == 1 && self.flatten_singleton { return Ok(NodeKind::Alias(self.schema(&variants[0], document, None, None, depth)?)); }
        let mut properties = Vec::new(); let mut visited = BTreeSet::new();
        for variant in variants { self.collect_properties(variant, document, &mut properties, &mut visited)?; }
        if !properties.is_empty() {
            let mut fields = Vec::new();
            for (name, property, document, required) in properties {
                let target = self.schema(property, document, None, None, depth)?;
                fields.push((name, if self.optional_fields && !required { self.alias(target, true) } else { target }));
            }
            return Ok(NodeKind::Struct(fields));
        }
        let last = |predicate: fn(&Json) -> bool| variants.iter().rev().find(|schema| predicate(schema));
        let combined = last(|schema| schema.get("allOf").is_some() || schema.get("oneOf").is_some() || schema.get("anyOf").is_some());
        let string = last(|schema| schema.get("type").and_then(Json::as_str) == Some("string"));
        let number = last(|schema| matches!(schema.get("type").and_then(Json::as_str), Some("integer" | "number")));
        let enumeration = last(|schema| schema.get("enum").is_some());
        let constant = last(|schema| schema.get("const").is_some());
        let reference = last(|schema| schema.get("$ref").is_some());
        let target = combined.or_else(|| if constant.is_some() { string.or(number) } else if enumeration.is_some() { if string.is_some() { enumeration } else { number } } else if string.is_some_and(|schema| schema.get("format").is_some()) { number.or(string) } else { reference }).ok_or_else(|| error("json-schema", "Unsupported allOf schema combination"))?;
        Ok(NodeKind::Alias(self.schema(target, document, None, version, depth)?))
    }
    fn collect_properties(&self, schema: &'a Json, document: &'a Json, properties: &mut Vec<(String, &'a Json, &'a Json, bool)>, visited: &mut BTreeSet<usize>) -> Result<()> {
        if !visited.insert(schema as *const Json as usize) { return Ok(()); }
        if let Some(variants) = schema.get("allOf").and_then(Json::as_array) { for variant in variants { self.collect_properties(variant, document, properties, visited)?; } }
        else if schema.get("$ref").is_some() { let (schema, document) = self.reference(schema, document)?; self.collect_properties(schema, document, properties, visited)?; }
        else if let Some(fields) = schema.get("properties").and_then(Json::as_object) {
            for (name, value) in fields {
                let required = schema.get("required").and_then(Json::as_array).is_some_and(|required| required.iter().any(|value| value.as_str() == Some(name)));
                let entry = (name.clone(), value, document, required);
                if let Some(previous) = properties.iter_mut().find(|(previous, _, _, _)| previous == name) { *previous = entry; } else { properties.push(entry); }
            }
        }
        Ok(())
    }
}
fn annotations(raw: &Json, version: Option<i32>) -> Result<Metadata> {
    let mut metadata = Metadata { name: raw.get("title").and_then(Json::as_str).map(str::to_owned), doc: raw.get("description").and_then(Json::as_str).map(str::to_owned), version: integer_annotation(raw, "connect.version")?.or(version), ..Default::default() };
    if let Some(parameters) = raw.get("connect.parameters") {
        metadata.parameters = parameters.as_object().ok_or_else(|| error("json-schema", "Connect parameters must be an object"))?.iter().map(|(name, value)| Ok((name.clone(), value.as_str().ok_or_else(|| error("json-schema", "Connect parameter must be a string"))?.to_owned()))).collect::<Result<_>>()?;
    }
    Ok(metadata)
}
fn integer_annotation(raw: &Json, name: &str) -> Result<Option<i32>> {
    match raw.get(name).filter(|value| !value.is_null()) {
        None => Ok(None),
        Some(value) if value.is_int() => Ok(value.int_value()),
        Some(_) => Err(error("json-schema", format!("{name} requires a Java Integer value"))),
    }
}
