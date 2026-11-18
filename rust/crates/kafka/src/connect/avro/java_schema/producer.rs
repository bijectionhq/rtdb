use super::*;

mod input;
mod native_bound;
mod pair;
mod java_text;
#[cfg(test)]
mod string_reader;
#[cfg(test)]
pub(in crate::connect) use string_reader::{SchemaStringFactory, InputBuffers, TokenBuffer, TokenRecycler};
use native_bound::NativeFootprint;
pub(in crate::connect::avro) use pair::{AdmittedSource, ParsedPair, PairSelection};

#[derive(Clone, Copy)]
pub(in crate::connect) struct SourcePolicy { pub validate_defaults: bool, pub fixed_bytes: FixedBytesLimit }
pub(super) struct CanonicalSource { pub root: SchemaHandle, native: NativeFootprint }
struct Producer<'a> { builder: SchemasBuilder, policy: SourcePolicy, native: NativeFootprint, logical: &'a mut dyn LogicalEnvironment }
const SCHEMA_RESERVED: &[&str] = &["doc", "fields", "items", "name", "namespace", "size", "symbols", "values", "type", "aliases"];
const FIELD_RESERVED: &[&str] = &["default", "doc", "name", "order", "type", "aliases"];

pub(super) fn parse(text: &str, references: &[String], policy: SourcePolicy, context: &mut ConversionContext) -> Result<CanonicalSource> {
    parse_with_logical(text, references, policy, &mut logical::Unregistered, context)
}
pub(super) fn parse_with_logical(text: &str, references: &[String], policy: SourcePolicy, logical: &mut dyn LogicalEnvironment, context: &mut ConversionContext) -> Result<CanonicalSource> {
    let mut producer = Producer { builder: SchemasBuilder::new(context)?, policy, native: NativeFootprint::new(), logical };
    for reference in references {
        let document = input::parse(reference, context)?;
        producer.native.document(reference.len(), &document)?;
        producer.schema(&document, None, context, 0)?;
    }
    let document = input::parse(text, context)?;
    producer.native.document(text.len(), &document)?;
    let root = producer.schema(&document, None, context, 0)?;
    Ok(CanonicalSource { root: producer.builder.finish(root, context)?, native: producer.native })
}
fn is(text: &Text, expected: &str) -> bool { text.view().units().eq(expected.encode_utf16()) }
fn order_is(text: &Text, expected: &str) -> bool {
    text.view().units().map(|unit| match unit {
        0x61..=0x7a => unit - 0x20,
        0x0131 => 0x49,
        0x017f => 0x53,
        _ => unit,
    }).eq(expected.encode_utf16())
}
fn required<'a>(node: &'a JsonNode, key: &str) -> Result<&'a JsonNode> {
    node.get(key).ok_or_else(|| error("avro-schema", "Required schema member is absent"))
}
fn text<'a>(node: &'a JsonNode, key: &str) -> Result<&'a Text> {
    required(node, key)?.as_text().ok_or_else(|| error("avro-schema", "Required schema member is not textual"))
}
fn array(node: &JsonNode) -> Result<&[JsonNode]> {
    match node.value() { JsonValue::Array(values) => Ok(values), _ => Err(error("avro-schema", "Required schema member is not an array")) }
}
fn namespace(name: &Name) -> Option<JavaText<'_>> {
    let full = name.full()?.view();
    let dot = full.units().enumerate().filter_map(|(at, unit)| (unit == 46).then_some(at)).last()?;
    full.slice_units(0..dot)
}
fn vector_bytes<T>(count: usize) -> Result<usize> { add(mul(mul(count, size_of::<T>())?, 16)?, 256) }
fn number(node: &JsonNode) -> bool {
    matches!(node.value(), JsonValue::I32(_) | JsonValue::I64(_) | JsonValue::Integer(_) | JsonValue::F32(_) | JsonValue::F64(_) | JsonValue::Decimal(_))
}
impl Producer<'_> {
    fn lookup(&mut self, reference: JavaText<'_>, space: Option<JavaText<'_>>, fallback: bool, context: &mut ConversionContext) -> Result<SchemaId> {
        let name = Name::new(Some(reference), space, context)?;
        self.native.name(&name)?;
        if let Some(id) = name.full().and_then(|full| self.builder.owner.bindings.get(full)) { return Ok(*id); }
        if fallback {
            let anonymous = Name::new(Some(reference), None, context)?;
            self.native.name(&anonymous)?;
            if let Some(id) = anonymous.full().and_then(|full| self.builder.owner.bindings.get(full)) { return Ok(*id); }
        }
        Err(error("avro-schema", "Undefined canonical schema reference"))
    }
    fn schema(&mut self, raw: &JsonNode, inherited: Option<JavaText<'_>>, context: &mut ConversionContext, depth: usize) -> Result<SchemaId> {
        guard_depth(depth, "Schema assembly exceeds the native traversal depth limit of 128")?;
        self.native.schema(depth)?;
        match raw.value() {
            JsonValue::Text(reference) => {
                if let Some(kind) = primitive(reference.view()) { self.builder.primitive(kind, context) }
                else { self.lookup(reference.view(), inherited, true, context) }
            }
            JsonValue::Array(variants) => {
                self.builder.admit(vector_bytes::<SchemaId>(variants.len())?, context)?;
                let mut parsed = Vec::new();
                parsed.try_reserve_exact(variants.len()).map_err(allocation)?;
                for variant in variants { parsed.push(self.schema(variant, inherited, context, depth + 1)?); }
                self.builder.union_with(0, context, || Ok(parsed))
            }
            JsonValue::Object(_) => self.object(raw, inherited, context, depth),
            _ => Err(error("avro-schema", "Schema is neither textual, an object nor an array")),
        }
    }
    fn object(&mut self, raw: &JsonNode, inherited: Option<JavaText<'_>>, context: &mut ConversionContext, depth: usize) -> Result<SchemaId> {
        let kind = text(raw, "type")?;
        let named = ["record", "error", "enum", "fixed"].iter().any(|name| is(kind, name));
        let doc = if named { raw.get("doc").and_then(JsonNode::as_text).cloned() } else { None };
        let name = if named {
            let space = raw.get("namespace").and_then(JsonNode::as_text).map(Text::view).or(inherited);
            Some(Name::new(Some(text(raw, "name")?.view()), space, context)?)
        } else { None };
        let space = if let Some(name) = &name { namespace(name) } else { inherited };
        if let Some(name) = &name { self.native.name(name)?; }
        let id = if let Some(kind) = primitive(kind.view()) {
            self.builder.primitive(kind, context)?
        } else if is(kind, "record") || is(kind, "error") {
            let id = self.builder.record(name.as_ref().unwrap().clone(), is(kind, "error"), context)?;
            self.builder.named_doc(id, doc.clone())?;
            let fields = array(required(raw, "fields")?)?;
            self.builder.admit(vector_bytes::<Field>(fields.len())?, context)?;
            let mut parsed = Vec::new();
            parsed.try_reserve_exact(fields.len()).map_err(allocation)?;
            for field in fields { parsed.push(self.field(field, space, context, depth + 1)?); }
            self.builder.fields_with(id, 0, context, || Ok(parsed))?;
            id
        } else if is(kind, "enum") {
            let symbols = array(required(raw, "symbols")?)?;
            let name = name.as_ref().unwrap().clone();
            validate_named(&name)?;
            let default = raw.get("default").and_then(JsonNode::as_text).cloned();
            let id = self.builder.enumeration_with(name, default, vector_bytes::<Text>(symbols.len())?, context, || {
                let mut parsed = Vec::new();
                parsed.try_reserve_exact(symbols.len()).map_err(allocation)?;
                for symbol in symbols {
                    let symbol = symbol.as_text().ok_or_else(|| error("avro-schema", "Null enum symbol text"))?;
                    if !valid_simple(symbol.view()) { return Err(error("avro-schema", "Invalid enum symbol")); }
                    if parsed.contains(symbol) { return Err(error("avro-schema", "Duplicate enum symbol")); }
                    parsed.push(symbol.clone());
                }
                Ok(parsed)
            })?;
            self.builder.named_doc(id, doc.clone())?;
            id
        } else if is(kind, "fixed") {
            let JsonValue::I32(size) = required(raw, "size")?.value() else { return Err(error("avro-schema", "Fixed size is not an IntNode")); };
            let id = self.builder.fixed(name.as_ref().unwrap().clone(), *size, self.policy.fixed_bytes, context)?;
            self.builder.named_doc(id, doc.clone())?;
            id
        } else if is(kind, "array") {
            let element = self.schema(required(raw, "items")?, space, context, depth + 1)?;
            self.builder.array(element, context)?
        } else if is(kind, "map") {
            let value = self.schema(required(raw, "values")?, space, context, depth + 1)?;
            self.builder.map(value, context)?
        } else {
            return self.lookup(kind.view(), inherited, false, context);
        };
        let properties = self.properties(raw, false, is(kind, "enum"), context)?;
        self.builder.properties(id, properties)?;
        self.builder.construct_logical(id, &mut *self.logical, context)?;
        if let Some(name) = name {
            let aliases = self.aliases(raw, context)?;
            let raw_aliases = raw.get("aliases").map(array).transpose()?.unwrap_or(&[]);
            let bytes = vector_bytes::<Name>(aliases.len())?;
            self.builder.admit(bytes, context)?;
            let mut parsed = Vec::new();
            parsed.try_reserve_exact(aliases.len()).map_err(allocation)?;
            for alias in aliases {
                let occurrences = raw_aliases.iter().filter(|raw| raw.as_text() == Some(&alias)).count();
                let alias = Name::new(Some(alias.view()), namespace(&name), context)?;
                self.native.name_occurrences(&alias, occurrences)?;
                if !parsed.contains(&alias) { parsed.push(alias); }
            }
            self.builder.aliases_with(id, 0, context, || Ok(parsed))?;
        }
        Ok(id)
    }
    fn field(&mut self, raw: &JsonNode, space: Option<JavaText<'_>>, context: &mut ConversionContext, depth: usize) -> Result<Field> {
        let name = text(raw, "name")?;
        let doc = raw.get("doc").and_then(JsonNode::as_text).cloned();
        let kind = required(raw, "type")?;
        if let Some(reference) = kind.as_text() {
            if primitive(reference.view()).is_none() { self.lookup(reference.view(), space, true, context)?; }
        }
        let schema = self.schema(kind, space, context, depth)?;
        let order = if let Some(order) = raw.get("order") {
            let order = order.as_text().ok_or_else(|| error("avro-schema-unchecked", "Field order has null text"))?;
            if order_is(order, "ASCENDING") { FieldOrder::Ascending }
            else if order_is(order, "DESCENDING") { FieldOrder::Descending }
            else if order_is(order, "IGNORE") { FieldOrder::Ignore }
            else { return Err(error("avro-schema-unchecked", "Unknown field order")); }
        } else { FieldOrder::Ascending };
        let mut default = raw.get("default").cloned();
        if matches!(source_type(&self.builder.owner.nodes[schema.index].kind), Type::Float | Type::Double) {
            if let Some(text) = default.as_ref().and_then(JsonNode::as_text) {
                let value = input::double(text.view(), context)?;
                default = Some(JsonNode::with(0, context, |_| Ok(JsonValue::F64(value)))?);
            }
        }
        if !valid_simple(name.view()) { return Err(error("avro-schema", "Invalid field name")); }
        if self.policy.validate_defaults {
            if let Some(value) = &default {
                if !self.valid_default(schema, value, depth + 1)? { return Err(error("avro-schema", "Invalid Avro field default")); }
            }
        }
        let field = Field::new(name.clone(), schema, default, order)?;
        let properties = self.properties(raw, true, false, context)?;
        let aliases = self.aliases(raw, context)?;
        Ok(field.properties(properties).annotations(doc, aliases))
    }
    fn aliases(&mut self, raw: &JsonNode, context: &mut ConversionContext) -> Result<Vec<Text>> {
        let Some(raw) = raw.get("aliases") else { return Ok(Vec::new()); };
        let raw = array(raw)?;
        self.builder.admit(vector_bytes::<Text>(raw.len())?, context)?;
        let mut aliases = Vec::new();
        aliases.try_reserve_exact(raw.len()).map_err(allocation)?;
        for alias in raw {
            let alias = alias.as_text().ok_or_else(|| error("avro-schema", "Alias is not textual"))?;
            if !aliases.contains(alias) { aliases.push(alias.clone()); }
        }
        Ok(aliases)
    }
    fn properties(&self, raw: &JsonNode, field: bool, enumeration: bool, context: &mut ConversionContext) -> Result<Properties> {
        let JsonValue::Object(members) = raw.value() else { return Err(error("avro-schema", "Schema property source is not an object")); };
        let keep = |key: &Text| !(if field { FIELD_RESERVED } else { SCHEMA_RESERVED }).iter().any(|reserved| is(key, reserved)) && !(enumeration && is(key, "default"));
        let count = members.iter().filter(|(key, _)| keep(key)).count();
        let values = Members::with(vector_bytes::<(Text, JsonNode)>(count)?, context, || {
            let mut values = Vec::new();
            values.try_reserve_exact(count).map_err(allocation)?;
            for (key, value) in members.iter().filter(|(key, _)| keep(key)) { values.push((key.clone(), value.clone())); }
            Ok(values)
        })?;
        Properties::from_members(values, context)
    }
    fn valid_default(&self, schema: SchemaId, value: &JsonNode, depth: usize) -> Result<bool> {
        guard_depth(depth, "Default validation exceeds the native traversal depth limit of 128")?;
        Ok(match &self.builder.owner.nodes[schema.index].kind {
            Kind::Primitive(Type::String | Type::Bytes) | Kind::Enum { .. } | Kind::Fixed { .. } => value.as_text().is_some(),
            Kind::Primitive(Type::Int) => value.integral_long().is_some_and(|value| i32::try_from(value).is_ok()),
            Kind::Primitive(Type::Long) => value.integral_long().is_some(),
            Kind::Primitive(Type::Float | Type::Double) => number(value),
            Kind::Primitive(Type::Boolean) => matches!(value.value(), JsonValue::Bool(_)),
            Kind::Primitive(Type::Null) => matches!(value.value(), JsonValue::Null),
            Kind::Array(child) => match value.value() {
                JsonValue::Array(values) => { for value in values { if !self.valid_default(*child, value, depth + 1)? { return Ok(false); } } true },
                _ => false,
            },
            Kind::Map(child) => match value.value() {
                JsonValue::Object(values) => { for (_, value) in values.iter() { if !self.valid_default(*child, value, depth + 1)? { return Ok(false); } } true },
                _ => false,
            },
            Kind::Union(variants) => self.valid_default(*variants.first().ok_or_else(|| error("avro-schema-unchecked", "Empty union default validation"))?, value, depth + 1)?,
            Kind::Record { fields, .. } => {
                if !matches!(value.value(), JsonValue::Object(_)) { return Ok(false); }
                let fields = fields.as_ref().ok_or_else(|| error("avro-schema-unchecked", "Default validation demanded unset record fields"))?;
                for field in fields {
                    let selected = match value.value() { JsonValue::Object(values) => values.get(&field.name), _ => unreachable!() }.or(field.default.as_ref());
                    let Some(selected) = selected else { return Ok(false); };
                    if !self.valid_default(field.schema, selected, depth + 1)? { return Ok(false); }
                }
                true
            }
            Kind::Primitive(_) => unreachable!(),
        })
    }
}

#[cfg(test)]
mod tests;
