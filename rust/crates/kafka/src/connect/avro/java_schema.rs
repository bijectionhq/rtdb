use std::{
    collections::BTreeMap,
    mem::size_of,
    sync::{Arc, atomic::{AtomicI32, AtomicU64, Ordering}},
};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Anomaly, Result, java_text::JavaText};
use super::{error, super::ConversionContext};

mod identity;
mod text;
mod names;
mod json;
mod properties;
mod property_order;
mod evaluation;
mod context;
pub(super) use text::Text;
pub(super) use names::Name;
pub(super) use json::{JsonNode, JsonValue, Members};
pub(super) use properties::Properties;
pub(super) use evaluation::{Evaluation, RecursionState, Witness};
pub(super) use context::{CycleContext, CycleReference};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SchemaId { region: u64, index: usize }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Type { Record, Enum, Array, Map, Union, Fixed, String, Bytes, Int, Long, Float, Double, Boolean, Null }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FieldOrder { Ascending, Descending, Ignore }
/// Supplies the effective source bytes limit after property parsing; DEFAULT covers an absent or invalid property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FixedBytesLimit { effective: i32 }

pub(super) struct SchemasBuilder { owner: Schemas }
struct Schemas {
    region: u64,
    nodes: Vec<Node>,
    bindings: BTreeMap<Text, SchemaId>,
    charges: Vec<Arc<MemoryReservation>>,
}
#[derive(Clone)]
pub(super) struct SchemaHandle { owner: Arc<Schemas>, id: SchemaId }
#[derive(Clone, Copy)]
pub(super) struct SchemaView<'a> { owner: &'a Schemas, id: SchemaId }
struct Node {
    kind: Kind,
    properties: Properties,
    doc: Option<Text>,
    aliases: Vec<Name>,
    hash: AtomicI32,
}
enum Kind {
    Primitive(Type),
    Record { name: Name, fields: Option<Vec<Field>>, is_error: bool },
    Enum { name: Name, symbols: Vec<Text>, default: Option<Text> },
    Array(SchemaId),
    Map(SchemaId),
    Union(Vec<SchemaId>),
    Fixed { name: Name, size: i32 },
}
pub(super) struct Field {
    name: Text,
    schema: SchemaId,
    default: Option<JsonNode>,
    order: FieldOrder,
    properties: Properties,
    doc: Option<Text>,
    aliases: Vec<Text>,
}
#[derive(Default)]
struct RetainSeen<'a> { arenas: Vec<&'a Schemas>, json: json::JsonSeen<'a> }

const NO_HASHCODE: i32 = i32::MIN;
const MAX_ARRAY_VM_LIMIT: i32 = i32::MAX - 8;
pub(super) const NATIVE_MAX_TRAVERSAL_DEPTH: usize = 128;
static NEXT_REGION: AtomicU64 = AtomicU64::new(1);
fn invalid(message: &'static str) -> Anomaly { error("avro-java-schema", message) }
fn overflow() -> Anomaly { error("allocation-overflow", "Canonical Avro schema storage overflow") }
fn allocation(cause: impl std::fmt::Display) -> Anomaly { error("allocation-failed", cause) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn guard_depth(depth: usize, operation: &'static str) -> Result<()> {
    if depth > NATIVE_MAX_TRAVERSAL_DEPTH {
        Err(error("avro-native-traversal-limit", operation))
    } else {
        Ok(())
    }
}
fn growth<T>(values: &Vec<T>) -> Result<Option<(usize, usize)>> {
    if values.len() < values.capacity() { return Ok(None); }
    let next = if values.capacity() == 0 { 4 } else { mul(values.capacity(), 2)? };
    Ok(Some((next, add(mul(mul(next, size_of::<T>())?, 16)?, 256)?)))
}
fn grow<T>(values: &mut Vec<T>, next: Option<(usize, usize)>) -> Result<()> {
    if let Some((next, _)) = next {
        values.try_reserve_exact(next - values.len()).map_err(allocation)?;
    }
    Ok(())
}
fn source_type(kind: &Kind) -> Type {
    match kind {
        Kind::Primitive(kind) => *kind,
        Kind::Record { .. } => Type::Record,
        Kind::Enum { .. } => Type::Enum,
        Kind::Array(_) => Type::Array,
        Kind::Map(_) => Type::Map,
        Kind::Union(_) => Type::Union,
        Kind::Fixed { .. } => Type::Fixed,
    }
}
fn named(kind: &Kind) -> Option<&Name> {
    match kind {
        Kind::Record { name, .. } | Kind::Enum { name, .. } | Kind::Fixed { name, .. } => Some(name),
        _ => None,
    }
}
fn type_name(kind: Type) -> &'static str {
    match kind {
        Type::Record => "record", Type::Enum => "enum", Type::Array => "array", Type::Map => "map",
        Type::Union => "union", Type::Fixed => "fixed", Type::String => "string", Type::Bytes => "bytes",
        Type::Int => "int", Type::Long => "long", Type::Float => "float", Type::Double => "double",
        Type::Boolean => "boolean", Type::Null => "null",
    }
}
fn full_name(kind: &Kind) -> Option<JavaText<'_>> {
    match named(kind) {
        Some(name) => name.full().map(Text::view),
        None => Some(JavaText::from_utf8(type_name(source_type(kind)))),
    }
}
fn valid_simple(text: JavaText<'_>) -> bool {
    let mut units = text.units();
    units.next().is_some_and(|unit| super::names::unit_flags(unit) & 1 != 0)
        && units.all(|unit| super::names::unit_flags(unit) & 2 != 0)
}
fn primitive(text: JavaText<'_>) -> Option<Type> {
    [
        ("string", Type::String), ("bytes", Type::Bytes), ("int", Type::Int), ("long", Type::Long),
        ("float", Type::Float), ("double", Type::Double), ("boolean", Type::Boolean), ("null", Type::Null),
    ].into_iter().find_map(|(name, kind)| text.units().eq(name.encode_utf16()).then_some(kind))
}
fn validate_named(name: &Name) -> Result<()> {
    if name.full().is_some_and(|name| primitive(name.view()).is_some()) {
        return Err(invalid("Avro schemas may not be named after primitives"));
    }
    Ok(())
}

impl FixedBytesLimit {
    pub(super) const DEFAULT: Self = Self { effective: MAX_ARRAY_VM_LIMIT };
    pub(super) fn effective(value: i32) -> Self { Self { effective: value } }
    fn check(self, size: i32) -> Result<()> {
        if size < 0 {
            return Err(error("avro-schema-unchecked", "Malformed fixed data length is negative"));
        }
        if size > MAX_ARRAY_VM_LIMIT {
            return Err(error("avro-schema-unsupported", "Fixed size exceeds the source VM array limit"));
        }
        if size > self.effective {
            return Err(error("avro-schema-system-limit", "Fixed size exceeds the effective source bytes limit"));
        }
        Ok(())
    }
}

impl Field {
    pub(super) fn new(name: Text, schema: SchemaId, default: Option<JsonNode>, order: FieldOrder) -> Result<Self> {
        if !valid_simple(name.view()) { return Err(invalid("Invalid Avro field name")); }
        Ok(Self { name, schema, default, order, properties: Properties::empty(), doc: None, aliases: Vec::new() })
    }
    pub(super) fn properties(mut self, properties: Properties) -> Self { self.properties = properties; self }
    pub(super) fn annotations(mut self, doc: Option<Text>, aliases: Vec<Text>) -> Self {
        self.doc = doc;
        self.aliases = aliases;
        self
    }
}
impl SchemasBuilder {
    pub(super) fn new(context: &mut ConversionContext) -> Result<Self> {
        let region = NEXT_REGION.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| next.checked_add(1))
            .map_err(|_| overflow())?;
        let charge = context.reserve(4096)?;
        Ok(Self { owner: Schemas { region, nodes: Vec::new(), bindings: BTreeMap::new(), charges: vec![charge] } })
    }
    fn admit(&mut self, bytes: usize, context: &mut ConversionContext) -> Result<()> {
        let next = growth(&self.owner.charges)?;
        let charge = context.reserve(add(bytes, next.map_or(0, |(_, bytes)| bytes))?)?;
        grow(&mut self.owner.charges, next)?;
        self.owner.charges.push(charge);
        Ok(())
    }
    fn require(&self, id: SchemaId) -> Result<()> { self.owner.require(id) }
    fn insert(&mut self, kind: Kind, context: &mut ConversionContext) -> Result<SchemaId> {
        if let Some(name) = named(&kind) { validate_named(name)?; }
        let full = named(&kind).and_then(Name::full).cloned();
        if full.as_ref().is_some_and(|name| self.owner.bindings.contains_key(name)) {
            return Err(invalid("An Avro declaration redefines its canonical full name"));
        }
        let next = growth(&self.owner.nodes)?;
        self.admit(add(next.map_or(0, |(_, bytes)| bytes), if full.is_some() { 1024 } else { 0 })?, context)?;
        grow(&mut self.owner.nodes, next)?;
        let id = SchemaId { region: self.owner.region, index: self.owner.nodes.len() };
        self.owner.nodes.push(Node { kind, properties: Properties::empty(), doc: None, aliases: Vec::new(), hash: AtomicI32::new(NO_HASHCODE) });
        if let Some(full) = full { self.owner.bindings.insert(full, id); }
        Ok(id)
    }
    pub(super) fn primitive(&mut self, kind: Type, context: &mut ConversionContext) -> Result<SchemaId> {
        if matches!(kind, Type::Record | Type::Enum | Type::Array | Type::Map | Type::Union | Type::Fixed) {
            return Err(invalid("Composite Avro types need their source constructor"));
        }
        self.insert(Kind::Primitive(kind), context)
    }
    pub(super) fn record(&mut self, name: Name, is_error: bool, context: &mut ConversionContext) -> Result<SchemaId> {
        self.insert(Kind::Record { name, fields: None, is_error }, context)
    }
    pub(super) fn fields_with(&mut self, record: SchemaId, field_and_payload_bytes: usize, context: &mut ConversionContext, build: impl FnOnce() -> Result<Vec<Field>>) -> Result<()> {
        self.require(record)?;
        if !matches!(&self.owner.nodes[record.index].kind, Kind::Record { fields: None, .. }) {
            return Err(invalid("Avro record fields are already set or the object is not a record"));
        }
        self.admit(field_and_payload_bytes, context)?;
        let fields = build()?;
        for (index, field) in fields.iter().enumerate() {
            self.require(field.schema)?;
            if fields[..index].iter().any(|old| old.name == field.name) { return Err(invalid("Duplicate Avro record field")); }
        }
        let Kind::Record { fields: stored, .. } = &mut self.owner.nodes[record.index].kind else { unreachable!() };
        *stored = Some(fields);
        self.owner.nodes[record.index].hash.store(NO_HASHCODE, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn array(&mut self, element: SchemaId, context: &mut ConversionContext) -> Result<SchemaId> {
        self.require(element)?;
        self.insert(Kind::Array(element), context)
    }
    pub(super) fn map(&mut self, element: SchemaId, context: &mut ConversionContext) -> Result<SchemaId> {
        self.require(element)?;
        self.insert(Kind::Map(element), context)
    }
    pub(super) fn union_with(&mut self, bound: usize, context: &mut ConversionContext, build: impl FnOnce() -> Result<Vec<SchemaId>>) -> Result<SchemaId> {
        self.admit(bound, context)?;
        let variants = build()?;
        for (index, id) in variants.iter().enumerate() {
            self.require(*id)?;
            let kind = &self.owner.nodes[id.index].kind;
            if matches!(kind, Kind::Union(_)) { return Err(invalid("Nested Avro union")); }
            let name = full_name(kind).ok_or_else(|| invalid("Nameless Avro union branch"))?;
            for old in &variants[..index] {
                if full_name(&self.owner.nodes[old.index].kind).is_some_and(|old| old.units().eq(name.units())) {
                    return Err(invalid("Duplicate Avro union branch name"));
                }
            }
        }
        self.insert(Kind::Union(variants), context)
    }
    pub(super) fn fixed(&mut self, name: Name, size: i32, bytes_limit: FixedBytesLimit, context: &mut ConversionContext) -> Result<SchemaId> {
        validate_named(&name)?;
        bytes_limit.check(size)?;
        self.insert(Kind::Fixed { name, size }, context)
    }
    pub(super) fn enumeration_with(&mut self, name: Name, default: Option<Text>, bound: usize, context: &mut ConversionContext, build: impl FnOnce() -> Result<Vec<Text>>) -> Result<SchemaId> {
        self.admit(bound, context)?;
        let symbols = build()?;
        for (index, symbol) in symbols.iter().enumerate() {
            if !valid_simple(symbol.view()) { return Err(invalid("Invalid Avro enum symbol")); }
            if symbols[..index].contains(symbol) { return Err(invalid("Duplicate Avro enum symbol")); }
        }
        if default.as_ref().is_some_and(|value| !symbols.contains(value)) { return Err(invalid("Avro enum default is not a symbol")); }
        self.insert(Kind::Enum { name, symbols, default }, context)
    }
    pub(super) fn properties(&mut self, id: SchemaId, properties: Properties) -> Result<()> {
        self.require(id)?;
        if matches!(&self.owner.nodes[id.index].kind, Kind::Union(_)) { return Err(invalid("Avro union properties cannot be installed")); }
        self.owner.nodes[id.index].properties = properties;
        self.owner.nodes[id.index].hash.store(NO_HASHCODE, Ordering::Relaxed);
        Ok(())
    }
    pub(super) fn annotations_with(&mut self, id: SchemaId, doc: Option<Text>, bound: usize, context: &mut ConversionContext, build: impl FnOnce() -> Result<Vec<Name>>) -> Result<()> {
        self.require(id)?;
        if named(&self.owner.nodes[id.index].kind).is_none() { return Err(invalid("Unnamed Avro type cannot receive named annotations")); }
        self.admit(bound, context)?;
        self.owner.nodes[id.index].aliases = build()?;
        self.owner.nodes[id.index].doc = doc;
        Ok(())
    }
    pub(super) fn reference(&mut self, name: JavaText<'_>, namespace: Option<JavaText<'_>>, context: &mut ConversionContext) -> Result<SchemaId> {
        if let Some(kind) = primitive(name) { return self.primitive(kind, context); }
        let resolved = Name::new(Some(name), namespace, context)?;
        if let Some(id) = resolved.full().and_then(|full| self.owner.bindings.get(full)) { return Ok(*id); }
        let fallback = Name::new(Some(name), None, context)?;
        fallback.full().and_then(|full| self.owner.bindings.get(full)).copied()
            .ok_or_else(|| invalid("Unknown canonical Avro reference"))
    }
    pub(super) fn finish(mut self, root: SchemaId, context: &mut ConversionContext) -> Result<SchemaHandle> {
        self.require(root)?;
        if self.owner.nodes.iter().any(|node| matches!(&node.kind, Kind::Record { fields: None, .. })) {
            return Err(invalid("An unfinished Avro record cannot be published"));
        }
        self.admit(4096, context)?;
        Ok(SchemaHandle { owner: Arc::new(self.owner), id: root })
    }
}
impl Schemas {
    fn require(&self, id: SchemaId) -> Result<()> {
        if id.region == self.region && id.index < self.nodes.len() { Ok(()) }
        else { Err(invalid("Avro schema ID is outside its canonical owner")) }
    }
}
impl SchemaHandle {
    pub(super) fn at(&self, id: SchemaId) -> Result<Self> {
        self.owner.require(id)?;
        Ok(Self { owner: Arc::clone(&self.owner), id })
    }
    pub(super) fn id(&self) -> SchemaId { self.id }
    pub(super) fn view(&self) -> SchemaView<'_> { SchemaView { owner: &self.owner, id: self.id } }
    pub(super) fn same_object(&self, other: &Self) -> bool { self.id == other.id && Arc::ptr_eq(&self.owner, &other.owner) }
    pub(super) fn is_record(&self) -> bool { matches!(&self.owner.nodes[self.id.index].kind, Kind::Record { .. }) }
    /// JSON/default traversal can return avro-native-traversal-limit beyond the native traversal depth limit of 128.
    pub(super) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        self.retain_with(context, &mut RetainSeen::default())
    }
    fn retain_with<'a>(&'a self, context: &mut ConversionContext, seen: &mut RetainSeen<'a>) -> Result<()> {
        if seen.arenas.iter().any(|owner| std::ptr::eq(*owner, self.owner.as_ref())) { return Ok(()); }
        let next = growth(&seen.arenas)?;
        if let Some((_, bytes)) = next { context.reserve(bytes)?; }
        grow(&mut seen.arenas, next)?;
        seen.arenas.push(self.owner.as_ref());
        for charge in &self.owner.charges { context.retain(Arc::clone(charge))?; }
        for node in &self.owner.nodes {
            if let Some(name) = named(&node.kind) { name.retain(context)?; }
            if let Some(doc) = &node.doc { doc.retain(context)?; }
            for alias in &node.aliases { alias.retain(context)?; }
            node.properties.retain(context, &mut seen.json)?;
            match &node.kind {
                Kind::Record { fields: Some(fields), .. } => for field in fields {
                    field.name.retain(context)?;
                    if let Some(value) = &field.default { value.retain(context, &mut seen.json, 0)?; }
                    field.properties.retain(context, &mut seen.json)?;
                    if let Some(doc) = &field.doc { doc.retain(context)?; }
                    for alias in &field.aliases { alias.retain(context)?; }
                },
                Kind::Enum { symbols, default, .. } => {
                    for symbol in symbols { symbol.retain(context)?; }
                    if let Some(default) = default { default.retain(context)?; }
                },
                _ => (),
            }
        }
        Ok(())
    }
    /// Native definitions and this arena must be paired by the same validated source producer.
    /// Equal full names in independently produced documents do not establish source object identity.
    pub(super) fn canonical_native_record(&self, source: &apache_avro::Schema, definitions: &BTreeMap<String, apache_avro::Schema>) -> Result<Option<Self>> {
        let apache_avro::Schema::Record(record) = super::resolve(source, definitions)? else { return Ok(None); };
        let parts = super::names::NameParts::parse(&record.name.name, record.name.namespace.as_deref())
            .map_err(|cause| error("avro-schema", cause))?;
        let found = self.owner.bindings.iter().find_map(|(name, id)| {
            let namespace = (!parts.namespace.is_empty()).then_some(parts.namespace);
            name.view().units().eq(namespace.into_iter().flat_map(str::encode_utf16)
                .chain((!parts.namespace.is_empty()).then_some(46)).chain(parts.simple.encode_utf16())).then_some(*id)
        }).ok_or_else(|| invalid("Native record is absent from its canonical Avro producer"))?;
        let result = self.at(found)?;
        if !result.is_record() { return Err(invalid("Canonical native record selected a non-record object")); }
        Ok(Some(result))
    }
}
impl<'a> SchemaView<'a> {
    fn node(self) -> &'a Node { &self.owner.nodes[self.id.index] }
    fn child(self, id: SchemaId) -> Result<SchemaView<'a>> {
        self.owner.require(id)?;
        Ok(SchemaView { owner: self.owner, id })
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod test_support;
