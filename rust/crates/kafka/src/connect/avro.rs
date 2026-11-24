use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc};
use apache_avro::Schema;
use rtdb_types::{Anomaly, DecimalValue, Result, Value};
use super::{ConnectData, ConnectSchema, ConversionContext, SchemaKind, error};
mod metadata;
mod graph;
mod union;
mod resolution;
mod migration;
mod datum;
mod output;
mod projection;
#[cfg(test)] mod projection_tests;
mod wire;
use wire::WireDatum;
mod default_json;
mod reader;
mod names;
mod java_schema;
pub(super) use java_schema::{FixedBytesLimit, SourcePolicy};
pub(super) use java_schema::{SchemaStringFactory, InputBuffers, TokenBuffer, TokenRecycler};
mod runtime;
pub(super) use runtime::{AvroSchemaRuntime, ThreadTokenPool};
pub(super) use datum::AvroDatum;
use datum::{PhysicalValue as Avro, AvroMap, MapKey};
pub(super) use resolution::AvroResolution;

pub(super) struct AvroDatumPlan {
    root: Schema,
    names: BTreeMap<String, Schema>,
    metadata: metadata::Metadata,
    defaults: default_json::Defaults,
    unions: graph::UnionOptions,
    reader: reader::ReaderMode,
    schema_bytes: usize,
    source: Option<java_schema::AdmittedSource>,
}
pub(super) struct AvroPlan { datum: Arc<AvroDatumPlan>, branch: Option<u32>, connect: Arc<ConnectSchema>, projection: projection::Projection, charges: Vec<Arc<rtdb_columnar::MemoryReservation>> }
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum AvroOutput { Writer, Reader { branch: Option<u32> } }
impl AvroPlan {
    #[cfg(test)]
    pub fn parse(text: &str, references: &[String]) -> Result<Self> { Self::parse_version(text, references, &BTreeMap::new(), None) }
    #[cfg(test)]
    pub fn parse_version(text: &str, references: &[String], options: &BTreeMap<String, String>, version: Option<i32>) -> Result<Self> {
        Self::parse_version_index(text, references, options, version, &mut 0)
    }
    #[cfg(test)]
    pub fn parse_version_index(text: &str, references: &[String], options: &BTreeMap<String, String>, version: Option<i32>, next_union: &mut i32) -> Result<Self> {
        let datum = Arc::new(AvroDatumPlan::parse(text, references, options)?);
        let connect = datum.build_connect(None, version, next_union)?;
        let projection = projection::Projection::compile(&datum.root, &connect, &datum);
        Ok(Self { datum, branch: None, connect, projection, charges: Vec::new() })
    }
    #[cfg(test)]
    pub fn legacy_convert(&self, datum: AvroDatum, next_union: &mut i32, context: &mut ConversionContext) -> Result<ConnectData> {
        self.datum.retain_source(context)?;
        datum.plan.retain_source(context)?;
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        let bytes = resolution::value_bound(&datum.value, 0)?.checked_mul(self.connect.view_allocation_bound().checked_div(4096).unwrap_or(0).checked_add(2).ok_or_else(resolution::overflow)?).ok_or_else(resolution::overflow)?;
        context.reserve(bytes)?;
        let value = if Arc::ptr_eq(&self.datum, &datum.plan) {
            if self.branch.is_some() { let Avro::Union(_, child) = datum.value else { return Err(error("avro-decode", "Selected record output requires union datum")); }; *child } else { datum.value }
        } else {
            context.reserve(datum.plan.schema_bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(resolution::overflow)?)?;
            output::bridge(self.datum.selected_root(self.branch)?, &datum.plan.root, datum.value, &self.connect, &self.datum, &datum.plan, next_union, context, 0)?
        };
        let value = metadata::coerce(convert(self.datum.selected_root(self.branch)?, value, &self.datum.names, self.datum.unions, &self.connect, 0)?, &self.connect)?;
        Ok(ConnectData { schema: Some(Arc::clone(&self.connect)), value })
    }
    pub fn convert(&self, datum: AvroDatum, next_union: &mut i32, context: &mut ConversionContext) -> Result<ConnectData> {
        let mut tape = wire::Tape::default();
        let root = tape.physical(datum.value);
        self.convert_wire(WireDatum { plan: datum.plan, tape, root }, next_union, context)
    }

    pub fn convert_wire(&self, mut datum: WireDatum, next_union: &mut i32, context: &mut ConversionContext) -> Result<ConnectData> {
        self.datum.retain_source(context)?;
        datum.plan.retain_source(context)?;
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        let bytes = datum.tape.bound(datum.root, 0)?.checked_mul(self.connect.view_allocation_bound().checked_div(4096).unwrap_or(0).checked_add(2).ok_or_else(resolution::overflow)?).ok_or_else(resolution::overflow)?;
        context.reserve(bytes)?;
        let root = if !Arc::ptr_eq(&self.datum, &datum.plan) {
            context.reserve(datum.plan.schema_bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(resolution::overflow)?)?;
            output::retarget(&mut datum.tape, self.datum.selected_root(self.branch)?, &datum.plan.root, datum.root, &self.connect, &self.datum, &datum.plan, next_union, context, 0)?
        } else if self.branch.is_some() {
            let wire::Node::Union(_, child) = datum.tape.node(datum.root) else { return Err(error("avro-decode", "Selected record output requires union datum")); };
            *child
        } else {
            datum.root
        };
        let value = self.projection.project(&mut datum.tape, root)?;
        Ok(ConnectData { schema: Some(Arc::clone(&self.connect)), value })
    }

    #[cfg(test)]
    pub fn decode(&self, bytes: &[u8], context: &mut ConversionContext) -> Result<ConnectData> {
        self.datum.validate_reader(None)?;
        self.convert_wire(self.datum.decode_wire(bytes, context)?, &mut 0, context)
    }
}
/// Java's `Schema.Parser` accepts any reference text, but only named declarations enter the shared name table.
/// The native list parser requires an object per reference, so texts that declare no names are dropped here.
fn naming_references(references: Vec<String>) -> Result<Vec<String>> {
    fn declares_names(value: &serde_json::Value) -> bool {
        match value {
            serde_json::Value::Object(_) => true,
            serde_json::Value::Array(items) => items.iter().any(declares_names),
            _ => false,
        }
    }
    let mut named = Vec::with_capacity(references.len());
    for text in references {
        let value: serde_json::Value = serde_json::from_str(&text).map_err(|cause| error("avro-schema", cause))?;
        match value {
            serde_json::Value::Object(_) => named.push(text),
            serde_json::Value::Array(ref items) if items.iter().any(declares_names) =>
                return Err(Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-avro-reference", "Union references with inline named declarations are not supported")),
            _ => {}
        }
    }
    Ok(named)
}
impl AvroDatumPlan {
    #[cfg(test)]
    pub fn parse_admitted_source(
        text: &str,
        references: &[String],
        options: &BTreeMap<String, String>,
        policy: SourcePolicy,
        context: &mut ConversionContext,
    ) -> Result<Self> {
        java_schema::ParsedPair::parse(text, references, options, policy, context).map(java_schema::ParsedPair::into_datum)
    }
    #[cfg(test)]
    pub(in crate::connect::avro) fn parse_admitted_source_with_logical(
        text: &str,
        references: &[String],
        options: &BTreeMap<String, String>,
        policy: SourcePolicy,
        logical: &mut dyn java_schema::LogicalEnvironment,
        context: &mut ConversionContext,
    ) -> Result<Self> {
        java_schema::ParsedPair::parse_with_logical(text, references, options, policy, logical, context)
            .map(java_schema::ParsedPair::into_datum)
    }
    #[cfg(test)]
    pub fn parse_admitted_java_text(
        text: rtdb_types::java_text::JavaText<'_>,
        references: &[rtdb_types::java_text::JavaText<'_>],
        options: &BTreeMap<String, String>,
        policy: SourcePolicy,
        context: &mut ConversionContext,
    ) -> Result<Self> {
        java_schema::ParsedPair::parse_java_text(text, references, options, policy, context)
            .map(java_schema::ParsedPair::into_datum)
    }
    pub(super) fn retain_source(&self, context: &mut ConversionContext) -> Result<()> {
        if let Some(source) = &self.source { source.retain(context)?; }
        Ok(())
    }
    #[cfg(test)]
    pub fn parse(text: &str, references: &[String], options: &BTreeMap<String, String>) -> Result<Self> {
        Self::parse_with_name_policy(text, references, options, None)
    }
    fn parse_with_name_policy(text: &str, references: &[String], options: &BTreeMap<String, String>, policy: Option<&dyn apache_avro::schema::SchemaNamePolicy>) -> Result<Self> {
        let defaults = default_json::Defaults::parse(text, references)?;
        let metadata_text = default_json::metadata_text(text)?;
        let metadata_references = references.iter().map(|text| default_json::metadata_text(text)).collect::<Result<Vec<_>>>()?;
        let metadata = metadata::Metadata::new(&metadata_text, &metadata_references, options)?;
        let unions = graph::UnionOptions::parse(options)?;
        let reader = reader::ReaderMode::parse(options)?;
        let schema_bytes = references.iter().try_fold(text.len(), |bytes, text| bytes.checked_add(text.len()).ok_or_else(|| error("allocation-overflow", "Avro schema size overflow")))?;
        let text = physical_logical_types(&metadata_text)?;
        let references = metadata_references.iter().map(|text| physical_logical_types(text)).collect::<Result<Vec<_>>>()?;
        let text = without_field_defaults(&text)?;
        let references = references.iter().map(|text| without_field_defaults(text)).collect::<Result<Vec<_>>>()?;
        let references = naming_references(references)?;
        let (mut root, mut references) = match policy {
            None => Schema::parse_str_with_list(&text, &references),
            Some(policy) => Schema::parse_str_with_list_with_name_policy(&text, &references, policy),
        }.map_err(|cause| error("avro-schema", cause))?;
        restore_field_defaults(&mut root, metadata.root(), &metadata);
        for schema in &mut references { restore_field_defaults(schema, metadata.root(), &metadata); }
        let mut names = BTreeMap::new(); collect_names(&root, &mut names);
        for reference in &references { collect_names(reference, &mut names); }
        Ok(Self { root, names, metadata, defaults, unions, reader, schema_bytes, source: None })
    }
    pub fn validate_reader(&self, selected: Option<&Self>) -> Result<()> { self.reader.validate(self, selected) }
    pub fn connect_plan(self: &Arc<Self>, branch: Option<u32>, version: Option<i32>, next_union: &mut i32, context: &mut ConversionContext) -> Result<AvroPlan> {
        self.retain_source(context)?;
        let bytes = self.connect_allocation_bound(branch)?;
        let charge = context.reserve(bytes)?;
        let connect = self.build_connect(branch, version, next_union)?;
        let projection = projection::Projection::compile(self.selected_root(branch)?, &connect, self);
        Ok(AvroPlan { datum: Arc::clone(self), branch, connect, projection, charges: vec![charge] })
    }
    fn selected_root(&self, branch: Option<u32>) -> Result<&Schema> {
        match (resolve(&self.root, &self.names)?, branch) {
            (Schema::Union(union), Some(index)) => union.variants().get(index as usize).ok_or_else(|| error("avro-schema", "Output union branch is absent")),
            (_, None) => Ok(&self.root),
            _ => Err(error("avro-schema", "Output branch requires a union schema")),
        }
    }
    fn build_connect(&self, branch: Option<u32>, version: Option<i32>, next_union: &mut i32) -> Result<Arc<ConnectSchema>> {
        let raw = self.metadata.root();
        let raw = match branch {
            None => raw,
            Some(index) => raw.as_array().or_else(|| raw.get("type").and_then(serde_json::Value::as_array)).and_then(|values| values.get(index as usize)).ok_or_else(|| error("avro-schema", "Output union metadata is absent"))?,
        };
        graph::build_at(self.selected_root(branch)?, raw, &self.names, &self.metadata, &self.defaults, self.unions, version, next_union)
    }
    fn connect_allocation_bound(&self, branch: Option<u32>) -> Result<usize> {
        let defaults = resolution::default_allocation_bound(self.selected_root(branch)?, &self.names, &self.defaults)?;
        self.schema_bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(65_536)).and_then(|bytes| bytes.checked_add(defaults)).ok_or_else(resolution::overflow)
    }
    pub fn decode_datum(self: &Arc<Self>, bytes: &[u8], context: &mut ConversionContext) -> Result<AvroDatum> {
        self.retain_source(context)?;
        if self.raw_bytes() {
            context.reserve(bytes.len().checked_mul(4).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Avro byte value allocation overflow"))?)?;
            return Ok(AvroDatum { value: Avro::Bytes(bytes.to_vec()), plan: Arc::clone(self) });
        }
        let mut preflight = Preflight { bytes, offset: 0, window: BinaryWindow::new(bytes.len()), allocated: 4096, limit: context.allocation_limit(), names: &self.names, view_bytes: 0 };
        preflight.value(&self.root, 0)?;
        context.reserve(preflight.allocated)?;
        let value = datum::decode(self, bytes)?;
        Ok(AvroDatum { value, plan: Arc::clone(self) })
    }
    pub fn root_record(&self) -> bool { matches!(resolve(&self.root, &self.names), Ok(Schema::Record(_))) }
    pub(super) fn subject_record_name(&self, context: &mut ConversionContext) -> Result<Option<String>> {
        let Schema::Record(record) = resolve(&self.root, &self.names)? else { return Ok(None); };
        let name = names::NameParts::parse(&record.name.name, record.name.namespace.as_deref())
            .map_err(|cause| error("avro-schema", cause))?;
        context.reserve(name.fullname_bytes()?.checked_add(256).ok_or_else(resolution::overflow)?)?;
        name.fullname().map(Some)
    }
    pub fn raw_bytes(&self) -> bool { match &self.root { Schema::Bytes => true, Schema::Decimal(decimal) => matches!(decimal.inner.as_ref(), Schema::Bytes), _ => false } }
    pub fn select_output(&self, reader: &Self, datum: &AvroDatum) -> Result<AvroOutput> {
        if !self.root_record() { return Ok(AvroOutput::Writer); }
        let branch = if let Avro::Union(index, _) = &datum.value { Some(*index) } else { None };
        if !matches!(resolve(reader.selected_root(branch)?, &reader.names)?, Schema::Record(_)) { return Err(error("avro-decode", "Record writer resolved to a non-record output")); }
        Ok(AvroOutput::Reader { branch })
    }
    pub fn prepare_wire_output(&self, reader: &Self, datum: &WireDatum, context: &mut ConversionContext) -> Result<AvroOutput> {
        self.retain_source(context)?;
        reader.retain_source(context)?;
        datum.plan.retain_source(context)?;
        if !self.root_record() { return Ok(AvroOutput::Writer); }
        let branch = match datum.tape.node(datum.root) { wire::Node::Union(tag, _) => Some(*tag), _ => None };
        if !matches!(resolve(reader.selected_root(branch)?, &reader.names)?, Schema::Record(_)) {
            return Err(error("avro-decode", "Record writer resolved to a non-record output"));
        }
        Ok(AvroOutput::Reader { branch })
    }
    pub fn prepare_output(&self, reader: &Self, datum: AvroDatum, context: &mut ConversionContext) -> Result<(AvroOutput, AvroDatum)> {
        self.retain_source(context)?;
        reader.retain_source(context)?;
        datum.plan.retain_source(context)?;
        let output = self.select_output(reader, &datum)?;
        Ok((output, datum))
    }
    pub fn wire_result(self: &Arc<Self>, mut datum: WireDatum, context: &mut ConversionContext) -> Result<WireDatum> {
        datum.string_result(self, context)?;
        Ok(datum)
    }
    pub fn binary_result(self: &Arc<Self>, mut datum: AvroDatum, context: &mut ConversionContext) -> Result<AvroDatum> {
        self.retain_source(context)?;
        datum.plan.retain_source(context)?;
        if matches!(resolve(&self.root, &self.names)?, Schema::String) {
            let mut value = datum.value;
            while let Avro::Union(_, child) = value { value = *child; }
            value = match value {
                Avro::String(value) => Avro::String(value),
                Avro::Utf8(value) => { context.reserve(value.len().checked_mul(6).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(resolution::overflow)?)?; Avro::String(datum::java_utf8(&value)) },
                Avro::Bytes(value) => { context.reserve(4096)?; Avro::String(format!("java.nio.HeapByteBuffer[pos=0 lim={} cap={}]", value.len(), value.len())) },
                _ => return Err(error("avro-decode", "String writer resolved to an invalid output")),
            };
            datum.value = value; datum.plan = Arc::clone(self);
        }
        Ok(datum)
    }
}
fn without_field_defaults(text: &str) -> Result<String> {
    fn strip(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(fields) => {
                if fields.get("type").and_then(serde_json::Value::as_str) == Some("record") {
                    if let Some(fields) = fields.get_mut("fields").and_then(serde_json::Value::as_array_mut) { for field in fields { if let Some(field) = field.as_object_mut() { field.remove("default"); } } }
                }
                for value in fields.values_mut() { strip(value); }
            }
            serde_json::Value::Array(values) => for value in values { strip(value); },
            _ => {}
        }
    }
    let mut value = serde_json::from_str(text).map_err(|cause| error("avro-schema", cause))?;
    strip(&mut value); serde_json::to_string(&value).map_err(|cause| error("avro-schema", cause))
}
fn restore_field_defaults(schema: &mut Schema, raw: &serde_json::Value, metadata: &metadata::Metadata) {
    match schema {
        Schema::Record(record) => {
            let raw = metadata.named(&record.name.fullname(None)).unwrap_or(raw);
            for field in &mut record.fields {
                if let Some(raw) = raw.get("fields").and_then(serde_json::Value::as_array).and_then(|fields| fields.iter().find(|raw| raw.get("name").and_then(serde_json::Value::as_str) == Some(field.name.as_str()))) {
                    field.default = raw.get("default").cloned();
                    restore_field_defaults(&mut field.schema, &raw["type"], metadata);
                }
            }
        }
        Schema::Array(array) => restore_field_defaults(&mut array.items, &raw["items"], metadata),
        Schema::Map(map) => restore_field_defaults(&mut map.types, &raw["values"], metadata),
        Schema::Union(union) => {
            let raw = raw.as_array().or_else(|| raw.get("type").and_then(serde_json::Value::as_array));
            if let Some(raw) = raw {
                let variants = union.variants().iter().cloned().zip(raw).map(|(mut schema, raw)| { restore_field_defaults(&mut schema, raw, metadata); schema }).collect();
                *union = apache_avro::schema::UnionSchema::new(variants).expect("preserved union member identities");
            }
        }
        _ => {}
    }
}
fn collect_names(schema: &Schema, names: &mut BTreeMap<String, Schema>) {
    match schema {
        Schema::Record(record) => { names.insert(record.name.fullname(None), schema.clone()); for field in &record.fields { collect_names(&field.schema, names); } }
        Schema::Enum(value) => { names.insert(value.name.fullname(None), schema.clone()); }
        Schema::Fixed(value) => { names.insert(value.name.fullname(None), schema.clone()); }
        Schema::Array(value) => collect_names(&value.items, names), Schema::Map(value) => collect_names(&value.types, names),
        Schema::Union(value) => for schema in value.variants() { collect_names(schema, names); },
        Schema::Decimal(value) => { if let Schema::Fixed(fixed) = value.inner.as_ref() { names.insert(fixed.name.fullname(None), schema.clone()); } else { collect_names(&value.inner, names); } }, _ => (),
    }
}
fn resolve<'a>(schema: &'a Schema, names: &'a BTreeMap<String, Schema>) -> Result<&'a Schema> {
    let Schema::Ref { name } = schema else { return Ok(schema); };
    names.iter().find(|(fullname, _)| {
        if !name.name.contains('.') {
            if let Some(namespace) = name.namespace.as_deref().filter(|namespace| !namespace.is_empty()) {
                return namespace.bytes().chain(std::iter::once(b'.')).chain(name.name.bytes()).eq(fullname.bytes());
            }
        }
        fullname.as_str() == name.name.as_str()
    }).map(|(_, schema)| schema).ok_or_else(|| error("avro-schema", format!("Unknown schema reference '{}'", name.fullname(None))))
}
#[cfg(test)] mod resolve_tests;
fn member_name(schema: &Schema) -> String {
    match schema {
        Schema::Record(value) => return value.name.name.clone(), Schema::Enum(value) => return value.name.name.clone(), Schema::Fixed(value) => return value.name.name.clone(),
        Schema::Null => "null", Schema::Boolean => "boolean", Schema::Int | Schema::Date | Schema::TimeMillis => "int",
        Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => "long",
        Schema::Float => "float", Schema::Double => "double", Schema::String | Schema::Uuid => "string", Schema::Bytes | Schema::BigDecimal => "bytes",
        Schema::Decimal(value) => return member_name(&value.inner),
        Schema::Array(_) => "array", Schema::Map(_) => "map", Schema::Union(_) => "union", Schema::Duration => "fixed", Schema::Ref { name } => return name.name.clone(),
    }.to_owned()
}
fn physical_logical_types(text: &str) -> Result<String> {
    fn normalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Array(values) => for value in values { normalize(value); },
            serde_json::Value::Object(fields) => {
                if let Some(name) = fields.get("logicalType").and_then(serde_json::Value::as_str).map(str::to_ascii_lowercase) {
                    if matches!(name.as_str(), "decimal" | "date" | "time-millis" | "timestamp-millis") { fields.insert("logicalType".into(), name.into()); } else { fields.remove("logicalType"); }
                }
                for value in fields.values_mut() { normalize(value); }
            }
            _ => (),
        }
    }
    let mut value = serde_json::from_str(text).map_err(|cause| error("avro-schema", cause))?; normalize(&mut value);
    serde_json::to_string(&value).map_err(|cause| error("avro-schema", cause))
}
struct BinaryWindow { origin: usize, compacted: bool, rewind_remaining: usize }
impl BinaryWindow {
    fn new(bytes: usize) -> Self { Self { origin: 0, compacted: false, rewind_remaining: bytes } }
}
struct Preflight<'a> { bytes: &'a [u8], offset: usize, window: BinaryWindow, allocated: usize, limit: usize, names: &'a BTreeMap<String, Schema>, view_bytes: usize }
impl Preflight<'_> {
    fn claim(&mut self, bytes: usize) -> Result<()> {
        self.allocated = self.allocated.checked_add(bytes).filter(|bytes| *bytes <= self.limit).ok_or_else(|| error("batch-too-large", "Avro decoded allocation exceeds its limit"))?; Ok(())
    }
    fn take(&mut self, count: usize) -> Result<&[u8]> {
        let end = self.offset.checked_add(count).ok_or_else(|| error("avro-decode", "Avro length overflow"))?;
        let bytes = self.bytes.get(self.offset..end).ok_or_else(|| error("avro-decode", "Truncated Avro datum"))?; self.offset = end; Ok(bytes)
    }
    fn long(&mut self) -> Result<i64> {
        self.compact_for(10);
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = self.take(1)?[0];
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 { return Ok(((value >> 1) as i64) ^ -((value & 1) as i64)); }
        }
        Err(error("avro-decode", "Invalid Avro varint"))
    }
    fn int(&mut self) -> Result<i32> {
        self.compact_for(5);
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let byte = self.take(1)?[0]; value ^= u32::from(byte & 127) << shift;
            if byte & 128 == 0 { return Ok(((value >> 1) as i32) ^ -((value & 1) as i32)); }
        }
        Err(error("avro-decode", "Invalid Avro int encoding"))
    }
    fn compact_for(&mut self, width: usize) {
        if !self.window.compacted && self.bytes.len() - self.offset < width {
            self.window.origin = self.offset; self.window.compacted = true;
        }
    }
    fn number_bytes(&mut self, width: usize) -> Result<&[u8]> { self.compact_for(width); self.take(width) }
    fn bytes(&mut self) -> Result<()> {
        let count = usize::try_from(self.long()?).map_err(|_| error("avro-decode", "Negative Avro byte count"))?;
        self.claim(count.checked_mul(4).ok_or_else(|| error("avro-decode", "Avro byte count overflow"))?)?; self.take(count)?; Ok(())
    }
    fn value(&mut self, schema: &Schema, depth: usize) -> Result<()> {
        if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
        self.claim(4096usize.saturating_add(self.view_bytes))?;
        match resolve(schema, self.names)? {
            Schema::Null => (), Schema::Boolean => { self.take(1)?; }
            Schema::Int | Schema::Date | Schema::TimeMillis => { self.int()?; }
            Schema::Enum(enumeration) => {
                let index = usize::try_from(self.int()?).map_err(|cause| error("avro-decode", cause))?;
                let symbol = enumeration.symbols.get(index).ok_or_else(|| error("avro-decode", "Enum index is out of range"))?;
                self.claim(symbol.len().checked_mul(4).ok_or_else(resolution::overflow)?)?;
            }
            Schema::Long | Schema::TimeMicros | Schema::TimestampMillis | Schema::TimestampMicros | Schema::TimestampNanos | Schema::LocalTimestampMillis | Schema::LocalTimestampMicros | Schema::LocalTimestampNanos => { self.long()?; }
            Schema::Float => { self.number_bytes(4)?; } Schema::Double => { self.number_bytes(8)?; }
            Schema::Bytes | Schema::String | Schema::Uuid | Schema::BigDecimal => self.bytes()?,
            Schema::Fixed(value) => { self.claim(value.size.saturating_mul(4))?; self.take(value.size)?; }
            Schema::Duration => { self.take(12)?; }
            Schema::Decimal(value) => self.value(&value.inner, depth + 1)?,
            Schema::Record(record) => for field in &record.fields { self.claim(field.name.len().saturating_mul(4))?; self.value(&field.schema, depth + 1)?; },
            Schema::Union(union) => {
                self.claim(union.variants().len().checked_mul(self.view_bytes.saturating_add(4096)).ok_or_else(|| error("allocation-overflow", "Avro union view allocation exceeds range"))?)?;
                let index = usize::try_from(self.int()?).map_err(|_| error("avro-decode", "Negative Avro union index"))?;
                let selected = union.variants().get(index).ok_or_else(|| error("avro-decode", "Avro union index is out of range"))?;
                self.value(selected, depth + 1)?;
            }
            Schema::Array(array) => self.collection(&array.items, false, depth + 1)?,
            Schema::Map(map) => self.collection(&map.types, true, depth + 1)?,
            Schema::Ref { .. } => return Err(error("avro-schema", "Unresolved schema reference")),
        }
        Ok(())
    }
    fn collection(&mut self, schema: &Schema, map: bool, depth: usize) -> Result<()> {
        loop {
            let count = datum::read_block(self)?; if count == 0 { return Ok(()); }
            self.claim(count.checked_mul(4096).ok_or_else(|| error("avro-decode", "Avro collection allocation overflow"))?)?;
            for _ in 0..count { if map { self.bytes()?; } self.value(schema, depth)?; }
        }
    }
}
#[cfg(test)]
fn convert(schema: &Schema, value: Avro, names: &BTreeMap<String, Schema>, unions: graph::UnionOptions, connect: &ConnectSchema, depth: usize) -> Result<Value> {
    if depth > 128 { return Err(error("avro-decode", "Avro datum nesting limit exceeded")); }
    let schema = resolve(schema, names)?;
    let connect = connect.resolved();
    if !matches!(schema, Schema::Union(_)) && !matches!(&value, Avro::Null) {
        let valid = match &connect.kind {
            SchemaKind::Bool => matches!(&value, Avro::Boolean(_)),
            SchemaKind::I8 | SchemaKind::I16 | SchemaKind::I32 => matches!(&value, Avro::Int(_)),
            SchemaKind::I64 => matches!(&value, Avro::Long(_)),
            SchemaKind::F32 => matches!(&value, Avro::Float(_)), SchemaKind::F64 => matches!(&value, Avro::Double(_)),
            SchemaKind::String => matches!(&value, Avro::String(_) | Avro::Utf8(_) | Avro::Enum(_, _)),
            SchemaKind::Bytes => matches!(&value, Avro::Bytes(_) | Avro::Fixed(_, _)),
            _ => true,
        };
        if !valid { return Err(error("avro-decode", "Resolved value does not match the selected Connect output type")); }
    }
    let logical_conversion = !matches!(&value, Avro::Null | Avro::Union(_, _));
    let converted = match value {
        Avro::Null => Value::Null, Avro::Boolean(value) => Value::Bool(value),
        Avro::Int(value) => Value::I32(value),
        Avro::Long(value) => Value::I64(value),
        Avro::Float(value) => Value::F32(value), Avro::Double(value) => Value::F64(value), Avro::String(value) | Avro::Enum(_, value) => Value::Utf8(value),
        Avro::Utf8(value) => Value::Utf8(datum::java_utf8(&value)),
        Avro::Bytes(value) | Avro::Fixed(_, value) => Value::Binary(value),
        Avro::PackedArray(packed) => {
            let Schema::Array(array) = schema else { return Err(error("avro-decode", "Array value lacks its schema")); };
            if let SchemaKind::Array(element) = &connect.kind {
                let element = element.resolved();
                if datum::PackedArray::layout(&array.items) == Some((packed.kind, packed.branches))
                    && packed.kind.matches_connect(&element.kind)
                    && !matches!(element.name.as_deref(), Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time"
                        | "org.apache.kafka.connect.data.Timestamp" | "org.apache.kafka.connect.data.Decimal"))
                    && (packed.values.is_empty() || depth + 1 + usize::from(packed.branches.is_some()) <= 128)
                {
                    Value::List(packed.values)
                } else {
                    return convert(schema, Avro::Array(packed.into_physical_values()), names, unions, &connect, depth);
                }
            } else {
                return convert(schema, Avro::Array(packed.into_physical_values()), names, unions, &connect, depth);
            }
        }
        Avro::Array(values) => {
            let Schema::Array(array) = schema else { return Err(error("avro-decode", "Array value lacks its schema")); };
            let element = match &connect.kind { SchemaKind::Array(element) => element.as_ref(), SchemaKind::Map(_, _) => connect.as_ref(), _ => return Err(error("avro-decode", "Array Connect schema is absent")) };
            Value::List(rtdb_types::ListValue::try_from_cells(values.into_iter().map(|value| convert(&array.items, value, names, unions, element, depth + 1)))?)
        }
        Avro::Map(values) => {
            let Schema::Map(map) = schema else { return Err(error("avro-decode", "Map value lacks its schema")); };
            let SchemaKind::Map(_, element) = &connect.kind else { return Err(error("avro-decode", "Map Connect schema is absent")); };
            let entries = values.ordered()?.into_iter().map(|(key, value)| Ok((MapKey::String(key.into_text()), convert(&map.types, value, names, unions, element, depth + 1)?))).collect::<Result<Vec<_>>>()?;
            let order = datum::string_map_order(&entries)?;
            let mut entries = entries.into_iter().map(Some).collect::<Vec<_>>();
            Value::Map { entries: order.into_iter().map(|index| { let (key, value) = entries[index].take().expect("unique converted map key"); (Value::Utf8(key.into_text()), value) }).collect(), sorted: false }
        }
        Avro::Record(values) => {
            let Schema::Record(record) = schema else { return Err(error("avro-decode", "Record value lacks its schema")); };
            Value::Struct(values.into_iter().map(|(name, value)| {
                let field = record.lookup.get(&name).and_then(|index| record.fields.get(*index)).ok_or_else(|| error("avro-decode", "Unknown record field"))?;
                let child = match &connect.kind {
                    SchemaKind::Struct(fields) => fields.iter().find(|field| field.name == name).map(|field| &field.schema),
                    SchemaKind::Map(key, value) => match name.as_str() { "key" => Some(key), "value" => Some(value), _ => None },
                    _ => None,
                }.ok_or_else(|| error("avro-decode", "Record field Connect schema is absent"))?;
                Ok((name, convert(&field.schema, value, names, unions, child, depth + 1)?))
            }).collect::<Result<_>>()?)
        }
        Avro::Union(index, value) => {
            let Schema::Union(union) = schema else { return Err(error("avro-decode", "Union value lacks its schema")); };
            let selected = union.variants().get(index as usize).ok_or_else(|| error("avro-decode", "Invalid union member"))?;
            if matches!(value.as_ref(), Avro::Null) { Value::Null }
            else if (union.variants().len() == 1 && unions.flatten_singleton) || (union.variants().len() == 2 && union.variants().iter().any(|schema| matches!(schema, Schema::Null))) { convert(selected, *value, names, unions, &connect, depth + 1)? }
            else { union::convert(union.variants(), selected, *value, names, unions, &connect, depth + 1)? }
        }
    };
    if logical_conversion {
        if connect.name.as_deref() == Some("org.apache.kafka.connect.data.Decimal") {
            let Value::Binary(bytes) = converted else { return Err(error("avro-decode", "Logical Decimal value has an incompatible primitive representation")); };
            return decimal_value(bytes, &connect);
        }
        let millis = match (connect.name.as_deref(), &connect.kind, &converted) {
            (Some("org.apache.kafka.connect.data.Date"), SchemaKind::I32, Value::I32(value)) => Some(i64::from(*value) * 86_400_000),
            (Some("org.apache.kafka.connect.data.Time"), SchemaKind::I32, Value::I32(value)) => {
                if !(0..=86_400_000).contains(value) { return Err(error("avro-decode", "Time is outside one day")); }
                Some(i64::from(*value))
            }
            (Some("org.apache.kafka.connect.data.Timestamp"), SchemaKind::I64, Value::I64(value)) => Some(*value),
            (Some("org.apache.kafka.connect.data.Date" | "org.apache.kafka.connect.data.Time" | "org.apache.kafka.connect.data.Timestamp"), _, _) => return Err(error("avro-decode", "Logical date value has an incompatible primitive representation")),
            _ => None,
        };
        if let Some(millis) = millis { return Ok(super::data::date_value(millis)); }
    }
    Ok(converted)
}
fn decimal_value(bytes: Vec<u8>, schema: &ConnectSchema) -> Result<Value> {
    decimal_value_with_scale(bytes, schema.parameters.get("scale").map(String::as_str))
}
fn decimal_value_with_scale(bytes: Vec<u8>, scale: Option<&str>) -> Result<Value> {
    if bytes.is_empty() { return Err(error("avro-decode", "Decimal coefficient bytes are empty")); }
    let text = scale.ok_or_else(|| error("avro-decode", "Decimal schema has no scale"))?;
    let (negative, digits) = if let Some(digits) = text.strip_prefix('-') { (true, digits) } else { (false, text.strip_prefix('+').unwrap_or(text)) };
    let invalid = || error("avro-decode", "Decimal scale is not a signed 32-bit integer");
    if digits.is_empty() { return Err(invalid()); }
    let magnitude = digits.chars().try_fold(0i64, |value, digit| {
        let digit = rtdb_types::decimal_digit(digit).filter(|_| digit.len_utf16() == 1).ok_or_else(invalid)?;
        value.checked_mul(10).and_then(|value| value.checked_add(i64::from(digit))).filter(|value| *value <= i64::from(i32::MAX) + i64::from(negative)).ok_or_else(invalid)
    })?;
    decimal_value_at_scale(bytes, if negative { -magnitude } else { magnitude } as i32)
}
fn decimal_value_at_scale(bytes: Vec<u8>, scale: i32) -> Result<Value> {
    if bytes.is_empty() { return Err(error("avro-decode", "Decimal coefficient bytes are empty")); }
    let coefficient = num_bigint::BigInt::from_signed_bytes_be(&bytes);
    let digits = coefficient.to_str_radix(10).trim_start_matches('-').len().max(1);
    let precision = if digits <= 32 { 32 } else if digits <= 64 { 64 } else { return Err(error("avro-decode", "Decimal precision exceeds the reference writer limit")); };
    let width = precision / 2; let bytes = coefficient.to_signed_bytes_be();
    if bytes.len() > width { return Err(error("avro-decode", "Decimal coefficient exceeds its width")); }
    let mut unscaled = vec![if coefficient.sign() == num_bigint::Sign::Minus { 255 } else { 0 }; width]; unscaled[width - bytes.len()..].copy_from_slice(&bytes);
    Ok(Value::Decimal(DecimalValue::new(unscaled, precision as i32, scale, precision as u16 * 4)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn references_that_declare_no_names_parse_like_java_and_inline_union_declarations_are_rejected() {
        let root = r#"{"type":"record","name":"Event","fields":[{"name":"child","type":"Child"}]}"#;
        let child = r#"{"type":"record","name":"Child","fields":[{"name":"value","type":"long"}]}"#.to_string();
        let plan = AvroDatumPlan::parse(root, &["\"long\"".into(), r#"["null","string"]"#.into(), child], &BTreeMap::new()).unwrap();
        assert!(plan.names.contains_key("Child"));
        let inline = r#"["null",{"type":"record","name":"Child","fields":[]}]"#.to_string();
        let Err(failure) = AvroDatumPlan::parse(r#""long""#, &[inline], &BTreeMap::new()) else { panic!("inline union reference parsed") };
        assert_eq!(failure.code, "xtdb.kafka-connect-source/unsupported-avro-reference");
    }
    #[test]
    fn scrub_invalid_names_preserves_ingested_connect_names_and_ascii_fields() {
        let schema = r#"{"type":"record","name":"wire.Event","connect.name":"connect.name-with spaces","fields":[{"name":"field_1","type":"long"},{"name":"nested","type":{"type":"record","name":"Child","connect.name":"1 child","fields":[{"name":"_value","type":"string"}]}}]}"#;
        for scrub in ["false", "true"] {
            let options = BTreeMap::from([("scrub.invalid.names".into(), scrub.into())]);
            let plan = AvroPlan::parse_version(schema, &[], &options, None).unwrap();
            assert_eq!(plan.connect.name.as_deref(), Some("connect.name-with spaces"));
            assert_eq!(plan.connect.field("nested").unwrap().schema.resolved().name.as_deref(), Some("1 child"));
            let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
            assert_eq!(plan.decode(&[14, 2, b'x'], &mut context).unwrap().value, Value::Struct(StructFields::from([
                ("field_1".into(), Value::I64(7)),
                ("nested".into(), Value::Struct(StructFields::from([("_value".into(), Value::Utf8("x".into()))]))),
            ])));
        }
    }
    #[test]
    fn recursive_avro_values_and_detached_field_views_keep_schema_definitions() {
        let plan = AvroPlan::parse(r#"{"type":"record","name":"Node","fields":[{"name":"value","type":"long"},{"name":"next","type":["null","Node"],"default":null}]}"#, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(value) = plan.decode(&[2, 2, 4, 0], &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(value["value"], Value::I64(1));
        assert_eq!(value["next"], Value::Struct(StructFields::from([("value".into(), Value::I64(2)), ("next".into(), Value::Null)])));
        let next = plan.connect.field("next").unwrap().schema; drop(plan);
        assert!(next.optional); assert_eq!(next.field("value").unwrap().schema.resolved().kind, SchemaKind::I64);
    }
    #[test]
    fn enhanced_avro_unions_preserve_namespaces_for_equal_short_names() {
        let schema = r#"[{"type":"record","name":"a.Item","fields":[]},{"type":"record","name":"b.Item","fields":[]}]"#;
        assert!(AvroPlan::parse(schema, &[]).is_err());
        let plan = AvroPlan::parse_version(schema, &[], &BTreeMap::from([("enhanced.avro.schema.support".into(), "true".into())]), None).unwrap();
        assert!(plan.connect.field("a.Item").is_some()); assert!(plan.connect.field("b.Item").is_some());
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&[2], &mut context).unwrap().value, Value::Struct(StructFields::from([("a.Item".into(), Value::Null), ("b.Item".into(), Value::Struct(StructFields::new()))])));
    }
    #[test]
    fn generalized_avro_unions_count_null_positions_and_share_converter_numbering() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into()), ("flatten.singleton.unions".into(), "false".into())]);
        let mut index = 5;
        let first = AvroPlan::parse_version_index(r#"["null","int","string"]"#, &[], &options, None, &mut index).unwrap();
        assert_eq!(first.connect.name.as_deref(), Some("connect_union_5"));
        assert!(first.connect.field("connect_union_field_0").is_none());
        let second = AvroPlan::parse_version_index(r#"["long"]"#, &[], &options, None, &mut index).unwrap();
        assert_eq!(second.connect.name.as_deref(), Some("connect_union_6"));
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(second.decode(&[0, 84], &mut context).unwrap().value, Value::Struct(StructFields::from([("connect_union_field_0".into(), Value::I64(42))])));
        let enumeration = AvroPlan::parse_version(r#"{"type":"enum","name":"Letter","symbols":["A","B"]}"#, &[], &options, None).unwrap();
        assert_eq!(enumeration.connect.parameters["org.apache.kafka.connect.data.Enum.B"], "1");
    }
    #[test]
    fn generalized_avro_null_positions_preserve_reference_field_lookup_failures() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]);
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let before = AvroPlan::parse_version(r#"["null","int","string"]"#, &[], &options, None).unwrap();
        assert_eq!(before.decode(&[0], &mut context).unwrap().value, Value::Null);
        assert!(before.decode(&[2, 84], &mut context).is_err());
        assert!(before.decode(&[4, 2, b'x'], &mut context).is_err());
        let after = AvroPlan::parse_version(r#"["int","string","null"]"#, &[], &options, None).unwrap();
        assert_eq!(after.decode(&[0, 84], &mut context).unwrap().value, Value::Struct(StructFields::from([("connect_union_field_0".into(), Value::I32(42)), ("connect_union_field_1".into(), Value::Null)])));
        assert_eq!(after.decode(&[2, 2, b'x'], &mut context).unwrap().value, Value::Struct(StructFields::from([("connect_union_field_0".into(), Value::Null), ("connect_union_field_1".into(), Value::Utf8("x".into()))])));
        let pair = AvroPlan::parse_version(r#"["null","int"]"#, &[], &options, None).unwrap();
        assert_eq!(pair.decode(&[2, 84], &mut context).unwrap().value, Value::I32(42));
    }
    #[test]
    fn generalized_avro_null_positions_keep_compatible_destination_fields() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]);
        let schema = r#"["null",{"type":"enum","name":"Letter","symbols":["A"]},"string"]"#;
        let plan = AvroPlan::parse_version(schema, &[], &options, None).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&[4, 2, b'x'], &mut context).unwrap().value, Value::Struct(StructFields::from([("connect_union_field_1".into(), Value::Utf8("x".into())), ("connect_union_field_2".into(), Value::Null)])));
        let schema = r#"[{"type":"fixed","name":"A","size":1},"null",{"type":"fixed","name":"B","size":2}]"#;
        let plan = AvroPlan::parse_version(schema, &[], &options, None).unwrap();
        assert_eq!(plan.decode(&[4, 7, 8], &mut context).unwrap().value, Value::Struct(StructFields::from([("connect_union_field_0".into(), Value::Binary(vec![7, 8])), ("connect_union_field_2".into(), Value::Null)])));
    }
    #[test]
    fn enhanced_avro_fixed_unions_match_full_names_before_converting_values() {
        let schema = r#"[{"type":"fixed","name":"a.Value","size":1},{"type":"fixed","name":"b.Value","size":2}]"#;
        let plan = AvroPlan::parse_version(schema, &[], &BTreeMap::from([("enhanced.avro.schema.support".into(), "true".into())]), None).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&[2, 7, 8], &mut context).unwrap().value, Value::Struct(StructFields::from([("a.Value".into(), Value::Null), ("b.Value".into(), Value::Binary(vec![7, 8]))])));
    }
    #[test]
    fn avro_narrow_integer_unions_only_convert_when_flattened() {
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        for (annotation, value) in [("int8", Value::I8(1)), ("int16", Value::I16(1))] {
            let member = format!(r#"{{"type":"int","connect.type":"{annotation}"}}"#);
            let singleton = format!("[{member}]");
            assert_eq!(AvroPlan::parse(&singleton, &[]).unwrap().decode(&[0, 2], &mut context).unwrap().value, value);
            let nullable = format!("[\"null\",{member}]");
            assert_eq!(AvroPlan::parse(&nullable, &[]).unwrap().decode(&[2, 2], &mut context).unwrap().value, value);
            let multiple = format!("[{member},\"string\"]");
            assert!(AvroPlan::parse(&multiple, &[]).unwrap().decode(&[0, 2], &mut context).is_err());
            let options = BTreeMap::from([("flatten.singleton.unions".into(), "false".into())]);
            assert!(AvroPlan::parse_version(&singleton, &[], &options, None).unwrap().decode(&[0, 2], &mut context).is_err());
        }
    }
    #[test]
    fn avro_invalid_connect_defaults_are_ignored_after_nested_validation() {
        let record = AvroPlan::parse(r#"{"type":"record","name":"R","connect.default":{},"fields":[{"name":"required","type":"string"}]}"#, &[]).unwrap();
        assert!(record.connect.default.is_none());
        let array = AvroPlan::parse(r#"{"type":"array","items":"string","connect.default":[null]}"#, &[]).unwrap();
        assert!(array.connect.default.is_none());
        let optional = AvroPlan::parse(r#"{"type":"array","items":["null","string"],"connect.default":[null]}"#, &[]).unwrap();
        assert_eq!(optional.connect.default, Some(Value::list(vec![Value::Null])));
        let record = AvroPlan::parse(r#"{"type":"record","name":"R","connect.default":{},"fields":[{"name":"required","type":"string","default":"x"}]}"#, &[]).unwrap();
        assert!(record.connect.default.is_none());
        let nested = AvroPlan::parse(r#"{"type":"record","name":"R","fields":[{"name":"nested","type":{"type":"record","name":"N","fields":[{"name":"required","type":"string","default":"x"}]},"default":{}}]}"#, &[]).unwrap();
        assert_eq!(nested.connect.field("nested").unwrap().schema.default, Some(Value::Struct(StructFields::from([("required".into(), Value::Utf8("x".into()))]))));
    }
    #[test]
    fn nested_avro_records_nullable_union_and_logical_types_decode_with_bounded_preflight() {
        use apache_avro::types::Value as Avro;
        let schema = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"optional","type":["null","string"]},{"name":"day","type":{"type":"int","logicalType":"date"}},{"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}}]}"#;
        let parsed = Schema::parse_str(schema).unwrap();
        let bytes = apache_avro::to_avro_datum(&parsed, Avro::Record(vec![
            ("id".into(), Avro::Long(7)), ("optional".into(), Avro::Union(1, Box::new(Avro::String("value".into())))),
            ("day".into(), Avro::Date(20_000)), ("amount".into(), Avro::Decimal(vec![0x30, 0x39].into())),
        ])).unwrap();
        let plan = AvroPlan::parse(schema, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(values) = plan.decode(&bytes, &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(values["id"], Value::I64(7)); assert_eq!(values["optional"], Value::Utf8("value".into())); assert_eq!(values["day"], super::super::data::date_value(20_000 * 86_400_000));
        let Value::Decimal(amount) = &values["amount"] else { panic!("expected Decimal") };
        assert_eq!(amount.precision, 32); assert_eq!(amount.scale, 2); assert_eq!(&amount.unscaled[14..], &[0x30, 0x39]);
    }
    #[test]
    fn avro_logical_dates_retain_raw_milliseconds_and_time_accepts_the_day_endpoint() {
        use apache_avro::types::Value as Avro;
        let date = AvroPlan::parse(r#"{"type":"int","logicalType":"date"}"#, &[]).unwrap();
        let time = AvroPlan::parse(r#"{"type":"int","logicalType":"time-millis"}"#, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        for days in [i32::MIN, -1, 0, i32::MAX] {
            let bytes = apache_avro::to_avro_datum(&Schema::Date, Avro::Date(days)).unwrap();
            assert_eq!(date.decode(&bytes, &mut context).unwrap().value, super::super::data::date_value(i64::from(days) * 86_400_000));
        }
        for millis in [0, 86_399_999, 86_400_000] {
            let bytes = apache_avro::to_avro_datum(&Schema::TimeMillis, Avro::TimeMillis(millis)).unwrap();
            assert_eq!(time.decode(&bytes, &mut context).unwrap().value, super::super::data::date_value(i64::from(millis)));
        }
        for millis in [-1, 86_400_001] {
            let bytes = apache_avro::to_avro_datum(&Schema::TimeMillis, Avro::TimeMillis(millis)).unwrap();
            assert!(time.decode(&bytes, &mut context).is_err());
        }
    }
    #[test]
    fn explicit_avro_date_names_keep_primitive_width_and_time_range_checks() {
        use apache_avro::types::Value as Avro;
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        for text in [
            r#"{"type":"long","connect.name":"org.apache.kafka.connect.data.Date"}"#,
            r#"{"type":"int","connect.name":"org.apache.kafka.connect.data.Timestamp"}"#,
            r#"{"type":"int","connect.type":"int8","connect.name":"org.apache.kafka.connect.data.Date"}"#,
        ] {
            assert!(AvroPlan::parse(text, &[]).unwrap().decode(&[2], &mut context).is_err());
        }
        let time = AvroPlan::parse(r#"{"type":"int","connect.name":"org.apache.kafka.connect.data.Time"}"#, &[]).unwrap();
        for millis in [-1, 86_400_000, 86_400_001] {
            let bytes = apache_avro::to_avro_datum(&Schema::Int, Avro::Int(millis)).unwrap();
            let result = time.decode(&bytes, &mut context);
            if millis == 86_400_000 { assert_eq!(result.unwrap().value, super::super::data::date_value(i64::from(millis))); }
            else { assert!(result.is_err()); }
        }
        let timestamp = AvroPlan::parse(r#"{"type":"long","connect.name":"org.apache.kafka.connect.data.Timestamp"}"#, &[]).unwrap();
        for millis in [i64::MIN, i64::MAX] {
            let bytes = apache_avro::to_avro_datum(&Schema::Long, Avro::Long(millis)).unwrap();
            assert_eq!(timestamp.decode(&bytes, &mut context).unwrap().value, super::super::data::date_value(millis));
        }
        let union = AvroPlan::parse(r#"["null",{"type":"int","connect.name":"org.apache.kafka.connect.data.Date"}]"#, &[]).unwrap();
        assert_eq!(union.decode(&[0], &mut context).unwrap().value, Value::Null);
        assert_eq!(union.decode(&[2,2], &mut context).unwrap().value, super::super::data::date_value(86_400_000));
    }
    #[test]
    fn explicit_date_names_preserve_defaults_built_before_name_assignment() {
        let explicit = AvroPlan::parse(r#"{"type":"int","connect.name":"org.apache.kafka.connect.data.Time","connect.default":86400001}"#, &[]).unwrap();
        assert_eq!(explicit.connect.default, Some(Value::I32(86_400_001)));
        let physical = AvroPlan::parse(r#"{"type":"int","logicalType":"time-millis","connect.default":86400001}"#, &[]).unwrap();
        assert!(physical.connect.default.is_none());
        let union = AvroPlan::parse(r#"{"type":"record","name":"Dates","fields":[{"name":"day","type":[{"type":"int","connect.name":"org.apache.kafka.connect.data.Date"},"null"],"default":1}]}"#, &[]).unwrap();
        assert_eq!(union.connect.field("day").unwrap().schema.default, Some(Value::I32(1)));
        assert!(AvroPlan::parse(r#"{"type":"bytes","logicalType":"decimal","precision":8,"scale":0,"connect.name":"org.apache.kafka.connect.data.Date"}"#, &[]).is_err());
    }
    #[test]
    fn nested_avro_values_convert_before_the_container_logical_date_name() {
        let schema = r#"{"type":"record","name":"Container","connect.name":"org.apache.kafka.connect.data.Date","fields":[{"name":"time","type":{"type":"int","connect.name":"org.apache.kafka.connect.data.Time"}}]}"#;
        let parsed = Schema::parse_str(schema).unwrap();
        let plan = AvroPlan::parse(schema, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        for (time, expected) in [(-1, "Time is outside one day"), (0, "Logical date value has an incompatible primitive representation")] {
            let bytes = apache_avro::to_avro_datum(&parsed, apache_avro::types::Value::Record(vec![("time".into(), apache_avro::types::Value::Int(time))])).unwrap();
            assert!(plan.decode(&bytes, &mut context).unwrap_err().message.as_deref().is_some_and(|message| message.contains(expected)));
        }
    }
    #[test]
    fn final_decimal_name_uses_schema_scale_and_keeps_default_name_order() {
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        for (scale, expected) in [("2", 2), ("-2147483648", i32::MIN), ("+٢", 2)] {
            let text = format!(r#"{{"type":"bytes","connect.name":"org.apache.kafka.connect.data.Decimal","connect.parameters":{{"scale":"{scale}"}},"connect.default":"\u0001"}}"#);
            let plan = AvroPlan::parse(&text, &[]).unwrap();
            assert_eq!(plan.connect.default, Some(Value::Binary(vec![1])));
            let Value::Decimal(value) = plan.decode(&[1], &mut context).unwrap().value else { panic!("expected Decimal") };
            assert_eq!(value.scale, expected);
            assert_eq!(num_bigint::BigInt::from_signed_bytes_be(&value.unscaled), 1.into());
        }
        let physical = AvroPlan::parse(r#"{"type":"bytes","logicalType":"decimal","precision":8,"scale":2,"connect.parameters":{"scale":"-2"},"connect.default":"\u0001"}"#, &[]).unwrap();
        let Value::Decimal(default) = physical.connect.default.as_ref().unwrap() else { panic!("expected Decimal default") };
        assert_eq!(default.scale, -2);
        let Value::Decimal(value) = physical.decode(&[1], &mut context).unwrap().value else { panic!("expected Decimal") };
        assert_eq!(value.scale, -2);
    }
    #[test]
    fn decimal_coefficients_and_children_fail_before_parent_scale_or_representation() {
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let missing_scale = AvroPlan::parse(r#"{"type":"bytes","connect.name":"org.apache.kafka.connect.data.Decimal"}"#, &[]).unwrap();
        assert_eq!(missing_scale.decode(&[], &mut context).unwrap_err().message.as_deref(), Some("Decimal coefficient bytes are empty"));
        assert_eq!(missing_scale.decode(&[1], &mut context).unwrap_err().message.as_deref(), Some("Decimal schema has no scale"));
        for scale in ["2147483648", "-2147483649", " 2", "2 ", "", "𝟚"] {
            let text = format!(r#"{{"type":"bytes","connect.name":"org.apache.kafka.connect.data.Decimal","connect.parameters":{{"scale":"{scale}"}}}}"#);
            assert_eq!(AvroPlan::parse(&text, &[]).unwrap().decode(&[1], &mut context).unwrap_err().message.as_deref(), Some("Decimal scale is not a signed 32-bit integer"));
        }
        let text = r#"{"type":"record","name":"Container","connect.name":"org.apache.kafka.connect.data.Decimal","fields":[{"name":"time","type":{"type":"int","connect.name":"org.apache.kafka.connect.data.Time"}}]}"#;
        let plan = AvroPlan::parse(text, &[]).unwrap();
        assert_eq!(plan.decode(&[1], &mut context).unwrap_err().message.as_deref(), Some("Time is outside one day"));
        assert_eq!(plan.decode(&[0], &mut context).unwrap_err().message.as_deref(), Some("Logical Decimal value has an incompatible primitive representation"));
    }
    #[test]
    fn confluent_root_bytes_have_no_avro_length_prefix() {
        let plan = AvroPlan::parse(r#""bytes""#, &[]).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 20));
        assert_eq!(plan.decode(&[255, 0, 3], &mut context).unwrap().value, Value::Binary(vec![255, 0, 3]));
    }
    #[test]
    fn avro_connect_annotations_defaults_and_registry_version_are_preserved() {
        use apache_avro::types::Value as Avro;
        let schema = r#"{"type":"record","name":"Event","doc":"avro doc","connect.doc":"connect doc","connect.version":8,"fields":[{"name":"small","type":{"type":"int","connect.type":"int8"},"default":257},{"name":"fixed","type":{"type":"fixed","name":"Bytes","size":2}},{"name":"choice","type":{"type":"enum","name":"Choice","symbols":["A","B"]}}]}"#;
        let plan = AvroPlan::parse_version(schema, &[], &BTreeMap::new(), Some(3)).unwrap();
        assert_eq!(plan.connect.version, Some(8)); assert_eq!(plan.connect.doc.as_deref(), Some("connect doc"));
        assert_eq!(plan.connect.parameters["io.confluent.connect.avro.record.doc"], "avro doc");
        assert_eq!(plan.connect.field("small").unwrap().schema.default, Some(Value::I8(1)));
        assert_eq!(plan.connect.field("fixed").unwrap().schema.parameters["connect.fixed.size"], "2");
        assert_eq!(plan.connect.field("choice").unwrap().schema.parameters["io.confluent.connect.avro.Enum.B"], "B");
        let parsed = Schema::parse_str(schema).unwrap();
        let bytes = apache_avro::to_avro_datum(&parsed, Avro::Record(vec![("small".into(), Avro::Int(257)), ("fixed".into(), Avro::Fixed(2, vec![0, 1])), ("choice".into(), Avro::Enum(1, "B".into()))])).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(values) = plan.decode(&bytes, &mut context).unwrap().value else { panic!("expected Struct") }; assert_eq!(values["small"], Value::I8(1));
    }
    #[test]
    fn avro_map_entry_arrays_become_non_string_key_maps() {
        use apache_avro::types::Value as Avro;
        let schema = r#"{"type":"array","items":{"type":"record","name":"MapEntry","namespace":"io.confluent.connect.avro","fields":[{"name":"key","type":"long"},{"name":"value","type":"string"}]}}"#;
        let parsed = Schema::parse_str(schema).unwrap();
        let bytes = apache_avro::to_avro_datum(&parsed, Avro::Array(vec![Avro::Record(vec![("key".into(), Avro::Long(9)), ("value".into(), Avro::String("nine".into()))])])).unwrap();
        let plan = AvroPlan::parse(schema, &[]).unwrap(); let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(&bytes, &mut context).unwrap().value, Value::Map { entries: vec![(Value::I64(9), Value::Utf8("nine".into()))], sorted: false });
    }
    #[test]
    fn avro_collection_counts_are_checked_before_allocating_elements() {
        let schema = Schema::parse_str(r#"{"type":"array","items":"null"}"#).unwrap();
        let names = BTreeMap::new();
        let mut preflight = Preflight { bytes: &[0x80, 0x80, 0x80, 0x01, 0], offset: 0, window: BinaryWindow::new(5), allocated: 0, limit: 1 << 20, names: &names, view_bytes: 0 };
        assert!(preflight.value(&schema, 0).unwrap_err().code.ends_with("batch-too-large"));
    }
}
