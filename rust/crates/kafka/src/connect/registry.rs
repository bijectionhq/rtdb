use std::{collections::{BTreeMap, VecDeque}, sync::Arc, time::Instant};
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_types::{Result, java_text::JavaText};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use super::{ConnectData, ConversionContext, Converter, PluginSpec, error};
mod security;
mod rules;
mod reader;
mod dlq;
mod http;
mod references;
mod json_admission;
mod schema_id;
mod factory;
pub(super) use factory::ParserFactories;
use references::{ResolvedReferences, ResolvedSchema};
#[cfg(test)]
mod http_tests;
mod response;
pub(super) use response::symbols as reader_symbols;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistryFormat { Avro, Json }
pub(super) struct RegistryConverter {
    client: Option<(http::Client, Arc<MemoryReservation>)>,
    urls: Vec<reqwest::Url>,
    security: security::Security,
    format: RegistryFormat,
    parsers: ParserFactories,
    avro: Option<super::avro::AvroSchemaRuntime>,
    is_key: bool,
    options: BTreeMap<String, String>,
    schemas: BTreeMap<(String, u32), WriterEntry>,
    order: VecDeque<(String, u32)>,
    capacity: usize,
    maximum_schema_bytes: usize,
    next_avro_union: i32,
    reader_selection: reader::Selection,
    latest: reader::LatestCache<SchemaDocument>,
    bindings: BTreeMap<(String, u32), ReaderBinding>,
    binding_order: VecDeque<(String, u32)>,
    dlq: BTreeMap<String, dlq::DlqProducer>,
}
struct SchemaDocument {
    root: RegistrySchema,
    references: ResolvedReferences,
    subject: String,
    version: i32,
    bytes: usize,
    plan: DocumentPlan,
    charge: Arc<MemoryReservation>,
}
enum DocumentPlan {
    Avro { datum: Arc<super::avro::AvroDatumPlan>, charge: Arc<MemoryReservation> },
    Json,
}
struct WriterEntry { document: Arc<SchemaDocument>, key_charge: Arc<MemoryReservation> }
struct ReaderBinding { plan: ReaderPlan, writer: Arc<SchemaDocument>, selected: Option<Arc<SchemaDocument>>, migrations: Vec<rules::ReadRules>, read_rules: rules::ReadRules, _charges: Vec<Arc<MemoryReservation>> }
enum ReaderPlan {
    Avro { writer: Arc<super::avro::AvroDatumPlan>, selection: AvroSelection, outputs: BTreeMap<super::avro::AvroOutput, super::avro::AvroPlan> },
    Json(JsonOutput),
}
enum Decoded { Data(ConnectData), Dlq { rule: Arc<rules::DlqRule>, original: Original } }
enum Original { Framed, Json(super::json_value::JsonValue) }
enum JsonOutput { Unprepared, Prepared(super::json_schema::JsonPlan) }
enum AvroSelection {
    Writer,
    Reader { plan: Arc<super::avro::AvroDatumPlan>, decode: AvroRead },
}
enum AvroRead { Binary(super::avro::AvroResolution), Migration, RawBytes }
impl SchemaDocument {
    fn equivalent(&self, other: &Self) -> bool { self.version == other.version && self.subject == other.subject && self.root == other.root && self.references == other.references }
    fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        context.retain(Arc::clone(&self.charge))?;
        if let DocumentPlan::Avro { datum, charge } = &self.plan {
            context.retain(Arc::clone(charge))?;
            datum.retain_source(context)?;
        }
        Ok(())
    }
    fn avro_plan(&self, context: &mut ConversionContext) -> Result<Arc<super::avro::AvroDatumPlan>> {
        let DocumentPlan::Avro { datum, .. } = &self.plan else { return Err(error("schema-registry", "Avro binding requires a parsed Avro document")); };
        self.retain(context)?;
        Ok(Arc::clone(datum))
    }
}
impl ReaderBinding {
    fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        self.writer.retain(context)?;
        if let Some(selected) = &self.selected { selected.retain(context)?; }
        for charge in &self._charges { context.retain(Arc::clone(charge))?; }
        Ok(())
    }
    fn decode(&mut self, bytes: &[u8], options: &BTreeMap<String, String>, next_union: &mut i32, context: &mut ConversionContext) -> Result<Decoded> {
        self.retain(context)?;
        match &mut self.plan {
            ReaderPlan::Json(output) => {
                let datum = super::json_schema::JsonDatum::decode(bytes, context)?;
                for migration in &self.migrations {
                    if let Some(rule) = migration.check(false)? { return Ok(Decoded::Dlq { rule, original: Original::Json(datum.value) }); }
                }
                if let Some(rule) = self.read_rules.check(false)? { return Ok(Decoded::Dlq { rule, original: Original::Framed }); }
                if matches!(output, JsonOutput::Unprepared) {
                    let document = self.selected.as_ref().unwrap_or(&self.writer);
                    *output = JsonOutput::Prepared(super::json_schema::JsonPlan::parse_version(&document.root.schema, document.references.by_name(), options, Some(document.version))?);
                }
                let JsonOutput::Prepared(plan) = output else { unreachable!() };
                plan.convert(datum, context).map(Decoded::Data)
            }
            ReaderPlan::Avro { writer, selection, outputs } => {
                if let AvroSelection::Reader { plan, decode: AvroRead::Migration } = selection {
                    let datum = writer.decode_datum(bytes, context)?;
                    let datum = writer.binary_result(datum, context)?;
                    let json = writer.to_json_datum(datum, context)?;
                    for migration in &self.migrations {
                        if let Some(rule) = migration.check(false)? {
                            return Ok(Decoded::Dlq { rule, original: Original::Json(super::json_value::JsonValue::from_serde(json)) });
                        }
                    }
                    let datum = plan.from_json_datum(json, context)?;
                    let is_null = datum.is_null();
                    if let Some(rule) = self.read_rules.check(is_null)? { return Ok(Decoded::Dlq { rule, original: Original::Framed }); }
                    if is_null { return Err(error("conversion-error", "Avro converter returned no schema and value")); }
                    let (output, datum) = writer.prepare_output(plan, datum, context)?;
                    let output = avro_output(outputs, output, writer, plan, self.writer.version, next_union, context)?;
                    return output.convert(datum, next_union, context).map(Decoded::Data);
                }
                let (reader, datum) = match selection {
                    AvroSelection::Writer => (&*writer, writer.wire_result(writer.decode_wire(bytes, context)?, context)?),
                    AvroSelection::Reader { plan, decode } => {
                        let datum = match decode {
                            AvroRead::Binary(resolution) => writer.wire_result(resolution.decode_wire(bytes, context)?, context)?,
                            AvroRead::RawBytes => writer.decode_wire(bytes, context)?,
                            AvroRead::Migration => unreachable!("migration uses Avro JSON"),
                        };
                        (&*plan, datum)
                    }
                };
                let is_null = datum.is_null();
                if let Some(rule) = self.read_rules.check(is_null)? { return Ok(Decoded::Dlq { rule, original: Original::Framed }); }
                if is_null { return Err(error("conversion-error", "Avro converter returned no schema and value")); }
                let output = writer.prepare_wire_output(reader, &datum, context)?;
                let output = avro_output(outputs, output, writer, reader, self.writer.version, next_union, context)?;
                output.convert_wire(datum, next_union, context).map(Decoded::Data)
            }
        }
    }
}
fn avro_output<'a>(
    outputs: &'a mut BTreeMap<super::avro::AvroOutput, super::avro::AvroPlan>,
    output: super::avro::AvroOutput,
    writer: &Arc<super::avro::AvroDatumPlan>,
    reader: &Arc<super::avro::AvroDatumPlan>,
    version: i32,
    next_union: &mut i32,
    context: &mut ConversionContext,
) -> Result<&'a super::avro::AvroPlan> {
    use std::collections::btree_map::Entry;
    Ok(match outputs.entry(output) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let (plan, branch) = match output {
                super::avro::AvroOutput::Writer => (writer, None),
                super::avro::AvroOutput::Reader { branch } => (reader, branch),
            };
            entry.insert(plan.connect_plan(branch, Some(version), next_union, context)?)
        }
    })
}

#[derive(Clone, PartialEq, Deserialize, Serialize)]
struct RegistrySchema {
    schema: String,
    #[serde(rename = "schemaType", default, skip_serializing_if = "Option::is_none")]
    schema_type: Option<String>,
    #[serde(default, deserialize_with = "references::deserialize_reference_list", skip_serializing_if = "Vec::is_empty")]
    references: Vec<Option<Reference>>,
    #[serde(rename = "ruleSet", default, skip_serializing_if = "Option::is_none")]
    rule_set: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Json>,
}
#[derive(Clone, PartialEq, Deserialize, Serialize)]
struct Reference {
    #[serde(default, skip_serializing_if = "references::empty_text")]
    name: Option<String>,
    #[serde(default, skip_serializing_if = "references::empty_text")]
    subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<i32>,
}
impl RegistryConverter {
    #[cfg(test)]
    pub fn open(spec: &PluginSpec, format: RegistryFormat, is_key: bool) -> Result<Self> {
        Self::with_parsers(spec, format, is_key, ParserFactories::default())
    }
    pub(super) fn with_parsers(spec: &PluginSpec, format: RegistryFormat, is_key: bool, parsers: ParserFactories) -> Result<Self> {
        let options = &spec.options;
        let urls = options.get("schema.registry.url").ok_or_else(|| error("invalid-plugin-config", "schema.registry.url is required"))?.split(',').map(|value| {
            let url = reqwest::Url::parse(value.trim()).map_err(|cause| error("invalid-plugin-config", cause))?;
            if !matches!(url.scheme(), "http" | "https") { return Err(error("invalid-plugin-config", "Schema Registry URL must use HTTP or HTTPS")); } Ok(url)
        }).collect::<Result<Vec<_>>>()?;
        let security = security::Security::open(options)?;
        rules::validate_plugins(options)?;
        let reader_selection = reader::Selection::parse(options)?;
        let latest = reader::LatestCache::open(options)?;
        let capacity = options.get("max.schemas.per.subject").map(|value| value.parse::<usize>().map_err(|cause| error("invalid-plugin-config", cause))).transpose()?.unwrap_or(1000);
        if capacity == 0 { return Err(error("invalid-plugin-config", "Schema cache capacity must be positive")); }
        if format == RegistryFormat::Avro {
            super::converter::option_bool(options, "scrub.invalid.names", false)?;
            super::converter::option_bool(options, "avro.use.logical.type.converters", false)?;
        }
        for key in ["specific.avro.reader", "schema.reflection"] {
            super::converter::option_bool(options, key, false)?;
        }
        Ok(Self { client: None, urls, security, format, parsers, avro: None, is_key, options: options.clone(), schemas: BTreeMap::new(), order: VecDeque::new(), capacity, maximum_schema_bytes: 256 * 1024, next_avro_union: 0, reader_selection, latest, bindings: BTreeMap::new(), binding_order: VecDeque::new(), dlq: BTreeMap::new() })
    }
    async fn fetch_schema_id(&mut self, id: u32, context: &mut ConversionContext) -> Result<RegistrySchema> {
        let response = self.request_raw(&["schemas".into(), "ids".into(), id.to_string()], &[], None, context).await?;
        if self.format == RegistryFormat::Avro {
            if let Some(bytes) = response.bytes() {
                if let Some(schema) = schema_id::decode(bytes, context)? { return Ok(schema); }
            }
        }
        let value = Self::decode_response(response, context)?;
        references::admit_typed_references(&value, context)?;
        serde_json::from_value(value).map_err(|cause| error("schema-registry", cause))
    }
    #[cfg(test)]
    async fn request(&mut self, path: &[String], body: Option<&Json>, context: &mut ConversionContext) -> Result<Json> {
        self.request_query(path, &[], body, context).await
    }
    async fn request_query(&mut self, path: &[String], query: &[(String, String)], body: Option<&Json>, context: &mut ConversionContext) -> Result<Json> {
        let response = self.request_raw(path, query, body, context).await?;
        Self::decode_response(response, context)
    }
    fn decode_response(response: http::RawResponse, context: &mut ConversionContext) -> Result<Json> {
        match response.bytes() {
            Some(bytes) => json_admission::parse(bytes, context),
            None => Ok(Json::Null),
        }
    }
    async fn request_raw(&mut self, path: &[String], query: &[(String, String)], body: Option<&Json>, context: &mut ConversionContext) -> Result<http::RawResponse> {
        match self.request_body(path, query, body, context).await? {
            Some((input, charge)) => http::RawResponse::collect(input, charge).await,
            None => Ok(http::RawResponse::NoContent),
        }
    }
    async fn request_body(&mut self, path: &[String], query: &[(String, String)], body: Option<&Json>, context: &mut ConversionContext) -> Result<Option<(http::BodyInput<http::Response>, Arc<MemoryReservation>)>> {
        let response_charge = context.reserve(self.maximum_schema_bytes.checked_mul(4).ok_or_else(|| error("allocation-overflow", "Schema response bound overflow"))?)?;
        if self.client.is_none() { self.client = Some(security::client(&self.options, context)?); }
        let (client, charge) = self.client.as_ref().expect("registry client initialized");
        context.retain(Arc::clone(charge))?;
        let client = client.clone();
        let mut failure = None;
        for base in &self.urls {
            let mut url = base.clone();
            url.path_segments_mut().map_err(|_| error("invalid-plugin-config", "Schema Registry URL cannot contain path segments"))?.pop_if_empty().extend(path.iter().map(String::as_str));
            if !query.is_empty() { url.query_pairs_mut().extend_pairs(query.iter().map(|(key, value)| (key.as_str(), value.as_str()))); }
            let mut headers = self.security.headers(&url, context).await?;
            url.set_username("").map_err(|_| error("invalid-plugin-config", "Registry URL cannot remove user info"))?;
            url.set_password(None).map_err(|_| error("invalid-plugin-config", "Registry URL cannot remove user info"))?;
            let (method, body) = match body {
                Some(body) => {
                    headers.insert(::http::header::CONTENT_TYPE, ::http::HeaderValue::from_static("application/json"));
                    (::http::Method::POST, serde_json::to_vec(body).map_err(|cause| error("schema-registry", cause))?)
                }
                None => (::http::Method::GET, Vec::new()),
            };
            let response = match client.request(method, url, headers, body).await { Ok(response) => response, Err(cause) => { failure = Some(error("schema-registry", cause.without_url())); continue; } };
            if response.status() == ::http::StatusCode::NO_CONTENT { return Ok(None); }
            if response.status() != ::http::StatusCode::OK {
                let cause = error("schema-registry", format!("Schema Registry returned HTTP {}", response.status()));
                if response.status().is_server_error() { failure = Some(cause); continue; } return Err(cause);
            }
            if response.content_length().is_some_and(|length| length > self.maximum_schema_bytes as u64) { return Err(error("schema-registry", "Schema response exceeds its byte limit")); }
            let input = http::BodyInput::new(response, self.maximum_schema_bytes);
            return Ok(Some((input, response_charge)));
        }
        Err(failure.unwrap_or_else(|| error("schema-registry", "No Schema Registry endpoint is configured")))
    }
    async fn schema(&mut self, topic: &str, id: u32, context: &mut ConversionContext) -> Result<Arc<SchemaDocument>> {
        let key_charge = context.reserve(topic.len().checked_mul(4).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(|| error("allocation-overflow", "Schema cache key allocation overflow"))?)?;
        let cache_key = (topic.to_owned(), id);
        if let Some(entry) = self.schemas.get(&cache_key) {
            let schema = Arc::clone(&entry.document);
            schema.retain(context)?;
            context.retain(Arc::clone(&entry.key_charge))?;
            self.order.retain(|cached| cached != &cache_key); self.order.push_back(cache_key);
            return Ok(schema);
        }
        let root = self.fetch_schema_id(id, context).await?;
        self.validate_format(&root)?;
        let resolved = self.resolve_references(root, context).await?;
        let plan = self.document_plan(&resolved, context)?;
        let subject = self.subject(topic, &resolved.root.schema, &plan, context)?;
        let lookup = serde_json::to_value(&resolved.root).map_err(|cause| error("schema-registry", cause))?;
        let lookup = self.request_query(&["subjects".into(), subject.clone()], &[("normalize".into(), "false".into()), ("deleted".into(), "true".into())], Some(&lookup), context).await?;
        let version = lookup.get("version").and_then(Json::as_i64).and_then(|version| i32::try_from(version).ok()).ok_or_else(|| error("schema-registry", "Subject lookup has no valid schema version"))?;
        if lookup.get("id").and_then(Json::as_u64) != Some(u64::from(id)) { return Err(error("schema-registry", "Subject lookup returned a different schema ID")); }
        let schema = self.finish_document(resolved, plan, subject, version, context)?;
        while self.schemas.len() >= self.capacity { if let Some(id) = self.order.pop_front() { self.schemas.remove(&id); } }
        self.schemas.insert(cache_key.clone(), WriterEntry { document: Arc::clone(&schema), key_charge }); self.order.push_back(cache_key); Ok(schema)
    }
    fn validate_format(&self, root: &RegistrySchema) -> Result<()> {
        let expected = match self.format { RegistryFormat::Avro => "AVRO", RegistryFormat::Json => "JSON" };
        if root.schema_type.as_deref().unwrap_or("AVRO") != expected { return Err(error("schema-registry", "Schema Registry format differs from the configured converter")); }
        Ok(())
    }
    async fn document(&mut self, root: RegistrySchema, subject: String, version: i32, context: &mut ConversionContext) -> Result<Arc<SchemaDocument>> {
        self.validate_format(&root)?;
        let resolved = self.resolve_references(root, context).await?;
        let plan = self.document_plan(&resolved, context)?;
        self.finish_document(resolved, plan, subject, version, context)
    }
    fn finish_document(&self, resolved: ResolvedSchema, plan: DocumentPlan, subject: String, version: i32, context: &mut ConversionContext) -> Result<Arc<SchemaDocument>> {
        let ResolvedSchema { root, references } = resolved;
        let root_bytes = references::serialized_size(&root)?;
        let bytes = references.by_name().iter().try_fold(root_bytes.checked_add(subject.len()).ok_or_else(|| error("allocation-overflow", "Schema subject size overflow"))?, |bytes, (name, text)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(text.len())).ok_or_else(|| error("allocation-overflow", "Schema graph size overflow")))?;
        let reference_bytes = references.retained_bound()?;
        let root_heap = references::root_retained_bound(&root)?;
        let charge = context.reserve(root_heap.checked_add(reference_bytes).and_then(|bytes| bytes.checked_add(subject.capacity())).and_then(|bytes| bytes.checked_add(std::mem::size_of::<SchemaDocument>())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Schema document allocation overflow"))?)?;
        if self.format == RegistryFormat::Json {
            context.reserve(root.schema.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "JSON schema syntax allocation overflow"))?)?;
            serde_json::from_str::<Json>(&root.schema).map_err(|cause| error("json-schema", cause))?;
        }
        if let Some(rule_set) = &root.rule_set {
            let rule_bytes = references::serialized_size(rule_set)?;
            context.reserve(rule_bytes.checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Schema rules allocation overflow"))?)?;
            rules::validate(rule_set)?;
        }
        Ok(Arc::new(SchemaDocument { root, references, subject, version, bytes, plan, charge }))
    }
    fn plan_charge(&self, document: &SchemaDocument, context: &mut ConversionContext) -> Result<Arc<MemoryReservation>> {
        let rule_bound = rules::ReadRules::allocation_bound(document.root.rule_set.as_ref(), &self.options)?;
        context.reserve(document.bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(rule_bound)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| error("allocation-overflow", "Schema graph allocation overflow"))?)
    }
    fn document_plan(&mut self, document: &ResolvedSchema, context: &mut ConversionContext) -> Result<DocumentPlan> {
        if self.format == RegistryFormat::Json { return Ok(DocumentPlan::Json); }
        let count = document.references.by_name().len();
        let slots = count.checked_mul(std::mem::size_of::<JavaText<'_>>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| error("allocation-overflow", "Avro reference slots exceed addressable memory"))?;
        context.reserve(slots)?;
        let mut references = Vec::new();
        references.try_reserve_exact(count).map_err(|cause| error("allocation-failed", cause))?;
        references.extend(document.references.texts().map(JavaText::from_utf8));
        if self.avro.is_none() { self.avro = Some(self.parsers.avro(context)?); }
        let runtime = self.avro.as_mut().expect("Avro runtime initialized");
        let policy = super::avro::SourcePolicy { validate_defaults: false, fixed_bytes: super::avro::FixedBytesLimit::DEFAULT };
        let plan = runtime.parse(Some(JavaText::from_utf8(&document.root.schema)), &references, &self.options, policy, context)?;
        let bytes = std::mem::size_of::<super::avro::AvroDatumPlan>().checked_add(512)
            .ok_or_else(|| error("allocation-overflow", "Avro plan wrapper exceeds addressable memory"))?;
        let charge = context.reserve(bytes)?;
        let datum = Arc::new(plan.expect("non-null Avro schema produced a datum plan"));
        Ok(DocumentPlan::Avro { datum, charge })
    }
    async fn reader_document(&mut self, writer: &SchemaDocument, id: u32, context: &mut ConversionContext) -> Result<Option<Arc<SchemaDocument>>> {
        context.reserve(self.reader_selection.request_bound(&writer.subject)?)?;
        let Some(key) = self.reader_selection.key(&writer.subject, id) else { return Ok(None); };
        if let Some(document) = self.latest.get(&key, Instant::now()) {
            document.retain(context)?;
            return Ok(Some(document));
        }
        let (path, query) = self.reader_selection.request(&writer.subject).ok_or_else(|| error("schema-registry", "Reader schema selection lost its request"))?;
        let value = self.request_query(&path, &query, None, context).await?;
        let version = value.get("version").and_then(Json::as_i64).and_then(|version| i32::try_from(version).ok()).ok_or_else(|| error("schema-registry", "Selected reader schema has no valid version"))?;
        references::admit_typed_references(&value, context)?;
        let root = serde_json::from_value(value).map_err(|cause| error("schema-registry", cause))?;
        let document = self.document(root, writer.subject.clone(), version, context).await?;
        self.latest.insert(key, Arc::clone(&document), Instant::now());
        Ok(Some(document))
    }
    async fn migrations(&mut self, writer: &Arc<SchemaDocument>, reader: &Arc<SchemaDocument>, context: &mut ConversionContext) -> Result<(Vec<rules::ReadRules>, Vec<Arc<MemoryReservation>>)> {
        if writer.version == reader.version { return Ok((Vec::new(), Vec::new())); }
        let (first, last, mode) = if writer.version < reader.version { (writer, reader, rules::Mode::Upgrade) } else { (reader, writer, rules::Mode::Downgrade) };
        let count = usize::try_from(i64::from(last.version) - i64::from(first.version) + 1).map_err(|_| error("allocation-overflow", "Migration version range exceeds addressable memory"))?;
        let range_charge = context.reserve(count.checked_mul(128).ok_or_else(|| error("allocation-overflow", "Migration version range allocation overflow"))?)?;
        let mut versions = vec![Arc::clone(first)];
        for version in i64::from(first.version) + 1..i64::from(last.version) {
            let value = self.request_query(&["subjects".into(), writer.subject.clone(), "versions".into(), version.to_string()], &[("deleted".into(), "true".into())], None, context).await?;
            let returned = value.get("version").and_then(Json::as_i64).and_then(|version| i32::try_from(version).ok()).ok_or_else(|| error("schema-registry", "Migration schema has no valid version"))?;
            references::admit_typed_references(&value, context)?;
            let root = serde_json::from_value(value).map_err(|cause| error("schema-registry", cause))?;
            let document = self.document(root, writer.subject.clone(), returned, context).await?;
            self.plan_charge(&document, context)?;
            if let DocumentPlan::Json = document.plan {
                serde_json::from_str::<Json>(&document.root.schema).map_err(|cause| error("json-schema", cause))?;
            }
            versions.push(document);
        }
        versions.push(Arc::clone(last));
        let mut migrations = Vec::new();
        let mut charges = vec![range_charge];
        for pair in versions.windows(2) {
            let document = &pair[1];
            let target = if mode == rules::Mode::Upgrade { &pair[1] } else { &pair[0] };
            if rules::ReadRules::has_migration(document.root.rule_set.as_ref(), mode)? {
                charges.push(self.plan_charge(document, context)?);
                migrations.push(rules::ReadRules::compile_at(document.root.rule_set.as_ref(), &self.options, mode, &writer.subject, target.root.metadata.as_ref())?);
            }
        }
        if mode == rules::Mode::Downgrade { migrations.reverse(); }
        Ok((migrations, charges))
    }
    async fn bind_reader(&mut self, topic: &str, id: u32, writer: Arc<SchemaDocument>, selected: Option<Arc<SchemaDocument>>, context: &mut ConversionContext) -> Result<(String, u32)> {
        let key_charge = context.reserve(topic.len().checked_mul(4).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(|| error("allocation-overflow", "Reader binding key allocation overflow"))?)?;
        let key = (topic.to_owned(), id);
        let matches = self.bindings.get(&key).is_some_and(|binding| binding.writer.equivalent(&writer) && match (&binding.selected, &selected) { (None, None) => true, (Some(previous), Some(selected)) => previous.equivalent(selected), _ => false });
        if matches {
            self.bindings[&key].retain(context)?;
            self.binding_order.retain(|entry| entry != &key); self.binding_order.push_back(key.clone());
            return Ok(key);
        }
        let (migrations, mut charges) = if let Some(selected) = &selected { self.migrations(&writer, selected, context).await? } else { (Vec::new(), Vec::new()) };
        charges.push(key_charge);
        let output = selected.as_ref().unwrap_or(&writer);
        charges.push(self.plan_charge(output, context)?);
        let read_rules = rules::ReadRules::compile_at(output.root.rule_set.as_ref(), &self.options, rules::Mode::Read, &writer.subject, output.root.metadata.as_ref())?;
        let plan = match self.format {
            RegistryFormat::Json => ReaderPlan::Json(JsonOutput::Unprepared),
            RegistryFormat::Avro => {
                charges.push(self.plan_charge(&writer, context)?);
                let writer = writer.avro_plan(context)?;
                let selection = if let Some(selected) = &selected {
                    let reader = selected.avro_plan(context)?;
                    if migrations.is_empty() { writer.validate_reader(Some(&reader))?; }
                    let decode = if !migrations.is_empty() { AvroRead::Migration }
                        else if writer.raw_bytes() { AvroRead::RawBytes }
                        else { AvroRead::Binary(super::avro::AvroResolution::compile(&writer, &reader, context)?) };
                    AvroSelection::Reader { plan: reader, decode }
                } else { writer.validate_reader(None)?; AvroSelection::Writer };
                ReaderPlan::Avro { writer, selection, outputs: BTreeMap::new() }
            }
        };
        let binding = ReaderBinding { writer, selected, plan, migrations, read_rules, _charges: charges };
        self.bindings.remove(&key); self.binding_order.retain(|entry| entry != &key);
        while self.bindings.len() >= self.capacity { if let Some(key) = self.binding_order.pop_front() { self.bindings.remove(&key); } }
        self.bindings.insert(key.clone(), binding); self.binding_order.push_back(key.clone());
        Ok(key)
    }
    fn subject(&self, topic: &str, schema: &str, plan: &DocumentPlan, context: &mut ConversionContext) -> Result<String> {
        let key = if self.is_key { "key.subject.name.strategy" } else { "value.subject.name.strategy" };
        let strategy = self.options.get(key).map_or("io.confluent.kafka.serializers.subject.TopicNameStrategy", String::as_str);
        if strategy == "io.confluent.kafka.serializers.subject.TopicNameStrategy" { return Ok(format!("{topic}-{}", if self.is_key { "key" } else { "value" })); }
        let name = match plan {
            DocumentPlan::Avro { datum, .. } => datum.subject_record_name(context)?,
            DocumentPlan::Json => {
                let schema: Json = serde_json::from_str(schema).map_err(|cause| error("schema-registry", cause))?;
                schema.get("title").and_then(Json::as_str).map(str::to_owned)
            }
        }.ok_or_else(|| error("schema-registry", "Record subject naming requires a named schema"))?;
        match strategy {
            "io.confluent.kafka.serializers.subject.RecordNameStrategy" => Ok(name),
            "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy" => Ok(format!("{topic}-{name}")),
            _ => Err(error("invalid-plugin-config", "Subject name strategy requires a registered native adapter")),
        }
    }
}
#[async_trait]
impl Converter for RegistryConverter {
    async fn decode(&mut self, topic: &str, bytes: Option<&[u8]>, context: &mut ConversionContext) -> Result<ConnectData> {
        if self.is_key { context.set_registry_key(bytes)?; }
        let result = self.decode_record(topic, bytes, context).await;
        if !self.is_key { context.clear_registry_key(); }
        result
    }
    async fn close(&mut self) -> Result<()> {
        for producer in self.dlq.values_mut() { producer.drain().await; }
        self.dlq.clear();
        Ok(())
    }
}
impl RegistryConverter {
    async fn decode_record(&mut self, topic: &str, bytes: Option<&[u8]>, context: &mut ConversionContext) -> Result<ConnectData> {
        let Some(bytes) = bytes else { return Ok(ConnectData::null()); };
        if bytes.len() < 5 || bytes[0] != 0 { return Err(error("conversion-error", "Invalid Confluent framing header")); }
        let id = u32::from_be_bytes(bytes[1..5].try_into().expect("fixed schema ID"));
        let writer = self.schema(topic, id, context).await?;
        let selected = self.reader_document(&writer, id, context).await?;
        let key = self.bind_reader(topic, id, writer, selected, context).await?;
        match self.bindings.get_mut(&key).expect("reader binding published").decode(&bytes[5..], &self.options, &mut self.next_avro_union, context)? {
            Decoded::Data(data) => Ok(data),
            Decoded::Dlq { rule, original } => {
                let destination = rule.topic.as_deref().filter(|topic| !topic.is_empty()).ok_or_else(|| error("conversion-error", "Could not send to DLQ as no topic is configured"))?;
                let sent = self.send_dlq(&rule, destination, topic, original, bytes, context).await;
                let failure = rule.failure();
                Err(match sent { Ok(()) => failure, Err(cause) => failure.with_context("dlq-error", cause.to_string().into()) })
            }
        }
    }
    async fn send_dlq(&mut self, rule: &rules::DlqRule, destination: &str, topic: &str, original: Original, framed: &[u8], context: &mut ConversionContext) -> Result<()> {
        let record = dlq::Record::new(rule, destination, topic, self.is_key, original, framed, context)?;
        if !self.dlq.contains_key(&rule.action.name) {
            let producer = dlq::DlqProducer::open(&rule.action.options, context)?;
            self.dlq.insert(rule.action.name.clone(), producer);
        }
        self.dlq.get_mut(&rule.action.name).expect("DLQ producer retained").send(record, context).await
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;

    fn configured(mode: &str, value: &str) -> Result<RegistryConverter> {
        let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([
            ("schema.registry.url".into(), "http://registry.invalid".into()), (mode.into(), value.into()),
        ]) };
        RegistryConverter::open(&spec, RegistryFormat::Avro, false)
    }

    async fn document(converter: &mut RegistryConverter, schema: &str, context: &mut ConversionContext) -> Arc<SchemaDocument> {
        let root = RegistrySchema { schema: schema.into(), schema_type: Some("AVRO".into()), references: Vec::new(), rule_set: None, metadata: None };
        converter.document(root, "events-value".into(), 1, context).await.unwrap()
    }

    #[tokio::test]
    async fn configured_avro_reader_modes_admit_tombstones_and_primitive_bindings() {
        assert!(configured("avro.use.logical.type.converters", "invalid").is_err());
        for mode in ["specific.avro.reader", "schema.reflection"] {
            assert!(configured(mode, "invalid").is_err());
            let mut converter = configured(mode, "true").unwrap();
            let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
            assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, rtdb_types::Value::Null);
            let writer = document(&mut converter, r#""int""#, &mut context).await;
            let key = converter.bind_reader("events", 1, writer, None, &mut context).await.unwrap();
            let Decoded::Data(value) = converter.bindings.get_mut(&key).unwrap().decode(&[14], &converter.options, &mut converter.next_avro_union, &mut context).unwrap() else { panic!("expected decoded primitive") };
            assert_eq!(value.value, rtdb_types::Value::I32(7));
        }
    }

    #[tokio::test]
    async fn reader_binding_rejects_class_dependent_schemas_after_configuration() {
        for mode in ["specific.avro.reader", "schema.reflection"] {
            let mut converter = configured(mode, "true").unwrap();
            let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
            for schema in [r#"{"type":"record","name":"Missing","fields":[]}"#, r#"{"type":"int","custom":"property"}"#] {
                let writer = document(&mut converter, schema, &mut context).await;
                assert!(converter.bind_reader("events", 1, writer, None, &mut context).await.is_err());
                assert!(converter.bindings.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn explicit_specific_reader_binding_bypasses_primitive_class_construction() {
        let mut converter = configured("specific.avro.reader", "true").unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let writer = document(&mut converter, r#"{"type":"int","custom":"property"}"#, &mut context).await;
        let reader = document(&mut converter, r#""long""#, &mut context).await;
        assert!(converter.bind_reader("events", 1, writer, Some(reader), &mut context).await.is_ok());
    }

    #[test]
    fn avro_accepts_name_scrubbing_and_validates_its_boolean() {
        for value in ["true", "false"] {
            let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([
                ("schema.registry.url".into(), "http://registry.invalid".into()), ("scrub.invalid.names".into(), value.into()),
            ]) };
            assert!(RegistryConverter::open(&spec, RegistryFormat::Avro, false).is_ok());
        }
        let spec = PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([
            ("schema.registry.url".into(), "http://registry.invalid".into()), ("scrub.invalid.names".into(), "invalid".into()),
        ]) };
        assert!(RegistryConverter::open(&spec, RegistryFormat::Avro, false).is_err());
        assert!(RegistryConverter::open(&spec, RegistryFormat::Json, false).is_ok());
    }
}
