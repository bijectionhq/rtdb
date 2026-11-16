use std::{collections::{BTreeMap, VecDeque}, sync::Arc, time::Instant};
use async_trait::async_trait;
use futures::StreamExt;
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use super::{ConnectData, ConversionContext, Converter, PluginSpec, error};
mod security;
mod rules;
mod reader;
mod dlq;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistryFormat { Avro, Json }
pub(super) struct RegistryConverter {
    client: Option<(reqwest::Client, Arc<MemoryReservation>)>,
    urls: Vec<reqwest::Url>,
    security: security::Security,
    format: RegistryFormat,
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
struct SchemaDocument { root: RegistrySchema, references: BTreeMap<String, String>, subject: String, version: i32, bytes: usize, charge: Arc<MemoryReservation> }
struct WriterEntry { document: Arc<SchemaDocument>, key_charge: Arc<MemoryReservation> }
struct ReaderBinding { writer: Arc<SchemaDocument>, selected: Option<Arc<SchemaDocument>>, plan: ReaderPlan, migrations: Vec<rules::ReadRules>, read_rules: rules::ReadRules, _charges: Vec<Arc<MemoryReservation>> }
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
}
impl ReaderBinding {
    fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        context.retain(Arc::clone(&self.writer.charge))?;
        if let Some(selected) = &self.selected { context.retain(Arc::clone(&selected.charge))?; }
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
                    *output = JsonOutput::Prepared(super::json_schema::JsonPlan::parse_version(&document.root.schema, &document.references, options, Some(document.version))?);
                }
                let JsonOutput::Prepared(plan) = output else { unreachable!() };
                plan.convert(datum, context).map(Decoded::Data)
            }
            ReaderPlan::Avro { writer, selection, outputs } => {
                let (reader, datum) = match selection {
                    AvroSelection::Writer => {
                        let datum = writer.decode_datum(bytes, context)?;
                        (&*writer, writer.binary_result(datum, context)?)
                    }
                    AvroSelection::Reader { plan, decode } => {
                        let datum = match decode {
                            AvroRead::Binary(resolution) => writer.binary_result(resolution.decode_datum(bytes, context)?, context)?,
                            AvroRead::RawBytes => writer.decode_datum(bytes, context)?,
                            AvroRead::Migration => {
                                let datum = writer.decode_datum(bytes, context)?;
                                let datum = writer.binary_result(datum, context)?;
                                let json = writer.to_json_datum(datum, context)?;
                                for migration in &self.migrations {
                                    if let Some(rule) = migration.check(false)? { return Ok(Decoded::Dlq { rule, original: Original::Json(super::json_value::JsonValue::from_serde(json)) }); }
                                }
                                plan.from_json_datum(json, context)?
                            }
                        };
                        (&*plan, datum)
                    }
                };
                let is_null = datum.is_null();
                if let Some(rule) = self.read_rules.check(is_null)? { return Ok(Decoded::Dlq { rule, original: Original::Framed }); }
                if is_null { return Err(error("conversion-error", "Avro converter returned no schema and value")); }
                let (output, datum) = writer.prepare_output(reader, datum, context)?;
                if !outputs.contains_key(&output) {
                    let (plan, branch) = match output { super::avro::AvroOutput::Writer => (&*writer, None), super::avro::AvroOutput::Reader { branch } => (reader, branch) };
                    outputs.insert(output, plan.connect_plan(branch, Some(self.writer.version), next_union, context)?);
                }
                outputs[&output].convert(datum, next_union, context).map(Decoded::Data)
            }
        }
    }
}
#[derive(Clone, PartialEq, Deserialize, Serialize)]
struct RegistrySchema {
    schema: String,
    #[serde(rename = "schemaType", default, skip_serializing_if = "Option::is_none")]
    schema_type: Option<String>,
    #[serde(default)]
    references: Vec<Reference>,
    #[serde(rename = "ruleSet", default, skip_serializing_if = "Option::is_none")]
    rule_set: Option<Json>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<Json>,
}
#[derive(Clone, PartialEq, Deserialize, Serialize)]
struct Reference { name: String, subject: String, version: i32 }
impl RegistryConverter {
    pub fn open(spec: &PluginSpec, format: RegistryFormat, is_key: bool) -> Result<Self> {
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
        for key in ["scrub.invalid.names"] {
            if super::converter::option_bool(options, key, false)? { return Err(error("invalid-plugin-config", format!("Native converter option '{key}' requires its schema mapping extension"))); }
        }
        for key in ["specific.avro.reader", "schema.reflection"] {
            if super::converter::option_bool(options, key, false)? { return Err(rtdb_types::Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-registry-option", format!("Registry option '{key}' requires a native reader-schema adapter"))); }
        }
        Ok(Self { client: None, urls, security, format, is_key, options: options.clone(), schemas: BTreeMap::new(), order: VecDeque::new(), capacity, maximum_schema_bytes: 256 * 1024, next_avro_union: 0, reader_selection, latest, bindings: BTreeMap::new(), binding_order: VecDeque::new(), dlq: BTreeMap::new() })
    }
    async fn fetch(&mut self, path: &[String], context: &mut ConversionContext) -> Result<RegistrySchema> {
        serde_json::from_value(self.request(path, None, context).await?).map_err(|cause| error("schema-registry", cause))
    }
    async fn request(&mut self, path: &[String], body: Option<&Json>, context: &mut ConversionContext) -> Result<Json> {
        self.request_query(path, &[], body, context).await
    }
    async fn request_query(&mut self, path: &[String], query: &[(String, String)], body: Option<&Json>, context: &mut ConversionContext) -> Result<Json> {
        context.reserve(self.maximum_schema_bytes.checked_mul(4).ok_or_else(|| error("allocation-overflow", "Schema response bound overflow"))?)?;
        if self.client.is_none() { self.client = Some(security::client(&self.options, context)?); }
        let (client, charge) = self.client.as_ref().expect("registry client initialized");
        context.retain(Arc::clone(charge))?;
        let client = client.clone();
        let mut failure = None;
        for base in &self.urls {
            let mut url = base.clone();
            url.path_segments_mut().map_err(|_| error("invalid-plugin-config", "Schema Registry URL cannot contain path segments"))?.pop_if_empty().extend(path.iter().map(String::as_str));
            if !query.is_empty() { url.query_pairs_mut().extend_pairs(query.iter().map(|(key, value)| (key.as_str(), value.as_str()))); }
            let headers = self.security.headers(&url, context).await?;
            url.set_username("").map_err(|_| error("invalid-plugin-config", "Registry URL cannot remove user info"))?;
            url.set_password(None).map_err(|_| error("invalid-plugin-config", "Registry URL cannot remove user info"))?;
            let request = match body { Some(body) => client.post(url).json(body), None => client.get(url) }.headers(headers);
            let response = match request.send().await { Ok(response) => response, Err(cause) => { failure = Some(error("schema-registry", cause.without_url())); continue; } };
            if !response.status().is_success() {
                let cause = error("schema-registry", format!("Schema Registry returned HTTP {}", response.status()));
                if response.status().is_server_error() { failure = Some(cause); continue; } return Err(cause);
            }
            if response.content_length().is_some_and(|length| length > self.maximum_schema_bytes as u64) { return Err(error("schema-registry", "Schema response exceeds its byte limit")); }
            let mut bytes = Vec::new(); let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|cause| error("schema-registry", cause.without_url()))?;
                if bytes.len().checked_add(chunk.len()).is_none_or(|size| size > self.maximum_schema_bytes) { return Err(error("schema-registry", "Schema response exceeds its byte limit")); }
                bytes.extend_from_slice(&chunk);
            }
            return serde_json::from_slice(&bytes).map_err(|cause| error("schema-registry", cause));
        }
        Err(failure.unwrap_or_else(|| error("schema-registry", "No Schema Registry endpoint is configured")))
    }
    async fn schema(&mut self, topic: &str, id: u32, context: &mut ConversionContext) -> Result<Arc<SchemaDocument>> {
        let key_charge = context.reserve(topic.len().checked_mul(4).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(|| error("allocation-overflow", "Schema cache key allocation overflow"))?)?;
        let cache_key = (topic.to_owned(), id);
        if let Some(entry) = self.schemas.get(&cache_key) {
            let schema = Arc::clone(&entry.document);
            self.order.retain(|cached| cached != &cache_key); self.order.push_back(cache_key);
            context.retain(Arc::clone(&schema.charge))?;
            context.retain(Arc::clone(&entry.key_charge))?;
            return Ok(schema);
        }
        while self.schemas.len() >= self.capacity { if let Some(id) = self.order.pop_front() { self.schemas.remove(&id); } }
        let root = self.fetch(&["schemas".into(), "ids".into(), id.to_string()], context).await?;
        self.validate_format(&root)?;
        let subject = self.subject(topic, &root.schema)?;
        let lookup = serde_json::to_value(&root).map_err(|cause| error("schema-registry", cause))?;
        let lookup = self.request(&["subjects".into(), subject.clone()], Some(&lookup), context).await?;
        let version = lookup.get("version").and_then(Json::as_i64).and_then(|version| i32::try_from(version).ok()).ok_or_else(|| error("schema-registry", "Subject lookup has no valid schema version"))?;
        if lookup.get("id").and_then(Json::as_u64) != Some(u64::from(id)) { return Err(error("schema-registry", "Subject lookup returned a different schema ID")); }
        let schema = self.document(root, subject, version, context).await?;
        self.schemas.insert(cache_key.clone(), WriterEntry { document: Arc::clone(&schema), key_charge }); self.order.push_back(cache_key); Ok(schema)
    }
    fn validate_format(&self, root: &RegistrySchema) -> Result<()> {
        let expected = match self.format { RegistryFormat::Avro => "AVRO", RegistryFormat::Json => "JSON" };
        if root.schema_type.as_deref().unwrap_or("AVRO") != expected { return Err(error("schema-registry", "Schema Registry format differs from the configured converter")); }
        Ok(())
    }
    async fn document(&mut self, root: RegistrySchema, subject: String, version: i32, context: &mut ConversionContext) -> Result<Arc<SchemaDocument>> {
        self.validate_format(&root)?;
        let mut pending = VecDeque::from(root.references.clone()); let mut references = BTreeMap::new();
        while let Some(reference) = pending.pop_front() {
            if references.contains_key(&reference.name) { continue; }
            if references.len() >= 128 { return Err(error("schema-registry", "Schema reference graph exceeds its limit")); }
            let fetched = self.fetch(&["subjects".into(), reference.subject, "versions".into(), reference.version.to_string()], context).await?;
            pending.extend(fetched.references); references.insert(reference.name, fetched.schema);
        }
        let root_bytes = serde_json::to_vec(&root).map_err(|cause| error("schema-registry", cause))?.len();
        let bytes = references.iter().try_fold(root_bytes.checked_add(subject.len()).ok_or_else(|| error("allocation-overflow", "Schema subject size overflow"))?, |bytes, (name, text)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(text.len())).ok_or_else(|| error("allocation-overflow", "Schema graph size overflow")))?;
        let charge = context.reserve(bytes.checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Schema document allocation overflow"))?)?;
        if self.format == RegistryFormat::Json {
            context.reserve(root.schema.len().checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "JSON schema syntax allocation overflow"))?)?;
            serde_json::from_str::<Json>(&root.schema).map_err(|cause| error("json-schema", cause))?;
        }
        if let Some(rule_set) = &root.rule_set {
            let rule_bytes = serde_json::to_vec(rule_set).map_err(|cause| error("schema-registry", cause))?.len();
            context.reserve(rule_bytes.checked_mul(64).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| error("allocation-overflow", "Schema rules allocation overflow"))?)?;
            rules::validate(rule_set)?;
        }
        Ok(Arc::new(SchemaDocument { root, references, subject, version, bytes, charge }))
    }
    fn plan_charge(&self, document: &SchemaDocument, context: &mut ConversionContext) -> Result<Arc<MemoryReservation>> {
        let rule_bound = rules::ReadRules::allocation_bound(document.root.rule_set.as_ref(), &self.options)?;
        context.reserve(document.bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(rule_bound)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| error("allocation-overflow", "Schema graph allocation overflow")))
    }
    fn avro_plan(&self, document: &SchemaDocument) -> Result<Arc<super::avro::AvroDatumPlan>> {
        super::avro::AvroDatumPlan::parse(&document.root.schema, &document.references.values().cloned().collect::<Vec<_>>(), &self.options).map(Arc::new)
    }
    async fn reader_document(&mut self, writer: &SchemaDocument, id: u32, context: &mut ConversionContext) -> Result<Option<Arc<SchemaDocument>>> {
        context.reserve(self.reader_selection.request_bound(&writer.subject)?)?;
        let Some(key) = self.reader_selection.key(&writer.subject, id) else { return Ok(None); };
        if let Some(document) = self.latest.get(&key, Instant::now()) {
            context.retain(Arc::clone(&document.charge))?;
            return Ok(Some(document));
        }
        let (path, query) = self.reader_selection.request(&writer.subject).ok_or_else(|| error("schema-registry", "Reader schema selection lost its request"))?;
        let value = self.request_query(&path, &query, None, context).await?;
        let version = value.get("version").and_then(Json::as_i64).and_then(|version| i32::try_from(version).ok()).ok_or_else(|| error("schema-registry", "Selected reader schema has no valid version"))?;
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
            let root = serde_json::from_value(value).map_err(|cause| error("schema-registry", cause))?;
            let document = self.document(root, writer.subject.clone(), returned, context).await?;
            self.plan_charge(&document, context)?;
            match self.format {
                RegistryFormat::Avro => { self.avro_plan(&document)?; }
                RegistryFormat::Json => { serde_json::from_str::<Json>(&document.root.schema).map_err(|cause| error("json-schema", cause))?; }
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
                let writer = self.avro_plan(&writer)?;
                let selection = if let Some(selected) = &selected {
                    let reader = self.avro_plan(selected)?;
                    let decode = if !migrations.is_empty() { AvroRead::Migration }
                        else if writer.raw_bytes() { AvroRead::RawBytes }
                        else { AvroRead::Binary(super::avro::AvroResolution::compile(&writer, &reader, context)?) };
                    AvroSelection::Reader { plan: reader, decode }
                } else { AvroSelection::Writer };
                ReaderPlan::Avro { writer, selection, outputs: BTreeMap::new() }
            }
        };
        let binding = ReaderBinding { writer, selected, plan, migrations, read_rules, _charges: charges };
        self.bindings.remove(&key); self.binding_order.retain(|entry| entry != &key);
        while self.bindings.len() >= self.capacity { if let Some(key) = self.binding_order.pop_front() { self.bindings.remove(&key); } }
        self.bindings.insert(key.clone(), binding); self.binding_order.push_back(key.clone());
        Ok(key)
    }
    fn subject(&self, topic: &str, schema: &str) -> Result<String> {
        let key = if self.is_key { "key.subject.name.strategy" } else { "value.subject.name.strategy" };
        let strategy = self.options.get(key).map_or("io.confluent.kafka.serializers.subject.TopicNameStrategy", String::as_str);
        if strategy == "io.confluent.kafka.serializers.subject.TopicNameStrategy" { return Ok(format!("{topic}-{}", if self.is_key { "key" } else { "value" })); }
        let schema: Json = serde_json::from_str(schema).map_err(|cause| error("schema-registry", cause))?;
        let name = match self.format {
            RegistryFormat::Json => schema.get("title").and_then(Json::as_str).map(str::to_owned),
            RegistryFormat::Avro => schema.get("name").and_then(Json::as_str).map(|name| if name.contains('.') { name.into() } else if let Some(namespace) = schema.get("namespace").and_then(Json::as_str).filter(|name| !name.is_empty()) { format!("{namespace}.{name}") } else { name.into() }),
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
