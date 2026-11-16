use std::{collections::{BTreeMap, VecDeque}, sync::Arc};
use async_trait::async_trait;
use futures::StreamExt;
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;
use super::{ConnectData, ConversionContext, Converter, PluginSpec, error};
mod security;
mod rules;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RegistryFormat { Avro, Json }
pub(super) struct RegistryConverter {
    client: Option<(reqwest::Client, Arc<MemoryReservation>)>,
    urls: Vec<reqwest::Url>,
    security: security::Security,
    format: RegistryFormat,
    is_key: bool,
    options: BTreeMap<String, String>,
    schemas: BTreeMap<(String, u32), Arc<CachedSchema>>,
    order: VecDeque<(String, u32)>,
    capacity: usize,
    maximum_schema_bytes: usize,
    next_avro_union: i32,
}
struct CachedSchema { plan: Plan, read_rules: rules::ReadRules, _charges: Vec<Arc<MemoryReservation>> }
enum Plan { Avro(super::avro::AvroPlan), Json(super::json_schema::JsonPlan) }
#[derive(Deserialize, Serialize)]
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
#[derive(Deserialize, Serialize)]
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
        let capacity = options.get("max.schemas.per.subject").map(|value| value.parse::<usize>().map_err(|cause| error("invalid-plugin-config", cause))).transpose()?.unwrap_or(1000);
        if capacity == 0 { return Err(error("invalid-plugin-config", "Schema cache capacity must be positive")); }
        for key in ["scrub.invalid.names"] {
            if super::converter::option_bool(options, key, false)? { return Err(error("invalid-plugin-config", format!("Native converter option '{key}' requires its schema mapping extension"))); }
        }
        for key in ["use.latest.version", "specific.avro.reader", "schema.reflection"] {
            if super::converter::option_bool(options, key, false)? { return Err(rtdb_types::Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-registry-option", format!("Registry option '{key}' requires a native reader-schema adapter"))); }
        }
        if options.contains_key("use.latest.with.metadata") { return Err(rtdb_types::Anomaly::unsupported("xtdb.kafka-connect-source/unsupported-registry-option", "Registry metadata selection requires a native reader-schema adapter")); }
        Ok(Self { client: None, urls, security, format, is_key, options: options.clone(), schemas: BTreeMap::new(), order: VecDeque::new(), capacity, maximum_schema_bytes: 256 * 1024, next_avro_union: 0 })
    }
    async fn fetch(&mut self, path: &[String], context: &mut ConversionContext) -> Result<RegistrySchema> {
        serde_json::from_value(self.request(path, None, context).await?).map_err(|cause| error("schema-registry", cause))
    }
    async fn request(&mut self, path: &[String], body: Option<&Json>, context: &mut ConversionContext) -> Result<Json> {
        context.reserve(self.maximum_schema_bytes.checked_mul(4).ok_or_else(|| error("allocation-overflow", "Schema response bound overflow"))?)?;
        if self.client.is_none() { self.client = Some(security::client(&self.options, context)?); }
        let (client, charge) = self.client.as_ref().expect("registry client initialized");
        context.retain(Arc::clone(charge))?;
        let client = client.clone();
        let mut failure = None;
        for base in &self.urls {
            let mut url = base.clone();
            url.path_segments_mut().map_err(|_| error("invalid-plugin-config", "Schema Registry URL cannot contain path segments"))?.pop_if_empty().extend(path.iter().map(String::as_str));
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
    async fn schema(&mut self, topic: &str, id: u32, context: &mut ConversionContext) -> Result<Arc<CachedSchema>> {
        let cache_key = (topic.to_owned(), id);
        if let Some(schema) = self.schemas.get(&cache_key).cloned() {
            self.order.retain(|cached| cached != &cache_key); self.order.push_back(cache_key);
            for charge in &schema._charges { context.retain(Arc::clone(charge))?; }
            return Ok(schema);
        }
        while self.schemas.len() >= self.capacity { if let Some(id) = self.order.pop_front() { self.schemas.remove(&id); } }
        let root = self.fetch(&["schemas".into(), "ids".into(), id.to_string()], context).await?;
        let expected = match self.format { RegistryFormat::Avro => "AVRO", RegistryFormat::Json => "JSON" };
        if root.schema_type.as_deref().unwrap_or("AVRO") != expected { return Err(error("schema-registry", "Schema Registry format differs from the configured converter")); }
        let subject = self.subject(topic, &root.schema)?;
        let lookup = serde_json::to_value(&root).map_err(|cause| error("schema-registry", cause))?;
        let lookup = self.request(&["subjects".into(), subject], Some(&lookup), context).await?;
        let version = lookup.get("version").and_then(Json::as_i64).and_then(|version| i32::try_from(version).ok()).ok_or_else(|| error("schema-registry", "Subject lookup has no valid schema version"))?;
        if lookup.get("id").and_then(Json::as_u64) != Some(u64::from(id)) { return Err(error("schema-registry", "Subject lookup returned a different schema ID")); }
        let mut pending = VecDeque::from(root.references); let mut references = BTreeMap::new();
        while let Some(reference) = pending.pop_front() {
            if references.contains_key(&reference.name) { continue; }
            if references.len() >= 128 { return Err(error("schema-registry", "Schema reference graph exceeds its limit")); }
            let fetched = self.fetch(&["subjects".into(), reference.subject, "versions".into(), reference.version.to_string()], context).await?;
            pending.extend(fetched.references); references.insert(reference.name, fetched.schema);
        }
        let rule_bytes = root.rule_set.as_ref().map_or(0, |rules| rules.to_string().len());
        let bytes = references.values().try_fold(root.schema.len().checked_add(rule_bytes).ok_or_else(|| error("allocation-overflow", "Schema rule size overflow"))?, |bytes, text| bytes.checked_add(text.len()).ok_or_else(|| error("allocation-overflow", "Schema graph size overflow")))?;
        let rule_bound = rules::ReadRules::allocation_bound(root.rule_set.as_ref(), &self.options)?;
        let charge = context.reserve(bytes.checked_mul(512).and_then(|bytes| bytes.checked_add(rule_bound)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| error("allocation-overflow", "Schema graph allocation overflow"))?)?;
        let read_rules = rules::ReadRules::compile(root.rule_set.as_ref(), &self.options)?;
        let plan = match self.format {
            RegistryFormat::Avro => Plan::Avro(super::avro::AvroPlan::parse_version_index(&root.schema, &references.values().cloned().collect::<Vec<_>>(), &self.options, Some(version), &mut self.next_avro_union)?),
            RegistryFormat::Json => Plan::Json(super::json_schema::JsonPlan::parse_version(&root.schema, &references, &self.options, Some(version))?),
        };
        let schema = Arc::new(CachedSchema { plan, read_rules, _charges: vec![charge] }); self.schemas.insert(cache_key.clone(), Arc::clone(&schema)); self.order.push_back(cache_key); Ok(schema)
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
        let Some(bytes) = bytes else { return Ok(ConnectData::null()); };
        if bytes.len() < 5 || bytes[0] != 0 { return Err(error("conversion-error", "Invalid Confluent framing header")); }
        let id = u32::from_be_bytes(bytes[1..5].try_into().expect("fixed schema ID"));
        let schema = self.schema(topic, id, context).await?;
        match &schema.plan {
            Plan::Avro(plan) => {
                let datum = plan.decode_datum(&bytes[5..], context)?;
                let mut value = &datum;
                while let apache_avro::types::Value::Union(_, nested) = value { value = nested; }
                schema.read_rules.check(matches!(value, apache_avro::types::Value::Null))?;
                plan.convert(datum)
            }
            Plan::Json(plan) => {
                let datum = plan.decode_datum(&bytes[5..], context)?;
                schema.read_rules.check(false)?;
                plan.convert(datum)
            }
        }
    }
}
