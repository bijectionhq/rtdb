use std::collections::{BTreeMap, BTreeSet};
use prost::Message;
use prost_types::Any;
use rtdb_types::Result;
use super::error;

pub use rtdb_format::generated::xtdb::kafka::connectsrc::proto;
pub const SOURCE_TYPE: &str = "proto.xtdb.com/xtdb.kafka.connectsrc.proto.KafkaConnectSourceConfig";
pub const DOCS_TYPE: &str = "proto.xtdb.com/xtdb.kafka.connectsrc.proto.DocsIndexerConfig";

#[derive(Clone, Debug, PartialEq)]
pub struct SourceConfig { pub remote: String, pub topic: String, pub connect_config: BTreeMap<String, String>, pub indexer: Any }
impl SourceConfig {
    /// Durable decoding preserves historic configuration; validate before accepting a new attachment.
    pub fn decode(value: &Any) -> Result<Self> {
        if value.type_url != SOURCE_TYPE { return Err(error("invalid-config", "Unexpected Kafka Connect source protobuf type")); }
        let value = proto::KafkaConnectSourceConfig::decode(value.value.as_slice()).map_err(|cause| error("invalid-config", cause))?;
        Ok(Self { remote: value.remote.unwrap_or_default(), topic: value.topic.unwrap_or_default(), connect_config: value.connect_config,
            indexer: value.indexer.ok_or_else(|| error("invalid-config", "Kafka Connect indexer is required"))? })
    }
    pub fn encode(&self) -> Any {
        Any { type_url: SOURCE_TYPE.into(), value: proto::KafkaConnectSourceConfig { remote: Some(self.remote.clone()), topic: Some(self.topic.clone()),
            connect_config: self.connect_config.clone(), indexer: Some(self.indexer.clone()) }.encode_to_vec() }
    }
    pub fn validate(&self) -> Result<ConnectConfig> {
        if self.remote.is_empty() || self.topic.is_empty() { return Err(error("invalid-config", "Kafka remote and topic must be nonempty")); }
        ConnectConfig::parse(&self.connect_config)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResumeToken(pub i64);
impl ResumeToken {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let token = proto::KafkaConnectSourceToken::decode(bytes).map_err(|cause| error("invalid-token", cause))?;
        let offset = token.offset.unwrap_or_default();
        if offset < 0 { return Err(error("invalid-token", "Kafka resume offset is negative")); }
        Ok(Self(offset))
    }
    pub fn encode(self) -> Vec<u8> { proto::KafkaConnectSourceToken { offset: Some(self.0) }.encode_to_vec() }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginSpec { pub class_name: String, pub options: BTreeMap<String, String> }
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransformSpec { pub alias: String, pub plugin: PluginSpec, pub predicate: Option<String>, pub negate: bool }
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectConfig { pub key: PluginSpec, pub value: PluginSpec, pub transforms: Vec<TransformSpec>, pub predicates: BTreeMap<String, PluginSpec> }
impl ConnectConfig {
    pub fn parse(properties: &BTreeMap<String, String>) -> Result<Self> {
        let transforms = aliases(properties.get("transforms"), "transforms")?;
        let predicates = aliases(properties.get("predicates"), "predicates")?;
        for alias in &transforms {
            if let Some(predicate) = properties.get(&format!("transforms.{alias}.predicate")) {
                if !predicates.contains(predicate) { return Err(error("undeclared-predicate", format!("Transform '{alias}' references undeclared predicate '{predicate}'"))); }
            }
        }
        for key in properties.keys() {
            if !matches!(key.as_str(), "key.converter" | "value.converter" | "transforms" | "predicates")
                && !key.starts_with("key.converter.") && !key.starts_with("value.converter.")
                && !transforms.iter().any(|alias| key.starts_with(&format!("transforms.{alias}.")))
                && !predicates.iter().any(|alias| key.starts_with(&format!("predicates.{alias}."))) {
                return Err(error("unknown-connect-config-key", format!("Unknown connectConfig key '{key}'")));
            }
        }
        let transforms = transforms.into_iter().map(|alias| {
            let prefix = format!("transforms.{alias}");
            let class_name = properties.get(&format!("{prefix}.type")).cloned().ok_or_else(|| error("missing-transform-type", format!("Missing {prefix}.type")))?;
            let predicate = properties.get(&format!("{prefix}.predicate")).cloned();
            let negate = match properties.get(&format!("{prefix}.negate")) {
                None => false,
                Some(_) if predicate.is_none() => return Err(error("negate-without-predicate", format!("Transform '{alias}' has negate without predicate"))),
                Some(value) => boolean(value).ok_or_else(|| error("invalid-negate", format!("Invalid negate value '{value}'")))?,
            };
            Ok(TransformSpec { alias, predicate, negate, plugin: PluginSpec { class_name, options: subconfig(properties, &format!("{prefix}."), &["type", "predicate", "negate"]) } })
        }).collect::<Result<_>>()?;
        let predicates = predicates.into_iter().map(|alias| {
            let prefix = format!("predicates.{alias}.");
            let class_name = properties.get(&format!("{prefix}type")).cloned().ok_or_else(|| error("missing-predicate-type", format!("Missing {prefix}type")))?;
            Ok((alias, PluginSpec { class_name, options: subconfig(properties, &prefix, &["type"]) }))
        }).collect::<Result<_>>()?;
        let converter = |role: &str| -> Result<PluginSpec> {
            let class_name = properties.get(role).cloned().ok_or_else(|| error("missing-converter", format!("Missing '{role}' in connectConfig")).with_context("role", role.into()))?;
            Ok(PluginSpec { class_name, options: subconfig(properties, &format!("{role}."), &[]) })
        };
        Ok(Self { key: converter("key.converter")?, value: converter("value.converter")?, transforms, predicates })
    }
}
pub(super) fn boolean(value: &str) -> Option<bool> { match value.trim().to_ascii_lowercase().as_str() { "true" => Some(true), "false" => Some(false), _ => None } }
fn aliases(value: Option<&String>, key: &str) -> Result<Vec<String>> {
    let value = value.map_or("", String::as_str).trim();
    if value.is_empty() { return Ok(Vec::new()); }
    let aliases = value.split(',').map(|part| part.trim().to_owned()).collect::<Vec<_>>();
    let mut seen = BTreeSet::new();
    for alias in &aliases {
        if alias.is_empty() { return Err(error("empty-alias", format!("Empty alias in '{key}'"))); }
        if !seen.insert(alias) { return Err(error("duplicate-alias", format!("Duplicate alias '{alias}' in '{key}'"))); }
    }
    Ok(aliases)
}
fn subconfig(properties: &BTreeMap<String, String>, prefix: &str, excluded: &[&str]) -> BTreeMap<String, String> {
    properties.iter().filter_map(|(key, value)| key.strip_prefix(prefix).filter(|key| !excluded.contains(key)).map(|key| (key.into(), value.clone()))).collect()
}
