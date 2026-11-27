use std::collections::BTreeMap;

use prost::Message;
use prost_types::Any;
use rtdb_types::{Anomaly, Result};
pub use serde_yaml::Value as Yaml;

use crate::database::proto::{self as db, database_config::{Log, Storage}};
use crate::generated::xtdb::{aws::proto as aws, azure::proto as azure, gcp::proto as gcp, kafka::proto as kafka, postgres::proto as postgres};

/// Decodes the durable database configuration accepted by `ATTACH DATABASE`.
/// Environment substitution belongs to node configuration and is not performed here.
pub fn decode_database_yaml(text: &str) -> Result<db::DatabaseConfig> {
    decode_database_value(parse_yaml(&trim_indent(text))?)
}

pub fn parse_yaml(text: &str) -> Result<Yaml> {
    serde_yaml::from_str(text).map_err(|error| invalid(format!("Invalid YAML: {error}")))
}

/// Resolves `!Env` values recursively while preserving provider tags and mapping keys.
/// The supplied lookup returns `None` for an unset environment variable.
pub fn resolve_env_vars(value: &mut Yaml, lookup: &impl Fn(&str) -> Option<String>) -> Result<()> {
    match value {
        Yaml::Tagged(tagged) if tagged.tag == "Env" => {
            let name = scalar_string(&tagged.value, "!Env")?;
            let resolved = lookup(&name).ok_or_else(|| invalid(format!("Environment variable '{name}' not found")))?;
            *value = Yaml::String(resolved);
        }
        Yaml::Tagged(tagged) => resolve_env_vars(&mut tagged.value, lookup)?,
        Yaml::Sequence(values) => for value in values { resolve_env_vars(value, lookup)?; },
        Yaml::Mapping(values) => for (_, value) in values { resolve_env_vars(value, lookup)?; },
        _ => (),
    }
    Ok(())
}

pub fn decode_database_value(value: Yaml) -> Result<db::DatabaseConfig> {
    let mut fields = YamlFields::new(value, "database")?;
    let log = fields.take("log").map(decode_log).transpose()?.unwrap_or_else(default_log);
    let storage = fields.take("storage").map(decode_storage).transpose()?.unwrap_or_else(default_storage);
    let mode = match fields.string("mode")?.as_deref().unwrap_or("read-write") {
        "read-write" => db::DatabaseMode::ReadWrite,
        "read-only" => db::DatabaseMode::ReadOnly,
        _ => return Err(invalid("Database mode must be 'read-write' or 'read-only'")),
    };
    let external_source = fields.take_nullable("externalSource").map(decode_external_source).transpose()?;
    let critical = fields.boolean("critical")?.unwrap_or(false);
    let partitions = fields.integer("partitions")?.unwrap_or(1);
    if partitions < 1 { return Err(invalid(format!("partitions must be >= 1, got {partitions}"))); }
    fields.finish_known(&["critical", "externalSource", "log", "mode", "partitions", "storage"])?;
    Ok(db::DatabaseConfig { log: Some(log), storage: Some(storage), mode: Some(mode as i32), external_source, critical: Some(critical), partitions: Some(partitions) })
}

pub fn default_database_config() -> db::DatabaseConfig {
    db::DatabaseConfig { log: Some(default_log()), storage: Some(default_storage()), mode: Some(db::DatabaseMode::ReadWrite as i32),
        external_source: None, critical: Some(false), partitions: Some(1) }
}

fn default_log() -> Log { Log::InMemoryLog(db::InMemoryLog { epoch: Some(0), replay_buffer_size: Some(4096) }) }
fn default_storage() -> Storage { Storage::InMemoryStorage(db::InMemoryStorage { epoch: Some(0) }) }

pub fn decode_log(value: Yaml) -> Result<Log> {
    let (tag, mut fields) = YamlFields::tagged(value, "log")?;
    let epoch = Some(fields.integer("epoch")?.unwrap_or(0));
    let log = match tag.as_str() {
        "!InMemory" => Log::InMemoryLog(db::InMemoryLog { epoch, replay_buffer_size: Some(fields.integer("replayBufferSize")?.unwrap_or(4096)) }),
        "!Local" => {
            let path = fields.required_string("path")?;
            fields.boolean("useInstantSourceForNonTx")?;
            Log::LocalLog(db::LocalLog { path: Some(path), epoch })
        }
        "!Kafka" => {
            let cluster = fields.required_string("cluster")?;
            let topic = fields.required_string("topic")?;
            let replica_cluster = fields.string("replicaCluster")?.unwrap_or_else(|| cluster.clone());
            let replica_topic = fields.string("replicaTopic")?.unwrap_or_else(|| format!("{topic}-replica"));
            fields.boolean("autoCreateTopic")?;
            Log::OtherLog(pack("xtdb.kafka.proto.KafkaLogConfig", kafka::KafkaLogConfig {
                log_cluster_alias: Some(cluster), topic: Some(topic), epoch,
                replica_cluster_alias: Some(replica_cluster), replica_topic: Some(replica_topic),
            }))
        }
        _ => return Err(invalid(format!("Unknown log configuration tag '{tag}'"))),
    };
    fields.finish()?;
    Ok(log)
}

pub fn decode_storage(value: Yaml) -> Result<Storage> {
    let (tag, mut fields) = YamlFields::tagged(value, "storage")?;
    let epoch = Some(fields.integer("epoch")?.unwrap_or(0));
    let storage = match tag.as_str() {
        "!InMemory" => Storage::InMemoryStorage(db::InMemoryStorage { epoch }),
        "!Local" => Storage::LocalStorage(db::LocalStorage { epoch, path: Some(fields.required_string("path")?) }),
        "!Remote" => Storage::RemoteStorage(db::RemoteStorage { epoch, object_store: Some(decode_object_store(fields.require("objectStore")?)?) }),
        _ => return Err(invalid(format!("Unknown storage configuration tag '{tag}'"))),
    };
    fields.finish()?;
    Ok(storage)
}

pub const S3_STORE: &str = "xtdb.aws.proto.S3ObjectStoreConfig";
pub const S3_JVM_LITERAL_STORE: &str = "xtdb.aws.proto.S3JvmLiteralObjectStoreConfig";

pub fn unpack_s3_store(config: &Any) -> Result<aws::S3ObjectStoreConfig> {
    match config.type_url.rsplit('/').next() {
        Some(S3_STORE) => Ok(aws::S3ObjectStoreConfig::decode(config.value.as_slice()).map_err(|error| invalid(error.to_string()))?),
        Some(S3_JVM_LITERAL_STORE) => aws::S3JvmLiteralObjectStoreConfig::decode(config.value.as_slice())
            .map_err(|error| invalid(error.to_string()))?.store.ok_or_else(|| invalid("Literal S3 storage is missing its provider configuration")),
        _ => Err(invalid("Expected a Classic S3 object store configuration")),
    }
}

pub fn decode_object_store(value: Yaml) -> Result<Any> {
    let (tag, mut fields) = YamlFields::tagged(value, "objectStore")?;
    let object_store = match tag.as_str() {
        "!S3" => {
            let bucket = Some(fields.required_string("bucket")?);
            let region = fields.nullable_string("region")?;
            let prefix = fields.nullable_string("prefix")?;
            let endpoint = fields.nullable_string("endpoint")?;
            let path_style_access_enabled = Some(fields.boolean("pathStyleAccessEnabled")?.unwrap_or(false));
            let remote = fields.nullable_string("remote")?;
            let credentials = fields.take_nullable("credentials").map(|value| {
                let mut credentials = YamlFields::new(value, "credentials")?;
                let result = aws::S3Credentials { access_key: Some(credentials.required_string("accessKey")?), secret_key: Some(credentials.required_string("secretKey")?) };
                credentials.finish()?;
                Ok::<_, Anomaly>(result)
            }).transpose()?;
            let store = aws::S3ObjectStoreConfig {
                bucket, region, prefix, endpoint, path_style_access_enabled,
                credentials: if remote.is_some() { None } else { credentials }, remote,
            };
            match fields.string("keyLayout")?.as_deref() {
                None | Some("legacy_rtdb") => pack(S3_STORE, store),
                Some("jvm_literal") if store.region.as_ref().is_some_and(|region| !region.is_empty()) =>
                    pack(S3_JVM_LITERAL_STORE, aws::S3JvmLiteralObjectStoreConfig { store: Some(store) }),
                Some("jvm_literal") => return Err(invalid("The JVM literal S3 layout requires an explicit region")),
                Some(layout) => return Err(invalid(format!("Unknown Classic S3 key layout '{layout}'"))),
            }
        }
        "!Azure" => {
            let storage_account = Some(fields.required_string("storageAccount")?);
            let container = Some(fields.required_string("container")?);
            let prefix = fields.nullable_string("prefix")?;
            let storage_account_key = fields.nullable_string("storageAccountKey")?;
            let user_managed_identity_client_id = fields.nullable_string("userManagedIdentityClientId")?;
            let storage_account_endpoint = fields.nullable_string("storageAccountEndpoint")?;
            let remote = fields.nullable_string("remote")?;
            pack("xtdb.azure.proto.AzureBlobStorageConfig", azure::AzureBlobStorageConfig {
                storage_account, container, prefix, user_managed_identity_client_id, storage_account_endpoint,
                storage_account_key: if remote.is_some() { None } else { storage_account_key }, remote,
            })
        }
        "!GoogleCloud" => pack("xtdb.gcp.proto.GcsObjectStoreConfig", gcp::GcsObjectStoreConfig {
            project_id: Some(fields.required_string("projectId")?), bucket: Some(fields.required_string("bucket")?), prefix: fields.nullable_string("prefix")?,
        }),
        _ => return Err(invalid(format!("Unknown object store configuration tag '{tag}'"))),
    };
    fields.finish()?;
    Ok(object_store)
}

pub fn decode_external_source(value: Yaml) -> Result<Any> {
    let (tag, mut fields) = YamlFields::tagged(value, "externalSource")?;
    let external_source = match tag.as_str() {
        "!Postgres" => {
            let remote = Some(fields.required_string("remote")?);
            let slot_name = Some(fields.required_string("slotName")?);
            let publication_name = Some(fields.required_string("publicationName")?);
            let (tag, indexer_fields) = YamlFields::tagged(fields.require("indexer")?, "Postgres indexer")?;
            if tag != "!DirectMirror" { return Err(invalid(format!("Unknown Postgres indexer tag '{tag}'"))); }
            indexer_fields.finish()?;
            let indexer = Some(pack("xtdb.postgres.proto.DirectMirrorConfig", postgres::DirectMirrorConfig {}));
            let mut ignore_tables = Vec::new();
            for value in fields.sequence("ignoreTables")?.unwrap_or_default() {
                let dotted = scalar_string(&value, "ignoreTables")?;
                let table = rtdb_types::TableRef::parse(&dotted)?;
                let normalized = format!("{}.{}", table.schema, table.table);
                if !ignore_tables.contains(&normalized) { ignore_tables.push(normalized); }
            }
            pack("xtdb.postgres.proto.PostgresSourceConfig", postgres::PostgresSourceConfig { remote, slot_name, publication_name, indexer, ignore_tables })
        }
        "!KafkaConnect" => {
            use crate::generated::xtdb::kafka::connectsrc::proto::{KafkaConnectSourceConfig, DocsIndexerConfig};
            let remote = Some(fields.required_string("remote")?);
            let topic = Some(fields.required_string("topic")?);
            let connect_config = fields.string_map("connectConfig")?.unwrap_or_default();
            let (tag, mut indexer_fields) = YamlFields::tagged(fields.require("indexer")?, "Kafka Connect indexer")?;
            if tag != "!Docs" { return Err(invalid(format!("Unknown Kafka Connect indexer tag '{tag}'"))); }
            let table = Some(indexer_fields.required_string("table")?);
            indexer_fields.finish()?;
            let indexer = Some(pack("xtdb.kafka.connectsrc.proto.DocsIndexerConfig", DocsIndexerConfig { table }));
            pack("xtdb.kafka.connectsrc.proto.KafkaConnectSourceConfig", KafkaConnectSourceConfig { remote, topic, connect_config, indexer })
        }
        _ => return Err(invalid(format!("Unknown external source configuration tag '{tag}'"))),
    };
    fields.finish()?;
    Ok(external_source)
}

fn pack(name: &str, value: impl Message) -> Any { Any { type_url: format!("proto.xtdb.com/{name}"), value: value.encode_to_vec() } }

pub struct YamlFields { fields: BTreeMap<String, Yaml>, context: String }
impl YamlFields {
    pub fn into_entries(self) -> BTreeMap<String, Yaml> { self.fields }
    pub fn new(value: Yaml, context: impl Into<String>) -> Result<Self> {
        let context = context.into();
        let Yaml::Mapping(mapping) = value else { return Err(invalid(format!("{context} must be a mapping"))); };
        let mut fields = BTreeMap::new();
        for (key, value) in mapping {
            let key = scalar_string(&key, &context)?;
            if fields.insert(key.clone(), value).is_some() { return Err(invalid(format!("Duplicate field '{key}' in {context}"))); }
        }
        Ok(Self { fields, context })
    }
    pub fn tagged(value: Yaml, context: &str) -> Result<(String, Self)> {
        let Yaml::Tagged(tagged) = value else { return Err(invalid(format!("{context} requires a configuration tag"))); };
        let tag = tagged.tag.to_string();
        let value = if tagged.value == Yaml::Null { Yaml::Mapping(Default::default()) } else { tagged.value };
        Ok((tag, Self::new(value, context)?))
    }
    pub fn take(&mut self, key: &str) -> Option<Yaml> { self.fields.remove(key) }
    pub fn take_nullable(&mut self, key: &str) -> Option<Yaml> { self.take(key).filter(|value| value != &Yaml::Null) }
    pub fn require(&mut self, key: &str) -> Result<Yaml> { self.take(key).ok_or_else(|| invalid(format!("Missing '{key}' in {}", self.context))) }
    pub fn string(&mut self, key: &str) -> Result<Option<String>> { self.take(key).map(|value| scalar_string(&value, key)).transpose() }
    pub fn nullable_string(&mut self, key: &str) -> Result<Option<String>> { self.take_nullable(key).map(|value| scalar_string(&value, key)).transpose() }
    pub fn required_string(&mut self, key: &str) -> Result<String> { scalar_string(&self.require(key)?, key) }
    pub fn integer(&mut self, key: &str) -> Result<Option<i32>> {
        self.string(key)?.map(|value| value.parse().map_err(|_| invalid(format!("'{key}' must be a 32-bit integer")))).transpose()
    }
    pub fn boolean(&mut self, key: &str) -> Result<Option<bool>> {
        self.string(key)?.map(|value| match value.as_str() { "true" => Ok(true), "false" => Ok(false), _ => Err(invalid(format!("'{key}' must be a boolean"))) }).transpose()
    }
    pub fn sequence(&mut self, key: &str) -> Result<Option<Vec<Yaml>>> {
        self.take(key).map(|value| match value { Yaml::Sequence(values) => Ok(values), _ => Err(invalid(format!("'{key}' must be a sequence"))) }).transpose()
    }
    pub fn string_map(&mut self, key: &str) -> Result<Option<BTreeMap<String, String>>> {
        self.take(key).map(|value| Self::new(value, key)?.fields.into_iter().map(|(name, value)| Ok((name, scalar_string(&value, key)?))).collect()).transpose()
    }
    /// Rejects a leftover key with the reference YAML decoder's wording, which lists the keys the type declares.
    pub fn finish_known(self, known: &[&str]) -> Result<()> {
        if let Some(key) = self.fields.keys().next() { return Err(invalid(format!("Unknown property '{key}'. Known properties are: {}", known.join(", ")))); }
        Ok(())
    }
    pub fn finish(self) -> Result<()> {
        if let Some(key) = self.fields.keys().next() { return Err(invalid(format!("Unknown field '{key}' in {}", self.context))); }
        Ok(())
    }
}

pub fn scalar_string(value: &Yaml, context: &str) -> Result<String> {
    match value {
        Yaml::String(value) => Ok(value.clone()),
        Yaml::Bool(value) => Ok(value.to_string()),
        Yaml::Number(value) => Ok(value.to_string()),
        _ => Err(invalid(format!("'{context}' must be a scalar"))),
    }
}

fn trim_indent(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let indent = lines.iter().filter(|line| !line.trim().is_empty()).map(|line| line.chars().take_while(|c| c.is_whitespace()).count()).min().unwrap_or(0);
    lines.iter().enumerate().filter(|(index, line)| !((*index == 0 || *index + 1 == lines.len()) && line.trim().is_empty()))
        .map(|(_, line)| line.char_indices().nth(indent).map_or("", |(offset, _)| &line[offset..])).collect::<Vec<_>>().join("\n")
}

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb/invalid-database-config", message) }
