use std::collections::BTreeMap;
use std::path::Path;
use prost::Message;
use rtdb_auth::{AuthConfig, WorkloadGrantConfig, WorkloadIssuerConfig};
use rtdb_format::{database::proto::{DatabaseConfig, database_config::{Log, Storage}}, generated::xtdb::{aws, azure, gcp, kafka, postgres}, keys::{CURRENT_STORAGE_VERSION, storage_root}};
use rtdb_server::config::{ModuleSettings, NodeConfiguration, RemoteConfig};
use rtdb_storage::ObjectKey;
use rtdb_types::{SourcePosition, StorageEpoch};
use serde_json::{Value, json};
use crate::{DatabaseReport, QualificationReport};

pub(crate) fn describe(config: &NodeConfiguration, report: &mut QualificationReport) {
    report.authentication = Some(match &config.auth {
        AuthConfig::SingleRootUser { password } => json!({"kind":"single-root-user", "password_present": password.is_some()}),
        AuthConfig::UserList { users, rules, grants } => json!({"kind":"user-list", "users":users.keys().collect::<Vec<_>>(), "rules":rules, "grants":grants.iter().map(|(user, databases)| (user.clone(), databases.iter().map(|(database, access)| (database.clone(), access.as_str())).collect::<BTreeMap<_, _>>())).collect::<BTreeMap<_, _>>()}),
        AuthConfig::OpenIdConnect(oidc) => {
            let mut authentication = json!({"kind":"openid-connect", "issuer":endpoint(oidc.issuer_url.as_str(), "authn.issuerUrl", report), "client_id":oidc.client_id, "client_secret":"[redacted]", "rules":oidc.rules});
            if oidc.issuer_ca_pem.is_some() {
                authentication["issuer_ca_configured"] = json!(true);
            }
            if let Some(grants) = &oidc.workload_grants {
                authentication["workload_grants"] = describe_workload_grants(grants);
            }
            authentication
        }
        AuthConfig::ManagedWorkloadOidc(config) => {
            let mut authentication = json!({
                "kind": "managed-workload-oidc",
                "create": describe_workload_issuer(config.create(), "authn.create.issuerUrl", report),
                "assignment": describe_workload_issuer(config.assignment(), "authn.assignment.issuerUrl", report),
            });
            if let Some(scope) = config.scoped_audience() {
                authentication["scoped_audience"] = json!({
                    "issuer": endpoint(scope.issuer_url().as_str(), "authn.scopedAudience.issuerUrl", report),
                    "issuer_ca_configured": true,
                    "client_id_prefix": scope.client_id_prefix(),
                });
            }
            if let Some(issuer) = config.original_family() {
                authentication["original_family"] = describe_workload_issuer(issuer, "authn.originalFamily.issuerUrl", report);
            }
            if let Some(issuer) = config.original_family_readback() {
                authentication["original_family_readback"] = describe_workload_issuer(
                    issuer, "authn.originalFamilyReadback.issuerUrl", report,
                );
            }
            authentication
        }
    });
    for (name, remote) in &config.remotes {
        let path = format!("remotes.{name}");
        let description = match remote {
            RemoteConfig::Kafka { bootstrap_servers, properties, properties_file, schema_registry_url } => {
                report.feature("kafka", &path);
                if bootstrap_servers.is_empty() { report.gap("empty-kafka-bootstrap", &path, "Kafka bootstrap servers must be nonempty."); }
                let bootstrap = if bootstrap_servers.contains(['@', '/', '?', '#']) {
                    report.gap("invalid-kafka-bootstrap", &path, "Kafka bootstrap servers must contain broker addresses without URI credentials or query components.");
                    "[redacted-invalid-bootstrap]"
                } else { bootstrap_servers.as_str() };
                if properties.contains_key("bootstrap.servers") { report.gap("overridden-kafka-identity", &path, "A Kafka property overrides the declared bootstrap identity; normalize it into bootstrapServers before qualification."); }
                if properties_file.is_some() { report.gap("uninspected-properties-file", &path, "Properties files are not opened by this offline tool; inline and qualify their effective values explicitly."); }
                for key in properties.keys() {
                    if !KAFKA_PROPERTIES.contains(&key.as_str()) { report.gap("unqualified-kafka-property", format!("{path}.propertiesMap.{key}"), "This property has no qualified offline native mapping; review the native librdkafka contract before cutover."); }
                }
                #[cfg(feature = "kafka")]
                {
                    let values = properties.iter().map(|(key, value)| (key.clone(), value.expose().to_owned())).collect();
                    if rtdb_kafka::native_properties(&values).is_err() { report.gap("unsupported-kafka-auth-properties", &path, "Native Kafka property translation rejected this credential configuration."); }
                }
                json!({"kind":"kafka", "bootstrap_servers":bootstrap, "property_names":properties.keys().collect::<Vec<_>>(),
                    "properties_file":properties_file, "schema_registry":schema_registry_url.as_deref().map(|value| endpoint(value, &path, report)), "property_values":"[redacted]"})
            }
            RemoteConfig::S3 { .. } => { report.feature("s3", &path); json!({"kind":"s3", "credentials":"[redacted]"}) }
            RemoteConfig::Azure { .. } => { report.feature("azure", &path); json!({"kind":"azure", "credentials":"[redacted]"}) }
            RemoteConfig::Postgres { hostname, port, database, username, .. } => {
                report.feature("pg-source", &path);
                if hostname.is_empty() || database.is_empty() || username.is_empty() { report.gap("invalid-postgres-remote", &path, "PostgreSQL hostname, database and username must be nonempty."); }
                json!({"kind":"postgres", "hostname":hostname, "port":port, "database":database, "username":username, "password":"[redacted]"})
            }
        };
        report.remotes.insert(name.clone(), description);
    }
    for module in &config.modules {
        if matches!(module, ModuleSettings::CloudWatch { .. }) { report.feature("cloudwatch", "modules.CloudWatch"); }
        report.modules.push(match module {
            ModuleSettings::CloudWatch { namespace } => json!({"kind":"cloudwatch", "namespace":namespace}),
            ModuleSettings::AzureMonitor { .. } => json!({"kind":"azure-monitor", "connection_string":"[redacted]"}),
        });
    }
    report.runtime = Some(json!({
        "membership":if config.fixed_databases.is_some() { "fixed-ingest" } else { "durable-primary-catalog" },
        "read_only_databases":config.read_only_databases, "skipped_databases":config.skipped_databases,
        "default_timezone":config.node_timezone(), "indexer_enabled":config.indexer.enabled,
        "compactor_threads":config.compactor.threads, "gc_enabled":config.garbage.enabled,
        "rows_per_block":config.indexer.rows_per_block, "skipped_transactions":config.indexer.skip_transactions,
        "disk_cache":config.disk_cache.as_ref().map(|cache| json!({"path":cache.path, "maximum_bytes":cache.maximum_bytes, "maximum_ratio":cache.maximum_ratio})),
        "pgwire":config.server.as_ref().map(|server| json!({"host":server.host, "port":server.port, "read_only_port":server.read_only_port,
            "tls_keystore":server.ssl.as_ref().map(|ssl| &ssl.path), "tls_password":server.ssl.as_ref().map(|_| "[redacted]")})),
        "flight":config.flight_sql.as_ref().map(|flight| json!({"host":flight.host, "port":flight.port})),
        "tracing": {"enabled":config.tracing.enabled, "endpoint":endpoint(&config.tracing.endpoint, "tracer.endpoint", report)}
    }));
    if let Some(databases) = &config.fixed_databases {
        for (name, database) in databases { database_report(name, database, config, report); }
    } else {
        database_report("xtdb", &config.primary, config, report);
        report.gap("attached-databases-uninventoried", "databases", "Durable attached databases live in the primary catalog and cannot be discovered offline; supply their separately captured configurations in an ingest inventory report.");
    }
}

fn describe_workload_issuer(issuer: &WorkloadIssuerConfig, path: &str, report: &mut QualificationReport) -> Value {
    json!({
        "issuer": endpoint(issuer.issuer_url().as_str(), path, report),
        "issuer_ca_configured": true,
        "workload_grants": describe_workload_grants(issuer.grants()),
    })
}

fn describe_workload_grants(grants: &WorkloadGrantConfig) -> Value {
    let mut description = json!({"audience": grants.audience(), "client_id_prefix": grants.client_id_prefix()});
    if let Some(point) = grants.assignment() {
        description["assignment"] = json!({
            "relation": format!("{}.{}", point.table.schema, point.table.table),
            "row_id": point.id, "valid_at_micros": point.valid_at.0,
        });
    }
    description
}

fn database_report(name: &str, database: &DatabaseConfig, node: &NodeConfiguration, report: &mut QualificationReport) {
    let path = format!("databases.{name}");
    let partitions = database.partitions.unwrap_or(1);
    if partitions != 1 { report.gap("unsupported-partition-count", &path, "Only the production single-partition configuration is currently enabled."); }
    let log = match database.log.as_ref() {
        Some(Log::LocalLog(log)) => {
            let root = log.path.as_deref().unwrap_or_default();
            local_path(root, &path, report);
            epoch(log.epoch.unwrap_or(0), &path, report);
            json!({"kind":"local", "epoch":log.epoch.unwrap_or(0), "source_path":Path::new(root).join("LOG"), "replica_path":Path::new(root).join("REPLICA_LOG")})
        }
        Some(Log::InMemoryLog(log)) => {
            epoch(log.epoch.unwrap_or(0), &path, report);
            report.gap("ephemeral-log", &path, "In-memory logs do not provide a persistent handoff and restore boundary.");
            json!({"kind":"in-memory", "epoch":log.epoch.unwrap_or(0)})
        }
        Some(Log::OtherLog(any)) if kind(any) == "xtdb.kafka.proto.KafkaLogConfig" => {
            report.feature("kafka", &path);
            match kafka::proto::KafkaLogConfig::decode(any.value.as_slice()) {
                Ok(log) => {
                    let cluster = log.log_cluster_alias.as_deref().unwrap_or_default();
                    let topic = log.topic.as_deref().unwrap_or_default();
                    let replica_cluster = log.replica_cluster_alias.as_deref().unwrap_or(cluster);
                    let replica_topic = log.replica_topic.clone().unwrap_or_else(|| format!("{topic}-replica"));
                    remote(cluster, "kafka", node, &path, report); remote(replica_cluster, "kafka", node, &path, report);
                    if cluster == replica_cluster && topic == replica_topic { report.gap("aliased-log-identities", &path, "Source and replica logs must use distinct topics or clusters."); }
                    for topic in [topic, replica_topic.as_str()] {
                        if topic.is_empty() || topic.len() > 249 || matches!(topic, "." | "..") || !topic.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte)) {
                            report.gap("invalid-kafka-topic", &path, "Kafka topic names must satisfy the production topic-name constraint.");
                        }
                    }
                    epoch(log.epoch.unwrap_or(0), &path, report);
                    json!({"kind":"kafka", "epoch":log.epoch.unwrap_or(0), "source":{"cluster":cluster,"topic":topic}, "replica":{"cluster":replica_cluster,"topic":replica_topic}})
                }
                Err(_) => { report.gap("invalid-log-description", &path, "The native protobuf log description is invalid."); Value::Null }
            }
        }
        _ => { report.gap("unsupported-log-description", &path, "No native log description exists for this configuration."); Value::Null }
    };
    let storage = match database.storage.as_ref() {
        Some(Storage::InMemoryStorage(_)) | None => {
            report.gap("ephemeral-storage", &path, "In-memory storage does not provide protected files for cutover and rollback."); json!({"kind":"in-memory"})
        }
        Some(Storage::LocalStorage(storage)) => {
            let path_value = storage.path.as_deref().unwrap_or_default();
            local_path(path_value, &path, report);
            let root = root(storage.epoch.unwrap_or(0), &path, report);
            json!({"kind":"local", "storage_version":CURRENT_STORAGE_VERSION.0,"epoch":storage.epoch.unwrap_or(0), "root":root.as_ref().map(|root| Path::new(path_value).join(root))})
        }
        Some(Storage::RemoteStorage(storage)) => {
            if node.disk_cache.is_none() { report.gap("missing-disk-cache", &path, "Production remote storage requires a configured disk cache."); }
            let root = root(storage.epoch.unwrap_or(0), &path, report);
            let provider = storage.object_store.as_ref().map(|any| object_store(any, root.as_deref(), node, &path, report));
            json!({"kind":"remote", "storage_version":CURRENT_STORAGE_VERSION.0,"epoch":storage.epoch.unwrap_or(0), "provider":provider})
        }
    };
    let source = database.external_source.as_ref().map(|any| external_source(any, node, &path, report));
    report.databases.push(DatabaseReport { name:name.into(), partitions, mode:database.mode.unwrap_or_default(), log, storage, external_source:source });
}

fn object_store(any: &prost_types::Any, root: Option<&str>, node: &NodeConfiguration, path: &str, report: &mut QualificationReport) -> Value {
    match kind(any) {
        "xtdb.aws.proto.S3ObjectStoreConfig" => {
            report.feature("s3", path);
            let Ok(config) = aws::proto::S3ObjectStoreConfig::decode(any.value.as_slice()) else { return malformed(path, report); };
            nonempty(config.bucket.as_deref(), path, report);
            if let Some(alias) = &config.remote { remote(alias, "s3", node, path, report); }
            let prefix = prefix(config.prefix.as_deref(), root, path, report);
            #[cfg(feature = "s3")]
            if let Some(root) = root.and_then(|root| ObjectKey::new(root).ok()) {
                if rtdb_s3::S3Config::from_proto(&config).object_prefix(&root).is_err() { report.gap("invalid-native-prefix", path, "The production provider rejected its object prefix."); }
            }
            json!({"kind":"s3", "bucket":config.bucket, "region":config.region, "root":prefix, "endpoint":config.endpoint.as_deref().map(|value| endpoint(value,path,report)), "path_style":config.path_style_access_enabled, "credential_alias":config.remote, "credentials":"[redacted]"})
        }
        "xtdb.azure.proto.AzureBlobStorageConfig" => {
            report.feature("azure", path);
            let Ok(config) = azure::proto::AzureBlobStorageConfig::decode(any.value.as_slice()) else { return malformed(path, report); };
            nonempty(config.storage_account.as_deref(), path, report); nonempty(config.container.as_deref(), path, report);
            if let Some(alias) = &config.remote { remote(alias, "azure", node, path, report); }
            let prefix = prefix(config.prefix.as_deref(), root, path, report);
            #[cfg(feature = "azure")]
            if let Some(root) = root.and_then(|root| ObjectKey::new(root).ok()) {
                if rtdb_azure::AzureConfig::from_proto(&config).object_prefix(&root).is_err() { report.gap("invalid-native-prefix", path, "The production provider rejected its object prefix."); }
            }
            json!({"kind":"azure", "storage_account":config.storage_account, "container":config.container, "root":prefix,
                "endpoint":config.storage_account_endpoint.as_deref().map(|value| endpoint(value,path,report)), "credential_alias":config.remote,
                "managed_identity_client_id":config.user_managed_identity_client_id, "credentials":"[redacted]"})
        }
        "xtdb.gcp.proto.GcsObjectStoreConfig" => {
            report.feature("gcs", path);
            let Ok(config) = gcp::proto::GcsObjectStoreConfig::decode(any.value.as_slice()) else { return malformed(path, report); };
            nonempty(config.project_id.as_deref(), path, report); nonempty(config.bucket.as_deref(), path, report);
            let prefix = prefix(config.prefix.as_deref(), root, path, report);
            #[cfg(feature = "gcs")]
            if let Some(root) = root.and_then(|root| ObjectKey::new(root).ok()) {
                if rtdb_gcs::GcsConfig::from_proto(&config).object_prefix(&root).is_err() { report.gap("invalid-native-prefix", path, "The production provider rejected its object prefix."); }
            }
            json!({"kind":"gcs", "project_id":config.project_id, "bucket":config.bucket, "root":prefix, "credentials":"application-default-chain-uninspected"})
        }
        _ => { report.gap("unsupported-object-store", path, "No native object-store adapter is available for this protobuf description."); Value::Null }
    }
}

fn external_source(any: &prost_types::Any, node: &NodeConfiguration, path: &str, report: &mut QualificationReport) -> Value {
    match kind(any) {
        "xtdb.postgres.proto.PostgresSourceConfig" => {
            report.feature("pg-source", path);
            let Ok(config) = postgres::proto::PostgresSourceConfig::decode(any.value.as_slice()) else { return malformed(path, report); };
            remote(config.remote.as_deref().unwrap_or_default(), "postgres", node, path, report);
            #[cfg(feature = "pg-source")]
            if rtdb_pg_source::SourceConfig::decode(any).is_err() { report.gap("invalid-postgres-source", path, "The production PostgreSQL source description failed validation."); }
            if config.indexer.as_ref().is_some_and(|indexer| kind(indexer) != "xtdb.postgres.proto.DirectMirrorConfig") { report.gap("unsupported-postgres-indexer", path, "Only the native DirectMirror indexer is available."); }
            json!({"kind":"postgres", "remote":config.remote, "slot":config.slot_name, "publication":config.publication_name, "indexer":"direct-mirror", "ignore_tables":config.ignore_tables})
        }
        "xtdb.kafka.connectsrc.proto.KafkaConnectSourceConfig" => {
            use rtdb_format::generated::xtdb::kafka::connectsrc::proto::{KafkaConnectSourceConfig, DocsIndexerConfig};
            report.feature("kafka-connect", path);
            let Ok(config) = KafkaConnectSourceConfig::decode(any.value.as_slice()) else { return malformed(path, report); };
            #[cfg(feature = "kafka-connect")]
            if rtdb_kafka::connect::SourceConfig::decode(any).and_then(|config| config.validate()).is_err() {
                report.gap("invalid-connect-description", path, "The production Kafka Connect description failed structural validation.");
            }
            remote(config.remote.as_deref().unwrap_or_default(), "kafka", node, path, report);
            let table = config.indexer.as_ref().filter(|indexer| kind(indexer) == "xtdb.kafka.connectsrc.proto.DocsIndexerConfig")
                .and_then(|indexer| DocsIndexerConfig::decode(indexer.value.as_slice()).ok()).and_then(|config| config.table);
            if table.is_none() { report.gap("unsupported-connect-indexer", path, "Only the native Docs indexer is available."); }
            json!({"kind":"kafka-connect", "remote":config.remote, "topic":config.topic, "table":table, "property_names":config.connect_config.keys().collect::<Vec<_>>(), "property_values":"[redacted]"})
        }
        _ => { report.gap("unsupported-external-source", path, "This external source requires a native adapter."); Value::Null }
    }
}

fn kind(any: &prost_types::Any) -> &str { any.type_url.rsplit('/').next().unwrap_or("") }
fn malformed(path: &str, report: &mut QualificationReport) -> Value { report.gap("invalid-backend-description", path, "The native backend protobuf description is invalid."); Value::Null }
fn nonempty(value: Option<&str>, path: &str, report: &mut QualificationReport) { if value.is_none_or(str::is_empty) { report.gap("missing-backend-identity", path, "Backend identities must be present and nonempty."); } }
fn epoch(value: i32, path: &str, report: &mut QualificationReport) { if SourcePosition::from_epoch_offset(StorageEpoch(value), -1).is_err() { report.gap("invalid-log-epoch", path, "The log epoch is outside the production message-ID range."); } }
fn root(epoch: i32, path: &str, report: &mut QualificationReport) -> Option<String> {
    match storage_root(CURRENT_STORAGE_VERSION, StorageEpoch(epoch), 0, 1) {
        Ok(root) => Some(root), Err(_) => { report.gap("invalid-storage-epoch", path, "The storage epoch is outside the production storage-root range."); None }
    }
}
fn prefix(prefix: Option<&str>, root: Option<&str>, path: &str, report: &mut QualificationReport) -> Option<String> {
    let root = root?;
    let value = match prefix.filter(|value| !value.is_empty()) { Some(prefix) => format!("{prefix}/{root}"), None => root.into() };
    match ObjectKey::new(value) { Ok(value) => Some(value.to_string()), Err(_) => { report.gap("invalid-object-prefix", path, "The object prefix violates production object-key restrictions."); None } }
}
fn local_path(value: &str, path: &str, report: &mut QualificationReport) {
    if value.is_empty() || !Path::new(value).is_absolute() { report.gap("unresolved-local-path", path, "Use an absolute deployment path; this tool does not assume a service working directory."); }
}
fn endpoint(value: &str, path: &str, report: &mut QualificationReport) -> Option<String> {
    let Ok(mut url) = url::Url::parse(value) else { report.gap("invalid-endpoint", path, "The endpoint must be an absolute URL."); return None; };
    if url.host_str().is_none() || !matches!(url.scheme(), "http" | "https") { report.gap("invalid-endpoint", path, "The endpoint must be an HTTP or HTTPS URL with a host."); return None; }
    if !url.username().is_empty() || url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
        report.gap("endpoint-components-unqualified", path, "Endpoint userinfo, query and fragment components are redacted and require explicit provider qualification.");
    }
    let _ = url.set_username(""); let _ = url.set_password(None); url.set_query(None); url.set_fragment(None);
    Some(url.to_string())
}
fn remote(alias: &str, expected: &str, node: &NodeConfiguration, path: &str, report: &mut QualificationReport) {
    let actual = node.remotes.get(alias).map(|remote| match remote { RemoteConfig::Kafka { .. } => "kafka", RemoteConfig::S3 { .. } => "s3", RemoteConfig::Azure { .. } => "azure", RemoteConfig::Postgres { .. } => "postgres" });
    if actual != Some(expected) { report.gap("missing-or-wrong-remote", path, "A referenced remote alias is absent or names a different backend type."); }
}
const KAFKA_PROPERTIES: &[&str] = &["bootstrap.servers", "security.protocol", "sasl.mechanism", "sasl.mechanisms", "sasl.username", "sasl.password", "sasl.jaas.config", "ssl.ca.location", "ssl.certificate.location", "ssl.key.location", "ssl.key.password", "enable.idempotence", "acks", "compression.type", "linger.ms", "message.max.bytes", "request.timeout.ms", "delivery.timeout.ms", "client.id", "socket.timeout.ms", "max.in.flight.requests.per.connection"];

#[cfg(test)]
mod tests {
    use prost::Message;
    use prost_types::Any;
    use rtdb_columnar::MemoryBudget;
    use rtdb_format::generated::xtdb::postgres::proto::PostgresSourceConfig;
    use rtdb_server::config::NodeConfiguration;
    use serde_json::json;

    use crate::{InputFormat, qualify};
    use super::external_source;

    #[test]
    fn legacy_postgres_source_without_an_indexer_reports_direct_mirror() {
        let node = NodeConfiguration::from_yaml_with_environment(
            "remotes: {upstream: !Postgres {hostname: pg.example, database: source, username: reader, password: fixture}}",
            &|_| None,
            true,
        ).unwrap();
        let provenance = json!({
            "reference_revision": "a".repeat(40),
            "implementation_revision": "b".repeat(40),
            "server_artifact_sha256": "c".repeat(64),
            "migrator_artifact_sha256": "d".repeat(64),
        }).to_string();
        let memory = MemoryBudget::new(16 * 1024 * 1024);
        for (indexer_type, unsupported) in [
            (None, false),
            (Some("proto.xtdb.com/xtdb.postgres.proto.DirectMirrorConfig"), false),
            (Some("proto.xtdb.com/example.CustomIndexer"), true),
        ] {
            let mut report = qualify("databases: {}", InputFormat::IngestYaml, b"{}", provenance.as_bytes(), &memory).unwrap();
            assert!(report.configuration_gaps.is_empty());
            let source = Any {
                type_url: "proto.xtdb.com/xtdb.postgres.proto.PostgresSourceConfig".into(),
                value: PostgresSourceConfig {
                    remote: Some("upstream".into()),
                    slot_name: Some("legacy_slot".into()),
                    publication_name: Some("publication".into()),
                    indexer: indexer_type.map(|type_url| Any { type_url: type_url.into(), value: Vec::new() }),
                    ignore_tables: Vec::new(),
                }.encode_to_vec(),
            };
            let description = external_source(&source, &node, "databases.mirror", &mut report);
            if !unsupported { assert_eq!(description["indexer"], "direct-mirror"); }
            let mut expected = Vec::new();
            if !cfg!(feature = "pg-source") { expected.push("feature-not-compiled"); }
            if unsupported { expected.push("unsupported-postgres-indexer"); }
            let findings: Vec<_> = report.configuration_gaps.iter().map(|finding| finding.code).collect();
            assert_eq!(findings, expected, "{indexer_type:?}");
        }
    }
}
