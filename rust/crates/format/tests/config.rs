use prost::Message;
use rtdb_format::{config::*, database::proto::{DatabaseMode, database_config::{Log, Storage}}, generated::xtdb};

#[test]
fn database_defaults_have_explicit_wire_presence() {
    let config = decode_database_yaml("{}").unwrap();
    assert_eq!(config, default_database_config());
    assert_eq!(config.mode, Some(DatabaseMode::ReadWrite as i32));
    assert_eq!(config.critical, Some(false));
    assert_eq!(config.partitions, Some(1));
    let Some(Log::InMemoryLog(log)) = config.log else { panic!() };
    assert_eq!(log.epoch, Some(0));
    assert_eq!(log.replay_buffer_size, Some(4096));
    assert!(matches!(config.storage, Some(Storage::InMemoryStorage(_))));
}

#[test]
fn database_yaml_dedents_and_preserves_log_and_storage_epochs() {
    let config = decode_database_yaml("\n        log: !Local\n          path: /var/lib/xtdb/log\n          epoch: 4\n          useInstantSourceForNonTx: true\n        storage: !Local\n          path: /var/lib/xtdb/store\n          epoch: 7\n        mode: read-only\n        critical: true\n    ").unwrap();
    let Some(Log::LocalLog(log)) = config.log else { panic!() };
    let Some(Storage::LocalStorage(storage)) = config.storage else { panic!() };
    assert_eq!(log.path.as_deref(), Some("/var/lib/xtdb/log"));
    assert_eq!(log.epoch, Some(4));
    assert_eq!(storage.epoch, Some(7));
    assert_eq!(config.mode, Some(DatabaseMode::ReadOnly as i32));
    assert_eq!(config.critical, Some(true));
}

#[test]
fn kafka_defaults_are_materialized_before_durable_encoding() {
    let config = decode_database_yaml("log: !Kafka\n  cluster: my-kafka\n  topic: db-log\n  autoCreateTopic: false").unwrap();
    let Some(Log::OtherLog(log)) = config.log else { panic!() };
    assert_eq!(log.type_url, "proto.xtdb.com/xtdb.kafka.proto.KafkaLogConfig");
    let log = xtdb::kafka::proto::KafkaLogConfig::decode(log.value.as_slice()).unwrap();
    assert_eq!(log.log_cluster_alias.as_deref(), Some("my-kafka"));
    assert_eq!(log.replica_cluster_alias.as_deref(), Some("my-kafka"));
    assert_eq!(log.replica_topic.as_deref(), Some("db-log-replica"));
    assert_eq!(log.epoch, Some(0));
}

#[test]
fn provider_alias_excludes_inline_secrets_from_durable_config() {
    let config = decode_database_yaml("storage: !Remote\n  epoch: 5\n  objectStore: !S3\n    bucket: data\n    remote: my-s3\n    credentials:\n      accessKey: discarded-access\n      secretKey: discarded-secret").unwrap();
    let Some(Storage::RemoteStorage(storage)) = config.storage else { panic!() };
    assert_eq!(storage.epoch, Some(5));
    let any = storage.object_store.unwrap();
    let provider = xtdb::aws::proto::S3ObjectStoreConfig::decode(any.value.as_slice()).unwrap();
    assert_eq!(provider.remote.as_deref(), Some("my-s3"));
    assert!(provider.credentials.is_none());
    assert_eq!(provider.path_style_access_enabled, Some(false));
    assert!(provider.region.is_none());

    let config = decode_database_yaml("storage: !Remote\n  objectStore: !Azure\n    storageAccount: account\n    container: data\n    remote: my-azure\n    storageAccountKey: discarded-key").unwrap();
    let Some(Storage::RemoteStorage(storage)) = config.storage else { panic!() };
    let provider = xtdb::azure::proto::AzureBlobStorageConfig::decode(storage.object_store.unwrap().value.as_slice()).unwrap();
    assert_eq!(provider.remote.as_deref(), Some("my-azure"));
    assert!(provider.storage_account_key.is_none());
}

#[test]
fn external_source_indexers_are_explicit_and_nested() {
    let config = decode_database_yaml("externalSource: !Postgres\n  remote: source-db\n  slotName: slot\n  publicationName: pub\n  indexer: !DirectMirror\n  ignoreTables: [docs, public.docs, audit.events]").unwrap();
    let wire = xtdb::postgres::proto::PostgresSourceConfig::decode(config.external_source.unwrap().value.as_slice()).unwrap();
    assert_eq!(wire.ignore_tables, vec!["public.docs", "audit.events"]);
    assert_eq!(wire.indexer.unwrap().type_url, "proto.xtdb.com/xtdb.postgres.proto.DirectMirrorConfig");
    assert!(decode_database_yaml("externalSource: !Postgres\n  remote: db\n  slotName: slot\n  publicationName: pub").is_err());

    let config = decode_database_yaml("externalSource: !KafkaConnect\n  remote: kafka\n  topic: docs\n  connectConfig:\n    key.converter: org.apache.kafka.connect.storage.StringConverter\n    value.converter: org.apache.kafka.connect.json.JsonConverter\n    value.converter.schemas.enable: false\n  indexer: !Docs\n    table: public.docs").unwrap();
    let wire = xtdb::kafka::connectsrc::proto::KafkaConnectSourceConfig::decode(config.external_source.unwrap().value.as_slice()).unwrap();
    assert_eq!(wire.connect_config["value.converter.schemas.enable"], "false");
    let indexer = wire.indexer.unwrap();
    assert_eq!(indexer.type_url, "proto.xtdb.com/xtdb.kafka.connectsrc.proto.DocsIndexerConfig");
    assert_eq!(xtdb::kafka::connectsrc::proto::DocsIndexerConfig::decode(indexer.value.as_slice()).unwrap().table.as_deref(), Some("public.docs"));
}

#[test]
fn only_node_config_performs_environment_substitution() {
    let input = "log: !Local\n  path: !Env LOG_PATH\nstorage: !InMemory\npartitions: !Env PARTITIONS";
    assert!(decode_database_yaml(input).is_err());
    let mut node = parse_yaml(input).unwrap();
    resolve_env_vars(&mut node, &|key| match key { "LOG_PATH" => Some("/data/log".into()), "PARTITIONS" => Some("1".into()), _ => None }).unwrap();
    let config = decode_database_value(node).unwrap();
    let Some(Log::LocalLog(log)) = config.log else { panic!() };
    assert_eq!(log.path.as_deref(), Some("/data/log"));
    let mut missing = parse_yaml("path: !Env MISSING").unwrap();
    assert!(resolve_env_vars(&mut missing, &|_| None).is_err());
}

#[test]
fn config_rejects_unknown_fields_null_nonnullable_fields_and_zero_partitions() {
    for yaml in ["partitons: 1", "partitions: 0", "partitions: null", "mode: READ_ONLY", "storage: !Local {path: data, typo: 1}", "log: !InMemory {epoch: 2147483648}", "critical: true\ncritical: false", "log: !Absent", "storage: null"] {
        assert!(decode_database_yaml(yaml).is_err(), "{yaml}");
    }
    assert_eq!(decode_database_yaml("partitions: 2").unwrap().partitions, Some(2));
    assert!(decode_database_yaml("externalSource: null").unwrap().external_source.is_none());
}
