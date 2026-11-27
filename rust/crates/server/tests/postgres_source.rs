#![cfg(feature = "pg-source")]

use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::MemoryBudget;
use rtdb_format::config::decode_database_yaml;
use rtdb_metrics::Registry;
use rtdb_server::{config::NodeConfiguration, open_backend_resources};
use rtdb_storage::IoRegion;
use rtdb_types::AnomalyCategory;
use serde_json::Value;

#[tokio::test]
async fn postgres_source_distinguishes_missing_aliases_from_other_remote_types() {
    let config = NodeConfiguration::from_yaml_with_environment(
        r#"
logClusters:
  broker: !Kafka
    bootstrapServers: broker.invalid:9092
remotes:
  objects: !S3
    accessKey: unused
    secretKey: unused
  blobs: !AzureBlob
    storageAccountKey: unused
  upstream: !Postgres
    hostname: upstream.invalid
    database: app
    username: replicator
    password: unused
"#,
        &|_| None,
        false,
    ).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let metrics = Registry::new("fixture", Arc::clone(&memory)).unwrap();
    let (_, sources, _) = open_backend_resources(&config, &memory, &metrics).unwrap();
    let factory = &sources["xtdb.postgres.proto.PostgresSourceConfig"];
    let source = |alias: &str| decode_database_yaml(&format!(
        "externalSource: !Postgres\n  remote: {alias}\n  slotName: test_slot\n  publicationName: test_publication\n  indexer: !DirectMirror {{}}",
    )).unwrap().external_source.unwrap();

    let postgres = source("upstream");
    factory.validate(&postgres).unwrap();
    factory.open(&postgres, "mirror", IoRegion::new()).await.unwrap();

    for (alias, actual_type) in [
        ("absent", None),
        ("broker", Some("KafkaCluster")),
        ("objects", Some("S3Remote")),
        ("blobs", Some("AzureRemote")),
    ] {
        let source = source(alias);
        let validation_error = factory.validate(&source).unwrap_err();
        let opening_error = factory.open(&source, "mirror", IoRegion::new()).await
            .err().expect("opening the source unexpectedly succeeded");
        assert_eq!(opening_error, validation_error);
        assert_eq!(validation_error.category, AnomalyCategory::Incorrect);

        let mut context = BTreeMap::from([("alias".into(), Value::from(alias))]);
        let (code, message) = match actual_type {
            Some(actual_type) => {
                context.insert("actualType".into(), Value::from(actual_type));
                ("xtdb.postgres/wrong-remote-type", format!("remote '{alias}' is a {actual_type}, expected a !Postgres remote"))
            }
            None => (
                "xtdb.postgres/missing-remote",
                format!("no remote configured with alias '{alias}' — add a '!Postgres' entry under 'remotes:' in node config"),
            ),
        };
        assert_eq!(validation_error.code, code);
        assert_eq!(validation_error.message.as_deref(), Some(message.as_str()));
        assert_eq!(validation_error.context, context);
    }
}
