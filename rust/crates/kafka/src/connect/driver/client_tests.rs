use super::*;
use std::collections::BTreeMap;
use crate::connect::{DateEnvironmentInput, DocsIndexer};

fn driver(memory: &Arc<MemoryBudget>) -> SourceDriver {
    let mut cluster = KafkaClusterConfig::new("");
    cluster.properties_map = BTreeMap::from([
        ("security.protocol".into(), "SSL".into()),
        ("ssl.truststore.location".into(), format!("{}/../tls/tests/fixtures/empty.jks", env!("CARGO_MANIFEST_DIR"))),
        ("ssl.truststore.password".into(), "12345678".into()),
        ("max.poll.records".into(), "3".into()), ("auto.offset.reset".into(), "none".into()),
        ("key.deserializer".into(), "ignored.Deserializer".into()), ("value.deserializer".into(), "ignored.Deserializer".into()),
        ("enable.auto.commit".into(), "true".into()), ("enable.auto.offset.store".into(), "true".into()),
    ]);
    let source = SourceConfig { remote: "remote".into(), topic: "topic".into(), indexer: prost_types::Any { type_url: "test/indexer".into(), value: vec![] },
        connect_config: BTreeMap::from([("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()), ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into())]) };
    let environment = DateEnvironment::from_inputs(DateEnvironmentInput {
        language: "en".into(), script: String::new(), region: "US".into(), variant: String::new(), unicode: BTreeMap::new(), timezone: "UTC".into(),
        initialized_at: 1_600_000_000_000, observed_at: 1_600_000_000_000, constructed_at: 1_600_000_000_000,
    }, &MemoryBudget::new(256 << 20)).unwrap();
    SourceDriver::new(cluster, source, SourceOptions::default(), PluginRegistry::default(), Box::new(DocsIndexer::new(rtdb_types::TableRef::new("public", "docs"))), Arc::clone(memory), environment).unwrap()
}

#[test]
fn source_consumer_keeps_tls_after_assignment_configuration_is_dropped() {
    let memory = MemoryBudget::new(32 << 20); let mut driver = driver(&memory);
    let configuration = driver.consumer_configuration().unwrap();
    assert_eq!(driver.options.maximum_batch_records, 3);
    assert_eq!(configuration.get("enable.auto.commit"), Some("false"));
    assert_eq!(configuration.get("enable.auto.offset.store"), Some("false"));
    assert_eq!(configuration.get("auto.offset.reset"), Some("error"));
    assert_eq!(configuration.get("statistics.interval.ms"), Some("1000"));
    for name in ["max.poll.records", "key.deserializer", "value.deserializer", "ssl.truststore.location", "ssl.truststore.password"] { assert!(configuration.get(name).is_none()); }
    let consumer: BaseConsumer<SourceContext> = configuration.create_with_context(SourceContext { progress: Arc::clone(&driver.progress), topic: "topic".into(), partition: 0 }).unwrap();
    drop(configuration); drop(driver);
    assert!(memory.used_bytes() > 0);
    drop(consumer);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_tls_admission_uses_the_assignment_budget_before_opening_a_consumer() {
    let memory = MemoryBudget::new(0); let mut driver = driver(&memory);
    assert_eq!(driver.consumer_configuration().unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
#[cfg(unix)]
fn source_resolves_cluster_providers_before_cutting_batch_size_and_constructing_the_consumer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("source.properties");
    let ignored = root.path().join("ignored.properties");
    let cluster = rdkafka::mocking::MockCluster::new(1).unwrap();
    std::fs::write(&path, format!("brokers={}\nlimit=\u{661}\u{667} \n", cluster.bootstrap_servers())).unwrap();
    std::fs::write(&ignored, "setting=\\uGGGG").unwrap();
    let memory = MemoryBudget::new(32 << 20);
    let mut driver = driver(&memory);
    driver.cluster.properties_map = BTreeMap::from([
        ("config.providers".into(), "file".into()),
        ("config.providers.file.class".into(), "org.apache.kafka.common.config.provider.FileConfigProvider".into()),
        ("config.providers.file.param.allowed.paths".into(), root.path().to_str().unwrap().into()),
        ("bootstrap.servers".into(), format!("${{file:{}:brokers}}", path.display())),
        ("max.poll.records".into(), format!("${{file:{}:limit}}", path.display())),
        ("enable.auto.commit".into(), format!("${{file:{}:setting}}", ignored.display())),
    ]);
    let configuration = driver.consumer_configuration().unwrap();
    assert_eq!(driver.options.maximum_batch_records, 17);
    assert_eq!(configuration.get("bootstrap.servers"), Some(cluster.bootstrap_servers().as_str()));
    assert_eq!(configuration.get("enable.auto.commit"), Some("false"));
    assert!(!configuration.config_map().keys().any(|name| name.starts_with("config.providers")));
    let consumer: BaseConsumer<SourceContext> = configuration.create_with_context(SourceContext { progress: Arc::clone(&driver.progress), topic: "topic".into(), partition: 0 }).unwrap();
    assert!(memory.used_bytes() > 0);
    drop(consumer);
    drop(configuration);
    drop(driver);
    assert_eq!(memory.used_bytes(), 0);
}
