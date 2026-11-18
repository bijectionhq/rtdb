use super::*;
use std::collections::BTreeMap;
use super::super::{DateEnvironment, PluginRegistry, SourceConfig, SourceContext, SourceDriver, SourceOptions};
use crate::{KafkaClusterConfig, connect::{DateEnvironmentInput, DocsIndexer}};
use rdkafka::consumer::{BaseConsumer, Consumer};

fn configuration() -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("queued.max.messages.kbytes", "8192");
    config
}

fn driver(memory: &Arc<MemoryBudget>) -> SourceDriver {
    let source = SourceConfig { remote: "remote".into(), topic: "topic".into(),
        indexer: prost_types::Any { type_url: "test/indexer".into(), value: vec![] },
        connect_config: BTreeMap::from([
            ("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
            ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
        ]) };
    let environment = DateEnvironment::from_inputs(DateEnvironmentInput {
        language: "en".into(), script: String::new(), region: "US".into(), variant: String::new(),
        unicode: BTreeMap::new(), timezone: "UTC".into(), initialized_at: 1_600_000_000_000,
        observed_at: 1_600_000_000_000, constructed_at: 1_600_000_000_000,
    }, &MemoryBudget::new(256 << 20)).unwrap();
    SourceDriver::new(KafkaClusterConfig::new(""), source, SourceOptions::default(), PluginRegistry::default(),
        Box::new(DocsIndexer::new(rtdb_types::TableRef::new("public", "docs"))), Arc::clone(memory), environment).unwrap()
}

fn context(driver: &SourceDriver) -> SourceContext {
    SourceContext { progress: Arc::clone(&driver.progress), topic: "topic".into(), partition: 0 }
}

#[test]
fn source_native_limits_follow_the_real_queued_clamp_and_finite_receive_policy() {
    let limits = Limits::from_config(&configuration()).unwrap();
    assert_eq!(limits, Limits { fetch: 8 << 20, receive: 9 << 20, capacity: 26 << 20 });
    let mut small = configuration();
    small.set("queued.max.messages.kbytes", "128").set("message.max.bytes", (2 << 20).to_string());
    assert_eq!(Limits::from_config(&small).unwrap(), Limits { fetch: 2 << 20, receive: 3 << 20, capacity: 14 << 20 });
}

#[test]
fn source_limit_probe_ignores_unrelated_native_setters() {
    let mut config = configuration();
    config.set("plugin.library.paths", "/nonexistent/native-plugin-fixture")
        .set("not.a.native.property", "not-validated-by-a-scalar-probe");
    assert_eq!(Limits::from_config(&config).unwrap(), Limits { fetch: 8 << 20, receive: 9 << 20, capacity: 26 << 20 });
    assert_eq!(config.get("plugin.library.paths"), Some("/nonexistent/native-plugin-fixture"));
}

#[test]
fn source_native_limits_preserve_explicit_larger_fetch_and_receive_demands() {
    let mut explicit = configuration();
    explicit.set("fetch.max.bytes", (50 << 20).to_string());
    assert_eq!(Limits::from_config(&explicit).unwrap(), Limits { fetch: 50 << 20, receive: 51 << 20, capacity: 110 << 20 });
    explicit.set("receive.message.max.bytes", (60 << 20).to_string());
    assert_eq!(Limits::from_config(&explicit).unwrap(), Limits { fetch: 50 << 20, receive: 60 << 20, capacity: 128 << 20 });
}

#[test]
fn source_native_limits_reject_incompatible_receive_and_fetch_without_reserving_capacity() {
    let memory = MemoryBudget::new(64 << 20);
    let mut config = configuration();
    config.set("fetch.max.bytes", (8 << 20).to_string()).set("receive.message.max.bytes", (8 << 20).to_string());
    assert!(install(&mut config, &memory).unwrap_err().code.ends_with("invalid-config"));
    assert_eq!(memory.used_bytes(), 0);
    config.set("fetch.max.bytes", "1024").set("receive.message.max.bytes", (8 << 20).to_string());
    assert!(install(&mut config, &memory).unwrap_err().code.ends_with("invalid-config"));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_native_limit_integer_product_and_aggregate_overflow_have_no_fallback() {
    assert_eq!(native_bytes(i32::MAX as usize).unwrap(), i32::MAX as usize);
    assert!(native_bytes(i32::MAX as usize + 1).unwrap_err().code.ends_with("allocation-overflow"));
    assert!(capacity(usize::MAX).unwrap_err().code.ends_with("allocation-overflow"));
    assert!(capacity(usize::MAX / 2).unwrap_err().code.ends_with("allocation-overflow"));
    let mut config = configuration();
    config.set("queued.max.messages.kbytes", (i32::MAX / 1024).to_string());
    assert_eq!(Limits::from_config(&config).unwrap(), Limits { fetch: 50 << 20, receive: 51 << 20, capacity: 110 << 20 });
}

#[test]
fn source_native_capacity_is_admitted_exactly_before_configuration_publication() {
    let mut config = configuration();
    let memory = MemoryBudget::new((26 << 20) - 1);
    assert_eq!(install(&mut config, &memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
    assert!(config.get("fetch.max.bytes").is_none());
    assert!(config.get("receive.message.max.bytes").is_none());
    let memory = MemoryBudget::new(26 << 20);
    install(&mut config, &memory).unwrap();
    assert_eq!(memory.used_bytes(), 26 << 20);
    assert_eq!(config.get("fetch.max.bytes"), Some("8388608"));
    assert_eq!(config.get("receive.message.max.bytes"), Some("9437184"));
    let copy = config.clone();
    drop(config);
    assert_eq!(memory.used_bytes(), 26 << 20);
    drop(copy);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn actual_source_configuration_denial_releases_its_input_and_native_capacity_work() {
    let memory = MemoryBudget::new(64 << 20);
    let mut admitted = driver(&memory);
    admitted.cluster.properties_map.insert("max.poll.records".into(), "17".into());
    let config = admitted.consumer_configuration().unwrap();
    let total = memory.used_bytes();
    assert!(total >= 26 << 20);
    drop(config);
    drop(admitted);
    assert_eq!(memory.used_bytes(), 0);
    let short = MemoryBudget::new(total - 1);
    let mut denied = driver(&short);
    denied.cluster.properties_map.insert("max.poll.records".into(), "17".into());
    assert_eq!(denied.consumer_configuration().unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(denied.options.maximum_batch_records, 17);
    assert_eq!(short.used_bytes(), 0);
}

#[test]
fn source_consumer_retains_the_capacity_after_its_configuration_and_driver_drop() {
    let memory = MemoryBudget::new(64 << 20);
    let mut driver = driver(&memory);
    let config = driver.consumer_configuration().unwrap();
    let consumer: BaseConsumer<SourceContext> = config.create_with_context(context(&driver)).unwrap();
    drop(config);
    drop(driver);
    assert!(memory.used_bytes() >= 26 << 20);
    drop(consumer);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_source_consumer_creation_finishes_before_its_configuration_capacity_is_released() {
    let memory = MemoryBudget::new(64 << 20);
    let mut driver = driver(&memory);
    driver.cluster.properties_map.insert("group.protocol".into(), "classic".into());
    driver.cluster.properties_map.insert("max.poll.interval.ms".into(), "1000".into());
    driver.cluster.properties_map.insert("session.timeout.ms".into(), "6000".into());
    let config = driver.consumer_configuration().unwrap();
    let result: rdkafka::error::KafkaResult<BaseConsumer<SourceContext>> = config.create_with_context(context(&driver));
    assert!(matches!(result, Err(rdkafka::error::KafkaError::ClientCreation(_))));
    assert!(memory.used_bytes() >= 26 << 20);
    drop(config);
    drop(driver);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn actual_source_metadata_retains_capacity_after_native_consumer_configuration_and_driver_drop() {
    let cluster = rdkafka::mocking::MockCluster::new(1).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let mut driver = driver(&memory);
    driver.cluster.properties_map.insert("bootstrap.servers".into(), cluster.bootstrap_servers());
    let config = driver.consumer_configuration().unwrap();
    let consumer: BaseConsumer<SourceContext> = config.create_with_context(context(&driver)).unwrap();
    let metadata = consumer.fetch_metadata(None, std::time::Duration::from_secs(10)).unwrap();
    drop(config);
    drop(consumer);
    drop(driver);
    assert_eq!(metadata.brokers().len(), 1);
    assert!(memory.used_bytes() >= 26 << 20);
    drop(metadata);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
#[cfg(unix)]
fn source_provider_scope_is_retained_beside_the_shared_native_capacity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.properties");
    std::fs::write(&path, "limit=17\n").unwrap();
    let memory = MemoryBudget::new(32 << 20);
    let mut driver = driver(&memory);
    driver.cluster.properties_map = BTreeMap::from([
        ("config.providers".into(), "file".into()),
        ("config.providers.file.class".into(), "org.apache.kafka.common.config.provider.FileConfigProvider".into()),
        ("config.providers.file.param.allowed.paths".into(), directory.path().to_str().unwrap().into()),
        ("max.poll.records".into(), format!("${{file:{}:limit}}", path.display())),
    ]);
    let config = driver.consumer_configuration().unwrap();
    assert!(config._providers.is_some());
    assert_eq!(driver.options.maximum_batch_records, 17);
    assert_eq!(config.get("receive.message.max.bytes"), Some("9437184"));
    assert!(memory.used_bytes() >= 26 << 20);
    drop(config);
    drop(driver);
    assert_eq!(memory.used_bytes(), 0);
}
