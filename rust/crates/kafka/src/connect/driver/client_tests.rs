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
