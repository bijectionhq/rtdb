use std::collections::BTreeMap;
use rtdb_kafka::{KafkaClusterConfig, KafkaLogConfig, native_properties, parse_properties, producer_properties};

#[test]
fn producer_forces_durable_acknowledgements_and_preserves_order_without_idempotence() {
    let properties = BTreeMap::from([("acks".into(), "0".into()), ("enable.idempotence".into(), "false".into()), ("max.in.flight.requests.per.connection".into(), "5".into())]);
    let producer = producer_properties(&properties, false);
    assert_eq!(producer["acks"], "all");
    assert_eq!(producer["max.in.flight.requests.per.connection"], "1");
    assert_eq!(producer["linger.ms"], "0");
    assert_eq!(producer["compression.type"], "snappy");
    assert!(!producer_properties(&BTreeMap::new(), true).contains_key("linger.ms"));
    assert_eq!(producer_properties(&BTreeMap::from([("linger.ms".into(), "20".into())]), false)["linger.ms"], "20");
}

#[test]
fn java_properties_keep_iso_8859_1_continuations_and_escaped_separators() {
    let properties = parse_properties(b"# comment\r\nname = caf\xe9\r\nlong = first\\\r\n  second\r\nkey\\:part: value\\nline\nemoji=\\uD83D\\uDE00\nslash=last\\\\\nnext = yes\n").unwrap();
    assert_eq!(properties["name"], "caf\u{e9}");
    assert_eq!(properties["long"], "firstsecond");
    assert_eq!(properties["key:part"], "value\nline");
    assert_eq!(properties["emoji"], "\u{1f600}");
    assert_eq!(properties["slash"], "last\\");
    assert_eq!(properties["next"], "yes");
    assert!(parse_properties(b"bad=\\uXYZ1").is_err());
}

#[test]
fn property_file_overrides_map_and_debug_does_not_expose_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("kafka.properties");
    std::fs::write(&path, "bootstrap.servers=file:9092\nsasl.password=secret-file").unwrap();
    let mut config = KafkaClusterConfig::new("base:9092");
    config.properties_map.insert("bootstrap.servers".into(), "map:9092".into());
    config.properties_map.insert("sasl.password".into(), "secret-map".into());
    config.properties_file = Some(path);
    assert_eq!(config.properties().unwrap()["bootstrap.servers"], "file:9092");
    assert_eq!(config.properties().unwrap()["sasl.password"], "secret-file");
    assert!(!format!("{config:?}").contains("secret-map"));
}

#[test]
fn persisted_log_configuration_resolves_replica_defaults_and_retains_epoch() {
    let config = KafkaLogConfig::new("cluster", "source");
    let restored = KafkaLogConfig::from_proto(&config.to_proto());
    assert_eq!(restored.replica_cluster(), "cluster");
    assert_eq!(restored.replica_topic(), "source-replica");
    let config = KafkaLogConfig { replica_cluster: Some("other".into()), replica_topic: Some("replica".into()), epoch: 42, ..config };
    assert_eq!(KafkaLogConfig::from_proto(&config.to_proto()), config);
}

#[test]
fn documented_plain_jaas_credentials_translate_without_revealing_secrets_in_errors() {
    let config = BTreeMap::from([("security.protocol".into(), "SASL_SSL".into()), ("sasl.mechanism".into(), "PLAIN".into()),
        ("sasl.jaas.config".into(), r#"org.apache.kafka.common.security.plain.PlainLoginModule required username="service user" password="a\"b\\c";"#.into())]);
    let native = native_properties(&config).unwrap();
    assert!(!native.contains_key("sasl.jaas.config"));
    assert_eq!(native["sasl.username"], "service user");
    assert_eq!(native["sasl.password"], "a\"b\\c");
    assert_eq!(native["security.protocol"], "SASL_SSL");
    let invalid = BTreeMap::from([("sasl.jaas.config".into(), "bad module secret-value".into())]);
    assert!(!format!("{:?}", native_properties(&invalid).unwrap_err()).contains("secret-value"));
}
