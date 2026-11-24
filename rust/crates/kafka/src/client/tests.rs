use super::*;
use std::sync::Mutex;
use rdkafka::{admin::AdminClient, client::{Client, DefaultClientContext}, producer::BaseProducer, types::RDKafkaType};
use crate::{KafkaLog, KafkaLogOptions, KafkaObserver, KafkaWarning};

#[derive(Default)]
struct Warnings(Mutex<Vec<KafkaWarning>>);
impl KafkaObserver for Warnings { fn warning(&self, warning: KafkaWarning) { self.0.lock().unwrap().push(warning); } }

fn ssl_options() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("security.protocol".into(), "SSL".into()),
        ("ssl.truststore.location".into(), format!("{}/../tls/tests/fixtures/empty.jks", env!("CARGO_MANIFEST_DIR"))),
        ("ssl.truststore.password".into(), "12345678".into()),
    ])
}

#[test]
fn java_fetch_wait_is_translated_before_native_consumer_configuration() {
    let memory = MemoryBudget::new(1 << 20);
    let mut properties = BTreeMap::from([("fetch.max.wait.ms".into(), " +\u{661}\u{665}\u{660} ".into())]);
    let prepared = prepare(&properties, &memory).unwrap();
    assert_eq!(prepared.get("fetch.max.wait.ms"), None);
    let native = prepared.create_native_config().unwrap();
    assert_eq!(native.get("fetch.wait.max.ms").unwrap(), "150");
    properties.insert("fetch.wait.max.ms".into(), "151".into());
    assert_eq!(prepare(&properties, &memory).unwrap_err().code, "xtdb.kafka/conflicting-config");
    properties.remove("fetch.wait.max.ms");
    properties.insert("fetch.max.wait.ms".into(), "-1".into());
    assert_eq!(prepare(&properties, &memory).unwrap_err().code, "xtdb.kafka/invalid-config");
}

fn options(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries.iter().map(|(name, value)| ((*name).to_owned(), (*value).to_owned())).collect()
}

#[test]
fn java_client_properties_take_their_native_equivalents() {
    let memory = MemoryBudget::new(1 << 20);
    let configuration = prepare(&options(&[
        ("send.buffer.bytes", "-1"), ("receive.buffer.bytes", "65536"), ("buffer.memory", "33554433"), ("max.request.size", " 2097152 "),
        ("compression.type", "zstd"), ("compression.zstd.level", "7"), ("compression.gzip.level", "3"), ("auto.offset.reset", "none"),
        ("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor, org.apache.kafka.clients.consumer.CooperativeStickyAssignor"),
        ("acks", "all"), ("linger.ms", "20"), ("retries", "3"), ("request.timeout.ms", "40000"), ("delivery.timeout.ms", "120000"),
    ]), &memory).unwrap();
    for (name, value) in [
        ("socket.send.buffer.bytes", "0"), ("socket.receive.buffer.bytes", "65536"), ("queue.buffering.max.kbytes", "32769"),
        ("message.max.bytes", "2097152"), ("compression.level", "7"), ("auto.offset.reset", "error"),
        ("partition.assignment.strategy", "range,cooperative-sticky"), ("acks", "all"), ("linger.ms", "20"),
    ] {
        assert_eq!(configuration.get(name), Some(value), "{name}");
    }
    for name in ["send.buffer.bytes", "receive.buffer.bytes", "buffer.memory", "max.request.size", "compression.zstd.level", "compression.gzip.level"] {
        assert_eq!(configuration.get(name), None, "{name}");
    }
    configuration.create_native_config().unwrap();
}

#[test]
fn java_only_client_properties_are_dropped_and_reported() {
    let memory = MemoryBudget::new(1 << 20);
    let java = options(&[
        ("bootstrap.servers", "localhost:9092"), ("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"),
        ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"), ("max.block.ms", "60000"),
        ("max.poll.records", "500"), ("default.api.timeout.ms", "60000"), ("metric.reporters", ""), ("interceptor.classes", " "),
        ("partition.assignment.strategy", "org.apache.kafka.clients.consumer.StickyAssignor"),
    ]);
    let (native, ignored) = crate::config::native_client_properties(&java).unwrap();
    assert_eq!(native, options(&[("bootstrap.servers", "localhost:9092")]));
    assert_eq!(ignored, ["default.api.timeout.ms", "max.block.ms", "max.poll.records", "metric.reporters", "partition.assignment.strategy"]);
    prepare(&java, &memory).unwrap().create_native_config().unwrap();
}

#[test]
fn java_client_properties_without_native_meaning_or_agreement_are_rejected() {
    for (entries, code) in [
        (&[("interceptor.classes", "com.example.Interceptor")][..], "xtdb.kafka/unsupported-config"),
        (&[("sasl.login.callback.handler.class", "com.example.Handler")][..], "xtdb.kafka/unsupported-config"),
        (&[("buffer.memory", "1024"), ("queue.buffering.max.kbytes", "2")][..], "xtdb.kafka/conflicting-config"),
        (&[("send.buffer.bytes", "-2")][..], "xtdb.kafka/invalid-config"),
        (&[("buffer.memory", "-1")][..], "xtdb.kafka/invalid-config"),
    ] {
        assert_eq!(crate::config::native_client_properties(&options(entries)).unwrap_err().code, code, "{entries:?}");
    }
}

#[test]
fn java_tls_options_are_consumed_and_unknown_native_properties_are_preserved() {
    let memory = MemoryBudget::new(32 << 20);
    let mut options = ssl_options();
    options.extend([
        ("ssl.protocol".into(), " TLSv1.2 ".into()), ("ssl.enabled.protocols".into(), "TLSv1.2".into()),
        ("ssl.cipher.suites".into(), "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256".into()),
        ("ssl.provider".into(), " SunJSSE ".into()), ("ssl.secure.random.implementation".into(), " SHA1PRNG ".into()),
        ("ssl.engine.factory.class".into(), " org.apache.kafka.common.security.ssl.DefaultSslEngineFactory ".into()),
        ("ssl.endpoint.identification.algorithm".into(), " HTTPS ".into()),
        ("ssl.unknown.property".into(), "still-invalid".into()),
    ]);
    let path = options["ssl.truststore.location"].clone();
    options.insert("ssl.truststore.location".into(), format!(" {path} "));
    let configuration = prepare(&options, &memory).unwrap();
    for name in options.keys().filter(|name| name.as_str() != "security.protocol" && name.as_str() != "ssl.endpoint.identification.algorithm" && name.as_str() != "ssl.unknown.property") {
        assert!(configuration.get(name).is_none(), "{name}");
    }
    assert_eq!(configuration.get("ssl.endpoint.identification.algorithm"), Some("https"));
    assert_eq!(configuration.get("ssl.unknown.property"), Some("still-invalid"));
    assert!(configuration.create_native_config().is_err());
    drop(configuration);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn plaintext_skips_tls_files_and_admission_while_native_credentials_remain_available() {
    let memory = MemoryBudget::new(0);
    let options = BTreeMap::from([
        ("security.protocol".into(), "SASL_PLAINTEXT".into()), ("ssl.truststore.location".into(), "/absent/truststore".into()),
        ("sasl.jaas.config".into(), "org.apache.kafka.common.security.plain.PlainLoginModule required username=service password=secret;".into()),
        ("ssl.key.password".into(), "native-password".into()), ("ssl.ca.location".into(), "native-roots.pem".into()),
        ("ssl.endpoint.identification.algorithm".into(), String::new()),
    ]);
    let configuration = prepare(&options, &memory).unwrap();
    assert_eq!(configuration.get("sasl.username"), Some("service"));
    assert_eq!(configuration.get("sasl.password"), Some("secret"));
    assert_eq!(configuration.get("ssl.key.password"), Some("native-password"));
    assert_eq!(configuration.get("ssl.ca.location"), Some("native-roots.pem"));
    assert_eq!(configuration.get("ssl.endpoint.identification.algorithm"), Some("none"));
    assert!(configuration.get("ssl.truststore.location").is_none());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn invalid_tls_names_and_mixed_native_material_release_admitted_memory() {
    let memory = MemoryBudget::new(32 << 20);
    for (name, value) in [
        ("ssl.engine.factory.class", "custom.Factory"), ("ssl.engine.factory.class", ""),
        ("ssl.provider", "custom.Provider"), ("ssl.secure.random.implementation", "custom.Random"),
        ("ssl.ca.pem", "native roots"),
    ] {
        let mut options = ssl_options(); options.insert(name.into(), value.into());
        assert_eq!(prepare(&options, &memory).unwrap_err().code, "xtdb.kafka/invalid-tls-config");
        assert_eq!(memory.used_bytes(), 0);
    }
    let mut options = ssl_options();
    options.insert("ssl.keystore.location".into(), "/absent/identity".into());
    options.insert("ssl.key.pem".into(), "native key".into());
    assert_eq!(prepare(&options, &memory).unwrap_err().code, "xtdb.kafka/invalid-tls-config");
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(prepare(&ssl_options(), &MemoryBudget::new(0)).unwrap_err().code, "rtdb/memory-budget-exceeded");
    let mut options = ssl_options(); options.insert("ssl.truststore.password".into(), " 12345678 ".into());
    assert!(prepare(&options, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_native_client_creation_releases_the_last_tls_configuration() {
    let memory = MemoryBudget::new(32 << 20);
    let mut configuration = prepare(&ssl_options(), &memory).unwrap();
    configuration.set("enable.idempotence", "true").set("acks", "0");
    let native = configuration.create_native_config().unwrap();
    drop(configuration);
    assert!(Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_client_copies_retain_one_tls_snapshot_after_the_log_and_store_file_are_gone() {
    let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("trust.jks");
    std::fs::write(&file, include_bytes!("../../../tls/tests/fixtures/empty.jks")).unwrap();
    let memory = MemoryBudget::new(32 << 20); let mut options = ssl_options();
    options.insert("ssl.truststore.location".into(), file.to_str().unwrap().into());
    let configuration = prepare(&options, &memory).unwrap();
    let retained = memory.used_bytes();
    assert!(retained > 0);
    std::fs::remove_file(file).unwrap();
    let log_options = KafkaLogOptions::new("tls-ownership");
    let producer_config = crate::producer_config(&configuration, &log_options, &Warnings::default(), false);
    let native = producer_config.create_native_config().unwrap();
    let duplicate = native.clone(); drop(native); drop(producer_config);
    let log = KafkaLog { configuration: crate::LogConfiguration::Common(configuration), options: log_options, progress: Arc::new(crate::Progress::new(-1)), access: crate::Access::ReadOnly };
    let consumer = crate::consumer(&log.configuration, &log.options).unwrap();
    let admin: AdminClient<DefaultClientContext> = log.configuration.prepare(crate::Role::Admin).unwrap().configuration.create().unwrap();
    let producer = Client::new(&ClientConfig::new(), duplicate, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    drop(log);
    assert_eq!(memory.used_bytes(), retained);
    drop(admin); drop(producer);
    assert_eq!(memory.used_bytes(), retained);
    drop(consumer);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn producer_derivation_preserves_tls_ownership_overrides_and_warning_order() {
    let memory = MemoryBudget::new(32 << 20); let mut options = ssl_options();
    options.extend([("acks".into(), "0".into()), ("enable.idempotence".into(), "false".into()), ("max.in.flight.requests.per.connection".into(), "5".into()),
        ("linger.ms".into(), "17".into()), ("compression.type".into(), "gzip".into()), ("message.max.bytes".into(), "4096".into())]);
    let configuration = prepare(&options, &memory).unwrap(); let warnings = Warnings::default();
    let mut log_options = KafkaLogOptions::new("log"); log_options.queue_capacity = 7; log_options.queued_bytes = 4096;
    let derived = crate::producer_config(&configuration, &log_options, &warnings, false);
    assert_eq!(derived.get("acks"), Some("all"));
    assert_eq!(derived.get("enable.idempotence"), Some("false"));
    assert_eq!(derived.get("max.in.flight.requests.per.connection"), Some("1"));
    assert_eq!(derived.get("linger.ms"), Some("17"));
    assert_eq!(derived.get("compression.type"), Some("gzip"));
    assert_eq!(derived.get("message.max.bytes"), Some("4096"));
    assert_eq!(derived.get("queue.buffering.max.messages"), Some("7"));
    assert_eq!(derived.get("queue.buffering.max.kbytes"), Some("4"));
    assert_eq!(*warnings.0.lock().unwrap(), [KafkaWarning::AcknowledgementsOverridden { configured: "0".into() }, KafkaWarning::IdempotenceDisabled]);
    let producer: BaseProducer = derived.create().unwrap();
    drop(configuration); drop(derived);
    assert!(memory.used_bytes() > 0);
    drop(producer); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn producer_derivation_applies_defaults_without_changing_the_shared_base() {
    let configuration = prepare(&BTreeMap::new(), &MemoryBudget::new(0)).unwrap();
    for pipelined in [false, true] {
        let mut options = KafkaLogOptions::new("log"); options.pipelined = pipelined;
        let derived = crate::producer_config(&configuration, &options, &Warnings::default(), false);
        assert_eq!(derived.get("acks"), Some("all"));
        assert_eq!(derived.get("enable.idempotence"), Some("true"));
        assert_eq!(derived.get("compression.type"), Some("snappy"));
        assert_eq!(derived.get("linger.ms"), if pipelined { None } else { Some("0") });
    }
    assert!(configuration.config_map().is_empty());
}

#[test]
fn prepared_tls_replaces_automatic_roots_only_when_it_owns_the_trust_material() {
    let memory = MemoryBudget::new(64 << 20);
    for (options, expected) in [
        (BTreeMap::from([("security.protocol".into(), "SSL".into())]), "true"),
        (ssl_options(), "true"),
        (BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.ca.location".into(), "native-roots.pem".into())]), "false"),
    ] {
        let configuration = prepare(&options, &memory).unwrap();
        let native = configuration.create_native_config().unwrap();
        assert_eq!(native.get("ssl.ctx.replaces.default.trust").unwrap(), expected);
        drop(configuration); drop(native);
        assert_eq!(memory.used_bytes(), 0);
    }
}
