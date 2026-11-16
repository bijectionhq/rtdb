use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConnectData, ConversionContext, Converter, PluginRegistry, PluginSpec};
use rtdb_types::Value;
use serde_json::{Value as Json, json};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, task::JoinHandle};

async fn registry(schema: &str, format: &str, rules: Json) -> (Box<dyn Converter>, JoinHandle<Vec<(String, Json)>>) {
    registry_options(schema, format, rules, BTreeMap::new()).await
}

async fn registry_options(schema: &str, format: &str, rules: Json, options: BTreeMap<String, String>) -> (Box<dyn Converter>, JoinHandle<Vec<(String, Json)>>) {
    registry_role(schema, format, rules, options, false).await
}

async fn registry_role(schema: &str, format: &str, rules: Json, mut options: BTreeMap<String, String>, is_key: bool) -> (Box<dyn Converter>, JoinHandle<Vec<(String, Json)>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let body = json!({"schema":schema,"schemaType":format,"metadata":{"properties":{"application":"ledger"}},"ruleSet":rules}).to_string();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for body in [body, json!({"id":7,"version":3}).to_string()] {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0]; stream.read_exact(&mut byte).await.unwrap(); header.push(byte[0]); assert!(header.len() < 8192);
            }
            let header = String::from_utf8(header).unwrap();
            let length = header.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").map(|value| value.trim().parse::<usize>().unwrap())).unwrap_or(0);
            let mut payload = vec![0; length]; stream.read_exact(&mut payload).await.unwrap();
            let payload = if payload.is_empty() { Json::Null } else { serde_json::from_slice(&payload).unwrap() };
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
            requests.push((header, payload));
        }
        requests
    });
    let class = if format == "AVRO" { "io.confluent.connect.avro.AvroConverter" } else { "io.confluent.connect.json.JsonSchemaConverter" };
    options.insert("schema.registry.url".into(), format!("http://{address}"));
    let spec = PluginSpec { class_name: class.into(), options };
    (PluginRegistry::default().converter(&spec, is_key).unwrap(), server)
}

fn framed(datum: &[u8]) -> Vec<u8> { let mut bytes = vec![0, 0, 0, 0, 7]; bytes.extend_from_slice(datum); bytes }
fn context(memory: &Arc<MemoryBudget>) -> ConversionContext { ConversionContext::new(Arc::clone(memory)) }

#[tokio::test]
async fn read_ignored_rules_are_cached_and_subject_lookup_preserves_metadata() {
    let rules = json!({"domainRules":[
        {"name":"write","mode":"WRITE","type":"absent"},
        {"name":"disabled","disabled":true,"type":"absent"},
        {"name":"tolerated","type":"absent","onFailure":"ERROR,NONE"}
    ],"migrationRules":[{"name":"upgrade","mode":"UPGRADE","type":"absent"}]});
    let schema = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;
    let (mut converter, server) = registry(schema, "AVRO", rules.clone()).await;
    let memory = MemoryBudget::new(1 << 26);
    let value = converter.decode("events", Some(&framed(&[14])), &mut context(&memory)).await.unwrap();
    assert_eq!(value.value, Value::Struct(BTreeMap::from([("id".into(), Value::I64(7))])));
    let requests = server.await.unwrap();
    assert_eq!(requests[1].1["metadata"], json!({"properties":{"application":"ledger"}}));
    assert_eq!(requests[1].1["ruleSet"], rules);
    assert_eq!(converter.decode("events", Some(&framed(&[14])), &mut context(&memory)).await.unwrap(), value);
    assert_eq!(converter.decode("events", None, &mut context(&memory)).await.unwrap(), ConnectData::null());
}

#[tokio::test]
async fn avro_null_skips_read_rules_and_bad_datum_fails_before_rules() {
    let (mut converter, server) = registry(r#"["null","long"]"#, "AVRO", json!({"domainRules":[{"name":"read-guard","type":"absent"}]})).await;
    let memory = MemoryBudget::new(1 << 26);
    let error = converter.decode("events", Some(&framed(&[0])), &mut context(&memory)).await.unwrap_err();
    assert!(error.to_string().contains("no schema and value"));
    assert!(!error.to_string().contains("read-guard"));
    server.await.unwrap();
    let error = converter.decode("events", Some(&framed(&[2, 14])), &mut context(&memory)).await.unwrap_err();
    assert!(error.to_string().contains("read-guard"));
    let error = converter.decode("events", Some(&framed(&[2])), &mut context(&memory)).await.unwrap_err();
    assert!(!error.to_string().contains("read-guard"));
    assert!(error.to_string().contains("Truncated Avro datum"));
}

#[tokio::test]
async fn json_null_is_a_read_rule_message_and_rule_runs_before_connect_conversion() {
    let (mut converter, server) = registry(r#"{"type":"integer"}"#, "JSON", json!({"domainRules":[{"name":"json-guard","type":"absent"}]})).await;
    let memory = MemoryBudget::new(1 << 26);
    let error = converter.decode("events", Some(&framed(b"null")), &mut context(&memory)).await.unwrap_err();
    assert!(error.to_string().contains("json-guard"));
    server.await.unwrap();
    let error = converter.decode("events", Some(&framed(br#""not-an-integer""#)), &mut context(&memory)).await.unwrap_err();
    assert!(error.to_string().contains("json-guard"));
    let error = converter.decode("events", Some(&framed(b"{")), &mut context(&memory)).await.unwrap_err();
    assert!(!error.to_string().contains("json-guard"));
}

#[tokio::test]
async fn configured_rule_failure_expansion_is_reserved_before_cache_publication() {
    let rules = json!({"domainRules":(0..16).map(|index| json!({"name":format!("rule-{index}"),"type":"absent"})).collect::<Vec<_>>()});
    let options = BTreeMap::from([("rule.executors._default_.onFailure".into(), "x".repeat(1 << 20))]);
    let (mut converter, server) = registry_options(r#""long""#, "AVRO", rules, options).await;
    let memory = MemoryBudget::new(1 << 27);
    let mut context = ConversionContext::with_limit(Arc::clone(&memory), 32 << 20);
    let error = converter.decode("events", Some(&framed(&[14])), &mut context).await.unwrap_err();
    assert_eq!(error.code, "xtdb.kafka-connect-source/batch-too-large");
    assert_eq!(server.await.unwrap().len(), 2);
    drop(context); drop(converter);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn cached_rule_reservation_covers_repeated_failures_until_last_context_drops() {
    let rules = json!({"domainRules":[{"name":"read-guard","type":"absent"}]});
    let options = BTreeMap::from([("rule.executors._default_.onFailure".into(), "x".repeat(4096))]);
    let (mut converter, server) = registry_options(r#""long""#, "AVRO", rules, options).await;
    let memory = MemoryBudget::new(1 << 27);
    let mut first = context(&memory);
    assert_eq!(converter.decode("events", Some(&framed(&[14])), &mut first).await.unwrap_err().code, "xtdb.kafka-connect-source/invalid-plugin-config");
    assert_eq!(server.await.unwrap().len(), 2);
    drop(first);
    let cached = memory.used_bytes();
    assert!(cached > 4096);
    let mut constrained = ConversionContext::with_limit(Arc::clone(&memory), 1);
    assert_eq!(converter.decode("events", Some(&framed(&[14])), &mut constrained).await.unwrap_err().code, "xtdb.kafka-connect-source/batch-too-large");
    drop(constrained);
    let mut retained = context(&memory);
    assert_eq!(converter.decode("events", Some(&framed(&[14])), &mut retained).await.unwrap_err().code, "xtdb.kafka-connect-source/invalid-plugin-config");
    drop(converter);
    assert!(memory.used_bytes() > 0);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dlq_read_publishes_original_frames_and_close_drains_after_rule_failure() {
    use std::time::Duration;
    use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList, consumer::{BaseConsumer, Consumer}, message::Headers, mocking::MockCluster};
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("failures", 1, 1).unwrap();
    let options = BTreeMap::from([
        ("bootstrap.servers".into(), cluster.bootstrap_servers()),
        ("dlq.topic".into(), "failures".into()),
        ("buffer.memory".into(), (1 << 20).to_string()),
    ]);
    let (mut key, key_server) = registry_role(r#""long""#, "AVRO", json!({}), BTreeMap::new(), true).await;
    let (mut value, value_server) = registry_options(r#"{"type":"integer"}"#, "JSON", json!({"domainRules":[{"name":"guard","type":"missing","onFailure":"DLQ"}]}), options).await;
    let memory = MemoryBudget::new(1 << 28);
    let mut context = context(&memory);
    let key_frame = framed(&[14]); let value_frame = framed(b"9");
    key.decode("events", Some(&key_frame), &mut context).await.unwrap();
    let failure = value.decode("events", Some(&value_frame), &mut context).await.unwrap_err();
    assert!(failure.to_string().contains("Rule failed: guard"));
    key_server.await.unwrap(); value_server.await.unwrap();
    drop(context);
    tokio::time::timeout(Duration::from_secs(10), value.close()).await.unwrap().unwrap();
    key.close().await.unwrap();
    let consumer: BaseConsumer = ClientConfig::new().set("bootstrap.servers", cluster.bootstrap_servers()).set("group.id", "dlq-observation").set("enable.auto.commit", "false").create().unwrap();
    let mut assignment = TopicPartitionList::new(); assignment.add_partition_offset("failures", 0, Offset::Beginning).unwrap(); consumer.assign(&assignment).unwrap();
    let message = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(message) = consumer.poll(Duration::ZERO) { break message.unwrap().detach(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(message.key(), Some(key_frame.as_slice()));
    assert_eq!(message.payload(), Some(value_frame.as_slice()));
    let headers = message.headers().unwrap();
    assert_eq!(headers.count(), 5);
    assert_eq!(headers.get(0).key, "__rule.name"); assert_eq!(headers.get(0).value, Some(b"guard".as_slice()));
    assert_eq!(headers.get(1).value, Some(b"READ".as_slice()));
    assert_eq!(headers.get(2).value, Some(b"events-value".as_slice()));
    assert_eq!(headers.get(3).value, Some(b"events".as_slice()));
    assert_eq!(headers.get(4).value, Some(b"Could not find rule executor of type missing".as_slice()));
    drop(key); drop(value); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn dlq_missing_topic_precedes_producer_open_and_send_errors_preserve_rule_failure() {
    let rules = json!({"domainRules":[{"name":"guard","type":"missing","onFailure":"DLQ"}]});
    let (mut no_topic, server) = registry(r#""long""#, "AVRO", rules.clone()).await;
    let memory = MemoryBudget::new(1 << 27);
    let failure = no_topic.decode("events", Some(&framed(&[14])), &mut context(&memory)).await.unwrap_err();
    assert!(failure.to_string().contains("no topic is configured")); server.await.unwrap();
    no_topic.close().await.unwrap();
    let (mut no_broker, server) = registry_options(r#""long""#, "AVRO", rules, BTreeMap::from([("dlq.topic".into(), "failures".into())])).await;
    let failure = no_broker.decode("events", Some(&framed(&[14])), &mut context(&memory)).await.unwrap_err();
    assert!(failure.to_string().contains("Rule failed: guard")); server.await.unwrap();
    no_broker.close().await.unwrap();
    drop(no_broker); drop(no_topic); assert_eq!(memory.used_bytes(), 0);
}
