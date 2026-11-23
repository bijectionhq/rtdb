use std::{collections::BTreeMap, sync::Arc, time::Duration};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList, consumer::{BaseConsumer, Consumer}, message::Headers, mocking::MockCluster,
    producer::{FutureProducer, FutureRecord}};
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::{KafkaClusterConfig, connect::{DateEnvironment, DateEnvironmentInput, DocsIndexer, PluginRegistry, ResumeToken, SourceConfig, SourceDriver, SourceOptions}};
use rtdb_replication::{BlockStore, PartitionLogs, ProgressMonitor, ReplicationConfig, ReplicationDriver, Role};
use rtdb_storage::{MemoryStore, cache::BufferPool, log::MemoryLog};
use rtdb_types::{SourcePosition, StorageEpoch, TableRef, Timestamp};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

struct Effects;
#[async_trait::async_trait]
impl rtdb_replication::DatabaseEffects for Effects {
    fn check_db_op(&self, _: &rtdb_format::log::proto::resolved_tx::DbOp) -> rtdb_types::Result<()> { Ok(()) }
    async fn apply_db_op(&self, _: &rtdb_format::log::proto::resolved_tx::DbOp) -> rtdb_types::Result<()> { Ok(()) }
    fn secondary_databases(&self) -> rtdb_types::Result<BTreeMap<String, rtdb_format::database::proto::DatabaseConfig>> { Ok(Default::default()) }
}
struct Authority;
impl rtdb_tx::TransactionAuthorization for Authority { fn check_role_membership(&self, _: &str, _: Option<&str>) -> rtdb_types::Result<()> { Ok(()) } }

const SCHEMA: &str = r#"{"type":"record","name":"Event","fields":[{"name":"name","type":"string"}]}"#;
const GOOD_ID: u8 = 7;
const GUARDED_ID: u8 = 8;

async fn schema_registry() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") { let mut byte = [0]; stream.read_exact(&mut byte).await.unwrap(); header.push(byte[0]); }
            let header = String::from_utf8(header).unwrap();
            let length = header.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").map(|value| value.trim().parse::<usize>().unwrap())).unwrap_or(0);
            let mut payload = vec![0; length]; stream.read_exact(&mut payload).await.unwrap();
            let guarded = json!({"domainRules":[{"name":"guard","type":"missing","onFailure":"DLQ"}]});
            let body = if header.starts_with(&format!("GET /schemas/ids/{GOOD_ID} ")) { json!({"schema":SCHEMA}) }
                else if header.starts_with(&format!("GET /schemas/ids/{GUARDED_ID} ")) { json!({"schema":SCHEMA,"ruleSet":guarded}) }
                else if header.starts_with("POST /subjects/") && String::from_utf8_lossy(&payload).contains("guard") { json!({"id":GUARDED_ID,"version":2}) }
                else if header.starts_with("POST /subjects/") { json!({"id":GOOD_ID,"version":1}) }
                else { panic!("unexpected registry request {header}") }.to_string();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    format!("http://{address}")
}

fn avro(schema: u8, name: &str) -> Vec<u8> {
    let mut frame = vec![0, 0, 0, 0, schema, (name.len() as u8) << 1];
    frame.extend_from_slice(name.as_bytes());
    frame
}

async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async { while !predicate() { tokio::time::sleep(Duration::from_millis(10)).await; } }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_avro_records_become_documents_and_a_rule_failure_reaches_the_dlq() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("events", 1, 1).unwrap();
    cluster.create_topic("failures", 1, 1).unwrap();
    let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", cluster.bootstrap_servers()).create().unwrap();
    for (key, name) in [("doc-1", "alpha"), ("doc-2", "beta")] {
        producer.send(FutureRecord::to("events").partition(0).key(key).payload(&avro(GOOD_ID, name)), Duration::from_secs(5)).await.unwrap();
    }

    let memory = MemoryBudget::new(1 << 30);
    let store = Arc::new(MemoryStore::default());
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 27));
    let logs = PartitionLogs::new(Arc::new(MemoryLog::new(1 << 27)), Arc::new(MemoryLog::new(1 << 27)), StorageEpoch(0), StorageEpoch(0),
        rtdb_format::keys::CURRENT_STORAGE_VERSION, StorageEpoch(0), store.clone(), Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let blocks = BlockStore::new(store, Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(rtdb_trie::TrieConfig::default(), SourcePosition(-1), Timestamp(0), rtdb_replication::BlockDiscovery::List).await.unwrap();
    let partition = Arc::clone(&recovered.partition);
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let resolve = Arc::new(rtdb_tx::ResolveContext { placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "connect".into(), storage: cache, memory: Arc::clone(&memory), query_options: Default::default(),
        scan_accelerators: Default::default(),
        cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(Authority), execution_observer: None, tracer: None });
    let (handle, driver) = ReplicationDriver::new(ReplicationConfig { external_source: true, ..Default::default() }, logs, Arc::clone(&blocks), recovered, progress,
        resolve, Arc::new(Effects), CancellationToken::new()).unwrap();
    let replica_job = tokio::spawn(driver.run());
    wait_for(|| matches!(handle.status().role, Role::Leading(_))).await;

    let registry = schema_registry().await;
    let source = SourceConfig { remote: "kafka".into(), topic: "events".into(), indexer: prost_types::Any { type_url: "test/docs".into(), value: vec![] },
        connect_config: BTreeMap::from([
            ("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
            ("value.converter".into(), "io.confluent.connect.avro.AvroConverter".into()),
            ("value.converter.schema.registry.url".into(), registry),
            ("value.converter.bootstrap.servers".into(), cluster.bootstrap_servers()),
            ("value.converter.dlq.topic".into(), "failures".into()),
            ("value.converter.buffer.memory".into(), (1 << 20).to_string()),
        ]) };
    let environment = DateEnvironment::from_inputs(DateEnvironmentInput {
        language: "en".into(), script: String::new(), region: "US".into(), variant: String::new(), unicode: BTreeMap::new(), timezone: "UTC".into(),
        initialized_at: 1_600_000_000_000, observed_at: 1_600_000_000_000, constructed_at: 1_600_000_000_000,
    }, &memory).unwrap();
    let table = TableRef::new("public", "events");
    let source = SourceDriver::new(KafkaClusterConfig::new(cluster.bootstrap_servers()), source, SourceOptions::default(), PluginRegistry::default(),
        Box::new(DocsIndexer::new(table.clone())), Arc::clone(&memory), environment).unwrap();
    let cancellation = CancellationToken::new();
    let source_job = tokio::spawn(source.run(handle.term().unwrap(), None, handle.watch_status(), cancellation.clone()));

    let applied = || handle.progress().current().external_token.as_deref().map(|token| ResumeToken::decode(token).unwrap());
    wait_for(|| applied() == Some(ResumeToken(1))).await;
    assert_eq!(partition.value().live().tables().get(&table).map(|table| table.row_count()), Some(2));

    let rejected = avro(GUARDED_ID, "gamma");
    producer.send(FutureRecord::to("events").partition(0).key("doc-3").payload(&rejected), Duration::from_secs(5)).await.unwrap();
    let failure = tokio::time::timeout(Duration::from_secs(20), source_job).await.unwrap().unwrap().unwrap_err();
    assert!(failure.to_string().contains("Rule failed: guard"), "{failure}");
    assert_eq!(applied(), Some(ResumeToken(1)));
    assert_eq!(partition.value().live().tables().get(&table).map(|table| table.row_count()), Some(2));

    let consumer: BaseConsumer = ClientConfig::new().set("bootstrap.servers", cluster.bootstrap_servers()).set("group.id", "dlq-observation").set("enable.auto.commit", "false").create().unwrap();
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset("failures", 0, Offset::Beginning).unwrap();
    consumer.assign(&assignment).unwrap();
    let message = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(message) = consumer.poll(Duration::ZERO) { break message.unwrap().detach(); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.unwrap();
    assert_eq!(message.payload(), Some(rejected.as_slice()));
    let headers = message.headers().unwrap();
    assert_eq!(headers.get(0).key, "__rule.name");
    assert_eq!(headers.get(0).value, Some(b"guard".as_slice()));
    assert_eq!(headers.get(3).value, Some(b"events".as_slice()));

    cancellation.cancel();
    handle.shutdown();
    replica_job.await.unwrap().unwrap();
}
