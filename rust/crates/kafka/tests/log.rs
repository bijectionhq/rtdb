use std::sync::{Arc, Mutex};
use std::time::Duration;
use bytes::Bytes;
use rdkafka::mocking::MockCluster;
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::{KafkaClusterConfig, KafkaLog, KafkaLogOptions, KafkaObserver, KafkaWarning};
use rtdb_storage::{Error, log::{Log, LogOffset}};
use rtdb_types::Timestamp;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Warnings(Mutex<Vec<KafkaWarning>>);
impl KafkaObserver for Warnings { fn warning(&self, warning: KafkaWarning) { self.0.lock().unwrap().push(warning); } }

#[tokio::test]
async fn log_tls_admission_fails_before_broker_setup_and_releases_failed_material() {
    let mut configuration = KafkaClusterConfig::new("unused.invalid:9092");
    configuration.properties_map.insert("security.protocol".into(), "SSL".into());
    let memory = MemoryBudget::new(0);
    let result = KafkaLog::open(&configuration, KafkaLogOptions::new("log"), Arc::new(Warnings::default()), Arc::clone(&memory)).await;
    assert_eq!(result.err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(32 << 20);
    configuration.properties_map.insert("ssl.secure.random.implementation".into(), "unknown.Random".into());
    let result = KafkaLog::open(&configuration, KafkaLogOptions::new("log"), Arc::new(Warnings::default()), Arc::clone(&memory)).await;
    assert_eq!(result.err().unwrap().code, "xtdb.kafka/invalid-tls-config");
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_delivery_is_read_back_by_offset_and_read_only_handles_have_no_driver() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("log", 1, 1).unwrap();
    let configuration = KafkaClusterConfig::new(cluster.bootstrap_servers());
    let mut options = KafkaLogOptions::new("log");
    options.auto_create_topic = false;
    let opened = KafkaLog::open(&configuration, options.clone(), Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    let cancellation = CancellationToken::new();
    let driver = tokio::spawn(opened.driver.unwrap().run(cancellation.clone()));
    let first = opened.log.enqueue(Bytes::from_static(b"first"), Timestamp(42)).await.unwrap().durable().await.unwrap();
    let second = opened.log.enqueue(Bytes::from_static(b"second"), Timestamp(43)).await.unwrap().durable().await.unwrap();
    assert_eq!(first.offset, LogOffset(0));
    assert_eq!(second.offset, LogOffset(1));
    assert_eq!(opened.log.latest().unwrap(), second.offset);
    assert_eq!(first.timestamp.0 % 1000, 0);
    options.read_only = true;
    let reader = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    assert!(reader.driver.is_none());
    assert!(matches!(reader.log.enqueue(Bytes::new(), Timestamp(0)).await, Err(Error::ReadOnlyLog(_))));
    let records = reader.log.read_range(LogOffset(0), LogOffset(2)).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].payload, Bytes::from_static(b"first"));
    assert_eq!(records[1].metadata, second);
    let mut range = reader.log.range(LogOffset(0), LogOffset(1)).await.unwrap();
    let bound = range.allocation_bound();
    assert_eq!(range.next(rtdb_storage::object_store::ReadReservation::new(bound, ())).await.unwrap().unwrap().metadata, first);
    assert!(range.next(rtdb_storage::object_store::ReadReservation::new(bound, ())).await.unwrap().is_none());
    let mut tail = reader.log.tail(second.offset).await.unwrap();
    assert!(tail.poll(Duration::from_millis(20)).await.unwrap().is_empty());
    cancellation.cancel();
    driver.await.unwrap().unwrap();
    assert!(matches!(opened.log.latest(), Err(Error::Closed(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_wakes_enqueues_waiting_for_capacity_and_finishes_pending_receipts() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("shutdown", 1, 1).unwrap();
    let configuration = KafkaClusterConfig::new(cluster.bootstrap_servers());
    let mut options = KafkaLogOptions::new("shutdown");
    options.auto_create_topic = false;
    options.queue_capacity = 1;
    options.maximum_message_bytes = 1024;
    options.queued_bytes = 2048;
    options.shutdown_timeout = Duration::from_millis(100);
    let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    cluster.broker_down(1).unwrap();
    let cancellation = CancellationToken::new();
    let driver = tokio::spawn(opened.driver.unwrap().run(cancellation.clone()));
    let receipt = opened.log.enqueue(Bytes::from_static(b"first"), Timestamp(0)).await.unwrap();
    let log = Arc::clone(&opened.log);
    let mut waiting = tokio::spawn(async move { log.enqueue(Bytes::from_static(b"second"), Timestamp(0)).await });
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    cancellation.cancel();
    assert!(tokio::time::timeout(Duration::from_secs(5), waiting).await.unwrap().unwrap().is_err());
    let _ = driver.await.unwrap();
    assert!(receipt.durable().await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partitioned_topics_and_oversized_records_are_rejected_before_receipt() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("partitions", 2, 1).unwrap();
    cluster.create_topic("size", 1, 1).unwrap();
    let configuration = KafkaClusterConfig::new(cluster.bootstrap_servers());
    assert!(KafkaLog::open(&configuration, KafkaLogOptions::new("partitions"), Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.is_err());
    let mut options = KafkaLogOptions::new("size");
    options.maximum_message_bytes = 4;
    let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    assert!(matches!(opened.log.enqueue(Bytes::from_static(b"large"), Timestamp(0)).await, Err(Error::MessageTooLarge { .. })));
    drop(opened.driver);
}
