use std::sync::{Arc, Mutex};
use std::time::Duration;
use bytes::Bytes;
use rdkafka::{ClientConfig, mocking::MockCluster, producer::{FutureProducer, FutureRecord}};
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::{KafkaClusterConfig, KafkaLog, KafkaLogOptions, KafkaObserver, KafkaWarning};
use rtdb_storage::{Error, log::{Log, LogOffset}, object_store::ReadReservation};
use rtdb_types::Timestamp;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Warnings(Mutex<Vec<KafkaWarning>>);
impl KafkaObserver for Warnings { fn warning(&self, warning: KafkaWarning) { self.0.lock().unwrap().push(warning); } }

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_clients_resolve_provider_bootstrap_and_keep_configuration_through_native_destruction() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("provider-log", 1, 1).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("client.properties");
    std::fs::write(&file, format!("brokers={}\n", cluster.bootstrap_servers())).unwrap();
    let mut configuration = KafkaClusterConfig::new(format!("${{file:{}:brokers}}", file.display()));
    configuration.properties_map.extend([
        ("config.providers".into(), "file".into()),
        ("config.providers.file.class".into(), "org.apache.kafka.common.config.provider.FileConfigProvider".into()),
        ("config.providers.file.param.allowed.paths".into(), directory.path().to_str().unwrap().into()),
    ]);
    let memory = MemoryBudget::new(64 << 20);
    let mut options = KafkaLogOptions::new("provider-log");
    options.auto_create_topic = false;
    let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), Arc::clone(&memory)).await.unwrap();
    let cancellation = CancellationToken::new();
    let driver = tokio::spawn(opened.driver.unwrap().run(cancellation.clone()));
    let written = opened.log.enqueue(Bytes::from_static(b"configured"), Timestamp(0)).await.unwrap().durable().await.unwrap();
    let tail = opened.log.tail(LogOffset(-1)).await.unwrap();
    assert_eq!(written.offset, LogOffset(0));
    let before_releasing_log = memory.used_bytes();
    drop(opened.log);
    cancellation.cancel();
    driver.await.unwrap().unwrap();
    assert!(memory.used_bytes() < before_releasing_log);
    assert!(memory.used_bytes() > 0);
    drop(tail);
    assert_eq!(memory.used_bytes(), 0);
}

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

#[tokio::test]
async fn log_property_file_requires_admission_before_client_creation() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("cluster.properties");
    std::fs::write(&file, "client.id=one\n".repeat(4096)).unwrap();
    let mut configuration = KafkaClusterConfig::new("unused.invalid:9092");
    configuration.properties_file = Some(file);
    let memory = MemoryBudget::new(16 * 1024);
    let result = KafkaLog::open(&configuration, KafkaLogOptions::new("log"), Arc::new(Warnings::default()), Arc::clone(&memory)).await;
    assert_eq!(result.err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shared_log_kafka_durable_delivery_is_read_back_by_offset_and_read_only_handles_have_no_driver() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("log", 1, 1).unwrap();
    let configuration = KafkaClusterConfig::new(cluster.bootstrap_servers());
    let mut options = KafkaLogOptions::new("log");
    options.auto_create_topic = false;
    let opened = KafkaLog::open(&configuration, options.clone(), Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    let cancellation = CancellationToken::new();
    let driver = tokio::spawn(opened.driver.unwrap().run(cancellation.clone()));
    let before = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64 * 1000;
    let shared: &dyn rtdb_storage::log::SharedLog = opened.log.as_ref();
    assert_eq!(shared.latest().unwrap(), LogOffset(-1));
    let first = shared.append(Bytes::from_static(b"first"), Timestamp(42)).await.unwrap().durable().await.unwrap();
    let second = shared.append(Bytes::from_static(b"second"), Timestamp(43)).await.unwrap().durable().await.unwrap();
    assert_eq!(first.offset, LogOffset(0));
    assert_eq!(second.offset, LogOffset(1));
    assert_eq!(shared.latest().unwrap(), second.offset);
    assert_eq!(first.timestamp.0 % 1000, 0);
    options.read_only = true;
    let reader = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    assert!(reader.driver.is_none());
    assert!(matches!(reader.log.enqueue(Bytes::new(), Timestamp(0)).await, Err(Error::ReadOnlyLog(_))));
    let records = reader.log.read_range(LogOffset(0), LogOffset(2)).await.unwrap();
    let after = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64 * 1000;
    assert_eq!(records.len(), 2);
    for record in &records {
        assert!((before..=after).contains(&record.metadata.timestamp.0));
        assert_eq!(record.metadata.timestamp.0 % 1000, 0);
    }
    assert_eq!(records[0].payload, Bytes::from_static(b"first"));
    assert_eq!(records[1].metadata.offset, second.offset);
    assert_eq!(second.timestamp, Timestamp(1_234_000));
    let mut range = reader.log.range(LogOffset(0), LogOffset(1)).await.unwrap();
    let bound = range.allocation_bound();
    assert_eq!(range.next(rtdb_storage::object_store::ReadReservation::new(bound, ())).await.unwrap().unwrap().metadata, records[0].metadata);
    assert!(range.next(rtdb_storage::object_store::ReadReservation::new(bound, ())).await.unwrap().is_none());
    let shared_reader: &dyn rtdb_storage::log::SharedLog = reader.log.as_ref();
    let mut cursor = shared_reader.read_after(first.offset).await.unwrap();
    let head = cursor.poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(head.metadata, records[1].metadata);
    let record = cursor.read_head(head, ReadReservation::new(head.allocation_bound, ())).await.unwrap();
    assert_eq!(record.payload, Bytes::from_static(b"second"));
    assert_eq!(cursor.poll_head(Duration::from_millis(20)).await.unwrap(), None);
    let mut tail = reader.log.tail(second.offset).await.unwrap();
    assert!(tail.poll(Duration::from_millis(20)).await.unwrap().is_empty());
    cancellation.cancel();
    driver.await.unwrap().unwrap();
    assert!(matches!(opened.log.latest(), Err(Error::Closed(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_kafka_head_survives_refused_and_cancelled_delivery_and_can_be_read_by_direct_poll() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("replay", 1, 1).unwrap();
    let configuration = KafkaClusterConfig::new(cluster.bootstrap_servers());
    let mut options = KafkaLogOptions::new("replay");
    options.auto_create_topic = false;
    let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    let cancellation = CancellationToken::new();
    let writer = tokio::spawn(opened.driver.unwrap().run(cancellation.clone()));
    let first = opened.log.enqueue(Bytes::from_static(b"first"), Timestamp(42)).await.unwrap().durable().await.unwrap();
    let second = opened.log.enqueue(Bytes::from_static(b"second"), Timestamp(43)).await.unwrap().durable().await.unwrap();
    assert_eq!((first.offset, second.offset), (LogOffset(0), LogOffset(1)));
    let mut tail = opened.log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(head.metadata.offset, first.offset);
    assert_eq!(head.metadata.timestamp.0 % 1000, 0);
    assert_eq!(head.payload_len, 5);
    assert_eq!(head.allocation_bound, 261);

    let memory = MemoryBudget::new(head.allocation_bound);
    let undersized = ReadReservation::new(260, memory.reserve(260).unwrap());
    assert!(matches!(replay.read_head(head, undersized).await, Err(Error::Task(message)) if message == "log read reservation is too small"));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));
    let cancelled = ReadReservation::new(261, memory.reserve(261).unwrap());
    drop(replay.read_head(head, cancelled));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

    let exact = ReadReservation::new(261, memory.reserve(261).unwrap());
    let (mut tail, record) = tokio::spawn(async move {
        let record = tail.replay().unwrap().read_head(head, exact).await.unwrap();
        (tail, record)
    }).await.unwrap();
    assert_eq!(record.metadata, head.metadata);
    let clone = record.payload.clone();
    drop(record);
    assert_eq!(clone.as_ref(), b"first");
    assert_eq!(memory.used_bytes(), 261);
    drop(clone);
    assert_eq!(memory.used_bytes(), 0);

    let head = tail.replay().unwrap().poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(head.metadata.offset, second.offset);
    let batch = tail.poll(Duration::ZERO).await.unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].metadata, head.metadata);
    assert_eq!(batch[0].payload.as_ref(), b"second");
    assert_eq!(tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), None);

    let mut closing_tail = opened.log.tail(first.offset).await.unwrap();
    let closing = closing_tail.replay().unwrap();
    let head = closing.poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(head.metadata, batch[0].metadata);
    let closure_budget = MemoryBudget::new(head.allocation_bound);
    let exact = ReadReservation::new(head.allocation_bound, closure_budget.reserve(head.allocation_bound).unwrap());
    cancellation.cancel();
    writer.await.unwrap().unwrap();
    assert!(matches!(closing.read_head(head, exact).await, Err(Error::Closed(_))));
    assert!(matches!(closing.poll_head(Duration::ZERO).await, Err(Error::Closed(_))));
    assert_eq!(closure_budget.used_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_kafka_tail_distinguishes_empty_tombstone_and_oversized_payloads() {
    let cluster = MockCluster::new(1).unwrap();
    let endpoint = cluster.bootstrap_servers();
    let configuration = KafkaClusterConfig::new(endpoint.as_str());
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", endpoint.as_str())
        .set("message.timeout.ms", "5000")
        .create().unwrap();
    for (topic, payload) in [("replay-tombstone", None), ("replay-oversized", Some(b"large".as_slice()))] {
        cluster.create_topic(topic, 1, 1).unwrap();
        let record = FutureRecord::<(), [u8]>::to(topic).partition(0);
        let record = match payload { Some(payload) => record.payload(payload), None => record };
        producer.send(record, Duration::from_secs(5)).await.unwrap();
        producer.send(FutureRecord::<(), [u8]>::to(topic).partition(0).payload(b"next"), Duration::from_secs(5)).await.unwrap();
        let mut options = KafkaLogOptions::new(topic);
        options.auto_create_topic = false;
        options.read_only = true;
        options.maximum_message_bytes = 4;
        let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
        let mut tail = opened.log.tail(LogOffset(-1)).await.unwrap();
        for timeout in [Duration::from_secs(5), Duration::ZERO] {
            let result = tail.replay().unwrap().poll_head(timeout).await;
            if payload.is_some() {
                assert!(matches!(result, Err(Error::MessageTooLarge { size: 5, maximum: 4 })));
            } else {
                assert!(matches!(result, Err(Error::Remote { key, message }) if key == topic && message == "Kafka log contains a tombstone"));
            }
        }
        let mut restarted = opened.log.tail(LogOffset(0)).await.unwrap();
        let head = restarted.replay().unwrap().poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
        assert_eq!(head.metadata.offset, LogOffset(1));
    }

    cluster.create_topic("replay-empty", 1, 1).unwrap();
    producer.send(FutureRecord::<(), [u8]>::to("replay-empty").partition(0).payload(b""), Duration::from_secs(5)).await.unwrap();
    let mut options = KafkaLogOptions::new("replay-empty");
    options.auto_create_topic = false;
    options.read_only = true;
    let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    let mut tail = opened.log.tail(LogOffset(-1)).await.unwrap();
    let replay = tail.replay().unwrap();
    let head = replay.poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
    assert_eq!(head.payload_len, 0);
    assert_eq!(head.allocation_bound, 256);
    let memory = MemoryBudget::new(256);
    let reservation = ReadReservation::new(256, memory.reserve(256).unwrap());
    let record = replay.read_head(head, reservation).await.unwrap();
    assert!(record.payload.is_empty());
    assert_eq!(memory.used_bytes(), 256);
    drop(record);
    assert_eq!(memory.used_bytes(), 0);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn java_client_properties_open_a_writable_log() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("java-props", 1, 1).unwrap();
    let mut configuration = KafkaClusterConfig::new(cluster.bootstrap_servers());
    configuration.properties_map = [
        ("max.request.size", "2097152"), ("buffer.memory", "33554432"), ("max.block.ms", "60000"), ("send.buffer.bytes", "-1"),
        ("default.api.timeout.ms", "60000"), ("max.poll.records", "500"), ("request.timeout.ms", "30000"), ("retries", "2147483647"),
        ("linger.ms", "5"), ("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"),
        ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"),
    ].into_iter().map(|(name, value)| (name.to_owned(), value.to_owned())).collect();
    let mut options = KafkaLogOptions::new("java-props");
    options.auto_create_topic = false;
    let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
    let cancellation = CancellationToken::new();
    let driver = tokio::spawn(opened.driver.unwrap().run(cancellation.clone()));
    let receipt = opened.log.enqueue(Bytes::from_static(b"java"), Timestamp(0)).await.unwrap().durable().await.unwrap();
    assert_eq!(opened.log.read_range(receipt.offset, LogOffset(receipt.offset.0 + 1)).await.unwrap()[0].payload, Bytes::from_static(b"java"));
    cancellation.cancel();
    driver.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_only_log_reads_batches_written_with_every_java_producer_codec() {
    let cluster = MockCluster::new(1).unwrap();
    let endpoint = cluster.bootstrap_servers();
    let configuration = KafkaClusterConfig::new(endpoint.as_str());
    let payload = b"xtdb-log-record ".repeat(4096);
    for codec in ["gzip", "snappy", "lz4", "zstd"] {
        let topic = format!("replay-{codec}");
        cluster.create_topic(&topic, 1, 1).unwrap();
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", endpoint.as_str())
            .set("compression.type", codec)
            .create().unwrap();
        producer.send(FutureRecord::<(), [u8]>::to(&topic).partition(0).payload(payload.as_slice()), Duration::from_secs(5)).await.unwrap();
        let mut options = KafkaLogOptions::new(topic.as_str());
        options.auto_create_topic = false;
        options.read_only = true;
        let opened = KafkaLog::open(&configuration, options, Arc::new(Warnings::default()), MemoryBudget::new(64 << 20)).await.unwrap();
        let mut tail = opened.log.tail(LogOffset(-1)).await.unwrap();
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::from_secs(5)).await.unwrap().unwrap();
        assert_eq!(head.payload_len, payload.len(), "{codec}");
        let memory = MemoryBudget::new(head.allocation_bound);
        let reservation = ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound).unwrap());
        let record = replay.read_head(head, reservation).await.unwrap();
        assert_eq!(record.payload.as_ref(), payload.as_slice(), "{codec}");
    }
}
