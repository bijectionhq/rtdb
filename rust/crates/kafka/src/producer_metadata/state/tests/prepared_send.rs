use rdkafka::{
    ClientConfig,
    error::{KafkaError, RDKafkaErrorCode},
    producer::{BaseProducer, BaseRecord},
};

use super::*;

struct CachedClock;

impl MetadataClock for CachedClock {
    fn milliseconds(&mut self) -> rtdb_types::Result<i64> { panic!("cached metadata requires no wall sample") }
    fn nanoseconds(&mut self) -> rtdb_types::Result<i64> { panic!("cached metadata requires no monotonic sample") }
}

async fn cached_wait(monitor: &MetadataMonitor, partition: Option<i32>) -> TopicMetadata<'_> {
    monitor.wait_for_topic(
        TopicWaitRequest { topic: "t", partition, started_ms: 10, maximum_wait_ms: 50 },
        &mut CachedClock, &mut NoIdentityWitness, || panic!("cached metadata does not wake the sender"),
    ).await.unwrap()
}

fn initial(partitions: &[(i32, i32, i32, i16)]) -> (MetadataMonitor, RequestScope) {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    let topics = [topic(b"t", 1, partitions)];
    assert_eq!(apply(&mut producer, 12, &topics, 1).unwrap(), ResponseAction::Published);
    producer.request_full(false).unwrap();
    let scope = producer.request(10).unwrap().scope();
    (MetadataMonitor::new(producer).unwrap(), scope)
}

fn publish(monitor: &MetadataMonitor, scope: RequestScope, partitions: &[(i32, i32, i32, i16)]) {
    let body = Wire::body(13, &[(1, b"broker", 9092)], &[topic(b"t", 1, partitions)], 0);
    let seeds = JavaRandomSeeds::new();
    let mut slot = None;
    let mut shuffle = JavaShuffleRandom::new(&mut slot, &seeds, || Ok(0));
    assert_eq!(monitor.apply_response(
        RawMetadataView::parse(13, &body).unwrap(), scope, 11, true, &mut NoIdentityWitness, &mut shuffle,
    ).unwrap(), ResponseAction::Published);
}

#[tokio::test]
async fn partitioning_uses_the_waited_generation_and_keeps_the_producer_counter() {
    let (monitor, scope) = initial(&[(5, 1, 1, 0), (1, 1, 1, 0)]);
    let waited = cached_wait(&monitor, None).await;
    assert_eq!((waited.append_time_ms(), waited.queue_wait_ms()), (10, 50));
    publish(&monitor, scope, &[(99, 1, 2, 0), (100, 1, 2, 0)]);

    let original = waited.prepare_round_robin(BaseRecord::<[u8], [u8]>::to("t").key(b"key")).unwrap();
    assert_eq!(original.record().partition, Some(5));
    assert_eq!(original.record().key, Some(b"key".as_slice()));
    let next = cached_wait(&monitor, None).await;
    let expected = next.snapshot().partitions("t")[1].id;
    let next = next.prepare_round_robin(BaseRecord::<[u8], [u8]>::to("t")).unwrap();
    assert_eq!(next.record().partition, Some(expected));

    drop(monitor);
    assert_eq!(original.snapshot().partition("t", 5).unwrap().epoch(), Some(1));
    assert!(original.snapshot().partition("t", 99).is_none());
    assert_eq!(next.snapshot().partition("t", 99).unwrap().epoch(), Some(2));
}

#[tokio::test]
async fn a_metadata_wait_charges_the_original_budget_before_queue_admission() {
    struct Clock;

    impl MetadataClock for Clock {
        fn milliseconds(&mut self) -> rtdb_types::Result<i64> { Ok(43) }
        fn nanoseconds(&mut self) -> rtdb_types::Result<i64> { Ok(100) }
    }

    let (monitor, scope) = initial(&[(0, 1, 1, 0)]);
    let waited = monitor.wait_for_topic(
        TopicWaitRequest { topic: "t", partition: Some(1), started_ms: 10, maximum_wait_ms: 50 },
        &mut Clock, &mut NoIdentityWitness, || {
            publish(&monitor, scope, &[(0, 1, 2, 0), (1, 1, 2, 0)]);
            Ok(())
        },
    ).await.unwrap();
    assert_eq!(waited.elapsed_ms(), 33);
    assert_eq!((waited.append_time_ms(), waited.queue_wait_ms()), (43, 17));
}

#[tokio::test]
async fn explicit_partition_and_mismatched_records_leave_the_round_robin_counter_untouched() {
    let (monitor, _) = initial(&[(5, 1, 1, 0), (1, 1, 1, 0)]);
    let explicit = cached_wait(&monitor, Some(1)).await;
    let explicit = explicit.prepare_round_robin(BaseRecord::<[u8], [u8]>::to("t").partition(1)).unwrap();
    assert_eq!(explicit.record().partition, Some(1));

    let mismatch = cached_wait(&monitor, None).await;
    assert!(matches!(mismatch.prepare_round_robin(
        BaseRecord::<[u8], [u8]>::to("other"),
    ), Err(Failure::RecordMetadataMismatch)));
    let chosen = cached_wait(&monitor, None).await;
    let chosen = chosen.prepare_round_robin(BaseRecord::<[u8], [u8]>::to("t")).unwrap();
    assert_eq!(chosen.record().partition, Some(5));
}

#[tokio::test]
async fn negative_custom_result_consumes_one_draw_before_the_source_rejects_it() {
    let (monitor, scope) = initial(&[(-9, 1, 1, 0)]);
    let waited = cached_wait(&monitor, None).await;
    assert!(matches!(waited.prepare_round_robin(
        BaseRecord::<[u8], [u8]>::to("t"),
    ), Err(Failure::InvalidPartition(-9))));

    publish(&monitor, scope, &[(5, 1, 2, 0), (1, 1, 2, 0)]);
    let next = cached_wait(&monitor, None).await;
    let selected = next.prepare_round_robin(BaseRecord::<[u8], [u8]>::to("t")).unwrap();
    assert_eq!(selected.record().partition, Some(1));
}

#[tokio::test]
async fn a_native_c_string_cannot_represent_a_waited_topic_with_a_zero_byte() {
    let mut producer = producer(MemoryBudget::new(1 << 20));
    producer.add_topic("t\0", 0, &mut NoIdentityWitness).unwrap();
    let topics = [topic(b"t\0", 1, &[(1, 1, 1, 0)])];
    assert_eq!(apply(&mut producer, 12, &topics, 1).unwrap(), ResponseAction::Published);
    let monitor = MetadataMonitor::new(producer).unwrap();
    let waited = monitor.wait_for_topic(
        TopicWaitRequest { topic: "t\0", partition: None, started_ms: 10, maximum_wait_ms: 50 },
        &mut CachedClock, &mut NoIdentityWitness, || panic!("cached metadata does not wake the sender"),
    ).await.unwrap();
    assert!(matches!(waited.prepare_round_robin(
        BaseRecord::<[u8], [u8]>::to("t\0"),
    ), Err(Failure::InvalidNativeRecordTopic)));
}

#[tokio::test]
async fn native_queue_full_returns_the_selected_partition_and_original_record() {
    let native: BaseProducer = ClientConfig::new()
        .set("queue.buffering.max.messages", "1")
        .set("message.timeout.ms", "300000")
        .create().unwrap();
    native.send(BaseRecord::<[u8], [u8]>::to("fill").payload(b"fill")).unwrap();
    let (monitor, _) = initial(&[(5, 1, 1, 0), (1, 1, 1, 0)]);
    let waited = cached_wait(&monitor, None).await;
    let chosen = waited.prepare_round_robin(
        BaseRecord::<[u8], [u8]>::to("t").key(b"key").payload(b"payload"),
    ).unwrap();
    let (record, partition, snapshot) = chosen.into_parts();
    let (error, retry) = native.send(record).unwrap_err();
    assert!(matches!(error, KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)));
    assert_eq!(retry.partition, Some(partition.id()));
    assert_eq!(retry.partition, Some(5));
    assert_eq!(retry.topic, "t");
    assert_eq!(retry.key, Some(b"key".as_slice()));
    assert_eq!(retry.payload, Some(b"payload".as_slice()));
    assert_eq!(snapshot.partitions("t").len(), 2);
}
