use std::{
    sync::{Arc, atomic::{AtomicUsize, Ordering}},
    time::{Duration, Instant},
};
use rdkafka::{
    ClientConfig, Message, Offset, TopicPartitionList, bindings as sys,
    consumer::{BaseConsumer, Consumer}, error::KafkaError,
    message::RetainedConsumerMessage, mocking::MockCluster,
    native_byte_account::NativeByteAccount, producer::{DefaultProducerContext, FutureProducer, FutureRecord},
    types::RDKafkaErrorCode,
};

struct Reservation(Arc<AtomicUsize>);

impl Drop for Reservation {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn account(capacity: usize) -> (Arc<NativeByteAccount>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (NativeByteAccount::new(capacity, Reservation(Arc::clone(&drops))).unwrap(), drops)
}

fn assigned_consumer(
    cluster: &MockCluster<'_, DefaultProducerContext>,
    topic: &str,
    account: Arc<NativeByteAccount>,
) -> (ClientConfig, Arc<BaseConsumer>) {
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account)
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", topic).set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false").set("fetch.wait.max.ms", "10");
    let consumer: Arc<BaseConsumer> = Arc::new(config.create().unwrap());
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, 0, Offset::Beginning).unwrap();
    consumer.assign(&assignment).unwrap();
    (config, consumer)
}

async fn retained_compressed_fetch(codec: &str) {
    let cluster = MockCluster::new(1).unwrap();
    let topic = format!("account-fetch-{codec}");
    cluster.create_topic(&topic, 1, 1).unwrap();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("compression.type", codec).create().unwrap();
    let payload = vec![b'x'; 256 << 10];
    producer.send(FutureRecord::to(&topic).partition(0).payload(payload.as_slice()).key("key"),
                  Duration::from_secs(5)).await.unwrap();
    let (account, drops) = account(8 << 20);
    let weak = Arc::downgrade(&account);
    let (config, consumer) = assigned_consumer(&cluster, &topic, account);
    let deadline = Instant::now() + Duration::from_secs(10);
    let retained = loop {
        match RetainedConsumerMessage::poll_from(&consumer, Duration::from_millis(100)) {
            Some(Ok(message)) => break message,
            Some(Err(error)) => panic!("accounted {codec} fetch failed: {error:?}"),
            None => assert!(Instant::now() < deadline, "no accounted {codec} record arrived"),
        }
    };
    drop(config);
    drop(consumer);
    assert_eq!(retained.as_borrowed().payload(), Some(payload.as_slice()));
    assert!(weak.upgrade().unwrap().used() > payload.len());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(retained);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retained_snappy_fetch_keeps_decoded_storage_until_final_message_drop() {
    retained_compressed_fetch("snappy").await;
}

#[tokio::test]
async fn snappy_output_denial_preserves_the_accepted_prefix_and_failed_offset() {
    let cluster = MockCluster::new(1).unwrap();
    let topic = "account-snappy-denial";
    cluster.create_topic(topic, 1, 1).unwrap();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("message.max.bytes", "12000000")
        .set("compression.type", "snappy").create().unwrap();
    let first = producer.send(FutureRecord::to(topic).partition(0).payload("accepted").key("key"),
                              Duration::from_secs(5)).await.unwrap();
    let payload = vec![b'x'; 8 << 20];
    let failed = producer.send(FutureRecord::to(topic).partition(0).payload(payload.as_slice()).key("key"),
                               Duration::from_secs(5)).await.unwrap();
    assert_eq!(failed.offset, first.offset + 1);
    let (account, drops) = account(2 << 20);
    let weak = Arc::downgrade(&account);
    let (config, consumer) = assigned_consumer(&cluster, topic, account);
    let deadline = Instant::now() + Duration::from_secs(10);
    let accepted = loop {
        match RetainedConsumerMessage::poll_from(&consumer, Duration::from_millis(100)) {
            Some(Ok(message)) => break message,
            Some(Err(error)) => panic!("error before accepted prefix: {error:?}"),
            None => assert!(Instant::now() < deadline, "no accepted Snappy prefix arrived"),
        }
    };
    assert_eq!(accepted.as_borrowed().offset(), first.offset);
    assert_eq!(accepted.as_borrowed().payload(), Some(b"accepted".as_slice()));
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::CriticalSystemResource))) => break,
            Some(Err(error)) => panic!("unexpected Snappy denial: {error:?}"),
            Some(Ok(message)) => panic!("denied message was published at {}", message.offset()),
            None => (),
        }
        assert!(Instant::now() < deadline, "no Snappy resource error arrived");
    }
    assert_eq!(consumer.position().unwrap().find_partition(topic, 0).unwrap().offset(), Offset::Offset(failed.offset));
    let mut error = [0 as std::ffi::c_char; 512];
    assert_eq!(unsafe { sys::rd_kafka_fatal_error(consumer.client().native_ptr(), error.as_mut_ptr(), error.len()) },
               sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    assert_eq!(accepted.as_borrowed().payload(), Some(b"accepted".as_slice()));
    assert!(weak.upgrade().unwrap().used() <= 2 << 20);
    drop(config);
    drop(consumer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(accepted);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retained_gzip_fetch_keeps_decoded_storage_until_final_message_drop() {
    retained_compressed_fetch("gzip").await;
}

#[tokio::test]
async fn gzip_output_denial_preserves_the_accepted_prefix_and_failed_offset() {
    let cluster = MockCluster::new(1).unwrap();
    let topic = "account-gzip-denial";
    cluster.create_topic(topic, 1, 1).unwrap();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("message.max.bytes", "12000000")
        .set("compression.type", "gzip").create().unwrap();
    let first = producer.send(FutureRecord::to(topic).partition(0).payload("accepted").key("key"),
                              Duration::from_secs(5)).await.unwrap();
    let payload = vec![b'x'; 8 << 20];
    let failed = producer.send(FutureRecord::to(topic).partition(0).payload(payload.as_slice()).key("key"),
                               Duration::from_secs(5)).await.unwrap();
    assert_eq!(failed.offset, first.offset + 1);
    let (account, drops) = account(2 << 20);
    let weak = Arc::downgrade(&account);
    let (config, consumer) = assigned_consumer(&cluster, topic, account);
    let deadline = Instant::now() + Duration::from_secs(10);
    let accepted = loop {
        match RetainedConsumerMessage::poll_from(&consumer, Duration::from_millis(100)) {
            Some(Ok(message)) => break message,
            Some(Err(error)) => panic!("error before accepted prefix: {error:?}"),
            None => assert!(Instant::now() < deadline, "no accepted Gzip prefix arrived"),
        }
    };
    assert_eq!(accepted.as_borrowed().offset(), first.offset);
    assert_eq!(accepted.as_borrowed().payload(), Some(b"accepted".as_slice()));
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::CriticalSystemResource))) => break,
            Some(Err(error)) => panic!("unexpected Gzip denial: {error:?}"),
            Some(Ok(message)) => panic!("denied message was published at {}", message.offset()),
            None => (),
        }
        assert!(Instant::now() < deadline, "no Gzip resource error arrived");
    }
    assert_eq!(consumer.position().unwrap().find_partition(topic, 0).unwrap().offset(), Offset::Offset(failed.offset));
    let mut error = [0 as std::ffi::c_char; 512];
    assert_eq!(unsafe { sys::rd_kafka_fatal_error(consumer.client().native_ptr(), error.as_mut_ptr(), error.len()) },
               sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    assert_eq!(accepted.as_borrowed().payload(), Some(b"accepted".as_slice()));
    assert!(weak.upgrade().unwrap().used() <= 2 << 20);
    drop(config);
    drop(consumer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(accepted);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
