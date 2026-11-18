use std::{ptr, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList, bindings as sys,
    admin::AdminClient, client::{Client, DefaultClientContext}, consumer::{BaseConsumer, Consumer},
    error::KafkaError, message::RetainedConsumerMessage, mocking::MockCluster,
    native_byte_account::NativeByteAccount, producer::{BaseProducer, FutureProducer, FutureRecord},
    types::{RDKafkaErrorCode, RDKafkaType}};

struct Reservation(Arc<AtomicUsize>);
impl Drop for Reservation {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

fn account(capacity: usize) -> (Arc<NativeByteAccount>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (NativeByteAccount::new(capacity, Reservation(Arc::clone(&drops))).unwrap(), drops)
}

struct RawConsumerFixture {
    client: *mut sys::rd_kafka_t,
    config: *mut sys::rd_kafka_conf_t,
    account: *mut sys::rd_kafka_byte_account_t,
}

impl Drop for RawConsumerFixture {
    fn drop(&mut self) {
        unsafe {
            if !self.client.is_null() { sys::rd_kafka_destroy(self.client); }
            if !self.config.is_null() { sys::rd_kafka_conf_destroy(self.config); }
            sys::rd_kafka_byte_account_destroy(self.account);
        }
    }
}

#[test]
fn consumer_enabled_configuration_and_native_duplicates_share_one_capacity_owner() {
    let (account, drops) = account(0);
    let weak = Arc::downgrade(&account);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account);
    let native = config.create_native_config().unwrap();
    let duplicate = native.clone();
    let producer: BaseProducer = config.create().unwrap();
    let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
    drop(native);
    drop(config);
    let consumer = Client::new(&ClientConfig::new(), duplicate, RDKafkaType::RD_KAFKA_CONSUMER, DefaultClientContext).unwrap();
    assert_eq!(unsafe { sys::rd_kafka_type(consumer.native_ptr()) }, RDKafkaType::RD_KAFKA_CONSUMER);
    assert_eq!(weak.upgrade().unwrap().used(), 0);
    drop(producer);
    drop(admin);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(consumer);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn replacing_the_attachment_replaces_its_entire_consumer_capability_and_owner() {
    let (first, first_drops) = account(0);
    let (second, second_drops) = account(0);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(first);
    config.set_native_byte_account(Arc::clone(&second));
    assert_eq!(first_drops.load(Ordering::SeqCst), 1);
    assert!(matches!(config.create::<BaseConsumer>(), Err(KafkaError::ClientCreation(ref message))
        if message == "Native byte accounts require a producer"));
    config.set_native_byte_account_with_consumers(second);
    let consumer: BaseConsumer = config.create().unwrap();
    drop(config);
    assert_eq!(second_drops.load(Ordering::SeqCst), 0);
    drop(consumer);
    assert_eq!(second_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn consumer_validation_failure_returns_its_configuration_and_capacity_owners() {
    let (account, drops) = account(0);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account)
        .set("group.protocol", "classic")
        .set("max.poll.interval.ms", "1000").set("session.timeout.ms", "6000");
    let native = config.create_native_config().unwrap();
    drop(config);
    let result = Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_CONSUMER, DefaultClientContext);
    assert!(matches!(result, Err(KafkaError::ClientCreation(ref message))
        if message.contains("max.poll.interval.ms") && message.contains("session.timeout.ms")));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn raw_consumer_factory_preserves_missing_input_and_validation_ownership() {
    unsafe {
        let mut raw = RawConsumerFixture { client: ptr::null_mut(), config: ptr::null_mut(), account: sys::rd_kafka_byte_account_new(0) };
        assert!(!raw.account.is_null());
        let mut error = [0 as std::ffi::c_char; 512];
        raw.config = sys::rd_kafka_conf_new();
        assert!(!raw.config.is_null());
        let original = raw.config;
        raw.client = sys::rd_kafka_new_accounted_consumer(&mut raw.config, ptr::null_mut(), error.as_mut_ptr(), error.len());
        assert!(raw.client.is_null());
        assert_eq!(raw.config, original);
        assert_eq!(sys::rd_kafka_last_error(), sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__INVALID_ARG);
        raw.client = sys::rd_kafka_new_accounted_consumer(ptr::null_mut(), raw.account, error.as_mut_ptr(), error.len());
        assert!(raw.client.is_null());
        for (name, value) in [(c"group.protocol", c"classic"), (c"max.poll.interval.ms", c"1000"), (c"session.timeout.ms", c"6000")] {
            assert_eq!(sys::rd_kafka_conf_set(raw.config, name.as_ptr(), value.as_ptr(), error.as_mut_ptr(), error.len()),
                       sys::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK);
        }
        raw.client = sys::rd_kafka_new_accounted_consumer(&mut raw.config, raw.account, error.as_mut_ptr(), error.len());
        assert!(raw.client.is_null());
        assert_eq!(raw.config, original);
        assert_eq!(sys::rd_kafka_byte_account_used(raw.account), 0);
        sys::rd_kafka_conf_destroy(raw.config);
        raw.config = ptr::null_mut();
        raw.client = sys::rd_kafka_new_accounted_consumer(&mut raw.config, raw.account, error.as_mut_ptr(), error.len());
        assert!(!raw.client.is_null());
        assert!(raw.config.is_null());
        assert_eq!(sys::rd_kafka_type(raw.client), RDKafkaType::RD_KAFKA_CONSUMER);
        sys::rd_kafka_destroy(raw.client);
        raw.client = ptr::null_mut();
        assert_eq!(sys::rd_kafka_byte_account_used(raw.account), 0);
    }
}

#[test]
fn consumer_handshake_admission_denial_is_a_nonfatal_consumption_error() {
    let cluster = MockCluster::new(1).unwrap();
    let (account, drops) = account(0);
    let weak = Arc::downgrade(&account);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account)
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("enable.sparse.connections", "false")
        .set("group.id", "account-denied").set("enable.auto.commit", "false");
    let consumer: BaseConsumer = config.create().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match consumer.poll(Duration::from_millis(100)) {
            Some(Err(KafkaError::MessageConsumption(RDKafkaErrorCode::CriticalSystemResource))) => break,
            Some(Err(KafkaError::MessageConsumptionFatal(error))) => panic!("admission denial became fatal: {error:?}"),
            _ => (),
        }
        assert!(Instant::now() < deadline, "no native admission error reached the consumer");
    }
    let mut error = [0 as std::ffi::c_char; 512];
    assert_eq!(unsafe { sys::rd_kafka_fatal_error(consumer.client().native_ptr(), error.as_mut_ptr(), error.len()) },
               sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    assert_eq!(weak.upgrade().unwrap().used(), 0);
    drop(config);
    drop(consumer);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn explicit_native_fatal_state_keeps_its_distinct_consumer_error() {
    let (account, drops) = account(0);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account).set("group.id", "account-fatal");
    let consumer: BaseConsumer = config.create().unwrap();
    assert_eq!(unsafe { sys::rd_kafka_test_fatal_error(consumer.client().native_ptr(),
        sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__FAIL, c"native account fatal fixture".as_ptr()) },
        sys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(Err(KafkaError::MessageConsumptionFatal(error))) = consumer.poll(Duration::from_millis(100)) {
            assert_eq!(error, RDKafkaErrorCode::Fail);
            break;
        }
        assert!(Instant::now() < deadline, "no native fatal event reached the consumer");
    }
    assert!(matches!(consumer.close_queue(), Err(KafkaError::ConsumerQueueClose(RDKafkaErrorCode::Fail))));
    drop(config);
    drop(consumer);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retained_uncompressed_fetch_keeps_the_account_and_native_consumer_until_final_drop() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("account-fetch", 1, 1).unwrap();
    let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", cluster.bootstrap_servers())
        .set("compression.type", "none").create().unwrap();
    let payload = vec![b'x'; 256 << 10];
    producer.send(FutureRecord::to("account-fetch").payload(payload.as_slice()).key("key"), Duration::from_secs(5)).await.unwrap();
    let (account, drops) = account(8 << 20);
    let weak = Arc::downgrade(&account);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account)
        .set("bootstrap.servers", cluster.bootstrap_servers())
        .set("group.id", "account-fetch").set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false").set("fetch.wait.max.ms", "10");
    let consumer: Arc<BaseConsumer> = Arc::new(config.create().unwrap());
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset("account-fetch", 0, Offset::Beginning).unwrap();
    consumer.assign(&assignment).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let retained = loop {
        match RetainedConsumerMessage::poll_from(&consumer, Duration::from_millis(100)) {
            Some(Ok(message)) => break message,
            Some(Err(error)) => panic!("accounted fetch failed: {error:?}"),
            None => assert!(Instant::now() < deadline, "no accounted fetched record arrived"),
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

#[test]
fn returned_consumer_metadata_keeps_its_account_after_consumer_and_config_drop() {
    let cluster = MockCluster::new(1).unwrap();
    let (account, drops) = account(8 << 20);
    let weak = Arc::downgrade(&account);
    let mut config = ClientConfig::new();
    config.set_native_byte_account_with_consumers(account).set("bootstrap.servers", cluster.bootstrap_servers());
    let consumer: BaseConsumer = config.create().unwrap();
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10)).unwrap();
    drop(config);
    drop(consumer);
    assert_eq!(metadata.brokers().len(), 1);
    assert!(!metadata.brokers()[0].host().is_empty());
    assert!(weak.upgrade().unwrap().used() > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(metadata);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
