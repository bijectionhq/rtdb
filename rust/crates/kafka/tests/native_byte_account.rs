use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use rdkafka::admin::AdminClient;
use rdkafka::client::{Client, DefaultClientContext};
use rdkafka::consumer::BaseConsumer;
use rdkafka::error::KafkaError;
use rdkafka::native_byte_account::NativeByteAccount;
use rdkafka::producer::FutureProducer;
use rdkafka::types::RDKafkaType;
use rdkafka::ClientConfig;

struct Reservation(Arc<AtomicUsize>);

impl Drop for Reservation {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn accounted_config() -> (ClientConfig, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let account = NativeByteAccount::new(0, Reservation(Arc::clone(&drops))).unwrap();
    assert_eq!(account.used(), 0);
    let mut configuration = ClientConfig::new();
    configuration.set_native_byte_account(account);
    (configuration, drops)
}

#[test]
fn shared_producer_handles_and_separate_admin_clients_retain_one_reservation() {
    let (configuration, drops) = accounted_config();
    let producer: FutureProducer = configuration.create().unwrap();
    let copy = producer.clone();
    let admin: AdminClient<DefaultClientContext> = configuration.clone().create().unwrap();
    drop(configuration);
    drop(producer);
    drop(admin);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(copy);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn duplicating_native_configuration_carries_the_reservation_into_the_client() {
    let (configuration, drops) = accounted_config();
    let native = configuration.create_native_config().unwrap();
    let duplicate = native.clone();
    drop(configuration);
    drop(native);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let client = Client::new(
        &ClientConfig::new(),
        duplicate,
        RDKafkaType::RD_KAFKA_PRODUCER,
        DefaultClientContext,
    )
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(client);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn consumers_reject_accounts_before_creation_and_failed_producers_release_the_reservation() {
    let (mut configuration, drops) = accounted_config();
    configuration.set("group.id", "account-test");
    let consumer = configuration.create::<BaseConsumer>();
    assert!(matches!(consumer, Err(KafkaError::ClientCreation(ref message))
        if message == "Native byte accounts require a producer"));
    configuration.set("enable.idempotence", "true").set("acks", "0");
    let native = configuration.create_native_config().unwrap();
    drop(configuration);
    let client = Client::new(
        &ClientConfig::new(),
        native,
        RDKafkaType::RD_KAFKA_PRODUCER,
        DefaultClientContext,
    );
    assert!(matches!(client, Err(KafkaError::ClientCreation(_))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn native_byte_admission_follows_frames_requests_and_queue_nodes_through_final_release() {
    assert_eq!(unsafe { unittest_byte_account() }, 0);
}

unsafe extern "C" {
    fn unittest_byte_account() -> std::ffi::c_int;
}

#[test]
fn parsed_metadata_arena_admission_and_disposal_source_regressions() {
    assert_eq!(unsafe { unittest_metadata_account() }, 0);
}

unsafe extern "C" {
    fn unittest_metadata_account() -> std::ffi::c_int;
}

#[test]
fn returned_metadata_keeps_the_account_until_its_last_native_arena_is_freed() {
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{BaseProducer, Producer};
    use std::time::Duration;

    let cluster = MockCluster::new(1).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let account = NativeByteAccount::new(8 << 20, Reservation(Arc::clone(&drops))).unwrap();
    let weak = Arc::downgrade(&account);
    let mut configuration = ClientConfig::new();
    configuration.set("bootstrap.servers", cluster.bootstrap_servers());
    configuration.set_native_byte_account(account);
    let producer: BaseProducer = configuration.create().unwrap();
    drop(configuration);
    let first = producer.client().fetch_metadata(None, Duration::from_secs(10)).unwrap();
    let second = producer.client().fetch_metadata(None, Duration::from_secs(10)).unwrap();
    drop(producer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let two_arenas = weak.upgrade().unwrap().used();
    assert!(two_arenas > 0);
    assert_eq!(second.brokers().len(), 1);
    assert!(!second.brokers()[0].host().is_empty());
    drop(first);
    let one_arena = weak.upgrade().unwrap().used();
    assert!(one_arena > 0 && one_arena < two_arenas);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(second);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
