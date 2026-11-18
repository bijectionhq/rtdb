use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};
use rdkafka::{ClientConfig, mocking::MockCluster, native_byte_account::NativeByteAccount,
    ordered_metadata::{MetadataTopic, NativeConnectionObservation, OrderedMetadataError, OrderedMetadataRequest},
    producer::{BaseProducer, Producer}};

struct Reservation(Arc<AtomicUsize>);
impl Drop for Reservation {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

unsafe extern "C" {
    fn unittest_connection_observation() -> std::ffi::c_int;
}

#[test]
fn native_connection_owners_controls_and_decommission_follow_exact_lifetimes() {
    assert_eq!(unsafe { unittest_connection_observation() }, 0);
}

fn observed(observation: &NativeConnectionObservation<'_>) -> rdkafka::ordered_metadata::NativeConnectionSnapshot {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(result) = observation.poll() { return result.unwrap(); }
        assert!(Instant::now() < deadline, "native observation did not complete");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn request(producer: &BaseProducer, id: i32) -> OrderedMetadataRequest {
    const NATIVE_UP: i32 = 6;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observation = producer.client().observe_native_connection(id).unwrap();
        let snapshot = observed(&observation);
        assert_eq!(snapshot.node_id, id);
        assert_ne!(snapshot.owner_serial, 0);
        if snapshot.state_code == NATIVE_UP && snapshot.transport_present != 0 &&
            snapshot.connection_generation != 0 && snapshot.connection_generation != u64::MAX {
            match observation.into_metadata(&[MetadataTopic::new("observed")], 65536, Duration::from_secs(5)) {
                Ok(request) => return request,
                Err(OrderedMetadataError::BrokerNotReady) => (),
                result => panic!("observed submission: {result:?}"),
            }
        }
        assert!(Instant::now() < deadline, "learned native connection did not become ready");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn observed_learned_submission_reuses_only_its_native_connection_and_keeps_completed_storage() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("observed", 1, 1).unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let account = NativeByteAccount::new(8 << 20, Reservation(Arc::clone(&drops))).unwrap();
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", cluster.bootstrap_servers());
    config.set("enable.sparse.connections", "false");
    config.set("enable.ordered.metadata.bootstrap", "true");
    config.set_native_byte_account(account);
    let producer: BaseProducer = config.create().unwrap();
    drop(config);
    let metadata = producer.client().fetch_metadata(None, Duration::from_secs(10)).unwrap();
    let id = metadata.brokers()[0].id();
    drop(metadata);
    assert!(matches!(producer.client().observe_native_connection(i32::MAX), Err(OrderedMetadataError::BrokerNotReady)));
    let request = request(&producer, id);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(result) = request.poll_raw() {
            assert!(result.unwrap().summary().brokers > 0);
            break;
        }
        assert!(Instant::now() < deadline, "observed Metadata request did not complete");
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(producer);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(request.poll_raw().unwrap().is_ok());
    drop(request);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
