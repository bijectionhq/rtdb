use std::time::{Duration, Instant};

use rdkafka::{
    ClientConfig,
    mocking::MockCluster,
    native_byte_account::NativeByteAccount,
    ordered_metadata::{NativeConnectionObservation, OrderedMetadataError, RawEvent},
    producer::{BaseProducer, Producer},
};

use super::*;

pub(super) fn open_native(endpoint: &str, account: Arc<NativeByteAccount>) -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", endpoint)
        .set("enable.sparse.connections", "false")
        .set("enable.ordered.metadata.bootstrap", "true")
        .set("allow.auto.create.topics", "true")
        .set_native_byte_account(account)
        .create().unwrap()
}

pub(super) fn ready_observation<'a>(native: &'a BaseProducer, host: &str, port: u16) -> NativeConnectionObservation<'a> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observation = native.client().observe_native_endpoint(host, port).unwrap();
        let snapshot = loop {
            if let Some(result) = observation.poll() { break result.unwrap(); }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(10));
        };
        if snapshot.state_code == 6 && snapshot.transport_present != 0
            && (1..u64::MAX).contains(&snapshot.connection_generation) {
            return observation;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn the_captured_request_submits_its_original_topics_on_an_observed_bootstrap_socket() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("original", 2, 1).unwrap();
    cluster.create_topic("later", 1, 1).unwrap();
    let endpoint = cluster.bootstrap_servers();
    let (host, port) = endpoint.rsplit_once(':').unwrap();
    let port = port.parse().unwrap();
    let account = NativeByteAccount::new(4 * 1024 * 1024, ()).unwrap();
    let native = open_native(&endpoint, Arc::clone(&account));
    assert_eq!(native.client().fetch_metadata(None, Duration::from_secs(5)).unwrap().brokers().len(), 1);

    let memory = MemoryBudget::new(1 << 20);
    let mut metadata = producer(memory.clone());
    metadata.add_topic("original", 1, &mut NoIdentityWitness).unwrap();
    let request = metadata.request(2).unwrap();
    let scope = request.scope();
    assert_eq!(request.topics().collect::<Vec<_>>(), ["original"]);
    metadata.add_topic("later", 3, &mut NoIdentityWitness).unwrap();
    assert_ne!(metadata.request(4).unwrap().scope().version(), scope.version());
    let observation = ready_observation(&native, host, port);
    let exchange = request.submit_observed(observation, 65536, Duration::from_secs(5)).unwrap();
    drop(metadata);
    assert_eq!(memory.used_bytes(), 0);

    let deadline = Instant::now() + Duration::from_secs(10);
    let (returned_scope, response) = loop {
        if let Some(result) = exchange.poll() { break result.unwrap(); }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(returned_scope, scope);
    assert_eq!((response.summary().topics, response.summary().partitions), (1, 2));
    response.visit(|event| {
        if let RawEvent::Topic(topic) = event {
            assert_eq!(topic.name, Some(b"original".as_slice()));
        }
    }).unwrap();
    drop(native);
    assert_eq!(exchange.poll().unwrap().unwrap().0, scope);
    drop(exchange);
    assert_eq!(account.used(), 0);
}

#[test]
fn descriptor_denial_precedes_native_validation_and_releases_the_captured_request() {
    let native = open_native("127.0.0.1:0", NativeByteAccount::new(1 << 20, ()).unwrap());
    let observation = native.client().observe_native_endpoint("127.0.0.1", 0).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let mut metadata = producer(memory.clone());
    metadata.add_topic("original", 1, &mut NoIdentityWitness).unwrap();
    let used = memory.used_bytes();
    let request = metadata.request(2).unwrap();
    let hold = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let result = request.submit_observed(observation, 64, Duration::ZERO);
    assert!(matches!(result, Err(SubmissionFailure::Preparation(Failure::Native(_)))));
    drop(hold);
    assert_eq!(memory.used_bytes(), used);

    let observation = native.client().observe_native_endpoint("127.0.0.1", 0).unwrap();
    let result = metadata.request(2).unwrap().submit_observed(observation, 64, Duration::ZERO);
    assert!(matches!(result, Err(SubmissionFailure::Transport(OrderedMetadataError::InvalidTimeout))));
    assert_eq!(memory.used_bytes(), used);
    assert!(metadata.request(3).is_ok());
    drop(metadata);
    assert_eq!(memory.used_bytes(), 0);
}
