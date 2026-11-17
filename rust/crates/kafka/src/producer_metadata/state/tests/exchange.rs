use std::time::{Duration, Instant};

use rdkafka::{
    ClientConfig,
    mocking::MockCluster,
    native_byte_account::NativeByteAccount,
    ordered_metadata::{OrderedMetadataError, RawEvent},
    producer::{BaseProducer, Producer},
};

use super::*;

#[test]
fn native_exchange_keeps_the_built_scope_and_wire_selection_after_state_changes() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("selected", 2, 1).unwrap();
    cluster.create_topic("other", 1, 1).unwrap();
    let endpoint = cluster.bootstrap_servers();
    let (host, port) = endpoint.rsplit_once(':').unwrap();
    let port = port.parse().unwrap();
    let account = NativeByteAccount::new(4 * 1024 * 1024, ()).unwrap();
    let client: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &endpoint)
        .set("enable.sparse.connections", "false")
        .set("enable.ordered.metadata.bootstrap", "true")
        .set("allow.auto.create.topics", "true")
        .set_native_byte_account(Arc::clone(&account))
        .create()
        .unwrap();
    assert_eq!(
        client.client().fetch_metadata(None, Duration::from_secs(5)).unwrap().brokers().len(),
        1,
    );
    let memory = MemoryBudget::new(1 << 20);
    let mut producer = ProducerMetadata::new(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        memory.clone(), &mut Observations, &mut NoIdentityWitness,
    ).unwrap();
    producer.add_topic("selected", 1, &mut NoIdentityWitness).unwrap();
    let scope = producer.request(2).unwrap().scope();
    assert_eq!(scope.kind(), RequestKind::NewTopics);
    let deadline = Instant::now() + Duration::from_secs(10);
    let request = loop {
        match producer.start_exchange(
            client.client(), MetadataTarget::Bootstrap { host, port }, 2, 65536,
            Duration::from_secs(5),
        ) {
            Ok(request) => break request,
            Err(SubmissionFailure::Transport(OrderedMetadataError::BrokerNotReady)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("Metadata exchange failed: {error:?}"),
        }
    };
    producer.add_topic("other", 3, &mut NoIdentityWitness).unwrap();
    assert_ne!(producer.request(3).unwrap().scope().version(), scope.version());
    let response_deadline = Instant::now() + Duration::from_secs(10);
    let (returned_scope, response) = loop {
        if let Some(response) = request.poll() { break response.unwrap(); }
        assert!(Instant::now() < response_deadline);
        std::thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(returned_scope, scope);
    assert_eq!(response.summary().topics, 1);
    assert_eq!(response.summary().partitions, 2);
    response.visit(|event| {
        if let RawEvent::Topic(topic) = event {
            assert_eq!(topic.name, Some(b"selected".as_slice()));
        }
    }).unwrap();
    drop(producer);
    assert_eq!(memory.used_bytes(), 0);
    drop(client);
    assert_eq!(request.poll().unwrap().unwrap().0, scope);
    assert!(account.used() >= 65536);
    drop(request);
    assert_eq!(account.used(), 0);
}
