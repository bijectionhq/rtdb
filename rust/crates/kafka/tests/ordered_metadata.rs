use std::{sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};
use rdkafka::{ClientConfig, native_byte_account::NativeByteAccount,
    ordered_metadata::{Event, MetadataTopic, MetadataView, OrderedMetadataError, OrderedMetadataRequest, ProtocolError},
    producer::{BaseProducer, Producer}};

struct Reservation(Arc<AtomicUsize>);
impl Drop for Reservation { fn drop(&mut self) { self.0.fetch_add(1,Ordering::SeqCst); } }

fn start(producer: &BaseProducer, name: &str) -> OrderedMetadataRequest {
    let end=Instant::now()+Duration::from_secs(10);
    loop {
        match producer.client().start_ordered_metadata(&[MetadataTopic::new(name)],65536,Duration::from_secs(5)) {
            Ok(request) => return request,
            Err(OrderedMetadataError::BrokerNotReady) if Instant::now()<end => std::thread::sleep(Duration::from_millis(10)),
            result => panic!("submission: {result:?}"),
        }
    }
}
fn wait(request: &OrderedMetadataRequest) -> MetadataView<'_> {
    let end=Instant::now()+Duration::from_secs(10);
    loop {
        if let Some(result)=request.poll() { return result.unwrap(); }
        assert!(Instant::now()<end,"request did not complete");
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn ordered_transport_preserves_accounted_native_lifetimes_and_errors() {
    assert_eq!(unsafe { unittest_ordered_metadata() },0);
}
unsafe extern "C" { fn unittest_ordered_metadata() -> std::ffi::c_int; }

#[test]
fn ordered_submission_requires_an_account_and_nonblocking_readiness() {
    let producer: BaseProducer=ClientConfig::new().create().unwrap();
    assert!(matches!(producer.client().start_ordered_metadata(&[MetadataTopic::new("topic")],32,Duration::from_secs(1)),Err(OrderedMetadataError::AccountRequired)));
    let account=NativeByteAccount::new(1024,()).unwrap();
    let producer: BaseProducer=ClientConfig::new().set_native_byte_account(Arc::clone(&account)).create().unwrap();
    assert!(matches!(producer.client().start_ordered_metadata(&[MetadataTopic::new("topic")],32,Duration::ZERO),Err(OrderedMetadataError::InvalidTimeout)));
    assert!(matches!(producer.client().start_ordered_metadata(&[MetadataTopic::new("topic")],32,Duration::from_secs(1)),Err(OrderedMetadataError::BrokerNotReady)));
    assert!(matches!(producer.client().start_ordered_metadata(&[],32,Duration::from_secs(1)),Err(OrderedMetadataError::Native(_))));
    drop(producer); assert_eq!(account.used(),0);
}

#[test]
fn actual_mock_response_outlives_the_producer_and_releases_its_reservation_last() {
    let drops=Arc::new(AtomicUsize::new(0));
    let account=NativeByteAccount::new(4*1024*1024,Reservation(Arc::clone(&drops))).unwrap();
    let producer: BaseProducer=ClientConfig::new().set("test.mock.num.brokers","1").set_native_byte_account(Arc::clone(&account)).create().unwrap();
    producer.client().mock_cluster().unwrap().create_topic("ordered",3,1).unwrap();
    let request=start(&producer,"ordered");
    let view=wait(&request);
    let mut count=0;
    view.visit(|event| if let Event::Partition(partition)=event { assert_eq!(partition.error,0); count+=1; }).unwrap();
    assert_eq!(count,3);
    let raw=view.raw().to_vec();
    drop(producer);
    assert_eq!(request.poll().unwrap().unwrap().raw(),raw);
    assert!(account.used()>=65536);
    drop(account); assert_eq!(drops.load(Ordering::SeqCst),0);
    drop(request); assert_eq!(drops.load(Ordering::SeqCst),1);
}

#[test]
fn abandoning_an_accepted_request_leaves_teardown_to_drain_native_storage() {
    let account=NativeByteAccount::new(4*1024*1024,()).unwrap();
    let producer: BaseProducer=ClientConfig::new().set("test.mock.num.brokers","1").set_native_byte_account(Arc::clone(&account)).create().unwrap();
    producer.client().mock_cluster().unwrap().create_topic("abandoned",2,1).unwrap();
    let request=start(&producer,"abandoned");
    drop(request); drop(producer);
    assert_eq!(account.used(),0);
}

#[test]
fn borrowed_view_preserves_unsorted_legacy_partition_ids_and_all_truncations_fail() {
    let body=[0,0,0,0, 0,0,0,1, 0,0, 0,1,b'z', 0,0,0,2,
        0,0, 0,0,0,7, 255,255,255,255, 0,0,0,0, 0,0,0,0,
        255,255, 0,0,0,1, 0,0,0,9, 0,0,0,0, 0,0,0,0];
    let view=MetadataView::parse(0,&body).unwrap();
    let mut partitions=Vec::new();
    view.visit(|event| if let Event::Partition(partition)=event { partitions.push((partition.id,partition.error,partition.leader)); }).unwrap();
    assert_eq!(partitions,[(7,0,-1),(1,-1,9)]);
    for end in 0..body.len() { assert!(MetadataView::parse(0,&body[..end]).is_err()); }
    assert_eq!(MetadataView::parse(14,&body).unwrap_err(),ProtocolError::UnsupportedVersion);
}

#[test]
fn broker_only_metadata_omits_existing_topics_and_retains_the_response() {
    let account = NativeByteAccount::new(4 * 1024 * 1024, ()).unwrap();
    let producer: BaseProducer = ClientConfig::new()
        .set("test.mock.num.brokers", "1")
        .set_native_byte_account(Arc::clone(&account))
        .create()
        .unwrap();
    producer.client().mock_cluster().unwrap().create_topic("present", 2, 1).unwrap();

    let named = start(&producer, "present");
    assert_eq!(wait(&named).summary().topics, 1);
    drop(named);
    let deadline = Instant::now() + Duration::from_secs(10);
    let request = loop {
        match producer.client().start_ordered_metadata_brokers(65536, Duration::from_secs(5)) {
            Ok(request) => break request,
            Err(OrderedMetadataError::BrokerNotReady) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => panic!("broker-only submission: {result:?}"),
        }
    };
    let view = wait(&request);
    assert!((1..=13).contains(&view.version()));
    assert_eq!(view.summary().brokers, 1);
    assert_eq!(view.summary().topics, 0);
    assert_eq!(view.summary().partitions, 0);
    drop(producer);

    assert_eq!(request.poll_raw().unwrap().unwrap().summary(), view.summary());
    assert!(account.used() >= 65536);
    drop(request);
    assert_eq!(account.used(), 0);
}

#[test]
fn targeted_metadata_uses_learned_broker_ids_without_readiness_fallback() {
    let account = NativeByteAccount::new(4 * 1024 * 1024, ()).unwrap();
    let producer: BaseProducer = ClientConfig::new()
        .set("test.mock.num.brokers", "2")
        .set_native_byte_account(Arc::clone(&account))
        .create()
        .unwrap();
    let client = producer.client();
    client.mock_cluster().unwrap().create_topic("targeted", 2, 1).unwrap();
    let metadata = client.fetch_metadata(None, Duration::from_secs(10)).unwrap();
    assert_eq!(metadata.brokers().len(), 2);
    let first = metadata.brokers()[0].id();
    let second = metadata.brokers()[1].id();
    assert_ne!(first, second);
    drop(metadata);

    let send = |id, topics: &[MetadataTopic<'_>]| {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match client.start_ordered_metadata_at(id, topics, 65536, Duration::from_secs(5)) {
                Ok(request) => break request,
                Err(OrderedMetadataError::BrokerNotReady) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                result => panic!("targeted submission: {result:?}"),
            }
        }
    };
    let named = send(first, &[MetadataTopic::new("targeted")]);
    assert_eq!(wait(&named).summary().partitions, 2);
    let brokers_only = send(second, &[]);
    assert_eq!(wait(&brokers_only).summary().brokers, 2);
    assert_eq!(wait(&brokers_only).summary().topics, 0);

    assert!(matches!(client.start_ordered_metadata_at(i32::MAX, &[], 65536, Duration::from_secs(5)),
        Err(OrderedMetadataError::BrokerNotReady)));
    assert!(matches!(client.start_ordered_metadata_at(-1, &[], 65536, Duration::from_secs(5)),
        Err(OrderedMetadataError::Native(_))));
    drop(named);
    drop(brokers_only);
    drop(producer);
    assert_eq!(account.used(), 0);
}

#[test]
fn endpoint_metadata_uses_the_exact_configured_bootstrap_connection() {
    let cluster = rdkafka::mocking::MockCluster::new(1).unwrap();
    cluster.create_topic("endpoint", 2, 1).unwrap();
    let endpoint = cluster.bootstrap_servers();
    let (host, port) = endpoint.rsplit_once(':').unwrap();
    let port: u16 = port.parse().unwrap();
    let account = NativeByteAccount::new(4 * 1024 * 1024, ()).unwrap();
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &endpoint)
        .set("enable.sparse.connections", "false")
        .set("enable.ordered.metadata.bootstrap", "true")
        .set_native_byte_account(Arc::clone(&account))
        .create()
        .unwrap();
    let client = producer.client();
    let timeout = Duration::from_secs(5);
    assert_eq!(client.fetch_metadata(None, timeout).unwrap().brokers().len(), 1);
    let deadline = Instant::now() + Duration::from_secs(10);
    let request = loop {
        match client.start_ordered_metadata_endpoint(host, port, &[], 65536, timeout) {
            Ok(request) => break request,
            Err(OrderedMetadataError::BrokerNotReady) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => panic!("endpoint submission: {result:?}"),
        }
    };
    assert_eq!(wait(&request).summary().brokers, 1);
    assert_eq!(wait(&request).summary().topics, 0);
    assert!(matches!(
        client.start_ordered_metadata_endpoint(host, port ^ 1, &[], 65536, timeout),
        Err(OrderedMetadataError::BrokerNotReady),
    ));
    assert!(matches!(
        client.start_ordered_metadata_endpoint("invalid\0host", port, &[], 65536, timeout),
        Err(OrderedMetadataError::Native(_)),
    ));
    drop(producer);
    assert_eq!(request.poll_raw().unwrap().unwrap().summary().topics, 0);
    drop(request);
    assert_eq!(account.used(), 0);
}

#[test]
fn endpoint_metadata_requires_retained_bootstrap_ownership() {
    let account = NativeByteAccount::new(4 * 1024 * 1024, ()).unwrap();
    let producer: BaseProducer = ClientConfig::new()
        .set_native_byte_account(Arc::clone(&account))
        .create()
        .unwrap();
    assert!(matches!(
        producer.client().start_ordered_metadata_endpoint("127.0.0.1", 9092, &[], 65536, Duration::from_secs(5)),
        Err(OrderedMetadataError::Native(code)) if code == rdkafka::error::RDKafkaErrorCode::UnsupportedFeature as i32,
    ));
    drop(producer);
    assert_eq!(account.used(), 0);
}
