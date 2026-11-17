use super::*;
use rdkafka::{mocking::MockCluster, ordered_metadata::{MetadataTopic, OrderedMetadataError}};
use rtdb_columnar::MemoryBudget;

#[test]
fn the_actual_dlq_producer_attaches_its_native_account_and_a_response_retains_the_capacity_lease() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("accounted-dlq", 1, 1).unwrap();
    let options = BTreeMap::from([
        ("bootstrap.servers".into(), cluster.bootstrap_servers()),
        ("buffer.memory".into(), (1 << 20).to_string()),
    ]);
    let capacity = config::Settings::parse(&options).unwrap().native_byte_capacity().unwrap();
    let memory = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let producer = DlqProducer::open(&options, &mut context).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let request = loop {
        producer.producer.poll(Duration::ZERO);
        match producer.producer.client().start_ordered_metadata(&[MetadataTopic::new("accounted-dlq")], 65536, Duration::from_secs(5)) {
            Ok(request) => break request,
            Err(OrderedMetadataError::BrokerNotReady) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            result => panic!("actual DLQ metadata submission: {result:?}"),
        }
    };
    let view = loop {
        producer.producer.poll(Duration::ZERO);
        if let Some(result) = request.poll_raw() { break result.unwrap(); }
        assert!(Instant::now() < deadline, "actual DLQ metadata did not complete");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(view.summary().brokers > 0);
    let original = view.raw().to_vec();
    drop(context);
    drop(producer);
    assert_eq!(memory.used_bytes(), capacity);
    assert_eq!(request.poll_raw().unwrap().unwrap().raw(), original);
    drop(request);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_actual_native_client_configuration_releases_the_account_after_its_context() {
    let options = BTreeMap::from([
        ("bootstrap.servers".into(), "localhost:1".into()),
        ("buffer.memory".into(), (1 << 20).to_string()),
        ("acks".into(), "not-an-acknowledgement-setting".into()),
    ]);
    let memory = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    assert!(DlqProducer::open(&options, &mut context).is_err());
    assert!(memory.used_bytes() > 0);
    drop(context);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_capacity_admission_fails_before_client_construction() {
    let options = BTreeMap::from([
        ("bootstrap.servers".into(), "localhost:1".into()),
        ("buffer.memory".into(), (1 << 20).to_string()),
    ]);
    let configuration = options.iter().fold(65_536usize, |bytes, (name, value)| bytes + name.len() + value.len()) * 8;
    let memory = MemoryBudget::new(configuration);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let cause = DlqProducer::open(&options, &mut context).err().unwrap();
    assert!(cause.code.ends_with("/batch-too-large"));
    assert_eq!(memory.used_bytes(), configuration);
    drop(context);
    assert_eq!(memory.used_bytes(), 0);
}
