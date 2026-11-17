use std::{ffi::CString, sync::{Arc, atomic::{AtomicUsize, Ordering}}, time::{Duration, Instant}};
use rdkafka::{ClientConfig, bindings, native_byte_account::NativeByteAccount,
    ordered_metadata::{MetadataTopic, OrderedMetadataError, ProtocolError, RawEvent},
    producer::{BaseProducer, Producer}};

struct Reservation(Arc<AtomicUsize>);
impl Drop for Reservation { fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); } }

#[test]
fn actual_raw_response_preserves_malformed_rack_and_outlives_the_producer() {
    let drops = Arc::new(AtomicUsize::new(0));
    let account = NativeByteAccount::new(4*1024*1024, Reservation(Arc::clone(&drops))).unwrap();
    let producer: BaseProducer = ClientConfig::new().set("test.mock.num.brokers", "1").set_native_byte_account(Arc::clone(&account)).create().unwrap();
    producer.client().mock_cluster().unwrap().create_topic("raw", 1, 1).unwrap();
    let rack = CString::new([0xed, 0xa0, 0x80]).unwrap();
    let mock = unsafe { bindings::rd_kafka_handle_mock_cluster(producer.client().native_ptr()) };
    assert!(!mock.is_null());
    assert_eq!(unsafe { bindings::rd_kafka_mock_broker_set_rack(mock, 1, rack.as_ptr()) }, bindings::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR);
    let deadline = Instant::now() + Duration::from_secs(10);
    let request = loop {
        match producer.client().start_ordered_metadata(&[MetadataTopic::new("raw")], 65536, Duration::from_secs(5)) {
            Ok(request) => break request,
            Err(OrderedMetadataError::BrokerNotReady) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(10)),
            result => panic!("submission: {result:?}"),
        }
    };
    let view = loop {
        if let Some(result) = request.poll_raw() { break result.unwrap(); }
        assert!(Instant::now() < deadline, "raw response did not complete");
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut seen = false;
    view.visit(|event| if let RawEvent::Broker(broker) = event {
        assert_eq!(broker.rack, Some([0xed, 0xa0, 0x80].as_slice())); seen = true;
    }).unwrap();
    assert!(seen);
    assert!(matches!(request.poll(), Some(Err(OrderedMetadataError::Protocol(ProtocolError::UnsupportedUtf8)))));
    let raw = view.raw().to_vec();
    drop(producer);
    assert_eq!(request.poll_raw().unwrap().unwrap().raw(), raw);
    drop(account); assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(request); assert_eq!(drops.load(Ordering::SeqCst), 1);
}
