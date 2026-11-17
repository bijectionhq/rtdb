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
