use super::*;
use super::super::rules::{DlqAction, Mode};
use rdkafka::mocking::MockCluster;
use rtdb_columnar::MemoryBudget;

fn rule() -> DlqRule {
    DlqRule { name: Some("guard".into()), executor: "missing".into(), mode: Mode::Read, subject: "events-value".into(), topic: Some("receipt-dlq".into()), action: Arc::new(DlqAction { name: "_default_".into(), options: BTreeMap::new() }) }
}

#[tokio::test]
async fn actual_delivery_keeps_its_record_lease_after_the_sdk_callback_until_owner_cleanup() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("receipt-dlq", 1, 1).unwrap();
    let options = BTreeMap::from([
        ("bootstrap.servers".into(), cluster.bootstrap_servers()),
        ("buffer.memory".into(), (1 << 20).to_string()),
    ]);
    let memory = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let mut producer = DlqProducer::open(&options, &mut context).unwrap();
    let record = Record::new(&rule(), "receipt-dlq", "events", false, Original::Framed, &[0,0,0,0,7,14], &mut context).unwrap();
    let lease = Arc::downgrade(&record.charge);
    producer.send(record, &mut context).await.unwrap();
    drop(context);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        producer.producer.poll(Duration::ZERO);
        if producer.producer.context().deliveries.lock().unwrap().completed.is_some() { break; }
        assert!(Instant::now() < deadline, "DLQ delivery was not reported");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(lease.upgrade().is_some());
    producer.poll();
    assert!(lease.upgrade().is_none());
    drop(producer);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn receipt_admission_rejects_the_send_before_headers_or_native_queue_ownership() {
    let cluster = MockCluster::new(1).unwrap();
    cluster.create_topic("receipt-dlq", 1, 1).unwrap();
    let options = BTreeMap::from([
        ("bootstrap.servers".into(), cluster.bootstrap_servers()),
        ("buffer.memory".into(), (1 << 20).to_string()),
    ]);
    let memory = MemoryBudget::new(1 << 26);
    let mut context = ConversionContext::new(Arc::clone(&memory));
    let mut producer = DlqProducer::open(&options, &mut context).unwrap();
    let record = Record::new(&rule(), "receipt-dlq", "events", false, Original::Framed, &[0,0,0,0,7,14], &mut context).unwrap();
    context.reserve(context.allocation_remaining() - 4095).unwrap();
    assert!(producer.send(record, &mut context).await.unwrap_err().code.ends_with("/batch-too-large"));
    assert!(producer.producer.context().deliveries.lock().unwrap().receipts.is_empty());
    drop(context);
    drop(producer);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn reported_and_unreported_receipts_release_at_their_distinct_owner_boundaries() {
    for explicit in [false, true] {
        let memory = MemoryBudget::new(1 << 27);
        let delivery_context = DeliveryContext::new(memory.reserve(4096).unwrap());
        let mut leases = Vec::new();
        for index in 0..10000 {
            let charge = memory.reserve(1).unwrap();
            leases.push(Arc::downgrade(&charge));
            let receipt_charge = memory.reserve(4096).unwrap();
            let receipt = delivery_context.register(charge, receipt_charge);
            let identity = receipt.identity;
            receipt.native_enqueue_accepted();
            if index % 2 == 0 { delivery_context.completed(identity); delivery_context.completed(identity); }
        }
        assert!(leases.iter().all(|lease| lease.upgrade().is_some()));
        if explicit {
            delivery_context.release_completed();
            assert_eq!(memory.used_bytes(), 4096 + 5000 * 4097);
            assert!(leases.iter().enumerate().all(|(index, lease)| lease.upgrade().is_some() == (index % 2 == 1)));
        }
        drop(delivery_context);
        assert_eq!(memory.used_bytes(), 0);
        assert!(leases.iter().all(|lease| lease.upgrade().is_none()));
    }
}

#[test]
fn a_poisoned_completion_lock_keeps_and_then_releases_its_receipt_owners() {
    let memory = MemoryBudget::new(1 << 20);
    let delivery_context = DeliveryContext::new(memory.reserve(4096).unwrap());
    let charge = memory.reserve(1).unwrap();
    let lease = Arc::downgrade(&charge);
    let receipt = delivery_context.register(charge, memory.reserve(4096).unwrap());
    let identity = receipt.identity;
    receipt.native_enqueue_accepted();
    delivery_context.completed(identity);
    assert!(std::panic::catch_unwind(|| {
        let _guard = delivery_context.deliveries.lock().unwrap();
        panic!("completion lock fixture");
    }).is_err());
    assert!(lease.upgrade().is_some());
    delivery_context.release_completed();
    assert!(lease.upgrade().is_none());
    drop(delivery_context);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn an_unaccepted_registration_rolls_back_without_a_native_delivery_callback() {
    let memory = MemoryBudget::new(1 << 20);
    let delivery_context = DeliveryContext::new(memory.reserve(4096).unwrap());
    let charge = memory.reserve(1).unwrap();
    let lease = Arc::downgrade(&charge);
    let receipt = delivery_context.register(charge, memory.reserve(4096).unwrap());
    assert!(lease.upgrade().is_some());
    assert_eq!(delivery_context.deliveries.lock().unwrap().receipts.len(), 1);
    drop(receipt);
    assert!(lease.upgrade().is_none());
    assert!(delivery_context.deliveries.lock().unwrap().receipts.is_empty());
    drop(delivery_context);
    assert_eq!(memory.used_bytes(), 0);
}
