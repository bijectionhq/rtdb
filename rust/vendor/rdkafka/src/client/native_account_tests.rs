use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Weak;
use std::time::Duration;

struct Reservation(Arc<AtomicUsize>);

impl Drop for Reservation {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

unsafe extern "C" {
    fn unittest_native_account_owner_enqueue(queue: *mut RDKafkaQueue) -> std::ffi::c_int;
}

fn accounted_client(capacity: usize) -> (Client, Weak<NativeByteAccount>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let account = NativeByteAccount::new(capacity, Reservation(drops.clone())).unwrap();
    let weak = Arc::downgrade(&account);
    let mut config = ClientConfig::new();
    config.set_native_byte_account(account);
    let client = Client::new(
        &config,
        config.create_native_config().unwrap(),
        RDKafkaType::RD_KAFKA_PRODUCER,
        DefaultClientContext,
    ).unwrap();
    drop(config);
    (client, weak, drops)
}

#[test]
fn independent_queue_purges_charged_ops_before_releasing_its_last_account_owner() {
    let (client, weak, drops) = accounted_client(8192);
    let queue = client.new_native_queue();
    assert!(Arc::ptr_eq(queue.byte_account().unwrap(), client.native.byte_account().unwrap()));
    assert_eq!(unsafe { unittest_native_account_owner_enqueue(queue.ptr()) }, 1);
    assert!(weak.upgrade().unwrap().used() > 0);
    drop(client);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(weak.upgrade().unwrap().used() > 0);
    drop(queue);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn polled_event_releases_its_charge_before_the_last_account_after_queue_and_client_drop() {
    let (client, weak, drops) = accounted_client(8192);
    let queue = client.new_native_queue();
    assert_eq!(unsafe { unittest_native_account_owner_enqueue(queue.ptr()) }, 1);
    let event = queue.poll(Duration::ZERO).unwrap();
    assert!(Arc::ptr_eq(event.byte_account().unwrap(), queue.byte_account().unwrap()));
    assert_eq!(unsafe { rdsys::rd_kafka_event_type(event.ptr()) }, rdsys::RD_KAFKA_EVENT_ERROR);
    assert_eq!(unsafe { rdsys::rd_kafka_event_error(event.ptr()) }, RDKafkaRespErr::RD_KAFKA_RESP_ERR__FAIL);
    drop(queue);
    drop(client);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(weak.upgrade().unwrap().used() > 0);
    drop(event);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn rejected_oneshot_handoff_destroys_the_charged_event_before_its_final_account_lease() {
    let (client, weak, drops) = accounted_client(8192);
    let queue = client.new_native_queue();
    assert_eq!(unsafe { unittest_native_account_owner_enqueue(queue.ptr()) }, 1);
    let event = queue.poll(Duration::ZERO).unwrap();
    let (sender, receiver) = futures_channel::oneshot::channel();
    drop(receiver);
    drop(queue);
    drop(client);
    assert!(weak.upgrade().unwrap().used() > 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _ = sender.send(event);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn main_queue_owner_teardown_purges_ops_before_its_remaining_handle_releases_the_account() {
    let (client, weak, drops) = accounted_client(8192);
    let queue = client.main_queue();
    assert_eq!(unsafe { unittest_native_account_owner_enqueue(queue.ptr()) }, 1);
    assert!(weak.upgrade().unwrap().used() > 0);
    drop(client);
    assert_eq!(weak.upgrade().unwrap().used(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(queue);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_poll_and_missing_consumer_queue_retain_no_extra_account_owner() {
    let (client, weak, drops) = accounted_client(0);
    let queue = client.new_native_queue();
    let owners = weak.strong_count();
    assert!(queue.poll(Duration::ZERO).is_none());
    assert!(client.consumer_queue().is_none());
    assert_eq!(weak.strong_count(), owners);
    assert_eq!(weak.upgrade().unwrap().used(), 0);
    drop(client);
    drop(queue);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn denied_native_op_is_not_published_and_does_not_retain_an_event_owner() {
    let (client, weak, drops) = accounted_client(0);
    let queue = client.new_native_queue();
    let owners = weak.strong_count();
    assert_eq!(unsafe { unittest_native_account_owner_enqueue(queue.ptr()) }, 0);
    assert!(queue.poll(Duration::ZERO).is_none());
    assert_eq!(weak.strong_count(), owners);
    assert_eq!(weak.upgrade().unwrap().used(), 0);
    drop(queue);
    drop(client);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn legacy_client_queue_and_polled_event_keep_their_unaccounted_profile() {
    let config = ClientConfig::new();
    let client = Client::new(
        &config,
        config.create_native_config().unwrap(),
        RDKafkaType::RD_KAFKA_PRODUCER,
        DefaultClientContext,
    ).unwrap();
    let queue = client.new_native_queue();
    assert!(client.native.byte_account().is_none());
    assert!(queue.byte_account().is_none());
    assert_eq!(unsafe { unittest_native_account_owner_enqueue(queue.ptr()) }, 1);
    let event = queue.poll(Duration::ZERO).unwrap();
    assert!(event.byte_account().is_none());
    assert_eq!(unsafe { rdsys::rd_kafka_event_type(event.ptr()) }, rdsys::RD_KAFKA_EVENT_ERROR);
    drop(event);
    drop(queue);
    drop(client);
}
