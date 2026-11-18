use super::*;
use crate::native_byte_account::NativeByteAccount;
use std::sync::atomic::AtomicUsize;

struct Reservation(Arc<AtomicUsize>);

impl Drop for Reservation {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn real_admin_result_in_oneshot_retains_account_after_admin_client_and_config_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let account = NativeByteAccount::new(0, Reservation(drops.clone())).unwrap();
    let weak = Arc::downgrade(&account);
    let mut config = ClientConfig::new();
    config.set_native_byte_account(account);
    let admin: AdminClient<DefaultClientContext> = config.create().unwrap();
    let first = NewTopic::new("account-owner-timeout-first", 1, TopicReplication::Fixed(1));
    let second = NewTopic::new("account-owner-timeout-second", 1, TopicReplication::Fixed(1));
    let options = AdminOptions::new().request_timeout(Some(Duration::from_millis(100)));
    let receiver = admin.create_topics_inner([&first, &second], &options).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(10), receiver).await.unwrap().unwrap();
    assert!(Arc::ptr_eq(event.byte_account().unwrap(), admin.client.native_client().byte_account().unwrap()));
    assert_eq!(unsafe { rdsys::rd_kafka_event_type(event.ptr()) }, rdsys::RD_KAFKA_EVENT_CREATETOPICS_RESULT);
    assert!(matches!(event.check_error(), Err(KafkaError::AdminOp(_))));
    drop(admin);
    drop(config);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(weak.upgrade().is_some());
    assert!(!unsafe { rdsys::rd_kafka_event_CreateTopics_result(event.ptr()) }.is_null());
    drop(event);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
