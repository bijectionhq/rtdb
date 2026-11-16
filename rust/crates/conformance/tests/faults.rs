use async_trait::async_trait;
use bytes::Bytes;
use rtdb_conformance::faults::{Action, EventKind, FaultLog, FaultSchedule, FaultStore, KeyMatch, Operation, Phase, Rule};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, Result, object_store::StoredObject, log::{LocalLog, LocalLogOptions, Log, LogOffset, MemoryLog}};
use rtdb_types::Timestamp;
use std::{future::{Future, pending, poll_fn}, ops::Range, path::Path, sync::{Arc, atomic::{AtomicBool, Ordering}}, task::Poll, time::Duration};
use tokio_util::sync::CancellationToken;

fn rule(operation: Operation, phase: Phase, action: Action) -> Rule {
    Rule { owner: "database".into(), operation, phase, key: KeyMatch::Any, occurrence: 1, action }
}

#[derive(Default)]
struct PendingRead { inner: MemoryStore, started: AtomicBool, dropped: AtomicBool }
struct ReadGuard<'a>(&'a AtomicBool);
impl Drop for ReadGuard<'_> { fn drop(&mut self) { self.0.store(true, Ordering::SeqCst); } }
#[async_trait]
impl ObjectStore for PendingRead {
    async fn get(&self, _: &ObjectKey) -> Result<Bytes> {
        let _guard = ReadGuard(&self.dropped);
        self.started.store(true, Ordering::SeqCst);
        pending().await
    }
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> { self.inner.head(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, path: &Path) -> Result<()> { self.inner.put_file(key, path).await }
    async fn list(&self, key: Option<&ObjectKey>) -> Result<Vec<StoredObject>> { self.inner.list(key).await }
    async fn delete(&self, key: &ObjectKey) -> Result<()> { self.inner.delete(key).await }
}

#[tokio::test]
async fn cancelling_a_pending_provider_records_abandonment_and_drops_the_provider_future() {
    let schedule = FaultSchedule::new(vec![], 32).unwrap();
    let provider = Arc::new(PendingRead::default());
    let store = FaultStore::new(provider.clone(), "database".into(), schedule.clone());
    let key = ObjectKey::new("tables/t/data/page.arrow").unwrap();
    let mut read = Box::pin(store.get(&key));
    poll_fn(|context| { assert!(read.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    assert!(provider.started.load(Ordering::SeqCst));
    assert!(schedule.assert_exhausted().is_err());
    drop(read);
    assert!(provider.dropped.load(Ordering::SeqCst));
    schedule.assert_exhausted().unwrap();
    let events = schedule.events();
    let started = events.iter().find(|event| event.kind == EventKind::ProviderStarted).unwrap();
    let ended: Vec<_> = events.iter().filter(|event| event.hit == started.hit).collect();
    assert_eq!(ended.len(), 2);
    assert_eq!(ended[1].kind, EventKind::ProviderAbandoned);
    assert_eq!(ended[1].point.operation, Operation::Get);
    assert_eq!(ended[1].point.key.as_deref(), Some(key.as_str()));
    assert!(!events.iter().any(|event| event.point.phase == Phase::After));
}

#[tokio::test]
async fn trace_capacity_exhausted_by_provider_abandonment_cannot_establish_a_scenario() {
    let schedule = FaultSchedule::new(vec![], 3).unwrap();
    let provider = Arc::new(PendingRead::default());
    let store = FaultStore::new(provider.clone(), "database".into(), schedule.clone());
    let key = ObjectKey::new("tables/t/data/page.arrow").unwrap();
    let mut read = Box::pin(store.get(&key));
    poll_fn(|context| { assert!(read.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    drop(read);
    assert!(provider.dropped.load(Ordering::SeqCst));
    assert!(schedule.assert_exhausted().is_err());
    assert_eq!(schedule.events().len(), 3);
}

#[tokio::test]
async fn publication_failure_after_put_keeps_identical_retry_bytes() {
    let schedule = FaultSchedule::new(vec![rule(Operation::Put, Phase::After, Action::Fail("lost object acknowledgement".into()))], 128).unwrap();
    let inner = Arc::new(MemoryStore::default());
    let store = FaultStore::new(inner.clone(), "database".into(), schedule.clone());
    let key = ObjectKey::new("tables/t/meta/trie.arrow").unwrap();
    let payload = Bytes::from_static(b"immutable metadata");
    assert!(store.put(&key, payload.clone()).await.is_err());
    assert_eq!(inner.get(&key).await.unwrap(), payload);
    store.put(&key, payload.clone()).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap(), payload);
    assert!(store.put(&key, Bytes::from_static(b"different retry")).await.is_err());
    schedule.assert_exhausted().unwrap();
    assert!(schedule.events().iter().any(|event| event.kind == EventKind::InjectedFailure));
    assert!(schedule.events().iter().any(|event| event.kind == EventKind::ProviderFailure));
}

#[tokio::test]
async fn publication_failure_before_put_leaves_no_completion_marker() {
    let schedule = FaultSchedule::new(vec![rule(Operation::Put, Phase::Before, Action::Fail("before publication".into()))], 32).unwrap();
    let inner = Arc::new(MemoryStore::default());
    let store = FaultStore::new(inner.clone(), "database".into(), schedule.clone());
    let key = ObjectKey::new("blocks/00000000.binpb").unwrap();
    assert!(store.put(&key, Bytes::from_static(b"block")).await.is_err());
    assert!(inner.head(&key).await.is_err());
    schedule.assert_exhausted().unwrap();
}

#[tokio::test]
async fn cancelled_acknowledgement_does_not_cancel_an_accepted_append() {
    let schedule = FaultSchedule::new(vec![rule(Operation::DurableReceipt, Phase::After, Action::Hold("ack".into()))], 128).unwrap();
    let inner = Arc::new(MemoryLog::new(1024));
    let log = FaultLog::new(inner.clone(), "database".into(), schedule.clone());
    let receipt = log.enqueue(Bytes::from_static(b"accepted"), Timestamp(4)).await.unwrap();
    let mut waiting = Box::pin(receipt.durable());
    poll_fn(|context| { assert!(waiting.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    schedule.wait_for_rule(0).await.unwrap();
    assert!(schedule.assert_exhausted().is_err());
    let records = inner.read_range(LogOffset(0), LogOffset(1)).await.unwrap();
    assert_eq!(records[0].payload.as_ref(), b"accepted");
    drop(waiting);
    schedule.assert_exhausted().unwrap();
    assert!(schedule.events().iter().any(|event| event.kind == EventKind::Abandoned));
    assert_eq!(inner.latest().unwrap(), LogOffset(0));
}

#[tokio::test]
async fn local_durability_survives_lost_acknowledgement_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("LOG");
    let options = LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 4 };
    let (inner, driver) = LocalLog::open(&path, options.clone()).unwrap();
    let cancellation = CancellationToken::new();
    let owner = tokio::spawn(driver.run(cancellation.clone()));
    let schedule = FaultSchedule::new(vec![rule(Operation::DurableReceipt, Phase::After, Action::Fail("acknowledgement connection lost".into()))], 64).unwrap();
    let log = FaultLog::new(inner.clone(), "database".into(), schedule.clone());
    assert!(log.enqueue(Bytes::from_static(b"survives"), Timestamp(9)).await.unwrap().durable().await.is_err());
    schedule.assert_exhausted().unwrap();
    cancellation.cancel(); owner.await.unwrap().unwrap();
    inner.close_and_wait().await; drop(log); drop(inner);
    let (reopened, driver) = LocalLog::open(&path, options).unwrap();
    let records = reopened.read_range(LogOffset(0), LogOffset(i64::MAX)).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].payload.as_ref(), b"survives");
    assert_eq!(records[0].metadata.timestamp, Timestamp(9));
    drop(driver); reopened.close_and_wait().await;
}

#[tokio::test]
async fn post_poll_failure_replays_from_the_last_applied_position_on_a_new_tail() {
    let inner = Arc::new(MemoryLog::new(1024));
    inner.enqueue(Bytes::from_static(b"one"), Timestamp(1)).await.unwrap().durable().await.unwrap();
    inner.enqueue(Bytes::from_static(b"two"), Timestamp(2)).await.unwrap().durable().await.unwrap();
    let schedule = FaultSchedule::new(vec![rule(Operation::Poll, Phase::After, Action::Fail("reader disconnected".into()))], 128).unwrap();
    let log = FaultLog::new(inner, "database".into(), schedule.clone());
    let mut failed = log.tail(LogOffset(-1)).await.unwrap();
    assert!(failed.poll(Duration::ZERO).await.is_err());
    drop(failed);
    let mut replay = log.tail(LogOffset(-1)).await.unwrap();
    let records = replay.poll(Duration::ZERO).await.unwrap();
    assert_eq!(records.iter().map(|record| record.metadata.offset.0).collect::<Vec<_>>(), vec![0, 1]);
    schedule.assert_exhausted().unwrap();
}

#[tokio::test]
async fn gates_release_on_the_recorded_occurrence_and_close_wakes_remaining_waiters() {
    let mut selected = rule(Operation::Delete, Phase::Before, Action::Hold("before-delete".into()));
    selected.occurrence = 2;
    selected.key = KeyMatch::Prefix("tables/t/meta/".into());
    let schedule = FaultSchedule::new(vec![selected], 128).unwrap();
    let inner = Arc::new(MemoryStore::default());
    let store = FaultStore::new(inner, "database".into(), schedule.clone());
    let key = ObjectKey::new("tables/t/meta/trie.arrow").unwrap();
    store.delete(&key).await.unwrap();
    let mut deletion = Box::pin(store.delete(&key));
    poll_fn(|context| { assert!(deletion.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    schedule.wait_for_rule(0).await.unwrap();
    schedule.release("before-delete").unwrap();
    deletion.await.unwrap();
    schedule.assert_exhausted().unwrap();
    schedule.close();
    assert!(schedule.release("before-delete").is_err());

    let schedule = FaultSchedule::new(vec![rule(Operation::Delete, Phase::Before, Action::Hold("blocked".into()))], 32).unwrap();
    let store = FaultStore::new(Arc::new(MemoryStore::default()), "database".into(), schedule.clone());
    let mut deletion = Box::pin(store.delete(&key));
    poll_fn(|context| { assert!(deletion.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    schedule.close();
    assert!(deletion.await.is_err());
    schedule.assert_exhausted().unwrap();
}

#[tokio::test]
async fn missing_fault_injections_and_trace_overflow_cannot_establish_a_scenario() {
    let schedule = FaultSchedule::new(vec![rule(Operation::Put, Phase::Before, Action::Fail("never reached".into()))], 8).unwrap();
    assert!(schedule.assert_exhausted().is_err());
    let schedule = FaultSchedule::new(vec![], 1).unwrap();
    let store = FaultStore::new(Arc::new(MemoryStore::default()), "database".into(), schedule.clone());
    assert!(store.list(None).await.is_err());
    assert!(schedule.assert_exhausted().is_err());
}
