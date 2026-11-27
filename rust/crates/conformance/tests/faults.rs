use async_trait::async_trait;
use bytes::Bytes;
use rtdb_conformance::faults::{Action, EventKind, FaultLog, FaultSchedule, FaultStore, KeyMatch, Operation, Phase, Rule};
use rtdb_storage::{Error, MemoryStore, ObjectKey, ObjectStore, Result, object_store::{ReadReservation, StoredObject},
    log::{LocalLog, LocalLogOptions, Log, LogOffset, MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}};
use rtdb_types::{MemoryBudget, Timestamp};
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
    let options = || LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 4 };
    let (inner, driver) = LocalLog::open(&path, options()).unwrap();
    let cancellation = CancellationToken::new();
    let owner = tokio::spawn(driver.run(cancellation.clone()));
    let schedule = FaultSchedule::new(vec![rule(Operation::DurableReceipt, Phase::After, Action::Fail("acknowledgement connection lost".into()))], 64).unwrap();
    let log = FaultLog::new(inner.clone(), "database".into(), schedule.clone());
    assert!(log.enqueue(Bytes::from_static(b"survives"), Timestamp(9)).await.unwrap().durable().await.is_err());
    schedule.assert_exhausted().unwrap();
    cancellation.cancel(); owner.await.unwrap().unwrap();
    inner.close_and_wait().await; drop(log); drop(inner);
    let (reopened, driver) = LocalLog::open(&path, options()).unwrap();
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

#[test]
fn selected_local_record_survives_lost_readiness_and_cancelled_delivery() {
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let (inner, driver) = LocalLog::open(directory.path().join("LOG"), LocalLogOptions { maximum_message_bytes: 128 * 1024, queue_capacity: 4 }).unwrap();
        let cancellation = CancellationToken::new();
        let writer = tokio::spawn(driver.run(cancellation.clone()));
        // Exceed LocalLog's 64 KiB inline page-cache read threshold so read_head exercises the blocking pool.
        let payload = Bytes::from(vec![b's'; 64 * 1024 + 1]);
        let metadata = inner.enqueue(payload.clone(), Timestamp(9)).await.unwrap().durable().await.unwrap();
        let mut lost_readiness = rule(Operation::ReplayPrepare, Phase::After, Action::Fail("readiness response lost".into()));
        lost_readiness.occurrence = 2;
        let schedule = FaultSchedule::new(vec![lost_readiness,
            rule(Operation::ReplayDelivery, Phase::Before, Action::Hold("selected delivery".into()))], 128).unwrap();
        let log = FaultLog::new(inner.clone(), "database".into(), schedule.clone());
        let mut tail = log.tail(LogOffset(-1)).await.unwrap();
        let replay = tail.replay().unwrap();
        let head = replay.poll_head(Duration::ZERO).await.unwrap().unwrap();
        assert_eq!(head.metadata, metadata);
        assert_eq!(head.payload_len, payload.len());
        assert!(matches!(replay.poll_head(Duration::ZERO).await,
            Err(Error::Task(message)) if message == "injected fault: readiness response lost"));
        assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

        let memory = MemoryBudget::new(head.allocation_bound);
        let reserve = || ReadReservation::new(head.allocation_bound, memory.reserve(head.allocation_bound).unwrap());
        let mut held = replay.read_head(head, reserve());
        poll_fn(|context| { assert!(held.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert_eq!(memory.used_bytes(), head.allocation_bound);
        assert!(schedule.assert_exhausted().is_err());
        assert!(schedule.events().iter().any(|event| event.point.operation == Operation::ReplayDelivery && event.kind == EventKind::Waiting));
        drop(held);
        assert_eq!(memory.used_bytes(), 0);
        schedule.assert_exhausted().unwrap();
        assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), Some(head));

        let (started, waiting) = tokio::sync::oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            paused.recv().unwrap();
        });
        waiting.await.unwrap();
        let mut delivery = replay.read_head(head, reserve());
        poll_fn(|context| { assert!(delivery.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(schedule.assert_exhausted().is_err());
        resume.send(()).unwrap();
        blocker.await.unwrap();
        let record = delivery.await.unwrap();
        assert_eq!(record.metadata, metadata);
        let clone = record.payload.clone();
        drop(record);
        assert_eq!(clone, payload);
        assert_eq!(memory.used_bytes(), head.allocation_bound);
        drop(clone);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(replay.poll_head(Duration::ZERO).await.unwrap(), None);

        let delivery_events: Vec<_> = schedule.events().into_iter().filter(|event| event.point.operation == Operation::ReplayDelivery)
            .map(|event| (event.point.phase, event.kind)).collect();
        assert_eq!(delivery_events, [(Phase::Before, EventKind::Entered), (Phase::Before, EventKind::Waiting),
            (Phase::Before, EventKind::Abandoned), (Phase::Before, EventKind::Entered), (Phase::Before, EventKind::Passed)]);
        drop(tail);
        schedule.close();
        cancellation.cancel();
        writer.await.unwrap().unwrap();
        inner.close_and_wait().await;
        schedule.assert_exhausted().unwrap();
    });
}

#[test]
fn selected_replay_rules_are_keyless_and_exclude_after_delivery_and_batch_rules_for_the_same_owner() {
    let action = || Action::Fail("injected".into());
    assert!(matches!(FaultSchedule::new(vec![rule(Operation::ReplayDelivery, Phase::After, action())], 32),
        Err(Error::Task(message)) if message == "selected replay delivery fault rules require Phase::Before"));
    for operation in [Operation::ReplayPrepare, Operation::ReplayDelivery] {
        let mut keyed = rule(operation, Phase::Before, action());
        keyed.key = KeyMatch::Prefix("log/".into());
        assert!(matches!(FaultSchedule::new(vec![keyed], 32),
            Err(Error::Task(message)) if message == "selected replay fault rules require KeyMatch::Any"));
        assert!(matches!(FaultSchedule::new(vec![rule(operation, Phase::Before, action()), rule(Operation::Poll, Phase::After, action())], 32),
            Err(Error::Task(message)) if message == "fault log 'database' mixes poll and selected replay rules"));
    }
}

#[tokio::test]
async fn named_logs_select_independent_modes_and_selected_tails_keep_direct_batch_polling() {
    let mut batch = rule(Operation::Poll, Phase::After, Action::Fail("batch observation lost".into()));
    batch.owner = "batch".into();
    let schedule = FaultSchedule::new(vec![batch, rule(Operation::ReplayPrepare, Phase::Before, Action::Fail("selected preparation refused".into()))], 64).unwrap();
    let inner = Arc::new(MemoryLog::new(1024));
    let batch_log = FaultLog::new(inner.clone(), "batch".into(), schedule.clone());
    let selected_log = FaultLog::new(inner, "database".into(), schedule.clone());
    let mut batch_tail = batch_log.tail(LogOffset(-1)).await.unwrap();
    let mut selected_tail = selected_log.tail(LogOffset(-1)).await.unwrap();
    assert!(batch_tail.replay().is_none());
    assert!(matches!(batch_tail.poll(Duration::ZERO).await,
        Err(Error::Task(message)) if message == "injected fault: batch observation lost"));
    assert!(selected_tail.poll(Duration::ZERO).await.unwrap().is_empty());
    assert!(matches!(selected_tail.replay().unwrap().poll_head(Duration::ZERO).await,
        Err(Error::Task(message)) if message == "injected fault: selected preparation refused"));
    schedule.assert_exhausted().unwrap();
}

#[tokio::test]
async fn selected_endpoint_rejects_a_backend_without_replay_capability() {
    let directory = tempfile::tempdir().unwrap();
    let inner = ReadOnlyLocalLog::open(directory.path().join("LOG"), ReadOnlyLocalLogOptions::default()).unwrap();
    let schedule = FaultSchedule::new(vec![rule(Operation::ReplayDelivery, Phase::Before, Action::Hold("selected delivery".into()))], 32).unwrap();
    let log = FaultLog::new(inner, "database".into(), schedule);
    assert!(matches!(log.tail(LogOffset(-1)).await,
        Err(Error::Task(message)) if message == "fault log 'database' does not support selected replay"));
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
