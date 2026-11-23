use std::{collections::BTreeMap, sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::Poll};
use async_trait::async_trait;
use rtdb_format::{catalog::legacy_table_ref, log::proto::TriesAdded};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{CompactionDriver, CompactionExecutor, CompactionJob, CompactionPublisher, CycleDriver, CycleWorker, SnapshotSource, available_jobs};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, Result, SourcePosition, TableRef, Timestamp};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct FailingExecutor(AtomicUsize);
#[async_trait]
impl CompactionExecutor for FailingExecutor {
    async fn execute(&self, _: &CompactionJob, _: &CancellationToken) -> Result<TriesAdded> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(Anomaly::fault("test/job-failed", "job failed"))
    }
}
struct Publisher;
#[async_trait]
impl CompactionPublisher for Publisher {
    async fn publish(&self, _: TriesAdded) -> Result<SourcePosition> { panic!("Failed jobs must not publish") }
}

#[tokio::test]
async fn later_compact_all_reports_the_same_failure_without_retrying_the_job() {
    let table = TableRef::new("public", "foo");
    let catalog = TrieCatalog::default().with_tries(table, [CatalogTrie { key: "l00-rc-b00".parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None }], Timestamp(0)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, TableCatalog::default(), TrieConfig::default()).unwrap()));
    let executor = Arc::new(FailingExecutor(AtomicUsize::new(0)));
    let (driver, handle) = CompactionDriver::new(state, executor.clone(), Arc::new(Publisher), 2, 8, false).unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(driver.run(cancel.clone()));
    for _ in 0..2 { assert_eq!(handle.compact_all().await.unwrap_err().code, "test/job-failed"); }
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

struct CountingSource { state: Arc<PartitionState>, calls: AtomicUsize }
impl SnapshotSource for CountingSource {
    fn snapshot(&self) -> Arc<PartitionValue> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.state.value()
    }
}

struct NotifyingExecutor;
#[async_trait]
impl CompactionExecutor for NotifyingExecutor {
    async fn execute(&self, job: &CompactionJob, _: &CancellationToken) -> Result<TriesAdded> {
        let trie = CatalogTrie::notification(job.output.clone(), 1, None)?;
        Ok(TriesAdded { tries: vec![trie.to_proto(&job.table)], ..Default::default() })
    }
}

struct ApplyingPublisher(Arc<PartitionState>);
#[async_trait]
impl CompactionPublisher for ApplyingPublisher {
    async fn publish(&self, notification: TriesAdded) -> Result<SourcePosition> {
        let [trie] = notification.tries.as_slice() else { panic!("Expected one compaction output"); };
        let table = legacy_table_ref(trie.table_name.as_deref().unwrap())?;
        self.0.add_tries(&BTreeMap::from([(table, vec![CatalogTrie::from_proto(trie)?])]), Timestamp(0))?;
        Ok(SourcePosition(0))
    }
}

#[tokio::test]
async fn manual_compaction_schedules_only_during_a_request() {
    let table = TableRef::new("public", "foo");
    let first = CatalogTrie::notification("l00-rc-b00".parse().unwrap(), 1, None).unwrap();
    let catalog = TrieCatalog::default().with_tries(table.clone(), [first], Timestamp(0)).unwrap();
    assert_eq!(available_jobs(&catalog).unwrap().len(), 1);
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, TableCatalog::default(), TrieConfig::default()).unwrap()));
    let snapshots = Arc::new(CountingSource { state: Arc::clone(&state), calls: AtomicUsize::new(0) });
    let (driver, handle) = CompactionDriver::new(snapshots.clone(), Arc::new(NotifyingExecutor), Arc::new(ApplyingPublisher(Arc::clone(&state))), 1, 1, true).unwrap();
    let cancel = CancellationToken::new();
    let mut run = Box::pin(driver.run(cancel.clone()));
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(snapshots.calls.load(Ordering::SeqCst), 0);
    handle.signal_block();
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(snapshots.calls.load(Ordering::SeqCst), 0);

    let mut request = Box::pin(handle.compact_all());
    assert!(futures::poll!(request.as_mut()).is_pending());
    // The requested job runs on a thread of its own, so the driver is driven until the request completes.
    let completed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Poll::Ready(result) = futures::poll!(request.as_mut()) { break result; }
            assert!(futures::poll!(run.as_mut()).is_pending());
            tokio::task::yield_now().await;
        }
    }).await.expect("the requested compaction completes");
    assert_eq!(completed, Ok(()));
    assert!(available_jobs(state.value().trie_catalog()).unwrap().is_empty());
    let calls = snapshots.calls.load(Ordering::SeqCst);
    assert!(calls > 0);

    let next = CatalogTrie::notification("l00-rc-b01".parse().unwrap(), 1, None).unwrap();
    state.add_tries(&BTreeMap::from([(table, vec![next])]), Timestamp(1)).unwrap();
    handle.signal_block();
    assert!(futures::poll!(run.as_mut()).is_pending());
    assert_eq!(snapshots.calls.load(Ordering::SeqCst), calls);
    assert_eq!(available_jobs(state.value().trie_catalog()).unwrap().len(), 1);
    cancel.cancel();
    run.await.unwrap();
}

struct GatedCycle { started: Semaphore, release: Semaphore, calls: AtomicUsize }
#[async_trait]
impl CycleWorker for GatedCycle {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.add_permits(1);
        tokio::select! {
            _ = cancellation.cancelled() => Err(Anomaly::fault("test/cancelled", "cancelled")),
            permit = self.release.acquire() => { permit.unwrap().forget(); Ok(()) }
        }
    }
}

#[tokio::test]
async fn concurrent_gc_awaiters_receive_a_cycle_started_after_their_arrival() {
    let worker = Arc::new(GatedCycle { started: Semaphore::new(0), release: Semaphore::new(0), calls: AtomicUsize::new(0) });
    let (driver, handle) = CycleDriver::new(worker.clone(), true, 8).unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(driver.run(cancel.clone()));
    handle.signal();
    worker.started.acquire().await.unwrap().forget();
    let mut first = Box::pin(handle.await_cycle());
    let mut second = Box::pin(handle.await_cycle());
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert!(futures::poll!(second.as_mut()).is_pending());
    worker.release.add_permits(1);
    worker.started.acquire().await.unwrap().forget();
    assert!(futures::poll!(first.as_mut()).is_pending());
    assert!(futures::poll!(second.as_mut()).is_pending());
    worker.release.add_permits(1);
    first.await.unwrap(); second.await.unwrap();
    assert_eq!(worker.calls.load(Ordering::SeqCst), 2);
    cancel.cancel(); task.await.unwrap().unwrap();
}

#[tokio::test]
async fn disabled_gc_accepts_admin_requests_and_cancellation_completes_waiters() {
    let worker = Arc::new(GatedCycle { started: Semaphore::new(0), release: Semaphore::new(0), calls: AtomicUsize::new(0) });
    let (driver, handle) = CycleDriver::new(worker.clone(), false, 8).unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(driver.run(cancel.clone()));
    let mut awaiter = Box::pin(handle.await_cycle());
    assert!(futures::poll!(awaiter.as_mut()).is_pending());
    worker.started.acquire().await.unwrap().forget();
    cancel.cancel();
    assert_eq!(awaiter.await.unwrap_err().code, "rtdb.maintenance/closed");
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn request_capacity_includes_awaiters_already_received_by_the_driver() {
    let worker = Arc::new(GatedCycle { started: Semaphore::new(0), release: Semaphore::new(0), calls: AtomicUsize::new(0) });
    let (driver, handle) = CycleDriver::new(worker.clone(), true, 1).unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(driver.run(cancel.clone()));
    let mut first = Box::pin(handle.await_cycle());
    assert!(futures::poll!(first.as_mut()).is_pending());
    worker.started.acquire().await.unwrap().forget();
    let mut second = Box::pin(handle.await_cycle());
    assert!(futures::poll!(second.as_mut()).is_pending());
    worker.release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(1), first).await.unwrap().unwrap();
    assert_eq!(worker.calls.load(Ordering::SeqCst), 1);
    assert!(futures::poll!(second.as_mut()).is_pending());
    worker.started.acquire().await.unwrap().forget();
    worker.release.add_permits(1);
    second.await.unwrap();
    cancel.cancel();
    task.await.unwrap().unwrap();
}
