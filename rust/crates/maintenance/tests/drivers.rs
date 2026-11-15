use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use async_trait::async_trait;
use rtdb_format::log::proto::TriesAdded;
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{CompactionDriver, CompactionExecutor, CompactionJob, CompactionPublisher, CycleDriver, CycleWorker};
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
    let catalog = TrieCatalog::default().with_tries(table, [CatalogTrie { key: "l00-rc-b00".parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live }], Timestamp(0)).unwrap();
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
