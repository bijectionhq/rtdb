use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rtdb_format::{keys::TrieKey, log::proto::TriesAdded};
use rtdb_types::{Anomaly, AnomalyCategory, Result, TableRef};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use crate::{CompactionFailure, CompactionJob, CompactionObserver, CompactionPublisher, SnapshotSource, LoggingCompactionObserver, available_jobs};

#[async_trait]
pub trait CompactionExecutor: Send + Sync {
    /// Cancellation is cooperative; this future must finish any started file upload before releasing its files.
    async fn execute(&self, job: &CompactionJob, cancellation: &CancellationToken) -> Result<TriesAdded>;
}

#[derive(Clone)]
pub struct CompactionHandle {
    signals: mpsc::Sender<()>,
    requests: mpsc::Sender<Completion>,
    capacity: Arc<Semaphore>,
    ignore_signal: bool,
}

impl CompactionHandle {
    pub fn signal_block(&self) { if !self.ignore_signal { let _ = self.signals.try_send(()); } }
    pub async fn compact_all(&self) -> Result<()> {
        let permit = Arc::clone(&self.capacity).acquire_owned().await.map_err(|_| crate::closed())?;
        let (send, receive) = oneshot::channel();
        self.requests.send(Completion { reply: send, _permit: permit }).await.map_err(|_| crate::closed())?;
        receive.await.map_err(|_| crate::closed())?
    }
}

pub struct CompactionDriver {
    snapshots: Arc<dyn SnapshotSource>,
    executor: Arc<dyn CompactionExecutor>,
    publisher: Arc<dyn CompactionPublisher>,
    observer: Arc<dyn CompactionObserver>,
    job_slots: Option<Arc<Semaphore>>,
    parallelism: usize,
    automatic: bool,
    signals: mpsc::Receiver<()>,
    requests: mpsc::Receiver<Completion>,
}

pub(crate) struct Completion { pub(crate) reply: oneshot::Sender<Result<()>>, pub(crate) _permit: OwnedSemaphorePermit }
impl Completion { pub(crate) fn send(self, result: Result<()>) { let _ = self.reply.send(result); } }

type JobKey = (TableRef, TrieKey);

impl CompactionDriver {
    pub fn new(snapshots: Arc<dyn SnapshotSource>, executor: Arc<dyn CompactionExecutor>, publisher: Arc<dyn CompactionPublisher>,
        parallelism: usize, request_capacity: usize, ignore_signal: bool) -> Result<(Self, CompactionHandle)> {
        if parallelism == 0 || request_capacity == 0 { return Err(crate::invalid("Maintenance concurrency and request capacity must be positive")); }
        let (signals, signal_receiver) = mpsc::channel(1);
        let (requests, request_receiver) = mpsc::channel(request_capacity);
        Ok((Self { snapshots, executor, publisher, observer: Arc::new(LoggingCompactionObserver), job_slots: None, parallelism, automatic: !ignore_signal,
                signals: signal_receiver, requests: request_receiver },
            CompactionHandle { signals, requests, capacity: Arc::new(Semaphore::new(request_capacity)), ignore_signal }))
    }

    /// Replaces the default stderr observer before the driver starts; the driver retains it until all workers finish.
    pub fn with_observer(mut self, observer: Arc<dyn CompactionObserver>) -> Self { self.observer = observer; self }

    /// Shares job execution slots across database drivers on one node.
    pub fn with_job_slots(mut self, slots: Arc<Semaphore>) -> Self {
        self.job_slots = Some(slots);
        self
    }

    fn report_failure(&self, key: &JobKey, error: &Anomaly) {
        if error.code == "rtdb.maintenance/closed" || error.category == AnomalyCategory::Interrupted { return; }
        self.observer.failed(&CompactionFailure::new(&key.0, &key.1, error));
    }

    /// The owner must cancel and join this run before releasing its storage or publication capabilities.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let workers = cancellation.child_token();
        let mut running: FuturesUnordered<BoxFuture<'static, (JobKey, Result<()>)>> = FuturesUnordered::new();
        let mut active = BTreeSet::new();
        let mut failed: BTreeMap<JobKey, Anomaly> = BTreeMap::new();
        let mut pending: Vec<Completion> = Vec::new();
        let outcome = loop {
            if cancellation.is_cancelled() { break Ok(()); }
            let jobs = if self.automatic || !pending.is_empty() {
                let snapshot = self.snapshots.snapshot();
                match available_jobs(snapshot.trie_catalog()) { Ok(jobs) => jobs, Err(error) => break Err(error) }
            } else { Vec::new() };
            for job in jobs {
                if running.len() == self.parallelism { break; }
                let key = job.key();
                if active.contains(&key) || failed.contains_key(&key) { continue; }
                active.insert(key.clone());
                let executor = Arc::clone(&self.executor);
                let publisher = Arc::clone(&self.publisher);
                let snapshots = Arc::clone(&self.snapshots);
                let job_slots = self.job_slots.clone();
                let token = workers.clone();
                running.push(async move {
                    let result = async {
                        let notification = {
                            let _slot = if let Some(slots) = job_slots {
                                Some(tokio::select! {
                                    biased;
                                    _ = token.cancelled() => return Err(crate::closed()),
                                    permit = slots.acquire_owned() => permit.map_err(|_| crate::closed())?,
                                })
                            } else { None };
                            let worker_token = token.clone();
                            let runtime = tokio::runtime::Handle::current();
                            let worker = tokio::task::spawn_blocking(move || runtime.block_on(async move {
                                if !available_jobs(snapshots.snapshot().trie_catalog())?.iter().any(|available| available == &job) {
                                    return Ok(None);
                                }
                                executor.execute(&job, &worker_token).await.map(Some)
                            }));
                            let result = worker.await.map_err(|error| Anomaly::fault(
                                "rtdb.maintenance/compaction-worker", format!("Compaction worker failed: {error}"),
                            ))??;
                            let Some(notification) = result else { return Ok(()); };
                            notification
                        };
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => Err(crate::closed()),
                            result = publisher.publish(notification) => result.map(|_| ()),
                        }
                    }.await;
                    (key, result)
                }.boxed());
            }
            if running.is_empty() {
                let result = failed.values().next().cloned().map_or(Ok(()), Err);
                for request in pending.drain(..) { let _ = request.send(result.clone()); }
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Ok(()),
                Some((key, result)) = running.next(), if !running.is_empty() => {
                    active.remove(&key);
                    if let Err(error) = result { self.report_failure(&key, &error); failed.insert(key, error); }
                }
                Some(request) = self.requests.recv() => pending.push(request),
                Some(()) = self.signals.recv() => (),
                else => break Ok(()),
            }
        };
        self.requests.close();
        self.signals.close();
        workers.cancel();
        while let Some((key, result)) = running.next().await {
            if let Err(error) = result { self.report_failure(&key, &error); }
        }
        while let Ok(request) = self.requests.try_recv() { pending.push(request); }
        let error = outcome.as_ref().err().cloned().unwrap_or_else(crate::closed);
        for request in pending { let _ = request.send(Err(error.clone())); }
        outcome
    }
}

#[async_trait]
pub trait CycleWorker: Send + Sync {
    /// Complete all started object operations before returning on cancellation.
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()>;
}

#[derive(Clone)]
pub struct CycleHandle { signals: mpsc::Sender<()>, requests: mpsc::Sender<Completion>, capacity: Arc<Semaphore> }

impl CycleHandle {
    pub fn signal(&self) { let _ = self.signals.try_send(()); }
    pub async fn await_cycle(&self) -> Result<()> {
        let permit = Arc::clone(&self.capacity).acquire_owned().await.map_err(|_| crate::closed())?;
        let (send, receive) = oneshot::channel();
        self.requests.send(Completion { reply: send, _permit: permit }).await.map_err(|_| crate::closed())?;
        receive.await.map_err(|_| crate::closed())?
    }
}

pub struct CycleDriver {
    worker: Arc<dyn CycleWorker>,
    enabled: bool,
    signals: mpsc::Receiver<()>,
    requests: mpsc::Receiver<Completion>,
}

impl CycleDriver {
    pub fn new(worker: Arc<dyn CycleWorker>, enabled: bool, request_capacity: usize) -> Result<(Self, CycleHandle)> {
        if request_capacity == 0 { return Err(crate::invalid("Maintenance request capacity must be positive")); }
        let (signals, signal_receiver) = mpsc::channel(1);
        let (requests, request_receiver) = mpsc::channel(request_capacity);
        Ok((Self { worker, enabled, signals: signal_receiver, requests: request_receiver }, CycleHandle { signals, requests, capacity: Arc::new(Semaphore::new(request_capacity)) }))
    }

    /// Awaiters are acknowledged after a cycle started after their request was received.
    /// The owner must cancel and join this run when its leader term ends.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let mut pending: Vec<Completion> = Vec::new();
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                Some(request) = self.requests.recv() => pending.push(request),
                Some(()) = self.signals.recv(), if self.enabled => (),
                else => break,
            }
            loop {
                let _ = self.signals.try_recv();
                while let Ok(request) = self.requests.try_recv() { pending.push(request); }
                let result = self.worker.collect(&cancellation).await;
                if cancellation.is_cancelled() { break; }
                if result.is_err() {
                    for request in pending.drain(..) { let _ = request.send(result.clone()); }
                    break;
                }
                let mut again = self.signals.try_recv().is_ok();
                while let Ok(request) = self.requests.try_recv() { pending.push(request); again = true; }
                if !again {
                    for request in pending.drain(..) { let _ = request.send(Ok(())); }
                    break;
                }
            }
            if cancellation.is_cancelled() { break; }
        }
        self.requests.close();
        while let Ok(request) = self.requests.try_recv() { pending.push(request); }
        for request in pending { let _ = request.send(Err(crate::closed())); }
        Ok(())
    }
}
