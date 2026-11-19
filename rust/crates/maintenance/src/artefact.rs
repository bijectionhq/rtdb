use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rtdb_format::{keys::TrieKey, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactsAdded}};
use rtdb_index::{PartitionValue, TrieCatalog, TrieState};
use rtdb_types::{Anomaly, AnomalyCategory, Result, SourcePosition, StorageEpoch, StorageVersion, TableRef};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{CompactionFailure, CompactionObserver, SnapshotSource, StderrCompactionObserver};
use crate::driver::Completion;

/// One version of one definition of an artefact kind over one table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArtefactDefinition { pub table: TableRef, pub kind: String, pub id: String, pub version: i64 }

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArtefactJob { pub definition: ArtefactDefinition, pub trie: TrieKey }

#[async_trait]
pub trait ArtefactBuilder: Send + Sync {
    /// The definitions in force as of the snapshot; jobs are a function of these and the snapshot's catalog.
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>>;
    /// Uploads the artefact under its key and returns its record; an existing identical object is not an error.
    async fn build(&self, job: &ArtefactJob, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord>;
}

#[async_trait]
pub trait ArtefactPublisher: Send + Sync {
    /// Completes after the source record is durably appended and admitted processing has applied it.
    async fn publish(&self, message: ArtefactsAdded) -> Result<SourcePosition>;
}

/// Live tries above level 0 that lack an artefact for a definition, current side first, then higher levels first.
/// A level-1 current trie below the file size target is rewritten within a block or two, so it is left to the scan.
pub fn available_artefact_jobs(catalog: &TrieCatalog, definitions: &[ArtefactDefinition]) -> Vec<ArtefactJob> {
    let mut jobs: Vec<_> = definitions.iter().flat_map(|definition| catalog.current_tries(&definition.table).into_iter()
        .filter(|trie| trie.state == TrieState::Live && trie.key.level() > 0)
        .filter(|trie| !(trie.key.level() == 1 && trie.key.recency().is_none() && trie.data_file_size < catalog.file_size_target()))
        .filter(|trie| trie.artefact(&definition.kind, &definition.id, definition.version).is_none())
        .map(|trie| ArtefactJob { definition: definition.clone(), trie: trie.key.clone() })).collect();
    jobs.sort_by(|left, right| (left.trie.recency().is_some(), std::cmp::Reverse(left.trie.level()), &left.trie, &left.definition)
        .cmp(&(right.trie.recency().is_some(), std::cmp::Reverse(right.trie.level()), &right.trie, &right.definition)));
    jobs
}

#[derive(Clone)]
pub struct ArtefactHandle { signals: mpsc::Sender<()>, requests: mpsc::Sender<Completion>, capacity: Arc<Semaphore> }

impl ArtefactHandle {
    pub fn signal_block(&self) { let _ = self.signals.try_send(()); }
    /// Returns once every available job has been built and published, or with the first failure.
    pub async fn build_all(&self) -> Result<()> {
        let permit = Arc::clone(&self.capacity).acquire_owned().await.map_err(|_| crate::closed())?;
        let (send, receive) = oneshot::channel();
        self.requests.send(Completion { reply: send, _permit: permit }).await.map_err(|_| crate::closed())?;
        receive.await.map_err(|_| crate::closed())?
    }
}

pub struct ArtefactDriver {
    snapshots: Arc<dyn SnapshotSource>,
    builder: Arc<dyn ArtefactBuilder>,
    publisher: Arc<dyn ArtefactPublisher>,
    observer: Arc<dyn CompactionObserver>,
    storage: (StorageVersion, StorageEpoch),
    parallelism: usize,
    automatic: bool,
    signals: mpsc::Receiver<()>,
    requests: mpsc::Receiver<Completion>,
}

impl ArtefactDriver {
    pub fn new(snapshots: Arc<dyn SnapshotSource>, builder: Arc<dyn ArtefactBuilder>, publisher: Arc<dyn ArtefactPublisher>, storage: (StorageVersion, StorageEpoch),
        parallelism: usize, request_capacity: usize, automatic: bool) -> Result<(Self, ArtefactHandle)> {
        if parallelism == 0 || request_capacity == 0 { return Err(crate::invalid("Artefact lane concurrency and request capacity must be positive")); }
        let (signals, signal_receiver) = mpsc::channel(1);
        let (requests, request_receiver) = mpsc::channel(request_capacity);
        Ok((Self { snapshots, builder, publisher, observer: Arc::new(StderrCompactionObserver), storage, parallelism, automatic, signals: signal_receiver, requests: request_receiver },
            ArtefactHandle { signals, requests, capacity: Arc::new(Semaphore::new(request_capacity)) }))
    }

    pub fn with_observer(mut self, observer: Arc<dyn CompactionObserver>) -> Self { self.observer = observer; self }

    fn report_failure(&self, job: &ArtefactJob, error: &Anomaly) {
        if error.code == "rtdb.maintenance/closed" || error.category == AnomalyCategory::Interrupted { return; }
        self.observer.failed(&CompactionFailure::new(&job.definition.table, &job.trie, error));
    }

    async fn available(&self, cancellation: &CancellationToken) -> Result<Vec<ArtefactJob>> {
        let snapshot = self.snapshots.snapshot();
        let definitions = self.builder.definitions(&snapshot, cancellation).await?;
        Ok(available_artefact_jobs(snapshot.trie_catalog(), &definitions))
    }

    /// The owner must cancel and join this run before releasing its storage or publication capabilities.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let workers = cancellation.child_token();
        let mut running: FuturesUnordered<BoxFuture<'static, (ArtefactJob, Result<()>)>> = FuturesUnordered::new();
        let mut active = BTreeSet::new();
        let mut failed: BTreeMap<ArtefactJob, Anomaly> = BTreeMap::new();
        let mut pending: Vec<Completion> = Vec::new();
        let outcome = loop {
            if cancellation.is_cancelled() { break Ok(()); }
            let jobs = if self.automatic || !pending.is_empty() {
                match self.available(&cancellation).await {
                    Ok(jobs) => jobs,
                    Err(error) => {
                        for request in pending.drain(..) { let _ = request.send(Err(error.clone())); }
                        Vec::new()
                    }
                }
            } else { Vec::new() };
            for job in jobs {
                if running.len() == self.parallelism { break; }
                if active.contains(&job) || failed.contains_key(&job) { continue; }
                active.insert(job.clone());
                let (builder, publisher, snapshots, token, storage) = (Arc::clone(&self.builder), Arc::clone(&self.publisher), Arc::clone(&self.snapshots), workers.clone(), self.storage);
                running.push(async move {
                    let result = async {
                        let snapshot = snapshots.snapshot();
                        let record = builder.build(&job, &snapshot, &token).await?;
                        let message = ArtefactsAdded {
                            storage_version: Some(storage.0.0), storage_epoch: Some(storage.1.0), table_name: Some(job.definition.table.schema_and_table()),
                            entries: vec![ArtefactEntry { trie_key: Some(job.trie.to_string()), record: Some(record) }], source_msg_id: None,
                        };
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => Err(crate::closed()),
                            result = publisher.publish(message) => result.map(|_| ()),
                        }
                    }.await;
                    (job, result)
                }.boxed());
            }
            if running.is_empty() {
                let result = failed.values().next().cloned().map_or(Ok(()), Err);
                for request in pending.drain(..) { let _ = request.send(result.clone()); }
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Ok(()),
                Some((job, result)) = running.next(), if !running.is_empty() => {
                    active.remove(&job);
                    if let Err(error) = result { self.report_failure(&job, &error); failed.insert(job, error); }
                }
                Some(request) = self.requests.recv() => pending.push(request),
                Some(()) = self.signals.recv() => (),
                else => break Ok(()),
            }
        };
        self.requests.close();
        self.signals.close();
        workers.cancel();
        while let Some((job, result)) = running.next().await {
            if let Err(error) = result { self.report_failure(&job, &error); }
        }
        while let Ok(request) = self.requests.try_recv() { pending.push(request); }
        let error = outcome.as_ref().err().cloned().unwrap_or_else(crate::closed);
        for request in pending { let _ = request.send(Err(error.clone())); }
        outcome
    }
}
