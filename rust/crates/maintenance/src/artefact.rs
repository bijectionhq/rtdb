use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rtdb_format::{keys::{TableSlug, TrieKey}, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactsAdded}};
use rtdb_index::{PartitionValue, TrieCatalog, TrieState};
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, Result, SourcePosition, StorageEpoch, StorageVersion, TableRef};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{CompactionFailure, CompactionObserver, SnapshotSource, StderrCompactionObserver};
use crate::driver::Completion;

/// One version of one definition of an artefact kind over one table.
/// Its version is the id of the transaction that made it, so a snapshot knows every version up to its basis.
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
    /// Jobs for artefacts already recorded whose files need building again, taken ahead of the snapshot's ordinary jobs.
    async fn repairs(&self, _snapshot: &Arc<PartitionValue>, _cancellation: &CancellationToken) -> Result<Vec<ArtefactJob>> { Ok(Vec::new()) }
    /// Completes when the builder has work the lane would otherwise wait for a block to find.
    async fn wait_for_work(&self) { std::future::pending::<()>().await }
}

/// What a kind of artefact is handed to build one database's artefacts.
pub struct ArtefactDatabase {
    pub name: String,
    pub store: Arc<dyn rtdb_storage::ObjectStore>,
    pub storage: Arc<rtdb_storage::cache::BufferPool>,
    pub memory: Arc<rtdb_columnar::MemoryBudget>,
    pub clock: Arc<dyn Fn() -> rtdb_types::Timestamp + Send + Sync>,
}

/// A kind of artefact a node can build, such as search files; it opens one builder per database.
pub trait ArtefactKind: Send + Sync {
    fn name(&self) -> &str;
    fn builder(&self, database: ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>>;
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

/// Objects of `kind` under the table's artefacts that no record names and no job will publish: under a live trie, for a definition neither in force nor newer than the snapshot's basis.
/// A builder uploaded each and failed before publishing it; one under a trie that is not live is left to the collector.
pub fn stray_artefacts<'a>(catalog: &TrieCatalog, table: &TableRef, slug: &TableSlug, kind: &str, definitions: &[ArtefactDefinition], basis: i64, listed: impl IntoIterator<Item = &'a ObjectKey>) -> Vec<ObjectKey> {
    listed.into_iter().filter(|object| {
        let Some((trie, found, id, version)) = slug.parse_artefact_key(object.as_str()) else { return false };
        found == kind && version <= basis
            && !definitions.iter().any(|definition| &definition.table == table && definition.kind == kind && definition.id == id && definition.version == version)
            && catalog.iter_tries(table).any(|candidate| candidate.key == trie && candidate.state == TrieState::Live && candidate.artefact(kind, &id, version).is_none())
    }).cloned().collect()
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
    strays: StraySweep,
}

/// Where the lane looks for its kind's stray objects, the definitions in force per table when it last looked (`None` until its first look, which covers every table), and the tables to look at again: their last sweep, or a job of theirs, failed.
struct StraySweep { kind: String, store: Arc<dyn ObjectStore>, swept: Option<BTreeMap<TableRef, BTreeSet<(String, i64)>>>, failed: BTreeSet<TableRef> }

impl StraySweep {
    /// Deletes the stray objects of every table whose definitions changed since the last sweep or that is to be looked at again, or of every table on the first.
    async fn sweep(&mut self, snapshot: &PartitionValue, definitions: &[ArtefactDefinition]) {
        let mut current: BTreeMap<TableRef, BTreeSet<(String, i64)>> = BTreeMap::new();
        for definition in definitions { current.entry(definition.table.clone()).or_default().insert((definition.id.clone(), definition.version)); }
        let snapshot = snapshot.snapshot();
        let tables: BTreeSet<TableRef> = match &self.swept {
            None => snapshot.tables(),
            Some(swept) => swept.keys().chain(current.keys()).filter(|table| swept.get(*table) != current.get(*table)).chain(&self.failed).cloned().collect(),
        };
        self.failed.clear();
        let basis = snapshot.tx_basis().map_or(i64::MIN, |basis| basis.tx_id.0);
        let mut swept = self.swept.take().unwrap_or_default();
        for table in tables {
            let Some(entry) = snapshot.table_entry(&table) else { continue };
            let outcome: Result<()> = async {
                let prefix = ObjectKey::new(entry.slug.artefacts_prefix()).map_err(crate::invalid)?;
                let listed = self.store.list(Some(&prefix)).await.map_err(crate::invalid)?;
                for key in stray_artefacts(snapshot.trie_catalog(), &table, &entry.slug, &self.kind, definitions, basis, listed.iter().map(|object| &object.key)) {
                    self.store.delete(&key).await.map_err(crate::invalid)?;
                }
                Ok(())
            }.await;
            if outcome.is_err() { self.failed.insert(table.clone()); }
            match current.get(&table) { Some(definitions) => { swept.insert(table, definitions.clone()); } None => { swept.remove(&table); } }
        }
        self.swept = Some(swept);
    }
}

impl ArtefactDriver {
    /// `kind` names the builder's artefacts under `store`, where the lane deletes the ones a failed builder left unpublished.
    pub fn new(snapshots: Arc<dyn SnapshotSource>, builder: Arc<dyn ArtefactBuilder>, publisher: Arc<dyn ArtefactPublisher>, storage: (StorageVersion, StorageEpoch),
        kind: &str, store: Arc<dyn ObjectStore>, parallelism: usize, request_capacity: usize, automatic: bool) -> Result<(Self, ArtefactHandle)> {
        if parallelism == 0 || request_capacity == 0 { return Err(crate::invalid("Artefact lane concurrency and request capacity must be positive")); }
        let (signals, signal_receiver) = mpsc::channel(1);
        let (requests, request_receiver) = mpsc::channel(request_capacity);
        let strays = StraySweep { kind: kind.to_string(), store, swept: None, failed: BTreeSet::new() };
        Ok((Self { snapshots, builder, publisher, observer: Arc::new(StderrCompactionObserver), storage, parallelism, automatic, signals: signal_receiver, requests: request_receiver, strays },
            ArtefactHandle { signals, requests, capacity: Arc::new(Semaphore::new(request_capacity)) }))
    }

    pub fn with_observer(mut self, observer: Arc<dyn CompactionObserver>) -> Self { self.observer = observer; self }

    fn report_failure(&self, job: &ArtefactJob, error: &Anomaly) {
        if error.code == "rtdb.maintenance/closed" || error.category == AnomalyCategory::Interrupted { return; }
        self.observer.failed(&CompactionFailure::new(&job.definition.table, &job.trie, error));
    }

    /// Strays are swept only while no job runs, since a running job may be uploading an object its definition no longer wants.
    async fn available(&mut self, cancellation: &CancellationToken, idle: bool) -> Result<Vec<ArtefactJob>> {
        let snapshot = self.snapshots.snapshot();
        let mut jobs = self.builder.repairs(&snapshot, cancellation).await?;
        let definitions = self.builder.definitions(&snapshot, cancellation).await?;
        if idle { self.strays.sweep(&snapshot, &definitions).await; }
        jobs.extend(available_artefact_jobs(snapshot.trie_catalog(), &definitions));
        Ok(jobs)
    }

    /// The owner must cancel and join this run before releasing its storage or publication capabilities.
    pub async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let workers = cancellation.child_token();
        let mut running: FuturesUnordered<BoxFuture<'static, (ArtefactJob, Result<()>)>> = FuturesUnordered::new();
        let mut active = BTreeSet::new();
        let mut failed: BTreeMap<ArtefactJob, Anomaly> = BTreeMap::new();
        let mut pending: Vec<Completion> = Vec::new();
        let builder = Arc::clone(&self.builder);
        let outcome = loop {
            if cancellation.is_cancelled() { break Ok(()); }
            let jobs = if self.automatic || !pending.is_empty() {
                match self.available(&cancellation, running.is_empty()).await {
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
                    // A job that failed after uploading leaves an object only a sweep of its table can find.
                    if let Err(error) = result { self.report_failure(&job, &error); self.strays.failed.insert(job.definition.table.clone()); failed.insert(job, error); }
                }
                Some(request) = self.requests.recv() => pending.push(request),
                // A new block may have healed whatever failed, so failed jobs become eligible again.
                Some(()) = self.signals.recv() => failed.clear(),
                () = builder.wait_for_work(), if self.automatic => (),
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
