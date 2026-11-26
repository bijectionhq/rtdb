use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use rtdb_format::{keys::{ArtefactOwner, ArtefactPack, TableSlug, TrieKey}, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactScope, ArtefactsAdded, ArtefactsRemoved}};
use rtdb_index::{PartitionValue, TrieCatalog, TrieState};
use rtdb_storage::{HeldView, IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, Result, SourcePosition, StorageEpoch, StorageVersion, TableRef};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{CompactionFailure, CompactionObserver, SnapshotSource, LoggingCompactionObserver};
use crate::driver::Completion;

/// One version of one definition of an artefact kind over one table.
/// Its version is the id of the transaction that made it, so a snapshot knows every version up to its basis.
/// One that is not `historical` covers the current side alone.
/// With `group_bytes`, a partition group whose members' data files hold at most that many bytes together takes one artefact over all of them, rather than one per member.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArtefactDefinition { pub table: TableRef, pub kind: String, pub id: String, pub version: i64, pub historical: bool, pub group_bytes: Option<i64>, pub packed: bool }

/// One artefact to build: over one trie's data file, or over every member's of a partition group.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ArtefactJob { pub definition: ArtefactDefinition, pub owner: ArtefactOwner }

impl ArtefactJob {
    /// The tries whose catalog entries record the built artefact, in bucket order.
    pub fn tries(&self) -> Vec<TrieKey> { self.owner.tries() }
    /// The scope the artefact's records carry.
    pub fn scope(&self) -> ArtefactScope { match self.owner { ArtefactOwner::Trie(_) => ArtefactScope::Trie, ArtefactOwner::Group(_) => ArtefactScope::PartGroup, ArtefactOwner::Pack(_) => ArtefactScope::Pack } }
    fn first_trie(&self) -> TrieKey { self.tries().swap_remove(0) }

    fn entries(&self, mut record: ArtefactRecord) -> Vec<ArtefactEntry> {
        record.scope = Some(self.scope() as i32);
        if let ArtefactOwner::Pack(pack) = &self.owner {
            record.pack_members = pack.members().iter().map(ToString::to_string).collect();
        }
        self.tries().into_iter().map(|trie| ArtefactEntry {
            trie_key: Some(trie.to_string()),
            record: Some(record.clone()),
        }).collect()
    }
}

/// Where an artefact is built from: its trie's data file already on this node's disk, as compaction has just written it, or the stored object, which the builder fetches.
#[derive(Clone, Copy)]
pub enum ArtefactSource<'a> { Local { data: &'a std::path::Path, size: u64 }, Stored }

#[async_trait]
pub trait ArtefactBuilder: Send + Sync {
    fn builds_live_pages(&self) -> bool { false }

    fn prepares_serving_search(&self) -> bool { false }

    async fn prepare_serving_search(&self, _snapshot: rtdb_index::Snapshot, _cancellation: &CancellationToken) -> Result<Option<rtdb_index::ServingSearch>> { Ok(None) }

    fn journals_search_deltas(&self) -> bool { false }

    async fn prepare_search_delta(&self, _snapshot: rtdb_index::Snapshot, _write: std::sync::Arc<rtdb_index::StagedWrite>,
        _cancellation: &CancellationToken) -> Result<Option<rtdb_index::PreparedSearchDelta>> { Ok(None) }

    async fn install_search_delta(&self, snapshot: rtdb_index::Snapshot, _write: &rtdb_index::StagedWrite, _delta: Option<rtdb_index::SearchDelta>,
        cancellation: &CancellationToken) -> Result<Option<rtdb_index::ServingSearch>> {
        self.prepare_serving_search(snapshot, cancellation).await
    }

    async fn seal_serving_search(&self, _snapshot: &Arc<PartitionValue>, _table: &TableRef, _trie: &TrieKey,
        _file: &std::path::Path, _size: u64, _cancellation: &CancellationToken) -> Result<Vec<ArtefactRecord>> { Ok(Vec::new()) }

    fn retire_with_owner(&self, _record: &ArtefactRecord) -> bool { false }

    async fn build_live_pages(&self, _snapshot: &Arc<PartitionValue>, _cancellation: &CancellationToken) -> Result<()> { Ok(()) }

    async fn maintain_independent(&self, _snapshots: &dyn SnapshotSource, _publisher: &dyn ArtefactPublisher,
        _storage: (StorageVersion, StorageEpoch), _cancellation: &CancellationToken) -> Result<()> { Ok(()) }

    /// The definitions in force as of the snapshot; jobs are a function of these and the snapshot's catalog.
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>>;
    /// Includes compatible formats that remain readable while only the active format receives new build jobs.
    fn retained_definitions(&self, definitions: &[ArtefactDefinition]) -> Vec<ArtefactDefinition> { definitions.to_vec() }
    /// Uploads the artefact under its key and returns its record; an existing identical object is not an error.
    async fn build(&self, job: &ArtefactJob, source: ArtefactSource<'_>, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord>;
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
    pub history: Option<Arc<dyn crate::TableHistorySource>>,
    pub clock: Arc<dyn Fn() -> rtdb_types::Timestamp + Send + Sync>,
}

/// A kind of artefact a node can build, such as search files; it opens one builder per database.
pub trait ArtefactKind: Send + Sync {
    fn name(&self) -> &str;
    fn enables_event_history(&self) -> bool { false }
    fn builder(&self, database: ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>>;
}

#[async_trait]
pub trait ArtefactPublisher: Send + Sync {
    /// Completes after the source record is durably appended and admitted processing has applied it.
    async fn publish(&self, message: ArtefactsAdded) -> Result<SourcePosition>;
    /// Completes after the removal is durably appended and admitted processing has applied it.
    async fn remove(&self, message: ArtefactsRemoved) -> Result<SourcePosition>;
    /// Holds the recovery gate through durable source append and releases it before waiting for application.
    async fn remove_with_recovery_gate(&self, _message: ArtefactsRemoved, _gate: Arc<dyn Send + Sync>) -> Result<SourcePosition> {
        Err(Anomaly::unsupported("rtdb.maintenance/search-prune", "The artefact publisher cannot submit a prune under the recovery gate"))
    }
}

/// Whether a definition wants an artefact on a trie: above level 0, other than a level-1 current trie below the file size target, which is rewritten within a block or two, and historical only for a historical definition.
pub fn wants(definition: &ArtefactDefinition, trie: &TrieKey, size: i64, file_size_target: i64) -> bool {
    (definition.historical || trie.recency().is_none()) && (definition.packed
        || (trie.level() > 0 && !(trie.level() == 1 && trie.recency().is_none() && size < file_size_target)))
}

/// Live tries above level 0 that lack an artefact for a definition, current side first, then higher levels first; historical tries only for a historical definition.
/// A level-1 current trie below the file size target is rewritten within a block or two, so it is left to the scan.
/// For a definition with `group_bytes`, a partition group every member of which is live and lacks the artefact, within that many bytes, is one job; a group that is not becomes a job per member that lacks it.
pub fn available_artefact_jobs(catalog: &TrieCatalog, definitions: &[ArtefactDefinition]) -> Vec<ArtefactJob> {
    let mut jobs = Vec::new();
    for definition in definitions {
        if definition.packed {
            jobs.extend(packed_jobs(catalog, definition));
            continue;
        }
        let lacking = |trie: &rtdb_index::CatalogTrie| trie.state == TrieState::Live && trie.artefact(&definition.kind, &definition.id, definition.version).is_none();
        let mut grouped = BTreeSet::new();
        for trie in catalog.current_tries(&definition.table).into_iter()
            .filter(|trie| wants(definition, &trie.key, trie.data_file_size, catalog.file_size_target()))
            .filter(|trie| lacking(trie)) {
            if let (Some(limit), Some(group)) = (definition.group_bytes, trie.key.part_group()) {
                if grouped.contains(&group) { continue; }
                let members = catalog.group_members(&definition.table, &group);
                let size = members.iter().fold(0i64, |total, member| total.saturating_add(member.data_file_size));
                if members.len() == group.members().len() && members.iter().all(|member| lacking(member)) && size <= limit {
                    grouped.insert(group.clone());
                    jobs.push(ArtefactJob { definition: definition.clone(), owner: ArtefactOwner::Group(group) });
                    continue;
                }
            }
            jobs.push(ArtefactJob { definition: definition.clone(), owner: ArtefactOwner::Trie(trie.key.clone()) });
        }
    }
    jobs.sort_by_cached_key(|job| { let trie = job.first_trie(); (trie.recency().is_some(), std::cmp::Reverse(trie.level()), trie, job.definition.clone()) });
    jobs
}

fn packed_jobs(catalog: &TrieCatalog, definition: &ArtefactDefinition) -> Vec<ArtefactJob> {
    let mut groups = Vec::<(Vec<&rtdb_index::CatalogTrie>, i64)>::new();
    let mut jobs = Vec::new();
    let record = |trie: &rtdb_index::CatalogTrie| trie.artefact(&definition.kind, &definition.id, definition.version).map(ArtefactRecord::scope);
    let limit = definition.group_bytes.unwrap_or(0);
    for trie in catalog.current_tries(&definition.table).into_iter()
        .filter(|trie| wants(definition, &trie.key, trie.data_file_size, catalog.file_size_target())) {
        let scope = record(trie);
        if scope.is_some_and(|scope| scope != ArtefactScope::Trie) { continue; }
        if trie.key.level() == 0 || trie.key.part().is_empty() || trie.data_file_size > limit {
            if scope.is_none() { jobs.push(ArtefactJob { definition: definition.clone(), owner: ArtefactOwner::Trie(trie.key.clone()) }); }
            continue;
        }
        if let Some((members, bytes)) = groups.iter_mut().find(|(members, bytes)| {
            members.len() < ArtefactPack::MAX_MEMBERS && bytes.saturating_add(trie.data_file_size) <= limit
                && members.iter().all(|member| member.key.recency() == trie.key.recency()
                    && !trie.key.part().starts_with(member.key.part()) && !member.key.part().starts_with(trie.key.part()))
        }) {
            members.push(trie);
            *bytes += trie.data_file_size;
        } else {
            groups.push((vec![trie], trie.data_file_size));
        }
    }
    for (members, _) in groups {
        let keys = members.iter().map(|member| member.key.clone()).collect();
        if let Ok(pack) = ArtefactPack::new(keys) {
            jobs.push(ArtefactJob { definition: definition.clone(), owner: ArtefactOwner::Pack(pack) });
        } else {
            jobs.extend(members.into_iter().filter(|member| record(member).is_none()).map(|member| ArtefactJob {
                definition: definition.clone(), owner: ArtefactOwner::Trie(member.key.clone()),
            }));
        }
    }
    jobs
}

/// A definition version no job will build again: neither in force, nor newer than the snapshot's basis and so perhaps in force beyond it.
fn retired(definitions: &[ArtefactDefinition], table: &TableRef, kind: &str, id: &str, version: i64, basis: i64) -> bool {
    version <= basis && !definitions.iter().any(|definition| &definition.table == table && definition.kind == kind && definition.id == id && definition.version == version)
}

/// A trie that is not live is left to the collector.
pub fn retired_artefacts(catalog: &TrieCatalog, table: &TableRef, kind: &str, definitions: &[ArtefactDefinition], basis: i64) -> Vec<(TrieKey, ArtefactRecord)> {
    catalog.iter_tries(table).filter(|trie| trie.state == TrieState::Live).flat_map(|trie| trie.artefacts.iter()
        .filter(|record| record.kind() == kind && retired(definitions, table, kind, record.definition_id(), record.definition_version(), basis))
        .map(|record| (trie.key.clone(), record.clone()))).collect()
}

/// Objects of `kind`, for a retired definition, under a live trie or a partition group with a live member, that no live trie they cover records: a builder uploaded each and failed before publishing it.
pub fn stray_artefacts<'a>(catalog: &TrieCatalog, table: &TableRef, slug: &TableSlug, kind: &str, definitions: &[ArtefactDefinition], basis: i64, listed: impl IntoIterator<Item = &'a ObjectKey>) -> Vec<ObjectKey> {
    listed.into_iter().filter(|object| {
        let Some((owner, found, id, version)) = slug.parse_artefact_key(object.as_str()) else { return false };
        let live: Vec<_> = owner.tries().iter().filter_map(|key| catalog.trie(table, key)).filter(|trie| trie.state == TrieState::Live).collect();
        found == kind && retired(definitions, table, kind, &id, version, basis) && !live.is_empty() && live.iter().all(|trie| trie.artefact(kind, &id, version).is_none())
    }).cloned().collect()
}

fn input_collected(error: &Anomaly, catalog: &TrieCatalog, table: &TableRef, slug: &TableSlug, key: &TrieKey) -> bool {
    error.code == "rtdb.storage/missing-object"
        && error.context.get("key").and_then(|key| key.as_str()).is_some_and(|missing| missing == slug.data_key(key).as_str())
        && catalog.trie(table, key).is_none_or(|trie| matches!(trie.state, TrieState::Garbage { .. }))
}

async fn build_stored(
    builder: &dyn ArtefactBuilder, store: &dyn ObjectStore, job: &ArtefactJob,
    snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken,
) -> Result<ArtefactRecord> {
    let captured = snapshot.snapshot();
    let live = job.tries().iter().all(|key| {
        snapshot.trie_catalog().trie(&job.definition.table, key).is_some_and(|trie| trie.state == TrieState::Live)
    });
    // The compacted inventory names live inputs. A retry with an older selection needs the conservative hold.
    let paths = live.then(|| captured.catalog_file_paths()).flatten();
    let view = HeldView::catalog(store, snapshot.clone(), captured.catalog_cut(), paths).await?;
    IoRegion::reading(view, builder.build(job, ArtefactSource::Stored, snapshot, cancellation)).await
}

async fn build_entries(job: &ArtefactJob, builder: &dyn ArtefactBuilder, store: &dyn ObjectStore,
    snapshots: &dyn SnapshotSource, cancellation: &CancellationToken,
) -> Result<Vec<ArtefactEntry>> {
    let snapshot = snapshots.snapshot();
    let error = match build_stored(builder, store, job, &snapshot, cancellation).await {
        Ok(record) => return Ok(job.entries(record)),
        Err(error) => error,
    };
    if matches!(job.owner, ArtefactOwner::Trie(_)) { return Err(error); }
    let members = job.tries();
    let table = &job.definition.table;
    let snapshot = snapshots.snapshot();
    let catalog = snapshot.trie_catalog();
    let Some(table_entry) = snapshot.table_catalog().entry(table) else { return Err(error) };
    let slug = &table_entry.slug;
    if !members.iter().any(|key| input_collected(&error, catalog, table, slug, key)) {
        return Err(error);
    }

    let mut entries = Vec::new();
    for trie in members.iter().filter_map(|key| catalog.trie(table, key)).filter(|trie| trie.state == TrieState::Live) {
        if trie.artefact(&job.definition.kind, &job.definition.id, job.definition.version).is_some_and(|record| record.scope() == ArtefactScope::Trie) {
            continue;
        }
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let replacement = ArtefactJob { definition: job.definition.clone(), owner: ArtefactOwner::Trie(trie.key.clone()) };
        match build_stored(builder, store, &replacement, &snapshot, cancellation).await {
            Ok(record) => entries.extend(replacement.entries(record)),
            // GC can collect another sibling while we build the replacements.
            Err(error) if input_collected(&error, snapshots.snapshot().trie_catalog(), table, slug, &trie.key) => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(entries)
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
    retirement: Retirement,
}

/// How the lane retires its kind's artefacts; `looked` is `None` until its first look, which covers every table.
struct Retirement { kind: String, store: Arc<dyn ObjectStore>, publisher: Arc<dyn ArtefactPublisher>, storage: (StorageVersion, StorageEpoch), looked: Option<BTreeMap<TableRef, BTreeSet<(String, i64)>>>, again: BTreeSet<TableRef> }

impl Retirement {
    /// Retires the artefacts of every table whose definitions changed since the last look or that is to be looked at again, or of every table on the first look.
    async fn retire(&mut self, snapshot: &PartitionValue, definitions: &[ArtefactDefinition], builder: &dyn ArtefactBuilder) {
        let mut current: BTreeMap<TableRef, BTreeSet<(String, i64)>> = BTreeMap::new();
        for definition in definitions { current.entry(definition.table.clone()).or_default().insert((definition.id.clone(), definition.version)); }
        let snapshot = snapshot.snapshot();
        let tables: BTreeSet<TableRef> = match &self.looked {
            None => snapshot.tables(),
            Some(looked) => looked.keys().chain(current.keys()).filter(|table| looked.get(*table) != current.get(*table)).chain(&self.again).cloned().collect(),
        };
        self.again.clear();
        let basis = snapshot.tx_basis().map_or(i64::MIN, |basis| basis.tx_id.0);
        let mut looked = self.looked.take().unwrap_or_default();
        for table in tables {
            let Some(entry) = snapshot.table_entry(&table) else { continue };
            let outcome: Result<()> = async {
                let mut records = retired_artefacts(snapshot.trie_catalog(), &table, &self.kind, definitions, basis);
                records.retain(|(_, record)| !builder.retire_with_owner(record));
                if !records.is_empty() {
                    // A group's members each record its one object.
                    let keys = records.iter().map(|(trie, record)| entry.slug.recorded_artefact_key(trie, record).map_err(crate::invalid).and_then(|key| ObjectKey::new(key).map_err(crate::invalid))).collect::<Result<BTreeSet<_>>>()?;
                    self.publisher.remove(ArtefactsRemoved {
                        storage_version: Some(self.storage.0.0), storage_epoch: Some(self.storage.1.0), table_name: Some(table.schema_and_table()),
                        entries: records.into_iter().map(|(trie, record)| ArtefactEntry { trie_key: Some(trie.to_string()), record: Some(record) }).collect(), source_msg_id: None,
                    }).await?;
                    // A reader still holding a snapshot that records one treats its missing object as absent.
                    for key in keys { self.store.delete(&key).await.map_err(crate::invalid)?; }
                }
                let prefix = ObjectKey::new(entry.slug.artefacts_prefix()).map_err(crate::invalid)?;
                let listed = self.store.list(Some(&prefix)).await.map_err(crate::invalid)?;
                for key in stray_artefacts(snapshot.trie_catalog(), &table, &entry.slug, &self.kind, definitions, basis, listed.iter().map(|object| &object.key)) {
                    self.store.delete(&key).await.map_err(crate::invalid)?;
                }
                Ok(())
            }.await;
            if outcome.is_err() { self.again.insert(table.clone()); }
            match current.get(&table) { Some(definitions) => { looked.insert(table, definitions.clone()); } None => { looked.remove(&table); } }
        }
        self.looked = Some(looked);
    }
}

impl ArtefactDriver {
    /// `kind` names the builder's artefacts under `store`, where the lane deletes those whose definitions are retired.
    pub fn new(snapshots: Arc<dyn SnapshotSource>, builder: Arc<dyn ArtefactBuilder>, publisher: Arc<dyn ArtefactPublisher>, storage: (StorageVersion, StorageEpoch),
        kind: &str, store: Arc<dyn ObjectStore>, parallelism: usize, request_capacity: usize, automatic: bool) -> Result<(Self, ArtefactHandle)> {
        if parallelism == 0 || request_capacity == 0 { return Err(crate::invalid("Artefact lane concurrency and request capacity must be positive")); }
        let (signals, signal_receiver) = mpsc::channel(1);
        let (requests, request_receiver) = mpsc::channel(request_capacity);
        let retirement = Retirement { kind: kind.to_string(), store, publisher: Arc::clone(&publisher), storage, looked: None, again: BTreeSet::new() };
        Ok((Self { snapshots, builder, publisher, observer: Arc::new(LoggingCompactionObserver), storage, parallelism, automatic, signals: signal_receiver, requests: request_receiver, retirement },
            ArtefactHandle { signals, requests, capacity: Arc::new(Semaphore::new(request_capacity)) }))
    }

    pub fn with_observer(mut self, observer: Arc<dyn CompactionObserver>) -> Self { self.observer = observer; self }

    fn report_failure(&self, job: &ArtefactJob, error: &Anomaly) {
        if error.code == "rtdb.maintenance/closed" || error.category == AnomalyCategory::Interrupted { return; }
        self.observer.failed(&CompactionFailure::new(&job.definition.table, &job.first_trie(), error));
    }

    /// Artefacts are retired only while no job runs, since a running job may be uploading an object its definition no longer wants.
    async fn available(&mut self, cancellation: &CancellationToken, idle: bool) -> Result<Vec<ArtefactJob>> {
        if idle { self.builder.maintain_independent(self.snapshots.as_ref(), self.publisher.as_ref(), self.storage, cancellation).await?; }
        let snapshot = self.snapshots.snapshot();
        let mut jobs = self.builder.repairs(&snapshot, cancellation).await?;
        let definitions = self.builder.definitions(&snapshot, cancellation).await?;
        if idle { self.retirement.retire(&snapshot, &self.builder.retained_definitions(&definitions), self.builder.as_ref()).await; }
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
                let store = Arc::clone(&self.retirement.store);
                running.push(async move {
                    let result = async {
                        let entries = build_entries(&job, builder.as_ref(), store.as_ref(), snapshots.as_ref(), &token).await?;
                        if entries.is_empty() { return Ok(()); }
                        let message = ArtefactsAdded {
                            storage_version: Some(storage.0.0), storage_epoch: Some(storage.1.0), table_name: Some(job.definition.table.schema_and_table()),
                            entries, source_msg_id: None,
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
                    // A job that failed after uploading leaves an object only another look at its table finds.
                    if let Err(error) = result { self.report_failure(&job, &error); self.retirement.again.insert(job.definition.table.clone()); failed.insert(job, error); }
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
