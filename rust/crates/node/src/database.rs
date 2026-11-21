use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{database::proto::{DatabaseConfig, database_config}, keys::TrieKey, log::proto::{ArtefactsAdded, ArtefactsRemoved, TriesAdded}};
use rtdb_index::PartitionState;
use rtdb_maintenance::{ArtefactDatabase, ArtefactDriver, ArtefactHandle, ArtefactKind, ArtefactPublisher, BlockCollector, CollectionObserver, CompactionDriver, CompactionExecutor, CompactionHandle, CompactionJob, CompactionPublisher, CycleDriver, CycleHandle, SegmentCompactor, SnapshotSource, TrieCollector, TrieDeletionPublisher};
use rtdb_replication::{BlockStore, PartitionLogs, ProgressMonitor, ReplicationDriver, ReplicationHandle, Role, TermHandle};
use rtdb_storage::{IoRegion, ObjectStore, cache::{BufferPool, CacheBudget}};
use rtdb_tx::{ResolveContext, TransactionAuthorization};
use rtdb_types::{Anomaly, AnomalyCategory, ReplicaPosition, Result, SourcePosition, TableRef, Term};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::ordered_index::OrderedIndexMaintenance;
use crate::{Clock, DatabaseCatalog, ExternalSource, ExternalSourceFactory, ExternalSources, NodeOptions, ResourceDriver, ResourceFactory, catalog::{CatalogCommand, CatalogEffects}};

type OrderedIndexCycle = watch::Sender<Option<CycleHandle>>;

pub struct Database {
    name: String,
    configuration: DatabaseConfig,
    read_only: bool,
    pub(crate) partition: Arc<PartitionState>,
    pub(crate) storage: Arc<BufferPool>,
    pub(crate) logs: Arc<PartitionLogs>,
    pub(crate) replication: ReplicationHandle,
    transaction_admission: RwLock<parking_lot::Mutex<Option<SourcePosition>>>,
    compaction: Option<CompactionHandle>,
    artefacts: Vec<ArtefactHandle>,
    ordered_cycle: Option<OrderedIndexCycle>,
    cancellation: CancellationToken,
    io: Arc<IoRegion>,
}

impl Database {
    pub fn name(&self) -> &str { &self.name }
    pub fn configuration(&self) -> &DatabaseConfig { &self.configuration }
    pub fn read_only(&self) -> bool { self.read_only }
    pub fn is_critical(&self) -> bool { self.name == "xtdb" || self.configuration.critical.unwrap_or(false) }
    pub fn startup_replica_target(&self) -> Result<Option<ReplicaPosition>> { self.logs.startup_replica_target() }
    pub fn latest_submitted_position(&self) -> Result<SourcePosition> { self.logs.latest_source_position() }
    pub(crate) async fn await_durable_source(&self, cancellation: &CancellationToken) -> Result<()> {
        if self.external_source() {
            return Err(Anomaly::incorrect("rtdb/source-barrier", "The database ingests transactions outside its source log"));
        }
        let logs = Arc::clone(&self.logs);
        let position = self.io.run_blocking(move || Ok(logs.latest_source_position())).await??;
        self.replication.await_source(position, cancellation).await
    }

    pub async fn block_lag(&self) -> Result<i64> {
        let current = self.status().adopted_block.as_ref().map_or(-1, |block| block.block_index().0);
        let prefix = rtdb_storage::ObjectKey::new("blocks")?;
        let latest = self.storage.object_store().list(Some(&prefix)).await?.iter()
            .filter_map(|entry| rtdb_format::keys::block_index_from_key(entry.key.as_str()).ok()).map(|index| index.0).max().unwrap_or(-1);
        Ok(latest.saturating_sub(current).max(0))
    }
    pub fn external_source(&self) -> bool { self.configuration.external_source.is_some() }
    pub fn progress(&self) -> &Arc<ProgressMonitor> { self.replication.progress() }
    pub fn status(&self) -> rtdb_replication::DriverStatus { self.replication.status() }
    /// Observes the replication driver's current status and subsequent changes.
    pub fn watch_status(&self) -> watch::Receiver<rtdb_replication::DriverStatus> { self.replication.watch_status() }
    pub fn compaction(&self) -> Option<&CompactionHandle> { self.compaction.as_ref() }
    /// Rows applied since the last block, held in the live index until the next flush.
    pub fn live_rows(&self) -> u64 { self.partition.value().live().block_row_count() }
    /// Pauses new user submissions and waits for the admitted ones to apply.
    pub async fn pause_transactions(&self) -> Result<PausedTransactions<'_>> {
        let paused = self.transaction_admission.write().await;
        let latest = *paused.lock();
        if let Some(latest) = latest { self.replication.await_source(latest, &self.cancellation).await?; }
        Ok(PausedTransactions { _admission: paused })
    }
    pub(crate) async fn admit_transaction(&self) -> Result<RwLockReadGuard<'_, parking_lot::Mutex<Option<SourcePosition>>>> {
        let permit = self.transaction_admission.read().await;
        if self.cancellation.is_cancelled() {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, crate::node::NOT_HELD, format!("This node no longer holds {}", self.name)));
        }
        Ok(permit)
    }
    /// Publishes a compaction produced elsewhere, as this database's writer.
    pub async fn publish_compaction(&self, notification: TriesAdded, cancellation: &CancellationToken) -> Result<SourcePosition> {
        self.require_writable()?;
        self.replication.publish_compaction(notification, cancellation).await
    }
    pub fn trie_catalog(&self) -> Arc<rtdb_index::TrieCatalog> { Arc::clone(self.partition.value().trie_catalog()) }
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> { self.storage.object_store() }
    /// The database's latest applied state, read with no basis or system-time cap.
    pub fn snapshot(&self) -> rtdb_index::Snapshot { self.partition.snapshot() }
    pub fn storage(&self) -> &Arc<BufferPool> { &self.storage }
    /// Builds every artefact the lanes can currently build, returning once each is published or with the first failure.
    pub async fn build_artefacts(&self) -> Result<()> {
        for lane in &self.artefacts { lane.build_all().await?; }
        if let Some(cycle) = &self.ordered_cycle {
            let mut installed = cycle.subscribe();
            let wait = async {
                let cycle = installed.wait_for(Option::is_some).await.map_err(|_| closed())?;
                cycle.as_ref().cloned().ok_or_else(closed)
            };
            let order = self.cancellation.run_until_cancelled(wait).await.ok_or_else(closed)??;
            order.await_cycle().await?;
        }
        Ok(())
    }
    pub async fn flush(&self) -> Result<SourcePosition> {
        self.require_writable()?;
        self.replication.flush().await
    }
    pub(crate) fn cancel(&self) { self.cancellation.cancel(); }
    pub(crate) async fn close_io(&self) { self.io.close_and_wait().await; }
    pub(crate) fn require_writable(&self) -> Result<()> {
        if self.cancellation.is_cancelled() { return Err(closed()); }
        if self.read_only {
            let message = match self.configuration.log { Some(rtdb_format::database::proto::database_config::Log::LocalLog(_)) => "Cannot write to read-only database log", _ => "Cannot append to read-only database log" };
            return Err(Anomaly::incorrect("xtdb/read-only-database", message).with_context("db-name", self.name.clone().into()));
        }
        Ok(())
    }
}

pub struct PausedTransactions<'a> { _admission: RwLockWriteGuard<'a, parking_lot::Mutex<Option<SourcePosition>>> }

pub(crate) struct DatabaseFactory {
    pub options: NodeOptions,
    pub resources: Arc<dyn ResourceFactory>,
    pub memory: Arc<MemoryBudget>,
    pub cache: Arc<CacheBudget>,
    pub clock: Arc<dyn Clock>,
    pub authorization: Arc<dyn TransactionAuthorization>,
    pub observer: Arc<dyn CollectionObserver>,
    pub execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
    pub external_sources: ExternalSources,
    pub compaction_relay: Option<Arc<dyn CompactionRelay>>,
    pub artefact_kinds: Vec<Arc<dyn ArtefactKind>>,
    pub accelerators: Arc<[Arc<dyn rtdb_query::ScanAccelerator>]>,
    pub compaction_slots: Option<Arc<Semaphore>>,
}

/// Publishes a compaction a read-only database produced through the database's writer, which alone may append it.
#[async_trait]
pub trait CompactionRelay: Send + Sync {
    async fn publish(&self, database: &str, notification: TriesAdded) -> Result<SourcePosition>;
}

struct RelayCompaction { relay: Arc<dyn CompactionRelay>, database: String }
#[async_trait]
impl CompactionPublisher for RelayCompaction {
    async fn publish(&self, notification: TriesAdded) -> Result<SourcePosition> { self.relay.publish(&self.database, notification).await }
}

pub(crate) struct OpenedDatabase {
    pub database: Arc<Database>,
    pub driver: DatabaseDriver,
    pub secondaries: BTreeMap<String, DatabaseConfig>,
}

impl DatabaseFactory {
    pub fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> {
        self.resources.validate(name, configuration)?;
        if self.options.replication.enabled && self.options.replication.may_lead && !self.options.read_only && configuration.mode.unwrap_or(0) != 1 {
            if let Some(source) = &configuration.external_source { self.external_factory(source)?.validate(source)?; }
        }
        Ok(())
    }
    fn external_factory(&self, source: &prost_types::Any) -> Result<Arc<dyn ExternalSourceFactory>> {
        let name = source.type_url.rsplit('/').next().unwrap_or_default();
        self.external_sources.get(name).cloned().ok_or_else(|| Anomaly::incorrect("xtdb.db-catalog/invalid-db-config", format!("No native external-source factory is registered for {}", source.type_url)))
    }
    pub async fn open(&self, name: String, configuration: DatabaseConfig, catalog: Arc<DatabaseCatalog>, commands: mpsc::Sender<CatalogCommand>, parent: &CancellationToken) -> Result<OpenedDatabase> {
        self.validate(&name, &configuration)?;
        let read_only = self.options.read_only || configuration.mode.unwrap_or(0) == 1;
        let cancellation = parent.child_token();
        let log_cancellation = CancellationToken::new();
        let access = if read_only { crate::LogAccess::ReadOnly } else if self.options.replication.may_lead { crate::LogAccess::ReadWrite } else { crate::LogAccess::SourceWriter };
        let resources = self.resources.open_storage(&name, &configuration).await?;
        let io = Arc::clone(&resources.io);
        let opened = async {
            let storage = Arc::new(BufferPool::with_metadata_budget(Arc::clone(&resources.store), Arc::clone(&self.cache), Arc::clone(&self.memory)));
            let blocks = BlockStore::new(Arc::clone(&resources.store), Arc::clone(&storage), Arc::clone(&self.memory), self.options.file_size_target)?;
            let recovered = blocks.recover(self.options.trie, SourcePosition(-1), self.clock.now(), resources.blocks).await?;
            let opened_logs = self.resources.open_logs(&name, &configuration, access, log_cancellation.clone(), &resources).await?;
            let logs = PartitionLogs::new(opened_logs.source, opened_logs.replica, opened_logs.source_epoch, opened_logs.replica_epoch,
                resources.storage_version, resources.storage_epoch, Arc::clone(&resources.store), Arc::clone(&storage), Arc::clone(&self.memory))?;
            let recovered = if self.options.replication.enabled {
                blocks.recover_for_replay(recovered, &logs, self.options.trie, self.clock.now(), &cancellation).await?
            } else { recovered.with_source_floor(logs.source_floor()?) };
            let secondaries = recovered.block.as_ref().map(|block| block.secondary_databases().clone()).unwrap_or_default();
            let partition = Arc::clone(&recovered.partition);
            let progress = ProgressMonitor::new(recovered.progress.clone());
            let resolve = Arc::new(ResolveContext { database: name.clone(), storage: Arc::clone(&storage), memory: Arc::clone(&self.memory),
                query_options: self.options.query.clone(),
                scan_accelerators: Arc::clone(&self.accelerators),
                cancellation: cancellation.clone(), default_tz: self.options.default_timezone.clone(), authorization: Arc::clone(&self.authorization),
                execution_observer: self.execution_observer.clone(), tracer: self.tracer.clone() });
            let effects = Arc::new(CatalogEffects { catalog, commands, primary: name == "xtdb" });
            let mut replication_options = self.options.replication.clone();
            replication_options.may_lead &= !read_only;
            replication_options.external_source = configuration.external_source.is_some();
            replication_options.assertion_interval = assertion_interval(configuration.log.as_ref(), replication_options.assertion_interval);
            let (replication, replica_driver) = ReplicationDriver::new(replication_options, Arc::clone(&logs), blocks, recovered, progress, resolve, effects, cancellation.clone())?;
            let snapshots: Arc<dyn SnapshotSource> = partition.clone();
            let builders = if read_only || self.options.artefact_threads == 0 { Vec::new() } else {
                self.artefact_kinds.iter().map(|kind| {
                    let clock = Arc::clone(&self.clock);
                    Ok((Arc::clone(kind), kind.builder(ArtefactDatabase { name: name.clone(), store: Arc::clone(&resources.store), storage: Arc::clone(&storage), memory: Arc::clone(&self.memory), clock: Arc::new(move || clock.now()) })?))
                }).collect::<Result<Vec<_>>>()?
            };
            let publisher: Option<Arc<dyn CompactionPublisher>> = match (&self.compaction_relay, read_only) {
                _ if self.options.compaction_threads == 0 => None,
                (_, false) => Some(Arc::new(PublishCompaction { replication: replication.clone(), cancellation: cancellation.clone() })),
                (Some(relay), true) => Some(Arc::new(RelayCompaction { relay: Arc::clone(relay), database: name.clone() })),
                (None, true) => None,
            };
            let (compactor, compaction) = match publisher { None => (None, None), Some(publisher) => {
                let executor = Arc::new(TimedCompaction { executor: SegmentCompactor::new(Arc::clone(&resources.store), Arc::clone(&snapshots), Arc::clone(&self.memory), resources.storage_version, resources.storage_epoch, self.options.compaction.clone())?.with_artefacts(builders.iter().map(|(_, builder)| Arc::clone(builder)).collect()), observer: self.execution_observer.clone() });
                let (driver, handle) = CompactionDriver::new(Arc::clone(&snapshots), executor, publisher, self.options.compaction_threads, self.options.command_capacity, !self.options.automatic_compaction)?;
                let driver = match &self.compaction_slots {
                    Some(slots) => driver.with_job_slots(Arc::clone(slots)),
                    None => driver,
                };
                (Some(driver), Some(handle))
            } };
            let mut artefact_drivers = Vec::new();
            let mut artefacts = Vec::new();
            for (kind, builder) in builders {
                let publisher = Arc::new(PublishArtefacts { replication: replication.clone(), cancellation: cancellation.clone() });
                let (driver, handle) = ArtefactDriver::new(Arc::clone(&snapshots), builder, publisher, (resources.storage_version, resources.storage_epoch), kind.name(), Arc::clone(&resources.store),
                    self.options.artefact_threads, self.options.command_capacity, self.options.automatic_artefacts)?;
                artefact_drivers.push(driver);
                artefacts.push(handle);
            }
            let source = if self.options.replication.enabled && self.options.replication.may_lead && !read_only {
                match &configuration.external_source { Some(source) => Some(self.external_factory(source)?.open(source, &name, Arc::clone(&io)).await?), None => None }
            } else { None };
            let ordered_worker = (!read_only && self.options.replication.enabled && self.options.replication.may_lead).then(|| Arc::new(OrderedIndexMaintenance::new(
                name.clone(), Arc::clone(&storage), Arc::clone(&self.memory), Arc::clone(&self.clock), Arc::clone(&snapshots),
            )));
            let ordered_cycle = ordered_worker.as_ref().map(|_| watch::channel(None).0);
            let supervisor = MaintenanceSupervisor { store: resources.store, partition: Arc::clone(&partition), replication: replication.clone(), compaction: compaction.clone(), artefacts: artefacts.clone(),
                observer: Arc::clone(&self.observer), options: self.options.clone(), source, ordered_worker, ordered_cycle: ordered_cycle.clone() };
            let database = Arc::new(Database { name, configuration, read_only, partition, storage, logs, replication, transaction_admission: RwLock::default(), compaction, artefacts, ordered_cycle, cancellation: cancellation.clone(), io: Arc::clone(&io) });
            let driver = DatabaseDriver { database: Arc::clone(&database), replication: replica_driver, compactor, artefacts: artefact_drivers, supervisor, logs: opened_logs.drivers, log_cancellation };
            Ok(OpenedDatabase { database, driver, secondaries })
        }.await;
        if opened.is_err() { io.close_and_wait().await; }
        opened
    }
}

pub(crate) struct DatabaseDriver {
    database: Arc<Database>,
    replication: ReplicationDriver,
    compactor: Option<CompactionDriver>,
    artefacts: Vec<ArtefactDriver>,
    supervisor: MaintenanceSupervisor,
    logs: Vec<ResourceDriver>,
    log_cancellation: CancellationToken,
}

impl DatabaseDriver {
    pub async fn run(self) -> Result<()> {
        let mut logs = JoinSet::new();
        for driver in self.logs { logs.spawn(driver); }
        let mut tasks = JoinSet::new();
        tasks.spawn(self.replication.run());
        if let Some(compactor) = self.compactor { tasks.spawn(compactor.run(self.database.cancellation.clone())); }
        for lane in self.artefacts { tasks.spawn(lane.run(self.database.cancellation.clone())); }
        tasks.spawn(self.supervisor.run(self.database.cancellation.clone()));
        let failure = tokio::select! {
            biased;
            _ = self.database.cancellation.cancelled() => None,
            result = tasks.join_next() => Some(component_exit(result)),
            result = logs.join_next(), if !logs.is_empty() => Some(component_exit(result)),
        };
        if let Some(error) = failure.as_ref().filter(|error| error.category != AnomalyCategory::Interrupted) {
            self.database.progress().ingestion_failed(error.clone());
        }
        self.database.cancel();
        let mut failure = failure;
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = joined(result) { if failure.is_none() && error.category != AnomalyCategory::Interrupted { failure = Some(error); } }
        }
        self.log_cancellation.cancel();
        while let Some(result) = logs.join_next().await {
            if let Err(error) = joined(result) { if failure.is_none() { failure = Some(error); } }
        }
        if let Some(error) = failure { self.database.progress().ingestion_failed(error.clone()); Err(error) } else { Ok(()) }
    }
}

struct PublishCompaction { replication: ReplicationHandle, cancellation: CancellationToken }
/// Times each completed compaction job into `compactor.job.timer`; the reference stops its job timer only once the job returns.
struct TimedCompaction { executor: SegmentCompactor, observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>> }
#[async_trait]
impl CompactionExecutor for TimedCompaction {
    async fn execute(&self, job: &CompactionJob, cancellation: &CancellationToken) -> Result<TriesAdded> {
        let started = std::time::Instant::now();
        let added = self.executor.execute(job, cancellation).await?;
        if let Some(observer) = &self.observer { observer.record_duration(rtdb_plan::ExecutionTimer::CompactorJob, started.elapsed()); }
        Ok(added)
    }
}

#[async_trait]
impl CompactionPublisher for PublishCompaction {
    async fn publish(&self, notification: TriesAdded) -> Result<SourcePosition> { self.replication.publish_compaction(notification, &self.cancellation).await }
}
struct PublishArtefacts { replication: ReplicationHandle, cancellation: CancellationToken }
#[async_trait]
impl ArtefactPublisher for PublishArtefacts {
    async fn publish(&self, message: ArtefactsAdded) -> Result<SourcePosition> { self.replication.publish_artefacts(message, &self.cancellation).await }
    async fn remove(&self, message: ArtefactsRemoved) -> Result<SourcePosition> { self.replication.remove_artefacts(message, &self.cancellation).await }
}
struct PublishDeletions(TermHandle);
#[async_trait]
impl TrieDeletionPublisher for PublishDeletions {
    async fn publish(&self, table: TableRef, keys: Vec<TrieKey>) -> Result<ReplicaPosition> { self.0.publish_deletions(table, keys).await }
}

struct TermRegion { term: Term, cancellation: CancellationToken, trie: CycleHandle, blocks: CycleHandle, ordered: Option<CycleHandle>, tasks: JoinSet<Result<()>> }
impl TermRegion {
    async fn join(mut self) -> Result<()> {
        self.cancellation.cancel();
        let mut failure = None;
        while let Some(result) = self.tasks.join_next().await { if let Err(error) = joined(result) { if failure.is_none() { failure = Some(error); } } }
        failure.map_or(Ok(()), Err)
    }
}

struct MaintenanceSupervisor {
    store: Arc<dyn ObjectStore>,
    partition: Arc<PartitionState>,
    replication: ReplicationHandle,
    compaction: Option<CompactionHandle>,
    artefacts: Vec<ArtefactHandle>,
    observer: Arc<dyn CollectionObserver>,
    options: NodeOptions,
    source: Option<Box<dyn ExternalSource>>,
    ordered_worker: Option<Arc<OrderedIndexMaintenance>>,
    ordered_cycle: Option<OrderedIndexCycle>,
}

impl MaintenanceSupervisor {
    async fn start_term(&mut self, handle: TermHandle, parent: &CancellationToken) -> Result<TermRegion> {
        let cancellation = parent.child_token();
        let publisher = Arc::new(PublishDeletions(handle.clone()));
        let trie_collector = Arc::new(TrieCollector::new(Arc::clone(&self.store), Arc::clone(&self.partition), publisher, Arc::clone(&self.observer), self.options.garbage.clone())?);
        let block_collector = Arc::new(BlockCollector::new(Arc::clone(&self.store), self.partition.clone(), Arc::clone(&self.observer), self.options.garbage.clone())?);
        let (trie_driver, trie) = CycleDriver::new(trie_collector, self.options.automatic_gc, self.options.command_capacity)?;
        let (block_driver, blocks) = CycleDriver::new(block_collector, self.options.automatic_gc, self.options.command_capacity)?;
        let source = match &mut self.source {
            Some(source) => Some(source.assign().await?), None => None,
        };
        let mut tasks = JoinSet::new();
        tasks.spawn(trie_driver.run(cancellation.clone()));
        tasks.spawn(block_driver.run(cancellation.clone()));
        if let Some(source) = source {
            let term = handle.clone();
            let after = self.replication.progress().current().external_token;
            let status = self.replication.watch_status();
            let token = cancellation.clone();
            tasks.spawn(async move { source.run(term, after, status, token).await });
        }
        let ordered = if let Some(worker) = &self.ordered_worker {
            let worker: Arc<dyn rtdb_maintenance::CycleWorker> = worker.clone();
            let (driver, handle) = CycleDriver::new(worker, self.options.automatic_artefacts, self.options.command_capacity)?;
            tasks.spawn(driver.run(cancellation.clone()));
            handle.signal();
            Some(handle)
        } else { None };
        if let Some(cycle) = &self.ordered_cycle { cycle.send_replace(ordered.clone()); }
        Ok(TermRegion { term: handle.term(), cancellation, trie, blocks, ordered, tasks })
    }
    async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        let mut status = self.replication.watch_status();
        let mut region: Option<TermRegion> = None;
        let mut adopted = None;
        let result = loop {
            if cancellation.is_cancelled() { break Ok(()); }
            let current = status.borrow_and_update().clone();
            let term = match current.role { Role::Leading(term) => Some(term), _ => None };
            if region.as_ref().map(|region| region.term) != term {
                if let Some(cycle) = &self.ordered_cycle { cycle.send_replace(None); }
                if let Some(previous) = region.take() { if let Err(error) = previous.join().await { if !cancellation.is_cancelled() && error.category != AnomalyCategory::Interrupted { break Err(error); } } }
                if let Some(handle) = self.replication.term() {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => break Ok(()),
                        changed = status.changed() => { if changed.is_err() { break Err(closed()); } continue; }
                        started = self.start_term(handle, &cancellation) => match started { Ok(next) => region = Some(next), Err(error) => break Err(error) },
                    }
                }
            }
            let next = current.adopted_block.as_ref().map(|block| block.block_index());
            if next != adopted {
                adopted = next;
                if let Some(compaction) = &self.compaction { compaction.signal_block(); }
                for lane in &self.artefacts { lane.signal_block(); }
                if let Some(region) = &region {
                    region.trie.signal();
                    region.blocks.signal();
                    if let Some(ordered) = &region.ordered { ordered.signal(); }
                }
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Ok(()),
                changed = status.changed() => if changed.is_err() { break Err(closed()); },
                result = async { region.as_mut().expect("guarded term region").tasks.join_next().await }, if region.is_some() => break Err(component_exit(result)),
            }
        };
        if let Some(cycle) = &self.ordered_cycle { cycle.send_replace(None); }
        if let Some(region) = region { let drained = region.join().await; if result.is_ok() && !cancellation.is_cancelled() { return drained; } }
        result
    }
}

fn joined(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> { result.map_err(|error| Anomaly::fault("rtdb.node/driver-panic", error.to_string()))? }
fn component_exit(result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>) -> Anomaly {
    match result { Some(result) => joined(result).err().unwrap_or_else(|| Anomaly::fault("rtdb.node/driver-ended", "A database driver ended before shutdown")), None => Anomaly::fault("rtdb.node/driver-missing", "A database has no running drivers") }
}
fn closed() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/database-closed", "Database is closed") }

/// As the reference's election driver times assertions off the log: a remote log's append-to-visibility time
/// needs ten times an in-process log's interval.
fn assertion_interval(log: Option<&database_config::Log>, in_process: std::time::Duration) -> std::time::Duration {
    match log {
        None | Some(database_config::Log::InMemoryLog(_) | database_config::Log::LocalLog(_)) => in_process,
        Some(_) => in_process.saturating_mul(10),
    }
}

#[cfg(test)]
mod assertion_interval_tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn remote_logs_assert_ten_times_less_often_than_in_process_logs() {
        let base = Duration::from_millis(100);
        assert_eq!(assertion_interval(None, base), base);
        assert_eq!(assertion_interval(Some(&database_config::Log::InMemoryLog(Default::default())), base), base);
        assert_eq!(assertion_interval(Some(&database_config::Log::LocalLog(Default::default())), base), base);
        assert_eq!(assertion_interval(Some(&database_config::Log::OtherLog(Default::default())), base), Duration::from_secs(1));
    }
}
