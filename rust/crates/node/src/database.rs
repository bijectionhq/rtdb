use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{database::proto::DatabaseConfig, keys::TrieKey, log::proto::TriesAdded};
use rtdb_index::PartitionState;
use rtdb_maintenance::{BlockCollector, CollectionObserver, CompactionDriver, CompactionHandle, CompactionPublisher, CycleDriver, CycleHandle, SegmentCompactor, SnapshotSource, TrieCollector, TrieDeletionPublisher};
use rtdb_replication::{BlockStore, PartitionLogs, ProgressMonitor, ReplicationDriver, ReplicationHandle, Role, TermHandle};
use rtdb_storage::{ObjectStore, cache::{BufferPool, CacheBudget}};
use rtdb_tx::{ResolveContext, TransactionAuthorization};
use rtdb_types::{Anomaly, AnomalyCategory, ReplicaPosition, Result, SourcePosition, TableRef, Term};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{Clock, DatabaseCatalog, ExternalSource, ExternalSourceFactory, ExternalSources, NodeOptions, ResourceDriver, ResourceFactory, catalog::{CatalogCommand, CatalogEffects}};

pub struct Database {
    name: String,
    configuration: DatabaseConfig,
    read_only: bool,
    pub(crate) partition: Arc<PartitionState>,
    pub(crate) storage: Arc<BufferPool>,
    pub(crate) logs: Arc<PartitionLogs>,
    pub(crate) replication: ReplicationHandle,
    compaction: Option<CompactionHandle>,
    cancellation: CancellationToken,
}

impl Database {
    pub fn name(&self) -> &str { &self.name }
    pub fn configuration(&self) -> &DatabaseConfig { &self.configuration }
    pub fn read_only(&self) -> bool { self.read_only }
    pub fn is_critical(&self) -> bool { self.name == "xtdb" || self.configuration.critical.unwrap_or(false) }
    pub fn startup_replica_target(&self) -> Result<Option<ReplicaPosition>> { self.logs.startup_replica_target() }
    pub fn latest_submitted_position(&self) -> Result<SourcePosition> { self.logs.latest_source_position() }
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
    pub fn compaction(&self) -> Option<&CompactionHandle> { self.compaction.as_ref() }
    pub async fn flush(&self) -> Result<SourcePosition> {
        self.require_writable()?;
        self.replication.flush().await
    }
    pub(crate) fn cancel(&self) { self.cancellation.cancel(); }
    pub(crate) fn require_writable(&self) -> Result<()> {
        if self.cancellation.is_cancelled() { return Err(closed()); }
        if self.read_only { return Err(Anomaly::incorrect("xtdb/read-only-database", "Database is read-only").with_context("db-name", self.name.clone().into())); }
        Ok(())
    }
}

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
        let resources = self.resources.open(&name, &configuration, access, log_cancellation.clone()).await?;
        let storage = Arc::new(BufferPool::with_budget(Arc::clone(&resources.store), Arc::clone(&self.cache)));
        let logs = PartitionLogs::new(resources.logs.source, resources.logs.replica, resources.logs.source_epoch, resources.logs.replica_epoch,
            resources.storage_version, resources.storage_epoch, Arc::clone(&resources.store), Arc::clone(&storage), Arc::clone(&self.memory))?;
        let blocks = BlockStore::new(Arc::clone(&resources.store), Arc::clone(&storage), Arc::clone(&self.memory), self.options.file_size_target)?;
        let recovered = blocks.recover(self.options.trie, logs.source_floor()?, self.clock.now()).await?;
        let secondaries = recovered.block.as_ref().map(|block| block.secondary_databases().clone()).unwrap_or_default();
        let partition = Arc::clone(&recovered.partition);
        let progress = ProgressMonitor::new(recovered.progress.clone());
        let resolve = Arc::new(ResolveContext { database: name.clone(), storage: Arc::clone(&storage), memory: Arc::clone(&self.memory),
            query_options: self.options.query.clone(), cancellation: cancellation.clone(), default_tz: self.options.default_timezone.clone(), authorization: Arc::clone(&self.authorization), execution_observer: self.execution_observer.clone(), tracer: self.tracer.clone() });
        let effects = Arc::new(CatalogEffects { catalog, commands, primary: name == "xtdb" });
        let mut replication_options = self.options.replication.clone();
        replication_options.may_lead &= !read_only;
        replication_options.external_source = configuration.external_source.is_some();
        let (replication, replica_driver) = ReplicationDriver::new(replication_options, Arc::clone(&logs), blocks, recovered, progress, resolve, effects, cancellation.clone())?;
        let snapshots: Arc<dyn SnapshotSource> = partition.clone();
        let (compactor, compaction) = if read_only || self.options.compaction_threads == 0 { (None, None) } else {
            let executor = Arc::new(SegmentCompactor::new(Arc::clone(&resources.store), Arc::clone(&snapshots), Arc::clone(&self.memory), resources.storage_version, resources.storage_epoch, self.options.compaction.clone())?);
            let publisher = Arc::new(PublishCompaction { replication: replication.clone(), cancellation: cancellation.clone() });
            let (driver, handle) = CompactionDriver::new(Arc::clone(&snapshots), executor, publisher, self.options.compaction_threads, self.options.command_capacity, !self.options.automatic_compaction)?;
            (Some(driver), Some(handle))
        };
        let source = if self.options.replication.enabled && self.options.replication.may_lead && !read_only {
            match &configuration.external_source { Some(source) => Some(self.external_factory(source)?.open(source, &name).await?), None => None }
        } else { None };
        let supervisor = MaintenanceSupervisor { store: resources.store, snapshots, replication: replication.clone(), compaction: compaction.clone(),
            observer: Arc::clone(&self.observer), options: self.options.clone(), source };
        let database = Arc::new(Database { name, configuration, read_only, partition, storage, logs, replication, compaction, cancellation: cancellation.clone() });
        let driver = DatabaseDriver { database: Arc::clone(&database), replication: replica_driver, compactor, supervisor, logs: resources.logs.drivers, log_cancellation };
        Ok(OpenedDatabase { database, driver, secondaries })
    }
}

pub(crate) struct DatabaseDriver {
    database: Arc<Database>,
    replication: ReplicationDriver,
    compactor: Option<CompactionDriver>,
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
        tasks.spawn(self.supervisor.run(self.database.cancellation.clone()));
        let failure = tokio::select! {
            biased;
            _ = self.database.cancellation.cancelled() => None,
            result = tasks.join_next() => Some(component_exit(result)),
            result = logs.join_next(), if !logs.is_empty() => Some(component_exit(result)),
        };
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
#[async_trait]
impl CompactionPublisher for PublishCompaction {
    async fn publish(&self, notification: TriesAdded) -> Result<SourcePosition> { self.replication.publish_compaction(notification, &self.cancellation).await }
}
struct PublishDeletions(TermHandle);
#[async_trait]
impl TrieDeletionPublisher for PublishDeletions {
    async fn publish(&self, table: TableRef, keys: Vec<TrieKey>) -> Result<ReplicaPosition> { self.0.publish_deletions(table, keys).await }
}

struct TermRegion { term: Term, cancellation: CancellationToken, trie: CycleHandle, blocks: CycleHandle, tasks: JoinSet<Result<()>> }
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
    snapshots: Arc<dyn SnapshotSource>,
    replication: ReplicationHandle,
    compaction: Option<CompactionHandle>,
    observer: Arc<dyn CollectionObserver>,
    options: NodeOptions,
    source: Option<Box<dyn ExternalSource>>,
}

impl MaintenanceSupervisor {
    async fn start_term(&mut self, handle: TermHandle, parent: &CancellationToken) -> Result<TermRegion> {
        let cancellation = parent.child_token();
        let publisher = Arc::new(PublishDeletions(handle.clone()));
        let trie_collector = Arc::new(TrieCollector::new(Arc::clone(&self.store), Arc::clone(&self.snapshots), publisher, Arc::clone(&self.observer), self.options.garbage.clone())?);
        let block_collector = Arc::new(BlockCollector::new(Arc::clone(&self.store), Arc::clone(&self.snapshots), Arc::clone(&self.observer), self.options.garbage.clone())?);
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
        Ok(TermRegion { term: handle.term(), cancellation, trie, blocks, tasks })
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
                if let Some(region) = &region { region.trie.signal(); region.blocks.signal(); }
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Ok(()),
                changed = status.changed() => if changed.is_err() { break Err(closed()); },
                result = async { region.as_mut().expect("guarded term region").tasks.join_next().await }, if region.is_some() => break Err(component_exit(result)),
            }
        };
        if let Some(region) = region { let drained = region.join().await; if result.is_ok() && !cancellation.is_cancelled() { return drained; } }
        result
    }
}

fn joined(result: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> { result.map_err(|error| Anomaly::fault("rtdb.node/driver-panic", error.to_string()))? }
fn component_exit(result: Option<std::result::Result<Result<()>, tokio::task::JoinError>>) -> Anomaly {
    match result { Some(result) => joined(result).err().unwrap_or_else(|| Anomaly::fault("rtdb.node/driver-ended", "A database driver ended before shutdown")), None => Anomaly::fault("rtdb.node/driver-missing", "A database has no running drivers") }
}
fn closed() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/database-closed", "Database is closed") }
