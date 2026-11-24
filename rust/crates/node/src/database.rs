use std::collections::BTreeMap;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{database::proto::{DatabaseConfig, database_config}, keys::TrieKey, log::proto::{ArtefactsAdded, ArtefactsRemoved, TriesAdded}};
use rtdb_index::PartitionState;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDatabase, ArtefactDriver, ArtefactHandle, ArtefactKind, ArtefactPublisher, BlockCollector, CollectionObserver, CompactionDriver, CompactionExecutor, CompactionHandle, CompactionJob, CompactionPublisher, CycleDriver, CycleHandle, SegmentCompactor, SnapshotSource, TrieCollector, TrieDeletionPublisher};
use rtdb_replication::{BlockStore, PartitionLogs, ProgressMonitor, ReplicationDriver, ReplicationHandle, Role, TermHandle};
use rtdb_storage::{IoRegion, ObjectStore, cache::{BufferPool, CacheBudget}};
use rtdb_tx::{ResolveContext, TransactionAuthorization};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, ReplicaPosition, Result, SourcePosition, TableRef, Term, TransactionKey};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Semaphore, mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::ordered_index::OrderedIndexMaintenance;
use crate::read_currency::ReadCurrency;
use crate::{Clock, DatabaseCatalog, ExternalSource, ExternalSourceFactory, ExternalSources, NodeOptions, REPLICA_BEHIND, ReplicaReadIndex, ResourceDriver, ResourceFactory, catalog::{CatalogCommand, CatalogEffects}};

type OrderedIndexCycle = watch::Sender<Option<CycleHandle>>;

struct ReplicaReadSession(std::sync::Weak<Database>);

impl Drop for ReplicaReadSession {
    fn drop(&mut self) {
        if let Some(database) = self.0.upgrade() {
            database.replica_read_sessions.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct Database {
    pub(crate) protected_equality_epoch: uuid::Uuid,
    name: String,
    configuration: DatabaseConfig,
    read_only: bool,
    pub(crate) partition: Arc<PartitionState>,
    pub(crate) storage: Arc<BufferPool>,
    pub(crate) resolve: Arc<ResolveContext>,
    pub(crate) audience_storage: Option<crate::AudienceReadStorage>,
    pub(crate) logs: Arc<PartitionLogs>,
    pub(crate) replication: ReplicationHandle,
    replica_read_index: Option<Box<dyn ReplicaReadIndex>>,
    pub(crate) await_authority: crate::AwaitAuthority,
    transaction_admission: RwLock<parking_lot::Mutex<Option<SourcePosition>>>,
    submitted_bytes: AtomicU64,
    replica_read_sessions: AtomicUsize,
    query_cpu: Option<Arc<rtdb_query::QueryCpu>>,
    fragment_residence: Option<rtdb_query::FragmentResidence>,
    compaction: Option<CompactionHandle>,
    artefacts: Vec<ArtefactHandle>,
    ordered_cycle: Option<OrderedIndexCycle>,
    cancellation: CancellationToken,
    io: Arc<IoRegion>,
    currency: Arc<ReadCurrency>,
}

impl Database {
    pub fn serves_audience_storage(&self, source: uuid::Uuid) -> bool {
        self.audience_storage.as_ref().is_some_and(|storage| storage.source == source)
    }
    pub fn name(&self) -> &str { &self.name }
    pub fn configuration(&self) -> &DatabaseConfig { &self.configuration }
    /// The immutable receipt authority of this live writer attachment.
    pub fn placed_writer_scope(&self) -> Option<rtdb_types::PlacedAttemptScope> {
        match &self.await_authority {
            crate::AwaitAuthority::Live { codec, .. } => Some(codec.scope()),
            _ => None,
        }
    }
    pub fn read_only(&self) -> bool { self.read_only }
    pub fn fragment_residence(&self) -> Option<&rtdb_query::FragmentResidence> { self.fragment_residence.as_ref() }
    pub fn is_critical(&self) -> bool { self.name == "xtdb" || self.configuration.critical.unwrap_or(false) }
    pub fn startup_replica_target(&self) -> Result<Option<ReplicaPosition>> { self.logs.startup_replica_target() }
    pub fn latest_submitted_position(&self) -> Result<SourcePosition> { self.logs.latest_source_position() }
    pub(crate) async fn await_installed(
        &self, ready: impl Fn(rtdb_replication::InstalledPrefix) -> bool, cancellation: &CancellationToken,
    ) -> Result<ReplicaPosition> {
        let mut status = self.watch_status();
        let waiting = status.wait_for(|status| status.role == Role::Stopped || status.installed.is_some_and(&ready));
        let stopped = || Anomaly::new(AnomalyCategory::Unavailable, crate::NOT_HELD, format!("This node no longer applies {}", self.name));
        let status = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(Anomaly::new(AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "Operation was cancelled")),
            _ = self.cancellation.cancelled() => return Err(stopped()),
            status = waiting => status.map_err(|_| stopped())?,
        };
        if status.role == Role::Stopped { return Err(stopped()); }
        status.installed.map(|prefix| prefix.replica).ok_or_else(stopped)
    }

    /// Waits for a changed event count in the relation's applied history.
    pub async fn wait_for_relation_change(&self, table: &TableRef, after: Option<i64>, span: Duration) -> Result<i64> {
        let mut status = self.watch_status();
        let stopped = || Anomaly::new(AnomalyCategory::Unavailable, crate::NOT_HELD, format!("This node no longer applies {}", self.name));
        let deadline = tokio::time::sleep(span);
        tokio::pin!(deadline);
        loop {
            if status.borrow_and_update().role == Role::Stopped || self.cancellation.is_cancelled() {
                return Err(stopped());
            }
            if let Some(error) = self.progress().fault() { return Err(error); }
            let value = self.partition.value();
            let applied = self.progress().current().tx_id();
            // The live index can advance just before that transaction is published to waiters.
            if value.live().latest_completed_tx().is_none_or(|key| key.tx_id <= applied) {
                let block = value.table_catalog().metadata(table).map_or(0, |meta| meta.row_count);
                let live = value.live().tables().get(table).map_or(0, |table| i64::from(table.row_count()));
                let count = block.checked_add(live).ok_or_else(|| Anomaly::fault("rtdb.node/relation-event-count", "Relation event count overflow"))?;
                if Some(count) != after { return Ok(count); }
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Err(stopped()),
                _ = &mut deadline => return after.ok_or_else(|| Anomaly::new(
                    AnomalyCategory::Unavailable, "rtdb.node/relation-watch-not-ready", "Relation application is still in progress",
                )),
                updated = status.changed() => updated.map_err(|_| stopped())?,
            }
        }
    }

    pub(crate) fn replica_read_deployment(&self) -> Option<uuid::Uuid> {
        self.replica_read_index.as_ref()?.deployment()
    }

    pub(crate) fn has_replica_read_index(&self) -> bool {
        self.replica_read_index.is_some()
    }

    pub fn requires_anchored_catalog(&self) -> bool {
        self.replica_read_index.as_ref().is_some_and(|index| index.requires_anchored_catalog())
    }

    pub async fn confirm_replica_snapshot(&self, cancellation: &CancellationToken) -> Result<()> {
        if let Some(index) = &self.replica_read_index {
            self.confirm_replica_cut(index.as_ref(), cancellation).await?;
        }
        Ok(())
    }

    pub(crate) async fn capture_replica_snapshot(&self, cancellation: &CancellationToken) -> Result<rtdb_index::Snapshot> {
        let Some(index) = &self.replica_read_index else { return Ok(self.partition.snapshot()); };
        // The reader can sample again if apply already passed its selection.
        // Each retry is a fresh confirmation within the original invocation.
        for _ in 0..3 {
            let (barrier, position) = self.confirm_replica_cut(index.as_ref(), cancellation).await?;
            let snapshot = match position {
                Some(through) => self.partition.published_snapshot(CatalogCut {
                    storage_version: self.logs.storage_version,
                    storage_epoch: self.logs.storage_epoch,
                    replica_epoch: self.logs.replica_epoch,
                    through,
                }),
                None => Some(self.partition.snapshot()).filter(|snapshot| {
                    crate::read_selection::empty_catalog_matches_cut(snapshot.catalog_cut(), snapshot.tx_basis())
                }),
            };
            if let Some(snapshot) = snapshot {
                return tokio::select! {
                    biased;
                    _ = cancellation.cancelled() => Err(Anomaly::new(AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "Operation was cancelled")),
                    _ = self.cancellation.cancelled() => Err(self.replica_behind()),
                    retained = rtdb_query::retain_catalog_snapshot(snapshot, &self.storage) => {
                        let retained = retained.map_err(|error| self.replica_behind().with_context("cause", error.code.into()))?;
                        if let Some(read) = &barrier.certified_read {
                            if retained.catalog_cut().is_none_or(|cut| cut.through.after_offset(cut.replica_epoch) != read.through) {
                                return Err(self.replica_behind());
                            }
                            read.observe("certified-catalog-capture");
                            Ok(retained.with_certified_read(read.clone()))
                        } else {
                            Ok(retained)
                        }
                    }
                };
            }
        }
        Err(self.replica_behind())
    }

    fn replica_behind(&self) -> Anomaly {
        Anomaly::new(AnomalyCategory::Unavailable, REPLICA_BEHIND,
            format!("This replica of {} could not establish a fresh snapshot", self.name))
    }

    async fn confirm_replica_cut(&self, read_index: &dyn ReplicaReadIndex, cancellation: &CancellationToken)
        -> Result<(crate::ProtectedReadIndex, Option<ReplicaPosition>)>
    {
        let confirm = async {
            let mut status = self.watch_status();
            let applied = status.borrow().installed.map_or(-1, |prefix| {
                prefix.replica.after_offset(self.logs.replica_epoch)
            });
            let barrier = read_index.protect(rtdb_storage::log::LogOffset(applied)).await
                .map_err(|error| self.replica_behind().with_context("cause", error.code.into()))?;
            let confirmed = if barrier.offset.0 == -1 { None } else {
                Some(ReplicaPosition::from_epoch_offset(self.logs.replica_epoch, barrier.offset.0)?)
            };
            let applied = status.wait_for(|status| status.role == Role::Stopped
                || confirmed.is_none_or(|position| status.installed.is_some_and(|prefix| prefix.replica >= position)));
            let status = tokio::time::timeout(barrier.wait, applied).await
                .map_err(|_| self.replica_behind())?.map_err(|_| self.replica_behind())?;
            if status.role == Role::Stopped { return Err(self.replica_behind()); }
            Ok((barrier, confirmed))
        };
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(Anomaly::new(AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "Operation was cancelled")),
            _ = self.cancellation.cancelled() => Err(self.replica_behind()),
            result = confirm => result,
        }
    }

    pub(crate) async fn await_durable_source(&self, cancellation: &CancellationToken) -> Result<Option<TransactionKey>> {
        if self.external_source() {
            return Err(Anomaly::incorrect("rtdb/source-barrier", "The database ingests transactions outside its source log"));
        }
        let logs = Arc::clone(&self.logs);
        let position = self.io.run_blocking(move || Ok(logs.latest_source_position())).await??;
        self.replication.await_source(position, cancellation).await?;
        Ok(self.progress().current().tx_key)
    }

    /// The lease for a snapshot of this database captured after this returns.
    /// A node whose catalog cannot be confirmed current waits up to the lease margin for it, then returns an expired lease for admission to refuse.
    pub(crate) async fn read_lease(&self, clock: &Arc<dyn Clock>, garbage: &rtdb_maintenance::GarbageConfig) -> Result<Arc<rtdb_storage::ReadLease>> {
        use rtdb_storage::read_lease::{lease_admits, lease_deadline};
        let (lease, margin) = (garbage.reader_lease_micros()?, garbage.reader_lease_margin_micros()?);
        let deadline = |anchor: Option<i64>| anchor.and_then(|anchor| lease_deadline(anchor, lease, margin));
        let observed_at = clock.monotonic_micros();
        let installed = self.replication.status().installed.map(|prefix| prefix.replica);
        if self.currency.needs_observation(installed, observed_at, lease / 4) {
            let logs = Arc::clone(&self.logs);
            if let Ok(Ok(tip)) = self.io.run_blocking(move || Ok(logs.latest_replica_position())).await { self.currency.observe(observed_at, tip); }
        }
        let mut current = deadline(self.currency.anchor(self.replication.status().installed.map(|prefix| prefix.replica)));
        if current.is_none_or(|deadline| !lease_admits(clock.monotonic_micros(), deadline)) {
            if let Some(tip) = self.currency.pending_tip() {
                let mut status = self.replication.watch_status();
                let caught_up = status.wait_for(|status| status.installed.is_some_and(|prefix| prefix.replica >= tip));
                let _ = tokio::time::timeout(garbage.reader_lease_margin, self.cancellation.run_until_cancelled(caught_up)).await;
                current = deadline(self.currency.anchor(self.replication.status().installed.map(|prefix| prefix.replica)));
            }
        }
        let clock = Arc::clone(clock);
        Ok(Arc::new(rtdb_storage::ReadLease::new(current, Arc::new(move || clock.monotonic_micros()))))
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
    pub fn live_batch_bytes(&self) -> u64 { self.partition.value().live().live_batch_bytes() }
    pub fn cache_bytes(&self) -> u64 { self.storage.pool_retained_bytes() as u64 }
    pub fn replica_read_sessions(&self) -> usize { self.replica_read_sessions.load(Ordering::Relaxed) }
    pub(crate) fn track_replica_read_session(self: &Arc<Self>) -> impl Send + use<> {
        self.replica_read_sessions.fetch_add(1, Ordering::Relaxed);
        ReplicaReadSession(Arc::downgrade(self))
    }
    pub fn observe_query_cpu(&self) -> Option<u64> { self.query_cpu.as_ref()?.observe() }
    pub(crate) fn query_cpu_tracker(&self) -> Option<Arc<rtdb_query::QueryCpu>> {
        self.query_cpu.as_ref().filter(|counter| counter.is_observed()).cloned()
    }
    pub fn submitted_bytes(&self) -> u64 { self.submitted_bytes.load(Ordering::Relaxed) }
    pub(crate) fn record_submission(&self, bytes: u64) { self.submitted_bytes.fetch_add(bytes, Ordering::Relaxed); }
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
    pub decision_clock: Option<Arc<dyn rtdb_tx::DecisionClock>>,
    pub authorization: Arc<dyn TransactionAuthorization>,
    pub observer: Arc<dyn CollectionObserver>,
    pub execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
    pub external_sources: ExternalSources,
    pub compaction_relay: Option<Arc<dyn CompactionRelay>>,
    pub world_constraints: Option<Arc<dyn rtdb_tx::WorldConstraintArchive>>,
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
        let mut resources = self.resources.open_storage(&name, &configuration).await?;
        let io = Arc::clone(&resources.io);
        let checkpoint_metadata = resources.checkpoint_metadata.take();
        let recovery_storage = Arc::clone(&resources.store);
        let local_recovery = recovery_storage.local_maintenance_store().filter(|store| store.defers_trie_releases());
        let assembly = async {
            let storage = Arc::new(BufferPool::with_metadata_budget(Arc::clone(&resources.store), Arc::clone(&self.cache), Arc::clone(&self.memory)));
            let blocks = BlockStore::new(Arc::clone(&resources.store), Arc::clone(&storage), Arc::clone(&self.memory), self.options.file_size_target)?;
            let recovered = blocks.recover(self.options.trie, SourcePosition(-1), self.clock.now(), resources.blocks).await?;
            let opened_logs = self.resources.open_logs(&name, &configuration, access, log_cancellation.clone(), &resources).await?;
            if let crate::AwaitAuthority::Checkpoint { block, through, root, .. } = &opened_logs.await_authority
                && (!read_only || through.0 < 0 || through.epoch() != opened_logs.replica_epoch
                    || !matches!(resources.blocks,
                        rtdb_replication::BlockDiscovery::AtPublished(checkpoint)
                        | rtdb_replication::BlockDiscovery::AtErasure(checkpoint)
                        if checkpoint.block == *block && checkpoint.boundary == *through && checkpoint.root_sha256 == Some(*root))
                    || !recovered.block.as_ref().is_some_and(|loaded| loaded.block_index() == *block
                        && loaded.boundary_replica_position() == Some(*through) && loaded.publication_sha256() == Some(*root)))
            {
                return Err(Anomaly::fault("rtdb.node/checkpoint-await-cut", "The opened checkpoint does not establish its await-token cut"));
            }
            let logs = PartitionLogs::new(opened_logs.source, opened_logs.replica, opened_logs.source_epoch, opened_logs.replica_epoch,
                resources.storage_version, resources.storage_epoch, Arc::clone(&resources.store), Arc::clone(&storage), Arc::clone(&self.memory))?
                .with_placed_transfer(opened_logs.placed_transfer)?;
            let recovered = if self.options.replication.enabled {
                blocks.recover_for_replay(recovered, &logs, self.options.trie, self.clock.now(), &cancellation).await?
            } else {
                let through = match &recovered.block {
                    Some(block) => block.boundary_replica_position()
                        .filter(|position| position.after_offset(logs.replica_epoch) >= 0),
                    None => Some(logs.replica_floor()?),
                };
                if let Some(through) = through {
                    recovered.partition.publish_catalog_cut(CatalogCut {
                        storage_version: logs.storage_version, storage_epoch: logs.storage_epoch,
                        replica_epoch: logs.replica_epoch, through,
                    })?;
                }
                recovered.with_source_floor(logs.source_floor()?)
            };
            logs.install_replica_reference_source(recovered.block.clone(), self.options.file_size_target)?;
            let secondaries = recovered.block.as_ref().map(|block| block.secondary_databases().clone()).unwrap_or_default();
            let partition = Arc::clone(&recovered.partition);
            let progress = ProgressMonitor::new(recovered.progress.clone());
            let event_history = !read_only && self.options.artefact_threads != 0
                && self.artefact_kinds.iter().any(|kind| kind.enables_event_history());
            let resolve = Arc::new(ResolveContext {
                decision_clock: self.decision_clock.clone(),
                write_event_origins: event_history || !read_only && matches!(&opened_logs.await_authority, crate::AwaitAuthority::Live { .. }),
                placed_receipt_retirement: matches!(&opened_logs.await_authority, crate::AwaitAuthority::Live { placed_receipt_retirement: true, .. }),
                owned_witness_scope: match &opened_logs.await_authority {
                    crate::AwaitAuthority::Live { codec, .. } => Some(codec.scope()),
                    _ => None,
                },
                world_constraints: self.world_constraints.clone(),
                database: name.clone(), storage: Arc::clone(&storage), memory: Arc::clone(&self.memory),
                query_options: self.options.query.clone(),
                scan_accelerators: Arc::clone(&self.accelerators),
                cancellation: cancellation.clone(), default_tz: self.options.default_timezone.clone(), authorization: Arc::clone(&self.authorization),
                execution_observer: self.execution_observer.clone(), tracer: self.tracer.clone() });
            let query_cpu = catalog.is_placed().then(|| Arc::new(rtdb_query::QueryCpu::default()));
            let effects = Arc::new(CatalogEffects { catalog, commands, primary: name == "xtdb" });
            let mut replication_options = self.options.replication.clone();
            replication_options.may_lead &= !read_only;
            replication_options.external_source = configuration.external_source.is_some();
            replication_options.assertion_interval = assertion_interval(configuration.log.as_ref(), replication_options.assertion_interval);
            let (replication, replica_driver) = ReplicationDriver::new(replication_options, Arc::clone(&logs), blocks, recovered, progress, Arc::clone(&resolve), effects, cancellation.clone())?;
            let snapshots: Arc<dyn SnapshotSource> = partition.clone();
            let mut builders = if read_only || self.options.artefact_threads == 0 { Vec::new() } else {
                self.artefact_kinds.iter().map(|kind| {
                    let clock = Arc::clone(&self.clock);
                    Ok((Arc::clone(kind), kind.builder(ArtefactDatabase { name: name.clone(), store: Arc::clone(&resources.store), storage: Arc::clone(&storage), memory: Arc::clone(&self.memory),
                        retained: Some(Arc::new(RetainedArtefactJournal(Arc::clone(&logs)))), clock: Arc::new(move || clock.now()) })?))
                }).collect::<Result<Vec<_>>>()?
            };
            let history = if event_history {
                let kind: Arc<dyn ArtefactKind> = Arc::new(rtdb_maintenance::EntityHistoryArtefacts);
                let clock = Arc::clone(&self.clock);
                let builder = kind.builder(ArtefactDatabase { name: name.clone(), store: Arc::clone(&resources.store),
                    storage: Arc::clone(&storage), memory: Arc::clone(&self.memory), retained: None, clock: Arc::new(move || clock.now()) })?;
                builders.push((kind, Arc::clone(&builder)));
                Some(builder)
            } else { None };
            let search = builders.iter().find(|(_, builder)| builder.prepares_serving_search()).map(|(_, builder)| Arc::clone(builder));
            let replica_driver = if history.is_some() || search.is_some() {
                replica_driver.with_serving_search(Arc::new(PrepareSearch { search, history }))
            } else { replica_driver };
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
            let mut live_artefacts = Vec::new();
            for (kind, builder) in builders {
                if builder.builds_live_pages() && !builder.prepares_serving_search() { live_artefacts.push(Arc::clone(&builder)); }
                let publisher = Arc::new(PublishArtefacts { replication: replication.clone(), cancellation: cancellation.clone() });
                let (driver, handle) = ArtefactDriver::new(Arc::clone(&snapshots), builder, publisher, (resources.storage_version, resources.storage_epoch), kind.name(), Arc::clone(&resources.store),
                    self.options.artefact_threads.max(1), self.options.command_capacity, self.options.automatic_artefacts)?;
                artefact_drivers.push(driver);
                artefacts.push(handle);
            }
            let source = if self.options.replication.enabled && self.options.replication.may_lead && !read_only {
                match &configuration.external_source { Some(source) => Some(self.external_factory(source)?.open(source, &name, Arc::clone(&io)).await?), None => None }
            } else { None };
            let ordered_worker = (!read_only && self.options.replication.enabled && self.options.replication.may_lead).then(|| Arc::new(OrderedIndexMaintenance::new(
                name.clone(), Arc::clone(&storage), Arc::clone(&self.memory), Arc::clone(&self.clock), Arc::clone(&snapshots), Arc::clone(&logs),
                self.options.ordered_index_arrow_references,
            )));
            let ordered_cycle = ordered_worker.as_ref().map(|_| watch::channel(None).0);
            let currency = Arc::<ReadCurrency>::default();
            let supervisor = MaintenanceSupervisor { database: name.clone(), storage: Arc::clone(&storage), memory: Arc::clone(&self.memory), currency: Arc::clone(&currency), logs: Arc::clone(&logs), store: resources.store, partition: Arc::clone(&partition), replication: replication.clone(), compaction: compaction.clone(), artefacts: artefacts.clone(),
                observer: Arc::clone(&self.observer), options: self.options.clone(), source, ordered_worker, ordered_cycle: ordered_cycle.clone(), clock: Arc::clone(&self.clock) };
            let fragment_residence = resources.fragment_storage.map(|storage| rtdb_query::FragmentResidence {
                storage, replica_epoch: logs.replica_epoch.0,
            });
            let database = Arc::new(Database { protected_equality_epoch: uuid::Uuid::new_v4(), name, configuration, read_only, partition, storage, resolve,
                audience_storage: resources.audience_storage, logs, replication, replica_read_index: opened_logs.replica_read_index,
                await_authority: opened_logs.await_authority,
                transaction_admission: RwLock::default(), submitted_bytes: AtomicU64::new(0), replica_read_sessions: AtomicUsize::new(0),
                query_cpu, fragment_residence, compaction, artefacts, ordered_cycle, cancellation: cancellation.clone(), io: Arc::clone(&io), currency });
            let driver = DatabaseDriver { database: Arc::clone(&database), replication: replica_driver, compactor,
                artefacts: artefact_drivers, live_artefacts, supervisor, logs: opened_logs.drivers, log_cancellation };
            Ok(OpenedDatabase { database, driver, secondaries })
        };
        let opened = async {
            let hold = match (checkpoint_metadata, local_recovery) {
                (Some(hold), _) => Some(hold),
                (None, Some(store)) => Some(store.hold_recovery_open().await?),
                (None, None) => None,
            };
            match hold {
                Some(hold) => IoRegion::reading(rtdb_storage::HeldView::recovery(hold), assembly).await,
                None => assembly.await,
            }
        }.await;
        if opened.is_err() { io.close_and_wait().await; }
        opened
    }
}

struct RetainedArtefactJournal(Arc<PartitionLogs>);

#[async_trait::async_trait]
impl rtdb_maintenance::RetainedArtefactSource for RetainedArtefactJournal {
    async fn replay_table(&self, target: &rtdb_format::catalog::BlockCatalog, table: &rtdb_types::TableRef,
        visit: &mut rtdb_maintenance::ReplayVisitor<'_>, cancellation: &CancellationToken) -> Result<bool> {
        let Some(mut replay) = rtdb_replication::RetainedTableReplay::open(&self.0, target, table).await? else { return Ok(false); };
        while replay.next_block(cancellation, &mut *visit).await?.is_some() {}
        Ok(true)
    }
}

struct PrepareSearch { search: Option<Arc<dyn ArtefactBuilder>>, history: Option<Arc<dyn ArtefactBuilder>> }

#[async_trait::async_trait]
impl rtdb_replication::ServingSearchBuilder for PrepareSearch {
    async fn prepare(&self, snapshot: rtdb_index::Snapshot, cancellation: &CancellationToken) -> Result<Option<rtdb_index::ServingSearch>> {
        let Some(search) = &self.search else { return Ok(None); };
        search.prepare_serving_search(snapshot, cancellation).await
    }

    fn journals_deltas(&self) -> bool { self.search.as_ref().is_some_and(|search| search.journals_search_deltas()) }

    async fn prepare_delta(&self, snapshot: rtdb_index::Snapshot, write: Arc<rtdb_index::StagedWrite>, cancellation: &CancellationToken)
        -> Result<Option<bytes::Bytes>> {
        let Some(search) = &self.search else { return Ok(None); };
        search.prepare_search_delta(snapshot, write, cancellation).await
    }

    async fn install_delta(&self, snapshot: rtdb_index::Snapshot, write: &rtdb_index::StagedWrite, delta: Option<&[u8]>, cancellation: &CancellationToken)
        -> Result<Option<rtdb_index::ServingSearch>> {
        let Some(search) = &self.search else { return Ok(None); };
        search.install_search_delta(snapshot, write, delta, cancellation).await
    }

    async fn seal(&self, value: &Arc<rtdb_index::PartitionValue>, table: &rtdb_types::TableRef, trie: &TrieKey,
        file: &std::path::Path, size: u64, cancellation: &CancellationToken) -> Result<Vec<rtdb_format::log::proto::ArtefactRecord>> {
        let mut records = Vec::new();
        for builder in [&self.history, &self.search].into_iter().flatten() {
            records.extend(builder.seal_serving_search(value, table, trie, file, size, cancellation).await?);
        }
        Ok(records)
    }
}

pub(crate) struct DatabaseDriver {
    database: Arc<Database>,
    replication: ReplicationDriver,
    compactor: Option<CompactionDriver>,
    artefacts: Vec<ArtefactDriver>,
    live_artefacts: Vec<Arc<dyn rtdb_maintenance::ArtefactBuilder>>,
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
        if !self.live_artefacts.is_empty() {
            let database = Arc::clone(&self.database);
            let builders = self.live_artefacts;
            tasks.spawn(async move {
                let mut applied = database.replication.watch_status();
                loop {
                    if database.cancellation.is_cancelled() { return Ok(()); }
                    let partition = database.partition.value();
                    for builder in &builders {
                        if let Err(error) = builder.build_live_pages(&partition, &database.cancellation).await
                            && !database.cancellation.is_cancelled() {
                            eprintln!("{} live search coverage: {error}", database.name);
                        }
                    }
                    tokio::select! {
                        _ = database.cancellation.cancelled() => return Ok(()),
                        result = applied.changed() => if result.is_err() { return Ok(()); },
                    }
                }
            });
        }
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
pub(crate) struct PublishDeletions(pub(crate) TermHandle);
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
    database: String,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
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
    clock: Arc<dyn Clock>,
    currency: Arc<ReadCurrency>,
    logs: Arc<PartitionLogs>,
}

impl MaintenanceSupervisor {
    async fn start_term(&mut self, handle: TermHandle, parent: &CancellationToken) -> Result<TermRegion> {
        let cancellation = parent.child_token();
        let publisher = Arc::new(PublishDeletions(handle.clone()));
        let artefacts = Arc::new(PublishArtefacts { replication: self.replication.clone(), cancellation: cancellation.clone() });
        let trie_collector = Arc::new(TrieCollector::new(Arc::clone(&self.store), Arc::clone(&self.partition), publisher, Arc::clone(&self.observer), self.options.garbage.clone(), {
            let clock = Arc::clone(&self.clock);
            Arc::new(move || clock.monotonic_micros())
        })?.with_search(artefacts, self.logs.storage_version, self.logs.storage_epoch).with_retirements(Arc::new(crate::table_retirement::RetiredRelations {
            database: self.database.clone(), partition: Arc::clone(&self.partition), storage: Arc::clone(&self.storage),
            memory: Arc::clone(&self.memory), term: handle.clone(),
        })));
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
        let mut trie_retry = tokio::time::interval(self.options.garbage.reader_lease.clamp(
            std::time::Duration::from_millis(100), std::time::Duration::from_secs(60),
        ));
        trie_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let result = loop {
            if cancellation.is_cancelled() { break Ok(()); }
            let current = status.borrow_and_update().clone();
            // Keeps the read lease anchor fresh while the log processor applies records, so queries rarely observe the tip themselves.
            if self.options.replication.enabled && !self.store.defers_trie_releases() {
                if let Ok(lease) = self.options.garbage.reader_lease_micros() {
                    let observed_at = self.clock.monotonic_micros();
                    let installed = current.installed.map(|prefix| prefix.replica);
                    if self.currency.needs_observation(installed, observed_at, lease / 4) {
                        let logs = Arc::clone(&self.logs);
                        if let Ok(Ok(tip)) = tokio::task::spawn_blocking(move || logs.latest_replica_position()).await { self.currency.observe(observed_at, tip); }
                        self.currency.anchor(installed);
                    }
                }
            }
            let term = match current.role { Role::Leading(term) => Some(term), _ => None };
            if region.as_ref().map(|region| region.term) != term {
                if let Some(cycle) = &self.ordered_cycle { cycle.send_replace(None); }
                if let Some(previous) = region.take() { if let Err(error) = previous.join().await { if !cancellation.is_cancelled() && error.category != AnomalyCategory::Interrupted { break Err(error); } } }
                if let Some(handle) = self.replication.term() {
                    tokio::select! {
                        biased;
                        _ = cancellation.cancelled() => break Ok(()),
                        changed = status.changed() => { if changed.is_err() { break Err(closed()); } continue; }
                        started = self.start_term(handle, &cancellation) => match started {
                            Ok(next) => { region = Some(next); trie_retry.reset_immediately(); }
                            Err(error) => break Err(error),
                        },
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
                _ = trie_retry.tick(), if self.options.automatic_gc && region.is_some() => {
                    if let Some(region) = &region { region.trie.signal(); }
                }
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
