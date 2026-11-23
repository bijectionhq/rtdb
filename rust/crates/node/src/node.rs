mod historical_owned_read;
mod hosted_control;
mod hosted_observation;
mod owned_changes;
mod owned_usage;
pub use owned_usage::OwnedStorageUsage;
mod owned_identities;
mod owned_unique;
pub use owned_identities::OwnedIdentityPage;
pub use owned_changes::{ExpiryCandidate, ExpiryCandidatePage, OwnedChangeCommit, OwnedMergedChanges, OwnedRelationChanges, OwnedWorldVersionRead, OWNED_MERGED_CHANGE_RELATIONS};

use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_format::basis::decode_tx_basis;
use rtdb_format::{database::proto::DatabaseConfig, envelope::{SourceBody, SourceMessage}, log::proto::{AttachDatabase, DetachDatabase, resolved_tx::DbOp}};
use rtdb_maintenance::CollectionObserver;
use rtdb_plan::{CatalogMetrics, CatalogUsers};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryCursor};
use rtdb_storage::cache::CacheBudget;
use rtdb_tx::{IDEMPOTENCY_GUARD_RESULT, RetainedAnomaly, SubmitOptions, SubmittedTx, TransactionAuthorization, TransactionResult, TxOp, prepare_submission_with_sql_options};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, PlacedAttempt, ReplicaPosition, Result, ScalarType, SourcePosition, TimeBasis, TimeUnit, Timestamp, TransactionKey, TxBasis, TxId, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{AwaitAuthority, Clock, Database, DatabaseAccess, DatabaseCatalog, ExternalSources, Membership, NativeResources, NodeOptions, QueryBasis, ResourceFactory, Session, SessionIdentity, SessionService, SystemClock,
    catalog::CatalogCommand, database::{DatabaseDriver, DatabaseFactory, OpenedDatabase}, placed_receipts::{PlacedToken, invalid_token, is_placed_token}, statement::prepare_query};


#[derive(Clone)]
pub struct NodeServices {
    pub resources: Arc<dyn ResourceFactory>,
    pub transaction_authorization: Arc<dyn TransactionAuthorization>,
    pub collection_observer: Arc<dyn CollectionObserver>,
    pub external_sources: ExternalSources,
    pub clock: Arc<dyn Clock>,
    pub catalog_users: Arc<dyn CatalogUsers>,
    pub catalog_metrics: Arc<dyn CatalogMetrics>,
    pub execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
    /// Where a read-only database's compactor sends what it produced, to be published by the database's writer elsewhere.
    /// Absent, a read-only database runs no compactor.
    pub compaction_relay: Option<Arc<dyn crate::database::CompactionRelay>>,
    /// Asked before every query whether this node may still serve its database: a linearizable read waits on it, and a deposed holder is refused.
    pub read_fence: Option<Arc<dyn ReadFence>>,
    /// Splits a placed database's large scans into fragments other nodes read; absent, every scan reads its tries here.
    pub scan_fragmenter: Option<Arc<dyn rtdb_query::ScanFragmenter>>,
    /// The kinds of derived artefact this node builds; empty disables the artefact lane.
    pub artefact_kinds: Vec<Arc<dyn rtdb_maintenance::ArtefactKind>>,
    /// Consulted by every scan before it opens, unless `NodeOptions::scan_acceleration` is off.
    pub scan_accelerators: Vec<Arc<dyn rtdb_query::ScanAccelerator>>,
}

#[async_trait::async_trait]
pub trait ReadFence: Send + Sync {
    async fn confirm(&self, database: &str) -> Result<()>;
    fn hold_active_call(&self, _database: &Database) -> Option<Box<dyn Send + Sync>> { None }
    async fn confirm_hosted(&self, _database: &Database) -> Result<String> {
        Err(hosted_source_unavailable())
    }
}

pub(crate) struct HostedSourceReceipt {
    pub(crate) history: String,
    pub(crate) applied: Option<TransactionKey>,
}
fn default_search() -> SearchServices {
    #[cfg(feature = "search")] {
        let runtime = rtdb_search::SearchRuntime::new(256 << 20);
        SearchServices { artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()] }
    }
    #[cfg(not(feature = "search"))] { SearchServices { artefact_kinds: Vec::new(), scan_accelerators: Vec::new() } }
}
struct SearchServices { artefact_kinds: Vec<Arc<dyn rtdb_maintenance::ArtefactKind>>, scan_accelerators: Vec<Arc<dyn rtdb_query::ScanAccelerator>> }
impl NodeServices {
    pub fn local(authorization: Arc<dyn TransactionAuthorization>, observer: Arc<dyn CollectionObserver>, users: Arc<dyn CatalogUsers>, metrics: Arc<dyn CatalogMetrics>) -> Self {
        let search = default_search();
        Self { resources: Arc::new(NativeResources::default()), transaction_authorization: authorization, collection_observer: observer, external_sources: BTreeMap::new(), clock: Arc::new(SystemClock), catalog_users: users, catalog_metrics: metrics, execution_observer: None, tracer: None,
            compaction_relay: None, read_fence: None, scan_fragmenter: None,
            artefact_kinds: search.artefact_kinds, scan_accelerators: search.scan_accelerators }
    }
}

#[derive(Clone, Copy)]
enum CatalogReadCut {
    Current,
    Reserved(TransactionKey),
    HostedApplied(TransactionKey),
}

#[derive(Clone)]
pub struct Node {
    catalog: Arc<DatabaseCatalog>,
    factory: Arc<DatabaseFactory>,
    commands: mpsc::Sender<CatalogCommand>,
    cancellation: CancellationToken,
    pub(crate) runtime: Arc<crate::runtime::RuntimeProviders>,
    read_fence: Option<Arc<dyn ReadFence>>,
    scan_fragmenter: Option<Arc<dyn rtdb_query::ScanFragmenter>>,
    placement_slots: Option<Arc<Semaphore>>,
}

pub struct NodeDriver {
    node: Node,
    commands: mpsc::Receiver<CatalogCommand>,
    initial: Vec<(Arc<Database>, DatabaseDriver)>,
    local_opens: BTreeMap<String, LocalOpen>,
}

type AttachResult = oneshot::Sender<Result<Option<Arc<Database>>>>;

struct LocalOpen {
    _slot: OwnedSemaphorePermit,
    completion: AttachResult,
    queued: VecDeque<LocalRequest>,
}

enum LocalRequest {
    Attach { configuration: DatabaseConfig, slot: OwnedSemaphorePermit, completion: AttachResult },
    Detach { completion: oneshot::Sender<Result<()>> },
}

enum DatabaseCompletion {
    Worker { database: Arc<Database>, result: Result<()> },
    Drained(Arc<Database>),
    LocalOpened { name: String, result: Result<OpenedDatabase> },
}

const MAXIMUM_PLACED_OPENS: usize = 8;

impl Node {
    /// The provider observes current catalog generations and keeps no node or database alive between samples.
    pub fn metrics_provider(&self) -> Result<Arc<dyn CatalogMetrics>> {
        Ok(Arc::new(crate::metrics::NodeMetrics::new(Arc::downgrade(&self.catalog), Arc::downgrade(&self.factory), Arc::clone(self.memory()))?))
    }
    pub fn execution_observer(&self) -> Option<Arc<dyn rtdb_plan::ExecutionObserver>> { self.factory.execution_observer.clone() }
    pub fn tracer(&self) -> Option<Arc<dyn rtdb_plan::Tracer>> { self.factory.tracer.clone() }
    pub(crate) fn require_offline_export(&self) -> Result<()> {
        self.ensure_open()?;
        if self.factory.options.replication.enabled || self.factory.options.compaction_threads > 0 {
            return Err(Anomaly::incorrect("rtdb.export/active-node", "Snapshot export requires indexing and compaction to be disabled"));
        }
        Ok(())
    }
    pub(crate) fn require_compactor_reset(&self) -> Result<()> {
        self.ensure_open()?;
        if !self.factory.options.replication.enabled || self.factory.options.compaction_threads > 0 || self.factory.options.automatic_gc {
            return Err(Anomaly::incorrect("rtdb.compactor-reset/active-maintenance", "Compactor reset requires indexing enabled, zero compactor threads and automatic GC disabled"));
        }
        Ok(())
    }
    /// Opens persisted catalogs and returns their unstarted owner.
    /// Run the driver once, then request shutdown and await it before releasing the node's resources.
    pub async fn open(options: NodeOptions, services: NodeServices) -> Result<(Self, NodeDriver)> {
        let memory = MemoryBudget::new(options.execution_memory_bytes);
        Self::open_with_memory(options, services, memory).await
    }
    /// Authentication, source factories and the node may share this execution allocation ceiling.
    /// The budget limit must equal `options.execution_memory_bytes`.
    pub async fn open_with_memory(options: NodeOptions, services: NodeServices, memory: Arc<MemoryBudget>) -> Result<(Self, NodeDriver)> {
        let resources = Arc::clone(&services.resources);
        match Self::open_region(options, services, memory).await {
            Ok(opened) => Ok(opened),
            Err(error) => match resources.close().await { Ok(()) => Err(error), Err(close) => Err(error.with_context("resource-close-error", close.to_string().into())) },
        }
    }
    async fn open_region(mut options: NodeOptions, mut services: NodeServices, memory: Arc<MemoryBudget>) -> Result<(Self, NodeDriver)> {
        if memory.limit_bytes() != options.execution_memory_bytes { return Err(Anomaly::incorrect("rtdb.node/invalid-memory-budget", "Node options and supplied execution budget have different limits")); }
        if options.command_capacity == 0 || options.execution_memory_bytes == 0 || options.cache_bytes == 0 {
            return Err(Anomaly::incorrect("rtdb.node/invalid-options", "Node capacities must be positive"));
        }
        options.default_timezone = rtdb_expr::normalize_timezone(&options.default_timezone)?;
        let catalog = DatabaseCatalog::new(options.membership.clone());
        let runtime = Arc::new(crate::runtime::RuntimeProviders { users: services.catalog_users, metrics: services.catalog_metrics });
        let read_fence = services.read_fence.clone();
        let scan_fragmenter = services.scan_fragmenter.clone();
        let compaction_slots = (matches!(options.membership, Membership::Placed) && options.compaction_threads > 0)
            .then(|| Arc::new(Semaphore::new(options.compaction_threads.min(Semaphore::MAX_PERMITS))));
        services.scan_accelerators.insert(0, Arc::new(crate::ordered_index::OrderedIndexAccelerator));
        let factory = Arc::new(DatabaseFactory { memory, cache: CacheBudget::new(options.cache_bytes),
            resources: services.resources, authorization: services.transaction_authorization, observer: services.collection_observer, execution_observer: services.execution_observer, tracer: services.tracer,
            external_sources: services.external_sources, compaction_relay: services.compaction_relay, compaction_slots, clock: services.clock, artefact_kinds: services.artefact_kinds,
            accelerators: Arc::from(if options.scan_acceleration { services.scan_accelerators } else { Vec::new() }), options });
        let (commands, receiver) = mpsc::channel(factory.options.command_capacity);
        let placement_slots = catalog.is_placed().then(|| Arc::new(Semaphore::new(MAXIMUM_PLACED_OPENS)));
        let node = Self { catalog, factory, commands, cancellation: CancellationToken::new(), runtime, read_fence, scan_fragmenter, placement_slots };
        let mut initial = Vec::new();
        match node.factory.options.membership.clone() {
            Membership::Durable { primary, skipped } => {
                if skipped.contains("xtdb") { return Err(Anomaly::incorrect("xtdb/cannot-skip-primary", "The primary database must run on this node")); }
                let opened = node.factory.open("xtdb".into(), primary, Arc::clone(&node.catalog), node.commands.clone(), &node.cancellation).await?;
                let secondaries = opened.secondaries;
                node.catalog.insert_open(Arc::clone(&opened.database))?;
                initial.push((opened.database, opened.driver));
                for (name, configuration) in secondaries {
                    if name == "xtdb" { continue; }
                    crate::resources::validate_database_shape(&name, &configuration)?;
                    if node.catalog.skipped(&name) { node.catalog.insert_skipped(name, configuration)?; }
                    else {
                        let opened = node.factory.open(name, configuration, Arc::clone(&node.catalog), node.commands.clone(), &node.cancellation).await?;
                        node.catalog.insert_open(Arc::clone(&opened.database))?;
                        initial.push((opened.database, opened.driver));
                    }
                }
            }
            Membership::Placed => {}
            Membership::Fixed(databases) => for (name, configuration) in databases {
                let opened = node.factory.open(name, configuration, Arc::clone(&node.catalog), node.commands.clone(), &node.cancellation).await?;
                node.catalog.insert_open(Arc::clone(&opened.database))?;
                initial.push((opened.database, opened.driver));
            },
        }
        Ok((node.clone(), NodeDriver { node, commands: receiver, initial, local_opens: BTreeMap::new() }))
    }
    pub fn catalog(&self) -> &Arc<DatabaseCatalog> { &self.catalog }
    pub fn memory(&self) -> &Arc<MemoryBudget> { &self.factory.memory }
    /// The budget a query or a result stream reserves from, as the node's query options shape it.
    pub fn query_memory(&self) -> Arc<MemoryBudget> { self.factory.options.query.query_memory(&self.factory.memory) }
    pub fn cache_budget(&self) -> &Arc<CacheBudget> { &self.factory.cache }
    pub fn hold_active_call(&self, database: &Database) -> Option<Box<dyn Send + Sync>> {
        self.read_fence.as_ref()?.hold_active_call(database)
    }
    pub fn now(&self) -> Timestamp { self.factory.clock.now() }
    /// Captures the query clock without narrowing to the durable timestamp unit.
    pub fn query_instant(&self) -> rtdb_types::Instant { self.factory.clock.query_instant() }
    pub fn shutdown(&self) { self.cancellation.cancel(); }
    pub fn cancellation(&self) -> &CancellationToken { &self.cancellation }
    pub fn critical_ingestion_error(&self) -> Option<Anomaly> {
        self.catalog.open_databases().values().filter(|db| db.name() == "xtdb" || db.configuration().critical.unwrap_or(false)).find_map(|db| db.progress().fault())
    }
    fn ensure_open(&self) -> Result<()> { if self.cancellation.is_cancelled() { Err(closed()) } else { Ok(()) } }

    async fn admit(&self, database: &str, access: DatabaseAccess, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<()> {
        self.ensure_open()?;
        self.cancellation.run_until_cancelled(cancellation.run_until_cancelled(identity.revalidate(self.now()))).await.flatten().ok_or_else(cancelled)??;
        if cancellation.is_cancelled() { return Err(cancelled()); }
        self.ensure_open()?;
        // A placed node may stop holding a session's database at any moment; the command is refused before anything runs, so the gateway may resend it to the new holder.
        if matches!(self.factory.options.membership, Membership::Placed) && self.catalog.database(database).is_err() {
            return Err(lost_placement(database));
        }
        identity.authorize_database(database, access)
    }

    pub fn session(&self, database: impl Into<String>, identity: Arc<dyn SessionIdentity>) -> Result<NativeSession> {
        self.ensure_open()?;
        let database = database.into();
        identity.authorize_database(&database, DatabaseAccess::Read)?;
        let local = self.catalog.database(&database)?;
        let placement = self.catalog.is_placed().then(|| Arc::downgrade(&local));
        let service = Arc::new(NativeSessionService { node: self.clone(), identity, database: database.clone(), placement });
        let state = Session::new(service.clone(), database, &self.factory.options.default_timezone, &self.cancellation)?;
        Ok(NativeSession { state, service })
    }
    pub fn snapshot_basis(&self) -> Result<TimeBasis> {
        self.ensure_open()?;
        Ok(TimeBasis { databases: self.catalog.open_databases().into_iter().map(|(name, database)| {
            let key = database.partition.value().live().latest_completed_tx();
            (name, vec![key.map(|key| key.system_time.into())])
        }).collect() })
    }
    pub fn latest_completed_transactions(&self) -> Result<BTreeMap<String, Vec<Option<TransactionKey>>>> {
        self.ensure_open()?;
        Ok(self.catalog.open_databases().into_iter().map(|(name, database)| (name, vec![database.partition.value().live().latest_completed_tx()])).collect())
    }
    pub fn latest_submitted_positions(&self) -> Result<BTreeMap<String, Vec<SourcePosition>>> {
        self.ensure_open()?;
        self.catalog.open_databases().into_iter().map(|(name, database)| Ok((name, vec![database.logs.latest_source_position()?]))).collect()
    }
    /// Awaiting indexed progress fails on a node whose indexer is disabled, as the reference's awaits do.
    fn ensure_indexing(&self) -> Result<()> {
        if self.factory.options.replication.enabled { Ok(()) } else { Err(Anomaly::fault("xtdb/log-processor-not-initialised", "log processor not initialised")) }
    }

    async fn confirm_hosted_source_owner(
        &self, database: &str, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken,
    ) -> Result<String> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        self.ensure_indexing()?;
        let database = self.attached_database(database, placement)?;
        let fence = self.read_fence.as_ref().ok_or_else(hosted_source_unavailable)?;
        let confirmation = cancellation.run_until_cancelled(async {
            let history = fence.confirm_hosted(&database).await?;
            if !Arc::ptr_eq(&database, &self.catalog.database(database.name())?) {
                return Err(hosted_source_unavailable());
            }
            Ok(history)
        });
        tokio::time::timeout(Duration::from_secs(60), confirmation)
            .await.map_err(|_| timeout_error())?.ok_or_else(cancelled)?
    }

    async fn await_durable_source(
        &self, database: &str, identity: &dyn SessionIdentity, hosted: bool, placement: Option<&Weak<Database>>, cancellation: &CancellationToken,
    ) -> Result<Option<HostedSourceReceipt>> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        self.ensure_indexing()?;
        let database = self.attached_database(database, placement)?;
        let barrier = cancellation.run_until_cancelled(async {
            let history = if hosted {
                let fence = self.read_fence.as_ref().ok_or_else(hosted_source_unavailable)?;
                Some(fence.confirm_hosted(&database).await?)
            } else {
                None
            };
            let applied = database.await_durable_source(cancellation).await?;
            let Some(history) = history else { return Ok(None); };
            if !Arc::ptr_eq(&database, &self.catalog.database(database.name())?) {
                return Err(hosted_source_unavailable());
            }
            Ok(Some(HostedSourceReceipt { history, applied }))
        });
        tokio::time::timeout(Duration::from_secs(60), barrier)
            .await.map_err(|_| timeout_error())?.ok_or_else(cancelled)?
    }

    pub async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        self.ensure_open()?;
        let Some(basis) = basis else { return Ok(()); };
        if basis.databases.values().any(|positions| !positions.is_empty()) { self.ensure_indexing()?; }
        let waits = self.catalog.open_databases().into_iter().filter_map(|(name, database)| basis.databases.get(&name).and_then(|positions| positions.first()).copied().map(|position| async move {
            if !matches!(database.await_authority, AwaitAuthority::SourcePosition) { return Err(invalid_token()); }
            database.replication.await_source(SourcePosition(position.0), cancellation).await
        }));
        tokio::time::timeout(timeout, try_join_all(waits)).await.map_err(|_| timeout_error())??;
        Ok(())
    }
    /// Submits a flush request to each open writable database and waits for its source position.
    pub async fn flush_all(&self, timeout: Duration) -> Result<()> {
        self.ensure_open()?;
        let databases = self.catalog.open_database_snapshot(self.memory())?;
        let work = async {
            for database in databases.rows().iter().filter(|database| !database.read_only()) {
                let position = database.flush().await?;
                database.progress().await_source(position, &self.cancellation).await?;
            }
            Ok(())
        };
        tokio::time::timeout(timeout, self.cancellation.run_until_cancelled(work))
            .await.map_err(|_| timeout_error())?.ok_or_else(closed)?
    }
    pub async fn sync_all(&self, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        self.ensure_open()?;
        let work = async {
            let mut seen = BTreeSet::new();
            loop {
                let fresh = self.catalog.open_databases().into_iter().filter(|(name, _)| !seen.contains(name)).collect::<Vec<_>>();
                if fresh.is_empty() { return Ok(()); }
                for (name, _) in &fresh { seen.insert(name.clone()); }
                try_join_all(fresh.into_iter().map(|(_, database)| async move {
                    let position = database.logs.latest_source_position()?;
                    database.replication.await_source(position, cancellation).await
                })).await?;
            }
        };
        tokio::time::timeout(timeout, work).await.map_err(|_| timeout_error())?
    }

    fn attached_database(&self, database: &str, placement: Option<&Weak<Database>>) -> Result<Arc<Database>> {
        let current = self.catalog.database(database).map_err(|error| if self.catalog.is_placed() { lost_placement(database) } else { error })?;
        if placement.is_some_and(|expected| !std::ptr::eq(expected.as_ptr(), Arc::as_ptr(&current))) {
            return Err(lost_placement(database));
        }
        Ok(current)
    }

    pub async fn query_context(&self, database: &str, basis: &QueryBasis, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.query_context_in_placement(database, basis, identity, None, CatalogReadCut::Current, cancellation).await
    }

    async fn query_context_in_placement(&self, database: &str, basis: &QueryBasis, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cut: CatalogReadCut, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        let placed = self.catalog.is_placed();
        let held = (placed || !matches!(cut, CatalogReadCut::Current)).then(|| self.attached_database(database, placement)).transpose()?;
        match cut {
            CatalogReadCut::HostedApplied(_) => {
                self.ensure_indexing()?;
                let attached = held.as_ref().filter(|_| placed && placement.is_some()).ok_or_else(hosted_source_unavailable)?;
                let fence = self.read_fence.as_ref().ok_or_else(hosted_source_unavailable)?;
                fence.confirm_hosted(attached).await?;
            }
            CatalogReadCut::Current | CatalogReadCut::Reserved(_) => {
                if let Some(fence) = &self.read_fence { fence.confirm(database).await?; }
            }
        }
        if let Some(previous) = &held {
            let current = self.catalog.database(database).map_err(|_| lost_placement(database))?;
            if !Arc::ptr_eq(previous, &current) { return Err(lost_placement(database)); }
        }

        let query_cpu = held.as_ref().and_then(|database| database.query_cpu_tracker());
        let fragment_residence = held.as_ref().and_then(|database| database.fragment_residence().cloned());
        let mut snapshots = BTreeMap::new();
        let mut lease: Option<Arc<rtdb_storage::ReadLease>> = None;
        let mut applied = None;
        let classic = if held.is_some() { BTreeMap::new() } else { self.catalog.open_databases() };
        for (name, database) in held.into_iter().map(|held| (database.to_owned(), held)).chain(classic) {
            if !placed && identity.authorize_database(&name, DatabaseAccess::Read).is_err() { continue; }
            let database_lease = match cut {
                CatalogReadCut::HostedApplied(_) => Some(database.applied_read_lease(&self.factory.clock, &self.factory.options.garbage)?),
                CatalogReadCut::Current | CatalogReadCut::Reserved(_) => self.catalog_read_lease(&database).await?,
            };
            if let Some(database_lease) = database_lease {
                lease = Some(match lease { Some(lease) => lease.earlier(database_lease), None => database_lease });
            }
            let snapshot = match cut {
                CatalogReadCut::Current => {
                    let head = database.partition.value().live().latest_completed_tx().map(|key| key.system_time);
                    database.partition.open_snapshot(head)
                }
                CatalogReadCut::Reserved(reserved) => {
                    let snapshot = database.partition.snapshot();
                    if snapshot.tx_basis() != Some(reserved) {
                        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/capture-raced", "The exact reserved transaction is no longer the applied catalog head"));
                    }
                    snapshot
                }
                CatalogReadCut::HostedApplied(minimum) => {
                    crate::read_currency::require_current(&name, lease.as_deref())?;
                    let snapshot = database.partition.snapshot();
                    let selected = snapshot.tx_basis().filter(|selected| *selected == minimum
                        || (selected.tx_id > minimum.tx_id && selected.system_time > minimum.system_time))
                        .ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/applied-below-floor",
                            "The local applied catalog does not cover the caller's transaction floor"))?;
                    applied = Some(selected);
                    snapshot
                }
            };
            let requested = basis.snapshot.databases.get(&name).and_then(|partitions| partitions.first()).copied().flatten();
            if let Some(requested) = requested.filter(|requested| snapshot.tx_basis().is_none_or(|head| *requested > head.system_time.into())) {
                return Err(rtdb_query::unindexed_tx(requested, snapshot.tx_basis()));
            }
            let cap = requested.map(|time| time.as_micros()).transpose()?;
            let cap = match (cap, basis.snapshot_time) { (Some(time), Some(upper)) => Some(time.min(upper)), (time, _) => time };
            let snapshot = rtdb_query::retain_catalog_snapshot(snapshot.with_system_time_cap(cap)?, &database.storage).await?;
            snapshots.insert(name, DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&database.storage) });
        }
        crate::read_currency::require_current(database, lease.as_deref())?;
        let applied_basis = applied.map(|selected| QueryBasis {
            snapshot: TimeBasis { databases: BTreeMap::from([(database.to_owned(), vec![Some(selected.system_time.into())])]) },
            snapshot_time: Some(selected.system_time),
            current_time: selected.system_time.into(),
            timezone: basis.timezone.clone(),
        });
        let basis = applied_basis.as_ref().unwrap_or(basis);
        let mut catalog = CatalogSnapshot::new(database, snapshots, false)?.with_runtime(Arc::new(crate::runtime::NodeCatalogRuntime::new(self.clone(), cancellation.clone())))
            .with_ordered_indexes(Arc::new(crate::ordered_index::Inventory))
            .with_fragmenter(self.scan_fragmenter.clone(), fragment_residence);
        if let Some(lease) = lease { catalog = catalog.with_read_lease(lease); }
        QueryContext::with_owner(catalog, self.factory.options.query.query_memory(&self.factory.memory), basis.current_time, basis.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?
            .with_accelerators(Arc::clone(&self.factory.accelerators))
            .with_read_options(&basis.snapshot, basis.snapshot_time, self.query_instant())
            .map(|context| context.with_query_cpu(query_cpu))
    }

    pub(crate) async fn catalog_read_lease(&self, database: &Database) -> Result<Option<Arc<rtdb_storage::ReadLease>>> {
        if !self.factory.options.replication.enabled { return Ok(None); }
        database.read_lease(&self.factory.clock, &self.factory.options.garbage).await.map(Some)
    }

    /// Waits for this node's copy to reach the delegated fragment's transaction basis.
    /// The evaluator checks the exact snapshot it captures after the wait.
    pub async fn scan_fragment(&self, fragment: &rtdb_query::ScanFragment, catch_up: Duration, cancellation: &CancellationToken) -> Result<Vec<u8>> {
        Ok(self.scan_fragment_retained(fragment, catch_up, cancellation).await?.into_bytes())
    }

    /// Serves the fragment with a reservation for the response's retained bytes.
    pub async fn scan_fragment_retained(&self, fragment: &rtdb_query::ScanFragment, catch_up: Duration,
        cancellation: &CancellationToken) -> Result<rtdb_query::FragmentBytes> {
        self.ensure_open()?;
        let database = self.catalog.database(fragment.database())?;
        if !fragment.residence.as_ref().is_some_and(|expected| database.fragment_residence() == Some(expected)) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.node/fragment-residence", "The node's captured database is in another storage residence or replica epoch"));
        }
        let deadline = tokio::time::Instant::now() + catch_up;
        while database.partition.value().live().latest_completed_tx().is_none_or(|head| head.tx_id < fragment.basis.tx_id) {
            if tokio::time::Instant::now() >= deadline {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.node/fragment-basis", format!("{} did not reach the fragment's basis in time", database.name())));
            }
            cancellation.run_until_cancelled(tokio::time::sleep(Duration::from_millis(5))).await.ok_or_else(cancelled)?;
        }
        let lease = self.catalog_read_lease(&database).await?;
        let snapshot = database.partition.open_snapshot(Some(fragment.basis.system_time));
        let snapshot = rtdb_query::retain_catalog_snapshot(snapshot, &database.storage).await?;
        crate::read_currency::require_current(database.name(), lease.as_deref())?;
        let snapshots = BTreeMap::from([(database.name().to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&database.storage) })]);
        let mut catalog = CatalogSnapshot::new(database.name(), snapshots, false)?;
        if let Some(lease) = &lease { catalog = catalog.with_read_lease(Arc::clone(lease)); }
        let context = QueryContext::with_owner(catalog, Arc::clone(&self.factory.memory), self.query_instant(), fragment.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?;
        rtdb_storage::IoRegion::leased(lease, rtdb_query::evaluate_fragment_retained(context, fragment)).await
    }

    async fn submit(&self, database: &str, operations: Vec<TxOp>, mut options: SubmitOptions, permit: Option<crate::ReplaySubmissionPermit>, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<SubmittedTx> {
        self.admit(database, DatabaseAccess::Write, identity, cancellation).await?;
        if !self.factory.options.owned_relations
            && operations.iter().any(TxOp::is_owned)
        {
            return Err(owned_relations_disabled());
        }
        let database = self.attached_database(database, placement)?;
        let admission = if self.catalog.is_placed() {
            Some(cancellation.run_until_cancelled(database.admit_transaction()).await.ok_or_else(cancelled)??)
        } else { None };
        let replay = permit.map(|permit| {
            let key = options.user_metadata.as_ref().and_then(|metadata| idempotency_key(metadata.value()));
            if key.is_some_and(|key| key != permit.key) {
                return Err(Anomaly::incorrect("rtdb.node/conflicting-replay-key", "Application key conflicts with the private replay permit"));
            }
            let submitted_at = self.now();
            let replay = rtdb_tx::ReplaySubmission::admit(&permit.key, submitted_at, permit.not_after)?;
            Ok((replay, submitted_at))
        }).transpose()?;
        database.require_writable()?;
        if database.external_source() { return Err(Anomaly::incorrect("xtdb/submit-tx-to-external-source-db", "Cannot submit transactions to an external-source database").with_context("db-name", database.name().into())); }
        options.user = identity.user().map(str::to_owned);
        options.default_tz = rtdb_expr::normalize_timezone(&options.default_tz)?;
        let sql_options = rtdb_sql::PrepareOptions { generate_series_end_exclusive: self.factory.options.query.generate_series_end_exclusive, ..Default::default() };
        let authority = match &database.await_authority {
            AwaitAuthority::Live { codec, .. } => Some(codec.as_ref()),
            _ => None,
        };
        let seed = authority.map(|authority| authority.scope().issue());
        let source = prepare_submission_with_sql_options(operations, options, database.name(), &self.factory.memory, &sql_options)?
            .with_placed_seed(seed);
        let (source, submitted_at) = match replay {
            Some((replay, submitted_at)) => (source.with_replay_submission(replay), Some(submitted_at)),
            None => (source, None),
        };
        let payload = source.encode()?.into_bytes();
        let submitted_bytes = payload.len() as u64;
        let append_time = submitted_at.unwrap_or_else(|| self.now());
        let position = cancellation.run_until_cancelled(database.replication.submit_source_encoded(payload, append_time)).await.ok_or_else(cancelled)??;
        if let Some(admission) = &admission {
            database.record_submission(submitted_bytes);
            let mut latest = admission.lock();
            *latest = (*latest).max(Some(position));
        }
        let placed_receipt = authority.zip(seed).map(|(authority, seed)| {
            let attempt = seed.identify(position, identity.user())?;
            PlacedToken::Pending { attempt, submitted_at: append_time }.seal(authority)
        }).transpose()?;
        Ok(SubmittedTx { tx_id: TxId(position.0), placed_receipt })
    }

    /// The committed outcome visible to the retry when its duplicate guard ran.
    async fn historical_keyed_transaction(&self, database: &str, key: &str, retry_time: Timestamp, submitted_at: Timestamp, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<Option<TransactionKey>> {
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let timestamp = |time: Timestamp| Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let arguments = vec![timestamp(retry_time), timestamp(submitted_at), Value::Utf8(key.to_owned())];
        let options = rtdb_sql::PrepareOptions {
            parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(),
            default_timezone: context.timezone.clone(),
            generate_series_end_exclusive: context.options.generate_series_end_exclusive,
            ..Default::default()
        };
        let sql = "SELECT _id, system_time FROM xt.txs FOR ALL VALID_TIME WHERE committed = true AND system_time = (SELECT k._system_from FROM public.rtdb_idempotency_keys FOR SYSTEM_TIME AS OF ? FOR VALID_TIME FROM ? TO NULL AS k WHERE k._id = ? ORDER BY k._system_from DESC LIMIT 1)";
        let prepared = match prepare_query(sql, &context, &options) {
            Ok(prepared) => prepared,
            Err(error) if error.code == "xtdb/table-not-found" || error.code.contains("unknown-table") => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut cursor = prepared.execute(context, arguments).await?;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            let id = read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)?;
            let time = read_value(batch.schema().field(1), batch.column(1).as_ref(), 0)?;
            let (Value::I64(id), Value::Timestamp { value, unit: TimeUnit::Microsecond, .. }) = (id, time) else {
                return Err(Anomaly::fault("rtdb.node/idempotency-result", "Stored idempotency outcome has no transaction key"));
            };
            return Ok(Some(TransactionKey { tx_id: TxId(id), system_time: Timestamp(value) }));
        }
        Ok(None)
    }
    async fn placed_outcome_key(&self, database: &str, attempt: PlacedAttempt, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<Option<TransactionKey>> {
        let basis = QueryBasis { snapshot: TimeBasis::default(), snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let options = rtdb_sql::PrepareOptions { parameter_types: BTreeMap::from([(0, LogicalType::Scalar(ScalarType::Utf8))]), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let sql = "SELECT tx_id, tx_system_time FROM xt.placed_attempt_outcomes FOR ALL VALID_TIME WHERE _id = ?";
        let prepared = match prepare_query(sql, &context, &options) {
            Ok(prepared) => prepared,
            Err(error) if error.code == "xtdb/table-not-found" || error.code.contains("unknown-table") => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut cursor = prepared.execute(context, vec![Value::Utf8(attempt.outcome_key())]).await?;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            let tx_id = read_value(batch.schema().field(0), batch.column(0).as_ref(), 0)?;
            let time = read_value(batch.schema().field(1), batch.column(1).as_ref(), 0)?;
            if let (Value::I64(tx_id), Value::Timestamp { value, unit: TimeUnit::Microsecond, .. }) = (tx_id, time)
                && tx_id == attempt.position().0
            {
                return Ok(Some(TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(value) }));
            }
            return Err(bad_attempt_outcome());
        }
        Ok(None)
    }

    async fn redeem_placed_attempt(&self, db: &Database, attempt: PlacedAttempt, submitted_at: Timestamp, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<(TransactionResult, String)> {
        let codec = db.await_authority.codec().ok_or_else(invalid_token)?;
        if self.now().0 >= submitted_at.0.saturating_add(rtdb_tx::RECEIPT_WINDOW_MICROS) {
            return Err(Anomaly::incorrect("rtdb/await-token-expired", "The transaction attempt receipt has expired"));
        }
        let epoch = attempt.scope().writer_epoch();
        let current = codec.scope().writer_epoch();
        let through = match &db.await_authority {
            AwaitAuthority::Checkpoint { through, .. } => {
                if epoch > current { return Err(receipt_checkpoint_behind(db.name())); }
                *through
            }
            AwaitAuthority::Live { recovery, .. } => {
                if epoch > current { return Err(receipt_replica_behind(db.name())); }
                if epoch == current {
                    db.await_installed(|prefix| prefix.source >= attempt.position(), cancellation).await?
                } else {
                    // Only the recovered end of the closed epoch authorizes a negative outcome.
                    let offset = cancellation.run_until_cancelled(recovery.confirm()).await.ok_or_else(cancelled)?
                        .map_err(|error| receipt_replica_behind(db.name()).with_context("cause", error.code.into()))?;
                    let recovered = ReplicaPosition::from_epoch_offset(db.logs.replica_epoch, offset.0)?;
                    db.await_installed(|prefix| prefix.replica >= recovered, cancellation).await?
                }
            }
            _ => return Err(invalid_token()),
        };
        let key = match db.progress().current().placed_outcome(attempt) {
            Some(key) => Some(key),
            None => self.placed_outcome_key(db.name(), attempt, identity, placement, cancellation).await?,
        };
        let Some(key) = key else {
            return Err(match db.await_authority {
                AwaitAuthority::Checkpoint { .. } => receipt_checkpoint_behind(db.name()),
                _ if epoch < current => Anomaly::incorrect("rtdb/attempt-unresolved", "The closed writer epoch has no outcome for this transaction attempt"),
                _ => Anomaly::new(AnomalyCategory::Unavailable, "rtdb/attempt-pending", "The transaction attempt has no installed outcome"),
            });
        };
        let result = self.await_transaction_result(db.name(), key.tx_id, Some(key), identity, placement, cancellation).await?;
        let token = PlacedToken::Applied { attempt, through }.seal(codec)?;
        Ok((result, token))
    }

    pub async fn await_transaction(&self, database: &str, tx_id: TxId, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.await_transaction_result(database, tx_id, None, identity, None, cancellation).await
    }

    async fn await_transaction_result(&self, database: &str, tx_id: TxId, expected: Option<TransactionKey>, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        self.ensure_indexing()?;
        let db = self.attached_database(database, placement)?;
        if expected.is_none() && !matches!(db.await_authority, AwaitAuthority::SourcePosition) { return Err(invalid_token()); }
        let latest = db.replication.await_tx(tx_id, cancellation).await?
            .filter(|result| expected.is_none_or(|key| key == result.tx_key()));
        if let Some(result) = &latest && !result.error().is_some_and(is_historical_guard_abort) {
            if !db.external_source()
                && let Some(original) = result.error().map(rtdb_tx::private_replay_original).transpose()?.flatten()
            {
                return Ok(TransactionResult::Committed { tx_key: original });
            }
            return Ok(if db.external_source() { result.as_ref().clone() } else { answer_duplicate(result.as_ref().clone()) });
        }
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let options = rtdb_sql::PrepareOptions { parameter_types: BTreeMap::from([(0, LogicalType::Scalar(rtdb_types::ScalarType::I64))]), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let mut cursor = prepare_query("SELECT system_time, committed, error, user_metadata FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?", &context, &options)?
            .execute(context, vec![Value::I64(tx_id.0)]).await?;
        let batch = cursor.next_batch().await?.ok_or_else(|| Anomaly::fault("xtdb/tx-await-missing", format!("tx {tx_id} not found after await")))?;
        let schema = batch.batch().schema();
        let cell = |name: &str| -> Result<Value> {
            let index = schema.index_of(name).map_err(|error| Anomaly::fault("rtdb.node/tx-result-schema", error.to_string()))?;
            read_value(schema.field(index), batch.batch().column(index).as_ref(), 0)
        };
        let system_time = match cell("system_time")? { Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Timestamp(value), _ => return Err(Anomaly::fault("rtdb.node/tx-result-time", "Stored transaction result has no microsecond system-time")) };
        let tx_key = TransactionKey { tx_id, system_time };
        if expected.is_some_and(|key| key != tx_key) { return Err(bad_attempt_outcome()); }
        match cell("committed")? {
            Value::Bool(true) => Ok(TransactionResult::Committed { tx_key }),
            Value::Bool(false) => {
                let error = if let Some(TransactionResult::Aborted { error, .. }) = latest.as_deref() {
                    error.clone()
                } else {
                    match cell("error")? {
                        Value::Null | Value::Undefined => None,
                        Value::Transit(bytes) => Some(RetainedAnomaly::from_transit(&bytes, &self.factory.memory)?),
                        _ => return Err(Anomaly::fault("rtdb.node/tx-result-error", "Stored transaction error is not Transit")),
                    }
                };
                if !db.external_source()
                    && let Some(error) = &error
                    && let Some(original) = rtdb_tx::private_replay_original(error)?
                {
                    return Ok(TransactionResult::Committed { tx_key: original });
                }
                if !db.external_source()
                    && let Some(error) = &error
                    && is_historical_guard_abort(error)
                    && let Some(key) = idempotency_key(&cell("user_metadata")?)
                    && let Some(submitted_at) = historical_guard_sample(error)
                    && let Some(original) = self.historical_keyed_transaction(database, key, system_time, submitted_at, identity, placement, cancellation).await?
                {
                    return Ok(TransactionResult::Committed { tx_key: original });
                }
                let result = TransactionResult::Aborted { tx_key, error };
                Ok(if db.external_source() { result } else { answer_duplicate(result) })
            }
            _ => Err(Anomaly::fault("rtdb.node/tx-result-status", "Stored transaction result has no commit status")),
        }
    }
    pub async fn attach_database(&self, name: String, configuration: DatabaseConfig, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.admit("xtdb", DatabaseAccess::Write, identity, cancellation).await?;
        crate::resources::validate_database_shape(&name, &configuration)?;
        let primary = self.catalog.database("xtdb")?;
        primary.require_writable()?;
        let message = SourceMessage::Protobuf(SourceBody::AttachDatabase(AttachDatabase { db_name: Some(name), config: Some(configuration) }));
        let position = cancellation.run_until_cancelled(primary.replication.submit_source(&message, self.now())).await.ok_or_else(cancelled)??;
        self.await_transaction("xtdb", TxId(position.0), identity, cancellation).await
    }
    pub async fn detach_database(&self, name: String, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.admit("xtdb", DatabaseAccess::Write, identity, cancellation).await?;
        let primary = self.catalog.database("xtdb")?;
        primary.require_writable()?;
        let message = SourceMessage::Protobuf(SourceBody::DetachDatabase(DetachDatabase { db_name: Some(name) }));
        let position = cancellation.run_until_cancelled(primary.replication.submit_source(&message, self.now())).await.ok_or_else(cancelled)??;
        self.await_transaction("xtdb", TxId(position.0), identity, cancellation).await
    }
    pub async fn database_or_attach(&self, name: String, configuration: DatabaseConfig) -> Result<Option<Arc<Database>>> {
        self.ensure_open()?;
        if let Ok(database) = self.catalog.database(&name) { return Ok(Some(database)); }
        let slot = if let Some(slots) = &self.placement_slots {
            let permit = self.cancellation.run_until_cancelled(Arc::clone(slots).acquire_owned()).await
                .ok_or_else(closed)?.map_err(|_| closed())?;
            if let Ok(database) = self.catalog.database(&name) { return Ok(Some(database)); }
            Some(permit)
        } else { None };
        let (completion, result) = oneshot::channel();
        self.commands.send(CatalogCommand::AttachLocal { name, configuration, slot, completion }).await.map_err(|_| closed())?;
        result.await.map_err(|_| closed())?
    }
    /// Stops running a database this node holds under a placed catalog: its driver is cancelled and its resources drained.
    /// Returns once the detach has begun; the name stays occupied until the drain completes.
    pub async fn detach_local(&self, name: String) -> Result<()> {
        self.ensure_open()?;
        let (completion, result) = oneshot::channel();
        self.commands.send(CatalogCommand::DetachLocal { name, completion }).await.map_err(|_| closed())?;
        result.await.map_err(|_| closed())?
    }
}

impl NodeDriver {
    /// Closes an unstarted node, drops its database drivers, and drains its resource region.
    /// Use after listener setup fails or after read-only startup inspection; `run` consumes this same owner.
    pub async fn close_without_run(mut self) -> Result<()> {
        self.node.shutdown();
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            match command {
                CatalogCommand::Apply { completion, .. } | CatalogCommand::DetachLocal { completion, .. } => { let _ = completion.send(Err(closed())); },
                CatalogCommand::AttachLocal { completion, .. } => { let _ = completion.send(Err(closed())); }
            }
        }
        self.node.catalog.cancel_all();
        self.initial.clear();
        futures::future::join_all(self.node.catalog.held_databases().into_iter().map(|database| async move { database.close_io().await })).await;
        self.node.catalog.clear_after_join();
        self.node.factory.resources.close().await
    }
    pub async fn run(mut self) -> Result<()> {
        let mut tasks = JoinSet::new();
        for (database, driver) in self.initial.drain(..) { tasks.spawn(async move { DatabaseCompletion::Worker { database, result: driver.run().await } }); }
        let mut failure = None;
        loop {
            if self.node.cancellation.is_cancelled() { break; }
            if self.node.catalog.is_placed() && let Some(completion) = tasks.try_join_next() {
                match completion {
                    Ok(completion) => { let _ = self.complete(completion, &mut tasks); }
                    Err(error) => { failure = Some(Anomaly::fault("rtdb.node/database-task", error.to_string())); break; }
                }
                continue;
            }
            tokio::select! {
                biased;
                _ = self.node.cancellation.cancelled() => break,
                command = self.commands.recv() => match command {
                    Some(command) => {
                        let token = self.node.cancellation.clone();
                        if token.run_until_cancelled(self.command(command, &mut tasks)).await.is_none() { break; }
                    }
                    None => break,
                },
                completion = tasks.join_next(), if !tasks.is_empty() => match completion {
                    Some(Ok(completion)) => { let _ = self.complete(completion, &mut tasks); }
                    Some(Err(error)) => { failure = Some(Anomaly::fault("rtdb.node/database-task", error.to_string())); break; }
                    None => (),
                },
            }
        }
        self.node.shutdown();
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            match command {
                CatalogCommand::Apply { completion, .. } | CatalogCommand::DetachLocal { completion, .. } => { let _ = completion.send(Err(closed())); },
                CatalogCommand::AttachLocal { completion, .. } => { let _ = completion.send(Err(closed())); }
            }
        }
        self.node.catalog.cancel_all();
        while let Some(completion) = tasks.join_next().await {
            match completion {
                Ok(completion) => if let Err(error) = self.complete(completion, &mut tasks) { if failure.is_none() { failure = Some(error); } },
                Err(error) => if failure.is_none() { failure = Some(Anomaly::fault("rtdb.node/database-task", error.to_string())); },
            }
        }
        futures::future::join_all(self.node.catalog.held_databases().into_iter().map(|database| async move { database.close_io().await })).await;
        self.node.catalog.clear_after_join();
        if let Err(error) = self.node.factory.resources.close().await { if failure.is_none() { failure = Some(error); } }
        failure.map_or(Ok(()), Err)
    }
    fn drain(database: Arc<Database>, tasks: &mut JoinSet<DatabaseCompletion>) {
        tasks.spawn(async move { database.close_io().await; DatabaseCompletion::Drained(database) });
    }
    fn complete(&mut self, completion: DatabaseCompletion, tasks: &mut JoinSet<DatabaseCompletion>) -> Result<()> {
        match completion {
            DatabaseCompletion::Worker { database, result } => {
                if let Err(error) = &result { database.progress().ingestion_failed(error.clone()); }
                if self.node.catalog.driver_finished(&database) { Self::drain(database, tasks); }
                result
            }
            DatabaseCompletion::Drained(database) => { self.node.catalog.teardown_finished(&database); Ok(()) }
            DatabaseCompletion::LocalOpened { name, result } => {
                self.finish_local_open(name, result, tasks);
                Ok(())
            }
        }
    }

    fn dispatch_local(&mut self, name: String, request: LocalRequest, tasks: &mut JoinSet<DatabaseCompletion>) {
        if self.node.cancellation.is_cancelled() {
            match request {
                LocalRequest::Attach { completion, .. } => { let _ = completion.send(Err(closed())); }
                LocalRequest::Detach { completion } => { let _ = completion.send(Err(closed())); }
            }
            return;
        }
        if let Some(opening) = self.local_opens.get_mut(&name) {
            opening.queued.push_back(request);
            return;
        }
        match request {
            LocalRequest::Attach { configuration, slot, completion } => {
                if let Ok(database) = self.node.catalog.database(&name) {
                    let _ = completion.send(Ok(Some(database)));
                    return;
                }
                if let Err(error) = self.node.catalog.check_local_attach(&name, &configuration) {
                    let _ = completion.send(Err(error));
                    return;
                }
                self.local_opens.insert(name.clone(), LocalOpen { _slot: slot, completion, queued: VecDeque::new() });
                let node = self.node.clone();
                tasks.spawn(async move {
                    let prepare = node.factory.open(name.clone(), configuration, Arc::clone(&node.catalog), node.commands.clone(), &node.cancellation);
                    let result = node.cancellation.run_until_cancelled(prepare).await.unwrap_or_else(|| Err(closed()));
                    DatabaseCompletion::LocalOpened { name, result }
                });
            }
            LocalRequest::Detach { completion } => {
                let result = self.node.catalog.begin_local_detach(&name).map(|ready| {
                    if let Some(database) = ready { Self::drain(database, tasks); }
                });
                let _ = completion.send(result);
            }
        }
    }

    fn finish_local_open(&mut self, name: String, result: Result<OpenedDatabase>, tasks: &mut JoinSet<DatabaseCompletion>) {
        let opening = self.local_opens.remove(&name).expect("an open owns its name until it completes");
        let result = result.and_then(|opened| {
            let database = Arc::clone(&opened.database);
            let admitted = if self.node.cancellation.is_cancelled() { Err(closed()) }
                else { self.node.catalog.insert_open(Arc::clone(&database)) };
            match admitted {
                Ok(()) => {
                    tasks.spawn(async move { DatabaseCompletion::Worker { database: opened.database, result: opened.driver.run().await } });
                    Ok(Some(database))
                }
                Err(error) => {
                    drop(opened);
                    Self::drain(database, tasks);
                    Err(error)
                }
            }
        });
        let _ = opening.completion.send(result);
        for request in opening.queued { self.dispatch_local(name.clone(), request, tasks); }
    }

    async fn command(&mut self, command: CatalogCommand, tasks: &mut JoinSet<DatabaseCompletion>) {
        match command {
            CatalogCommand::Apply { operation, completion } => {
                let result = match operation {
                    DbOp::AttachDatabase(attach) => self.attach(attach.db_name.unwrap_or_default(), attach.config.unwrap_or_default(), false, tasks).await.map(|_| ()),
                    DbOp::DetachDatabase(detach) => self.node.catalog.begin_detach(detach.db_name.as_deref().unwrap_or_default()).map(|ready| {
                        if let Some(database) = ready { Self::drain(database, tasks); }
                    }),
                };
                let _ = completion.send(result);
            }
            CatalogCommand::AttachLocal { name, configuration, slot: Some(slot), completion } => {
                self.dispatch_local(name, LocalRequest::Attach { configuration, slot, completion }, tasks);
            }
            CatalogCommand::DetachLocal { name, completion } if self.node.catalog.is_placed() => {
                self.dispatch_local(name, LocalRequest::Detach { completion }, tasks);
            }
            CatalogCommand::AttachLocal { name, configuration, slot: None, completion } => {
                let result = match self.node.catalog.database(&name) { Ok(database) => Ok(Some(database)), Err(_) => self.attach(name, configuration, true, tasks).await };
                let _ = completion.send(result);
            }
            CatalogCommand::DetachLocal { name, completion } => {
                let result = self.node.catalog.begin_local_detach(&name).map(|ready| { if let Some(database) = ready { Self::drain(database, tasks); } });
                let _ = completion.send(result);
            }
        }
    }
    async fn attach(&mut self, name: String, configuration: DatabaseConfig, local: bool, tasks: &mut JoinSet<DatabaseCompletion>) -> Result<Option<Arc<Database>>> {
        if local { self.node.catalog.check_local_attach(&name, &configuration)?; } else { self.node.catalog.check_attach(&name, &configuration)?; }
        if self.node.catalog.skipped(&name) { self.node.catalog.insert_skipped(name, configuration)?; return Ok(None); }
        let opened = self.node.factory.open(name, configuration, Arc::clone(&self.node.catalog), self.node.commands.clone(), &self.node.cancellation).await?;
        let database = Arc::clone(&opened.database);
        if let Err(error) = self.node.catalog.insert_open(Arc::clone(&database)) {
            drop(opened);
            database.close_io().await;
            return Err(error);
        }
        tasks.spawn(async move { DatabaseCompletion::Worker { database: opened.database, result: opened.driver.run().await } });
        Ok(Some(database))
    }
}

/// Relation-local evidence from a successfully covered held page.
#[derive(Clone)]
pub struct OwnedRangeWitness {
    relation: rtdb_types::OwnedRelation,
    basis: TransactionKey,
    scope: rtdb_types::PlacedAttemptScope,
    frontier: i64,
    until: Timestamp,
    fixed: Option<Arc<rtdb_types::FixedRangeEvidence>>,
}

impl OwnedRangeWitness {
    pub fn operation(self) -> TxOp {
        TxOp::OwnedCheckRange {
            relation: self.relation, basis: self.basis, scope: self.scope, frontier: self.frontier, until: self.until, fixed: self.fixed,
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.fixed.as_ref().map_or(0, |fixed| fixed.retained_bytes())
    }

    pub fn same_range(&self, other: &Self) -> bool { self.fixed == other.fixed && self.until == other.until }

    pub fn matches_scope(&self, scope: Option<rtdb_types::PlacedAttemptScope>) -> bool {
        Some(self.scope) == scope
    }
}

pub struct OwnedChangePage {
    pub groups: Vec<rtdb_types::OwnedChangeGroup>,
    pub covered_through: Timestamp,
    pub retained_bytes: usize,
    _reservations: Vec<Arc<rtdb_columnar::MemoryReservation>>,
}

/// A held history cursor that can issue relation evidence only after successful exhaustion.
pub struct OwnedValidHistory<'a> {
    held: &'a OwnedReadSession,
    relation: &'a rtdb_types::OwnedRelation,
    cursor: Option<QueryCursor>,
    at_head: bool,
    complete: bool,
    failed: bool,
}

impl OwnedValidHistory<'_> {
    pub fn runtime_schema(&self) -> Option<&rtdb_plan::Schema> {
        self.cursor.as_ref().map(QueryCursor::runtime_schema)
    }

    pub async fn next_batch(&mut self) -> Result<Option<rtdb_columnar::OwnedBatch>> {
        if self.failed {
            return Err(Anomaly::fault("rtdb.owned/history-read", "An owned history cursor failed"));
        }
        self.held.check_relation(self.relation)?;
        let result = match &mut self.cursor {
            Some(cursor) => cursor.next_batch().await,
            None => Ok(None),
        };
        self.complete = matches!(&result, Ok(None));
        self.failed = result.is_err();
        result
    }

    pub fn witness(&self) -> Result<Option<OwnedRangeWitness>> {
        if !self.complete || self.failed {
            return Err(Anomaly::fault("rtdb.owned/history-read", "Owned history must complete before issuing evidence"));
        }
        if self.at_head {
            self.held.relation_witness(self.relation)
        } else {
            self.held.check_relation(self.relation)?;
            Ok(None)
        }
    }
}

pub struct OwnedVersion {
    pub revision: u64,
    pub system_time: Timestamp,
    pub timeline: QueryCursor,
}

#[derive(Clone, Copy)]
pub enum OwnedVersionOrder {
    Ascending,
    Descending,
}

pub struct OwnedVersionRequest {
    pub system: rtdb_types::Interval,
    pub after: u64,
    pub scan_limit: usize,
    pub result_limit: usize,
    pub order: OwnedVersionOrder,
}

pub struct OwnedVersionPage {
    pub floor: Timestamp,
    pub next_revision: u64,
    pub done: bool,
    pub scanned: usize,
    pub witness: Option<OwnedRangeWitness>,
    pub versions: Vec<OwnedVersion>,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

/// A database-scoped capture held by the same authenticated native session.
pub struct OwnedReadSession {
    context: Arc<QueryContext>,
    node: Node,
    identity: Arc<dyn SessionIdentity>,
    database: String,
    owner: String,
    present: TransactionKey,
    witness_scope: Option<rtdb_types::PlacedAttemptScope>,
    witness_until: Timestamp,
    fork_until: Timestamp,
    cancellation: CancellationToken,
    session_cancellation: CancellationToken,
    historical: Option<historical_owned_read::Scope>,
    scoped_root: Option<BTreeSet<rtdb_types::OwnedRelation>>,
    attachment: std::sync::Weak<Database>,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

pub struct OwnedReplayWorld {
    pub world: rtdb_types::OwnedWorld,
    pub source_cut: Timestamp,
    pub destination_parent: Timestamp,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

impl OwnedReadSession {
    pub fn present(&self) -> TransactionKey { self.present }

    pub fn evidence_until(&self) -> Timestamp { self.witness_until }

    /// Derive a read-only Root capability from the exact original placed hold.
    pub async fn scope_root_read(
        &self, relations: Vec<rtdb_types::OwnedRelation>,
    ) -> Result<Self> {
        self.admit_read().await?;
        self.require_unscoped_root()?;
        if self.witness_scope.is_none() || !(1..=32).contains(&relations.len())
            || relations.iter().any(|relation| !relation.is_root_data())
        {
            return Err(scoped_root_denied());
        }
        self.check_live_change_relations(&relations)?;
        let count = relations.len();
        let reservation = self.context.memory.reserve(count * 512 + 2048)?;
        let relations = relations.into_iter().collect::<BTreeSet<_>>();
        if relations.len() != count { return Err(scoped_root_denied()); }
        let cancellation = self.cancellation.child_token();
        let context = self.context.with_owned_system_cut(self.present.system_time, &cancellation)?;
        Ok(Self {
            context, node: self.node.clone(), identity: Arc::clone(&self.identity),
            database: self.database.clone(), owner: self.owner.clone(), present: self.present,
            witness_scope: self.witness_scope, witness_until: self.witness_until, fork_until: self.fork_until,
            cancellation, session_cancellation: self.session_cancellation.clone(),
            historical: None, scoped_root: Some(relations), attachment: self.attachment.clone(),
            _reservation: reservation,
        })
    }

    pub fn require_unscoped_root(&self) -> Result<()> {
        if self.scoped_root.is_some() { return Err(scoped_root_denied()); }
        Ok(())
    }

    pub fn check_witness_relation(&self, relation: &rtdb_types::OwnedRelation, evidence: &OwnedRangeWitness) -> Result<()> {
        self.check()?;
        if &evidence.relation != relation || evidence.basis != self.present
            || !evidence.matches_scope(self.witness_scope)
        {
            return Err(scoped_root_denied());
        }
        self.relation_release_guard_for([relation], true)?()
    }

    /// A completed scoped point can conservatively depend on its relation's frontier.
    pub fn scoped_point_witness(
        &self, relation: &rtdb_types::OwnedRelation, expiry: Option<Timestamp>,
    ) -> Result<Option<OwnedRangeWitness>> {
        if self.scoped_root.is_none() { return Ok(None); }
        self.relation_witness_for(relation, true, expiry)
    }

    pub async fn fork_cut(&self, scope: rtdb_types::PlacedAttemptScope) -> Result<OwnedForkCut> {
        self.require_unscoped_root()?;
        self.admit_access().await?;
        let attached_scope = self.attachment.upgrade()
            .and_then(|database| database.await_authority.codec().map(|codec| codec.scope()));
        if attached_scope != Some(scope) || self.node.now() >= self.fork_until {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        if let Some(historical) = &self.historical {
            self.check_relations(&historical.relations)?;
        }
        Ok(OwnedForkCut {
            basis: self.system_time(),
            live: self.historical.is_none().then_some(self.present),
            issued_on: self.present,
            publication: false,
            renewal: false,
            until: self.fork_until,
            scope,
        })
    }

    /// Retain the application publication seen by this original live hold.
    /// The placed archive resolves its full transaction key.
    pub async fn fork_publication_cut(&self, scope: rtdb_types::PlacedAttemptScope) -> Result<OwnedForkCut> {
        let mut cut = self.fork_cut(scope).await?;
        let head = cut.live.take().ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let publication = rtdb_query::read_owned_root_publication(Arc::clone(&self.context), &self.owner)
            .await?.filter(|publication| *publication <= head.system_time)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        self.check()?;
        cut.basis = publication;
        cut.publication = true;
        Ok(cut)
    }

    pub async fn fork_renewal_cut(
        &self, scope: rtdb_types::PlacedAttemptScope, predecessor: TransactionKey,
    ) -> Result<OwnedForkCut> {
        let mut cut = self.fork_cut(scope).await?;
        if cut.live.is_none_or(|live| predecessor.system_time >= live.system_time || predecessor.tx_id >= live.tx_id) {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        cut.live = Some(predecessor);
        cut.basis = predecessor.system_time;
        cut.renewal = true;
        Ok(cut)
    }

    pub fn system_time(&self) -> Timestamp {
        self.historical.as_ref().map_or(self.present.system_time, |scope| scope.system_time)
    }

    /// Admit a placed restore only while the original writer and replay
    /// hold still own the destination and no release has sealed it.
    pub async fn replay_parent_hold(&self, binding: [u8; 32], before_replay: bool) -> Result<Vec<u8>> {
        let archive = self.active_replay_archive(binding).await?;
        if self.replay_parent_control("replayed").await?.is_some()
            || (before_replay && self.replay_parent_control("native-frontier.v1").await?.is_some())
        {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        self.check()?;
        Ok(archive)
    }

    /// The completed inventory follows all destination parent pins and
    /// readmissions. Only that seal releases the original replay window.
    pub async fn sealed_replay_parents(&self, binding: [u8; 32]) -> Result<()> {
        let archive = self.active_replay_archive(binding).await?;
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let (seal, at) = self.replay_parent_control("replayed").await?.ok_or_else(unavailable)?;
        let (inventory, inventory_at) = self.replay_parent_control("factstore-inventory.v1").await?
            .ok_or_else(unavailable)?;
        let (frontier, frontier_at) = self.replay_parent_control("native-frontier.v1").await?
            .ok_or_else(unavailable)?;
        if seal != binding || inventory_at != at || frontier_at != at || frontier.as_slice() != b"sealed" {
            return Err(unavailable());
        }
        rtdb_tx::replayed_archive_cut(&inventory, binding, &archive)?;
        self.check()
    }

    async fn active_replay_archive(&self, binding: [u8; 32]) -> Result<Vec<u8>> {
        self.require_unscoped_root()?;
        self.admit_access().await?;
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        if self.historical.is_some() || self.witness_scope.is_none() || binding == [0; 32] {
            return Err(unavailable());
        }
        let (hold, _) = self.replay_parent_control("restore").await?.ok_or_else(unavailable)?;
        let (archive, _) = self.replay_parent_control("archive").await?.ok_or_else(unavailable)?;
        if hold != binding || archive.len() != 40 || self.replay_parent_control("released").await?.is_some() {
            return Err(unavailable());
        }
        self.check()?;
        Ok(archive)
    }

    /// A retained archive suffix is selected by the physical revision of
    /// this receipt. Always rediscover the full destination transaction key
    /// in the certified journal instead of trusting a caller timestamp.
    pub async fn replayed_parent_cut(
        &self, binding: [u8; 32], original: TransactionKey, scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<(OwnedForkCut, Timestamp)> {
        let archive = self.replay_parent_hold(binding, false).await?;
        let id = format!("native-transaction/{}", original.tx_id.0);
        let (body, destination) = self.replay_parent_control(&id).await?
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let logical = rtdb_tx::replayed_parent_logical_time(&body, binding, &archive, original)?;
        if destination <= original.system_time || destination >= self.present.system_time {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let mut cut = self.fork_cut(scope).await?;
        cut.basis = destination;
        cut.live = None;
        cut.publication = true;
        Ok((cut, logical))
    }

    /// Read a provisional world and its co-ordered parent proof under the
    /// same held restore binding.
    pub async fn replayed_world(
        &self, binding: [u8; 32], id: rtdb_types::Iid,
    ) -> Result<OwnedReplayWorld> {
        let archive = self.replay_parent_hold(binding, false).await?;
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let (frontier, _) = self.replay_parent_control("native-frontier.v1").await?
            .ok_or_else(unavailable)?;
        let source_cut = rtdb_tx::replayed_archive_cut(&frontier, binding, &archive)?;
        let (world, reservation) = self.world_registration(id).await?
            .filter(|(world, _)| world.id == id && !world.dropped && source_cut < world.until)
            .ok_or_else(unavailable)?;
        let proof = rtdb_tx::replayed_world_parent_locator(&world);
        let (body, _) = self.replay_parent_control(&proof).await?.ok_or_else(unavailable)?;
        let destination_parent = rtdb_tx::replayed_world_parent(&body, binding, &world)?;
        self.check()?;
        Ok(OwnedReplayWorld { world, source_cut, destination_parent, _reservation: reservation })
    }

    async fn replay_parent_control(&self, id: &str) -> Result<Option<(Vec<u8>, Timestamp)>> {
        use arrow_array::{Array, BinaryArray};
        use rtdb_types::{EntityId, TableRef};

        let point = rtdb_query::read_hosted_control_point(
            Arc::clone(&self.context), TableRef::new("public", "bj_rtdb_restore_hold"),
            &EntityId::String(id.to_owned()), true,
        ).await?;
        self.check()?;
        point.row.map(|(row, revision)| {
            let bytes = row.batch().column_by_name("binding")
                .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .filter(|bytes| bytes.is_valid(0) && bytes.value(0).len() <= 4096)
                .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
            Ok((bytes.value(0).to_vec(), revision))
        }).transpose()
    }

    pub async fn world_registration(&self, world: rtdb_types::Iid) -> Result<Option<(rtdb_types::OwnedWorld, Arc<rtdb_columnar::MemoryReservation>)>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let record = rtdb_query::read_owned_world(Arc::clone(&self.context), &self.owner, world).await?;
        self.check()?;
        Ok(record)
    }

    /// Reads the exact local shadow, retaining its revision even when that shadow is a tombstone.
    pub async fn world_application_clock(&self, world: rtdb_types::Iid) -> Result<Option<Timestamp>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let clock = rtdb_query::read_owned_world_clock(Arc::clone(&self.context), &self.owner, world).await?;
        self.check()?;
        Ok(clock)
    }

    pub async fn read_world_shadow(
        &self, world: rtdb_types::Iid, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        valid_at: Timestamp, observed: bool,
    ) -> Result<OwnedWorldShadowRead> {
        self.read_world_shadow_at(world, relation, id, valid_at, self.present.system_time, observed).await
    }

    async fn read_world_shadow_at(
        &self, world: rtdb_types::Iid, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        valid_at: Timestamp, system: Timestamp, observed: bool,
    ) -> Result<OwnedWorldShadowRead> {
        let valid = rtdb_types::Interval::at(valid_at)?;
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let context = if system == self.present.system_time { context }
            else { context.with_owned_system_cut(system, &context.cancellation)? };
        let root = relation;
        let relation = root.in_world(world)?;
        let mut row = None;
        let mut local_revision = None;
        if let Some(mut cursor) = rtdb_query::open_owned_point(
            Arc::clone(&context), &self.owner, &relation, id, Some(valid), None,
        ).await? {
            while let Some(batch) = cursor.next_batch().await? {
                if batch.batch().num_rows() == 0 { continue; }
                if row.is_some() || batch.batch().num_rows() != 1 {
                    return Err(rtdb_types::OwnedWorld::unavailable());
                }
                let revision = rtdb_query::owned_row_revision(batch.batch(), 0, id, &self.context.memory)?;
                if revision > system { return Err(rtdb_types::OwnedWorld::unavailable()); }
                local_revision = Some(revision);
                row = Some(batch);
            }
        }
        if let Some(revision) = local_revision {
            self.check_point_purge_floor(root, id, revision).await?;
        }
        context.check_cancelled()?;
        let witness = if let Some(scope) = self.witness_scope.filter(|_| observed) {
            let (registered, _charge) = self.world_registration(world).await?
                .filter(|(record, _)| !record.dropped && record.until > self.present.system_time)
                .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
            let database = self.context.catalog.database(&self.database)?;
            let [snapshot] = database.partitions.as_slice() else { return Err(rtdb_types::OwnedWorld::unavailable()); };
            let purge_frontier = snapshot.relation_event_frontier(&root.purge_table(&self.owner))?;
            Some(OwnedWorldWitness {
                world, pin: registered.pin, relation, purge_frontier, scope,
                observation: OwnedWorldObservation::Point { id: id.clone(), valid_at, revision: local_revision },
                until: self.witness_until.min(registered.until),
            })
        } else {
            None
        };
        Ok(OwnedWorldShadowRead { row, witness })
    }

    pub async fn open_world_shadow_boundaries(
        &self, world: rtdb_types::Iid, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        valid: rtdb_types::Interval,
    ) -> Result<Option<QueryCursor>> {
        let (admitted, _charge) = self.world_registration(world).await?
            .filter(|(world, _)| !world.dropped && world.until > self.present.system_time)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        rtdb_query::open_owned_valid_boundaries(
            self.relation_context(relation)?, &self.owner, &relation.in_world(admitted.id)?,
            id, valid, self.present.system_time,
        ).await
    }

    pub fn close(&self) { self.cancellation.cancel(); }
    pub fn check(&self) -> Result<()> { self.context.check_cancelled() }

    /// A release fence for the selected logical relations at this hold.
    pub fn relation_release_guard<'a>(
        &self, relations: impl IntoIterator<Item = &'a rtdb_types::OwnedRelation>,
    ) -> Result<Arc<dyn Fn() -> Result<()> + Send + Sync>> {
        self.relation_release_guard_for(relations, false)
    }

    fn relation_release_guard_for<'a>(
        &self, relations: impl IntoIterator<Item = &'a rtdb_types::OwnedRelation>, present_point: bool,
    ) -> Result<Arc<dyn Fn() -> Result<()> + Send + Sync>> {
        let database = self.retained_root_context().catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/read-purged", "The held cut has no single purge frontier"));
        };
        let obsolete = rtdb_types::OwnedRelation::obsolete_purge_table(&self.owner);
        if snapshot.contains_table(&obsolete) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/purge-history-reset", "Owner-wide purge history requires a reset before native held reads"));
        }
        let frontiers = relations.into_iter().map(|relation| {
            self.require_live_root_scope(relation)?;
            let root = relation.root_relation();
            let logical = root.as_ref().unwrap_or(relation);
            let lifetime = logical.lifetime_table(&self.owner);
            let governed = snapshot.contains_table(&lifetime);
            if governed && (!present_point || root.is_some() || self.historical.is_some()) {
                return Err(lifetime_read_unavailable());
            }
            let table = logical.purge_table(&self.owner);
            let expected = snapshot.relation_event_frontier(&table)?;
            let lifetime_frontier = governed.then(|| snapshot.relation_event_frontier(&lifetime)).transpose()?;
            Ok((table, expected, lifetime, lifetime_frontier))
        }).collect::<Result<Vec<_>>>()?;
        let charge = self.context.memory.reserve(frontiers.len() * 512)?;
        let node = self.node.clone();
        let database = self.database.clone();
        let placement = self.attachment.clone();
        let context = Arc::clone(&self.context);
        Ok(Arc::new(move || {
            let _charge = &charge;
            context.check_cancelled()?;
            let current = node.attached_database(&database, Some(&placement))?.snapshot();
            if current.contains_table(&obsolete) {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/purge-history-reset", "Owner-wide purge history requires a reset before native held reads"));
            }
            for (table, expected, lifetime, lifetime_frontier) in &frontiers {
                if current.contains_table(lifetime) != lifetime_frontier.is_some() {
                    return Err(lifetime_read_unavailable());
                }
                if let Some(frontier) = lifetime_frontier
                    && current.relation_event_frontier(lifetime)? != *frontier
                {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/lifetime-collected",
                        "The held relation's lifetime replay admission changed"));
                }
                if current.relation_event_frontier(table)? != *expected {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/read-purged", "The held relation was revoked by an ordered erase"));
                }
            }
            Ok(())
        }))
    }

    pub fn check_relation(&self, relation: &rtdb_types::OwnedRelation) -> Result<()> {
        self.check_relations([relation])
    }

    pub fn check_relations<'a>(&self, relations: impl IntoIterator<Item = &'a rtdb_types::OwnedRelation>) -> Result<()> {
        let guard = self.relation_release_guard(relations)?;
        guard()
    }

    fn guard_context<'a>(
        &self, context: Arc<QueryContext>, relations: impl IntoIterator<Item = &'a rtdb_types::OwnedRelation>,
    ) -> Result<Arc<QueryContext>> {
        let guard = self.relation_release_guard(relations)?;
        let context = context.with_owned_read_guard(guard);
        context.check_cancelled()?;
        Ok(context)
    }

    fn relation_context(&self, relation: &rtdb_types::OwnedRelation) -> Result<Arc<QueryContext>> {
        self.guard_context(Arc::clone(&self.context), [relation])
    }

    pub async fn open_point(&self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId) -> Result<Option<QueryCursor>> {
        self.open_point_at(relation, id, None).await
    }

    /// Reads the business-valid instant while retaining this session's original system cut.
    pub async fn open_point_at(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, valid_at: Option<Timestamp>,
    ) -> Result<Option<QueryCursor>> {
        self.admit_access().await?;
        self.require_scoped_relation(relation)?;
        if let Some(scope) = &self.historical {
            self.check_point_purge_floor(relation, id, scope.system_time).await?;
        }
        if valid_at.is_none() && relation.world().is_none()
            && rtdb_query::read_owned_relation_schema(Arc::clone(&self.context), &self.owner, relation)
                .await?.is_some_and(|(schema, _)| schema.requires_bounded_period())
        {
            return Err(rtdb_types::Anomaly::incorrect(
                "rtdb.owned/validity-policy", "A bounded-period point requires an explicit valid instant",
            ));
        }
        let lifetime = self.lifetime_point_duration(relation).await?;
        if lifetime.is_some() && valid_at.is_some_and(|valid| valid != self.present.system_time) {
            return Err(lifetime_read_unavailable());
        }
        let valid_at = if lifetime.is_some() { Some(self.present.system_time) } else { valid_at };
        let valid = valid_at.map(rtdb_types::Interval::at).transpose()?;
        let guard = self.relation_release_guard_for([relation], true)?;
        let context = Arc::clone(&self.context).with_owned_read_guard(guard);
        context.check_cancelled()?;
        match (lifetime, valid) {
            (Some(_), Some(valid)) => rtdb_query::open_owned_lifetime_point(context, &self.owner, relation, id, valid).await,
            _ => rtdb_query::open_owned_point(context, &self.owner, relation, id, valid, None).await,
        }
    }

    /// The declaration is immutable and present on the same held cut as
    /// its private relation fence.
    pub async fn lifetime_point_duration(&self, relation: &rtdb_types::OwnedRelation) -> Result<Option<i64>> {
        Ok(self.lifetime_point_policy(relation).await?.map(|floor| floor.duration))
    }

    pub async fn lifetime_point_policy(
        &self, relation: &rtdb_types::OwnedRelation,
    ) -> Result<Option<rtdb_types::OwnedLifetimeFloor>> {
        self.require_live_root_scope(relation)?;
        self.admit_access().await?;
        let database = self.retained_root_context().catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(lifetime_read_unavailable()); };
        let root = relation.root_relation();
        let marker = root.as_ref().unwrap_or(relation).lifetime_table(&self.owner);
        if !snapshot.contains_table(&marker) {
            return Ok(None);
        }
        if self.historical.is_some() || root.is_some() {
            return Err(lifetime_read_unavailable());
        }
        let schema = rtdb_query::read_owned_relation_schema(Arc::clone(&self.context), &self.owner, relation)
            .await?.and_then(|(schema, _)| schema.lifetime_micros())
            .ok_or_else(lifetime_read_unavailable)?;
        let floor = rtdb_query::read_owned_lifetime_floor(Arc::clone(&self.context), &self.owner, relation).await?;
        floor.admit_recorded_present(self.present.system_time)?;
        floor.check(schema, self.present.system_time)?;
        Ok(Some(floor))
    }

    /// Nominate one exact, already expired identity from a placed live hold.
    /// This authenticates discovery only. Replay retention authorizes removal.
    pub async fn nominate_expired_identity(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<(String, Timestamp, Timestamp)> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if self.witness_scope != Some(scope) || !relation.is_root_data() || self.node.now() >= self.witness_until {
            return Err(lifetime_read_unavailable());
        }
        let database = self.node.attached_database(&self.database, Some(&self.attachment))?;
        let AwaitAuthority::Live { codec, .. } = &database.await_authority else {
            return Err(lifetime_read_unavailable());
        };
        if codec.scope() != scope { return Err(lifetime_read_unavailable()); }

        let floor = self.lifetime_point_policy(relation).await?.ok_or_else(lifetime_read_unavailable)?;
        let guard = self.relation_release_guard_for([relation], true)?;
        let context = Arc::clone(&self.context).with_owned_read_guard(guard);
        let mut cursor = rtdb_query::open_owned_point(
            Arc::clone(&context), &self.owner, relation, id,
            Some(rtdb_types::Interval::ALL), Some(&["_id", "_system_from"]),
        ).await?.ok_or_else(lifetime_read_unavailable)?;
        let mut created = None;
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() {
                if created.is_some() { return Err(lifetime_read_unavailable()); }
                created = Some(rtdb_query::owned_row_revision(batch.batch(), row, id, &context.memory)?);
            }
        }
        let created = created.ok_or_else(lifetime_read_unavailable)?;
        let expiry = Timestamp(created.0.checked_add(floor.duration).ok_or_else(lifetime_read_unavailable)?);
        if expiry > self.present.system_time { return Err(lifetime_read_unavailable()); }

        let mut nomination = Vec::with_capacity(73);
        nomination.push(0xe1);
        nomination.extend_from_slice(&expiry_nomination_digest(scope, &self.owner, relation, id));
        for coordinate in [created.0, expiry.0, self.present.tx_id.0, self.present.system_time.0, self.witness_until.0] {
            nomination.extend_from_slice(&coordinate.to_be_bytes());
        }
        context.check_cancelled()?;
        if self.node.now() >= self.witness_until { return Err(lifetime_read_unavailable()); }
        Ok((format!("rtdblife1.{}", codec.seal(&nomination)?), created, expiry))
    }

    async fn resolve_expiry_nomination(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, nomination: &str,
        scope: rtdb_types::PlacedAttemptScope, deadline: Timestamp,
    ) -> Result<(Timestamp, Timestamp)> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let now = self.node.now();
        if self.historical.is_some() || self.witness_scope != Some(scope) || !relation.is_root_data()
            || deadline <= now || deadline > self.witness_until
        {
            return Err(lifetime_read_unavailable());
        }
        let token = nomination.strip_prefix("rtdblife1.").filter(|token| !token.is_empty() && token.len() <= 192)
            .ok_or_else(lifetime_read_unavailable)?;
        let database = self.node.attached_database(&self.database, Some(&self.attachment))?;
        let AwaitAuthority::Live { codec, .. } = &database.await_authority else {
            return Err(lifetime_read_unavailable());
        };
        if codec.scope() != scope { return Err(lifetime_read_unavailable()); }
        let decoded = codec.open(token).map_err(|_| lifetime_read_unavailable())?;
        let bytes: &[u8; 73] = decoded.as_slice().try_into().map_err(|_| lifetime_read_unavailable())?;
        let coordinate = |index: usize| i64::from_be_bytes(bytes[index..index + 8].try_into().expect("fixed nomination coordinate"));
        let created = Timestamp(coordinate(33));
        let expiry = Timestamp(coordinate(41));
        if bytes[0] != 0xe1 || bytes[1..33] != expiry_nomination_digest(scope, &self.owner, relation, id)
            || coordinate(49) != self.present.tx_id.0 || coordinate(57) != self.present.system_time.0
            || coordinate(65) != self.witness_until.0 || expiry > self.present.system_time
        {
            return Err(lifetime_read_unavailable());
        }
        let floor = self.lifetime_point_policy(relation).await?.ok_or_else(lifetime_read_unavailable)?;
        if created.0.checked_add(floor.duration) != Some(expiry.0) {
            return Err(lifetime_read_unavailable());
        }
        self.check_live_point_relation(relation)?;
        Ok((created, expiry))
    }

    pub fn check_live_point_relation(&self, relation: &rtdb_types::OwnedRelation) -> Result<()> {
        self.relation_release_guard_for([relation], true)?()
    }

    /// Reads a business-valid point at an earlier system time certified by its journal.
    pub async fn open_historical_point(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        system: Timestamp, valid: Timestamp,
    ) -> Result<Option<QueryCursor>> {
        self.admit_read().await?;
        let _ = self.certify_historical_cut(relation, system).await?;
        self.check_point_purge_floor(relation, id, system).await?;
        self.check()?;
        let valid = rtdb_types::Interval::new(valid, valid.next()?)?;
        rtdb_query::open_owned_point_at_system(self.relation_context(relation)?, &self.owner, relation, id, valid, system).await
    }

    /// Prevents returning bytes from an older archive or historical cut after an ordered erase.
    pub async fn check_point_purge_floor(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, system: Timestamp,
    ) -> Result<()> {
        self.point_purge_guard(relation, id, system, false).await.map(|_| ())
    }

    /// A retained point applies the live relation fence before reopening its older data.
    pub async fn parent_point_release_guard(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, system: Timestamp,
    ) -> Result<Arc<dyn Fn() -> Result<()> + Send + Sync>> {
        self.point_purge_guard(relation, id, system, true).await
    }

    async fn point_purge_guard(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, system: Timestamp,
        present_point: bool,
    ) -> Result<Arc<dyn Fn() -> Result<()> + Send + Sync>> {
        self.admit_access().await?;
        let guard = self.relation_release_guard_for([relation], present_point)?;
        let context = Arc::clone(self.retained_root_context()).with_owned_read_guard(Arc::clone(&guard));
        if rtdb_query::owned_purge_floor(Arc::clone(&context), &self.owner, relation, id)
            .await?.is_some_and(|floor| system < floor)
        {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/read-purged", "The requested cut precedes its ordered erase"));
        }
        context.check_cancelled()?;
        Ok(guard)
    }

    /// Reads the winning valid slices at an admitted system cut of this root hold.
    pub async fn open_valid_during_observed<'a>(
        &'a self,
        relation: &'a rtdb_types::OwnedRelation,
        id: &rtdb_types::EntityId,
        valid: rtdb_types::Interval,
        system: Timestamp,
    ) -> Result<OwnedValidHistory<'a>> {
        let at_head = self.admit_root_system(relation, system).await?;
        if !at_head {
            self.check_point_purge_floor(relation, id, system).await?;
        }
        let cursor = rtdb_query::open_owned_valid_during_at(
            self.relation_context(relation)?, &self.owner, relation, id, valid, system,
        ).await?;
        self.check_relation(relation)?;
        Ok(OwnedValidHistory {
            held: self, relation, cursor, at_head, complete: false, failed: false,
        })
    }

    /// Computes the visible corpus of one covered clear-text path at this read's cut.
    #[cfg(feature = "search")]
    pub async fn search_corpus(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation, query: &str,
    ) -> Result<rtdb_text::Corpus> {
        self.admit_read().await?;
        let work = rtdb_search::owned_corpus(self.relation_context(relation)?, &self.owner, relation, path, query);
        let corpus = rtdb_storage::IoRegion::leased(self.context.catalog.read_lease(), work).await?;
        self.check_relation(relation)?;
        Ok(corpus)
    }

    /// Ranks a covered clear-text query at this read's cut and validates its full identities.
    #[cfg(feature = "search")]
    pub async fn ranked_search(
        &self, partitions: &[(rtdb_types::OwnedRelation, rtdb_types::OwnedRelation)], query: &str, limit: usize, observe: bool,
    ) -> Result<(Vec<rtdb_search::OwnedSearchHit>, Vec<OwnedRangeWitness>)> {
        let (page, witnesses) = self.ranked_search_page(partitions, query, 0, limit, observe).await?;
        Ok((page.hits, witnesses))
    }

    /// Ranks an exact bounded window and observes the same held population once.
    #[cfg(feature = "search")]
    pub async fn ranked_search_page(
        &self, partitions: &[(rtdb_types::OwnedRelation, rtdb_types::OwnedRelation)], query: &str,
        offset: usize, limit: usize, observe: bool,
    ) -> Result<(rtdb_search::OwnedSearchPage, Vec<OwnedRangeWitness>)> {
        self.ranked_search_with_overlay(partitions, query, offset, limit, observe, None, None).await
    }

    #[cfg(feature = "search")]
    pub async fn ranked_search_with_overlay(
        &self, partitions: &[(rtdb_types::OwnedRelation, rtdb_types::OwnedRelation)], query: &str,
        offset: usize, limit: usize, observe: bool, valid: Option<Timestamp>, overlay: Option<rtdb_search::HeldTextCorpus>,
    ) -> Result<(rtdb_search::OwnedSearchPage, Vec<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let work = rtdb_search::owned_ranked_with_overlay(self.guard_context(Arc::clone(&self.context), partitions.iter().map(|(relation, _)| relation))?, &self.owner, partitions, query, offset, limit, valid, overlay);
        let hits = rtdb_storage::IoRegion::leased(self.context.catalog.read_lease(), work).await?;
        let mut witnesses = Vec::with_capacity(partitions.len());
        if observe {
            for (relation, _) in partitions {
                if let Some(witness) = self.relation_witness(relation)? {
                    witnesses.push(witness);
                }
            }
        }
        self.check_relations(partitions.iter().map(|(relation, _)| relation))?;
        Ok((hits, witnesses))
    }

    /// Observe the retained parent and, for a held world, the complete local overlay relation.
    pub async fn world_relation_witnesses<'a>(
        &self, world: rtdb_types::Iid, relations: impl IntoIterator<Item = &'a rtdb_types::OwnedRelation>,
    ) -> Result<Vec<OwnedWorldWitness>> {
        self.world_relation_checks(world, relations, true).await
    }

    /// Observe only the parent pin and the relation's purge frontier.
    pub async fn world_parent_witness(
        &self, world: rtdb_types::Iid, relation: &rtdb_types::OwnedRelation,
    ) -> Result<OwnedWorldWitness> {
        self.world_relation_checks(world, [relation], false).await?
            .pop().ok_or_else(rtdb_types::OwnedWorld::unavailable)
    }

    async fn world_relation_checks<'a>(
        &self, world: rtdb_types::Iid, relations: impl IntoIterator<Item = &'a rtdb_types::OwnedRelation>,
        include_overlay: bool,
    ) -> Result<Vec<OwnedWorldWitness>> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let (registered, _charge) = self.world_registration(world).await?
            .filter(|(record, _)| !record.dropped && record.until > self.present.system_time)
            .ok_or_else(unavailable)?;
        let scope = self.witness_scope.ok_or_else(unavailable)?;
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(unavailable()); };
        if snapshot.contains_table(&rtdb_types::OwnedRelation::obsolete_purge_table(&self.owner)) {
            return Err(unavailable());
        }
        let mut selected = Vec::new();
        for relation in relations {
            self.check_relation(relation)?;
            if selected.len() == 32 { return Err(unavailable()); }
            let local = relation.in_world(world)?;
            let local_frontier = (include_overlay && registered.mode.is_writable())
                .then(|| snapshot.relation_event_frontier(&local.table(&self.owner))).transpose()?;
            selected.push(OwnedWorldWitness {
                world, pin: registered.pin, relation: local, scope,
                observation: OwnedWorldObservation::Relation { local_frontier },
                purge_frontier: snapshot.relation_event_frontier(&relation.purge_table(&self.owner))?,
                until: self.witness_until.min(registered.until),
            });
        }
        Ok(selected)
    }

    pub async fn range_world_overlay(
        &self, world: rtdb_types::Iid, relation: &rtdb_types::OwnedRelation,
        path: &rtdb_types::OwnedRelation, request: crate::OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<crate::OwnedRangePage> {
        self.range_page(&relation.in_world(world)?, path, crate::ordered_index::RowBounds::Compound(request), Some(valid)).await
    }

    /// Reads one covered page from the named ordered path at this held cut.
    pub async fn range(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        lower: Option<&Value>, upper: Option<&Value>, limit: usize,
    ) -> Result<crate::OwnedRangePage> {
        self.range_page(relation, path, crate::ordered_index::RowBounds::Leading { lower, upper, limit }, None).await
    }

    /// Reads in either direction with exclusive full-key continuation at this held cut.
    pub async fn range_page_by_key(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedRangeRequest<'_>,
    ) -> Result<crate::OwnedRangePage> {
        self.range_page(relation, path, crate::ordered_index::RowBounds::Compound(request), None).await
    }

    /// Issues evidence only after the covering query completed, including a covered empty page.
    pub async fn range_page_by_key_observed(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedRangeRequest<'_>,
    ) -> Result<(crate::OwnedRangePage, Option<OwnedRangeWitness>)> {
        self.range_page_by_key_at_observed(relation, path, request, None).await
    }

    pub async fn range_page_by_key_at_observed(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedRangeRequest<'_>, valid_at: Option<Timestamp>,
    ) -> Result<(crate::OwnedRangePage, Option<OwnedRangeWitness>)> {
        let (lower, upper, after) = (request.lower, request.upper, request.after);
        let page = self.range_page(relation, path, crate::ordered_index::RowBounds::Compound(request), valid_at).await?;
        let mut evidence = self.relation_witness(relation)?;
        if let (Some(evidence), Some(valid_at)) = (evidence.as_mut(), valid_at.filter(|_| after.is_none())) {
            evidence.fixed = self.fixed_range_evidence(relation, path, evidence.frontier, valid_at, lower, upper)
                .await.unwrap_or(None).map(Arc::new);
        }
        self.check_relation(relation)?;
        Ok((page, evidence))
    }

    /// A live lifetime page reuses the covered path at held present, then
    /// hydrates each nominated identity to certify its release horizon.
    pub async fn range_page_by_key_lifetime(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedRangeRequest<'_>, valid_at: Option<Timestamp>,
    ) -> Result<(crate::OwnedRangePage, Option<OwnedRangeWitness>, Option<Timestamp>)> {
        let Some(duration) = self.lifetime_point_duration(relation).await? else {
            return self.range_page_by_key_at_observed(relation, path, request, valid_at)
                .await.map(|(page, witness)| (page, witness, None));
        };
        self.admit_read().await?;
        let present = self.present.system_time;
        if valid_at.is_some_and(|at| at != present) {
            return Err(lifetime_read_unavailable());
        }
        let valid = rtdb_types::Interval::at(present)?;
        let guard = self.relation_release_guard_for([relation], true)?;
        let context = Arc::clone(&self.context).with_owned_read_guard(guard);
        context.check_cancelled()?;
        let page = crate::ordered_index::owned_range(Arc::clone(&context), &self.owner, relation, path,
            crate::ordered_index::RowBounds::Compound(request), Some(present)).await?;
        let mut expiry: Option<Timestamp> = None;
        if let Some(batch) = &page.batch {
            let batch = batch.batch();
            let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(lifetime_read_unavailable)?;
            for row in 0..batch.num_rows() {
                context.check_cancelled()?;
                let ids = batch.column(column).as_ref();
                let size = rtdb_columnar::estimate_read_value_bytes(field, ids, row)?;
                let _identity = context.memory.reserve(size.saturating_add(4096))?;
                let id = read_value(field, ids, row)?.entity_id()?;
                let mut point = rtdb_query::open_owned_point(
                    Arc::clone(&context), &self.owner, relation, &id, Some(valid), Some(&["_id", "_system_from", "_valid_from", "_valid_to"]),
                ).await?.ok_or_else(lifetime_read_unavailable)?;
                let mut until = None;
                while let Some(part) = point.next_batch().await? {
                    for index in 0..part.batch().num_rows() {
                        if until.is_some() { return Err(lifetime_read_unavailable()); }
                        let revision = rtdb_query::owned_row_revision(part.batch(), index, &id, &context.memory)?;
                        let (_, cutoff) = rtdb_query::owned_row_lifetime(part.batch(), index, revision, duration)?;
                        if revision > present || cutoff <= present { return Err(lifetime_read_unavailable()); }
                        until = Some(cutoff);
                    }
                }
                let until = until.ok_or_else(lifetime_read_unavailable)?;
                expiry = Some(expiry.map_or(until, |prior| prior.min(until)));
            }
        }
        self.check_lifetime_range_release(relation, expiry)?;
        let evidence = self.relation_witness_for(relation, true, expiry)?;
        Ok((page, evidence, expiry))
    }

    pub fn check_lifetime_range_release(
        &self, relation: &rtdb_types::OwnedRelation, until: Option<Timestamp>,
    ) -> Result<()> {
        self.check_live_point_relation(relation)?;
        if until.is_some_and(|until| until <= self.node.now()) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/lifetime-expired",
                "The held range reached its creation lifetime"));
        }
        Ok(())
    }

    fn relation_witness(&self, relation: &rtdb_types::OwnedRelation) -> Result<Option<OwnedRangeWitness>> {
        self.relation_witness_for(relation, false, None)
    }

    fn relation_witness_for(
        &self, relation: &rtdb_types::OwnedRelation, lifetime: bool, expiry: Option<Timestamp>,
    ) -> Result<Option<OwnedRangeWitness>> {
        let guard = self.relation_release_guard_for([relation], lifetime)?;
        guard()?;
        let Some(scope) = self.witness_scope else { return Ok(None); };
        let database = self.context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else {
            return Err(Anomaly::incorrect("rtdb.owned/range-witness", "An owned range witness requires one held partition"));
        };
        let frontier = snapshot.relation_event_frontier(&relation.table(&self.owner))?;
        if snapshot.tx_basis() != Some(self.present) {
            return Err(Anomaly::incorrect("rtdb.owned/range-witness", "The exact owned range basis is unavailable"));
        }
        guard()?;
        Ok(Some(OwnedRangeWitness {
            relation: relation.clone(), basis: self.present, scope, frontier,
            until: expiry.map_or(self.witness_until, |expiry| expiry.min(self.witness_until)), fixed: None,
        }))
    }

    async fn fixed_range_evidence(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        frontier: i64, valid_at: Timestamp,
        lower: Option<&rtdb_types::StructFields<Value>>, upper: Option<&rtdb_types::StructFields<Value>>,
    ) -> Result<Option<rtdb_types::FixedRangeEvidence>> {
        use rtdb_types::{FixedRangeEvidence, FixedRangeKey};
        let rule = rtdb_query::read_owned_fixed_range_rule(Arc::clone(&self.context), &self.owner, relation).await?;
        let Some(rule) = rule.filter(|rule| rule.valid_at == valid_at && rule.path == path.name()) else { return Ok(None); };
        let revision = self.covered_range_anchor(relation, frontier).await?;
        let lower = lower.map(|bound| FixedRangeKey::from_bound(bound, &rule.column)).transpose()?;
        let upper = upper.map(|bound| FixedRangeKey::from_bound(bound, &rule.column)).transpose()?;
        Ok(Some(FixedRangeEvidence { rule, revision, lower, upper }))
    }

    /// Counts a covered full-key range at this held system cut and an explicit valid instant.
    pub async fn count_range(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedCountRequest<'_>,
    ) -> Result<crate::OwnedRangeCount> {
        self.admit_read().await?;
        let lifetime = self.lifetime_point_duration(relation).await?;
        let context = if lifetime.is_some() {
            if request.valid_at != self.present.system_time {
                return Err(lifetime_read_unavailable());
            }
            Arc::clone(&self.context).with_owned_read_guard(self.relation_release_guard_for([relation], true)?)
        } else {
            self.relation_context(relation)?
        };
        let result = crate::ordered_index::owned_count_at(context, &self.owner, relation, path, request, lifetime).await?;
        self.check_lifetime_range_release(relation, result.expiry)?;
        Ok(result)
    }

    pub async fn count_range_observed(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedCountRequest<'_>,
    ) -> Result<(crate::OwnedRangeCount, Option<OwnedRangeWitness>)> {
        let counted = self.count_range(relation, path, request).await?;
        let evidence = self.relation_witness_for(relation, true, counted.expiry)?;
        Ok((counted, evidence))
    }

    /// Read an exact covered sum of the path's last signed Int64 field before the identity.
    pub async fn measure_range_observed(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: crate::OwnedCountRequest<'_>,
    ) -> Result<(crate::OwnedRangeCount, Option<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let lifetime = self.lifetime_point_duration(relation).await?;
        if lifetime.is_some() {
            return Err(crate::ordered_index::uncovered_owned_range());
        }
        let context = self.relation_context(relation)?;
        let counted = crate::ordered_index::owned_total_at(
            context, &self.owner, relation, path, request, None, true,
        ).await?;
        let evidence = self.relation_witness_for(relation, true, None)?;
        Ok((counted, evidence))
    }

    /// Reads a covered ordered page at an admitted historical Root cut.
    pub async fn range_page_by_key_at_system(
        &self,
        relation: &rtdb_types::OwnedRelation,
        path: &rtdb_types::OwnedRelation,
        request: crate::OwnedRangeRequest<'_>,
        system: Timestamp,
        valid: Timestamp,
    ) -> Result<crate::OwnedRangePage> {
        valid.next()?;
        let context = self.ordered_root_context(relation, system).await?;
        let page = crate::ordered_index::owned_range(
            Arc::clone(&context), &self.owner, relation, path,
            crate::ordered_index::RowBounds::Compound(request), Some(valid),
        ).await?;
        let Some((column, field)) = page.schema.column_with_name("_id") else {
            return Err(historical_owned_read::unavailable());
        };
        if let Some(batch) = &page.batch {
            let array = batch.batch().column(column).as_ref();
            for row in 0..array.len() {
                let bytes = rtdb_columnar::estimate_read_value_bytes(field, array, row)?;
                let _charge = context.memory.reserve(bytes.saturating_add(4096))?;
                let id = rtdb_columnar::read_execution_value(field, array, row)?.entity_id()?;
                self.check_point_purge_floor(relation, &id, system).await?;
            }
        }
        self.check_relation(relation)?;
        Ok(page)
    }

    /// Counts a covered historical Root range using the erase-filtered index roots.
    pub async fn count_range_at_system(
        &self,
        relation: &rtdb_types::OwnedRelation,
        path: &rtdb_types::OwnedRelation,
        request: crate::OwnedCountRequest<'_>,
        system: Timestamp,
    ) -> Result<crate::OwnedRangeCount> {
        let context = self.ordered_root_context(relation, system).await?;
        let result = crate::ordered_index::owned_count(context, &self.owner, relation, path, request).await?;
        self.check_relation(relation)?;
        Ok(result)
    }

    /// Measures a covered historical Root range under the same retained cut as count.
    pub async fn measure_range_at_system(
        &self,
        relation: &rtdb_types::OwnedRelation,
        path: &rtdb_types::OwnedRelation,
        request: crate::OwnedCountRequest<'_>,
        system: Timestamp,
    ) -> Result<crate::OwnedRangeCount> {
        let context = self.ordered_root_context(relation, system).await?;
        let result = crate::ordered_index::measure_owned_cut(context, &self.owner, relation, path, request).await?;
        self.check_relation(relation)?;
        Ok(result)
    }

    async fn ordered_root_context(
        &self, relation: &rtdb_types::OwnedRelation, system: Timestamp,
    ) -> Result<Arc<QueryContext>> {
        rtdb_types::OwnedRelation::new(relation.name())?;
        self.admit_root_system(relation, system).await?;
        let context = self.relation_context(relation)?;
        if context.current_time.as_micros()? == system {
            Ok(context)
        } else {
            context.with_owned_system_cut(system, &context.cancellation)
        }
    }

    async fn range_page(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        bounds: crate::ordered_index::RowBounds<'_>, valid_at: Option<Timestamp>,
    ) -> Result<crate::OwnedRangePage> {
        self.admit_read().await?;
        let result = crate::ordered_index::owned_range(self.relation_context(relation)?, &self.owner, relation, path, bounds, valid_at).await?;
        self.check_relation(relation)?;
        Ok(result)
    }

    async fn admit_read(&self) -> Result<()> {
        if self.historical.is_some() { return Err(historical_owned_read::unavailable()); }
        self.admit_access().await
    }

    async fn admit_root_system(&self, relation: &rtdb_types::OwnedRelation, system: Timestamp) -> Result<bool> {
        self.admit_access().await?;
        let at_head = self.historical.is_none() && system == self.present.system_time;
        self.require_scoped_relation(relation)?;
        if let Some(scope) = &self.historical {
            if system != scope.system_time {
                return Err(historical_owned_read::unavailable());
            }
        } else if !at_head {
            self.certify_historical_cut(relation, system).await?;
        }
        Ok(at_head)
    }

    fn retained_root_context(&self) -> &Arc<QueryContext> {
        self.historical.as_ref().map_or(&self.context, |scope| &scope.root_context)
    }

    fn require_live_root_scope(&self, relation: &rtdb_types::OwnedRelation) -> Result<()> {
        if self.scoped_root.as_ref().is_some_and(|scope| !scope.contains(relation)) {
            return Err(scoped_root_denied());
        }
        Ok(())
    }

    fn require_scoped_relation(&self, relation: &rtdb_types::OwnedRelation) -> Result<()> {
        self.require_live_root_scope(relation)?;
        if self.historical.as_ref().is_some_and(|scope| !scope.relations.contains(relation)) {
            return Err(historical_owned_read::unavailable());
        }
        Ok(())
    }

    async fn admit_access(&self) -> Result<()> {
        self.check()?;
        self.node.admit(&self.database, DatabaseAccess::Read, self.identity.as_ref(), &self.cancellation).await?;
        if self.identity.user() != Some(self.owner.as_str()) {
            return Err(Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/read-owner", "The captured read belongs to another authenticated principal"));
        }
        self.check()
    }
}

fn scoped_root_denied() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/read-scope",
        "The held Root read does not admit the requested relation or operation")
}

fn lifetime_read_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/lifetime-uncovered",
        "The owned lifetime requires a point at the original store-issued present")
}

impl Drop for OwnedReadSession {
    fn drop(&mut self) { self.close(); }
}

pub struct OwnedWorldShadowRead {
    pub row: Option<rtdb_columnar::OwnedBatch>,
    pub witness: Option<OwnedWorldWitness>,
}

#[derive(Clone)]
pub struct OwnedWorldWitness {
    world: rtdb_types::Iid,
    pin: rtdb_types::Iid,
    relation: rtdb_types::OwnedRelation,
    observation: OwnedWorldObservation,
    scope: rtdb_types::PlacedAttemptScope,
    until: Timestamp,
    purge_frontier: i64,
}

#[derive(Clone)]
enum OwnedWorldObservation {
    Point { id: rtdb_types::EntityId, valid_at: Timestamp, revision: Option<Timestamp> },
    Relation { local_frontier: Option<i64> },
}

impl OwnedWorldWitness {
    pub fn purge_frontier(&self) -> i64 { self.purge_frontier }

    pub fn is_absent_point(&self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId) -> bool {
        self.relation.root_relation().as_ref() == Some(relation)
            && matches!(&self.observation, OwnedWorldObservation::Point { id: observed, revision: None, .. } if observed == id)
    }

    pub fn allocation_bound(&self) -> usize {
        use rtdb_types::EntityId;
        let point = match &self.observation {
            OwnedWorldObservation::Point { id, .. } => Some(id),
            OwnedWorldObservation::Relation { .. } => None,
        };
        let bytes = match point {
            Some(EntityId::String(id)) => id.len(),
            Some(EntityId::Keyword(id)) => id.as_str().len(),
            _ => 0,
        };
        bytes.saturating_add(self.relation.name().len()).saturating_add(1024)
    }
}

pub struct OwnedForkCut {
    basis: Timestamp,
    live: Option<TransactionKey>,
    issued_on: TransactionKey,
    publication: bool,
    renewal: bool,
    until: Timestamp,
    scope: rtdb_types::PlacedAttemptScope,
}

impl OwnedForkCut {
    pub fn basis(&self) -> Timestamp { self.basis }
    pub fn live(&self) -> Option<TransactionKey> { self.live }
    pub fn is_publication(&self) -> bool { self.publication }
    pub fn is_renewal(&self) -> bool { self.renewal }
    pub fn matches_basis(&self, basis: TransactionKey) -> bool {
        self.basis == basis.system_time && self.live.is_none_or(|live| live == basis)
            && basis.tx_id <= self.issued_on.tx_id
            && ((basis.tx_id == self.issued_on.tx_id) == (basis.system_time == self.issued_on.system_time))
    }
    pub fn until(&self) -> Timestamp { self.until }
    pub fn scope(&self) -> rtdb_types::PlacedAttemptScope { self.scope }
}

#[derive(Clone, Copy)]
pub enum WorldClockRequest {
    Retained,
    Held { start: Option<Timestamp> },
    Proposal,
}

pub struct OwnedWorldFork {
    pub world: rtdb_types::Iid,
    pub pin: rtdb_types::Iid,
    pub previous: Option<rtdb_types::Iid>,
    pub sibling: Option<rtdb_types::Iid>,
    pub cut: OwnedForkCut,
    pub requested: WorldClockRequest,
    pub deadline: Timestamp,
    pub attempt: String,
    pub fingerprint: [u8; 32],
    pub controls: Vec<rtdb_tx::HostedControlWrite>,
    pub root: Option<OwnedRootSection>,
}

impl OwnedWorldFork {
    pub fn shortens_proposal(&self) -> bool {
        matches!(self.requested, WorldClockRequest::Proposal) && self.cut.is_renewal()
            && self.previous.is_some()
            && self.root.as_ref().and_then(|root| root.publication.as_ref())
                .is_some_and(|publication| matches!(publication.writes.as_slice(), [TxOp::OwnedDelete { .. }]))
    }
}


#[derive(Default)]
pub struct OwnedRootSection {
    pub proposal_overlay: bool,
    pub proposal_land: bool,
    pub proposal_close: bool,
    pub checks: Vec<TxOp>,
    pub publication: Option<OwnedRootPublication>,
}

pub struct OwnedRootPublication {
    pub attempt: String,
    pub writes: Vec<TxOp>,
}

pub struct OwnedWorldSection {
    pub world: rtdb_types::Iid,
    pub parent_pin: rtdb_types::Iid,
    pub parent_basis: TransactionKey,
    pub checks: Vec<OwnedWorldWitness>,
    pub attempt: String,
    pub expected_clock: Option<Timestamp>,
    pub effect: OwnedWorldEffect,
}

pub enum OwnedWorldEffect {
    SettleProposal,
    Overlay(Vec<OwnedWorldWrite>),
    Clock(Timestamp),
    Drop,
}

pub struct OwnedWorldWrite {
    pub relation: rtdb_types::OwnedRelation,
    pub id: rtdb_types::EntityId,
    pub shadow: rtdb_types::WorldShadow,
    pub create_only: bool,
    pub valid: Option<rtdb_types::Interval>,
}

fn expiry_nomination_digest(
    scope: rtdb_types::PlacedAttemptScope, owner: &str,
    relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let handle = id.handle_bytes();
    let mut digest = Sha256::new();
    digest.update(b"rtdb.expiry-nomination.v1\0");
    digest.update(scope.encode());
    for input in [owner.as_bytes(), relation.name().as_bytes(), handle.as_slice()] {
        digest.update((input.len() as u64).to_be_bytes());
        digest.update(input);
    }
    digest.finalize().into()
}

pub struct NativeSession { pub(crate) state: Session, pub(crate) service: Arc<NativeSessionService> }
impl NativeSession {
    pub fn track_replica_read_session(&self) -> Result<impl Send + use<>> {
        Ok(self.service.database()?.track_replica_read_session())
    }
    pub fn state(&self) -> &Session { &self.state }
    pub fn state_mut(&mut self) -> &mut Session { &mut self.state }
    /// Identifies the placed replica history of this authenticated attachment.
    pub fn placed_history(&self) -> Result<Option<rtdb_types::PlacedAttemptScope>> {
        Ok(self.service.database()?.await_authority.codec().map(|codec| codec.scope()))
    }

    /// Apply a hosted nomination against the original held identity. The
    /// resolver decides eligibility and orders the relation replay floor.
    pub async fn sweep_owned_expired(
        &mut self, held: &OwnedReadSession, relation: rtdb_types::OwnedRelation,
        id: rtdb_types::EntityId, nomination: &str, deadline: Timestamp,
    ) -> Result<TransactionResult> {
        let scope = self.placed_history()?.ok_or_else(lifetime_read_unavailable)?;
        if self.state.user() != Some(held.owner.as_str())
            || !held.attachment.ptr_eq(&Arc::downgrade(&self.service.database()?))
        {
            return Err(lifetime_read_unavailable());
        }
        let (created, expiry) = held.resolve_expiry_nomination(&relation, &id, nomination, scope, deadline).await?;
        self.state.execute_tx(vec![
            TxOp::OwnedDeadline { until: deadline },
            TxOp::OwnedSweepExpired { relation, id, created, expiry, scope },
        ], Default::default()).await
    }

    pub async fn register_owned_world_checked(
        &mut self, record: &rtdb_types::OwnedWorld, fork: OwnedWorldFork,
    ) -> Result<TransactionResult> {
        let until = fork.deadline.min(fork.cut.until).min(record.until);
        let inherited = record.inherited_clock();
        let requested = match fork.requested {
            WorldClockRequest::Retained => rtdb_types::WorldClockMode::Retained,
            WorldClockRequest::Held { start } => rtdb_types::WorldClockMode::Held { start: start.unwrap_or(inherited) },
            WorldClockRequest::Proposal => rtdb_types::WorldClockMode::Proposal,
        };
        use rtdb_tx::OwnedForkRoot;
        let proposal_publication = requested == rtdb_types::WorldClockMode::Proposal
            && fork.cut.is_publication() && fork.root.is_some();
        let proposal_hold = requested == rtdb_types::WorldClockMode::Proposal
            && fork.cut.is_renewal() && fork.previous.is_some();
        let root = match &fork.root {
            Some(_) if fork.shortens_proposal() => OwnedForkRoot::ProposalShorten,
            Some(section) if proposal_hold && section.publication.is_some() => OwnedForkRoot::ProposalHold,
            Some(section) if section.publication.is_some() && fork.sibling.is_some() && fork.cut.is_renewal() =>
                OwnedForkRoot::WritesSibling,
            Some(section) if section.publication.is_some() && fork.previous.is_some() && fork.cut.is_renewal() =>
                OwnedForkRoot::WritesRenewal,
            Some(section) if section.publication.is_some() && fork.previous.is_some() => OwnedForkRoot::WritesAdvance,
            Some(section) if proposal_publication && section.publication.is_some() => OwnedForkRoot::ProposalWrites,
            Some(section) if section.publication.is_some() => OwnedForkRoot::Writes,
            Some(_) if proposal_publication => OwnedForkRoot::ProposalControls,
            Some(_) => OwnedForkRoot::Controls,
            None => OwnedForkRoot::None,
        };
        let requires_live_tip = record.initial_clock() > inherited
            || (requested == rtdb_types::WorldClockMode::Proposal && !proposal_publication && !proposal_hold);
        let requires_tip = requires_live_tip || proposal_publication
            || (root != OwnedForkRoot::None && !fork.cut.is_renewal()
                && record.parent_publication == Some(record.basis.system_time));
        let effects = fork.controls.len()
            .saturating_add(fork.root.as_ref().map_or(0, |section| section.checks.len()))
            .saturating_add(fork.root.as_ref().and_then(|section| section.publication.as_ref()).map_or(0, |root| root.writes.len() + 1))
            .saturating_add(3 + usize::from(requires_tip));
        if record.id != fork.world || record.pin != fork.pin || !fork.cut.matches_basis(record.basis)
            || ((fork.previous.is_none() || fork.sibling.is_some()) != (record.pin == record.id)) || fork.controls.is_empty()
            || fork.previous.is_some_and(|pin| pin == record.pin || (record.mode.is_writable() && !proposal_hold))
            || record.mode != requested
            || (requires_live_tip && fork.cut.live.is_none())
            || (fork.cut.is_publication() && (root == OwnedForkRoot::None
                || record.parent_publication != Some(fork.cut.basis())))
            || effects > 4096 || !fork.controls.iter().any(rtdb_tx::HostedControlWrite::is_check)
            || (root == OwnedForkRoot::None && fork.controls.iter().any(|control| !control.is_check()))
            || (matches!(root, OwnedForkRoot::Controls | OwnedForkRoot::ProposalControls | OwnedForkRoot::ProposalWrites)
                && fork.controls.iter().all(rtdb_tx::HostedControlWrite::is_check))
            || (root == OwnedForkRoot::WritesAdvance && !fork.cut.is_publication())
            || (fork.cut.is_renewal() && !matches!(root, OwnedForkRoot::WritesRenewal | OwnedForkRoot::WritesSibling | OwnedForkRoot::ProposalHold | OwnedForkRoot::ProposalShorten))
            || fork.sibling.is_some_and(|sibling| sibling == record.id || fork.previous.is_none()
                || root != OwnedForkRoot::WritesSibling)
            || (root != OwnedForkRoot::None && ((fork.previous.is_some() && !matches!(root, OwnedForkRoot::WritesAdvance | OwnedForkRoot::WritesRenewal | OwnedForkRoot::WritesSibling | OwnedForkRoot::ProposalHold | OwnedForkRoot::ProposalShorten))
                || (requested != rtdb_types::WorldClockMode::Retained && !proposal_publication && !proposal_hold)
                || (fork.cut.live.is_none() && !fork.cut.is_publication())))
            || fork.root.iter().flat_map(|section| &section.checks).any(|check| match check {
                TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. } =>
                    !relation.is_root_data(),
                _ => true,
            })
            || fork.root.as_ref().and_then(|section| section.publication.as_ref()).is_some_and(|root| {
                root.attempt != fork.attempt || root.writes.is_empty() || root.writes.iter().any(|write| match write {
                    TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                    | TxOp::OwnedDelete { relation, .. } => !relation.is_root_data(),
                    _ => true,
                })
            })
            || self.placed_history()? != Some(fork.cut.scope)
        {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let _operations = self.service.node.memory().reserve(effects * (std::mem::size_of::<TxOp>() + 512))?;
        let mut ops = Vec::with_capacity(effects);
        ops.push(TxOp::OwnedDeadline { until });
        if let Some(section) = fork.root {
            ops.extend(section.checks);
            ops.extend(section.publication.into_iter().flat_map(|root| root.writes));
        }
        ops.extend(fork.controls.into_iter().map(TxOp::HostedControl));
        if requires_tip {
            ops.push(TxOp::OwnedCheck {
                relation: rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?,
                id: rtdb_types::EntityId::String("head".into()),
                valid_at: record.basis.system_time,
                expected: record.parent_publication,
            });
        }
        ops.push(TxOp::OwnedForkWorld { record: record.record(), previous: fork.previous, sibling: fork.sibling,
            attempt: fork.attempt, fingerprint: fork.fingerprint, root });
        self.state.execute_tx(ops, Default::default()).await
    }

    pub async fn validate_owned_world(
        &mut self, record: &rtdb_types::OwnedWorld, checks: Vec<OwnedWorldWitness>,
        controls: Vec<rtdb_tx::HostedControlWrite>, deadline: Timestamp,
    ) -> Result<TransactionResult> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let count = controls.len().saturating_add(checks.len().saturating_mul(2)).saturating_add(2);
        if !(1..=128).contains(&checks.len()) || controls.is_empty() || count > 4096
            || controls.iter().any(|control| !control.is_check())
        {
            return Err(unavailable());
        }
        let scope = self.placed_history()?.ok_or_else(unavailable)?;
        let now = self.service.node.now();
        let mut until = deadline.min(record.until);
        for check in &checks {
            if check.world != record.id || check.pin != record.pin
                || check.relation.world() != Some(record.id) || scope != check.scope || now >= check.until
            {
                return Err(unavailable());
            }
            until = until.min(check.until);
        }
        if now >= until { return Err(unavailable()); }
        let _operations = self.service.node.memory().reserve(count * (std::mem::size_of::<TxOp>() + 512))?;
        let mut ops = Vec::with_capacity(count);
        ops.push(TxOp::OwnedDeadline { until });
        ops.push(TxOp::OwnedCheckWorldScope { scope });
        ops.extend(controls.into_iter().map(TxOp::HostedControl));
        Self::append_world_checks(&mut ops, checks);
        self.state.execute_tx(ops, Default::default()).await
    }

    pub async fn commit_owned_world_batch(
        &mut self, sections: Vec<(Timestamp, OwnedWorldSection)>,
        root: OwnedRootSection, fingerprint: [u8; 32],
        controls: Vec<rtdb_tx::HostedControlWrite>, deadline: Timestamp,
    ) -> Result<TransactionResult> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let terminal = sections.first().filter(|(_, section)| sections.len() == 1
            && matches!(&section.effect, OwnedWorldEffect::Drop)
            && section.checks.is_empty() && section.expected_clock.is_none());
        let delete_root = !root.proposal_close && terminal.is_some_and(|(_, section)| root.publication.as_ref().is_some_and(|root| {
            root.attempt == section.attempt && matches!(root.writes.as_slice(), [
                TxOp::OwnedDelete { relation, valid_from: None, valid_to: None, .. }
            ] if relation.is_root_data())
        })) && rtdb_tx::retained_delete_controls(&controls);
        let retained_release = delete_root || (terminal.is_some()
            && root.checks.is_empty() && root.publication.is_none()
            && rtdb_tx::retained_release_controls(&controls));
        let proposal_overlay = root.proposal_overlay;
        let proposal_close = root.proposal_close;
        let proposal_land = root.proposal_land || proposal_close;
        if proposal_land && (sections.len() != 1 || proposal_overlay
            || !matches!(&sections[0].1.effect, OwnedWorldEffect::Drop)
            || (root.proposal_land && proposal_close)
            || if proposal_close {
                !sections[0].1.checks.is_empty()
                    || root.publication.as_ref().is_none_or(|root| root.attempt != sections[0].1.attempt
                        || !matches!(&root.writes[..], [TxOp::OwnedDelete { relation, valid_from: None, valid_to: None, .. }]
                            if relation.is_root_data()))
                    || !rtdb_tx::proposal_close_controls(&controls)
            } else {
                sections[0].1.expected_clock.is_none()
                    || root.publication.as_ref().is_none_or(|root| {
                        if root.attempt != sections[0].1.attempt { return true; }
                        let mut facts = root.writes.iter().filter(|op| !rtdb_tx::is_proposal_landing_frontier(op));
                        let mut rows = 0usize;
                        facts.any(|op| {
                            let Some(count) = rtdb_tx::proposal_landing_write_rows(op) else { return true; };
                            rows = rows.saturating_add(count);
                            rows > rtdb_tx::MAX_PROPOSAL_LAND_ROWS
                        }) || rows == 0
                    })
                    || !rtdb_tx::proposal_land_controls(&controls)
            })
        {
            return Err(unavailable());
        }
        if proposal_overlay && (sections.len() != 1 || root.publication.is_some()
            || match &sections[0].1.effect {
                OwnedWorldEffect::Overlay(_) => !rtdb_tx::proposal_overlay_controls(&controls),
                OwnedWorldEffect::SettleProposal => !rtdb_tx::retained_delete_controls(&controls),
                _ => true,
            })
        {
            return Err(unavailable());
        }
        if !(1..=8).contains(&sections.len()) || controls.is_empty()
            || (!retained_release && !proposal_overlay && !proposal_land && controls.iter().any(|control| !control.is_check()))
            || root.checks.iter().any(|check| match check {
                TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. } =>
                    relation.world().is_some() || relation.is_world_catalog(),
                _ => true,
            })
        {
            return Err(unavailable());
        }
        let scope = self.placed_history()?.ok_or_else(unavailable)?;
        let now = self.service.node.now();
        let mut until = deadline;
        let mut worlds = std::collections::BTreeSet::new();
        let mut attempts = std::collections::BTreeSet::new();
        let mut effects = controls.len().saturating_add(root.checks.len()).saturating_add(2);
        if let Some(root) = &root.publication {
            if root.writes.is_empty() || root.writes.iter().any(|write| match write {
                TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                | TxOp::OwnedDelete { relation, .. } => !relation.is_root_data(),
                _ => true,
            }) {
                return Err(unavailable());
            }
            if !delete_root && !proposal_land {
                attempts.insert(&root.attempt);
            }
            effects = effects.saturating_add(root.writes.len()).saturating_add(if delete_root || proposal_land { 1 } else { 3 });
        }
        for (retained_until, section) in &sections {
            let terminal_proposal = matches!(&section.effect, OwnedWorldEffect::SettleProposal);
            if terminal_proposal && !proposal_overlay {
                return Err(unavailable());
            }
            let writes = match &section.effect {
                OwnedWorldEffect::Overlay(writes) if !writes.is_empty() => writes.len(),
                OwnedWorldEffect::Overlay(_) => return Err(unavailable()),
                OwnedWorldEffect::Clock(_) | OwnedWorldEffect::Drop | OwnedWorldEffect::SettleProposal => 0,
            };
            if !worlds.insert(section.world) || (!retained_release && !terminal_proposal && !proposal_close && !(1..=128).contains(&section.checks.len()))
                || section.checks.len() > 128
                || !attempts.insert(&section.attempt)
            {
                return Err(unavailable());
            }
            effects = effects.saturating_add(writes)
                .saturating_add(section.checks.len().saturating_mul(2)).saturating_add(2);
            until = until.min(*retained_until);
            for check in &section.checks {
                if check.world != section.world || check.pin != section.parent_pin
                    || check.relation.world() != Some(section.world) || check.scope != scope || now >= check.until
                {
                    return Err(unavailable());
                }
                until = until.min(check.until);
            }
        }
        if effects > 4096 || now >= until { return Err(unavailable()); }
        let _operations = self.service.node.memory().reserve(effects * (std::mem::size_of::<TxOp>() + 512))?;
        let mut ops = Vec::with_capacity(effects);
        ops.push(TxOp::OwnedDeadline { until });
        ops.push(TxOp::OwnedCheckWorldScope { scope });
        ops.extend(controls.into_iter().map(TxOp::HostedControl));
        ops.extend(root.checks);
        if let Some(root) = root.publication {
            ops.extend(root.writes);
            if !delete_root && !proposal_land {
                ops.push(TxOp::OwnedPublishRoot { attempt: root.attempt, fingerprint });
            }
        }
        for (_, section) in sections {
            Self::append_world_checks(&mut ops, section.checks);
            let (advance_to, drop_world, settlement_pin) = match section.effect {
                OwnedWorldEffect::Overlay(writes) => {
                    self.append_world_writes(&mut ops, section.world, writes, proposal_overlay)?;
                    (None, false, None)
                }
                OwnedWorldEffect::Clock(to) => (Some(to), false, None),
                OwnedWorldEffect::Drop => (None, true, None),
                OwnedWorldEffect::SettleProposal => (None, false, Some(section.parent_pin)),
            };
            ops.push(TxOp::OwnedPublishWorld { world: section.world, attempt: section.attempt,
                fingerprint, expected: section.expected_clock, advance_to, drop_world,
                settlement_pin,
                land_root: proposal_land.then_some(section.parent_pin),
                delete_root: delete_root.then_some(section.parent_pin) });
        }
        self.state.execute_tx(ops, Default::default()).await
    }

    pub fn append_hosted_world_read_checks(
        &self, ops: &mut Vec<TxOp>, checks: Vec<OwnedWorldWitness>,
    ) -> Result<()> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let Some(TxOp::OwnedDeadline { until }) = ops.first_mut() else {
            return Err(unavailable());
        };
        let scope = self.placed_history()?.ok_or_else(unavailable)?;
        let now = self.service.node.now();
        let mut worlds = std::collections::BTreeSet::new();
        for check in &checks {
            if check.scope != scope || check.relation.world() != Some(check.world) {
                return Err(unavailable());
            }
            worlds.insert(check.world);
            *until = (*until).min(check.until);
        }
        if !(1..=8).contains(&worlds.len()) || checks.len() > 8 * 128 || now >= *until {
            return Err(unavailable());
        }
        ops.push(TxOp::OwnedCheckWorldScope { scope });
        Self::append_world_checks(ops, checks);
        Ok(())
    }

    fn append_world_checks(ops: &mut Vec<TxOp>, checks: Vec<OwnedWorldWitness>) {
        for check in checks {
            let local_frontier = match &check.observation {
                OwnedWorldObservation::Point { .. } => None,
                OwnedWorldObservation::Relation { local_frontier } => *local_frontier,
            };
            ops.push(TxOp::OwnedCheckWorldPurge {
                relation: check.relation.clone(), pin: check.pin, frontier: check.purge_frontier, local_frontier,
            });
            if let OwnedWorldObservation::Point { id, valid_at, revision } = check.observation {
                ops.push(TxOp::OwnedCheck {
                    relation: check.relation, id, valid_at, expected: revision,
                });
            }
        }
    }

    fn append_world_writes(
        &self, ops: &mut Vec<TxOp>, world: rtdb_types::Iid, writes: Vec<OwnedWorldWrite>,
        proposal_overlay: bool,
    ) -> Result<()> {
        for OwnedWorldWrite { relation, id, shadow, valid, create_only } in writes {
            if create_only && (!proposal_overlay || valid.is_some()
                || !matches!(&shadow, rtdb_types::WorldShadow::Present(_)))
            {
                return Err(rtdb_types::OwnedWorld::unavailable());
            }
            let relation = relation.in_world(world)?;
            let _encode = self.service.node.memory().reserve(shadow.allocation_bound()?)?;
            let documents = rtdb_tx::Documents::new(vec![shadow.record(&id)?], self.service.node.memory())?;
            let valid_from = valid.map(|valid| valid.lower());
            let valid_to = valid.map(|valid| valid.upper());
            ops.push(if create_only {
                TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to }
            } else {
                TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }
            });
        }
        Ok(())
    }
    /// Reserves a fresh system time by awaiting an ordered, fact-free source transaction.
    /// The key is durable in this database's history. Later transactions in that history have strictly greater system times.
    pub async fn reserve_ordered_present(&mut self) -> Result<TransactionKey> {
        self.state.reserve_ordered_present().await
    }
    /// Retains the exact applied catalog at one store-reserved transaction for repeated reads.
    pub async fn open_owned_read(&mut self) -> Result<OwnedReadSession> {
        self.retain_owned_read(None).await
    }
    /// Retains the already-applied catalog from the current recovered hosted holder.
    pub async fn open_applied_owned_read(&mut self, minimum: TransactionKey) -> Result<OwnedReadSession> {
        self.retain_owned_read(Some(minimum)).await
    }
    async fn retain_owned_read(&mut self, applied_floor: Option<TransactionKey>) -> Result<OwnedReadSession> {
        if !self.service.node.factory.options.owned_relations { return Err(owned_relations_disabled()); }
        let node = self.service.node.clone();
        let identity = Arc::clone(&self.service.identity);
        let database = self.state.database().to_owned();
        let owner = self.state.user().filter(|owner| !owner.is_empty()).map(str::to_owned)
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/no-owner", "Owned relations require an authenticated principal"))?;
        let charge = node.memory().reserve(database.capacity().checked_add(owner.capacity()).and_then(|bytes| bytes.checked_add(2048))
            .ok_or_else(|| Anomaly::incorrect("rtdb.owned/read-size", "Retained read allocation overflow"))?)?;
        let attached = node.attached_database(&database, self.service.placement.as_ref())?;
        let witness_scope = match &attached.await_authority {
            AwaitAuthority::Live { codec, .. } => Some(codec.scope()),
            _ => None,
        };
        let attached = Arc::downgrade(&attached);
        let (reserved, cut) = match applied_floor {
            Some(minimum) => (None, CatalogReadCut::HostedApplied(minimum)),
            None => {
                let present = self.reserve_ordered_present().await?;
                (Some(present.system_time), CatalogReadCut::Reserved(present))
            }
        };
        let basis = QueryBasis {
            snapshot: TimeBasis { databases: BTreeMap::from([(database.clone(), vec![reserved.map(Into::into)])]) },
            snapshot_time: reserved,
            current_time: reserved.map_or_else(|| node.query_instant(), Into::into),
            timezone: self.state.session_timezone().to_owned(),
        };
        let session_cancellation = self.state.cancellation().clone();
        let cancellation = session_cancellation.child_token();
        let context = node.query_context_in_placement(
            &database, &basis, identity.as_ref(), Some(&attached), cut, &cancellation,
        ).await?;
        let present = context.catalog.database(&database)?.partitions.first().and_then(|partition| partition.tx_basis())
            .ok_or_else(hosted_source_unavailable)?;
        let lease = context.catalog.read_lease();
        let witness_scope = witness_scope.filter(|_| lease.as_ref().and_then(|lease| lease.deadline()).is_some());
        let clock = Arc::clone(&node.factory.clock);
        let now = clock.monotonic_micros();
        let deadline = now.saturating_add(60_000_000);
        let lease_deadline = lease.as_ref().map_or(deadline, |lease| lease.deadline().unwrap_or(now).min(deadline));
        let witness_until = if witness_scope.is_some() {
            Timestamp(clock.now().0.saturating_add(lease_deadline.saturating_sub(now))
                .min(present.system_time.0.saturating_add(60_000_000)))
        } else {
            present.system_time
        };
        let placement = attached.clone();
        let guarded_node = node.clone();
        let guarded_database = database.clone();
        let context = context.with_owned_read_guard(Arc::new(move || {
            if clock.monotonic_micros() >= deadline {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/read-expired", "The retained owned read expired"));
            }
            guarded_node.attached_database(&guarded_database, Some(&attached))?;
            if let Some(lease) = &lease { lease.check()?; }
            Ok(())
        }));
        context.check_cancelled()?;
        Ok(OwnedReadSession {
            context, node, identity, database, owner, present, witness_scope, witness_until, fork_until: witness_until,
            cancellation, session_cancellation, historical: None, scoped_root: None, attachment: placement, _reservation: charge,
        })
    }
    pub async fn query_context(&mut self) -> Result<Arc<QueryContext>> {
        let basis = self.state.read_basis().await?;
        Ok(self.service.query_context(self.state.database(), &basis, self.state.cancellation()).await?.with_observer(self.service.execution_observer()).with_tracer(self.service.node.tracer(), None))
    }
    pub async fn query_sql(&mut self, sql: &str, arguments: Vec<Value>) -> Result<QueryCursor> {
        let context = self.query_context().await?;
        let options = rtdb_sql::PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, rtdb_columnar::execution_type(&value.logical_type()))).collect(), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        prepare_query(sql, &context, &options)?.execute_column_rows(context, arguments, None).await
    }
    pub async fn query_xtql(&mut self, source: &str, arguments: Vec<Value>) -> Result<QueryCursor> {
        let context = self.query_context().await?;
        let options = rtdb_xtql::CompileOptions { parameter_types: arguments.iter().map(|value| rtdb_columnar::execution_type(&value.logical_type())).collect(), default_timezone: context.timezone.clone(), ..Default::default() };
        let prepared = rtdb_xtql::compile_in(source, &context.catalog, &options, &context.memory)?;
        PreparedQuery::from_prepared(prepared, &context.memory)?.execute_column_rows(context, arguments, None).await
    }

    /// Opens a logical relation against the same transaction read basis as SQL and XTQL.
    /// The operation token belongs to this session's cancellation tree.
    pub async fn query_ra(&mut self, source: &str, arguments: StructFields<Value>, cancellation: &CancellationToken) -> Result<QueryCursor> {
        let database = self.state.database().to_owned();
        let context = self.query_context_for(&database, cancellation).await?;
        let overflow = || Anomaly::incorrect("rtdb.query/parameter-size", "Logical relation parameter metadata exceeds addressable memory");
        let per_name = std::mem::size_of::<(String, (usize, LogicalType))>() * 16 + 256;
        let metadata_bytes = arguments.iter().try_fold(4096usize, |total, (name, value)| {
            let names = name.len().checked_mul(4).ok_or_else(overflow)?;
            let types = value.retained_bytes()?.checked_mul(16).ok_or_else(overflow)?;
            total.checked_add(per_name).and_then(|bytes| bytes.checked_add(names))
                .and_then(|bytes| bytes.checked_add(types)).ok_or_else(overflow)
        })?;
        let metadata = context.memory.reserve(metadata_bytes)?;
        let named_parameters = arguments.iter().enumerate().map(|(index, (name, value))| {
            (name.clone(), (index, rtdb_columnar::execution_type(&value.logical_type())))
        }).collect();
        let options = rtdb_xtql::CompileOptions { named_parameters, default_timezone: context.timezone.clone(), ..Default::default() };
        let prepared = rtdb_xtql::compile_ra_in(source, &context.catalog, &options, &context.memory)?;
        let query = PreparedQuery::from_prepared(prepared, &context.memory)?;
        drop(options);
        drop(metadata);
        let container_bytes = arguments.len().checked_mul(std::mem::size_of::<Value>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let _container = context.memory.reserve(container_bytes)?;
        query.execute_pages(context, arguments.into_values().collect()).await
    }
}

pub(crate) struct NativeSessionService {
    pub(crate) node: Node,
    pub(crate) identity: Arc<dyn SessionIdentity>,
    pub(crate) database: String,
    placement: Option<Weak<Database>>,
}

impl NativeSessionService {
    fn database(&self) -> Result<Arc<Database>> {
        self.node.attached_database(&self.database, self.placement.as_ref())
    }

    async fn confirm_placed_token(&self, db: &Database, token: PlacedToken, timeout: Duration, cancellation: &CancellationToken) -> Result<Option<(TransactionResult, String)>> {
        self.node.admit(db.name(), DatabaseAccess::Read, self.identity.as_ref(), cancellation).await?;
        self.node.ensure_indexing()?;
        let work = async {
            match token {
                PlacedToken::Pending { attempt, submitted_at } => self.node.redeem_placed_attempt(db, attempt, submitted_at, self.identity.as_ref(), self.placement.as_ref(), cancellation).await.map(Some),
                PlacedToken::Applied { attempt, through } => {
                    let codec = db.await_authority.codec().ok_or_else(invalid_token)?;
                    let within_history = attempt.scope().writer_epoch() <= codec.scope().writer_epoch()
                        && through.epoch() == db.logs.replica_epoch;
                    match &db.await_authority {
                        AwaitAuthority::Checkpoint { through: checkpoint, .. } => {
                            if !within_history || through > *checkpoint { return Err(receipt_checkpoint_behind(db.name())); }
                        }
                        AwaitAuthority::Live { .. } => {
                            if !within_history { return Err(receipt_replica_behind(db.name())); }
                            db.await_installed(|prefix| prefix.replica >= through, cancellation).await?;
                        }
                        _ => return Err(invalid_token()),
                    }
                    Ok(None)
                }
            }
        };
        let replica = db.read_only() && matches!(db.await_authority, AwaitAuthority::Live { .. });
        let deadline = if replica { timeout.min(Duration::from_millis(250)) } else { timeout };
        tokio::time::timeout(deadline, work).await.map_err(|_| {
            if replica { receipt_replica_behind(db.name()) } else { timeout_error() }
        })?
    }

    pub(crate) async fn query_context(&self, database: &str, basis: &QueryBasis, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.node.query_context_in_placement(database, basis, self.identity.as_ref(), self.placement.as_ref(), CatalogReadCut::Current, cancellation).await
    }

    pub(crate) async fn await_durable_source(&self, database: &str, hosted: bool, cancellation: &CancellationToken) -> Result<Option<HostedSourceReceipt>> {
        self.node.await_durable_source(database, self.identity.as_ref(), hosted, self.placement.as_ref(), cancellation).await
    }

    pub(crate) async fn confirm_hosted_source_owner(&self, database: &str, cancellation: &CancellationToken) -> Result<String> {
        self.node.confirm_hosted_source_owner(database, self.identity.as_ref(), self.placement.as_ref(), cancellation).await
    }

    pub(crate) fn visible_databases(&self) -> Result<BTreeMap<String, Arc<Database>>> {
        let catalog = self.node.catalog();
        if catalog.is_placed() {
            let database = self.database()?;
            Ok(BTreeMap::from([(self.database.clone(), database)]))
        } else {
            Ok(catalog.open_databases())
        }
    }
}
#[async_trait]
impl SessionService for NativeSessionService {
    fn memory(&self) -> &Arc<MemoryBudget> { self.node.memory() }
    fn execution_observer(&self) -> Option<Arc<dyn rtdb_plan::ExecutionObserver>> { self.node.factory.execution_observer.clone() }
    fn user(&self) -> Option<&str> { self.identity.user() }
    fn check_open(&self) -> Result<()> {
        self.node.ensure_open()?;
        if self.placement.is_some() { self.database()?; }
        Ok(())
    }
    async fn revalidate(&self) -> Result<()> { self.check_open()?; self.identity.revalidate(self.node.now()).await }
    fn now(&self) -> Timestamp { self.node.now() }
    fn query_instant(&self) -> rtdb_types::Instant { self.node.query_instant() }
    fn snapshot_basis(&self) -> Result<TimeBasis> {
        if !self.node.catalog.is_placed() { return self.node.snapshot_basis(); }
        self.node.ensure_open()?;
        let database = self.database()?;
        let time = database.partition.value().live().latest_completed_tx().map(|key| key.system_time.into());
        Ok(TimeBasis { databases: BTreeMap::from([(self.database.clone(), vec![time])]) })
    }
    async fn fresh_snapshot_basis(&self, cancellation: &CancellationToken) -> Result<TimeBasis> {
        if self.node.catalog.is_placed() {
            self.database()?.confirm_replica_snapshot(cancellation).await?;
        }
        self.snapshot_basis()
    }
    async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        if !self.node.catalog.is_placed() { return self.node.await_basis(basis, timeout, cancellation).await; }
        let Some(basis) = basis else { return Ok(()); };
        let db = self.database()?;
        if !matches!(db.await_authority, AwaitAuthority::SourcePosition) { return Err(invalid_token()); }
        let Some(position) = basis.databases.get(&self.database).and_then(|positions| positions.first()) else { return Ok(()); };
        self.node.ensure_indexing()?;
        tokio::time::timeout(timeout, db.replication.await_source(SourcePosition(position.0), cancellation)).await.map_err(|_| timeout_error())?
    }
    async fn await_client_token(&self, token: Option<&str>, timeout: Duration, cancellation: &CancellationToken) -> Result<Option<String>> {
        let Some(token) = token else { return Ok(None); };
        if !is_placed_token(token) {
            self.await_basis(Some(decode_tx_basis(token)?), timeout, cancellation).await?;
            return Ok(None);
        }
        let db = self.database()?;
        let authority = db.await_authority.codec().ok_or_else(invalid_token)?;
        let token = PlacedToken::open(token, authority, self.identity.user())?;
        Ok(self.confirm_placed_token(&db, token, timeout, cancellation).await?.map(|(_, applied)| applied))
    }
    async fn check_submission_basis(&self, database: &str, token: Option<&str>, cancellation: &CancellationToken) -> Result<()> {
        if !self.node.catalog.is_placed() { return Ok(()); }
        let Some(token) = token else { return Ok(()); };
        let db = self.node.attached_database(database, self.placement.as_ref())?;
        if matches!(db.await_authority, AwaitAuthority::SourcePosition) { return Ok(()); }
        db.require_writable()?;
        let authority = db.await_authority.codec().ok_or_else(invalid_token)?;
        let token = PlacedToken::open(token, authority, self.identity.user())?;
        if let PlacedToken::Pending { attempt, submitted_at } = token
            && attempt.scope() == authority.scope()
            && self.node.now().0 < submitted_at.0.saturating_add(rtdb_tx::RECEIPT_WINDOW_MICROS)
        {
            // A later submission in this intake can only resolve after earlier attempts in the same epoch.
            return Ok(());
        }
        self.confirm_placed_token(&db, token, Duration::from_secs(60), cancellation).await?;
        Ok(())
    }
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, cancellation: &CancellationToken) -> Result<SubmittedTx> {
        self.node.submit(database, operations, options, None, self.identity.as_ref(), self.placement.as_ref(), cancellation).await
    }
    async fn submit_replay(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, permit: crate::ReplaySubmissionPermit, cancellation: &CancellationToken) -> Result<SubmittedTx> {
        self.node.submit(database, operations, options, Some(permit), self.identity.as_ref(), self.placement.as_ref(), cancellation).await
    }
    async fn await_transaction(&self, database: &str, tx_id: TxId, cancellation: &CancellationToken) -> Result<TransactionResult> { self.node.await_transaction_result(database, tx_id, None, self.identity.as_ref(), self.placement.as_ref(), cancellation).await }
    async fn await_submission(&self, database: &str, submitted: &SubmittedTx, cancellation: &CancellationToken) -> Result<(TransactionResult, Option<String>)> {
        let Some(receipt) = &submitted.placed_receipt else {
            return Ok((self.await_transaction(database, submitted.tx_id, cancellation).await?, None));
        };
        let db = self.node.attached_database(database, self.placement.as_ref())?;
        let authority = db.await_authority.codec().ok_or_else(invalid_token)?;
        let token = PlacedToken::open(receipt, authority, self.identity.user())?;
        self.confirm_placed_token(&db, token, Duration::from_secs(60), cancellation).await?
            .map(|(result, applied)| (result, Some(applied))).ok_or_else(invalid_token)
    }
}

/// A retry of a committed transaction under the same identity and body is answered with the original's outcome.
fn answer_duplicate(result: TransactionResult) -> TransactionResult {
    let TransactionResult::Aborted { error: Some(error), .. } = &result else { return result };
    if error.code != "rtdb/idempotency-duplicate" { return result; }
    let field = |name: &str| error.context.get(name).and_then(serde_json::Value::as_i64);
    match (field("original-tx-id"), field("original-system-time-micros")) {
        (Some(tx_id), Some(system_time)) => TransactionResult::Committed { tx_key: TransactionKey { tx_id: TxId(tx_id), system_time: Timestamp(system_time) } },
        _ => result,
    }
}

/// The `idempotency_key` a transaction's metadata carries, set with `BEGIN READ WRITE WITH (METADATA = {idempotency_key: '...'})`.
fn idempotency_key(metadata: &Value) -> Option<&str> {
    let Value::Struct(fields) = metadata else { return None };
    let Value::Utf8(key) = fields.get("idempotency_key")? else { return None };
    Some(key)
}

/// The legacy SQL parameter for keying each transaction by its position within a command.
pub const IDEMPOTENCY_KEY_PARAMETER: &str = "rtdb_idempotency_key";

pub(crate) fn with_idempotency_key(metadata: Option<rtdb_tx::UserMetadata>, key: &str, memory: &Arc<MemoryBudget>) -> Result<rtdb_tx::UserMetadata> {
    let mut fields = match metadata.as_ref().map(|metadata| metadata.value()) {
        Some(value) if idempotency_key(value).is_some() => return Ok(metadata.expect("present")),
        Some(Value::Struct(fields)) => fields.clone(),
        _ => rtdb_types::StructFields::new(),
    };
    fields.insert("idempotency_key".into(), Value::Utf8(key.to_owned()));
    rtdb_tx::UserMetadata::new(Value::Struct(fields), memory)
}

fn historical_guard_sample(error: &Anomaly) -> Option<Timestamp> {
    error.context.get(IDEMPOTENCY_GUARD_RESULT)?.as_i64().map(Timestamp)
}

fn is_historical_guard_abort(error: &Anomaly) -> bool {
    error.code == "xtdb/assert-failed" && historical_guard_sample(error).is_some()
}

/// The code a placed node refuses a command with when it no longer holds the command's database.
pub const NOT_HELD: &str = "rtdb.node/database-not-held";
pub const REPLICA_BEHIND: &str = "rtdb.node/replica-behind";

fn lost_placement(database: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, NOT_HELD, format!("This node no longer holds {database}")).with_context("db-name", database.into())
}

fn owned_relations_disabled() -> Anomaly {
    Anomaly::unsupported("rtdb.owned/disabled", "Native owned relations are disabled on this node")
}

fn closed() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/node-closed", "Node is closed") }
fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "Operation was cancelled") }
fn hosted_source_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.node/hosted-source-unavailable", "The host cannot confirm this database's durable history")
}
fn bad_attempt_outcome() -> Anomaly {
    Anomaly::fault("rtdb.node/attempt-outcome", "Installed attempt outcome does not match the durable transaction")
}
fn receipt_replica_behind(database: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, REPLICA_BEHIND, format!("This replica of {database} could not confirm the await token"))
}
fn receipt_checkpoint_behind(database: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/checkpoint-behind", format!("This checkpoint of {database} does not cover the await token"))
}
fn timeout_error() -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "xtdb/await-tx-timeout", "Timed out awaiting indexed progress") }
