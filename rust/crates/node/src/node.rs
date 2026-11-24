mod historical_owned_read;
mod hosted_control;
pub use hosted_control::RootCatalogRead;
mod hosted_observation;
mod transaction_evidence;
mod funded_mutation_clock;
pub use funded_mutation_clock::{FundedMutationClock, MAX_FUNDED_MUTATION_CLOCK_TOKEN_BYTES};
mod durable_range;
pub use durable_range::{DurableRange, MAX_DURABLE_RANGE_BYTES};
pub use hosted_observation::{AppliedOwnedPoint, AppliedReadConfirmation, AppliedReadFloor};
use hosted_observation::PreparedAppliedCatalog;
mod owned_changes;
mod owned_usage;
pub use owned_usage::OwnedStorageUsage;
mod owned_identities;
mod owned_unique;
pub use owned_identities::OwnedIdentityPage;
pub use owned_changes::{ChangeEdgeOrder, OwnedChangeEdge, archived_changes_at_cut, archived_path_touch_at_cut, archived_versions_at_cut, ExpiryCandidate, ExpiryCandidatePage, OwnedChangeCommit, OwnedMergedChanges, OwnedPublicationLookup, OwnedRelationChanges, OwnedWorldVersionRead, OWNED_MERGED_CHANGE_RELATIONS};

use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use rtdb_columnar::{ArrowCell, ColumnReader, EntityIdRef, MemoryBudget, read_execution_entity_id_as, read_value};
use rtdb_format::basis::decode_tx_basis;
use rtdb_format::{database::proto::DatabaseConfig, envelope::{SourceBody, SourceMessage}, log::proto::{AttachDatabase, DetachDatabase, resolved_tx::DbOp}};
use rtdb_maintenance::CollectionObserver;
use rtdb_plan::{CatalogMetrics, CatalogUsers};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryCursor};
use rtdb_storage::cache::{BufferPool, CacheBudget};
use rtdb_tx::{IDEMPOTENCY_GUARD_RESULT, RetainedAnomaly, SubmitOptions, SubmittedTx, TransactionAuthorization, TransactionResult, TxOp, prepare_submission_with_sql_options};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, EntityId, Interval, LogicalType, PlacedAttempt, PlacedDecision, ReplicaPosition, Result, ScalarType, SourcePosition, TimeBasis, TimeUnit, Timestamp, TransactionKey, TxBasis, TxId, Value};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{AwaitAuthority, Clock, Database, DatabaseAccess, DatabaseCatalog, ExternalSources, Membership, NativeResources, NodeOptions, PreparedSql, QueryBasis, ResourceFactory, ResultSignaturePolicy, Session, SessionIdentity, SessionService, SystemClock,
    catalog::CatalogCommand, database::{DatabaseDriver, DatabaseFactory, OpenedDatabase}, placed_receipts::{PlacedToken, invalid_token, is_placed_token}, statement::prepare_query};


#[derive(Clone)]
pub struct NodeServices {
    pub resources: Arc<dyn ResourceFactory>,
    pub transaction_authorization: Arc<dyn TransactionAuthorization>,
    pub collection_observer: Arc<dyn CollectionObserver>,
    pub external_sources: ExternalSources,
    pub clock: Arc<dyn Clock>,
    pub decision_clock: Option<Arc<dyn rtdb_tx::DecisionClock>>,
    pub catalog_users: Arc<dyn CatalogUsers>,
    pub catalog_metrics: Arc<dyn CatalogMetrics>,
    pub execution_observer: Option<Arc<dyn rtdb_plan::ExecutionObserver>>,
    pub tracer: Option<Arc<dyn rtdb_plan::Tracer>>,
    /// Where a read-only database's compactor sends what it produced, to be published by the database's writer elsewhere.
    /// Absent, a read-only database runs no compactor.
    pub compaction_relay: Option<Arc<dyn crate::database::CompactionRelay>>,
    pub world_constraints: Option<Arc<dyn rtdb_tx::WorldConstraintArchive>>,
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
    fn archive_history(&self, _database: &str) -> bool { false }
    fn hold_active_call(&self, _database: &Database) -> Option<Box<dyn Send + Sync>> { None }
    async fn hold_transaction_custody(&self, _database: &Database) -> Result<Option<Arc<dyn Send + Sync>>> { Ok(None) }
    async fn confirm_hosted(&self, _database: &Database) -> Result<String> {
        Err(hosted_source_unavailable())
    }
    /// Confirms the complete current placement. Applied snapshots also require journal leadership.
    async fn confirm_audience(&self, _database: &Database, _require_quorum: bool) -> Result<String> {
        Err(hosted_source_unavailable())
    }
    async fn confirm_held_audience(&self, database: &Database) -> Result<String> {
        self.confirm_audience(database, false).await
    }
    /// Reserves one deployment seat until the final session owner drops its release guard.
    async fn hold_audience_seat(&self, _database: Weak<Database>, _index: usize, ready: oneshot::Sender<Result<()>>,
        _release: CancellationToken) {
        let _ = ready.send(Err(hosted_source_unavailable()));
    }
}

pub(crate) struct HostedSourceReceipt {
    pub(crate) history: String,
    pub(crate) applied: Option<TransactionKey>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SearchFileFormat {
    Tantivy,
    Arrow,
    #[default]
    Paged,
    Tree,
    Packed,
    Logical,
}

fn default_search(_format: SearchFileFormat) -> SearchServices {
    #[cfg(feature = "search")] {
        let bytes = 256 << 20;
        let runtime = match _format {
            SearchFileFormat::Tantivy => rtdb_search::SearchRuntime::with_tantivy_files(bytes),
            SearchFileFormat::Arrow => rtdb_search::SearchRuntime::with_paged_files(bytes, false),
            SearchFileFormat::Paged => rtdb_search::SearchRuntime::with_paged_files(bytes, true),
            SearchFileFormat::Tree => rtdb_search::SearchRuntime::with_tree_files(bytes),
            SearchFileFormat::Packed => rtdb_search::SearchRuntime::with_search_packs(bytes),
            SearchFileFormat::Logical => rtdb_search::SearchRuntime::with_logical_search(bytes),
        };
        SearchServices { artefact_kinds: vec![runtime.artefacts()], scan_accelerators: vec![runtime.accelerator()] }
    }
    #[cfg(not(feature = "search"))] { SearchServices { artefact_kinds: Vec::new(), scan_accelerators: Vec::new() } }
}
struct SearchServices { artefact_kinds: Vec<Arc<dyn rtdb_maintenance::ArtefactKind>>, scan_accelerators: Vec<Arc<dyn rtdb_query::ScanAccelerator>> }
impl NodeServices {
    pub fn local(authorization: Arc<dyn TransactionAuthorization>, observer: Arc<dyn CollectionObserver>, users: Arc<dyn CatalogUsers>, metrics: Arc<dyn CatalogMetrics>) -> Self {
        Self::local_with_search_format(authorization, observer, users, metrics, SearchFileFormat::default())
    }

    pub fn local_with_search_format(authorization: Arc<dyn TransactionAuthorization>, observer: Arc<dyn CollectionObserver>,
        users: Arc<dyn CatalogUsers>, metrics: Arc<dyn CatalogMetrics>, format: SearchFileFormat) -> Self {
        let search = default_search(format);
        Self { resources: Arc::new(NativeResources::default()), transaction_authorization: authorization, collection_observer: observer, external_sources: BTreeMap::new(), clock: Arc::new(SystemClock), catalog_users: users, catalog_metrics: metrics, execution_observer: None, tracer: None,
            decision_clock: None, compaction_relay: None, world_constraints: None, read_fence: None, scan_fragmenter: None,
            artefact_kinds: search.artefact_kinds.into_iter().chain([
                Arc::new(rtdb_spatial::SpatialArtefacts) as Arc<dyn rtdb_maintenance::ArtefactKind>,
                Arc::new(rtdb_vector::VectorArtefacts) as Arc<dyn rtdb_maintenance::ArtefactKind>,
            ]).collect(),
            scan_accelerators: search.scan_accelerators.into_iter()
                .chain([Arc::new(rtdb_vector::VectorAccelerator::default()) as Arc<dyn rtdb_query::ScanAccelerator>]).collect() }
    }
}

#[derive(Clone, Copy)]
enum CatalogReadCut<'a> {
    Current,
    Single,
    Reserved(TransactionKey),
    HostedApplied {
        minimum: TransactionKey,
        history: uuid::Uuid,
        memory: &'a Arc<MemoryBudget>,
        admission: AppliedCatalogAdmission<'a>,
    },
}

#[derive(Clone, Copy)]
enum AppliedCatalogAdmission<'a> {
    Ordinary,
    PrepareAudience(&'a Arc<BufferPool>),
    PreparedAudience(&'a PreparedAppliedCatalog),
}

enum OwnedReadBasis<'a> {
    ClockReservation,
    Fresh { deployment: uuid::Uuid, required: Option<&'a AppliedReadFloor> },
    Applied { floor: &'a AppliedReadFloor, preparation: Option<Arc<PreparedAppliedCatalog>> },
}

fn covers_transaction(selected: TransactionKey, floor: TransactionKey) -> bool {
    selected == floor
        || (selected.tx_id > floor.tx_id && selected.system_time >= floor.system_time)
}

#[derive(Clone, Copy)]
enum OutcomeQuery {
    Historical,
    Placed,
    Await,
    Bounded,
}

impl OutcomeQuery {
    fn sql(self) -> &'static str {
        match self {
            Self::Historical => rtdb_tx::HISTORICAL_KEYED_TRANSACTION_SQL,
            Self::Placed => "SELECT tx_id, tx_system_time, committed FROM xt.placed_attempt_outcomes FOR ALL VALID_TIME WHERE _id = ?",
            Self::Await => "SELECT system_time, committed, error, user_metadata FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?",
            Self::Bounded => "SELECT system_time, committed, error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?",
        }
    }
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
    sql_snapshots: Arc<crate::sql_snapshots::SqlSnapshots>,
    outcome_queries: Arc<[std::sync::Mutex<Option<PreparedSql>>; 4]>,
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
            external_sources: services.external_sources, compaction_relay: services.compaction_relay, world_constraints: services.world_constraints, compaction_slots, clock: services.clock, decision_clock: services.decision_clock, artefact_kinds: services.artefact_kinds,
            accelerators: Arc::from(if options.scan_acceleration { services.scan_accelerators } else { Vec::new() }), options });
        let (commands, receiver) = mpsc::channel(factory.options.command_capacity);
        let placement_slots = catalog.is_placed().then(|| Arc::new(Semaphore::new(MAXIMUM_PLACED_OPENS)));
        let node = Self { catalog, factory, commands, cancellation: CancellationToken::new(), runtime, read_fence, scan_fragmenter, placement_slots,
            sql_snapshots: Arc::default(), outcome_queries: Default::default() };
        node.sql_snapshots.start(node.cancellation.clone());
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
    /// Rechecks the serving placement before reading an already-held audience snapshot.
    pub async fn confirm_audience_route(&self, database: &Arc<Database>, source: uuid::Uuid) -> Result<()> {
        self.confirm_audience_attachment(database, source, false).await
    }

    pub async fn confirm_held_audience_route(&self, database: &Arc<Database>, source: uuid::Uuid) -> Result<()> {
        self.confirm_audience_attachment(database, source, true).await
    }

    async fn confirm_audience_attachment(&self, database: &Arc<Database>, source: uuid::Uuid, held: bool) -> Result<()> {
        self.ensure_open()?;
        let fence = self.read_fence.as_ref().ok_or_else(hosted_source_unavailable)?;
        let history = if held {
            fence.confirm_held_audience(database).await?
        } else {
            fence.confirm_audience(database, false).await?
        };
        let attached = self.attached_database(database.name(), None)?;
        if uuid::Uuid::parse_str(&history).ok() != Some(source) || !Arc::ptr_eq(&attached, database) {
            return Err(hosted_source_unavailable());
        }
        Ok(())
    }
    pub async fn hold_audience_seat(&self, database: Weak<Database>, index: usize, ready: oneshot::Sender<Result<()>>,
        release: CancellationToken) {
        if let Some(fence) = self.read_fence.as_ref() && self.ensure_open().is_ok() {
            fence.hold_audience_seat(database, index, ready, release).await;
        } else {
            let _ = ready.send(Err(hosted_source_unavailable()));
        }
    }
    pub async fn receiver_time(&self) -> Result<Timestamp> {
        match &self.factory.decision_clock {
            Some(clock) => Ok(clock.observe().await?.instant),
            None => Ok(self.now()),
        }
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
        self.session_in_memory(database.into(), identity, Arc::clone(self.memory()), None)
    }

    /// Opens a placed audience session backed by the holder's prepaid execution and cache allocations.
    pub fn audience_session(&self, database: impl Into<String>, identity: Arc<dyn SessionIdentity>, source: uuid::Uuid, memory: Arc<MemoryBudget>, cache: Arc<CacheBudget>) -> Result<NativeSession> {
        if !self.catalog.is_placed() || !memory.prepaid_by(self.memory()) || !cache.prepaid_by(self.memory()) {
            return Err(Anomaly::incorrect("rtdb.node/unfunded-session", "A placed audience session requires this holder's prepaid execution and cache allocations"));
        }
        self.session_in_memory(database.into(), identity, memory, Some((source, cache)))
    }

    fn session_in_memory(&self, database: String, identity: Arc<dyn SessionIdentity>, memory: Arc<MemoryBudget>, audience: Option<(uuid::Uuid, Arc<CacheBudget>)>) -> Result<NativeSession> {
        self.ensure_open()?;
        identity.authorize_database(&database, DatabaseAccess::Read)?;
        let local = self.catalog.database(&database)?;
        let placement = self.catalog.is_placed().then(|| Arc::downgrade(&local));
        let audience_storage = if let Some((source, cache)) = audience {
            let storage = local.audience_storage.as_ref().filter(|storage| storage.source == source)
                .ok_or_else(hosted_source_unavailable)?;
            Some(Arc::new(BufferPool::with_metadata_budget(Arc::clone(&storage.store), cache, Arc::clone(&memory))))
        } else {
            None
        };
        let service = Arc::new(NativeSessionService { node: self.clone(), identity, database: database.clone(), placement, memory, audience_storage });
        let state = Session::new(service.clone(), database, &self.factory.options.default_timezone, &self.cancellation)?;
        Ok(NativeSession { state, service, applied_preparation: None })
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

    /// Re-admit access to the databases of an already held catalog.
    pub async fn admit_retained_catalog(
        &self, catalog: &CatalogSnapshot, identity: &dyn SessionIdentity, cancellation: &CancellationToken,
    ) -> Result<()> {
        for name in catalog.database_names() {
            self.admit(name, DatabaseAccess::Read, identity, cancellation).await?;
            if let Some(fence) = &self.read_fence {
                fence.confirm(name).await?;
            }
            let current = self.catalog.database(name)?;
            if !catalog.belongs_to_storage(name, &current.storage)? {
                return Err(lost_placement(name));
            }
        }
        if let Some(lease) = catalog.read_lease() {
            lease.check()?;
        }
        Ok(())
    }

    pub async fn query_context(&self, database: &str, basis: &QueryBasis, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.query_context_in_placement(database, basis, identity, None, CatalogReadCut::Current, cancellation).await
    }

    /// Captures one database with the node's clock and time zone.
    /// `linearizable` establishes a fresh read barrier before capture.
    /// An attachment change during the read invalidates the capture.
    pub async fn wire_query_context(
        &self, database: &str, identity: &dyn SessionIdentity, linearizable: bool,
        cancellation: &CancellationToken,
    ) -> Result<Arc<QueryContext>> {
        let mut basis = QueryBasis {
            snapshot: TimeBasis::default(), replica_capture: None, resume_token: None,
            has_await_floor: false, snapshot_time: None, current_time: self.query_instant(),
            timezone: self.factory.options.default_timezone.clone(),
        };
        let local = self.catalog.database(database)?;
        let placement = Arc::downgrade(&local);
        if linearizable {
            self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
            if local.has_replica_read_index() {
                let capture = local.capture_replica_snapshot(cancellation).await?;
                basis.replica_capture = Some(crate::session::ReplicaQueryCapture {
                    attachment: placement.clone(), snapshot: Arc::new(capture),
                });
            } else {
                self.await_durable_source(database, identity, false, Some(&placement), cancellation).await?;
            }
        }
        self.query_context_in_placement(database, &basis, identity, Some(&placement), CatalogReadCut::Single, cancellation).await
    }

    /// After admission, waits up to `timeout` for applied system time to reach or exceed `time`.
    /// The caller captures its read context separately.
    pub async fn wire_await_time(&self, database: &str, identity: &dyn SessionIdentity, time: Timestamp, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        self.ensure_indexing()?;
        let database = self.catalog.database(database)?;
        tokio::time::timeout(timeout, database.progress().await_system_time(time, cancellation))
            .await.map_err(|_| timeout_error())??;
        Ok(())
    }

    async fn query_context_in_placement(&self, database: &str, basis: &QueryBasis, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cut: CatalogReadCut<'_>, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        let placed = self.catalog.is_placed();
        let held = (placed || !matches!(cut, CatalogReadCut::Current)).then(|| self.attached_database(database, placement)).transpose()?;
        let replica_capture = basis.replica_capture.as_ref();
        if let Some(capture) = replica_capture {
            let same_attachment = held.as_ref().is_some_and(|database| {
                std::ptr::eq(capture.attachment.as_ptr(), Arc::as_ptr(database))
            });
            if !same_attachment { return Err(lost_placement(database)); }
        }
        let hosted_fence = match cut {
            CatalogReadCut::HostedApplied { history, admission, .. } => {
                self.ensure_indexing()?;
                if !placed || placement.is_none() { return Err(hosted_source_unavailable()); }
                Some((self.read_fence.as_ref().ok_or_else(hosted_source_unavailable)?, history, admission))
            }
            CatalogReadCut::Current | CatalogReadCut::Single | CatalogReadCut::Reserved(_) => {
                if replica_capture.is_none() && let Some(fence) = &self.read_fence { fence.confirm(database).await?; }
                None
            }
        };
        let audience = hosted_fence.is_some_and(|(_, _, admission)| !matches!(admission, AppliedCatalogAdmission::Ordinary));
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
                CatalogReadCut::HostedApplied { .. } => database.object_store().catalog_delivery_lease().await?,
                CatalogReadCut::Current | CatalogReadCut::Single | CatalogReadCut::Reserved(_) => self.catalog_read_lease(&database).await?,
            };
            if let Some(database_lease) = database_lease {
                lease = Some(match lease { Some(lease) => lease.earlier(database_lease), None => database_lease });
            }
            let snapshot = match cut {
                CatalogReadCut::Current | CatalogReadCut::Single => crate::read_selection::query_snapshot(
                    replica_capture.map(|capture| (*capture.snapshot).clone()),
                    || database.partition.snapshot(),
                ),
                CatalogReadCut::Reserved(reserved) => {
                    let snapshot = database.partition.snapshot();
                    if snapshot.tx_basis() != Some(reserved) {
                        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/capture-raced", "The exact reserved transaction is no longer the applied catalog head"));
                    }
                    snapshot
                }
                CatalogReadCut::HostedApplied { minimum, .. } => {
                    let snapshot = database.partition.snapshot();
                    let selected = snapshot.tx_basis().filter(|selected| covers_transaction(*selected, minimum))
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
            let upper = applied.map(|selected| selected.system_time).or(basis.snapshot_time);
            let cap = match (cap, upper) { (Some(time), Some(upper)) => Some(time.min(upper)), (time, _) => time };
            let snapshot = snapshot.with_system_time_cap(cap)?;
            let storage = match hosted_fence.map(|(_, _, admission)| admission) {
                Some(AppliedCatalogAdmission::PrepareAudience(storage)) => Arc::clone(storage),
                Some(AppliedCatalogAdmission::PreparedAudience(preparation)) => Arc::clone(preparation.storage()),
                _ => Arc::clone(&database.storage),
            };
            let snapshot = if let Some((fence, expected_history, admission)) = hosted_fence {
                let cut = snapshot.catalog_cut().ok_or_else(applied_catalog_unretained)?;
                let permit = if database.requires_anchored_catalog() {
                    if let AppliedCatalogAdmission::PreparedAudience(preparation) = admission {
                        preparation.pin(&database, cut)?;
                    }
                    let checkpoint = snapshot.catalog_checkpoint()?.ok_or_else(applied_catalog_unretained)?;
                    storage.hold_anchored_catalog(cut, checkpoint, None).await?
                } else {
                    match admission {
                        AppliedCatalogAdmission::PreparedAudience(preparation) => preparation.pin(&database, cut)?,
                        AppliedCatalogAdmission::PrepareAudience(_) => storage.hold_catalog(Some(cut)).await?,
                        AppliedCatalogAdmission::Ordinary =>
                            storage.hold_catalog_with_paths(Some(cut), snapshot.catalog_file_paths()).await?,
                    }
                };
                if !permit.is_registered() { return Err(applied_catalog_unretained()); }
                let history = match admission {
                    AppliedCatalogAdmission::Ordinary => fence.confirm_hosted(&database).await?,
                    AppliedCatalogAdmission::PrepareAudience(_) | AppliedCatalogAdmission::PreparedAudience(_) =>
                        fence.confirm_audience(&database, true).await?,
                };
                if uuid::Uuid::parse_str(&history).ok() != Some(expected_history) {
                    return Err(hosted_source_unavailable());
                }
                self.attached_database(&name, placement)?;
                snapshot.with_catalog_read(permit)
            } else {
                rtdb_query::retain_catalog_snapshot(snapshot, &database.storage).await?
            };
            snapshots.insert(name, DatabaseSnapshot { partitions: vec![snapshot], storage });
        }
        crate::read_currency::require_current(database, lease.as_deref())?;
        let applied_basis = applied.map(|selected| QueryBasis {
            snapshot: TimeBasis { databases: BTreeMap::from([(database.to_owned(), vec![Some(selected.system_time.into())])]) },
            replica_capture: None,
            resume_token: None,
            has_await_floor: false,
            snapshot_time: Some(selected.system_time),
            current_time: basis.current_time,
            timezone: basis.timezone.clone(),
        });
        let basis = applied_basis.as_ref().unwrap_or(basis);
        let mut catalog = CatalogSnapshot::new(database, snapshots, false)?
            .with_ordered_indexes(Arc::new(crate::ordered_index::Inventory));
        if !matches!(cut, CatalogReadCut::Single) {
            catalog = catalog.with_runtime(Arc::new(crate::runtime::NodeCatalogRuntime::new(self.clone(), cancellation.clone())));
        }
        let archive_history = self.read_fence.as_ref().is_some_and(|fence| fence.archive_history(database));
        if archive_history { catalog = catalog.with_archive_history(); }
        if !audience && !archive_history { catalog = catalog.with_fragmenter(self.scan_fragmenter.clone(), fragment_residence); }
        if let Some(lease) = lease { catalog = catalog.with_read_lease(lease); }
        let memory = match cut {
            CatalogReadCut::HostedApplied { memory, .. } => Arc::clone(memory),
            _ => self.factory.options.query.query_memory(&self.factory.memory),
        };
        let context = QueryContext::with_owner(catalog, memory, basis.current_time, basis.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?;
        let context = if audience { context } else { context.with_accelerators(Arc::clone(&self.factory.accelerators)) };
        context
            .with_read_options(&basis.snapshot, basis.snapshot_time, basis.current_time)
            .map(|context| context.with_query_cpu(query_cpu))
    }

    /// Reads every local erase marker from the exact protected snapshot.
    pub async fn sql_erasure_source(
        &self, database: &Database, cut: CatalogCut, checkpoint: Option<rtdb_replication::PublishedCheckpoint>,
        cancellation: &CancellationToken,
    ) -> Result<rtdb_replication::SqlErasureSource> {
        let replay = cancellation.run_until_cancelled(
            rtdb_replication::SnapshotReplay::protect(&database.logs, cut, checkpoint),
        ).await.ok_or_else(cancelled)??;
        let owner = replay.provider().ok_or_else(crate::sql_snapshots::unretained)?;
        let custody = replay.custody().ok_or_else(crate::sql_snapshots::unretained)?;
        rtdb_storage::IoRegion::retaining(custody, async {
            let store = database.object_store().open_log_snapshot(owner).await?;
            let storage = Arc::new(BufferPool::with_budget(Arc::clone(&store), Arc::clone(database.storage.budget())));
            let blocks = rtdb_replication::BlockStore::new(Arc::clone(&store),
                storage, Arc::clone(self.memory()), self.factory.options.file_size_target)?;
            let mut recovered = match checkpoint {
                Some(checkpoint) => cancellation.run_until_cancelled(blocks.recover(self.factory.options.trie,
                    database.logs.source_floor()?, self.now(), rtdb_replication::BlockDiscovery::AtPublished(checkpoint)))
                    .await.ok_or_else(cancelled)??,
                None => blocks.empty_snapshot_base(self.factory.options.trie, database.logs.source_floor()?, self.now())?,
            };
            recovered.replay_sql_erasure(&database.logs, replay, store, cancellation).await
        }).await
    }

    pub(crate) async fn recover_sql_snapshot(
        &self, database: &Database, cut: CatalogCut, checkpoint: Option<rtdb_replication::PublishedCheckpoint>,
        cancellation: &CancellationToken,
    ) -> Result<crate::sql_snapshots::RecoveredSqlSnapshot> {
        let replay = cancellation.run_until_cancelled(
            rtdb_replication::SnapshotReplay::protect(&database.logs, cut, checkpoint),
        ).await.ok_or_else(cancelled)??;
        let installed = &database.storage;
        let (storage, catalog_hold) = if let Some(owner) = replay.provider() {
            let view = installed.object_store().open_log_snapshot(owner).await?;
            (Arc::new(BufferPool::with_budget(view, Arc::clone(installed.budget()))), None)
        } else {
            let hold = installed.hold_catalog(Some(cut)).await?;
            if !hold.is_registered() { return Err(crate::sql_snapshots::unretained()); }
            (Arc::clone(installed), Some(hold))
        };
        let dependencies = Arc::new((catalog_hold.clone(), replay.custody()));
        rtdb_storage::IoRegion::retaining(dependencies, async {
            let blocks = rtdb_replication::BlockStore::new(Arc::clone(storage.object_store()),
                Arc::clone(&storage), Arc::clone(self.memory()), self.factory.options.file_size_target)?;
            let mut recovered = match checkpoint {
                Some(checkpoint) => cancellation.run_until_cancelled(blocks.recover(self.factory.options.trie,
                    database.logs.source_floor()?, self.now(), rtdb_replication::BlockDiscovery::AtPublished(checkpoint)))
                    .await.ok_or_else(cancelled)??,
                None => blocks.empty_snapshot_base(self.factory.options.trie, database.logs.source_floor()?, self.now())?,
            };
            let suffix = recovered.replay_snapshot_suffix(&database.logs, replay, cancellation).await?;
            recovered.partition.publish_catalog_cut(cut)?;
            let snapshot = recovered.partition.snapshot();
            let snapshot = if let Some(hold) = catalog_hold { snapshot.with_catalog_read(hold) } else { snapshot };
            let partition = rtdb_query::retain_catalog_snapshot(snapshot, &storage).await?;
            let storage = (!Arc::ptr_eq(installed, &storage)).then_some(storage);
            Ok(crate::sql_snapshots::RecoveredSqlSnapshot { partition, suffix, storage })
        }).await
    }

    pub(crate) async fn catalog_read_lease(&self, database: &Database) -> Result<Option<Arc<rtdb_storage::ReadLease>>> {
        let objects = database.object_store();
        if let Some(lease) = objects.catalog_delivery_lease().await? {
            return Ok(Some(lease));
        }
        if !self.factory.options.replication.enabled || objects.defers_trie_releases() {
            return Ok(None);
        }
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
        self.scan_fragment_with_arguments(fragment, None, catch_up, cancellation).await
    }

    pub async fn scan_fragment_with_arguments(
        &self, fragment: &rtdb_query::ScanFragment, arguments: Option<rtdb_query::BoundParameters>,
        catch_up: Duration, cancellation: &CancellationToken,
    ) -> Result<rtdb_query::FragmentBytes> {
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
        let snapshot = database.partition.open_snapshot(Some(fragment.basis.tx_id));
        let snapshot = rtdb_query::retain_catalog_snapshot(snapshot, &database.storage).await?;
        crate::read_currency::require_current(database.name(), lease.as_deref())?;
        let snapshots = BTreeMap::from([(database.name().to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&database.storage) })]);
        let mut catalog = CatalogSnapshot::new(database.name(), snapshots, false)?;
        if let Some(lease) = &lease { catalog = catalog.with_read_lease(Arc::clone(lease)); }
        let context = QueryContext::with_owner(catalog, Arc::clone(&self.factory.memory), self.query_instant(), fragment.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?;
        rtdb_storage::IoRegion::leased(lease, rtdb_query::evaluate_fragment_with_arguments(context, fragment, arguments)).await
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
        // Enter broker custody while holding the local submission gate. After
        // the broker closes, pausing this gate drains every admitted submit.
        let _custody = match &self.read_fence {
            Some(fence) => fence.hold_transaction_custody(&database).await?,
            None => None,
        };
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
        let prepared_clock = source.proposal_generation_reading()?;
        let payload = source.encode()?.into_bytes();
        drop(source);
        if let Some(reading) = prepared_clock {
            let delay = reading.0.saturating_sub(self.now().0);
            if delay > 1_000_000 {
                return Err(Anomaly::incorrect("rtdb.owned/clock-slot-missed", "The offered transaction clock is unavailable"));
            }
            if delay > 0 {
                let wait = tokio::time::sleep(std::time::Duration::from_micros(delay as u64));
                cancellation.run_until_cancelled(wait).await.ok_or_else(cancelled)?;
            }
        }
        let submitted_bytes = payload.len() as u64;
        let append_time = submitted_at.unwrap_or_else(|| self.now());
        let position = cancellation.run_until_cancelled(database.replication.submit_source_encoded(payload, append_time)).await.ok_or_else(cancelled)??;
        if let Some(admission) = &admission {
            database.record_submission(submitted_bytes);
            let mut latest = admission.lock();
            *latest = (*latest).max(Some(position));
        }
        let placed_receipt = match authority.zip(seed) {
            Some((authority, seed)) => {
                let attempt = seed.identify(position, identity.user())?;
                if !database.progress().await_placed_admission(attempt, append_time, cancellation).await?
                    && self.placed_outcome(database.name(), attempt, identity, placement, cancellation).await?.is_none()
                {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/attempt-unresolved",
                        "The receiver could not confirm admission of the original transaction"));
                }
                Some(PlacedToken::Pending { attempt, submitted_at: append_time }.seal(authority)?)
            }
            None => None,
        };
        Ok(SubmittedTx { tx_id: TxId(position.0), placed_receipt })
    }

    fn outcome_query(&self, query: OutcomeQuery, context: &Arc<QueryContext>, arguments: &[Value]) -> Result<PreparedQuery> {
        let types = arguments.iter().map(Value::logical_type).collect::<Vec<_>>();
        let mut slot = self.outcome_queries[query as usize].lock()
            .map_err(|_| Anomaly::fault("rtdb.node/outcome-query", "Outcome statement cache was poisoned"))?;
        if let Some(prepared) = &*slot { return prepared.bind(context, &types, false); }
        let options = rtdb_sql::PrepareOptions {
            parameter_types: types.iter().cloned().enumerate().collect(),
            default_timezone: context.timezone.clone(),
            generate_series_end_exclusive: context.options.generate_series_end_exclusive,
            ..Default::default()
        };
        let mut planning = Arc::clone(context);
        Arc::make_mut(&mut planning).memory = Arc::clone(self.memory());
        let prepared = PreparedSql::prepare(query.sql(), &planning, options, ResultSignaturePolicy::Dynamic)?;
        let bound = prepared.bind(context, &types, false)?;
        *slot = Some(prepared);
        Ok(bound)
    }

    /// The committed outcome visible to the retry when its duplicate guard ran.
    async fn historical_keyed_transaction(&self, database: &str, key: &str, retry_time: Timestamp, submitted_at: Timestamp, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<Option<TransactionKey>> {
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let timestamp = |time: Timestamp| Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let arguments = vec![timestamp(retry_time), timestamp(submitted_at), Value::Utf8(key.to_owned())];
        let prepared = match self.outcome_query(OutcomeQuery::Historical, &context, &arguments) {
            Ok(prepared) => prepared,
            Err(error) if error.code == "xtdb/table-not-found" || error.code.contains("unknown-table") => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut cursor = prepared.execute(context, arguments).await?;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            let (Some(id), Some(system_time)) = (outcome_id(outcome_cell(batch, 0))?, outcome_time(outcome_cell(batch, 1))?) else {
                return Err(Anomaly::fault("rtdb.node/idempotency-result", "Stored idempotency outcome has no transaction key"));
            };
            return Ok(Some(TransactionKey { tx_id: TxId(id), system_time }));
        }
        Ok(None)
    }
    async fn placed_outcome(&self, database: &str, attempt: PlacedAttempt, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<Option<PlacedDecision>> {
        let basis = QueryBasis { snapshot: TimeBasis::default(), replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let arguments = vec![Value::Utf8(attempt.outcome_key())];
        let prepared = match self.outcome_query(OutcomeQuery::Placed, &context, &arguments) {
            Ok(prepared) => prepared,
            Err(error) if error.code == "xtdb/table-not-found" || error.code.contains("unknown-table") => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut cursor = prepared.execute(context, arguments).await?;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            if let (Some(tx_id), Some(system_time), Some(committed)) = (
                outcome_id(outcome_cell(batch, 0))?, outcome_time(outcome_cell(batch, 1))?, outcome_committed(outcome_cell(batch, 2))?,
            )
                && tx_id == attempt.position().0
            {
                return Ok(Some(PlacedDecision { key: TransactionKey { tx_id: TxId(tx_id), system_time }, committed }));
            }
            return Err(bad_attempt_outcome());
        }
        Ok(None)
    }

    async fn check_placed_receipt_window(&self, database: &str, submitted_at: Timestamp, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<()> {
        let basis = QueryBasis { snapshot: TimeBasis::default(), replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let installed_time = match &self.factory.decision_clock {
            Some(clock) => {
                context.check_cancelled()?;
                let sample = clock.observe().await?;
                context.check_cancelled()?;
                Some(sample.instant)
            }
            None => {
                let [installed] = context.catalog.database(database)?.partitions.as_slice() else {
                    return Err(bad_attempt_outcome());
                };
                match installed.tx_basis() {
                    Some(latest) => rtdb_tx::installed_receipt_time(context, latest).await?,
                    None => None,
                }
            }
        };
        if installed_time.is_some_and(|value| value.0 >= submitted_at.0.saturating_add(rtdb_tx::RECEIPT_WINDOW_MICROS)) {
            return Err(Anomaly::incorrect("rtdb/await-token-expired", "The transaction attempt receipt has expired"));
        }
        Ok(())
    }

    async fn redeem_placed_attempt(&self, db: &Database, attempt: PlacedAttempt, submitted_at: Timestamp, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<(TransactionResult, String)> {
        let codec = db.await_authority.codec().ok_or_else(invalid_token)?;
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
                    // Search the authentic closed prefix before reporting an unresolved attempt.
                    let offset = cancellation.run_until_cancelled(recovery.confirm()).await.ok_or_else(cancelled)?
                        .map_err(|error| receipt_replica_behind(db.name()).with_context("cause", error.code.into()))?;
                    let recovered = ReplicaPosition::from_epoch_offset(db.logs.replica_epoch, offset.0)?;
                    db.await_installed(|prefix| prefix.replica >= recovered, cancellation).await?
                }
            }
            _ => return Err(invalid_token()),
        };
        self.check_placed_receipt_window(db.name(), submitted_at, identity, placement, cancellation).await?;
        let outcome = match db.progress().current().placed_outcome(attempt) {
            Some(outcome) => Some(outcome),
            None => self.placed_outcome(db.name(), attempt, identity, placement, cancellation).await?,
        };
        let Some(outcome) = outcome else {
            return Err(match db.await_authority {
                AwaitAuthority::Checkpoint { .. } => receipt_checkpoint_behind(db.name()),
                _ if epoch < current => Anomaly::incorrect("rtdb/attempt-unresolved", "The closed writer epoch has no outcome for this transaction attempt"),
                _ => Anomaly::new(AnomalyCategory::Unavailable, "rtdb/attempt-pending", "The transaction attempt has no installed outcome"),
            });
        };
        let result = self.await_transaction_result(db.name(), outcome.key.tx_id, Some(outcome), identity, placement, cancellation).await?;
        let token = PlacedToken::Applied { attempt, through }.seal(codec)?;
        Ok((result, token))
    }

    pub async fn await_transaction(&self, database: &str, tx_id: TxId, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.await_transaction_result(database, tx_id, None, identity, None, cancellation).await
    }

    async fn await_transaction_result(&self, database: &str, tx_id: TxId, expected: Option<PlacedDecision>, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        self.ensure_indexing()?;
        let db = self.attached_database(database, placement)?;
        if expected.is_none() && !matches!(db.await_authority, AwaitAuthority::SourcePosition) { return Err(invalid_token()); }
        let latest = db.replication.await_tx(tx_id, cancellation).await?
            .filter(|result| expected.is_none_or(|outcome| outcome.key == result.tx_key()));
        if expected.zip(latest.as_deref()).is_some_and(|(outcome, result)| outcome.committed != result.committed()) {
            return Err(bad_attempt_outcome());
        }
        if let Some(result) = &latest && !result.error().is_some_and(is_historical_guard_abort) {
            if !db.external_source()
                && let Some(original) = result.error().map(rtdb_tx::private_replay_original).transpose()?.flatten()
            {
                return Ok(TransactionResult::Committed { tx_key: original });
            }
            if !db.external_source()
                && let Some(original) = result.error().map(rtdb_tx::bounded_retry_original).transpose()?.flatten()
            {
                return self.read_bounded_outcome(database, tx_id, original, identity, placement, cancellation).await;
            }
            return Ok(if db.external_source() { result.as_ref().clone() } else { answer_duplicate(result.as_ref().clone()) });
        }
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let arguments = vec![Value::I64(tx_id.0)];
        let mut cursor = self.outcome_query(OutcomeQuery::Await, &context, &arguments)?.execute(context, arguments).await?;
        let batch = cursor.next_batch().await?.ok_or_else(|| Anomaly::fault("xtdb/tx-await-missing", format!("tx {tx_id} not found after await")))?;
        let schema = batch.batch().schema();
        let cell = |name: &str| -> Result<ArrowCell<'_>> {
            let index = schema.index_of(name).map_err(|error| Anomaly::fault("rtdb.node/tx-result-schema", error.to_string()))?;
            Ok(outcome_cell(batch.batch(), index))
        };
        let system_time = outcome_time(cell("system_time")?)?
            .ok_or_else(|| Anomaly::fault("rtdb.node/tx-result-time", "Stored transaction result has no microsecond system-time"))?;
        let tx_key = TransactionKey { tx_id, system_time };
        let committed = outcome_committed(cell("committed")?)?;
        if expected.is_some_and(|outcome| outcome.key != tx_key || committed != Some(outcome.committed)) {
            return Err(bad_attempt_outcome());
        }
        match committed {
            Some(true) => Ok(TransactionResult::Committed { tx_key }),
            Some(false) => {
                let error = if let Some(TransactionResult::Aborted { error, .. }) = latest.as_deref() {
                    error.clone()
                } else {
                    outcome_error(cell("error")?, &self.factory.memory, || {
                        Anomaly::fault("rtdb.node/tx-result-error", "Stored transaction error is not Transit")
                    })?
                };
                if !db.external_source()
                    && let Some(error) = &error
                    && let Some(original) = rtdb_tx::private_replay_original(error)?
                {
                    return Ok(TransactionResult::Committed { tx_key: original });
                }
                if !db.external_source()
                    && let Some(error) = &error
                    && let Some(original) = rtdb_tx::bounded_retry_original(error)?
                {
                    return self.read_bounded_outcome(database, tx_id, original, identity, placement, cancellation).await;
                }
                if !db.external_source()
                    && let Some(error) = &error
                    && is_historical_guard_abort(error)
                    && let Some(original) = rtdb_tx::attributed_historical_guard_original(error)?
                {
                    return Ok(TransactionResult::Committed { tx_key: original });
                }
                if !db.external_source()
                    && let Some(error) = &error
                    && is_historical_guard_abort(error)
                    && let Some(key) = outcome_metadata_key(cell("user_metadata")?)?
                    && let Some(submitted_at) = historical_guard_sample(error)
                    && let Some(original) = self.historical_keyed_transaction(database, key.as_ref(), system_time, submitted_at, identity, placement, cancellation).await?
                {
                    return Ok(TransactionResult::Committed { tx_key: original });
                }
                let result = TransactionResult::Aborted { tx_key, error };
                Ok(if db.external_source() { result } else { answer_duplicate(result) })
            }
            _ => Err(Anomaly::fault("rtdb.node/tx-result-status", "Stored transaction result has no commit status")),
        }
    }
    async fn read_bounded_outcome(&self, database: &str, retry: TxId, original: TransactionKey, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<TransactionResult> {
        let invalid = || Anomaly::fault("rtdb.node/bounded-retry-outcome", "The bounded retry has no matching original outcome");
        if original.tx_id >= retry { return Err(invalid()); }
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, replica_capture: None, resume_token: None, has_await_floor: false, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, CatalogReadCut::Current, cancellation).await?;
        let arguments = vec![Value::I64(original.tx_id.0)];
        let mut cursor = self.outcome_query(OutcomeQuery::Bounded, &context, &arguments)?.execute(context, arguments).await?;
        while let Some(batch) = cursor.next_batch().await? {
            let batch = batch.batch();
            if batch.num_rows() == 0 { continue; }
            if outcome_time(outcome_cell(batch, 0))? != Some(original.system_time) { return Err(invalid()); }
            return match outcome_committed(outcome_cell(batch, 1))? {
                Some(true) => Ok(TransactionResult::Committed { tx_key: original }),
                Some(false) => {
                    let error = outcome_error(outcome_cell(batch, 2), &self.factory.memory, invalid)?;
                    Ok(TransactionResult::Aborted { tx_key: original, error })
                }
                _ => Err(invalid()),
            };
        }
        Err(invalid())
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
    region: Option<Arc<rtdb_types::OwnedRangeEvidence>>,
}

impl OwnedRangeWitness {
    pub fn operation(self) -> TxOp {
        TxOp::OwnedCheckRange {
            relation: self.relation, basis: self.basis, scope: self.scope, frontier: self.frontier, until: self.until, region: self.region,
        }
    }

    pub fn retained_bytes(&self) -> usize {
        self.region.as_ref().map_or(0, |region| region.retained_bytes())
    }

    pub fn same_range(&self, other: &Self) -> bool { self.region == other.region && self.until == other.until }

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
    region: Option<OwnedRangeWitness>,
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
        if let Some(region) = &self.region {
            self.held.check_relation(self.relation)?;
            Ok(Some(region.clone()))
        } else if self.at_head {
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
    receiver_observation: Option<rtdb_tx::DecisionObservation>,
    witness_scope: Option<rtdb_types::PlacedAttemptScope>,
    applied_scope: Option<rtdb_types::PlacedAttemptScope>,
    applied_preparation: Option<Arc<PreparedAppliedCatalog>>,
    witness_until: Timestamp,
    fork_until: Timestamp,
    cancellation: CancellationToken,
    session_cancellation: CancellationToken,
    historical: Option<historical_owned_read::Scope>,
    scoped_root: Option<BTreeSet<rtdb_types::OwnedRelation>>,
    attachment: std::sync::Weak<Database>,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

pub struct HeldSpatialRange {
    held_context: std::sync::Weak<QueryContext>,
    matches: rtdb_spatial::PreparedRange,
}

pub struct HeldSpatialProbe {
    held_context: std::sync::Weak<QueryContext>,
    matches: rtdb_spatial::PreparedProbe,
}

pub struct HeldSpatialNearest {
    held_context: std::sync::Weak<QueryContext>,
    ranking: rtdb_spatial::PreparedNearest,
}

pub struct OwnedReplayWorld {
    pub world: rtdb_types::OwnedWorld,
    pub source_cut: Timestamp,
    pub destination_parent: TransactionKey,
    _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

impl OwnedReadSession {
    pub fn present(&self) -> TransactionKey { self.present }

    pub fn is_applied(&self) -> bool { self.applied_scope.is_some() }

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
            receiver_observation: self.receiver_observation,
            witness_scope: self.witness_scope, applied_scope: None, applied_preparation: None, witness_until: self.witness_until, fork_until: self.fork_until,
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

    /// A completed point can bind the exact identity region at its original held cut.
    pub async fn entity_region_witness(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        valid: Interval,
    ) -> Result<Option<OwnedRangeWitness>> {
        self.require_scoped_relation(relation)?;
        let Some(mut evidence) = self.relation_witness(relation)? else { return Ok(None); };
        let anchor = self.covered_range_anchor(relation, evidence.frontier).await?;
        evidence.region = Some(Arc::new(rtdb_types::OwnedRangeEvidence::Entity {
            anchor, region: rtdb_types::OwnedEntityRegion { id: id.clone(), valid },
        }));
        Ok(Some(evidence))
    }

    pub fn is_scoped_root(&self) -> bool {
        self.scoped_root.is_some()
    }

    pub fn scoped_lifetime_point_witness(
        &self, relation: &rtdb_types::OwnedRelation, expiry: Option<Timestamp>,
    ) -> Result<Option<OwnedRangeWitness>> {
        if !self.is_scoped_root() { return Ok(None); }
        self.relation_witness_for(relation, true, expiry)
    }

    pub async fn fork_cut(&self, scope: rtdb_types::PlacedAttemptScope) -> Result<OwnedForkCut> {
        self.require_unscoped_root()?;
        self.admit_access().await?;
        let attached_scope = self.attachment.upgrade()
            .and_then(|database| database.await_authority.codec().map(|codec| codec.scope()));
        if self.is_applied() || attached_scope != Some(scope) || self.node.now() >= self.fork_until {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        if let Some(historical) = &self.historical {
            self.check_relations(&historical.relations)?;
        }
        Ok(OwnedForkCut {
            root_point: None,
            public_root: false,
            exact_retained: false,
            basis: self.system_time(),
            live: self.historical.is_none().then_some(self.present),
            issued_on: self.present,
            publication: None,
            retained_world: None,
            renewal: false,
            until: self.fork_until,
            scope,
        })
    }

    pub async fn root_point_at(&self, valid_at: Timestamp, public: bool) -> Result<Option<rtdb_types::OwnedPointVersion>> {
        self.require_unscoped_root()?;
        self.admit_access().await?;
        let relation = rtdb_types::OwnedRelation::new(if public {
            rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION
        } else {
            rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
        })?;
        rtdb_query::read_owned_point_version(Arc::clone(&self.context), &self.owner, &relation,
            &rtdb_types::EntityId::String("head".into()), Interval::at(valid_at)?).await
    }

    /// Retain the application publication seen by this original live hold.
    /// The placed archive resolves its full transaction key.
    pub async fn fork_publication_cut(&self, scope: rtdb_types::PlacedAttemptScope) -> Result<OwnedForkCut> {
        let mut cut = self.fork_cut(scope).await?;
        let head = cut.live.take().ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let publication = rtdb_query::read_owned_root_publication(Arc::clone(&self.context), &self.owner)
            .await?.filter(|publication| publication.tx_id <= head.tx_id && publication.system_time <= head.system_time)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        self.check()?;
        cut.basis = publication.system_time;
        cut.publication = Some(publication);
        Ok(cut)
    }

    /// Admit a pin at the current public version of the caller's original hold.
    pub async fn fork_public_version_cut(
        &self, scope: rtdb_types::PlacedAttemptScope, publication: TransactionKey,
    ) -> Result<OwnedForkCut> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let mut cut = self.fork_cut(scope).await?;
        let head = cut.live.take().ok_or_else(unavailable)?;
        let relation = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION)?;
        if !publication.at_or_before(head)
            || self.lookup_publication(&relation, OwnedPublicationLookup::Latest).await?
                .is_none_or(|(_, selected)| selected != publication)
        {
            return Err(unavailable());
        }
        self.check()?;
        cut.basis = publication.system_time;
        cut.publication = Some(publication);
        cut.public_root = true;
        Ok(cut)
    }

    /// Certify an exact original publication for a new Held or retained world.
    pub async fn fork_root_publication_at(
        &self, scope: rtdb_types::PlacedAttemptScope, basis: Timestamp, mode: WorldClockRequest,
    ) -> Result<OwnedForkCut> {
        let mut cut = self.fork_publication_cut(scope).await?;
        if basis > cut.basis() {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let context = self.context.with_owned_system_cut(basis, &self.context.cancellation)?;
        let publication = rtdb_query::read_owned_root_publication(context, &self.owner).await?
            .filter(|publication| publication.system_time == basis && publication.tx_id <= cut.issued_on.tx_id)
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        self.check()?;
        cut.basis = publication.system_time;
        cut.publication = Some(publication);
        cut.exact_retained = matches!(mode, WorldClockRequest::Retained);
        Ok(cut)
    }

    /// Certifies the original Held publication for an independently retained fork.
    pub async fn fork_overlay_cut(
        &self, scope: rtdb_types::PlacedAttemptScope, world: &rtdb_types::OwnedWorld, publication: TransactionKey,
        archived: Option<&rtdb_query::ArchivedHeldClock>,
    ) -> Result<OwnedForkCut> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let mut cut = self.fork_cut(scope).await?;
        if cut.live.is_none() {
            return Err(unavailable());
        }
        if !(1..=32).contains(&world.schemas.relations().count()) { return Err(unavailable()); }
        let clock = match (world.ancestor_covering(publication), archived) {
            (Some(ancestor), Some(archived)) if world.mode != rtdb_types::WorldClockMode::Proposal
                && ancestor.basis.tx_id < self.present.tx_id && archived.publication() == publication =>
                archived.reading(ancestor, publication.system_time)?,
            (None, None) if matches!(world.mode, rtdb_types::WorldClockMode::Held { .. }) => {
                let relations = world.schemas.relations().map(|(name, _)| rtdb_types::OwnedRelation::new(name))
                    .collect::<Result<Vec<_>>>()?;
                self.certify_world_clock_at(world, &relations, publication).await?
            },
            _ => return Err(unavailable()),
        };
        self.check_overlay_standing(world).await?;
        cut.live = None;
        cut.basis = publication.system_time;
        cut.retained_world = Some(RetainedWorldCut::Overlay(world.id, world.pin, publication, clock));
        cut.until = cut.until.min(world.until);
        Ok(cut)
    }

    pub async fn fork_inherited_root_cut(
        &self,
        scope: rtdb_types::PlacedAttemptScope,
        world: &rtdb_types::OwnedWorld,
    ) -> Result<OwnedForkCut> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let mut cut = self.fork_cut(scope).await?;
        if cut.live.is_none_or(|live| world.basis.system_time > live.system_time || world.basis.tx_id >= live.tx_id)
            || !world.ancestors.is_empty() || world.dropped
            || world.mode == rtdb_types::WorldClockMode::Proposal
            || world.parent_publication.is_none()
            || !(1..=32).contains(&world.schemas.relations().count())
        {
            return Err(unavailable());
        }
        self.check_overlay_standing(world).await?;
        cut.live = Some(world.basis);
        cut.basis = world.basis.system_time;
        cut.retained_world = Some(RetainedWorldCut::Parent(world.id, world.pin));
        cut.until = cut.until.min(world.until);
        Ok(cut)
    }

    pub async fn check_overlay_standing(&self, world: &rtdb_types::OwnedWorld) -> Result<()> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        for (name, _) in world.schemas.relations() {
            let relation = rtdb_types::OwnedRelation::new(name)?;
            if self.world_parent_witness(world.id, &relation).await?.purge_frontier() != 0 {
                return Err(unavailable());
            }
        }
        Ok(())
    }

    pub async fn fork_renewal_cut(
        &self, scope: rtdb_types::PlacedAttemptScope, predecessor: TransactionKey,
    ) -> Result<OwnedForkCut> {
        let mut cut = self.fork_cut(scope).await?;
        if cut.live.is_none_or(|live| predecessor.system_time > live.system_time || predecessor.tx_id >= live.tx_id) {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        cut.live = Some(predecessor);
        cut.basis = predecessor.system_time;
        cut.renewal = true;
        Ok(cut)
    }

    pub fn system_time(&self) -> Timestamp {
        self.historical.as_ref().map_or(self.present.system_time, |scope| scope.publication.system_time)
    }

    /// The original publication selected by a historical Root hold.
    pub fn historical_publication(&self) -> Option<TransactionKey> {
        self.historical.as_ref().map(|scope| scope.publication)
    }

    pub fn query_time(&self) -> Result<Timestamp> { self.context.current_time.as_micros() }

    pub fn receiver_observation(&self) -> Result<rtdb_tx::DecisionObservation> {
        self.check()?;
        self.receiver_observation.ok_or_else(hosted_source_unavailable)
    }

    /// Recover clock evidence only from an attempt assertion on this original Root cut.
    pub async fn owned_attempt_observation(&self, key: TransactionKey) -> Result<Option<rtdb_tx::OrderedClockEvidence>> {
        self.require_unscoped_root()?;
        self.admit_access().await?;
        if self.historical.is_some() || self.applied_scope.is_some() || key.tx_id > self.present.tx_id {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        if self.node.factory.decision_clock.is_none() { return Ok(None); }
        ordered_commit_observation(Arc::clone(&self.context), key).await
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

    /// The held receipt names the exact destination archive cut.
    pub async fn replayed_parent_cut(
        &self, binding: [u8; 32], original: TransactionKey, scope: rtdb_types::PlacedAttemptScope,
    ) -> Result<(OwnedForkCut, Timestamp)> {
        let archive = self.replay_parent_hold(binding, false).await?;
        let id = format!("native-transaction/{}", original.tx_id.0);
        let (body, destination) = self.replay_parent_control(&id).await?
            .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
        let logical = rtdb_tx::replayed_parent_logical_time(&body, binding, &archive, original)?;
        if destination.system_time <= original.system_time || destination.tx_id >= self.present.tx_id
            || !destination.at_or_before(self.present)
        {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let mut cut = self.fork_cut(scope).await?;
        cut.basis = destination.system_time;
        cut.live = None;
        cut.publication = Some(destination);
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

    async fn replay_parent_control(&self, id: &str) -> Result<Option<(Vec<u8>, TransactionKey)>> {
        use arrow_array::{Array, BinaryArray};
        use rtdb_types::{EntityId, TableRef};

        let point = rtdb_query::read_hosted_control_point(
            Arc::clone(&self.context), TableRef::new("public", "bj_rtdb_restore_hold"),
            &EntityId::String(id.to_owned()), true,
        ).await?;
        self.check()?;
        point.row.map(|(row, system_time)| {
            let position = point.receipt_tx_id.ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
            let bytes = row.batch().column_by_name("binding")
                .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .filter(|bytes| bytes.is_valid(0) && bytes.value(0).len() <= 4096)
                .ok_or_else(rtdb_types::OwnedWorld::unavailable)?;
            Ok((bytes.value(0).to_vec(), TransactionKey { tx_id: rtdb_types::TxId(position), system_time }))
        }).transpose()
    }

    pub async fn world_registration(&self, world: rtdb_types::Iid) -> Result<Option<(rtdb_types::OwnedWorld, Arc<rtdb_columnar::MemoryReservation>)>> {
        Ok(self.world_registration_with_revision(world).await?
            .map(|(record, _, charge)| (record, charge)))
    }

    pub async fn world_registration_with_revision(
        &self, world: rtdb_types::Iid,
    ) -> Result<Option<(rtdb_types::OwnedWorld, Timestamp, Arc<rtdb_columnar::MemoryReservation>)>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let record = rtdb_query::read_owned_world_with_revision(Arc::clone(&self.context), &self.owner, world).await?;
        self.check()?;
        Ok(record)
    }

    /// Tests whether the ordered decision installed this exact world and pin.
    pub async fn registered_world_at(
        &self, world: &rtdb_types::OwnedWorld, at: TransactionKey,
    ) -> Result<bool> {
        self.require_unscoped_root()?;
        if !at.at_or_before(self.present) { return Err(rtdb_types::OwnedWorld::unavailable()); }
        self.admit_read().await?;
        let record = rtdb_query::read_owned_world_at(
            Arc::clone(&self.context), &self.owner, world.id, at.system_time,
        ).await?;
        self.check()?;
        Ok(record.is_some_and(|(record, revision, _)| &record == world && revision == at.system_time))
    }

    /// Reads the exact local shadow, retaining its revision even when that shadow is a tombstone.
    /// Certifies the exact physical revision of a protected writable-world clock publication.
    pub async fn certify_world_accepted_cut<'a>(
        &'a self, world: &'a rtdb_types::OwnedWorld, relation: &'a rtdb_types::OwnedRelation,
        system: Timestamp,
    ) -> Result<OwnedWorldAcceptedCut<'a>> {
        self.certify_world_accepted_scope(world, std::slice::from_ref(relation), system).await?;
        Ok(OwnedWorldAcceptedCut { held: self, world: world.id, relation, system })
    }

    /// Certify the same protected publication against every explicitly selected relation.
    pub async fn certify_world_accepted_scope(
        &self, world: &rtdb_types::OwnedWorld, relations: &[rtdb_types::OwnedRelation], system: Timestamp,
    ) -> Result<Timestamp> {
        Ok(self.certify_world_clock_publication(world, relations, system).await?.1)
    }

    pub async fn certify_world_clock_publication(
        &self, world: &rtdb_types::OwnedWorld, relations: &[rtdb_types::OwnedRelation], system: Timestamp,
    ) -> Result<(TransactionKey, Timestamp)> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        if system <= world.inherited_system() { return Err(unavailable()); }
        let context = self.admit_world_accepted_scope(world, relations, system).await?;
        let at = context.with_owned_system_cut(system, &context.cancellation)?;
        let (reading, publication) = rtdb_query::read_owned_world_clock_publication(at, &self.owner, world.id)
            .await?.filter(|(_, publication)| publication.system_time == system).ok_or_else(unavailable)?;
        if self.read_certified_world_clock(&context, world, relations, publication).await? != reading {
            return Err(unavailable());
        }
        Ok((publication, reading))
    }

    pub async fn certify_world_clock_at(
        &self, world: &rtdb_types::OwnedWorld, relations: &[rtdb_types::OwnedRelation],
        publication: TransactionKey,
    ) -> Result<Timestamp> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        if world.inherited_position().tx_id >= publication.tx_id
            || !world.inherited_position().at_or_before(publication)
        {
            return Err(unavailable());
        }
        let context = self.admit_world_accepted_scope(world, relations, publication.system_time).await?;
        self.read_certified_world_clock(&context, world, relations, publication).await
    }

    async fn read_certified_world_clock(
        &self, context: &Arc<QueryContext>, world: &rtdb_types::OwnedWorld,
        relations: &[rtdb_types::OwnedRelation], publication: TransactionKey,
    ) -> Result<Timestamp> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let at = context.with_owned_transaction_cut(publication, &context.cancellation)?;
        let (registration, _schema) = rtdb_query::read_owned_world(Arc::clone(&at), &self.owner, world.id)
            .await?.ok_or_else(unavailable)?;
        let (reading, _) = rtdb_query::read_owned_world_clock_publication(at, &self.owner, world.id)
            .await?.filter(|(reading, event)| *event == publication && world.admits_clock_publication(*reading, publication.system_time))
            .ok_or_else(unavailable)?;
        if &registration != world { return Err(unavailable()); }
        self.check_relations(relations)?;
        Ok(reading)
    }

    /// Seek the protected clock history by logical reading over original positions.
    pub async fn select_world_accepted_scope(
        &self, world: &rtdb_types::OwnedWorld, relations: &[rtdb_types::OwnedRelation], requested: Timestamp,
    ) -> Result<Option<(TransactionKey, Timestamp)>> {
        let context = self.admit_world_accepted_scope(world, relations, self.present.system_time).await?;
        let selected = rtdb_query::select_owned_world_clock(
            &context, &self.owner, world.id, world.inherited_position(), world.initial_clock(), requested,
        ).await?;
        if let Some((publication, reading)) = selected {
            self.admit_world_accepted_scope(world, relations, publication.system_time).await?;
            if self.read_certified_world_clock(&context, world, relations, publication).await? != reading {
                return Err(rtdb_types::OwnedWorld::unavailable());
            }
        } else {
            self.admit_world_accepted_scope(world, relations, world.inherited_system()).await?;
        }
        Ok(selected)
    }

    async fn admit_world_accepted_scope(
        &self, world: &rtdb_types::OwnedWorld, relations: &[rtdb_types::OwnedRelation], system: Timestamp,
    ) -> Result<Arc<QueryContext>> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        self.require_unscoped_root()?;
        self.admit_read().await?;
        if !(1..=32).contains(&relations.len()) || !world.mode.is_writable() || world.dropped
            || relations.iter().any(|relation| !relation.is_root_data() || world.schemas.relation(relation)
                .is_none_or(|schema| schema.has_opaque_columns() || schema.lifetime_micros().is_some()))
            || system < world.inherited_system() || system > self.present.system_time
        {
            return Err(unavailable());
        }
        let (current, _schema) = self.world_registration(world.id).await?.ok_or_else(unavailable)?;
        if &current != world { return Err(unavailable()); }
        self.world_merged_changes(world.id, relations, system, system, 1).await?;
        self.guard_context(Arc::clone(&self.context), relations)
    }

    pub async fn world_application_publication(
        &self, world: rtdb_types::Iid,
    ) -> Result<Option<(Timestamp, TransactionKey)>> {
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let publication = rtdb_query::read_owned_world_clock_publication(
            Arc::clone(&self.context), &self.owner, world,
        ).await?;
        self.check()?;
        Ok(publication)
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
        self.require_unscoped_root()?;
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let context = if system == self.present.system_time { context }
            else { context.with_owned_system_cut(system, &context.cancellation)? };
        self.read_world_shadow_in_context(context, world, relation, id, valid_at, system, observed).await
    }

    async fn read_world_shadow_in_context(
        &self, context: Arc<QueryContext>, world: rtdb_types::Iid, relation: &rtdb_types::OwnedRelation,
        id: &rtdb_types::EntityId, valid_at: Timestamp, system: Timestamp, observed: bool,
    ) -> Result<OwnedWorldShadowRead> {
        let valid = rtdb_types::Interval::at(valid_at)?;
        let root = relation;
        let relation = root.in_world(world)?;
        let mut row = None;
        let mut local_revision = None;
        let mut local_version = None;
        if let Some(mut cursor) = rtdb_query::open_owned_assertion_point(
            Arc::clone(&context), &self.owner, &relation, id, Some(valid), false,
        ).await? {
            while let Some(batch) = cursor.next_batch().await? {
                if batch.batch().num_rows() == 0 { continue; }
                if row.is_some() || batch.batch().num_rows() != 1 {
                    return Err(rtdb_types::OwnedWorld::unavailable());
                }
                let revision = rtdb_query::owned_row_revision(batch.batch(), 0, id, &self.context.memory)?;
                if revision > system { return Err(rtdb_types::OwnedWorld::unavailable()); }
                local_revision = Some(revision);
                if observed {
                    let position = rtdb_query::owned_row_event_position(batch.batch(), 0)?;
                    local_version = Some(rtdb_types::OwnedPointVersion { history: revision, position });
                }
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
                observation: OwnedWorldObservation::Point { id: id.clone(), valid_at, revision: local_version },
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
            self.check_point_purge_floor(relation, id, scope.publication.system_time).await?;
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
        if relation.is_world_catalog() {
            rtdb_query::open_owned_point(context, &self.owner, relation, id, valid, None).await
        } else {
            rtdb_query::open_owned_assertion_point(context, &self.owner, relation, id, valid, lifetime.is_some()).await
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

    pub async fn open_historical_position_point(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        system: Timestamp, valid: Timestamp, mask: (Timestamp, u32),
    ) -> Result<Option<QueryCursor>> {
        let context = self.position_read_context(relation, id, system, mask.0).await?;
        let valid = rtdb_types::Interval::new(valid, valid.next()?)?;
        rtdb_query::open_owned_point_with_position_mask(context, &self.owner, relation, id, valid, system, mask).await
    }

    pub async fn open_historical_position_valid_during<'a>(
        &'a self,
        relation: &'a rtdb_types::OwnedRelation,
        id: &rtdb_types::EntityId,
        valid: rtdb_types::Interval,
        system: Timestamp,
        mask: (Timestamp, u32),
    ) -> Result<OwnedValidHistory<'a>> {
        let context = self.position_read_context(relation, id, system, mask.0).await?;
        let cursor = rtdb_query::open_owned_valid_during_with_position_mask(
            context, &self.owner, relation, id, valid, system, mask,
        ).await?;
        self.check_relation(relation)?;
        Ok(OwnedValidHistory {
            held: self, relation, cursor, at_head: false, region: None, complete: false, failed: false,
        })
    }

    async fn position_read_context(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        system: Timestamp, commit: Timestamp,
    ) -> Result<Arc<QueryContext>> {
        self.admit_read().await?;
        self.require_scoped_relation(relation)?;
        if !relation.is_root_data() || system.max(commit) > self.present.system_time {
            return Err(historical_owned_read::unavailable());
        }
        let context = self.relation_context(relation)?;
        let (schema, _charge) = rtdb_query::read_owned_relation_schema(Arc::clone(&context), &self.owner, relation)
            .await?.ok_or_else(historical_owned_read::unavailable)?;
        if schema.has_opaque_columns() || schema.lifetime_micros().is_some() {
            return Err(historical_owned_read::unavailable());
        }
        self.certify_historical_cut(relation, system.min(commit)).await?;
        let purge = self.point_purge_guard(relation, id, system.min(commit), true).await?;
        Ok(context.with_owned_read_guard(purge))
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
        valid: Interval,
        system: Timestamp,
    ) -> Result<OwnedValidHistory<'a>> {
        self.open_valid_during(relation, id, valid, system, false).await
    }

    pub async fn open_valid_during_region<'a>(
        &'a self,
        relation: &'a rtdb_types::OwnedRelation,
        id: &rtdb_types::EntityId,
        valid: Interval,
        system: Timestamp,
    ) -> Result<OwnedValidHistory<'a>> {
        self.open_valid_during(relation, id, valid, system, true).await
    }

    async fn open_valid_during<'a>(
        &'a self, relation: &'a rtdb_types::OwnedRelation, id: &rtdb_types::EntityId,
        valid: Interval, system: Timestamp, exact_region: bool,
    ) -> Result<OwnedValidHistory<'a>> {
        let at_head = self.admit_root_system(relation, system).await?;
        if !at_head {
            self.check_point_purge_floor(relation, id, system).await?;
        }
        let region = if at_head && exact_region {
            self.entity_region_witness(relation, id, valid).await?
        } else {
            None
        };
        let cursor = rtdb_query::open_owned_valid_during_at(
            self.relation_context(relation)?, &self.owner, relation, id, valid, system,
        ).await?;
        self.check_relation(relation)?;
        Ok(OwnedValidHistory {
            held: self, relation, cursor, at_head, region, complete: false, failed: false,
        })
    }

    /// Computes the visible corpus of one covered clear-text path at this read's cut.
    #[cfg(feature = "search")]
    pub async fn search_corpus(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation, query: &str,
    ) -> Result<rtdb_text::Corpus> {
        self.admit_read().await?;
        let work = rtdb_search::owned_corpus(self.relation_context(relation)?, &self.owner, relation, path, query);
        let corpus = self.context.reading(work).await?;
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
        let (batch, witnesses) = self.ranked_search_batch(partitions, query, offset, limit, observe, valid, overlay).await?;
        Ok((batch.into_page()?, witnesses))
    }

    #[cfg(feature = "search")]
    pub async fn ranked_search_batch(
        &self, partitions: &[(rtdb_types::OwnedRelation, rtdb_types::OwnedRelation)], query: &str,
        offset: usize, limit: usize, observe: bool, valid: Option<Timestamp>, overlay: Option<rtdb_search::HeldTextCorpus>,
    ) -> Result<(rtdb_search::OwnedSearchBatch, Vec<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let work = rtdb_search::owned_ranked_overlay_batch(self.guard_context(Arc::clone(&self.context), partitions.iter().map(|(relation, _)| relation))?, &self.owner, partitions, query, offset, limit, valid, overlay);
        let hits = self.context.reading(work).await?;
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

    /// Ranks the selected clear vectors by exact cosine distance at this read's cut.
    #[cfg(feature = "search")]
    pub async fn nearest_batch(
        &self, partitions: &[rtdb_search::NearestPartition], query: &rtdb_search::NearestQuery,
        observe: bool, valid: Option<Timestamp>, overlay: Option<rtdb_search::HeldVectorOverlay>,
    ) -> Result<(rtdb_search::OwnedSearchBatch, Vec<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let relations = || partitions.iter().map(|partition| &partition.relation);
        let work = rtdb_search::owned_nearest_batch(self.guard_context(Arc::clone(&self.context), relations())?, &self.owner, partitions, query, valid, overlay);
        let hits = self.context.reading(work).await?;
        let mut witnesses = Vec::with_capacity(partitions.len());
        if observe {
            for relation in relations() {
                if let Some(witness) = self.relation_witness(relation)? {
                    witnesses.push(witness);
                }
            }
        }
        self.check_relations(relations())?;
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

    pub async fn spatial_path_ready(&self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation) -> Result<bool> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let work = Box::pin(rtdb_spatial::path_ready(Arc::clone(&context), &self.owner, relation, path));
        let ready = context.reading(work).await?;
        self.check_relation(relation)?;
        Ok(ready)
    }

    /// Evaluates a spatial path at this held cut.
    pub async fn spatial(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, observe: bool,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        self.spatial_projected(relation, path, request, observe, rtdb_spatial::Projection::Identities).await
    }

    /// Evaluates a spatial path and returns the requested Arrow projection at this held cut.
    pub async fn spatial_projected(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, observe: bool, projection: rtdb_spatial::Projection<'_>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        self.spatial_page(relation, path, request, observe, projection, None).await
    }

    pub async fn spatial_page(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, observe: bool, projection: rtdb_spatial::Projection<'_>,
        prepared: Option<&rtdb_spatial::PreparedRegion>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let work = Box::pin(async {
            if let Some(prepared) = prepared {
                rtdb_spatial::read_prepared(Arc::clone(&context), &self.owner, relation, path, prepared, request, projection).await
            } else {
                rtdb_spatial::read_held(Arc::clone(&context), &self.owner, relation, path, request, projection).await
            }
        });
        let page = context.reading(work).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((page, witness))
    }

    pub async fn open_spatial_range(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, observe: bool, projection: rtdb_spatial::Projection<'_>,
        region: &rtdb_spatial::PreparedRegion,
    ) -> Result<(HeldSpatialRange, rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let lease = context.catalog.read_lease();
        let work = async {
            let matches = rtdb_spatial::PreparedRange::prepare(
                Arc::clone(&context), &self.owner, relation, path, region, &request,
            ).await?;
            let after = match request.page {
                rtdb_spatial::PageOrder::Identity { after } => after,
                rtdb_spatial::PageOrder::NearestOnce => unreachable!("validated range order"),
            };
            let page = matches.page(Arc::clone(&context), &self.owner, after, request.limit, projection).await?;
            Ok::<_, Anomaly>((matches, page))
        };
        let (matches, page) = rtdb_storage::IoRegion::retaining(context.clone(), rtdb_storage::IoRegion::leased(lease, work)).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((HeldSpatialRange { held_context: Arc::downgrade(&self.context), matches }, page, witness))
    }

    pub async fn next_spatial_range(
        &self, held: &HeldSpatialRange, after: rtdb_columnar::EntityIdRef<'_>,
        limit: usize, observe: bool, projection: rtdb_spatial::Projection<'_>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        if !held.held_context.upgrade().is_some_and(|original| Arc::ptr_eq(&original, &self.context)) {
            return Err(Anomaly::incorrect("rtdb.owned/spatial-hold", "Spatial range belongs to another read"));
        }
        self.admit_read().await?;
        let relation = held.matches.relation();
        let context = self.relation_context(relation)?;
        let lease = context.catalog.read_lease();
        let work = held.matches.page(Arc::clone(&context), &self.owner, Some(after), limit, projection);
        let page = rtdb_storage::IoRegion::retaining(context, rtdb_storage::IoRegion::leased(lease, work)).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((page, witness))
    }

    pub async fn open_spatial_probe(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, observe: bool, columns: &[&str],
        region: &rtdb_spatial::PreparedRegion, probe: rtdb_spatial::Probe<'_>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>, Option<HeldSpatialProbe>)> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let work = Box::pin(rtdb_spatial::open_probe(Arc::clone(&context), &self.owner, relation,
            path, region, request, columns, probe));
        let (page, matches) = context.reading(work).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        let retained = matches.map(|matches| HeldSpatialProbe { held_context: Arc::downgrade(&self.context), matches });
        Ok((page, witness, retained))
    }

    pub async fn next_spatial_probe(
        &self, held: &HeldSpatialProbe, after: rtdb_columnar::EntityIdRef<'_>,
        limit: usize, observe: bool, columns: &[&str], probe: rtdb_spatial::Probe<'_>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        if !held.held_context.upgrade().is_some_and(|original| Arc::ptr_eq(&original, &self.context)) {
            return Err(Anomaly::incorrect("rtdb.owned/spatial-hold", "Spatial probe belongs to another read"));
        }
        self.admit_read().await?;
        let relation = held.matches.relation();
        let context = self.relation_context(relation)?;
        let work = Box::pin(held.matches.page(Arc::clone(&context), &self.owner, after, limit, columns, probe));
        let page = context.reading(work).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((page, witness))
    }

    pub async fn spatial_probe_page(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, observe: bool, columns: &[&str],
        prepared: &rtdb_spatial::PreparedRegion, probe: rtdb_spatial::Probe<'_>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let work = Box::pin(rtdb_spatial::read_probe(Arc::clone(&context), &self.owner, relation,
            path, prepared, request, columns, probe));
        let page = context.reading(work).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((page, witness))
    }

    pub async fn gather_spatial_document(
        &self, relation: &rtdb_types::OwnedRelation, id: rtdb_columnar::EntityIdRef<'_>,
        revision: Timestamp, columns: &[&str],
    ) -> Result<rtdb_spatial::Page> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let work = Box::pin(rtdb_spatial::gather_document(Arc::clone(&context), &self.owner,
            relation, self.system_time(), id, revision, columns));
        let page = context.reading(work).await?;
        self.check_relation(relation)?;
        Ok(page)
    }

    pub async fn open_spatial_nearest(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        request: rtdb_spatial::Request<'_>, after_radius: Option<f64>, observe: bool, projection: rtdb_spatial::Projection<'_>,
    ) -> Result<(HeldSpatialNearest, rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        self.admit_read().await?;
        let context = self.relation_context(relation)?;
        let limit = request.limit;
        let work = Box::pin(async {
            let ranking = rtdb_spatial::PreparedNearest::prepare(Arc::clone(&context), &self.owner, relation, path, request, after_radius).await?;
            let page = ranking.page(Arc::clone(&context), &self.owner, None, limit, projection).await?;
            Ok::<_, Anomaly>((ranking, page))
        });
        let (ranking, page) = context.reading(work).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((HeldSpatialNearest { held_context: Arc::downgrade(&self.context), ranking }, page, witness))
    }

    pub async fn next_spatial_nearest(
        &self, held: &HeldSpatialNearest, after: rtdb_spatial::NearestPosition<'_>,
        limit: usize, observe: bool, projection: rtdb_spatial::Projection<'_>,
    ) -> Result<(rtdb_spatial::Page, Option<OwnedRangeWitness>)> {
        if !held.held_context.upgrade().is_some_and(|original| Arc::ptr_eq(&original, &self.context)) {
            return Err(Anomaly::incorrect("rtdb.owned/spatial-hold", "Spatial ranking belongs to another read"));
        }
        self.admit_read().await?;
        let relation = held.ranking.relation();
        let context = self.relation_context(relation)?;
        let work = Box::pin(held.ranking.page(Arc::clone(&context), &self.owner, Some(after), limit, projection));
        let page = context.reading(work).await?;
        let witness = if observe { self.relation_witness(relation)? } else { None };
        self.check_relation(relation)?;
        Ok((page, witness))
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
            evidence.region = self.fixed_range_evidence(relation, path, evidence.frontier, valid_at, lower, upper)
                .await.unwrap_or(None).map(rtdb_types::OwnedRangeEvidence::FixedPath).map(Arc::new);
        }
        self.check_relation(relation)?;
        Ok((page, evidence))
    }

    /// A live lifetime page certifies its release horizon at held present.
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
        let expiry = if let Some(batch) = &page.batch {
            let batch = batch.batch();
            match rtdb_query::try_owned_lifetime_range_expiry(&context, &self.owner, relation, batch, valid, duration).await {
                Ok(Some(expiry)) => Some(expiry),
                // A batch can encounter failures in IID order. Reopen the
                // points in page order to preserve the first refusal.
                _ => self.lifetime_range_expiry_by_point(&context, relation, batch, valid, duration).await?,
            }
        } else { None };
        self.check_lifetime_range_release(relation, expiry)?;
        let evidence = self.relation_witness_for(relation, true, expiry)?;
        Ok((page, evidence, expiry))
    }

    async fn lifetime_range_expiry_by_point(
        &self, context: &Arc<QueryContext>, relation: &rtdb_types::OwnedRelation,
        batch: &arrow_array::RecordBatch, valid: Interval, duration: i64,
    ) -> Result<Option<Timestamp>> {
        let present = valid.lower();
        let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(lifetime_read_unavailable)?;
        let ids = batch.column(column).as_ref();
        let mut expiry: Option<Timestamp> = None;
        for row in 0..batch.num_rows() {
            context.check_cancelled()?;
            let size = rtdb_columnar::estimate_read_value_bytes(field, ids, row)?;
            let _identity = context.memory.reserve(size.saturating_add(4096))?;
            let id = owned_range_id(ColumnReader::new(field)?.read_entity_id(ids, row)?)?;
            let mut point = rtdb_query::open_owned_point(
                Arc::clone(context), &self.owner, relation, &id, Some(valid), Some(&["_id", "_system_from", "_valid_from", "_valid_to"]),
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
        Ok(expiry)
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
            until: expiry.map_or(self.witness_until, |expiry| expiry.min(self.witness_until)), region: None,
        }))
    }

    async fn fixed_range_evidence(
        &self, relation: &rtdb_types::OwnedRelation, path: &rtdb_types::OwnedRelation,
        frontier: i64, valid_at: Timestamp,
        lower: Option<crate::OwnedKeyBound<'_>>, upper: Option<crate::OwnedKeyBound<'_>>,
    ) -> Result<Option<rtdb_types::FixedRangeEvidence>> {
        use rtdb_types::FixedRangeEvidence;
        let rule = rtdb_query::read_owned_fixed_range_rule(Arc::clone(&self.context), &self.owner, relation).await?;
        let Some(rule) = rule.filter(|rule| rule.valid_at == valid_at && rule.path == path.name()) else { return Ok(None); };
        let revision = self.covered_range_anchor(relation, frontier).await?;
        let lower = lower.map(|bound| bound.fixed_key(&rule.column)).transpose()?;
        let upper = upper.map(|bound| bound.fixed_key(&rule.column)).transpose()?;
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
                let id = owned_range_id(read_execution_entity_id_as(None, field, array, row)?)?;
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
            if system != scope.publication.system_time {
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

fn owned_range_id(id: Option<EntityIdRef<'_>>) -> Result<EntityId> {
    id.map_or_else(|| Value::Null.entity_id(), |id| Ok(id.into_owned()))
}

impl Drop for OwnedReadSession {
    fn drop(&mut self) { self.close(); }
}

/// A certified earlier coordinate tied to the original hold and relation.
pub struct OwnedWorldAcceptedCut<'a> {
    held: &'a OwnedReadSession,
    world: rtdb_types::Iid,
    relation: &'a rtdb_types::OwnedRelation,
    system: Timestamp,
}

impl OwnedWorldAcceptedCut<'_> {
    pub fn system_time(&self) -> Timestamp { self.system }

    pub async fn boundaries(
        &self, id: &rtdb_types::EntityId, valid: rtdb_types::Interval,
    ) -> Result<Option<QueryCursor>> {
        rtdb_query::open_owned_valid_boundaries(
            self.held.relation_context(self.relation)?, &self.held.owner,
            &self.relation.in_world(self.world)?, id, valid, self.system,
        ).await
    }

    pub async fn overlay_range(
        &self, path: &rtdb_types::OwnedRelation, request: crate::OwnedRangeRequest<'_>, valid: Timestamp,
    ) -> Result<crate::OwnedRangePage> {
        let context = self.held.relation_context(self.relation)?;
        let context = context.with_owned_system_cut(self.system, &context.cancellation)?;
        let overlay = self.relation.in_world(self.world)?;
        let result = crate::ordered_index::owned_range(
            context, &self.held.owner, &overlay, path,
            crate::ordered_index::RowBounds::Compound(request), Some(valid),
        ).await?;
        self.held.check_relation(self.relation)?;
        Ok(result)
    }

    pub async fn overlay_identities(
        &self, valid: Timestamp, after: Option<&str>, fetch: usize,
    ) -> Result<rtdb_query::OwnedIdentityCandidates> {
        self.held.world_identity_candidates_at(self.world, self.relation, self.system, valid, after, fetch).await
    }

    pub async fn shadow(
        &self, id: &rtdb_types::EntityId, valid: Timestamp,
    ) -> Result<OwnedWorldShadowRead> {
        self.held.read_world_shadow_at(self.world, self.relation, id, valid, self.system, false).await
    }
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
    Point { id: rtdb_types::EntityId, valid_at: Timestamp, revision: Option<rtdb_types::OwnedPointVersion> },
    Relation { local_frontier: Option<i64> },
}

impl OwnedWorldWitness {
    pub fn purge_frontier(&self) -> i64 { self.purge_frontier }

    pub fn matches_scope(&self, scope: rtdb_types::PlacedAttemptScope) -> bool { self.scope == scope }

    pub fn is_absent_point(
        &self, relation: &rtdb_types::OwnedRelation, id: &rtdb_types::EntityId, at: Option<Timestamp>,
    ) -> bool {
        self.relation.root_relation().as_ref() == Some(relation)
            && matches!(&self.observation,
                OwnedWorldObservation::Point { id: observed, valid_at, revision: None }
                    if observed == id && at.is_none_or(|at| at == *valid_at))
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

#[cfg(test)]
#[test]
fn world_witness_matches_only_its_original_history_and_writer_epoch() {
    use rtdb_types::{Iid, OwnedRelation, PlacedAttemptScope};

    let world = Iid([1; 16]);
    let scope = PlacedAttemptScope::new("history-a", 1).unwrap();
    let witness = OwnedWorldWitness {
        world,
        pin: Iid([2; 16]),
        relation: OwnedRelation::new("facts").unwrap().in_world(world).unwrap(),
        observation: OwnedWorldObservation::Relation { local_frontier: Some(0) },
        scope,
        until: Timestamp(42),
        purge_frontier: 0,
    };
    assert!(witness.matches_scope(scope));
    assert!(!witness.matches_scope(PlacedAttemptScope::new("history-a", 2).unwrap()));
    assert!(!witness.matches_scope(PlacedAttemptScope::new("history-b", 1).unwrap()));
}

#[cfg(test)]
#[test]
fn world_create_absence_matches_the_observed_valid_instant() {
    use rtdb_types::{EntityId, Iid, OwnedRelation, PlacedAttemptScope};

    let world = Iid([1; 16]);
    let relation = OwnedRelation::new("facts").unwrap();
    let id = EntityId::String("one".into());
    let witness = OwnedWorldWitness {
        world,
        pin: Iid([2; 16]),
        relation: relation.in_world(world).unwrap(),
        observation: OwnedWorldObservation::Point { id: id.clone(), valid_at: Timestamp(12), revision: None },
        scope: PlacedAttemptScope::new("history", 1).unwrap(),
        until: Timestamp(42),
        purge_frontier: 0,
    };
    assert!(witness.is_absent_point(&relation, &id, Some(Timestamp(12))));
    assert!(!witness.is_absent_point(&relation, &id, Some(Timestamp(11))));
}

pub struct OwnedForkCut {
    root_point: Option<rtdb_types::OwnedPointVersion>,
    public_root: bool,
    exact_retained: bool,
    basis: Timestamp,
    live: Option<TransactionKey>,
    issued_on: TransactionKey,
    publication: Option<TransactionKey>,
    retained_world: Option<RetainedWorldCut>,
    renewal: bool,
    until: Timestamp,
    scope: rtdb_types::PlacedAttemptScope,
}

#[derive(Clone, Copy)]
enum RetainedWorldCut {
    Parent(rtdb_types::Iid, rtdb_types::Iid),
    Overlay(rtdb_types::Iid, rtdb_types::Iid, TransactionKey, Timestamp),
}

impl OwnedForkCut {
    pub fn basis(&self) -> Timestamp { self.basis }
    pub fn original_root_point(&mut self, point: Option<rtdb_types::OwnedPointVersion>) {
        self.root_point = point;
    }
    pub fn live(&self) -> Option<TransactionKey> { self.live }
    pub fn is_publication(&self) -> bool { self.publication.is_some() }
    pub fn positioned_basis(&self) -> Option<TransactionKey> {
        match self.retained_world {
            Some(RetainedWorldCut::Overlay(_, _, publication, _)) => Some(publication),
            _ => self.publication.or(self.live),
        }
    }
    pub fn overlay(&self) -> Option<(rtdb_types::Iid, rtdb_types::Iid, Timestamp)> {
        match self.retained_world {
            Some(RetainedWorldCut::Overlay(world, pin, _, clock)) => Some((world, pin, clock)),
            Some(RetainedWorldCut::Parent(..)) | None => None,
        }
    }
    pub fn retained_parent(&self) -> Option<(rtdb_types::Iid, rtdb_types::Iid)> {
        match self.retained_world {
            Some(RetainedWorldCut::Parent(world, pin)) => Some((world, pin)),
            Some(RetainedWorldCut::Overlay(..)) | None => None,
        }
    }
    pub fn is_renewal(&self) -> bool { self.renewal }
    pub fn matches_basis(&self, basis: TransactionKey) -> bool {
        self.basis == basis.system_time && self.positioned_basis().is_none_or(|original| original == basis)
            && basis.tx_id <= self.issued_on.tx_id && basis.system_time <= self.issued_on.system_time
    }
    pub fn admits_overlay(&self, media: TransactionKey, selected: TransactionKey) -> bool {
        let Some(RetainedWorldCut::Overlay(_, _, publication, _)) = self.retained_world else { return false; };
        publication == selected && selected.at_or_before(media) && media.at_or_before(self.issued_on)
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

/// Local admission never submitted. Ordered results have a known disposition.
pub enum OwnedWorldRegistrationOutcome {
    Refused(Anomaly),
    Ordered(TransactionResult),
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
                .is_some_and(|publication| publication.has_single_fact(|op| matches!(op, TxOp::OwnedDelete { .. })))
    }
}


#[derive(Default)]
pub struct OwnedRootSection {
    pub clock_slot: Option<rtdb_tx::OwnedClockSlot>,
    pub proposal_overlay: bool,
    pub proposal_land: bool,
    pub refusal: Vec<rtdb_tx::HostedControlWrite>,
    pub proposal_close: bool,
    pub checks: Vec<TxOp>,
    pub publication: Option<OwnedRootPublication>,
}

#[derive(Default)]
pub struct OwnedWorldObservations {
    pub checks: Vec<OwnedWorldWitness>,
    pub clocks: Vec<(rtdb_types::WorldClockLimit, Timestamp)>,
}

pub struct OwnedRootPublication {
    pub attempt: String,
    pub writes: Vec<TxOp>,
}

impl OwnedRootPublication {
    fn has_single_fact(&self, admit: impl FnOnce(&TxOp) -> bool) -> bool {
        let mut facts = self.writes.iter().filter(|op| !rtdb_tx::is_root_partition_frontier(op) && !rtdb_tx::is_public_root_version_marker(op));
        facts.next().is_some_and(admit) && facts.next().is_none()
    }
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
    TimedOverlay { at: Timestamp, writes: Vec<OwnedWorldWrite> },
    Clock(Timestamp),
    Drop,
}

pub struct OwnedWorldWrite {
    pub relation: rtdb_types::OwnedRelation,
    pub id: rtdb_types::EntityId,
    pub shadow: rtdb_types::WorldShadow,
    pub create_only: bool,
    pub valid: Option<rtdb_types::Interval>,
    pub ordered_keys: Option<Vec<u8>>,
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

pub struct NativeSession {
    pub(crate) state: Session,
    pub(crate) service: Arc<NativeSessionService>,
    applied_preparation: Option<Arc<PreparedAppliedCatalog>>,
}
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
            TxOp::owned_deadline(deadline),
            TxOp::OwnedSweepExpired { relation, id, created, expiry, scope },
        ], Default::default()).await
    }

    pub async fn register_owned_world_checked(
        &mut self, record: &rtdb_types::OwnedWorld, fork: OwnedWorldFork,
    ) -> Result<OwnedWorldRegistrationOutcome> {
        let (ops, _charge) = match self.checked_world_registration(record, fork) {
            Ok(prepared) => prepared,
            Err(error) => return Ok(OwnedWorldRegistrationOutcome::Refused(error)),
        };
        self.state.execute_tx(ops, Default::default()).await.map(OwnedWorldRegistrationOutcome::Ordered)
    }

    fn checked_world_registration(
        &self, record: &rtdb_types::OwnedWorld, fork: OwnedWorldFork,
    ) -> Result<(Vec<TxOp>, Arc<rtdb_columnar::MemoryReservation>)> {
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
        let held_publication = matches!(fork.requested, WorldClockRequest::Held { start: None })
            && fork.cut.is_publication() && fork.root.is_some() && fork.previous.is_none();
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
            || (root != OwnedForkRoot::None && !fork.cut.is_renewal() && !held_publication
                && !fork.cut.exact_retained && record.parent_publication == Some(record.basis.system_time));
        let effects = fork.controls.len()
            .saturating_add(fork.root.as_ref().map_or(0, |section| section.checks.len()
                .saturating_add(if section.refusal.is_empty() { 0 } else { section.refusal.len() + 3 })))
            .saturating_add(fork.root.as_ref().and_then(|section| section.publication.as_ref()).map_or(0, |root| root.writes.len() + 1))
            .saturating_add(3 + usize::from(requires_tip));
        let overlay_source = fork.cut.overlay();
        let parent_source = fork.cut.retained_parent();
        let retaining_child = root == OwnedForkRoot::None && fork.previous.is_none()
            && fork.sibling.is_none();
        let can_seed_root = matches!(fork.requested, WorldClockRequest::Retained | WorldClockRequest::Held { start: None });
        let matches_archive = match (record.ancestor(), overlay_source) {
            (None, None) => fork.cut.matches_basis(record.basis)
                && parent_source.is_none_or(|(source, _)| retaining_child && can_seed_root && record.id != source),
            (Some(ancestor), Some((source, _, clock))) => record.id != source && ancestor.logical == clock
                && retaining_child && can_seed_root
                && fork.cut.admits_overlay(ancestor.basis, ancestor.publication),
            _ => false,
        };
        if record.id != fork.world || record.pin != fork.pin || !matches_archive
            || ((fork.previous.is_none() || fork.sibling.is_some()) != (record.pin == record.id)) || fork.controls.is_empty()
            || fork.previous.is_some_and(|pin| pin == record.pin || (record.mode.is_writable() && !proposal_hold))
            || record.mode != requested
            || (held_publication && !record.is_held_root_fork())
            || (requires_live_tip && fork.cut.live.is_none())
            || (fork.cut.is_publication() && (root == OwnedForkRoot::None
                || record.parent_publication != Some(fork.cut.basis())))
            || (fork.cut.exact_retained && (requested != rtdb_types::WorldClockMode::Retained
                || root != OwnedForkRoot::Controls || fork.previous.is_some()
                || fork.root.as_ref().is_some_and(|section| !section.checks.is_empty() || !section.refusal.is_empty())))
            || effects > 4096 || !fork.controls.iter().any(rtdb_tx::HostedControlWrite::is_check)
            || (root == OwnedForkRoot::None && fork.controls.iter().any(|control| !control.is_check()))
            || ((held_publication || matches!(root, OwnedForkRoot::Controls | OwnedForkRoot::ProposalControls | OwnedForkRoot::ProposalWrites))
                && fork.controls.iter().all(rtdb_tx::HostedControlWrite::is_check))
            || (root == OwnedForkRoot::WritesAdvance && !fork.cut.is_publication())
            || (fork.cut.is_renewal() && !matches!(root, OwnedForkRoot::WritesRenewal | OwnedForkRoot::WritesSibling | OwnedForkRoot::ProposalHold | OwnedForkRoot::ProposalShorten))
            || fork.sibling.is_some_and(|sibling| sibling == record.id || fork.previous.is_none()
                || root != OwnedForkRoot::WritesSibling)
            || (root != OwnedForkRoot::None && ((fork.previous.is_some() && !matches!(root, OwnedForkRoot::WritesAdvance | OwnedForkRoot::WritesRenewal | OwnedForkRoot::WritesSibling | OwnedForkRoot::ProposalHold | OwnedForkRoot::ProposalShorten))
                || (requested != rtdb_types::WorldClockMode::Retained && !proposal_publication && !proposal_hold && !held_publication)
                || (fork.cut.live.is_none() && !fork.cut.is_publication())))
            || fork.root.iter().flat_map(|section| &section.checks).any(|check| match check {
                TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. }
                | TxOp::OwnedCheckCapacity { relation, .. } =>
                    !relation.is_root_data(),
                _ => true,
            })
            || fork.root.as_ref().and_then(|section| section.publication.as_ref()).is_some_and(|root| {
                root.attempt != fork.attempt || root.writes.is_empty() || root.writes.iter().any(|write| match write {
                    _ if rtdb_tx::is_root_partition_frontier(write) || rtdb_tx::is_public_root_version_marker(write) => false,
                    TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                    | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedRelativePeriod { relation, .. } | TxOp::OwnedDelete { relation, .. } => !relation.is_root_data(),
                    _ => true,
                })
            })
            || self.placed_history()? != Some(fork.cut.scope)
        {
            return Err(rtdb_types::OwnedWorld::unavailable());
        }
        let charge = self.service.node.memory().reserve(effects * (std::mem::size_of::<TxOp>() + 512))?;
        let mut ops = Vec::with_capacity(effects);
        ops.push(TxOp::owned_deadline(until));
        let refusal = if let Some(section) = fork.root {
            ops.extend(section.checks);
            ops.extend(section.publication.into_iter().flat_map(|root| root.writes));
            section.refusal
        } else {
            Vec::new()
        };
        ops.extend(fork.controls.into_iter().map(TxOp::HostedControl));
        if requires_tip {
            if fork.cut.root_point.map(|version| version.history) != record.parent_publication
                || (fork.cut.is_publication() && fork.cut.root_point.is_none_or(|version|
                    version.position.transaction != record.basis.tx_id))
            {
                return Err(rtdb_types::OwnedWorld::unavailable());
            }
            ops.push(TxOp::OwnedCheck {
                relation: rtdb_types::OwnedRelation::new(if fork.cut.public_root {
                    rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION
                } else {
                    rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
                })?,
                id: rtdb_types::EntityId::String("head".into()),
                valid_at: record.basis.system_time,
                expected: fork.cut.root_point,
            });
        }
        let marker = (!refusal.is_empty())
            .then(|| self.decision_attempt_marker(&fork.attempt, fork.fingerprint)).transpose()?;
        ops.push(TxOp::OwnedForkWorld { record: record.record(), previous: fork.previous, sibling: fork.sibling,
            overlay_source: parent_source.or_else(|| overlay_source.map(|(world, pin, _)| (world, pin))),
            attempt: fork.attempt, fingerprint: fork.fingerprint, root });
        if let Some(marker) = marker {
            ops.push(TxOp::HostedConstraintRefusal);
            ops.push(marker);
            ops.push(self.decision_receipt_head()?);
            ops.extend(refusal.into_iter().map(TxOp::HostedControl));
        }
        Ok((ops, charge))
    }

    fn decision_attempt_marker(&self, attempt: &str, fingerprint: [u8; 32]) -> Result<TxOp> {
        let relation = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ATTEMPT_RELATION)?;
        let documents = rtdb_tx::Documents::new(
            vec![rtdb_tx::attempt_marker_document(attempt, fingerprint)], self.service.node.memory(),
        )?;
        Ok(TxOp::OwnedInsertDocs {
            relation, documents, valid_from: Some(Timestamp::MIN), valid_to: Some(Timestamp::MAX),
        })
    }

    fn decision_receipt_head(&self) -> Result<TxOp> {
        let relation = rtdb_types::OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
        let documents = rtdb_tx::Documents::new(vec![rtdb_types::StructFields::from([
            ("_id".into(), rtdb_types::Value::Utf8("sync_receipt_head".to_owned())),
        ])], self.service.node.memory())?;
        Ok(TxOp::OwnedReplaceDocs {
            relation, documents, valid_from: Some(Timestamp::MIN), valid_to: Some(Timestamp::MAX),
        })
    }

    pub async fn commit_owned_world_batch(
        &mut self, sections: Vec<(Timestamp, OwnedWorldSection)>,
        root: OwnedRootSection, observed: OwnedWorldObservations, fingerprint: [u8; 32],
        controls: Vec<rtdb_tx::HostedControlWrite>, deadline: Timestamp,
    ) -> Result<TransactionResult> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let terminal = sections.first().filter(|(_, section)| sections.len() == 1
            && matches!(&section.effect, OwnedWorldEffect::Drop)
            && section.checks.is_empty() && section.expected_clock.is_none());
        let delete_root = !root.proposal_close && terminal.is_some_and(|(_, section)| root.publication.as_ref().is_some_and(|root| {
            root.attempt == section.attempt && root.has_single_fact(|op| matches!(op,
                TxOp::OwnedDelete { relation, valid_from: None, valid_to: None, .. } if relation.is_root_data()
            ))
        })) && rtdb_tx::retained_delete_controls(&controls);
        let retained_release = delete_root || (terminal.is_some()
            && root.checks.is_empty() && root.publication.is_none()
            && rtdb_tx::retained_release_controls(&controls));
        let proposal_overlay = root.proposal_overlay;
        let proposal_close = root.proposal_close;
        let proposal_land = root.proposal_land || proposal_close;
        if (root.proposal_land && root.refusal.len() != 2)
            || (!root.refusal.is_empty() && !(proposal_land || proposal_overlay || delete_root))
        {
            return Err(unavailable());
        }
        let refusal = if !root.refusal.is_empty() {
            let attempt = &sections.first().ok_or_else(unavailable)?.1.attempt;
            Some(self.decision_attempt_marker(attempt, fingerprint)?)
        } else {
            None
        };
        if proposal_land && (sections.len() != 1 || proposal_overlay
            || !matches!(&sections[0].1.effect, OwnedWorldEffect::Drop)
            || (root.proposal_land && proposal_close)
            || if proposal_close {
                !sections[0].1.checks.is_empty()
                    || root.publication.as_ref().is_none_or(|root| root.attempt != sections[0].1.attempt
                        || !root.has_single_fact(|op| matches!(op,
                            TxOp::OwnedDelete { relation, valid_from: None, valid_to: None, .. } if relation.is_root_data()
                        )))
                    || !rtdb_tx::proposal_close_controls(&controls)
            } else {
                sections[0].1.expected_clock.is_none()
                    || root.publication.as_ref().is_none_or(|root| {
                        if root.attempt != sections[0].1.attempt { return true; }
                        let mut facts = root.writes.iter().filter(|op| {
                            !rtdb_tx::is_proposal_landing_frontier(op) && !rtdb_tx::is_root_partition_frontier(op) && !rtdb_tx::is_public_root_version_marker(op)
                        });
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
                OwnedWorldEffect::Overlay(_) => !rtdb_tx::proposal_overlay_controls(&controls, true),
                OwnedWorldEffect::SettleProposal => !rtdb_tx::proposal_overlay_controls(&controls, false),
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
                TxOp::OwnedCheckCapacity { relation, .. } => relation.is_world_catalog()
                    || relation.world().is_some_and(|world| !proposal_overlay
                        || sections.first().is_none_or(|(_, section)| section.world != world)),
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
        let mut effects = controls.len().saturating_add(root.checks.len()).saturating_add(2 + usize::from(root.clock_slot.is_some()));
        if let Some(root) = &root.publication {
            if root.writes.is_empty() || root.writes.iter().any(|write| match write {
                _ if rtdb_tx::is_root_partition_frontier(write) || rtdb_tx::is_public_root_version_marker(write) || proposal_land && rtdb_tx::is_proposal_landing_frontier(write) => false,
                TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedRelativePeriod { relation, .. } | TxOp::OwnedDelete { relation, .. } => !relation.is_root_data(),
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
                OwnedWorldEffect::Overlay(writes) | OwnedWorldEffect::TimedOverlay { writes, .. } => {
                    if writes.is_empty() { return Err(unavailable()); }
                    writes.len()
                },
                OwnedWorldEffect::Clock(_) | OwnedWorldEffect::Drop | OwnedWorldEffect::SettleProposal => 0,
            };
            let standing = !proposal_overlay && matches!(&section.effect,
                OwnedWorldEffect::Overlay(_) | OwnedWorldEffect::TimedOverlay { .. }
                    | OwnedWorldEffect::Clock(_) | OwnedWorldEffect::Drop);
            if !worlds.insert(section.world)
                || (section.checks.is_empty() && !standing && !retained_release && !terminal_proposal && !proposal_close)
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
        let observed_worlds = observed.checks.iter().map(|check| check.world)
            .chain(observed.clocks.iter().map(|(clock, _)| clock.world))
            .collect::<std::collections::BTreeSet<_>>();
        if !observed_worlds.is_empty()
            && (!observed_worlds.is_disjoint(&worlds) || worlds.len() + observed_worlds.len() > 8
                || retained_release || proposal_land || proposal_overlay)
        {
            return Err(unavailable());
        }
        effects = effects.saturating_add(observed.checks.len().saturating_mul(2))
            .saturating_add(observed.clocks.len());
        if refusal.is_some() {
            effects = effects.saturating_add(root.refusal.len() + 3);
        }
        if effects > 4096 || now >= until { return Err(unavailable()); }
        let _operations = self.service.node.memory().reserve(effects * (std::mem::size_of::<TxOp>() + 512))?;
        let mut ops = Vec::with_capacity(effects);
        ops.push(TxOp::owned_deadline(until));
        ops.push(TxOp::OwnedCheckWorldScope { scope, clock: None });
        ops.extend(root.clock_slot.map(|slot| TxOp::OwnedClockSlot { slot }));
        ops.extend(controls.into_iter().map(TxOp::HostedControl));
        ops.extend(root.checks);
        if !observed_worlds.is_empty() {
            self.append_hosted_world_read_checks(&mut ops, observed)?;
        }
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
                    self.append_world_writes(&mut ops, section.world, writes)?;
                    (None, false, None)
                }
                OwnedWorldEffect::TimedOverlay { at, writes } => {
                    self.append_world_writes(&mut ops, section.world, writes)?;
                    (Some(at), false, None)
                }
                OwnedWorldEffect::Clock(to) => (Some(to), false, None),
                OwnedWorldEffect::Drop => (None, true, None),
                OwnedWorldEffect::SettleProposal => (None, false, Some(section.parent_pin)),
            };
            ops.push(TxOp::OwnedPublishWorld { world: section.world, parent_pin: section.parent_pin, attempt: section.attempt,
                fingerprint, expected: section.expected_clock, advance_to, drop_world,
                settlement_pin,
                land_root: proposal_land.then_some(section.parent_pin),
                delete_root: delete_root.then_some(section.parent_pin) });
        }
        if let Some(marker) = refusal {
            ops.push(TxOp::HostedConstraintRefusal);
            ops.push(marker);
            if !root.proposal_land {
                ops.push(self.decision_receipt_head()?);
            }
            ops.extend(root.refusal.into_iter().map(TxOp::HostedControl));
        }
        self.state.execute_tx(ops, Default::default()).await
    }

    pub fn append_hosted_world_read_checks(
        &self, ops: &mut Vec<TxOp>, observed: OwnedWorldObservations,
    ) -> Result<()> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let scope = self.placed_history()?.ok_or_else(unavailable)?;
        let scoped = ops.iter().any(|op| matches!(op,
            TxOp::OwnedCheckWorldScope { scope: selected, clock: None } if *selected == scope
        ));
        let Some(TxOp::OwnedDeadline { until, .. }) = ops.first_mut() else {
            return Err(unavailable());
        };
        let OwnedWorldObservations { checks, clocks } = observed;
        let now = self.service.node.now();
        let mut worlds = std::collections::BTreeSet::new();
        for check in &checks {
            if check.scope != scope || check.relation.world() != Some(check.world) {
                return Err(unavailable());
            }
            worlds.insert(check.world);
            *until = (*until).min(check.until);
        }
        let mut bounds = std::collections::BTreeMap::<_, rtdb_types::WorldClockLimit>::new();
        for (clock, expires) in clocks {
            worlds.insert(clock.world);
            *until = (*until).min(expires);
            let bound = bounds.entry(clock.world).or_insert(clock);
            if bound.pin != clock.pin {
                return Err(unavailable());
            }
            bound.before = bound.before.min(clock.before);
        }
        if !(1..=8).contains(&worlds.len()) || checks.len() > 8 * 128 || now >= *until {
            return Err(unavailable());
        }
        if !scoped {
            ops.push(TxOp::OwnedCheckWorldScope { scope, clock: None });
        }
        ops.extend(bounds.into_values().map(|clock| TxOp::OwnedCheckWorldScope {
            scope, clock: Some(clock),
        }));
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
    ) -> Result<()> {
        for OwnedWorldWrite { relation, id, shadow, valid, create_only, ordered_keys } in writes {
            if create_only && (valid.is_some()
                || !matches!(&shadow, rtdb_types::WorldShadow::Present(_)))
            {
                return Err(rtdb_types::OwnedWorld::unavailable());
            }
            let relation = relation.in_world(world)?;
            let _encode = self.service.node.memory().reserve(shadow.allocation_bound()?)?;
            let mut row = shadow.record(&id)?;
            if let Some(slots) = ordered_keys {
                if rtdb_types::WorldShadow::is_tombstone(&row) {
                    return Err(rtdb_types::OwnedWorld::unavailable());
                }
                row.insert("_ordered_micros".into(), rtdb_types::Value::Binary(slots));
            }
            let documents = rtdb_tx::Documents::new(vec![row], self.service.node.memory())?;
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
        self.retain_owned_read(OwnedReadBasis::ClockReservation).await
    }

    pub async fn open_fresh_owned_read(&mut self, deployment: uuid::Uuid, required: Option<&AppliedReadFloor>) -> Result<OwnedReadSession> {
        self.retain_owned_read(OwnedReadBasis::Fresh { deployment, required }).await
    }

    /// Enrolls one nonrenewable lower catalog pin for a prepaid session before use.
    pub async fn prepare_applied_owned_reads(&mut self, floor: &AppliedReadFloor) -> Result<u64> {
        if self.service.audience_storage.is_none() || self.applied_preparation.is_some() {
            return Err(hosted_source_unavailable());
        }
        let read = self.retain_owned_read(OwnedReadBasis::Applied { floor, preparation: None }).await?;
        let preparation = PreparedAppliedCatalog::from_read(&read, &self.service.memory)?;
        let remaining = preparation.remaining_micros()?;
        self.applied_preparation = Some(Arc::new(preparation));
        Ok(remaining)
    }
    /// Retains the already-applied catalog from the current recovered hosted holder.
    pub async fn open_applied_owned_read(&mut self, floor: &AppliedReadFloor) -> Result<OwnedReadSession> {
        let preparation = if self.service.audience_storage.is_some() {
            Some(Arc::clone(self.applied_preparation.as_ref().ok_or_else(hosted_source_unavailable)?))
        } else {
            None
        };
        self.retain_owned_read(OwnedReadBasis::Applied { floor, preparation }).await
    }
    async fn retain_owned_read(
        &mut self, read_basis: OwnedReadBasis<'_>,
    ) -> Result<OwnedReadSession> {
        if !self.service.node.factory.options.owned_relations { return Err(owned_relations_disabled()); }
        let applied = match &read_basis {
            OwnedReadBasis::Applied { floor, .. } => Some(*floor),
            _ => None,
        };
        if applied.is_none() && self.service.audience_storage.is_some() { return Err(hosted_source_unavailable()); }
        let node = self.service.node.clone();
        let identity = Arc::clone(&self.service.identity);
        let database = self.state.database().to_owned();
        let owner = self.state.user().filter(|owner| !owner.is_empty()).map(str::to_owned)
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/no-owner", "Owned relations require an authenticated principal"))?;
        let charge = self.service.memory.reserve(database.capacity().checked_add(owner.capacity()).and_then(|bytes| bytes.checked_add(2048))
            .ok_or_else(|| Anomaly::incorrect("rtdb.owned/read-size", "Retained read allocation overflow"))?)?;
        let attached = node.attached_database(&database, self.service.placement.as_ref())?;
        let attached_scope = match &attached.await_authority {
            AwaitAuthority::Live { codec, .. } => Some(codec.scope()),
            _ => None,
        };
        let applied_scope = applied.map(|_| attached_scope.ok_or_else(hosted_source_unavailable)).transpose()?;
        let session_cancellation = self.state.cancellation().clone();
        let cancellation = session_cancellation.child_token();
        let attachment = Arc::downgrade(&attached);
        let applied_memory;
        let mut replica_capture = None;
        let mut floor_to_check = None;
        let mut applied_preparation = None;
        let (reserved, cut) = match read_basis {
            OwnedReadBasis::Applied { floor, ref preparation } => {
                applied_preparation = preparation.clone();
                floor_to_check = Some(floor);
                applied_memory = node.factory.options.query.query_memory(&self.service.memory);
                let admission = match applied_preparation.as_deref() {
                    Some(preparation) => AppliedCatalogAdmission::PreparedAudience(preparation),
                    None => self.service.audience_storage.as_ref().map_or(
                        AppliedCatalogAdmission::Ordinary, AppliedCatalogAdmission::PrepareAudience,
                    ),
                };
                (None, floor.cut(None, &applied_memory, admission)?)
            }
            OwnedReadBasis::Fresh { deployment, required } => {
                if attached.replica_read_deployment() != Some(deployment)
                    || deployment.is_nil() || required.is_some_and(|floor| floor.history() != deployment)
                {
                    return Err(hosted_source_unavailable());
                }
                node.admit(&database, DatabaseAccess::Read, identity.as_ref(), &cancellation).await?;
                let snapshot = attached.capture_replica_snapshot(&cancellation).await?;
                let present = snapshot.tx_basis().ok_or_else(hosted_source_unavailable)?;
                if required.is_some_and(|floor| !covers_transaction(present, floor.minimum())) {
                    return Err(hosted_source_unavailable());
                }
                replica_capture = Some(crate::ReplicaQueryCapture {
                    attachment: attachment.clone(), snapshot: Arc::new(snapshot),
                });
                floor_to_check = required;
                (None, CatalogReadCut::Current)
            }
            OwnedReadBasis::ClockReservation => {
                let present = self.reserve_ordered_present().await?;
                (Some(present.system_time), CatalogReadCut::Reserved(present))
            }
        };
        let receiver_observation = match &node.factory.decision_clock {
            Some(clock) => Some(clock.observe().await?),
            None => None,
        };
        let observed = receiver_observation.map_or_else(|| node.now(), |sample| sample.instant);
        let attached = attachment;
        let selected = reserved.or_else(|| replica_capture.as_ref().and_then(|capture| {
            capture.snapshot.tx_basis().map(|key| key.system_time)
        }));
        let basis = QueryBasis {
            snapshot: TimeBasis { databases: BTreeMap::from([(database.clone(), vec![selected.map(Into::into)])]) },
            replica_capture,
            resume_token: None,
            has_await_floor: false,
            snapshot_time: selected,
            current_time: observed.into(),
            timezone: self.state.session_timezone().to_owned(),
        };
        let context = node.query_context_in_placement(
            &database, &basis, identity.as_ref(), Some(&attached), cut, &cancellation,
        ).await?;
        let present = context.catalog.database(&database)?.partitions.first().and_then(|partition| partition.tx_basis())
            .ok_or_else(hosted_source_unavailable)?;
        if let Some(floor) = floor_to_check {
            floor.require_transactions(&context, present, None).await?;
        }
        let lease = context.catalog.read_lease();
        let witness_scope = attached_scope.filter(|_| applied_scope.is_none() && lease.as_ref().and_then(|lease| lease.deadline()).is_some());
        let clock = Arc::clone(&node.factory.clock);
        let now = clock.monotonic_micros();
        let deadline = now.saturating_add(60_000_000);
        let lease_deadline = lease.as_ref().map_or(deadline, |lease| lease.deadline().unwrap_or(now).min(deadline));
        let witness_until = if witness_scope.is_some() {
            Timestamp(observed.0.saturating_add(lease_deadline.saturating_sub(now)))
        } else {
            present.system_time
        };
        let placement = attached.clone();
        let guarded_node = node.clone();
        let guarded_database = database.clone();
        let guarded_preparation = applied_preparation.clone();
        let context = context.with_owned_read_guard(Arc::new(move || {
            if clock.monotonic_micros() >= deadline {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/read-expired", "The retained owned read expired"));
            }
            guarded_node.attached_database(&guarded_database, Some(&attached))?;
            if let Some(lease) = &lease { lease.check()?; }
            if let Some(preparation) = &guarded_preparation { preparation.check_live()?; }
            Ok(())
        }));
        context.check_cancelled()?;
        Ok(OwnedReadSession {
            context, node, identity, database, owner, present, receiver_observation, witness_scope, applied_scope, applied_preparation, witness_until, fork_until: witness_until,
            cancellation, session_cancellation, historical: None, scoped_root: None, attachment: placement, _reservation: charge,
        })
    }
    /// Reads an acceptance observation by the exact installed transaction key.
    pub async fn owned_commit_observation(&self, key: TransactionKey) -> Result<Option<rtdb_tx::OrderedClockEvidence>> {
        let node = &self.service.node;
        if node.factory.decision_clock.is_none() { return Ok(None); }
        let basis = QueryBasis { snapshot: TimeBasis::default(), replica_capture: None, resume_token: None,
            has_await_floor: false, snapshot_time: None, current_time: node.query_instant(), timezone: self.state.session_timezone().to_owned() };
        let context = node.query_context_in_placement(self.state.database(), &basis, self.service.identity.as_ref(),
            self.service.placement.as_ref(), CatalogReadCut::Current, self.state.cancellation()).await?;
        ordered_commit_observation(context, key).await
    }

    pub async fn query_context(&mut self) -> Result<Arc<QueryContext>> {
        let basis = self.state.read_basis().await?;
        Ok(self.instrument_query_context(self.service.query_context(self.state.database(), &basis, self.state.cancellation()).await?))
    }

    pub async fn owned_relation_layout(&mut self, relation: &rtdb_types::OwnedRelation) -> Result<Option<(rtdb_types::OwnedSchema, Arc<rtdb_columnar::MemoryReservation>)>> {
        if !self.service.node.factory.options.owned_relations { return Err(owned_relations_disabled()); }
        let owner = self.state.user().filter(|owner| !owner.is_empty()).map(str::to_owned)
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Forbidden, "rtdb.owned/no-owner", "Owned relations require an authenticated principal"))?;
        let context = self.query_context().await?;
        rtdb_query::read_owned_relation_schema(context, &owner, relation).await
    }
    pub async fn query_sql(&mut self, sql: &str, arguments: Vec<Value>) -> Result<QueryCursor> {
        let context = self.query_context().await?;
        let options = rtdb_sql::PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, rtdb_columnar::execution_type(&value.logical_type()))).collect(), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let (prepared, context) = crate::statement::prepare_query_at_snapshot(sql, context, &options, &arguments).await?;
        prepared.execute_column_rows(context, arguments, None).await
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

    pub async fn query_ra_arrow(&mut self, source: &str, names: &[String], arguments: &rtdb_tx::SqlArguments, cancellation: &CancellationToken) -> Result<QueryCursor> {
        let batch = arguments.single_arrow_page().filter(|batch| batch.batch().num_rows() == 1)
            .ok_or_else(|| Anomaly::incorrect("rtdb.query/parameter-row", "Logical relation arguments require one Arrow row"))?;
        let database = self.state.database().to_owned();
        let context = self.query_context_for(&database, cancellation).await?;
        let overflow = || Anomaly::incorrect("rtdb.query/parameter-size", "Logical relation parameter metadata exceeds addressable memory");
        let per_name = std::mem::size_of::<(String, (usize, LogicalType))>() * 16 + 256;
        let metadata_bytes = names.iter().zip(arguments.types()).try_fold(4096usize, |total, (name, ty)| {
            let names = name.len().checked_mul(4).ok_or_else(overflow)?;
            let types = ty.retained_bytes()?.checked_mul(16).ok_or_else(overflow)?;
            total.checked_add(per_name).and_then(|bytes| bytes.checked_add(names))
                .and_then(|bytes| bytes.checked_add(types)).ok_or_else(overflow)
        })?;
        let _metadata = context.memory.reserve(metadata_bytes)?;
        let named_parameters = names.iter().cloned().zip(arguments.execution_types()).enumerate()
            .map(|(index, (name, ty))| (name, (index, ty))).collect::<BTreeMap<_, _>>();
        if named_parameters.len() != names.len() || names.len() != batch.batch().num_columns() {
            return Err(Anomaly::incorrect("rtdb.query/parameter-row", "Logical relation arguments require unique column names"));
        }
        let options = rtdb_xtql::CompileOptions { named_parameters, default_timezone: context.timezone.clone(), ..Default::default() };
        let prepared = rtdb_xtql::compile_ra_in(source, &context.catalog, &options, &context.memory)?;
        let query = PreparedQuery::from_prepared(prepared, &context.memory)?;
        drop(options);
        drop(_metadata);
        query.execute_arrow_pages(context, batch.clone(), arguments.types()).await
    }
}

pub(crate) struct NativeSessionService {
    pub(crate) node: Node,
    pub(crate) identity: Arc<dyn SessionIdentity>,
    pub(crate) database: String,
    placement: Option<Weak<Database>>,
    memory: Arc<MemoryBudget>,
    audience_storage: Option<Arc<BufferPool>>,
}

async fn ordered_commit_observation(
    context: Arc<QueryContext>, key: TransactionKey,
) -> Result<Option<rtdb_tx::OrderedClockEvidence>> {
    let unavailable = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/no-acceptance-observation",
        "The original ordered receipt has no verified receiver clock observation");
    let args = vec![Value::I64(key.tx_id.0)];
    let options = rtdb_sql::PrepareOptions {
        parameter_types: args.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(),
        default_timezone: context.timezone.clone(),
        ..Default::default()
    };
    let mut cursor = prepare_query(
        "SELECT _id, system_time, decision_clock FROM xt.txs FOR ALL VALID_TIME WHERE _id = ? AND committed",
        &context, &options,
    )?.execute(context, args).await?;
    let batch = cursor.next_batch().await?.filter(|batch| batch.batch().num_rows() == 1).ok_or_else(unavailable)?;
    if cursor.next_batch().await?.is_some() { return Err(unavailable()); }
    let batch = batch.batch();
    if !hosted_observation::matches_transaction_result(batch, false, key)? {
        return Err(unavailable());
    }
    let Value::Binary(bytes) = read_value(batch.schema_ref().field(2), batch.column(2).as_ref(), 0)? else {
        return Err(unavailable());
    };
    rtdb_tx::OrderedClockEvidence::decode(&bytes, key.tx_id).map(Some)
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
        let registry = &self.node.sql_snapshots;
        let admission = Arc::new(crate::sql_snapshots::SqlSnapshotAdmission {
            node: self.node.clone(), registry: Arc::clone(registry), identity: Arc::clone(&self.identity),
        });
        let context = self.node.query_context_in_placement(
            database, basis, self.identity.as_ref(), self.placement.as_ref(), CatalogReadCut::Current, cancellation,
        ).await?;
        let context = if let Some(token) = &basis.resume_token {
            let view = admission.restore(token, &context, cancellation).await?;
            context.with_snapshot_view(view, token.clone())?
        } else {
            context
        };
        let context = registry.with_issuer(context, self.user(), self.node.clone(), self.placement.clone());
        Ok(if basis.has_await_floor { context } else { context.with_snapshot_admission(admission) })
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
    fn memory(&self) -> &Arc<MemoryBudget> { &self.memory }
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
    async fn fresh_query_instant(&self) -> Result<rtdb_types::Instant> {
        match &self.node.factory.decision_clock {
            Some(_) => Ok(self.node.receiver_time().await?.into()),
            None => Ok(self.node.query_instant()),
        }
    }

    async fn fresh_snapshot_basis(&self, cancellation: &CancellationToken)
        -> Result<(TimeBasis, Option<crate::ReplicaQueryCapture>)>
    {
        if !self.node.catalog.is_placed() { return Ok((self.snapshot_basis()?, None)); }
        self.node.ensure_open()?;
        let database = self.database()?;
        if !database.has_replica_read_index() { return Ok((self.snapshot_basis()?, None)); }
        let snapshot = database.capture_replica_snapshot(cancellation).await?;
        if !Arc::ptr_eq(&database, &self.database()?) {
            return Err(hosted_source_unavailable());
        }
        let time = snapshot.tx_basis().map(|key| key.system_time.into());
        let basis = TimeBasis { databases: BTreeMap::from([(self.database.clone(), vec![time])]) };
        Ok((basis, Some(crate::ReplicaQueryCapture {
            attachment: Arc::downgrade(&database), snapshot: Arc::new(snapshot),
        })))
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
        {
            self.node.check_placed_receipt_window(database, submitted_at, self.identity.as_ref(), self.placement.as_ref(), cancellation).await?;
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
fn outcome_cell(batch: &arrow_array::RecordBatch, column: usize) -> ArrowCell<'_> {
    ArrowCell { field: batch.schema_ref().field(column), array: batch.column(column).as_ref(), row: 0 }
}

fn outcome_id(cell: ArrowCell<'_>) -> Result<Option<i64>> {
    if let Some(id) = cell.int64() { return Ok(Some(id)); }
    Ok(match read_value(cell.field, cell.array, cell.row)? { Value::I64(id) => Some(id), _ => None })
}

fn outcome_time(cell: ArrowCell<'_>) -> Result<Option<Timestamp>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::TimestampMicrosecondType;
    if let Ok(physical) = cell.physical()
        && matches!(physical.field.data_type(), arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _))
        && physical.field.metadata().is_empty() && !physical.array.is_null(physical.row)
    {
        return Ok(Some(Timestamp(physical.array.as_primitive::<TimestampMicrosecondType>().value(physical.row))));
    }
    Ok(match read_value(cell.field, cell.array, cell.row)? {
        Value::Timestamp { value, unit: TimeUnit::Microsecond, .. } => Some(Timestamp(value)),
        _ => None,
    })
}

fn outcome_committed(cell: ArrowCell<'_>) -> Result<Option<bool>> {
    use arrow_array::cast::AsArray;
    if let Ok(physical) = cell.physical() && physical.field.data_type() == &arrow_schema::DataType::Boolean
        && physical.field.metadata().is_empty() && !physical.array.is_null(physical.row)
    {
        return Ok(Some(physical.array.as_boolean().value(physical.row)));
    }
    Ok(match read_value(cell.field, cell.array, cell.row)? { Value::Bool(committed) => Some(committed), _ => None })
}

fn outcome_error(cell: ArrowCell<'_>, memory: &Arc<MemoryBudget>, invalid: impl FnOnce() -> Anomaly) -> Result<Option<RetainedAnomaly>> {
    use arrow_array::cast::AsArray;
    if let Ok(physical) = cell.physical()
        && physical.field.data_type() == &arrow_schema::DataType::Binary
        && physical.field.metadata().get(rtdb_columnar::EXTENSION_NAME_KEY).is_some_and(|name| name == "xt/transit+msgpack")
        && !physical.array.is_null(physical.row)
    {
        return RetainedAnomaly::from_transit(physical.array.as_binary::<i32>().value(physical.row), memory).map(Some);
    }
    match read_value(cell.field, cell.array, cell.row)? {
        Value::Null | Value::Undefined => Ok(None),
        Value::Transit(bytes) => RetainedAnomaly::from_transit(&bytes, memory).map(Some),
        _ => Err(invalid()),
    }
}

fn outcome_metadata_key(cell: ArrowCell<'_>) -> Result<Option<std::borrow::Cow<'_, str>>> {
    if let Some(mut fields) = cell.structure()
        && let Some(key) = fields.find(|field| field.field.name() == "idempotency_key").and_then(ArrowCell::utf8)
    {
        return Ok(Some(std::borrow::Cow::Borrowed(key)));
    }
    Ok(idempotency_key(&read_value(cell.field, cell.array, cell.row)?).map(|key| std::borrow::Cow::Owned(key.to_owned())))
}

fn idempotency_key(metadata: &Value) -> Option<&str> {
    let Value::Struct(fields) = metadata else { return None };
    let Value::Utf8(key) = fields.get("idempotency_key")? else { return None };
    Some(key)
}

/// The legacy SQL parameter for keying each transaction by its position within a command.
pub const IDEMPOTENCY_KEY_PARAMETER: &str = "rtdb_idempotency_key";

pub(crate) fn with_idempotency_key(metadata: Option<rtdb_tx::UserMetadata>, key: &str, memory: &Arc<MemoryBudget>) -> Result<rtdb_tx::UserMetadata> {
    let mut fields = match metadata.as_ref().map(|metadata| metadata.value()) {
        Some(value) if idempotency_key(value).is_some()
            || matches!(value, Value::Struct(fields) if fields.contains_key(rtdb_tx::BOUNDED_RETRY_METADATA_KEY)) => return Ok(metadata.expect("present")),
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
fn applied_catalog_unretained() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/applied-catalog-unretained", "The applied catalog has no exact storage retention")
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
