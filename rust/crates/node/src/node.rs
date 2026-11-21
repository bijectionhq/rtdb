use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_format::{database::proto::DatabaseConfig, envelope::{SourceBody, SourceMessage}, log::proto::{AttachDatabase, DetachDatabase, resolved_tx::DbOp}};
use rtdb_maintenance::CollectionObserver;
use rtdb_plan::{CatalogMetrics, CatalogUsers};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryCursor};
use rtdb_storage::cache::CacheBudget;
use rtdb_tx::{RetainedAnomaly, SubmitOptions, SubmittedTx, TransactionAuthorization, TransactionResult, TxOp, prepare_submission_with_sql_options};
use rtdb_types::{Anomaly, AnomalyCategory, LogicalType, Result, SourcePosition, TimeBasis, TimeUnit, Timestamp, TransactionKey, TxBasis, TxId, Value};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{Clock, Database, DatabaseAccess, DatabaseCatalog, ExternalSources, Membership, NativeResources, NodeOptions, QueryBasis, ResourceFactory, Session, SessionIdentity, SessionService, SystemClock,
    catalog::CatalogCommand, database::{DatabaseDriver, DatabaseFactory}, statement::prepare_query};


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

#[derive(Clone)]
pub struct Node {
    catalog: Arc<DatabaseCatalog>,
    factory: Arc<DatabaseFactory>,
    commands: mpsc::Sender<CatalogCommand>,
    cancellation: CancellationToken,
    pub(crate) runtime: Arc<crate::runtime::RuntimeProviders>,
    read_fence: Option<Arc<dyn ReadFence>>,
    scan_fragmenter: Option<Arc<dyn rtdb_query::ScanFragmenter>>,
}

pub struct NodeDriver {
    node: Node,
    commands: mpsc::Receiver<CatalogCommand>,
    initial: Vec<(Arc<Database>, DatabaseDriver)>,
}

enum DatabaseCompletion { Worker { database: Arc<Database>, result: Result<()> }, Drained(Arc<Database>) }

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
        let node = Self { catalog, factory, commands, cancellation: CancellationToken::new(), runtime, read_fence, scan_fragmenter };
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
        Ok((node.clone(), NodeDriver { node, commands: receiver, initial }))
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
    async fn await_durable_source(
        &self, database: &str, identity: &dyn SessionIdentity, hosted: bool, placement: Option<&Weak<Database>>, cancellation: &CancellationToken,
    ) -> Result<Option<String>> {
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
            database.await_durable_source(cancellation).await?;
            if hosted && !Arc::ptr_eq(&database, &self.catalog.database(database.name())?) {
                return Err(hosted_source_unavailable());
            }
            Ok(history)
        });
        tokio::time::timeout(Duration::from_secs(60), barrier)
            .await.map_err(|_| timeout_error())?.ok_or_else(cancelled)?
    }

    pub async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        self.ensure_open()?;
        let Some(basis) = basis else { return Ok(()); };
        if basis.databases.values().any(|positions| !positions.is_empty()) { self.ensure_indexing()?; }
        let databases = self.catalog.open_databases();
        let waits = databases.into_iter().filter_map(|(name, database)| basis.databases.get(&name).and_then(|positions| positions.first()).copied().map(|position| async move {
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
        self.query_context_in_placement(database, basis, identity, None, cancellation).await
    }

    async fn query_context_in_placement(&self, database: &str, basis: &QueryBasis, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        let placed = self.catalog.is_placed();
        let held = placed.then(|| self.attached_database(database, placement)).transpose()?;
        if let Some(fence) = &self.read_fence { fence.confirm(database).await?; }
        if let Some(previous) = &held {
            let current = self.catalog.database(database).map_err(|_| lost_placement(database))?;
            if !Arc::ptr_eq(previous, &current) { return Err(lost_placement(database)); }
        }

        let query_cpu = held.as_ref().and_then(|database| database.query_cpu_tracker());
        let fragment_residence = held.as_ref().and_then(|database| database.fragment_residence().cloned());
        let mut snapshots = BTreeMap::new();
        let classic = if placed { BTreeMap::new() } else { self.catalog.open_databases() };
        for (name, database) in held.into_iter().map(|held| (database.to_owned(), held)).chain(classic) {
            if !placed && identity.authorize_database(&name, DatabaseAccess::Read).is_err() { continue; }
            let head = database.partition.value().live().latest_completed_tx().map(|key| key.system_time);
            let snapshot = database.partition.open_snapshot(head);
            let requested = basis.snapshot.databases.get(&name).and_then(|partitions| partitions.first()).copied().flatten();
            if let Some(requested) = requested.filter(|requested| snapshot.tx_basis().is_none_or(|head| *requested > head.system_time.into())) {
                return Err(rtdb_query::unindexed_tx(requested, snapshot.tx_basis()));
            }
            let cap = requested.map(|time| time.as_micros()).transpose()?;
            let cap = match (cap, basis.snapshot_time) { (Some(time), Some(upper)) => Some(time.min(upper)), (time, _) => time };
            snapshots.insert(name, DatabaseSnapshot { partitions: vec![snapshot.with_system_time_cap(cap)?], storage: Arc::clone(&database.storage) });
        }
        let catalog = CatalogSnapshot::new(database, snapshots, false)?.with_runtime(Arc::new(crate::runtime::NodeCatalogRuntime::new(self.clone(), cancellation.clone())))
            .with_fragmenter(self.scan_fragmenter.clone(), fragment_residence);
        QueryContext::with_owner(catalog, self.factory.options.query.query_memory(&self.factory.memory), basis.current_time, basis.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?
            .with_accelerators(Arc::clone(&self.factory.accelerators))
            .with_read_options(&basis.snapshot, basis.snapshot_time, self.query_instant())
            .map(|context| context.with_query_cpu(query_cpu))
    }

    /// Waits for this node's copy to reach the delegated fragment's transaction basis.
    /// The evaluator checks the exact snapshot it captures after the wait.
    pub async fn scan_fragment(&self, fragment: &rtdb_query::ScanFragment, catch_up: Duration, cancellation: &CancellationToken) -> Result<Vec<u8>> {
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
        let snapshot = database.partition.open_snapshot(Some(fragment.basis.system_time));
        let snapshots = BTreeMap::from([(database.name().to_owned(), DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::clone(&database.storage) })]);
        let catalog = CatalogSnapshot::new(database.name(), snapshots, false)?;
        let context = QueryContext::with_owner(catalog, Arc::clone(&self.factory.memory), self.query_instant(), fragment.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?;
        rtdb_query::evaluate_fragment(context, fragment).await
    }

    async fn submit(&self, database: &str, mut operations: Vec<TxOp>, mut options: SubmitOptions, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<SubmittedTx> {
        self.admit(database, DatabaseAccess::Write, identity, cancellation).await?;
        let key = options.user_metadata.as_ref().and_then(|metadata| idempotency_key(metadata.value()));
        let database = self.attached_database(database, placement)?;
        let admission = if self.catalog.is_placed() {
            Some(cancellation.run_until_cancelled(database.admit_transaction()).await.ok_or_else(cancelled)??)
        } else { None };
        if let Some(key) = key {
            if key == IDEMPOTENCY_SENTINEL { return Err(Anomaly::incorrect("rtdb.node/reserved-idempotency-key", format!("'{IDEMPOTENCY_SENTINEL}' is reserved and cannot be an idempotency key"))); }
            // A transaction carrying a key that already committed within the window aborts on its assertion, and awaiting it answers with the original's outcome.
            let table = rtdb_types::TableRef::new("public", IDEMPOTENCY_TABLE);
            let put = |id: &str, valid_to: Option<rtdb_types::Timestamp>| -> Result<TxOp> {
                let documents = rtdb_tx::Documents::new(vec![rtdb_types::StructFields::from([("_id".to_owned(), Value::Utf8(id.to_owned()))])], &self.factory.memory)?;
                Ok(TxOp::PutDocs { table: table.clone(), documents, valid_from: None, valid_to })
            };
            let assertion = TxOp::Sql { sql: idempotency_guard(key), args: None };
            let value = database.partition.value();
            if value.table_catalog().metadata(&table).is_some() || value.live().tables().contains_key(&table) {
                operations.insert(0, assertion);
            } else {
                // Until the keys table exists, a sentinel row creates it within the transaction, so the assertion can read it.
                operations.splice(0..0, [put(IDEMPOTENCY_SENTINEL, None)?, assertion]);
            }
            // A key expires after the window, so the table's current rows stay as few as the keys still in use.
            operations.push(put(key, Some(rtdb_types::Timestamp(self.now().0 + IDEMPOTENCY_WINDOW.as_micros() as i64)))?);
        }
        database.require_writable()?;
        if database.external_source() { return Err(Anomaly::incorrect("xtdb/submit-tx-to-external-source-db", "Cannot submit transactions to an external-source database").with_context("db-name", database.name().into())); }
        options.user = identity.user().map(str::to_owned);
        options.default_tz = rtdb_expr::normalize_timezone(&options.default_tz)?;
        let sql_options = rtdb_sql::PrepareOptions { generate_series_end_exclusive: self.factory.options.query.generate_series_end_exclusive, ..Default::default() };
        let source = prepare_submission_with_sql_options(operations, options, database.name(), &self.factory.memory, &sql_options)?;
        let payload = source.encode()?.into_bytes();
        let submitted_bytes = payload.len() as u64;
        let position = cancellation.run_until_cancelled(database.replication.submit_source_encoded(payload, self.now())).await.ok_or_else(cancelled)??;
        if let Some(admission) = &admission {
            database.record_submission(submitted_bytes);
            let mut latest = admission.lock();
            *latest = (*latest).max(Some(position));
        }
        Ok(SubmittedTx { tx_id: TxId(position.0) })
    }

    /// The committed outcome visible to the retry when its duplicate guard ran.
    async fn keyed_transaction(&self, database: &str, key: &str, retry_time: Timestamp, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<Option<TransactionKey>> {
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, cancellation).await?;
        let time = Value::Timestamp { value: retry_time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        let arguments = vec![time.clone(), time, Value::Utf8(key.to_owned())];
        let options = rtdb_sql::PrepareOptions {
            parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(),
            default_timezone: context.timezone.clone(),
            generate_series_end_exclusive: context.options.generate_series_end_exclusive,
            ..Default::default()
        };
        let sql = format!("SELECT _id, system_time FROM xt.txs FOR ALL VALID_TIME WHERE committed = true AND system_time = (SELECT k._system_from FROM {IDEMPOTENCY_TABLE} FOR SYSTEM_TIME AS OF ? FOR VALID_TIME AS OF ? AS k WHERE k._id = ?)");
        let prepared = match prepare_query(&sql, &context, &options) {
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
    pub async fn await_transaction(&self, database: &str, tx_id: TxId, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.await_transaction_in_placement(database, tx_id, identity, None, cancellation).await
    }

    async fn await_transaction_in_placement(&self, database: &str, tx_id: TxId, identity: &dyn SessionIdentity, placement: Option<&Weak<Database>>, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        self.ensure_indexing()?;
        let db = self.attached_database(database, placement)?;
        let latest = db.replication.await_tx(tx_id, cancellation).await?;
        if let Some(result) = &latest && !result.error().is_some_and(|error| error.code == "xtdb/assert-failed") {
            return Ok(result.as_ref().clone());
        }
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context_in_placement(database, &basis, identity, placement, cancellation).await?;
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
                if let Some(error) = &error
                    && error.code == "xtdb/assert-failed"
                    && error.context.get("tx-op-idx").and_then(|value| value.as_u64()).is_some_and(|index| index <= 1)
                    && let Some(key) = idempotency_key(&cell("user_metadata")?)
                    && error.context.get("sql").and_then(|value| value.as_str()).is_some_and(|sql| sql == idempotency_guard(key))
                    && let Some(original) = self.keyed_transaction(database, key, system_time, identity, placement, cancellation).await?
                {
                    return Ok(TransactionResult::Committed { tx_key: original });
                }
                Ok(TransactionResult::Aborted { tx_key, error })
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
        let (completion, result) = oneshot::channel();
        self.commands.send(CatalogCommand::AttachLocal { name, configuration, completion }).await.map_err(|_| closed())?;
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
    fn complete(&self, completion: DatabaseCompletion, tasks: &mut JoinSet<DatabaseCompletion>) -> Result<()> {
        match completion {
            DatabaseCompletion::Worker { database, result } => {
                if let Err(error) = &result { database.progress().ingestion_failed(error.clone()); }
                if self.node.catalog.driver_finished(&database) { Self::drain(database, tasks); }
                result
            }
            DatabaseCompletion::Drained(database) => { self.node.catalog.teardown_finished(&database); Ok(()) }
        }
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
            CatalogCommand::AttachLocal { name, configuration, completion } => {
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

pub struct NativeSession { pub(crate) state: Session, pub(crate) service: Arc<NativeSessionService> }
impl NativeSession {
    pub fn state(&self) -> &Session { &self.state }
    pub fn state_mut(&mut self) -> &mut Session { &mut self.state }
    pub async fn query_context(&mut self) -> Result<Arc<QueryContext>> {
        let basis = self.state.read_basis().await?;
        Ok(self.service.query_context(self.state.database(), &basis, self.state.cancellation()).await?.with_observer(self.service.execution_observer()).with_tracer(self.service.node.tracer(), None))
    }
    pub async fn query_sql(&mut self, sql: &str, arguments: Vec<Value>) -> Result<QueryCursor> {
        let context = self.query_context().await?;
        let options = rtdb_sql::PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, rtdb_columnar::execution_type(&value.logical_type()))).collect(), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        prepare_query(sql, &context, &options)?.execute(context, arguments).await
    }
    pub async fn query_xtql(&mut self, source: &str, arguments: Vec<Value>) -> Result<QueryCursor> {
        let context = self.query_context().await?;
        let options = rtdb_xtql::CompileOptions { parameter_types: arguments.iter().map(|value| rtdb_columnar::execution_type(&value.logical_type())).collect(), default_timezone: context.timezone.clone(), ..Default::default() };
        let prepared = rtdb_xtql::compile_in(source, &context.catalog, &options, &context.memory)?;
        PreparedQuery::from_prepared(prepared, &context.memory)?.execute(context, arguments).await
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

    pub(crate) async fn query_context(&self, database: &str, basis: &QueryBasis, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.node.query_context_in_placement(database, basis, self.identity.as_ref(), self.placement.as_ref(), cancellation).await
    }

    pub(crate) async fn await_durable_source(&self, database: &str, hosted: bool, cancellation: &CancellationToken) -> Result<Option<String>> {
        self.node.await_durable_source(database, self.identity.as_ref(), hosted, self.placement.as_ref(), cancellation).await
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
    async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        if !self.node.catalog.is_placed() { return self.node.await_basis(basis, timeout, cancellation).await; }
        self.node.ensure_open()?;
        let Some(position) = basis.as_ref().and_then(|basis| basis.databases.get(&self.database)).and_then(|positions| positions.first()) else {
            return Ok(());
        };
        self.node.ensure_indexing()?;
        let database = self.database()?;
        tokio::time::timeout(timeout, database.replication.await_source(SourcePosition(position.0), cancellation)).await.map_err(|_| timeout_error())??;
        Ok(())
    }
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, cancellation: &CancellationToken) -> Result<SubmittedTx> { self.node.submit(database, operations, options, self.identity.as_ref(), self.placement.as_ref(), cancellation).await }
    async fn await_transaction(&self, database: &str, tx_id: TxId, cancellation: &CancellationToken) -> Result<TransactionResult> { self.node.await_transaction_in_placement(database, tx_id, self.identity.as_ref(), self.placement.as_ref(), cancellation).await }
}

/// Where a transaction's idempotency key is recorded, keyed by the key, so a retry finds it with one point lookup.
pub const IDEMPOTENCY_TABLE: &str = "rtdb_idempotency_keys";
const IDEMPOTENCY_SENTINEL: &str = "~table";
/// How long a committed transaction's key keeps a retry from running it again (`idempotency_window`).
const IDEMPOTENCY_WINDOW: std::time::Duration = std::time::Duration::from_secs(3600);

/// The `idempotency_key` a transaction's metadata carries, set with `BEGIN READ WRITE WITH (METADATA = {idempotency_key: '...'})`.
fn idempotency_key(metadata: &Value) -> Option<&str> {
    let Value::Struct(fields) = metadata else { return None };
    let Value::Utf8(key) = fields.get("idempotency_key")? else { return None };
    Some(key)
}

/// The session parameter a gateway sets at the start of each command: the command's transactions are keyed by it and their order within the command.
pub const IDEMPOTENCY_KEY_PARAMETER: &str = "rtdb_idempotency_key";

/// `metadata` with `key` as its idempotency key, unless it names one already.
pub(crate) fn with_idempotency_key(metadata: Option<rtdb_tx::UserMetadata>, key: &str, memory: &Arc<MemoryBudget>) -> Result<rtdb_tx::UserMetadata> {
    let mut fields = match metadata.as_ref().map(|metadata| metadata.value()) {
        Some(Value::Struct(fields)) if fields.iter().any(|(name, _)| name.as_str() == "idempotency_key") => return Ok(metadata.expect("present")),
        Some(Value::Struct(fields)) => fields.clone(),
        _ => rtdb_types::StructFields::new(),
    };
    fields.insert("idempotency_key".into(), Value::Utf8(key.to_owned()));
    rtdb_tx::UserMetadata::new(Value::Struct(fields), memory)
}

fn idempotency_guard(key: &str) -> String {
    let literal = key.replace('\'', "''");
    format!("ASSERT NOT EXISTS (SELECT 1 FROM {IDEMPOTENCY_TABLE} WHERE _id = '{literal}')")
}

/// The code a placed node refuses a command with when it no longer holds the command's database.
pub const NOT_HELD: &str = "rtdb.node/database-not-held";

fn lost_placement(database: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, NOT_HELD, format!("This node no longer holds {database}")).with_context("db-name", database.into())
}

fn closed() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/node-closed", "Node is closed") }
fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "Operation was cancelled") }
fn hosted_source_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.node/hosted-source-unavailable", "The host cannot confirm this database's durable history")
}
fn timeout_error() -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "xtdb/await-tx-timeout", "Timed out awaiting indexed progress") }
