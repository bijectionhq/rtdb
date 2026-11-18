use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
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
use tokio::sync::{mpsc, oneshot};
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
}
impl NodeServices {
    pub fn local(authorization: Arc<dyn TransactionAuthorization>, observer: Arc<dyn CollectionObserver>, users: Arc<dyn CatalogUsers>, metrics: Arc<dyn CatalogMetrics>) -> Self {
        Self { resources: Arc::new(NativeResources::default()), transaction_authorization: authorization, collection_observer: observer, external_sources: BTreeMap::new(), clock: Arc::new(SystemClock), catalog_users: users, catalog_metrics: metrics, execution_observer: None, tracer: None }
    }
}

#[derive(Clone)]
pub struct Node {
    catalog: Arc<DatabaseCatalog>,
    factory: Arc<DatabaseFactory>,
    commands: mpsc::Sender<CatalogCommand>,
    cancellation: CancellationToken,
    pub(crate) runtime: Arc<crate::runtime::RuntimeProviders>,
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
    async fn open_region(mut options: NodeOptions, services: NodeServices, memory: Arc<MemoryBudget>) -> Result<(Self, NodeDriver)> {
        if memory.limit_bytes() != options.execution_memory_bytes { return Err(Anomaly::incorrect("rtdb.node/invalid-memory-budget", "Node options and supplied execution budget have different limits")); }
        if options.command_capacity == 0 || options.execution_memory_bytes == 0 || options.cache_bytes == 0 {
            return Err(Anomaly::incorrect("rtdb.node/invalid-options", "Node capacities must be positive"));
        }
        options.default_timezone = rtdb_expr::normalize_timezone(&options.default_timezone)?;
        let catalog = DatabaseCatalog::new(options.membership.clone());
        let runtime = Arc::new(crate::runtime::RuntimeProviders { users: services.catalog_users, metrics: services.catalog_metrics });
        let factory = Arc::new(DatabaseFactory { memory, cache: CacheBudget::new(options.cache_bytes),
            resources: services.resources, authorization: services.transaction_authorization, observer: services.collection_observer, execution_observer: services.execution_observer, tracer: services.tracer,
            external_sources: services.external_sources, clock: services.clock, options });
        let (commands, receiver) = mpsc::channel(factory.options.command_capacity);
        let node = Self { catalog, factory, commands, cancellation: CancellationToken::new(), runtime };
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
    pub fn cache_budget(&self) -> &Arc<CacheBudget> { &self.factory.cache }
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
        identity.authorize_database(database, access)
    }

    pub fn session(&self, database: impl Into<String>, identity: Arc<dyn SessionIdentity>) -> Result<NativeSession> {
        self.ensure_open()?;
        let database = database.into();
        identity.authorize_database(&database, DatabaseAccess::Read)?;
        self.catalog.database(&database)?;
        let service = Arc::new(NativeSessionService { node: self.clone(), identity });
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
    pub async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()> {
        self.ensure_open()?;
        let Some(basis) = basis else { return Ok(()); };
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

    pub async fn query_context(&self, database: &str, basis: &QueryBasis, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<Arc<QueryContext>> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        let mut snapshots = BTreeMap::new();
        for (name, database) in self.catalog.open_databases() {
            if identity.authorize_database(&name, DatabaseAccess::Read).is_err() { continue; }
            let head = database.partition.value().live().latest_completed_tx().map(|key| key.system_time);
            let snapshot = database.partition.open_snapshot(head);
            let requested = basis.snapshot.databases.get(&name).and_then(|partitions| partitions.first()).copied().flatten();
            if requested.is_some_and(|requested| snapshot.tx_basis().is_none_or(|head| requested > head.system_time.into())) {
                return Err(Anomaly::incorrect("xtdb/unindexed-tx", "Requested system-time is after the latest completed transaction").with_context("db-name", name.into()));
            }
            let cap = requested.map(|time| time.as_micros()).transpose()?;
            let cap = match (cap, basis.snapshot_time) { (Some(time), Some(upper)) => Some(time.min(upper)), (time, _) => time };
            snapshots.insert(name, DatabaseSnapshot { partitions: vec![snapshot.with_system_time_cap(cap)?], storage: Arc::clone(&database.storage) });
        }
        let catalog = CatalogSnapshot::new(database, snapshots, false)?.with_runtime(Arc::new(crate::runtime::NodeCatalogRuntime::new(self.clone(), cancellation.clone())));
        QueryContext::with_owner(catalog, Arc::clone(&self.factory.memory), basis.current_time, basis.timezone.clone(), self.factory.options.query.clone(), cancellation, &self.cancellation)?
            .with_read_options(&basis.snapshot, basis.snapshot_time, self.query_instant())
    }

    async fn submit(&self, database: &str, operations: Vec<TxOp>, mut options: SubmitOptions, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<SubmittedTx> {
        self.admit(database, DatabaseAccess::Write, identity, cancellation).await?;
        let database = self.catalog.database(database)?;
        database.require_writable()?;
        if database.external_source() { return Err(Anomaly::incorrect("xtdb/submit-tx-to-external-source-db", "Cannot submit transactions to an external-source database").with_context("db-name", database.name().into())); }
        options.user = identity.user().map(str::to_owned);
        options.default_tz = rtdb_expr::normalize_timezone(&options.default_tz)?;
        let sql_options = rtdb_sql::PrepareOptions { generate_series_end_exclusive: self.factory.options.query.generate_series_end_exclusive, ..Default::default() };
        let source = prepare_submission_with_sql_options(operations, options, database.name(), &self.factory.memory, &sql_options)?;
        let payload = source.encode()?.into_bytes();
        let position = cancellation.run_until_cancelled(database.replication.submit_source_encoded(payload, self.now())).await.ok_or_else(cancelled)??;
        Ok(SubmittedTx { tx_id: TxId(position.0) })
    }
    pub async fn await_transaction(&self, database: &str, tx_id: TxId, identity: &dyn SessionIdentity, cancellation: &CancellationToken) -> Result<TransactionResult> {
        self.admit(database, DatabaseAccess::Read, identity, cancellation).await?;
        let db = self.catalog.database(database)?;
        if let Some(result) = db.replication.await_tx(tx_id, cancellation).await? { return Ok(result.as_ref().clone()); }
        let basis = QueryBasis { snapshot: self.snapshot_basis()?, snapshot_time: None, current_time: self.query_instant(), timezone: self.factory.options.default_timezone.clone() };
        let context = self.query_context(database, &basis, identity, cancellation).await?;
        let options = rtdb_sql::PrepareOptions { parameter_types: BTreeMap::from([(0, LogicalType::Scalar(rtdb_types::ScalarType::I64))]), default_timezone: context.timezone.clone(), generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let mut cursor = prepare_query("SELECT system_time, committed, error FROM xt.txs FOR ALL VALID_TIME WHERE _id = ?", &context, &options)?
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
                let error = match cell("error")? { Value::Null | Value::Undefined => None, Value::Transit(bytes) => Some(RetainedAnomaly::from_transit(&bytes, &self.factory.memory)?), _ => return Err(Anomaly::fault("rtdb.node/tx-result-error", "Stored transaction error is not Transit")) };
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
}

impl NodeDriver {
    /// Closes an unstarted node, drops its database drivers, and drains its resource region.
    /// Use after listener setup fails or after read-only startup inspection; `run` consumes this same owner.
    pub async fn close_without_run(mut self) -> Result<()> {
        self.node.shutdown();
        self.commands.close();
        while let Ok(command) = self.commands.try_recv() {
            match command { CatalogCommand::Apply { completion, .. } => { let _ = completion.send(Err(closed())); }, CatalogCommand::AttachLocal { completion, .. } => { let _ = completion.send(Err(closed())); } }
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
            match command { CatalogCommand::Apply { completion, .. } => { let _ = completion.send(Err(closed())); }, CatalogCommand::AttachLocal { completion, .. } => { let _ = completion.send(Err(closed())); } }
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
                    DbOp::AttachDatabase(attach) => self.attach(attach.db_name.unwrap_or_default(), attach.config.unwrap_or_default(), tasks).await.map(|_| ()),
                    DbOp::DetachDatabase(detach) => self.node.catalog.begin_detach(detach.db_name.as_deref().unwrap_or_default()).map(|ready| {
                        if let Some(database) = ready { Self::drain(database, tasks); }
                    }),
                };
                let _ = completion.send(result);
            }
            CatalogCommand::AttachLocal { name, configuration, completion } => {
                let result = match self.node.catalog.database(&name) { Ok(database) => Ok(Some(database)), Err(_) => self.attach(name, configuration, tasks).await };
                let _ = completion.send(result);
            }
        }
    }
    async fn attach(&mut self, name: String, configuration: DatabaseConfig, tasks: &mut JoinSet<DatabaseCompletion>) -> Result<Option<Arc<Database>>> {
        self.node.catalog.check_attach(&name, &configuration)?;
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
        Ok(self.service.node.query_context(self.state.database(), &basis, self.service.identity.as_ref(), self.state.cancellation()).await?.with_observer(self.service.execution_observer()).with_tracer(self.service.node.tracer(), None))
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
    pub async fn query_ra(&mut self, source: &str, arguments: BTreeMap<String, Value>, cancellation: &CancellationToken) -> Result<QueryCursor> {
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

pub(crate) struct NativeSessionService { pub(crate) node: Node, pub(crate) identity: Arc<dyn SessionIdentity> }
#[async_trait]
impl SessionService for NativeSessionService {
    fn memory(&self) -> &Arc<MemoryBudget> { self.node.memory() }
    fn execution_observer(&self) -> Option<Arc<dyn rtdb_plan::ExecutionObserver>> { self.node.factory.execution_observer.clone() }
    fn user(&self) -> Option<&str> { self.identity.user() }
    fn check_open(&self) -> Result<()> { self.node.ensure_open() }
    async fn revalidate(&self) -> Result<()> { self.node.ensure_open()?; self.identity.revalidate(self.node.now()).await }
    fn now(&self) -> Timestamp { self.node.now() }
    fn query_instant(&self) -> rtdb_types::Instant { self.node.query_instant() }
    fn snapshot_basis(&self) -> Result<TimeBasis> { self.node.snapshot_basis() }
    async fn await_basis(&self, basis: Option<TxBasis>, timeout: Duration, cancellation: &CancellationToken) -> Result<()> { self.node.await_basis(basis, timeout, cancellation).await }
    async fn submit(&self, database: &str, operations: Vec<TxOp>, options: SubmitOptions, cancellation: &CancellationToken) -> Result<SubmittedTx> { self.node.submit(database, operations, options, self.identity.as_ref(), cancellation).await }
    async fn await_transaction(&self, database: &str, tx_id: TxId, cancellation: &CancellationToken) -> Result<TransactionResult> { self.node.await_transaction(database, tx_id, self.identity.as_ref(), cancellation).await }
}

fn closed() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/node-closed", "Node is closed") }
fn cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "xtdb/operation-cancelled", "Operation was cancelled") }
fn timeout_error() -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "xtdb/await-tx-timeout", "Timed out awaiting indexed progress") }
