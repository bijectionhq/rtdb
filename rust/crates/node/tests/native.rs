use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, atomic::{AtomicBool, AtomicI64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use futures::FutureExt;
use rtdb_columnar::read_value;
use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
use rtdb_maintenance::CollectionObserver;
use rtdb_node::*;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::QueryCursor;
use rtdb_tx::{Documents, SqlArguments, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp, TxBasis, TxId, Value};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct Authority;
impl TransactionAuthorization for Authority { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
#[async_trait]
impl SessionIdentity for Authority {
    fn user(&self) -> Option<&str> { Some("xtdb") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}
struct Observer;
impl CollectionObserver for Observer { fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly) { panic!("unexpected collection failure for {table:?}: {error}"); } }
struct CatalogFixtures;
impl CatalogUsers for CatalogFixtures {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
    fn is_superuser(&self, _: &str) -> bool { false }
}
impl CatalogMetrics for CatalogFixtures { fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) } }
struct TickClock(AtomicI64);
impl Clock for TickClock { fn now(&self) -> Timestamp { Timestamp(self.0.fetch_add(1000, Ordering::Relaxed)) } }
fn services() -> NodeServices {
    NodeServices { clock: Arc::new(TickClock(AtomicI64::new(1_000_000))), ..NodeServices::local(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures)) }
}
fn options() -> NodeOptions {
    let mut options = NodeOptions::default();
    options.replication.assertion_interval = Duration::from_millis(1);
    options.replication.election_seed = 1;
    options.replication.flush_timeout = Duration::from_secs(3600);
    options.replication.rows_per_block = 10_000;
    options.automatic_compaction = false;
    options.automatic_gc = false;
    options
}
async fn running(options: NodeOptions, services: NodeServices) -> (Node, tokio::task::JoinHandle<Result<()>>) {
    let (node, driver) = Node::open(options, services).await.unwrap();
    (node, tokio::spawn(driver.run()))
}
async fn stop(node: &Node, driver: tokio::task::JoinHandle<Result<()>>) {
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
fn put(node: &Node, id: i64, value: i64) -> TxOp {
    TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![BTreeMap::from([("_id".into(), Value::I64(id)), ("value".into(), Value::I64(value))])], node.memory()).unwrap(), valid_from: None, valid_to: None }
}
async fn rows(mut cursor: QueryCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            rows.push(schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.batch().column(column).as_ref(), row).unwrap()).collect());
        }
    }
    rows
}

#[tokio::test]
async fn native_sessions_execute_data_dependent_writes_and_keep_historical_reads() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let first = session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap();
        assert!(first.committed());
        assert_eq!(rows(session.query_sql("SELECT value FROM docs WHERE _id = 1", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)]]);
        let update = TxOp::Sql { sql: "UPDATE docs SET value = value + ? WHERE _id = 1".into(), args: Some(SqlArguments::from_rows(vec![vec![Value::I64(5)]], node.memory()).unwrap()) };
        let updated = session.state_mut().execute_tx(vec![update], AutonomousTxOptions::default()).await.unwrap();
        assert!(updated.committed(), "{:?}", updated.error());
        assert_eq!(node.await_transaction("xtdb", first.tx_key().tx_id, &Authority, &CancellationToken::new()).await.unwrap(), first);
        assert_eq!(rows(session.query_sql("SELECT value FROM docs WHERE _id = 1", vec![]).await.unwrap()).await, vec![vec![Value::I64(15)]]);
        assert_eq!(rows(session.query_sql("SELECT value FROM docs FOR ALL SYSTEM_TIME WHERE _id = 1 ORDER BY _system_from", vec![]).await.unwrap()).await, vec![vec![Value::I64(10)], vec![Value::I64(15)]]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn asynchronous_submission_is_recorded_before_application_and_read_awaits_the_token() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        let submitted = session.state_mut().submit_tx(vec![put(&node, 1, 42)], AutonomousTxOptions::default()).await.unwrap();
        assert_eq!(session.state().last_submitted().unwrap().tx_id(), submitted.tx_id);
        assert_eq!(rows(session.query_sql("SELECT value FROM docs", vec![]).await.unwrap()).await, vec![vec![Value::I64(42)]]);
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn local_recovery_reads_flushed_and_unflushed_transactions_and_retained_batches_survive_shutdown() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let directory = tempfile::tempdir().unwrap();
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.path().join("log").to_string_lossy().into_owned()), epoch: Some(0) })),
            storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.path().join("store").to_string_lossy().into_owned()), epoch: Some(0) })),
            ..Default::default()
        };
        let mut configuration_options = options();
        configuration_options.membership = Membership::Durable { primary: configuration, skipped: BTreeSet::new() };
        let runtime_services = services();
        let (node, driver) = running(configuration_options.clone(), runtime_services.clone()).await;
        let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
        assert!(session.state_mut().execute_tx(vec![put(&node, 1, 10)], AutonomousTxOptions::default()).await.unwrap().committed());
        node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        node.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        node.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        assert!(session.state_mut().execute_tx(vec![put(&node, 2, 20)], AutonomousTxOptions::default()).await.unwrap().committed());
        let mut cursor = session.query_sql("SELECT _id, value FROM docs ORDER BY _id", vec![]).await.unwrap();
        let retained = cursor.next_batch().await.unwrap().unwrap();
        drop(cursor);
        stop(&node, driver).await;
        assert_eq!(retained.batch().num_rows(), 2);
        assert_eq!(read_value(retained.batch().schema().field(1), retained.batch().column(1).as_ref(), 1).unwrap(), Value::I64(20));
        let (reopened, driver) = running(configuration_options, runtime_services).await;
        reopened.sync_all(Duration::from_secs(10), &CancellationToken::new()).await.unwrap();
        let mut session = reopened.session("xtdb", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT _id, value FROM docs ORDER BY _id", vec![]).await.unwrap()).await, vec![vec![Value::I64(1), Value::I64(10)], vec![Value::I64(2), Value::I64(20)]]);
        stop(&reopened, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn skipped_database_remains_a_member_until_detach_and_await_ignores_unknown_names() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut options = options();
        options.membership = Membership::Durable { primary: Default::default(), skipped: BTreeSet::from(["dormant".into()]) };
        let (node, driver) = running(options, services()).await;
        let identity = Authority;
        let token = CancellationToken::new();
        assert!(node.attach_database("dormant".into(), Default::default(), &identity, &token).await.unwrap().committed());
        assert!(node.catalog().database("dormant").is_err());
        assert!(node.catalog().secondary_databases().contains_key("dormant"));
        node.await_basis(Some(TxBasis { databases: BTreeMap::from([("unknown".into(), vec![TxId(i64::MAX)])]) }), Duration::from_millis(1), &token).await.unwrap();
        assert!(node.detach_database("dormant".into(), &identity, &token).await.unwrap().committed());
        assert!(!node.catalog().secondary_databases().contains_key("dormant"));
        let primary = node.detach_database("xtdb".into(), &identity, &token).await.unwrap();
        assert!(!primary.committed());
        assert_eq!(primary.error().unwrap().code, "xtdb/cannot-detach-primary");
        stop(&node, driver).await;
    }).await.unwrap();
}

struct HeldTeardown {
    native: NativeResources,
    armed: AtomicBool,
    started: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl ResourceFactory for HeldTeardown {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn open(&self, name: &str, configuration: &DatabaseConfig, read_only: bool, cancellation: CancellationToken) -> Result<DatabaseResources> {
        let mut resources = self.native.open(name, configuration, read_only, cancellation.clone()).await?;
        if name == "other" && self.armed.swap(false, Ordering::AcqRel) {
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);
            resources.logs.drivers.push(async move {
                cancellation.cancelled().await;
                started.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(())
            }.boxed());
        }
        Ok(resources)
    }
}

#[tokio::test]
async fn detach_removes_lookup_and_membership_before_join_but_reuses_the_name_only_after_join() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let resources = Arc::new(HeldTeardown { native: NativeResources::default(), armed: AtomicBool::new(true), started: Arc::new(Semaphore::new(0)), release: Arc::new(Semaphore::new(0)) });
        let mut services = services();
        services.resources = resources.clone();
        let (node, driver) = running(options(), services).await;
        let token = CancellationToken::new();
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        assert!(node.detach_database("other".into(), &Authority, &token).await.unwrap().committed());
        resources.started.acquire().await.unwrap().forget();
        assert!(node.catalog().database("other").is_err());
        assert!(!node.catalog().secondary_databases().contains_key("other"));
        let rejected = node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap();
        assert!(!rejected.committed());
        assert_eq!(rejected.error().unwrap().code, "xtdb/db-being-detached");
        resources.release.add_permits(1);
        while node.catalog().check_attach("other", &Default::default()).is_err() { tokio::time::sleep(Duration::from_millis(1)).await; }
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        stop(&node, driver).await;
    }).await.unwrap();
}

struct FailedResource { native: NativeResources, armed: AtomicBool }
#[async_trait]
impl ResourceFactory for FailedResource {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> { self.native.validate(name, configuration) }
    async fn open(&self, name: &str, configuration: &DatabaseConfig, read_only: bool, cancellation: CancellationToken) -> Result<DatabaseResources> {
        let mut resources = self.native.open(name, configuration, read_only, cancellation).await?;
        if name == "other" && self.armed.swap(false, Ordering::AcqRel) {
            resources.logs.drivers.push(async { Err(Anomaly::fault("test/resource-failed", "Injected resource failure")) }.boxed());
        }
        Ok(resources)
    }
}

#[tokio::test]
async fn failed_database_remains_queryable_and_can_be_detached_after_its_driver_joined() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut services = services();
        services.resources = Arc::new(FailedResource { native: NativeResources::default(), armed: AtomicBool::new(true) });
        let (node, driver) = running(options(), services).await;
        let token = CancellationToken::new();
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        let database = node.catalog().database("other").unwrap();
        while database.progress().fault().is_none() { tokio::task::yield_now().await; }
        let mut session = node.session("other", Arc::new(Authority)).unwrap();
        assert_eq!(rows(session.query_sql("SELECT 1", Vec::new()).await.unwrap()).await, vec![vec![Value::I64(1)]]);
        assert!(node.detach_database("other".into(), &Authority, &token).await.unwrap().committed());
        while node.catalog().check_attach("other", &Default::default()).is_err() { tokio::task::yield_now().await; }
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &token).await.unwrap().committed());
        stop(&node, driver).await;
    }).await.unwrap();
}

#[tokio::test]
async fn health_counts_noncritical_failures_but_only_critical_failures_make_alive_unhealthy() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let (node, driver) = running(options(), services()).await;
        let health = HealthChecks::new(node.clone()).unwrap();
        assert_eq!(health.started().status, 200);
        assert_eq!(health.ready().body, "Ready.");
        assert_eq!(health.alive().await.unwrap().body, "Alive.");
        assert_eq!(health.finish_block(Some("missing")).await.status, 500);
        assert_eq!(health.finish_block(Some("xtdb")).await.status, 200);
        assert!(node.attach_database("other".into(), Default::default(), &Authority, &CancellationToken::new()).await.unwrap().committed());
        node.catalog().database("other").unwrap().progress().ingestion_failed(Anomaly::fault("test/noncritical", "Noncritical ingestion failed"));
        let response = health.alive().await.unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.headers["X-XTDB-Databases-Unhealthy"], "1");
        assert_eq!(response.headers["X-XTDB-Databases-Checked"], "2");
        node.catalog().database("xtdb").unwrap().progress().ingestion_failed(Anomaly::fault("test/critical", "Primary ingestion failed"));
        let response = health.alive().await.unwrap();
        assert_eq!(response.status, 503);
        assert!(response.body.contains("Primary ingestion failed"));
        assert_eq!(response.headers["X-XTDB-Databases-Unhealthy"], "2");
        node.shutdown();
        let _ = driver.await.unwrap();
    }).await.unwrap();
}

struct PendingIdentity { entered: Semaphore, released: Arc<AtomicBool> }
struct ReleaseFlag(Arc<AtomicBool>);
impl Drop for ReleaseFlag { fn drop(&mut self) { self.0.store(true, Ordering::Release); } }
#[async_trait]
impl SessionIdentity for PendingIdentity {
    fn user(&self) -> Option<&str> { Some("subject") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> {
        let _release = ReleaseFlag(Arc::clone(&self.released));
        self.entered.add_permits(1);
        futures::future::pending().await
    }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}

#[tokio::test]
async fn caller_or_node_cancellation_drops_inflight_identity_refresh_before_query_admission() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for cancel_node in [false, true] {
            let (node, driver) = running(options(), services()).await;
            let identity = Arc::new(PendingIdentity { entered: Semaphore::new(0), released: Arc::new(AtomicBool::new(false)) });
            let cancellation = CancellationToken::new();
            let request = {
                let node = node.clone(); let identity = Arc::clone(&identity); let cancellation = cancellation.clone();
                tokio::spawn(async move {
                    let basis = QueryBasis { snapshot: node.snapshot_basis()?, snapshot_time: None, current_time: node.now(), timezone: "UTC".into() };
                    node.query_context("xtdb", &basis, identity.as_ref(), &cancellation).await
                })
            };
            identity.entered.acquire().await.unwrap().forget();
            if cancel_node { node.shutdown(); } else { cancellation.cancel(); }
            assert_eq!(request.await.unwrap().err().unwrap().category, rtdb_types::AnomalyCategory::Interrupted);
            assert!(identity.released.load(Ordering::Acquire));
            stop(&node, driver).await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn node_shutdown_cancels_direct_query_contexts_with_independent_caller_tokens() {
    let (node, driver) = running(options(), services()).await;
    let cancellation = CancellationToken::new();
    let basis = QueryBasis { snapshot: node.snapshot_basis().unwrap(), snapshot_time: None, current_time: node.now(), timezone: "UTC".into() };
    let context = node.query_context("xtdb", &basis, &Authority, &cancellation).await.unwrap();
    let prepared = PreparedSql::prepare("SELECT 1", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let mut cursor = prepared.open_query(context, Vec::new()).unwrap();
    stop(&node, driver).await;
    assert!(!cancellation.is_cancelled());
    assert_eq!(cursor.next_batch().await.unwrap_err().category, rtdb_types::AnomalyCategory::Interrupted);
}

#[tokio::test]
async fn skipped_membership_does_not_require_a_node_local_backend_factory() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut options = options();
        options.membership = Membership::Durable { primary: Default::default(), skipped: BTreeSet::from(["dormant".into()]) };
        let (node, driver) = running(options, services()).await;
        let configuration = DatabaseConfig {
            log: Some(database_config::Log::OtherLog(prost_types::Any { type_url: "proto.example/RemoteLog".into(), value: vec![1, 2, 3] })),
            ..Default::default()
        };
        let outcome = node.attach_database("dormant".into(), configuration.clone(), &Authority, &CancellationToken::new()).await.unwrap();
        assert!(outcome.committed(), "{:?}", outcome.error());
        assert_eq!(node.catalog().secondary_databases().get("dormant"), Some(&configuration));
        assert!(node.catalog().database("dormant").is_err());
        stop(&node, driver).await;
    }).await.unwrap();
}
