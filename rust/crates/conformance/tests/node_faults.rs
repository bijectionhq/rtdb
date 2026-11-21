use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_conformance::faults::{Action, FaultResources, FaultSchedule, KeyMatch, Operation, Phase, Rule};
use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
use rtdb_format::keys::block_key;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{AutonomousTxOptions, Clock, DatabaseAccess, Membership, NativeResources, Node, NodeOptions, NodeServices, SessionIdentity};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::QueryCursor;
use rtdb_tx::{Documents, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, BlockIndex, Result, TableRef, Timestamp, Value};
use rtdb_types::StructFields;
use std::{sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};

struct Identity;
impl TransactionAuthorization for Identity {
    fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) }
}
#[async_trait]
impl SessionIdentity for Identity {
    fn user(&self) -> Option<&str> { Some("xtdb") }
    async fn revalidate(&self, _: Timestamp) -> Result<()> { Ok(()) }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> { Ok(()) }
}
struct Observer;
impl CollectionObserver for Observer {
    fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { panic!("unexpected maintenance failure: {error}"); }
}
struct Catalog;
impl CatalogUsers for Catalog {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
    fn is_superuser(&self, _: &str) -> bool { true }
}
impl CatalogMetrics for Catalog {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
}
struct ClockFixture(AtomicI64);
impl Clock for ClockFixture {
    fn now(&self) -> Timestamp { Timestamp(self.0.fetch_add(1000, Ordering::Relaxed)) }
}

fn options(directory: &std::path::Path) -> NodeOptions {
    let mut options = NodeOptions::default();
    options.membership = Membership::Durable { primary: DatabaseConfig {
        log: Some(database_config::Log::LocalLog(LocalLog { path: Some(directory.join("log").to_string_lossy().into_owned()), epoch: None })),
        storage: Some(database_config::Storage::LocalStorage(LocalStorage { path: Some(directory.join("storage").to_string_lossy().into_owned()), epoch: None })),
        ..Default::default()
    }, skipped: Default::default() };
    options.automatic_compaction = false;
    options.automatic_gc = false;
    options.compaction_threads = 0;
    options.replication.assertion_interval = Duration::from_millis(1);
    options.replication.flush_timeout = Duration::from_secs(3600);
    options.replication.election_seed = 42;
    options
}

fn services(clock: Arc<ClockFixture>, schedule: Arc<FaultSchedule>) -> NodeServices {
    let resources = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false));
    NodeServices { resources: FaultResources::new(resources, schedule), clock,
        ..NodeServices::local(Arc::new(Identity), Arc::new(Observer), Arc::new(Catalog), Arc::new(Catalog)) }
}

async fn rows(mut cursor: QueryCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns())
                .map(|(field, array)| read_value(field, array.as_ref(), row).unwrap()).collect());
        }
    }
    rows
}

#[tokio::test]
async fn lost_source_acknowledgement_recovers_the_original_write_exactly_once() {
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ClockFixture(AtomicI64::new(1_000_000)));
    let schedule = FaultSchedule::new(vec![Rule { owner: "xtdb/source".into(), operation: Operation::DurableReceipt,
        phase: Phase::After, key: KeyMatch::Any, occurrence: 1, action: Action::Fail("source acknowledgement lost".into()) }], 10_000).unwrap();
    let (node, driver) = Node::open(options(directory.path()), services(clock.clone(), schedule.clone())).await.unwrap();
    let owner = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let documents = Documents::new(vec![StructFields::from([("_id".into(), Value::I64(1)), ("value".into(), Value::I64(42))])], node.memory()).unwrap();
    let write = session.state_mut().execute_tx(vec![TxOp::PutDocs { table: TableRef::new("public", "docs"), documents,
        valid_from: None, valid_to: None }], AutonomousTxOptions::default()).await;
    assert!(write.is_err());
    node.shutdown(); owner.await.unwrap().unwrap();
    schedule.assert_exhausted().unwrap();
    drop(session); drop(node);

    let replay = FaultSchedule::new(vec![], 10_000).unwrap();
    let (node, driver) = Node::open(options(directory.path()), services(clock, replay.clone())).await.unwrap();
    let owner = tokio::spawn(driver.run());
    node.sync_all(Duration::from_secs(10), node.cancellation()).await.unwrap();
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    assert_eq!(rows(session.query_sql("SELECT _id, value FROM docs", vec![]).await.unwrap()).await,
               vec![vec![Value::I64(1), Value::I64(42)]]);
    assert_eq!(rows(session.query_sql("SELECT _id, value FROM docs FOR ALL SYSTEM_TIME", vec![]).await.unwrap()).await,
               vec![vec![Value::I64(1), Value::I64(42)]]);
    node.shutdown(); owner.await.unwrap().unwrap();
    replay.assert_exhausted().unwrap();
}

#[tokio::test]
async fn recovery_replays_the_log_if_interrupted_before_putting_the_checkpoint_root() {
    recover_interrupted_checkpoint(Phase::Before).await;
}

#[tokio::test]
async fn recovery_reads_the_complete_checkpoint_if_interrupted_after_putting_the_root() {
    recover_interrupted_checkpoint(Phase::After).await;
}

async fn recover_interrupted_checkpoint(phase: Phase) {
    let block = BlockIndex(0);
    let root = block_key(block).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ClockFixture(AtomicI64::new(1_000_000)));
    let schedule = FaultSchedule::new(vec![Rule {
        owner: "xtdb/store".into(), operation: Operation::Put, phase,
        key: KeyMatch::Exact(root), occurrence: 1, action: Action::Hold("checkpoint-root".into()),
    }], 10_000).unwrap();
    let (node, driver) = Node::open(options(directory.path()), services(clock.clone(), schedule.clone())).await.unwrap();
    let owner = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let documents = Documents::new(vec![StructFields::from([
        ("_id".into(), Value::I64(1)), ("value".into(), Value::I64(42)),
    ])], node.memory()).unwrap();
    let write = session.state_mut().execute_tx(vec![TxOp::PutDocs {
        table: TableRef::new("public", "docs"), documents, valid_from: None, valid_to: None,
    }], AutonomousTxOptions::default()).await.unwrap();
    assert!(write.committed());

    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), schedule.wait_for_rule(0)).await.unwrap().unwrap();
    assert!(database.status().adopted_block.is_none(), "the root put had not returned");
    node.shutdown();
    schedule.close();
    if let Err(interrupted) = tokio::time::timeout(Duration::from_secs(10), owner).await.unwrap().unwrap() {
        assert_eq!(interrupted.code, "rtdb.storage/closed");
        assert!(interrupted.to_string().contains("fault scenario closed"), "{interrupted}");
    }
    schedule.assert_exhausted().unwrap();
    drop(session);
    drop(database);
    drop(node);

    let replay = FaultSchedule::new(vec![], 10_000).unwrap();
    let (node, driver) = Node::open(options(directory.path()), services(clock, replay.clone())).await
        .unwrap_or_else(|error| panic!("checkpoint could not open after {phase:?} interruption: {error}"));
    let database = node.catalog().database("xtdb").unwrap();
    assert_eq!(
        database.status().adopted_block.as_ref().map(|catalog| catalog.block_index()),
        (phase == Phase::After).then_some(block),
    );
    let owner = tokio::spawn(driver.run());
    node.sync_all(Duration::from_secs(10), node.cancellation()).await.unwrap();
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    assert_eq!(
        rows(session.query_sql("SELECT _id, value FROM docs FOR ALL SYSTEM_TIME", vec![]).await.unwrap()).await,
        vec![vec![Value::I64(1), Value::I64(42)]],
        "{phase:?}",
    );
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), owner).await.unwrap().unwrap().unwrap();
    replay.assert_exhausted().unwrap();
}
