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

struct CloseSchedule(Arc<FaultSchedule>);
impl Drop for CloseSchedule {
    fn drop(&mut self) { self.0.close(); }
}

async fn flush_and_adopt(database: &rtdb_node::Database) -> BlockIndex {
    let target = database.status().open_block;
    database.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !database.status().adopted_block.is_some_and(|block| block.block_index() >= target) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    target
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
async fn recovery_advances_after_collection_of_the_listed_checkpoint() {
    recover_during_block_collection(RecoveryPause::Listed).await;
}

#[tokio::test]
async fn recovery_advances_after_collection_of_a_selected_table_block() {
    recover_during_block_collection(RecoveryPause::LoadingTable).await;
}

enum RecoveryPause { Listed, LoadingTable }

async fn recover_during_block_collection(pause: RecoveryPause) {
    use rtdb_maintenance::{BlockCollector, CycleWorker, GarbageConfig};
    use rtdb_node::ResourceFactory;
    use rtdb_replication::{BlockDiscovery, BlockStore};
    use rtdb_storage::{Error, ObjectKey, cache::BufferPool};
    use rtdb_types::SourcePosition;

    async fn commit_checkpoint(
        node: &Node, database: &rtdb_node::Database, session: &mut rtdb_node::NativeSession, id: i64,
    ) -> BlockIndex {
        let documents = Documents::new(vec![StructFields::from([
            ("_id".into(), Value::I64(id)), ("value".into(), Value::I64(id * 42)),
        ])], node.memory()).unwrap();
        assert!(session.state_mut().execute_tx(vec![TxOp::PutDocs {
            table: TableRef::new("public", "docs"), documents, valid_from: None, valid_to: None,
        }], AutonomousTxOptions::default()).await.unwrap().committed());
        flush_and_adopt(database).await
    }

    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ClockFixture(AtomicI64::new(1_000_000)));
    let schedule = FaultSchedule::new(vec![], 10_000).unwrap();
    let settings = options(directory.path());
    let (writer, driver) = Node::open(settings.clone(), services(clock.clone(), schedule)).await.unwrap();
    let writer_owner = tokio::spawn(driver.run());
    let database = writer.catalog().database("xtdb").unwrap();
    let mut session = writer.session("xtdb", Arc::new(Identity)).unwrap();
    let old = commit_checkpoint(&writer, &database, &mut session, 1).await;
    let table = TableRef::new("public", "docs");
    let old_catalog = database.status().adopted_block.unwrap();
    let entry = old_catalog.table_entries().unwrap().into_iter().find(|entry| entry.table == table).unwrap();
    let old_table = entry.slug.block_key(old).unwrap();
    let (operation, phase, key) = match pause {
        RecoveryPause::Listed => (Operation::List, Phase::After, "blocks".to_owned()),
        RecoveryPause::LoadingTable => (Operation::Head, Phase::Before, old_table.clone()),
    };

    let reader_schedule = FaultSchedule::new(vec![Rule {
        owner: "xtdb/store".into(), operation, phase, key: KeyMatch::Exact(key),
        occurrence: 1, action: Action::Hold("checkpoint-open".into()),
    }], 10_000).unwrap();
    let _close = CloseSchedule(reader_schedule.clone());
    let reader_file_size = settings.file_size_target;
    let mut reader_options = settings;
    reader_options.read_only = true;
    let reader_services = services(clock, reader_schedule.clone());
    let open = tokio::spawn(async move { Node::open(reader_options, reader_services).await });
    tokio::time::timeout(Duration::from_secs(10), reader_schedule.wait_for_rule(0)).await.unwrap().unwrap();

    let latest = commit_checkpoint(&writer, &database, &mut session, 2).await;
    let storage = NativeResources::default().open_storage("xtdb", database.configuration()).await.unwrap();
    let store = &storage.store;
    let cache = Arc::new(BufferPool::new(Arc::clone(store), 16 << 20));
    let blocks = BlockStore::new(Arc::clone(store), cache, Arc::clone(writer.memory()), reader_file_size).unwrap();
    let recovered = blocks.recover(
        Default::default(), SourcePosition(-1), Timestamp(0), BlockDiscovery::At(latest),
    ).await.unwrap();
    let collector = BlockCollector::new(
        Arc::clone(store), recovered.partition, Arc::new(Observer),
        GarbageConfig { blocks_to_keep: 1, ..GarbageConfig::default() },
    ).unwrap();
    collector.collect(writer.cancellation()).await.unwrap();
    for key in [block_key(old).unwrap(), old_table] {
        assert!(matches!(store.head_fresh(&ObjectKey::new(key).unwrap()).await, Err(Error::MissingObject(_))));
    }
    store.head_fresh(&ObjectKey::new(block_key(latest).unwrap()).unwrap()).await.unwrap();
    storage.io.close_and_wait().await;
    reader_schedule.release("checkpoint-open").unwrap();

    let (reader, driver) = tokio::time::timeout(Duration::from_secs(10), open).await.unwrap().unwrap()
        .unwrap_or_else(|error| panic!("recovery could not use the newer checkpoint: {}", error.code));
    let recovered = reader.catalog().database("xtdb").unwrap();
    assert_eq!(recovered.status().adopted_block.unwrap().block_index(), latest);
    let reader_owner = tokio::spawn(driver.run());
    reader.sync_all(Duration::from_secs(10), reader.cancellation()).await.unwrap();
    let mut session = reader.session("xtdb", Arc::new(Identity)).unwrap();
    assert_eq!(
        rows(session.query_sql("SELECT _id, value FROM docs ORDER BY _id", vec![]).await.unwrap()).await,
        vec![vec![Value::I64(1), Value::I64(42)], vec![Value::I64(2), Value::I64(84)]],
    );
    reader.shutdown();
    writer.shutdown();
    for owner in [reader_owner, writer_owner] {
        tokio::time::timeout(Duration::from_secs(10), owner).await.unwrap().unwrap().unwrap();
    }
    reader_schedule.assert_exhausted().unwrap();
}
