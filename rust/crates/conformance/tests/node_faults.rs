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
    fn monotonic_micros(&self) -> i64 { self.0.load(Ordering::Relaxed) }
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
    let mut status = database.watch_status();
    let target = status.borrow().open_block;
    database.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), status.wait_for(|state| {
        state.adopted_block.as_ref().is_some_and(|block| block.block_index() >= target)
    })).await.unwrap().unwrap();
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

#[tokio::test]
async fn recovery_preserves_queries_through_tiered_publication() {
    use rtdb_format::{catalog::TableBlock, keys::TrieKey};
    use rtdb_index::TrieState;
    use rtdb_storage::ObjectKey;

    async fn visible(session: &mut rtdb_node::NativeSession) -> [Vec<Vec<Value>>; 2] {
        let current = "SELECT _id, value, _system_from, _valid_from, _valid_to FROM docs ORDER BY _id";
        let history = "SELECT _id, value, _system_from, _system_to, _valid_from, _valid_to \
                       FROM docs FOR ALL SYSTEM_TIME ORDER BY _id, _system_from";
        [
            rows(session.query_sql(current, vec![]).await.unwrap()).await,
            rows(session.query_sql(history, vec![]).await.unwrap()).await,
        ]
    }

    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ClockFixture(AtomicI64::new(1_000_000)));
    let schedule = FaultSchedule::new(vec![Rule {
        owner: "xtdb/store".into(),
        operation: Operation::PutFile,
        phase: Phase::Before,
        key: KeyMatch::Prefix("tables/public$docs/data/l02-rc-p3-b".into()),
        occurrence: 1,
        action: Action::Hold("last-sibling".into()),
    }], 20_000).unwrap();
    let _close = CloseSchedule(schedule.clone());
    let mut config = options(directory.path());
    config.compaction_threads = 1;
    config.file_size_target = 1;
    config.automatic_artefacts = false;
    config.artefact_threads = 0;
    let (node, driver) = Node::open(config.clone(), services(clock.clone(), schedule.clone())).await.unwrap();
    let owner = tokio::spawn(driver.run());
    let database = node.catalog().database("xtdb").unwrap();
    let table = TableRef::new("public", "docs");
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    for block in 0..4i64 {
        let data = std::iter::once(0).chain(block * 32 + 1..=block * 32 + 32).map(|id|
            StructFields::from([("_id".into(), Value::I64(id)), ("value".into(), Value::I64(block * 1000 + id))])
        ).collect();
        let documents = Documents::new(data, node.memory()).unwrap();
        assert!(session.state_mut().execute_tx(vec![TxOp::PutDocs {
            table: table.clone(), documents, valid_from: None, valid_to: None,
        }], AutonomousTxOptions::default()).await.unwrap().committed());
        flush_and_adopt(&database).await;
    }
    assert_eq!(database.trie_catalog().current_tries(&table).iter().filter(|trie| trie.key.level() == 0).count(), 4);
    let before = visible(&mut session).await;
    assert_eq!(before[0].len(), 129);
    assert_eq!(before[1].len(), 132);

    let handle = database.compaction().unwrap().clone();
    let mut compaction = tokio::spawn(async move { handle.compact_all().await });
    tokio::select! {
        early = &mut compaction => panic!("compaction completed before reaching the final sibling upload: {early:?}"),
        held = tokio::time::timeout(Duration::from_secs(20), schedule.wait_for_rule(0)) => held.unwrap().unwrap(),
    }
    let catalog = database.trie_catalog();
    let pending = catalog.all_tries(&table).into_iter()
        .filter(|trie| trie.key.level() == 2 && trie.state == TrieState::Nascent).count();
    assert_eq!(pending, 3);
    assert_eq!(catalog.current_tries(&table).iter().filter(|trie| trie.key.level() == 1 && trie.key.recency().is_none()).count(), 4);
    assert_eq!(visible(&mut session).await, before);

    let marker = Documents::new(vec![StructFields::from([("_id".into(), Value::I64(1))])], node.memory()).unwrap();
    assert!(session.state_mut().execute_tx(vec![TxOp::PutDocs {
        table: TableRef::new("public", "marker"), documents: marker, valid_from: None, valid_to: None,
    }], AutonomousTxOptions::default()).await.unwrap().committed());
    let checkpoint = flush_and_adopt(&database).await;
    let snapshot = database.snapshot();
    let slug = &snapshot.table_entry(&table).unwrap().slug;
    let key = ObjectKey::new(slug.block_key(checkpoint).unwrap()).unwrap();
    let block = TableBlock::decode(&database.object_store().get(&key).await.unwrap()).unwrap();
    let persisted: Vec<_> = block.partitions().iter().filter(|part| part.level == Some(2))
        .flat_map(|part| &part.tries).map(|detail| {
            assert_eq!(detail.trie_state, Some(0));
            detail.trie_key.as_deref().unwrap().parse::<TrieKey>().unwrap().part()[0]
        }).collect();
    assert_eq!(persisted, [0, 1, 2]);

    schedule.release("last-sibling").unwrap();
    tokio::time::timeout(Duration::from_secs(20), compaction).await.unwrap().unwrap().unwrap();
    assert_eq!(database.trie_catalog().current_tries(&table).iter().filter(|trie| trie.key.level() == 2).count(), 4);
    assert_eq!(visible(&mut session).await, before);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), owner).await.unwrap().unwrap().unwrap();
    schedule.assert_exhausted().unwrap();
    drop(session);
    drop(database);
    drop(node);

    let replay = FaultSchedule::new(vec![], 20_000).unwrap();
    let (node, driver) = Node::open(config, services(clock, replay.clone())).await.unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    assert_eq!(database.status().adopted_block.unwrap().block_index(), checkpoint);
    let recovered = database.trie_catalog();
    assert_eq!(recovered.all_tries(&table).iter()
        .filter(|trie| trie.key.level() == 2 && trie.state == TrieState::Nascent).count(), 3);
    let owner = tokio::spawn(driver.run());
    node.sync_all(Duration::from_secs(10), node.cancellation()).await.unwrap();
    assert_eq!(database.trie_catalog().current_tries(&table).iter().filter(|trie| trie.key.level() == 2).count(), 4);
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    assert_eq!(visible(&mut session).await, before);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), owner).await.unwrap().unwrap().unwrap();
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

#[tokio::test]
async fn a_follower_cursor_keeps_its_files_while_its_lease_lasts() {
    follower_cursor_during_collection(false).await;
}

#[tokio::test]
async fn a_follower_cursor_past_its_lease_is_refused_before_reading_collected_files() {
    follower_cursor_during_collection(true).await;
}

async fn follower_cursor_during_collection(expire: bool) {
    use rtdb_format::keys::TrieKey;
    use rtdb_replication::Role;
    use rtdb_storage::{Error, ObjectKey};

    let directory = tempfile::tempdir().unwrap();
    let clock = Arc::new(ClockFixture(AtomicI64::new(1_000_000)));
    let table = TableRef::new("public", "docs");
    let old_trie: TrieKey = "l01-rc-b00".parse().unwrap();
    let slug = rtdb_format::keys::TableSlug::for_unrecorded_table(&table).unwrap();
    let data = ObjectKey::new(slug.data_key(&old_trie)).unwrap();
    let metadata = ObjectKey::new(slug.metadata_key(&old_trie)).unwrap();
    let rules = if expire { vec![Rule {
        owner: "xtdb/store".into(),
        operation: Operation::Delete,
        phase: Phase::After,
        key: KeyMatch::Exact(metadata.as_str().to_owned()),
        occurrence: 1,
        action: Action::Hold("follower-trie-deleted".into()),
    }] } else { vec![] };
    let schedule = FaultSchedule::new(rules, 50_000).unwrap();
    let _close = CloseSchedule(schedule.clone());

    let mut writer_options = options(directory.path());
    writer_options.compaction_threads = 1;
    writer_options.automatic_gc = true;
    writer_options.garbage.blocks_to_keep = 1;
    writer_options.garbage.garbage_lifetime = Duration::ZERO;
    let lease = writer_options.garbage.reader_lease;
    let writer_resources = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), true));
    let writer_services = NodeServices {
        resources: FaultResources::new(writer_resources, schedule.clone()),
        ..services(clock.clone(), schedule.clone())
    };
    let (writer, driver) = Node::open(writer_options, writer_services).await.unwrap();
    let writer_owner = tokio::spawn(driver.run());
    let database = writer.catalog().database("xtdb").unwrap();
    let mut writer_session = writer.session("xtdb", Arc::new(Identity)).unwrap();
    let limit = Duration::from_secs(10);
    let put = |id| vec![TxOp::Sql { sql: format!("INSERT INTO docs RECORDS {{_id: {id}}}"), args: None }];

    assert!(writer_session.state_mut().execute_tx(put(1), AutonomousTxOptions::default()).await.unwrap().committed());
    flush_and_adopt(&database).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    database.object_store().head_fresh(&data).await.unwrap();

    let mut follower_options = options(directory.path());
    follower_options.read_only = true;
    let follower_schedule = FaultSchedule::new(vec![], 50_000).unwrap();
    let (follower, driver) = Node::open(follower_options, services(clock.clone(), follower_schedule)).await.unwrap();
    let follower_owner = tokio::spawn(driver.run());
    follower.sync_all(limit, follower.cancellation()).await.unwrap();
    let follower_database = follower.catalog().database("xtdb").unwrap();
    assert_eq!(follower_database.status().role, Role::Following);
    assert_eq!(follower_database.snapshot().trie_entries(&table).iter()
        .map(|trie| &trie.key).collect::<Vec<_>>(), [&old_trie]);
    let mut follower_session = follower.session("xtdb", Arc::new(Identity)).unwrap();
    let mut held = follower_session.query_sql("SELECT _id FROM docs", vec![]).await.unwrap();

    assert!(writer_session.state_mut().execute_tx(put(2), AutonomousTxOptions::default()).await.unwrap().committed());
    flush_and_adopt(&database).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    assert!(database.trie_catalog().garbage_tries(&table, Timestamp::MAX).iter().any(|trie| trie.key == old_trie));

    let resumed = if expire {
        let deleted = schedule.wait_for_rule(0);
        tokio::pin!(deleted);
        for id in 3.. {
            assert!(id < 10, "the writer must collect the superseded trie once a lease has passed");
            clock.0.fetch_add(i64::try_from(lease.as_micros()).unwrap() * 2, Ordering::Relaxed);
            assert!(writer_session.state_mut().execute_tx(put(id), AutonomousTxOptions::default()).await.unwrap().committed());
            flush_and_adopt(&database).await;
            if tokio::time::timeout(Duration::from_millis(500), &mut deleted).await.is_ok() { break; }
        }
        for object in [&data, &metadata] {
            assert!(matches!(database.object_store().head_fresh(object).await, Err(Error::MissingObject(_))));
        }
        let resumed = tokio::time::timeout(limit, held.next_batch()).await.unwrap();
        schedule.release("follower-trie-deleted").unwrap();
        resumed
    } else {
        for id in 3..6 {
            assert!(writer_session.state_mut().execute_tx(put(id), AutonomousTxOptions::default()).await.unwrap().committed());
            flush_and_adopt(&database).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        database.object_store().head_fresh(&data).await.unwrap();
        database.object_store().head_fresh(&metadata).await.unwrap();
        tokio::time::timeout(limit, held.next_batch()).await.unwrap()
    };
    drop((held, follower_session, writer_session));
    follower.shutdown();
    writer.shutdown();
    for owner in [follower_owner, writer_owner] {
        tokio::time::timeout(limit, owner).await.unwrap().unwrap().unwrap();
    }
    schedule.assert_exhausted().unwrap();

    if expire {
        let Err(error) = resumed else { panic!("the follower cursor read past its lease") };
        assert_eq!(error.code, "rtdb.storage/read-lease-expired", "{error}");
    } else {
        let batch = resumed.unwrap().expect("the follower cursor returns its admitted row");
        assert_eq!(batch.batch().num_rows(), 1);
    }
}
