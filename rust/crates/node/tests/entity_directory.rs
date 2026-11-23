#![cfg(feature = "search")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_format::{cow_tree, entity_directory, keys::TrieKey};
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{
    AutonomousTxOptions, Clock, DatabaseAccess, NativeResources, Node, NodeOptions, NodeServices,
    SessionIdentity,
};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::{
    ObjectPageStore, QueryCursor,
    entity_directory::{DirectoryRead, table_events},
};
use rtdb_storage::ObjectKey;
use rtdb_tx::{TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, BlockIndex, EventOp, Result, TableRef, TimeUnit, Timestamp, Value};
use tokio_util::sync::CancellationToken;

struct Authority;

impl TransactionAuthorization for Authority {
    fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SessionIdentity for Authority {
    fn user(&self) -> Option<&str> {
        Some("xtdb")
    }
    async fn revalidate(&self, _: Timestamp) -> Result<()> {
        Ok(())
    }
    fn authorize_database(&self, _: &str, _: DatabaseAccess) -> Result<()> {
        Ok(())
    }
}

struct Observer;
impl CollectionObserver for Observer {
    fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly) {
        panic!("collection for {table:?}: {error}");
    }
}

struct CatalogFixtures;
impl CatalogUsers for CatalogFixtures {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> {
        Ok(CatalogRows::new(Vec::new(), Arc::new(())))
    }
    fn is_superuser(&self, _: &str) -> bool {
        false
    }
}
impl CatalogMetrics for CatalogFixtures {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        Ok(CatalogRows::new(Vec::new(), Arc::new(())))
    }
}

struct TickClock(AtomicI64);
impl Clock for TickClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.fetch_add(1000, Ordering::Relaxed))
    }
}

fn sql(sql: &str) -> TxOp {
    TxOp::Sql {
        sql: sql.into(),
        args: None,
    }
}

async fn rows(mut cursor: QueryCursor) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        for row in 0..batch.batch().num_rows() {
            rows.push(
                batch
                    .batch()
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, field)| {
                        read_value(field, batch.batch().column(i).as_ref(), row).unwrap()
                    })
                    .collect(),
            );
        }
    }
    rows
}

#[tokio::test]
async fn sealed_directory_points_survive_missing_history_relocation_and_erase() {
    let runtime = rtdb_search::SearchRuntime::with_logical_search(64 << 20);
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let services = NodeServices {
        resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)),
        clock,
        artefact_kinds: vec![runtime.artefacts()],
        scan_accelerators: vec![runtime.accelerator()],
        ..NodeServices::local(
            Arc::new(Authority),
            Arc::new(Observer),
            Arc::new(CatalogFixtures),
            Arc::new(CatalogFixtures),
        )
    };
    let mut options = NodeOptions::default();
    options.replication.election_seed = 1;
    options.replication.assertion_interval = Duration::from_millis(1);
    options.replication.flush_timeout = Duration::from_secs(3600);
    options.automatic_gc = false;
    options.automatic_compaction = false;
    options.automatic_artefacts = false;
    options.file_size_target = 1;

    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let table = TableRef::new("public", "docs");
    let cancellation = CancellationToken::new();
    let flush = async || {
        database.flush().await.unwrap();
        node.sync_all(Duration::from_secs(20), &cancellation)
            .await
            .unwrap();
    };
    let first = session
        .state_mut()
        .execute_tx(
            vec![sql(
                "INSERT INTO docs RECORDS {_id: 1, v: 10}, {_id: 2, v: 20}",
            )],
            AutonomousTxOptions::default(),
        )
        .await
        .unwrap();
    assert!(first.committed());
    let before_erase = Value::Timestamp {
        value: first.tx_key().system_time.0,
        unit: TimeUnit::Microsecond,
        timezone: Some("UTC".into()),
    };
    flush().await;

    let before = database.snapshot();
    let original = before
        .trie_catalog()
        .entity_directory(&table)
        .unwrap()
        .clone();
    assert_eq!(original.source_rows(), 2);
    let entry = before.table_entry(&table).unwrap();
    let first_trie = TrieKey::l0(BlockIndex(original.through_block())).unwrap();
    let artefacts = before.trie_catalog().trie(&table, &first_trie).unwrap();
    let history = artefacts
        .artefact(
            rtdb_format::entity_history::KIND,
            rtdb_format::entity_history::ID,
            rtdb_format::entity_history::VERSION,
        )
        .unwrap();
    let oracle = rows(
        session
            .query_sql(
                "SELECT v FROM docs WHERE _id = ? OR v = -999999",
                vec![Value::I64(1)],
            )
            .await
            .unwrap(),
    )
    .await;
    let mut removed = Vec::new();
    for key in [
        entry.slug.metadata_key(&first_trie),
        entry
            .slug
            .recorded_artefact_key(&first_trie, history)
            .unwrap(),
    ] {
        let key = ObjectKey::new(key).unwrap();
        removed.push((
            key.clone(),
            database.object_store().get(&key).await.unwrap(),
        ));
        database.storage().invalidate(&key).await.unwrap();
        database.object_store().delete(&key).await.unwrap();
    }
    let point = "SELECT v FROM docs WHERE _id = ?";
    assert_eq!(
        rows(session.query_sql(point, vec![Value::I64(1)]).await.unwrap()).await,
        oracle
    );
    for (key, bytes) in removed {
        database.object_store().put(&key, bytes).await.unwrap();
    }

    assert!(
        session
            .state_mut()
            .execute_tx(
                vec![
                    sql("ERASE FROM docs WHERE _id = 1"),
                    sql("INSERT INTO docs RECORDS {_id: 1, v: 11}")
                ],
                AutonomousTxOptions::default()
            )
            .await
            .unwrap()
            .committed()
    );
    flush().await;
    let captured = database.snapshot();
    let latest = captured.trie_catalog().entity_directory(&table).unwrap();
    assert!(latest.through_block() > original.through_block());
    assert_eq!(latest.source_rows(), 3);
    assert!(latest.has_erases());
    assert_ne!(
        latest.erase_frontier_digest(),
        original.erase_frontier_digest()
    );

    for (snapshot, expected) in [(&before, 2), (&captured, 3)] {
        let read = DirectoryRead {
            snapshot,
            storage: database.storage(),
            memory: node.memory(),
            cancellation: &cancellation,
        };
        let mut events = Vec::new();
        table_events(&read, &table, |event| {
            events.push(event);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(events.len(), expected);
        assert_eq!(
            events
                .iter()
                .filter(|event| event.header.op == EventOp::Erase)
                .count(),
            expected - 2
        );
    }

    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(20), &cancellation)
        .await
        .unwrap();
    let compacted = database.snapshot();
    assert_eq!(
        compacted.trie_catalog().entity_directory(&table),
        Some(latest)
    );
    assert!(
        !compacted
            .trie_catalog()
            .current_tries(&table)
            .iter()
            .any(|trie| trie.key == first_trie)
    );
    for (id, expected) in [(1, Some(11)), (2, Some(20)), (3, None)] {
        for system_time in ["", " FOR SYSTEM_TIME AS OF ?"] {
            let mut args = Vec::new();
            if !system_time.is_empty() {
                args.push(before_erase.clone());
            }
            args.push(Value::I64(id));
            let point = format!("SELECT v FROM docs{system_time} WHERE _id = ?");
            let scan = format!("{point} OR v = -999999");
            let found = rows(session.query_sql(&point, args.clone()).await.unwrap()).await;
            assert_eq!(
                found,
                rows(session.query_sql(&scan, args).await.unwrap()).await
            );
            let expected = expected.filter(|_| id != 1 || system_time.is_empty());
            assert_eq!(
                found,
                expected
                    .into_iter()
                    .map(|v| vec![Value::I64(v)])
                    .collect::<Vec<_>>()
            );
        }
    }

    let store = ObjectPageStore {
        store: database.object_store().clone(),
        storage: database.storage().clone(),
        prefix: entity_directory::prefix(&entry.slug),
    };
    let pages = cow_tree::reachable(&store, &latest.reference().unwrap())
        .await
        .unwrap();
    assert!(!pages.is_empty());
    use rtdb_format::catalog::{CheckpointContents, TableBlock};
    let catalog = compacted.trie_catalog();
    let block = TableBlock::from_parts(
        Default::default(),
        &Default::default(),
        4,
        catalog.partitions(&table).unwrap(),
    )
    .unwrap();
    let block = catalog.with_entity_directory(&table, block);
    let closure = block
        .checkpoint_trie_files_with_pages(&entry.slug, CheckpointContents::Replay, &store)
        .await
        .unwrap();
    assert!(!closure.contains(&entry.slug.data_key(&first_trie)));
    for page in pages {
        assert!(closure.contains(&cow_tree::page_key(&store.prefix, page)));
    }
    drop(session);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(20), driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
