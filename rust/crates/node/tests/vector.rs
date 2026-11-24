use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::*;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::QueryCursor;
use rtdb_tx::{TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, Result, TableRef, Timestamp, Value};

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
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock, ..NodeServices::local_with_search_format(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures), rtdb_node::SearchFileFormat::Paged) }
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
fn sql(statement: &str) -> TxOp { TxOp::Sql { sql: statement.into(), args: None } }

fn ids(rows: Vec<Vec<Value>>) -> Vec<i64> {
    rows.into_iter().map(|row| match row[0] { Value::I64(id) => id, ref other => panic!("unexpected id {other:?}") }).collect()
}

async fn commit(session: &mut NativeSession, statement: &str) {
    assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed(), "{statement}");
}

fn nearest(label: &str, approximate: bool) -> String {
    let opt_in = if approximate { " AND APPROXIMATE_NEAREST(embedding, ARRAY[1.0, 0.25], 3)" } else { "" };
    format!("SELECT _id FROM vecs WHERE label = '{label}'{opt_in} ORDER BY COSINE_DISTANCE(embedding, ARRAY[1.0, 0.25]), _id LIMIT 3")
}

#[tokio::test]
async fn approximate_nearest_ranks_exactly_without_a_vector_index() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for (id, x, y) in [(1, 1.0, 0.0), (2, 0.0, 1.0), (3, 0.9, 0.3), (4, -1.0, 0.0)] {
        commit(&mut session, &format!("INSERT INTO vecs RECORDS {{_id: {id}, label: 'a', embedding: [{x}, {y}]}}")).await;
    }
    let exact = ids(rows(session.query_sql(&nearest("a", false), vec![]).await.unwrap()).await);
    let approximate = ids(rows(session.query_sql(&nearest("a", true), vec![]).await.unwrap()).await);
    assert_eq!(exact, [3, 1, 2]);
    assert_eq!(approximate, exact);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn vector_index_definitions_require_their_options() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    commit(&mut session, "INSERT INTO vecs RECORDS {_id: 1, label: 'a', embedding: [1.0, 0.0]}").await;
    for statement in [
        "CREATE VECTOR INDEX vecs_embedding ON vecs (embedding)",
        "CREATE VECTOR INDEX vecs_embedding ON vecs (embedding) WITH (dimensions = 2, method = 'hnsw', recall = 0.9)",
        "CREATE VECTOR INDEX vecs_embedding ON vecs (embedding) WITH (dimensions = 2, method = 'ivf', recall = 1.5)",
    ] {
        let result = session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await;
        assert!(result.map_or(true, |result| !result.committed()), "{statement}");
    }
    commit(&mut session, "CREATE VECTOR INDEX vecs_embedding ON vecs (embedding) PARTITION BY (label) WITH (dimensions = 2, method = 'ivf', recall = 0.95)").await;
    commit(&mut session, "CREATE VECTOR INDEX IF NOT EXISTS vecs_embedding ON vecs (embedding) WITH (dimensions = 2, method = 'ivf', recall = 0.5)").await;
    let definition = rows(session.query_sql("SELECT column_name, partition_columns, dimensions, method, recall FROM xt.vector_indexes WHERE _id = 'vecs_embedding'", vec![]).await.unwrap()).await;
    assert_eq!(definition.len(), 1);
    assert_eq!(definition[0][0], Value::Utf8("embedding".into()));
    assert_eq!(definition[0][2], Value::I64(2));
    assert_eq!(definition[0][3], Value::Utf8("ivf".into()));
    assert_eq!(definition[0][4], Value::F64(0.95));
    commit(&mut session, "DROP VECTOR INDEX vecs_embedding").await;
    assert!(rows(session.query_sql("SELECT _id FROM xt.vector_indexes", vec![]).await.unwrap()).await.is_empty());
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn the_approximate_route_reads_published_generations_and_the_frontier_at_the_cut() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    commit(&mut session, "CREATE VECTOR INDEX vecs_embedding ON vecs (embedding) PARTITION BY (label) WITH (dimensions = 2, method = 'ivf', recall = 0.95)").await;
    for id in 0..40 {
        let angle = f64::from(id) * 0.15;
        let label = if id % 2 == 0 { "a" } else { "b" };
        commit(&mut session, &format!("INSERT INTO vecs RECORDS {{_id: {id}, label: '{label}', embedding: [{}, {}]}}", angle.cos(), angle.sin())).await;
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let catalog = database.trie_catalog();
    let settled: Vec<_> = catalog.current_tries(&TableRef::new("public", "vecs")).into_iter().filter(|trie| trie.key.level() > 0).collect();
    assert!(!settled.is_empty(), "compaction produced no settled trie");
    for trie in settled {
        let record = trie.artefacts.iter().find(|record| record.kind() == "vector").unwrap_or_else(|| panic!("{} has no vector generation", trie.key));
        assert_eq!(record.definition_id(), "vecs_embedding~ivff1");
    }

    commit(&mut session, "INSERT INTO vecs RECORDS {_id: 100, label: 'a', embedding: [1.0, 0.24]}").await;
    commit(&mut session, "ERASE FROM vecs WHERE _id = 2").await;
    let exact = ids(rows(session.query_sql(&nearest("a", false), vec![]).await.unwrap()).await);
    let approximate = ids(rows(session.query_sql(&nearest("a", true), vec![]).await.unwrap()).await);
    assert_eq!(exact[0], 100, "the live frontier row is nearest");
    assert!(!exact.contains(&2));
    assert_eq!(approximate, exact, "one cluster per small partition probes every row");
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
