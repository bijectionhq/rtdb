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
    NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock, ..NodeServices::local(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures)) }
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

#[tokio::test]
async fn versions_one_transaction_wrote_resolve_in_its_order_after_compaction_splits_them_by_recency() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let statements = vec![
        sql("INSERT INTO docs RECORDS {_id: 1, v: 'a', _valid_from: TIMESTAMP '2020-01-01Z'}"),
        sql("INSERT INTO docs RECORDS {_id: 1, v: 'b', _valid_from: TIMESTAMP '2021-01-01Z'}"),
    ];
    assert!(session.state_mut().execute_tx(statements, AutonomousTxOptions::default()).await.unwrap().committed());
    let queries = [
        ("SELECT v FROM docs FOR SYSTEM_TIME ALL FOR VALID_TIME AS OF TIMESTAMP '2022-06-01Z'", vec![vec![Value::Utf8("b".into())]]),
        ("SELECT v FROM docs FOR SYSTEM_TIME ALL FOR VALID_TIME AS OF TIMESTAMP '2020-06-01Z'", vec![vec![Value::Utf8("a".into())]]),
        ("SELECT v FROM docs FOR VALID_TIME AS OF TIMESTAMP '2022-06-01Z'", vec![vec![Value::Utf8("b".into())]]),
    ];
    for (query, expected) in &queries { assert_eq!(&rows(session.query_sql(query, vec![]).await.unwrap()).await, expected, "live: {query}"); }
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    for (query, expected) in &queries { assert_eq!(&rows(session.query_sql(query, vec![]).await.unwrap()).await, expected, "level 0: {query}"); }
    database.compaction().unwrap().compact_all().await.unwrap();
    let tries: Vec<String> = database.trie_catalog().current_tries(&TableRef::new("public", "docs")).iter().map(|trie| trie.key.to_string()).collect();
    assert!(tries.iter().any(|key| key.contains("-r")), "compaction split the versions by recency: {tries:?}");
    for (query, expected) in &queries { assert_eq!(&rows(session.query_sql(query, vec![]).await.unwrap()).await, expected, "compacted into {tries:?}: {query}"); }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn compaction_merges_a_column_holding_text_numbers_and_nothing() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    for (id, record) in [(0, "body: 'text'"), (1, "body: 42"), (2, "title: 'untitled'"), (3, "body: 'more text'")] {
        assert!(session.state_mut().execute_tx(vec![sql(&format!("INSERT INTO docs RECORDS {{_id: {id}, {record}}}"))], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    assert_eq!(rows(session.query_sql("SELECT _id FROM docs ORDER BY _id", vec![]).await.unwrap()).await.len(), 4);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
