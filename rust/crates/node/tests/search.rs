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
fn ids(rows: Vec<Vec<Value>>) -> Vec<i64> {
    let mut ids: Vec<i64> = rows.into_iter().map(|row| match row[0] { Value::I64(id) => id, ref other => panic!("unexpected id {other:?}") }).collect();
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn match_selects_the_version_visible_at_the_queried_valid_time() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    for statement in [
        "INSERT INTO docs RECORDS {_id: 1, body: 'The invoice is OVERDUE', _valid_from: TIMESTAMP '2024-01-01Z'}",
        "INSERT INTO docs RECORDS {_id: 2, body: 'An overdue library book', _valid_from: TIMESTAMP '2024-01-01Z'}",
        "INSERT INTO docs RECORDS {_id: 3, body: 42, _valid_from: TIMESTAMP '2024-01-01Z'}",
        "INSERT INTO docs RECORDS {_id: 1, body: 'The invoice is paid', _valid_from: TIMESTAMP '2024-06-01Z'}",
    ] {
        assert!(session.state_mut().execute_tx(vec![sql(statement)], AutonomousTxOptions::default()).await.unwrap().committed(), "{statement}");
    }
    let query = |at: &str| format!("SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '{at}' WHERE MATCH(body, 'overdue INVOICE')");
    assert_eq!(ids(rows(session.query_sql(&query("2024-03-01Z"), vec![]).await.unwrap()).await), [1]);
    assert!(ids(rows(session.query_sql(&query("2024-07-01Z"), vec![]).await.unwrap()).await).is_empty(), "the later version no longer matches");
    let any = "SELECT _id FROM docs FOR VALID_TIME AS OF TIMESTAMP '2024-03-01Z' WHERE MATCH(body, 'overdue')";
    assert_eq!(ids(rows(session.query_sql(any, vec![]).await.unwrap()).await), [1, 2]);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
