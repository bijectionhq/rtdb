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

#[tokio::test]
async fn search_index_definitions_are_documents_as_of_their_transaction() {
    let (node, driver) = Node::open(options(), services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    let created = session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_text ON docs (title, body)")], AutonomousTxOptions::default()).await.unwrap();
    assert!(created.committed());
    let definitions = "SELECT _id, schema_name, table_name, columns, analyzer FROM xt.search_indexes";
    assert_eq!(rows(session.query_sql(definitions, vec![]).await.unwrap()).await, [vec![
        Value::Utf8("docs_text".into()), Value::Utf8("public".into()), Value::Utf8("docs".into()),
        Value::List(vec![Value::Utf8("title".into()), Value::Utf8("body".into())]), Value::Utf8("simple".into()),
    ]]);
    let version = rows(session.query_sql("SELECT version FROM xt.search_indexes", vec![]).await.unwrap()).await;
    assert_eq!(version, [vec![Value::I64(created.tx_key().tx_id.0)]], "the version is the defining transaction");
    assert!(session.state_mut().execute_tx(vec![sql("DROP SEARCH INDEX docs_text")], AutonomousTxOptions::default()).await.unwrap().committed());
    assert!(rows(session.query_sql(definitions, vec![]).await.unwrap()).await.is_empty());
    let created_at = Value::Timestamp { value: created.tx_key().system_time.0, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) };
    let before_drop = "SELECT _id FROM xt.search_indexes FOR SYSTEM_TIME AS OF ?";
    assert_eq!(rows(session.query_sql(before_drop, vec![created_at]).await.unwrap()).await, [vec![Value::Utf8("docs_text".into())]]);
    for invalid in ["CREATE SEARCH INDEX docs_text ON docs ()", "CREATE SEARCH INDEX docs_text ON docs (_id)", "CREATE SEARCH INDEX docs_text ON docs (body, body)", "DROP SEARCH INDEX"] {
        assert!(session.state_mut().execute_tx(vec![sql(invalid)], AutonomousTxOptions::default()).await.map(|result| !result.committed()).unwrap_or(true), "{invalid}");
    }
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn the_lane_records_a_search_file_on_every_settled_trie_of_an_indexed_table() {
    let mut options = options();
    options.file_size_target = 1;
    options.automatic_artefacts = false;
    let (node, driver) = Node::open(options, services()).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    assert!(session.state_mut().execute_tx(vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")], AutonomousTxOptions::default()).await.unwrap().committed());
    for id in 0..20 {
        let statement = format!("INSERT INTO docs RECORDS {{_id: {id}, body: 'document number {id} about {}'}}", if id % 2 == 0 { "invoices" } else { "books" });
        assert!(session.state_mut().execute_tx(vec![sql(&statement)], AutonomousTxOptions::default()).await.unwrap().committed());
    }
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    node.sync_all(Duration::from_secs(10), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    database.build_artefacts().await.unwrap();
    let docs = TableRef::new("public", "docs");
    let catalog = database.trie_catalog();
    let settled: Vec<_> = catalog.current_tries(&docs).into_iter().filter(|trie| trie.key.level() > 0).collect();
    assert!(!settled.is_empty(), "compaction produced no settled trie");
    for trie in settled {
        let record = trie.artefacts.iter().find(|record| record.kind() == "search").unwrap_or_else(|| panic!("{} has no search file", trie.key));
        assert!(record.definition_id().starts_with("docs_body~a"));
        let stats = rtdb_search::SearchStats::decode(record.stats()).unwrap();
        assert!(stats.documents > 0 && stats.tokens["body"] >= 5 * stats.documents);
    }
    database.build_artefacts().await.unwrap();
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}
