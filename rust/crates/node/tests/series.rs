use std::collections::BTreeMap;
use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::*;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_tx::{SqlArguments, TransactionAuthorization, TxOp};
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

struct Fixture { node: Node, driver: tokio::task::JoinHandle<Result<()>>, session: NativeSession }

impl Fixture {
    async fn open() -> Self { Self::open_with(|_| {}).await }
    async fn open_with(configure: impl FnOnce(&mut NodeOptions)) -> Self {
        let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
        let services = NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock, ..NodeServices::local(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures)) };
        let mut options = NodeOptions::default();
        options.replication.assertion_interval = Duration::from_millis(1);
        options.replication.election_seed = 1;
        options.replication.flush_timeout = Duration::from_secs(3600);
        options.automatic_compaction = false;
        options.automatic_gc = false;
        configure(&mut options);
        let (node, driver) = Node::open(options, services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        let session = node.session("xtdb", Arc::new(Authority)).unwrap();
        Self { node, driver, session }
    }
    async fn close(self) {
        drop(self.session);
        self.node.shutdown();
        tokio::time::timeout(Duration::from_secs(60), self.driver).await.unwrap().unwrap().unwrap();
    }
    async fn commit(&mut self, sql: &str, rows: Vec<Vec<Value>>) -> Result<()> {
        let args = if rows.is_empty() { None } else { Some(SqlArguments::from_rows(rows, self.node.memory())?) };
        let result = self.session.state_mut().execute_tx(vec![TxOp::Sql { sql: sql.into(), args }], AutonomousTxOptions::default()).await?;
        assert!(result.committed(), "{sql}: {result:?}");
        Ok(())
    }
    async fn flush(&self) {
        self.node.catalog().database("xtdb").unwrap().flush().await.unwrap();
        self.node.sync_all(Duration::from_secs(60), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    }
    async fn compact(&self) {
        self.node.catalog().database("xtdb").unwrap().compaction().unwrap().compact_all().await.unwrap();
        self.node.sync_all(Duration::from_secs(60), &tokio_util::sync::CancellationToken::new()).await.unwrap();
    }
    async fn rows(&mut self, sql: &str, params: Vec<Value>) -> Vec<Vec<Value>> {
        let mut cursor = self.session.query_sql(sql, params).await.unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            let schema = batch.batch().schema();
            for row in 0..batch.batch().num_rows() {
                rows.push(schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.batch().column(column).as_ref(), row).unwrap()).collect());
            }
        }
        rows
    }
    /// The scan counters EXPLAIN ANALYZE reports, summed over the plan's scans.
    async fn counters(&mut self, sql: &str, params: Vec<Value>) -> BTreeMap<String, i64> {
        fn visit(value: &Value, totals: &mut BTreeMap<String, i64>) {
            let Value::Struct(fields) = value else { return };
            for (name, value) in fields.iter() {
                match value { Value::I64(count) if name.starts_with("scan_") => *totals.entry(name.clone()).or_default() += count, other => visit(other, totals) }
            }
        }
        let mut totals = BTreeMap::new();
        for row in self.rows(&format!("EXPLAIN ANALYZE {sql}"), params).await { for value in &row { visit(value, &mut totals); } }
        totals
    }
}

fn text_id(index: usize) -> Value { Value::Utf8(format!("doc-{index:06}")) }
fn placeholders(count: usize) -> String { vec!["?"; count].join(", ") }

#[tokio::test]
async fn a_long_id_list_of_constants_narrows_reads_and_erases_to_its_iids() {
    let mut fixture = Fixture::open().await;
    const DOCS: usize = 20_000;
    for start in (0..DOCS).step_by(5_000) {
        fixture.commit("INSERT INTO docs (_id, v) VALUES (?, ?)", (start..start + 5_000).map(|index| vec![text_id(index), Value::I64(index as i64)]).collect()).await.unwrap();
    }
    fixture.flush().await;
    let all = fixture.counters("SELECT v FROM docs", vec![]).await;
    assert!(all["scan_pages_used"] > 12, "the table spans more pages than the list has IDs: {all:?}");

    let listed: Vec<Value> = (0..12).map(|index| text_id(index * 1_500 + 7)).collect();
    let select = format!("SELECT v FROM docs WHERE _id IN ({})", placeholders(12));
    let mut found = fixture.rows(&select, listed.clone()).await;
    found.sort_by_key(|row| match row[0] { Value::I64(v) => v, _ => panic!("v is an integer") });
    assert_eq!(found, (0..12).map(|index| vec![Value::I64(index * 1_500 + 7)]).collect::<Vec<_>>());
    let narrowed = fixture.counters(&select, listed.clone()).await;
    assert!(narrowed["scan_pages_used"] <= 12, "one page at most per listed ID, of {}: {narrowed:?}", all["scan_pages_used"]);

    let literals = (0..12).map(|index| format!("'doc-{:06}'", index * 1_500 + 7)).collect::<Vec<_>>().join(", ");
    let by_literal = fixture.counters(&format!("SELECT v FROM docs WHERE _id IN ({literals})"), vec![]).await;
    assert_eq!(by_literal["scan_pages_used"], narrowed["scan_pages_used"]);

    // A list holding another kind of value is compared as a relation of candidates, and reads the table.
    let mut mixed = listed.clone();
    mixed[11] = Value::I64(7);
    assert_eq!(fixture.rows(&select, mixed).await.len(), 11);

    let erased: Vec<Value> = (0..256).map(|index| text_id(index * 70 + 3)).collect();
    fixture.commit(&format!("ERASE FROM docs WHERE _id IN ({})", placeholders(256)), vec![erased.clone()]).await.unwrap();
    assert_eq!(fixture.rows("SELECT COUNT(*) FROM docs", vec![]).await, [vec![Value::I64((DOCS - 256) as i64)]]);
    assert!(fixture.rows(&format!("SELECT _id FROM docs WHERE _id IN ({})", placeholders(256)), erased).await.is_empty());
    fixture.close().await;
}
