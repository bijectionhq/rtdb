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

/// A clustered UUID: the series in the top 32 bits, then the instant, then a discriminator.
fn point_id(series: u32, at: u64, discriminator: u32) -> u128 { (u128::from(series) << 96) | (u128::from(at) << 32) | u128::from(discriminator) }
fn uuid(id: u128) -> Value { Value::Uuid(id.to_be_bytes()) }
fn mix(mut x: u64) -> u64 { x = x.wrapping_add(0x9e3779b97f4a7c15); x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9); x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb); x ^ (x >> 31) }
fn ids(rows: Vec<Vec<Value>>) -> Vec<u128> {
    let mut ids: Vec<u128> = rows.into_iter().map(|row| match &row[0] { Value::Uuid(bytes) => u128::from_be_bytes(*bytes), other => panic!("expected a UUID, found {other:?}") }).collect();
    ids.sort_unstable();
    ids
}

/// 40 series of 500 points each, the first three quarters persisted and compacted, the rest live.
async fn points(fixture: &mut Fixture) -> Vec<u128> {
    let all: Vec<u128> = (0..40u32).flat_map(|series| (0..500u64).map(move |tick| point_id(series * 0x0400_0001, tick * 60, mix(u64::from(series) << 32 | tick) as u32))).collect();
    let (persisted, live) = all.split_at(all.len() * 3 / 4);
    let rows = |ids: &[u128]| ids.iter().map(|id| vec![uuid(*id), Value::I64((*id >> 32) as i64 & 0xffff_ffff)]).collect::<Vec<_>>();
    // The order of arrival is not the order of the IDs.
    let mut shuffled = persisted.to_vec();
    shuffled.sort_by_key(|id| mix(*id as u64));
    for chunk in shuffled.chunks(5_000) { fixture.commit("INSERT INTO pts (_id, at) VALUES (?, ?)", rows(chunk)).await.unwrap(); }
    fixture.flush().await;
    fixture.compact().await;
    fixture.commit("INSERT INTO pts (_id, at) VALUES (?, ?)", rows(live)).await.unwrap();
    let mut all = all;
    all.sort_unstable();
    all
}

#[tokio::test]
async fn an_id_range_over_uuid_ids_reads_only_the_pages_of_its_iid_interval() {
    let mut fixture = Fixture::open().await;
    let all = points(&mut fixture).await;
    assert_eq!(ids(fixture.rows("SELECT _id FROM pts", vec![]).await), all);
    let whole = fixture.counters("SELECT _id FROM pts", vec![]).await;

    let expect = |keep: &dyn Fn(u128) -> bool| all.iter().copied().filter(|id| keep(*id)).collect::<Vec<_>>();
    let series = 7 * 0x0400_0001u32;
    let (first, last) = (point_id(series, 100 * 60, 0), point_id(series, 200 * 60, 0));
    let present = all[all.len() / 3];
    let cases: Vec<(&str, Vec<Value>, Vec<u128>)> = vec![
        ("_id >= ? AND _id < ?", vec![uuid(first), uuid(last)], expect(&|id| id >= first && id < last)),
        ("_id > ? AND _id <= ?", vec![uuid(first), uuid(last)], expect(&|id| id > first && id <= last)),
        ("_id BETWEEN ? AND ?", vec![uuid(first), uuid(last)], expect(&|id| id >= first && id <= last)),
        ("? <= _id AND ? > _id", vec![uuid(first), uuid(last)], expect(&|id| id >= first && id < last)),
        // Bounds that are rows: inclusive bounds keep them, strict bounds drop them.
        ("_id >= ? AND _id <= ?", vec![uuid(present), uuid(present)], vec![present]),
        ("_id > ? AND _id < ?", vec![uuid(present), uuid(all[all.len() / 3 + 2])], vec![all[all.len() / 3 + 1]]),
        ("_id >= ? AND _id < ?", vec![uuid(present), uuid(present)], vec![]),
        ("_id > ? AND _id <= ?", vec![uuid(last), uuid(first)], vec![]),
        // The lowest and highest IDs there are, and the ends of the ID space.
        ("_id <= ?", vec![uuid(all[0])], vec![all[0]]),
        ("_id >= ?", vec![uuid(*all.last().unwrap())], vec![*all.last().unwrap()]),
        ("_id < ?", vec![uuid(0)], vec![]),
        ("_id > ?", vec![uuid(u128::MAX)], vec![]),
        ("_id >= ? AND _id <= ?", vec![uuid(0), uuid(u128::MAX)], all.clone()),
        // Mixed with predicates on other columns, in either order.
        ("at >= 6600 AND _id >= ? AND _id < ? AND at < 9000", vec![uuid(first), uuid(last)], expect(&|id| id >= first && id < last && (6600..9000).contains(&((id >> 32) as u64 & 0xffff_ffff)))),
        ("(_id >= ? AND _id < ?) AND (at = 6600 OR at = 7200)", vec![uuid(first), uuid(last)], expect(&|id| id >= first && id < last && [6600, 7200].contains(&((id >> 32) as u64 & 0xffff_ffff)))),
        // A disjunction of ranges is evaluated on every row.
        ("_id < ? OR _id >= ?", vec![uuid(first), uuid(last)], expect(&|id| id < first || id >= last)),
    ];
    for (predicate, params, expected) in &cases {
        let sql = format!("SELECT _id FROM pts WHERE {predicate}");
        assert_eq!(&ids(fixture.rows(&sql, params.clone()).await), expected, "{sql}");
    }
    // A live range in the last series, which was never persisted.
    let series = 39 * 0x0400_0001u32;
    let (first, last) = (point_id(series, 0, 0), point_id(series, 50 * 60, 0));
    assert_eq!(ids(fixture.rows("SELECT _id FROM pts WHERE _id >= ? AND _id < ?", vec![uuid(first), uuid(last)]).await), expect(&|id| id >= first && id < last));

    let series = 7 * 0x0400_0001u32;
    let (first, last) = (point_id(series, 100 * 60, 0), point_id(series, 200 * 60, 0));
    let confined = fixture.counters("SELECT _id FROM pts WHERE _id >= ? AND _id < ?", vec![uuid(first), uuid(last)]).await;
    assert!(confined["scan_pages_used"] * 8 <= whole["scan_pages_used"], "100 of 20,000 rows read {confined:?} of {whole:?}");
    assert!(confined["scan_rows_read"] <= 4 * 1024, "{confined:?}");
    let literal = |id: u128| { let hex = format!("{id:032x}"); format!("UUID '{}-{}-{}-{}-{}'", &hex[..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..]) };
    let by_literal = fixture.counters(&format!("SELECT _id FROM pts WHERE _id >= {} AND _id < {}", literal(first), literal(last)), vec![]).await;
    assert_eq!(by_literal, confined);
    let nothing = fixture.counters("SELECT _id FROM pts WHERE _id > ? AND _id < ?", vec![uuid(last), uuid(first)]).await;
    assert_eq!(nothing.get("scan_pages_used").copied().unwrap_or(0), 0, "{nothing:?}");
    fixture.close().await;
}

#[tokio::test]
async fn an_id_range_over_hashed_ids_is_evaluated_on_every_row() {
    let mut fixture = Fixture::open().await;
    // Text IDs, and a table holding both text and UUID IDs: neither orders its IIDs as it orders its IDs.
    fixture.commit("INSERT INTO docs (_id, v) VALUES (?, ?)", (0..3_000).map(|index| vec![text_id(index), Value::I64(index as i64)]).collect()).await.unwrap();
    let mixed: Vec<Vec<Value>> = (0..3_000u64).map(|index| vec![if index % 2 == 0 { uuid(u128::from(index) << 64) } else { text_id(index as usize) }, Value::I64(index as i64)]).collect();
    fixture.commit("INSERT INTO mixed (_id, v) VALUES (?, ?)", mixed).await.unwrap();
    fixture.flush().await;
    fixture.compact().await;

    let texts = fixture.rows("SELECT v FROM docs WHERE _id >= ? AND _id < ? ORDER BY v", vec![text_id(1_000), text_id(1_100)]).await;
    assert_eq!(texts, (1_000..1_100).map(|index| vec![Value::I64(index)]).collect::<Vec<_>>());
    let integers = "INSERT INTO numbered (_id, v) VALUES (?, ?)";
    fixture.commit(integers, (0..3_000).map(|index| vec![Value::I64(index), Value::I64(index)]).collect()).await.unwrap();
    fixture.flush().await;
    let numbers = fixture.rows("SELECT v FROM numbered WHERE _id >= 1000 AND _id < 1100 ORDER BY v", vec![]).await;
    assert_eq!(numbers, (1_000..1_100).map(|index| vec![Value::I64(index)]).collect::<Vec<_>>());

    let (first, last) = (uuid(1_000u128 << 64), uuid(1_100u128 << 64));
    let uuids = fixture.rows("SELECT v FROM mixed WHERE _id >= ? AND _id < ? ORDER BY v", vec![first, last]).await;
    assert_eq!(uuids, (1_000..1_100).filter(|index| index % 2 == 0).map(|index| vec![Value::I64(index)]).collect::<Vec<_>>());
    fixture.close().await;
}
