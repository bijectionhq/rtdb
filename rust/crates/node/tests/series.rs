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

struct Fixture { node: Node, driver: tokio::task::JoinHandle<Result<()>>, session: NativeSession, clock: Arc<TickClock> }

impl Fixture {
    async fn open() -> Self { Self::open_at(1_000_000).await }
    /// A node whose clock, and so its system time, starts at `now`.
    async fn open_at(now: i64) -> Self {
        let clock = Arc::new(TickClock(AtomicI64::new(now)));
        let services = NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock: clock.clone(), ..NodeServices::local(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures)) };
        let mut options = NodeOptions::default();
        options.replication.assertion_interval = Duration::from_millis(1);
        options.replication.election_seed = 1;
        options.replication.flush_timeout = Duration::from_secs(3600);
        options.automatic_compaction = false;
        options.automatic_gc = false;
        let (node, driver) = Node::open(options, services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        let session = node.session("xtdb", Arc::new(Authority)).unwrap();
        Self { node, driver, session, clock }
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

#[tokio::test]
async fn a_limit_over_uuid_ids_in_id_order_stops_reading_once_it_has_its_rows() {
    let mut fixture = Fixture::open().await;
    let all = points(&mut fixture).await;
    let whole = fixture.counters("SELECT _id FROM pts", vec![]).await;
    let in_order = |rows: Vec<Vec<Value>>| rows.into_iter().map(|row| match &row[0] { Value::Uuid(bytes) => u128::from_be_bytes(*bytes), other => panic!("expected a UUID, found {other:?}") }).collect::<Vec<_>>();
    let at = |id: u128| (id >> 32) as u64 & 0xffff_ffff;

    // The newest and oldest points of one series: its ID prefix, in either order.
    let series = 7 * 0x0400_0001u32;
    let (first, last) = (point_id(series, 0, 0), point_id(series + 1, 0, 0) - 1);
    let of_series: Vec<u128> = all.iter().copied().filter(|id| *id >= first && *id <= last).collect();
    let range = vec![uuid(first), uuid(last)];
    let latest = "SELECT _id FROM pts WHERE _id >= ? AND _id <= ? ORDER BY _id DESC LIMIT 1";
    assert_eq!(in_order(fixture.rows(latest, range.clone()).await), [*of_series.last().unwrap()]);
    let counters = fixture.counters(latest, range.clone()).await;
    assert!(counters["scan_pages_used"] <= 2, "the newest row is in the last page of the interval: {counters:?} of {whole:?}");
    let earliest = "SELECT _id FROM pts WHERE _id >= ? AND _id <= ? ORDER BY _id LIMIT 3";
    assert_eq!(in_order(fixture.rows(earliest, range.clone()).await), of_series[..3]);
    assert!(fixture.counters(earliest, range.clone()).await["scan_pages_used"] <= 2);

    // The newest point at or before an instant, and a window further in.
    let before = point_id(series, 250 * 60, 0);
    let expected: Vec<u128> = of_series.iter().rev().copied().filter(|id| *id <= before).take(5).collect();
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts WHERE _id >= ? AND _id <= ? ORDER BY _id DESC LIMIT 5", vec![uuid(first), uuid(before)]).await), expected);
    let expected: Vec<u128> = of_series.iter().rev().copied().skip(10).take(4).collect();
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts WHERE _id >= ? AND _id <= ? ORDER BY _id DESC LIMIT 4 OFFSET 10", range.clone()).await), expected);

    // A predicate on another column is applied before the limit counts a row.
    let expected: Vec<u128> = of_series.iter().rev().copied().filter(|id| at(*id) < 6_000).take(2).collect();
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts WHERE _id >= ? AND _id <= ? AND at < 6000 ORDER BY _id DESC LIMIT 2", range.clone()).await), expected);
    let expected: Vec<u128> = of_series.iter().copied().filter(|id| at(*id) % 600 == 0 && at(*id) > 0).take(3).collect();
    assert_eq!(in_order(fixture.rows("SELECT _id FROM (SELECT _id, at FROM pts WHERE _id >= ? AND _id <= ?) AS p WHERE p.at = 600 OR p.at = 1200 OR p.at = 1800 ORDER BY _id LIMIT 3", range.clone()).await), expected);

    // A predicate that no page's statistics admit: each leaf is tested when the read reaches it, and none is read.
    let unmatched = "SELECT _id FROM pts WHERE _id >= ? AND _id <= ? AND at > 1000000 ORDER BY _id DESC LIMIT 1";
    assert!(fixture.rows(unmatched, range.clone()).await.is_empty());
    let counters = fixture.counters(unmatched, range.clone()).await;
    assert!(counters["scan_pages_used"] == 0 && counters["scan_pages_pruned"] > 0, "{counters:?}");

    // The whole table, whose newest rows are live and whose oldest are persisted.
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts ORDER BY _id DESC LIMIT 7", vec![]).await), all.iter().rev().copied().take(7).collect::<Vec<_>>());
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts ORDER BY _id LIMIT 7", vec![]).await), all[..7]);
    let counters = fixture.counters("SELECT _id FROM pts ORDER BY _id LIMIT 7", vec![]).await;
    assert!(counters["scan_pages_used"] * 8 <= whole["scan_pages_used"], "{counters:?} of {whole:?}");
    // More rows than a page holds, across many scan tasks, and more than there are.
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts ORDER BY _id DESC LIMIT 3000", vec![]).await), all.iter().rev().copied().take(3_000).collect::<Vec<_>>());
    assert_eq!(in_order(fixture.rows("SELECT _id FROM pts WHERE _id >= ? AND _id <= ? ORDER BY _id DESC LIMIT 100000", range.clone()).await), of_series.iter().rev().copied().collect::<Vec<_>>());
    assert!(fixture.rows("SELECT _id FROM pts WHERE _id > ? AND _id < ? ORDER BY _id DESC LIMIT 1", vec![uuid(last), uuid(first)]).await.is_empty());

    // A superseded version and a deleted row are not counted towards the limit.
    let newest = *of_series.last().unwrap();
    fixture.commit("UPDATE pts SET at = -1 WHERE _id = ?", vec![vec![uuid(newest)]]).await.unwrap();
    fixture.commit("DELETE FROM pts WHERE _id = ?", vec![vec![uuid(of_series[of_series.len() - 2])]]).await.unwrap();
    assert_eq!(fixture.rows("SELECT _id, at FROM pts WHERE _id >= ? AND _id <= ? ORDER BY _id DESC LIMIT 2", range.clone()).await,
        [vec![uuid(newest), Value::I64(-1)], vec![uuid(of_series[of_series.len() - 3]), Value::I64(at(of_series[of_series.len() - 3]) as i64)]]);
    fixture.close().await;
}

/// The first column of a cursor's rows, or the code of the error that ended it.
async fn first_column(cursor: Result<rtdb_query::QueryCursor>) -> std::result::Result<Vec<Value>, String> {
    let mut cursor = cursor.map_err(|error| error.code)?;
    let mut values = Vec::new();
    while let Some(batch) = cursor.next_batch().await.map_err(|error| error.code)? {
        for row in 0..batch.batch().num_rows() { values.push(read_value(batch.batch().schema().field(0), batch.batch().column(0).as_ref(), row).unwrap()); }
    }
    Ok(values)
}

#[tokio::test]
async fn a_plan_prepared_over_uuid_ids_answers_as_a_fresh_one_once_the_table_holds_another_id_type() {
    let mut fixture = Fixture::open().await;
    let rows = |ids: std::ops::Range<u64>| ids.map(|index| vec![uuid(u128::from(index) << 64), Value::I64(index as i64)]).collect::<Vec<_>>();
    fixture.commit("INSERT INTO docs (_id, v) VALUES (?, ?)", rows(0..2_000)).await.unwrap();
    fixture.flush().await;
    let statements = ["SELECT v FROM docs WHERE _id >= ? AND _id < ? ORDER BY v", "SELECT v FROM docs WHERE v < ? OR v > ? ORDER BY _id DESC LIMIT 2"];
    let arguments = [vec![uuid(100u128 << 64), uuid(110u128 << 64)], vec![Value::I64(0), Value::I64(0)]];
    let context = fixture.session.query_context().await.unwrap();
    let prepared: Vec<PreparedSql> = statements.iter().zip(&arguments).map(|(sql, arguments)| {
        let options = rtdb_sql::PrepareOptions { parameter_types: arguments.iter().enumerate().map(|(index, value)| (index, value.logical_type())).collect(), ..Default::default() };
        PreparedSql::prepare(sql, &context, options, ResultSignaturePolicy::Dynamic).unwrap()
    }).collect();
    assert_eq!(first_column(prepared[0].open_query(Arc::clone(&context), arguments[0].clone()).await).await, Ok((100..110).map(Value::I64).collect()));
    assert_eq!(first_column(prepared[1].open_query(Arc::clone(&context), arguments[1].clone()).await).await, Ok(vec![Value::I64(1_999), Value::I64(1_998)]));
    drop(context);

    // A text ID arrives after the plans were made. Its IID is a hash, wherever its ID sorts, so the plans may no
    // longer read by IID: whatever a fresh plan answers over the mixed IDs, a comparison error included, they answer.
    fixture.commit("INSERT INTO docs (_id, v) VALUES (?, ?)", vec![vec![Value::Utf8("zzz".into()), Value::I64(-1)]]).await.unwrap();
    for ((sql, arguments), prepared) in statements.iter().zip(&arguments).zip(&prepared) {
        let fresh = first_column(fixture.session.query_sql(sql, arguments.clone()).await).await;
        let context = fixture.session.query_context().await.unwrap();
        assert_eq!(first_column(prepared.open_query(context, arguments.clone()).await).await, fresh, "{sql}");
    }
    fixture.close().await;
}

#[tokio::test]
async fn a_limit_over_hashed_ids_in_id_order_sorts_them() {
    let mut fixture = Fixture::open().await;
    fixture.commit("INSERT INTO docs (_id, v) VALUES (?, ?)", (0..3_000).map(|index| vec![text_id(index), Value::I64(index as i64)]).collect()).await.unwrap();
    fixture.flush().await;
    assert_eq!(fixture.rows("SELECT v FROM docs ORDER BY _id DESC LIMIT 3", vec![]).await, [vec![Value::I64(2_999)], vec![Value::I64(2_998)], vec![Value::I64(2_997)]]);
    assert_eq!(fixture.rows("SELECT v FROM docs WHERE _id >= ? ORDER BY _id LIMIT 2", vec![text_id(1_500)]).await, [vec![Value::I64(1_500)], vec![Value::I64(1_501)]]);
    fixture.close().await;
}

const WEEK: i64 = 7 * 86_400_000_000;
/// 2020-01-06T00:00:00Z, a Monday, so that week `n` after it is one recency partition.
const EPOCH: i64 = 1_578_268_800_000_000;
fn instant(micros: i64) -> Value { Value::Timestamp { value: micros, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
fn timestamp_literal(micros: i64) -> String {
    let (seconds, fraction) = (micros.div_euclid(1_000_000), micros.rem_euclid(1_000_000));
    let (days, second) = (seconds.div_euclid(86_400), seconds.rem_euclid(86_400));
    let date = rtdb_types::CivilDate::from_epoch_day(days).unwrap();
    format!("TIMESTAMP '{date}T{:02}:{:02}:{:02}.{fraction:06}Z'", second / 3600, second % 3600 / 60, second % 60)
}

#[tokio::test]
async fn a_valid_time_read_prunes_the_files_and_pages_whose_valid_extents_lie_outside_it() {
    let mut fixture = Fixture::open_at(EPOCH + 20 * WEEK).await;
    // Twelve weeks of instants: each row is valid for the one microsecond it was sampled at.
    const WEEKS: i64 = 12;
    const PER_WEEK: i64 = 400;
    let at = |week: i64, sample: i64| EPOCH + week * WEEK + sample * (WEEK / PER_WEEK) + 1;
    let rows: Vec<Vec<Value>> = (0..WEEKS).flat_map(|week| (0..PER_WEEK).map(move |sample| (week, sample)))
        .map(|(week, sample)| vec![uuid(point_id(1, (week * PER_WEEK + sample) as u64, 0)), Value::I64(week * PER_WEEK + sample), instant(at(week, sample)), instant(at(week, sample) + 1)]).collect();
    fixture.commit("INSERT INTO samples (_id, n, _valid_from, _valid_to) VALUES (?, ?, ?, ?)", rows).await.unwrap();
    fixture.flush().await;
    fixture.compact().await;
    let tries = fixture.node.catalog().database("xtdb").unwrap().trie_catalog().current_tries(&TableRef::new("public", "samples")).len() as i64;
    assert!(tries >= WEEKS, "compaction wrote a file per week of recency: {tries}");

    let numbers = |rows: Vec<Vec<Value>>| { let mut numbers: Vec<i64> = rows.into_iter().map(|row| match row[0] { Value::I64(n) => n, _ => panic!("n is an integer") }).collect(); numbers.sort_unstable(); numbers };
    // One week, the instants at its two ends included and excluded as a half-open period is.
    let week = format!("SELECT n FROM samples FOR VALID_TIME FROM {} TO {}", timestamp_literal(at(5, 0)), timestamp_literal(at(6, 0)));
    assert_eq!(numbers(fixture.rows(&week, vec![]).await), (5 * PER_WEEK..6 * PER_WEEK).collect::<Vec<_>>());
    let counters = fixture.counters(&week, vec![]).await;
    assert!(counters["scan_files_used"] <= 2 && counters["scan_files_pruned"] >= tries - 2, "one week of {tries} files: {counters:?}");

    let point = format!("SELECT n FROM samples FOR VALID_TIME AS OF {}", timestamp_literal(at(7, 123)));
    assert_eq!(numbers(fixture.rows(&point, vec![]).await), [7 * PER_WEEK + 123]);
    let counters = fixture.counters(&point, vec![]).await;
    assert!(counters["scan_files_used"] <= 1 && counters["scan_pages_used"] <= 1, "one instant: {counters:?}");
    assert_eq!(numbers(fixture.rows(&format!("SELECT n FROM samples FOR VALID_TIME AS OF {}", timestamp_literal(at(7, 123) + 1)), vec![]).await), Vec::<i64>::new());

    // Three weeks, and every instant: nothing in range is dropped.
    let span = format!("SELECT n FROM samples FOR VALID_TIME FROM {} TO {}", timestamp_literal(at(2, 100)), timestamp_literal(at(5, 100)));
    assert_eq!(numbers(fixture.rows(&span, vec![]).await), (2 * PER_WEEK + 100..5 * PER_WEEK + 100).collect::<Vec<_>>());
    let counters = fixture.counters(&span, vec![]).await;
    assert!(counters["scan_files_used"] <= 5 && counters["scan_files_pruned"] >= tries - 5, "{counters:?}");
    assert_eq!(numbers(fixture.rows("SELECT n FROM samples FOR ALL VALID_TIME", vec![]).await), (0..WEEKS * PER_WEEK).collect::<Vec<_>>());

    // Rows written after the compaction are read from the live index beside the pruned files.
    fixture.commit("INSERT INTO samples (_id, n, _valid_from, _valid_to) VALUES (?, ?, ?, ?)", vec![vec![uuid(point_id(1, 1 << 30, 0)), Value::I64(-1), instant(at(5, 7) + 3), instant(at(5, 7) + 4)]]).await.unwrap();
    let mut expected: Vec<i64> = (5 * PER_WEEK..6 * PER_WEEK).collect();
    expected.insert(0, -1);
    assert_eq!(numbers(fixture.rows(&week, vec![]).await), expected);
    let counters = fixture.counters(&week, vec![]).await;
    assert!(counters["scan_files_pruned"] >= tries - 2, "{counters:?}");
    fixture.close().await;
}

#[tokio::test]
async fn a_read_of_a_long_history_prunes_the_files_of_versions_superseded_before_it() {
    let mut fixture = Fixture::open_at(EPOCH).await;
    // One entity rewritten every week, each version holding from the moment it was accepted.
    const WEEKS: i64 = 12;
    for week in 0..WEEKS {
        fixture.clock.0.store(EPOCH + week * WEEK + 3_600_000_000, Ordering::Relaxed);
        fixture.commit("INSERT INTO prices (_id, n) VALUES (?, ?)", vec![vec![Value::Utf8("widget".into()), Value::I64(week)]]).await.unwrap();
    }
    fixture.clock.0.store(EPOCH + WEEKS * WEEK, Ordering::Relaxed);
    fixture.flush().await;
    fixture.compact().await;
    let tries = fixture.node.catalog().database("xtdb").unwrap().trie_catalog().current_tries(&TableRef::new("public", "prices")).len() as i64;
    assert!(tries >= WEEKS - 1, "compaction wrote a file per week in which a version was superseded: {tries}");

    // The value now: the files of the versions superseded before this week are behind the read.
    assert_eq!(fixture.rows("SELECT n FROM prices", vec![]).await, [vec![Value::I64(WEEKS - 1)]]);
    let counters = fixture.counters("SELECT n FROM prices", vec![]).await;
    assert!(counters["scan_files_used"] <= 2 && counters["scan_files_pruned"] >= tries - 2, "{tries} files: {counters:?}");
    // The value at an earlier instant, and as it was recorded at one.
    for week in [0, 4, WEEKS - 2] {
        let instant = timestamp_literal(EPOCH + week * WEEK + WEEK / 2);
        for period in [format!("FOR VALID_TIME AS OF {instant}"), format!("FOR SYSTEM_TIME AS OF {instant}")] {
            let sql = format!("SELECT n FROM prices {period}");
            assert_eq!(fixture.rows(&sql, vec![]).await, [vec![Value::I64(week)]], "{sql}");
        }
        let counters = fixture.counters(&format!("SELECT n FROM prices FOR VALID_TIME AS OF {instant}"), vec![]).await;
        assert!(counters["scan_files_used"] <= 3 && counters["scan_files_pruned"] >= tries - 3, "week {week} of {tries} files: {counters:?}");
    }
    assert_eq!(fixture.rows("SELECT COUNT(*) FROM prices FOR ALL VALID_TIME", vec![]).await, [vec![Value::I64(WEEKS)]]);
    fixture.close().await;
}

/// A write row as the store adapter shapes one: its typed columns, a 48-character key and a 40-byte image.
fn write_row(index: usize) -> Vec<Value> {
    vec![uuid(point_id(index as u32 % 100, index as u64, 0)), Value::I64(1), Value::Utf8(format!("{:048x}", index)), Value::I64(index as i64), Value::Utf8("put".into()),
        Value::Binary(vec![index as u8; 40]), Value::I64(0), Value::Utf8("label/telemetry".into()), Value::list(vec![Value::Utf8(format!("unit/{}", index % 16))]),
        Value::Utf8(format!("machine-{:05}", index % 100)), instant(EPOCH + index as i64), Value::F64(index as f64 / 8.0)]
}
const WRITE_COLUMNS: &str = "_id, world, key, idx, op, value, number, label, units, series, at, v";

#[tokio::test]
async fn a_ten_thousand_row_insert_commits_within_the_default_execution_budget() {
    let mut fixture = Fixture::open_at(EPOCH + 20 * WEEK).await;
    assert_eq!(fixture.node.memory().limit_bytes(), 512 * 1024 * 1024);
    let sql = format!("INSERT INTO writes ({WRITE_COLUMNS}) VALUES ({})", placeholders(12));
    fixture.commit(&sql, (0..10_000).map(write_row).collect()).await.unwrap();
    let peak = fixture.node.memory().peak_bytes();
    assert!(peak < 256 * 1024 * 1024, "10,000 rows of some 300 bytes reserved {peak} bytes");
    // Each row with its own valid time, as an instant sample carries: one put operation per row.
    let stamped = format!("INSERT INTO samples ({WRITE_COLUMNS}, _valid_from, _valid_to) VALUES ({})", placeholders(14));
    fixture.commit(&stamped, (0..10_000).map(|index| { let mut row = write_row(index); row.push(instant(EPOCH + index as i64)); row.push(instant(EPOCH + index as i64 + 1)); row }).collect()).await.unwrap();
    let stamped_peak = fixture.node.memory().peak_bytes();
    eprintln!("peak execution memory for 10,000 rows: {peak} bytes with one valid time, {stamped_peak} bytes with a valid time each");
    assert!(stamped_peak < 384 * 1024 * 1024, "10,000 single-row operations reserved {stamped_peak} bytes");
    assert_eq!(fixture.rows("SELECT COUNT(*) FROM writes", vec![]).await, [vec![Value::I64(10_000)]]);
    assert_eq!(fixture.rows("SELECT COUNT(*) FROM samples FOR ALL VALID_TIME", vec![]).await, [vec![Value::I64(10_000)]]);
    fixture.close().await;
}
