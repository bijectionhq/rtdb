//! A scan split into trie-prefix fragments, each read by `Node::scan_fragment` as another node would, returns the rows a
//! whole scan returns.

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock, atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::read_value;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::*;
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_query::{FragmentDepth, QueryCursor, ScanFragment, ScanFragmenter};
use rtdb_tx::{Documents, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, Result, StructFields, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;

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

/// Reads every fragment through the node's own `scan_fragment`, as the node a fragment belongs to would, except those under `failing`.
#[derive(Default)]
struct Loopback {
    node: OnceLock<Node>,
    enabled: AtomicBool,
    failing: Mutex<BTreeSet<String>>,
    read: AtomicU64,
    failed: AtomicU64,
    last: Mutex<Option<ScanFragment>>,
}

#[async_trait]
impl ScanFragmenter for Loopback {
    fn depth(&self, _: &str, _: &TableRef, persisted_bytes: u64) -> Option<FragmentDepth> {
        (self.enabled.load(Ordering::Relaxed) && persisted_bytes > 0).then(|| FragmentDepth::new(2).unwrap())
    }
    async fn dispatch(&self, fragment: &ScanFragment) -> Result<Option<Vec<u8>>> {
        *self.last.lock() = Some(fragment.clone());
        if self.failing.lock().contains(&fragment.prefix.to_string()) { self.failed.fetch_add(1, Ordering::Relaxed); return Err(Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "test/fragment-node-down", "The fragment's node is down")); }
        let bytes = self.node.get().unwrap().scan_fragment(fragment, Duration::from_secs(1), &CancellationToken::new()).await?;
        self.read.fetch_add(1, Ordering::Relaxed);
        Ok(Some(bytes))
    }
}

async fn start() -> (Node, tokio::task::JoinHandle<Result<()>>, Arc<Loopback>) {
    let mut options = NodeOptions::default();
    options.replication.assertion_interval = Duration::from_millis(1);
    options.replication.election_seed = 1;
    options.replication.flush_timeout = Duration::from_secs(3600);
    options.replication.rows_per_block = 100_000;
    options.automatic_compaction = false;
    options.automatic_gc = false;
    options.trie = rtdb_trie::TrieConfig::new(16, 32).unwrap();
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let fragmenter = Arc::new(Loopback::default());
    let services = NodeServices { resources: Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false)), clock, scan_fragmenter: Some(fragmenter.clone()),
        ..NodeServices::local(Arc::new(Authority), Arc::new(Observer), Arc::new(CatalogFixtures), Arc::new(CatalogFixtures)) };
    let (node, driver) = Node::open(options, services).await.unwrap();
    let _ = fragmenter.node.set(node.clone());
    (node, tokio::spawn(driver.run()), fragmenter)
}

fn order(id: i64) -> StructFields<Value> {
    let mut document = StructFields::from([("_id".to_owned(), Value::I64(id)), ("note".to_owned(), Value::Utf8(format!("order {id}")))]);
    if id % 11 != 0 { document.insert("total".to_owned(), Value::I64(id)); }
    if id % 5 == 0 {
        document.insert("meta".to_owned(), Value::Struct(StructFields::from([("rank".to_owned(), Value::I64(id % 3)), ("tags".to_owned(), Value::list(vec![Value::Utf8("a".into()), Value::Utf8(format!("t{id}"))]))])));
    }
    document
}

fn put(node: &Node, table: &str, documents: Vec<StructFields<Value>>) -> TxOp {
    TxOp::PutDocs { table: TableRef::new("public", table), documents: Documents::new(documents, node.memory()).unwrap(), valid_from: None, valid_to: None }
}

async fn execute(session: &mut NativeSession, operation: TxOp) {
    let result = session.state_mut().execute_operation(operation).await.unwrap();
    if let Some(result) = result { assert!(result.committed(), "{:?}", result.error()); }
}

/// Three tables whose rows span many trie leaves, part flushed to a block and part in the live index, with history across both.
async fn populate(node: &Node, session: &mut NativeSession) {
    for chunk in 0..6 { execute(session, put(node, "orders", (chunk * 100..(chunk + 1) * 100).map(order).collect())).await; }
    let items = (0..400).map(|id| StructFields::from([("_id".to_owned(), Value::Utf8(format!("item-{id}"))), ("order_id".to_owned(), Value::I64(id % 600)), ("qty".to_owned(), Value::I64(id % 9))])).collect();
    execute(session, put(node, "items", items)).await;
    execute(session, put(node, "users", (0..40).map(|id| StructFields::from([("_id".to_owned(), Value::I64(id)), ("name".to_owned(), Value::Utf8(format!("user {id}")))])).collect())).await;
    execute(session, TxOp::Sql { sql: "DELETE FROM orders WHERE _id < 20".into(), args: None }).await;
    execute(session, TxOp::Sql { sql: "UPDATE orders SET total = 0 WHERE _id BETWEEN 100 AND 130".into(), args: None }).await;
    execute(session, TxOp::Sql { sql: "UPDATE orders SET total = total + 0.5 WHERE _id % 7 = 0".into(), args: None }).await;
    let database = node.catalog().database("xtdb").unwrap();
    database.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async { while database.status().adopted_block.is_none() { tokio::time::sleep(Duration::from_millis(5)).await; } }).await.unwrap();
    execute(session, put(node, "orders", (600..700).map(order).collect())).await;
    execute(session, TxOp::Sql { sql: "UPDATE orders SET note = 'tail' WHERE _id BETWEEN 200 AND 260".into(), args: None }).await;
    execute(session, TxOp::Sql { sql: "DELETE FROM items WHERE qty = 0".into(), args: None }).await;
}

const QUERIES: [&str; 7] = [
    "SELECT * FROM orders",
    "SELECT _id, total FROM orders WHERE total > 300",
    "SELECT note FROM orders WHERE _id = 250",
    "SELECT count(*) AS n, sum(total) AS total, max(note) AS note FROM orders",
    "SELECT _id, _valid_from, _valid_to, _system_from, _system_to, total, note FROM orders FOR ALL SYSTEM_TIME FOR ALL VALID_TIME",
    "SELECT i._id, i.qty, o.total FROM items AS i JOIN orders AS o ON i.order_id = o._id",
    "SELECT name FROM users WHERE _id > 10",
];

async fn rows(mut cursor: QueryCursor) -> Vec<String> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let schema = batch.batch().schema();
        for row in 0..batch.batch().num_rows() {
            let values: Vec<Value> = schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.batch().column(column).as_ref(), row).unwrap()).collect();
            rows.push(format!("{values:?}"));
        }
    }
    rows.sort();
    rows
}

async fn answers(session: &mut NativeSession) -> Vec<Vec<String>> {
    let mut answers = Vec::new();
    for sql in QUERIES { answers.push(rows(session.query_sql(sql, vec![]).await.unwrap()).await); }
    answers
}

async fn stop(node: &Node, driver: tokio::task::JoinHandle<Result<()>>) {
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(10), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scan_read_as_fragments_returns_the_rows_of_a_whole_scan() {
    let (node, driver, fragmenter) = start().await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    populate(&node, &mut session).await;
    let whole = answers(&mut session).await;
    assert_eq!(fragmenter.read.load(Ordering::Relaxed), 0, "a disabled fragmenter splits nothing");
    assert!(whole[0].len() > 600 && whole[4].len() > whole[0].len() && !whole[5].is_empty(), "the queries read history across the block and the live index");

    fragmenter.enabled.store(true, Ordering::Relaxed);
    let fragmented = answers(&mut session).await;
    for ((sql, whole), fragmented) in QUERIES.iter().zip(&whole).zip(&fragmented) { assert_eq!(fragmented, whole, "{sql}"); }
    assert!(fragmenter.read.load(Ordering::Relaxed) >= 16 * 5, "every whole-table scan was read as its sixteen fragments");
    stop(&node, driver).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fragment_whose_node_fails_is_read_by_the_coordinator() {
    let (node, driver, fragmenter) = start().await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    populate(&node, &mut session).await;
    let whole = answers(&mut session).await;
    fragmenter.enabled.store(true, Ordering::Relaxed);
    *fragmenter.failing.lock() = BTreeSet::from(["00".to_owned(), "13".to_owned(), "32".to_owned()]);
    let fragmented = answers(&mut session).await;
    for ((sql, whole), fragmented) in QUERIES.iter().zip(&whole).zip(&fragmented) { assert_eq!(fragmented, whole, "{sql}"); }
    assert!(fragmenter.failed.load(Ordering::Relaxed) >= 3 * 5, "each whole-table scan lost three fragments to the failing node");
    stop(&node, driver).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fragment_reads_as_of_its_basis_on_a_node_at_or_past_it() {
    let (node, driver, fragmenter) = start().await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    populate(&node, &mut session).await;
    fragmenter.enabled.store(true, Ordering::Relaxed);
    let _ = answers(&mut session).await;
    let fragment = fragmenter.last.lock().clone().unwrap();
    let at_basis = node.scan_fragment(&fragment, Duration::ZERO, &CancellationToken::new()).await.unwrap();
    let slices: Vec<_> = (0..4u8).flat_map(|first| (0..4u8).map(move |second| rtdb_query::TriePrefix::new(vec![first, second]).unwrap())).map(|prefix| {
        let mut slice = fragment.clone();
        slice.prefix = prefix;
        slice
    }).collect();
    let mut before = Vec::new();
    for slice in &slices { before.push(node.scan_fragment(slice, Duration::ZERO, &CancellationToken::new()).await.unwrap()); }

    let mut ahead = fragment.clone();
    ahead.basis.tx_id = rtdb_types::TxId(fragment.basis.tx_id.0 + 1);
    let error = node.scan_fragment(&ahead, Duration::from_millis(50), &CancellationToken::new()).await.unwrap_err();
    assert_eq!(error.code, "rtdb.node/fragment-basis", "a basis this node has not reached is waited for, then refused");

    // Every row changes, some are deleted and new ones arrive, after the basis.
    for table in ["orders", "items", "users"] {
        execute(&mut session, TxOp::Sql { sql: format!("UPDATE {table} SET later = 1"), args: None }).await;
    }
    execute(&mut session, TxOp::Sql { sql: "DELETE FROM orders WHERE _id % 3 = 0".into(), args: None }).await;
    execute(&mut session, TxOp::Sql { sql: "DELETE FROM users".into(), args: None }).await;
    execute(&mut session, put(&node, "users", vec![StructFields::from([("_id".to_owned(), Value::I64(1000)), ("name".to_owned(), Value::Utf8("later".into()))])])).await;
    let _ = rows(session.query_sql("SELECT name FROM users WHERE _id = 1000", vec![]).await.unwrap()).await;
    assert_eq!(node.scan_fragment(&fragment, Duration::ZERO, &CancellationToken::new()).await.unwrap(), at_basis, "a node past the basis reads the fragment as of the basis");
    for (slice, before) in slices.iter().zip(&before) {
        assert_eq!(&node.scan_fragment(slice, Duration::ZERO, &CancellationToken::new()).await.unwrap(), before, "slice {} reads as of the basis", slice.prefix);
    }
    stop(&node, driver).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fragment_carries_the_predicates_its_reader_evaluates_alike_and_no_others() {
    let (node, driver, fragmenter) = start().await;
    let mut session = node.session("xtdb", Arc::new(Authority)).unwrap();
    populate(&node, &mut session).await;
    fragmenter.enabled.store(true, Ordering::Relaxed);
    let _ = rows(session.query_sql("SELECT _id FROM orders WHERE _id > 500", vec![]).await.unwrap()).await;
    let filtered = fragmenter.last.lock().clone().unwrap();
    assert!(!filtered.scan.predicates.is_empty(), "a literal filter travels with the fragment");
    let mut unfiltered = filtered.clone();
    unfiltered.scan.predicates.clear();
    let read = |fragment: rtdb_query::ScanFragment| { let node = node.clone(); async move { node.scan_fragment(&fragment, Duration::ZERO, &CancellationToken::new()).await.unwrap().len() } };
    assert!(read(filtered).await < read(unfiltered).await, "and the reader returns only the rows that pass it");
    let _ = rows(session.query_sql("SELECT _id FROM orders WHERE _id > ?", vec![Value::I64(500)]).await.unwrap()).await;
    assert!(fragmenter.last.lock().as_ref().unwrap().scan.predicates.is_empty(), "a filter on a parameter stays with the coordinator");
    stop(&node, driver).await;
}
