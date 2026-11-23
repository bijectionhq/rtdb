use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{
    AutonomousTxOptions, Clock, DatabaseAccess, NativeResources, Node, NodeOptions, NodeServices,
    NativeSession, SessionIdentity,
};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_tx::{Documents, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, Result, StructFields, TableRef, Timestamp, Value};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

#[path = "support/fts_corpus_checkpoint.rs"]
mod corpus_checkpoint;
#[path = "support/fts_generations.rs"]
mod generations;
#[path = "support/vector_workload.rs"]
mod vector_workload;
#[cfg(feature = "fts-experiments")]
#[path = "support/fts_same_files.rs"]
mod same_files;
#[cfg(feature = "fts-experiments")]
#[path = "support/fts_workload.rs"]
mod workload;

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
    fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly) {
        panic!("collection failed for {table:?}: {error}");
    }
}

struct EmptyCatalog;

impl CatalogUsers for EmptyCatalog {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> {
        Ok(CatalogRows::new(Vec::new(), Arc::new(())))
    }

    fn is_superuser(&self, _: &str) -> bool { false }
}

impl CatalogMetrics for EmptyCatalog {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        Ok(CatalogRows::new(Vec::new(), Arc::new(())))
    }
}

struct TickClock(AtomicI64);

impl Clock for TickClock {
    fn now(&self) -> Timestamp { Timestamp(self.0.fetch_add(1000, Ordering::Relaxed)) }
}

async fn commit(session: &mut NativeSession, operations: Vec<TxOp>) -> Timestamp {
    let result = session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await.unwrap();
    assert!(result.committed(), "{result:?}");
    result.tx_key().system_time
}

fn sql(query: &str) -> TxOp { TxOp::Sql { sql: query.into(), args: None } }

fn where_ids(ids: impl IntoIterator<Item = i64>) -> String {
    let predicates = ids.into_iter().map(|id| format!("_id = {id}")).collect::<Vec<_>>();
    assert!(!predicates.is_empty());
    format!(" WHERE {}", predicates.join(" OR "))
}

async fn query(session: &mut NativeSession, statement: &str) -> Vec<Vec<Value>> {
    let mut cursor = session.query_sql(statement, vec![]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().enumerate()
                .map(|(column, field)| read_value(field, batch.column(column).as_ref(), row).unwrap())
                .collect());
        }
    }
    rows
}

fn body(id: i64) -> String {
    let approved = id % 5 == 0;
    let review = id % 3 == 0;
    let rare = id % 101 == 0;
    let audit_repetitions = if approved { 1 + id % 3 } else { 10 + id % 9 };
    format!("report {} {} {} department section {}{}",
        if approved { "approved" } else { "pending" },
        if review { "review" } else { "published" },
        if rare { "needle" } else { "routine" },
        id % 1000, " audit".repeat(audit_repetitions as usize))
}

async fn flush(node: &Node) {
    node.catalog().database("xtdb").unwrap().flush().await.unwrap();
    node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
}

struct Case {
    name: &'static str,
    statement: String,
    expected: Vec<Vec<Value>>,
    samples_us: Vec<f64>,
    wrong_trials: usize,
    example_rows: Option<usize>,
}

impl Case {
    fn new(name: &'static str, statement: impl Into<String>, expected: Vec<Vec<Value>>) -> Self {
        Self { name, statement: statement.into(), expected, samples_us: Vec::new(), wrong_trials: 0, example_rows: None }
    }

    fn observe(&mut self, elapsed: Duration, rows: Vec<Vec<Value>>) {
        if rows == self.expected {
            self.samples_us.push(elapsed.as_secs_f64() * 1_000_000.0);
        } else {
            self.wrong_trials += 1;
            self.example_rows.get_or_insert(rows.len());
        }
    }

    fn report(self) -> Json {
        let fingerprint = Sha256::digest(format!("{:?}", self.expected));
        json!({
            "name": self.name,
            "sql": self.statement,
            "expected_rows": self.expected.len(),
            "expected_sha256": format!("{fingerprint:x}"),
            "example_wrong_rows": self.example_rows,
            "wrong_trials": self.wrong_trials,
            "samples_us": self.samples_us,
        })
    }
}

fn bench_options() -> NodeOptions {
    let mut options = NodeOptions::default();
    options.replication.assertion_interval = Duration::from_millis(1);
    options.replication.election_seed = 1;
    options.replication.rows_per_block = 1_000_000;
    options.replication.flush_timeout = Duration::from_secs(3600);
    options.file_size_target = 512 * 1024;
    options.automatic_compaction = false;
    options.automatic_gc = false;
    options.automatic_artefacts = false;
    options
}

fn bench_services(clock: &Arc<TickClock>) -> NodeServices {
    let mut services = NodeServices::local(Arc::new(Identity), Arc::new(Observer), Arc::new(EmptyCatalog), Arc::new(EmptyCatalog));
    let log_clock = Arc::clone(clock);
    services.resources = Arc::new(NativeResources::default().with_log_clock(Arc::new(move || Ok(log_clock.now()))));
    services.clock = Arc::clone(clock) as Arc<dyn Clock>;
    services
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let mut args = std::env::args().skip(1);
    let workload = args.next().unwrap();
    if matches!(workload.as_str(), "vectors" | "vectors-churn" | "vectors-full") {
        let data = std::path::PathBuf::from(args.next().expect("embedding data directory"));
        let queries = args.next().expect("query count").parse().unwrap();
        let file_backed = match args.next().as_deref() {
            None => false,
            Some("--file-backed") => true,
            Some(flag) => panic!("Unknown vector benchmark option: {flag}"),
        };
        vector_workload::run(&data, queries, &workload, file_backed).await;
        return;
    }
    if workload == "checkpoint" {
        corpus_checkpoint::run(args.next().unwrap().parse().unwrap()).await;
        return;
    }
    if workload == "generations" {
        generations::run(args.next().unwrap().parse().unwrap(), args.next().unwrap().parse().unwrap()).await;
        return;
    }
    #[cfg(feature = "fts-experiments")]
    if workload == "same-files" {
        same_files::run(args.next().unwrap().parse().unwrap(), args.next().unwrap().parse().unwrap()).await;
        return;
    }
    #[cfg(feature = "fts-experiments")]
    if matches!(workload.as_str(), "ranked-sql" | "ranked-pack" | "ranked-logical") {
        let format = match workload.as_str() { "ranked-logical" => 16, "ranked-pack" => 15, _ => 14 };
        same_files::run_ranked_sql(args.next().unwrap().parse().unwrap(), args.next().unwrap().parse().unwrap(), format).await;
        return;
    }
    #[cfg(feature = "fts-experiments")]
    if workload == "workload" || workload == "ranked-filter" {
        workload::run(args.next().unwrap().parse().unwrap(), args.next().unwrap().parse().unwrap(),
            workload == "ranked-filter").await;
        return;
    }
    let document_count: i64 = workload.parse().unwrap();
    let iterations: usize = args.next().unwrap().parse().unwrap();
    let trial: usize = args.next().unwrap().parse().unwrap();
    assert!(document_count >= 20_000 && document_count % 20_000 == 0);

    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let (node, driver) = Node::open(bench_options(), bench_services(&clock)).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let database = node.catalog().database("xtdb").unwrap();
    let table = TableRef::new("public", "docs");
    commit(&mut session, vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")]).await;
    for start in (0..document_count).step_by(4_000) {
        let documents = (start..start + 4_000).map(|id| StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("body".into(), Value::Utf8(body(id).into())),
            ("department".into(), Value::I64(id % 137)),
        ])).collect();
        let put = TxOp::PutDocs { table: table.clone(), documents: Documents::new(documents, node.memory()).unwrap(),
            valid_from: Some(Timestamp(0)), valid_to: None };
        commit(&mut session, vec![put]).await;
        if (start + 4_000) % 20_000 == 0 { flush(&node).await; }
    }
    let compaction_start = Instant::now();
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
    let homogeneous_compaction_ms = compaction_start.elapsed().as_secs_f64() * 1000.0;
    database.build_artefacts().await.unwrap();
    let catalog = database.trie_catalog();
    let tries = catalog.current_tries(&table);
    let indexed_tries = tries.iter().filter(|trie| trie.artefacts.iter().any(|record| record.kind() == "search")).count();
    assert!(!tries.is_empty() && indexed_tries == tries.len(), "{} total tries, {indexed_tries} indexed", tries.len());
    assert!(tries.iter().all(|trie| trie.key.level() > 0));

    let mut cases = vec![
        Case::new("count_all", "SELECT COUNT(*) FROM docs", vec![vec![Value::I64(document_count)]]),
        Case::new("count_rare_match", "SELECT COUNT(*) FROM docs WHERE MATCH(body, 'needle')",
            vec![vec![Value::I64((document_count + 100) / 101)]]),
        Case::new("count_common_match", "SELECT COUNT(*) FROM docs WHERE MATCH(body, 'review')",
            vec![vec![Value::I64((document_count + 2) / 3)]]),
    ];
    for (name, filter, expected_count) in [
        ("top20_single_match", "MATCH(body, 'report')", document_count),
        ("top20_selective_match", "MATCH(body, 'report needle')", (document_count + 100) / 101),
        ("top20_two_matches", "MATCH(body, 'report') AND MATCH(body, 'approved')", document_count / 5),
    ] {
        let base = format!("SELECT _id, SEARCH_SCORE(body, 'audit') AS score FROM docs WHERE {filter} ORDER BY score DESC, _id");
        let exhaustive = query(&mut session, &base).await;
        assert_eq!(exhaustive.len(), expected_count as usize, "{name} oracle row count");
        cases.push(Case::new(name, format!("{base} LIMIT 20"), exhaustive.into_iter().take(20).collect()));
    }

    for case in &cases {
        for _ in 0..4 { std::hint::black_box(query(&mut session, &case.statement).await); }
    }
    for sample in 0..iterations {
        for index in 0..cases.len() {
            let position = (index + sample + trial) % cases.len();
            let case = &mut cases[position];
            let start = Instant::now();
            let rows = query(&mut session, &case.statement).await;
            case.observe(start.elapsed(), rows);
        }
    }

    let mixed_expected = 4;
    for (id, record) in [(0, "body: 'text'"), (1, "body: 42"), (2, "title: 'untitled'"), (3, "body: 'more text'")] {
        commit(&mut session, vec![sql(&format!("INSERT INTO mixed RECORDS {{_id: {id}, {record}}}"))]).await;
    }
    flush(&node).await;
    let mixed_start = Instant::now();
    let mixed_compaction = match database.compaction().unwrap().compact_all().await {
        Ok(()) => {
            node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
            let elapsed_ms = mixed_start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(query(&mut session, "SELECT _id FROM mixed ORDER BY _id").await.len(), mixed_expected);
            json!({"success": true, "elapsed_ms": elapsed_ms, "rows_verified": mixed_expected})
        }
        Err(error) => json!({"success": false, "error": error.to_string(), "elapsed_ms": mixed_start.elapsed().as_secs_f64() * 1000.0}),
    };
    println!("{}", json!({
        "documents": document_count,
        "iterations": iterations,
        "trial": trial,
        "indexed_tries": indexed_tries,
        "homogeneous_compaction_ms": homogeneous_compaction_ms,
        "mixed_compaction": mixed_compaction,
        "queries": cases.into_iter().map(Case::report).collect::<Vec<_>>(),
    }));
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
}
