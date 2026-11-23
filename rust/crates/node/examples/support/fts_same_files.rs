use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicI64, Ordering}};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_index::Snapshot;
use rtdb_node::{Database, NativeSession, Node};
use rtdb_plan::{Scan, TableBinding, QualifiedTable, Expr, TemporalFilter};
use rtdb_query::{CandidateVerifier, ConstantEvaluator, MatchAnswers, ScanAccelerator, ScanRequest, ScanSelection};
use rtdb_search::{SearchAccelerator, SearchRuntime, accelerate::experiment::{Fallback, PointRanking, Ranking, Work}};
use rtdb_text::Query;
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{Iid, Interval, Result, StructFields, TableRef, TemporalBounds, Timestamp, Value};
use serde_json::{Value as Json, json};
use tokio_util::sync::CancellationToken;

use super::{Identity, TickClock, bench_options, bench_services, commit, flush, query, sql, where_ids};
use super::corpus_checkpoint::{as_of, id};

struct Inspection {
    search: Arc<SearchAccelerator>,
    narrowed: Mutex<Option<Option<usize>>>,
    score_only: AtomicBool,
}

#[async_trait]
impl ScanAccelerator for Inspection {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        self.search.candidate_iids(request).await
    }

    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        let mut score_only = request.scan.clone();
        let request = if self.score_only.load(Ordering::Relaxed) {
            if let Some(top_k) = &mut score_only.top_k { top_k.ascending_id = None; }
            ScanRequest { scan: &score_only, ..*request }
        } else {
            ScanRequest { ..*request }
        };
        let selection = self.search.select_candidates(&request).await?;
        if request.scan.top_k.is_some() {
            *self.narrowed.lock().unwrap() = Some(selection.as_ref().map(|selection| selection.iids.len()));
        }
        Ok(selection)
    }

    async fn match_answers(&self, request: &ScanRequest<'_>, column: &str, query: &str) -> Result<Option<MatchAnswers>> {
        self.search.match_answers(request, column, query).await
    }
}

struct LiteralOnly;

#[async_trait]
impl ConstantEvaluator for LiteralOnly {
    async fn constant(&self, expression: &Expr) -> Result<Option<Value>> {
        Ok(match expression { Expr::Literal(value) => Some(value.clone()), _ => None })
    }
}

#[async_trait]
impl CandidateVerifier for LiteralOnly {
    async fn score_visible(&self, _: BTreeSet<Iid>, _: rtdb_plan::ColumnId, _: Option<rtdb_plan::ColumnId>, _: &rtdb_text::Scorer) -> Result<rtdb_query::CandidateScores> {
        panic!("Point ranker must fall back before needing ordinary scan verification")
    }

}

#[derive(Clone, Copy)]
struct Case { name: &'static str, matching: &'static str, scoring: &'static str, limit: usize }

impl Case {
    fn statement(self, valid: Timestamp, system: Timestamp) -> String {
        format!(
            "SELECT _id, SEARCH_SCORE(body, '{}') AS score, department, _system_from FROM docs {} \
             WHERE MATCH(body, '{}') ORDER BY score DESC, _id",
            self.scoring, as_of(valid, system), self.matching,
        )
    }
}

const CASES: [Case; 5] = [
    Case { name: "uniform", matching: "report", scoring: "report", limit: 20 },
    Case { name: "uniform_100", matching: "report", scoring: "report", limit: 100 },
    Case { name: "dense", matching: "report", scoring: "audit", limit: 20 },
    Case { name: "selective", matching: "needle report", scoring: "audit", limit: 20 },
    Case { name: "phrase", matching: "\"report approved\" needle", scoring: "audit", limit: 20 },
];

fn text(id: i64) -> String {
    let frequency = 1 + id % 12;
    format!("report {} {} {} section{}{}",
        if id % 5 == 0 { "approved" } else { "pending" },
        if id % 101 == 0 { "needle" } else { "routine" },
        id % 1000, " audit".repeat(frequency as usize), " note".repeat((12 - frequency) as usize),
    )
}

async fn oracle(session: &mut NativeSession, statement: &str, limit: usize) -> Vec<Vec<Value>> {
    let mut cursor = session.query_sql(statement, vec![]).await.unwrap();
    let mut out = Vec::new();
    while out.len() < limit {
        let Some(batch) = cursor.next_batch().await.unwrap() else { break };
        let batch = batch.batch();
        for row in 0..batch.num_rows().min(limit - out.len()) {
            out.push(batch.schema().fields().iter().enumerate().map(|(column, field)| {
                read_value(field, batch.column(column).as_ref(), row).unwrap()
            }).collect());
        }
    }
    out
}

async fn captured_ids(session: &mut NativeSession, basis: Timestamp) -> Vec<(Iid, i64)> {
    let statement = format!("SELECT _id, _iid FROM docs {}", as_of(basis, basis));
    let mut cursor = session.query_sql(&statement, vec![]).await.unwrap();
    let mut ids = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let batch = batch.batch();
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let Value::I64(id) = read_value(schema.field(0), batch.column(0).as_ref(), row).unwrap()
                else { panic!("Fixture ID must be an integer") };
            let Value::FixedSizeBinary(iid) = read_value(schema.field(1), batch.column(1).as_ref(), row).unwrap()
                else { panic!("Fixture IID must be binary") };
            ids.push((Iid::from_slice(&iid).unwrap(), id));
        }
    }
    ids
}

struct Fixture<'a> {
    node: &'a Node,
    database: &'a Database,
    inspection: &'a Inspection,
    table: TableRef,
}

impl Fixture<'_> {
    fn scan(&self, snapshot: &Snapshot) -> Scan {
        Scan {
            table: TableBinding { name: QualifiedTable { database: "xtdb".into(), table: self.table.clone() },
                columns: snapshot.column_types(&self.table), oid: None },
            columns: Vec::new(), predicates: Vec::new(), matches: Vec::new(), iids: None, top_k: None, ordered_top_k: None,
            valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default, clamp_valid_time: false,
        }
    }

    async fn ranked(
        &self, session: &mut NativeSession, ranker: &mut PointRanking, ranking: Ranking,
        snapshot: &Snapshot, points: (Timestamp, Timestamp), case: Case,
    ) -> (Vec<Vec<Value>>, Work, Option<Fallback>) {
        let (valid, system) = points;
        let bounds = TemporalBounds { valid_time: Interval::at(valid).unwrap(), system_time: Interval::at(system).unwrap() };
        let scan = self.scan(snapshot);
        let memory = self.node.query_memory();
        let request = ScanRequest { database: "xtdb", scan: &scan, snapshot, storage: self.database.storage(), bounds,
            memory: &memory, current_time: self.node.query_instant(), cancellation: self.node.cancellation(),
            constants: &LiteralOnly, partitions: std::slice::from_ref(snapshot), verifier: &LiteralOnly };
        let (hits, work) = ranker.rank(&self.inspection.search, &request, "body", &Query::parse(case.matching),
            case.scoring, case.limit, ranking).await.unwrap();
        let hits = match hits {
            Ok(hits) => hits,
            Err(reason) => {
                let rows = query(session, &format!("{} LIMIT {}", case.statement(valid, system), case.limit)).await;
                return (rows, work, Some(reason));
            }
        };
        if hits.is_empty() { return (Vec::new(), work, None); }
        let selected = where_ids(hits.iter().map(|hit| hit.id));
        let raw = query(session, &format!(
            "SELECT _id, _iid, department, _system_from FROM docs {}{selected}", as_of(valid, system),
        )).await;
        assert_eq!(raw.len(), hits.len());
        let mut by_id: BTreeMap<_, _> = raw.into_iter().map(|row| (id(&row), row)).collect();
        let rows = hits.into_iter().map(|hit| {
            let row = by_id.remove(&hit.id).unwrap();
            assert_eq!(row[1], Value::FixedSizeBinary(hit.iid.0.to_vec()));
            vec![row[0].clone(), Value::F64(hit.score), row[2].clone(), row[3].clone()]
        }).collect();
        (rows, work, None)
    }
}

fn elapsed(start: Instant) -> f64 { start.elapsed().as_secs_f64() * 1e3 }

fn sample(ms: f64, work: &Work, fallback: Option<Fallback>) -> Json {
    json!({
        "ms": ms, "files": work.files, "windows": work.windows, "scored": work.scored,
        "hidden": work.hidden, "summary_ms": work.preparation.as_secs_f64() * 1e3,
        "summary_bytes": work.summary_bytes, "fallback": fallback.map(|reason| format!("{reason:?}")),
    })
}

async fn stress_versions(fixture: &Fixture<'_>, session: &mut NativeSession, ranker: &mut PointRanking, old: Timestamp) -> Json {
    let operations = vec![
        sql("INSERT INTO docs RECORDS {_id: 0, body: 'report approved needle audit audit', department: -1, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z', _valid_to: TIMESTAMP '1970-01-01T00:00:02Z'}"),
        sql("ERASE FROM docs WHERE _id = 1"),
        sql("DELETE FROM docs WHERE _id = 2"),
        sql("INSERT INTO docs RECORDS {_id: 3, department: -3, body: NULL}"),
    ];
    let new = commit(session, operations).await;
    let case = CASES[2];
    let snapshot = fixture.database.snapshot();
    let reference = oracle(session, &case.statement(new, new), case.limit).await;
    let (actual, _, fallback) = fixture.ranked(session, ranker, Ranking::ScoreAndId, &snapshot, (new, new), case).await;
    assert_eq!(actual, reference);
    assert_eq!(fallback, Some(Fallback::Visibility));

    flush(fixture.node).await;
    fixture.database.compaction().unwrap().compact_all().await.unwrap();
    fixture.node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
    fixture.database.build_artefacts().await.unwrap();
    let snapshot = fixture.database.snapshot();
    let mut probes = Vec::new();
    for points in [(old, old), (old, new), (new, new), (Timestamp(3_000_000), new)] {
        for case in [CASES[0], CASES[4]] {
            let expected = oracle(session, &case.statement(points.0, points.1), case.limit).await;
            let (rows, work, fallback) = fixture.ranked(session, ranker, Ranking::ScoreAndId, &snapshot, points, case).await;
            assert_eq!(rows, expected, "{points:?} {}", case.name);
            probes.push(sample(0.0, &work, fallback));
        }
    }
    json!({ "live_fallback": true, "persisted_probes": probes })
}

async fn populate(node: &Node, session: &mut NativeSession, table: &TableRef, documents: i64) -> Timestamp {
    let mut basis = commit(session, vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")]).await;
    for start in (0..documents).step_by(4000) {
        let rows = (start..(start + 4000).min(documents)).map(|id| StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("body".into(), Value::Utf8(text(id).into())),
            ("department".into(), Value::I64(id % 137)),
        ])).collect();
        basis = commit(session, vec![TxOp::PutDocs { table: table.clone(),
            documents: Documents::new(rows, node.memory()).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None,
        }]).await;
        if (start + 4000) % 20_000 == 0 { flush(node).await; }
    }
    flush(node).await;
    basis
}

pub(super) async fn run(documents: i64, iterations: usize) {
    assert!(documents >= 4000 && iterations > 0);
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let runtime = SearchRuntime::new(768 << 20);
    let scan = Arc::new(Inspection { search: runtime.accelerator(), narrowed: Mutex::default(), score_only: AtomicBool::new(false) });
    let mut services = bench_services(&clock);
    services.artefact_kinds = vec![runtime.artefacts()];
    services.scan_accelerators = vec![scan.clone()];
    let mut options = bench_options();
    options.cache_bytes = 768 << 20;
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let table = TableRef::new("public", "docs");
    let database = node.catalog().database("xtdb").unwrap();
    let fixture = Fixture { node: &node, database: &database, inspection: &scan, table: table.clone() };
    let basis = populate(&node, &mut session, &table, documents).await;
    let build_start = Instant::now();
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(60), &CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();
    let build_ms = elapsed(build_start);
    let snapshot = database.snapshot();
    assert_eq!(snapshot.tx_basis().unwrap().system_time, basis);
    assert!(snapshot.iter_segments(&table).all(|segment| segment.row_count() == 0));
    let tries = database.trie_catalog().current_tries(&table).len();
    let capture = Instant::now();
    let ids = captured_ids(&mut session, basis).await;
    assert_eq!(ids.len(), documents as usize);
    let mut ranker = PointRanking::new(ids).expect("Fixture SQL IDs must agree with stored IIDs");
    let id_capture_ms = elapsed(capture);
    let mut results = Vec::new();
    for case in CASES {
        let base = case.statement(basis, basis);
        let limited = format!("{base} LIMIT {}", case.limit);
        let expected = oracle(&mut session, &base, case.limit).await;
        let cold = Instant::now();
        let (first, work, fallback) = fixture.ranked(&mut session, &mut ranker, Ranking::ScoreAndId,
            &snapshot, (basis, basis), case).await;
        let first_tie = sample(elapsed(cold), &work, fallback);
        assert_eq!(first, expected, "{} first tie rank", case.name);
        let mut samples = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        for iteration in 0..iterations {
            for position in 0..4 {
                let index = (iteration + position) % 4;
                let start = Instant::now();
                let (actual, timing) = if index <= 1 {
                    scan.score_only.store(index == 1, Ordering::Relaxed);
                    let rows = query(&mut session, &limited).await;
                    scan.score_only.store(false, Ordering::Relaxed);
                    let elapsed = elapsed(start);
                    let narrowed = scan.narrowed.lock().unwrap().take().expect("optimizer reached ranked scan");
                    (rows, json!({"ms": elapsed, "candidate_iids": narrowed, "fallback": narrowed.is_none()}))
                } else {
                    let ranking = if index == 2 { Ranking::ScoreOnly } else { Ranking::ScoreAndId };
                    let (rows, work, fallback) = fixture.ranked(&mut session, &mut ranker, ranking,
                        &snapshot, (basis, basis), case).await;
                    (rows, sample(elapsed(start), &work, fallback))
                };
                assert_eq!(actual, expected, "{} variant {index}", case.name);
                samples[index].push(timing);
            }
        }
        let [sql, sql_score_only, direct, ties] = samples;
        results.push(json!({"case": case.name, "limit": case.limit, "result_rows": expected.len(),
            "first_tie": first_tie, "sql": sql, "sql_score_only": sql_score_only, "direct": direct, "tie_bounds": ties}));
    }
    let churn = if documents <= 100_000 { Some(stress_versions(&fixture, &mut session, &mut ranker, basis).await) } else { None };
    println!("{}", json!({"documents": documents, "tries": tries, "build_ms": build_ms, "id_capture_ms": id_capture_ms, "iterations": iterations,
        "queries": results, "churn": churn}));
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
}

struct InterleavedSearch {
    accelerator: Arc<SearchAccelerator>,
    baseline: AtomicBool,
    diagnostics: Mutex<Option<Json>>,
}

#[async_trait]
impl ScanAccelerator for InterleavedSearch {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        self.accelerator.candidate_iids(request).await
    }

    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        use rtdb_search::accelerate::experiment::workload::Policy;
        let policy = if self.baseline.load(Ordering::Relaxed) { Policy::Baseline } else { Policy::Current };
        let (selection, diagnostics) = self.accelerator.workload_selection(request, policy).await?;
        if request.scan.top_k.is_some() { *self.diagnostics.lock().unwrap() = Some(diagnostics); }
        Ok(selection)
    }

    async fn match_answers(&self, request: &ScanRequest<'_>, column: &str, query: &str) -> Result<Option<MatchAnswers>> {
        self.accelerator.match_answers(request, column, query).await
    }
}

fn sql_bench_storage(directory: &std::path::Path) -> rtdb_format::database::proto::database_config::Storage {
    use rtdb_format::database::proto::{LocalStorage, database_config};
    match std::env::var("RTDB_FTS_AZURE_EMULATOR") {
        Ok(endpoint) => {
            #[cfg(feature = "azure")]
            {
                use prost::Message;
                use rtdb_format::database::proto::RemoteStorage;
                let mut azure = rtdb_azure::AzureConfig::new(
                    std::env::var("RTDB_FTS_AZURE_ACCOUNT").expect("benchmark emulator account"),
                    "fts-benchmark",
                );
                azure.storage_account_endpoint = Some(endpoint);
                azure.storage_account_key = Some(std::env::var("RTDB_FTS_AZURE_KEY").expect("benchmark emulator key"));
                azure.prefix = Some(format!("qualification/{}", uuid::Uuid::new_v4()));
                database_config::Storage::RemoteStorage(RemoteStorage {
                    epoch: Some(0),
                    object_store: Some(prost_types::Any {
                        type_url: "type.googleapis.com/xtdb.azure.proto.AzureBlobStorageConfig".into(),
                        value: azure.to_proto().unwrap().encode_to_vec(),
                    }),
                })
            }
            #[cfg(not(feature = "azure"))]
            {
                let _ = endpoint;
                panic!("Build with the azure feature to use an Azure emulator");
            }
        }
        Err(_) => database_config::Storage::LocalStorage(LocalStorage {
            path: Some(directory.join("store").to_string_lossy().into_owned()),
            epoch: Some(0),
        }),
    }
}

fn sql_bench_services(clock: &Arc<TickClock>, remote: bool, cache: &std::path::Path) -> rtdb_node::NodeServices {
    let services = bench_services(clock);
    if !remote { return services; }
    #[cfg(feature = "azure")]
    {
        let mut services = services;
        let disk = rtdb_storage::disk_cache::DiskCache::open(cache, 2 << 30, rtdb_columnar::MemoryBudget::new(256 << 20)).unwrap();
        let clock = Arc::clone(clock);
        services.resources = Arc::new(rtdb_node::NativeResources::default()
            .with_log_clock(Arc::new(move || Ok(rtdb_node::Clock::now(&*clock))))
            .with_azure(Arc::new(BTreeMap::<String, rtdb_azure::AzureCredentials>::new())).unwrap()
            .with_disk_cache(disk));
        services
    }
    #[cfg(not(feature = "azure"))]
    {
        let _ = cache;
        unreachable!("remote benchmark storage requires Azure")
    }
}

pub(super) async fn run_ranked_sql(documents: i64, iterations: usize) {
    use rtdb_format::database::proto::{DatabaseConfig, LocalLog, database_config};
    use rtdb_node::Membership;
    assert!(documents >= 4000 && iterations > 0);
    let directory = tempfile::tempdir().unwrap();
    let path = |name: &str| Some(directory.path().join(name).to_string_lossy().into_owned());
    let mut options = bench_options();
    options.cache_bytes = 768 << 20;
    let storage = sql_bench_storage(directory.path());
    let remote = matches!(storage, database_config::Storage::RemoteStorage(_));
    options.membership = Membership::Durable {
        primary: DatabaseConfig {
            log: Some(database_config::Log::LocalLog(LocalLog { path: path("log"), epoch: Some(0) })),
            storage: Some(storage),
            ..Default::default()
        },
        skipped: Default::default(),
    };
    let table = TableRef::new("public", "docs");
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let fixture = |phase: &str| sql_bench_services(&clock, remote, &directory.path().join(phase));
    let basis = {
        let runtime = SearchRuntime::new(768 << 20);
        let mut services = fixture("v13");
        services.artefact_kinds = vec![runtime.artefacts()];
        services.scan_accelerators = vec![runtime.accelerator()];
        let (node, driver) = Node::open(options.clone(), services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
        let basis = populate(&node, &mut session, &table, documents).await;
        let database = node.catalog().database("xtdb").unwrap();
        database.compaction().unwrap().compact_all().await.unwrap();
        node.sync_all(Duration::from_secs(60), &CancellationToken::new()).await.unwrap();
        database.build_artefacts().await.unwrap();
        drop(session);
        drop(database);
        node.shutdown();
        tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
        basis
    };
    let tree_build_ms = {
        let mut services = fixture("v14");
        let runtime = SearchRuntime::with_tree_files(768 << 20);
        services.artefact_kinds = vec![runtime.artefacts()];
        services.scan_accelerators = vec![runtime.accelerator()];
        let (node, driver) = Node::open(options.clone(), services).await.unwrap();
        let driver = tokio::spawn(driver.run());
        node.sync_all(Duration::from_secs(60), &CancellationToken::new()).await.unwrap();
        let database = node.catalog().database("xtdb").unwrap();
        let build = Instant::now();
        database.build_artefacts().await.unwrap();
        let runtime = elapsed(build);
        drop(database);
        node.shutdown();
        tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
        runtime
    };
    let mut services = fixture("read");
    let runtime = SearchRuntime::with_tree_files(768 << 20);
    let scan = Arc::new(InterleavedSearch { accelerator: runtime.accelerator(), baseline: AtomicBool::new(false), diagnostics: Mutex::new(None) });
    services.artefact_kinds = vec![runtime.artefacts()];
    services.scan_accelerators = vec![scan.clone()];
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let database = node.catalog().database("xtdb").unwrap();
    node.sync_all(Duration::from_secs(60), &CancellationToken::new()).await.unwrap();
    let catalog = database.trie_catalog();
    let tries = catalog.current_tries(&table);
    assert!(tries.iter().any(|trie| trie.artefacts.iter().any(|record| record.definition_id().ends_with("f14"))));
    assert!(tries.iter().any(|trie| trie.artefacts.iter().any(|record| record.definition_id().ends_with("f13"))));
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let cold_case = CASES[3];
    let cold_base = cold_case.statement(basis, basis);
    let cold_sql = format!("{cold_base} LIMIT {}", cold_case.limit);
    let start = Instant::now();
    let cold = query(&mut session, &cold_sql).await;
    let cold_ms = elapsed(start);
    let cold_diagnostics = scan.diagnostics.lock().unwrap().take().expect("the cold SQL query produces a ranked scan");
    assert_eq!(cold, oracle(&mut session, &cold_base, cold_case.limit).await);
    let mut cases = Vec::new();
    for case in CASES {
        let base = case.statement(basis, basis);
        let sql = format!("{base} LIMIT {}", case.limit);
        let expected = oracle(&mut session, &base, case.limit).await;
        let mut samples = [Vec::new(), Vec::new()];
        let mut diagnostics = [Json::Null, Json::Null];
        for iteration in 0..=iterations {
            for position in 0..2 {
                let variant = (iteration + position) % 2;
                scan.baseline.store(variant == 0, Ordering::Relaxed);
                let start = Instant::now();
                let actual = query(&mut session, &sql).await;
                let ms = elapsed(start);
                assert_eq!(actual, expected, "{} format {}", case.name, 13 + variant);
                let profile = scan.diagnostics.lock().unwrap().take().expect("the SQL query produces a ranked scan");
                if iteration > 0 { samples[variant].push(ms); }
                diagnostics[variant] = profile;
            }
        }
        if case.name == "phrase" {
            assert_eq!(diagnostics[1]["tree"]["decline"], "phrase-paged");
        } else {
            assert!(diagnostics[1]["tree"]["opened_roots"].as_u64().unwrap() > 0);
            assert_eq!(diagnostics[1]["tree"]["retained_rows"].as_u64(), Some(case.limit as u64));
        }
        let [v13, v14] = samples;
        cases.push(json!({"case": case.name, "limit": case.limit, "format13_ms": v13, "format14_ms": v14, "diagnostics": diagnostics}));
    }
    println!("{}", json!({"documents": documents, "warm_samples_per_format": iterations, "tree_build_ms": tree_build_ms,
        "storage": if remote { "azure-emulator" } else { "local" }, "tries": tries.len(), "cases": cases,
        "cold": {"case": cold_case.name, "milliseconds": cold_ms, "diagnostics": cold_diagnostics}}));
    drop(session);
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
}
