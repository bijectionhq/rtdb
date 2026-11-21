use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, atomic::AtomicI64};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_index::Snapshot;
use rtdb_node::{Database, NativeSession, Node};
use rtdb_plan::{Scan, TableBinding, QualifiedTable, Expr, TemporalFilter};
use rtdb_query::{CandidateVerifier, ConstantEvaluator, CorpusRegistry, MatchAnswers, ScanAccelerator, ScanRequest};
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
}

#[async_trait]
impl ScanAccelerator for Inspection {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        let iids = self.search.candidate_iids(request).await?;
        if request.scan.top_k.is_some() {
            *self.narrowed.lock().unwrap() = Some(iids.as_ref().map(BTreeSet::len));
        }
        Ok(iids)
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
    async fn visible(&self, _: BTreeSet<Iid>, _: rtdb_plan::ColumnId) -> Result<Vec<(Iid, Value)>> {
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
            columns: Vec::new(), predicates: Vec::new(), matches: Vec::new(), iids: None, top_k: None,
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
        let corpora = CorpusRegistry::default();
        let memory = self.node.query_memory();
        let request = ScanRequest { database: "xtdb", scan: &scan, snapshot, storage: self.database.storage(), bounds,
            memory: &memory, current_time: self.node.query_instant(), cancellation: self.node.cancellation(),
            constants: &LiteralOnly, corpora: &corpora, partitions: std::slice::from_ref(snapshot), verifier: &LiteralOnly };
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

pub(super) async fn run(documents: i64, iterations: usize) {
    assert!(documents >= 4000 && iterations > 0);
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let runtime = SearchRuntime::new(768 << 20);
    let scan = Arc::new(Inspection { search: runtime.accelerator(), narrowed: Mutex::default() });
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
    let mut basis = commit(&mut session, vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")]).await;
    for start in (0..documents).step_by(4000) {
        let rows = (start..(start + 4000).min(documents)).map(|id| StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("body".into(), Value::Utf8(text(id).into())),
            ("department".into(), Value::I64(id % 137)),
        ])).collect();
        basis = commit(&mut session, vec![TxOp::PutDocs { table: table.clone(),
            documents: Documents::new(rows, node.memory()).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None,
        }]).await;
        if (start + 4000) % 20_000 == 0 { flush(&node).await; }
    }
    flush(&node).await;
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
        let mut samples = [Vec::new(), Vec::new(), Vec::new()];
        for iteration in 0..iterations {
            for position in 0..3 {
                let index = (iteration + position) % 3;
                let start = Instant::now();
                let (actual, timing) = if index == 0 {
                    let rows = query(&mut session, &limited).await;
                    let elapsed = elapsed(start);
                    let narrowed = scan.narrowed.lock().unwrap().take().expect("optimizer reached ranked scan");
                    (rows, json!({"ms": elapsed, "candidate_iids": narrowed, "fallback": narrowed.is_none()}))
                } else {
                    let ranking = if index == 1 { Ranking::ScoreOnly } else { Ranking::ScoreAndId };
                    let (rows, work, fallback) = fixture.ranked(&mut session, &mut ranker, ranking,
                        &snapshot, (basis, basis), case).await;
                    (rows, sample(elapsed(start), &work, fallback))
                };
                assert_eq!(actual, expected, "{} variant {index}", case.name);
                samples[index].push(timing);
            }
        }
        let [sql, direct, ties] = samples;
        results.push(json!({"case": case.name, "limit": case.limit, "result_rows": expected.len(),
            "first_tie": first_tie, "sql": sql, "direct": direct, "tie_bounds": ties}));
    }
    let churn = if documents <= 100_000 { Some(stress_versions(&fixture, &mut session, &mut ranker, basis).await) } else { None };
    println!("{}", json!({"documents": documents, "tries": tries, "build_ms": build_ms, "id_capture_ms": id_capture_ms, "iterations": iterations,
        "queries": results, "churn": churn}));
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
}
