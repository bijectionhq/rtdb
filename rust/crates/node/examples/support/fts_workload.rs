use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::{Arc, atomic::AtomicI64};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rtdb_columnar::read_value;
use rtdb_node::{NativeSession, Node};
use rtdb_query::{MatchAnswers, PreparedQuery, QueryContext, ScanAccelerator, ScanRequest};
use rtdb_search::{SearchAccelerator, SearchRuntime, accelerate::experiment::workload::Policy};
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{Iid, Result, StructFields, TableRef, Timestamp, Value};
use serde_json::{Value as Json, json};
use tokio::sync::Barrier;
use tokio_util::sync::CancellationToken;

use super::{Identity, TickClock, bench_options, bench_services, commit, flush, sql};
use super::corpus_checkpoint::{as_of, id};

#[derive(Clone, Debug)]
enum Mode { Current, Baseline, Exhaustive, FilterFirst(Arc<BTreeSet<Iid>>), SkipRetry, Estimate }

struct Observation {
    mode: Mode,
    plans: RefCell<Vec<Json>>,
}

tokio::task_local! {
    static OBSERVATION: Observation;
}

struct ObservedSearch(Arc<SearchAccelerator>);

#[async_trait]
impl ScanAccelerator for ObservedSearch {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        if request.scan.table.name.table.table != "docs" {
            return self.0.candidate_iids(request).await;
        }
        let Ok(mode) = OBSERVATION.try_with(|observation| observation.mode.clone()) else {
            return self.0.candidate_iids(request).await;
        };
        let policy = match &mode {
            Mode::Current => Policy::Current,
            Mode::Baseline => Policy::Baseline,
            Mode::Exhaustive => Policy::Exhaustive,
            Mode::FilterFirst(scope) => Policy::FilterFirst(scope),
            Mode::SkipRetry => Policy::SkipRetry,
            Mode::Estimate => Policy::Estimate,
        };
        let mut scan = request.scan.clone();
        if matches!(mode, Mode::Baseline | Mode::SkipRetry | Mode::Estimate)
            && let Some(top_k) = &mut scan.top_k {
            top_k.ascending_id = None;
        }
        let request = ScanRequest { scan: &scan, ..*request };
        let (picked, plan) = self.0.workload_candidates(&request, policy).await?;
        OBSERVATION.with(|observation| observation.plans.borrow_mut().push(plan));
        Ok(picked)
    }

    async fn match_answers(&self, request: &ScanRequest<'_>, column: &str, query: &str) -> Result<Option<MatchAnswers>> {
        if OBSERVATION.try_with(|observation| matches!(observation.mode, Mode::Exhaustive)).unwrap_or(false) {
            return Ok(None);
        }
        self.0.match_answers(request, column, query).await
    }
}

type Rows = Vec<Vec<Value>>;

async fn query(context: &Arc<QueryContext>, statement: &str, mode: Mode) -> (Rows, Vec<Json>) {
    let observation = Observation { mode, plans: RefCell::default() };
    OBSERVATION.scope(observation, async {
        let options = rtdb_sql::PrepareOptions { default_timezone: context.timezone.clone(),
            generate_series_end_exclusive: context.options.generate_series_end_exclusive, ..Default::default() };
        let prepared = rtdb_sql::prepare_in(statement, &context.catalog, &options, &context.memory).unwrap();
        let query = PreparedQuery::from_prepared(prepared, &context.memory).unwrap();
        let mut cursor = query.execute(Arc::clone(context), Vec::new()).await.unwrap();
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await.unwrap() {
            let batch = batch.batch();
            let schema = batch.schema();
            for row in 0..batch.num_rows() {
                rows.push(schema.fields().iter().enumerate().map(|(column, field)| {
                    read_value(field, batch.column(column).as_ref(), row).unwrap()
                }).collect());
            }
        }
        (rows, OBSERVATION.with(|observation| observation.plans.take()))
    }).await
}

fn millis(start: Instant) -> f64 { start.elapsed().as_secs_f64() * 1000.0 }

async fn sample(context: &Arc<QueryContext>, statement: &str, mode: Mode, expected: &Rows) -> Json {
    let start = Instant::now();
    let (rows, plans) = query(context, statement, mode).await;
    let ms = millis(start);
    assert_eq!(&rows, expected, "{statement}");
    assert_eq!(plans.len(), 1, "{statement}");
    json!({"ms": ms, "plans": plans})
}

fn ranked(at: &str, predicate: &str, score: &str, limit: usize) -> String {
    format!("SELECT _id, SEARCH_SCORE(body, '{score}') AS score, status, cohort, _valid_from, _valid_to, _system_from, _system_to \
        FROM docs {at} WHERE {predicate} ORDER BY score DESC, _id, _valid_from, _system_from, _valid_to, _system_to LIMIT {limit}")
}

fn points(basis: Timestamp) -> String { as_of(basis, basis) }

fn random(seed: i64) -> u64 {
    let mut n = (seed as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    n = (n ^ (n >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    n = (n ^ (n >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    n ^ (n >> 31)
}

fn fields(id: i64, revision: usize) -> StructFields<Value> {
    let bits = random(id + revision as i64 * 13_711);
    let product = ["billing", "network", "workspace", "database", "identity", "mobile"][(bits % 6) as usize];
    let detail = ["latency", "failed", "timeout", "retry", "resolved", "upgrade", "access"][(bits >> 7) as usize % 7];
    let body = if revision > 0 && id % 2000 == 0 {
        "incident administrative correction".to_owned()
    } else {
        let length = (1 + (bits >> 16).trailing_zeros() as usize).min(10);
        format!("incident {product} {detail} ticket{} {}{}{}",
            bits % 257,
            if bits % 13 == 0 { "payment declined " } else { "request from customer " },
            "diagnostic audit trace followup known issue ".repeat(length),
            if id % 2000 == 0 { "criticalsignal criticalsignal" } else { "" },
        )
    };
    StructFields::from([
        ("_id".into(), Value::I64(id)),
        ("body".into(), Value::Utf8(body.into())),
        ("cohort".into(), Value::I64(((bits >> 32) % 1000) as i64)),
        ("status".into(), Value::Utf8(if bits & 0x100 == 0 { "open" } else { "closed" }.into())),
    ])
}

async fn put(node: &Node, write: &mut NativeSession, ids: impl Iterator<Item = i64>, revision: usize, valid_to: Option<Timestamp>) -> Timestamp {
    let rows = ids.map(|id| fields(id, revision)).collect();
    commit(write, vec![TxOp::PutDocs { table: TableRef::new("public", "docs"),
        documents: Documents::new(rows, node.memory()).unwrap(), valid_from: Some(Timestamp(0)), valid_to,
    }]).await
}

async fn flush_and_index(node: &Node) {
    flush(node).await;
    let database = node.catalog().database("xtdb").unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(60), &CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();
}

async fn mask(context: &Arc<QueryContext>, at: &str, structured: &str) -> Arc<BTreeSet<Iid>> {
    let (rows, plans) = query(context, &format!("SELECT _iid FROM docs {at} WHERE {structured}"), Mode::Exhaustive).await;
    assert!(plans.iter().all(|plan| plan["route"] == "scan"));
    Arc::new(rows.into_iter().map(|row| {
        let Value::FixedSizeBinary(bytes) = &row[0] else { panic!("Expected an IID"); };
        Iid::from_slice(bytes).unwrap()
    }).collect())
}

async fn filter_first(context: &Arc<QueryContext>, current: Timestamp, iterations: usize) -> Vec<Json> {
    let mut out = Vec::new();
    let at = points(current);
    for ceiling in [2, 20, 200, 1000] {
        let structured = format!("status = 'open' AND cohort < {ceiling}");
        let statement = ranked(&at, &format!("MATCH(body, 'incident') AND {structured}"), "audit", 20);
        let scope = mask(context, &at, &structured).await;
        let expected = query(context, &statement, Mode::Exhaustive).await.0;
        let mut samples = [Vec::new(), Vec::new(), Vec::new()];
        for iteration in 0..iterations {
            for order in 0..3 {
                let index = (order + iteration) % 3;
                if index == 2 {
                    let start = Instant::now();
                    let scope = mask(context, &at, &structured).await;
                    let mask_ms = millis(start);
                    let mut measured = sample(context, &statement, Mode::FilterFirst(scope), &expected).await;
                    measured["mask_ms"] = json!(mask_ms);
                    measured["ms"] = json!(millis(start));
                    samples[index].push(measured);
                } else {
                    let mode = if index == 0 { Mode::Baseline } else { Mode::FilterFirst(Arc::clone(&scope)) };
                    samples[index].push(sample(context, &statement, mode, &expected).await);
                }
            }
        }
        let [current, prepared, materialized] = samples;
        out.push(json!({"cohort_lt": ceiling, "scope_iids": scope.len(), "result_rows": expected.len(),
            "current": current, "prepared": prepared, "materialized": materialized}));
    }
    out
}

async fn fallback(context: &Arc<QueryContext>, basis: Timestamp, iterations: usize) -> Vec<Json> {
    let at = points(basis);
    let mut out = Vec::new();
    for (name, predicate, score) in [
        ("uniform", "MATCH(body, 'incident')", "noindexedscore"),
        ("rare-high-scores", "MATCH(body, 'incident')", "criticalsignal"),
        ("verification-budget", "MATCH(body, 'incident') AND status = 'open' AND cohort < 2", "audit"),
        ("phrase", "MATCH(body, '\"payment declined\"')", "audit"),
    ] {
        let statement = ranked(&at, predicate, score, 20);
        let expected = query(context, &statement, Mode::Exhaustive).await.0;
        let mut samples = [Vec::new(), Vec::new(), Vec::new()];
        for iteration in 0..iterations {
            for order in 0..3 {
                let index = (order + iteration) % 3;
                let mode = [Mode::Baseline, Mode::SkipRetry, Mode::Estimate][index].clone();
                samples[index].push(sample(context, &statement, mode, &expected).await);
            }
        }
        let [current, skip_retry, estimate] = samples;
        out.push(json!({"case": name, "current": current, "skip_retry": skip_retry, "estimate": estimate}));
    }
    out
}

fn exhaustive_context(context: &Arc<QueryContext>, search: &Arc<dyn ScanAccelerator>) -> Arc<QueryContext> {
    Arc::clone(context).with_accelerators(Arc::from([Arc::clone(search)]))
}

async fn version_canary(
    node: &Node, write: &mut NativeSession, read: &mut NativeSession,
    search: &Arc<dyn ScanAccelerator>, ordered: bool,
) -> Timestamp {
    let old = commit(write, vec![
        sql("INSERT INTO docs RECORDS {_id: -1, body: 'forensiconly', status: 'closed', cohort: 0, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z'}"),
        sql("INSERT INTO docs RECORDS {_id: -2, body: 'forensiconly', status: 'open', cohort: 0, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z'}"),
    ]).await;
    flush_and_index(node).await;
    let after = commit(write, vec![
        sql("INSERT INTO docs RECORDS {_id: -1, body: 'administrative', status: 'open', cohort: 0, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z', _valid_to: TIMESTAMP '1970-01-01T00:00:03Z'}"),
        sql("INSERT INTO docs RECORDS {_id: -2, body: 'forensiconly changed', status: 'open', cohort: 0, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z'}"),
    ]).await;
    for compacted in [false, true] {
        if compacted { flush_and_index(node).await; }
        let context = read.query_context().await.unwrap();
        let exhaustive = exhaustive_context(&context, search);
        for (valid, system) in [(old, old), (old, after), (Timestamp(3_000_000), after)] {
            let at = as_of(valid, system);
            let structured = "status = 'open' AND cohort = 0";
            let scope = mask(&exhaustive, &at, structured).await;
            let statement = ranked(&at, &format!("MATCH(body, 'forensiconly') AND {structured}"), "forensiconly", 20);
            let expected = query(&exhaustive, &statement, Mode::Exhaustive).await.0;
            assert_eq!(expected.iter().map(|row| id(row)).collect::<Vec<_>>(), [-2]);
            let current = sample(&context, &statement, Mode::Current, &expected).await;
            if ordered && compacted {
                assert!(current["plans"][0]["filter_scope"].as_u64().is_some(), "{statement}");
            }
            sample(&context, &statement, Mode::FilterFirst(scope), &expected).await;
        }
    }
    after
}

async fn replay(node: &Node, read: &mut NativeSession, old: Timestamp, waves: usize, documents: i64) -> Vec<Json> {
    let mut out = Vec::new();
    let mut latest = old;
    for wave in 0..waves {
        let context = read.query_context().await.unwrap();
        let now = points(latest);
        let historical = as_of(old, old);
        let instant = historical.split_once(" FOR SYSTEM_TIME").unwrap().1;
        let range = format!("FOR ALL VALID_TIME FOR SYSTEM_TIME{instant}");
        let requests = [
            ("broad", ranked(&now, "MATCH(body, 'incident')", "audit", 20)),
            ("fresh", ranked(&now, &format!("MATCH(body, 'incident ticket{}')", (wave * 7) % 257), "failed retry", 20)),
            ("phrase", ranked(&now, "MATCH(body, '\"payment declined\"')", "billing audit", 20)),
            ("multiple", ranked(&now, "MATCH(body, 'incident') AND MATCH(body, 'database retry') AND status = 'open'", "timeout audit", 20)),
            ("historical", ranked(&historical, "MATCH(body, 'database') AND status = 'open'", "database audit", 20)),
            ("temporal-range", ranked(&range, "MATCH(body, 'billing')", "billing audit", 20)),
        ];
        let start = Arc::new(Barrier::new(requests.len() + 1));
        let mut readers = Vec::new();
        for (name, statement) in requests {
            let context = Arc::clone(&context);
            let start = Arc::clone(&start);
            readers.push(tokio::spawn(async move {
                start.wait().await;
                let began = Instant::now();
                let (rows, plans) = query(&context, &statement, Mode::Current).await;
                let ended = Instant::now();
                (name, statement, rows, plans, began, ended)
            }));
        }
        let mut writer = node.session("xtdb", Arc::new(Identity)).unwrap();
        let writer_node = node.clone();
        let writer_task = tokio::spawn(async move {
            start.wait().await;
            let began = Instant::now();
            let high_score_id = (wave as i64 * 2000) % documents;
            let ids = (0..64).map(|offset| (high_score_id + offset * 1999) % documents);
            let basis = put(&writer_node, &mut writer, ids, wave + 1, Some(Timestamp(30_000_000))).await;
            (basis, began, Instant::now())
        });
        let (basis, writer_start, writer_end) = writer_task.await.unwrap();
        for reader in readers {
            let (name, statement, actual, plans, began, ended) = reader.await.unwrap();
            let expected = query(&context, &statement, Mode::Exhaustive).await.0;
            assert_eq!(actual, expected, "replay wave {wave} {statement}");
            assert_eq!(plans.len(), 1, "{statement}");
            out.push(json!({"case": name, "wave": wave, "ms": (ended - began).as_secs_f64()*1000.0,
                "write_overlap": began < writer_end && writer_start < ended, "plans": plans}));
        }
        latest = basis;
    }
    out
}

async fn production(context: &Arc<QueryContext>, oracle: &Arc<QueryContext>, basis: Timestamp, iterations: usize) -> Vec<Json> {
    let at = points(basis);
    let mut out = Vec::new();
    let mut cases = vec![
        ("uniform".to_string(), "MATCH(body, 'incident')".to_string(), "noindexedscore"),
        ("rare-winners".to_string(), "MATCH(body, 'incident')".to_string(), "criticalsignal"),
    ];
    cases.extend([2, 20, 200, 1000].map(|ceiling| (
        format!("status-and-cohort-{ceiling}"),
        format!("MATCH(body, 'incident') AND status = 'open' AND cohort < {ceiling}"),
        "audit",
    )));
    for (name, predicate, score) in cases {
        let statement = ranked(&at, &predicate, score, 20);
        let expected = query(oracle, &statement, Mode::Exhaustive).await.0;
        let mut runs = [Vec::new(), Vec::new()];
        for iteration in 0..iterations {
            for order in 0..2 {
                let index = (iteration + order) % 2;
                let mode = [Mode::Baseline, Mode::Current][index].clone();
                runs[index].push(sample(context, &statement, mode, &expected).await);
            }
        }
        let [baseline, current] = runs;
        out.push(json!({"case": name, "baseline": baseline, "current": current}));
    }
    out
}

async fn run_fixture(documents: i64, iterations: usize, candidate_limit: usize, ordered: bool) -> Json {
    assert!(documents >= 4000 && iterations > 0);
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let runtime = SearchRuntime::new(512 << 20);
    let mut services = bench_services(&clock);
    services.artefact_kinds = vec![runtime.artefacts()];
    let mut accelerator = SearchAccelerator::with_runtime(runtime);
    accelerator.max_candidates = candidate_limit;
    let search: Arc<dyn ScanAccelerator> = Arc::new(ObservedSearch(Arc::new(accelerator)));
    services.scan_accelerators = vec![Arc::clone(&search)];
    let mut options = bench_options();
    options.cache_bytes = 512 << 20;
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut write = node.session("xtdb", Arc::new(Identity)).unwrap();
    let mut read = node.session("xtdb", Arc::new(Identity)).unwrap();
    commit(&mut write, vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")]).await;
    if ordered { commit(&mut write, vec![sql("CREATE INDEX docs_status ON docs (status, cohort)")]).await; }
    for offset in (0..documents).step_by(4000) {
        put(&node, &mut write, offset..(offset + 4000).min(documents), 0, None).await;
        if (offset + 4000) % 20_000 == 0 { flush(&node).await; }
    }
    flush_and_index(&node).await;
    let basis = version_canary(&node, &mut write, &mut read, &search, ordered).await;
    let context = read.query_context().await.unwrap();
    let result = if ordered {
        let oracle = exhaustive_context(&context, &search);
        let cases = production(&context, &oracle, basis, iterations).await;
        json!({"documents": documents, "candidate_limit": candidate_limit, "iterations": iterations,
            "temporal_canary_comparisons": 12, "cases": cases})
    } else {
        let filters = filter_first(&context, basis, iterations).await;
        let fallbacks = fallback(&context, basis, iterations).await;
        drop(context);
        let observations = replay(&node, &mut read, basis, iterations, documents).await;
        let after = node.catalog().database("xtdb").unwrap().snapshot().tx_basis().unwrap().system_time;
        let changed = read.query_context().await.unwrap();
        let corrected_fallbacks = fallback(&changed, after, iterations).await;
        let tries = node.catalog().database("xtdb").unwrap().snapshot().trie_entries(&TableRef::new("public", "docs")).len();
        json!({"documents": documents, "iterations": iterations, "tries": tries, "candidate_limit": candidate_limit,
            "temporal_canary_comparisons": 12, "filter_first": filters, "fallback": fallbacks,
            "replay": observations, "fallback_after_corrections": corrected_fallbacks})
    };
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
    result
}

pub(super) async fn run(documents: i64, iterations: usize, ordered: bool) {
    println!("{}", run_fixture(documents, iterations, 100_000, ordered).await);
}

#[cfg(test)]
mod tests {
    use super::run_fixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn workload_preserves_exact_results_with_filter_and_fallback_policies() {
        let result = run_fixture(4000, 1, 1000, false).await;
        let uniform = &result["fallback"][0];
        let current = &uniform["current"][0]["plans"][0];
        let skip_retry = &uniform["skip_retry"][0]["plans"][0];
        let estimate = &uniform["estimate"][0]["plans"][0];
        assert_eq!(current["rank_decline"], "candidate-budget");
        assert!(current["unranked_events"].as_u64().unwrap() > 1000);
        assert_eq!(skip_retry["rank_decline"], "candidate-budget");
        assert_eq!(skip_retry["unranked_events"], 0);
        assert_eq!(estimate["rank_decline"], "posting-estimate");
        assert_eq!(estimate["score_evaluations"], 0);
        let filtered = &result["filter_first"][0]["prepared"][0]["plans"][0];
        assert_eq!(filtered["route"], "ranked");
        assert!(filtered["filter_scope"].as_u64().unwrap() < 1000);
        assert_eq!(result["replay"].as_array().unwrap().len(), 6);
        assert_eq!(result["temporal_canary_comparisons"], 12);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn production_ranking_reuses_ordered_iids_and_skips_a_provably_oversize_retry() {
        let result = run_fixture(4000, 1, 1000, true).await;
        let cases = result["cases"].as_array().unwrap();
        let baseline = &cases[0]["baseline"][0]["plans"][0];
        let current = &cases[0]["current"][0]["plans"][0];
        assert!(baseline["unranked_events"].as_u64().unwrap() > 1000);
        assert_eq!(current["rank_decline"], "candidate-budget");
        assert_eq!(current["retry_redundant"], true);
        assert_eq!(current["unranked_events"], 0);
        let baseline = &cases[2]["baseline"][0]["plans"][0];
        let current = &cases[2]["current"][0]["plans"][0];
        assert_eq!(current["route"], "ranked");
        assert!(current["filter_scope"].as_u64().is_some_and(|count| count > 0 && count < 1000));
        assert!(current["score_evaluations"].as_u64().unwrap() < baseline["score_evaluations"].as_u64().unwrap());
    }
}
