use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use rtdb_node::{NativeSession, Node};
use rtdb_text::{Corpus, Query, bm25, stored_length, tokens};
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{StructFields, TableRef, Timestamp, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use tokio_util::sync::CancellationToken;

use super::{Identity, TickClock, bench_options, bench_services, commit, flush, query, sql, where_ids};

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
struct Contribution {
    length: u32,
    terms: BTreeSet<String>,
}

impl Contribution {
    fn of(value: &Value) -> Option<Self> {
        let words = match value {
            Value::Null | Value::Undefined => return None,
            Value::Utf8(text) => tokens(text),
            _ => Vec::new(),
        };
        Some(Self { length: stored_length(words.len()), terms: words.into_iter().collect() })
    }

    fn add(&self, corpus: &mut Corpus) {
        corpus.documents += 1;
        corpus.tokens += u64::from(self.length);
        for term in &self.terms { *corpus.frequencies.entry(term.clone()).or_default() += 1; }
    }

    fn remove(&self, corpus: &mut Corpus) {
        corpus.documents -= 1;
        corpus.tokens -= u64::from(self.length);
        for term in &self.terms {
            let count = corpus.frequencies.get_mut(term).unwrap();
            *count -= 1;
            if *count == 0 { corpus.frequencies.remove(term); }
        }
    }
}

#[derive(Deserialize, Serialize)]
struct CorpusDelta {
    from: Timestamp,
    through: Timestamp,
    replacements: BTreeMap<i64, Option<Contribution>>,
}

struct CorpusCheckpoint {
    basis: Timestamp,
    visible: BTreeMap<i64, Contribution>,
    totals: Corpus,
}

impl CorpusCheckpoint {
    async fn capture(session: &mut NativeSession, basis: Timestamp) -> Self {
        let mut checkpoint = Self { basis, visible: BTreeMap::new(), totals: Corpus::default() };
        checkpoint.add_rows(visible(session, basis, basis, "").await);
        checkpoint
    }

    fn add_rows(&mut self, rows: Vec<Vec<Value>>) {
        for row in rows {
            if let Some(contribution) = Contribution::of(&row[1]) {
                contribution.add(&mut self.totals);
                assert!(self.visible.insert(id(&row), contribution).is_none());
            }
        }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let (basis, visible): (_, BTreeMap<i64, Contribution>) = serde_json::from_slice(bytes).unwrap();
        let mut totals = Corpus::default();
        for contribution in visible.values() { contribution.add(&mut totals); }
        Self { basis, totals, visible }
    }

    fn apply(&mut self, delta: &CorpusDelta) {
        assert_eq!(delta.from, self.basis);
        assert!(delta.through >= delta.from);
        for (&id, replacement) in &delta.replacements {
            if let Some(old) = self.visible.remove(&id) { old.remove(&mut self.totals); }
            if let Some(replacement) = replacement {
                replacement.add(&mut self.totals);
                self.visible.insert(id, replacement.clone());
            }
        }
        self.basis = delta.through;
    }

    async fn advance(
        &mut self,
        session: &mut NativeSession,
        basis: Timestamp,
        mut touched: BTreeSet<i64>,
        boundaries: &BTreeMap<Timestamp, BTreeSet<i64>>,
    ) -> CorpusDelta {
        assert!(basis >= self.basis);
        for (_, scheduled) in boundaries.range((Excluded(self.basis), Included(basis))) {
            touched.extend(scheduled);
        }
        let ids: Vec<_> = touched.iter().copied().collect();
        let mut replacements: BTreeMap<_, _> = touched.into_iter().map(|id| (id, None)).collect();
        for ids in ids.chunks(128) {
            for row in visible(session, basis, basis, &where_ids(ids.iter().copied())).await {
                *replacements.get_mut(&id(&row)).unwrap() = Contribution::of(&row[1]);
            }
        }
        let delta = CorpusDelta { from: self.basis, through: basis, replacements };
        self.apply(&delta);
        delta
    }
}

pub(super) fn id(row: &[Value]) -> i64 {
    let Value::I64(id) = row[0] else { panic!("Expected integer IID source, got {:?}", row[0]); };
    id
}

fn time(timestamp: Timestamp) -> String {
    chrono::DateTime::from_timestamp_micros(timestamp.0).unwrap().to_rfc3339()
}

pub(super) fn as_of(valid: Timestamp, system: Timestamp) -> String {
    format!("FOR VALID_TIME AS OF TIMESTAMP '{}' FOR SYSTEM_TIME AS OF TIMESTAMP '{}'", time(valid), time(system))
}

async fn visible(session: &mut NativeSession, valid: Timestamp, system: Timestamp, constraint: &str) -> Vec<Vec<Value>> {
    query(session, &format!("SELECT _id, body FROM docs {}{constraint}", as_of(valid, system))).await
}

pub(super) fn exhaustive_corpus(rows: &[Vec<Value>]) -> Corpus {
    let mut corpus = Corpus::default();
    for row in rows {
        let body = &row[1];
        if matches!(body, Value::Null | Value::Undefined) { continue; }
        corpus.documents += 1;
        let words = match body { Value::Utf8(text) => tokens(text), _ => Vec::new() };
        corpus.tokens += u64::from(stored_length(words.len()));
        for term in words.into_iter().collect::<BTreeSet<_>>() {
            *corpus.frequencies.entry(term).or_default() += 1;
        }
    }
    corpus
}

async fn verify_ranking(
    session: &mut NativeSession, checkpoint: &CorpusCheckpoint, valid: Timestamp, system: Timestamp,
) {
    let matching = Query::parse("invoice");
    let scoring = Query::parse("alpha beta");
    let mut expected: Vec<_> = visible(session, valid, system, "").await.into_iter().filter_map(|row| {
        let Value::Utf8(body) = &row[1] else { return None; };
        let words = tokens(body);
        matching.matches(&words).then(|| (id(&row), bm25(&scoring, &words, Some(&checkpoint.totals))))
    }).collect();
    expected.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score.total_cmp(left_score).then_with(|| left_id.cmp(right_id))
    });
    let expected: Vec<_> = expected.into_iter().take(20)
        .map(|(id, score)| vec![Value::I64(id), Value::F64(score)]).collect();
    let ranked = format!(
        "SELECT _id, SEARCH_SCORE(body, 'alpha beta') AS score FROM docs {} WHERE MATCH(body, 'invoice') ORDER BY score DESC, _id LIMIT 20",
        as_of(valid, system),
    );
    assert_eq!(query(session, &ranked).await, expected, "{ranked} at checkpoint {:?}", checkpoint.basis);
}

async fn observe(
    session: &mut NativeSession,
    checkpoint: &mut CorpusCheckpoint,
    label: &str,
    basis: Timestamp,
    touched: BTreeSet<i64>,
    boundaries: &BTreeMap<Timestamp, BTreeSet<i64>>,
    history_at: Timestamp,
    replay: &mut CorpusCheckpoint,
) -> Json {
    let start = Instant::now();
    let delta = checkpoint.advance(session, basis, touched, boundaries).await;
    let delta_us = start.elapsed().as_secs_f64() * 1e6;
    let encoded = serde_json::to_vec(&delta).unwrap();
    let decoded = serde_json::from_slice(&encoded).unwrap();
    replay.apply(&decoded);
    assert_eq!(replay.totals, checkpoint.totals, "{label} replayed totals");
    assert_eq!(replay.visible, checkpoint.visible, "{label} replayed contributions");
    assert_eq!(replay.basis, basis);
    let start = Instant::now();
    let scan = visible(session, basis, basis, "").await;
    let exhaustive = exhaustive_corpus(&scan);
    let exhaustive_us = start.elapsed().as_secs_f64() * 1e6;
    assert_eq!(checkpoint.totals, exhaustive, "{label}");
    verify_ranking(session, checkpoint, basis, basis).await;
    verify_ranking(session, checkpoint, history_at, history_at).await;
    json!({
        "transition": label,
        "affected_iids": delta.replacements.len(),
        "delta_json_bytes": encoded.len(),
        "delta_us": delta_us,
        "exhaustive_us": exhaustive_us,
        "visible_documents": checkpoint.totals.documents,
        "term_frequencies": checkpoint.totals.frequencies,
    })
}

pub(super) async fn run(background_documents: i64) {
    assert!(background_documents > 0);
    let clock = Arc::new(TickClock(1_000_000.into()));
    let (node, driver) = Node::open(bench_options(), bench_services(&clock)).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let table = TableRef::new("public", "docs");
    commit(&mut session, vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")]).await;
    for start in (0..background_documents).step_by(4000) {
        let rows = (start..(start + 4000).min(background_documents)).map(|offset| {
            StructFields::from([
                ("_id".into(), Value::I64(offset + 10)),
                ("body".into(), Value::Utf8(if offset % 5 == 0 { "beta beta invoice" } else { "alpha invoice" }.into())),
            ])
        }).collect();
        commit(&mut session, vec![TxOp::PutDocs { table: table.clone(), documents: Documents::new(rows, node.memory()).unwrap(),
            valid_from: Some(Timestamp(0)), valid_to: None }]).await;
    }
    let past = commit(&mut session, vec![
        sql("INSERT INTO docs RECORDS {_id: 1, body: 'alpha alpha invoice'}"),
        sql("INSERT INTO docs RECORDS {_id: 2, body: 'beta invoice', _valid_from: TIMESTAMP '1970-01-01T00:00:02Z', _valid_to: TIMESTAMP '1970-01-01T00:00:04Z'}"),
        sql("INSERT INTO docs RECORDS {_id: 3, body: 'alpha invoice'}"),
        sql("INSERT INTO docs RECORDS {_id: 4, body: 71}"),
        sql("INSERT INTO docs RECORDS {_id: 5, title: 'body absent'}"),
    ]).await;
    flush(&node).await;
    let database = node.catalog().database("xtdb").unwrap();
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();

    let boundaries = BTreeMap::from([
        (Timestamp(2_000_000), BTreeSet::from([2])),
        (Timestamp(4_000_000), BTreeSet::from([2])),
        (Timestamp(4_500_000), BTreeSet::from([3])),
    ]);
    let start = Instant::now();
    let mut checkpoint = CorpusCheckpoint::capture(&mut session, past).await;
    let checkpoint_us = start.elapsed().as_secs_f64() * 1e6;
    assert_eq!(checkpoint.totals.documents, background_documents as u64 + 3);
    let checkpoint_file = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(checkpoint_file.as_file(), &(checkpoint.basis, &checkpoint.visible)).unwrap();
    let bytes = std::fs::read(checkpoint_file.path()).unwrap();
    let mut replay = CorpusCheckpoint::from_bytes(&bytes);
    assert_eq!(replay.totals, checkpoint.totals);
    assert_eq!(replay.visible, checkpoint.visible);
    verify_ranking(&mut session, &checkpoint, past, past).await;

    clock.0.store(2_100_000, Ordering::Relaxed);
    let activated = commit(&mut session, vec![sql("INSERT INTO other_table RECORDS {_id: 1}")]).await;
    let activation = observe(&mut session, &mut checkpoint, "scheduled activation with unrelated write",
        activated, BTreeSet::new(), &boundaries, past, &mut replay).await;
    assert_eq!(checkpoint.totals.documents, background_documents as u64 + 4);

    let corrected = commit(&mut session, vec![
        sql("INSERT INTO docs RECORDS {_id: 3, body: 'superseded invoice', _valid_from: TIMESTAMP '1970-01-01T00:00:01.150Z', _valid_to: TIMESTAMP '1970-01-01T00:00:04.500Z'}"),
        sql("INSERT INTO docs RECORDS {_id: 3, body: 'beta beta beta invoice', _valid_from: TIMESTAMP '1970-01-01T00:00:01.150Z', _valid_to: TIMESTAMP '1970-01-01T00:00:04.500Z'}"),
        sql("DELETE FROM docs WHERE _id = 1"),
        sql("INSERT INTO docs RECORDS {_id: 6, body: 'alpha beta invoice'}"),
    ]).await;
    let correction = observe(&mut session, &mut checkpoint, "backdated correction and same-transaction puts, delete and insert",
        corrected, BTreeSet::from([1, 3, 6]), &boundaries, past, &mut replay).await;

    clock.0.store(4_200_000, Ordering::Relaxed);
    let expired = commit(&mut session, vec![sql("INSERT INTO other_table RECORDS {_id: 2}")]).await;
    let expiry = observe(&mut session, &mut checkpoint, "scheduled expiry with unrelated write",
        expired, BTreeSet::new(), &boundaries, past, &mut replay).await;
    assert_eq!(checkpoint.totals.documents, background_documents as u64 + 3);

    clock.0.store(4_600_000, Ordering::Relaxed);
    let erased = commit(&mut session, vec![sql("ERASE FROM docs WHERE _id = 6")]).await;
    let erase = observe(&mut session, &mut checkpoint, "erase and corrected interval expiry",
        erased, BTreeSet::from([6]), &boundaries, past, &mut replay).await;
    assert_eq!(checkpoint.totals.documents, background_documents as u64 + 2);
    let mut churn = Vec::new();
    for changed in [10, 100, 1000].into_iter().filter(|count| *count <= background_documents) {
        let first = 10;
        let ids: BTreeSet<_> = (first..first + changed).collect();
        let rows = ids.iter().map(|id| StructFields::from([
            ("_id".into(), Value::I64(*id)),
            ("body".into(), Value::Utf8(format!("invoice gamma beta {changed}").into())),
        ])).collect();
        let next = commit(&mut session, vec![TxOp::PutDocs { table: table.clone(),
            documents: Documents::new(rows, node.memory()).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None,
        }]).await;
        churn.push(observe(&mut session, &mut checkpoint, "batch correction", next, ids,
            &boundaries, past, &mut replay).await);
    }
    println!("{}", json!({
        "checkpoint_json_bytes": bytes.len(),
        "churn": churn,
        "background_documents": background_documents,
        "initial_checkpoint_us": checkpoint_us,
        "transitions": [activation, correction, expiry, erase],
    }));
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
}
