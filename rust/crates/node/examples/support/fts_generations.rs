use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, atomic::AtomicI64};
use std::time::{Duration, Instant};

use arrow_buffer::Buffer;
use rtdb_format::keys::TableSlug;
use rtdb_node::{NativeSession, Node};
use rtdb_search::{SearchFile, SearchRow, build_search_file};
use rtdb_storage::{ObjectKey, ObjectStore, StoredObject};
use rtdb_text::{Corpus, Query};
use rtdb_tx::{Documents, TxOp};
use rtdb_types::{EntityId, EventOp, Interval, StructFields, TableRef, TemporalBounds, Timestamp, Value};
use serde_json::{Value as Json, json};
use tokio_util::sync::CancellationToken;

use super::{Identity, TickClock, bench_options, bench_services, body, commit, flush, query, sql, where_ids};
use super::corpus_checkpoint::{as_of, exhaustive_corpus, id};

fn tokens(text: &str) -> Vec<String> { rtdb_text::Analyzer::Simple.tokens(text) }

const LIMIT: usize = 20;
const RANGE_BLOCK_BYTES: usize = 64 * 1024;

type Iid = [u8; 16];
type Row = Vec<Value>;

struct Bundle {
    path: PathBuf,
    bytes: usize,
}

impl Bundle {
    fn write(directory: &Path, name: &str, rows: Vec<&Row>, basis: Timestamp) -> Self {
        let mut rows: Vec<_> = rows.into_iter().map(|row| SearchRow {
            position: None,
            identity: None, iid: EntityId::Integer(id(row)).iid().0,
            system_from: basis.0,
            valid_from: Timestamp::MIN.0,
            valid_to: Timestamp::MAX.0,
            op: EventOp::Put,
            values: vec![match &row[1] {
                Value::Utf8(value) => Some(tokens(value)),
                Value::Null | Value::Undefined => None,
                _ => Some(Vec::new()),
            }],
        }).collect();
        rows.sort_unstable_by_key(|row| row.iid);
        let bytes = build_search_file(&columns(), &rows, name).unwrap();
        let path = directory.join(name);
        fs::write(&path, &bytes).unwrap();
        Self { path, bytes: bytes.len() }
    }

    fn open(&self, ranged: bool, io: &Arc<Mutex<Reads>>) -> Arc<SearchFile> {
        if ranged {
            let reader = RangedFile {
                file: File::open(&self.path).unwrap(),
                len: self.bytes,
                blocks: Mutex::default(),
                reads: Arc::clone(io),
            };
            Arc::new(SearchFile::open_with_ranges(self.bytes, move |range| reader.read_bytes(range), &columns()).unwrap())
        } else {
            let bytes = fs::read(&self.path).unwrap();
            io.lock().unwrap().record(bytes.len());
            Arc::new(SearchFile::open(bytes, &columns()).unwrap())
        }
    }
}

fn columns() -> [String; 1] { ["body".into()] }

#[derive(Debug, Default)]
struct Reads {
    requests: usize,
    bytes: usize,
}

impl Reads {
    fn record(&mut self, bytes: usize) {
        self.requests += 1;
        self.bytes += bytes;
    }
}

#[derive(Debug)]
struct RangedFile {
    file: File,
    len: usize,
    blocks: Mutex<BTreeMap<usize, Buffer>>,
    reads: Arc<Mutex<Reads>>,
}

impl RangedFile {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<Buffer> {
        if range.is_empty() { return Ok(Buffer::from(Vec::<u8>::new())); }
        let mut blocks = self.blocks.lock().unwrap();
        let first = range.start / RANGE_BLOCK_BYTES;
        let last = (range.end - 1) / RANGE_BLOCK_BYTES;
        let mut result = Vec::new();
        for block in first..=last {
            let start = block * RANGE_BLOCK_BYTES;
            let end = (start + RANGE_BLOCK_BYTES).min(self.len);
            let bytes = match blocks.entry(block) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let mut bytes = vec![0; end - start];
                    let mut file = &self.file;
                    file.seek(SeekFrom::Start(start as u64))?;
                    file.read_exact(&mut bytes)?;
                    self.reads.lock().unwrap().record(bytes.len());
                    entry.insert(Buffer::from(bytes))
                }
            };
            let part = range.start.max(start) - start..range.end.min(end) - start;
            if first == last { return Ok(bytes.slice_with_length(part.start, part.len())); }
            result.extend_from_slice(&bytes.as_slice()[part]);
        }
        Ok(Buffer::from(result))
    }
}

struct Generation {
    basis: Timestamp,
    valid: Timestamp,
    rows: Arc<BTreeMap<Iid, Row>>,
    bundles: Vec<Bundle>,
}

impl Generation {
    fn build(directory: &Path, basis: Timestamp, valid: Timestamp, rows: Arc<BTreeMap<Iid, Row>>, files: usize) -> Self {
        let mut groups = vec![Vec::new(); files];
        for (iid, row) in rows.iter() { groups[partition(iid, files)].push(row); }
        let bundles = groups.into_iter().enumerate().filter(|(_, rows)| !rows.is_empty())
            .map(|(index, rows)| Bundle::write(directory, &format!("base-{files}-{index}"), rows, basis)).collect();
        Self { basis, valid, rows, bundles }
    }
}

fn partition(iid: &Iid, files: usize) -> usize {
    let hash = u64::from_be_bytes(iid[..8].try_into().unwrap());
    (hash % files as u64) as usize
}

struct Overlay {
    from: Timestamp,
    through: Timestamp,
    valid: Timestamp,
    changed: BTreeSet<Iid>,
    rows: BTreeMap<Iid, Row>,
    bundle: Bundle,
}

impl Overlay {
    async fn resolve(
        directory: &Path, session: &mut NativeSession, base: &Generation, through: Timestamp,
        ids: &[i64], complete: bool,
    ) -> Option<Self> {
        if !complete || through < base.basis { return None; }
        let rows = snapshot_rows(session, base.valid, through, &where_ids(ids.iter().copied())).await;
        let changed = ids.iter().map(|id| EntityId::Integer(*id).iid().0).collect();
        let rows = by_iid(rows);
        let bundle = Bundle::write(directory, "overlay", rows.values().collect(), through);
        Some(Self { from: base.basis, through, valid: base.valid, changed, rows, bundle })
    }
}

async fn snapshot_rows(session: &mut NativeSession, valid: Timestamp, basis: Timestamp, constraint: &str) -> Vec<Row> {
    query(session, &format!("SELECT _id, body, department, _system_from FROM docs {}{constraint}", as_of(valid, basis))).await
}

fn by_iid(rows: Vec<Row>) -> BTreeMap<Iid, Row> {
    rows.into_iter().map(|row| (EntityId::Integer(id(&row)).iid().0, row)).collect()
}

struct Search<'a> {
    generation: &'a Generation,
    overlay: Option<&'a Overlay>,
    basis: Timestamp,
    corpus: &'a Corpus,
    files: Vec<Arc<SearchFile>>,
}

impl<'a> Search<'a> {
    fn open(
        generation: &'a Generation, overlay: Option<&'a Overlay>, basis: Timestamp, corpus: &'a Corpus,
        ranged: bool, reads: &Arc<Mutex<Reads>>,
    ) -> Option<Self> {
        match overlay {
            Some(overlay) if overlay.from == generation.basis && overlay.through == basis
                && overlay.valid == generation.valid => {}
            None if basis == generation.basis => {}
            _ => return None,
        }
        let files = generation.bundles.iter().chain(overlay.map(|entry| &entry.bundle))
            .map(|bundle| bundle.open(ranged, reads)).collect();
        Some(Self { generation, overlay, basis, corpus, files })
    }

    fn ranked(&self, matching: &Query, scoring: &Query) -> (Vec<Row>, Work) {
        let bounds = TemporalBounds {
            valid_time: Interval::at(self.generation.valid).unwrap(),
            system_time: Interval::at(self.basis).unwrap(),
        };
        let mut work = Work::default();
        let scorer = Arc::new(rtdb_text::Scorer::new(scoring.clone(), Some(self.corpus))).for_ranking().unwrap();
        let mut files: Vec<_> = self.files.iter()
            .map(|file| file.ranked("body", matching, &scorer, bounds).unwrap()).collect();
        let mut top: Vec<(f64, i64, &Row)> = Vec::new();
        while let Some((index, bound)) = files.iter().enumerate().filter_map(|(index, file)| {
            file.bound().map(|bound| (index, bound))
        }).max_by(|left, right| left.1.total_cmp(&right.1)) {
            if top.len() == LIMIT && bound < top[LIMIT - 1].0 { break; }
            work.expansions += 1;
            for (doc, score) in files[index].expand().unwrap() {
                work.scored += 1;
                let row = if index == self.generation.bundles.len() {
                    &self.overlay.unwrap().rows[&doc.iid]
                } else {
                    if self.overlay.is_some_and(|overlay| overlay.changed.contains(&doc.iid)) { continue; }
                    &self.generation.rows[&doc.iid]
                };
                top.push((score, id(row), row));
            }
            top.sort_unstable_by(|left, right| right.0.total_cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            top.truncate(LIMIT);
        }
        work.rows_materialized = top.len();
        let result = top.into_iter().map(|(score, _, row)| {
            vec![row[0].clone(), Value::F64(score), row[2].clone(), row[3].clone()]
        }).collect();
        (result, work)
    }
}

#[derive(Default)]
struct Work {
    expansions: usize,
    scored: usize,
    rows_materialized: usize,
}

fn micros(start: Instant) -> f64 { start.elapsed().as_secs_f64() * 1e6 }

fn report(samples: &[f64]) -> Json {
    let mut values = samples.to_vec();
    values.sort_unstable_by(f64::total_cmp);
    let percentile = |p: f64| values[(p * values.len() as f64).ceil() as usize - 1];
    json!({"p50_us": percentile(0.50), "p95_us": percentile(0.95), "samples": samples})
}

const FILTERS: [(&str, &str); 3] = [
    ("broad", "report"),
    ("selective", "report needle"),
    ("phrase", "\"report approved\" needle"),
];

fn ranking_sql(filter: &str, valid: Timestamp, basis: Timestamp, corpus: &Corpus, limited: bool) -> String {
    let limit = if limited { " LIMIT 20" } else { "" };
    let score = super::corpus_checkpoint::corpus_score_sql("audit", corpus);
    format!("SELECT _id, {score} AS score, department, _system_from FROM docs {} \
             WHERE MATCH(body, '{filter}') ORDER BY score DESC, _id{limit}", as_of(valid, basis))
}

async fn baseline(session: &mut NativeSession, valid: Timestamp, basis: Timestamp, corpus: &Corpus, iterations: usize) -> (Vec<Vec<Row>>, Vec<Json>) {
    let mut expected = Vec::new();
    let mut timings = Vec::new();
    for (name, filter) in FILTERS {
        let all = query(session, &ranking_sql(filter, valid, basis, corpus, false)).await;
        let top: Vec<_> = all.into_iter().take(LIMIT).collect();
        let statement = ranking_sql(filter, valid, basis, corpus, true);
        let mut samples = Vec::with_capacity(iterations);
        for _ in 0..iterations {
            let start = Instant::now();
            let result = query(session, &statement).await;
            samples.push(micros(start));
            assert_eq!(result, top, "{statement}");
        }
        timings.push(json!({"filter": name, "timing": report(&samples)}));
        expected.push(top);
    }
    (expected, timings)
}

fn measure(
    generation: &Generation, corpus: &Corpus, expected: &[Vec<Row>], iterations: usize,
) -> Vec<Json> {
    let mut results = Vec::new();
    let reads = Arc::new(Mutex::new(Reads::default()));
    let warmed = Search::open(generation, None, generation.basis, corpus, false, &reads).unwrap();
    for ((name, filter), expected) in FILTERS.into_iter().zip(expected) {
        let matching = Query::parse(filter);
        let scoring = Query::parse("audit");
        for mode in ["warm", "cold_full", "cold_range"] {
            let mut samples = Vec::new();
            let mut metrics = Vec::new();
            for _ in 0..iterations {
                let reads = Arc::new(Mutex::new(Reads::default()));
                let start = Instant::now();
                let cold;
                let search = if mode == "warm" { &warmed } else {
                    cold = Search::open(generation, None, generation.basis, corpus, mode == "cold_range", &reads).unwrap();
                    &cold
                };
                let (actual, work) = search.ranked(&matching, &scoring);
                samples.push(micros(start));
                assert_eq!(&actual, expected, "{name} {mode} {} files", generation.bundles.len());
                let reads = reads.lock().unwrap();
                metrics.push((reads.requests, reads.bytes, work.expansions, work.scored, work.rows_materialized));
            }
            assert!(metrics.iter().all(|sample| *sample == metrics[0]));
            results.push(json!({
                "filter": name, "mode": mode, "timing": report(&samples), "requests": metrics[0].0,
                "bytes": metrics[0].1, "expansions": metrics[0].2, "docs_scored": metrics[0].3,
                "rows_materialized": metrics[0].4,
            }));
        }
    }
    results
}

async fn primary_fetch(
    session: &mut NativeSession, generation: &Generation, corpus: &Corpus,
    expected: &[Vec<Row>], iterations: usize,
) -> Vec<Json> {
    let reads = Arc::new(Mutex::new(Reads::default()));
    let search = Search::open(generation, None, generation.basis, corpus, false, &reads).unwrap();
    let clock = as_of(generation.valid, generation.basis);
    let scoring = Query::parse("audit");
    let mut results = Vec::new();
    for ((name, filter), expected) in FILTERS.into_iter().zip(expected) {
        let matching = Query::parse(filter);
        let mut samples = Vec::new();
        for _ in 0..iterations {
            let start = Instant::now();
            let (mut ranked, _) = search.ranked(&matching, &scoring);
            if !ranked.is_empty() {
                let selected = where_ids(ranked.iter().map(|row| id(row)));
                let fetched = query(session, &format!(
                    "SELECT _id, department, _system_from FROM docs {clock}{selected}"
                )).await;
                let by_id: BTreeMap<_, _> = fetched.into_iter().map(|row| (id(&row), row)).collect();
                for row in &mut ranked {
                    let source = &by_id[&id(row)];
                    row[2] = source[1].clone();
                    row[3] = source[2].clone();
                }
            }
            samples.push(micros(start));
            assert_eq!(&ranked, expected, "{name} primary fetch");
        }
        results.push(json!({"filter": name, "timing": report(&samples)}));
    }
    results
}

fn rebuild_bytes(directory: &Path, generation: &Generation, overlay: &Overlay) -> usize {
    let files = generation.bundles.len();
    let affected: BTreeSet<_> = overlay.changed.iter().map(|iid| partition(iid, files)).collect();
    affected.into_iter().map(|shard| {
        let remaining = generation.rows.iter().filter(|(iid, _)| !overlay.changed.contains(*iid));
        let rows = remaining.chain(&overlay.rows)
            .filter_map(|(iid, row)| (partition(iid, files) == shard).then_some(row)).collect();
        Bundle::write(directory, &format!("replacement-{files}-{shard}"), rows, overlay.through).bytes
    }).sum()
}

async fn listing(store: &dyn ObjectStore, table: &TableRef) -> Vec<StoredObject> {
    let slug = TableSlug::for_unrecorded_table(table).unwrap();
    store.list(Some(&ObjectKey::new(slug.table_prefix()).unwrap())).await.unwrap()
}

fn write_sizes(objects: Vec<StoredObject>, previous: &BTreeSet<ObjectKey>) -> Json {
    let mut sizes = BTreeMap::new();
    for object in objects {
        if previous.contains(&object.key) { continue; }
        let path = object.key.as_str();
        let kind = if path.contains("/artefacts/") && path.contains("/search/") { "search" }
        else if path.contains("/data/") { "data" }
        else if path.contains("/meta/") { "metadata" }
        else { continue };
        let (count, bytes) = sizes.entry(kind).or_insert((0, 0));
        *count += 1;
        *bytes += object.size;
    }
    json!(sizes.into_iter().map(|(kind, (count, bytes))|
        (kind, json!({"files": count, "bytes": bytes}))).collect::<BTreeMap<_, _>>())
}

pub(super) async fn run(documents: i64, iterations: usize) {
    assert!(documents >= 4000 && iterations > 0);
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let (node, driver) = Node::open(bench_options(), bench_services(&clock)).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    let table = TableRef::new("public", "docs");
    let database = node.catalog().database("xtdb").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let directory = directory.path();
    let mut basis = commit(&mut session, vec![sql("CREATE SEARCH INDEX docs_body ON docs (body)")]).await;
    for start in (0..documents).step_by(4000) {
        let rows = (start..(start + 4000).min(documents)).map(|id| StructFields::from([
            ("_id".into(), Value::I64(id)),
            ("body".into(), Value::Utf8(body(id).into())),
            ("department".into(), Value::I64(id % 137)),
        ])).collect();
        basis = commit(&mut session, vec![TxOp::PutDocs { table: table.clone(),
            documents: Documents::new(rows, node.memory()).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None,
        }]).await;
        if (start + 4000) % 20_000 == 0 { flush(&node).await; }
    }
    flush(&node).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();
    let primary_tries = database.trie_catalog().current_tries(&table).len();
    let old_objects = listing(database.object_store().as_ref(), &table).await;
    let prefix_storage = write_sizes(old_objects.clone(), &BTreeSet::new());
    let old_keys = old_objects.into_iter().map(|object| object.key).collect();
    let valid = basis;
    let rows = snapshot_rows(&mut session, valid, basis, "").await;
    assert_eq!(rows.len(), documents as usize);
    let corpus = exhaustive_corpus(&rows);
    let rows = Arc::new(by_iid(rows));
    let (expected, sql_baseline) = baseline(&mut session, valid, basis, &corpus, iterations).await;

    let mut generations = Vec::new();
    for files in [5, 25, 100] {
        let start = Instant::now();
        let generation = Generation::build(directory, basis, valid, Arc::clone(&rows), files);
        let build_us = micros(start);
        let measurements = measure(&generation, &corpus, &expected, iterations);
        generations.push((generation, build_us, measurements));
    }

    let primary_fetch = primary_fetch(&mut session, &generations[0].0, &corpus, &expected, iterations).await;

    let after = commit(&mut session, vec![
        sql("INSERT INTO docs RECORDS {_id: 0, body: 'report approved needle audit audit audit', department: -1, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z', _valid_to: TIMESTAMP '1970-01-01T00:00:02Z'}"),
        sql("ERASE FROM docs WHERE _id = 101"),
        sql("INSERT INTO docs RECORDS {_id: 202, body: 'report approved needle audit', department: -2, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z'}"),
        sql("INSERT INTO docs RECORDS {_id: 305, body: NULL, department: -3, _valid_from: TIMESTAMP '1970-01-01T00:00:00Z'}"),
    ]).await;
    let touched = [0, 101, 202, 305];
    let after_corpus = exhaustive_corpus(&snapshot_rows(&mut session, valid, after, "").await);
    let (after_expected, _) = baseline(&mut session, valid, after, &after_corpus, 1).await;
    let base = &generations[0].0;
    assert!(Overlay::resolve(directory, &mut session, base, after, &touched, false).await.is_none());
    let start = Instant::now();
    let overlay = Overlay::resolve(directory, &mut session, base, after, &touched, true).await.unwrap();
    let overlay_us = micros(start);
    let reads = Arc::new(Mutex::new(Reads::default()));
    assert!(Search::open(base, None, after, &after_corpus, true, &reads).is_none());
    let pinned = Search::open(base, None, basis, &corpus, true, &reads).unwrap();
    let updated = Search::open(base, Some(&overlay), after, &after_corpus, true, &reads).unwrap();
    for ((_, filter), (before, after)) in FILTERS.into_iter().zip(expected.iter().zip(&after_expected)) {
        let matching = Query::parse(filter);
        let scoring = Query::parse("audit");
        assert_eq!(&pinned.ranked(&matching, &scoring).0, before);
        assert_eq!(&updated.ranked(&matching, &scoring).0, after);
    }
    flush(&node).await;
    database.compaction().unwrap().compact_all().await.unwrap();
    node.sync_all(Duration::from_secs(30), &CancellationToken::new()).await.unwrap();
    database.build_artefacts().await.unwrap();
    let churn_storage = write_sizes(listing(database.object_store().as_ref(), &table).await, &old_keys);
    let (after_compaction, _) = baseline(&mut session, valid, after, &after_corpus, 1).await;
    assert_eq!(after_compaction, after_expected);
    let mut layouts = Vec::new();
    for (generation, build_us, measurements) in &generations {
        let with_overlay = Search::open(generation, Some(&overlay), after, &after_corpus, true, &reads).unwrap();
        for ((_, filter), (before, after)) in FILTERS.into_iter().zip(expected.iter().zip(&after_expected)) {
            let matching = Query::parse(filter);
            let scoring = Query::parse("audit");
            assert_eq!(&pinned.ranked(&matching, &scoring).0, before);
            assert_eq!(&with_overlay.ranked(&matching, &scoring).0, after);
        }
        layouts.push(json!({
            "files": generation.bundles.len(),
            "build_us": build_us,
            "initial_search_bytes": generation.bundles.iter().map(|bundle| bundle.bytes).sum::<usize>(),
            "replace_affected_search_bytes": rebuild_bytes(directory, generation, &overlay),
            "queries": measurements,
        }));
    }
    println!("{}", json!({
        "documents": documents, "iterations": iterations, "primary_tries": primary_tries,
        "sql_baseline": sql_baseline, "warm_direct_with_primary_fetch": primary_fetch,
        "layouts": layouts, "range_block_bytes": RANGE_BLOCK_BYTES,
        "initial_primary_objects": prefix_storage, "new_primary_objects_after_churn": churn_storage,
        "overlay": {"touched_iids": touched.len(), "build_us": overlay_us, "search_bytes": overlay.bundle.bytes},
    }));
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();
}
