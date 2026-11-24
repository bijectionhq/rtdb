use std::{collections::{BTreeMap, btree_map::Entry}, ops::Range, sync::Arc};

use arrow_array::{Array, StructArray, cast::AsArray, types::UInt32Type};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::cow_tree::{self, Edit, PageGeneration, PageStore, RootRef, Row, Summary};
use rtdb_text::{Query, RankScorer};
use rtdb_types::{Anomaly, Result};

type RunId = [u8; 32];

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct TermStats {
    pub doc_freq: u32,
    pub max_tf: u32,
    pub min_len: u32,
    pub unit_bound: f64,
    pub first_window: u32,
    pub last_window: u32,
}

impl TermStats {
    fn encode(self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(28);
        bytes.extend(self.doc_freq.to_be_bytes());
        bytes.extend(self.max_tf.to_be_bytes());
        bytes.extend(self.min_len.to_be_bytes());
        bytes.extend(self.unit_bound.to_be_bytes());
        bytes.extend(self.first_window.to_be_bytes());
        bytes.extend(self.last_window.to_be_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; 28] = bytes.try_into().map_err(|_| invalid("Invalid term directory row length"))?;
        let word = |at| u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
        let stats = Self { doc_freq: word(0), max_tf: word(4), min_len: word(8),
            unit_bound: f64::from_be_bytes(bytes[12..20].try_into().unwrap()), first_window: word(20), last_window: word(24) };
        if stats.doc_freq == 0 || stats.max_tf == 0 || stats.min_len == 0 || !stats.unit_bound.is_finite()
            || stats.unit_bound <= 0.0 || stats.first_window > stats.last_window {
            return Err(invalid("Invalid term directory row statistics"));
        }
        Ok(stats)
    }
}

#[derive(Default)]
pub(crate) struct TermSummaries {
    pages: Vec<(StructArray, StructArray)>,
    _charge: Option<Arc<MemoryReservation>>,
}

impl TermSummaries {
    pub fn push(&mut self, terms: StructArray, nodes: StructArray) { self.pages.push((terms, nodes)); }

    pub fn retain(&mut self, mut charge: Option<Arc<MemoryReservation>>) -> Result<()> {
        if let Some(charge) = &mut charge {
            let charge = Arc::get_mut(charge).expect("Search writer owns its allocation");
            let mut bytes = self.pages.first().map_or(0, |(terms, _)| terms.get_array_memory_size());
            for (_, nodes) in &self.pages {
                bytes = bytes.saturating_add(nodes.get_array_memory_size()).saturating_add(512);
            }
            if charge.bytes() >= bytes { charge.shrink_to(bytes)?; }
            else { charge.grow_to(bytes)?; }
        }
        self._charge = charge;
        Ok(())
    }

    pub fn visit_page(terms: &StructArray, nodes: &StructArray,
        visit: &mut impl FnMut(u32, &str, TermStats) -> Result<()>) -> Result<()> {
        if terms.len() != nodes.len() { return Err(invalid("Posting terms and rank summaries differ in length")); }
        let fields = terms.column(0).as_primitive::<UInt32Type>();
        let names = terms.column(1).as_string::<i32>();
        let frequencies = terms.column(4).as_primitive::<UInt32Type>();
        for index in 0..terms.len() {
            if fields.is_null(index) || names.is_null(index) || frequencies.is_null(index) {
                return Err(invalid("Posting term summary is incomplete"));
            }
            visit(fields.value(index), names.value(index), crate::file::term_stats(nodes, index, frequencies.value(index))?)?;
        }
        Ok(())
    }

    pub fn visit(&self, mut visit: impl FnMut(u32, &str, TermStats) -> Result<()>) -> Result<()> {
        for (terms, nodes) in &self.pages { Self::visit_page(terms, nodes, &mut visit)?; }
        Ok(())
    }
}

fn term_prefix(field: u32, term: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(5 + term.len());
    key.extend(field.to_be_bytes());
    key.extend(term.as_bytes());
    key.push(0);
    key
}

fn term_range(field: u32, term: &str) -> Range<Vec<u8>> {
    let start = term_prefix(field, term);
    let mut end = start.clone();
    *end.last_mut().unwrap() = 1;
    start..end
}

struct MaxUnitBound;

impl Summary for MaxUnitBound {
    fn leaf(&self, rows: &[Row]) -> Vec<u8> {
        rows.iter().map(|row| TermStats::decode(&row.value).map_or(f64::INFINITY, |stats| stats.unit_bound))
            .fold(0.0, f64::max).to_be_bytes().to_vec()
    }

    fn branch(&self, children: &[&[u8]]) -> Vec<u8> {
        children.iter().map(|bytes| (*bytes).try_into().map_or(f64::INFINITY, f64::from_be_bytes))
            .fold(0.0, f64::max).to_be_bytes().to_vec()
    }
}

pub(crate) struct TermEdits {
    edits: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    charge: Arc<MemoryReservation>,
    retained: usize,
}

impl TermEdits {
    pub fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { edits: BTreeMap::new(), charge: memory.reserve(4096)?, retained: 0 })
    }

    pub fn record(&mut self, field: u32, term: &str, run: RunId, stats: Option<TermStats>) -> Result<()> {
        if term.is_empty() || term.contains('\0') { return Err(invalid("Invalid analyzed term")); }
        self.retained = self.retained.saturating_add(176 + term.len());
        grow(&mut self.charge, self.retained)?;
        let mut key = term_prefix(field, term);
        key.extend(run);
        let value = stats.map(TermStats::encode);
        if let Some(value) = &value { TermStats::decode(value)?; }
        if self.edits.insert(key, value).is_some() { return Err(invalid("A run repeats a term summary")); }
        Ok(())
    }

    pub async fn apply(self, store: &dyn PageStore, base: Option<&RootRef>, memory: &Arc<MemoryBudget>)
        -> Result<Option<RootRef>> {
        let Self { edits, charge: _charge, .. } = self;
        let edits = edits.into_iter().map(|(key, value)| match value {
            Some(value) => Edit::Put(Row { key, value }),
            None => Edit::Delete(key),
        }).collect();
        let generation = PageGeneration::Attempt(uuid::Uuid::new_v4());
        let (root, _) = cow_tree::apply_with_generation(store, base, edits, &MaxUnitBound, memory, Some(generation)).await?;
        Ok(root)
    }
}

pub(crate) struct TermRoutes {
    scores: BTreeMap<usize, f64>,
    _charge: Arc<MemoryReservation>,
}

impl TermRoutes {
    pub fn iter(&self) -> impl Iterator<Item = (usize, f64)> + '_ {
        self.scores.iter().map(|(&index, &score)| (index, score))
    }
}

pub(crate) async fn read_routes(store: &dyn PageStore, root: Option<&RootRef>, field: u32, matching: &Query,
    scoring: &RankScorer, runs: &BTreeMap<RunId, usize>, memory: &Arc<MemoryBudget>) -> Result<TermRoutes> {
    let mut routes = TermRoutes { scores: BTreeMap::new(), _charge: memory.reserve(0)? };
    let Some(root) = root else { return Ok(routes); };
    let mut bounds_charge = memory.reserve(0)?;
    let mut bounds = BTreeMap::<&str, TermRows>::new();
    for term in matching.terms() {
        let rows = read_term(store, root, field, term, runs, memory).await?;
        if rows.stats.is_empty() { return Ok(routes); }
        admit_map_entry(&mut bounds_charge, bounds.len(), 128)?;
        bounds.insert(term.as_str(), rows);
    }
    let Some(rarest) = bounds.values().min_by_key(|rows| {
        rows.stats.values().fold(0u64, |count, stats| count.saturating_add(u64::from(stats.doc_freq)))
    }) else { return Ok(routes); };
    for run in rarest.stats.keys().filter(|&run| bounds.values().all(|rows| rows.stats.contains_key(run))) {
        admit_map_entry(&mut routes._charge, routes.scores.len(), 64)?;
        routes.scores.insert(*run, 0.0);
    }
    if routes.scores.is_empty() { return Ok(routes); }
    let scorer = scoring.scorer();
    let scoring_terms = scorer.query().terms();
    let unit_scores = scorer.uses_unit_term_scores();
    bounds.retain(|term, _| scoring_terms.iter().any(|scored| scored.as_str() == *term));
    for (index, term) in scoring_terms.iter().enumerate() {
        let rows = match bounds.remove(term.as_str()) {
            Some(rows) => rows,
            None => read_term(store, root, field, term, runs, memory).await?,
        };
        for (run, stats) in &rows.stats {
            if let Some(score) = routes.scores.get_mut(run) {
                *score += if unit_scores { stats.unit_bound }
                    else { scoring.term_bound(index, stats.max_tf, stats.min_len) };
            }
        }
    }
    Ok(routes)
}

struct TermRows {
    stats: BTreeMap<usize, TermStats>,
    _charge: Arc<MemoryReservation>,
}

async fn read_term(store: &dyn PageStore, root: &RootRef, field: u32, term: &str, runs: &BTreeMap<RunId, usize>,
    memory: &Arc<MemoryBudget>) -> Result<TermRows> {
    let range = term_range(field, term);
    let start = range.start.clone();
    let mut charge = memory.reserve(0)?;
    let mut stats = BTreeMap::new();
    cow_tree::visit(store, root, range, &|_| true, memory, |row| {
        let run: RunId = row.key.strip_prefix(start.as_slice()).and_then(|bytes| bytes.try_into().ok())
            .ok_or_else(|| invalid("Invalid term directory row key"))?;
        let index = runs.get(&run).copied().ok_or_else(|| invalid("Term directory names an inactive run"))?;
        let summary = TermStats::decode(&row.value)?;
        let entries = stats.len();
        let Entry::Vacant(entry) = stats.entry(index) else {
            return Err(invalid("Term directory repeats a run for a term"));
        };
        admit_map_entry(&mut charge, entries, 192)?;
        entry.insert(summary);
        Ok(true)
    }).await?;
    Ok(TermRows { stats, _charge: charge })
}

fn admit_map_entry(charge: &mut Arc<MemoryReservation>, entries: usize, bytes_per_entry: usize) -> Result<()> {
    grow(charge, 512usize.saturating_add(entries.saturating_add(1).saturating_mul(bytes_per_entry)))
}

fn grow(charge: &mut Arc<MemoryReservation>, bytes: usize) -> Result<()> {
    let charge = Arc::get_mut(charge).expect("Term directory owns its scratch reservation");
    if charge.bytes() < bytes { charge.grow_to(bytes)?; }
    Ok(())
}

fn invalid(message: &str) -> Anomaly { Anomaly::fault("rtdb.search/term-directory", message) }

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rtdb_format::cow_tree::PageDigest;
    use rtdb_text::{Corpus, Scorer, unit_term_score};

    #[derive(Default)]
    struct Pages(std::sync::Mutex<BTreeMap<PageDigest, Bytes>>);

    #[async_trait::async_trait]
    impl PageStore for Pages {
        async fn get(&self, digest: PageDigest, _length: u64) -> Result<Bytes> {
            self.0.lock().unwrap().get(&digest).cloned().ok_or_else(|| invalid("Missing test page"))
        }

        async fn put(&self, digest: PageDigest, bytes: Bytes) -> Result<()> {
            self.0.lock().unwrap().insert(digest, bytes);
            Ok(())
        }
    }

    fn stats(doc_freq: u32, max_tf: u32, min_len: u32) -> TermStats {
        TermStats { doc_freq, max_tf, min_len, unit_bound: unit_term_score(max_tf), first_window: 0, last_window: 2 }
    }

    #[tokio::test]
    async fn required_term_intersection_preserves_every_matching_run_and_bounds_its_exact_scores() {
        let memory = MemoryBudget::new(8 << 20);
        let pages = Pages::default();
        let [first, second, prefix_only] = [[1; 32], [2; 32], [3; 32]];
        let mut edits = TermEdits::new(&memory).unwrap();
        for (field, term, run, summary) in [
            (0, "audit", first, stats(2, 2, 2)),
            (0, "report", first, stats(2, 1, 2)),
            (0, "memo", first, stats(1, 1, 3)),
            (0, "audit", second, stats(1, 1, 3)),
            (0, "report", second, stats(1, 2, 3)),
            (0, "audits", prefix_only, stats(1, 1, 2)),
            (1, "audit", prefix_only, stats(1, 1, 2)),
            (0, "report", prefix_only, stats(1, 1, 2)),
        ] {
            edits.record(field, term, run, Some(summary)).unwrap();
        }
        let root = edits.apply(&pages, None, &memory).await.unwrap().unwrap();
        assert_eq!(root.summary, unit_term_score(2).to_be_bytes());
        let matching = Query::parse("\"audit report\"");
        let scoring = Query::parse("memo report audit");
        let runs = BTreeMap::from([(first, 0), (second, 1), (prefix_only, 2)]);
        let corpus = Corpus { documents: 12, tokens: 60, frequencies: BTreeMap::from([
            ("audit".to_owned(), 4), ("report".to_owned(), 5), ("memo".to_owned(), 1),
        ]) };
        for corpus in [None, Some(&corpus)] {
            let scorer = Arc::new(Scorer::new(scoring.clone(), corpus));
            let ranking = scorer.for_ranking().unwrap();
            let routes = read_routes(&pages, Some(&root), 0, &matching, &ranking, &runs, &memory).await.unwrap();
            let bounds: BTreeMap<_, _> = routes.iter().collect();
            assert!(!bounds.contains_key(&runs[&prefix_only]));
            for (run, document) in [(first, "audit audit report memo"), (first, "audit report"), (second, "audit report report")] {
                let tokens = rtdb_text::Analyzer::Simple.tokens(document);
                assert!(matching.matches(&tokens));
                assert!(scorer.bm25(&tokens) <= bounds[&runs[&run]]);
            }
        }
    }

    #[tokio::test]
    async fn replacement_removes_retired_term_rows_while_the_old_root_keeps_them() {
        let memory = MemoryBudget::new(8 << 20);
        let pages = Pages::default();
        let [old, kept, replacement] = [[1; 32], [2; 32], [3; 32]];
        let mut edits = TermEdits::new(&memory).unwrap();
        edits.record(0, "report", old, Some(stats(1, 1, 1))).unwrap();
        edits.record(0, "memo", kept, Some(stats(1, 1, 1))).unwrap();
        let before = edits.apply(&pages, None, &memory).await.unwrap().unwrap();
        let mut edits = TermEdits::new(&memory).unwrap();
        edits.record(0, "report", old, None).unwrap();
        edits.record(0, "report", replacement, Some(stats(1, 2, 2))).unwrap();
        let after = edits.apply(&pages, Some(&before), &memory).await.unwrap().unwrap();
        assert_eq!(read_term(&pages, &before, 0, "report", &BTreeMap::from([(old, 0), (kept, 1)]), &memory)
            .await.unwrap().stats.keys().copied().collect::<Vec<_>>(), [0]);
        let active = BTreeMap::from([(replacement, 2), (kept, 1)]);
        assert_eq!(read_term(&pages, &after, 0, "report", &active, &memory).await.unwrap().stats.keys().copied().collect::<Vec<_>>(), [2]);
        assert_eq!(read_term(&pages, &after, 0, "memo", &active, &memory).await.unwrap().stats.keys().copied().collect::<Vec<_>>(), [1]);

        pages.0.lock().unwrap().insert(after.digest, Bytes::from_static(b"corrupt"));
        assert!(read_term(&pages, &after, 0, "report", &active, &memory).await.is_err());
    }

    #[tokio::test]
    async fn rebuilding_identical_terms_survives_a_sweep_of_the_previous_pages() {
        let memory = MemoryBudget::new(8 << 20);
        let pages = Pages::default();
        let run = [1; 32];
        let rebuild = || {
            let mut edits = TermEdits::new(&memory).unwrap();
            edits.record(0, "report", run, Some(stats(1, 1, 1))).unwrap();
            edits.apply(&pages, None, &memory)
        };

        let abandoned = rebuild().await.unwrap().unwrap();
        let sweep = cow_tree::reachable(&pages, &abandoned).await.unwrap();
        let published = rebuild().await.unwrap().unwrap();
        for digest in sweep {
            pages.0.lock().unwrap().remove(&digest).unwrap();
        }

        let routes = read_term(&pages, &published, 0, "report", &BTreeMap::from([(run, 0)]), &memory).await.unwrap();
        assert_eq!(routes.stats.keys().copied().collect::<Vec<_>>(), [0]);
    }
}
