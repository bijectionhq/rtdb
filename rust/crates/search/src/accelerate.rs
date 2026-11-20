use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_format::data::StoredOperation;
use rtdb_index::{Snapshot, TrieState};
use rtdb_plan::{BinaryOp, Expr};
use rtdb_query::{MatchAnswers, ScanAccelerator, ScanRequest};
use rtdb_storage::ObjectKey;
use rtdb_text::Query;
use rtdb_types::{Iid, Result, TemporalBounds, Timestamp, Value};

use crate::{KIND, SearchDefinition, SearchFile, runtime::{Suspect, TrieFile}};

/// Narrows a scan with a `MATCH` predicate to the IIDs whose events hold the query's terms, ranks it, and answers its MATCH, from search files (fts.allium: the query plan).
pub struct SearchAccelerator {
    /// Above this many candidates the scan runs unnarrowed: reading every page costs less than resolving that many point lookups.
    pub max_candidates: usize,
    /// Above this many bytes of tries the lane should have indexed but has not, the scan uses no search file rather than build them all itself.
    pub max_unindexed_bytes: i64,
    runtime: Arc<crate::SearchRuntime>,
}

impl SearchAccelerator {
    pub fn with_runtime(runtime: Arc<crate::SearchRuntime>) -> Self { Self { max_candidates: 100_000, max_unindexed_bytes: 64 * 1024 * 1024, runtime } }
}

impl Default for SearchAccelerator {
    fn default() -> Self { Self::with_runtime(crate::SearchRuntime::new(256 << 20)) }
}

/// The column and query of each MATCH conjunct, whether a predicate calls it or a column the scan evaluates holds it.
fn match_conjuncts<'a>(scan: &'a rtdb_plan::Scan, expression: &'a Expr, found: &mut Vec<(&'a str, &'a Expr)>) {
    let name = |id: &rtdb_plan::ColumnId| scan.columns.iter().find(|field| field.id == *id).map(|field| field.name.as_str());
    match expression {
        Expr::Binary { op: BinaryOp::And, left, right } => { match_conjuncts(scan, left, found); match_conjuncts(scan, right, found); }
        Expr::Call { name: function, args, .. } if function.eq_ignore_ascii_case("match") && args.len() == 2 => {
            if let Some(column) = match &args[0] { Expr::Column { id, .. } => name(id), _ => None } { found.push((column, &args[1])); }
        }
        Expr::Column { id, .. } => if let Some(evaluated) = scan.matches.iter().find(|evaluated| evaluated.id == *id) { found.push((&evaluated.column, &evaluated.query)); },
        _ => {}
    }
}

/// An event can be the visible version only if its own valid and system interval reaches the query's, and its recency is past the query's earliest instant.
/// Resolution only ever narrows an event's interval, so this rules candidates out and never in.
fn may_be_visible(valid_from: i64, valid_to: i64, system_from: i64, recency: i64, bounds: TemporalBounds) -> bool {
    let (valid, system) = (bounds.valid_time, bounds.system_time);
    valid_from < valid.upper().0 && valid_to > valid.lower().0 && system_from < system.upper().0 && valid.lower().0.min(system.lower().0) < recency
}

#[async_trait]
impl ScanAccelerator for SearchAccelerator {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        let scan = request.scan;
        let mut conjuncts = Vec::new();
        for predicate in &scan.predicates { match_conjuncts(scan, predicate, &mut conjuncts); }
        let mut chosen = None;
        for (column, query) in conjuncts {
            let Some(Value::Utf8(text)) = request.constants.constant(query).await? else { continue };
            if let Some(definition) = self.definition(request, column).await? { chosen = Some((column, Query::parse(&text), definition)); break; }
        }
        let Some((column, query, definition)) = chosen else { return Ok(None) };
        if query.terms().is_empty() { return Ok(Some(BTreeSet::new())); }
        self.register_corpora(request, column, &definition).await?;
        if let Some(top_k) = &scan.top_k {
            let ranked = scan.columns.iter().find(|field| field.id == top_k.column).is_some_and(|field| field.name == column);
            if ranked { if let Some(candidates) = self.top_k(request, top_k, column, &query, &definition).await? { return Ok(Some(candidates)); } }
        }
        let TrieFiles::Complete(files) = self.trie_files(request, &definition).await? else { return Ok(None) };
        let cutter = Cutter::new(&files, request, &self.runtime);
        let mut candidates = BTreeSet::new();
        for (own, trie) in files.iter().enumerate() {
            for doc in trie.file.matches(column, &query, request.bounds)? {
                if !matches!(cutter.judge(own, doc)?, Judged::Hidden) { candidates.insert(Iid(doc.iid)); }
            }
            if candidates.len() > self.max_candidates { return Ok(None); }
        }
        candidates.extend(self.live_candidates(request, column, &query)?);
        Ok((candidates.len() <= self.max_candidates).then_some(candidates))
    }

    async fn match_answers(&self, request: &ScanRequest<'_>, column: &str, query: &str) -> Result<Option<MatchAnswers>> {
        let Some(definition) = self.definition(request, column).await? else { return Ok(None) };
        let files = match self.trie_files(request, &definition).await? { TrieFiles::Complete(files) | TrieFiles::Recorded(files) => files };
        let query = Query::parse(query);
        let mut answers = MatchAnswers::default();
        for trie in files { answers.insert(&trie.data, self.runtime.match_set(request.database, &trie, column, &query)?); }
        Ok((!answers.is_empty()).then_some(answers))
    }
}

/// A partition's search files: one for every live trie, or only the recorded ones where building the rest would exceed max_unindexed_bytes.
enum TrieFiles { Complete(Vec<TrieFile>), Recorded(Vec<TrieFile>) }

/// Whether the scan's other sources leave a file's document visible in the query's bounds, and whether that is exact.
enum Judged { Hidden, Visible { exact: bool } }

/// Cuts a file's documents by the events of the same entity in the partition's other tries and its live segments (fts.allium: CutByLaterEvents).
struct Cutter<'a> { files: &'a [TrieFile], segments: Vec<&'a Arc<rtdb_index::LiveTable>>, runtime: &'a crate::SearchRuntime, database: &'a str, bounds: TemporalBounds }

impl<'a> Cutter<'a> {
    fn new(files: &'a [TrieFile], request: &'a ScanRequest<'_>, runtime: &'a crate::SearchRuntime) -> Self {
        Self { files, segments: request.snapshot.iter_segments(&request.scan.table.name.table).collect(), runtime, database: request.database, bounds: request.bounds }
    }

    fn judge(&self, own: usize, doc: crate::DocMatch) -> Result<Judged> {
        let mut rectangles: Vec<[i64; 4]> = self.files[own].file.rectangles(doc.doc).collect();
        let mut tied = false;
        for (index, other) in self.files.iter().enumerate() {
            if index == own || !covers(other.trie.part(), &doc.iid) { continue; }
            tied |= cut(&mut rectangles, other.file.events(&doc.iid)?);
        }
        for segment in &self.segments { tied |= cut(&mut rectangles, self.runtime.live_events(self.database, segment, &doc.iid)?); }
        if !rectangles.iter().any(|rectangle| crate::file::meets(*rectangle, self.bounds)) { return Ok(Judged::Hidden); }
        Ok(Judged::Visible { exact: !tied })
    }
}

/// Whether a trie's IID path covers an entity: its part is the entity's leading base-four digits.
fn covers(part: &[u8], iid: &[u8; 16]) -> bool {
    part.iter().enumerate().all(|(depth, digit)| (iid[depth / 4] >> (6 - 2 * (depth % 4))) & 3 == *digit)
}

/// Removes from rectangles what another source's events of the entity take from them, and returns whether any event shared a rectangle's system-from.
/// A later put or delete over a valid interval takes that interval from its system time on; a later erase takes everything; an earlier event takes nothing.
/// An event at the same instant takes nothing either: the scan orders such events itself, so the document is left, and marked inexact.
fn cut(rectangles: &mut Vec<[i64; 4]>, events: Vec<crate::file::KeyedEvent>) -> bool {
    let mut tied = false;
    for event in events {
        let mut kept = Vec::with_capacity(rectangles.len());
        for rectangle in rectangles.drain(..) {
            let [valid_from, valid_to, system_from, system_to] = rectangle;
            tied |= event.system_from == system_from;
            if event.system_from <= system_from || event.system_from >= system_to { kept.push(rectangle); continue; }
            if event.op == rtdb_types::EventOp::Erase { continue; }
            let (cut_from, cut_to) = (event.valid_from.max(valid_from), event.valid_to.min(valid_to));
            if cut_from >= cut_to { kept.push(rectangle); continue; }
            if valid_from < cut_from { kept.push([valid_from, cut_from, system_from, system_to]); }
            if cut_to < valid_to { kept.push([cut_to, valid_to, system_from, system_to]); }
            kept.push([cut_from, cut_to, system_from, event.system_from]);
        }
        *rectangles = kept;
    }
    tied
}

impl SearchAccelerator {
    async fn definition(&self, request: &ScanRequest<'_>, column: &str) -> Result<Option<SearchDefinition>> {
        let definitions = self.runtime.definitions(request.database, request.partitions, request.storage, request.memory, request.current_time, request.cancellation).await?;
        let table = &request.scan.table.name.table;
        Ok(definitions.iter().find(|definition| &definition.table == table && definition.columns.iter().any(|indexed| indexed == column)).cloned())
    }

    /// The key of a trie's recorded search file, if it has one.
    fn recorded_key(snapshot: &Snapshot, table: &rtdb_types::TableRef, trie: &rtdb_index::CatalogTrie, definition: &SearchDefinition) -> Result<Option<ObjectKey>> {
        let artefact_id = definition.artefact_id();
        let (Some(entry), Some(_)) = (snapshot.table_entry(table), trie.artefact(KIND, &artefact_id, definition.version)) else { return Ok(None) };
        Ok(Some(ObjectKey::new(entry.slug.artefact_key(&trie.key, KIND, &artefact_id, definition.version)?)?))
    }

    /// A trie's recorded search file, or `None` where it cannot be read; one that is missing or corrupt is reported to the lane.
    async fn recorded(&self, request: &ScanRequest<'_>, key: ObjectKey, trie: &rtdb_index::CatalogTrie, definition: &SearchDefinition) -> Result<Option<(ObjectKey, Arc<SearchFile>)>> {
        let (table, artefact_id) = (&request.scan.table.name.table, definition.artefact_id());
        match self.runtime.file(request.database, request.storage, &key, &definition.columns).await {
            Ok(file) => Ok(Some((key, file))),
            Err(error) => {
                if error.needs_repair() { self.runtime.report(Suspect { database: request.database.to_string(), table: table.clone(), trie: trie.key.clone(), artefact_id, version: definition.version }); }
                Ok(None)
            }
        }
    }

    /// The search files of the partition's live tries that can hold a visible event: every such trie's, its recorded one or one this node builds from its data file, unless the tries the lane should have indexed but has not exceed max_unindexed_bytes.
    /// The lane never indexes level 0, the partial level-1 current trie, or history for a definition that excludes it, so those do not count.
    async fn trie_files(&self, request: &ScanRequest<'_>, definition: &SearchDefinition) -> Result<TrieFiles> {
        let (snapshot, table) = (request.snapshot, &request.scan.table.name.table);
        let Some(entry) = snapshot.table_entry(table) else { return Ok(TrieFiles::Complete(Vec::new())) };
        let mut files = Vec::new();
        let mut unbuilt = Vec::new();
        let mut unindexed = 0i64;
        let earliest = request.bounds.valid_time.lower().0.min(request.bounds.system_time.lower().0);
        // A trie's recency bounds its rows' recency above, so a trie at or before the query's earliest instant holds nothing it can see.
        let visible = |trie: &&rtdb_index::CatalogTrie| trie.key.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest);
        for trie in snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).filter(visible) {
            let data = ObjectKey::new(entry.slug.data_key(&trie.key))?;
            let recorded = match Self::recorded_key(snapshot, table, trie, definition)? { Some(key) => self.recorded(request, key, trie, definition).await?, None => None };
            if let Some((key, file)) = recorded { files.push(TrieFile { trie: trie.key.clone(), data, identity: key.as_str().to_string(), file }); continue; }
            let by_policy = trie.key.level() == 0 || (trie.key.level() == 1 && trie.key.recency().is_none() && trie.data_file_size < snapshot.trie_catalog().file_size_target())
                || (!definition.history && trie.key.recency().is_some());
            if !by_policy { unindexed = unindexed.saturating_add(trie.data_file_size); }
            unbuilt.push((trie.key.clone(), data));
        }
        if unindexed > self.max_unindexed_bytes { return Ok(TrieFiles::Recorded(files)); }
        for (trie, data) in unbuilt { files.push(self.runtime.built(request.database, request.storage, &trie, &data, &definition.columns, request.memory, request.cancellation).await?); }
        Ok(TrieFiles::Complete(files))
    }

    /// Registers the table's current-side recorded search files in every partition, so the corpus a score fixes is the same whichever partition, or scan, fixes it first.
    async fn register_corpora(&self, request: &ScanRequest<'_>, column: &str, definition: &SearchDefinition) -> Result<()> {
        let table = &request.scan.table.name.table;
        for snapshot in request.partitions {
            for trie in snapshot.trie_catalog().current_tries(table) {
                if trie.state != TrieState::Live || trie.key.recency().is_some() { continue; }
                let Some(key) = Self::recorded_key(snapshot, table, trie, definition)? else { continue };
                let name = format!("{key}#{column}");
                if request.corpora.holds(&name) { continue; }
                let Some((_, file)) = self.recorded(request, key, trie, definition).await? else { continue };
                request.corpora.register(name, Arc::new(FileCorpus { file, column: column.to_string() }));
            }
        }
        Ok(())
    }
    fn live_candidates(&self, request: &ScanRequest<'_>, column: &str, query: &Query) -> Result<BTreeSet<Iid>> {
        let mut candidates = BTreeSet::new();
        for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
            for row in self.runtime.live_rows(request.database, segment, column, query)? {
                let source = segment.row(row)?;
                let header = source.header()?;
                if !may_be_visible(header.valid_time.lower().0, header.valid_time.upper().0, header.system_from.0, Timestamp::MAX.0, request.bounds) { continue; }
                if !query.phrases().is_empty() {
                    let event = source.event()?;
                    let StoredOperation::Put(document) = &event.operation else { continue };
                    let Some(Value::Utf8(text)) = document.get(column) else { continue };
                    if !query.matches(&rtdb_text::tokens(text)) { continue; }
                }
                candidates.insert(header.iid);
            }
        }
        Ok(candidates)
    }
}

/// A row's ranking key: its score, then its IID, so that ties sort deterministically.
fn descending(left: &(f64, Iid), right: &(f64, Iid)) -> std::cmp::Ordering { right.0.total_cmp(&left.0).then_with(|| left.1.cmp(&right.1)) }

/// A scored event waiting to be handed out, best score first, and whether its visibility is exact.
struct Pending(f64, Iid, bool);
impl PartialEq for Pending { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Pending {}
impl PartialOrd for Pending { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for Pending { fn cmp(&self, other: &Self) -> std::cmp::Ordering { descending(&(other.0, other.1), &(self.0, self.1)) } }

/// The IIDs of the matching events that may be visible, each at its best event's score, best first.
/// Search files are expanded a batch of windows at a time, only while one may hold an event above every event already scored.
struct Candidates<'a> { files: Vec<(usize, crate::file::Ranked)>, cutter: Cutter<'a>, pending: std::collections::BinaryHeap<Pending>, seen: BTreeSet<Iid>, scored: usize }

impl Candidates<'_> {
    fn push(&mut self, iid: Iid, score: f64, exact: bool) { self.scored += 1; self.pending.push(Pending(score, iid, exact)); }
    fn drop_seen(&mut self) { while self.pending.peek().is_some_and(|top| self.seen.contains(&top.1)) { self.pending.pop(); } }
    /// The file whose next windows may hold the best event not yet scored.
    fn best_file(&self) -> Option<(usize, f64)> {
        self.files.iter().enumerate().filter_map(|(index, (_, file))| file.bound().map(|bound| (index, bound))).max_by(|left, right| left.1.total_cmp(&right.1))
    }
    /// No IID not yet handed out has a matching event scoring above this.
    fn frontier(&mut self) -> f64 {
        self.drop_seen();
        let pending = self.pending.peek().map_or(f64::NEG_INFINITY, |top| top.0);
        self.best_file().map_or(pending, |(_, bound)| bound.max(pending))
    }
    /// The next IID, best first, with its best event's score and whether that event is exactly visible.
    fn next(&mut self) -> Result<Option<(f64, Iid, bool)>> {
        loop {
            self.drop_seen();
            match (self.pending.peek().map(|top| top.0), self.best_file()) {
                (pending, Some((index, bound))) if pending.is_none_or(|score| bound > score) => {
                    let own = self.files[index].0;
                    for (doc, score) in self.files[index].1.expand()? {
                        match self.cutter.judge(own, doc)? { Judged::Hidden => {}, Judged::Visible { exact } => self.push(Iid(doc.iid), score, exact) }
                    }
                }
                _ => {
                    let Some(Pending(score, iid, exact)) = self.pending.pop() else { return Ok(None) };
                    self.seen.insert(iid);
                    return Ok(Some((score, iid, exact)));
                }
            }
        }
    }
}

impl SearchAccelerator {
    /// The IIDs that can hold one of the `limit` best-scoring visible rows, or `None` to narrow as an unranked MATCH does.
    /// Every matching event is scored exactly, and an IID's best event bounds every row it can resolve to.
    /// IIDs are verified best bound first, in batches, until `limit` verified rows outscore every bound still unverified; a search file's events are scored only as far as that takes.
    async fn top_k(&self, request: &ScanRequest<'_>, top_k: &rtdb_plan::ScanTopK, column: &str, matching: &Query, definition: &SearchDefinition) -> Result<Option<BTreeSet<Iid>>> {
        let Some(Value::Utf8(text)) = request.constants.constant(&top_k.query).await? else { return Ok(None) };
        let limit = match request.constants.constant(&top_k.limit).await.ok().flatten() {
            Some(Value::I64(limit)) => limit, Some(Value::I32(limit)) => i64::from(limit), Some(Value::I16(limit)) => i64::from(limit), _ => return Ok(None),
        };
        let Ok(limit) = usize::try_from(limit) else { return Ok(None) };
        if limit == 0 { return Ok(Some(BTreeSet::new())); }
        let TrieFiles::Complete(files) = self.trie_files(request, definition).await? else { return Ok(None) };
        let scoring = Query::parse(&text);
        let corpus = rtdb_expr::CorpusLookup::corpus(request.corpora, &text);
        let ranked = files.iter().enumerate().map(|(own, trie)| Ok((own, trie.file.ranked(column, matching, &scoring, corpus.as_ref(), request.bounds)?))).collect::<Result<Vec<_>>>()?;
        let mut candidates = Candidates { files: ranked, cutter: Cutter::new(&files, request, &self.runtime), pending: std::collections::BinaryHeap::new(), seen: BTreeSet::new(), scored: 0 };
        // The scan resolves exactly what the index decides only when MATCH is all it filters by; any other predicate is the verifier's to apply.
        let trusted = request.scan.predicates.iter().all(|predicate| match predicate {
            Expr::Column { id, .. } => request.scan.matches.iter().any(|evaluated| evaluated.id == *id),
            Expr::Call { name, .. } => name.eq_ignore_ascii_case("match"),
            _ => false,
        });
        for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
            for row in self.runtime.live_rows(request.database, segment, column, matching)? {
                let event = segment.row(row)?.event()?;
                if !may_be_visible(event.valid_time.lower().0, event.valid_time.upper().0, event.system_from.0, Timestamp::MAX.0, request.bounds) { continue; }
                let StoredOperation::Put(document) = &event.operation else { continue };
                let Some(Value::Utf8(value)) = document.get(column) else { continue };
                let tokens = rtdb_text::tokens(value);
                if matching.matches(&tokens) { candidates.push(event.iid, rtdb_text::bm25(&scoring, &tokens, corpus.as_ref()), false); }
            }
        }
        let mut verified: Vec<(f64, Iid)> = Vec::new();
        let batch = limit.saturating_mul(2).max(64);
        let mut rounds = 0;
        loop {
            if verified.len() >= limit {
                verified.sort_by(descending);
                if verified[limit - 1].0 > candidates.frontier() { break; }
            }
            let mut iids = BTreeSet::new();
            let mut handed = 0;
            while handed < batch {
                let Some((score, iid, exact)) = candidates.next()? else { break };
                handed += 1;
                // An exactly visible event's row is in the scan's output with its score, so it needs no verifying.
                if exact && trusted { verified.push((score, iid)); } else { iids.insert(iid); }
            }
            if candidates.scored > self.max_candidates { return Ok(None); }
            if handed == 0 { break; }
            if iids.is_empty() { continue; }
            rounds += 1;
            if rounds > 16 { return Ok(None); }
            for (iid, value) in request.verifier.visible(iids, top_k.column).await? {
                let Value::Utf8(value) = value else { continue };
                verified.push((rtdb_text::bm25(&scoring, &rtdb_text::tokens(&value), corpus.as_ref()), iid));
            }
        }
        verified.sort_by(descending);
        let kth = verified.get(limit - 1).map_or(f64::NEG_INFINITY, |(score, _)| *score);
        Ok(Some(verified.into_iter().filter(|(score, _)| *score >= kth).map(|(_, iid)| iid).collect()))
    }
}

/// A current-side search file a scan opened, counting its column's corpus for whatever terms a score asks about.
struct FileCorpus { file: Arc<SearchFile>, column: String }

impl rtdb_query::CorpusSource for FileCorpus {
    fn corpus(&self, query: &Query) -> Result<rtdb_text::Corpus> { self.file.corpus(&self.column, query) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::Interval;

    fn at(valid: i64, system: i64) -> TemporalBounds {
        TemporalBounds { valid_time: Interval::new(Timestamp(valid), Timestamp(valid + 1)).unwrap(), system_time: Interval::new(Timestamp(system), Timestamp(system + 1)).unwrap() }
    }

    #[test]
    fn an_event_stays_a_candidate_exactly_when_its_own_intervals_reach_the_query() {
        assert!(may_be_visible(10, 20, 5, i64::MAX, at(10, 5)), "an event created at the queried system instant is visible then");
        assert!(may_be_visible(10, 20, 5, i64::MAX, at(19, 100)));
        assert!(!may_be_visible(10, 20, 5, i64::MAX, at(20, 100)), "valid-to is exclusive");
        assert!(!may_be_visible(10, 20, 5, i64::MAX, at(9, 100)));
        assert!(!may_be_visible(10, 20, 6, i64::MAX, at(10, 5)), "an event recorded after the queried system time is not yet known");
        assert!(!may_be_visible(10, 100, 5, 30, at(35, 40)), "superseded by its recency before both of the query's instants");
        assert!(may_be_visible(10, 100, 5, 30, at(29, 40)), "the earlier of the two instants decides, as the scan prunes a page");
        assert!(may_be_visible(10, 100, 5, 30, at(35, 29)));
    }

    fn event(system_from: i64, valid_from: i64, valid_to: i64, op: rtdb_types::EventOp) -> crate::file::KeyedEvent { crate::file::KeyedEvent { system_from, valid_from, valid_to, op } }

    #[test]
    fn a_later_event_takes_its_valid_interval_from_its_system_time_on_and_an_erase_takes_everything() {
        use rtdb_types::EventOp::{Delete, Erase, Put};
        let whole = [0, 100, 10, i64::MAX];
        let mut rectangles = vec![whole];
        assert!(!cut(&mut rectangles, vec![event(20, 30, 60, Put)]));
        rectangles.sort_unstable();
        assert_eq!(rectangles, [[0, 30, 10, i64::MAX], [30, 60, 10, 20], [60, 100, 10, i64::MAX]]);
        let mut rectangles = vec![whole];
        cut(&mut rectangles, vec![event(5, 0, 100, Put), event(10, 0, 100, Delete)]);
        assert_eq!(rectangles, [whole], "an earlier event, or one at the same instant, takes nothing");
        assert!(cut(&mut vec![whole], vec![event(10, 0, 100, Put)]), "the same instant is a tie the scan breaks");
        let mut rectangles = vec![whole];
        cut(&mut rectangles, vec![event(40, 50, 50, Delete)]);
        assert_eq!(rectangles, [whole], "an empty valid interval takes nothing");
        let mut rectangles = vec![whole, [0, 5, 1, 3]];
        cut(&mut rectangles, vec![event(30, 0, 1, Erase)]);
        assert_eq!(rectangles, [[0, 5, 1, 3]], "an erase takes every rectangle it is later than");
    }

    #[test]
    fn a_trie_covers_the_entities_whose_leading_digits_are_its_part() {
        let iid = [0b0110_1100; 16];
        assert!(covers(&[], &iid));
        assert!(covers(&[1, 2, 3, 0, 1], &iid));
        assert!(!covers(&[1, 3], &iid));
    }
}
