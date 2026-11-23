#[cfg(feature = "experiments")]
pub mod experiment;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_format::{data::StoredOperation, keys::ArtefactOwner};
use rtdb_index::{Snapshot, TrieState};
use rtdb_plan::{BinaryOp, Catalog, Expr};
use rtdb_query::{MatchAnswers, QueryContext, ScanAccelerator, ScanRequest};
use rtdb_storage::ObjectKey;
use rtdb_text::Query;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, EventOp, Iid, Interval, OwnedRelation, Result, TemporalBounds, Timestamp, Value};

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

fn optional_build_refused(error: &Anomaly) -> bool {
    matches!(error.code.as_str(),
        "rtdb/memory-budget-exceeded" | "rtdb.storage/memory-limit" | "rtdb.search/input-order")
}

#[async_trait]
impl ScanAccelerator for SearchAccelerator {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>> {
        self.narrow(request, Narrowing::default(), &mut NarrowingWork::default()).await
    }

    async fn match_answers(&self, request: &ScanRequest<'_>, column: &str, query: &str) -> Result<Option<MatchAnswers>> {
        let Some(definition) = self.definition(request, column).await? else { return Ok(None) };
        let files = match self.trie_files(request, &definition).await? { TrieFiles::Complete(found) | TrieFiles::Partial(found) => found.files };
        let query = Query::parse(query);
        let mut answers = MatchAnswers::default();
        let mut sets = BTreeMap::new();
        for trie in &files {
            let set = match sets.entry(trie.identity.as_str()) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => entry.insert(
                    self.runtime.match_set_for_scan(request.database, trie, column, &query, request.scan.iids.as_ref())?,
                ),
            };
            answers.insert(&trie.data, Arc::new(set.member(trie.member)?));
        }
        Ok((!answers.is_empty()).then_some(answers))
    }
}

#[derive(Clone, Copy, Default)]
struct Narrowing<'a> {
    is_oracle: bool,
    baseline: bool,
    skip_retry: bool,
    estimate_first: bool,
    scope: Option<&'a BTreeSet<Iid>>,
}

#[derive(Default)]
struct NarrowingWork {
    tried_ranking: bool,
    filter_scope: Option<usize>,
    retry_redundant: bool,
    ranked: bool,
    decline: Option<&'static str>,
    plain_decline: Option<&'static str>,
    score_evaluations: usize,
    postings_visited: usize,
    verifier_rows: usize,
    verification_rounds: usize,
    plain_events: usize,
    estimated_postings: Option<usize>,
}

impl NarrowingWork {
    fn budget_exceeded(&self) -> bool {
        matches!(self.decline, Some("candidate-budget" | "verification-budget"))
    }
}

impl SearchAccelerator {
    async fn narrow(&self, request: &ScanRequest<'_>, options: Narrowing<'_>, work: &mut NarrowingWork) -> Result<Option<BTreeSet<Iid>>> {
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
        if options.is_oracle { return Ok(None); }
        if let Some(top_k) = &scan.top_k {
            let ranked = scan.columns.iter().find(|field| field.id == top_k.column).is_some_and(|field| field.name == column);
            if ranked {
                work.tried_ranking = true;
                let existing = if options.baseline { None } else { scan.iids.as_ref() };
                let scope = options.scope.or(existing);
                work.filter_scope = scope.map(BTreeSet::len);
                if scope.is_some_and(BTreeSet::is_empty) {
                    work.ranked = true;
                    return Ok(Some(BTreeSet::new()));
                }
                let ranking = Narrowing { scope, ..options };
                if let Some(candidates) = self.top_k(request, top_k, column, &query, &definition, ranking, work).await? {
                    work.ranked = true;
                    return Ok(Some(candidates));
                }
                if work.retry_redundant || work.decline == Some("posting-estimate")
                    || (options.skip_retry && work.budget_exceeded()) {
                    return Ok(None);
                }
            }
        }
        if let Some(scope) = options.scope {
            return Ok(Some(scope.clone()));
        }
        let TrieFiles::Complete(Found { files, skipped }) = self.trie_files(request, &definition).await? else {
            work.plain_decline = Some("coverage");
            return Ok(None);
        };
        let cutter = Cutter::new(&files, &skipped, request, &self.runtime);
        let Some(mut candidates) = file_candidates(&cutter, column, &query, self.max_candidates, &mut work.plain_events)? else {
            work.plain_decline = Some("candidate-budget");
            return Ok(None);
        };
        candidates.extend(self.live_candidates(request, column, &query)?);
        if candidates.len() > self.max_candidates { work.plain_decline = Some("candidate-budget"); return Ok(None); }
        Ok(Some(candidates))
    }
}

/// A partition's search files with complete or partial coverage of the live tries which can hold visible rows.
enum TrieFiles { Complete(Found), Partial(Found) }

/// A partition's search files, and the recorded tries left unopened because no lifespan section of theirs meets the bounds.
struct Found { files: Vec<TrieFile>, skipped: Vec<rtdb_format::keys::TrieKey> }

/// Whether the scan's other sources leave a file's document visible in the query's bounds, and whether that is exact.
enum Judged { Hidden, Visible { exact: bool } }

/// Cuts a file's documents by the events of the same entity in the partition's other tries and its live segments (fts.allium: CutByLaterEvents).
/// A trie left unopened may still hold an event that cuts, a delete say, so a document whose entity it covers is no longer exact.
struct Cutter<'a> { files: &'a [TrieFile], skipped: &'a [rtdb_format::keys::TrieKey], segments: Vec<&'a Arc<rtdb_index::LiveTable>>, runtime: &'a crate::SearchRuntime, database: &'a str, bounds: TemporalBounds }

impl<'a> Cutter<'a> {
    fn new(files: &'a [TrieFile], skipped: &'a [rtdb_format::keys::TrieKey], request: &'a ScanRequest<'_>, runtime: &'a crate::SearchRuntime) -> Self {
        Self { files, skipped, segments: request.snapshot.iter_segments(&request.scan.table.name.table).collect(), runtime, database: request.database, bounds: request.bounds }
    }

    fn judge(&self, own: usize, doc: crate::DocMatch) -> Result<Judged> {
        self.judge_key(own, doc, None)
    }

    fn judge_key(&self, own: usize, doc: crate::DocMatch, key: Option<&[u8]>) -> Result<Judged> {
        if key.is_some() && !self.segments.is_empty() { return Err(uncovered_owned_search()); }
        let own = &self.files[own];
        let mut rectangles: Vec<[i64; 4]> = own.file.rectangles(doc.doc).collect();
        let mut tied = false;
        for other in self.files {
            if other.identity == own.identity || !covers(other.trie.part(), &doc.iid) { continue; }
            let events = if let Some(key) = key {
                other.file.rows_for_key(&doc.iid, key).map(|row| row.map(|(_, event)| event)).collect::<Result<_>>()?
            } else {
                other.file.events(&doc.iid)?
            };
            tied |= cut(&mut rectangles, events);
        }
        for segment in &self.segments { tied |= cut(&mut rectangles, self.runtime.live_events(self.database, segment, &doc.iid)?); }
        if !rectangles.iter().any(|rectangle| crate::file::meets(*rectangle, self.bounds)) { return Ok(Judged::Hidden); }
        Ok(Judged::Visible { exact: !tied && !self.skipped.iter().any(|trie| covers(trie.part(), &doc.iid)) })
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
            tied |= event.system_from == rectangle[2];
            kept.extend(crate::rectangle::cut_rectangle(
                rectangle, event.valid_from, event.valid_to, event.system_from,
                event.op == rtdb_types::EventOp::Erase,
            ).into_iter().flatten());
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

    /// Where a trie's recorded search file is, if it has one: its key, the trie or partition group it was built over, and which member of it the trie is.
    fn recorded_key(snapshot: &Snapshot, table: &rtdb_types::TableRef, trie: &rtdb_index::CatalogTrie, definition: &SearchDefinition) -> Result<Option<Recorded>> {
        let artefact_id = definition.artefact_id();
        let (Some(entry), Some(record)) = (snapshot.table_entry(table), trie.artefact(KIND, &artefact_id, definition.version)) else { return Ok(None) };
        let owner = ArtefactOwner::of(&trie.key, record)?;
        let member = owner.tries().iter().position(|key| key == &trie.key)
            .ok_or_else(|| Anomaly::fault("rtdb.search/artefact-owner", "Search artefact does not contain its recorded trie"))? as u32;
        Ok(Some(Recorded { key: ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record)?)?, owner, member }))
    }

    /// A trie's recorded search file, or `None` where it cannot be read; one that is missing or corrupt is reported to the lane.
    async fn recorded(&self, request: &ScanRequest<'_>, recorded: &Recorded, definition: &SearchDefinition) -> Result<Option<Arc<SearchFile>>> {
        let (table, artefact_id) = (&request.scan.table.name.table, definition.artefact_id());
        match self.runtime.file(request.database, request.storage, &recorded.key, &definition.columns).await {
            Ok(file) => Ok(Some(file)),
            Err(error) => {
                if error.needs_repair() { self.runtime.report(Suspect { database: request.database.to_string(), table: table.clone(), owner: recorded.owner.clone(), artefact_id, version: definition.version }); }
                Ok(None)
            }
        }
    }

    /// The search files of the partition's live tries that can hold a visible event: every such trie's, its recorded one or one this node builds from its data file, unless the tries the lane should have indexed but has not exceed max_unindexed_bytes.
    /// The lane never indexes level 0, the partial level-1 current trie, or history for a definition that excludes it, so those do not count.
    async fn trie_files(&self, request: &ScanRequest<'_>, definition: &SearchDefinition) -> Result<TrieFiles> {
        let snapshot = request.snapshot;
        let bounds = request.bounds;
        let table = &request.scan.table.name.table;
        let Some(entry) = snapshot.table_entry(table) else { return Ok(TrieFiles::Complete(Found { files: Vec::new(), skipped: Vec::new() })) };
        let artefact_id = definition.artefact_id();
        let mut skipped = Vec::new();
        let mut files = Vec::new();
        let mut opened: BTreeMap<ObjectKey, Option<Arc<SearchFile>>> = BTreeMap::new();
        let mut unbuilt = Vec::new();
        let mut unindexed = 0i64;
        let earliest = bounds.valid_time.lower().0.min(bounds.system_time.lower().0);
        // A trie's recency bounds its rows' recency above, so a trie at or before the query's earliest instant holds nothing it can see.
        let visible = |trie: &&rtdb_index::CatalogTrie| trie.key.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest);
        for trie in snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).filter(visible) {
            let sections = trie.artefact(KIND, &artefact_id, definition.version).and_then(|record| record.stats.as_deref()).and_then(crate::file::decode_sections);
            if sections.is_some_and(|sections| !sections.iter().any(|section| section.meets(bounds))) { skipped.push(trie.key.clone()); continue; }
            let data = ObjectKey::new(entry.slug.data_key(&trie.key))?;
            if let Some(recorded) = Self::recorded_key(snapshot, table, trie, definition)? {
                let file = match opened.get(&recorded.key) {
                    Some(file) => file.clone(),
                    None => {
                        let file = self.recorded(request, &recorded, definition).await?;
                        opened.insert(recorded.key.clone(), file.clone());
                        file
                    }
                };
                if let Some(file) = file {
                    files.push(TrieFile { trie: trie.key.clone(), data, identity: recorded.key.as_str().to_string(), file, member: recorded.member });
                    continue;
                }
            }
            let by_policy = trie.key.level() == 0 || (trie.key.level() == 1 && trie.key.recency().is_none() && trie.data_file_size < snapshot.trie_catalog().file_size_target())
                || (!definition.history && trie.key.recency().is_some());
            if !by_policy { unindexed = unindexed.saturating_add(trie.data_file_size); }
            unbuilt.push((trie.key.clone(), data));
        }
        if unindexed > self.max_unindexed_bytes { return Ok(TrieFiles::Partial(Found { files, skipped })); }
        for (trie, data) in unbuilt {
            match self.runtime.built(request.database, request.storage, &trie, &data, &definition.columns, request.memory, request.cancellation).await {
                Ok(file) => files.push(file),
                Err(error) if optional_build_refused(&error) => {
                    return Ok(TrieFiles::Partial(Found { files, skipped }));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(TrieFiles::Complete(Found { files, skipped }))
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

/// Where a trie's recorded search file is, and which of its members the trie is.
struct Recorded { key: ObjectKey, owner: ArtefactOwner, member: u32 }

/// A shared file's selected members and their positions in the scan's trie list.
struct FileMembers<'a> { file: &'a Arc<SearchFile>, members: BTreeMap<u32, usize> }

fn by_file(files: &[TrieFile]) -> impl Iterator<Item = FileMembers<'_>> {
    let mut grouped = BTreeMap::new();
    for (index, trie) in files.iter().enumerate() {
        let selected = grouped.entry(trie.identity.as_str()).or_insert_with(|| FileMembers { file: &trie.file, members: BTreeMap::new() });
        selected.members.insert(trie.member, index);
    }
    grouped.into_values()
}

fn uncovered_owned_search() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/search-uncovered", "Published search artefacts do not cover the held cut")
}

/// Computes exact statistics for the admitted owned relation at the held point.
/// Opens only artefacts recorded on that cut and never builds a replacement.
pub async fn owned_corpus(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation, text: &str,
) -> Result<rtdb_text::Corpus> {
    Ok(covered_owned_corpus(context, owner, relation, path, &Query::parse(text), None, false, None).await?.corpus)
}

struct CoveredCorpus {
    column: String,
    bounds: TemporalBounds,
    corpus: rtdb_text::Corpus,
    visible_runs: Vec<(Arc<SearchFile>, BTreeSet<u32>)>,
    _scratch: Arc<rtdb_columnar::MemoryReservation>,
}

async fn covered_owned_corpus(
    context: Arc<QueryContext>, owner: &str, relation: &OwnedRelation, path: &OwnedRelation, query: &Query,
    valid_at: Option<Timestamp>, retain_runs: bool,
    shadowed: Option<&BTreeSet<EntityId>>,
) -> Result<CoveredCorpus> {
    let definition = crate::definitions::owned_definition(Arc::clone(&context), owner, relation, path).await?
        .ok_or_else(|| Anomaly::incorrect("rtdb.owned/search-path", "Owned relation has no declared clear-text search path with this name"))?;
    let database = context.catalog.default_database();
    let captured = context.catalog.database(database)?;
    let [snapshot] = captured.partitions.as_slice() else { return Err(uncovered_owned_search()); };
    let present = context.current_time.as_micros()?;
    if snapshot.system_time_cap() != Some(present) {
        return Err(uncovered_owned_search());
    }
    let [column] = definition.columns.as_slice() else { return Err(uncovered_owned_search()); };
    let bounds = TemporalBounds {
        valid_time: Interval::at(valid_at.unwrap_or(present))?,
        system_time: Interval::at(present)?,
    };
    if !crate::definitions::has_owned_search_text(snapshot, &definition.table, column)? {
        return Ok(CoveredCorpus {
            column: column.clone(), bounds, corpus: rtdb_text::Corpus::default(), visible_runs: Vec::new(),
            _scratch: context.memory.reserve(4096)?,
        });
    }
    if snapshot.iter_segments(&definition.table).any(|segment| segment.row_count() != 0) {
        return Err(uncovered_owned_search());
    }
    let runtime = crate::SearchRuntime::new(0);
    let mut files = Vec::new();
    let mut opened = BTreeMap::<ObjectKey, Arc<SearchFile>>::new();
    let entry = snapshot.table_entry(&definition.table).ok_or_else(uncovered_owned_search)?;
    let artefact_id = definition.artefact_id();
    for trie in snapshot.trie_catalog().current_tries(&definition.table) {
        context.check_cancelled()?;
        let recorded = SearchAccelerator::recorded_key(snapshot, &definition.table, trie, &definition)?
            .ok_or_else(uncovered_owned_search)?;
        let file = if let Some(file) = opened.get(&recorded.key) {
            Arc::clone(file)
        } else {
            let file = runtime.file(database, &captured.storage, &recorded.key, &definition.columns).await
                .map_err(|_| uncovered_owned_search())?;
            let identity = crate::file::artefact_identity(entry.slug.as_str(), &recorded.owner, &artefact_id, definition.version);
            if !file.matches_artefact(&identity, definition.version) {
                return Err(uncovered_owned_search());
            }
            opened.insert(recorded.key.clone(), Arc::clone(&file));
            file
        };
        if !file.has_full_event_keys() { return Err(uncovered_owned_search()); }
        let data = ObjectKey::new(entry.slug.data_key(&trie.key))?;
        files.push(TrieFile { trie: trie.key.clone(), data, identity: recorded.key.as_str().to_owned(), file, member: recorded.member });
    }

    let cutter = Cutter { files: &files, skipped: &[], segments: Vec::new(), runtime: &runtime, database, bounds };
    let mut corpus = rtdb_text::Corpus {
        frequencies: query.terms().iter().cloned().map(|term| (term, 0)).collect(),
        ..rtdb_text::Corpus::default()
    };
    // The physical slot is only a local instruction for rereading the source.
    // Corpus membership and uniqueness use the complete typed key.
    let mut seen = BTreeSet::new();
    let mut visible_runs = Vec::new();
    let mut scratch = context.memory.reserve(4096)?;
    for selected in by_file(&files) {
        let mut visible = BTreeSet::new();
        for (&member, &own) in &selected.members {
            for doc in selected.file.single_column_member_runs(member)? {
                if doc.doc % 128 == 0 { context.check_cancelled()?; }
                if !selected.file.visible(doc.doc, bounds) { continue; }
                Arc::get_mut(&mut scratch).expect("corpus owns its reservation").grow_to(4096usize.saturating_add(seen.len().saturating_add(1).saturating_mul(256)))?;
                let key = selected.file.run_identity_bytes(doc)?;
                let _key = context.memory.reserve(key.len())?;
                let id = selected.file.run_identity(doc)?;
                match cutter.judge_key(own, doc, Some(key))? {
                    Judged::Hidden => (),
                    Judged::Visible { exact: false } => return Err(uncovered_owned_search()),
                    Judged::Visible { exact: true } => {
                        if shadowed.is_some_and(|ids| ids.contains(&id)) { continue; }
                        if !seen.insert(key) { return Err(uncovered_owned_search()); }
                        visible.insert(doc.doc);
                        corpus.documents += 1;
                        corpus.tokens += u64::from(selected.file.token_length(doc.doc)?);
                    }
                }
            }
        }
        for term in query.terms() {
            use tantivy::DocSet;
            let Some(mut postings) = selected.file.postings(column, term)? else { continue; };
            let mut frequency = 0u64;
            for &doc in &visible {
                if postings.doc() < doc { postings.seek(doc); }
                if postings.doc() == doc { frequency += 1; }
                if postings.doc() == tantivy::TERMINATED { break; }
            }
            *corpus.frequencies.entry(term.clone()).or_default() += frequency;
        }
        if retain_runs && !visible.is_empty() {
            visible_runs.push((Arc::clone(selected.file), visible));
        }
    }
    context.check_cancelled()?;
    Ok(CoveredCorpus { column: column.clone(), bounds, corpus, visible_runs, _scratch: scratch })
}

/// One exact match on a held owned search path, identified by its full typed key.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnedSearchHit {
    pub partition: usize,
    pub id: EntityId,
    pub score: f64,
}

pub(crate) fn owned_rank_order(left: &OwnedSearchHit, right: &OwnedSearchHit) -> std::cmp::Ordering {
    left.score.total_cmp(&right.score).reverse()
        .then_with(|| left.id.cmp(&right.id))
        .then_with(|| left.partition.cmp(&right.partition))
}

/// The requested rank window and the exact number of verified matches.
pub struct OwnedSearchPage {
    pub hits: Vec<OwnedSearchHit>,
    pub total: usize,
    pub(crate) _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

/// Ranks nominated runs at one cut using statistics from all selected partitions.
pub async fn owned_ranked(
    context: Arc<QueryContext>, owner: &str, partitions: &[(OwnedRelation, OwnedRelation)], text: &str,
    offset: usize, limit: usize, valid_at: Option<Timestamp>,
) -> Result<OwnedSearchPage> {
    owned_ranked_with_overlay(context, owner, partitions, text, offset, limit, valid_at, None).await
}

/// Rank the admitted archive and original overlay with one set of statistics and one rank window.
pub async fn owned_ranked_with_overlay(
    context: Arc<QueryContext>, owner: &str, partitions: &[(OwnedRelation, OwnedRelation)], text: &str,
    offset: usize, limit: usize, valid_at: Option<Timestamp>, overlay: Option<crate::HeldTextCorpus>,
) -> Result<OwnedSearchPage> {
    if offset > 4096 || !(1..=128).contains(&limit) || !(1..=32).contains(&partitions.len()) || text.len() > 4096 {
        return Err(Anomaly::incorrect("rtdb.owned/search-limit", "Owned search exceeds its result, partition, or query bound"));
    }
    let distinct: BTreeSet<_> = partitions.iter().map(|(relation, _)| relation).collect();
    if distinct.len() != partitions.len() {
        return Err(Anomaly::incorrect("rtdb.owned/search-relations", "An owned search relation must be selected once"));
    }
    let query = Query::parse(text);
    if let Some(overlay) = &overlay { overlay.require_query(&query)?; }
    match rank_owned_artefacts(&context, owner, partitions, &query, offset, limit, valid_at, overlay.as_ref()).await {
        Ok((scorer, mut rank)) => {
            if let Some(overlay) = overlay { overlay.rank(&scorer, &mut rank)?; }
            context.check_cancelled()?;
            Ok(rank.finish())
        }
        Err(error) if error.code == "rtdb.owned/search-uncovered" => {
            let mut corpus = match overlay {
                Some(overlay) => overlay,
                None => crate::HeldTextCorpus::new(Arc::clone(&context.memory), text)?,
            };
            let valid = valid_at.unwrap_or(context.current_time.as_micros()?);
            for (partition, (relation, path)) in partitions.iter().enumerate() {
                corpus.read_root_partition(&context, owner, relation, path, partition, valid).await?;
            }
            context.check_cancelled()?;
            corpus.finish(offset, limit)
        }
        Err(error) => Err(error),
    }
}

async fn rank_owned_artefacts(
    context: &Arc<QueryContext>, owner: &str, partitions: &[(OwnedRelation, OwnedRelation)], query: &Query,
    offset: usize, limit: usize, valid_at: Option<Timestamp>, overlay: Option<&crate::HeldTextCorpus>,
) -> Result<(rtdb_text::Scorer, OwnedRankWindow)> {
    let mut corpus = rtdb_text::Corpus::default();
    let mut covered = Vec::with_capacity(partitions.len());
    for (partition, (relation, path)) in partitions.iter().enumerate() {
        let shadowed = overlay.and_then(|part| part.shadowed(partition));
        let part = covered_owned_corpus(Arc::clone(context), owner, relation, path, query, valid_at, true, shadowed).await?;
        corpus.add(&part.corpus);
        covered.push((relation, part));
    }
    if let Some(overlay) = overlay { corpus.add(overlay.statistics()); }
    let scorer = rtdb_text::Scorer::new(query.clone(), Some(&corpus));
    let mut rank = OwnedRankWindow::new(&context.memory, offset, limit)?;
    for (partition, (relation, covered)) in covered.iter().enumerate() {
        for (file, visible) in &covered.visible_runs {
            context.check_cancelled()?;
            for run in file.matches(&covered.column, query, covered.bounds)? {
                if !visible.contains(&run.doc) { continue; }
                rank.admit_candidate()?;
                let key = file.run_identity_bytes(run)?;
                let _key = context.memory.reserve(key.len())?;
                let id = file.run_identity(run)?;
                let columns = ["_id", covered.column.as_str()];
                let Some(mut rows) = rtdb_query::open_owned_artefact_candidate(
                    Arc::clone(context), owner, relation, &id, Iid(run.iid), covered.bounds.valid_time, &columns,
                ).await? else { return Err(uncovered_owned_search()); };
                let mut resolved_score = None;
                while let Some(batch) = rows.next_batch().await? {
                    let batch = batch.batch();
                    if batch.num_rows() == 0 { continue; }
                    if resolved_score.is_some() || batch.num_rows() != 1 { return Err(uncovered_owned_search()); }
                    let revision = rtdb_query::owned_row_revision(batch, 0, &id, &context.memory)?;
                    let at = covered.bounds.valid_time.lower().0;
                    let mut same_run = false;
                    for row in file.rows_for_key(&run.iid, key) {
                        let (row, event) = row?;
                        if event.op == EventOp::Put && event.system_from == revision.0 && event.valid_from <= at
                            && at < event.valid_to && file.run_of(&covered.column, row) == Some(Some(run.doc))
                        {
                            same_run = true;
                            break;
                        }
                    }
                    if !same_run { return Err(uncovered_owned_search()); }
                    let (column, field) = batch.schema_ref().column_with_name(&covered.column).ok_or_else(uncovered_owned_search)?;
                    let values = batch.column(column).as_ref();
                    let bytes = rtdb_columnar::estimate_read_value_bytes(field, values, 0)?;
                    let _decode = context.memory.reserve(bytes.saturating_mul(64).saturating_add(4096))?;
                    let Value::Utf8(text) = rtdb_columnar::read_value(field, values, 0)? else { return Err(uncovered_owned_search()); };
                    let tokens = rtdb_text::tokens(&text);
                    if !query.matches(&tokens) || tokens.len() != file.token_length(run.doc)? as usize {
                        return Err(uncovered_owned_search());
                    }
                    let score = scorer.bm25(&tokens);
                    if !score.is_finite() { return Err(uncovered_owned_search()); }
                    resolved_score = Some(score);
                }

                let hit = OwnedSearchHit { partition, id, score: resolved_score.ok_or_else(uncovered_owned_search)? };
                rank.insert(hit)?;
            }
        }
    }
    Ok((scorer, rank))
}

pub(crate) struct OwnedRankWindow {
    hits: Vec<OwnedSearchHit>,
    memory: Arc<rtdb_columnar::MemoryReservation>,
    offset: usize,
    window: usize,
    total: usize,
}

impl OwnedRankWindow {
    pub(crate) fn new(memory: &Arc<rtdb_columnar::MemoryBudget>, offset: usize, limit: usize) -> Result<Self> {
        let window = offset + limit;
        let memory = memory.reserve(4096 + (window + 1) * size_of::<OwnedSearchHit>())?;
        Ok(Self { hits: Vec::with_capacity(window + 1), memory, offset, window, total: 0 })
    }

    pub(crate) fn admit_candidate(&mut self) -> Result<()> {
        self.total += 1;
        if self.total > 100_000 {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/search-candidate-limit", "Covered search exceeded its candidate budget"));
        }
        Ok(())
    }

    pub(crate) fn insert(&mut self, hit: OwnedSearchHit) -> Result<()> {
        let at = self.hits.partition_point(|previous| owned_rank_order(previous, &hit).is_lt());
        if at < self.window {
            self.hits.insert(at, hit);
            self.hits.truncate(self.window);
            let bytes = self.hits.iter().try_fold(4096usize, |bytes, hit| {
                Ok::<_, Anomaly>(bytes.saturating_add(hit.id.as_value().retained_bytes()?).saturating_add(128))
            })?;
            let reservation = Arc::get_mut(&mut self.memory).expect("ranking owns its reservation");
            reservation.grow_to(bytes.max(reservation.bytes()))?;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> OwnedSearchPage {
        self.hits.drain(..self.offset.min(self.hits.len()));
        OwnedSearchPage { hits: self.hits, total: self.total, _reservation: self.memory }
    }
}

fn file_candidates(cutter: &Cutter<'_>, column: &str, query: &Query, max_candidates: usize, events: &mut usize) -> Result<Option<BTreeSet<Iid>>> {
    let mut candidates = BTreeSet::new();
    for selected in by_file(cutter.files) {
        let docs = selected.file.matches(column, query, cutter.bounds)?;
        if cfg!(feature = "experiments") { *events = events.saturating_add(docs.len()); }
        for doc in docs {
            let Some(&own) = selected.members.get(&doc.member) else { continue };
            if !matches!(cutter.judge(own, doc)?, Judged::Hidden) { candidates.insert(Iid(doc.iid)); }
        }
        if candidates.len() > max_candidates { return Ok(None); }
    }
    Ok(Some(candidates))
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
struct Candidates<'a> { files: Vec<(BTreeMap<u32, usize>, crate::file::Ranked)>, cutter: Cutter<'a>, pending: std::collections::BinaryHeap<Pending>, seen: BTreeSet<Iid>, scored: usize }

/// Counts only distinct IIDs already nominated by the ranker.
fn exceeds_retry_limit(
    seen: &BTreeSet<Iid>,
    pending: &std::collections::BinaryHeap<Pending>,
    limit: usize,
    memory: &Arc<rtdb_columnar::MemoryBudget>,
) -> bool {
    let Some(remaining) = limit.checked_sub(seen.len()) else { return true };
    if remaining >= pending.len() { return false; }
    let needed = remaining + 1;
    let Ok(_charge) = memory.reserve(needed.saturating_mul(size_of::<Iid>() + 32)) else { return false };
    let mut distinct = HashSet::new();
    if distinct.try_reserve(needed).is_err() { return false; }
    pending.iter().any(|candidate| {
        !seen.contains(&candidate.1) && distinct.insert(&candidate.1) && distinct.len() > remaining
    })
}

impl Candidates<'_> {
    fn push(&mut self, iid: Iid, score: f64, exact: bool) { self.scored += 1; self.pending.push(Pending(score, iid, exact)); }
    fn drop_seen(&mut self) { while self.pending.peek().is_some_and(|top| self.seen.contains(&top.1)) { self.pending.pop(); } }
    /// The file whose next windows may hold the best event not yet scored.
    fn best_file(&self) -> Option<(usize, f64)> {
        self.files.iter().enumerate().filter_map(|(index, (_, file))| file.bound().map(|bound| (index, bound))).max_by(|left, right| left.1.total_cmp(&right.1))
    }
    /// Stop only when the kth verified score beats both remaining sources.
    fn can_stop(&mut self, kth: f64) -> bool {
        self.drop_seen();
        let pending = self.pending.peek().map(|top| top.0.to_bits());
        let unread = self.best_file().map(|(_, bound)| bound.to_bits());
        crate::rank_cutoff::can_stop(kth.to_bits(), pending, unread)
    }
    /// The next IID, best first, with its best event's score and whether that event is exactly visible.
    fn next(&mut self, scope: Option<&BTreeSet<Iid>>, work: &mut NarrowingWork) -> Result<Option<(f64, Iid, bool)>> {
        loop {
            self.drop_seen();
            match (self.pending.peek().map(|top| top.0), self.best_file()) {
                (pending, Some((index, bound))) if pending.is_none_or(|score| bound > score) => {
                    let scored = self.files[index].1.expand_in(scope, &mut work.postings_visited)?;
                    if cfg!(feature = "experiments") { work.score_evaluations += scored.len(); }
                    for (doc, score) in scored {
                        let Some(&own) = self.files[index].0.get(&doc.member) else { continue };
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
    async fn top_k(&self, request: &ScanRequest<'_>, top_k: &rtdb_plan::ScanTopK, column: &str, matching: &Query, definition: &SearchDefinition, options: Narrowing<'_>, work: &mut NarrowingWork) -> Result<Option<BTreeSet<Iid>>> {
        let Some(Value::Utf8(text)) = request.constants.constant(&top_k.query).await? else { work.decline = Some("unsupported-order"); return Ok(None) };
        let limit = match request.constants.constant(&top_k.limit).await.ok().flatten() {
            Some(Value::I64(limit)) => limit, Some(Value::I32(limit)) => i64::from(limit), Some(Value::I16(limit)) => i64::from(limit), _ => { work.decline = Some("unsupported-limit"); return Ok(None); },
        };
        let Ok(limit) = usize::try_from(limit) else { work.decline = Some("unsupported-limit"); return Ok(None) };
        if limit == 0 { return Ok(Some(BTreeSet::new())); }
        let TrieFiles::Complete(Found { files, skipped }) = self.trie_files(request, definition).await? else {
            work.decline = Some("coverage");
            return Ok(None);
        };
        if options.estimate_first && options.scope.is_none() && request.scan.iids.is_none() && matching.terms().len() == 1 {
            let term = &matching.terms()[0];
            let mut estimate = 0usize;
            for group in by_file(&files) {
                estimate = estimate.saturating_add(group.file.document_frequency(column, term)? as usize);
            }
            for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
                estimate = estimate.saturating_add(segment.row_count() as usize);
            }
            work.estimated_postings = Some(estimate);
            if estimate > self.max_candidates {
                work.decline = Some("posting-estimate");
                return Ok(None);
            }
        }
        let scoring = Arc::new(rtdb_text::Scorer::new(Query::parse(&text), None));
        let Some(rank_scorer) = scoring.for_ranking() else { work.decline = Some("score-domain"); return Ok(None); };
        let ranked = by_file(&files).map(|selected| {
            Ok((selected.members, selected.file.ranked(column, matching, &rank_scorer, request.bounds)?))
        }).collect::<Result<Vec<_>>>()?;
        let mut candidates = Candidates { files: ranked, cutter: Cutter::new(&files, &skipped, request, &self.runtime), pending: std::collections::BinaryHeap::new(), seen: BTreeSet::new(), scored: 0 };
        let trusted = match request.scan.predicates.as_slice() {
            [Expr::Column { id, .. }] => request.scan.matches.iter().any(|evaluated| evaluated.id == *id),
            [Expr::Call { name, .. }] => name.eq_ignore_ascii_case("match"),
            _ => false,
        };
        for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
            for row in self.runtime.live_rows(request.database, segment, column, matching)? {
                if cfg!(feature = "experiments") { work.postings_visited += 1; }
                let event = segment.row(row)?.event()?;
                if options.scope.is_some_and(|scope| !scope.contains(&event.iid)) { continue; }
                if !may_be_visible(event.valid_time.lower().0, event.valid_time.upper().0, event.system_from.0, Timestamp::MAX.0, request.bounds) { continue; }
                let StoredOperation::Put(document) = &event.operation else { continue };
                let Some(Value::Utf8(value)) = document.get(column) else { continue };
                let tokens = rtdb_text::tokens(value);
                if matching.matches(&tokens) {
                    if cfg!(feature = "experiments") { work.score_evaluations += 1; }
                    candidates.push(event.iid, scoring.bm25(&tokens), false);
                }
            }
        }
        let mut verified: Vec<(f64, Iid)> = Vec::new();
        let batch = limit.saturating_mul(2).max(64);

        loop {
            if verified.len() >= limit {
                verified.sort_by(descending);
                if candidates.can_stop(verified[limit - 1].0) { break; }
            }
            let mut iids = BTreeSet::new();
            let mut handed = 0;
            while handed < batch {
                let Some((score, iid, exact)) = candidates.next(options.scope, work)? else { break };
                handed += 1;
                if request.scan.iids.as_ref().is_some_and(|scope| !scope.contains(&iid)) { continue; }
                // An exactly visible event's row is in the scan's output with its score, so it needs no verifying.
                if exact && trusted { verified.push((score, iid)); } else { iids.insert(iid); }
            }
            if candidates.scored > self.max_candidates {
                work.decline = Some("candidate-budget");
                if !options.baseline && options.scope.is_none() {
                    // Each nominated IID would also be in the unranked MATCH candidate union.
                    work.retry_redundant = exceeds_retry_limit(&candidates.seen, &candidates.pending, self.max_candidates, request.memory);
                }
                return Ok(None);
            }
            if handed == 0 { break; }
            if iids.is_empty() { continue; }
            if work.verification_rounds == 16 { work.decline = Some("verification-budget"); return Ok(None); }
            work.verification_rounds += 1;
            if cfg!(feature = "experiments") { work.verifier_rows += iids.len(); }
            for (iid, value) in request.verifier.visible(iids, top_k.column).await? {
                let Value::Utf8(value) = value else { continue };
                verified.push((scoring.bm25(&rtdb_text::tokens(&value)), iid));
            }
        }
        verified.sort_by(descending);
        let kth = verified.get(limit - 1).map_or(f64::NEG_INFINITY, |(score, _)| *score);
        Ok(Some(verified.into_iter().filter(|(score, _)| *score >= kth).map(|(_, iid)| iid).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::Interval;

    fn at(valid: i64, system: i64) -> TemporalBounds {
        TemporalBounds { valid_time: Interval::new(Timestamp(valid), Timestamp(valid + 1)).unwrap(), system_time: Interval::new(Timestamp(system), Timestamp(system + 1)).unwrap() }
    }

    #[test]
    fn retry_budget_requires_distinct_iids_and_available_scratch() {
        let iid = |n| Iid([n; 16]);
        let seen = BTreeSet::from([iid(1)]);
        let mut pending = std::collections::BinaryHeap::from([
            Pending(1.0, iid(1), true), Pending(0.5, iid(2), true), Pending(0.25, iid(2), true),
        ]);
        let memory = rtdb_columnar::MemoryBudget::new(1024);
        assert!(!exceeds_retry_limit(&seen, &pending, 2, &memory));
        pending.push(Pending(1.0, iid(3), true));
        assert!(exceeds_retry_limit(&seen, &pending, 2, &memory));
        assert!(!exceeds_retry_limit(&seen, &pending, 2, &rtdb_columnar::MemoryBudget::new(1)));
        let seen = BTreeSet::from([iid(1), iid(2), iid(3)]);
        assert!(exceeds_retry_limit(&seen, &pending, 2, &rtdb_columnar::MemoryBudget::new(1)));
    }

    #[test]
    fn shared_search_uses_only_the_selected_members_for_candidates_and_ranking() {
        use crate::file::write_group_search_file;

        let columns = vec!["body".to_string()];
        let row = |id: u8, body: &str| crate::SearchRow {
            identity: None, iid: [id; 16], system_from: 0, valid_from: 0, valid_to: i64::MAX,
            op: rtdb_types::EventOp::Put, values: vec![Some(rtdb_text::tokens(body))],
        };
        let mut bytes = Vec::new();
        let members = [vec![Ok(vec![row(1, "overdue invoice")])], vec![Ok(vec![row(64, "overdue")])], Vec::new(), Vec::new()];
        write_group_search_file(&columns, members, "group", None, tantivy::directory::RamDirectory::create(), &mut bytes).unwrap();
        let file = Arc::new(SearchFile::open(bytes, &columns).unwrap());
        let trie = |member: u32| TrieFile {
            trie: format!("l02-rc-p{member}-b00").parse().unwrap(),
            data: ObjectKey::new(format!("tables/docs/data/l02-rc-p{member}-b00.arrow")).unwrap(),
            identity: "group".into(), file: Arc::clone(&file), member,
        };
        let runtime = crate::SearchRuntime::new(1 << 20);
        let bounds = at(0, 0);
        let query = Query::parse("overdue");
        let files = [trie(1)];
        let cutter = Cutter { files: &files, skipped: &[], segments: Vec::new(), runtime: &runtime, database: "xtdb", bounds };
        let iids = file_candidates(&cutter, "body", &query, 1, &mut 0).unwrap().unwrap();
        assert_eq!(iids, BTreeSet::from([Iid([64; 16])]));
        assert_eq!(by_file(&[trie(0), trie(1)]).count(), 1);

        let rank_scorer = Arc::new(rtdb_text::Scorer::new(query.clone(), None)).for_ranking().unwrap();
        let ranked = file.ranked("body", &query, &rank_scorer, bounds).unwrap();
        let mut candidates = Candidates {
            files: vec![(BTreeMap::from([(1, 0)]), ranked)], cutter,
            pending: Default::default(), seen: BTreeSet::new(), scored: 0,
        };
        assert_eq!(candidates.next(None, &mut NarrowingWork::default()).unwrap().map(|(_, iid, exact)| (iid, exact)), Some((Iid([64; 16]), true)));
        assert!(candidates.next(None, &mut NarrowingWork::default()).unwrap().is_none());
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
    fn an_intervening_write_in_another_file_does_not_hide_the_later_match() {
        let columns = ["body".to_string()];
        let iid = [1; 16];
        let row = |system_from, valid_to, text: &str| crate::SearchRow {
            identity: None, iid, system_from, valid_from: 0, valid_to, op: rtdb_types::EventOp::Put,
            values: vec![Some(rtdb_text::tokens(text))],
        };
        let trie = |key: &str, rows: &[crate::SearchRow]| {
            let bytes = crate::build_search_file(&columns, rows, key).unwrap();
            TrieFile {
                trie: key.parse().unwrap(),
                data: ObjectKey::new(format!("tables/docs/data/{key}.arrow")).unwrap(),
                identity: key.to_string(),
                file: Arc::new(SearchFile::open(bytes, &columns).unwrap()),
                member: 0,
            }
        };
        let files = [
            trie("l01-r20200101-b00", &[row(30, 10, "apple"), row(10, 10, "apple")]),
            trie("l01-rc-b00", &[row(20, i64::MAX, "pear")]),
        ];
        let runtime = crate::SearchRuntime::new(1 << 20);
        let query = Query::parse("apple");
        for (system, expected) in [(15, true), (25, false), (35, true)] {
            let cutter = Cutter {
                files: &files, skipped: &[], segments: Vec::new(), runtime: &runtime,
                database: "xtdb", bounds: at(5, system),
            };
            let candidates = file_candidates(&cutter, "body", &query, 1, &mut 0).unwrap().unwrap();
            assert_eq!(candidates.contains(&Iid(iid)), expected, "at system time {system}");
        }
    }

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
        let mut rectangles = vec![whole, [0, 5, 1, 3], [0, 5, 40, 50]];
        cut(&mut rectangles, vec![event(30, 0, 1, Erase)]);
        assert_eq!(rectangles, [[0, 5, 40, 50]], "an erase also removes versions superseded before it");
    }

    #[test]
    fn a_trie_covers_the_entities_whose_leading_digits_are_its_part() {
        let iid = [0b0110_1100; 16];
        assert!(covers(&[], &iid));
        assert!(covers(&[1, 2, 3, 0, 1], &iid));
        assert!(!covers(&[1, 3], &iid));
    }
}
