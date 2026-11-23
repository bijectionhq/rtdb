#[cfg(feature = "experiments")]
pub mod experiment;
mod tree;
mod point;
pub(crate) mod routing;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use arrow_array::{Float64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use rtdb_columnar::{EntityIdRef, OwnedBatch};
use rtdb_format::{data::PutText, keys::ArtefactOwner};
use rtdb_index::{Snapshot, TrieState};
use rtdb_plan::{BinaryOp, Catalog, Expr};
use rtdb_query::{MatchAnswers, QueryContext, ScanAccelerator, ScanRequest, ScanSelection};
use rtdb_storage::ObjectKey;
use rtdb_text::Query;
use rtdb_types::{Anomaly, AnomalyCategory, EntityId, EventOp, Iid, Interval, OwnedRelation, Result, TableRef, TemporalBounds, Timestamp, Value};

use crate::{KIND, SearchDefinition, SearchFile, rank_cutoff::RankKey, runtime::{Suspect, TrieFile}};

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

    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        let mut work = NarrowingWork::default();
        let candidates = self.narrow(request, Narrowing::default(), &mut work).await.and_then(|result| {
            result.map(|iids| {
                Ok(ScanSelection { iids: rtdb_query::CandidateIids::from_set(iids, request.memory)?, scores: work.scores })
            }).transpose()
        });
        match candidates {
            Err(error) if optional_build_refused(&error) => Ok(None),
            result => result,
        }
    }

    async fn rank_source<'a>(&'a self, request: ScanRequest<'a>) -> Result<Option<Box<dyn rtdb_query::RankSource + 'a>>> {
        let Some(top) = request.scan.top_k.as_ref() else { return Ok(None); };
        let Some(column) = request.scan.columns.iter().find(|field| field.id == top.column).map(|field| field.name.as_str()) else { return Ok(None); };
        let Some((matched_column, matching, definition)) = self.match_definition(&request).await? else { return Ok(None); };
        if matched_column != column { return Ok(None); }
        let Some(Value::Utf8(scoring)) = request.constants.constant(&top.query).await? else { return Ok(None); };
        let Some(limit) = request.ranked_prefix_len(top).await else { return Ok(None); };
        let result = tree::PartitionRankSource::open(self, request, definition, column, top.column, matching, Query::parse(&scoring),
            top.ascending_id, limit, Narrowing::default()).await;
        match result {
            Ok(source) => Ok(source.map(|source| source as Box<dyn rtdb_query::RankSource + 'a>)),
            Err(error) if optional_build_refused(&error) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn point_rows(&self, request: &ScanRequest<'_>, iids: &BTreeSet<Iid>) -> Result<Option<Vec<rtdb_query::ResolvedSearchRow>>> {
        let table = &request.scan.table.name.table;
        if table.is_owned_event_data() || TableRef::owned_schema(&table.schema)
            || (self.runtime.write_format() != crate::file::LOGICAL_FORMAT_VERSION
                && request.snapshot.trie_catalog().search_runs(table).next().is_none()) { return Ok(None); }
        let result = async {
            let definitions = self.runtime.definitions(request.database, request.partitions, request.storage, request.memory,
                request.current_time, request.cancellation).await?;
            let Some(definition) = definitions.iter().find(|definition| &definition.table == table
                && !definition.columns.is_empty() && crate::lane::LogicalIndex::supports(definition)) else { return Ok(None); };
            let sources = tree::RecordedTree::select(&self.runtime, request, definition, 0, &Query::parse(""), true, true)?;
            let history = point::PackedVisibility::new(request, table, &sources, 0, true);
            history.resolve(&self.runtime, request, definition, &sources, iids).await
                .map(|result| result.map(|rows| rows.into_iter().map(|row| row.row).collect()))
        }.await;
        match result {
            Err(error) if error.category != AnomalyCategory::Interrupted => Ok(None),
            result => result,
        }
    }

    async fn match_answers(&self, request: &ScanRequest<'_>, column: &str, query: &str) -> Result<Option<MatchAnswers>> {
        if request.storage.prepaid_metadata_budget().is_some() { return Ok(None); }
        if request.scan.top_k.is_some() && request.scan.iids.as_ref().is_some_and(|iids| iids.len() <= 128) {
            return Ok(None);
        }
        let Some(definition) = self.definition(request, column).await? else { return Ok(None) };
        if tree::logical_runs(&self.runtime, request, &definition).is_some() { return Ok(None); }
        let query = Query::parse(query);
        let files = match self.trie_files(request, &definition, column, &[&query], LoadSearch::Match, None).await? {
            TrieFiles::Complete(found) | TrieFiles::Partial(found) => found.files,
        };
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

    fn live_search_terms(&self, request: &ScanRequest<'_>, column: &str) -> Option<Arc<dyn rtdb_query::LiveSearchTerms>> {
        self.runtime.serving_terms(request, column)
    }
}

#[derive(Clone, Copy, Default)]
struct Narrowing<'a> {
    is_oracle: bool,
    baseline: bool,
    skip_retry: bool,
    estimate_first: bool,
    omit_packs: bool,
    omit_logical: bool,
    scope: Option<&'a BTreeSet<Iid>>,
}

#[derive(Default)]
struct NarrowingWork {
    scores: Option<rtdb_query::CandidateScores>,
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
    tree_roots: usize,
    logical_roots: usize,
    tree_expansions: usize,
    uncovered_live_rows: usize,
    retained_rank_rows: usize,
    packed_resolved: usize,
    deferred_payloads: usize,
    shared_cutoff_prunes: usize,
    tree_io: crate::file::TreeIoStats,
    tree_decline: Option<&'static str>,
}

impl NarrowingWork {
    fn budget_exceeded(&self) -> bool {
        matches!(self.decline, Some("candidate-budget" | "verification-budget"))
    }
}

impl SearchAccelerator {
    async fn match_definition<'a>(&self, request: &ScanRequest<'a>) -> Result<Option<(&'a str, Query, SearchDefinition)>> {
        let scan = request.scan;
        let mut conjuncts = Vec::new();
        for predicate in &scan.predicates { match_conjuncts(scan, predicate, &mut conjuncts); }
        for (column, query) in conjuncts {
            let Some(Value::Utf8(text)) = request.constants.constant(query).await? else { continue; };
            if let Some(definition) = self.definition(request, column).await? {
                return Ok(Some((column, Query::parse(&text), definition)));
            }
        }
        Ok(None)
    }

    async fn narrow(&self, request: &ScanRequest<'_>, options: Narrowing<'_>, work: &mut NarrowingWork) -> Result<Option<BTreeSet<Iid>>> {
        let scan = request.scan;
        let Some((column, query, definition)) = self.match_definition(request).await? else { return Ok(None); };
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
                if work.retry_redundant || matches!(work.decline, Some("posting-estimate" | "ranked-file"))
                    || (options.skip_retry && work.budget_exceeded()) {
                    return Ok(None);
                }
            }
        }
        if let Some(scope) = options.scope {
            return Ok(Some(scope.clone()));
        }
        if !options.omit_packs && !options.omit_logical && let Some(found) = self.logical_candidates(request, &definition, column, &query, work).await? {
            return Ok(Some(found));
        }
        let TrieFiles::Complete(Found { files, skipped }) = self.trie_files(request, &definition, column, &[&query], LoadSearch::Match, None).await? else {
            work.plain_decline = Some("coverage");
            return Ok(None);
        };
        let cutter = Cutter::new(&files, &skipped, request);
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
struct Cutter<'a> { files: &'a [TrieFile], skipped: &'a [rtdb_format::keys::TrieKey], segments: Vec<&'a Arc<rtdb_index::LiveTable>>, bounds: TemporalBounds }

impl<'a> Cutter<'a> {
    fn new(files: &'a [TrieFile], skipped: &'a [rtdb_format::keys::TrieKey], request: &'a ScanRequest<'_>) -> Self {
        Self { files, skipped, segments: request.snapshot.iter_segments(&request.scan.table.name.table).collect(), bounds: request.bounds }
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
        for segment in &self.segments {
            tied |= cut(&mut rectangles, segment.iid_headers(Iid(doc.iid))?.map(crate::file::KeyedEvent::from));
        }
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
fn cut(rectangles: &mut Vec<[i64; 4]>, events: impl IntoIterator<Item = crate::file::KeyedEvent>) -> bool {
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
        let Some(entry) = snapshot.table_entry(table) else { return Ok(None) };
        let formats = [crate::file::FORMAT_VERSION, crate::file::LEGACY_FORMAT_VERSION, crate::file::TANTIVY_FORMAT_VERSION, crate::file::TREE_FORMAT_VERSION, crate::file::PACKED_FORMAT_VERSION];
        let Some(record) = formats.into_iter().find_map(|format| {
            trie.artefact(KIND, &definition.artefact_id_for(format), definition.version)
        }) else { return Ok(None) };
        let owner = ArtefactOwner::of(&trie.key, record)?;
        let member = owner.tries().iter().position(|key| key == &trie.key)
            .ok_or_else(|| Anomaly::fault("rtdb.search/artefact-owner", "Search artefact does not contain its recorded trie"))? as u32;
        let locator = record.stats.as_deref().and_then(crate::file::decode_footer_locator)
            .filter(|locator| i64::try_from(locator.length).ok() == record.file_size);
        let sections = record.stats.as_deref().and_then(crate::file::decode_sections);
        let tree = record.stats.as_deref().and_then(crate::file::decode_tree_locator)
            .filter(|locator| i64::try_from(locator.length).ok() == record.file_size);
        Ok(Some(Recorded {
            key: ObjectKey::new(entry.slug.recorded_artefact_key(&trie.key, record)?)?,
            artefact_id: record.definition_id().to_owned(), owner, member, locator, tree, sections,
        }))
    }

    async fn open_recorded(runtime: &crate::SearchRuntime, database: &str, storage: &Arc<rtdb_storage::cache::BufferPool>,
        recorded: &Recorded, definition: &SearchDefinition, identity: &str) -> std::result::Result<Arc<SearchFile>, crate::runtime::FileError> {
        if let Some(locator) = &recorded.tree {
            return runtime.materialized_tree(database, storage, &recorded.key, &definition.columns, identity, definition.version, locator).await;
        }
        runtime.file_with_locator(database, storage, &recorded.key, &definition.columns, recorded.locator.as_ref()).await
    }

    /// A trie's recorded search file, or `None` where it cannot be read; one that is missing or corrupt is reported to the lane.
    async fn recorded(
        &self, request: &ScanRequest<'_>, recorded: &Recorded, definition: &SearchDefinition,
        column: &str, queries: &[&Query], load: LoadSearch,
    ) -> Result<Option<Arc<SearchFile>>> {
        let table = &request.scan.table.name.table;
        let entry = request.snapshot.table_entry(table).ok_or_else(uncovered_owned_search)?;
        let identity = crate::file::artefact_identity(entry.slug.as_str(), &recorded.owner, &recorded.artefact_id, definition.version);
        let error = match Self::open_recorded(&self.runtime, request.database, request.storage, recorded, definition, &identity).await {
            Ok(file) if file.matches_artefact(&identity, definition.version) => {
                if matches!(load, LoadSearch::Rank) && !file.needs_ranked_postings() { return Ok(Some(file)); }
                match file.prepare_terms(column, queries).await.and_then(|()| file.check_terms(column, queries)) {
                    Ok(()) => return Ok(Some(file)),
                    Err(error) => crate::runtime::FileError::from_anomaly(error).needs_repair(),
                }
            }
            Ok(_) => true,
            Err(error) => error.needs_repair(),
        };
        if error {
            self.runtime.report(Suspect {
                database: request.database.to_string(), table: table.clone(), owner: recorded.owner.clone(),
                artefact_id: recorded.artefact_id.clone(), version: definition.version,
            });
        }
        Ok(None)
    }

    /// The search files of the partition's live tries that can hold a visible event: every such trie's, its recorded one or one this node builds from its data file, unless the tries the lane should have indexed but has not exceed max_unindexed_bytes.
    /// The lane never indexes level 0, the partial level-1 current trie, or history for a definition that excludes it, so those do not count.
    async fn trie_files(
        &self, request: &ScanRequest<'_>, definition: &SearchDefinition, column: &str, queries: &[&Query], load: LoadSearch, exclude: Option<&BTreeSet<rtdb_format::keys::TrieKey>>,
    ) -> Result<TrieFiles> {
        let snapshot = request.snapshot;
        let bounds = request.bounds;
        let table = &request.scan.table.name.table;
        let Some(entry) = snapshot.table_entry(table) else { return Ok(TrieFiles::Complete(Found { files: Vec::new(), skipped: Vec::new() })) };
        let mut skipped = Vec::new();
        let mut files = Vec::new();
        let mut loads = BTreeMap::new();
        let mut selected = Vec::new();
        let mut unbuilt = Vec::new();
        let mut unindexed = 0i64;
        let earliest = bounds.valid_time.lower().0.min(bounds.system_time.lower().0);
        let visible = |trie: &&rtdb_index::CatalogTrie| trie.key.recency().is_none_or(|date| date.epoch_day().saturating_mul(86_400_000_000) > earliest);
        for trie in snapshot.trie_catalog().current_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).filter(visible) {
            if exclude.is_some_and(|keys| keys.contains(&trie.key)) { continue; }
            let recorded = Self::recorded_key(snapshot, table, trie, definition)?;
            if recorded.as_ref().and_then(|recorded| recorded.sections.as_ref())
                .is_some_and(|sections| !sections.iter().any(|section| section.meets(bounds))) {
                skipped.push(trie.key.clone());
                continue;
            }
            let data = ObjectKey::new(entry.slug.data_key(&trie.key))?;
            let published = recorded.map(|recorded| {
                let published = (recorded.key.clone(), recorded.member);
                loads.entry(recorded.key.clone()).or_insert(recorded);
                published
            });
            selected.push((trie, data, published));
        }
        let opened: BTreeMap<_, _> = futures::stream::iter(loads.into_values()).map(|recorded| async move {
            let file = self.recorded(request, &recorded, definition, column, queries, load).await?;
            Ok::<_, Anomaly>((recorded.key, file))
        }).buffer_unordered(8).try_collect().await?;
        for (trie, data, published) in selected {
            if let Some((key, member)) = published
                && let Some(file) = opened.get(&key).and_then(Option::as_ref) {
                files.push(TrieFile { trie: trie.key.clone(), data, identity: key.as_str().to_string(), file: Arc::clone(file), member });
                continue;
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
            let complete = self.runtime.visit_live_matches_at(Some(request.snapshot), request.database, segment, column, query, true, request.cancellation, |row, _| {
                let header = segment.header(row)?;
                if may_be_visible(header.valid_time.lower().0, header.valid_time.upper().0, header.system_from.0, Timestamp::MAX.0, request.bounds) {
                    candidates.insert(header.iid);
                }
                Ok(candidates.len() <= self.max_candidates)
            })?;
            if !complete { break; }
        }
        Ok(candidates)
    }
}

/// Where a trie's recorded search file is, and which of its members the trie is.
struct Recorded {
    key: ObjectKey,
    owner: ArtefactOwner,
    member: u32,
    artefact_id: String,
    locator: Option<crate::file::FooterLocator>,
    tree: Option<crate::file::TreeLocator>,
    sections: Option<Vec<crate::file::Section>>,
}

#[derive(Clone, Copy)]
enum LoadSearch { Match, Rank }

/// A shared file's selected members and their positions in the scan's trie list.
struct FileMembers { file: Arc<SearchFile>, members: BTreeMap<u32, usize> }

fn by_file(files: &[TrieFile]) -> impl Iterator<Item = FileMembers> {
    let mut grouped = BTreeMap::new();
    for (index, trie) in files.iter().enumerate() {
        let selected = grouped.entry(trie.identity.as_str()).or_insert_with(|| FileMembers { file: Arc::clone(&trie.file), members: BTreeMap::new() });
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
    let query = Query::parse(text);
    let covered = covered_owned_corpus(context, owner, relation, path, &query, None, false, None).await?;
    Ok(rtdb_text::Corpus {
        documents: covered.documents, tokens: covered.tokens,
        frequencies: query.terms().iter().cloned().zip(covered.frequencies.values().iter().copied()).collect(),
    })
}

struct CoveredCorpus {
    column: String,
    bounds: TemporalBounds,
    documents: u64,
    tokens: u64,
    frequencies: UInt64Array,
    visible_runs: Vec<(Arc<SearchFile>, UInt32Array)>,
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
            column: column.clone(), bounds, documents: 0, tokens: 0,
            frequencies: UInt64Array::from(vec![0; query.terms().len()]), visible_runs: Vec::new(),
            _scratch: context.memory.reserve(4096)?,
        });
    }
    if snapshot.iter_segments(&definition.table).any(|segment| segment.row_count() != 0) {
        return Err(uncovered_owned_search());
    }
    let runtime = crate::SearchRuntime::for_query(Arc::clone(&context.memory));
    let mut files = Vec::new();
    let mut opened = BTreeMap::<ObjectKey, Arc<SearchFile>>::new();
    let entry = snapshot.table_entry(&definition.table).ok_or_else(uncovered_owned_search)?;
    for trie in snapshot.trie_catalog().current_tries(&definition.table) {
        context.check_cancelled()?;
        let recorded = SearchAccelerator::recorded_key(snapshot, &definition.table, trie, &definition)?
            .ok_or_else(uncovered_owned_search)?;
        let file = if let Some(file) = opened.get(&recorded.key) {
            Arc::clone(file)
        } else {
            let identity = crate::file::artefact_identity(entry.slug.as_str(), &recorded.owner, &recorded.artefact_id, definition.version);
            let file = SearchAccelerator::open_recorded(&runtime, database, &captured.storage, &recorded, &definition, &identity).await
                .map_err(|_| uncovered_owned_search())?;
            file.prepare_terms(column, &[query]).await.map_err(|_| uncovered_owned_search())?;
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

    let cutter = Cutter { files: &files, skipped: &[], segments: Vec::new(), bounds };
    let mut documents = 0;
    let mut tokens = 0;
    let mut frequencies = vec![0; query.terms().len()];
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
                let id = selected.file.run_identity_key(doc)?;
                match cutter.judge_key(own, doc, Some(key))? {
                    Judged::Hidden => (),
                    Judged::Visible { exact: false } => return Err(uncovered_owned_search()),
                    Judged::Visible { exact: true } => {
                        if shadowed.is_some_and(|ids| ids.iter().any(|value| EntityIdRef::from(value) == id)) { continue; }
                        if !seen.insert(key.to_vec()) { return Err(uncovered_owned_search()); }
                        visible.insert(doc.doc);
                        documents += 1;
                        tokens += u64::from(selected.file.token_length(doc.doc)?);
                    }
                }
            }
        }
        for (term, frequency) in query.terms().iter().zip(&mut frequencies) {
            let Some(postings) = selected.file.postings(column, term).map_err(|_| uncovered_owned_search())? else { continue; };
            *frequency += postings.documents().iter().filter(|doc| visible.contains(doc)).count() as u64;
        }
        if retain_runs && !visible.is_empty() {
            visible_runs.push((Arc::clone(&selected.file), UInt32Array::from_iter_values(visible)));
        }
    }
    context.check_cancelled()?;
    Ok(CoveredCorpus {
        column: column.clone(), bounds, documents, tokens, frequencies: UInt64Array::from(frequencies),
        visible_runs, _scratch: scratch,
    })
}

/// One exact match on a held owned search path, identified by its full typed key.
#[derive(Clone, Debug, PartialEq)]
pub struct OwnedSearchHit {
    pub partition: usize,
    pub id: EntityId,
    pub score: f64,
}

fn owned_rank_key(left_partition: usize, left_id: EntityIdRef<'_>, left_score: f64,
    right_partition: usize, right_id: EntityIdRef<'_>, right_score: f64) -> std::cmp::Ordering {
    left_score.total_cmp(&right_score).reverse()
        .then_with(|| left_id.cmp(&right_id))
        .then_with(|| left_partition.cmp(&right_partition))
}

/// The requested rank window and the exact number of verified matches.
pub struct OwnedSearchPage {
    pub hits: Vec<OwnedSearchHit>,
    pub total: usize,
    pub(crate) _reservation: Arc<rtdb_columnar::MemoryReservation>,
}

/// The verified hit columns and the exact match count before slicing.
pub struct OwnedSearchBatch {
    pub hits: OwnedBatch,
    pub total: usize,
}

impl OwnedSearchBatch {
    pub fn into_page(self) -> Result<OwnedSearchPage> {
        use arrow_array::{cast::AsArray, types::{Float64Type, UInt64Type}};
        let batch = self.hits.batch();
        let schema = batch.schema_ref();
        let ids = batch.column(0).as_ref();
        let scores = batch.column(1).as_primitive::<Float64Type>();
        let partitions = batch.column(2).as_primitive::<UInt64Type>();
        let hits = (0..batch.num_rows()).map(|row| {
            let id = rtdb_columnar::read_entity_id(schema.field(0), ids, row)?.into_owned();
            Ok(OwnedSearchHit { partition: partitions.value(row) as usize, id, score: scores.value(row) })
        }).collect::<Result<Vec<_>>>()?;
        Ok(OwnedSearchPage { hits, total: self.total, _reservation: Arc::clone(self.hits.reservation()) })
    }
}

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
    owned_ranked_overlay_batch(context, owner, partitions, text, offset, limit, valid_at, overlay).await?.into_page()
}

/// Rank the admitted archive and overlay into verified Arrow hit columns.
pub async fn owned_ranked_overlay_batch(
    context: Arc<QueryContext>, owner: &str, partitions: &[(OwnedRelation, OwnedRelation)], text: &str,
    offset: usize, limit: usize, valid_at: Option<Timestamp>, overlay: Option<crate::HeldTextCorpus>,
) -> Result<OwnedSearchBatch> {
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
            rank.finish_batch()
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
            corpus.finish_batch(offset, limit)
        }
        Err(error) => Err(error),
    }
}

async fn rank_owned_artefacts(
    context: &Arc<QueryContext>, owner: &str, partitions: &[(OwnedRelation, OwnedRelation)], query: &Query,
    offset: usize, limit: usize, valid_at: Option<Timestamp>, overlay: Option<&crate::HeldTextCorpus>,
) -> Result<(rtdb_text::Scorer, OwnedRankWindow)> {
    let mut documents = 0;
    let mut tokens = 0;
    let mut frequencies = vec![0; query.terms().len()];
    let mut covered = Vec::with_capacity(partitions.len());
    for (partition, (relation, path)) in partitions.iter().enumerate() {
        let shadowed = overlay.and_then(|part| part.shadowed(partition));
        let part = covered_owned_corpus(Arc::clone(context), owner, relation, path, query, valid_at, true, shadowed).await?;
        documents += part.documents;
        tokens += part.tokens;
        for (total, &frequency) in frequencies.iter_mut().zip(part.frequencies.values()) { *total += frequency; }
        covered.push((relation, part));
    }
    if let Some(overlay) = overlay {
        let stats = overlay.statistics();
        documents += stats.documents;
        tokens += stats.tokens;
        for (term, total) in query.terms().iter().zip(&mut frequencies) {
            *total += stats.frequencies.get(term).copied().unwrap_or(0);
        }
    }
    let scorer = rtdb_text::Scorer::from_frequencies(query.clone(), documents, tokens, &frequencies);
    let mut rank = OwnedRankWindow::new(&context.memory, offset, limit)?;
    for (partition, (relation, covered)) in covered.iter().enumerate() {
        for (file, visible) in &covered.visible_runs {
            context.check_cancelled()?;
            let mut matches = file.matched_scores(&covered.column, &scorer, covered.bounds, Some(visible.values()))
                .map_err(|_| uncovered_owned_search())?;
            loop {
                let available = OWNED_RANK_CANDIDATES - rank.total;
                if available == 0 {
                    if matches.next().is_some() { rank.admit_candidate()?; }
                    break;
                }
                let mut pending = Vec::new();
                for (run, score) in matches.by_ref().take(128.min(available)) {
                    context.check_cancelled()?;
                    rank.admit_candidate()?;
                    let key = file.run_identity_buffer(run)?;
                    let id = file.run_identity_key(run)?;
                    pending.push(OwnedCandidate { run, key, id, score, verified: None });
                }
                if pending.is_empty() { break; }
                let keys = pending.iter().map(|candidate| (Iid(candidate.run.iid), candidate.key.clone())).collect::<Vec<_>>();
                let Some(mut rows) = rtdb_query::open_owned_artefact_candidates(
                    Arc::clone(context), owner, relation, covered.bounds.valid_time, &covered.column, keys,
                ).await? else { return Err(uncovered_owned_search()); };
                let mut locators = BTreeMap::<Iid, Vec<usize>>::new();
                for (index, candidate) in pending.iter().enumerate() {
                    locators.entry(Iid(candidate.run.iid)).or_default().push(index);
                }
                while let Some(batch) = rows.next_batch().await? {
                    let batch = batch.batch();
                    let schema = batch.schema_ref();
                    let (column, field) = schema.column_with_name(&covered.column).ok_or_else(uncovered_owned_search)?;
                    let (id_column, id_field) = schema.column_with_name("_id").ok_or_else(uncovered_owned_search)?;
                    let iids = batch.column_by_name("_iid").ok_or_else(uncovered_owned_search)?;
                    let iids = rtdb_columnar::IidView::new(iids.as_ref())?;
                    let logical = rtdb_columnar::from_field(field)?;
                    for row in 0..batch.num_rows() {
                        context.check_cancelled_at(row)?;
                        let ids = batch.column(id_column).as_ref();
                        let fallback;
                        let id = match rtdb_columnar::read_entity_id(id_field, ids, row) {
                            Ok(id) => id,
                            Err(_) => {
                                fallback = rtdb_columnar::read_value(id_field, ids, row)?.entity_id()?;
                                EntityIdRef::from(&fallback)
                            }
                        };
                        let iid = iids.get(row)?;
                        let Some(&index) = locators.get(&iid).and_then(|indices| indices.iter().find(|&&index| pending[index].id == id)) else {
                            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.owned/iid-collision", "Owned row locator resolves to a different complete identity"));
                        };
                        let candidate = &mut pending[index];
                        if candidate.verified.is_some() { return Err(uncovered_owned_search()); }
                        let revision = rtdb_query::owned_row_revision_for(batch, row, candidate.id, &context.memory)?;
                        let at = covered.bounds.valid_time.lower().0;
                        let mut same_run = false;
                        let mut overlapping = 0;
                        for entry in file.rows_for_key(&candidate.run.iid, candidate.key.as_slice()) {
                            let (entry, event) = entry?;
                            if event.system_from == revision.0 && event.valid_from <= at && at < event.valid_to {
                                overlapping += 1;
                                same_run |= event.op == EventOp::Put
                                    && file.run_of(&covered.column, entry) == Some(Some(candidate.run.doc));
                            }
                        }
                        if !same_run { return Err(uncovered_owned_search()); }
                        let PutText::Utf8(text) = rtdb_columnar::text_cell(&logical, field, batch.column(column).as_ref(), row)?
                            else { return Err(uncovered_owned_search()); };
                        let score = if overlapping == 1 {
                            candidate.score
                        } else {
                            let _scratch = context.memory.reserve(text.len().saturating_mul(64).saturating_add(4096))?;
                            let (count, frequencies, matched) = query.text_terms(text, true);
                            if !matched || count.len() != file.token_length(candidate.run.doc)? as usize {
                                return Err(uncovered_owned_search());
                            }
                            scorer.score(&frequencies, rtdb_text::stored_length(count.len()))
                        };
                        if !score.is_finite() { return Err(uncovered_owned_search()); }
                        candidate.verified = Some(score);
                    }
                }
                for candidate in pending {
                    let score = candidate.verified.ok_or_else(uncovered_owned_search)?;
                    if let Some(at) = rank.position(partition, candidate.id, score) {
                        rank.insert_at(at, OwnedSearchHit { partition, id: candidate.id.into_owned(), score })?;
                    }
                }
            }
        }
    }
    Ok((scorer, rank))
}

struct OwnedCandidate<'a> {
    run: crate::DocMatch,
    key: arrow_buffer::Buffer,
    id: EntityIdRef<'a>,
    score: f64,
    verified: Option<f64>,
}

const OWNED_RANK_CANDIDATES: usize = 100_000;

pub(crate) struct OwnedRankWindow {
    hits: Vec<OwnedSearchHit>,
    memory: Arc<rtdb_columnar::MemoryReservation>,
    offset: usize,
    window: usize,
    total: usize,
    bytes: usize,
}

impl OwnedRankWindow {
    pub(crate) fn new(memory: &Arc<rtdb_columnar::MemoryBudget>, offset: usize, limit: usize) -> Result<Self> {
        let window = offset + limit;
        let memory = memory.reserve(4096 + (window + 1) * size_of::<OwnedSearchHit>())?;
        Ok(Self { hits: Vec::with_capacity(window + 1), memory, offset, window, total: 0, bytes: 4096 })
    }

    pub(crate) fn admit_candidate(&mut self) -> Result<()> {
        self.total += 1;
        if self.total > OWNED_RANK_CANDIDATES {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.owned/search-candidate-limit", "Covered search exceeded its candidate budget"));
        }
        Ok(())
    }

    pub(crate) fn position(&self, partition: usize, id: EntityIdRef<'_>, score: f64) -> Option<usize> {
        let at = self.hits.partition_point(|previous| {
            owned_rank_key(previous.partition, EntityIdRef::from(&previous.id), previous.score, partition, id, score).is_lt()
        });
        (at < self.window).then_some(at)
    }

    pub(crate) fn insert_at(&mut self, at: usize, hit: OwnedSearchHit) -> Result<()> {
        let added = owned_id_bytes(EntityIdRef::from(&hit.id)).saturating_add(128);
        if self.hits.len() == self.window {
            let evicted = self.hits.pop().expect("A full rank window has a last hit");
            self.bytes -= owned_id_bytes(EntityIdRef::from(&evicted.id)).saturating_add(128);
        }
        self.hits.insert(at, hit);
        self.bytes = self.bytes.saturating_add(added);
        let reservation = Arc::get_mut(&mut self.memory).expect("ranking owns its reservation");
        reservation.grow_to(self.bytes.max(reservation.bytes()))
    }

    pub(crate) fn finish_batch(self) -> Result<OwnedSearchBatch> {
        let hits = &self.hits[self.offset.min(self.hits.len())..];
        let bound = hits.iter().try_fold(4096usize, |size, hit| {
            size.checked_add(owned_id_bytes(EntityIdRef::from(&hit.id)).saturating_mul(4).saturating_add(128))
                .ok_or_else(uncovered_owned_search)
        })?;
        let reservation = self.memory.budget().reserve(bound)?;
        let schema = Arc::new(Schema::new(vec![
            rtdb_columnar::to_field("_id", &rtdb_columnar::entity_id_type())?,
            Field::new("score", DataType::Float64, false),
            Field::new("partition", DataType::UInt64, false),
        ]));
        let ids = rtdb_columnar::entity_id_array(hits.iter().map(|hit| EntityIdRef::from(&hit.id)))?;
        let score = Arc::new(Float64Array::from_iter_values(hits.iter().map(|hit| hit.score)));
        let partition = Arc::new(UInt64Array::from_iter_values(hits.iter().map(|hit| hit.partition as u64)));
        let output = RecordBatch::try_new(schema, vec![ids, score, partition])
            .map_err(|error| Anomaly::fault("rtdb.search/ranked-output", error.to_string()))?;
        Ok(OwnedSearchBatch { hits: OwnedBatch::with_reservation(output, reservation)?, total: self.total })
    }
}

fn owned_id_bytes(id: EntityIdRef<'_>) -> usize {
    size_of::<Value>().saturating_add(id.owned_payload_bytes())
}

fn file_candidates(cutter: &Cutter<'_>, column: &str, query: &Query, max_candidates: usize, events: &mut usize) -> Result<Option<BTreeSet<Iid>>> {
    let mut candidates = BTreeSet::new();
    for selected in by_file(cutter.files) {
        let docs = selected.file.matched_runs(column, query, cutter.bounds)?;
        if cfg!(feature = "experiments") { *events = events.saturating_add(docs.len()); }
        for doc in docs.iter() {
            let Some(&own) = selected.members.get(&doc.member) else { continue };
            if !matches!(cutter.judge(own, doc)?, Judged::Hidden) { candidates.insert(Iid(doc.iid)); }
        }
        if candidates.len() > max_candidates { return Ok(None); }
    }
    Ok(Some(candidates))
}

/// A scored event waiting to be handed out, best score first, and whether its visibility is exact.
struct Pending { key: RankKey, iid: Iid }
impl PartialEq for Pending { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Pending {}
impl PartialOrd for Pending { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for Pending {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.compare(other.key).then_with(|| other.iid.cmp(&self.iid))
    }
}

/// The IIDs of the matching events that may be visible, each at its best event's score, best first.
/// Search files are expanded a batch of windows at a time, only while one may hold an event above every event already scored.
struct Candidates<'a> {
    files: Vec<(BTreeMap<u32, usize>, crate::file::RankedSource)>,
    cutter: Cutter<'a>,
    pending: std::collections::BinaryHeap<Pending>,
    seen: BTreeSet<Iid>,
    scored: usize,
    max_candidates: usize,
    scratch: Arc<rtdb_columnar::MemoryReservation>,
}

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
        !seen.contains(&candidate.iid) && distinct.insert(candidate.iid) && distinct.len() > remaining
    })
}

impl Candidates<'_> {
    fn push(&mut self, iid: Iid, score: f64, id: Option<i64>) {
        self.scored += 1;
        self.pending.push(Pending { key: RankKey::integer(score, id), iid });
    }

    fn drop_seen(&mut self) { while self.pending.peek().is_some_and(|top| self.seen.contains(&top.iid)) { self.pending.pop(); } }
    /// The file whose next windows may hold the best event not yet scored.
    fn best_file(&self) -> Option<(usize, RankKey)> {
        self.files.iter().enumerate().filter_map(|(index, (_, file))| file.bound().map(|bound| (index, bound)))
            .max_by(|left, right| left.1.compare(right.1))
    }
    fn can_stop(&mut self, kth: RankKey) -> bool {
        self.drop_seen();
        let pending = self.pending.peek().map(|top| top.key);
        let unread = self.best_file().map(|(_, bound)| bound);
        if !kth.has_id() {
            return crate::rank_cutoff::can_stop(kth.score.to_bits(), pending.map(|key| key.score.to_bits()), unread.map(|key| key.score.to_bits()));
        }
        pending.into_iter().chain(unread).all(|bound| kth.beats(bound))
    }
    /// The next IID, best first, with its best event's score.
    async fn next(&mut self, scope: Option<&BTreeSet<Iid>>, ascending_id: bool, work: &mut NarrowingWork)
        -> std::result::Result<Option<Pending>, (Option<usize>, Anomaly)> {
        loop {
            if self.scored > self.max_candidates { return Ok(None); }
            self.drop_seen();
            match (self.pending.peek().map(|top| top.key), self.best_file()) {
                (pending, Some((index, bound))) if pending.is_none_or(|key| bound.compare(key).is_gt()) => {
                    let scored = self.files[index].1.expand(scope, &mut work.postings_visited).await
                        .map_err(|error| (self.files[index].0.values().next().copied(), error))?;
                    reserve_ranking(&mut self.scratch, self.scored.saturating_add(scored.len())).map_err(|error| (None, error))?;
                    if cfg!(feature = "experiments") { work.score_evaluations += scored.len(); }
                    for (doc, score) in scored.iter() {
                        let Some(&own) = self.files[index].0.get(&doc.member) else { continue };
                        if let Judged::Visible { .. } = self.cutter.judge(own, doc).map_err(|error| (None, error))? {
                            let id = ascending_id.then(|| self.cutter.files[own].file.integer_id(doc)).flatten();
                            self.push(Iid(doc.iid), score, id);
                        }
                    }
                }
                _ => {
                    let Some(pending) = self.pending.pop() else { return Ok(None) };
                    self.seen.insert(pending.iid);
                    return Ok(Some(pending));
                }
            }
        }
    }
}

pub(crate) fn search_cancelled() -> Anomaly {
    Anomaly::new(AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search query cancelled")
}

fn reserve_ranking(reservation: &mut Arc<rtdb_columnar::MemoryReservation>, candidates: usize) -> Result<()> {
    let bytes = 4096usize.saturating_add(candidates.saturating_mul(128));
    let reservation = Arc::get_mut(reservation).expect("Ranking owns its allocation");
    if reservation.bytes() < bytes { reservation.grow_to(bytes)?; }
    Ok(())
}

fn verified_cutoff(rows: &mut [(RankKey, Iid)], limit: usize) -> Option<RankKey> {
    rows.sort_unstable_by(|left, right| right.0.compare(left.0).then_with(|| left.1.cmp(&right.1)));
    let mut kth = rows.get(limit - 1)?.0;
    if rows.iter().any(|(key, _)| !key.has_id() && key.score_order(kth).is_eq()) {
        kth = kth.score_only();
    }
    Some(kth)
}

impl SearchAccelerator {
    /// The IIDs that can hold one of the `limit` best-scoring visible rows, or `None` to narrow as an unranked MATCH does.
    /// Every matching event is scored exactly, and an IID's best event bounds every row it can resolve to.
    async fn top_k(&self, request: &ScanRequest<'_>, top_k: &rtdb_plan::ScanTopK, column: &str, matching: &Query, definition: &SearchDefinition, options: Narrowing<'_>, work: &mut NarrowingWork) -> Result<Option<BTreeSet<Iid>>> {
        let Some(Value::Utf8(text)) = request.constants.constant(&top_k.query).await? else { work.decline = Some("unsupported-order"); return Ok(None) };
        let Some(limit) = request.ranked_prefix_len(top_k).await else { work.decline = Some("unsupported-limit"); return Ok(None) };
        if limit == 0 { return Ok(Some(BTreeSet::new())); }
        let scoring_query = Query::parse(&text);
        if !options.estimate_first && !options.baseline {
            let (ranked, io) = crate::file::observe_tree_io(Box::pin(async {
                let ranking = rtdb_query::QueryRanking::new(std::num::NonZeroUsize::new(limit).expect("nonempty prefix"), 1, request.memory)?;
                let source = Box::pin(tree::PartitionRankSource::open(self, *request, definition.clone(), column, top_k.column, matching.clone(),
                    scoring_query.clone(), top_k.ascending_id, limit, options));
                let Some(source) = rtdb_query::run_rank_partition(&ranking, 0, source, request.cancellation).await?
                    else { return Ok(None); };
                let tried_ranking = work.tried_ranking;
                let filter_scope = work.filter_scope;
                *work = source.work;
                work.tried_ranking = tried_ranking;
                work.filter_scope = filter_scope;
                let Some(mut ranked) = ranking.winners() else { return Ok(None); };
                request.ranked_selection(ranked.take_partition(0)).await
            })).await;
            work.tree_io = io;
            if let Some(selection) = ranked? {
                let iids = selection.iids.iter().collect();
                work.retained_rank_rows = selection.iids.iter().filter(|iid| selection.scores.as_ref().and_then(|scores| scores.row(*iid)).is_some()).count();
                work.scores = selection.scores;
                return Ok(Some(iids));
            }
        }
        let ascending_id = top_k.ascending_id.filter(|&id| request.ranked_id_is_integer(id)
            && request.bounds.valid_time.is_point() && request.bounds.system_time.is_point());
        let TrieFiles::Complete(Found { files, skipped }) = self.trie_files(request, definition, column, &[matching, &scoring_query], LoadSearch::Rank, None).await? else {
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
        let scoring = Arc::new(rtdb_text::Scorer::new(scoring_query, None));
        let Some(rank_scorer) = scoring.for_ranking() else { work.decline = Some("score-domain"); return Ok(None); };
        let ranked = by_file(&files).map(|selected| {
            Ok((selected.members, selected.file.ranked_for_query(column, matching, &rank_scorer, request.bounds, ascending_id.is_some())?))
        }).collect::<Result<Vec<_>>>()?;
        let mut candidates = Candidates {
            files: ranked, cutter: Cutter::new(&files, &skipped, request), pending: std::collections::BinaryHeap::new(),
            seen: BTreeSet::new(), scored: 0, max_candidates: self.max_candidates, scratch: request.memory.reserve(4096)?,
        };
        for segment in request.snapshot.iter_segments(&request.scan.table.name.table) {
            let complete = self.runtime.visit_live_matches_at(Some(request.snapshot), request.database, segment, column, matching, true, request.cancellation, |row, tokens| {
                if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
                if cfg!(feature = "experiments") { work.postings_visited += 1; }
                let header = segment.header(row)?;
                if options.scope.is_some_and(|scope| !scope.contains(&header.iid))
                    || !may_be_visible(header.valid_time.lower().0, header.valid_time.upper().0, header.system_from.0, Timestamp::MAX.0, request.bounds) {
                    return Ok(true);
                }
                if cfg!(feature = "experiments") { work.score_evaluations += 1; }
                let id = if ascending_id.is_some() {
                    let source = segment.row(row)?;
                    crate::lane::integer_sql_id(&source.batch.view().borrow_event(source.row)?)
                } else { None };
                if candidates.scored % 128 == 0 {
                    reserve_ranking(&mut candidates.scratch, candidates.scored.saturating_add(128))?;
                }
                candidates.push(header.iid, scoring.bm25_arrow(&tokens()), id);
                Ok(candidates.scored <= self.max_candidates)
            })?;
            if !complete {
                work.decline = Some("candidate-budget");
                return Ok(None);
            }
        }
        let mut verified: Vec<(RankKey, Iid)> = Vec::new();
        let batch = if ascending_id.is_some() { limit.min(128) } else { limit.saturating_mul(2).clamp(64, 512) };

        loop {
            if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
            reserve_ranking(&mut candidates.scratch, candidates.scored.saturating_add(batch))?;
            if verified.len() >= limit && verified_cutoff(&mut verified, limit).is_some_and(|kth| candidates.can_stop(kth)) { break; }
            let mut iids = BTreeSet::new();
            let mut handed = 0;
            while handed < batch {
                let next = request.cancellation.run_until_cancelled(candidates.next(options.scope, ascending_id.is_some(), work))
                    .await.ok_or_else(search_cancelled)?;
                let pending = match next {
                    Err((index, error)) if optional_build_refused(&error) || error.code.starts_with("rtdb.search/")
                        || error.code.starts_with("rtdb.storage/") => {
                        if error.code == "rtdb.storage/memory-limit" { self.runtime.release_file_cache(); }
                        if crate::runtime::FileError::from_anomaly(error).needs_repair()
                            && let Some(file) = index.and_then(|index| files.get(index)) {
                            let table = &request.scan.table.name.table;
                            if let Some(recorded) = request.snapshot.trie_catalog().trie(table, &file.trie)
                                .and_then(|trie| Self::recorded_key(request.snapshot, table, trie, definition).ok().flatten())
                                .filter(|recorded| recorded.key.as_str() == file.identity) {
                                self.runtime.report(Suspect {
                                    database: request.database.to_owned(), table: table.clone(), owner: recorded.owner,
                                    artefact_id: recorded.artefact_id, version: definition.version,
                                });
                            }
                        }
                        work.decline = Some("ranked-file");
                        return Ok(None);
                    }
                    Err((_, error)) => return Err(error),
                    Ok(pending) => pending,
                };
                let Some(Pending { iid, .. }) = pending else { break };

                handed += 1;
                if request.scan.iids.as_ref().is_some_and(|scope| !scope.contains(&iid)) { continue; }
                iids.insert(iid);
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
            let scores = request.verifier.score_visible(iids, top_k.column, ascending_id, &scoring).await?;
            verified.extend(scores.ranked().map(|(iid, score, id)| (RankKey::integer(score, id), iid)));
        }
        if let Some(kth) = verified_cutoff(&mut verified, limit) {
            if kth.has_id() { verified.truncate(limit); }
            else { verified.retain(|(key, _)| !key.score_order(kth).is_lt()); }
        }
        let iids = verified.iter().map(|&(_, iid)| iid).collect();
        if top_k.score.is_some() && request.bounds.valid_time.is_point() && request.bounds.system_time.is_point() {
            work.scores = rtdb_query::CandidateScores::from_verified(verified.into_iter().map(|(key, iid)| (iid, key.score)).collect(), request.memory)?;
        }
        Ok(Some(iids))
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
        let pending_event = |score, iid| Pending { key: RankKey::integer(score, None), iid };
        let seen = BTreeSet::from([iid(1)]);
        let mut pending = std::collections::BinaryHeap::from([
            pending_event(1.0, iid(1)), pending_event(0.5, iid(2)), pending_event(0.25, iid(2)),
        ]);
        let memory = rtdb_columnar::MemoryBudget::new(1024);
        assert!(!exceeds_retry_limit(&seen, &pending, 2, &memory));
        pending.push(pending_event(1.0, iid(3)));
        assert!(exceeds_retry_limit(&seen, &pending, 2, &memory));
        assert!(!exceeds_retry_limit(&seen, &pending, 2, &rtdb_columnar::MemoryBudget::new(1)));
        let seen = BTreeSet::from([iid(1), iid(2), iid(3)]);
        assert!(exceeds_retry_limit(&seen, &pending, 2, &rtdb_columnar::MemoryBudget::new(1)));
    }

    #[tokio::test]
    async fn shared_search_uses_only_the_selected_members_for_candidates_and_ranking() {
        use crate::file::write_group_search_file;

        let columns = vec!["body".to_string()];
        let row = |id: u8, body: &str| crate::SearchRow {
            position: None,
            identity: None, iid: [id; 16], system_from: 0, valid_from: 0, valid_to: i64::MAX,
            op: rtdb_types::EventOp::Put, values: vec![Some(rtdb_text::tokens(body))],
        };
        let mut bytes = Vec::new();
        let members = [vec![Ok(vec![row(1, "overdue invoice")])], vec![Ok(vec![row(64, "overdue")])], Vec::new(), Vec::new()];
        write_group_search_file(&columns, members, "group", None, &mut bytes).unwrap();
        let file = Arc::new(SearchFile::open(bytes, &columns).unwrap());
        let trie = |member: u32| TrieFile {
            trie: format!("l02-rc-p{member}-b00").parse().unwrap(),
            data: ObjectKey::new(format!("tables/docs/data/l02-rc-p{member}-b00.arrow")).unwrap(),
            identity: "group".into(), file: Arc::clone(&file), member,
        };
        let bounds = at(0, 0);
        let query = Query::parse("overdue");
        let files = [trie(1)];
        let cutter = Cutter { files: &files, skipped: &[], segments: Vec::new(), bounds };
        let iids = file_candidates(&cutter, "body", &query, 1, &mut 0).unwrap().unwrap();
        assert_eq!(iids, BTreeSet::from([Iid([64; 16])]));
        assert_eq!(by_file(&[trie(0), trie(1)]).count(), 1);

        let rank_scorer = Arc::new(rtdb_text::Scorer::new(query.clone(), None)).for_ranking().unwrap();
        let ranked = file.ranked("body", &query, &rank_scorer, bounds).unwrap();
        let mut candidates = Candidates {
            files: vec![(BTreeMap::from([(1, 0)]), crate::file::RankedSource::Legacy(ranked))], cutter,
            pending: Default::default(), seen: BTreeSet::new(), scored: 0, max_candidates: 10,
            scratch: rtdb_columnar::MemoryBudget::new(1 << 20).reserve(4096).unwrap(),
        };
        assert_eq!(candidates.next(None, false, &mut NarrowingWork::default()).await.unwrap().map(|row| row.iid), Some(Iid([64; 16])));
        assert!(candidates.next(None, false, &mut NarrowingWork::default()).await.unwrap().is_none());
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

    fn event(system_from: i64, valid_from: i64, valid_to: i64, op: rtdb_types::EventOp) -> crate::file::KeyedEvent { crate::file::KeyedEvent { position: None, system_from, valid_from, valid_to, op } }

    #[test]
    fn an_intervening_write_in_another_file_does_not_hide_the_later_match() {
        let columns = ["body".to_string()];
        let iid = [1; 16];
        let row = |system_from, valid_to, text: &str| crate::SearchRow {
            position: None,
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
        let query = Query::parse("apple");
        for (system, expected) in [(15, true), (25, false), (35, true)] {
            let cutter = Cutter {
                files: &files, skipped: &[], segments: Vec::new(),
                bounds: at(5, system),
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
