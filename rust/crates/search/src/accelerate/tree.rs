
use rtdb_format::keys::TrieKey;
use rtdb_format::{log::proto::{SearchRun, TermDirectoryRoot}, term_directory};
use rtdb_plan::ColumnId;
use rtdb_query::ObjectPageStore;
use rtdb_query::{RankOutcome, RankSource, RankWork};
use rtdb_text::{RankScorer, Scorer};
use tokio::sync::OnceCell;

use super::*;
use super::point::SearchWitness;
use crate::{SearchRuntime, file::{TREE_FORMAT_VERSION, TreeFile, TreeRankOrder, TreeRanked}};
use crate::term_directory::{TermRoutes, read_routes};

pub(super) struct RecordedTree {
    pub(super) pack: Arc<super::routing::Pack>,
    pub(super) members: BTreeMap<u32, TrieKey>,
    discovery: bool,
    opened: OnceCell<Arc<TreeFile>>,
    _routing: Arc<super::routing::TableRouting>,
}

impl RecordedTree {
    pub(super) fn select(runtime: &crate::SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition,
        column: usize, matching: &Query, include_packs: bool, history: bool) -> Result<Vec<Self>> {
        let routing = runtime.table_routing(request.snapshot, definition, request.storage, include_packs, history)?;
        Ok(routing.packs().filter_map(|pack| {
            let members = pack.visible_members(request.bounds);
            (!members.is_empty()).then(|| Self { pack: Arc::clone(pack), members,
                discovery: !history && pack.can_nominate(request.bounds, column, matching), opened: OnceCell::new(), _routing: Arc::clone(&routing) })
        }).collect())
    }

    pub(super) async fn open(&self, runtime: &crate::SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition) -> Result<Arc<TreeFile>> {
        self.opened.get_or_try_init(|| async {
            let entry = request.snapshot.table_entry(&definition.table).ok_or_else(uncovered_owned_search)?;
            let identity = crate::file::artefact_identity(entry.slug.as_str(), &self.pack.owner, &self.pack.definition_id, definition.version);
            match runtime.tree_file(request.storage, &self.pack.key, &self.pack.locator, &definition.columns, &identity, definition.version, false).await {
                Ok(file) => Ok(file),
                Err(crate::runtime::FileError::Missing) => Err(Anomaly::fault("rtdb.storage/missing-object", self.pack.key.as_str())),
                Err(crate::runtime::FileError::Corrupt(error) | crate::runtime::FileError::Unavailable(error)) => Err(error),
            }
        }).await.cloned()
    }

    pub(super) fn report(&self, runtime: &crate::SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition, error: &Anomaly) {
        if crate::runtime::FileError::from_anomaly(error.clone()).needs_repair() {
            runtime.report(Suspect { database: request.database.into(), table: definition.table.clone(), owner: self.pack.owner.clone(),
                artefact_id: self.pack.definition_id.clone(), version: definition.version });
        }
    }
}

enum Task {
    Open(usize),
    Expand(usize),
    OpenLogical(usize),
    Gap,
    ExpandLogical(usize),
    Legacy(usize),
    Live(usize, Arc<crate::runtime::LivePostingPage>),
    LiveSuffix(usize, u32),
    Verify(Vec<Nomination>),
}

struct Nomination {
    key: RankKey,
    iid: Iid,
    witness: Option<SearchWitness>,
}

impl Nomination {
    fn unknown(iid: Iid) -> Self { Self { key: RankKey::unknown(), iid, witness: None } }
}

pub(super) struct LogicalCoverage<'a> {
    runs: Vec<&'a SearchRun>,
    directory: Option<&'a TermDirectoryRoot>,
    covered: BTreeSet<rtdb_types::BlockIndex>,
    complete: bool,
}

pub(super) fn logical_runs<'a>(runtime: &SearchRuntime, request: &ScanRequest<'a>, definition: &SearchDefinition)
    -> Option<LogicalCoverage<'a>> {
    let format = crate::file::LOGICAL_FORMAT_VERSION;
    let id = definition.artefact_id_for(format);
    let runs: Vec<_> = request.snapshot.trie_catalog().search_runs(&definition.table)
        .filter(|run| run.definition_id() == id && run.definition_version() == definition.version).collect();
    if runs.is_empty() && runtime.write_format() != format { return None; }
    if !crate::lane::LogicalIndex::supports(definition) { return None; }
    let mut covered: BTreeSet<_> = runs.iter().flat_map(|run| run.blocks()).collect();
    let source_rows = runs.iter().try_fold(0i64, |count, run| count.checked_add(run.source_rows()))?;
    let durable = request.snapshot.table_catalog().metadata(&definition.table).map_or(0, |meta| meta.row_count);
    let complete = request.snapshot.table_catalog().block_index().is_none_or(|block| {
        request.snapshot.trie_catalog().l0_source(&definition.table, rtdb_types::BlockIndex(-1), block)
            .map(|trie| trie.key.block_index()).eq(covered.iter().copied())
    }) && source_rows == durable;
    if source_rows > durable { covered.clear(); }
    if !complete && !covered.is_empty() && request.snapshot.table_catalog().block_index().is_some_and(|block| {
        request.snapshot.trie_catalog().l0_source(&definition.table, rtdb_types::BlockIndex(-1), block)
            .map(|trie| trie.key.block_index()).eq(covered.iter().copied())
    }) {
        covered.clear();
    }
    let directory = request.snapshot.trie_catalog().term_directory(&definition.table, &id, definition.version);
    Some(LogicalCoverage { runs, directory, covered, complete })
}

impl LogicalCoverage<'_> {
    async fn routes(&self, runtime: &SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition,
        field: usize, matching: &Query, scoring: &RankScorer) -> Result<Option<TermRoutes>> {
        if self.runs.is_empty() { return Ok(None); }
        let id = definition.artefact_id_for(crate::file::LOGICAL_FORMAT_VERSION);
        let Some(directory) = self.directory else {
            runtime.report_term_directory(request.database, &definition.table, &id, definition.version);
            return Ok(None);
        };
        let read = async {
            if directory.runs() != self.runs.len() as i64 {
                return Err(Anomaly::fault("rtdb.search/term-directory", "Directory run count differs from its snapshot"));
            }
            let table = request.snapshot.table_entry(&definition.table)
                .ok_or_else(|| Anomaly::fault("rtdb.search/term-directory", "Directory table has no storage slug"))?;
            let _charge = request.memory.reserve(4096usize.saturating_add(self.runs.len().saturating_mul(80)))?;
            let runs = self.runs.iter().enumerate().map(|(index, run)| Ok((run.identity()?, index))).collect::<Result<BTreeMap<_, _>>>()?;
            let store = ObjectPageStore { store: Arc::clone(request.storage.object_store()), storage: Arc::clone(request.storage),
                prefix: term_directory::page_prefix(&table.slug) };
            let root = directory.page_root()?;
            read_routes(&store, root.as_ref(), field as u32, matching, scoring, &runs, request.memory).await
        };
        match request.cancellation.run_until_cancelled(read).await {
            Some(Ok(routes)) => Ok(Some(routes)),
            Some(Err(error)) if error.category == AnomalyCategory::Interrupted => Err(error),
            Some(Err(error)) => {
                if error.code != "rtdb/memory-budget-exceeded" {
                    runtime.report_term_directory(request.database, &definition.table, &id, definition.version);
                }
                Ok(None)
            }
            None => Err(search_cancelled()),
        }
    }
}

fn run_can_match(run: &rtdb_format::log::proto::SearchRun, field: usize, query: &Query) -> bool {
    let locator = std::str::from_utf8(run.locator()).ok().and_then(crate::file::TreeLocator::decode_route)
        .filter(|locator| locator.format() == crate::file::LOGICAL_FORMAT_VERSION && locator.length == run.file_size() as u64);
    locator.is_none_or(|locator| query.terms().iter().all(|term| locator.might_contain(field as u32, term)))
}
fn seed_logical<'a>(runs: impl Iterator<Item = (usize, &'a rtdb_format::log::proto::SearchRun)>,
    directory: Option<&TermRoutes>) -> Vec<(RankKey, usize)> {
    match directory {
        Some(directory) => directory.iter().map(|(index, bound)| (RankKey::integer(bound, None), index)).collect(),
        None => runs.map(|(index, _)| (RankKey::unknown(), index)).collect(),
    }
}

fn nominate_live(segment: &rtdb_index::LiveTable, row: u32, matched: crate::runtime::LiveMatch<'_>,
    request: &ScanRequest<'_>, scorer: &Scorer, order: TreeRankOrder, scope: Option<rtdb_query::IidScope<'_>>) -> Result<Option<Nomination>> {
    if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
    let source = segment.row(row)?;
    let header = source.header()?;
    if scope.is_some_and(|scope| !scope.contains(&header.iid)) || !may_be_visible(header.valid_time.lower().0,
        header.valid_time.upper().0, header.system_from.0, Timestamp::MAX.0, request.bounds) { return Ok(None); }
    let score = matched.score(scorer);
    let key = match order {
        TreeRankOrder::Integer => {
            RankKey::integer(score, matched.integer_id(segment, row)?)
        }
        TreeRankOrder::Uuid => RankKey::uuid(score, header.iid.0),
        TreeRankOrder::Score => RankKey::integer(score, None),
    };
    Ok(Some(Nomination { key, iid: header.iid, witness: Some(SearchWitness::live(&source)) }))
}

impl SearchAccelerator {
    pub(super) async fn logical_candidates(&self, request: &ScanRequest<'_>, definition: &SearchDefinition,
        column: &str, prepared: &rtdb_query::PreparedSearchQuery, work: &mut NarrowingWork) -> Result<Option<BTreeSet<Iid>>> {
        let Some(mut coverage) = logical_runs(&self.runtime, request, definition) else { return Ok(None); };
        let Some(field) = definition.columns.iter().position(|indexed| indexed == column) else { return Ok(None); };
        let query = prepared.query();
        let Some(scorer) = prepared.scorer().for_ranking() else { return Ok(None); };
        let routes = coverage.routes(&self.runtime, request, definition, field, query, &scorer).await?;
        let mut results = BTreeSet::new();
        let mut charge = request.memory.reserve(4096)?;
        let mut examined = 0usize;
        if let Some(table) = request.snapshot.table_entry(&definition.table) {
            let runs = coverage.runs.iter().copied().enumerate()
                .filter(|(_, run)| run_can_match(run, field, query));
            for (_, index) in seed_logical(runs, routes.as_ref()) {
                let run = coverage.runs[index];
                if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
                work.logical_roots += 1;
                let scanned = async {
                    let tree = self.runtime.logical_file(request.storage, &table.slug, run, &definition.columns).await?;
                    let mut rank = TreeRanked::open(tree, field, query, &scorer, request.bounds, TreeRankOrder::Score).await?;
                    while rank.bound().is_some() {
                        if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
                        for candidate in rank.expand(request.iid_scope(), &mut examined).await? { results.insert(candidate.iid); }
                        if examined > self.max_candidates.saturating_mul(8) || results.len() > self.max_candidates { return Ok(false); }
                        reserve_ranking(&mut charge, results.len())?;
                    }
                    Ok::<bool, Anomaly>(true)
                }.await;
                match scanned {
                    Ok(true) => self.runtime.record_logical_run_read(),
                    Ok(false) => { work.plain_decline = Some("logical-candidate-budget"); return Ok(None); }
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(error) => {
                        self.runtime.report_run(request.database, &definition.table, run, &error);
                        coverage.complete = false;
                        for block in run.blocks() { coverage.covered.remove(&block); }
                    }
                }
            }
        }
        if !coverage.complete {
            let available = self.max_candidates.saturating_sub(results.len());
            let Some(gaps) = rtdb_query::uncovered_history_iids(request, &coverage.covered, request.iid_scope(), available).await?
                else { work.plain_decline = Some("logical-gap-budget"); return Ok(None); };
            reserve_ranking(&mut charge, results.len().saturating_add(gaps.len()))?;
            results.extend(gaps);
        }
        work.plain_events += examined;
        for segment in request.snapshot.iter_segments(&definition.table) {
            let (_, covered) = self.runtime.live_pages_at(Some(request.snapshot), request.database, segment, column, definition.analyzer);
            if !self.runtime.can_scan_live_gap(segment, column, covered)? { return Ok(None); }
        }
        results.extend(self.live_candidates(request, column, query)?);
        Ok((results.len() <= self.max_candidates).then_some(results))
    }
}


pub(super) struct PartitionRankSource<'a> {
    search: &'a SearchAccelerator,
    request: ScanRequest<'a>,
    definition: SearchDefinition,
    column: String,
    column_id: ColumnId,
    column_index: usize,
    matching: Query,
    ranking: RankScorer,
    ascending_id: Option<ColumnId>,
    order: TreeRankOrder,
    verification_batch: usize,
    scope: Option<rtdb_query::IidScope<'a>>,
    sources: Vec<RecordedTree>,
    ranks: Vec<Option<TreeRanked>>,
    legacy: Vec<(FileMembers, crate::file::RankedSource)>,
    logical: Vec<&'a rtdb_format::log::proto::SearchRun>,
    logical_ranks: Vec<Option<TreeRanked>>,
    logical_seed: Vec<(RankKey, usize)>,
    coverage: BTreeSet<rtdb_types::BlockIndex>,
    gap_queued: bool,
    segments: Vec<&'a Arc<rtdb_index::LiveTable>>,
    visibility: Option<super::point::PackedVisibility>,
    points: Option<rtdb_query::IidScope<'a>>,
    checked: BTreeSet<Iid>,
    nominated: usize,
    reservation: Arc<rtdb_columnar::MemoryReservation>,
    pub(super) work: NarrowingWork,
}

impl<'a> PartitionRankSource<'a> {
    pub(super) async fn open(search: &'a SearchAccelerator, request: ScanRequest<'a>, definition: SearchDefinition, column: &str,
        column_id: ColumnId, matching: Query, scorer: Arc<Scorer>, ascending_id: Option<ColumnId>, limit: usize, options: Narrowing<'a>)
        -> Result<Option<Box<Self>>> {
        if !request.bounds.valid_time.is_point() || !request.bounds.system_time.is_point()
            || definition.table.is_owned_event_data() || TableRef::owned_schema(&definition.table.schema) {
            return Ok(None);
        }
        let Some(column_index) = definition.columns.iter().position(|indexed| indexed == column) else { return Ok(None); };
        let scope = options.scope.or(request.iid_scope());
        let points = scope.filter(|scope| scope.is_empty() || (!options.omit_logical && scope.len() <= 128
            && crate::lane::LogicalIndex::supports(&definition)));
        let include_packs = !options.omit_packs;
        let logical = if points.is_none() && include_packs && !options.omit_logical { logical_runs(&search.runtime, &request, &definition) } else { None };
        let mut sources = if points.is_none() {
            RecordedTree::select(&search.runtime, &request, &definition, column_index, &matching, include_packs, logical.is_some())?
        } else { Vec::new() };
        let prefer_paged = !matching.phrases().is_empty() && !sources.is_empty()
            && sources.iter().all(|source| source.pack.locator.format() == TREE_FORMAT_VERSION)
            && {
                let paged = definition.artefact_id_for(crate::file::FORMAT_VERSION);
                sources.iter().flat_map(|source| source.members.values()).all(|key| {
                    request.snapshot.trie_catalog().trie(&definition.table, key)
                        .is_some_and(|trie| trie.artefact(KIND, &paged, definition.version).is_some())
                })
            };
        if prefer_paged { sources.clear(); }
        let Some(ranking) = scorer.for_ranking() else { return Ok(None); };
        let routes = match &logical {
            Some(coverage) => coverage.routes(&search.runtime, &request, &definition, column_index, &matching, &ranking).await?,
            None => None,
        };
        let tree_tries = sources.iter().flat_map(|source| source.members.values().cloned()).collect();
        let visibility = points.is_none().then(|| super::point::PackedVisibility::new(&request, &definition.table, &sources, column_index, logical.is_some()));
        let files = if points.is_some() || logical.is_some() { Vec::new() } else {
            let TrieFiles::Complete(Found { files, .. }) = search.trie_files(&request, &definition, column, &[&matching, scorer.query()],
                LoadSearch::Rank { prefer_paged }, Some(&tree_tries)).await? else { return Ok(None); };
            files
        };
        let integer = ascending_id.is_some_and(|id| request.ranked_id_is_integer(id));
        let uuid = ascending_id.is_some_and(|id| request.ranked_id_is_uuid(id));
        let order = if integer {
            TreeRankOrder::Integer
        } else if logical.is_none() && ascending_id.is_some_and(|id| request.permits_ranked_uuid_id(id))
            && files.is_empty() && sources.iter().all(|source| source.pack.locator.has_canonical_uuid_iids()) {
            TreeRankOrder::Uuid
        } else {
            TreeRankOrder::Score
        };
        let legacy = by_file(&files).map(|selected| {
            let rank = selected.file.ranked_for_query(column, &matching, &ranking, request.bounds, integer)?;
            Ok((selected, rank))
        }).collect::<Result<Vec<_>>>()?;
        let ranks = (0..sources.len()).map(|_| None).collect();
        let (logical, coverage, gap_queued) = match logical {
            Some(coverage) => (coverage.runs, coverage.covered, !coverage.complete),
            None => (Vec::new(), BTreeSet::new(), false),
        };
        let logical_ranks = (0..logical.len()).map(|_| None).collect();
        let runs = logical.iter().copied().enumerate().filter(|(_, run)| run_can_match(run, column_index, &matching));
        let directory = routes.as_ref();
        let logical_seed = seed_logical(runs, directory);
        let segments = request.snapshot.iter_segments(&request.scan.table.name.table).collect();
        let reservation = request.memory.reserve(4096)?;
        Ok(Some(Box::new(Self {
            search, request, definition, column: column.into(), column_id, column_index, matching, ranking,
            ascending_id: ascending_id.filter(|_| integer || uuid), order, verification_batch: limit.clamp(16, 128),
            scope, sources, ranks, legacy, logical, logical_ranks, logical_seed, coverage,
            gap_queued, segments, visibility, points, checked: BTreeSet::new(), nominated: 0, reservation, work: NarrowingWork::default(),
        })))
    }

    fn nominate(&mut self, mut nominations: Vec<Nomination>, more: &mut Vec<RankWork>) -> Result<bool> {
        self.nominated = self.nominated.saturating_add(nominations.len());
        if self.nominated > self.search.max_candidates {
            self.work.tree_decline = Some("candidate-budget");
            return Ok(false);
        }
        reserve_ranking(&mut self.reservation, self.nominated)?;
        nominations.retain(|nomination| !self.checked.contains(&nomination.iid));
        nominations.sort_unstable_by(|left, right| left.key.compare(right.key).then_with(|| right.iid.cmp(&left.iid)));
        while !nominations.is_empty() {
            let chunk = nominations.split_off(nominations.len().saturating_sub(self.verification_batch));
            let bound = chunk.last().expect("nonempty verification batch").key;
            more.push(RankWork::new(bound, Task::Verify(chunk)));
        }
        Ok(true)
    }

    fn lost_run(&mut self, index: usize, error: &Anomaly, more: &mut Vec<RankWork>) {
        let run = self.logical[index];
        self.search.runtime.report_run(self.request.database, &self.definition.table, run, error);
        for block in run.blocks() { self.coverage.remove(&block); }
        if !self.gap_queued {
            self.gap_queued = true;
            more.push(RankWork::new(RankKey::unknown(), Task::Gap));
        }
    }

    async fn verify(&mut self, nominations: Vec<Nomination>) -> Result<Option<RankOutcome>> {
        let iids: BTreeSet<_> = nominations.iter().map(|nomination| nomination.iid).filter(|iid| !self.checked.contains(iid)).collect();
        if iids.is_empty() { return Ok(Some(RankOutcome::default())); }
        self.work.verification_rounds += 1;
        self.work.verifier_rows += iids.len();
        let resolved = match &self.visibility {
            Some(visibility) => visibility.resolve(&self.search.runtime, &self.request, &self.definition, &self.sources, &iids).await,
            None => rtdb_query::table_point_rows(&self.request, &iids).await.map(|rows| rows.map(|rows| {
                rows.into_iter().map(|row| super::point::ResolvedVersion { row, witness: None }).collect()
            })),
        };
        let (rows, witnesses) = match resolved {
            Ok(Some(resolved)) => {
                self.work.packed_resolved += resolved.len();
                let (rows, witnesses): (Vec<_>, BTreeMap<_, _>) = resolved.into_iter().map(|resolved| {
                    let row = resolved.row;
                    let witness = (row.version, resolved.witness);
                    (row, witness)
                }).unzip();
                (Some(rows), witnesses)
            }
            Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
            _ => (None, BTreeMap::new()),
        };
        let verified = self.request.verifier.verify_ranked(&iids, rows, self.column_id, self.ascending_id, self.ranking.scorer()).await?;
        let Some(verified) = verified else { return Ok(None); };
        for row in &verified {
            if let Some(Some(witness)) = witnesses.get(&row.version())
                && nominations.iter().any(|nomination| nomination.iid == row.version().iid
                    && nomination.witness == Some(*witness) && row.score() > nomination.key.score) {
                self.work.tree_decline = Some("winner-version");
                return Ok(None);
            }
        }
        self.checked.extend(iids);
        Ok(Some(RankOutcome { verified, ..Default::default() }))
    }
}

#[async_trait]
impl RankSource for PartitionRankSource<'_> {
    fn seed(&mut self) -> Result<Vec<RankWork>> {
        let mut seed = Vec::new();
        if let Some(points) = self.points {
            if !points.is_empty() {
                seed.push(RankWork::new(RankKey::unknown(), Task::Verify(points.iter().map(Nomination::unknown).collect())));
            }
            return Ok(seed);
        }
        for (index, (_, rank)) in self.legacy.iter().enumerate() {
            if let Some(bound) = rank.bound() { seed.push(RankWork::new(bound, Task::Legacy(index))); }
        }
        for (index, source) in self.sources.iter().enumerate() {
            if source.discovery { seed.push(RankWork::new(RankKey::unknown(), Task::Open(index))); }
        }
        seed.extend(std::mem::take(&mut self.logical_seed).into_iter().map(|(bound, index)| RankWork::new(bound, Task::OpenLogical(index))));
        if self.gap_queued { seed.push(RankWork::new(RankKey::unknown(), Task::Gap)); }
        for (index, segment) in self.segments.iter().enumerate() {
            let (pages, covered) = self.search.runtime.live_pages_at(Some(self.request.snapshot), self.request.database, segment, &self.column, self.definition.analyzer);
            for page in pages {
                if let Some(bound) = page.bound(&self.matching, self.ranking.scorer().query(), self.order == TreeRankOrder::Integer) {
                    seed.push(RankWork::new(bound, Task::Live(index, page)));
                }
            }
            if covered < segment.row_count() {
                seed.push(RankWork::new(RankKey::unknown(), Task::LiveSuffix(index, covered)));
            }
        }
        Ok(seed)
    }

    async fn execute(&mut self, work: RankWork) -> Result<Option<RankOutcome>> {
        let mut more = Vec::new();
        let request = &self.request;
        let definition = &self.definition;
        let runtime = &self.search.runtime;
        let mut nominations = Vec::new();
        match work.take::<Task>()? {
            Task::Verify(nominations) => return self.verify(nominations).await,
            Task::Gap => {
                self.gap_queued = false;
                let available = self.search.max_candidates.saturating_sub(self.nominated);
                let Some(iids) = rtdb_query::uncovered_history_iids(request, &self.coverage, self.scope, available).await?
                    else { self.work.tree_decline = Some("logical-gap-budget"); return Ok(None); };
                nominations.extend(iids.into_iter().map(Nomination::unknown));
            }
            Task::OpenLogical(index) => {
                let table = request.snapshot.table_entry(&definition.table).ok_or_else(uncovered_owned_search)?;
                let opened = async {
                    let file = runtime.logical_file(request.storage, &table.slug, self.logical[index], &definition.columns).await?;
                    TreeRanked::open(file, self.column_index, &self.matching, &self.ranking, request.bounds, self.order).await
                }.await;
                match opened {
                    Ok(rank) => {
                        self.work.tree_roots += 1;
                        self.work.logical_roots += 1;
                        runtime.record_logical_run_read();
                        if let Some(bound) = rank.bound() { more.push(RankWork::new(bound, Task::ExpandLogical(index))); }
                        self.logical_ranks[index] = Some(rank);
                    }
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(error) => self.lost_run(index, &error, &mut more),
                }
            }
            Task::ExpandLogical(index) => {
                let rank = self.logical_ranks[index].as_mut().expect("logical rank is opened before expansion");
                let before = self.work.postings_visited;
                let expanded = rank.expand(self.scope, &mut self.work.postings_visited).await;
                rtdb_storage::audience_work::scan_items(self.work.postings_visited - before)?;
                match expanded {
                    Ok(candidates) => {
                        self.work.tree_expansions += 1;
                        self.work.score_evaluations += candidates.len();
                        nominations.extend(candidates.into_iter().map(|candidate| Nomination {
                            key: candidate.key, iid: candidate.iid, witness: candidate.revision.map(SearchWitness::Logical),
                        }));
                        if let Some(bound) = rank.bound() { more.push(RankWork::new(bound, Task::ExpandLogical(index))); }
                    }
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(error) => self.lost_run(index, &error, &mut more),
                }
            }
            Task::Open(index) => {
                self.work.tree_roots += 1;
                let source = &self.sources[index];
                let opened = async {
                    let file = source.open(runtime, request, definition).await?;
                    TreeRanked::open(file, self.column_index, &self.matching, &self.ranking, request.bounds, self.order).await
                }.await;
                let rank = match opened {
                    Ok(rank) => rank,
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(error) => {
                        source.report(runtime, request, definition, &error);
                        self.work.tree_decline = Some("recorded-root");
                        return Ok(None);
                    }
                };
                if let Some(bound) = rank.bound() { more.push(RankWork::new(bound, Task::Expand(index))); }
                self.ranks[index] = Some(rank);
            }
            Task::Expand(index) => {
                self.work.tree_expansions += 1;
                let rank = self.ranks[index].as_mut().expect("rank is opened before it expands");
                let before = self.work.postings_visited;
                let expanded = rank.expand(self.scope, &mut self.work.postings_visited).await;
                rtdb_storage::audience_work::scan_items(self.work.postings_visited - before)?;
                let candidates = match expanded {
                    Ok(candidates) => candidates,
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(error) => {
                        self.sources[index].report(runtime, request, definition, &error);
                        self.work.tree_decline = Some("rank-page");
                        return Ok(None);
                    }
                };
                self.work.score_evaluations += candidates.len();
                nominations.extend(candidates.into_iter().filter(|candidate| self.sources[index].members.contains_key(&candidate.member))
                    .map(|candidate| Nomination { key: candidate.key, iid: candidate.iid,
                        witness: Some(SearchWitness::Packed(index, candidate.document)) }));
                if let Some(bound) = rank.bound() { more.push(RankWork::new(bound, Task::Expand(index))); }
            }
            Task::Legacy(index) => {
                let (selected, rank) = &mut self.legacy[index];
                let before = self.work.postings_visited;
                let expanded = rank.expand(self.scope, &mut self.work.postings_visited).await;
                rtdb_storage::audience_work::scan_items(self.work.postings_visited - before)?;
                let hits = match expanded {
                    Ok(hits) => hits,
                    Err(error) if error.category == AnomalyCategory::Interrupted => return Err(error),
                    Err(_) => { self.work.tree_decline = Some("legacy-source"); return Ok(None); }
                };
                self.work.score_evaluations += hits.len();
                nominations.extend(hits.iter().filter(|(run, _)| selected.members.contains_key(&run.member)).map(|(run, score)| {
                    let id = (self.order == TreeRankOrder::Integer).then(|| selected.file.integer_id(run)).flatten();
                    Nomination { key: RankKey::integer(score, id), iid: Iid(run.iid), witness: None }
                }));
                if let Some(bound) = rank.bound() { more.push(RankWork::new(bound, Task::Legacy(index))); }
            }
            task @ (Task::Live(..) | Task::LiveSuffix(..)) => {
                let available = self.search.max_candidates.saturating_sub(self.nominated);
                let mut charge = request.memory.reserve(4096)?;
                let index = match &task { Task::Live(index, _) | Task::LiveSuffix(index, _) => *index, _ => unreachable!() };
                let segment = self.segments[index];
                let visit = |row, matched: crate::runtime::LiveMatch<'_>| {
                    if let Some(hit) = nominate_live(segment, row, matched, request, self.ranking.scorer(), self.order, self.scope)? {
                        if nominations.len() % 128 == 0 { reserve_ranking(&mut charge, nominations.len() + 128)?; }
                        nominations.push(hit);
                    }
                    Ok(nominations.len() <= available)
                };
                let complete = match task {
                    Task::Live(_, page) => page.visit(&self.matching, true, visit)?,
                    Task::LiveSuffix(_, covered) => {
                        if !runtime.can_scan_live_gap(segment, &self.column, covered)? {
                            self.work.tree_decline = Some("live-coverage-budget");
                            return Ok(None);
                        }
                        self.work.uncovered_live_rows += (segment.row_count() - covered) as usize;
                        crate::SearchRuntime::visit_live_suffix(segment, &self.column, &self.matching, true, covered, request.cancellation, visit)?
                    }
                    _ => unreachable!(),
                };
                if !complete { self.work.tree_decline = Some("candidate-budget"); return Ok(None); }
            }
        }
        if !self.nominate(nominations, &mut more)? { return Ok(None); }
        Ok(Some(RankOutcome { more, ..Default::default() }))
    }
}
