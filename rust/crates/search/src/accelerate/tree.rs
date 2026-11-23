use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::OnceLock;

use rtdb_format::keys::TrieKey;
use rtdb_plan::ColumnId;
use rtdb_text::Scorer;
use tokio::sync::OnceCell;

use super::*;
use super::point::SearchWitness;
use crate::file::{TREE_FORMAT_VERSION, TreeFile, TreeRankOrder, TreeRanked};

pub(super) struct RecordedTree {
    pub(super) pack: Arc<super::routing::Pack>,
    pub(super) members: BTreeMap<u32, TrieKey>,
    discovery: bool,
    opened: OnceCell<Arc<TreeFile>>,
}

impl RecordedTree {
    fn select(runtime: &crate::SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition,
        column: usize, matching: &Query, include_packs: bool) -> Result<Vec<Self>> {
        let routing = runtime.table_routing(request.snapshot, definition, request.storage, include_packs)?;
        Ok(routing.packs().filter_map(|pack| {
            let members = pack.visible_members(request.bounds);
            (!members.is_empty()).then(|| Self { pack: Arc::clone(pack), members,
                discovery: pack.can_nominate(request.bounds, column, matching), opened: OnceCell::new() })
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

struct Nomination { iid: Iid, source: Option<usize>, witness: Option<SearchWitness>, visibility: Vec<[i64; 4]> }
enum Task {
    Open(usize),
    Expand(usize),
    Legacy(usize),
    Live(usize, Arc<crate::runtime::LivePostingPage>),
    Verify(Nomination),
    VerifyBatch(BinaryHeap<Entry>),
}
struct Entry { key: RankKey, sequence: u64, task: Task }

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.compare(other.key).then_with(|| other.sequence.cmp(&self.sequence))
    }
}
impl PartialOrd for Entry { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl PartialEq for Entry { fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() } }
impl Eq for Entry {}

#[derive(Default)]
struct Frontier { ready: BinaryHeap<Entry>, next: u64 }

impl Frontier {
    fn push(&mut self, key: RankKey, task: Task) {
        self.next += 1;
        self.ready.push(Entry { key, sequence: self.next, task });
    }

    fn nominate_batch(&mut self, nominations: impl IntoIterator<Item = (RankKey, Nomination)>) {
        let pending: BinaryHeap<_> = nominations.into_iter().map(|(key, nomination)| {
            self.next += 1;
            Entry { key, sequence: self.next, task: Task::Verify(nomination) }
        }).collect();
        if let Some(best) = pending.peek() {
            self.push(best.key, Task::VerifyBatch(pending));
        }
    }

    fn next(&mut self) -> Option<Entry> {
        let entry = self.ready.pop()?;
        let Task::VerifyBatch(mut pending) = entry.task else { return Some(entry); };
        let best = pending.pop().expect("only nonempty candidate batches enter the frontier");
        if let Some(next) = pending.peek() {
            self.push(next.key, Task::VerifyBatch(pending));
        }
        Some(best)
    }

}

fn nominate_live(segment: &rtdb_index::LiveTable, row: u32, tokens: &dyn Fn() -> arrow_array::StringArray,
    request: &ScanRequest<'_>, scorer: &Scorer, order: TreeRankOrder, scope: Option<&BTreeSet<Iid>>) -> Result<Option<(RankKey, Nomination)>> {
    if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
    let header = segment.header(row)?;
    if scope.is_some_and(|scope| !scope.contains(&header.iid)) || !may_be_visible(header.valid_time.lower().0,
        header.valid_time.upper().0, header.system_from.0, Timestamp::MAX.0, request.bounds) { return Ok(None); }
    let score = scorer.bm25_arrow(&tokens());
    let key = match order {
        TreeRankOrder::Integer => {
            let source = segment.row(row)?;
            RankKey::integer(score, crate::lane::integer_sql_id(&source.batch.view().borrow_event(source.row)?))
        }
        TreeRankOrder::Uuid => RankKey::uuid(score, header.iid.0),
        TreeRankOrder::Score => RankKey::integer(score, None),
    };
    Ok(Some((key, Nomination {
        iid: header.iid, source: None, witness: Some(SearchWitness::Live(header.version())), visibility: Vec::new(),
    })))
}

async fn visible(runtime: &crate::SearchRuntime, request: &ScanRequest<'_>, definition: &SearchDefinition,
    sources: &[RecordedTree], nomination: &mut Nomination) -> Result<bool> {
    let Some(own) = nomination.source else { return Ok(true); };
    let iid = nomination.iid.0;
    for (index, source) in sources.iter().enumerate() {
        if index == own || !source.members.values().any(|trie| covers(trie.part(), &iid)) { continue; }
        let result = async { source.open(runtime, request, definition).await?.events(iid).await }.await;
        let events = match result {
            Ok(events) => events,
            Err(error) => { source.report(runtime, request, definition, &error); return Err(error); }
        };
        cut(&mut nomination.visibility, events);
        if nomination.visibility.is_empty() { return Ok(false); }
    }
    for segment in request.snapshot.iter_segments(&definition.table) {
        cut(&mut nomination.visibility, segment.iid_headers(nomination.iid)?.map(crate::file::KeyedEvent::from));
    }
    Ok(nomination.visibility.iter().any(|rectangle| crate::file::meets(*rectangle, request.bounds)))
}

impl SearchAccelerator {
    pub(super) async fn tree_top_k(&self, request: &ScanRequest<'_>, definition: &SearchDefinition, column: &str,
        column_id: ColumnId, matching: &Query, scoring_query: Query, ascending_id: Option<ColumnId>,
        permit_uuid_order: bool, limit: usize, scope: Option<&BTreeSet<Iid>>, include_packs: bool, work: &mut NarrowingWork) -> Result<Option<BTreeSet<Iid>>> {
        if !request.bounds.valid_time.is_point() || !request.bounds.system_time.is_point() {
            work.tree_decline = Some("unsupported-snapshot");
            return Ok(None);
        }
        let Some(column_index) = definition.columns.iter().position(|indexed| indexed == column) else { return Ok(None); };
        let sources = RecordedTree::select(&self.runtime, request, definition, column_index, matching, include_packs)?;
        if sources.is_empty() && request.snapshot.serving_search::<crate::runtime::ServingPostings>().is_none() {
            work.tree_decline = Some("no-tree");
            return Ok(None);
        }
        if !matching.phrases().is_empty() && !sources.is_empty() && sources.iter().all(|source| source.pack.locator.format() == TREE_FORMAT_VERSION) {
            let paged = definition.artefact_id_for(crate::file::FORMAT_VERSION);
            if sources.iter().flat_map(|source| source.members.values()).all(|key| {
                request.snapshot.trie_catalog().trie(&definition.table, key)
                    .is_some_and(|trie| trie.artefact(KIND, &paged, definition.version).is_some())
            }) {
                work.tree_decline = Some("phrase-paged");
                return Ok(None);
            }
        }
        let scorer = Arc::new(Scorer::new(scoring_query, None));
        let Some(ranking) = scorer.for_ranking() else { return Ok(None); };
        let tree_tries = sources.iter().flat_map(|source| source.members.values().cloned()).collect();
        let visibility = super::point::PackedVisibility::new(request, &definition.table, &sources, column_index);
        let TrieFiles::Complete(Found { files, .. }) = self.trie_files(request, definition, column, &[matching, scorer.query()],
            LoadSearch::Rank, Some(&tree_tries)).await? else { work.tree_decline = Some("coverage"); return Ok(None); };
        let keep_ids = ascending_id.is_some();
        let order = if keep_ids {
            TreeRankOrder::Integer
        } else if permit_uuid_order && files.is_empty() && sources.iter().all(|source| source.pack.locator.has_canonical_uuid_iids()) {
            TreeRankOrder::Uuid
        } else {
            TreeRankOrder::Score
        };
        let mut legacy = by_file(&files).map(|selected| {
            let rank = selected.file.ranked_for_query(column, matching, &ranking, request.bounds, keep_ids)?;
            Ok((selected, rank))
        }).collect::<Result<Vec<_>>>()?;
        let mut frontier = Frontier::default();
        for (index, (_, rank)) in legacy.iter().enumerate() {
            if let Some(bound) = rank.bound() { frontier.push(bound, Task::Legacy(index)); }
        }
        let mut ranks = (0..sources.len()).map(|_| OnceLock::new()).collect::<Vec<OnceLock<TreeRanked>>>();
        for (index, source) in sources.iter().enumerate() {
            if source.discovery {
                frontier.push(RankKey::integer(f64::INFINITY, None), Task::Open(index));
            }
        }
        let mut reservation = request.memory.reserve(4096)?;
        let mut nominated = 0;
        let segments: Vec<_> = request.snapshot.iter_segments(&definition.table).collect();
        for (index, segment) in segments.iter().enumerate() {
            let (pages, covered) = self.runtime.live_pages_at(Some(request.snapshot), request.database, segment, column);
            if !self.runtime.can_scan_live_gap(segment, column, covered)? {
                work.tree_decline = Some("live-coverage-budget");
                return Ok(None);
            }
            for page in pages {
                if let Some(bound) = page.bound(matching, scorer.query(), keep_ids) {
                    frontier.push(bound, Task::Live(index, page));
                }
            }
            work.uncovered_live_rows += (segment.row_count() - covered) as usize;
            let complete = crate::SearchRuntime::visit_live_suffix(segment, column, matching, true, covered, request.cancellation, |row, tokens| {
                if let Some((key, nomination)) = nominate_live(segment, row, tokens, request, &scorer, order, scope)? {
                    nominated += 1;
                    if nominated % 128 == 1 { reserve_ranking(&mut reservation, nominated + 128)?; }
                    frontier.push(key, Task::Verify(nomination));
                }
                Ok(nominated <= self.max_candidates)
            })?;
            if !complete { work.tree_decline = Some("candidate-budget"); return Ok(None); }
        }

        let mut visited = BTreeSet::new();
        let mut verified = Vec::new();
        let mut rows = BTreeMap::new();
        let mut deferred = BTreeMap::<Iid, rtdb_query::ResolvedSearchRow>::new();
        let mut to_verify = BTreeMap::<Iid, (RankKey, Option<SearchWitness>)>::new();
        let shared_rank = request.verifier.partition_ranking();
        let trusted = request.scan.top_k.as_ref().is_some_and(|top| top.score.is_some())
            && request.verifier.permits_search_witness() && match request.scan.predicates.as_slice() {
                [Expr::Column { id, .. }] => request.scan.matches.iter().any(|evaluated| evaluated.id == *id),
                [Expr::Call { name, .. }] => name.eq_ignore_ascii_case("match"),
                _ => false,
            };
        let batch_size = limit.clamp(1, 128);
        let mut rounds = 0;
        loop {
            if request.cancellation.is_cancelled() { return Err(search_cancelled()); }
            if to_verify.is_empty() && verified.len() >= limit
                && verified_cutoff(&mut verified, limit).is_some_and(|kth| frontier.ready.peek().is_none_or(|entry| kth.beats(entry.key))) { break; }
            if to_verify.is_empty() && let Some(cutoff) = shared_rank.and_then(|rank| rank.cutoff(limit))
                && frontier.ready.peek().is_some_and(|entry| entry.key.score < cutoff) {
                work.shared_cutoff_prunes += frontier.ready.len();
                break;
            }
            if !to_verify.is_empty() && (to_verify.len() >= batch_size || frontier.ready.is_empty()) {
                rounds += 1;
                if rounds > 16 { work.tree_decline = Some("verification-budget"); return Ok(None); }
                work.verification_rounds += 1;
                work.verifier_rows += to_verify.len();
                let pending = std::mem::take(&mut to_verify);
                let iids = pending.keys().copied().collect();
                let resolved = visibility.resolve(&self.runtime, request, definition, &sources, &iids).await;
                let scores = match resolved {
                    Ok(Some(resolved)) => {
                        work.packed_resolved += resolved.len();
                        let mut gather = Vec::new();
                        for version in resolved {
                            let iid = version.row.version.iid;
                            let (key, witness) = pending[&iid];
                            if trusted && witness.is_some() && witness == version.witness {
                                verified.push((key, iid));
                                deferred.insert(iid, version.row);
                                work.deferred_payloads += 1;
                            } else { gather.push(version.row); }
                        }
                        match request.verifier.score_resolved(gather, column_id, ascending_id, &scorer).await {
                            Ok(Some(scores)) => scores,
                            Ok(None) => request.verifier.score_visible(iids, column_id, ascending_id, &scorer).await?,
                            Err(error) if error.code.starts_with("rtdb.storage/") || error.code == "rtdb.query/search-row" => {
                                request.verifier.score_visible(iids, column_id, ascending_id, &scorer).await?
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Ok(None) => request.verifier.score_visible(iids, column_id, ascending_id, &scorer).await?,
                    Err(error) if error.category == rtdb_types::AnomalyCategory::Interrupted => return Err(error),
                    Err(_) => request.verifier.score_visible(iids, column_id, ascending_id, &scorer).await?,
                };
                if let Some(rank) = shared_rank { rank.add_verified(limit, scores.iter()); }
                for (iid, score, id) in scores.ranked() {
                    let key = if order == TreeRankOrder::Uuid { RankKey::uuid(score, iid.0) } else { RankKey::integer(score, id) };
                    if deferred.remove(&iid).is_some() {
                        if let Some(entry) = verified.iter_mut().find(|(_, known)| *known == iid) { entry.0 = key; }
                    } else { verified.push((key, iid)); }
                    if let Some(row) = scores.row(iid) { rows.insert(iid, row); }
                }
                if let Some(kth) = verified_cutoff(&mut verified, limit) {
                    if kth.has_id() { verified.truncate(limit); }
                    else { verified.retain(|(key, _)| !key.score_order(kth).is_lt()); }
                    let winners: BTreeSet<_> = verified.iter().map(|(_, iid)| *iid).collect();
                    rows.retain(|iid, _| winners.contains(iid));
                    deferred.retain(|iid, _| winners.contains(iid));
                }
                continue;
            }
            let Some(entry) = frontier.next() else { break; };
            match entry.task {
                Task::Open(index) => {
                    work.tree_roots += 1;
                    let source = &sources[index];
                    let opened = async {
                        let file = source.open(&self.runtime, request, definition).await?;
                        TreeRanked::open(file, column_index, matching, &ranking, request.bounds, order).await
                    }.await;
                    let rank = match opened { Ok(rank) => rank, Err(error) => {
                        source.report(&self.runtime, request, definition, &error); work.tree_decline = Some("recorded-root"); return Ok(None);
                    } };
                    if let Some(bound) = rank.bound() { frontier.push(bound, Task::Expand(index)); }
                    let _ = ranks[index].set(rank);
                }
                Task::Expand(index) => {
                    work.tree_expansions += 1;
                    let rank = ranks[index].get_mut().expect("rank is opened before it expands");
                    let candidates = match rank.expand(scope, &mut work.postings_visited).await {
                        Ok(candidates) => candidates,
                        Err(error) => { sources[index].report(&self.runtime, request, definition, &error); work.tree_decline = Some("rank-page"); return Ok(None); }
                    };
                    work.score_evaluations += candidates.len();
                    nominated += candidates.len();
                    if nominated > self.max_candidates { work.tree_decline = Some("candidate-budget"); return Ok(None); }
                    reserve_ranking(&mut reservation, nominated)?;
                    if let Some(bound) = rank.bound() { frontier.push(bound, Task::Expand(index)); }
                    frontier.nominate_batch(candidates.into_iter().filter(|candidate| sources[index].members.contains_key(&candidate.member))
                        .map(|candidate| {
                            let key = candidate.key;
                            (key, Nomination { iid: candidate.iid, source: Some(index), witness: Some(SearchWitness::Packed(index, candidate.document)), visibility: candidate.rectangles })
                        }));
                }
                Task::Legacy(index) => {
                    let (selected, rank) = &mut legacy[index];
                    let hits = match rank.expand(scope, &mut work.postings_visited).await {
                        Ok(hits) => hits,
                        Err(_) => { work.tree_decline = Some("legacy-source"); return Ok(None); }
                    };
                    work.score_evaluations += hits.len();
                    nominated += hits.len();
                    if nominated > self.max_candidates { work.tree_decline = Some("candidate-budget"); return Ok(None); }
                    reserve_ranking(&mut reservation, nominated)?;
                    if let Some(bound) = rank.bound() { frontier.push(bound, Task::Legacy(index)); }
                    for (run, score) in hits.iter() {
                        if !selected.members.contains_key(&run.member) { continue; }
                        let id = keep_ids.then(|| selected.file.integer_id(run)).flatten();
                        frontier.push(RankKey::integer(score, id), Task::Verify(Nomination {
                            iid: Iid(run.iid), source: None, witness: None, visibility: Vec::new(),
                        }));
                    }
                }
                Task::Live(index, page) => {
                    let complete = page.visit(matching, true, |row, tokens| {
                        if let Some((key, nomination)) = nominate_live(segments[index], row, tokens, request, &scorer, order, scope)? {
                            nominated += 1;
                            if nominated % 128 == 1 { reserve_ranking(&mut reservation, nominated + 128)?; }
                            frontier.push(key, Task::Verify(nomination));
                        }
                        Ok(nominated <= self.max_candidates)
                    })?;
                    if !complete { work.tree_decline = Some("candidate-budget"); return Ok(None); }
                }
                Task::VerifyBatch(_) => unreachable!("the frontier returns an individual nomination"),
                Task::Verify(mut nomination) => {
                    let iid = nomination.iid;
                    if visited.contains(&iid) || request.scan.iids.as_ref().is_some_and(|scope| !scope.contains(&iid)) { continue; }
                    if visibility.covers(iid) {
                        visited.insert(iid);
                        to_verify.insert(iid, (entry.key, nomination.witness));
                        continue;
                    }
                    match visible(&self.runtime, request, definition, &sources, &mut nomination).await {
                        Ok(false) => continue,
                        Ok(true) => { visited.insert(iid); to_verify.insert(iid, (entry.key, None)); }
                        Err(_) => { work.tree_decline = Some("visibility-page"); return Ok(None); }
                    }
                }
            }
        }
        if !deferred.is_empty() {
            let remaining = deferred.keys().copied().collect::<BTreeSet<_>>();
            let scores = request.verifier.score_resolved(deferred.into_values().collect(), column_id, ascending_id, &scorer).await;
            let scores = match scores {
                Ok(Some(scores)) => scores,
                Err(error) if error.category == rtdb_types::AnomalyCategory::Interrupted => return Err(error),
                _ => { work.tree_decline = Some("winner-payload"); return Ok(None); }
            };
            let checked: BTreeMap<_, _> = scores.ranked().map(|(iid, score, id)| (iid, (score, id))).collect();
            for (key, iid) in verified.iter().filter(|(_, iid)| remaining.contains(iid)) {
                if checked.get(iid).is_none_or(|(score, id)| *score != key.score || (key.integer_id().is_some() && *id != key.integer_id())) {
                    work.tree_decline = Some("winner-version");
                    return Ok(None);
                }
                let Some(row) = scores.row(*iid) else { work.tree_decline = Some("winner-payload"); return Ok(None); };
                rows.insert(*iid, row);
            }
            if let Some(rank) = shared_rank { rank.add_verified(limit, scores.iter()); }
        }
        let iids = verified.iter().map(|&(_, iid)| iid).collect();
        if request.scan.top_k.as_ref().is_some_and(|top| top.score.is_some()) {
            work.retained_rank_rows = rows.len();
            work.scores = rtdb_query::CandidateScores::from_verified(verified.into_iter().map(|(key, iid)| (iid, key.score)).collect(), request.memory)?
                .map(|scores| scores.with_rows(rows));
        }
        Ok(Some(iids))
    }
}
