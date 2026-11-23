use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array, builder::FixedSizeBinaryBuilder};
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::keys::TrieKey;
use rtdb_index::{RowSource, Snapshot};
use rtdb_plan::{ColumnId, Expr, Scan, Schema};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{EventHeader, EventOrigin, Iid, Result, RowVersion, TemporalBounds, Value};
use tokio_util::sync::CancellationToken;

use crate::context::Execution;

/// Evaluates an expression with no column references, such as a literal or a bound parameter.
#[async_trait]
pub trait ConstantEvaluator: Send + Sync {
    async fn constant(&self, expression: &Expr) -> Result<Option<Value>>;
}

/// What an accelerator sees of one partition's scan before it opens.
pub struct ScanRequest<'a> {
    pub database: &'a str,
    pub scan: &'a Scan,
    pub snapshot: &'a Snapshot,
    pub storage: &'a Arc<BufferPool>,
    pub bounds: TemporalBounds,
    pub memory: &'a Arc<MemoryBudget>,
    pub current_time: rtdb_types::Instant,
    pub cancellation: &'a CancellationToken,
    pub constants: &'a dyn ConstantEvaluator,
    /// Every partition the database's scan reads, this request's `snapshot` among them.
    pub partitions: &'a [Snapshot],
    /// Resolves candidates within this partition exactly as the scan will, for an accelerator that must know which are visible before it can narrow.
    pub verifier: &'a dyn CandidateVerifier,
}

impl ScanRequest<'_> {
    pub async fn ranked_prefix_len(&self, top: &rtdb_plan::ScanTopK) -> Option<usize> {
        let count = async |expression: &Expr| {
            let value = self.constants.constant(expression).await.ok()??;
            rtdb_expr::integer(&value).and_then(crate::limit_bounds::checked_row_count)
        };
        let offset = match &top.offset { Some(offset) => count(offset).await?, None => 0 };
        let fetch = count(&top.limit).await?;
        usize::try_from(crate::limit_bounds::sort_prefix_len(offset, Some(fetch))).ok()
    }

    pub fn ranked_id_is_uuid(&self, id: ColumnId) -> bool {
        let uuid = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid);
        self.scan.columns.iter().any(|field| field.id == id && field.name == "_id" && field.data_type == uuid)
    }

    pub fn permits_ranked_uuid_id(&self, id: ColumnId) -> bool {
        let table = &self.scan.table.name.table;
        !table.is_owned_event_data()
            && self.ranked_id_is_uuid(id)
            && crate::scan::stores_uuid_ids(table, self.partitions)
            && self.snapshot.live_has_canonical_entity_iids(table)
    }
}

#[async_trait]
pub trait CandidateVerifier: Send + Sync {
    fn permits_search_witness(&self) -> bool { false }

    fn partition_ranking(&self) -> Option<&PartitionRanking> { None }

    /// Resolves the scan, applies its predicates, and scores each surviving UTF-8 cell.
    async fn score_visible(&self, iids: BTreeSet<Iid>, column: ColumnId, ascending_id: Option<ColumnId>, scorer: &rtdb_text::Scorer) -> Result<CandidateScores>;

    async fn score_resolved(&self, _rows: Vec<ResolvedSearchRow>, _column: ColumnId, _ascending_id: Option<ColumnId>,
        _scorer: &rtdb_text::Scorer) -> Result<Option<CandidateScores>> { Ok(None) }
}

pub struct PartitionRanking {
    winners: parking_lot::Mutex<PartitionWinners>,
    work: parking_lot::Mutex<RankWorkState>,
    ready: tokio::sync::Notify,
    memory: Arc<MemoryBudget>,
}

#[derive(Default)]
struct RankWorkState {
    waiting: BTreeMap<usize, f64>,
    in_flight: BTreeSet<usize>,
}

pub struct PartitionRankWork<'a> {
    ranking: &'a PartitionRanking,
    partition: usize,
    _charge: Arc<MemoryReservation>,
}

pub struct RankWorkPermit<'a> {
    ranking: &'a PartitionRanking,
    partition: usize,
}

impl PartitionRankWork<'_> {
    pub async fn acquire(&self, bound: f64, cancellation: &CancellationToken) -> Result<RankWorkPermit<'_>> {
        const MAX_IN_FLIGHT: usize = 4;
        self.ranking.work.lock().waiting.insert(self.partition, bound);
        loop {
            if cancellation.is_cancelled() {
                self.ranking.work.lock().waiting.remove(&self.partition);
                self.ranking.ready.notify_waiters();
                return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Interrupted,
                    "rtdb.query/cancelled", "Ranked work cancelled during admission"));
            }
            let notified = self.ranking.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut work = self.ranking.work.lock();
                let best = work.waiting.iter().max_by(|(a_id, a), (b_id, b)| a.total_cmp(b).then_with(|| b_id.cmp(a_id)));
                if work.in_flight.len() < MAX_IN_FLIGHT && best.is_some_and(|(&partition, _)| partition == self.partition) {
                    work.waiting.remove(&self.partition);
                    work.in_flight.insert(self.partition);
                    self.ranking.ready.notify_waiters();
                    return Ok(RankWorkPermit { ranking: self.ranking, partition: self.partition });
                }
            }
            cancellation.run_until_cancelled(notified).await;
        }
    }
}

impl Drop for RankWorkPermit<'_> {
    fn drop(&mut self) {
        self.ranking.work.lock().in_flight.remove(&self.partition);
        self.ranking.ready.notify_waiters();
    }
}

impl Drop for PartitionRankWork<'_> {
    fn drop(&mut self) {
        let mut work = self.ranking.work.lock();
        work.waiting.remove(&self.partition);
        self.ranking.ready.notify_waiters();
    }
}

#[derive(Default)]
struct PartitionWinners {
    keys: BTreeSet<ScoredIid>,
    scores: BTreeMap<Iid, f64>,
    charge: Option<Arc<MemoryReservation>>,
}

#[derive(Clone, Copy, PartialEq)]
struct ScoredIid { iid: Iid, score: f64 }

impl Eq for ScoredIid {}
impl PartialOrd for ScoredIid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for ScoredIid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score.total_cmp(&other.score).then_with(|| self.iid.cmp(&other.iid))
    }
}

impl PartitionRanking {
    pub(crate) fn new(memory: Arc<MemoryBudget>) -> Self { Self { winners: Default::default(), work: Default::default(), ready: Default::default(), memory } }

    pub fn register(&self, partition: &Snapshot) -> Option<PartitionRankWork<'_>> {
        let charge = self.memory.reserve(256).ok()?;
        let partition = std::ptr::from_ref(partition) as usize;
        Some(PartitionRankWork { ranking: self, partition, _charge: charge })
    }

    pub fn add_verified(&self, limit: usize, rows: impl IntoIterator<Item = (Iid, f64)>) {
        if !(1..=4096).contains(&limit) { return; }
        let mut winners = self.winners.lock();
        if winners.charge.is_none() {
            let Ok(charge) = self.memory.reserve(256 + limit * 160) else { return; };
            winners.charge = Some(charge);
        }
        for (iid, score) in rows {
            if !score.is_finite() { continue; }
            let score = if score == 0.0 { 0.0 } else { score };
            if let Some(&previous) = winners.scores.get(&iid) {
                if previous >= score { continue; }
                winners.keys.remove(&ScoredIid { iid, score: previous });
            }
            winners.scores.insert(iid, score);
            winners.keys.insert(ScoredIid { iid, score });
            if winners.keys.len() > limit {
                let removed = winners.keys.pop_first().expect("ranking exceeded its limit");
                winners.scores.remove(&removed.iid);
            }
        }
    }

    pub fn cutoff(&self, limit: usize) -> Option<f64> {
        let winners = self.winners.lock();
        (winners.keys.len() == limit).then(|| winners.keys.first().map(|key| key.score)).flatten()
    }
}

#[derive(Clone)]
pub enum SearchRowLocation {
    Persisted { trie: TrieKey, page: u32, row: u32 },
    Live(RowSource),
}

#[derive(Clone)]
pub struct ResolvedSearchRow {
    pub version: RowVersion,
    pub origin: Option<EventOrigin>,
    pub location: SearchRowLocation,
    pub visibility: TemporalBounds,
}

pub struct LocatedHistoryEvent<W> {
    pub header: EventHeader,
    pub origin: Option<EventOrigin>,
    pub location: SearchRowLocation,
    pub witness: Option<W>,
}

pub fn resolve_located_point<W: Copy>(mut history: BTreeMap<Iid, Vec<LocatedHistoryEvent<W>>>, bounds: TemporalBounds,
    clamp_valid_time: bool, memory: &Arc<MemoryBudget>) -> Result<Option<Vec<(ResolvedSearchRow, Option<W>)>>> {
    if !bounds.system_time.is_point() || !bounds.valid_time.is_point() { return Ok(None); }
    let mut visible = Vec::new();
    let mut resolver = rtdb_temporal::HistoryResolver::new_in(bounds, clamp_valid_time, Arc::clone(memory));
    for events in history.values_mut() {
        events.sort_unstable_by_key(|event| (std::cmp::Reverse(event.header.system_position()), event.header.valid_time.lower()));
        if events.windows(2).any(|pair| {
            let (left, right) = (&pair[0], &pair[1]);
            left.header.version() == right.header.version() && (left.origin.is_none() || left.origin != right.origin
                || left.header.op != right.header.op || left.header.valid_time.intersects(right.header.valid_time))
        }) { return Ok(None); }
        let first = visible.len();
        for event in events {
            rtdb_storage::audience_work::scan_items(1)?;
            resolver.resolve_fragments(&event.header, |valid_time, system_time| {
                visible.push((ResolvedSearchRow { version: event.header.version(), origin: event.origin, location: event.location.clone(),
                    visibility: TemporalBounds { valid_time, system_time } }, event.witness));
                Ok(())
            })?;
        }
        if visible.len() - first > 1 { return Ok(None); }
    }
    Ok(Some(visible))
}

#[derive(Clone)]
pub struct CandidateScores {
    iids: FixedSizeBinaryArray,
    scores: Float64Array,
    ids: Option<Int64Array>,
    rows: BTreeMap<Iid, SnapshotRow>,
    _reservation: Arc<MemoryReservation>,
}

#[derive(Clone)]
pub struct SnapshotRow {
    pub(crate) batch: Arc<crate::columnar::ColumnBatch>,
    pub(crate) row: usize,
}

impl CandidateScores {
    pub(crate) fn new(iids: FixedSizeBinaryArray, scores: Float64Array, reservation: Arc<MemoryReservation>) -> Self {
        debug_assert_eq!(iids.len(), scores.len());
        Self { iids, scores, ids: None, rows: BTreeMap::new(), _reservation: reservation }
    }

    pub(crate) fn release_rows(&mut self) { self.rows.clear(); }

    pub fn with_rows(mut self, rows: BTreeMap<Iid, SnapshotRow>) -> Self {
        self.rows = rows;
        self
    }

    pub fn row(&self, iid: Iid) -> Option<SnapshotRow> { self.rows.get(&iid).cloned() }

    pub(crate) fn with_ids(mut self, ids: Option<Int64Array>) -> Self {
        debug_assert!(ids.as_ref().is_none_or(|ids| ids.len() == self.iids.len()));
        self.ids = ids;
        self
    }

    pub fn from_verified(mut scores: Vec<(Iid, f64)>, memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        scores.sort_unstable_by_key(|&(iid, _)| iid);
        if scores.windows(2).any(|pair| pair[0].0 == pair[1].0) { return Ok(None); }
        let reservation = memory.reserve(256usize.saturating_add(scores.len().saturating_mul(48)))?;
        let mut iids = FixedSizeBinaryBuilder::with_capacity(scores.len(), 16);
        for &(iid, _) in &scores {
            iids.append_value(iid.0).map_err(|error| crate::invalid("verifier-iid", error))?;
        }
        let values = Float64Array::from_iter_values(scores.into_iter().map(|(_, score)| score));
        Ok(Some(Self::new(iids.finish(), values, reservation)))
    }

    pub(crate) fn score(&self, iid: Iid) -> Option<f64> {
        let (mut first, mut end) = (0, self.iids.len());
        while first < end {
            let middle = first + (end - first) / 2;
            match self.iids.value(middle).cmp(iid.0.as_slice()) {
                std::cmp::Ordering::Less => first = middle + 1,
                std::cmp::Ordering::Greater => end = middle,
                std::cmp::Ordering::Equal => return Some(self.scores.value(middle)),
            }
        }
        None
    }

    pub fn iter(&self) -> impl Iterator<Item = (Iid, f64)> + '_ {
        (0..self.iids.len()).map(|row| {
            (Iid(self.iids.value(row).try_into().expect("fixed width iid")), self.scores.value(row))
        })
    }

    pub fn ranked(&self) -> impl Iterator<Item = (Iid, f64, Option<i64>)> + '_ {
        self.iter().enumerate().map(|(row, (iid, score))| {
            let id = self.ids.as_ref().filter(|ids| ids.is_valid(row)).map(|ids| ids.value(row));
            (iid, score, id)
        })
    }
}

pub struct ScanSelection {
    pub iids: CandidateIids,
    pub scores: Option<CandidateScores>,
}

#[derive(Clone)]
pub struct CandidateIids {
    values: FixedSizeBinaryArray,
    _reservation: Arc<MemoryReservation>,
}

impl CandidateIids {
    pub fn from_set(iids: BTreeSet<Iid>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::build(iids.len(), iids, memory)
    }

    fn build(capacity: usize, iids: impl IntoIterator<Item = Iid>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(256usize.saturating_add(capacity.saturating_mul(16)))?;
        let mut values = FixedSizeBinaryBuilder::with_capacity(capacity, 16);
        for iid in iids { values.append_value(iid.0).map_err(|error| crate::invalid("candidate-iid", error))?; }
        Ok(Self { values: values.finish(), _reservation: reservation })
    }

    pub fn len(&self) -> usize { self.values.len() }
    pub fn is_empty(&self) -> bool { self.values.is_empty() }
    pub fn iter(&self) -> impl Iterator<Item = Iid> + '_ {
        (0..self.len()).map(|row| Iid(self.values.value(row).try_into().expect("fixed width iid")))
    }

    fn lower_bound(&self, iid: Iid) -> usize {
        let (mut first, mut end) = (0, self.len());
        while first < end {
            let middle = first + (end - first) / 2;
            if self.values.value(middle) < iid.0.as_slice() { first = middle + 1; } else { end = middle; }
        }
        first
    }

    pub fn contains(&self, iid: Iid) -> bool { self.intersects(iid, iid) }

    pub(crate) fn intersects(&self, lower: Iid, upper: Iid) -> bool {
        let row = self.lower_bound(lower);
        row < self.len() && self.values.value(row) <= upper.0.as_slice()
    }

    pub(crate) fn retain(self, mut keep: impl FnMut(Iid) -> bool, memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::build(self.len(), self.iter().filter(|&iid| keep(iid)), memory)
    }
}

/// Exact ordered output for a point scan in its requested order.
pub enum OrderedScan {
    Payload { candidates: Box<dyn OrderedCandidates>, verify: bool },
    Covered(Box<dyn OrderedBatches>),
}

#[async_trait]
pub trait OrderedCandidates: Send {
    /// A bounded prefix of the remaining IIDs in index order. An empty batch ends the seek.
    async fn next_iids(&mut self, maximum: usize) -> Result<Vec<Iid>>;
}

#[async_trait]
pub trait OrderedBatches: Send {
    async fn next_batch(&mut self) -> Result<Option<OwnedBatch>>;
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
}

/// Narrows a scan before it opens.
/// It returns a superset of the IIDs whose visible row can satisfy the scan's predicates at its bounds, or `None` to leave the scan unnarrowed.
/// The scan applies its visibility and predicates to the candidates.
/// The ranked point path may emit the matching rows its verifier already resolved.
#[async_trait]
pub trait ScanAccelerator: Send + Sync {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>>;
    /// Resolves complete, pinned histories for an explicit set of entities.
    async fn point_rows(&self, _request: &ScanRequest<'_>, _iids: &BTreeSet<Iid>) -> Result<Option<Vec<ResolvedSearchRow>>> { Ok(None) }
    /// Optionally supplies the scores of a ranked point scan alongside its candidate scope.
    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        let Some(iids) = self.candidate_iids(request).await? else { return Ok(None); };
        match CandidateIids::from_set(iids, request.memory) {
            Ok(iids) => Ok(Some(ScanSelection { iids, scores: None })),
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => Ok(None),
            Err(error) => Err(error),
        }
    }
    /// The first matching rows of a point read, in the requested SQL order.
    /// A covered result applies the window and projects the requested scan positions.
    /// A payload seek leaves windowing to the scan after any table predicates are checked.
    /// A distinct seek emits one representative of each qualifying SQL key.
    async fn ordered(&self, _request: &ScanRequest<'_>, _window: std::ops::Range<usize>, _projection: &[usize]) -> Result<Option<OrderedScan>> { Ok(None) }
    /// The values of `match(column, query)` on the rows of whichever of the scan's data files the accelerator can answer for, so the scan reads no text there.
    /// Each answer must equal MATCH evaluated on the row.
    async fn match_answers(&self, _request: &ScanRequest<'_>, _column: &str, _query: &str) -> Result<Option<MatchAnswers>> { Ok(None) }
}

/// MATCH on the rows of one data file, by page and row within the page: `None` where the column is null or absent, as MATCH is.
pub trait FileMatches: Send + Sync {
    fn value(&self, page: usize, row: usize) -> Result<Option<bool>>;
}

/// The values of a MATCH the scan evaluates, on every row of some of the data files it reads, by the file's object key.
#[derive(Default)]
pub struct MatchAnswers { files: std::collections::BTreeMap<String, Arc<dyn FileMatches>> }

impl MatchAnswers {
    pub fn insert(&mut self, data_file: &rtdb_storage::ObjectKey, matches: Arc<dyn FileMatches>) { self.files.insert(data_file.as_str().to_string(), matches); }
    pub fn file(&self, data_file: &rtdb_storage::ObjectKey) -> Option<&Arc<dyn FileMatches>> { self.files.get(data_file.as_str()) }
    pub fn is_empty(&self) -> bool { self.files.is_empty() }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Arc<dyn FileMatches>)> { self.files.iter().map(|(key, matches)| (key.as_str(), matches)) }
}

pub(crate) struct ExecutionConstants<'a>(pub(crate) &'a Execution);

#[async_trait]
impl ConstantEvaluator for ExecutionConstants<'_> {
    async fn constant(&self, expression: &Expr) -> Result<Option<Value>> {
        if !matches!(expression, Expr::Literal(_) | Expr::Parameter { .. }) { return Ok(None); }
        let scope = self.0.scope()?;
        Ok(Some(self.0.evaluate(&scope, expression, &Schema::default(), &[]).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_shared_cutoff_counts_distinct_finite_winners_from_each_partition() {
        let ranking = PartitionRanking::new(MemoryBudget::new(4096));
        let (first, second, third) = (Iid([1; 16]), Iid([2; 16]), Iid([3; 16]));
        ranking.add_verified(2, [(first, 3.0), (first, 4.0), (second, f64::NAN)]);
        assert_eq!(ranking.cutoff(2), None);
        ranking.add_verified(2, [(second, -0.0)]);
        assert_eq!(ranking.cutoff(2).unwrap().to_bits(), 0.0f64.to_bits());
        ranking.add_verified(2, [(third, 5.0), (first, 2.0)]);
        assert_eq!(ranking.cutoff(2), Some(4.0));
    }
}

#[cfg(test)]
#[tokio::test]
async fn a_free_rank_slot_admits_the_highest_waiting_bound_and_cancelled_work_releases_priority() {
    use std::task::Poll;

    let memory = MemoryBudget::new(1 << 20);
    let ranking = PartitionRanking::new(Arc::clone(&memory));
    let workers: Vec<_> = (0..7).map(|partition| PartitionRankWork {
        ranking: &ranking, partition, _charge: memory.reserve(256).unwrap(),
    }).collect();
    let stop = CancellationToken::new();
    let mut permits = Vec::new();
    for worker in &workers[..4] { permits.push(worker.acquire(1.0, &stop).await.unwrap()); }

    let cancelled = CancellationToken::new();
    let mut low = std::pin::pin!(workers[4].acquire(2.0, &stop));
    let mut high = std::pin::pin!(workers[5].acquire(4.0, &stop));
    let mut interrupted = std::pin::pin!(workers[6].acquire(f64::INFINITY, &cancelled));
    assert!(futures::poll!(&mut low).is_pending());
    assert!(futures::poll!(&mut high).is_pending());
    assert!(futures::poll!(&mut interrupted).is_pending());
    drop(permits.pop());
    assert!(futures::poll!(&mut high).is_pending());
    cancelled.cancel();
    assert!(matches!(futures::poll!(&mut interrupted), Poll::Ready(Err(_))));
    assert!(futures::poll!(&mut low).is_pending());
    let permit = high.await.unwrap();
    assert!(futures::poll!(&mut low).is_pending());
    drop(permit);
    assert!(low.await.is_ok());
}
