use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, FixedSizeBinaryArray, Float64Array, Int64Array, builder::FixedSizeBinaryBuilder};
use arrow_buffer::Buffer;
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

    /// The query a constant expression holds, tokenised by `analyzer`.
    async fn search_query(&self, expression: &Expr, analyzer: rtdb_text::Analyzer) -> Result<Option<Arc<PreparedSearchQuery>>> {
        Ok(match self.constant(expression).await? {
            Some(Value::Utf8(text)) => Some(Arc::new(PreparedSearchQuery::new(text, analyzer))),
            _ => None,
        })
    }
}

pub struct PreparedSearchQuery {
    text: String,
    query: Arc<rtdb_text::Query>,
    scorer: std::sync::OnceLock<Arc<rtdb_text::Scorer>>,
}

impl PreparedSearchQuery {
    pub fn new(text: String, analyzer: rtdb_text::Analyzer) -> Self {
        let query = Arc::new(rtdb_text::Query::analyzed(analyzer, &text));
        Self { text, query, scorer: Default::default() }
    }

    pub fn query(&self) -> &rtdb_text::Query { &self.query }

    pub(crate) fn parsed(&self) -> Arc<rtdb_text::Query> { Arc::clone(&self.query) }

    pub fn scorer(&self) -> &Arc<rtdb_text::Scorer> {
        self.scorer.get_or_init(|| Arc::new(rtdb_text::Scorer::new(self.query.as_ref().clone(), None)))
    }
}

#[derive(Default)]
pub(crate) struct SearchQueries(tokio::sync::Mutex<Vec<((Expr, rtdb_text::Analyzer), Option<Arc<PreparedSearchQuery>>)>>);

/// What an accelerator sees of one partition's scan before it opens.
#[derive(Clone, Copy)]
pub struct ScanRequest<'a> {
    pub database: &'a str,
    pub scan: &'a Scan,
    pub candidate_iids: Option<&'a CandidateIids>,
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

impl<'a> ScanRequest<'a> {
    pub fn iid_scope(&self) -> Option<IidScope<'a>> {
        self.candidate_iids.map(IidScope::Arrow).or_else(|| self.scan.iids.as_ref().map(IidScope::Set))
    }

    pub async fn ranked_prefix_len(&self, top: &rtdb_plan::ScanTopK) -> Option<usize> {
        let count = async |expression: &Expr| {
            let value = self.constants.constant(expression).await.ok()??;
            rtdb_expr::integer(&value).and_then(crate::limit_bounds::checked_row_count)
        };
        let offset = match &top.offset { Some(offset) => count(offset).await?, None => 0 };
        let fetch = count(&top.limit).await?;
        usize::try_from(crate::limit_bounds::sort_prefix_len(offset, Some(fetch))).ok()
    }

    pub async fn ranked_scorer(&self, top: &rtdb_plan::ScanTopK) -> Result<Option<Arc<rtdb_text::Scorer>>> {
        let Some(query) = self.constants.search_query(&top.query, top.analyzer).await? else { return Ok(None); };
        let Some(corpus) = &top.corpus else { return Ok(Some(Arc::clone(query.scorer()))); };
        let [documents, tokens, frequencies] = &**corpus;
        let (Some(documents), Some(tokens), Some(frequencies)) = (
            self.constants.constant(documents).await?,
            self.constants.constant(tokens).await?,
            self.constants.constant(frequencies).await?,
        ) else { return Ok(None); };
        let scorer = rtdb_expr::search_corpus_scorer(query.query().clone(), &documents, &tokens, &frequencies);
        Ok(scorer.ok().map(Arc::new))
    }

    pub fn ranked_id_is_uuid(&self, id: ColumnId) -> bool {
        let uuid = rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid);
        self.scan.columns.iter().any(|field| field.id == id && field.name == "_id" && field.data_type == uuid)
            && crate::scan::stores_uuid_ids(&self.scan.table.name.table, self.partitions)
    }

    pub fn permits_ranked_uuid_id(&self, id: ColumnId) -> bool {
        let table = &self.scan.table.name.table;
        !table.is_owned_event_data()
            && self.ranked_id_is_uuid(id)
            && self.snapshot.live_has_canonical_entity_iids(table)
    }

    pub fn ranked_id_is_integer(&self, id: ColumnId) -> bool {
        use rtdb_types::{LogicalType, ScalarType};
        let integer = |data_type: &LogicalType| {
            data_type.legs().iter().all(|leg| matches!(leg, LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64)))
        };
        self.scan.columns.iter().any(|field| field.id == id && field.name == "_id" && integer(&field.data_type))
            && self.partitions.iter().all(|snapshot| snapshot.column_types_for(&self.scan.table.name.table, ["_id".into()]).get("_id")
                .is_none_or(|data_type| *data_type == LogicalType::Nothing || integer(data_type)))
    }

    #[doc(hidden)]
    pub async fn ranked_selection(&self, rows: Vec<crate::VerifiedRankRow>) -> Result<Option<ScanSelection>> {
        let scores = rows.iter().map(|winner| (winner.row.version.iid, winner.key.score)).collect();
        let Some(scores) = CandidateScores::from_verified(scores, self.memory)? else { return Ok(None); };
        let iids = CandidateIids::build(scores.iids.len(), scores.iter().map(|(iid, _)| iid), self.memory)?;
        let rows = match self.verifier.gather_ranked(rows).await {
            Ok(rows) => rows,
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => BTreeMap::new(),
            Err(error) => return Err(error),
        };
        Ok(Some(ScanSelection { iids, scores: Some(scores.with_rows(rows)) }))
    }
}

#[async_trait]
pub trait CandidateVerifier: Send + Sync {
    /// Resolves the scan, applies its predicates, and scores each surviving UTF-8 cell.
    async fn score_visible(&self, iids: BTreeSet<Iid>, column: ColumnId, ascending_id: Option<ColumnId>, scorer: &rtdb_text::Scorer) -> Result<CandidateScores>;

    /// Uses the pinned row locations when available and resolves the IIDs in primary history otherwise.
    async fn verify_ranked(&self, _iids: &BTreeSet<Iid>, _rows: Option<Vec<ResolvedSearchRow>>, _column: ColumnId, _ascending_id: Option<ColumnId>,
        _scorer: &rtdb_text::Scorer) -> Result<Option<Vec<crate::VerifiedRankRow>>> { Ok(None) }

    /// Gathers the scan's output columns from verified versions after ranking.
    async fn gather_ranked(&self, _rows: Vec<crate::VerifiedRankRow>) -> Result<BTreeMap<Iid, SnapshotRow>> { Ok(BTreeMap::new()) }

    /// The IIDs whose visible version at the scan's cut satisfies every predicate of the scan, or `None` where this verifier cannot say.
    async fn retain_visible(&self, _iids: &CandidateIids) -> Result<Option<CandidateIids>> { Ok(None) }
}

#[derive(Clone)]
pub enum SearchRowLocation {
    Persisted { trie: TrieKey, page: u32, row: u32 },
    Live(RowSource),
}

impl SearchRowLocation {
    pub(crate) fn persisted_page(&self) -> Option<(&TrieKey, u32)> {
        match self {
            Self::Persisted { trie, page, .. } => Some((trie, *page)),
            Self::Live(_) => None,
        }
    }
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
        events.sort_unstable_by_key(|event| (std::cmp::Reverse(event.header.precedence()), event.header.valid_time.lower()));
        if events.windows(2).any(|pair| {
            let (left, right) = (&pair[0], &pair[1]);
            if left.header.version() != right.header.version() { return false; }
            match (left.origin, right.origin) {
                (Some(a), Some(b)) if a == b => left.header.op != right.header.op || left.header.valid_time.intersects(right.header.valid_time),
                (Some(a), Some(b)) => a.0[..16] != b.0[..16] || a.source_block() != b.source_block() || a.ordinal() == b.ordinal(),
                _ => true,
            }
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

pub struct OrderedIidBuffer {
    values: Vec<[u8; 16]>,
    reservation: Arc<MemoryReservation>,
}

impl OrderedIidBuffer {
    pub fn with_capacity(capacity: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(Self::reserved_bytes(capacity)?)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|error| crate::invalid("ordered-iid-capacity", error))?;
        Ok(Self { values, reservation })
    }

    fn reserved_bytes(capacity: usize) -> Result<usize> {
        capacity.checked_mul(16).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| crate::invalid("ordered-iid-capacity", "Ordered IID buffer exceeds addressable memory"))
    }

    pub fn len(&self) -> usize { self.values.len() }
    pub fn is_empty(&self) -> bool { self.values.is_empty() }
    pub fn iter(&self) -> impl Iterator<Item = Iid> + '_ { self.values.iter().copied().map(Iid) }

    pub fn push(&mut self, iid: Iid) {
        assert!(self.len() < self.values.capacity(), "Ordered IID buffer requires admitted capacity");
        self.values.push(iid.0);
    }

    fn reserve(&mut self, capacity: usize) -> Result<()> {
        let current = self.values.capacity();
        if capacity <= current { return Ok(()); }

        let retained = Self::reserved_bytes(capacity)?;
        let peak = retained.checked_add(current * 16)
            .ok_or_else(|| crate::invalid("ordered-iid-capacity", "Ordered IID growth exceeds addressable memory"))?;
        let charge = Arc::get_mut(&mut self.reservation).expect("Ordered IID buffer owns its reservation");
        let previous = charge.bytes();
        charge.grow_to(peak)?;
        let growth = self.values.try_reserve_exact(capacity - self.values.len());
        charge.shrink_to(if growth.is_ok() { retained } else { previous })?;
        growth.map_err(|error| crate::invalid("ordered-iid-capacity", error))
    }

    /// Grows the buffer to hold `capacity`, as `reserve` does, or answers `false` and changes nothing where the budget has
    /// no room for it.
    fn try_reserve(&mut self, capacity: usize) -> Result<bool> {
        let current = self.values.capacity();
        if capacity <= current { return Ok(true); }
        let retained = Self::reserved_bytes(capacity)?;
        let peak = retained.checked_add(current * 16)
            .ok_or_else(|| crate::invalid("ordered-iid-capacity", "Ordered IID growth exceeds addressable memory"))?;
        let charge = Arc::get_mut(&mut self.reservation).expect("Ordered IID buffer owns its reservation");
        let previous = charge.bytes();
        let Some(mut growth) = charge.budget().try_reserve(peak.saturating_sub(previous)) else { return Ok(false); };
        charge.absorb(Arc::get_mut(&mut growth).expect("a fresh reservation is unshared"))?;
        let grown = self.values.try_reserve_exact(capacity - self.values.len());
        charge.shrink_to(if grown.is_ok() { retained } else { previous })?;
        grown.map_err(|error| crate::invalid("ordered-iid-capacity", error))?;
        Ok(true)
    }

    fn sort_distinct(&mut self) {
        self.values.sort_unstable();
        self.values.dedup();
    }

    /// Merges a posting chunk into a sorted, distinct aggregate. An empty buffer starts the aggregate.
    pub fn merge_distinct(&mut self, mut chunk: Self, limit: usize) -> Result<bool> {
        debug_assert!(self.values.windows(2).all(|pair| pair[0] < pair[1]));
        chunk.sort_distinct();
        chunk.values.retain(|iid| self.values.binary_search(iid).is_err());
        let mut previous = self.len();
        let mut added = chunk.len();
        let Some(len) = previous.checked_add(added).filter(|&len| len <= limit) else {
            return Ok(false);
        };
        if added == 0 { return Ok(true); }
        if previous == 0 {
            *self = chunk;
            return Ok(true);
        }

        let capacity = self.values.capacity();
        if len > capacity {
            let preferred = len.max(capacity.saturating_mul(2)).min(limit);
            if !self.try_reserve(preferred)? { self.reserve(len)?; }
        }
        self.values.resize(len, [0; 16]);
        let mut write = len;
        while added != 0 {
            write -= 1;
            if previous != 0 && self.values[previous - 1] > chunk.values[added - 1] {
                previous -= 1;
                self.values[write] = self.values[previous];
            } else {
                added -= 1;
                self.values[write] = chunk.values[added];
            }
        }
        Ok(true)
    }

    pub fn into_sorted_membership(mut self) -> Result<CandidateIids> {
        if !self.values.windows(2).all(|pair| pair[0] < pair[1]) { self.sort_distinct(); }
        let Self { values, reservation } = self;
        let values = FixedSizeBinaryArray::try_new(16, Buffer::from_vec(values.into_flattened()), None)
            .map_err(|error| crate::invalid("candidate-iid", error))?;
        Ok(CandidateIids { values, _reservation: reservation })
    }

    pub(crate) fn sorted_membership(&self, memory: &Arc<MemoryBudget>) -> Result<CandidateIids> {
        let mut sorted = Self::with_capacity(self.len(), memory)?;
        sorted.values.extend_from_slice(&self.values);
        sorted.into_sorted_membership()
    }
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

    pub fn from_sorted(iids: &[Iid], memory: &Arc<MemoryBudget>) -> Result<Self> {
        if !iids.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(crate::invalid("candidate-iid", "Candidate IIDs must be strictly increasing"));
        }
        Self::build(iids.len(), iids.iter().copied(), memory)
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

    pub(crate) fn position(&self, iid: Iid) -> Option<usize> {
        let row = self.lower_bound(iid);
        (row < self.len() && self.values.value(row) == iid.0.as_slice()).then_some(row)
    }

    pub(crate) fn retain(self, keep: impl Fn(Iid) -> bool, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if self.iter().all(&keep) { return Ok(self); }
        Self::build(self.len(), self.iter().filter(|&iid| keep(iid)), memory)
    }

    pub(crate) fn select_visible(&self, visible: &[bool], memory: &Arc<MemoryBudget>) -> Result<Self> {
        if visible.len() != self.len() { return Err(crate::invalid("verifier-iid", "Visibility mask differs from its candidate selection")); }
        let count = visible.iter().filter(|&&selected| selected).count();
        if count == self.len() { return Ok(self.clone()); }
        Self::build(count, self.iter().zip(visible).filter_map(|(iid, &selected)| selected.then_some(iid)), memory)
    }
}

#[derive(Clone, Copy)]
pub enum IidScope<'a> {
    Arrow(&'a CandidateIids),
    Set(&'a BTreeSet<Iid>),
}

impl<'a> IidScope<'a> {
    pub fn len(self) -> usize {
        match self { Self::Arrow(iids) => iids.len(), Self::Set(iids) => iids.len() }
    }

    pub fn is_empty(self) -> bool { self.len() == 0 }

    pub fn contains(self, iid: &Iid) -> bool {
        match self { Self::Arrow(iids) => iids.contains(*iid), Self::Set(iids) => iids.contains(iid) }
    }

    pub fn intersects(self, lower: Iid, upper: Iid) -> bool {
        match self {
            Self::Arrow(iids) => iids.intersects(lower, upper),
            Self::Set(iids) => iids.range(lower..=upper).next().is_some(),
        }
    }

    pub fn iter(self) -> impl Iterator<Item = Iid> + 'a {
        let (arrow, set) = match self {
            Self::Arrow(iids) => (Some(iids), None),
            Self::Set(iids) => (None, Some(iids)),
        };
        arrow.into_iter().flat_map(CandidateIids::iter).chain(set.into_iter().flat_map(|iids| iids.iter().copied()))
    }
}

/// Exact ordered output for a point scan in its requested order.
pub enum OrderedScan {
    Payload { candidates: Box<dyn OrderedCandidates>, verify: bool },
    Covered(Box<dyn OrderedBatches>),
}

#[async_trait]
pub trait OrderedCandidates: Send {
    /// An admitted prefix of the remaining IIDs in index order. `None` ends the seek.
    async fn next_iids(&mut self, maximum: usize) -> Result<Option<OrderedIidBuffer>>;
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
    fn accepts_arrow_iids(&self) -> bool { false }

    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>>;
    /// Supplies exact ranking work for a point scan at the given cut.
    async fn rank_source<'a>(&'a self, _request: ScanRequest<'a>) -> Result<Option<Box<dyn crate::RankSource + 'a>>> { Ok(None) }

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
    /// The values of `match(column, query)` under the simple analyzer on the rows of whichever of the scan's data files the accelerator can answer for, so the scan reads no text there.
    /// Each answer must equal MATCH evaluated on the row.
    async fn match_answers(&self, _request: &ScanRequest<'_>, _column: &str, _query: &str) -> Result<Option<MatchAnswers>> { Ok(None) }
    /// Live postings of the column under the analyzer of the MATCH they answer.
    fn live_search_terms(&self, _request: &ScanRequest<'_>, _column: &str, _analyzer: rtdb_text::Analyzer) -> Option<Arc<dyn LiveSearchTerms>> { None }
    async fn match_answers_prepared(&self, request: &ScanRequest<'_>, column: &str, query: &PreparedSearchQuery) -> Result<Option<MatchAnswers>> {
        if query.query().analyzer() != rtdb_text::Analyzer::Simple { return Ok(None); }
        self.match_answers(request, column, &query.text).await
    }

}

/// Analyzed text from the admitted prefix of the snapshot's live Arrow batches.
pub trait LiveSearchTerms: Send + Sync {
    fn matches(&self, batch: &rtdb_index::IndexBatch, row: usize, query: &rtdb_text::Query) -> Option<bool>;
    fn score(&self, batch: &rtdb_index::IndexBatch, row: usize, scorer: &rtdb_text::Scorer) -> Option<f64>;
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

pub(crate) struct ExecutionConstants<'a> {
    pub(crate) execution: &'a Execution,
    pub(crate) search_queries: &'a SearchQueries,
}

#[async_trait]
impl ConstantEvaluator for ExecutionConstants<'_> {
    async fn constant(&self, expression: &Expr) -> Result<Option<Value>> {
        if !matches!(expression, Expr::Literal(_) | Expr::Parameter { .. }) { return Ok(None); }
        let scope = self.execution.scope()?;
        Ok(Some(self.execution.evaluate(&scope, expression, &Schema::default(), &[]).await?))
    }

    async fn search_query(&self, expression: &Expr, analyzer: rtdb_text::Analyzer) -> Result<Option<Arc<PreparedSearchQuery>>> {
        let mut queries = self.search_queries.0.lock().await;
        if let Some((_, query)) = queries.iter().find(|((bound, prepared), _)| bound == expression && *prepared == analyzer) {
            return Ok(query.clone());
        }
        let query = match self.constant(expression).await? {
            Some(Value::Utf8(text)) => Some(Arc::new(PreparedSearchQuery::new(text, analyzer))),
            _ => None,
        };
        queries.push(((expression.clone(), analyzer), query.clone()));
        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_index_candidate_scope_stays_in_sorted_arrow_storage() {
        let memory = MemoryBudget::new(4096);
        let first = Iid([1; 16]);
        let last = Iid([9; 16]);
        let scope = CandidateIids::from_sorted(&[first, last], &memory).unwrap();
        assert_eq!(scope.values.value(0), first.as_bytes());
        assert_eq!(scope.values.value(1), last.as_bytes());
        assert!(scope.intersects(Iid([0; 16]), first));
        assert!(!scope.intersects(Iid([2; 16]), Iid([8; 16])));
        assert_eq!(scope.iter().collect::<Vec<_>>(), [first, last]);
        assert_eq!(memory.used_bytes(), 256 + 2 * 16);
    }

    #[test]
    fn visibility_keeps_the_grant_for_an_unchanged_arrow_scope() {
        let memory = MemoryBudget::new(4096);
        let first = Iid([1; 16]);
        let last = Iid([9; 16]);
        let scope = CandidateIids::from_sorted(&[first, last], &memory).unwrap();
        let kept = scope.select_visible(&[true, true], &memory).unwrap();
        assert!(Arc::ptr_eq(&scope._reservation, &kept._reservation));
        let narrowed = scope.select_visible(&[false, true], &memory).unwrap();
        assert_eq!(narrowed.position(last), Some(0));
        assert_eq!(narrowed.position(first), None);
        drop((scope, kept));
        assert_eq!(memory.used_bytes(), 256 + 16);
    }

}
