use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, FixedSizeBinaryArray, Float64Array, UInt32Array, builder::{BooleanBuilder, FixedSizeBinaryBuilder}};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_index::Snapshot;
use rtdb_plan::{Expr, Scan, Schema};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{Iid, Result, TemporalBounds, Value};
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

#[async_trait]
pub trait CandidateVerifier: Send + Sync {
    /// Resolves the scan, applies its predicates, and scores each surviving UTF-8 cell.
    async fn score_visible(&self, iids: CandidateIids, column: rtdb_plan::ColumnId, scorer: &rtdb_text::Scorer) -> Result<CandidateScores>;
}

#[derive(Clone)]
pub struct CandidateScores {
    iids: FixedSizeBinaryArray,
    scores: Float64Array,
    _reservation: Arc<MemoryReservation>,
}

impl CandidateScores {
    pub(crate) fn new(iids: FixedSizeBinaryArray, scores: Float64Array, reservation: Arc<MemoryReservation>) -> Self {
        debug_assert_eq!(iids.len(), scores.len());
        Self { iids, scores, _reservation: reservation }
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

    pub fn from_array(values: FixedSizeBinaryArray, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if values.value_length() != 16 || values.null_count() != 0 {
            return Err(crate::invalid("candidate-iid", "Candidate selections require non-null 16-byte IIDs"));
        }
        let _scratch = memory.reserve(values.len().saturating_mul(size_of::<usize>()))?;
        let mut positions = (0..values.len()).collect::<Vec<_>>();
        positions.sort_unstable_by(|&left, &right| values.value(left).cmp(values.value(right)));
        positions.dedup_by(|left, right| values.value(*left) == values.value(*right));
        Self::build(positions.len(), positions.into_iter().map(|row| {
            Iid(values.value(row).try_into().expect("fixed width iid"))
        }), memory)
    }

    pub fn union(&self, other: &Self, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut left = self.iter().peekable();
        let mut right = other.iter().peekable();
        let merged = std::iter::from_fn(|| match (left.peek(), right.peek()) {
            (Some(a), Some(b)) if a == b => { right.next(); left.next() }
            (Some(a), Some(b)) if a < b => left.next(),
            (_, Some(_)) => right.next(),
            _ => left.next(),
        });
        Self::build(self.len().saturating_add(other.len()), merged, memory)
    }

    fn build(capacity: usize, iids: impl IntoIterator<Item = Iid>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(256usize.saturating_add(capacity.saturating_mul(16)))?;
        let mut values = FixedSizeBinaryBuilder::with_capacity(capacity, 16);
        for iid in iids { values.append_value(iid.0).map_err(|error| crate::invalid("candidate-iid", error))?; }
        Ok(Self { values: values.finish(), _reservation: reservation })
    }

    pub fn len(&self) -> usize { self.values.len() }
    pub fn is_empty(&self) -> bool { self.values.is_empty() }
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Iid> + '_ {
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

    pub(crate) fn between(&self, lower: Iid, upper: Iid) -> impl Iterator<Item = Iid> + '_ {
        (self.lower_bound(lower)..self.len()).map(|row| {
            Iid(self.values.value(row).try_into().expect("fixed width iid"))
        }).take_while(move |iid| *iid <= upper)
    }

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
/// The scan still resolves each candidate's history and re-checks every predicate, so narrowing decides cost, never results.
#[async_trait]
pub trait ScanAccelerator: Send + Sync {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>>;
    /// Optionally supplies the scores of a ranked point scan alongside its candidate scope.
    async fn select_candidates(&self, request: &ScanRequest<'_>) -> Result<Option<ScanSelection>> {
        self.candidate_iids(request).await?.map(|iids| {
            Ok(ScanSelection { iids: CandidateIids::from_set(iids, request.memory)?, scores: None })
        }).transpose()
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

    fn select(&self, page: usize, rows: &UInt32Array) -> (BooleanArray, BooleanBuffer) {
        let mut values = BooleanBuilder::with_capacity(rows.len());
        let mut answered = BooleanBufferBuilder::new(rows.len());
        for &row in rows.values() {
            let value = self.value(page, row as usize);
            answered.append(value.is_ok());
            values.append_option(value.ok().flatten());
        }
        (values.finish(), answered.finish())
    }
}

/// The values of a MATCH the scan evaluates, on every row of some of the data files it reads, by the file's object key.
#[derive(Clone, Default)]
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
