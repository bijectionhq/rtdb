use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, OwnedBatch};
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
    pub fn permits_ranked_uuid_id(&self, id: rtdb_plan::ColumnId) -> bool {
        let table = &self.scan.table.name.table;
        !table.is_owned_event_data()
            && self.scan.columns.iter().any(|field| field.id == id && field.name == "_id"
                && field.data_type == rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Uuid))
            && crate::scan::stores_uuid_ids(table, self.partitions)
            && self.snapshot.live_has_canonical_entity_iids(table)
    }
}

/// The rows the scan would return for a set of IIDs, after resolution and the scan's own predicates.
#[async_trait]
pub trait CandidateVerifier: Send + Sync {
    /// Each visible row's IID with its value of `column`.
    async fn visible(&self, iids: BTreeSet<Iid>, column: rtdb_plan::ColumnId) -> Result<Vec<(Iid, Value)>>;
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
