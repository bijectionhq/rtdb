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
    /// Where an accelerator records the corpus statistics `search_score` ranks against in this query.
    pub corpora: &'a CorpusRegistry,
    /// Every partition the database's scan reads, this request's `snapshot` among them.
    pub partitions: &'a [Snapshot],
    /// Resolves candidates within this partition exactly as the scan will, for an accelerator that must know which are visible before it can narrow.
    pub verifier: &'a dyn CandidateVerifier,
}

/// The rows the scan would return for a set of IIDs, after resolution and the scan's own predicates.
#[async_trait]
pub trait CandidateVerifier: Send + Sync {
    /// Each visible row's IID with its value of `column`.
    async fn visible(&self, iids: BTreeSet<Iid>, column: rtdb_plan::ColumnId) -> Result<Vec<(Iid, Value)>>;

    /// Counts the corpus from authoritative rows when an optional index cannot supply it.
    async fn snapshot_corpus(
        &self, _column: &str, _at: rtdb_types::Timestamp, _partitions: &[Snapshot],
    ) -> Result<Arc<dyn CorpusSource>> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.query/scan-corpus-unavailable", "This verifier cannot scan the corpus"))
    }
}

/// Something that can count, for any terms, the documents and tokens of the corpus a score is relative to.
pub trait CorpusSource: Send + Sync {
    fn corpus(&self, query: &rtdb_text::Query) -> Result<rtdb_text::Corpus>;
}

/// The corpus sources the scans of one query opened, once each however often a scan reopens; a score sums them all, whatever column or table each came from.
/// The score parameters for a query text are fixed at its first use and shared by pruning and expression evaluation.
#[derive(Default)]
pub struct CorpusRegistry {
    requested: bool,
    state: parking_lot::Mutex<Corpora>,
}

#[derive(Default)]
pub struct Corpora { sources: std::collections::BTreeMap<String, Arc<dyn CorpusSource>>, computed: std::collections::BTreeMap<String, Arc<rtdb_text::Scorer>> }

impl CorpusRegistry {
    pub(crate) fn for_query(requested: bool) -> Self { Self { requested, state: Default::default() } }
    pub fn scores_requested(&self) -> bool { self.requested }

    /// `key` names the source, such as its object key; a source already registered under it is kept.
    pub fn register(&self, key: String, source: Arc<dyn CorpusSource>) {
        let mut corpora = self.state.lock();
        corpora.sources.entry(key).or_insert(source);
    }
    pub fn holds(&self, key: &str) -> bool { self.state.lock().sources.contains_key(key) }
}

impl rtdb_expr::CorpusLookup for CorpusRegistry {
    fn scorer(&self, query: &str) -> Arc<rtdb_text::Scorer> {
        let mut corpora = self.state.lock();
        if let Some(computed) = corpora.computed.get(query) { return Arc::clone(computed); }
        let terms = rtdb_text::Query::parse(query);
        let mut total = rtdb_text::Corpus::default();
        let summed = corpora.sources.values().try_for_each(|source| { total.add(&source.corpus(&terms).ok()?); Some(()) });
        let corpus = summed.filter(|_| total.documents > 0).map(|_| &total);
        let scorer = Arc::new(rtdb_text::Scorer::new(terms, corpus));
        corpora.computed.insert(query.to_string(), Arc::clone(&scorer));
        scorer
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
