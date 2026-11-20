use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
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
}

/// Something that can count, for any terms, the documents and tokens of the corpus a score is relative to.
pub trait CorpusSource: Send + Sync {
    fn corpus(&self, query: &rtdb_text::Query) -> Result<rtdb_text::Corpus>;
}

/// The corpus sources the scans of one query opened, once each however often a scan reopens; a score sums them all, whatever column or table each came from.
/// A query text's corpus is fixed at its first use, so every score in one query, and any ranking an accelerator pruned by, is relative to the same corpus.
#[derive(Default)]
pub struct CorpusRegistry(parking_lot::Mutex<Corpora>);

#[derive(Default)]
pub struct Corpora { sources: std::collections::BTreeMap<String, Arc<dyn CorpusSource>>, computed: std::collections::BTreeMap<String, Option<rtdb_text::Corpus>> }

impl CorpusRegistry {
    /// `key` names the source, such as its object key; a source already registered under it is kept.
    pub fn register(&self, key: String, source: Arc<dyn CorpusSource>) {
        let mut corpora = self.0.lock();
        corpora.sources.entry(key).or_insert(source);
    }
    pub fn holds(&self, key: &str) -> bool { self.0.lock().sources.contains_key(key) }
}

impl rtdb_expr::CorpusLookup for CorpusRegistry {
    fn corpus(&self, query: &str) -> Option<rtdb_text::Corpus> {
        let mut corpora = self.0.lock();
        if let Some(computed) = corpora.computed.get(query) { return computed.clone(); }
        let terms = rtdb_text::Query::parse(query);
        let mut total = rtdb_text::Corpus::default();
        let summed = corpora.sources.values().try_for_each(|source| { total.add(&source.corpus(&terms).ok()?); Some(()) });
        let result = summed.filter(|_| total.documents > 0).map(|_| total);
        corpora.computed.insert(query.to_string(), result.clone());
        result
    }
}

/// Narrows a scan before it opens.
/// It returns a superset of the IIDs whose visible row can satisfy the scan's predicates at its bounds, or `None` to leave the scan unnarrowed.
/// The scan still resolves each candidate's history and re-checks every predicate, so narrowing decides cost, never results.
#[async_trait]
pub trait ScanAccelerator: Send + Sync {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>>;
    /// The values of `match(column, query)` on the rows of whichever of the scan's data files the accelerator can answer for, so the scan reads no text there.
    /// Each answer must equal MATCH evaluated on the row.
    async fn match_answers(&self, _request: &ScanRequest<'_>, _column: &str, _query: &rtdb_text::Query) -> Result<Option<MatchAnswers>> { Ok(None) }
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
