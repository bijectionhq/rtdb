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
}

/// Narrows a scan before it opens.
/// It returns a superset of the IIDs whose visible row can satisfy the scan's predicates at its bounds, or `None` to leave the scan unnarrowed.
/// The scan still resolves each candidate's history and re-checks every predicate, so an accelerator decides cost, never results.
#[async_trait]
pub trait ScanAccelerator: Send + Sync {
    async fn candidate_iids(&self, request: &ScanRequest<'_>) -> Result<Option<BTreeSet<Iid>>>;
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
