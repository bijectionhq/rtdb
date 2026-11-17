mod context;

use rtdb_types::{AllocationScope, Anomaly};
use super::grammar::{ArtifactIssue, Grammar, GrammarError};
pub(crate) use context::{ContextKey, ContextShape, ContextView, MergeMode, MergeRound, PredictionContexts};

type Result<T> = std::result::Result<T, PredictionError>;

#[derive(Debug)]
pub(crate) enum PredictionError {
    Admission(Anomaly),
    Artifact(Problem),
    ForeignOwner,
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Problem { Capacity, Context, Invocation(u32), CanonicalDomain, Grammar(ArtifactIssue) }

impl From<GrammarError> for PredictionError {
    fn from(error: GrammarError) -> Self {
        match error {
            GrammarError::Admission(error) => Self::Admission(error),
            GrammarError::Artifact(error) => Self::Artifact(Problem::Grammar(error)),
            GrammarError::ForeignOwner => Self::ForeignOwner,
        }
    }
}

pub(crate) fn with_prediction_contexts<'g, 'scope, R>(
    grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope,
    use_contexts: impl for<'id> FnOnce(&mut PredictionContexts<'id, 'g, 'scope>) -> Result<R>,
) -> Result<R> {
    use_contexts(&mut PredictionContexts::new(grammar, allocation))
}

fn capacity_error() -> PredictionError { PredictionError::Artifact(Problem::Capacity) }

fn vector<T>(allocation: &AllocationScope, count: usize) -> Result<Vec<T>> {
    let bytes = count.checked_mul(std::mem::size_of::<T>()).filter(|bytes| *bytes <= isize::MAX as usize).ok_or_else(capacity_error)?;
    allocation.reserve(bytes).map_err(PredictionError::Admission)?;
    Ok(Vec::with_capacity(count))
}

fn reserve<T>(values: &mut Vec<T>, additional: usize, allocation: &AllocationScope) -> Result<()> {
    let required = values.len().checked_add(additional).ok_or_else(capacity_error)?;
    if required <= values.capacity() { return Ok(()); }
    let capacity = required.max(values.capacity().checked_mul(2).ok_or_else(capacity_error)?).max(16);
    let mut replacement = vector(allocation, capacity)?;
    replacement.append(values);
    *values = replacement;
    Ok(())
}

fn push<T>(values: &mut Vec<T>, value: T, allocation: &AllocationScope) -> Result<()> {
    reserve(values, 1, allocation)?;
    values.push(value);
    Ok(())
}
