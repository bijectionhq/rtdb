mod context;
mod config;
mod semantic;
mod mode;
mod storage;
mod dfa;
mod closure;
mod runtime;
mod adaptive;
pub(super) use runtime::is_identifier_token;
pub use runtime::{ContextualStatementDiagnostic, StatementSyntaxDiagnostic};

use rtdb_types::{AllocationScope, Anomaly};
use super::grammar::{ArtifactIssue, GrammarError};
#[cfg(test)]
use super::grammar::Grammar;
pub(super) use closure::SourceAssertions;
pub(super) use runtime::{CapturedClassPolicy, ParserEnvironment};
use context::{ContextKey, MergeMode, MergeRound, PredictionContexts};
#[cfg(test)]
use context::ContextShape;
use config::{Alternative, AlternativeSet, ConfigChange, ConfigKey, ConfigSetKey, Configurations, SetMode};
use semantic::Semantic;
use mode::{AlternativeChoice, ModeWorkspace, PredictionMode};
#[cfg(test)]
use mode::SubsetsView;
use storage::{PredictionStorage, with_prediction_storage};
#[cfg(test)]
use storage::PredictionAccess;

type Result<T> = std::result::Result<T, PredictionError>;

#[derive(Debug)]
pub(crate) enum PredictionError {
    Admission(Anomaly),
    Artifact(Problem),
    SourceAssertion(SourceAssertionSite),
    ForeignOwner,
    Aborted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Problem { Capacity, Context, LocalClosure, Invocation(u32), CanonicalDomain, Alternative, Precedence, Readonly, EmptySubsets, DfaDecision, DfaPhase, ClosurePhase, Grammar(ArtifactIssue) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceAssertionSite { ReachStopEmpty, PopDepth, FalloffDepth, FullNoOuter }

impl From<GrammarError> for PredictionError {
    fn from(error: GrammarError) -> Self {
        match error {
            GrammarError::Admission(error) => Self::Admission(error),
            GrammarError::Artifact(error) => Self::Artifact(Problem::Grammar(error)),
            GrammarError::ForeignOwner => Self::ForeignOwner,
        }
    }
}

#[cfg(test)]
fn with_prediction_contexts<'g, 'scope, R>(
    grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope,
    use_contexts: impl for<'id> FnOnce(&mut PredictionContexts<'id, 'g, 'scope>) -> Result<R>,
) -> Result<R> {
    use_contexts(&mut PredictionContexts::new(grammar, allocation))
}

#[cfg(test)]
fn with_configurations<'id, 'g, 'scope, R>(
    contexts: &mut PredictionContexts<'id, 'g, 'scope>,
    use_configs: impl for<'cfg> FnOnce(&mut Configurations<'cfg, 'id, 'g, 'scope>, &mut PredictionContexts<'id, 'g, 'scope>) -> Result<R>,
) -> Result<R> {
    contexts.ready()?;
    let mut configs = Configurations::new(contexts.grammar(), contexts.allocation());
    let result = use_configs(&mut configs, contexts);
    contexts.finish(result)
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

mod factory_error_sealed { pub trait Sealed {} }
pub(super) trait FactoryError: factory_error_sealed::Sealed + From<PredictionError> {}
impl factory_error_sealed::Sealed for PredictionError {}
impl FactoryError for PredictionError {}
