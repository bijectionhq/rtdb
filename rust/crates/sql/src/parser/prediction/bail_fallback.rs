use rtdb_types::AllocationScope;
use super::{
    InvocationException, InvocationKey, ParserControl, ParserEnvironment, ParserProfile, ParserSession,
    PrecedenceFrame, RuntimeError, RuntimeProblem, RuntimeResult, with_parser_session,
};
use super::super::PredictionMode;

#[cfg(test)]
#[path = "bail_fallback_tests.rs"]
mod tests;

#[derive(Debug)]
pub(in crate::parser::prediction) struct BailCancellation<'parse> {
    exception: InvocationKey<'parse>,
}

#[derive(Debug)]
pub(in crate::parser::prediction) enum PassError<'parse, 'sql> {
    Runtime(RuntimeError<'sql>),
    Cancelled(BailCancellation<'parse>),
}

pub(in crate::parser::prediction) type PassResult<'parse, 'sql, T> = Result<T, PassError<'parse, 'sql>>;

impl<'parse, 'sql> From<RuntimeError<'sql>> for PassError<'parse, 'sql> {
    fn from(error: RuntimeError<'sql>) -> Self { Self::Runtime(error) }
}

pub(in crate::parser::prediction) fn with_bail_fallback<'sql, 'env, 'work, T>(
    environment: &'env mut ParserEnvironment,
    sql: &'sql str,
    allocation: &'work AllocationScope,
    mut parse_action: impl for<'parse> FnMut(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, PredictionMode,
    ) -> PassResult<'parse, 'sql, T>,
) -> RuntimeResult<'sql, T> {
    with_parser_session(environment, sql, allocation, |session| {
        session.profile = ParserProfile::SllBail;
        match parse_action(session, PredictionMode::Sll) {
            Ok(value) => Ok(value),
            Err(PassError::Runtime(error)) => Err(error),
            Err(PassError::Cancelled(cancellation)) => {
                session.reset_after_bail(cancellation)?;
                match parse_action(session, PredictionMode::Ll) {
                    Ok(value) => Ok(value),
                    Err(PassError::Runtime(error)) => Err(error),
                    Err(PassError::Cancelled(_)) => Err(RuntimeError::Control(RuntimeProblem::Aborted)),
                }
            }
        }
    })
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn catch_no_viable_alternative<T>(
        &mut self,
        local: InvocationKey<'parse>,
    ) -> PassResult<'parse, 'sql, T> {
        let report = self.pending_no_viable_report()
            .filter(|report| self.profile != ParserProfile::Kernel && report.invocation == local);
        let Some(report) = report else {
            self.control.abort();
            return Err(RuntimeError::Control(RuntimeProblem::Aborted).into());
        };
        let cause = report.cause;
        self.records[local.index].exception = Some(InvocationException::Owned(cause));
        self.report_no_viable_alternative()?;

        let mut ancestor = self.records[local.index].parent;
        while let Some(key) = ancestor {
            let record = &mut self.records[key.index];
            record.exception = Some(InvocationException::Shared(local));
            ancestor = record.parent;
        }
        self.control = ParserControl::BailCancelled(local);
        Err(PassError::Cancelled(BailCancellation { exception: local }))
    }

    fn reset_after_bail(&mut self, cancellation: BailCancellation<'parse>) -> RuntimeResult<'sql, ()> {
        if !matches!(&self.control, ParserControl::BailCancelled(exception) if *exception == cancellation.exception)
            || self.profile != ParserProfile::SllBail
            || self.current.is_some()
            || !matches!(self.precedence.as_slice(), [PrecedenceFrame::Base0])
        {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted)));
        }

        self.control = ParserControl::InErrorRecovery;
        self.input.seek(0)?;
        self.input.seek(0)?;

        self.control = ParserControl::Failed;
        self.current = None;
        self.syntax_errors = 0;
        self.matched_eof = false;
        self.precedence.clear();
        self.precedence.push(PrecedenceFrame::Base0);
        self.profile = ParserProfile::LlDefault;
        self.control = ParserControl::Ready;
        Ok(())
    }
}
