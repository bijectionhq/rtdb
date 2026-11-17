use rtdb_types::AllocationScope;
use crate::lexer::{IndexedLexeme, Kind};
use crate::parser::grammar::TokenType;
use super::{
    BailRecovery, InvocationCause, InvocationException, InvocationKey, ParserControl, ParserEnvironment, ParserProfile, ParserSession,
    PrecedenceFrame, RuntimeError, RuntimeProblem, RuntimeResult, with_parser_session,
};
use super::required_block_sync::TokenReport;
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
    pub(in crate::parser::prediction) fn match_bail(
        &mut self,
        expected: TokenType,
    ) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        let entered = self.ready().and_then(|()| {
            if self.profile != ParserProfile::SllBail {
                return Err(RuntimeError::Control(RuntimeProblem::Aborted));
            }
            let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            if self.record(current)?.start.is_none() {
                return Err(RuntimeError::Control(RuntimeProblem::Invocation));
            }
            Ok(())
        });
        self.finish(entered)?;
        let matched = self.lookahead(1)?.expect("positive lookahead");
        let actual = self.environment.storage.grammar_view().token_type(matched.lexeme().kind())
            .map_err(RuntimeError::from);
        if self.finish(actual)? != expected {
            let (current, cause) = self.capture_input_mismatch()?;
            self.records[current.index].exception = Some(InvocationException::Owned(InvocationCause::InputMismatch(cause)));
            return self.cancel_bail(current, BailRecovery::Clear);
        }

        if expected == TokenType::EOF {
            self.matched_eof = true;
        }
        let consumed = self.lookahead(1)?.expect("positive lookahead");
        if consumed.lexeme().kind() != Kind::Eof {
            self.advance_input()?;
        }
        Ok(matched)
    }

    pub(in crate::parser::prediction) fn catch_recognition<T>(
        &mut self,
        local: InvocationKey<'parse>,
    ) -> PassResult<'parse, 'sql, T> {
        let report = self.pending_no_viable_report()
            .filter(|report| self.profile != ParserProfile::Kernel && report.invocation == local);
        if let Some(report) = report {
            let cause = InvocationCause::NoViableAlternative(report.cause);
            self.records[local.index].exception = Some(InvocationException::Owned(cause));
            self.report_no_viable_alternative()?;
            return self.cancel_bail(local, BailRecovery::Entered);
        }

        if let ParserControl::PendingInputMismatch(report) = &self.control
            && self.profile == ParserProfile::LlDefault
            && local == report.invocation
            && self.current == Some(local)
            && self.state == report.cause.state
            && self.input.index() == Some(report.cause.offending.index())
        {
            let (cause, expected) = (report.cause, report.expected);
            self.records[local.index].exception = Some(InvocationException::Owned(InvocationCause::InputMismatch(cause)));
            self.control = ParserControl::InErrorRecovery;
            return self.report_token_syntax(TokenReport::Mismatch(cause), expected).map_err(PassError::from);
        }

        self.control.abort();
        Err(RuntimeError::Control(RuntimeProblem::Aborted).into())
    }

    fn cancel_bail<T>(&mut self, local: InvocationKey<'parse>, recovery: BailRecovery) -> PassResult<'parse, 'sql, T> {
        let mut ancestor = self.records[local.index].parent;
        while let Some(key) = ancestor {
            let record = &mut self.records[key.index];
            record.exception = Some(InvocationException::Shared(local));
            ancestor = record.parent;
        }
        self.control = ParserControl::BailCancelled { exception: local, recovery };
        Err(PassError::Cancelled(BailCancellation { exception: local }))
    }

    fn reset_after_bail(&mut self, cancellation: BailCancellation<'parse>) -> RuntimeResult<'sql, ()> {
        if !matches!(&self.control, ParserControl::BailCancelled { exception, .. } if *exception == cancellation.exception)
            || self.profile != ParserProfile::SllBail
            || self.current.is_some()
            || !matches!(self.precedence.as_slice(), [PrecedenceFrame::Base0])
        {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted)));
        }

        self.control.abort();
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
