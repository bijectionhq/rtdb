use std::ops::Range;
use rtdb_plan::{ControlCommitMode, ControlStatement, DiscardTarget};
use rtdb_types::{AllocationScope, Anomaly};
use crate::lexer::{IndexedLexeme, Kind, ScanError, Span, StreamError};
use super::{
    ContextChild, ContextClass, InputMismatch, InvocationKey, NoViableAlternative, ParserEnvironment, ParserSession,
    ParserState, PredictionError, RuntimeError, RuntimeProblem, RuntimeResult, with_bail_fallback,
};
use super::no_viable_report::ListenerDiagnostic;

/// An initial statement syntax report holding its listener allocation until drop.
#[derive(Debug)]
pub struct InitialStatementDiagnostic(ListenerDiagnostic);

impl InitialStatementDiagnostic {
    /// Borrows the incorrect `xtdb/sql-error` anomaly with the listener’s text and empty context.
    pub fn anomaly(&self) -> &Anomaly { &self.0.anomaly }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::parser) struct TokenOccurrence {
    pub(in crate::parser) index: usize,
    pub(in crate::parser) span: Span,
}

impl From<IndexedLexeme<'_>> for TokenOccurrence {
    fn from(token: IndexedLexeme<'_>) -> Self {
        Self { index: token.index(), span: token.lexeme().span() }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::parser) enum DirectControlRecognition {
    NoViableAlternative { start: TokenOccurrence, offending: TokenOccurrence, state: Option<u32> },
    InputMismatch { offending: TokenOccurrence, state: Option<u32> },
    CauseFreeToken { offending: TokenOccurrence },
}

impl DirectControlRecognition {
    pub(in crate::parser) fn offending_state(self) -> Option<u32> {
        match self {
            Self::NoViableAlternative { state, .. } | Self::InputMismatch { state, .. } => state,
            Self::CauseFreeToken { .. } => None,
        }
    }

    fn no_viable_alternative(cause: NoViableAlternative<'_>) -> Self {
        Self::NoViableAlternative {
            start: cause.start.into(), offending: cause.offending.into(), state: state_number(cause.state),
        }
    }

    fn token(offending: IndexedLexeme<'_>, cause: Option<InputMismatch<'_>>) -> Self {
        let offending = offending.into();
        match cause {
            Some(cause) => Self::InputMismatch { offending, state: state_number(cause.state) },
            None => Self::CauseFreeToken { offending },
        }
    }
}

fn state_number(state: ParserState) -> Option<u32> {
    match state {
        ParserState::State(number) => Some(number),
        ParserState::RootSentinel => None,
    }
}

#[derive(Debug)]
enum Failure {
    Prediction(PredictionError),
    InputAdmission(Anomaly),
    Lexical(Span),
    AbortedStream,
    ConsumeEof,
    Control(RuntimeProblem),
    Recognition(DirectControlRecognition),
    Reported { recognition: DirectControlRecognition, listener: ListenerDiagnostic },
    UnsupportedStatement,
}

#[derive(Debug)]
pub(in crate::parser) struct DirectControlError(Failure);

impl DirectControlError {
    pub(in crate::parser) fn anomaly(&self) -> Option<&Anomaly> {
        match &self.0 {
            Failure::Prediction(PredictionError::Admission(error)) | Failure::InputAdmission(error) => Some(error),
            Failure::Reported { listener, .. } => Some(&listener.anomaly),
            _ => None,
        }
    }

    pub(in crate::parser) fn recognition(&self) -> Option<DirectControlRecognition> {
        match &self.0 {
            Failure::Recognition(recognition) | Failure::Reported { recognition, .. } => Some(*recognition),
            _ => None,
        }
    }

    pub(in crate::parser) fn lexical_span(&self) -> Option<Span> {
        match &self.0 {
            Failure::Lexical(span) => Some(*span),
            _ => None,
        }
    }

    fn from_runtime(error: RuntimeError<'_>) -> Self {
        Self(match error {
            RuntimeError::Prediction(error) => Failure::Prediction(error),
            RuntimeError::Input(StreamError::Scan(ScanError::Admission(error))) => Failure::InputAdmission(error),
            RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error))) => Failure::Lexical(error.span()),
            RuntimeError::Input(StreamError::Aborted) => Failure::AbortedStream,
            RuntimeError::Input(StreamError::ConsumeEof) => Failure::ConsumeEof,
            RuntimeError::Control(error) => Failure::Control(error),
            RuntimeError::NoViableAlternative(cause) => Failure::Recognition(DirectControlRecognition::no_viable_alternative(cause)),
            RuntimeError::InputMismatch(cause) => Failure::Recognition(
                DirectControlRecognition::token(cause.offending, Some(cause)),
            ),
            RuntimeError::ReportedNoViableAlternative(diagnostic) => Failure::Reported {
                recognition: DirectControlRecognition::no_viable_alternative(diagnostic.cause()),
                listener: diagnostic.into_listener(),
            },
            RuntimeError::ReportedTokenSyntax(diagnostic) => Failure::Reported {
                recognition: DirectControlRecognition::token(diagnostic.offending(), diagnostic.cause()),
                listener: diagnostic.into_listener(),
            },
        })
    }
}

impl ParserEnvironment {
    pub(in crate::parser) fn initial_statement_diagnostic(
        &mut self, source: &str, span: &Range<usize>, work: &AllocationScope,
    ) -> rtdb_types::Result<Option<InitialStatementDiagnostic>> {
        let result = with_bail_fallback(self, source, work, |session, _| {
            session.direct_sql_statement()?;
            Ok(())
        });
        match result {
            Err(RuntimeError::ReportedTokenSyntax(report)) if report.is_initial_statement(span) => {
                Ok(Some(InitialStatementDiagnostic(report.into_listener())))
            }
            Err(RuntimeError::Prediction(PredictionError::Admission(error))
                | RuntimeError::Input(StreamError::Scan(ScanError::Admission(error)))) => Err(error),
            _ => Ok(None),
        }
    }

    pub(in crate::parser) fn parse_direct_control(
        &mut self, source: &str, work: &AllocationScope,
    ) -> Result<ControlStatement, DirectControlError> {
        let control = with_bail_fallback(self, source, work, |session, _| {
            let root = session.direct_sql_statement()?;
            Ok(project_control(session, root)?)
        }).map_err(DirectControlError::from_runtime)?;
        control.ok_or(DirectControlError(Failure::UnsupportedStatement))
    }
}

fn project_control<'parse, 'sql>(
    session: &ParserSession<'parse, 'sql, '_, '_>, root: InvocationKey<'parse>,
) -> RuntimeResult<'sql, Option<ControlStatement>> {
    let invalid_tree = || RuntimeError::Control(RuntimeProblem::Invocation);
    let view = session.invocation_view();
    let root = view.context(root).ok_or_else(invalid_tree)?;
    let Some(ContextChild::Context(statement)) = root.children().first() else { return Err(invalid_tree()); };
    let statement = view.context(*statement).ok_or_else(invalid_tree)?;
    let control = match statement.class() {
        Some(ContextClass::CommitStatement) => {
            let mode = match statement.children().get(1) {
                None => None,
                Some(ContextChild::Terminal(token)) => match token.lexeme().kind() {
                    Kind::Keyword(keyword) if keyword.name() == "SYNC" => Some(ControlCommitMode::Sync),
                    Kind::Keyword(keyword) if keyword.name() == "ASYNC" => Some(ControlCommitMode::Async),
                    _ => return Err(invalid_tree()),
                },
                _ => return Err(invalid_tree()),
            };
            ControlStatement::Commit(mode)
        }
        Some(ContextClass::RollbackStatement) => ControlStatement::Rollback,
        Some(ContextClass::DiscardStatement) => {
            let Some(ContextChild::Context(target)) = statement.children().get(1) else { return Err(invalid_tree()); };
            let target = match view.context(*target).and_then(|context| context.class()) {
                Some(ContextClass::DiscardAll) => DiscardTarget::All,
                Some(ContextClass::DiscardPlans) => DiscardTarget::Plans,
                Some(ContextClass::DiscardTemp) => DiscardTarget::Temporary,
                Some(ContextClass::DiscardSequences) => DiscardTarget::Sequences,
                _ => return Err(invalid_tree()),
            };
            ControlStatement::Discard(target)
        }
        Some(ContextClass::SetRoleStatement) => ControlStatement::SetRole,
        Some(ContextClass::QueryExpr) => return Ok(None),
        _ => return Err(invalid_tree()),
    };
    Ok(Some(control))
}

#[cfg(test)]
mod tests {
    use super::{ControlCommitMode, ControlStatement, DiscardTarget, Failure};
    use crate::parser::prediction::{CapturedClassPolicy, DirectControlError, DirectControlRecognition, ParserEnvironment, SourceAssertions};
    use rtdb_types::{AllocationScope, MemoryBudget};

    fn environment(memory: &std::sync::Arc<MemoryBudget>) -> ParserEnvironment {
        let policy = CapturedClassPolicy::capture_parser_atn(
            SourceAssertions::Enabled, |_| Ok::<_, std::convert::Infallible>(None),
        ).unwrap();
        ParserEnvironment::open_in(policy, memory).unwrap()
    }

    #[test]
    fn projected_controls_outlive_their_input_and_parser() {
        let retained = MemoryBudget::new(64 << 20);
        let request = MemoryBudget::new(64 << 20);
        let mut parser = environment(&retained);
        let cases = [
            ("COMMIT", ControlStatement::Commit(None)),
            ("commit sYnC;", ControlStatement::Commit(Some(ControlCommitMode::Sync))),
            ("COMMIT ASYNC", ControlStatement::Commit(Some(ControlCommitMode::Async))),
            ("ROLLBACK;", ControlStatement::Rollback),
            ("DISCARD ALL", ControlStatement::Discard(DiscardTarget::All)),
            ("DISCARD PLANS", ControlStatement::Discard(DiscardTarget::Plans)),
            ("DISCARD TEMPORARY", ControlStatement::Discard(DiscardTarget::Temporary)),
            ("DISCARD SEQUENCES", ControlStatement::Discard(DiscardTarget::Sequences)),
            ("SET ROLE NONE", ControlStatement::SetRole),
            (r#"SET ROLE "reviewer""#, ControlStatement::SetRole),
        ];
        let controls: Vec<_> = cases.iter().map(|(sql, _)| {
            let input = (*sql).to_owned();
            let work = AllocationScope::new(&request).unwrap();
            parser.parse_direct_control(&input, &work).unwrap()
        }).collect();
        drop(parser);
        assert_eq!(retained.used_bytes(), 0);
        assert_eq!(request.used_bytes(), 0);
        for (actual, (_, expected)) in controls.into_iter().zip(cases) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn projected_errors_keep_copied_cause_facts_and_the_listener_charge() {
        let retained = MemoryBudget::new(64 << 20);
        let request = MemoryBudget::new(64 << 20);
        let mut parser = environment(&retained);
        let parse = |parser: &mut ParserEnvironment, sql: &str| -> Result<ControlStatement, DirectControlError> {
            let input = sql.to_owned();
            let work = AllocationScope::new(&request).unwrap();
            parser.parse_direct_control(&input, &work)
        };

        assert_eq!(parse(&mut parser, "ROLLBACK").unwrap(), ControlStatement::Rollback);
        let warm_states = |parser: &mut ParserEnvironment| {
            let work = AllocationScope::new(&request).unwrap();
            crate::parser::prediction::with_prediction_storage(&mut parser.storage, &work, |access| {
                let (dfas, _, _) = access.dfa_parts();
                let start = dfas.start(34, 0)?.expect("warmed direct statement");
                Ok((dfas.view(34, start)?.number(), dfas.state_count(34)?))
            }).unwrap()
        };
        let start = warm_states(&mut parser);
        assert_eq!(start.0, 0);
        assert!(start.1 >= 2);

        let mismatch = parse(&mut parser, "SELEC 1").unwrap_err();
        let unwanted = parse(&mut parser, "extra SELECT 1").unwrap_err();
        for (error, cause, token_end, message) in [
            (&mismatch, Some(574), 5, "mismatched input 'SELEC' expecting "),
            (&unwanted, None, 5, "extraneous input 'extra' expecting "),
        ] {
            let recognition = error.recognition().unwrap();
            let offending = match recognition {
                DirectControlRecognition::InputMismatch { offending, .. } if cause.is_some() => offending,
                DirectControlRecognition::CauseFreeToken { offending } if cause.is_none() => offending,
                _ => panic!("expected an owned token diagnostic"),
            };
            assert_eq!(recognition.offending_state(), cause);
            assert_eq!(offending.index, 0);
            assert_eq!(offending.span.start.byte, 0);
            assert_eq!(offending.span.end.byte, token_end);
            let anomaly = error.anomaly().unwrap();
            assert_eq!(anomaly.code, "xtdb/sql-error");
            assert!(anomaly.message.as_deref().unwrap().contains(message));
        }
        let (state_after_errors, states_after_errors) = warm_states(&mut parser);
        assert_eq!(state_after_errors, start.0);
        assert!(states_after_errors >= start.1);
        let retained_diagnostics = request.used_bytes();
        assert!(retained_diagnostics > 0);
        assert_eq!(parse(&mut parser, "ROLLBACK").unwrap(), ControlStatement::Rollback);
        assert_eq!(warm_states(&mut parser), (state_after_errors, states_after_errors));
        assert_eq!(request.used_bytes(), retained_diagnostics);

        let lexical = parse(&mut parser, "ROLLBACK @").unwrap_err();
        assert_eq!(lexical.lexical_span().unwrap().start.byte, 9);
        assert!(matches!(parse(&mut parser, "SELECT 1").unwrap_err().0, Failure::UnsupportedStatement));
        drop(parser);
        assert_eq!(retained.used_bytes(), 0);
        assert_eq!(request.used_bytes(), retained_diagnostics);
        drop((mismatch, unwanted));
        assert_eq!(request.used_bytes(), 0);
    }
}
