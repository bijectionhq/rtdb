use crate::lexer::{IndexedLexeme, Kind};
use crate::parser::grammar::{ExpectedSymbol, TokenSet, TokenType};
use super::no_viable_report::{ListenerDiagnostic, listener_diagnostic, message_buffer, message_size, quoted_len, write_quoted};
use super::{
    InputMismatch, InvocationCause, InvocationException, InvocationKey, InvocationView, MismatchExpectation, NextTokensCapture,
    ParserControl, ParserProfile, ParserSession, ParserState,
    RuntimeError, RuntimeProblem, RuntimeResult,
};
use super::generated_actions::{SyncAction, SyncShape};
use super::super::PredictionError;

#[derive(Debug)]
pub(super) enum TokenReport<'sql> {
    Unwanted(IndexedLexeme<'sql>),
    Missing(IndexedLexeme<'sql>),
    Mismatch(InputMismatch<'sql>),
}

impl<'sql> TokenReport<'sql> {
    fn offending(&self) -> IndexedLexeme<'sql> {
        match self {
            Self::Unwanted(offending) | Self::Missing(offending) => *offending,
            Self::Mismatch(cause) => cause.offending,
        }
    }
}

#[derive(Debug)]
pub(in crate::parser::prediction) struct TokenDiagnostic<'sql> {
    report: TokenReport<'sql>,
    listener: ListenerDiagnostic,
}

impl<'sql> TokenDiagnostic<'sql> {
    #[cfg(test)]
    pub(in crate::parser::prediction) fn offending(&self) -> IndexedLexeme<'sql> { self.report.offending() }
    #[cfg(test)]
    pub(in crate::parser::prediction) fn cause(&self) -> Option<InputMismatch<'sql>> {
        match &self.report {
            TokenReport::Unwanted(_) | TokenReport::Missing(_) => None,
            TokenReport::Mismatch(cause) => Some(*cause),
        }
    }
    #[cfg(test)]
    pub(in crate::parser::prediction) fn anomaly(&self) -> &rtdb_types::Anomaly { &self.listener.anomaly }
    pub(super) fn into_listener(self) -> ListenerDiagnostic { self.listener }
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn sync_required_block(&mut self) -> RuntimeResult<'sql, ()> {
        self.sync_generated(|state| match state {
            ParserState::State(354) => Some((354, 0, SyncShape::RequiredBlock)),
            ParserState::State(574) => Some((574, 2, SyncShape::RequiredBlock)),
            ParserState::State(2886) => Some((2886, 164, SyncShape::RequiredBlock)),
            ParserState::State(3014) => Some((3014, 175, SyncShape::RequiredBlock)),
            _ => None,
        })
    }

    pub(super) fn sync_statement(&mut self) -> RuntimeResult<'sql, ()> {
        self.sync_generated(|state| match state {
            ParserState::State(363) => Some((363, 1, SyncShape::LoopEntry)),
            ParserState::State(365) => Some((365, 1, SyncShape::Loopback)),
            ParserState::State(367) => Some((367, 1, SyncShape::RequiredBlock)),
            ParserState::State(number @ (473 | 479 | 486)) => Some((number, 2, SyncShape::NullableBlock)),
            ParserState::State(475) => Some((475, 2, SyncShape::RequiredBlock)),
            ParserState::State(507) => Some((507, 2, SyncShape::RequiredBlock)),
            ParserState::State(580) => Some((580, 3, SyncShape::RequiredBlock)),
            ParserState::State(number @ (756 | 759)) => Some((number, 22, SyncShape::RequiredBlock)),
            ParserState::State(848) => Some((848, 23, SyncShape::RequiredBlock)),
            ParserState::State(1896) => Some((1896, 63, SyncShape::RequiredBlock)),
            _ => None,
        })
    }

    pub(super) fn sync_action(&mut self, action: SyncAction) -> RuntimeResult<'sql, ()> {
        self.generated_action_state(action.state, action.rule)?;
        self.sync_generated(|_| Some((action.state, action.rule, action.shape)))
    }

    pub(super) fn lookahead_action(&mut self, action: SyncAction) -> RuntimeResult<'sql, TokenType> {
        self.sync_action(action)?;
        Ok(self.lookahead_token(1)?.1)
    }

    fn sync_generated(
        &mut self, site: impl FnOnce(ParserState) -> Option<(u32, u16, SyncShape)>,
    ) -> RuntimeResult<'sql, ()> {
        let ready = self.ready();
        self.finish(ready)?;
        match self.profile {
            ParserProfile::SllBail => return Ok(()),
            ParserProfile::LlDefault => (),
            ParserProfile::Kernel => return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted))),
        }
        let site = (|| {
            let (number, rule, shape) = site(self.state).ok_or(RuntimeError::Control(RuntimeProblem::PredictionSite))?;
            let context = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            let record = self.record(context)?;
            if record.rule != rule || record.start.is_none() {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            let grammar = self.environment.storage.grammar_view();
            if !shape.agrees(grammar.state(number)?.kind()) {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            Ok((NextTokensCapture { context, state: number }, shape))
        })();
        let (site, shape) = self.finish(site)?;
        self.sync_tokens(site, Some(shape.nullable()), matches!(shape, SyncShape::Loopback))
    }

    pub(super) fn sync_replay_decision(&mut self, state: u32, rule: u16, loopback: bool) -> RuntimeResult<'sql, ()> {
        self.generated_action_state(state, rule)?;
        match self.profile {
            ParserProfile::SllBail => return Ok(()),
            ParserProfile::LlDefault => (),
            ParserProfile::Kernel => return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))),
        }
        let context = self.current.expect("validated replay invocation");
        self.sync_tokens(NextTokensCapture { context, state }, None, loopback)
    }

    fn sync_tokens(
        &mut self, site: NextTokensCapture<'parse>, nullable: Option<bool>, loopback: bool,
    ) -> RuntimeResult<'sql, ()> {
        let (_, first) = self.lookahead_token(1)?;
        let next = {
            let grammar = self.environment.storage.grammar_view();
            let scratch = self.work.scratch();
            let work = if nullable.is_some() { self.work } else { &scratch };
            grammar.state(site.state).and_then(|state| state.next_tokens(work)).map_err(RuntimeError::from)
        };
        let expected = self.finish(next)?;
        let accepts_epsilon = expected.contains(ExpectedSymbol::Epsilon);
        if nullable.is_some_and(|nullable| nullable != accepts_epsilon) {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        if expected.contains(ExpectedSymbol::Token(first)) {
            self.next_tokens_capture = None;
            return Ok(());
        }
        if accepts_epsilon {
            self.next_tokens_capture.get_or_insert(site);
            return Ok(());
        }
        if loopback {
            return self.report_unwanted_token(expected);
        }

        let (_, second) = self.lookahead_token(2)?;
        if expected.contains(ExpectedSymbol::Token(second)) {
            return self.report_unwanted_token(expected);
        }

        self.pend_input_mismatch(MismatchExpectation::Current(expected))
    }

    pub(super) fn report_unwanted_token<T>(&mut self, expected: TokenSet) -> RuntimeResult<'sql, T> {
        self.control = ParserControl::InErrorRecovery;
        let offending = self.input.lt(1)?.expect("positive lookahead");
        self.report_token_syntax(TokenReport::Unwanted(offending), MismatchExpectation::Current(expected))
    }

    pub(super) fn report_token_syntax<T>(&mut self, report: TokenReport<'sql>, expectation: MismatchExpectation<InvocationKey<'parse>>) -> RuntimeResult<'sql, T> {
        let prefix = match &report {
            TokenReport::Unwanted(_) => "extraneous input ",
            TokenReport::Missing(_) => "missing ",
            TokenReport::Mismatch(_) => "mismatched input ",
        };
        let missing = matches!(&report, TokenReport::Missing(_));
        let separator = if missing { " at " } else { " expecting " };
        let offending = report.offending();
        let token = offending.lexeme();
        let spelling = if token.kind() == Kind::Eof { "<EOF>" } else { token.raw() };
        let message = {
            let grammar = self.environment.storage.grammar_view();
            let quoted_before_expected = if missing { None } else { Some(quoted_len(spelling)?) };
            let expected = match expectation {
                MismatchExpectation::Current(expected) => expected,
                MismatchExpectation::Captured(owner) => {
                    let exception = self.record(owner)?.exception.as_ref();
                    let Some(InvocationException::CapturedMismatch { cause: InvocationCause::InputMismatch(cause), context }) = exception else {
                        return Err(RuntimeError::Control(RuntimeProblem::Invocation));
                    };
                    let ParserState::State(state) = cause.state else {
                        return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
                    };
                    let invocation = InvocationView { records: &self.records, current: Some(*context) };
                    grammar.state(state)?.expected_tokens(invocation.invocations(&grammar), self.work)?
                }
            };
            let expected_len = expected.display_len(&grammar)?;
            let (first, second) = match quoted_before_expected {
                None => (expected_len, quoted_len(spelling)?),
                Some(quoted) => (quoted, expected_len),
            };
            let capacity = message_size([prefix.len(), first, separator.len(), second])?;
            self.work.reserve(capacity).map_err(PredictionError::Admission)?;
            let mut message = message_buffer(capacity)?;
            message.push_str(prefix);
            if missing {
                expected.write_display(&grammar, &mut message).expect("String writing is infallible");
                message.push_str(separator);
                write_quoted(&mut message, spelling);
            } else {
                write_quoted(&mut message, spelling);
                message.push_str(separator);
                expected.write_display(&grammar, &mut message).expect("String writing is infallible");
            }
            message
        };

        self.syntax_errors = self.syntax_errors.wrapping_add(1);
        let position = offending.lexeme().span().start;
        let listener = listener_diagnostic(position, &message, self.work)?;
        Err(RuntimeError::ReportedTokenSyntax(TokenDiagnostic { report, listener }))
    }
}
