use rtdb_types::Anomaly;
use crate::lexer::{IndexedLexeme, Kind};
use crate::parser::grammar::{ExpectedSymbol, StateKindView, TokenSet, TokenType};
use super::no_viable_report::{ListenerDiagnostic, listener_diagnostic, message_buffer, message_size, quoted_len, write_quoted};
use super::{
    InputMismatch, ParserControl, ParserProfile, ParserSession, ParserState, PendingInputMismatch,
    RuntimeError, RuntimeProblem, RuntimeResult,
};
use super::super::PredictionError;

#[derive(Debug)]
pub(super) enum TokenReport<'sql> {
    Unwanted(IndexedLexeme<'sql>),
    Mismatch(InputMismatch<'sql>),
}

impl<'sql> TokenReport<'sql> {
    fn offending(&self) -> IndexedLexeme<'sql> {
        match self {
            Self::Unwanted(offending) => *offending,
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
    pub(in crate::parser::prediction) fn offending(&self) -> IndexedLexeme<'sql> { self.report.offending() }
    pub(in crate::parser::prediction) fn cause(&self) -> Option<InputMismatch<'sql>> {
        match &self.report {
            TokenReport::Unwanted(_) => None,
            TokenReport::Mismatch(cause) => Some(*cause),
        }
    }
    pub(in crate::parser::prediction) fn anomaly(&self) -> &Anomaly { &self.listener.anomaly }
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn sync_required_block(&mut self) -> RuntimeResult<'sql, ()> {
        let ready = self.ready();
        self.finish(ready)?;
        match self.profile {
            ParserProfile::SllBail => return Ok(()),
            ParserProfile::LlDefault => (),
            ParserProfile::Kernel => return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted))),
        }
        let site = (|| {
            let (number, rule) = match self.state {
                ParserState::State(2886) => (2886, 164),
                ParserState::State(3014) => (3014, 175),
                _ => return Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
            };
            let invocation = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            let record = self.record(invocation)?;
            if record.rule != rule || record.start.is_none() {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            let grammar = self.environment.storage.grammar_view();
            if !matches!(grammar.state(number)?.kind(), StateKindView::BlockStart { .. }) {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            Ok(number)
        })();
        let number = self.finish(site)?;
        let first = self.sync_lookahead_type(1)?;
        let next = (|| {
            let grammar = self.environment.storage.grammar_view();
            let tokens = grammar.state(number)?.next_tokens(self.work)?;
            if tokens.contains(ExpectedSymbol::Epsilon) {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            Ok(tokens)
        })();
        let expected = self.finish(next)?;
        if expected.contains(ExpectedSymbol::Token(first)) {
            return Ok(());
        }

        let second = self.sync_lookahead_type(2)?;
        if expected.contains(ExpectedSymbol::Token(second)) {
            self.control = ParserControl::InErrorRecovery;
            let offending = self.input.lt(1)?.expect("positive lookahead");
            return self.report_token_syntax(TokenReport::Unwanted(offending), expected);
        }

        let (invocation, cause) = self.capture_input_mismatch()?;
        self.control = ParserControl::PendingInputMismatch(PendingInputMismatch { invocation, cause, expected });
        Err(RuntimeError::InputMismatch(cause))
    }

    fn sync_lookahead_type(&mut self, offset: isize) -> RuntimeResult<'sql, TokenType> {
        let token = self.lookahead(offset)?.expect("positive lookahead");
        let kind = self.environment.storage.grammar_view().token_type(token.lexeme().kind())
            .map_err(RuntimeError::from);
        self.finish(kind)
    }

    pub(super) fn report_token_syntax<T>(&mut self, report: TokenReport<'sql>, expected: TokenSet) -> RuntimeResult<'sql, T> {
        const EXPECTING: &str = " expecting ";

        let prefix = match &report {
            TokenReport::Unwanted(_) => "extraneous input ",
            TokenReport::Mismatch(_) => "mismatched input ",
        };
        let offending = report.offending();
        let token = offending.lexeme();
        let spelling = if token.kind() == Kind::Eof { "<EOF>" } else { token.raw() };
        let quoted = quoted_len(spelling)?;
        let message = {
            let grammar = self.environment.storage.grammar_view();
            let capacity = message_size([prefix.len(), quoted, EXPECTING.len(), expected.display_len(&grammar)?])?;
            self.work.reserve(capacity).map_err(PredictionError::Admission)?;
            let mut message = message_buffer(capacity)?;
            message.push_str(prefix);
            write_quoted(&mut message, spelling);
            message.push_str(EXPECTING);
            expected.write_display(&grammar, &mut message).expect("String writing is infallible");
            message
        };

        self.syntax_errors = self.syntax_errors.wrapping_add(1);
        let position = offending.lexeme().span().start;
        let listener = listener_diagnostic(position, &message, self.work)?;
        Err(RuntimeError::ReportedTokenSyntax(TokenDiagnostic { report, listener }))
    }
}
