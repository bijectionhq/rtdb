use rtdb_types::Anomaly;
use crate::lexer::{IndexedLexeme, Kind};
use crate::parser::grammar::{ExpectedSymbol, StateKindView, TokenSet};
use super::no_viable_report::{ListenerDiagnostic, listener_diagnostic, message_buffer, message_size, quoted_len, write_quoted};
use super::{
    InputMismatch, ParserControl, ParserProfile, ParserSession, ParserState,
    RuntimeError, RuntimeProblem, RuntimeResult,
};
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
    pub(in crate::parser::prediction) fn offending(&self) -> IndexedLexeme<'sql> { self.report.offending() }
    pub(in crate::parser::prediction) fn cause(&self) -> Option<InputMismatch<'sql>> {
        match &self.report {
            TokenReport::Unwanted(_) | TokenReport::Missing(_) => None,
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
        let (_, first) = self.lookahead_token(1)?;
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

        let (_, second) = self.lookahead_token(2)?;
        if expected.contains(ExpectedSymbol::Token(second)) {
            return self.report_unwanted_token(expected);
        }

        self.pend_input_mismatch(expected)
    }

    pub(super) fn report_unwanted_token<T>(&mut self, expected: TokenSet) -> RuntimeResult<'sql, T> {
        self.control = ParserControl::InErrorRecovery;
        let offending = self.input.lt(1)?.expect("positive lookahead");
        self.report_token_syntax(TokenReport::Unwanted(offending), expected)
    }

    pub(super) fn report_token_syntax<T>(&mut self, report: TokenReport<'sql>, expected: TokenSet) -> RuntimeResult<'sql, T> {
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
            let (first, second) = if missing {
                (expected.display_len(&grammar)?, quoted_len(spelling)?)
            } else {
                (quoted_len(spelling)?, expected.display_len(&grammar)?)
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
