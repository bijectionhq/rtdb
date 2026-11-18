use crate::lexer::IndexedLexeme;
use crate::parser::grammar::{ExpectedSymbol, StateKindView, TransitionView};
use super::{MismatchExpectation, ParserControl, ParserProfile, ParserSession, ParserState, PassError, PassResult, RuntimeError, RuntimeProblem, RuntimeResult};
use super::required_block_sync::TokenReport;

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn match_isolation_atom(&mut self) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.match_generated_atom(|state| match state {
            ParserState::State(number @ (2882 | 2883)) => Some((number, 164)),
            ParserState::State(number @ 3007..=3013) => Some((number, 175)),
            _ => None,
        })
    }

    pub(super) fn match_window_atom(&mut self) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.match_generated_atom(|state| match state {
            ParserState::State(number @ (2218 | 2220)) => Some((number, 96)),
            _ => None,
        })
    }

    pub(super) fn match_statement_atom(&mut self) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.match_generated_atom(|state| match state {
            ParserState::State(number @ (353 | 356)) => Some((number, 0)),
            ParserState::State(488) => Some((488, 2)),
            _ => None,
        })
    }

    fn match_generated_atom(
        &mut self, site: impl FnOnce(ParserState) -> Option<(u32, u16)>,
    ) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        let site = (|| {
            self.ready()?;
            if self.profile == ParserProfile::Kernel {
                return Err(RuntimeError::Control(RuntimeProblem::Aborted));
            }
            let (number, rule) = site(self.state).ok_or(RuntimeError::Control(RuntimeProblem::PredictionSite))?;
            let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            let record = self.record(current)?;
            if record.rule != rule || record.start.is_none() {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            let grammar = self.environment.storage.grammar_view();
            let state = grammar.state(number)?;
            let mut edges = state.transitions();
            match (state.kind(), edges.next(), edges.len()) {
                (StateKindView::Basic, Some(TransitionView::Atom { token, .. }), 0) => Ok((number, token)),
                _ => Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
            }
        })();
        let (number, expected) = self.finish(site)?;
        if self.profile == ParserProfile::SllBail {
            return self.match_bail(expected);
        }
        let (matched, actual) = self.lookahead_token(1)?;
        if actual != expected {
            return self.recover_inline_atom(number).map_err(PassError::from);
        }
        self.consume_match(expected)?;
        Ok(matched)
    }

    fn recover_inline_atom(&mut self, number: u32) -> RuntimeResult<'sql, IndexedLexeme<'sql>> {
        let (_, second) = self.lookahead_token(2)?;
        let expected = {
            let grammar = self.environment.storage.grammar_view();
            grammar.state(number).and_then(|state| state.next_tokens(self.work)).map_err(RuntimeError::from)
        };
        let expected = self.finish(expected)?;
        if expected.contains(ExpectedSymbol::Token(second)) {
            return self.report_unwanted_token(expected);
        }

        let (_, first) = self.lookahead_token(1)?;
        let following = (|| {
            let grammar = self.environment.storage.grammar_view();
            let state = grammar.state(number)?;
            let Some(TransitionView::Atom { target, .. }) = state.transitions().next() else {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            };
            let invocations = self.invocation_view();
            Ok(target.next_tokens_with_context(invocations.invocations(&grammar), self.work)?)
        })();
        if self.finish(following)?.contains(ExpectedSymbol::Token(first)) {
            self.control = ParserControl::InErrorRecovery;
            let offending = self.input.lt(1)?.expect("positive lookahead");
            return self.report_token_syntax(TokenReport::Missing(offending), MismatchExpectation::Current(expected));
        }

        let expectation = self.next_tokens_capture.map_or(MismatchExpectation::Current(expected), MismatchExpectation::Captured);
        self.pend_input_mismatch(expectation)
    }
}
