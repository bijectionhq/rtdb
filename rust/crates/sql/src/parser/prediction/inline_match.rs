use crate::lexer::IndexedLexeme;
use crate::parser::grammar::{ExpectedSymbol, StateKindView, TransitionView};
use super::{ContextChild, InvocationKey, MismatchExpectation, ParserControl, ParserProfile, ParserSession, ParserState, PassError, PassResult, RuntimeError, RuntimeProblem, RuntimeResult};
use super::generated_actions::{TerminalAction, TerminalExpectation};
use super::required_block_sync::TokenReport;

#[derive(Clone, Copy)]
enum TerminalMatch { Atom, Set }

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    #[cfg(test)]
    pub(in crate::parser::prediction) fn match_isolation_atom(&mut self) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.match_generated_terminal(TerminalMatch::Atom, |state| match state {
            ParserState::State(number @ (2882 | 2883)) => Some((number, 164)),
            ParserState::State(number @ 3007..=3013) => Some((number, 175)),
            _ => None,
        })
    }

    pub(super) fn match_statement_atom(&mut self) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.match_generated_terminal(TerminalMatch::Atom, |state| match state {
            ParserState::State(number @ (353 | 356)) => Some((number, 0)),
            ParserState::State(number @ (359 | 366 | 369)) => Some((number, 1)),
            ParserState::State(number @ (469..=472 | 484 | 488 | 501 | 503 | 504 | 506)) => Some((number, 2)),
            ParserState::State(number @ (576 | 577 | 579)) => Some((number, 3)),
            ParserState::State(number @ (754 | 758)) => Some((number, 22)),
            ParserState::State(number @ (761..=796 | 801..=847)) => Some((number, 23)),
            ParserState::State(1895) => Some((1895, 63)),
            _ => None,
        })
    }

    pub(super) fn match_statement_set(&mut self) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.match_generated_terminal(TerminalMatch::Set, |state| match state {
            ParserState::State(485) => Some((485, 2)),
            ParserState::State(578) => Some((578, 3)),
            ParserState::State(734) => Some((734, 19)),
            ParserState::State(1804) => Some((1804, 44)),
            ParserState::State(1900) => Some((1900, 65)),
            ParserState::State(2718) => Some((2718, 147)),
            _ => None,
        })
    }

    fn match_generated_terminal(
        &mut self, shape: TerminalMatch, site: impl FnOnce(ParserState) -> Option<(u32, u16)>,
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
            match (shape, state.kind(), edges.next(), edges.len()) {
                (TerminalMatch::Atom, StateKindView::Basic, Some(TransitionView::Atom { token, .. }), 0) =>
                    Ok((number, TerminalExpectation::Atom(token))),
                (TerminalMatch::Set, StateKindView::Basic, Some(TransitionView::Set { tokens, .. }), 0) =>
                    Ok((number, TerminalExpectation::Set(*tokens))),
                _ => Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
            }
        })();
        let (number, expected) = self.finish(site)?;
        self.match_validated_terminal(number, expected)
    }

    pub(super) fn match_terminal_action(&mut self, action: TerminalAction) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        if self.profile == ParserProfile::Kernel {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::Aborted))).map_err(PassError::from);
        }
        self.generated_action_state(action.state, action.rule)?;
        let expected = action.read(&self.environment.storage.grammar_view())
            .ok_or(RuntimeError::Control(RuntimeProblem::PredictionSite));
        let expected = self.finish(expected)?;
        self.match_validated_terminal(action.state, expected)
    }

    pub(super) fn match_tree_action(&mut self, local: InvocationKey<'parse>, action: TerminalAction) -> PassResult<'parse, 'sql, ()> {
        let matched = self.match_terminal_action(action)?;
        self.append_child(local, ContextChild::Terminal(matched))?;
        Ok(())
    }

    pub(super) fn match_validated_terminal(
        &mut self, number: u32, expected: TerminalExpectation,
    ) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        if let TerminalExpectation::Atom(token) = expected && self.profile == ParserProfile::SllBail {
            return self.match_bail(token);
        }
        let (matched, actual) = self.lookahead_token(1)?;
        let accepts = match expected {
            TerminalExpectation::Atom(token) => actual == token,
            TerminalExpectation::Set(tokens) => tokens.contains(ExpectedSymbol::Token(actual)),
        };
        if !accepts {
            if self.profile == ParserProfile::SllBail {
                return self.cancel_inline_mismatch();
            }
            return self.recover_inline(number).map_err(PassError::from);
        }
        let current = match expected {
            TerminalExpectation::Atom(_) => actual,
            TerminalExpectation::Set(_) => self.lookahead_token(1)?.1,
        };
        self.consume_match(current)?;
        Ok(matched)
    }

    fn recover_inline(&mut self, number: u32) -> RuntimeResult<'sql, IndexedLexeme<'sql>> {
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
            let target = match state.transitions().next() {
                Some(TransitionView::Atom { target, .. } | TransitionView::Set { target, .. }) => target,
                _ => return Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
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
