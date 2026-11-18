use crate::parser::grammar::{ExpectedSymbol, StateKindView, TokenType};
use super::{ContextChild, InvocationKey, NextTokensCapture, ParserProfile, ParserSession, ParserState, PassError, PassResult, RuntimeError, RuntimeProblem, RuntimeResult};

#[cfg(test)]
#[path = "generated_window_tests.rs"]
mod tests;

const WINDOW_SPECIFICATION: u16 = 96;
const WINDOW_DETAILS: u16 = 97;

fn window_option_starts(state: u32, token: TokenType) -> bool {
    let token = token.number();
    match state {
        2223 => [
            (60, -660234687216492665_i64), (125, 334106536789082281),
            (190, -9194097995022889983), (255, -868006552961469279), (327, 859126005957),
        ].into_iter().any(|(offset, mask)| {
            let shift = token - offset;
            (0..64).contains(&shift) && mask & (1_i64 << shift) != 0
        }),
        2226 => token == 240,
        2229 => token == 233,
        2232 => matches!(token, 157 | 263 | 284),
        _ => unreachable!("guarded generated window option"),
    }
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn window_specification(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let site = self.ready().and_then(|()| {
            if self.profile != ParserProfile::Kernel && self.current.is_none() && self.state == ParserState::RootSentinel {
                Ok(())
            } else {
                Err(RuntimeError::Control(RuntimeProblem::PredictionSite))
            }
        });
        self.finish(site)?;
        let local = self.new_invocation(WINDOW_SPECIFICATION)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            self.match_window_terminal(local, 2218)?;
            self.set_state(2219)?;
            self.window_specification_details()?;
            self.match_window_terminal(local, 2220)?;
            Ok(local)
        })();
        self.finish_window_rule(local, body)
    }

    fn window_specification_details(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let local = self.new_invocation(WINDOW_DETAILS)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            for (state, call) in [(2223, 2222), (2226, 2225), (2229, 2228), (2232, 2231)] {
                self.set_state(state)?;
                self.sync_window_option()?;
                let (_, token) = self.lookahead_token(1)?;
                if window_option_starts(state, token) {
                    self.set_state(call)?;
                    return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
                }
            }
            Ok(local)
        })();
        self.finish_window_rule(local, body)
    }

    fn match_window_terminal(&mut self, local: InvocationKey<'parse>, state: u32) -> PassResult<'parse, 'sql, ()> {
        self.set_state(state)?;
        let token = self.match_window_atom()?;
        self.append_child(local, ContextChild::Terminal(token))?;
        Ok(())
    }

    fn finish_window_rule<T>(
        &mut self, local: InvocationKey<'parse>, body: PassResult<'parse, 'sql, T>,
    ) -> PassResult<'parse, 'sql, T> {
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.exit_rule()?;
        result
    }

    fn sync_window_option(&mut self) -> RuntimeResult<'sql, ()> {
        let site = (|| {
            self.ready()?;
            let ParserState::State(number @ (2223 | 2226 | 2229 | 2232)) = self.state else {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            };
            let context = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
            let record = self.record(context)?;
            if record.rule != WINDOW_DETAILS || record.invoking != ParserState::State(2219) || record.start.is_none() {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            let grammar = self.environment.storage.grammar_view();
            if !matches!(grammar.state(number)?.kind(), StateKindView::BlockStart { .. }) {
                return Err(RuntimeError::Control(RuntimeProblem::PredictionSite));
            }
            Ok(NextTokensCapture { context, state: number })
        })();
        let site = self.finish(site)?;
        match self.profile {
            ParserProfile::SllBail => return Ok(()),
            ParserProfile::LlDefault => (),
            ParserProfile::Kernel => return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))),
        }

        let (_, first) = self.lookahead_token(1)?;
        let next = {
            let grammar = self.environment.storage.grammar_view();
            grammar.state(site.state).and_then(|state| state.next_tokens(self.work)).map_err(RuntimeError::from)
        };
        let next = self.finish(next)?;
        if next.contains(ExpectedSymbol::Token(first)) {
            self.next_tokens_capture = None;
        } else if next.contains(ExpectedSymbol::Epsilon) {
            self.next_tokens_capture.get_or_insert(site);
        } else {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        Ok(())
    }
}
