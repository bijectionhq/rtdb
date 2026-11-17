use crate::lexer::IndexedLexeme;
use crate::parser::grammar::{TokenType, TransitionView};
use super::{ContextChild, ContextClass, InvocationKey, ParserProfile, ParserSession, PassError, PassResult, RuntimeError, RuntimeProblem, RuntimeResult};
use super::super::PredictionMode;

#[cfg(test)]
#[path = "generated_isolation_tests.rs"]
mod tests;

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    fn generated_mode(&mut self) -> RuntimeResult<'sql, PredictionMode> {
        let result = self.ready().and_then(|()| match self.profile {
            ParserProfile::SllBail => Ok(PredictionMode::Sll),
            ParserProfile::LlDefault => Ok(PredictionMode::Ll),
            ParserProfile::Kernel => Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
        });
        self.finish(result)
    }

    fn expected_at(&mut self, number: u32) -> RuntimeResult<'sql, TokenType> {
        let result = (|| {
            let grammar = self.environment.storage.grammar_view();
            let state = grammar.state(number)?;
            match state.transitions().next() {
                Some(TransitionView::Atom { token, .. }) => Ok(token),
                _ => Err(RuntimeError::Control(RuntimeProblem::PredictionSite)),
            }
        })();
        self.finish(result)
    }

    fn match_tree_at(&mut self, state: u32) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
        self.set_state(state)?;
        let token = self.match_isolation_atom()?;
        let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))?;
        self.append_child(current, ContextChild::Terminal(token))?;
        Ok(token)
    }

    pub(in crate::parser::prediction) fn level_of_isolation(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mode = self.generated_mode()?;
        let mut local = self.new_invocation(175)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            self.set_state(3014)?;
            self.sync_required_block()?;
            let alternative = self.adaptive_predict(371, mode)?.number();
            let (class, states): (ContextClass, &[u32]) = match alternative {
                1 => (ContextClass::ReadUncommittedIsolation, &[3007, 3008]),
                2 => (ContextClass::ReadCommittedIsolation, &[3009, 3010]),
                3 => (ContextClass::RepeatableReadIsolation, &[3011, 3012]),
                4 => (ContextClass::SerializableIsolation, &[3013]),
                _ => return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from),
            };
            local = self.labelled_context(local, class)?;
            self.enter_outer_alt(local)?;
            for &state in states { self.match_tree_at(state)?; }
            Ok(local)
        })();
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.exit_rule()?;
        result
    }

    pub(in crate::parser::prediction) fn common_tx_mode_isolation(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_mode()?;
        let mut local = self.new_invocation(164)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            self.set_state(2886)?;
            self.sync_required_block()?;
            let (_, actual) = self.lookahead_token(1)?;
            if actual != self.expected_at(2882)? {
                return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
            }
            local = self.labelled_context(local, ContextClass::IsolationLevelTxMode)?;
            self.enter_outer_alt(local)?;
            self.match_tree_at(2882)?;
            self.match_tree_at(2883)?;
            self.set_state(2884)?;
            self.level_of_isolation()?;
            Ok(local)
        })();
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.exit_rule()?;
        result
    }
}
