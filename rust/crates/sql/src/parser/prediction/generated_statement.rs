use super::{ContextChild, ContextClass, InvocationKey, ParserSession, ParserState, PassError, PassResult, RuntimeError, RuntimeProblem};
use super::super::PredictionMode;

#[cfg(test)]
#[path = "generated_statement_tests.rs"]
mod tests;

const DIRECT_SQL_STATEMENT: u16 = 0;
const DIRECTLY_EXECUTABLE_STATEMENT: u16 = 2;

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn direct_sql_statement(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mode = self.generated_mode()?;
        if self.current.is_some() || self.state != ParserState::RootSentinel {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
        }
        let local = self.new_invocation(DIRECT_SQL_STATEMENT)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            self.set_state(352)?;
            self.directly_executable_statement(mode)?;
            self.set_state(354)?;
            self.sync_required_block()?;
            let (_, token) = self.lookahead_token(1)?;
            if token.number() == 12 {
                self.match_statement_terminal(local, 353)?;
            }
            self.match_statement_terminal(local, 356)?;
            Ok(local)
        })();
        self.finish_statement_rule(local, body)
    }

    fn directly_executable_statement(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mut local = self.new_invocation(DIRECTLY_EXECUTABLE_STATEMENT)?;
        self.enter_tree_rule(local)?;
        let body = (|| {
            self.set_state(574)?;
            self.sync_required_block()?;
            if self.adaptive_predict(34, mode)?.number() != 14 {
                return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from);
            }
            local = self.labelled_context(local, ContextClass::RollbackStatement)?;
            self.enter_outer_alt(local)?;
            self.match_statement_terminal(local, 488)?;
            Ok(local)
        })();
        self.finish_statement_rule(local, body)
    }

    fn match_statement_terminal(&mut self, local: InvocationKey<'parse>, state: u32) -> PassResult<'parse, 'sql, ()> {
        self.set_state(state)?;
        let token = self.match_statement_atom()?;
        self.append_child(local, ContextChild::Terminal(token))?;
        Ok(())
    }

    fn finish_statement_rule<T>(
        &mut self, local: InvocationKey<'parse>, body: PassResult<'parse, 'sql, T>,
    ) -> PassResult<'parse, 'sql, T> {
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.exit_rule()?;
        result
    }
}
