use super::{ContextChild, ContextClass, InvocationKey, ParserSession, ParserState, PassError, PassResult, RuntimeError, RuntimeProblem, RuntimeResult};
use super::super::PredictionMode;

#[cfg(test)]
#[path = "generated_statement_tests.rs"]
mod tests;

const DIRECT_SQL_STATEMENT: u16 = 0;
const MULTI_SQL_STATEMENT: u16 = 1;
const DIRECTLY_EXECUTABLE_STATEMENT: u16 = 2;
const DISCARD_TARGET: u16 = 3;

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    fn statement_root_mode(&mut self) -> RuntimeResult<'sql, PredictionMode> {
        let mode = self.generated_mode()?;
        if self.current.is_some() || self.state != ParserState::RootSentinel {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        Ok(mode)
    }

    pub(in crate::parser::prediction) fn direct_sql_statement(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mode = self.statement_root_mode()?;
        self.generated_ordinary(DIRECT_SQL_STATEMENT, |session, local| {
            session.set_state(352)?;
            session.directly_executable_statement(mode, false)?;
            session.set_state(354)?;
            session.sync_required_block()?;
            let (_, token) = session.lookahead_token(1)?;
            if token.number() == 12 {
                session.match_statement_terminal(*local, 353)?;
            }
            session.match_statement_terminal(*local, 356)
        })
    }

    pub(in crate::parser::prediction) fn multi_sql_statement(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mode = self.statement_root_mode()?;
        self.generated_ordinary(MULTI_SQL_STATEMENT, |session, local| {
            session.set_state(358)?;
            session.directly_executable_statement(mode, true)?;
            session.set_state(363)?;
            session.sync_statement()?;
            while session.adaptive_predict(1, mode)?.number() == 1 {
                session.match_statement_terminal(*local, 359)?;
                session.set_state(360)?;
                session.directly_executable_statement(mode, true)?;
                session.set_state(365)?;
                session.sync_statement()?;
            }

            session.set_state(367)?;
            session.sync_statement()?;
            let (_, token) = session.lookahead_token(1)?;
            if token.number() == 12 {
                session.match_statement_terminal(*local, 366)?;
            }
            session.match_statement_terminal(*local, 369)
        })
    }

    fn directly_executable_statement(&mut self, mode: PredictionMode, batch: bool) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(DIRECTLY_EXECUTABLE_STATEMENT, |session, local| {
            session.set_state(574)?;
            session.sync_required_block()?;
            let alt = session.adaptive_predict(34, mode)?.number();
            let class = match alt {
                1 => ContextClass::QueryExpr,
                2 => ContextClass::InsertStatement,
                3 => ContextClass::UpdateStatement,
                4 => ContextClass::DeleteStatement,
                11 => ContextClass::StartTransactionStatement,
                13 => ContextClass::CommitStatement,
                14 => ContextClass::RollbackStatement,
                16 => ContextClass::DiscardStatement,
                17 => ContextClass::SetRoleStatement,
                _ => return session.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            match class {
                ContextClass::QueryExpr if batch => session.replay_statement_alternative(*local, alt, mode),
                ContextClass::QueryExpr => session.generated_query(*local, mode),
                ContextClass::InsertStatement | ContextClass::UpdateStatement | ContextClass::DeleteStatement => {
                    session.replay_statement_alternative(*local, alt, mode)
                }
                ContextClass::StartTransactionStatement => session.start_transaction_statement(*local, mode),
                ContextClass::CommitStatement => session.commit_statement(*local),
                ContextClass::RollbackStatement => session.match_statement_terminal(*local, 488),
                ContextClass::DiscardStatement => {
                    session.match_statement_terminal(*local, 501)?;
                    session.set_state(502)?;
                    session.discard_target()?;
                    Ok(())
                }
                ContextClass::SetRoleStatement => session.set_role_statement(*local),
                _ => unreachable!("selected statement class"),
            }
        })
    }

    /// `(START TRANSACTION | BEGIN TRANSACTION?) (transactionCharacteristics | beginWithCharacteristics)?`.
    /// The characteristics rules are not ported, so a prediction that enters either leaves the vendor error in place.
    fn start_transaction_statement(&mut self, local: InvocationKey<'parse>, mode: PredictionMode) -> PassResult<'parse, 'sql, ()> {
        self.set_state(475)?;
        self.sync_statement()?;
        let (_, token) = self.lookahead_token(1)?;
        match token.number() {
            298 => {
                self.match_statement_terminal(local, 469)?;
                self.match_statement_terminal(local, 470)?;
            }
            64 => {
                self.match_statement_terminal(local, 471)?;
                self.set_state(473)?;
                self.sync_statement()?;
                let (_, token) = self.lookahead_token(1)?;
                if token.number() == 323 { self.match_statement_terminal(local, 472)?; }
            }
            _ => return self.generated_no_viable().map_err(PassError::from),
        }
        self.set_state(479)?;
        self.sync_statement()?;
        match self.adaptive_predict(24, mode)?.number() {
            1 | 2 => self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from),
            _ => Ok(()),
        }
    }

    fn commit_statement(&mut self, local: InvocationKey<'parse>) -> PassResult<'parse, 'sql, ()> {
        self.match_statement_terminal(local, 484)?;
        self.set_state(486)?;
        self.sync_statement()?;
        let (_, token) = self.lookahead_token(1)?;
        if matches!(token.number(), 59 | 307) {
            self.match_statement_set_terminal(local, 485)?;
        }
        Ok(())
    }

    fn discard_target(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(DISCARD_TARGET, |session, local| {
            session.set_state(580)?;
            session.sync_statement()?;
            let (_, token) = session.lookahead_token(1)?;
            let (class, number) = match token.number() {
                48 => (ContextClass::DiscardAll, 576),
                253 => (ContextClass::DiscardPlans, 577),
                310 | 311 => (ContextClass::DiscardTemp, 578),
                288 => (ContextClass::DiscardSequences, 579),
                _ => return session.generated_no_viable().map_err(PassError::from),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            if number == 578 {
                session.match_statement_set_terminal(*local, number)
            } else {
                session.match_statement_terminal(*local, number)
            }
        })
    }

    fn set_role_statement(&mut self, local: InvocationKey<'parse>) -> PassResult<'parse, 'sql, ()> {
        self.match_statement_terminal(local, 503)?;
        self.match_statement_terminal(local, 504)?;
        self.set_state(507)?;
        self.sync_statement()?;
        let (_, token) = self.lookahead_token(1)?;
        if token.number() == 215 {
            self.match_statement_terminal(local, 506)?;
        } else if super::generated_names::is_identifier_token(token) {
            self.set_state(505)?;
            self.generated_identifier()?;
        } else {
            return self.generated_no_viable().map_err(PassError::from);
        }
        Ok(())
    }

    pub(super) fn match_statement_terminal(&mut self, local: InvocationKey<'parse>, state: u32) -> PassResult<'parse, 'sql, ()> {
        self.set_state(state)?;
        let token = self.match_statement_atom()?;
        self.append_child(local, ContextChild::Terminal(token))?;
        Ok(())
    }

    pub(super) fn match_statement_set_terminal(&mut self, local: InvocationKey<'parse>, state: u32) -> PassResult<'parse, 'sql, ()> {
        self.set_state(state)?;
        let token = self.match_statement_set()?;
        self.append_child(local, ContextChild::Terminal(token))?;
        Ok(())
    }
}
