use super::generated_actions::{CallAction, PredictionAction, SyncAction, SyncShape, TerminalAction, TokenAction};
use super::generated_names::is_identifier_token;
use super::{
    ContextChild, ContextClass, InvocationKey, InvocationRole, ParserControl, ParserSession, ParserState,
    PassError, PassResult, PrecedenceFrame, RecursionEntry, RecursionPhase, RuntimeError, RuntimeProblem, RuntimeResult,
};
use super::super::{Alternative, PredictionMode};

#[cfg(test)]
#[path = "generated_select_tests.rs"]
mod tests;

const STATEMENT: u16 = 2;
const COLUMN_LABEL: u16 = 25;
const SELECT_CLAUSE: u16 = 110;
const SELECT_LIST: u16 = 111;
const SELECT_SUBLIST: u16 = 113;
const DERIVED_COLUMN: u16 = 120;
const AS_CLAUSE: u16 = 121;
const QUERY_EXPRESSION: u16 = 122;
const QUERY_NO_WITH: u16 = 123;
const QUERY_BODY: u16 = 126;
const QUANTIFIER: u16 = 148;
const MAX_SELECT_CLAUSE_RESIDUE_DEPTH: usize = 9;

const fn call(rule: u16, state: u32, called: u16, start: u32, follow: u32) -> CallAction {
    CallAction { state, rule, called, start, follow, precedence: 0 }
}

const fn required(rule: u16, state: u32) -> SyncAction {
    SyncAction { state, rule, shape: SyncShape::RequiredBlock }
}

const fn nullable(rule: u16, state: u32) -> SyncAction {
    SyncAction { state, rule, shape: SyncShape::NullableBlock }
}

const fn decision(sync: SyncAction, decision: u16, alternatives: usize) -> PredictionAction {
    PredictionAction { sync, decision_state: sync.state, decision, alternatives }
}

const fn atom(rule: u16, state: u32, token: i32, target: u32) -> TerminalAction {
    TerminalAction { state, rule, target, tokens: TokenAction::Atom(token) }
}

const MAY_EXPLAIN: SyncAction = required(STATEMENT, 375);
const MAY_ANALYZE: SyncAction = required(STATEMENT, 373);
const MAY_SETTING: SyncAction = required(STATEMENT, 378);
const EXPLAIN: TerminalAction = atom(STATEMENT, 371, 137, 373);
const ANALYZE: TerminalAction = atom(STATEMENT, 372, 50, 374);
const SETTING_CALL: CallAction = call(STATEMENT, 377, 9, 18, 379);
const QUERY_CALL: CallAction = call(STATEMENT, 380, QUERY_EXPRESSION, 244, 575);

const MAY_WITH: SyncAction = required(QUERY_EXPRESSION, 2418);
const WITH_CALL: CallAction = call(QUERY_EXPRESSION, 2417, 124, 248, 2419);
const NO_WITH_CALL: CallAction = call(QUERY_EXPRESSION, 2420, QUERY_NO_WITH, 246, 2421);
const QUERY_BODY_CALL: CallAction = call(QUERY_NO_WITH, 2422, QUERY_BODY, 252, 2424);
const MAY_ORDER: SyncAction = nullable(QUERY_NO_WITH, 2424);
const MAY_OFFSET: SyncAction = nullable(QUERY_NO_WITH, 2427);
const ORDER_CALL: CallAction = call(QUERY_NO_WITH, 2423, 129, 258, 2425);
const OFFSET_CALL: CallAction = call(QUERY_NO_WITH, 2426, 130, 260, 2428);

const QUERY_BASE: SyncAction = required(QUERY_BODY, 2496);
const QUERY_SELECT: CallAction = call(QUERY_BODY, 2455, SELECT_CLAUSE, 220, 2457);
const QUERY_TAILS: [(PredictionAction, CallAction); 5] = [
    (decision(nullable(QUERY_BODY, 2457), 274, 2), call(QUERY_BODY, 2456, 75, 150, 2458)),
    (decision(nullable(QUERY_BODY, 2460), 275, 2), call(QUERY_BODY, 2459, 88, 176, 2461)),
    (decision(nullable(QUERY_BODY, 2463), 276, 2), call(QUERY_BODY, 2462, 89, 178, 2464)),
    (decision(nullable(QUERY_BODY, 2466), 277, 2), call(QUERY_BODY, 2465, 91, 182, 2467)),
    (decision(nullable(QUERY_BODY, 2469), 278, 2), call(QUERY_BODY, 2468, 92, 184, 2470)),
];
const QUERY_LOOP: PredictionAction = decision(
    SyncAction { state: 2512, rule: QUERY_BODY, shape: SyncShape::NullablePrecedenceEntry }, 286, 2,
);
const QUERY_COMBINATION: PredictionAction = decision(
    SyncAction { state: 2510, rule: QUERY_BODY, shape: SyncShape::RequiredStarBlock }, 285, 2,
);

const SELECT: TerminalAction = atom(SELECT_CLAUSE, 2297, 287, 2299);
const MAY_QUANTIFIER: PredictionAction = decision(nullable(SELECT_CLAUSE, 2299), 245, 2);
const QUANTIFIER_CALL: CallAction = call(SELECT_CLAUSE, 2298, QUANTIFIER, 296, 2300);
const QUANTIFIER_SET: TerminalAction = TerminalAction { state: 2720, rule: QUANTIFIER, target: 2721, tokens: TokenAction::Set(&[48, 119]) };
const SELECT_LIST_CALL: CallAction = call(SELECT_CLAUSE, 2301, SELECT_LIST, 222, 2302);

const LIST_BASE: PredictionAction = decision(nullable(SELECT_LIST, 2329), 252, 2);
const FIRST_STAR: CallAction = call(SELECT_LIST, 2303, 112, 224, 2310);
const FIRST_ITEM: PredictionAction = decision(nullable(SELECT_LIST, 2314), 248, 2);
const FIRST_ITEM_CALL: CallAction = call(SELECT_LIST, 2313, SELECT_SUBLIST, 226, 2315);
const LIST_LOOP: PredictionAction = decision(
    SyncAction { state: 2322, rule: SELECT_LIST, shape: SyncShape::NullableLoopEntry }, 250, 2,
);
const LIST_BACKEDGE: PredictionAction = PredictionAction {
    sync: SyncAction { state: 2324, rule: SELECT_LIST, shape: SyncShape::NullableLoopback },
    ..LIST_LOOP
};
const LIST_COMMA: TerminalAction = atom(SELECT_LIST, 2316, 10, 2318);
const NEXT_ITEM: PredictionAction = decision(nullable(SELECT_LIST, 2318), 249, 2);
const NEXT_ITEM_CALL: CallAction = call(SELECT_LIST, 2317, SELECT_SUBLIST, 226, 2319);
const TRAILING_STAR: PredictionAction = decision(nullable(SELECT_LIST, 2327), 251, 2);
const TRAILING_COMMA: TerminalAction = atom(SELECT_LIST, 2325, 10, 2326);
const TRAILING_STAR_CALL: CallAction = call(SELECT_LIST, 2326, 112, 224, 2328);

const SUBLIST_BASE: PredictionAction = decision(required(SELECT_SUBLIST, 2340), 255, 2);
const DERIVED_CALL: CallAction = call(SELECT_SUBLIST, 2338, DERIVED_COLUMN, 240, 2341);
const QUALIFIED_STAR_CALL: CallAction = call(SELECT_SUBLIST, 2339, 114, 228, 2341);
const MAY_ALIAS: PredictionAction = decision(nullable(DERIVED_COLUMN, 2407), 264, 2);
const AS_CALL: CallAction = call(DERIVED_COLUMN, 2406, AS_CLAUSE, 242, 2408);
const ALIAS_BASE: SyncAction = required(AS_CLAUSE, 2415);
const EXPLICIT_ALIAS: SyncAction = required(AS_CLAUSE, 2412);
const AS: TerminalAction = atom(AS_CLAUSE, 2409, 55, 2412);
const DEFAULT_ALIAS: TerminalAction = atom(AS_CLAUSE, 2411, 113, 2413);
const EXPLICIT_LABEL_CALL: CallAction = call(AS_CLAUSE, 2410, COLUMN_LABEL, 50, 2413);
const IMPLICIT_LABEL_CALL: CallAction = call(AS_CLAUSE, 2414, COLUMN_LABEL, 50, 2416);
const LABEL_BASE: SyncAction = required(COLUMN_LABEL, 854);
const LABEL_IDENTIFIER: CallAction = call(COLUMN_LABEL, 852, 22, 44, 855);
const CURRENT_USER_LABEL: TerminalAction = atom(COLUMN_LABEL, 853, 104, 855);

fn is_column_label(token: crate::parser::grammar::TokenType) -> bool {
    token.number() == 104 || is_identifier_token(token)
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(super) fn generated_query(&mut self, local: InvocationKey<'parse>, mode: PredictionMode) -> PassResult<'parse, 'sql, ()> {
        if self.lookahead_action(MAY_EXPLAIN)?.number() == 137 {
            self.match_tree_action(local, EXPLAIN)?;
            if self.lookahead_action(MAY_ANALYZE)?.number() == 50 {
                self.match_tree_action(local, ANALYZE)?;
            }
        }
        if self.lookahead_action(MAY_SETTING)?.number() == 292 {
            return self.decline_select_call(SETTING_CALL);
        }
        self.generated_call(QUERY_CALL)?;
        self.query_expression(mode).map(|_| ())
    }

    fn query_expression(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(QUERY_EXPRESSION, |session, _| {
            if session.lookahead_action(MAY_WITH)?.number() == 358 {
                return session.decline_select_call(WITH_CALL);
            }
            session.generated_call(NO_WITH_CALL)?;
            session.query_no_with(mode).map(|_| ())
        })
    }

    fn query_no_with(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(QUERY_NO_WITH, |session, _| {
            session.query_body(mode)?;
            if session.lookahead_action(MAY_ORDER)?.number() == 233 {
                return session.decline_select_call(ORDER_CALL);
            }
            if matches!(session.lookahead_action(MAY_OFFSET)?.number(), 140 | 194 | 228) {
                session.generated_call(OFFSET_CALL)?;
                session.offset_and_limit()?;
            }
            Ok(())
        })
    }

    fn query_body(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(QUERY_BODY_CALL)?;
        let pending = self.new_recursion_entry(QUERY_BODY)?;
        let entry = self.enter_recursion(pending, QUERY_BODY_CALL.precedence)?;
        let mut local = entry.key;
        let body = (|| {
            match self.lookahead_action(QUERY_BASE)?.number() {
                287 => (),
                15 | 152 | 269 | 346 | 361 => return self.decline_select(),
                _ => return self.generated_no_viable().map_err(PassError::from),
            }
            local = self.label_recursion_base(&entry, ContextClass::QuerySpecification)?;
            self.generated_call(QUERY_SELECT)?;
            self.select_clause(mode)?;
            for (optional, call) in QUERY_TAILS {
                if self.predict_select(optional, mode)?.number() == 1 {
                    return self.decline_select_call(call);
                }
            }
            let stop = self.input.lt(-1).map_err(RuntimeError::from);
            let stop = self.finish(stop)?;
            self.records[local.index].stop = stop;
            if self.predict_select(QUERY_LOOP, mode)?.number() == 1 {
                self.predict_select(QUERY_COMBINATION, mode)?;
                return self.decline_select();
            }
            Ok(())
        })();
        let result = match body {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.unroll_select_body(&entry)?;
        result.map(|()| local)
    }

    fn select_clause(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(SELECT_CLAUSE, |session, local| {
            session.match_tree_action(*local, SELECT)?;
            if session.predict_select(MAY_QUANTIFIER, mode)?.number() == 1 {
                session.generated_call(QUANTIFIER_CALL)?;
                session.generated_ordinary(QUANTIFIER, |session, quantifier| {
                    session.match_tree_action(*quantifier, QUANTIFIER_SET)
                })?;
            }
            session.generated_call(SELECT_LIST_CALL)?;
            session.select_list(mode).map(|_| ())
        })
    }

    pub(super) fn select_list(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(SELECT_LIST, |session, local| {
            if session.predict_select(LIST_BASE, mode)?.number() == 1 {
                return session.decline_select_call(FIRST_STAR);
            }
            if session.predict_select(FIRST_ITEM, mode)?.number() == 1 {
                session.generated_call(FIRST_ITEM_CALL)?;
                session.select_sublist(mode)?;
            }
            let mut repeat = LIST_LOOP;
            while session.predict_select(repeat, mode)?.number() == 1 {
                session.match_tree_action(*local, LIST_COMMA)?;
                if session.predict_select(NEXT_ITEM, mode)?.number() == 1 {
                    session.generated_call(NEXT_ITEM_CALL)?;
                    session.select_sublist(mode)?;
                }
                repeat = LIST_BACKEDGE;
            }
            if session.predict_select(TRAILING_STAR, mode)?.number() == 1 {
                session.match_tree_action(*local, TRAILING_COMMA)?;
                return session.decline_select_call(TRAILING_STAR_CALL);
            }
            Ok(())
        })
    }

    fn select_sublist(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(SELECT_SUBLIST, |session, _| {
            if session.predict_select(SUBLIST_BASE, mode)?.number() == 2 {
                return session.decline_select_call(QUALIFIED_STAR_CALL);
            }
            session.generated_call(DERIVED_CALL)?;
            session.generated_ordinary(DERIVED_COLUMN, |session, _| {
                session.derived_column_expression(mode)?;
                if session.predict_select(MAY_ALIAS, mode)?.number() == 1 {
                    session.generated_call(AS_CALL)?;
                    session.as_clause()?;
                }
                Ok(())
            }).map(|_| ())
        })
    }

    fn as_clause(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(AS_CLAUSE, |session, local| {
            let token = session.lookahead_action(ALIAS_BASE)?;
            let call = if token.number() == 55 {
                session.match_tree_action(*local, AS)?;
                let label = session.lookahead_action(EXPLICIT_ALIAS)?;
                if label.number() == 113 {
                    return session.match_tree_action(*local, DEFAULT_ALIAS);
                }
                if !is_column_label(label) {
                    return session.generated_no_viable().map_err(PassError::from);
                }
                EXPLICIT_LABEL_CALL
            } else if is_column_label(token) {
                IMPLICIT_LABEL_CALL
            } else {
                return session.generated_no_viable().map_err(PassError::from);
            };
            session.generated_call(call)?;
            session.column_label().map(|_| ())
        })
    }

    fn column_label(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(COLUMN_LABEL, |session, local| {
            let token = session.lookahead_action(LABEL_BASE)?;
            if token.number() == 104 {
                session.match_tree_action(*local, CURRENT_USER_LABEL)
            } else if is_identifier_token(token) {
                session.generated_call(LABEL_IDENTIFIER)?;
                session.generated_identifier().map(|_| ())
            } else {
                session.generated_no_viable().map_err(PassError::from)
            }
        })
    }

    fn predict_select(&mut self, action: PredictionAction, mode: PredictionMode) -> RuntimeResult<'sql, Alternative> {
        self.sync_action(action.sync)?;
        let agrees = action.agrees(&self.environment.storage.grammar_view());
        if !agrees {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        self.adaptive_predict(action.decision, mode)
    }

    fn decline_select<T>(&mut self) -> PassResult<'parse, 'sql, T> {
        self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from)
    }

    fn decline_select_call<T>(&mut self, call: CallAction) -> PassResult<'parse, 'sql, T> {
        self.generated_call(call)?;
        self.decline_select()
    }

    fn unroll_select_body(&mut self, entry: &RecursionEntry<'parse>) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            let record = self.record(entry.key)?;
            let InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head } } = record.role else {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            };
            if record.rule != QUERY_BODY
                || !matches!(self.precedence.last(), Some(PrecedenceFrame::Recursion { entry: top, .. }) if *top == entry.key)
            {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }
            let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Recursion))?;
            if current != head && !self.failed_select_clause_is_current(head, current)? {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }
            let returned = self.unroll_entered_recursion(entry.key, saved)?;
            if let Some(parent) = saved.parent {
                self.append_child_storage(parent, ContextChild::Context(returned))?;
            }
            Ok(())
        })();
        self.finish(result)
    }

    fn failed_select_clause_is_current(&self, head: InvocationKey<'parse>, current: InvocationKey<'parse>) -> RuntimeResult<'sql, bool> {
        let clause = self.record(current)?;
        if !matches!(self.control, ParserControl::Failed)
            || self.record(head)?.class != Some(ContextClass::QuerySpecification)
            || clause.rule != SELECT_CLAUSE || clause.parent != Some(head)
            || clause.invoking != ParserState::State(QUERY_SELECT.state)
            || clause.start.is_none() || clause.stop.is_some() || clause.class.is_some()
        {
            return Ok(false);
        }
        if matches!(clause.role, InvocationRole::OrdinaryTreeAttachmentFailed) {
            return Ok(self.state == ParserState::State(QUERY_SELECT.start));
        }
        if !matches!(clause.role, InvocationRole::Ordinary) {
            return Ok(false);
        }
        let left = match self.state {
            ParserState::State(2298) => QUANTIFIER_CALL,
            ParserState::State(2301) => SELECT_LIST_CALL,
            _ => return Ok(false),
        };
        let Some(mut child) = self.records.last() else { return Ok(false); };
        if !matches!(child.role, InvocationRole::OrdinaryTreeAttachmentFailed) || child.start.is_none() {
            return Ok(false);
        }
        for _ in 0..MAX_SELECT_CLAUSE_RESIDUE_DEPTH {
            let Some(parent) = child.parent else { return Ok(false); };
            if parent == current {
                return Ok(child.rule == left.called && child.invoking == ParserState::State(left.state));
            }
            child = self.record(parent)?;
        }
        Ok(false)
    }
}
