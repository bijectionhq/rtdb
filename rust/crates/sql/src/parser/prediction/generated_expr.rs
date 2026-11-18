use super::generated_actions::{CallAction, PredicateAction, PredictionAction, SyncAction, SyncShape, TerminalAction, TokenAction};
use super::{
    ContextChild, ContextClass, InvocationKey, InvocationRole, ParserControl, ParserSession, ParserState,
    PassError, PassResult, RecursionEntry, RecursionPhase, RuntimeError, RuntimeProblem, RuntimeResult,
};
use super::super::{Alternative, PredictionMode};

#[cfg(test)]
#[path = "generated_expr_tests.rs"]
mod tests;

const INTEGER_LITERAL: u16 = 13;
const LITERAL: u16 = 14;
const EXPR: u16 = 33;
const COMMON_VALUE: u16 = 34;
const SIGN_TOKENS: &[i32] = &[24, 27];

const fn prediction(rule: u16, state: u32, decision: u16, alternatives: usize, shape: SyncShape) -> PredictionAction {
    PredictionAction { sync: SyncAction { state, rule, shape }, decision_state: state, decision, alternatives }
}

const EXPR_BASE: PredictionAction = prediction(EXPR, 1001, 89, 3, SyncShape::RequiredBlock);
const EXPR_LOOP: PredictionAction = prediction(EXPR, 1131, 99, 2, SyncShape::NullablePrecedenceEntry);
const EXPR_TAIL: PredictionAction = prediction(EXPR, 1129, 98, 28, SyncShape::RequiredStarBlock);
const VALUE_BASE: PredictionAction = prediction(COMMON_VALUE, 1634, 151, 63, SyncShape::RequiredBlock);
const VALUE_LOOP: PredictionAction = prediction(COMMON_VALUE, 1661, 153, 2, SyncShape::NullablePrecedenceEntry);
const VALUE_BACKEDGE: PredictionAction = PredictionAction {
    sync: SyncAction { state: 1663, rule: COMMON_VALUE, shape: SyncShape::NullableLoopback },
    ..VALUE_LOOP
};
const VALUE_TAIL: PredictionAction = prediction(COMMON_VALUE, 1659, 152, 7, SyncShape::RequiredStarBlock);
const LITERAL_BASE: PredictionAction = prediction(LITERAL, 709, 55, 13, SyncShape::RequiredBlock);
const FLOAT_SIGN: SyncAction = SyncAction { state: 689, rule: LITERAL, shape: SyncShape::RequiredBlock };
const INTEGER_SIGN: SyncAction = SyncAction { state: 684, rule: INTEGER_LITERAL, shape: SyncShape::RequiredBlock };

const EXPR_VALUE: CallAction = CallAction {
    state: 1000, rule: EXPR, called: COMMON_VALUE, start: 68, follow: 1002, precedence: 0,
};
const VALUE_LITERAL: CallAction = CallAction {
    state: 1140, rule: COMMON_VALUE, called: LITERAL, start: 28, follow: 1635, precedence: 0,
};
const LITERAL_INTEGER: CallAction = CallAction {
    state: 692, rule: LITERAL, called: INTEGER_LITERAL, start: 26, follow: 710, precedence: 0,
};
const FACTOR_RHS: CallAction = CallAction {
    state: 1641, rule: COMMON_VALUE, called: COMMON_VALUE, start: 68, follow: 1660, precedence: 62,
};
const TERM_RHS: CallAction = CallAction {
    state: 1644, rule: COMMON_VALUE, called: COMMON_VALUE, start: 68, follow: 1660, precedence: 61,
};

const MATCH_FLOAT_SIGN: TerminalAction = TerminalAction { state: 688, rule: LITERAL, target: 690, tokens: TokenAction::Set(SIGN_TOKENS) };
const MATCH_FLOAT: TerminalAction = TerminalAction { state: 691, rule: LITERAL, target: 710, tokens: TokenAction::Atom(4) };
const MATCH_INTEGER_SIGN: TerminalAction = TerminalAction { state: 683, rule: INTEGER_LITERAL, target: 685, tokens: TokenAction::Set(SIGN_TOKENS) };
const MATCH_INTEGER: TerminalAction = TerminalAction { state: 686, rule: INTEGER_LITERAL, target: 687, tokens: TokenAction::Atom(5) };
const MATCH_FACTOR: TerminalAction = TerminalAction { state: 1640, rule: COMMON_VALUE, target: 1641, tokens: TokenAction::Set(&[28, 29, 30]) };
const MATCH_TERM: TerminalAction = TerminalAction { state: 1643, rule: COMMON_VALUE, target: 1644, tokens: TokenAction::Set(SIGN_TOKENS) };
const FACTOR_PREDICATE: PredicateAction = PredicateAction { state: 1639, rule: COMMON_VALUE, target: 1640, minimum: 61 };
const TERM_PREDICATE: PredicateAction = PredicateAction { state: 1642, rule: COMMON_VALUE, target: 1643, minimum: 60 };

#[derive(Clone, Copy)]
struct NumericTail {
    class: ContextClass,
    predicate: PredicateAction,
    terminal: TerminalAction,
    rhs: CallAction,
}

const NUMERIC_FACTOR: NumericTail = NumericTail {
    class: ContextClass::NumericFactorExpr, predicate: FACTOR_PREDICATE, terminal: MATCH_FACTOR, rhs: FACTOR_RHS,
};
const NUMERIC_TERM: NumericTail = NumericTail {
    class: ContextClass::NumericTermExpr, predicate: TERM_PREDICATE, terminal: MATCH_TERM, rhs: TERM_RHS,
};

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(in crate::parser::prediction) fn numeric_expression(&mut self) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let mode = self.generated_mode()?;
        self.numeric_expr_zero(mode)
    }

    pub(super) fn derived_column_expression(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(CallAction {
            state: 2405, rule: 120, called: EXPR, start: 66, follow: 2407, precedence: 0,
        })?;
        self.numeric_expr_zero(mode)
    }

    fn numeric_expr_zero(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_numeric_recursion(EXPR, 0, |session, entry, local| {
            if session.predict_numeric(EXPR_BASE, mode)?.number() != 3 {
                return session.decline_numeric_site();
            }
            *local = session.label_recursion_base(entry, ContextClass::CommonValueExpr0)?;
            session.call_common_value(EXPR_VALUE, mode)?;
            session.stop_numeric_base(*local)?;
            if session.predict_numeric(EXPR_LOOP, mode)?.number() == 1 {
                session.predict_numeric(EXPR_TAIL, mode)?;
                return session.decline_numeric_site();
            }
            Ok(())
        })
    }

    fn call_common_value(&mut self, call: CallAction, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_call(call)?;
        self.generated_numeric_recursion(COMMON_VALUE, call.precedence, |session, entry, local| {
            if session.predict_numeric(VALUE_BASE, mode)?.number() != 3 {
                return session.decline_numeric_site();
            }
            *local = session.label_recursion_base(entry, ContextClass::LiteralExpr)?;
            session.generated_call(VALUE_LITERAL)?;
            session.numeric_literal(mode)?;
            session.stop_numeric_base(*local)?;
            let mut next = VALUE_LOOP;
            while session.predict_numeric(next, mode)?.number() == 1 {
                let tail = match session.predict_numeric(VALUE_TAIL, mode)?.number() {
                    2 => NUMERIC_FACTOR,
                    3 => NUMERIC_TERM,
                    _ => return session.decline_numeric_site(),
                };
                let replacement = session.new_recursion_replacement(entry)?;
                session.records[replacement.key.index].class = Some(tail.class);
                *local = replacement.key;
                session.push_recursion_tree(entry, replacement)?;
                session.generated_predicate(tail.predicate)?;
                session.match_numeric_terminal(*local, tail.terminal)?;
                session.call_common_value(tail.rhs, mode)?;
                next = VALUE_BACKEDGE;
            }
            Ok(())
        })
    }

    fn numeric_literal(&mut self, mode: PredictionMode) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        self.generated_ordinary(LITERAL, |session, local| {
            let class = match session.predict_numeric(LITERAL_BASE, mode)?.number() {
                1 => ContextClass::FloatLiteral,
                2 => ContextClass::IntegerLiteral0,
                _ => return session.decline_numeric_site(),
            };
            *local = session.labelled_context(*local, class)?;
            session.enter_outer_alt(*local)?;
            if class == ContextClass::FloatLiteral {
                session.signed_numeric_terminal(*local, FLOAT_SIGN, MATCH_FLOAT_SIGN, MATCH_FLOAT)
            } else {
                session.generated_call(LITERAL_INTEGER)?;
                session.generated_ordinary(INTEGER_LITERAL, |session, integer| {
                    session.signed_numeric_terminal(*integer, INTEGER_SIGN, MATCH_INTEGER_SIGN, MATCH_INTEGER)
                }).map(|_| ())
            }
        })
    }

    fn signed_numeric_terminal(
        &mut self, local: InvocationKey<'parse>, sync: SyncAction, sign: TerminalAction, number: TerminalAction,
    ) -> PassResult<'parse, 'sql, ()> {
        self.sync_action(sync)?;
        let (_, next) = self.lookahead_token(1)?;
        if SIGN_TOKENS.contains(&next.number()) {
            self.match_numeric_terminal(local, sign)?;
        }
        self.match_numeric_terminal(local, number)
    }

    fn predict_numeric(&mut self, action: PredictionAction, mode: PredictionMode) -> RuntimeResult<'sql, Alternative> {
        self.sync_action(action.sync)?;
        let agrees = action.agrees(&self.environment.storage.grammar_view());
        if !agrees {
            return self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite)));
        }
        self.adaptive_predict(action.decision, mode)
    }

    fn match_numeric_terminal(&mut self, local: InvocationKey<'parse>, action: TerminalAction) -> PassResult<'parse, 'sql, ()> {
        let matched = self.match_terminal_action(action)?;
        self.append_child(local, ContextChild::Terminal(matched))?;
        Ok(())
    }

    fn stop_numeric_base(&mut self, local: InvocationKey<'parse>) -> RuntimeResult<'sql, ()> {
        let result = self.input.lt(-1).map_err(RuntimeError::from);
        let stop = self.finish(result)?;
        self.records[local.index].stop = stop;
        Ok(())
    }

    fn decline_numeric_site<T>(&mut self) -> PassResult<'parse, 'sql, T> {
        self.finish(Err(RuntimeError::Control(RuntimeProblem::PredictionSite))).map_err(PassError::from)
    }

    fn generated_numeric_recursion(
        &mut self, rule: u16, precedence: i32,
        body: impl FnOnce(&mut Self, &RecursionEntry<'parse>, &mut InvocationKey<'parse>) -> PassResult<'parse, 'sql, ()>,
    ) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
        let state = self.state;
        let site = match (rule, state, precedence) {
            (EXPR, _, 0) if self.current.is_none() => self.precedence() == 0,
            (EXPR, ParserState::State(2405), 0) => self.precedence() == 0,
            (COMMON_VALUE, ParserState::State(1000), 0) => self.precedence() == 0,
            (COMMON_VALUE, ParserState::State(1644), 61) => self.precedence() <= TERM_PREDICATE.minimum,
            (COMMON_VALUE, ParserState::State(1641), 62) => self.precedence() <= FACTOR_PREDICATE.minimum,
            _ => false,
        };
        if !site {
            return self.decline_numeric_site();
        }
        let pending = self.new_recursion_entry(rule)?;
        let entry = self.enter_recursion(pending, precedence)?;
        let mut local = entry.key;
        let result = match body(self, &entry, &mut local) {
            Err(PassError::Runtime(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_))) => self.catch_recognition(local),
            result => result,
        };
        self.unroll_numeric(&entry)?;
        result.map(|()| local)
    }

    fn unroll_numeric(&mut self, entry: &RecursionEntry<'parse>) -> RuntimeResult<'sql, ()> {
        let result = (|| {
            let record = self.record(entry.key)?;
            let InvocationRole::RecursiveEntry { saved, phase: RecursionPhase::Active { head } } = record.role else {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            };
            if !matches!(record.rule, EXPR | COMMON_VALUE) || !self.numeric_top_is(entry) {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }
            let current = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Recursion))?;
            if current != head && (record.rule != COMMON_VALUE || !self.failed_literal_is_current(head, current)?) {
                return Err(RuntimeError::Control(RuntimeProblem::Recursion));
            }
            let retctx = self.unroll_entered_recursion(entry.key, saved)?;
            if let Some(parent) = saved.parent {
                self.append_child_storage(parent, ContextChild::Context(retctx))?;
            }
            Ok(())
        })();
        self.finish(result)
    }

    fn numeric_top_is(&self, entry: &RecursionEntry<'parse>) -> bool {
        use super::PrecedenceFrame;

        matches!(self.precedence.last(), Some(PrecedenceFrame::Recursion { entry: top, .. }) if *top == entry.key)
    }

    fn failed_literal_is_current(&self, head: InvocationKey<'parse>, current: InvocationKey<'parse>) -> RuntimeResult<'sql, bool> {
        let literal = self.record(current)?;
        if !matches!(self.control, ParserControl::Failed)
            || self.record(head)?.class != Some(ContextClass::LiteralExpr)
            || literal.rule != LITERAL || literal.parent != Some(head)
            || literal.invoking != ParserState::State(VALUE_LITERAL.state)
            || literal.start.is_none() || literal.stop.is_some()
        {
            return Ok(false);
        }
        match (literal.class, literal.role, self.state) {
            (None, InvocationRole::OrdinaryTreeAttachmentFailed, ParserState::State(28)) => Ok(true),
            (Some(ContextClass::IntegerLiteral0), InvocationRole::Ordinary, ParserState::State(692)) => Ok(
                self.records.last().is_some_and(|integer| {
                    integer.rule == INTEGER_LITERAL && integer.parent == Some(current)
                        && integer.invoking == ParserState::State(LITERAL_INTEGER.state)
                        && integer.start.is_some()
                        && matches!(integer.role, InvocationRole::OrdinaryTreeAttachmentFailed)
                })
            ),
            _ => Ok(false),
        }
    }
}
