use crate::parser::grammar::{Grammar, StateKindView, TokenSet, TokenType, TransitionView};
use super::{ParserSession, RuntimeError, RuntimeProblem, RuntimeResult};

#[derive(Clone, Copy)]
pub(super) struct CallAction {
    pub(super) state: u32,
    pub(super) rule: u16,
    pub(super) called: u16,
    pub(super) start: u32,
    pub(super) follow: u32,
    pub(super) precedence: i32,
}

impl CallAction {
    pub(super) fn agrees(self, grammar: &Grammar<'_>) -> bool {
        grammar.state(self.state).is_ok_and(|state| {
            state.rule().number() == self.rule && matches!(state.kind(), StateKindView::Basic)
                && state.transitions().len() == 1
                && matches!(state.transitions().next(), Some(TransitionView::Rule { target, rule, precedence, follow })
                    if target.number() == self.start && rule.number() == self.called
                        && precedence == self.precedence && follow.number() == self.follow)
        })
    }
}

#[derive(Clone, Copy)]
pub(super) enum SyncShape {
    RequiredBlock,
    NullableBlock,
    LoopEntry,
    Loopback,
    RequiredStarBlock,
    NullablePrecedenceEntry,
    NullableLoopback,
}

impl SyncShape {
    pub(super) fn agrees(self, kind: StateKindView<'_, '_>) -> bool {
        matches!((self, kind),
            (Self::RequiredBlock | Self::NullableBlock, StateKindView::BlockStart { .. })
            | (Self::LoopEntry, StateKindView::StarLoopEntry { precedence: false, .. })
            | (Self::Loopback | Self::NullableLoopback, StateKindView::StarLoopback)
            | (Self::RequiredStarBlock, StateKindView::StarBlockStart { .. })
            | (Self::NullablePrecedenceEntry, StateKindView::StarLoopEntry { precedence: true, .. }))
    }

    pub(super) fn nullable(self) -> bool {
        matches!(self, Self::NullableBlock | Self::NullablePrecedenceEntry | Self::NullableLoopback)
    }
}

#[derive(Clone, Copy)]
pub(super) struct SyncAction {
    pub(super) state: u32,
    pub(super) rule: u16,
    pub(super) shape: SyncShape,
}

#[derive(Clone, Copy)]
pub(super) struct PredictionAction {
    pub(super) sync: SyncAction,
    pub(super) decision_state: u32,
    pub(super) decision: u16,
    pub(super) alternatives: usize,
}

impl PredictionAction {
    pub(super) fn agrees(self, grammar: &Grammar<'_>) -> bool {
        let Ok(sync) = grammar.state(self.sync.state) else { return false; };
        let Ok(decision) = grammar.decision(self.decision) else { return false; };
        let selected = decision.state();
        let linked = sync.number() == selected.number() || matches!(
            (sync.kind(), sync.transitions().next()),
            (StateKindView::StarLoopback, Some(TransitionView::Epsilon { target, .. }))
                if sync.transitions().len() == 1 && target.number() == selected.number());
        linked && self.sync.shape.agrees(sync.kind()) && selected.rule().number() == self.sync.rule
            && sync.rule().number() == self.sync.rule && selected.number() == self.decision_state
            && selected.transitions().len() == self.alternatives
    }
}

#[derive(Clone, Copy)]
pub(super) enum TokenAction {
    Atom(i32),
    Set(&'static [i32]),
}

#[derive(Clone, Copy)]
pub(super) struct TerminalAction {
    pub(super) state: u32,
    pub(super) rule: u16,
    pub(super) target: u32,
    pub(super) tokens: TokenAction,
}

#[derive(Clone, Copy)]
pub(super) enum TerminalExpectation {
    Atom(TokenType),
    Set(TokenSet),
}

impl TerminalAction {
    pub(super) fn read(self, grammar: &Grammar<'_>) -> Option<TerminalExpectation> {
        use crate::parser::grammar::ExpectedSymbol;

        let state = grammar.state(self.state).ok()?;
        if state.rule().number() != self.rule || !matches!(state.kind(), StateKindView::Basic) {
            return None;
        }
        let mut edges = state.transitions();
        let (edge, remaining) = (edges.next()?, edges.len());
        match (self.tokens, edge, remaining) {
            (TokenAction::Atom(expected), TransitionView::Atom { token, target }, 0)
                if token.number() == expected && target.number() == self.target =>
                Some(TerminalExpectation::Atom(token)),
            (TokenAction::Set(expected), TransitionView::Set { tokens, target }, 0)
                if target.number() == self.target && tokens.iter().map(|symbol| match symbol {
                    ExpectedSymbol::Token(token) => Some(token.number()),
                    ExpectedSymbol::Epsilon => None,
                }).eq(expected.iter().copied().map(Some)) =>
                Some(TerminalExpectation::Set(*tokens)),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct PredicateAction {
    pub(super) state: u32,
    pub(super) rule: u16,
    pub(super) target: u32,
    pub(super) minimum: i32,
}

impl PredicateAction {
    pub(super) fn agrees(self, grammar: &Grammar<'_>) -> bool {
        grammar.state(self.state).is_ok_and(|state| {
            state.rule().number() == self.rule && matches!(state.kind(), StateKindView::Basic)
                && state.transitions().len() == 1
                && matches!(state.transitions().next(), Some(TransitionView::Precedence { target, precedence })
                    if target.number() == self.target && precedence == self.minimum)
        })
    }
}

impl<'parse, 'sql> ParserSession<'parse, 'sql, '_, '_> {
    pub(super) fn generated_action_state(&mut self, state: u32, rule: u16) -> RuntimeResult<'sql, ()> {
        self.set_state(state)?;
        let result = self.current.ok_or(RuntimeError::Control(RuntimeProblem::Invocation))
            .and_then(|key| self.record(key))
            .and_then(|record| {
                if record.rule == rule && record.start.is_some() {
                    Ok(())
                } else {
                    Err(RuntimeError::Control(RuntimeProblem::PredictionSite))
                }
            });
        self.finish(result)
    }

    pub(super) fn generated_call(&mut self, call: CallAction) -> RuntimeResult<'sql, ()> {
        self.generated_action_state(call.state, call.rule)?;
        let agrees = call.agrees(&self.environment.storage.grammar_view());
        self.finish(if agrees { Ok(()) } else { Err(RuntimeError::Control(RuntimeProblem::PredictionSite)) })
    }

    pub(super) fn generated_predicate(&mut self, action: PredicateAction) -> RuntimeResult<'sql, ()> {
        self.generated_action_state(action.state, action.rule)?;
        let agrees = action.agrees(&self.environment.storage.grammar_view());
        self.finish(if agrees && self.precedence() <= action.minimum {
            Ok(())
        } else {
            Err(RuntimeError::Control(RuntimeProblem::PredictionSite))
        })
    }
}
