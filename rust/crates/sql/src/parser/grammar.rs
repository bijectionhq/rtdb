mod deserialize;
mod source;
mod token;
#[cfg(test)]
mod tests;

use std::fmt;
use rtdb_types::{AllocationScope, Anomaly};
pub(crate) use token::{ExpectedSymbol, TokenSet, TokenType};

type Result<T> = std::result::Result<T, GrammarError>;

#[derive(Debug)]
pub(crate) enum GrammarError {
    Admission(Anomaly),
    Artifact(ArtifactIssue),
    ForeignOwner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ArtifactIssue {
    pub(crate) kind: IssueKind,
    pub(crate) offset: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IssueKind {
    Source, Integer, Extent, Version, GrammarType, Token, State, Rule, Decision,
    Transition, Link, Invariant, Capacity, Overflow,
}

fn issue(kind: IssueKind, offset: usize) -> GrammarError {
    GrammarError::Artifact(ArtifactIssue { kind, offset })
}

fn bytes<T>(count: usize) -> Result<usize> {
    count.checked_mul(std::mem::size_of::<T>()).filter(|bytes| *bytes <= isize::MAX as usize)
        .ok_or_else(|| issue(IssueKind::Overflow, count))
}

fn vector<T>(allocation: &AllocationScope, count: usize) -> Result<Vec<T>> {
    allocation.reserve(bytes::<T>(count)?).map_err(GrammarError::Admission)?;
    Ok(Vec::with_capacity(count))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StateId(u32);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RuleId(u16);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DecisionId(u16);

pub(crate) struct Grammar<'scope> {
    states: Vec<StateData>,
    rules: Vec<RuleData>,
    decisions: Vec<StateId>,
    sets: Vec<TokenSet>,
    vocabulary: source::Vocabulary,
    allocation: &'scope AllocationScope,
}

impl Drop for Grammar<'_> {
    fn drop(&mut self) { let _ = self.allocation; }
}

impl<'scope> Grammar<'scope> {
    pub(crate) fn load(allocation: &'scope AllocationScope) -> Result<Self> {
        let source = source::Source::read(source::PINNED)?;
        let words = source.words(allocation)?;
        let layout = deserialize::Layout::read(&words)?;
        let vocabulary = source.vocabulary(allocation, layout.rule_count())?;
        deserialize::load(&words, layout, vocabulary, allocation)
    }

    pub(crate) fn state(&self, number: u32) -> Result<State<'_, 'scope>> {
        self.states.get(number as usize).ok_or_else(|| issue(IssueKind::State, number as usize))?;
        Ok(self.view(StateId(number)))
    }

    pub(crate) fn rule(&self, number: u16) -> Result<Rule<'_, 'scope>> {
        self.rules.get(usize::from(number)).ok_or_else(|| issue(IssueKind::Rule, usize::from(number)))?;
        Ok(Rule { grammar: self, id: RuleId(number) })
    }

    pub(crate) fn decision(&self, number: u16) -> Result<Decision<'_, 'scope>> {
        self.decisions.get(usize::from(number)).ok_or_else(|| issue(IssueKind::Decision, usize::from(number)))?;
        Ok(Decision { grammar: self, id: DecisionId(number) })
    }

    pub(crate) fn token_type(&self, kind: crate::lexer::Kind) -> Result<TokenType> {
        self.vocabulary.token_type(kind)
    }

    pub(crate) fn display_name(&self, token: TokenType) -> &'static str {
        self.vocabulary.display_name(token)
    }

    fn view(&self, id: StateId) -> State<'_, 'scope> { State { grammar: self, id } }
}

struct StateData {
    rule: RuleId,
    kind: StateKind,
    transitions: Vec<Transition>,
    epsilon_only: bool,
}

struct RuleData { start: StateId, stop: StateId }

#[derive(Clone, Copy)]
struct DecisionData { number: Option<DecisionId>, non_greedy: bool }

enum StateKind {
    Basic,
    RuleStart { stop: StateId, left_recursive: bool },
    BlockStart { end: StateId, decision: DecisionData },
    PlusBlockStart { end: StateId, loopback: StateId, decision: DecisionData },
    StarBlockStart { end: StateId, decision: DecisionData },
    RuleStop,
    BlockEnd { start: StateId },
    StarLoopback,
    StarLoopEntry { loopback: StateId, decision: DecisionData, precedence: bool },
    PlusLoopback { decision: DecisionData },
    LoopEnd { loopback: StateId },
}

#[derive(Clone, Copy, Debug)]
enum Transition {
    Epsilon { target: StateId, outermost_precedence_return: Option<RuleId> },
    Rule { target: StateId, rule: RuleId, precedence: i32, follow: StateId },
    Atom { target: StateId, token: TokenType },
    Action { target: StateId, rule: RuleId, action: i32, context_dependent: bool },
    Set { target: StateId, set: usize },
    Precedence { target: StateId, precedence: i32 },
}

impl Transition {
    fn target(self) -> StateId {
        match self {
            Self::Epsilon { target, .. } | Self::Rule { target, .. } | Self::Atom { target, .. }
                | Self::Action { target, .. } | Self::Set { target, .. } | Self::Precedence { target, .. } => target,
        }
    }

    fn epsilon(self) -> bool { !matches!(self, Self::Atom { .. } | Self::Set { .. }) }

    fn label(self, sets: &[TokenSet]) -> Option<TokenSet> {
        match self {
            Self::Atom { token, .. } => Some(TokenSet::singleton(ExpectedSymbol::Token(token))),
            Self::Set { set, .. } => Some(sets[set]),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct State<'g, 'scope> { grammar: &'g Grammar<'scope>, id: StateId }
#[derive(Clone, Copy)]
pub(crate) struct Rule<'g, 'scope> { grammar: &'g Grammar<'scope>, id: RuleId }
#[derive(Clone, Copy)]
pub(crate) struct Decision<'g, 'scope> { grammar: &'g Grammar<'scope>, id: DecisionId }

impl fmt::Debug for State<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("State").field(&self.id.0).finish() }
}
impl fmt::Debug for Rule<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("Rule").field(&self.id.0).finish() }
}
impl fmt::Debug for Decision<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.debug_tuple("Decision").field(&self.id.0).finish() }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DecisionView<'g, 'scope> {
    pub(crate) decision: Option<Decision<'g, 'scope>>,
    pub(crate) non_greedy: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum StateKindView<'g, 'scope> {
    Basic,
    RuleStart { stop: State<'g, 'scope>, left_recursive: bool },
    BlockStart { end: State<'g, 'scope>, decision: DecisionView<'g, 'scope> },
    PlusBlockStart { end: State<'g, 'scope>, loopback: State<'g, 'scope>, decision: DecisionView<'g, 'scope> },
    StarBlockStart { end: State<'g, 'scope>, decision: DecisionView<'g, 'scope> },
    RuleStop,
    BlockEnd { start: State<'g, 'scope> },
    StarLoopback,
    StarLoopEntry { loopback: State<'g, 'scope>, decision: DecisionView<'g, 'scope>, precedence: bool },
    PlusLoopback { decision: DecisionView<'g, 'scope> },
    LoopEnd { loopback: State<'g, 'scope> },
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum TransitionView<'g, 'scope> {
    Epsilon { target: State<'g, 'scope>, outermost_precedence_return: Option<Rule<'g, 'scope>> },
    Rule { target: State<'g, 'scope>, rule: Rule<'g, 'scope>, precedence: i32, follow: State<'g, 'scope> },
    Atom { target: State<'g, 'scope>, token: TokenType },
    Action { target: State<'g, 'scope>, rule: Rule<'g, 'scope>, action: i32, context_dependent: bool },
    Set { target: State<'g, 'scope>, tokens: &'g TokenSet },
    Precedence { target: State<'g, 'scope>, precedence: i32 },
}

impl<'g, 'scope> State<'g, 'scope> {
    pub(crate) fn number(self) -> u32 { self.id.0 }
    pub(crate) fn rule(self) -> Rule<'g, 'scope> {
        Rule { grammar: self.grammar, id: self.grammar.states[self.id.0 as usize].rule }
    }
    pub(crate) fn epsilon_only(self) -> bool { self.grammar.states[self.id.0 as usize].epsilon_only }

    pub(crate) fn kind(self) -> StateKindView<'g, 'scope> {
        let state = |id| self.grammar.view(id);
        let decision = |value: DecisionData| DecisionView {
            decision: value.number.map(|id| Decision { grammar: self.grammar, id }), non_greedy: value.non_greedy,
        };
        match self.grammar.states[self.id.0 as usize].kind {
            StateKind::Basic => StateKindView::Basic,
            StateKind::RuleStart { stop, left_recursive } => StateKindView::RuleStart { stop: state(stop), left_recursive },
            StateKind::BlockStart { end, decision: d } => StateKindView::BlockStart { end: state(end), decision: decision(d) },
            StateKind::PlusBlockStart { end, loopback, decision: d } => StateKindView::PlusBlockStart { end: state(end), loopback: state(loopback), decision: decision(d) },
            StateKind::StarBlockStart { end, decision: d } => StateKindView::StarBlockStart { end: state(end), decision: decision(d) },
            StateKind::RuleStop => StateKindView::RuleStop,
            StateKind::BlockEnd { start } => StateKindView::BlockEnd { start: state(start) },
            StateKind::StarLoopback => StateKindView::StarLoopback,
            StateKind::StarLoopEntry { loopback, decision: d, precedence } => StateKindView::StarLoopEntry { loopback: state(loopback), decision: decision(d), precedence },
            StateKind::PlusLoopback { decision: d } => StateKindView::PlusLoopback { decision: decision(d) },
            StateKind::LoopEnd { loopback } => StateKindView::LoopEnd { loopback: state(loopback) },
        }
    }

    pub(crate) fn transitions(self) -> impl ExactSizeIterator<Item = TransitionView<'g, 'scope>> {
        self.grammar.states[self.id.0 as usize].transitions.iter().map(move |transition| {
            let state = |id| self.grammar.view(id);
            let rule = |id| Rule { grammar: self.grammar, id };
            match *transition {
                Transition::Epsilon { target, outermost_precedence_return } => TransitionView::Epsilon { target: state(target), outermost_precedence_return: outermost_precedence_return.map(rule) },
                Transition::Rule { target, rule: r, precedence, follow } => TransitionView::Rule { target: state(target), rule: rule(r), precedence, follow: state(follow) },
                Transition::Atom { target, token } => TransitionView::Atom { target: state(target), token },
                Transition::Action { target, rule: r, action, context_dependent } => TransitionView::Action { target: state(target), rule: rule(r), action, context_dependent },
                Transition::Set { target, set } => TransitionView::Set { target: state(target), tokens: &self.grammar.sets[set] },
                Transition::Precedence { target, precedence } => TransitionView::Precedence { target: state(target), precedence },
            }
        })
    }
}

impl<'g, 'scope> Rule<'g, 'scope> {
    pub(crate) fn number(self) -> u16 { self.id.0 }
    pub(crate) fn name(self) -> &'static str { self.grammar.vocabulary.rules[usize::from(self.id.0)] }
    pub(crate) fn start(self) -> State<'g, 'scope> { self.grammar.view(self.grammar.rules[usize::from(self.id.0)].start) }
    pub(crate) fn stop(self) -> State<'g, 'scope> { self.grammar.view(self.grammar.rules[usize::from(self.id.0)].stop) }
}

impl<'g, 'scope> Decision<'g, 'scope> {
    pub(crate) fn number(self) -> u16 { self.id.0 }
    pub(crate) fn state(self) -> State<'g, 'scope> { self.grammar.view(self.grammar.decisions[usize::from(self.id.0)]) }
}
