use rtdb_types::AllocationScope;
use super::{DecisionData, DecisionId, ExpectedSymbol, Grammar, GrammarError, IssueKind, Result, RuleData, RuleId, StateData, StateId, StateKind, TokenSet, TokenType, Transition, bytes, issue, vector};
use super::source::{Vocabulary, Words};
use super::token::MAX_TOKEN;

#[derive(Clone, Copy)]
struct Section { offset: usize, count: usize }

#[derive(Clone, Copy)]
pub(super) struct Layout {
    states: Section,
    non_greedy: Section,
    left_recursive: Section,
    rules: Section,
    sets: Section,
    edges: Section,
    decisions: Section,
}

impl Layout {
    pub(super) fn read(words: &[i32]) -> Result<Self> {
        let mut input = Words::at(words, 0);
        if input.next()? != 4 { return Err(issue(IssueKind::Version, 0)); }
        if input.next()? != 1 { return Err(issue(IssueKind::GrammarType, 1)); }
        if input.next()? != i32::from(MAX_TOKEN) { return Err(issue(IssueKind::Token, 2)); }
        let count = input.count()?;
        if count > u32::MAX as usize { return Err(issue(IssueKind::State, input.position - 1)); }
        let states = Section { offset: input.position, count };
        for _ in 0..count {
            let offset = input.position;
            let kind = input.next()?;
            if !matches!(kind, 1..=5 | 7..=12) { return Err(issue(IssueKind::State, offset)); }
            input.next()?;
            if matches!(kind, 3..=5 | 12) { input.next()?; }
        }
        let non_greedy = Self::flat(&mut input, 1)?;
        let left_recursive = Self::flat(&mut input, 1)?;
        let rules = Self::flat(&mut input, 1)?;
        if rules.count > usize::from(u16::MAX) + 1 { return Err(issue(IssueKind::Rule, rules.offset)); }
        if input.next()? != 0 { return Err(issue(IssueKind::GrammarType, input.position - 1)); }
        let count = input.count()?;
        let sets = Section { offset: input.position, count };
        for _ in 0..count {
            let intervals = input.count()?;
            input.next()?;
            input.skip(intervals.checked_mul(2).ok_or_else(|| issue(IssueKind::Overflow, input.position))?)?;
        }
        let edges = Self::flat(&mut input, 6)?;
        let decisions = Self::flat(&mut input, 1)?;
        if decisions.count > usize::from(u16::MAX) + 1 { return Err(issue(IssueKind::Decision, decisions.offset)); }
        input.end()?;
        Ok(Self { states, non_greedy, left_recursive, rules, sets, edges, decisions })
    }

    fn flat(input: &mut Words<'_>, width: usize) -> Result<Section> {
        let count = input.count()?;
        let section = Section { offset: input.position, count };
        input.skip(count.checked_mul(width).ok_or_else(|| issue(IssueKind::Overflow, input.position))?)?;
        Ok(section)
    }

    pub(super) fn rule_count(self) -> usize { self.rules.count }
}

pub(super) struct StateBuilder {
    kind: u8,
    rule: RuleId,
    link: Option<StateId>,
    reverse: Option<StateId>,
    loopback: Option<StateId>,
    left_recursive: bool,
    non_greedy: bool,
    decision: Option<DecisionId>,
    precedence: bool,
    transitions: Vec<Transition>,
    requested_capacity: usize,
    epsilon_only: bool,
}

impl StateBuilder {
    fn decision_state(&self) -> bool { matches!(self.kind, 3..=5 | 10 | 11) }

    fn add(&mut self, transition: Transition, sets: &[TokenSet], number: usize) -> Result<()> {
        if self.transitions.is_empty() { self.epsilon_only = transition.epsilon(); }
        else if self.epsilon_only != transition.epsilon() { self.epsilon_only = false; }
        let duplicate = self.transitions.iter().any(|existing| {
            existing.target() == transition.target()
                && ((existing.epsilon() && transition.epsilon())
                    || existing.label(sets).is_some_and(|label| Some(label) == transition.label(sets)))
        });
        if !duplicate {
            if self.transitions.len() == self.requested_capacity { return Err(issue(IssueKind::Capacity, number)); }
            self.transitions.push(transition);
        }
        Ok(())
    }

    fn finish(self, number: usize) -> Result<StateData> {
        let required = |value: Option<StateId>| value.ok_or_else(|| issue(IssueKind::Link, number));
        let decision = DecisionData { number: self.decision, non_greedy: self.non_greedy };
        let kind = match self.kind {
            1 => StateKind::Basic,
            2 => StateKind::RuleStart { stop: required(self.link)?, left_recursive: self.left_recursive },
            3 => StateKind::BlockStart { end: required(self.link)?, decision },
            4 => StateKind::PlusBlockStart { end: required(self.link)?, loopback: required(self.loopback)?, decision },
            5 => StateKind::StarBlockStart { end: required(self.link)?, decision },
            7 => StateKind::RuleStop,
            8 => StateKind::BlockEnd { start: required(self.reverse)? },
            9 => StateKind::StarLoopback,
            10 => StateKind::StarLoopEntry { loopback: required(self.loopback)?, decision, precedence: self.precedence },
            11 => StateKind::PlusLoopback { decision },
            12 => StateKind::LoopEnd { loopback: required(self.link)? },
            _ => return Err(issue(IssueKind::State, number)),
        };
        Ok(StateData { rule: self.rule, kind, transitions: self.transitions, epsilon_only: self.epsilon_only })
    }
}

struct GrammarBuilder<'a, 'scope> {
    words: &'a [i32],
    layout: Layout,
    states: Vec<StateBuilder>,
    rules: Vec<RuleData>,
    sets: Vec<TokenSet>,
    decisions: Vec<StateId>,
    allocation: &'scope AllocationScope,
}

pub(super) fn load<'scope>(words: &[i32], layout: Layout, vocabulary: Vocabulary, allocation: &'scope AllocationScope) -> Result<Grammar<'scope>> {
    if vocabulary.rules.len() != layout.rules.count { return Err(issue(IssueKind::Rule, layout.rules.offset)); }
    let mut builder = GrammarBuilder::new(words, layout, allocation)?;
    builder.serialized_links()?;
    builder.rules()?;
    builder.sets()?;
    builder.reserve_transitions()?;
    builder.transitions()?;
    builder.return_edges()?;
    builder.reverse_links()?;
    builder.decisions()?;
    builder.mark_precedence()?;
    builder.verify()?;
    builder.finish(vocabulary)
}

impl<'a, 'scope> GrammarBuilder<'a, 'scope> {
    fn new(words: &'a [i32], layout: Layout, allocation: &'scope AllocationScope) -> Result<Self> {
        let mut states = vector(allocation, layout.states.count)?;
        let mut input = Words::at(words, layout.states.offset);
        for _ in 0..layout.states.count {
            let kind = input.next()? as u8;
            let rule = Self::rule_id(input.next()?, layout.rules.count, input.position - 1)?;
            let link = if matches!(kind, 3..=5 | 12) {
                Some(Self::state_id(input.next()?, layout.states.count, input.position - 1)?)
            } else { None };
            states.push(StateBuilder {
                kind, rule, link, reverse: None, loopback: None, left_recursive: false, non_greedy: false,
                decision: None, precedence: false, transitions: Vec::new(), requested_capacity: 0, epsilon_only: false,
            });
        }
        Ok(Self { words, layout, states, rules: Vec::new(), sets: Vec::new(), decisions: Vec::new(), allocation })
    }

    fn state_id(value: i32, count: usize, offset: usize) -> Result<StateId> {
        let number = usize::try_from(value).map_err(|_| issue(IssueKind::State, offset))?;
        if number >= count { return Err(issue(IssueKind::State, offset)); }
        Ok(StateId(number as u32))
    }

    fn rule_id(value: i32, count: usize, offset: usize) -> Result<RuleId> {
        let number = usize::try_from(value).map_err(|_| issue(IssueKind::Rule, offset))?;
        if number >= count { return Err(issue(IssueKind::Rule, offset)); }
        Ok(RuleId(number as u16))
    }

    fn state_word(&self, value: i32, offset: usize) -> Result<StateId> {
        Self::state_id(value, self.states.len(), offset)
    }

    fn state(&self, id: StateId) -> &StateBuilder { &self.states[id.0 as usize] }
    fn state_mut(&mut self, id: StateId) -> &mut StateBuilder { &mut self.states[id.0 as usize] }

    fn serialized_links(&mut self) -> Result<()> {
        for (number, state) in self.states.iter().enumerate() {
            if let Some(link) = state.link {
                let target = self.state(link);
                let valid_kind = if state.kind == 12 { matches!(target.kind, 9 | 11) } else { target.kind == 8 };
                if !valid_kind || state.rule != target.rule { return Err(issue(IssueKind::Link, number)); }
            }
        }
        let mut input = Words::at(self.words, self.layout.non_greedy.offset);
        for _ in 0..self.layout.non_greedy.count {
            let id = self.state_word(input.next()?, input.position - 1)?;
            let state = self.state_mut(id);
            if !state.decision_state() { return Err(issue(IssueKind::Decision, id.0 as usize)); }
            state.non_greedy = true;
        }
        let mut input = Words::at(self.words, self.layout.left_recursive.offset);
        for _ in 0..self.layout.left_recursive.count {
            let id = self.state_word(input.next()?, input.position - 1)?;
            let state = self.state_mut(id);
            if state.kind != 2 { return Err(issue(IssueKind::Rule, id.0 as usize)); }
            state.left_recursive = true;
        }
        Ok(())
    }

    fn rules(&mut self) -> Result<()> {
        let count = self.layout.rules.count;
        let mut stops = vector(self.allocation, count)?;
        stops.resize(count, None);
        for (number, state) in self.states.iter().enumerate() {
            if state.kind == 7 && stops[usize::from(state.rule.0)].replace(StateId(number as u32)).is_some() {
                return Err(issue(IssueKind::Rule, number));
            }
        }
        self.rules = vector(self.allocation, count)?;
        let mut input = Words::at(self.words, self.layout.rules.offset);
        for (number, stop) in stops.into_iter().enumerate() {
            let start = self.state_word(input.next()?, input.position - 1)?;
            let stop = stop.ok_or_else(|| issue(IssueKind::Rule, number))?;
            let state = self.state_mut(start);
            if state.kind != 2 || usize::from(state.rule.0) != number { return Err(issue(IssueKind::Rule, number)); }
            state.link = Some(stop);
            self.rules.push(RuleData { start, stop });
        }
        for (number, state) in self.states.iter().enumerate() {
            if state.kind == 2 && self.rules[usize::from(state.rule.0)].start != StateId(number as u32) {
                return Err(issue(IssueKind::Rule, number));
            }
        }
        Ok(())
    }

    fn sets(&mut self) -> Result<()> {
        self.sets = vector(self.allocation, self.layout.sets.count)?;
        let mut input = Words::at(self.words, self.layout.sets.offset);
        for _ in 0..self.layout.sets.count {
            let intervals = input.count()?;
            let eof = input.next()?;
            let mut set = TokenSet::empty();
            if eof != 0 { set.insert(ExpectedSymbol::Token(TokenType::EOF)); }
            for _ in 0..intervals {
                let offset = input.position;
                let first = input.next()?;
                let last = input.next()?;
                if first > last { continue; }
                let first = TokenType::read(first, offset)?;
                let last = TokenType::read(last, offset + 1)?;
                for number in first.number()..=last.number() {
                    set.insert(ExpectedSymbol::Token(TokenType::read(number, offset)?));
                }
            }
            self.sets.push(set);
        }
        Ok(())
    }

    fn edge(&self, input: &mut Words<'_>) -> Result<(StateId, Transition)> {
        let offset = input.position;
        let source = self.state_word(input.next()?, offset)?;
        let target = self.state_word(input.next()?, offset + 1)?;
        let kind = input.next()?;
        let first = input.next()?;
        let second = input.next()?;
        let third = input.next()?;
        let transition = match kind {
            1 => Transition::Epsilon { target, outermost_precedence_return: None },
            3 => {
                let callee = self.state_word(first, offset + 3)?;
                let rule = Self::rule_id(second, self.rules.len(), offset + 4)?;
                if self.state(callee).kind != 2 || self.state(callee).rule != rule || third < 0 {
                    return Err(issue(IssueKind::Transition, offset));
                }
                Transition::Rule { target: callee, rule, precedence: third, follow: target }
            }
            5 => Transition::Atom { target, token: TokenType::read(if third != 0 { -1 } else { first }, offset + 3)? },
            6 => Transition::Action { target, rule: Self::rule_id(first, self.rules.len(), offset + 3)?, action: second, context_dependent: third != 0 },
            7 => {
                let set = usize::try_from(first).map_err(|_| issue(IssueKind::Transition, offset + 3))?;
                if set >= self.sets.len() { return Err(issue(IssueKind::Transition, offset + 3)); }
                Transition::Set { target, set }
            }
            10 if first >= 0 => Transition::Precedence { target, precedence: first },
            _ => return Err(issue(IssueKind::Transition, offset + 2)),
        };
        Ok((source, transition))
    }

    fn reserve_transitions(&mut self) -> Result<()> {
        let mut input = Words::at(self.words, self.layout.edges.offset);
        for _ in 0..self.layout.edges.count {
            let (source, transition) = self.edge(&mut input)?;
            self.count_transition(source)?;
            if let Transition::Rule { rule, .. } = transition { self.count_transition(self.rules[usize::from(rule.0)].stop)?; }
        }
        let slots = self.states.iter().try_fold(0usize, |sum, state| {
            sum.checked_add(state.requested_capacity).ok_or_else(|| issue(IssueKind::Overflow, sum))
        })?;
        self.allocation.reserve(bytes::<Transition>(slots)?).map_err(GrammarError::Admission)?;
        for state in &mut self.states { state.transitions = Vec::with_capacity(state.requested_capacity); }
        Ok(())
    }

    fn count_transition(&mut self, state: StateId) -> Result<()> {
        let value = &mut self.state_mut(state).requested_capacity;
        *value = value.checked_add(1).ok_or_else(|| issue(IssueKind::Overflow, state.0 as usize))?;
        Ok(())
    }

    fn transitions(&mut self) -> Result<()> {
        let mut input = Words::at(self.words, self.layout.edges.offset);
        for _ in 0..self.layout.edges.count {
            let (source, transition) = self.edge(&mut input)?;
            self.states[source.0 as usize].add(transition, &self.sets, source.0 as usize)?;
        }
        Ok(())
    }

    fn return_edges(&mut self) -> Result<()> {
        for number in 0..self.states.len() {
            let count = self.states[number].transitions.len();
            for index in 0..count {
                if let Transition::Rule { target, rule, precedence, follow } = self.states[number].transitions[index] {
                    let stop = self.rules[usize::from(rule.0)].stop;
                    let marker = (self.state(target).left_recursive && precedence == 0).then_some(rule);
                    self.states[stop.0 as usize].add(Transition::Epsilon { target: follow, outermost_precedence_return: marker }, &self.sets, stop.0 as usize)?;
                }
            }
        }
        Ok(())
    }

    fn reverse_links(&mut self) -> Result<()> {
        for number in 0..self.states.len() {
            let id = StateId(number as u32);
            let kind = self.states[number].kind;
            if matches!(kind, 3..=5) {
                let end = self.states[number].link.ok_or_else(|| issue(IssueKind::Link, number))?;
                if self.state_mut(end).reverse.replace(id).is_some() { return Err(issue(IssueKind::Link, number)); }
            }
            if matches!(kind, 9 | 11) {
                for index in 0..self.states[number].transitions.len() {
                    let target = self.states[number].transitions[index].target();
                    if matches!((kind, self.state(target).kind), (9, 10) | (11, 4)) {
                        if self.state(target).rule != self.states[number].rule { return Err(issue(IssueKind::Link, number)); }
                        if self.state_mut(target).loopback.replace(id).is_some_and(|old| old != id) { return Err(issue(IssueKind::Link, number)); }
                    }
                }
            }
        }
        Ok(())
    }

    fn decisions(&mut self) -> Result<()> {
        self.decisions = vector(self.allocation, self.layout.decisions.count)?;
        let mut input = Words::at(self.words, self.layout.decisions.offset);
        for number in 0..self.layout.decisions.count {
            let id = self.state_word(input.next()?, input.position - 1)?;
            let state = self.state_mut(id);
            if !state.decision_state() || state.decision.replace(DecisionId(number as u16)).is_some() {
                return Err(issue(IssueKind::Decision, number));
            }
            self.decisions.push(id);
        }
        Ok(())
    }

    fn mark_precedence(&mut self) -> Result<()> {
        for number in 0..self.states.len() {
            let state = &self.states[number];
            if state.kind != 10 || !self.state(self.rules[usize::from(state.rule.0)].start).left_recursive { continue; }
            let target = state.transitions.last().ok_or_else(|| issue(IssueKind::Invariant, number))?.target();
            let target = self.state(target);
            if target.kind == 12 && target.epsilon_only {
                let exit = target.transitions.first().ok_or_else(|| issue(IssueKind::Invariant, number))?.target();
                if self.state(exit).kind == 7 { self.states[number].precedence = true; }
            }
        }
        Ok(())
    }

    fn verify(&self) -> Result<()> {
        for (number, state) in self.states.iter().enumerate() {
            let count = state.transitions.len();
            if !state.epsilon_only && count > 1 { return Err(issue(IssueKind::Invariant, number)); }
            let linked = match state.kind {
                2 | 3 | 5 | 12 => state.link.is_some(),
                4 => state.link.is_some() && state.loopback.is_some(),
                8 => state.reverse.is_some(),
                10 => state.loopback.is_some(),
                _ => true,
            };
            if !linked { return Err(issue(IssueKind::Link, number)); }
            if state.kind == 10 {
                if count != 2 { return Err(issue(IssueKind::Invariant, number)); }
                let first = self.state(state.transitions[0].target()).kind;
                let second = self.state(state.transitions[1].target()).kind;
                if !matches!((first, second, state.non_greedy), (5, 12, false) | (12, 5, true)) {
                    return Err(issue(IssueKind::Invariant, number));
                }
            }
            if state.kind == 9 && (count != 1 || self.state(state.transitions[0].target()).kind != 10) {
                return Err(issue(IssueKind::Invariant, number));
            }
            if count > 1 && (if state.decision_state() { state.decision.is_none() } else { state.kind != 7 }) {
                return Err(issue(IssueKind::Invariant, number));
            }
        }
        Ok(())
    }

    fn finish(self, vocabulary: Vocabulary) -> Result<Grammar<'scope>> {
        let mut states = vector(self.allocation, self.states.len())?;
        for (number, state) in self.states.into_iter().enumerate() { states.push(state.finish(number)?); }
        Ok(Grammar { states, rules: self.rules, sets: self.sets, decisions: self.decisions, vocabulary, allocation: self.allocation })
    }
}
