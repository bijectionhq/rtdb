#[cfg(test)]
#[path = "look_tests.rs"]
mod tests;

use rtdb_types::AllocationScope;
use super::{ExpectedSymbol, Grammar, GrammarError, IssueKind, Result, RuleId, State, StateId, StateKind, TokenSet, TokenType, Transition, issue, vector};

const INITIAL_CAPACITY: usize = 16;
const RULE_COUNT: usize = 176;

impl<'g, 'grammar> State<'g, 'grammar> {
    pub(crate) fn next_tokens(self, allocation: &AllocationScope) -> Result<TokenSet> {
        LookWorkspace::new(self.grammar, allocation)?.look(self.id, ReturnContext::LocalEnd)
    }

    pub(crate) fn next_tokens_with_context(self, invocations: impl Iterator<Item = State<'g, 'grammar>>, allocation: &AllocationScope) -> Result<TokenSet> {
        let mut workspace = LookWorkspace::new(self.grammar, allocation)?;
        let context = workspace.caller_context(invocations)?;
        workspace.look(self.id, context)
    }

    pub(crate) fn expected_tokens(self, mut invocations: impl Iterator<Item = State<'g, 'grammar>>, allocation: &AllocationScope) -> Result<TokenSet> {
        let mut workspace = LookWorkspace::new(self.grammar, allocation)?;
        let mut following = workspace.look(self.id, ReturnContext::LocalEnd)?;
        if !following.contains(ExpectedSymbol::Epsilon) { return Ok(following); }
        let mut expected = following;
        expected.remove_epsilon();
        while following.contains(ExpectedSymbol::Epsilon) {
            let Some(invocation) = invocations.next() else { break; };
            let follow = workspace.follow(invocation)?;
            workspace.clear();
            following = workspace.look(follow, ReturnContext::LocalEnd)?;
            expected.union(&following);
            expected.remove_epsilon();
        }
        if following.contains(ExpectedSymbol::Epsilon) { expected.insert(ExpectedSymbol::Token(TokenType::EOF)); }
        Ok(expected)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReturnContext { LocalEnd, FullEnd, Node(usize) }

#[derive(Clone, Copy)]
struct ContextNode { parent: ReturnContext, follow: StateId, hash: u64 }

#[derive(Clone, Copy)]
enum Frame {
    Enter { state: StateId, context: ReturnContext },
    Transitions { state: StateId, context: ReturnContext, next: usize },
    ClearCalledRule(RuleId),
    RestoreStoppedRule { rule: RuleId, was_set: bool },
}

#[derive(Clone, Copy)]
struct Visited { state: StateId, context: ReturnContext, hash: u64 }

struct LookWorkspace<'g, 'grammar, 'work> {
    grammar: &'g Grammar<'grammar>,
    allocation: &'work AllocationScope,
    frames: Vec<Frame>,
    contexts: Vec<ContextNode>,
    visited: Vec<Option<Visited>>,
    visited_len: usize,
    invocations: Vec<State<'g, 'grammar>>,
    called_rules: [u64; 3],
}

impl Drop for LookWorkspace<'_, '_, '_> {
    fn drop(&mut self) { let _ = self.allocation; }
}

impl<'g, 'grammar, 'work> LookWorkspace<'g, 'grammar, 'work> {
    fn new(grammar: &'g Grammar<'grammar>, allocation: &'work AllocationScope) -> Result<Self> {
        if grammar.rules.len() > RULE_COUNT { return Err(issue(IssueKind::Rule, grammar.rules.len())); }
        Ok(Self {
            grammar, allocation, frames: Vec::new(), contexts: Vec::new(), visited: Vec::new(),
            visited_len: 0, invocations: Vec::new(), called_rules: [0; 3],
        })
    }

    fn follow(&self, invocation: State<'_, '_>) -> Result<StateId> {
        if !std::ptr::eq(self.grammar, invocation.grammar) { return Err(GrammarError::ForeignOwner); }
        match self.grammar.states[invocation.id.0 as usize].transitions.first() {
            Some(Transition::Rule { follow, .. }) => Ok(*follow),
            _ => Err(issue(IssueKind::Transition, invocation.id.0 as usize)),
        }
    }

    fn caller_context(&mut self, invocations: impl Iterator<Item = State<'g, 'grammar>>) -> Result<ReturnContext> {
        for invocation in invocations { push(&mut self.invocations, invocation, self.allocation)?; }
        let mut context = ReturnContext::FullEnd;
        while let Some(invocation) = self.invocations.pop() {
            let follow = self.follow(invocation)?;
            context = self.context(context, follow)?;
        }
        Ok(context)
    }

    fn context(&mut self, parent: ReturnContext, follow: StateId) -> Result<ReturnContext> {
        let hash = self.context_hash(parent).wrapping_mul(31).wrapping_add(u64::from(follow.0)).wrapping_add(1);
        let index = self.contexts.len();
        push(&mut self.contexts, ContextNode { parent, follow, hash }, self.allocation)?;
        Ok(ReturnContext::Node(index))
    }

    fn context_hash(&self, context: ReturnContext) -> u64 {
        match context {
            ReturnContext::LocalEnd => 0,
            ReturnContext::FullEnd => 1,
            ReturnContext::Node(index) => self.contexts[index].hash,
        }
    }

    fn same_context(&self, mut left: ReturnContext, mut right: ReturnContext) -> bool {
        loop {
            if left == right { return true; }
            let (ReturnContext::Node(left_index), ReturnContext::Node(right_index)) = (left, right) else { return false; };
            let left_node = self.contexts[left_index];
            let right_node = self.contexts[right_index];
            if left_node.hash != right_node.hash || left_node.follow != right_node.follow || left_node.parent == ReturnContext::LocalEnd {
                return false;
            }
            left = left_node.parent;
            right = right_node.parent;
        }
    }

    fn visit(&mut self, state: StateId, context: ReturnContext) -> Result<bool> {
        let hash = self.context_hash(context).wrapping_mul(0x9e3779b185ebca87).rotate_left(17) ^ u64::from(state.0);
        let visit = Visited { state, context, hash };
        if !self.visited.is_empty() {
            let mask = self.visited.len() - 1;
            let mut slot = hash as usize & mask;
            while let Some(existing) = self.visited[slot] {
                if existing.hash == hash && existing.state == state && self.same_context(context, existing.context) { return Ok(false); }
                slot = (slot + 1) & mask;
            }
        }
        if self.visited_len >= self.visited.len() / 2 { self.grow_visited()?; }
        Self::insert_visit(&mut self.visited, visit);
        self.visited_len += 1;
        Ok(true)
    }

    fn grow_visited(&mut self) -> Result<()> {
        let count = next_capacity(self.visited.len())?;
        let mut replacement = vector(self.allocation, count)?;
        replacement.resize(count, None);
        for visit in self.visited.iter().flatten() { Self::insert_visit(&mut replacement, *visit); }
        self.visited = replacement;
        Ok(())
    }

    fn insert_visit(slots: &mut [Option<Visited>], visit: Visited) {
        let mask = slots.len() - 1;
        let mut slot = visit.hash as usize & mask;
        while slots[slot].is_some() { slot = (slot + 1) & mask; }
        slots[slot] = Some(visit);
    }

    fn called(&self, rule: RuleId) -> bool {
        let index = usize::from(rule.0);
        self.called_rules[index / 64] & (1u64 << (index % 64)) != 0
    }

    fn set_called(&mut self, rule: RuleId, value: bool) {
        let index = usize::from(rule.0);
        let bit = 1u64 << (index % 64);
        if value { self.called_rules[index / 64] |= bit; }
        else { self.called_rules[index / 64] &= !bit; }
    }

    fn push(&mut self, frame: Frame) -> Result<()> { push(&mut self.frames, frame, self.allocation) }

    fn clear(&mut self) {
        self.frames.clear();
        self.contexts.clear();
        self.visited.fill(None);
        self.visited_len = 0;
        self.called_rules = [0; 3];
    }

    fn look(&mut self, state: StateId, context: ReturnContext) -> Result<TokenSet> {
        let mut result = TokenSet::empty();
        self.push(Frame::Enter { state, context })?;
        while let Some(frame) = self.frames.pop() {
            match frame {
                Frame::Enter { state, context } => {
                    if !self.visit(state, context)? { continue; }
                    let data = &self.grammar.states[state.0 as usize];
                    if matches!(data.kind, StateKind::RuleStop) {
                        match context {
                            ReturnContext::LocalEnd => result.insert(ExpectedSymbol::Epsilon),
                            ReturnContext::FullEnd => result.insert(ExpectedSymbol::Token(TokenType::EOF)),
                            ReturnContext::Node(index) => {
                                let node = self.contexts[index];
                                let rule = data.rule;
                                let was_set = self.called(rule);
                                self.push(Frame::RestoreStoppedRule { rule, was_set })?;
                                self.set_called(rule, false);
                                self.push(Frame::Enter { state: node.follow, context: node.parent })?;
                            }
                        }
                    } else { self.push(Frame::Transitions { state, context, next: 0 })?; }
                }
                Frame::Transitions { state, context, next } => {
                    let transitions = &self.grammar.states[state.0 as usize].transitions;
                    let Some(transition) = transitions.get(next).copied() else { continue; };
                    if next + 1 < transitions.len() { self.push(Frame::Transitions { state, context, next: next + 1 })?; }
                    match transition {
                        Transition::Rule { target, rule, follow, .. } => {
                            if self.called(rule) { continue; }
                            let context = self.context(context, follow)?;
                            self.push(Frame::ClearCalledRule(rule))?;
                            self.set_called(rule, true);
                            self.push(Frame::Enter { state: target, context })?;
                        }
                        Transition::Epsilon { target, .. } | Transition::Action { target, .. } | Transition::Precedence { target, .. } => {
                            self.push(Frame::Enter { state: target, context })?;
                        }
                        Transition::Atom { token, .. } => result.insert(ExpectedSymbol::Token(token)),
                        Transition::Set { set, .. } => result.union(&self.grammar.sets[set]),
                    }
                }
                Frame::ClearCalledRule(rule) => self.set_called(rule, false),
                Frame::RestoreStoppedRule { rule, was_set } => { if was_set { self.set_called(rule, true); } }
            }
        }
        Ok(result)
    }
}

fn next_capacity(capacity: usize) -> Result<usize> {
    if capacity == 0 { Ok(INITIAL_CAPACITY) }
    else { capacity.checked_mul(2).ok_or_else(|| issue(IssueKind::Overflow, capacity)) }
}

fn push<T: Copy>(values: &mut Vec<T>, value: T, allocation: &AllocationScope) -> Result<()> {
    if values.len() == values.capacity() {
        let mut replacement = vector(allocation, next_capacity(values.capacity())?)?;
        replacement.extend_from_slice(values);
        *values = replacement;
    }
    values.push(value);
    Ok(())
}
