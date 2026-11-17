use std::marker::PhantomData;
use rtdb_types::AllocationScope;
use crate::parser::grammar::{Decision, ExpectedSymbol, State, StateKindView, TokenType, TransitionView};
use super::{ConfigChange, ConfigKey, ConfigSetKey, Configurations, ContextKey, MergeRound, PredictionContexts, PredictionError, Problem, Result, Semantic, SetMode, SourceAssertionSite, push, vector};
use super::dfa::{InternedStateKey, PredictionDfas};
use super::storage::{Retained, StoreMemory};
#[cfg(test)]
#[path = "closure_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum LoopEntryOption { Enabled, Disabled }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SourceAssertions { Enabled, Disabled }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ClosureOptions { pub(super) pruning: LoopEntryOption, pub(super) assertions: SourceAssertions }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MergePresence { Absent, Present }

#[derive(Clone, Copy, Debug)]
pub(super) struct LocalSet<'round, 'cfg> {
    key: ConfigSetKey<'cfg>,
    brand: PhantomData<fn(&'round mut ()) -> &'round mut ()>,
}
impl<'round, 'cfg> LocalSet<'round, 'cfg> {
    fn new(key: ConfigSetKey<'cfg>) -> Self { Self { key, brand: PhantomData } }
    pub(super) fn key(self) -> ConfigSetKey<'cfg> { self.key }
}
pub(super) struct PrecedenceStart<'round, 'cfg> { set: LocalSet<'round, 'cfg> }
impl<'cfg> PrecedenceStart<'_, 'cfg> { pub(super) fn prefilter(&self) -> ConfigSetKey<'cfg> { self.set.key } }
pub(super) enum StartSet<'round, 'cfg> { Ordinary(LocalSet<'round, 'cfg>), Precedence(PrecedenceStart<'round, 'cfg>) }

#[derive(Clone, Copy)]
enum Frame<'cfg> {
    CheckStop { config: ConfigKey<'cfg>, collect: bool, depth: i32, eof: bool },
    PopParents { config: ConfigKey<'cfg>, next: usize, collect: bool, depth: i32, eof: bool },
    Transitions { config: ConfigKey<'cfg>, next: usize, collect: bool, depth: i32, eof: bool },
}
#[derive(Clone, Copy)]
struct BusyEntry<'cfg> { hash: u32, config: ConfigKey<'cfg> }
struct BusySet<'cfg> { slots: Vec<Option<BusyEntry<'cfg>>>, len: usize }
impl<'cfg> BusySet<'cfg> {
    fn new() -> Self { Self { slots: Vec::new(), len: 0 } }
    fn clear(&mut self) { self.slots.fill(None); self.len = 0; }
    fn grow(&mut self, allocation: &AllocationScope) -> Result<()> {
        let capacity = if self.slots.is_empty() { 32 } else { self.slots.len().checked_mul(2).ok_or_else(super::capacity_error)? };
        let mut slots = vector(allocation, capacity)?;
        slots.resize(capacity, None);
        for entry in self.slots.iter().flatten().copied() {
            let mut index = entry.hash as usize & (capacity - 1);
            while slots[index].is_some() { index = (index + 1) & (capacity - 1); }
            slots[index] = Some(entry);
        }
        self.slots = slots;
        Ok(())
    }
    fn add<'id, 'g, 'scope, M: StoreMemory>(&mut self, incoming: ConfigKey<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>, allocation: &AllocationScope) -> Result<bool> {
        let hash = spread(configs.config_hash(incoming, contexts)?);
        if !self.slots.is_empty() {
            let mut index = hash as usize & (self.slots.len() - 1);
            while let Some(entry) = self.slots[index] {
                if entry.hash == hash && configs.equivalent_config_for_probe(incoming, entry.config, contexts)? { return Ok(false); }
                index = (index + 1) & (self.slots.len() - 1);
            }
        }
        if self.len >= self.slots.len() / 2 { self.grow(allocation)?; }
        let mut index = hash as usize & (self.slots.len() - 1);
        while self.slots[index].is_some() { index = (index + 1) & (self.slots.len() - 1); }
        self.slots[index] = Some(BusyEntry { hash, config: incoming });
        self.len += 1;
        Ok(true)
    }
}
fn spread(hash: i32) -> u32 { let hash = hash as u32; hash ^ (hash >> 16) }

#[derive(Clone, Copy)]
struct Alt1Entry<'id> { state: u32, context: ContextKey<'id> }
struct Alt1Map<'id> { slots: Vec<Option<Alt1Entry<'id>>>, len: usize }
impl<'id> Alt1Map<'id> {
    fn new() -> Self { Self { slots: Vec::new(), len: 0 } }
    fn clear(&mut self) { self.slots.fill(None); self.len = 0; }
    fn slot(&self, state: u32) -> usize {
        let mut index = spread(state as i32) as usize & (self.slots.len() - 1);
        while let Some(entry) = self.slots[index] {
            if entry.state == state { break; }
            index = (index + 1) & (self.slots.len() - 1);
        }
        index
    }
    fn get(&self, state: u32) -> Option<ContextKey<'id>> {
        if self.slots.is_empty() { None } else { self.slots[self.slot(state)].map(|entry| entry.context) }
    }
    fn put(&mut self, state: u32, context: ContextKey<'id>, allocation: &AllocationScope) -> Result<()> {
        if !self.slots.is_empty() {
            let index = self.slot(state);
            if self.slots[index].is_some() { self.slots[index] = Some(Alt1Entry { state, context }); return Ok(()); }
        }
        if self.len >= self.slots.len() / 2 {
            let capacity = if self.slots.is_empty() { 32 } else { self.slots.len().checked_mul(2).ok_or_else(super::capacity_error)? };
            let mut slots = vector(allocation, capacity)?;
            slots.resize(capacity, None);
            let mut replacement = Self { slots, len: self.len };
            for entry in self.slots.iter().flatten().copied() {
                let index = replacement.slot(entry.state);
                replacement.slots[index] = Some(entry);
            }
            *self = replacement;
        }
        let index = self.slot(state);
        self.slots[index] = Some(Alt1Entry { state, context });
        self.len += 1;
        Ok(())
    }
}

struct ClosureWork<'cfg, 'id, 'scope> {
    frames: Vec<Frame<'cfg>>,
    busy: BusySet<'cfg>,
    skipped: Vec<ConfigKey<'cfg>>,
    alt1: Alt1Map<'id>,
    allocation: &'scope AllocationScope,
}
impl Drop for ClosureWork<'_, '_, '_> { fn drop(&mut self) { let _ = self.allocation; } }
impl<'cfg, 'id, 'scope> ClosureWork<'cfg, 'id, 'scope> {
    fn new(allocation: &'scope AllocationScope) -> Self { Self { frames: Vec::new(), busy: BusySet::new(), skipped: Vec::new(), alt1: Alt1Map::new(), allocation } }
}

pub(super) struct ClosureRound<'call, 'round, 'cfg, 'id, 'g, 'scope, M: StoreMemory> {
    configs: &'call mut Configurations<'cfg, 'id, 'g, 'scope, M>,
    contexts: &'call mut MergeRound<'round, 'id, 'g, 'scope, M>,
    decision: Decision<'g, 'scope>,
    options: ClosureOptions,
    memo: MergePresence,
    work: ClosureWork<'cfg, 'id, 'scope>,
    aborted: bool,
}

pub(super) fn with_closure_round<'cfg, 'id, 'g, 'scope, M: StoreMemory, R>(
    configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>,
    decision: Decision<'g, 'scope>, options: ClosureOptions,
    use_round: impl for<'call, 'round> FnOnce(&mut ClosureRound<'call, 'round, 'cfg, 'id, 'g, 'scope, M>) -> Result<R>,
) -> Result<R> {
    let entered = configs.ready().and_then(|()| contexts.ready()).and_then(|()| {
        if decision.state().belongs_to(contexts.grammar()) { Ok(()) } else { Err(PredictionError::ForeignOwner) }
    });
    let entered = configs.finish(entered);
    contexts.finish(entered)?;
    let allocation = contexts.allocation();
    let result = contexts.with_merge_round(|merge| {
        let mut round = ClosureRound { configs: &mut *configs, contexts: merge, decision, options, memo: MergePresence::Absent, work: ClosureWork::new(allocation), aborted: false };
        let result = use_round(&mut round);
        round.finish(result)
    });
    configs.finish(result)
}

impl<'round, 'cfg, 'id, 'g, 'scope, M: StoreMemory> ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, M> {
    fn finish<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() { self.aborted = true; }
        let result = self.configs.finish(result);
        self.contexts.contexts().finish(result)
    }
    fn enter(&mut self) -> Result<()> {
        let result = if self.aborted { Err(PredictionError::Aborted) } else { self.configs.ready().and_then(|()| self.contexts.contexts().ready()) };
        self.finish(result)
    }
    /// Callers must preserve the active memo and cached configuration values.
    pub(super) fn parts(&mut self) -> (&mut Configurations<'cfg, 'id, 'g, 'scope, M>, &mut PredictionContexts<'id, 'g, 'scope, M>) { (self.configs, self.contexts.contexts()) }
    fn add(&mut self, set: ConfigSetKey<'cfg>, config: ConfigKey<'cfg>) -> Result<()> {
        match self.memo {
            MergePresence::Absent => self.configs.add_uncached(set, config, self.contexts.contexts()),
            MergePresence::Present => self.configs.add(set, config, self.contexts),
        }
    }
    fn local(&self, set: ConfigSetKey<'cfg>) -> Result<()> {
        if self.configs.view_set(set).mode() == SetMode::Local { Ok(()) } else { Err(PredictionError::Artifact(Problem::LocalClosure)) }
    }
    fn assertion(&self, condition: bool, site: SourceAssertionSite) -> Result<()> {
        if self.options.assertions == SourceAssertions::Enabled && !condition { Err(PredictionError::SourceAssertion(site)) } else { Ok(()) }
    }
    pub(super) fn local_start(&mut self) -> Result<StartSet<'round, 'cfg>> {
        self.enter()?;
        let result = (|| {
            let empty = self.contexts.contexts().empty();
            let output = self.configs.new_set(SetMode::Local)?;
            for (index, transition) in self.decision.state().transitions().enumerate() {
                let alt = self.configs.alternative(self.decision, index + 1)?;
                let config = self.configs.create(target(transition), alt, empty, Semantic::Unpredicated)?;
                self.work.busy.clear();
                self.closure(config, output, true, 0, false)?;
            }
            let set = LocalSet::new(output);
            Ok(if matches!(self.decision.state().kind(), StateKindView::StarLoopEntry { precedence: true, .. }) {
                StartSet::Precedence(PrecedenceStart { set })
            } else { StartSet::Ordinary(set) })
        })();
        self.finish(result)
    }
    pub(super) fn local_reach(&mut self, input: LocalSet<'round, 'cfg>, token: TokenType) -> Result<Option<LocalSet<'round, 'cfg>>> {
        self.enter()?;
        let result = (|| {
            self.local(input.key)?;
            self.memo = MergePresence::Present;
            let intermediate = self.configs.new_set(SetMode::Local)?;
            self.work.skipped.clear();
            let count = self.configs.view_set(input.key).len();
            for index in 0..count {
                let config = self.configs.view_set(input.key).entry(index).expect("input occurrence");
                let state = self.configs.view(config).state()?;
                if matches!(state.kind(), StateKindView::RuleStop) {
                    let context = self.configs.view(config).context();
                    let empty = self.contexts.contexts().view(context).is_empty();
                    self.assertion(empty, SourceAssertionSite::ReachStopEmpty)?;
                    if token == TokenType::EOF { push(&mut self.work.skipped, config, self.work.allocation)?; }
                    continue;
                }
                for transition in state.transitions() {
                    if let Some(target) = reachable(transition, token) {
                        let successor = self.configs.copy(config, ConfigChange::State(target))?;
                        self.add(intermediate, successor)?;
                    }
                }
            }
            let view = self.configs.view_set(intermediate);
            let trivial = self.work.skipped.is_empty() && token != TokenType::EOF && (view.len() == 1 || view.unique_alternative().is_some());
            let mut reach = if trivial { intermediate } else {
                let output = self.configs.new_set(SetMode::Local)?;
                self.work.busy.clear();
                for index in 0..self.configs.view_set(intermediate).len() {
                    let config = self.configs.view_set(intermediate).entry(index).expect("intermediate occurrence");
                    self.closure(config, output, false, 0, token == TokenType::EOF)?;
                }
                output
            };
            if token == TokenType::EOF && !self.configs.view_set(reach).all_rule_stop()? {
                let output = self.configs.new_set(SetMode::Local)?;
                for index in 0..self.configs.view_set(reach).len() {
                    let config = self.configs.view_set(reach).entry(index).expect("reach occurrence");
                    if matches!(self.configs.view(config).state()?.kind(), StateKindView::RuleStop) { self.add(output, config)?; }
                }
                reach = output;
            }
            for index in 0..self.work.skipped.len() { self.add(reach, self.work.skipped[index])?; }
            Ok(if self.configs.view_set(reach).len() == 0 { None } else { Some(LocalSet::new(reach)) })
        })();
        self.finish(result)
    }
    pub(super) fn precedence_filter(&mut self, start: PrecedenceStart<'round, 'cfg>, current_precedence: i32) -> Result<LocalSet<'round, 'cfg>> {
        self.enter()?;
        let result = (|| {
            self.local(start.set.key)?;
            let output = self.configs.new_set(SetMode::Local)?;
            self.work.alt1.clear();
            let count = self.configs.view_set(start.set.key).len();
            for index in 0..count {
                let config = self.configs.view_set(start.set.key).entry(index).expect("start occurrence");
                if self.configs.view(config).alternative().number() != 1 { continue; }
                let semantic = self.configs.view(config).semantic();
                let Some(updated) = semantic.evaluate_precedence(current_precedence) else { continue; };
                let view = self.configs.view(config);
                self.work.alt1.put(view.state()?.number(), view.context(), self.work.allocation)?;
                let config = if updated != semantic { self.configs.copy(config, ConfigChange::Semantic(updated))? } else { config };
                self.add(output, config)?;
            }
            for index in 0..count {
                let config = self.configs.view_set(start.set.key).entry(index).expect("start occurrence");
                let view = self.configs.view(config);
                if view.alternative().number() == 1 { continue; }
                if !view.suppressed() {
                    if let Some(captured) = self.work.alt1.get(view.state()?.number()) {
                        let incoming = view.context();
                        if self.contexts.contexts().equivalent(captured, incoming)? { continue; }
                    }
                }
                self.add(output, config)?;
            }
            Ok(LocalSet::new(output))
        })();
        self.finish(result)
    }
    fn closure(&mut self, config: ConfigKey<'cfg>, output: ConfigSetKey<'cfg>, collect: bool, depth: i32, eof: bool) -> Result<()> {
        self.work.frames.clear();
        let mut active = Some(Frame::CheckStop { config, collect, depth, eof });
        while let Some(frame) = active.take().or_else(|| self.work.frames.pop()) {
            match frame {
                Frame::CheckStop { config, collect, depth, eof } => {
                    let state = self.configs.view(config).state()?;
                    let context = self.configs.view(config).context();
                    if matches!(state.kind(), StateKindView::RuleStop) && !self.contexts.contexts().view(context).is_empty() {
                        active = Some(Frame::PopParents { config, next: 0, collect, depth, eof });
                    } else {
                        if !state.epsilon_only() { self.add(output, config)?; }
                        active = Some(Frame::Transitions { config, next: 0, collect, depth, eof });
                    }
                }
                Frame::PopParents { config, next, collect, depth, eof } => {
                    let context = self.configs.view(config).context();
                    let view = self.contexts.contexts().view(context);
                    if next >= view.len() { continue; }
                    let follow = view.follow(next)?.ok_or(PredictionError::Artifact(Problem::LocalClosure))?;
                    let parent = view.parent(next).flatten().ok_or(PredictionError::Artifact(Problem::LocalClosure))?;
                    let child = self.configs.copy(config, ConfigChange::StateContext(follow, parent))?;
                    self.assertion(depth > i32::MIN, SourceAssertionSite::PopDepth)?;
                    push(&mut self.work.frames, Frame::PopParents { config, next: next + 1, collect, depth, eof }, self.work.allocation)?;
                    active = Some(Frame::CheckStop { config: child, collect, depth: depth.wrapping_sub(1), eof });
                }
                Frame::Transitions { config, next, collect, depth, eof } => {
                    let state = self.configs.view(config).state()?;
                    let Some(transition) = state.transitions().nth(next) else { continue; };
                    let resume = Frame::Transitions { config, next: next + 1, collect, depth, eof };
                    if next == 0 && self.can_drop_loop_entry(config)? { active = Some(resume); continue; }
                    let collecting = collect && !matches!(transition, TransitionView::Action { .. });
                    let Some(child) = self.epsilon_target(config, state, next, transition, collecting, depth == 0, eof)? else { active = Some(resume); continue; };
                    let mut child_depth = depth;
                    if matches!(state.kind(), StateKindView::RuleStop) {
                        if matches!(self.decision.state().kind(), StateKindView::StarLoopEntry { precedence: true, .. }) {
                            if let TransitionView::Epsilon { outermost_precedence_return: Some(rule), .. } = transition {
                                if rule.number() == self.decision.state().rule().number() { self.configs.set_suppressed(child, true)?; }
                            }
                        }
                        self.configs.increment_outer(child)?;
                        if !self.work.busy.add(child, self.configs, self.contexts.contexts(), self.work.allocation)? { active = Some(resume); continue; }
                        self.configs.mark_outer_context(output)?;
                        self.assertion(child_depth > i32::MIN, SourceAssertionSite::FalloffDepth)?;
                        child_depth = child_depth.wrapping_sub(1);
                    } else {
                        if !epsilon(transition) && !self.work.busy.add(child, self.configs, self.contexts.contexts(), self.work.allocation)? { active = Some(resume); continue; }
                        if matches!(transition, TransitionView::Rule { .. }) && child_depth >= 0 { child_depth = child_depth.wrapping_add(1); }
                    }
                    push(&mut self.work.frames, resume, self.work.allocation)?;
                    active = Some(Frame::CheckStop { config: child, collect: collecting, depth: child_depth, eof });
                }
            }
        }
        Ok(())
    }
    fn epsilon_target(&mut self, config: ConfigKey<'cfg>, state: State<'g, 'scope>, index: usize, transition: TransitionView<'g, 'scope>, collect: bool, in_context: bool, eof: bool) -> Result<Option<ConfigKey<'cfg>>> {
        let change = match transition {
            TransitionView::Rule { target, follow, .. } => {
                let context = self.configs.view(config).context();
                let context = self.contexts.contexts().singleton(context, follow)?;
                ConfigChange::StateContext(target, context)
            }
            TransitionView::Precedence { target, .. } if collect && in_context => {
                let predicate = self.configs.precedence(state, index)?;
                let view = self.configs.view(config);
                let semantic = Semantic::and(Some(view.semantic()), Some(predicate)).ok_or(PredictionError::Artifact(Problem::Precedence))?;
                ConfigChange::StateContextSemantic(target, view.context(), semantic)
            }
            TransitionView::Epsilon { target, .. } | TransitionView::Action { target, .. } | TransitionView::Precedence { target, .. } => ConfigChange::State(target),
            TransitionView::Atom { target, token } if eof && token == TokenType::EOF => ConfigChange::State(target),
            TransitionView::Set { target, tokens } if eof && tokens.contains(ExpectedSymbol::Token(TokenType::EOF)) => ConfigChange::State(target),
            TransitionView::Atom { .. } | TransitionView::Set { .. } => return Ok(None),
        };
        self.configs.copy(config, change).map(Some)
    }
    fn can_drop_loop_entry(&mut self, config: ConfigKey<'cfg>) -> Result<bool> {
        if self.options.pruning == LoopEntryOption::Disabled { return Ok(false); }
        let view = self.configs.view(config);
        let state = view.state()?;
        if !matches!(state.kind(), StateKindView::StarLoopEntry { precedence: true, .. }) { return Ok(false); }
        let context = self.contexts.contexts().view(view.context());
        if context.is_empty() { return Ok(false); }
        for index in 0..context.len() { if context.follow(index)?.is_none() { return Ok(false); } }
        for index in 0..context.len() {
            if context.follow(index)?.expect("nonempty return").rule().number() != state.rule().number() { return Ok(false); }
        }
        let start = target(state.transitions().next().ok_or(PredictionError::Artifact(Problem::LocalClosure))?);
        let end = match start.kind() {
            StateKindView::BlockStart { end, .. } | StateKindView::PlusBlockStart { end, .. } | StateKindView::StarBlockStart { end, .. } => end,
            _ => return Err(PredictionError::Artifact(Problem::LocalClosure)),
        };
        if !matches!(end.kind(), StateKindView::BlockEnd { .. }) { return Err(PredictionError::Artifact(Problem::LocalClosure)); }
        for index in 0..context.len() {
            let follow = context.follow(index)?.expect("nonempty return");
            if follow.transitions().len() != 1 { return Ok(false); }
            let transition = follow.transitions().next().expect("single transition");
            if !epsilon(transition) { return Ok(false); }
            let destination = target(transition);
            if matches!(follow.kind(), StateKindView::BlockEnd { .. }) && destination.number() == state.number() { continue; }
            if follow.number() == end.number() || destination.number() == end.number() { continue; }
            if matches!(destination.kind(), StateKindView::BlockEnd { .. }) && destination.transitions().len() == 1 {
                let transition = destination.transitions().next().expect("single transition");
                if epsilon(transition) && target(transition).number() == state.number() { continue; }
            }
            return Ok(false);
        }
        Ok(true)
    }
}

impl<'round, 'cfg, 'id, 'g, 'scope> ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, Retained> {
    /// Requires ordinary cache rows from the closed local producer path.
    pub(super) fn from_cached(&mut self, dfas: &mut PredictionDfas<'cfg, 'g, 'scope>, key: InternedStateKey<'cfg>) -> Result<LocalSet<'round, 'cfg>> {
        let result = (|| {
            self.enter()?;
            let set = dfas.configs(self.decision.number(), key, self.configs)?;
            self.local(set)?;
            Ok(LocalSet::new(set))
        })();
        if result.is_err() {
            let _ = self.configs.finish::<()>(Err(PredictionError::Aborted));
            let _ = dfas.configs(self.decision.number(), key, self.configs);
        }
        self.finish(result)
    }
}

fn target<'g, 'scope>(transition: TransitionView<'g, 'scope>) -> State<'g, 'scope> {
    match transition {
        TransitionView::Epsilon { target, .. } | TransitionView::Rule { target, .. } | TransitionView::Atom { target, .. }
            | TransitionView::Action { target, .. } | TransitionView::Set { target, .. } | TransitionView::Precedence { target, .. } => target,
    }
}
fn epsilon(transition: TransitionView<'_, '_>) -> bool { !matches!(transition, TransitionView::Atom { .. } | TransitionView::Set { .. }) }
fn reachable<'g, 'scope>(transition: TransitionView<'g, 'scope>, token: TokenType) -> Option<State<'g, 'scope>> {
    match transition {
        TransitionView::Atom { target, token: label } if label == token => Some(target),
        TransitionView::Set { target, tokens } if tokens.contains(ExpectedSymbol::Token(token)) => Some(target),
        _ => None,
    }
}
