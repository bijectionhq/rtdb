use std::marker::PhantomData;
use rtdb_types::AllocationScope;
use crate::parser::grammar::{Decision, Grammar, State, StateKindView};
use super::{ContextKey, MergeMode, MergeRound, PredictionContexts, PredictionError, Problem, Result, Semantic, push, vector};
use super::context::{finish_hash, update_hash};
#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;

const MAX_ALTERNATIVE: usize = 87;
const SUPPRESSED: i32 = 0x40000000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Alternative(u8);
impl Alternative { pub(crate) fn number(self) -> usize { usize::from(self.0) } }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) struct AlternativeSet([u64; 2]);
impl AlternativeSet {
    pub(crate) fn insert(&mut self, alternative: Alternative) {
        let number = alternative.number();
        self.0[number / 64] |= 1u64 << (number % 64);
    }
    pub(crate) fn contains(self, alternative: Alternative) -> bool {
        let number = alternative.number();
        self.0[number / 64] & (1u64 << (number % 64)) != 0
    }
    pub(crate) fn union(&mut self, other: Self) { for (a, b) in self.0.iter_mut().zip(other.0) { *a |= b; } }
    pub(crate) fn count(self) -> u32 { self.0.iter().map(|word| word.count_ones()).sum() }
    pub(crate) fn first(self) -> Option<Alternative> {
        self.0.iter().enumerate().find(|(_, word)| **word != 0)
            .map(|(index, word)| Alternative((index * 64 + word.trailing_zeros() as usize) as u8))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SetMode { Local, Full }
impl SetMode { fn merge(self) -> MergeMode { match self { Self::Local => MergeMode::LocalWildcard, Self::Full => MergeMode::FullExact } } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfigKey<'cfg> { index: usize, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfigSetKey<'cfg> { index: usize, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConflictKey<'cfg> { index: usize, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }

#[derive(Clone, Copy)]
struct ConfigData<'id> { state: u32, alt: Alternative, context: ContextKey<'id>, semantic: Semantic, outer: i32 }
impl ConfigData<'_> {
    fn membership_hash(self) -> i32 { 7i32.wrapping_mul(31).wrapping_add(self.state as i32).wrapping_mul(31).wrapping_add(self.alt.0 as i32).wrapping_mul(31).wrapping_add(self.semantic.hash()) }
    fn same_membership(self, other: Self) -> bool { self.state == other.state && self.alt == other.alt && self.semantic == other.semantic }
    fn suppressed(self) -> bool { self.outer & SUPPRESSED != 0 }
    fn depth(self) -> i32 { self.outer & !SUPPRESSED }
}

pub(crate) enum ConfigChange<'id, 'g, 'scope> {
    Same,
    State(State<'g, 'scope>),
    Semantic(Semantic),
    StateContext(State<'g, 'scope>, ContextKey<'id>),
    StateContextSemantic(State<'g, 'scope>, ContextKey<'id>, Semantic),
}

struct Membership<'cfg> { slots: Vec<Option<ConfigKey<'cfg>>>, count: usize }
impl<'cfg> Membership<'cfg> {
    fn slot<'id>(&self, incoming: ConfigData<'id>, objects: &[ConfigData<'id>]) -> usize {
        let mut index = incoming.membership_hash() as u32 as usize & (self.slots.len() - 1);
        while let Some(key) = self.slots[index] {
            if objects[key.index].same_membership(incoming) { break; }
            index = (index + 1) & (self.slots.len() - 1);
        }
        index
    }
    fn prepare(&mut self, objects: &[ConfigData<'_>], allocation: &AllocationScope) -> Result<()> {
        if self.count < self.slots.len() / 2 { return Ok(()); }
        let capacity = self.slots.len().checked_mul(2).ok_or_else(super::capacity_error)?;
        let mut slots = vector(allocation, capacity)?;
        slots.resize(capacity, None);
        let mut replacement = Self { slots, count: self.count };
        for key in self.slots.iter().flatten().copied() {
            let index = replacement.slot(objects[key.index], objects);
            replacement.slots[index] = Some(key);
        }
        *self = replacement;
        Ok(())
    }
}

enum MembershipState<'cfg> { Building(Membership<'cfg>), Readonly { cached_hash: i32 } }
#[derive(Clone, Copy)]
struct SetFacts<'cfg> { mode: SetMode, unique: Option<Alternative>, conflicting: Option<ConflictKey<'cfg>>, semantic: bool, outer: bool }
struct ConfigSet<'cfg> { ordered: Vec<ConfigKey<'cfg>>, facts: SetFacts<'cfg>, membership: MembershipState<'cfg> }

pub(crate) struct Configurations<'cfg, 'id, 'g, 'scope> {
    grammar: &'g Grammar<'scope>,
    allocation: &'scope AllocationScope,
    objects: Vec<ConfigData<'id>>,
    sets: Vec<ConfigSet<'cfg>>,
    conflicts: Vec<AlternativeSet>,
    brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()>,
    aborted: bool,
}
impl Drop for Configurations<'_, '_, '_, '_> { fn drop(&mut self) { let _ = self.allocation; } }

pub(crate) struct ConfigView<'a, 'cfg, 'id, 'g, 'scope> { owner: &'a Configurations<'cfg, 'id, 'g, 'scope>, key: ConfigKey<'cfg> }
impl<'id, 'g, 'scope> ConfigView<'_, '_, 'id, 'g, 'scope> {
    pub(crate) fn state(&self) -> Result<State<'g, 'scope>> { Ok(self.owner.grammar.state(self.owner.data(self.key).state)?) }
    pub(crate) fn alternative(&self) -> Alternative { self.owner.data(self.key).alt }
    pub(crate) fn context(&self) -> ContextKey<'id> { self.owner.data(self.key).context }
    pub(crate) fn semantic(&self) -> Semantic { self.owner.data(self.key).semantic }
    pub(crate) fn outer_depth(&self) -> i32 { self.owner.data(self.key).depth() }
    pub(crate) fn suppressed(&self) -> bool { self.owner.data(self.key).suppressed() }
    pub(crate) fn packed_outer(&self) -> i32 { self.owner.data(self.key).outer }
}

pub(crate) struct ConfigSetView<'a, 'cfg, 'id, 'g, 'scope> { owner: &'a Configurations<'cfg, 'id, 'g, 'scope>, key: ConfigSetKey<'cfg> }
impl<'cfg> ConfigSetView<'_, 'cfg, '_, '_, '_> {
    pub(crate) fn len(&self) -> usize { self.owner.sets[self.key.index].ordered.len() }
    pub(crate) fn entry(&self, index: usize) -> Option<ConfigKey<'cfg>> { self.owner.sets[self.key.index].ordered.get(index).copied() }
    pub(crate) fn mode(&self) -> SetMode { self.owner.sets[self.key.index].facts.mode }
    pub(crate) fn unique(&self) -> Option<Alternative> { self.owner.sets[self.key.index].facts.unique }
    pub(crate) fn conflicting(&self) -> Option<ConflictKey<'cfg>> { self.owner.sets[self.key.index].facts.conflicting }
    pub(crate) fn has_semantic(&self) -> bool { self.owner.sets[self.key.index].facts.semantic }
    pub(crate) fn dips_outer(&self) -> bool { self.owner.sets[self.key.index].facts.outer }
    pub(crate) fn readonly(&self) -> bool { matches!(self.owner.sets[self.key.index].membership, MembershipState::Readonly { .. }) }
    pub(crate) fn alternatives(&self) -> AlternativeSet {
        let mut result = AlternativeSet::default();
        for key in &self.owner.sets[self.key.index].ordered { result.insert(self.owner.data(*key).alt); }
        result
    }
    pub(crate) fn unique_alternative(&self) -> Option<Alternative> {
        let alternatives = self.alternatives();
        if alternatives.count() == 1 { alternatives.first() } else { None }
    }
    pub(crate) fn any_rule_stop(&self) -> Result<bool> {
        for key in &self.owner.sets[self.key.index].ordered {
            if matches!(self.owner.view(*key).state()?.kind(), StateKindView::RuleStop) { return Ok(true); }
        }
        Ok(false)
    }
    pub(crate) fn all_rule_stop(&self) -> Result<bool> {
        for key in &self.owner.sets[self.key.index].ordered {
            if !matches!(self.owner.view(*key).state()?.kind(), StateKindView::RuleStop) { return Ok(false); }
        }
        Ok(true)
    }
}

impl<'cfg, 'id, 'g, 'scope> Configurations<'cfg, 'id, 'g, 'scope> {
    pub(super) fn new(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope) -> Self {
        Self { grammar, allocation, objects: Vec::new(), sets: Vec::new(), conflicts: Vec::new(), brand: PhantomData, aborted: false }
    }
    pub(super) fn ready(&self) -> Result<()> { if self.aborted { Err(PredictionError::Aborted) } else { Ok(()) } }
    pub(super) fn finish<T>(&mut self, result: Result<T>) -> Result<T> { if result.is_err() { self.aborted = true; } result }
    fn check_context(&mut self, contexts: &PredictionContexts<'id, 'g, 'scope>) -> Result<()> {
        let result = self.ready().and_then(|()| contexts.ready());
        self.finish(result)
    }
    pub(crate) fn mode_workspace(&self) -> super::ModeWorkspace<'cfg, 'id, 'scope> { super::ModeWorkspace::new(self.allocation) }
    fn data(&self, key: ConfigKey<'cfg>) -> ConfigData<'id> { self.objects[key.index] }
    pub(crate) fn view(&self, key: ConfigKey<'cfg>) -> ConfigView<'_, 'cfg, 'id, 'g, 'scope> { ConfigView { owner: self, key } }
    pub(crate) fn view_set(&self, key: ConfigSetKey<'cfg>) -> ConfigSetView<'_, 'cfg, 'id, 'g, 'scope> { ConfigSetView { owner: self, key } }
    pub(crate) fn conflict(&self, key: ConflictKey<'cfg>) -> AlternativeSet { self.conflicts[key.index] }

    pub(crate) fn alternative(&mut self, decision: Decision<'g, 'scope>, ordinal: usize) -> Result<Alternative> {
        self.ready()?;
        let result = if !decision.state().belongs_to(self.grammar) { Err(PredictionError::ForeignOwner) }
        else if ordinal == 0 || ordinal > MAX_ALTERNATIVE || ordinal > decision.state().transitions().len() { Err(PredictionError::Artifact(Problem::Alternative)) }
        else { Ok(Alternative(ordinal as u8)) };
        self.finish(result)
    }
    pub(crate) fn precedence(&mut self, state: State<'g, 'scope>, transition: usize) -> Result<Semantic> {
        self.ready()?;
        let result = Semantic::from_state(self.grammar, state, transition);
        self.finish(result)
    }
    fn append(&mut self, data: ConfigData<'id>) -> Result<ConfigKey<'cfg>> {
        let key = ConfigKey { index: self.objects.len(), brand: PhantomData };
        push(&mut self.objects, data, self.allocation)?;
        Ok(key)
    }
    pub(crate) fn create(&mut self, state: State<'g, 'scope>, alt: Alternative, context: ContextKey<'id>, semantic: Semantic) -> Result<ConfigKey<'cfg>> {
        self.ready()?;
        let result = if !state.belongs_to(self.grammar) { Err(PredictionError::ForeignOwner) }
        else { self.append(ConfigData { state: state.number(), alt, context, semantic, outer: 0 }) };
        self.finish(result)
    }
    pub(crate) fn copy(&mut self, key: ConfigKey<'cfg>, change: ConfigChange<'id, 'g, 'scope>) -> Result<ConfigKey<'cfg>> {
        self.ready()?;
        let result = (|| {
            let mut data = self.data(key);
            let state = match change {
                ConfigChange::Same => None,
                ConfigChange::Semantic(semantic) => { data.semantic = semantic; None },
                ConfigChange::State(state) => Some(state),
                ConfigChange::StateContext(state, context) => { data.context = context; Some(state) },
                ConfigChange::StateContextSemantic(state, context, semantic) => { data.context = context; data.semantic = semantic; Some(state) },
            };
            if let Some(state) = state {
                if !state.belongs_to(self.grammar) { return Err(PredictionError::ForeignOwner); }
                data.state = state.number();
            }
            self.append(data)
        })();
        self.finish(result)
    }
    pub(crate) fn increment_outer(&mut self, key: ConfigKey<'cfg>) -> Result<()> {
        self.ready()?;
        self.objects[key.index].outer = self.objects[key.index].outer.wrapping_add(1);
        Ok(())
    }
    pub(crate) fn set_suppressed(&mut self, key: ConfigKey<'cfg>, suppressed: bool) -> Result<()> {
        self.ready()?;
        let outer = &mut self.objects[key.index].outer;
        if suppressed { *outer |= SUPPRESSED; } else { *outer &= !SUPPRESSED; }
        Ok(())
    }
    pub(crate) fn new_set(&mut self, mode: SetMode) -> Result<ConfigSetKey<'cfg>> {
        self.ready()?;
        let result = (|| {
            let ordered = vector(self.allocation, 7)?;
            let mut slots = vector(self.allocation, 32)?;
            slots.resize(32, None);
            let key = ConfigSetKey { index: self.sets.len(), brand: PhantomData };
            push(&mut self.sets, ConfigSet {
                ordered, facts: SetFacts { mode, unique: None, conflicting: None, semantic: false, outer: false },
                membership: MembershipState::Building(Membership { slots, count: 0 }),
            }, self.allocation)?;
            Ok(key)
        })();
        self.finish(result)
    }
    pub(crate) fn add(&mut self, set: ConfigSetKey<'cfg>, key: ConfigKey<'cfg>, round: &mut MergeRound<'_, 'id, 'g, 'scope>) -> Result<()> {
        self.check_context(round.contexts())?;
        let result = self.add_with(set, key, |a, b, mode| round.merge(a, b, mode));
        self.finish(result)
    }
    pub(crate) fn add_uncached(&mut self, set: ConfigSetKey<'cfg>, key: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope>) -> Result<()> {
        self.check_context(contexts)?;
        let result = self.add_with(set, key, |a, b, mode| contexts.merge_uncached(a, b, mode));
        self.finish(result)
    }
    fn add_with(&mut self, set: ConfigSetKey<'cfg>, incoming: ConfigKey<'cfg>, merge: impl FnOnce(ContextKey<'id>, ContextKey<'id>, MergeMode) -> Result<ContextKey<'id>>) -> Result<()> {
        let data = self.data(incoming);
        let set = &mut self.sets[set.index];
        let MembershipState::Building(index) = &mut set.membership else { return Err(PredictionError::Artifact(Problem::Readonly)); };
        set.facts.semantic |= data.semantic != Semantic::Unpredicated;
        set.facts.outer |= data.depth() > 0;
        let slot = index.slot(data, &self.objects);
        let existing = if let Some(existing) = index.slots[slot] { existing }
        else {
            index.prepare(&self.objects, self.allocation)?;
            let slot = index.slot(data, &self.objects);
            index.slots[slot] = Some(incoming);
            index.count += 1;
            incoming
        };
        if existing == incoming { push(&mut set.ordered, incoming, self.allocation)?; }
        else {
            let prior = self.objects[existing.index];
            let context = merge(prior.context, data.context, set.facts.mode.merge())?;
            let mut outer = prior.outer.max(data.outer);
            if data.suppressed() { outer |= SUPPRESSED; }
            self.objects[existing.index] = ConfigData { context, outer, ..prior };
        }
        Ok(())
    }
    pub(crate) fn copy_set(&mut self, source: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope>) -> Result<ConfigSetKey<'cfg>> {
        self.check_context(contexts)?;
        let result = (|| {
            let copy = self.new_set(self.sets[source.index].facts.mode)?;
            for index in 0..self.sets[source.index].ordered.len() {
                let key = self.sets[source.index].ordered[index];
                self.add_uncached(copy, key, contexts)?;
            }
            self.sets[copy.index].facts = self.sets[source.index].facts;
            Ok(copy)
        })();
        self.finish(result)
    }
    pub(crate) fn clear(&mut self, set: ConfigSetKey<'cfg>) -> Result<()> {
        self.ready()?;
        let result = match &mut self.sets[set.index] {
            ConfigSet { ordered, membership: MembershipState::Building(index), .. } => { ordered.clear(); index.slots.fill(None); index.count = 0; Ok(()) },
            _ => Err(PredictionError::Artifact(Problem::Readonly)),
        };
        self.finish(result)
    }
    pub(crate) fn mark_readonly(&mut self, set: ConfigSetKey<'cfg>) -> Result<()> {
        self.ready()?;
        if matches!(self.sets[set.index].membership, MembershipState::Building(_)) {
            self.sets[set.index].membership = MembershipState::Readonly { cached_hash: -1 };
        }
        Ok(())
    }
    pub(crate) fn mark_outer_context(&mut self, set: ConfigSetKey<'cfg>) -> Result<()> { self.ready()?; self.sets[set.index].facts.outer = true; Ok(()) }
    pub(crate) fn set_unique(&mut self, set: ConfigSetKey<'cfg>, unique: Option<Alternative>) -> Result<()> { self.ready()?; self.sets[set.index].facts.unique = unique; Ok(()) }
    pub(crate) fn set_conflicting(&mut self, set: ConfigSetKey<'cfg>, conflicting: Option<ConflictKey<'cfg>>) -> Result<()> { self.ready()?; self.sets[set.index].facts.conflicting = conflicting; Ok(()) }
    pub(crate) fn publish_conflict(&mut self, alternatives: AlternativeSet) -> Result<ConflictKey<'cfg>> {
        self.ready()?;
        let key = ConflictKey { index: self.conflicts.len(), brand: PhantomData };
        let result = push(&mut self.conflicts, alternatives, self.allocation).map(|()| key);
        self.finish(result)
    }
    pub(crate) fn equivalent_config(&mut self, left: ConfigKey<'cfg>, right: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope>) -> Result<bool> {
        self.check_context(contexts)?;
        let result = self.equal_config(left, right, contexts);
        self.finish(result)
    }
    fn equal_config(&self, left: ConfigKey<'cfg>, right: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope>) -> Result<bool> {
        if left == right { return Ok(true); }
        let (a, b) = (self.data(left), self.data(right));
        Ok(a.state == b.state && a.alt == b.alt && contexts.equivalent(a.context, b.context)? && a.semantic == b.semantic && a.suppressed() == b.suppressed())
    }
    pub(crate) fn equivalent_set(&mut self, left: ConfigSetKey<'cfg>, right: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope>) -> Result<bool> {
        self.check_context(contexts)?;
        let result = (|| {
            if left == right { return Ok(true); }
            let (a, b) = (&self.sets[left.index], &self.sets[right.index]);
            if a.ordered.len() != b.ordered.len() { return Ok(false); }
            for (left, right) in a.ordered.iter().zip(&b.ordered) {
                if !self.equal_config(*left, *right, contexts)? { return Ok(false); }
            }
            let (a, b) = (a.facts, b.facts);
            Ok(a.mode == b.mode && a.unique == b.unique && a.conflicting == b.conflicting && a.semantic == b.semantic && a.outer == b.outer)
        })();
        self.finish(result)
    }
    pub(crate) fn config_hash(&mut self, key: ConfigKey<'cfg>, contexts: &PredictionContexts<'id, 'g, 'scope>) -> Result<i32> {
        self.check_context(contexts)?;
        Ok(self.hash_config(key, contexts))
    }
    fn hash_config(&self, key: ConfigKey<'cfg>, contexts: &PredictionContexts<'id, 'g, 'scope>) -> i32 {
        let data = self.data(key);
        let mut hash = 7;
        for value in [data.state as i32, data.alt.0 as i32, contexts.view(data.context).hash(), data.semantic.hash()] { hash = update_hash(hash, value); }
        finish_hash(hash, 4)
    }
    pub(crate) fn set_hash(&mut self, key: ConfigSetKey<'cfg>, contexts: &PredictionContexts<'id, 'g, 'scope>) -> Result<i32> {
        self.check_context(contexts)?;
        if let MembershipState::Readonly { cached_hash } = self.sets[key.index].membership {
            if cached_hash != -1 { return Ok(cached_hash); }
        }
        let mut hash = 1i32;
        for config in &self.sets[key.index].ordered { hash = hash.wrapping_mul(31).wrapping_add(self.hash_config(*config, contexts)); }
        if let MembershipState::Readonly { cached_hash } = &mut self.sets[key.index].membership { *cached_hash = hash; }
        Ok(hash)
    }
    pub(crate) fn optimize(&mut self, key: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope>) -> Result<()> {
        self.check_context(contexts)?;
        let result = (|| {
            if matches!(self.sets[key.index].membership, MembershipState::Readonly { .. }) { return Err(PredictionError::Artifact(Problem::Readonly)); }
            for index in 0..self.sets[key.index].ordered.len() {
                let config = self.sets[key.index].ordered[index];
                let context = contexts.canonicalize(self.data(config).context)?;
                self.objects[config.index].context = context;
            }
            Ok(())
        })();
        self.finish(result)
    }
}
