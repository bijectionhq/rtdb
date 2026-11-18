use std::marker::PhantomData;
use rtdb_types::AllocationScope;
use crate::parser::grammar::{Decision, Grammar, State, StateKindView};
use super::{ContextKey, MergeMode, MergeRound, PredictionContexts, PredictionError, Problem, Result, Semantic};
use super::context::{ContextId, ContextStore, EqualityPolicy, finish_hash, update_hash};
use super::storage::{Block, Pool, Retained, Scoped, SlotId, StoreMemory, StoreRef};
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
pub(super) struct ConfigId(SlotId);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SetId(SlotId);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ConflictId(SlotId);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfigKey<'cfg> { id: ConfigId, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfigSetKey<'cfg> { id: SetId, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConflictKey<'cfg> { id: ConflictId, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
impl ConfigKey<'_> {
    fn at(id: ConfigId) -> Self { Self { id, brand: PhantomData } }
    #[cfg(test)]
    pub(super) fn identity(self) -> ConfigId { self.id }
}
impl ConfigSetKey<'_> {
    pub(super) fn stored(self) -> SetId { self.id }
    fn at(id: SetId) -> Self { Self { id, brand: PhantomData } }
    #[cfg(test)]
    pub(super) fn identity(self) -> SetId { self.id }
}
impl ConflictKey<'_> {
    fn at(id: ConflictId) -> Self { Self { id, brand: PhantomData } }
    #[cfg(test)]
    pub(super) fn identity(self) -> ConflictId { self.id }
}

#[derive(Clone, Copy)]
struct ConfigData { state: u32, alt: Alternative, context: ContextId, semantic: Semantic, outer: i32 }
impl ConfigData {
    fn membership_hash(self) -> i32 { 7i32.wrapping_mul(31).wrapping_add(self.state as i32).wrapping_mul(31).wrapping_add(self.alt.0 as i32).wrapping_mul(31).wrapping_add(self.semantic.hash()) }
    fn same_membership(self, other: Self) -> bool { self.state == other.state && self.alt == other.alt && self.semantic == other.semantic }
    fn suppressed(self) -> bool { self.outer & SUPPRESSED != 0 }
    fn depth(self) -> i32 { self.outer & !SUPPRESSED }
}

pub(crate) enum ConfigChange<'id, 'g, 'scope> {
    #[cfg(test)]
    Same,
    State(State<'g, 'scope>),
    Semantic(Semantic),
    StateContext(State<'g, 'scope>, ContextKey<'id>),
    StateContextSemantic(State<'g, 'scope>, ContextKey<'id>, Semantic),
}

struct Membership<M: StoreMemory> { slots: Block<Option<ConfigId>, M>, count: usize }
impl<M: StoreMemory> Membership<M> {
    fn slot(&self, incoming: ConfigData, objects: &Pool<ConfigData, M>) -> usize {
        let mut index = incoming.membership_hash() as u32 as usize & (self.slots.len() - 1);
        while let Some(key) = self.slots[index] {
            if objects.get(key.0).expect("membership retains its config").same_membership(incoming) { break; }
            index = (index + 1) & (self.slots.len() - 1);
        }
        index
    }
    fn prepare(&mut self, objects: &Pool<ConfigData, M>, memory: &M) -> Result<()> {
        if self.count < self.slots.len() / 2 { return Ok(()); }
        let capacity = self.slots.len().checked_mul(2).ok_or_else(super::capacity_error)?;
        let mut slots = Block::with_capacity(memory, capacity)?;
        for _ in 0..capacity { slots.push(None); }
        let mut replacement = Self { slots, count: self.count };
        for key in self.slots.iter().flatten().copied() {
            let index = replacement.slot(*objects.get(key.0).expect("membership retains its config"), objects);
            replacement.slots[index] = Some(key);
        }
        *self = replacement;
        Ok(())
    }
}

enum MembershipState<M: StoreMemory> { Building(Membership<M>), Readonly { cached_hash: i32 } }
#[derive(Clone, Copy)]
struct SetFacts { mode: SetMode, unique: Option<Alternative>, conflicting: Option<ConflictId>, semantic: bool, outer: bool }
struct ConfigSet<M: StoreMemory> { ordered: Block<ConfigId, M>, facts: SetFacts, membership: MembershipState<M> }

pub(super) struct ConfigurationStore<M: StoreMemory> {
    objects: Pool<ConfigData, M>,
    sets: Pool<ConfigSet<M>, M>,
    conflicts: Pool<AlternativeSet, M>,
    memory: M,
}
impl<M: StoreMemory> ConfigurationStore<M> {
    pub(super) fn new(memory: M) -> Self { Self { objects: Pool::new(), sets: Pool::new(), conflicts: Pool::new(), memory } }
    pub(super) fn mark_set(&mut self, id: SetId) { self.sets.mark(id.0); }
    pub(super) fn collect(&mut self, contexts: &mut ContextStore<M>) {
        while let Some(id) = self.sets.next_marked() {
            let set = self.sets.get(id).expect("marked set retains its generation");
            for config in set.ordered.iter() { self.objects.mark(config.0); }
            if let MembershipState::Building(membership) = &set.membership {
                for config in membership.slots.iter().flatten() { self.objects.mark(config.0); }
            }
            if let Some(conflict) = set.facts.conflicting { self.conflicts.mark(conflict.0); }
        }
        while let Some(id) = self.objects.next_marked() {
            contexts.mark(self.objects.get(id).expect("marked config retains its generation").context);
        }
        while self.conflicts.next_marked().is_some() {}
        self.sets.sweep();
        self.objects.sweep();
        self.conflicts.sweep();
    }
    #[cfg(test)]
    pub(super) fn live(&self) -> (usize, usize, usize) { (self.objects.len(), self.sets.len(), self.conflicts.len()) }
}

pub(crate) struct Configurations<'cfg, 'id, 'g, 'scope, M: StoreMemory = Scoped<'scope>> {
    grammar: &'g Grammar<'scope>,
    allocation: &'scope AllocationScope,
    store: StoreRef<'g, ConfigurationStore<M>>,
    brand: PhantomData<fn(&'cfg mut (), &'id mut ()) -> (&'cfg mut (), &'id mut ())>,
    aborted: bool,
}
impl<M: StoreMemory> Drop for Configurations<'_, '_, '_, '_, M> { fn drop(&mut self) { let _ = self.allocation; } }

pub(crate) struct ConfigView<'a, 'cfg, 'id, 'g, 'scope, M: StoreMemory = Scoped<'scope>> { owner: &'a Configurations<'cfg, 'id, 'g, 'scope, M>, key: ConfigKey<'cfg> }
impl<'id, 'g, 'scope, M: StoreMemory> ConfigView<'_, '_, 'id, 'g, 'scope, M> {
    pub(crate) fn state(&self) -> Result<State<'g, 'scope>> { Ok(self.owner.grammar.state(self.owner.data(self.key).state)?) }
    pub(crate) fn alternative(&self) -> Alternative { self.owner.data(self.key).alt }
    pub(crate) fn context(&self) -> ContextKey<'id> { ContextKey::from_stored(self.owner.data(self.key).context) }
    pub(crate) fn semantic(&self) -> Semantic { self.owner.data(self.key).semantic }
    pub(crate) fn outer_depth(&self) -> i32 { self.owner.data(self.key).depth() }
    pub(crate) fn suppressed(&self) -> bool { self.owner.data(self.key).suppressed() }
    #[cfg(test)]
    pub(crate) fn packed_outer(&self) -> i32 { self.owner.data(self.key).outer }
}

pub(crate) struct ConfigSetView<'a, 'cfg, 'id, 'g, 'scope, M: StoreMemory = Scoped<'scope>> { owner: &'a Configurations<'cfg, 'id, 'g, 'scope, M>, key: ConfigSetKey<'cfg> }
impl<'cfg, M: StoreMemory> ConfigSetView<'_, 'cfg, '_, '_, '_, M> {
    pub(crate) fn len(&self) -> usize { self.owner.set(self.key).ordered.len() }
    pub(crate) fn entry(&self, index: usize) -> Option<ConfigKey<'cfg>> { self.owner.set(self.key).ordered.get(index).copied().map(ConfigKey::at) }
    pub(crate) fn mode(&self) -> SetMode { self.owner.set(self.key).facts.mode }
    #[cfg(test)]
    pub(crate) fn unique(&self) -> Option<Alternative> { self.owner.set(self.key).facts.unique }
    #[cfg(test)]
    pub(crate) fn conflicting(&self) -> Option<ConflictKey<'cfg>> { self.owner.set(self.key).facts.conflicting.map(ConflictKey::at) }
    pub(crate) fn has_semantic(&self) -> bool { self.owner.set(self.key).facts.semantic }
    pub(crate) fn dips_outer(&self) -> bool { self.owner.set(self.key).facts.outer }
    pub(crate) fn readonly(&self) -> bool { matches!(self.owner.set(self.key).membership, MembershipState::Readonly { .. }) }
    pub(crate) fn alternatives(&self) -> AlternativeSet {
        let mut result = AlternativeSet::default();
        for key in self.owner.set(self.key).ordered.iter() { result.insert(self.owner.data(ConfigKey::at(*key)).alt); }
        result
    }
    pub(crate) fn unique_alternative(&self) -> Option<Alternative> {
        let alternatives = self.alternatives();
        if alternatives.count() == 1 { alternatives.first() } else { None }
    }
    pub(crate) fn any_rule_stop(&self) -> Result<bool> {
        for key in self.owner.set(self.key).ordered.iter() {
            if matches!(self.owner.view(ConfigKey::at(*key)).state()?.kind(), StateKindView::RuleStop) { return Ok(true); }
        }
        Ok(false)
    }
    pub(crate) fn all_rule_stop(&self) -> Result<bool> {
        for key in self.owner.set(self.key).ordered.iter() {
            if !matches!(self.owner.view(ConfigKey::at(*key)).state()?.kind(), StateKindView::RuleStop) { return Ok(false); }
        }
        Ok(true)
    }
}

impl<'cfg, 'id, 'g, 'scope> Configurations<'cfg, 'id, 'g, 'scope> {
    #[cfg(test)]
    pub(super) fn new(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope) -> Self {
        Self::open(grammar, allocation, StoreRef::Local(ConfigurationStore::new(Scoped(allocation))))
    }
}
impl<'cfg, 'id, 'g, 'scope> Configurations<'cfg, 'id, 'g, 'scope, Retained> {
    pub(super) fn borrowed(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope, store: &'g mut ConfigurationStore<Retained>) -> Self {
        Self::open(grammar, allocation, StoreRef::Borrowed(store))
    }
}
impl<'cfg, 'id, 'g, 'scope, M: StoreMemory> Configurations<'cfg, 'id, 'g, 'scope, M> {
    fn open(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope, store: StoreRef<'g, ConfigurationStore<M>>) -> Self {
        Self { grammar, allocation, store, brand: PhantomData, aborted: false }
    }
    pub(super) fn bind_set(&self, id: SetId) -> Result<ConfigSetKey<'cfg>> {
        if self.store.sets.get(id.0).is_some() { Ok(ConfigSetKey::at(id)) }
        else { Err(PredictionError::Artifact(Problem::Context)) }
    }
    #[cfg(test)]
    pub(super) fn restore_set(&mut self, id: SetId) -> Result<ConfigSetKey<'cfg>> {
        self.ready()?;
        let result = self.bind_set(id);
        self.finish(result)
    }
    #[cfg(test)]
    pub(super) fn restore_config(&mut self, id: ConfigId) -> Result<ConfigKey<'cfg>> {
        self.ready()?;
        let result = if self.store.objects.get(id.0).is_some() { Ok(ConfigKey::at(id)) }
            else { Err(PredictionError::Artifact(Problem::Context)) };
        self.finish(result)
    }
    #[cfg(test)]
    pub(super) fn restore_conflict(&mut self, id: ConflictId) -> Result<ConflictKey<'cfg>> {
        self.ready()?;
        let result = if self.store.conflicts.get(id.0).is_some() { Ok(ConflictKey::at(id)) }
            else { Err(PredictionError::Artifact(Problem::Context)) };
        self.finish(result)
    }
    fn set(&self, key: ConfigSetKey<'cfg>) -> &ConfigSet<M> { self.store.sets.get(key.id.0).expect("branded set retains its occupied slot") }
    fn set_mut(&mut self, key: ConfigSetKey<'cfg>) -> &mut ConfigSet<M> { self.store.sets.get_mut(key.id.0).expect("branded set retains its occupied slot") }
    pub(super) fn ready(&self) -> Result<()> { if self.aborted { Err(PredictionError::Aborted) } else { Ok(()) } }
    pub(super) fn finish<T>(&mut self, result: Result<T>) -> Result<T> { if result.is_err() { self.aborted = true; } result }
    fn check_context(&mut self, contexts: &PredictionContexts<'id, 'g, 'scope, M>) -> Result<()> {
        let result = self.ready().and_then(|()| contexts.ready());
        self.finish(result)
    }
    pub(crate) fn mode_workspace(&self) -> super::ModeWorkspace<'cfg, 'id, 'scope> { super::ModeWorkspace::new(self.allocation) }
    fn data(&self, key: ConfigKey<'cfg>) -> ConfigData { *self.store.objects.get(key.id.0).expect("branded config retains its occupied slot") }
    pub(crate) fn view(&self, key: ConfigKey<'cfg>) -> ConfigView<'_, 'cfg, 'id, 'g, 'scope, M> { ConfigView { owner: self, key } }
    pub(crate) fn view_set(&self, key: ConfigSetKey<'cfg>) -> ConfigSetView<'_, 'cfg, 'id, 'g, 'scope, M> { ConfigSetView { owner: self, key } }
    #[cfg(test)]
    pub(crate) fn conflict(&self, key: ConflictKey<'cfg>) -> AlternativeSet { *self.store.conflicts.get(key.id.0).expect("branded conflict retains its occupied slot") }

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
    fn append(&mut self, data: ConfigData) -> Result<ConfigKey<'cfg>> {
        let store = &mut *self.store;
        store.objects.insert(data, &store.memory).map(|id| ConfigKey::at(ConfigId(id)))
    }
    pub(crate) fn create(&mut self, state: State<'g, 'scope>, alt: Alternative, context: ContextKey<'id>, semantic: Semantic) -> Result<ConfigKey<'cfg>> {
        self.ready()?;
        let result = if !state.belongs_to(self.grammar) { Err(PredictionError::ForeignOwner) }
        else { self.append(ConfigData { state: state.number(), alt, context: context.stored(), semantic, outer: 0 }) };
        self.finish(result)
    }
    pub(crate) fn copy(&mut self, key: ConfigKey<'cfg>, change: ConfigChange<'id, 'g, 'scope>) -> Result<ConfigKey<'cfg>> {
        self.ready()?;
        let result = (|| {
            let mut data = self.data(key);
            let state = match change {
                #[cfg(test)]
                ConfigChange::Same => None,
                ConfigChange::Semantic(semantic) => { data.semantic = semantic; None },
                ConfigChange::State(state) => Some(state),
                ConfigChange::StateContext(state, context) => { data.context = context.stored(); Some(state) },
                ConfigChange::StateContextSemantic(state, context, semantic) => { data.context = context.stored(); data.semantic = semantic; Some(state) },
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
        let outer = &mut self.store.objects.get_mut(key.id.0).expect("branded config retains its occupied slot").outer;
        *outer = outer.wrapping_add(1);
        Ok(())
    }
    pub(crate) fn set_suppressed(&mut self, key: ConfigKey<'cfg>, suppressed: bool) -> Result<()> {
        self.ready()?;
        let outer = &mut self.store.objects.get_mut(key.id.0).expect("branded config retains its occupied slot").outer;
        if suppressed { *outer |= SUPPRESSED; } else { *outer &= !SUPPRESSED; }
        Ok(())
    }
    pub(crate) fn new_set(&mut self, mode: SetMode) -> Result<ConfigSetKey<'cfg>> {
        self.ready()?;
        let result = (|| {
            let store = &mut *self.store;
            let ordered = Block::with_capacity(&store.memory, 7)?;
            let mut slots = Block::with_capacity(&store.memory, 32)?;
            for _ in 0..32 { slots.push(None); }
            store.sets.insert(ConfigSet {
                ordered, facts: SetFacts { mode, unique: None, conflicting: None, semantic: false, outer: false },
                membership: MembershipState::Building(Membership { slots, count: 0 }),
            }, &store.memory).map(|id| ConfigSetKey::at(SetId(id)))
        })();
        self.finish(result)
    }
    pub(crate) fn add(&mut self, set: ConfigSetKey<'cfg>, key: ConfigKey<'cfg>, round: &mut MergeRound<'_, 'id, 'g, 'scope, M>) -> Result<()> {
        self.check_context(round.contexts())?;
        let result = self.add_with(set, key, |a, b, mode| round.merge(a, b, mode));
        self.finish(result)
    }
    pub(crate) fn add_uncached(&mut self, set: ConfigSetKey<'cfg>, key: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<()> {
        self.check_context(contexts)?;
        let result = self.add_with(set, key, |a, b, mode| contexts.merge_uncached(a, b, mode));
        self.finish(result)
    }
    fn add_with(&mut self, set: ConfigSetKey<'cfg>, incoming: ConfigKey<'cfg>, merge: impl FnOnce(ContextKey<'id>, ContextKey<'id>, MergeMode) -> Result<ContextKey<'id>>) -> Result<()> {
        let data = self.data(incoming);
        let store = &mut *self.store;
        let set = store.sets.get_mut(set.id.0).expect("branded set retains its occupied slot");
        let MembershipState::Building(index) = &mut set.membership else { return Err(PredictionError::Artifact(Problem::Readonly)); };
        set.facts.semantic |= data.semantic != Semantic::Unpredicated;
        set.facts.outer |= data.depth() > 0;
        let slot = index.slot(data, &store.objects);
        let existing = if let Some(existing) = index.slots[slot] { existing }
        else {
            index.prepare(&store.objects, &store.memory)?;
            let slot = index.slot(data, &store.objects);
            index.slots[slot] = Some(incoming.id);
            index.count += 1;
            incoming.id
        };
        if existing == incoming.id {
            set.ordered.reserve(1, &store.memory)?;
            set.ordered.push(incoming.id);
        } else {
            let prior = *store.objects.get(existing.0).expect("membership retains its config");
            let context = merge(ContextKey::from_stored(prior.context), ContextKey::from_stored(data.context), set.facts.mode.merge())?;
            let mut outer = prior.outer.max(data.outer);
            if data.suppressed() { outer |= SUPPRESSED; }
            *store.objects.get_mut(existing.0).expect("membership retains its config") = ConfigData { context: context.stored(), outer, ..prior };
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn copy_set(&mut self, source: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<ConfigSetKey<'cfg>> {
        self.check_context(contexts)?;
        let result = (|| {
            let copy = self.new_set(self.set(source).facts.mode)?;
            for index in 0..self.set(source).ordered.len() {
                let key = ConfigKey::at(self.set(source).ordered[index]);
                self.add_uncached(copy, key, contexts)?;
            }
            self.set_mut(copy).facts = self.set(source).facts;
            Ok(copy)
        })();
        self.finish(result)
    }
    #[cfg(test)]
    pub(crate) fn clear(&mut self, set: ConfigSetKey<'cfg>) -> Result<()> {
        self.ready()?;
        let result = match self.set_mut(set) {
            ConfigSet { ordered, membership: MembershipState::Building(index), .. } => { ordered.clear(); index.slots.fill(None); index.count = 0; Ok(()) },
            _ => Err(PredictionError::Artifact(Problem::Readonly)),
        };
        self.finish(result)
    }
    pub(crate) fn mark_readonly(&mut self, set: ConfigSetKey<'cfg>) -> Result<()> {
        self.ready()?;
        if matches!(self.set(set).membership, MembershipState::Building(_)) {
            self.set_mut(set).membership = MembershipState::Readonly { cached_hash: -1 };
        }
        Ok(())
    }
    pub(crate) fn mark_outer_context(&mut self, set: ConfigSetKey<'cfg>) -> Result<()> { self.ready()?; self.set_mut(set).facts.outer = true; Ok(()) }
    pub(crate) fn set_unique(&mut self, set: ConfigSetKey<'cfg>, unique: Option<Alternative>) -> Result<()> { self.ready()?; self.set_mut(set).facts.unique = unique; Ok(()) }
    pub(crate) fn set_conflicting(&mut self, set: ConfigSetKey<'cfg>, conflicting: Option<ConflictKey<'cfg>>) -> Result<()> { self.ready()?; self.set_mut(set).facts.conflicting = conflicting.map(|key| key.id); Ok(()) }
    pub(crate) fn publish_conflict(&mut self, alternatives: AlternativeSet) -> Result<ConflictKey<'cfg>> {
        self.ready()?;
        let store = &mut *self.store;
        let result = store.conflicts.insert(alternatives, &store.memory).map(|id| ConflictKey::at(ConflictId(id)));
        self.finish(result)
    }
    #[cfg(test)]
    pub(crate) fn equivalent_config(&mut self, left: ConfigKey<'cfg>, right: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<bool> {
        self.check_context(contexts)?;
        let result = self.equal_config(left, right, contexts);
        self.finish(result)
    }
    pub(super) fn equivalent_config_for_probe(&mut self, incoming: ConfigKey<'cfg>, stored: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<bool> {
        self.check_context(contexts)?;
        let result = self.equal_config_using(incoming, stored, contexts, EqualityPolicy::LinkedFrames);
        self.finish(result)
    }
    #[cfg(test)]
    fn equal_config(&self, left: ConfigKey<'cfg>, right: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<bool> {
        self.equal_config_using(left, right, contexts, EqualityPolicy::ScopedFrames)
    }
    fn equal_config_using(&self, left: ConfigKey<'cfg>, right: ConfigKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>, policy: EqualityPolicy) -> Result<bool> {
        if left == right { return Ok(true); }
        let (a, b) = (self.data(left), self.data(right));
        Ok(a.state == b.state && a.alt == b.alt && contexts.equivalent_using(ContextKey::from_stored(a.context), ContextKey::from_stored(b.context), policy)? && a.semantic == b.semantic && a.suppressed() == b.suppressed())
    }
    #[cfg(test)]
    pub(crate) fn equivalent_set(&mut self, left: ConfigSetKey<'cfg>, right: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<bool> {
        self.equivalent_set_using(left, right, contexts, EqualityPolicy::ScopedFrames)
    }
    pub(super) fn equivalent_set_for_probe(&mut self, left: ConfigSetKey<'cfg>, right: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<bool> {
        self.equivalent_set_using(left, right, contexts, EqualityPolicy::LinkedFrames)
    }
    fn equivalent_set_using(&mut self, left: ConfigSetKey<'cfg>, right: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>, policy: EqualityPolicy) -> Result<bool> {
        self.check_context(contexts)?;
        let result = (|| {
            if left == right { return Ok(true); }
            let (a, b) = (self.set(left), self.set(right));
            if a.ordered.len() != b.ordered.len() { return Ok(false); }
            for (left, right) in a.ordered.iter().zip(b.ordered.iter()) {
                if !self.equal_config_using(ConfigKey::at(*left), ConfigKey::at(*right), contexts, policy)? { return Ok(false); }
            }
            let (a, b) = (a.facts, b.facts);
            Ok(a.mode == b.mode && a.unique == b.unique && a.conflicting == b.conflicting && a.semantic == b.semantic && a.outer == b.outer)
        })();
        self.finish(result)
    }
    pub(crate) fn config_hash(&mut self, key: ConfigKey<'cfg>, contexts: &PredictionContexts<'id, 'g, 'scope, M>) -> Result<i32> {
        self.check_context(contexts)?;
        Ok(self.hash_config(key, contexts))
    }
    fn hash_config(&self, key: ConfigKey<'cfg>, contexts: &PredictionContexts<'id, 'g, 'scope, M>) -> i32 {
        let data = self.data(key);
        let mut hash = 7;
        for value in [data.state as i32, data.alt.0 as i32, contexts.view(ContextKey::from_stored(data.context)).hash(), data.semantic.hash()] { hash = update_hash(hash, value); }
        finish_hash(hash, 4)
    }
    pub(crate) fn set_hash(&mut self, key: ConfigSetKey<'cfg>, contexts: &PredictionContexts<'id, 'g, 'scope, M>) -> Result<i32> {
        self.check_context(contexts)?;
        if let MembershipState::Readonly { cached_hash } = self.set(key).membership {
            if cached_hash != -1 { return Ok(cached_hash); }
        }
        let mut hash = 1i32;
        for config in self.set(key).ordered.iter() { hash = hash.wrapping_mul(31).wrapping_add(self.hash_config(ConfigKey::at(*config), contexts)); }
        if let MembershipState::Readonly { cached_hash } = &mut self.set_mut(key).membership { *cached_hash = hash; }
        Ok(hash)
    }
    pub(crate) fn optimize(&mut self, key: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<()> {
        self.check_context(contexts)?;
        let result = (|| {
            if matches!(self.set(key).membership, MembershipState::Readonly { .. }) { return Err(PredictionError::Artifact(Problem::Readonly)); }
            for index in 0..self.set(key).ordered.len() {
                let config = ConfigKey::at(self.set(key).ordered[index]);
                let context = contexts.canonicalize(ContextKey::from_stored(self.data(config).context))?;
                self.store.objects.get_mut(config.id.0).expect("ordered set retains its config").context = context.stored();
            }
            Ok(())
        })();
        self.finish(result)
    }
}
