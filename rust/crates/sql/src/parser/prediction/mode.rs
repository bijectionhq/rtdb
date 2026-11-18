use rtdb_types::AllocationScope;
use super::{Alternative, AlternativeSet, ConfigChange, ConfigKey, ConfigSetKey, Configurations, ContextKey, PredictionContexts, PredictionError, Result, Semantic, SetMode, reserve, vector};
use super::context::{finish_hash, update_hash};
use super::storage::StoreMemory;
#[cfg(test)]
#[path = "mode_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PredictionMode { Sll, Ll }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AlternativeChoice { Empty, Conflict, One(Alternative) }

struct Group<'cfg, 'id> { representative: ConfigKey<'cfg>, state: u32, context: ContextKey<'id>, alternatives: AlternativeSet, next: Option<usize> }
struct StateGroup { alternatives: AlternativeSet }
#[derive(Clone, Copy)]
struct Chain { hash: i32, head: usize, tail: usize }
struct Index { slots: Vec<Option<Chain>>, count: usize }
impl Index {
    fn new() -> Self { Self { slots: Vec::new(), count: 0 } }
    fn clear(&mut self) { self.slots.fill(None); self.count = 0; }
    fn slot(&self, hash: i32) -> Option<usize> {
        if self.slots.is_empty() { return None; }
        let mut index = hash as u32 as usize & (self.slots.len() - 1);
        loop {
            match self.slots[index] {
                Some(chain) if chain.hash == hash => return Some(index),
                None => return Some(index),
                _ => index = (index + 1) & (self.slots.len() - 1),
            }
        }
    }
    fn find(&self, hash: i32) -> Option<Chain> { self.slot(hash).and_then(|index| self.slots[index]) }
    fn prepare(&mut self, allocation: &AllocationScope) -> Result<()> {
        if self.count < self.slots.len() / 2 { return Ok(()); }
        let capacity = self.slots.len().checked_mul(2).ok_or_else(super::capacity_error)?.max(32);
        let mut slots = vector(allocation, capacity)?;
        slots.resize(capacity, None);
        let mut replacement = Self { slots, count: self.count };
        for chain in self.slots.iter().flatten().copied() {
            let slot = replacement.slot(chain.hash).unwrap();
            replacement.slots[slot] = Some(chain);
        }
        *self = replacement;
        Ok(())
    }
    fn insert(&mut self, hash: i32, entry: usize) {
        let index = self.slot(hash).unwrap();
        self.slots[index] = Some(Chain { hash, head: entry, tail: entry });
        self.count += 1;
    }
}

pub(crate) struct ModeWorkspace<'cfg, 'id, 'scope> {
    allocation: &'scope AllocationScope,
    groups: Vec<Group<'cfg, 'id>>,
    groups_by_hash: Index,
    states: Vec<StateGroup>,
    states_by_number: Index,
    aborted: bool,
}
impl Drop for ModeWorkspace<'_, '_, '_> { fn drop(&mut self) { let _ = self.allocation; } }

pub(crate) struct SubsetsView<'a, 'cfg, 'id> { groups: &'a [Group<'cfg, 'id>] }
impl SubsetsView<'_, '_, '_> {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize { self.groups.len() }
    #[cfg(test)]
    pub(crate) fn get(&self, index: usize) -> Option<AlternativeSet> { self.groups.get(index).map(|group| group.alternatives) }
    pub(crate) fn has_conflict(&self) -> bool { self.groups.iter().any(|group| group.alternatives.count() > 1) }
    #[cfg(test)]
    pub(crate) fn has_singleton(&self) -> bool { self.groups.iter().any(|group| group.alternatives.count() == 1) }
    #[cfg(test)]
    pub(crate) fn all_conflict(&self) -> bool { !self.has_singleton() }
    #[cfg(test)]
    pub(crate) fn all_equal(&self) -> Result<bool> {
        let Some(first) = self.groups.first() else { return Err(PredictionError::Artifact(super::Problem::EmptySubsets)); };
        Ok(self.groups[1..].iter().all(|group| group.alternatives == first.alternatives))
    }
    pub(crate) fn union(&self) -> AlternativeSet {
        let mut result = AlternativeSet::default();
        for group in self.groups { result.union(group.alternatives); }
        result
    }
    #[cfg(test)]
    pub(crate) fn unique(&self) -> Option<Alternative> {
        let union = self.union();
        if union.count() == 1 { union.first() } else { None }
    }
    pub(crate) fn single_viable(&self) -> AlternativeChoice {
        let mut result = AlternativeChoice::Empty;
        for group in self.groups {
            let alternative = group.alternatives.first().expect("nonempty alternative group");
            if let AlternativeChoice::One(prior) = result {
                if prior != alternative { return AlternativeChoice::Conflict; }
            }
            result = AlternativeChoice::One(alternative);
        }
        result
    }
}

impl<'cfg, 'id, 'scope> ModeWorkspace<'cfg, 'id, 'scope> {
    pub(super) fn new(allocation: &'scope AllocationScope) -> Self {
        Self { allocation, groups: Vec::new(), groups_by_hash: Index::new(), states: Vec::new(), states_by_number: Index::new(), aborted: false }
    }
    fn ready(&self) -> Result<()> { if self.aborted { Err(PredictionError::Aborted) } else { Ok(()) } }
    fn finish<T, M: StoreMemory>(&mut self, configs: &mut Configurations<'cfg, 'id, '_, 'scope, M>, result: Result<T>) -> Result<T> {
        if result.is_err() { self.aborted = true; }
        configs.finish(result)
    }
    fn enter<'g, M: StoreMemory>(&mut self, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<()> {
        let result = self.ready().and_then(|()| configs.ready()).and_then(|()| contexts.ready());
        let result = contexts.finish(result);
        self.finish(configs, result)
    }
    pub(crate) fn conflicting_subsets<'g, M: StoreMemory>(
        &mut self, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, set: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>,
    ) -> Result<SubsetsView<'_, 'cfg, 'id>> {
        self.enter(configs, contexts)?;
        let result = self.build_groups(configs, set, contexts);
        let result = contexts.finish(result);
        self.finish(configs, result)?;
        Ok(SubsetsView { groups: &self.groups })
    }
    fn build_groups<'g, M: StoreMemory>(&mut self, configs: &Configurations<'cfg, 'id, 'g, 'scope, M>, set: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>) -> Result<()> {
        self.groups.clear(); self.groups_by_hash.clear();
        let set = configs.view_set(set);
        for index in 0..set.len() {
            let key = set.entry(index).unwrap();
            let config = configs.view(key);
            let state = config.state()?.number();
            let context = config.context();
            let hash = finish_hash(update_hash(update_hash(7, state as i32), contexts.view(context).hash()), 2);
            let chain = self.groups_by_hash.find(hash);
            let mut next = chain.map(|chain| chain.head);
            let mut found = None;
            while let Some(index) = next {
                let group = &self.groups[index];
                if group.representative == key || group.state == state && contexts.equivalent(group.context, context)? {
                    found = Some(index); break;
                }
                next = group.next;
            }
            if let Some(index) = found { self.groups[index].alternatives.insert(config.alternative()); }
            else {
                reserve(&mut self.groups, 1, self.allocation)?;
                if chain.is_none() { self.groups_by_hash.prepare(self.allocation)?; }
                let mut alternatives = AlternativeSet::default();
                alternatives.insert(config.alternative());
                let index = self.groups.len();
                self.groups.push(Group { representative: key, state, context, alternatives, next: None });
                if let Some(chain) = chain {
                    self.groups[chain.tail].next = Some(index);
                    let slot = self.groups_by_hash.slot(hash).unwrap();
                    self.groups_by_hash.slots[slot].as_mut().unwrap().tail = index;
                } else { self.groups_by_hash.insert(hash, index); }
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn has_state_with_one_alt<M: StoreMemory>(&mut self, configs: &mut Configurations<'cfg, 'id, '_, 'scope, M>, set: ConfigSetKey<'cfg>) -> Result<bool> {
        let result = self.ready().and_then(|()| configs.ready());
        self.finish(configs, result)?;
        let result = self.state_one_alt(configs, set);
        self.finish(configs, result)
    }
    fn state_one_alt<M: StoreMemory>(&mut self, configs: &Configurations<'cfg, 'id, '_, 'scope, M>, set: ConfigSetKey<'cfg>) -> Result<bool> {
        self.states.clear(); self.states_by_number.clear();
        let set = configs.view_set(set);
        for index in 0..set.len() {
            let config = configs.view(set.entry(index).unwrap());
            let state = config.state()?.number() as i32;
            if let Some(chain) = self.states_by_number.find(state) { self.states[chain.head].alternatives.insert(config.alternative()); }
            else {
                reserve(&mut self.states, 1, self.allocation)?;
                self.states_by_number.prepare(self.allocation)?;
                let mut alternatives = AlternativeSet::default();
                alternatives.insert(config.alternative());
                let index = self.states.len();
                self.states.push(StateGroup { alternatives });
                self.states_by_number.insert(state, index);
            }
        }
        Ok(self.states.iter().any(|state| state.alternatives.count() == 1))
    }
    pub(crate) fn has_sll_conflict<'g, M: StoreMemory>(
        &mut self, mode: PredictionMode, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, set: ConfigSetKey<'cfg>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>,
    ) -> Result<bool> {
        self.enter(configs, contexts)?;
        let result = (|| {
            if configs.view_set(set).all_rule_stop()? { return Ok(true); }
            let set = if mode == PredictionMode::Sll && configs.view_set(set).has_semantic() {
                let stripped = configs.new_set(SetMode::Full)?;
                for index in 0..configs.view_set(set).len() {
                    let source = configs.view_set(set).entry(index).unwrap();
                    let copy = configs.copy(source, ConfigChange::Semantic(Semantic::Unpredicated))?;
                    configs.add_uncached(stripped, copy, contexts)?;
                }
                stripped
            } else { set };
            self.build_groups(configs, set, contexts)?;
            if !(SubsetsView { groups: &self.groups }).has_conflict() { return Ok(false); }
            Ok(!self.state_one_alt(configs, set)?)
        })();
        let result = contexts.finish(result);
        self.finish(configs, result)
    }
}
