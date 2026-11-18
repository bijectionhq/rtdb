use std::marker::PhantomData;
use rtdb_types::AllocationScope;
use crate::parser::grammar::{Grammar, StateKindView};
use super::{Alternative, ConfigSetKey, Configurations, PredictionContexts, PredictionError, Problem, Result, Semantic, SetMode, capacity_error};
use super::config::{ConfigurationStore, SetId};
use super::context::{finish_hash, update_hash};
use super::storage::{Block, Pool, Retained, SlotId, StoreMemory};
#[cfg(test)]
#[path = "dfa_tests.rs"]
mod tests;

const DECISIONS: usize = 372;
const ORDINARY_EDGES: usize = 370;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StateId(SlotId);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InternedId(StateId);
#[derive(Clone, Copy)]
struct SyntheticId(StateId);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DraftKey<'cfg> { id: StateId, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct InternedStateKey<'cfg> { id: InternedId, brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()> }
impl DraftKey<'_> { fn at(id: StateId) -> Self { Self { id, brand: PhantomData } } }
impl InternedStateKey<'_> { fn at(id: InternedId) -> Self { Self { id, brand: PhantomData } } }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DfaTarget<'cfg> { Error, Interned(InternedStateKey<'cfg>) }
#[derive(Clone, Copy)]
pub(super) enum Candidate<'cfg> {
    Error,
    Draft(DraftKey<'cfg>),
    #[cfg(test)]
    Existing(InternedStateKey<'cfg>),
}
#[derive(Clone, Copy)]
enum StoredTarget { Error, Interned(InternedId) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AcceptReason { Unique, Conflict }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct PredPrediction { pub(super) semantic: Semantic, pub(super) alternative: Alternative }
pub(super) enum Acceptance<'a> {
    NonAccept,
    Plain { reason: AcceptReason, prediction: Alternative },
    Predicated { reason: AcceptReason, pairs: &'a [PredPrediction] },
}
struct PredicateBlock<M: StoreMemory>(Block<PredPrediction, M>);
enum CompletedAcceptance<M: StoreMemory> {
    NonAccept,
    Plain { reason: AcceptReason, prediction: Alternative },
    Predicated { reason: AcceptReason, pairs: PredicateBlock<M> },
}
enum OrdinaryPhase<M: StoreMemory> {
    UnnumberedDraft,
    NumberedUnpublished { number: i32 },
    Interned { number: i32, edges: Block<Option<StoredTarget>, M> },
}
enum Body<M: StoreMemory> {
    Synthetic { prefilter: SetId, edges: Block<Option<InternedId>, M> },
    Ordinary { configs: SetId, acceptance: CompletedAcceptance<M>, phase: OrdinaryPhase<M> },
}
struct DfaStateData<M: StoreMemory> { decision: u16, body: Body<M> }
#[derive(Clone, Copy)]
enum Start { Ordinary(Option<InternedId>), Precedence(SyntheticId) }
#[derive(Clone, Copy)]
struct Record { hash: u32, key: InternedId, value: InternedId }
struct StateMap<M: StoreMemory> { slots: Block<Option<Record>, M>, len: usize }
impl<M: StoreMemory> StateMap<M> {
    fn new() -> Self { Self { slots: Block::new(), len: 0 } }
    fn vacant(&self, hash: u32) -> usize {
        let mut index = hash as usize & (self.slots.len() - 1);
        while self.slots[index].is_some() { index = (index + 1) & (self.slots.len() - 1); }
        index
    }
    fn prepare(&mut self, memory: &M) -> Result<()> {
        if self.len < self.slots.len() / 2 { return Ok(()); }
        let capacity = self.slots.len().checked_mul(2).ok_or_else(capacity_error)?.max(32);
        let mut slots = Block::with_capacity(memory, capacity)?;
        for _ in 0..capacity { slots.push(None); }
        let mut replacement = Self { slots, len: self.len };
        for record in self.slots.iter().flatten().copied() {
            let index = replacement.vacant(record.hash);
            replacement.slots[index] = Some(record);
        }
        *self = replacement;
        Ok(())
    }
}
struct DecisionDfa<M: StoreMemory> { number: u16, atn_start: u32, start: Start, states: StateMap<M> }
pub(super) struct DfaStore<M: StoreMemory> { decisions: Block<DecisionDfa<M>, M>, objects: Pool<DfaStateData<M>, M>, memory: M }
impl<M: StoreMemory> DfaStore<M> {
    pub(super) fn new(memory: M) -> Self { Self { decisions: Block::new(), objects: Pool::new(), memory } }
    pub(super) fn collect(&mut self, configs: &mut ConfigurationStore<M>) {
        for decision in self.decisions.iter() {
            match decision.start {
                Start::Ordinary(Some(id)) => self.objects.mark(id.0.0),
                Start::Precedence(id) => self.objects.mark(id.0.0),
                _ => {}
            }
            for record in decision.states.slots.iter().flatten() {
                self.objects.mark(record.key.0.0);
                self.objects.mark(record.value.0.0);
            }
        }
        while let Some(id) = self.objects.next_marked() {
            let state = self.objects.get(id).expect("marked DFA state retains its generation");
            let (set, length) = match &state.body {
                Body::Synthetic { prefilter, edges } => (*prefilter, edges.len()),
                Body::Ordinary { configs, phase, .. } => (*configs, match phase { OrdinaryPhase::Interned { edges, .. } => edges.len(), _ => 0 }),
            };
            configs.mark_set(set);
            for index in 0..length {
                let target = match &self.objects.get(id).unwrap().body {
                    Body::Synthetic { edges, .. } => edges[index],
                    Body::Ordinary { phase: OrdinaryPhase::Interned { edges, .. }, .. } => match edges[index] { Some(StoredTarget::Interned(id)) => Some(id), _ => None },
                    _ => unreachable!(),
                };
                if let Some(target) = target { self.objects.mark(target.0.0); }
            }
        }
        self.objects.sweep();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase { Synthetic, UnnumberedDraft, NumberedUnpublished(i32), Interned(i32) }
#[cfg(test)]
pub(super) struct DecisionView<'a> { decision: &'a DecisionDfa<Retained> }
#[cfg(test)]
impl DecisionView<'_> {
    pub(super) fn number(&self) -> u16 { self.decision.number }
    pub(super) fn atn_start(&self) -> u32 { self.decision.atn_start }
    pub(super) fn precedence(&self) -> bool { matches!(self.decision.start, Start::Precedence(_)) }
}
pub(super) struct StateView<'a> { state: &'a DfaStateData<Retained> }
impl StateView<'_> {
    pub(super) fn phase(&self) -> Phase {
        match &self.state.body {
            Body::Synthetic { .. } => Phase::Synthetic,
            Body::Ordinary { phase, .. } => match phase {
                OrdinaryPhase::UnnumberedDraft => Phase::UnnumberedDraft,
                OrdinaryPhase::NumberedUnpublished { number } => Phase::NumberedUnpublished(*number),
                OrdinaryPhase::Interned { number, .. } => Phase::Interned(*number),
            },
        }
    }
    #[cfg(test)]
    pub(super) fn number(&self) -> i32 { match self.phase() { Phase::Synthetic | Phase::UnnumberedDraft => -1, Phase::NumberedUnpublished(number) | Phase::Interned(number) => number } }
    pub(super) fn acceptance(&self) -> Acceptance<'_> {
        match &self.state.body {
            Body::Synthetic { .. } | Body::Ordinary { acceptance: CompletedAcceptance::NonAccept, .. } => Acceptance::NonAccept,
            Body::Ordinary { acceptance: CompletedAcceptance::Plain { reason, prediction }, .. } => Acceptance::Plain { reason: *reason, prediction: *prediction },
            Body::Ordinary { acceptance: CompletedAcceptance::Predicated { reason, pairs }, .. } => Acceptance::Predicated { reason: *reason, pairs: &pairs.0 },
        }
    }
}

pub(super) struct PredictionDfas<'cfg, 'g, 'scope> {
    store: &'g mut DfaStore<Retained>,
    grammar: &'g Grammar<'scope>,
    allocation: &'scope AllocationScope,
    brand: PhantomData<fn(&'cfg mut ()) -> &'cfg mut ()>,
    aborted: bool,
}
impl Drop for PredictionDfas<'_, '_, '_> { fn drop(&mut self) { let _ = self.allocation; } }
impl<'cfg, 'g, 'scope> PredictionDfas<'cfg, 'g, 'scope> {
    pub(super) fn borrowed(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope, store: &'g mut DfaStore<Retained>) -> Self {
        Self { store, grammar, allocation, brand: PhantomData, aborted: false }
    }
    fn ready(&self) -> Result<()> { if self.aborted { Err(PredictionError::Aborted) } else { Ok(()) } }
    fn finish<T>(&mut self, result: Result<T>) -> Result<T> { if result.is_err() { self.aborted = true; } result }
    fn enter_config<'id>(&mut self, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<()> {
        let result = self.ready().and_then(|()| configs.ready());
        self.finish_config(result, configs)
    }
    fn finish_config<'id, T>(&mut self, result: Result<T>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<T> {
        if result.is_err() { let _ = configs.finish::<()>(Err(PredictionError::Aborted)); }
        self.finish(result)
    }
    fn enter<'id>(&mut self, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<()> {
        let result = self.ready().and_then(|()| configs.ready()).and_then(|()| contexts.ready());
        self.finish_all(result, configs, contexts)
    }
    fn finish_all<'id, T>(&mut self, result: Result<T>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<T> {
        if result.is_err() { let _ = contexts.finish::<()>(Err(PredictionError::Aborted)); }
        self.finish_config(result, configs)
    }
    pub(super) fn abort<'id>(&mut self, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) {
        let _ = self.finish_all::<()>(Err(PredictionError::Aborted), configs, contexts);
    }
    fn decision(&self, number: u16) -> Result<&DecisionDfa<Retained>> {
        self.store.decisions.get(usize::from(number)).ok_or(PredictionError::Artifact(Problem::DfaDecision))
    }
    fn state(&self, decision: u16, id: StateId) -> Result<&DfaStateData<Retained>> {
        self.decision(decision)?;
        let state = self.store.objects.get(id.0).ok_or(PredictionError::Artifact(Problem::DfaPhase))?;
        if state.decision != decision { return Err(PredictionError::Artifact(Problem::DfaDecision)); }
        Ok(state)
    }
    fn ordinary(&self, decision: u16, id: StateId) -> Result<(SetId, Phase)> {
        let state = self.state(decision, id)?;
        let view = StateView { state };
        match &state.body { Body::Ordinary { configs, .. } => Ok((*configs, view.phase())), _ => Err(PredictionError::Artifact(Problem::DfaPhase)) }
    }
    fn interned(&self, decision: u16, id: InternedId) -> Result<SetId> {
        match self.ordinary(decision, id.0)? { (set, Phase::Interned(_)) => Ok(set), _ => Err(PredictionError::Artifact(Problem::DfaPhase)) }
    }
    pub(super) fn initialize<'id>(&mut self, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<()> {
        self.enter_config(configs)?;
        let result = (|| {
            if !self.store.decisions.is_empty() { return Ok(()); }
            let mut decisions = Block::with_capacity(&self.store.memory, DECISIONS)?;
            for number in 0..DECISIONS {
                let decision = self.grammar.decision(number as u16)?;
                let atn_start = decision.state().number();
                let start = if matches!(decision.state().kind(), StateKindView::StarLoopEntry { precedence: true, .. }) {
                    let prefilter = configs.new_set(SetMode::Full)?.stored();
                    let state = DfaStateData { decision: number as u16, body: Body::Synthetic { prefilter, edges: Block::new() } };
                    Start::Precedence(SyntheticId(StateId(self.store.objects.insert(state, &self.store.memory)?)))
                } else { Start::Ordinary(None) };
                decisions.push(DecisionDfa { number: number as u16, atn_start, start, states: StateMap::new() });
            }
            self.store.decisions = decisions;
            Ok(())
        })();
        self.finish_config(result, configs)
    }
    pub(super) fn new_draft<'id>(&mut self, decision: u16, set: ConfigSetKey<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<DraftKey<'cfg>> {
        self.enter_config(configs)?;
        let result = (|| {
            self.decision(decision)?;
            let set = configs.bind_set(set.stored())?;
            if configs.view_set(set).mode() != SetMode::Local { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
            let state = DfaStateData { decision, body: Body::Ordinary { configs: set.stored(), acceptance: CompletedAcceptance::NonAccept, phase: OrdinaryPhase::UnnumberedDraft } };
            self.store.objects.insert(state, &self.store.memory).map(|id| DraftKey::at(StateId(id)))
        })();
        self.finish_config(result, configs)
    }
    pub(super) fn complete_acceptance<'id>(&mut self, decision: u16, draft: DraftKey<'cfg>, input: Acceptance<'_>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<()> {
        self.enter_config(configs)?;
        let result = (|| {
            if self.ordinary(decision, draft.id)?.1 != Phase::UnnumberedDraft { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
            let max = self.grammar.decision(decision)?.state().transitions().len();
            let check = |alternative: Alternative| -> Result<()> {
                if alternative.number() == 0 || alternative.number() > max { Err(PredictionError::Artifact(Problem::Alternative)) } else { Ok(()) }
            };
            let completed = match input {
                Acceptance::NonAccept => CompletedAcceptance::NonAccept,
                Acceptance::Plain { reason, prediction } => { check(prediction)?; CompletedAcceptance::Plain { reason, prediction } }
                Acceptance::Predicated { reason, pairs } => {
                    if pairs.is_empty() { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
                    let mut stored = Block::with_capacity(&self.store.memory, pairs.len())?;
                    for pair in pairs { check(pair.alternative)?; stored.push(*pair); }
                    CompletedAcceptance::Predicated { reason, pairs: PredicateBlock(stored) }
                }
            };
            let Body::Ordinary { acceptance, .. } = &mut self.store.objects.get_mut(draft.id.0).unwrap().body else { unreachable!() };
            *acceptance = completed;
            Ok(())
        })();
        self.finish_config(result, configs)
    }
    #[cfg(test)]
    pub(super) fn view_draft(&self, decision: u16, draft: DraftKey<'cfg>) -> Result<StateView<'_>> {
        let state = self.state(decision, draft.id)?;
        Ok(StateView { state })
    }
    pub(super) fn view(&self, decision: u16, key: InternedStateKey<'cfg>) -> Result<StateView<'_>> {
        self.interned(decision, key.id)?;
        Ok(StateView { state: self.state(decision, key.id.0)? })
    }
    pub(super) fn configs<'id>(&mut self, decision: u16, key: InternedStateKey<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<ConfigSetKey<'cfg>> {
        self.enter_config(configs)?;
        let result = self.interned(decision, key.id).and_then(|set| configs.bind_set(set));
        self.finish_config(result, configs)
    }
    pub(super) fn replace_synthetic_prefilter<'id>(&mut self, decision: u16, set: ConfigSetKey<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>) -> Result<()> {
        self.enter_config(configs)?;
        let result = (|| {
            let Start::Precedence(id) = self.decision(decision)?.start else { return Err(PredictionError::Artifact(Problem::DfaPhase)); };
            let set = configs.bind_set(set.stored())?;
            if configs.view_set(set).mode() != SetMode::Local { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
            self.state(decision, id.0)?;
            let Body::Synthetic { prefilter, .. } = &mut self.store.objects.get_mut(id.0.0).unwrap().body else { return Err(PredictionError::Artifact(Problem::DfaPhase)); };
            *prefilter = set.stored();
            Ok(())
        })();
        self.finish_config(result, configs)
    }
    pub(super) fn start(&mut self, decision: u16, precedence: i32) -> Result<Option<InternedStateKey<'cfg>>> {
        self.ready()?;
        let result = (|| {
            let id = match self.decision(decision)?.start {
                Start::Ordinary(id) => id,
                Start::Precedence(id) => {
                    let state = self.state(decision, id.0)?;
                    let Body::Synthetic { edges, .. } = &state.body else { return Err(PredictionError::Artifact(Problem::DfaPhase)); };
                    usize::try_from(precedence).ok().and_then(|index| edges.get(index)).copied().flatten()
                }
            };
            id.map(|id| self.interned(decision, id).map(|_| InternedStateKey::at(id))).transpose()
        })();
        self.finish(result)
    }
    pub(super) fn set_start(&mut self, decision: u16, precedence: i32, target: InternedStateKey<'cfg>) -> Result<()> {
        self.ready()?;
        let result = (|| {
            match self.decision(decision)?.start {
                Start::Ordinary(_) => { self.interned(decision, target.id)?; self.store.decisions[usize::from(decision)].start = Start::Ordinary(Some(target.id)); }
                Start::Precedence(id) => {
                    if precedence < 0 { return Ok(()); }
                    let length = precedence.checked_add(1).ok_or_else(capacity_error)? as usize;
                    self.interned(decision, target.id)?;
                    self.state(decision, id.0)?;
                    let Body::Synthetic { edges, .. } = &mut self.store.objects.get_mut(id.0.0).unwrap().body else { return Err(PredictionError::Artifact(Problem::DfaPhase)); };
                    if length > edges.len() {
                        let mut replacement = Block::with_capacity(&self.store.memory, length)?;
                        for index in 0..length { replacement.push(edges.get(index).copied().flatten()); }
                        *edges = replacement;
                    }
                    edges[precedence as usize] = Some(target.id);
                }
            }
            Ok(())
        })();
        self.finish(result)
    }
    fn hash<'id>(&self, set: ConfigSetKey<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<u32> {
        let hash = finish_hash(update_hash(7, configs.set_hash(set, contexts)?), 1) as u32;
        Ok(hash ^ (hash >> 16))
    }
    fn find<'id>(&self, decision: u16, hash: u32, query: ConfigSetKey<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<Option<Record>> {
        let map = &self.decision(decision)?.states;
        if map.slots.is_empty() { return Ok(None); }
        let mut index = hash as usize & (map.slots.len() - 1);
        while let Some(record) = map.slots[index] {
            if record.hash == hash {
                let stored = configs.bind_set(self.interned(decision, record.key)?)?;
                if configs.equivalent_set_for_probe(query, stored, contexts)? { self.interned(decision, record.value)?; return Ok(Some(record)); }
            }
            index = (index + 1) & (map.slots.len() - 1);
        }
        Ok(None)
    }
    pub(super) fn add_state<'id>(&mut self, decision: u16, candidate: Candidate<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<DfaTarget<'cfg>> {
        self.enter(configs, contexts)?;
        let result = self.add_state_inner(decision, candidate, configs, contexts);
        self.finish_all(result, configs, contexts)
    }
    fn add_state_inner<'id>(&mut self, decision: u16, candidate: Candidate<'cfg>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<DfaTarget<'cfg>> {
        let (id, set, existing) = match candidate {
            Candidate::Error => return Ok(DfaTarget::Error),
            Candidate::Draft(draft) => {
                let (set, phase) = self.ordinary(decision, draft.id)?;
                if phase != Phase::UnnumberedDraft { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
                (draft.id, set, false)
            }
            #[cfg(test)]
            Candidate::Existing(key) => (key.id.0, self.interned(decision, key.id)?, true),
        };
        let set = configs.bind_set(set)?;
        if configs.view_set(set).mode() != SetMode::Local { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
        if !self.decision(decision)?.states.slots.is_empty() {
            let hash = self.hash(set, configs, contexts)?;
            if let Some(record) = self.find(decision, hash, set, configs, contexts)? { return Ok(DfaTarget::Interned(InternedStateKey::at(record.value))); }
        }
        if existing { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
        let number = i32::try_from(self.decision(decision)?.states.len).map_err(|_| capacity_error())?;
        let Body::Ordinary { phase, .. } = &mut self.store.objects.get_mut(id.0).unwrap().body else { unreachable!() };
        *phase = OrdinaryPhase::NumberedUnpublished { number };
        if !configs.view_set(set).readonly() { configs.optimize(set, contexts)?; configs.mark_readonly(set)?; }
        let hash = self.hash(set, configs, contexts)?;
        if self.find(decision, hash, set, configs, contexts)?.is_some() { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
        let store = &mut *self.store;
        let map = &mut store.decisions[usize::from(decision)].states;
        map.prepare(&store.memory)?;
        let index = map.vacant(hash);
        let next_len = map.len.checked_add(1).ok_or_else(capacity_error)?;
        let state = store.objects.get_mut(id.0).ok_or(PredictionError::Artifact(Problem::DfaPhase))?;
        if state.decision != decision { return Err(PredictionError::Artifact(Problem::DfaDecision)); }
        let Body::Ordinary { phase, .. } = &mut state.body else { return Err(PredictionError::Artifact(Problem::DfaPhase)); };
        if !matches!(phase, OrdinaryPhase::NumberedUnpublished { number: assigned } if *assigned == number) { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
        let slot = map.slots.get_mut(index).ok_or(PredictionError::Artifact(Problem::DfaPhase))?;
        if slot.is_some() { return Err(PredictionError::Artifact(Problem::DfaPhase)); }
        let published = InternedId(id);
        *phase = OrdinaryPhase::Interned { number, edges: Block::new() };
        *slot = Some(Record { hash, key: published, value: published });
        map.len = next_len;
        Ok(DfaTarget::Interned(InternedStateKey::at(published)))
    }
    pub(super) fn existing_edge(&mut self, decision: u16, source: InternedStateKey<'cfg>, token: i32) -> Result<Option<DfaTarget<'cfg>>> {
        self.ready()?;
        let result = (|| {
            self.interned(decision, source.id)?;
            let Body::Ordinary { phase: OrdinaryPhase::Interned { edges, .. }, .. } = &self.state(decision, source.id.0)?.body else { unreachable!() };
            let target = token.checked_add(1).and_then(|index| usize::try_from(index).ok()).and_then(|index| edges.get(index)).copied().flatten();
            target.map(|target| match target {
                StoredTarget::Error => Ok(DfaTarget::Error),
                StoredTarget::Interned(id) => self.interned(decision, id).map(|_| DfaTarget::Interned(InternedStateKey::at(id))),
            }).transpose()
        })();
        self.finish(result)
    }
    pub(super) fn add_edge<'id>(&mut self, decision: u16, from: Option<InternedStateKey<'cfg>>, token: i32, to: Option<Candidate<'cfg>>, configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>) -> Result<Option<DfaTarget<'cfg>>> {
        self.enter(configs, contexts)?;
        let result = (|| {
            let Some(to) = to else { return Ok(None); };
            let to = self.add_state_inner(decision, to, configs, contexts)?;
            let Some(from) = from else { return Ok(Some(to)); };
            if !(-1..=368).contains(&token) { return Ok(Some(to)); }
            self.interned(decision, from.id)?;
            let target = match to { DfaTarget::Error => StoredTarget::Error, DfaTarget::Interned(key) => { self.interned(decision, key.id)?; StoredTarget::Interned(key.id) } };
            let Body::Ordinary { phase: OrdinaryPhase::Interned { edges, .. }, .. } = &mut self.store.objects.get_mut(from.id.0.0).unwrap().body else { unreachable!() };
            if edges.is_empty() {
                let mut admitted = Block::with_capacity(&self.store.memory, ORDINARY_EDGES)?;
                for _ in 0..ORDINARY_EDGES { admitted.push(None); }
                *edges = admitted;
            }
            edges[(token + 1) as usize] = Some(target);
            Ok(Some(to))
        })();
        self.finish_all(result, configs, contexts)
    }
    #[cfg(test)]
    pub(super) fn view_decision(&self, number: u16) -> Result<DecisionView<'_>> {
        self.decision(number).map(|decision| DecisionView { decision })
    }
    #[cfg(test)]
    pub(super) fn state_count(&mut self, decision: u16) -> Result<usize> {
        self.ready()?;
        let result = self.decision(decision).map(|decision| decision.states.len);
        self.finish(result)
    }
    #[cfg(test)]
    pub(super) fn state_at(&mut self, decision: u16, number: i32) -> Result<Option<InternedStateKey<'cfg>>> {
        self.ready()?;
        let result = (|| {
            for record in self.decision(decision)?.states.slots.iter().flatten() {
                self.interned(decision, record.key)?;
                if (StateView { state: self.state(decision, record.key.0)? }).number() == number { return Ok(Some(InternedStateKey::at(record.key))); }
            }
            Ok(None)
        })();
        self.finish(result)
    }
}
