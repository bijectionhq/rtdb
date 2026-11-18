use std::{marker::PhantomData, ops::{Deref, DerefMut}, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget, MemoryReservation};
use crate::parser::grammar::OwnedGrammar;
use super::{PredictionContexts, PredictionError, Result, capacity_error};
use super::context::ContextStore;
use super::dfa::{DfaStore, PredictionDfas};
use super::config::{Configurations, ConfigurationStore};
#[cfg(test)]
use super::config::{ConfigSetKey, SetId};

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;

mod sealed { pub trait Sealed {} }

pub(crate) trait StoreMemory: sealed::Sealed {
    type Lease;
    fn admit(&self, bytes: usize) -> Result<Self::Lease>;
}

#[derive(Clone, Copy)]
pub(crate) struct Scoped<'scope>(pub(super) &'scope AllocationScope);
impl sealed::Sealed for Scoped<'_> {}
impl StoreMemory for Scoped<'_> {
    type Lease = ();
    fn admit(&self, bytes: usize) -> Result<()> { self.0.reserve(bytes).map_err(PredictionError::Admission) }
}

pub(crate) struct Retained { memory: Arc<MemoryBudget> }
impl Retained {
    fn new(memory: &Arc<MemoryBudget>) -> Self { Self { memory: Arc::clone(memory) } }
}
impl sealed::Sealed for Retained {}
impl StoreMemory for Retained {
    type Lease = Arc<MemoryReservation>;
    fn admit(&self, bytes: usize) -> Result<Self::Lease> {
        let bytes = bytes.checked_add(256).ok_or_else(capacity_error)?;
        self.memory.reserve(bytes).map_err(PredictionError::Admission)
    }
}

pub(super) enum Block<T, M: StoreMemory> {
    Empty(PhantomData<M>),
    Allocated { values: Vec<T>, _lease: M::Lease, memory: PhantomData<M> },
}
impl<T, M: StoreMemory> Block<T, M> {
    pub(super) fn new() -> Self { Self::Empty(PhantomData) }
    pub(super) fn with_capacity(memory: &M, capacity: usize) -> Result<Self> {
        if capacity == 0 { return Ok(Self::new()); }
        let bytes = capacity.checked_mul(std::mem::size_of::<T>())
            .filter(|bytes| *bytes <= isize::MAX as usize).ok_or_else(capacity_error)?;
        let lease = memory.admit(bytes)?;
        Ok(Self::Allocated { values: Vec::with_capacity(capacity), _lease: lease, memory: PhantomData })
    }
    pub(super) fn capacity(&self) -> usize {
        match self { Self::Empty(_) => 0, Self::Allocated { values, .. } => values.capacity() }
    }
    pub(super) fn push(&mut self, value: T) {
        match self {
            Self::Allocated { values, .. } if values.len() < values.capacity() => values.push(value),
            _ => panic!("prediction block capacity was admitted before append"),
        }
    }
    #[cfg(test)]
    pub(super) fn clear(&mut self) { if let Self::Allocated { values, .. } = self { values.clear(); } }
    pub(super) fn reserve(&mut self, additional: usize, memory: &M) -> Result<()> {
        let required = self.len().checked_add(additional).ok_or_else(capacity_error)?;
        if required <= self.capacity() { return Ok(()); }
        let capacity = required.max(self.capacity().checked_mul(2).ok_or_else(capacity_error)?).max(16);
        self.grow(memory, capacity)
    }
    pub(super) fn grow(&mut self, memory: &M, capacity: usize) -> Result<()> {
        let mut replacement = Self::with_capacity(memory, capacity)?;
        if let Self::Allocated { values, .. } = self {
            assert!(values.len() <= capacity);
            if let Self::Allocated { values: target, .. } = &mut replacement { target.append(values); }
        }
        *self = replacement;
        Ok(())
    }
}
impl<T, M: StoreMemory> Deref for Block<T, M> {
    type Target = [T];
    fn deref(&self) -> &[T] { match self { Self::Empty(_) => &[], Self::Allocated { values, .. } => values } }
}
impl<T, M: StoreMemory> DerefMut for Block<T, M> {
    fn deref_mut(&mut self) -> &mut [T] { match self { Self::Empty(_) => &mut [], Self::Allocated { values, .. } => values } }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SlotId { pub(super) index: usize, pub(super) generation: u64 }
pub(super) enum Trace { White, Queued { next: Option<SlotId> }, Black }
pub(super) enum Slot<T> {
    Occupied { generation: u64, trace: Trace, value: T },
    Vacant { generation: u64, next_free: Option<usize> },
    Exhausted,
}
pub(super) struct Pool<T, M: StoreMemory> {
    slots: Block<Slot<T>, M>,
    free: Option<usize>,
    queue: Option<SlotId>,
    live: usize,
}
impl<T, M: StoreMemory> Pool<T, M> {
    pub(super) fn new() -> Self { Self { slots: Block::new(), free: None, queue: None, live: 0 } }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize { self.live }
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool { self.live == 0 }
    #[cfg(test)]
    pub(super) fn values(&self) -> impl Iterator<Item = &T> {
        self.slots.iter().filter_map(|slot| match slot { Slot::Occupied { value, .. } => Some(value), _ => None })
    }
    pub(super) fn insert(&mut self, value: T, memory: &M) -> Result<SlotId> {
        let id = if let Some(index) = self.free {
            let Slot::Vacant { generation, next_free } = self.slots[index] else { unreachable!() };
            self.free = next_free;
            self.slots[index] = Slot::Occupied { generation, trace: Trace::White, value };
            SlotId { index, generation }
        } else {
            if self.slots.len() == self.slots.capacity() {
                let capacity = self.slots.capacity().checked_mul(2).ok_or_else(capacity_error)?.max(16);
                self.slots.grow(memory, capacity)?;
            }
            let id = SlotId { index: self.slots.len(), generation: 0 };
            self.slots.push(Slot::Occupied { generation: id.generation, trace: Trace::White, value });
            id
        };
        self.live += 1;
        Ok(id)
    }
    pub(super) fn get(&self, id: SlotId) -> Option<&T> {
        match self.slots.get(id.index)? {
            Slot::Occupied { generation, value, .. } if *generation == id.generation => Some(value),
            _ => None,
        }
    }
    pub(super) fn get_mut(&mut self, id: SlotId) -> Option<&mut T> {
        match self.slots.get_mut(id.index)? {
            Slot::Occupied { generation, value, .. } if *generation == id.generation => Some(value),
            _ => None,
        }
    }
    #[cfg(test)]
    pub(super) fn fixture_generation(&mut self, id: SlotId, next: u64) -> SlotId {
        let Slot::Occupied { generation, .. } = &mut self.slots[id.index] else { panic!("occupied fixture slot") };
        assert_eq!(*generation, id.generation);
        *generation = next;
        SlotId { index: id.index, generation: next }
    }
    pub(super) fn mark(&mut self, id: SlotId) {
        match &mut self.slots[id.index] {
            Slot::Occupied { generation, trace, .. } if *generation == id.generation => {
                if matches!(trace, Trace::White) {
                    *trace = Trace::Queued { next: self.queue };
                    self.queue = Some(id);
                }
            }
            _ => unreachable!("published prediction roots retain their generation"),
        }
    }
    pub(super) fn next_marked(&mut self) -> Option<SlotId> {
        let id = self.queue?;
        let Slot::Occupied { trace: Trace::Queued { next }, .. } = &self.slots[id.index] else { unreachable!() };
        self.queue = *next;
        let Slot::Occupied { trace, .. } = &mut self.slots[id.index] else { unreachable!() };
        *trace = Trace::Black;
        Some(id)
    }
    pub(super) fn sweep(&mut self) {
        assert!(self.queue.is_none());
        for index in 0..self.slots.len() {
            let replacement = match &mut self.slots[index] {
                Slot::Occupied { trace: Trace::White, generation, .. } => {
                    let replacement = match generation.checked_add(1) {
                        Some(generation) => {
                            let slot = Slot::Vacant { generation, next_free: self.free };
                            self.free = Some(index);
                            slot
                        }
                        None => Slot::Exhausted,
                    };
                    self.live -= 1;
                    Some(replacement)
                }
                Slot::Occupied { trace, .. } => { *trace = Trace::White; None }
                _ => None,
            };
            if let Some(replacement) = replacement { self.slots[index] = replacement; }
        }
    }
}

pub(super) enum StoreRef<'owner, T> {
    #[cfg(test)]
    Local(T),
    Borrowed(&'owner mut T),
}
impl<T> Deref for StoreRef<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        match self {
            #[cfg(test)]
            Self::Local(value) => value,
            Self::Borrowed(value) => value,
        }
    }
}
impl<T> DerefMut for StoreRef<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        match self {
            #[cfg(test)]
            Self::Local(value) => value,
            Self::Borrowed(value) => value,
        }
    }
}

pub(crate) struct PredictionStorage {
    dfas: DfaStore<Retained>,
    configs: ConfigurationStore<Retained>,
    contexts: ContextStore<Retained>,
    grammar: OwnedGrammar,
    #[cfg(test)]
    roots: FixtureRoots,
}
impl PredictionStorage {
    pub(crate) fn new(grammar: OwnedGrammar, memory: &Arc<MemoryBudget>) -> Self {
        Self { dfas: DfaStore::new(Retained::new(memory)), configs: ConfigurationStore::new(Retained::new(memory)), contexts: ContextStore::new(Retained::new(memory)), grammar, #[cfg(test)] roots: FixtureRoots::new() }
    }
    pub(super) fn grammar_view(&self) -> crate::parser::grammar::Grammar<'_> { self.grammar.view() }
    #[cfg(test)]
    pub(crate) fn with_contexts<R>(
        &mut self, work: &AllocationScope,
        use_contexts: impl for<'id, 'access> FnOnce(&mut PredictionContexts<'id, 'access, 'access, Retained>) -> Result<R>,
    ) -> Result<R> {
        let session = BorrowSession { owner: self };
        let grammar = session.owner.grammar.view();
        let mut contexts = PredictionContexts::borrowed(&grammar, work, &mut session.owner.contexts);
        use_contexts(&mut contexts)
    }
}
struct BorrowSession<'owner> { owner: &'owner mut PredictionStorage }
impl Drop for BorrowSession<'_> {
    fn drop(&mut self) {
        #[cfg(test)]
        self.owner.roots.mark(&mut self.owner.configs);
        self.owner.dfas.collect(&mut self.owner.configs);
        self.owner.configs.collect(&mut self.owner.contexts);
        self.owner.contexts.collect();
    }
}

pub(crate) struct PredictionAccess<'cfg, 'id, 'g, 'scope> {
    dfas: PredictionDfas<'cfg, 'g, 'scope>,
    configs: Configurations<'cfg, 'id, 'g, 'scope, Retained>,
    contexts: PredictionContexts<'id, 'g, 'scope, Retained>,
    #[cfg(test)]
    roots: &'g mut FixtureRoots,
}
impl<'cfg, 'id, 'g, 'scope> PredictionAccess<'cfg, 'id, 'g, 'scope> {
    pub(super) fn dfa_parts(&mut self) -> (&mut PredictionDfas<'cfg, 'g, 'scope>, &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, &mut PredictionContexts<'id, 'g, 'scope, Retained>) {
        (&mut self.dfas, &mut self.configs, &mut self.contexts)
    }
    #[cfg(test)]
    pub(crate) fn parts(&mut self) -> (&mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, &mut PredictionContexts<'id, 'g, 'scope, Retained>) {
        (&mut self.configs, &mut self.contexts)
    }
    #[cfg(test)]
    pub(super) fn fixture_state(&mut self, index: usize, pair: Option<(ConfigSetKey<'cfg>, ConfigSetKey<'cfg>)>) -> Result<()> {
        self.configs.ready()?;
        let pair = pair.map(|(key, value)| -> Result<(SetId, SetId)> {
            self.configs.bind_set(key.identity())?;
            self.configs.bind_set(value.identity())?;
            Ok((key.identity(), value.identity()))
        }).transpose()?;
        self.roots.states[index] = pair;
        Ok(())
    }
}

pub(crate) fn with_prediction_storage<R>(
    storage: &mut PredictionStorage, work: &AllocationScope,
    use_store: impl for<'id, 'cfg, 'access> FnOnce(&mut PredictionAccess<'cfg, 'id, 'access, 'access>) -> Result<R>,
) -> Result<R> {
    with_prediction_storage_result(storage, work, use_store)
}

pub(super) fn with_prediction_storage_result<R, E: super::FactoryError>(
    storage: &mut PredictionStorage, work: &AllocationScope,
    use_store: impl for<'id, 'cfg, 'access> FnOnce(&mut PredictionAccess<'cfg, 'id, 'access, 'access>) -> std::result::Result<R, E>,
) -> std::result::Result<R, E> {
    let session = BorrowSession { owner: storage };
    let grammar = session.owner.grammar.view();
    let contexts = PredictionContexts::borrowed(&grammar, work, &mut session.owner.contexts);
    let configs = Configurations::borrowed(&grammar, work, &mut session.owner.configs);
    let dfas = PredictionDfas::borrowed(&grammar, work, &mut session.owner.dfas);
    let mut access = PredictionAccess { dfas, configs, contexts, #[cfg(test)] roots: &mut session.owner.roots };
    use_store(&mut access)
}

#[cfg(test)]
struct FixtureRoots { states: [Option<(SetId, SetId)>; 2] }
#[cfg(test)]
impl FixtureRoots {
    fn new() -> Self { Self { states: [None; 2] } }
    fn mark(&self, configs: &mut ConfigurationStore<Retained>) {
        for (key, value) in self.states.iter().flatten() { configs.mark_set(*key); configs.mark_set(*value); }
    }
}
