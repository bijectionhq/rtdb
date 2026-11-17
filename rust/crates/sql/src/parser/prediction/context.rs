use std::marker::PhantomData;
use rtdb_types::AllocationScope;
use crate::parser::grammar::{Grammar, State, TransitionView};
use super::{PredictionError, Problem, Result, capacity_error, push, vector};
use super::storage::{Block, Pool, Retained, Scoped, SlotId, StoreMemory, StoreRef};
#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "probe_tests.rs"]
mod probe_tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextKey<'id> { id: ContextId, brand: PhantomData<fn(&'id mut ()) -> &'id mut ()> }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ContextId { Empty, Node(SlotId) }

impl ContextKey<'_> {
    fn at(id: ContextId) -> Self { Self { id, brand: PhantomData } }
    fn empty() -> Self { Self::at(ContextId::Empty) }
    pub(super) fn stored(self) -> ContextId { self.id }
    pub(super) fn from_stored(id: ContextId) -> Self { Self::at(id) }
    fn index(self) -> usize { match self.id { ContextId::Empty => usize::MAX, ContextId::Node(id) => id.index } }
    #[cfg(test)]
    pub(super) fn identity(self) -> ContextId { self.id }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeMode { LocalWildcard, FullExact }
#[derive(Clone, Copy, PartialEq, Eq)]
enum MemoUse { Round, Absent }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ContextShape { Empty, Singleton, Array }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReturnAddress { Follow(u32), Empty }
impl ReturnAddress {
    fn number(self) -> i32 { match self { Self::Follow(state) => state as i32, Self::Empty => i32::MAX } }
}

#[derive(Clone, Copy)]
struct Edge<'id> { parent: Option<ContextKey<'id>>, address: ReturnAddress }
#[derive(Clone, Copy)]
struct StoredEdge { parent: Option<ContextId>, address: ReturnAddress }
impl Edge<'_> {
    fn stored(self) -> StoredEdge { StoredEdge { parent: self.parent.map(|key| key.id), address: self.address } }
}
impl StoredEdge {
    fn branded<'id>(self) -> Edge<'id> { Edge { parent: self.parent.map(ContextKey::at), address: self.address } }
}
struct EqualityLink { right: ContextId, next: usize, previous: Option<ContextId> }
enum Shape<M: StoreMemory> { Singleton(StoredEdge), Array { edges: Block<StoredEdge, M>, continuation: Option<EqualityLink> } }
struct Node<M: StoreMemory> { shape: Shape<M>, hash: i32, local: bool }
impl<M: StoreMemory> Node<M> {
    fn len(&self) -> usize { match &self.shape { Shape::Singleton(_) => 1, Shape::Array { edges, .. } => edges.len() } }
    fn edge(&self, index: usize) -> StoredEdge { match &self.shape { Shape::Singleton(edge) => *edge, Shape::Array { edges, .. } => edges[index] } }
}

#[derive(Clone, Copy)]
struct Record<K, V> { hash: u64, key: K, value: V }
struct Table<K, V, M: StoreMemory> { slots: Block<Option<Record<K, V>>, M>, len: usize }
impl<K: Copy, V: Copy, M: StoreMemory> Table<K, V, M> {
    fn new() -> Self { Self { slots: Block::new(), len: 0 } }
    fn clear(&mut self) { self.slots.fill(None); self.len = 0; }
    fn prepare(&mut self, memory: &M) -> Result<()> {
        if self.len < self.slots.len() / 2 { return Ok(()); }
        let capacity = self.slots.len().checked_mul(2).ok_or_else(capacity_error)?.max(32);
        let mut slots = Block::with_capacity(memory, capacity)?;
        for _ in 0..capacity { slots.push(None); }
        for record in self.slots.iter().flatten().copied() {
            let mut index = record.hash as usize & (capacity - 1);
            while slots[index].is_some() { index = (index + 1) & (capacity - 1); }
            slots[index] = Some(record);
        }
        self.slots = slots;
        Ok(())
    }
    fn insert(&mut self, record: Record<K, V>) {
        let mut index = record.hash as usize & (self.slots.len() - 1);
        while self.slots[index].is_some() { index = (index + 1) & (self.slots.len() - 1); }
        self.slots[index] = Some(record);
        self.len += 1;
    }
}

pub(super) struct ContextStore<M: StoreMemory> {
    nodes: Pool<Node<M>, M>,
    canonical: Table<ContextId, ContextId, M>,
    memory: M,
}
impl<M: StoreMemory> ContextStore<M> {
    pub(super) fn new(memory: M) -> Self { Self { nodes: Pool::new(), canonical: Table::new(), memory } }
    fn node(&self, id: ContextId) -> &Node<M> {
        let ContextId::Node(id) = id else { unreachable!() };
        self.nodes.get(id).expect("branded context retains its occupied slot")
    }
    fn contains(&self, id: ContextId) -> bool {
        match id { ContextId::Empty => true, ContextId::Node(id) => self.nodes.get(id).is_some() }
    }
    pub(super) fn mark(&mut self, id: ContextId) { if let ContextId::Node(id) = id { self.nodes.mark(id); } }
    pub(super) fn collect(&mut self) {
        for record in self.canonical.slots.iter().flatten() {
            if let ContextId::Node(id) = record.key { self.nodes.mark(id); }
            if let ContextId::Node(id) = record.value { self.nodes.mark(id); }
        }
        while let Some(id) = self.nodes.next_marked() {
            let len = self.nodes.get(id).unwrap().len();
            for index in 0..len {
                let parent = self.nodes.get(id).unwrap().edge(index).parent;
                if let Some(ContextId::Node(parent)) = parent { self.nodes.mark(parent); }
            }
        }
        self.nodes.sweep();
    }
    #[cfg(test)]
    pub(super) fn live(&self) -> usize { self.nodes.len() }
    #[cfg(test)]
    pub(super) fn canonical_len(&self) -> usize { self.canonical.len }
    #[cfg(test)]
    pub(super) fn array_edges(&self) -> usize {
        self.nodes.values().map(|node| match &node.shape { Shape::Array { edges, .. } => edges.len(), _ => 0 }).sum()
    }
    #[cfg(test)]
    pub(super) fn array_bytes(length: usize) -> usize { 256 + length * std::mem::size_of::<StoredEdge>() }
    #[cfg(test)]
    pub(super) fn first_canonical_work_bytes() -> usize {
        256 + std::mem::size_of::<Edge<'_>>() + 256 + 16 * std::mem::size_of::<CanonicalFrame<'_>>()
    }
}

#[derive(Clone, Copy)]
enum EqualityFrame<'id> {
    Pair(ContextKey<'id>, ContextKey<'id>),
    Parents { left: ContextKey<'id>, right: ContextKey<'id>, next: usize },
}

struct CandidateArray<'id> { edges: Vec<Edge<'id>> }
struct ArrayMerge<'id> { left: ContextKey<'id>, right: ContextKey<'id>, i: usize, j: usize, candidate: CandidateArray<'id> }
enum MergeFrame<'id> {
    Visit(ContextKey<'id>, ContextKey<'id>),
    SingletonParents { left: ContextKey<'id>, right: ContextKey<'id> },
    Array(ArrayMerge<'id>),
    ArrayParent { merge: ArrayMerge<'id>, address: ReturnAddress },
}

struct CanonicalParents<'id> { original: ContextKey<'id>, next: usize, changed: bool, candidate: CandidateArray<'id> }
enum CanonicalFrame<'id> { Visit(ContextKey<'id>), Parent(CanonicalParents<'id>) }

pub(crate) struct PredictionContexts<'id, 'g, 'scope, M: StoreMemory = Scoped<'scope>> {
    grammar: &'g Grammar<'scope>,
    allocation: &'scope AllocationScope,
    store: StoreRef<'g, ContextStore<M>>,
    equality: Vec<EqualityFrame<'id>>,
    merges: Vec<MergeFrame<'id>>,
    canonicalizing: Vec<CanonicalFrame<'id>>,
    invocations: Vec<State<'g, 'scope>>,
    memo: Table<(ContextKey<'id>, ContextKey<'id>), ContextKey<'id>, Scoped<'scope>>,
    visited: Table<ContextKey<'id>, ContextKey<'id>, Scoped<'scope>>,
    common_parents: Table<Option<ContextKey<'id>>, Option<ContextKey<'id>>, Scoped<'scope>>,
    aborted: bool,
}

impl<M: StoreMemory> Drop for PredictionContexts<'_, '_, '_, M> { fn drop(&mut self) { let _ = self.allocation; } }

pub(crate) struct ContextView<'a, 'id, 'g, 'scope, M: StoreMemory = Scoped<'scope>> { owner: &'a PredictionContexts<'id, 'g, 'scope, M>, key: ContextKey<'id> }
impl<'id, 'g, 'scope, M: StoreMemory> ContextView<'_, 'id, 'g, 'scope, M> {
    pub(crate) fn shape(&self) -> ContextShape { self.owner.shape(self.key) }
    pub(crate) fn is_empty(&self) -> bool { self.owner.is_empty(self.key) }
    pub(crate) fn len(&self) -> usize { self.owner.len(self.key) }
    pub(crate) fn hash(&self) -> i32 { self.owner.hash(self.key) }
    pub(crate) fn parent(&self, index: usize) -> Option<Option<ContextKey<'id>>> {
        (index < self.len()).then(|| self.owner.edge(self.key, index).parent)
    }
    pub(crate) fn follow(&self, index: usize) -> Result<Option<State<'g, 'scope>>> {
        if index >= self.len() { return Err(PredictionError::Artifact(Problem::Context)); }
        match self.owner.edge(self.key, index).address {
            ReturnAddress::Follow(number) => Ok(Some(self.owner.grammar.state(number)?)),
            ReturnAddress::Empty => Ok(None),
        }
    }
}

pub(crate) struct MergeRound<'round, 'id, 'g, 'scope, M: StoreMemory = Scoped<'scope>> { owner: &'round mut PredictionContexts<'id, 'g, 'scope, M> }
impl<M: StoreMemory> Drop for MergeRound<'_, '_, '_, '_, M> { fn drop(&mut self) { self.owner.memo.clear(); } }
impl<'id, 'g, 'scope, M: StoreMemory> MergeRound<'_, 'id, 'g, 'scope, M> {
    pub(crate) fn merge(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, mode: MergeMode) -> Result<ContextKey<'id>> {
        self.owner.ready()?;
        let result = self.owner.merge(left, right, mode, MemoUse::Round);
        self.owner.finish(result)
    }
    pub(super) fn contexts(&mut self) -> &mut PredictionContexts<'id, 'g, 'scope, M> { self.owner }
}

impl<'id, 'g, 'scope> PredictionContexts<'id, 'g, 'scope> {
    pub(super) fn new(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope) -> Self {
        Self::open(grammar, allocation, StoreRef::Local(ContextStore::new(Scoped(allocation))))
    }
}
impl<'id, 'g, 'scope> PredictionContexts<'id, 'g, 'scope, Retained> {
    pub(super) fn borrowed(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope, store: &'g mut ContextStore<Retained>) -> Self {
        Self::open(grammar, allocation, StoreRef::Borrowed(store))
    }
}
impl<'id, 'g, 'scope, M: StoreMemory> PredictionContexts<'id, 'g, 'scope, M> {
    fn open(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope, store: StoreRef<'g, ContextStore<M>>) -> Self {
        Self {
            grammar, allocation, store, equality: Vec::new(), merges: Vec::new(),
            canonicalizing: Vec::new(), invocations: Vec::new(), memo: Table::new(),
            visited: Table::new(), common_parents: Table::new(), aborted: false,
        }
    }
    #[cfg(test)]
    pub(super) fn restore(&mut self, id: ContextId) -> Result<ContextKey<'id>> {
        self.ready()?;
        let result = self.bind(id);
        self.finish(result)
    }
    fn bind(&self, id: ContextId) -> Result<ContextKey<'id>> {
        if self.store.contains(id) { Ok(ContextKey::at(id)) }
        else { Err(PredictionError::Artifact(Problem::Context)) }
    }

    pub(super) fn ready(&self) -> Result<()> { if self.aborted { Err(PredictionError::Aborted) } else { Ok(()) } }
    pub(super) fn finish<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() { self.aborted = true; }
        result
    }
    pub(super) fn grammar(&self) -> &'g Grammar<'scope> { self.grammar }
    pub(super) fn allocation(&self) -> &'scope AllocationScope { self.allocation }
    #[cfg(test)]
    pub(super) fn null_parent_fixture(&mut self, follow: State<'g, 'scope>) -> Result<ContextKey<'id>> {
        self.ready()?;
        let result = if follow.belongs_to(self.grammar) {
            self.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(follow.number()) })
        } else { Err(PredictionError::ForeignOwner) };
        self.finish(result)
    }
    pub(crate) fn empty(&self) -> ContextKey<'id> { ContextKey::empty() }
    pub(crate) fn view(&self, key: ContextKey<'id>) -> ContextView<'_, 'id, 'g, 'scope, M> { ContextView { owner: self, key } }

    fn shape(&self, key: ContextKey<'id>) -> ContextShape {
        if key == self.empty() { return ContextShape::Empty; }
        match &self.store.node(key.id).shape { Shape::Singleton(_) => ContextShape::Singleton, Shape::Array { .. } => ContextShape::Array }
    }
    fn len(&self, key: ContextKey<'id>) -> usize {
        if key == self.empty() { 1 } else { self.store.node(key.id).len() }
    }
    fn edge(&self, key: ContextKey<'id>, index: usize) -> Edge<'id> {
        if key == self.empty() { return Edge { parent: None, address: ReturnAddress::Empty }; }
        self.store.node(key.id).edge(index).branded()
    }
    fn is_empty(&self, key: ContextKey<'id>) -> bool {
        key == self.empty() || self.shape(key) == ContextShape::Array && self.edge(key, 0).address == ReturnAddress::Empty
    }
    fn hash(&self, key: ContextKey<'id>) -> i32 { if key == self.empty() { finish_hash(1, 0) } else { self.store.node(key.id).hash } }
    fn local(&self, key: ContextKey<'id>) -> bool { key == self.empty() || self.store.node(key.id).local }
    fn parent_hash(&self, parent: Option<ContextKey<'id>>) -> i32 { parent.map_or(0, |key| self.hash(key)) }
    fn edge_local(&self, edge: Edge<'id>) -> bool {
        matches!(edge.address, ReturnAddress::Follow(_)) && edge.parent.is_some_and(|parent| self.local(parent))
    }

    fn append(&mut self, node: Node<M>) -> Result<ContextKey<'id>> {
        let store = &mut *self.store;
        let id = store.nodes.insert(node, &store.memory)?;
        Ok(ContextKey::at(ContextId::Node(id)))
    }
    fn singleton_edge(&mut self, edge: Edge<'id>) -> Result<ContextKey<'id>> {
        if edge.address == ReturnAddress::Empty && edge.parent.is_none() { return Ok(self.empty()); }
        let hash = match edge.parent {
            None => finish_hash(1, 0),
            Some(parent) => finish_hash(update_hash(update_hash(1, self.hash(parent)), edge.address.number()), 2),
        };
        self.append(Node { shape: Shape::Singleton(edge.stored()), hash, local: self.edge_local(edge) })
    }
    fn array_hash(&self, edges: &[Edge<'id>]) -> i32 {
        let mut hash = 1;
        for edge in edges { hash = update_hash(hash, self.parent_hash(edge.parent)); }
        for edge in edges { hash = update_hash(hash, edge.address.number()); }
        finish_hash(hash, (edges.len() as u32).wrapping_mul(2))
    }
    fn array(&mut self, candidate: &CandidateArray<'id>) -> Result<ContextKey<'id>> {
        if candidate.edges.is_empty() || candidate.edges.windows(2).any(|pair| pair[0].address.number() >= pair[1].address.number()) {
            return Err(PredictionError::Artifact(Problem::Context));
        }
        let mut edges = Block::with_capacity(&self.store.memory, candidate.edges.len())?;
        for edge in &candidate.edges { edges.push(edge.stored()); }
        let node = Node {
            shape: Shape::Array { edges, continuation: None }, hash: self.array_hash(&candidate.edges),
            local: candidate.edges.iter().all(|edge| self.edge_local(*edge)),
        };
        self.append(node)
    }

    pub(crate) fn singleton(&mut self, parent: ContextKey<'id>, follow: State<'g, 'scope>) -> Result<ContextKey<'id>> {
        self.ready()?;
        let result = if follow.belongs_to(self.grammar) {
            self.singleton_edge(Edge { parent: Some(parent), address: ReturnAddress::Follow(follow.number()) })
        } else { Err(PredictionError::ForeignOwner) };
        self.finish(result)
    }

    pub(crate) fn from_invocations(&mut self, invocations: impl Iterator<Item = State<'g, 'scope>>) -> Result<ContextKey<'id>> {
        self.ready()?;
        let result = self.invocation_context(invocations);
        self.finish(result)
    }
    fn invocation_context(&mut self, invocations: impl Iterator<Item = State<'g, 'scope>>) -> Result<ContextKey<'id>> {
        self.invocations.clear();
        for invocation in invocations { push(&mut self.invocations, invocation, self.allocation)?; }
        let mut context = self.empty();
        while let Some(invocation) = self.invocations.pop() {
            if !invocation.belongs_to(self.grammar) { return Err(PredictionError::ForeignOwner); }
            let Some(TransitionView::Rule { follow, .. }) = invocation.transitions().next() else {
                return Err(PredictionError::Artifact(Problem::Invocation(invocation.number())));
            };
            context = self.singleton_edge(Edge { parent: Some(context), address: ReturnAddress::Follow(follow.number()) })?;
        }
        Ok(context)
    }

    pub(crate) fn equivalent(&mut self, left: ContextKey<'id>, right: ContextKey<'id>) -> Result<bool> {
        self.ready()?;
        let result = self.equal(left, right);
        self.finish(result)
    }
    fn equal(&mut self, left: ContextKey<'id>, right: ContextKey<'id>) -> Result<bool> {
        self.equal_using(left, right, EqualityPolicy::ScopedFrames)
    }
    pub(super) fn equivalent_using(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, policy: EqualityPolicy) -> Result<bool> {
        self.ready()?;
        let result = self.equal_using(left, right, policy);
        self.finish(result)
    }
    fn equal_using(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, policy: EqualityPolicy) -> Result<bool> {
        EqualitySession::new(self, policy).run(left, right)
    }

    fn memo_slot(&mut self, left: ContextKey<'id>, right: ContextKey<'id>) -> Result<Option<usize>> {
        if self.memo.slots.is_empty() { return Ok(None); }
        let hash = pair_hash(self.hash(left), self.hash(right));
        let mut index = hash as usize & (self.memo.slots.len() - 1);
        loop {
            let Some(record) = self.memo.slots[index] else { return Ok(None); };
            if record.hash == hash && self.equal(left, record.key.0)? && self.equal(right, record.key.1)? { return Ok(Some(index)); }
            index = (index + 1) & (self.memo.slots.len() - 1);
        }
    }
    fn memo_get(&mut self, left: ContextKey<'id>, right: ContextKey<'id>) -> Result<Option<ContextKey<'id>>> {
        if let Some(index) = self.memo_slot(left, right)? { return Ok(self.memo.slots[index].map(|record| record.value)); }
        Ok(self.memo_slot(right, left)?.and_then(|index| self.memo.slots[index].map(|record| record.value)))
    }
    fn memo_put(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, value: ContextKey<'id>) -> Result<()> {
        if let Some(index) = self.memo_slot(left, right)? { self.memo.slots[index].as_mut().unwrap().value = value; return Ok(()); }
        self.memo.prepare(&Scoped(self.allocation))?;
        self.memo.insert(Record { hash: pair_hash(self.hash(left), self.hash(right)), key: (left, right), value });
        Ok(())
    }

    fn merged_lookup(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, memo: MemoUse) -> Result<Option<ContextKey<'id>>> {
        if memo == MemoUse::Round { self.memo_get(left, right) } else { Ok(None) }
    }
    fn merged_record(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, value: ContextKey<'id>, memo: MemoUse) -> Result<()> {
        if memo == MemoUse::Round { self.memo_put(left, right, value) } else { Ok(()) }
    }
    pub(crate) fn merge_uncached(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, mode: MergeMode) -> Result<ContextKey<'id>> {
        self.ready()?;
        let result = self.merge(left, right, mode, MemoUse::Absent);
        self.finish(result)
    }

    pub(crate) fn with_merge_round<R>(&mut self, use_round: impl for<'round> FnOnce(&mut MergeRound<'round, 'id, 'g, 'scope, M>) -> Result<R>) -> Result<R> {
        self.with_merge_round_result(use_round)
    }
    pub(super) fn with_merge_round_result<R, E: super::FactoryError>(&mut self, use_round: impl for<'round> FnOnce(&mut MergeRound<'round, 'id, 'g, 'scope, M>) -> std::result::Result<R, E>) -> std::result::Result<R, E> {
        self.ready().map_err(E::from)?;
        self.memo.clear();
        let result = use_round(&mut MergeRound { owner: self });
        if result.is_err() { self.aborted = true; }
        result
    }

    fn candidate(&self, capacity: usize) -> Result<CandidateArray<'id>> { Ok(CandidateArray { edges: vector(self.allocation, capacity)? }) }
    fn pair_array(&mut self, left: Edge<'id>, right: Edge<'id>) -> Result<ContextKey<'id>> {
        let mut candidate = self.candidate(2)?;
        if left.address.number() < right.address.number() { candidate.edges.extend([left, right]); } else { candidate.edges.extend([right, left]); }
        self.array(&candidate)
    }
    fn as_array(&mut self, key: ContextKey<'id>) -> Result<ContextKey<'id>> {
        if self.shape(key) == ContextShape::Array { return Ok(key); }
        let mut candidate = self.candidate(1)?;
        candidate.edges.push(self.edge(key, 0));
        self.array(&candidate)
    }
    fn merge(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, mode: MergeMode, memo: MemoUse) -> Result<ContextKey<'id>> {
        self.merges.clear();
        push(&mut self.merges, MergeFrame::Visit(left, right), self.allocation)?;
        let mut result = self.empty();
        while let Some(frame) = self.merges.pop() {
            match frame {
                MergeFrame::Visit(left, right) => {
                    if self.equal(left, right)? { result = left; continue; }
                    if self.shape(left) != ContextShape::Array && self.shape(right) != ContextShape::Array {
                        if let Some(cached) = self.merged_lookup(left, right, memo)? { result = cached; continue; }
                        let (a, b) = (self.edge(left, 0), self.edge(right, 0));
                        if left == self.empty() || right == self.empty() {
                            result = match mode {
                                MergeMode::LocalWildcard => self.empty(),
                                MergeMode::FullExact => self.pair_array(a, b)?,
                            };
                            self.merged_record(left, right, result, memo)?;
                        } else if a.address == b.address {
                            let (Some(a), Some(b)) = (a.parent, b.parent) else { return Err(PredictionError::Artifact(Problem::Context)); };
                            push(&mut self.merges, MergeFrame::SingletonParents { left, right }, self.allocation)?;
                            push(&mut self.merges, MergeFrame::Visit(a, b), self.allocation)?;
                        } else {
                            let parent = match (a.parent, b.parent) {
                                (Some(a), Some(b)) if self.equal(a, b)? => Some(a),
                                _ => None,
                            };
                            result = if let Some(parent) = parent {
                                self.pair_array(Edge { parent: Some(parent), address: a.address }, Edge { parent: Some(parent), address: b.address })?
                            } else { self.pair_array(a, b)? };
                            self.merged_record(left, right, result, memo)?;
                        }
                    } else {
                        if mode == MergeMode::LocalWildcard && (left == self.empty() || right == self.empty()) { result = self.empty(); continue; }
                        let left = self.as_array(left)?;
                        let right = self.as_array(right)?;
                        if let Some(cached) = self.merged_lookup(left, right, memo)? { result = cached; continue; }
                        let capacity = self.len(left).checked_add(self.len(right)).ok_or_else(capacity_error)?;
                        let candidate = self.candidate(capacity)?;
                        push(&mut self.merges, MergeFrame::Array(ArrayMerge { left, right, i: 0, j: 0, candidate }), self.allocation)?;
                    }
                }
                MergeFrame::SingletonParents { left, right } => {
                    let (a, b) = (self.edge(left, 0), self.edge(right, 0));
                    if a.parent == Some(result) { result = left; }
                    else if b.parent == Some(result) { result = right; }
                    else {
                        result = self.singleton_edge(Edge { parent: Some(result), address: a.address })?;
                        self.merged_record(left, right, result, memo)?;
                    }
                }
                MergeFrame::Array(mut merge) => {
                    let (left_len, right_len) = (self.len(merge.left), self.len(merge.right));
                    if merge.i < left_len && merge.j < right_len {
                        let (a, b) = (self.edge(merge.left, merge.i), self.edge(merge.right, merge.j));
                        if a.address == b.address {
                            merge.i += 1; merge.j += 1;
                            let both_empty = a.address == ReturnAddress::Empty && a.parent.is_none() && b.parent.is_none();
                            let same_parent = match (a.parent, b.parent) { (Some(a), Some(b)) => self.equal(a, b)?, _ => false };
                            if both_empty || same_parent {
                                merge.candidate.edges.push(a);
                                push(&mut self.merges, MergeFrame::Array(merge), self.allocation)?;
                            } else {
                                let (Some(a_parent), Some(b_parent)) = (a.parent, b.parent) else { return Err(PredictionError::Artifact(Problem::Context)); };
                                push(&mut self.merges, MergeFrame::ArrayParent { merge, address: a.address }, self.allocation)?;
                                push(&mut self.merges, MergeFrame::Visit(a_parent, b_parent), self.allocation)?;
                            }
                        } else {
                            if a.address.number() < b.address.number() { merge.candidate.edges.push(a); merge.i += 1; }
                            else { merge.candidate.edges.push(b); merge.j += 1; }
                            push(&mut self.merges, MergeFrame::Array(merge), self.allocation)?;
                        }
                    } else {
                        while merge.i < left_len { merge.candidate.edges.push(self.edge(merge.left, merge.i)); merge.i += 1; }
                        while merge.j < right_len { merge.candidate.edges.push(self.edge(merge.right, merge.j)); merge.j += 1; }
                        result = self.finish_array_merge(merge, memo)?;
                    }
                }
                MergeFrame::ArrayParent { mut merge, address } => {
                    merge.candidate.edges.push(Edge { parent: Some(result), address });
                    push(&mut self.merges, MergeFrame::Array(merge), self.allocation)?;
                }
            }
        }
        Ok(result)
    }

    fn candidate_equal(&mut self, candidate: &CandidateArray<'id>, key: ContextKey<'id>, hash: i32) -> Result<bool> {
        if self.shape(key) != ContextShape::Array || self.len(key) != candidate.edges.len() || self.hash(key) != hash { return Ok(false); }
        for (index, edge) in candidate.edges.iter().enumerate() {
            if edge.address != self.edge(key, index).address { return Ok(false); }
        }
        for (index, edge) in candidate.edges.iter().enumerate() {
            match (edge.parent, self.edge(key, index).parent) {
                (None, None) => {}
                (Some(a), Some(b)) if self.equal(a, b)? => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }
    fn common_slot(&mut self, parent: Option<ContextKey<'id>>) -> Result<Option<usize>> {
        if self.common_parents.slots.is_empty() { return Ok(None); }
        let hash = self.parent_hash(parent) as u32 as u64;
        let mut index = hash as usize & (self.common_parents.slots.len() - 1);
        loop {
            let Some(record) = self.common_parents.slots[index] else { return Ok(None); };
            let equal = if record.hash != hash { false } else {
                match (parent, record.key) { (None, None) => true, (Some(a), Some(b)) => self.equal(a, b)?, _ => false }
            };
            if equal { return Ok(Some(index)); }
            index = (index + 1) & (self.common_parents.slots.len() - 1);
        }
    }
    fn normalize_parents(&mut self, candidate: &mut CandidateArray<'id>) -> Result<()> {
        self.common_parents.clear();
        for edge in &candidate.edges {
            if self.common_slot(edge.parent)?.is_none() {
                self.common_parents.prepare(&Scoped(self.allocation))?;
                self.common_parents.insert(Record { hash: self.parent_hash(edge.parent) as u32 as u64, key: edge.parent, value: edge.parent });
            }
        }
        for edge in &mut candidate.edges {
            let index = self.common_slot(edge.parent)?.ok_or(PredictionError::Artifact(Problem::Context))?;
            edge.parent = self.common_parents.slots[index].unwrap().value;
        }
        Ok(())
    }
    fn finish_array_merge(&mut self, mut merge: ArrayMerge<'id>, memo: MemoUse) -> Result<ContextKey<'id>> {
        let key = if merge.candidate.edges.len() == 1 {
            self.singleton_edge(merge.candidate.edges[0])?
        } else {
            let hash = self.array_hash(&merge.candidate.edges);
            if self.candidate_equal(&merge.candidate, merge.left, hash)? { merge.left }
            else if self.candidate_equal(&merge.candidate, merge.right, hash)? { merge.right }
            else { self.normalize_parents(&mut merge.candidate)?; self.array(&merge.candidate)? }
        };
        self.merged_record(merge.left, merge.right, key, memo)?;
        Ok(key)
    }

    fn canonical_slot(&mut self, context: ContextKey<'id>) -> Result<Option<usize>> {
        if self.store.canonical.slots.is_empty() { return Ok(None); }
        let hash = self.hash(context) as u32 as u64;
        let mut index = hash as usize & (self.store.canonical.slots.len() - 1);
        loop {
            let Some(record) = self.store.canonical.slots[index] else { return Ok(None); };
            if record.hash == hash && self.equal(context, self.bind(record.key)?)? { return Ok(Some(index)); }
            index = (index + 1) & (self.store.canonical.slots.len() - 1);
        }
    }
    fn cache_add(&mut self, context: ContextKey<'id>) -> Result<ContextKey<'id>> {
        if context == self.empty() { return Ok(context); }
        if let Some(index) = self.canonical_slot(context)? { return self.bind(self.store.canonical.slots[index].unwrap().value); }
        let hash = self.hash(context) as u32 as u64;
        let store = &mut *self.store;
        store.canonical.prepare(&store.memory)?;
        store.canonical.insert(Record { hash, key: context.id, value: context.id });
        Ok(context)
    }
    fn visited_get(&self, context: ContextKey<'id>) -> Option<ContextKey<'id>> {
        if self.visited.slots.is_empty() { return None; }
        let mut index = context.index() & (self.visited.slots.len() - 1);
        loop {
            let record = self.visited.slots[index]?;
            if record.key == context { return Some(record.value); }
            index = (index + 1) & (self.visited.slots.len() - 1);
        }
    }
    fn visited_put(&mut self, context: ContextKey<'id>, value: ContextKey<'id>) -> Result<()> {
        if !self.visited.slots.is_empty() {
            let mut index = context.index() & (self.visited.slots.len() - 1);
            while let Some(record) = self.visited.slots[index] {
                if record.key == context { self.visited.slots[index].as_mut().unwrap().value = value; return Ok(()); }
                index = (index + 1) & (self.visited.slots.len() - 1);
            }
        }
        self.visited.prepare(&Scoped(self.allocation))?;
        self.visited.insert(Record { hash: context.index() as u64, key: context, value });
        Ok(())
    }
    pub(crate) fn canonicalize(&mut self, context: ContextKey<'id>) -> Result<ContextKey<'id>> {
        self.ready()?;
        let result = if !self.is_empty(context) && !self.local(context) { Err(PredictionError::Artifact(Problem::CanonicalDomain)) }
        else { self.canonicalize_local(context) };
        self.finish(result)
    }
    fn canonicalize_local(&mut self, context: ContextKey<'id>) -> Result<ContextKey<'id>> {
        self.visited.clear(); self.canonicalizing.clear();
        let mut frame = CanonicalFrame::Visit(context);
        let mut result = self.empty();
        loop {
            match frame {
                CanonicalFrame::Visit(context) => {
                    if self.is_empty(context) { result = context; }
                    else if let Some(cached) = self.visited_get(context) { result = cached; }
                    else if let Some(index) = self.canonical_slot(context)? {
                        result = self.bind(self.store.canonical.slots[index].unwrap().value)?;
                        self.visited_put(context, result)?;
                    } else {
                        let mut candidate = self.candidate(self.len(context))?;
                        for index in 0..self.len(context) { candidate.edges.push(self.edge(context, index)); }
                        let parent = candidate.edges[0].parent.ok_or(PredictionError::Artifact(Problem::CanonicalDomain))?;
                        push(&mut self.canonicalizing, CanonicalFrame::Parent(CanonicalParents { original: context, next: 0, changed: false, candidate }), self.allocation)?;
                        frame = CanonicalFrame::Visit(parent);
                        continue;
                    }
                }
                CanonicalFrame::Parent(mut parents) => {
                    parents.changed |= parents.candidate.edges[parents.next].parent != Some(result);
                    parents.candidate.edges[parents.next].parent = Some(result);
                    parents.next += 1;
                    if parents.next < parents.candidate.edges.len() {
                        let parent = parents.candidate.edges[parents.next].parent.ok_or(PredictionError::Artifact(Problem::CanonicalDomain))?;
                        push(&mut self.canonicalizing, CanonicalFrame::Parent(parents), self.allocation)?;
                        frame = CanonicalFrame::Visit(parent);
                        continue;
                    }
                    result = if parents.changed {
                        if parents.candidate.edges.len() == 1 { self.singleton_edge(parents.candidate.edges[0])? }
                        else { self.array(&parents.candidate)? }
                    } else { parents.original };
                    self.cache_add(result)?;
                    if parents.changed { self.visited_put(result, result)?; }
                    self.visited_put(parents.original, result)?;
                }
            }
            let Some(next) = self.canonicalizing.pop() else { return Ok(result); };
            frame = next;
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum EqualityPolicy { ScopedFrames, LinkedFrames }
struct EqualitySession<'a, 'id, 'g, 'scope, M: StoreMemory> {
    owner: &'a mut PredictionContexts<'id, 'g, 'scope, M>,
    policy: EqualityPolicy,
    head: Option<ContextId>,
}
impl<'a, 'id, 'g, 'scope, M: StoreMemory> EqualitySession<'a, 'id, 'g, 'scope, M> {
    fn new(owner: &'a mut PredictionContexts<'id, 'g, 'scope, M>, policy: EqualityPolicy) -> Self {
        if matches!(policy, EqualityPolicy::ScopedFrames) { owner.equality.clear(); }
        Self { owner, policy, head: None }
    }
    fn save(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, next: usize) -> Result<()> {
        match self.policy {
            EqualityPolicy::ScopedFrames => push(&mut self.owner.equality, EqualityFrame::Parents { left, right, next }, self.owner.allocation),
            EqualityPolicy::LinkedFrames => {
                let ContextId::Node(id) = left.id else { unreachable!() };
                let node = self.owner.store.nodes.get_mut(id).expect("comparison retains its left node");
                let Shape::Array { continuation, .. } = &mut node.shape else { unreachable!() };
                assert!(continuation.is_none());
                *continuation = Some(EqualityLink { right: right.id, next, previous: self.head });
                self.head = Some(left.id);
                Ok(())
            }
        }
    }
    fn pop(&mut self) -> Option<EqualityFrame<'id>> {
        match self.policy {
            EqualityPolicy::ScopedFrames => self.owner.equality.pop(),
            EqualityPolicy::LinkedFrames => {
                let left = self.head?;
                let ContextId::Node(id) = left else { unreachable!() };
                let node = self.owner.store.nodes.get_mut(id).expect("comparison retains its continuation");
                let Shape::Array { continuation, .. } = &mut node.shape else { unreachable!() };
                let link = continuation.take().expect("active array owns its continuation");
                self.head = link.previous;
                Some(EqualityFrame::Parents { left: ContextKey::at(left), right: ContextKey::at(link.right), next: link.next })
            }
        }
    }
    fn run(&mut self, left: ContextKey<'id>, right: ContextKey<'id>) -> Result<bool> {
        let mut frame = EqualityFrame::Pair(left, right);
        loop {
            match frame {
                EqualityFrame::Pair(left, right) => {
                    if left != right {
                        if self.owner.shape(left) != self.owner.shape(right) || self.owner.hash(left) != self.owner.hash(right) { return Ok(false); }
                        match self.owner.shape(left) {
                            ContextShape::Empty => return Ok(false),
                            ContextShape::Singleton => {
                                let (left, right) = (self.owner.edge(left, 0), self.owner.edge(right, 0));
                                if left.address != right.address { return Ok(false); }
                                let (Some(left), Some(right)) = (left.parent, right.parent) else { return Ok(false); };
                                frame = EqualityFrame::Pair(left, right);
                                continue;
                            }
                            ContextShape::Array => {
                                if self.owner.len(left) != self.owner.len(right) { return Ok(false); }
                                for index in 0..self.owner.len(left) {
                                    if self.owner.edge(left, index).address != self.owner.edge(right, index).address { return Ok(false); }
                                }
                                frame = EqualityFrame::Parents { left, right, next: 0 };
                                continue;
                            }
                        }
                    }
                }
                EqualityFrame::Parents { left, right, next } => {
                    if next < self.owner.len(left) {
                        let (a, b) = (self.owner.edge(left, next).parent, self.owner.edge(right, next).parent);
                        match (a, b) {
                            (None, None) => { frame = EqualityFrame::Parents { left, right, next: next + 1 }; continue; }
                            (Some(a), Some(b)) => {
                                self.save(left, right, next + 1)?;
                                frame = EqualityFrame::Pair(a, b);
                                continue;
                            }
                            _ => return Ok(false),
                        }
                    }
                }
            }
            let Some(next) = self.pop() else { return Ok(true); };
            frame = next;
        }
    }
}
impl<M: StoreMemory> Drop for EqualitySession<'_, '_, '_, '_, M> {
    fn drop(&mut self) {
        if matches!(self.policy, EqualityPolicy::LinkedFrames) { while self.pop().is_some() {} }
    }
}

pub(super) fn update_hash(hash: i32, value: i32) -> i32 {
    let value = (value as u32).wrapping_mul(0xcc9e2d51).rotate_left(15).wrapping_mul(0x1b873593);
    ((hash as u32 ^ value).rotate_left(13).wrapping_mul(5).wrapping_add(0xe6546b64)) as i32
}
pub(super) fn finish_hash(hash: i32, words: u32) -> i32 {
    let mut hash = hash as u32 ^ words.wrapping_mul(4);
    hash ^= hash >> 16; hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13; hash = hash.wrapping_mul(0xc2b2ae35);
    (hash ^ hash >> 16) as i32
}
fn pair_hash(left: i32, right: i32) -> u64 { (left as u32 as u64).wrapping_mul(0x9e3779b1) ^ right as u32 as u64 }
