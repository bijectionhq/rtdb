use std::marker::PhantomData;
use rtdb_types::AllocationScope;
use crate::parser::grammar::{Grammar, State, TransitionView};
use super::{PredictionError, Problem, Result, capacity_error, push, reserve, vector};
#[cfg(test)]
#[path = "context_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContextKey<'id> { index: usize, brand: PhantomData<fn(&'id mut ()) -> &'id mut ()> }

impl ContextKey<'_> {
    fn at(index: usize) -> Self { Self { index, brand: PhantomData } }
    fn empty() -> Self { Self::at(usize::MAX) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MergeMode { LocalWildcard, FullExact }
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
enum Shape<'id> { Singleton(Edge<'id>), Array { start: usize, len: usize } }
#[derive(Clone, Copy)]
struct Node<'id> { shape: Shape<'id>, hash: i32, local: bool }

#[derive(Clone, Copy)]
struct Record<K, V> { hash: u64, key: K, value: V }
struct Table<K, V> { slots: Vec<Option<Record<K, V>>>, len: usize }
impl<K: Copy, V: Copy> Table<K, V> {
    fn new() -> Self { Self { slots: Vec::new(), len: 0 } }
    fn clear(&mut self) { self.slots.fill(None); self.len = 0; }
    fn prepare(&mut self, allocation: &AllocationScope) -> Result<()> {
        if self.len < self.slots.len() / 2 { return Ok(()); }
        let capacity = self.slots.len().checked_mul(2).ok_or_else(capacity_error)?.max(32);
        let mut slots = vector(allocation, capacity)?;
        slots.resize(capacity, None);
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

pub(crate) struct PredictionContexts<'id, 'g, 'scope> {
    grammar: &'g Grammar<'scope>,
    allocation: &'scope AllocationScope,
    nodes: Vec<Node<'id>>,
    edges: Vec<Edge<'id>>,
    equality: Vec<EqualityFrame<'id>>,
    merges: Vec<MergeFrame<'id>>,
    canonicalizing: Vec<CanonicalFrame<'id>>,
    invocations: Vec<State<'g, 'scope>>,
    memo: Table<(ContextKey<'id>, ContextKey<'id>), ContextKey<'id>>,
    canonical: Table<ContextKey<'id>, ContextKey<'id>>,
    visited: Table<ContextKey<'id>, ContextKey<'id>>,
    common_parents: Table<Option<ContextKey<'id>>, Option<ContextKey<'id>>>,
    aborted: bool,
}

impl Drop for PredictionContexts<'_, '_, '_> { fn drop(&mut self) { let _ = self.allocation; } }

pub(crate) struct ContextView<'a, 'id, 'g, 'scope> { owner: &'a PredictionContexts<'id, 'g, 'scope>, key: ContextKey<'id> }
impl<'id, 'g, 'scope> ContextView<'_, 'id, 'g, 'scope> {
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

pub(crate) struct MergeRound<'round, 'id, 'g, 'scope> { owner: &'round mut PredictionContexts<'id, 'g, 'scope> }
impl Drop for MergeRound<'_, '_, '_, '_> { fn drop(&mut self) { self.owner.memo.clear(); } }
impl<'id> MergeRound<'_, 'id, '_, '_> {
    pub(crate) fn merge(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, mode: MergeMode) -> Result<ContextKey<'id>> {
        self.owner.ready()?;
        let result = self.owner.merge(left, right, mode);
        self.owner.finish(result)
    }
}

impl<'id, 'g, 'scope> PredictionContexts<'id, 'g, 'scope> {
    pub(super) fn new(grammar: &'g Grammar<'scope>, allocation: &'scope AllocationScope) -> Self {
        Self {
            grammar, allocation, nodes: Vec::new(), edges: Vec::new(), equality: Vec::new(), merges: Vec::new(),
            canonicalizing: Vec::new(), invocations: Vec::new(), memo: Table::new(), canonical: Table::new(),
            visited: Table::new(), common_parents: Table::new(), aborted: false,
        }
    }

    fn ready(&self) -> Result<()> { if self.aborted { Err(PredictionError::Aborted) } else { Ok(()) } }
    fn finish<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() { self.aborted = true; }
        result
    }
    pub(crate) fn empty(&self) -> ContextKey<'id> { ContextKey::empty() }
    pub(crate) fn view(&self, key: ContextKey<'id>) -> ContextView<'_, 'id, 'g, 'scope> { ContextView { owner: self, key } }

    fn shape(&self, key: ContextKey<'id>) -> ContextShape {
        if key == self.empty() { return ContextShape::Empty; }
        match self.nodes[key.index].shape { Shape::Singleton(_) => ContextShape::Singleton, Shape::Array { .. } => ContextShape::Array }
    }
    fn len(&self, key: ContextKey<'id>) -> usize {
        if key == self.empty() { return 1; }
        match self.nodes[key.index].shape { Shape::Singleton(_) => 1, Shape::Array { len, .. } => len }
    }
    fn edge(&self, key: ContextKey<'id>, index: usize) -> Edge<'id> {
        if key == self.empty() { return Edge { parent: None, address: ReturnAddress::Empty }; }
        match self.nodes[key.index].shape { Shape::Singleton(edge) => edge, Shape::Array { start, .. } => self.edges[start + index] }
    }
    fn is_empty(&self, key: ContextKey<'id>) -> bool {
        key == self.empty() || self.shape(key) == ContextShape::Array && self.edge(key, 0).address == ReturnAddress::Empty
    }
    fn hash(&self, key: ContextKey<'id>) -> i32 { if key == self.empty() { finish_hash(1, 0) } else { self.nodes[key.index].hash } }
    fn local(&self, key: ContextKey<'id>) -> bool { key == self.empty() || self.nodes[key.index].local }
    fn parent_hash(&self, parent: Option<ContextKey<'id>>) -> i32 { parent.map_or(0, |key| self.hash(key)) }
    fn edge_local(&self, edge: Edge<'id>) -> bool {
        matches!(edge.address, ReturnAddress::Follow(_)) && edge.parent.is_some_and(|parent| self.local(parent))
    }

    fn append(&mut self, node: Node<'id>) -> Result<ContextKey<'id>> {
        let key = ContextKey::at(self.nodes.len());
        push(&mut self.nodes, node, self.allocation)?;
        Ok(key)
    }
    fn singleton_edge(&mut self, edge: Edge<'id>) -> Result<ContextKey<'id>> {
        if edge.address == ReturnAddress::Empty && edge.parent.is_none() { return Ok(self.empty()); }
        let hash = match edge.parent {
            None => finish_hash(1, 0),
            Some(parent) => finish_hash(update_hash(update_hash(1, self.hash(parent)), edge.address.number()), 2),
        };
        self.append(Node { shape: Shape::Singleton(edge), hash, local: self.edge_local(edge) })
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
        reserve(&mut self.edges, candidate.edges.len(), self.allocation)?;
        reserve(&mut self.nodes, 1, self.allocation)?;
        let start = self.edges.len();
        let node = Node {
            shape: Shape::Array { start, len: candidate.edges.len() }, hash: self.array_hash(&candidate.edges),
            local: candidate.edges.iter().all(|edge| self.edge_local(*edge)),
        };
        self.edges.extend_from_slice(&candidate.edges);
        let key = ContextKey::at(self.nodes.len());
        self.nodes.push(node);
        Ok(key)
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
        self.equality.clear();
        let mut frame = EqualityFrame::Pair(left, right);
        loop {
            match frame {
                EqualityFrame::Pair(left, right) => {
                    if left != right {
                        if self.shape(left) != self.shape(right) || self.hash(left) != self.hash(right) { return Ok(false); }
                        match self.shape(left) {
                            ContextShape::Empty => return Ok(false),
                            ContextShape::Singleton => {
                                let (left, right) = (self.edge(left, 0), self.edge(right, 0));
                                if left.address != right.address { return Ok(false); }
                                let (Some(left), Some(right)) = (left.parent, right.parent) else { return Ok(false); };
                                frame = EqualityFrame::Pair(left, right);
                                continue;
                            }
                            ContextShape::Array => {
                                if self.len(left) != self.len(right) { return Ok(false); }
                                for index in 0..self.len(left) {
                                    if self.edge(left, index).address != self.edge(right, index).address { return Ok(false); }
                                }
                                frame = EqualityFrame::Parents { left, right, next: 0 };
                                continue;
                            }
                        }
                    }
                }
                EqualityFrame::Parents { left, right, next } => {
                    if next < self.len(left) {
                        let (a, b) = (self.edge(left, next).parent, self.edge(right, next).parent);
                        match (a, b) {
                            (None, None) => { frame = EqualityFrame::Parents { left, right, next: next + 1 }; continue; }
                            (Some(a), Some(b)) => {
                                push(&mut self.equality, EqualityFrame::Parents { left, right, next: next + 1 }, self.allocation)?;
                                frame = EqualityFrame::Pair(a, b);
                                continue;
                            }
                            _ => return Ok(false),
                        }
                    }
                }
            }
            let Some(next) = self.equality.pop() else { return Ok(true); };
            frame = next;
        }
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
        self.memo.prepare(self.allocation)?;
        self.memo.insert(Record { hash: pair_hash(self.hash(left), self.hash(right)), key: (left, right), value });
        Ok(())
    }

    pub(crate) fn with_merge_round<R>(&mut self, use_round: impl for<'round> FnOnce(&mut MergeRound<'round, 'id, 'g, 'scope>) -> Result<R>) -> Result<R> {
        self.ready()?;
        self.memo.clear();
        let result = use_round(&mut MergeRound { owner: self });
        self.finish(result)
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
    fn merge(&mut self, left: ContextKey<'id>, right: ContextKey<'id>, mode: MergeMode) -> Result<ContextKey<'id>> {
        self.merges.clear();
        push(&mut self.merges, MergeFrame::Visit(left, right), self.allocation)?;
        let mut result = self.empty();
        while let Some(frame) = self.merges.pop() {
            match frame {
                MergeFrame::Visit(left, right) => {
                    if self.equal(left, right)? { result = left; continue; }
                    if self.shape(left) != ContextShape::Array && self.shape(right) != ContextShape::Array {
                        if let Some(cached) = self.memo_get(left, right)? { result = cached; continue; }
                        let (a, b) = (self.edge(left, 0), self.edge(right, 0));
                        if left == self.empty() || right == self.empty() {
                            result = match mode {
                                MergeMode::LocalWildcard => self.empty(),
                                MergeMode::FullExact => self.pair_array(a, b)?,
                            };
                            self.memo_put(left, right, result)?;
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
                            self.memo_put(left, right, result)?;
                        }
                    } else {
                        if mode == MergeMode::LocalWildcard && (left == self.empty() || right == self.empty()) { result = self.empty(); continue; }
                        let left = self.as_array(left)?;
                        let right = self.as_array(right)?;
                        if let Some(cached) = self.memo_get(left, right)? { result = cached; continue; }
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
                        self.memo_put(left, right, result)?;
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
                        result = self.finish_array_merge(merge)?;
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
                self.common_parents.prepare(self.allocation)?;
                self.common_parents.insert(Record { hash: self.parent_hash(edge.parent) as u32 as u64, key: edge.parent, value: edge.parent });
            }
        }
        for edge in &mut candidate.edges {
            let index = self.common_slot(edge.parent)?.ok_or(PredictionError::Artifact(Problem::Context))?;
            edge.parent = self.common_parents.slots[index].unwrap().value;
        }
        Ok(())
    }
    fn finish_array_merge(&mut self, mut merge: ArrayMerge<'id>) -> Result<ContextKey<'id>> {
        let key = if merge.candidate.edges.len() == 1 {
            self.singleton_edge(merge.candidate.edges[0])?
        } else {
            let hash = self.array_hash(&merge.candidate.edges);
            if self.candidate_equal(&merge.candidate, merge.left, hash)? { merge.left }
            else if self.candidate_equal(&merge.candidate, merge.right, hash)? { merge.right }
            else { self.normalize_parents(&mut merge.candidate)?; self.array(&merge.candidate)? }
        };
        self.memo_put(merge.left, merge.right, key)?;
        Ok(key)
    }

    fn canonical_slot(&mut self, context: ContextKey<'id>) -> Result<Option<usize>> {
        if self.canonical.slots.is_empty() { return Ok(None); }
        let hash = self.hash(context) as u32 as u64;
        let mut index = hash as usize & (self.canonical.slots.len() - 1);
        loop {
            let Some(record) = self.canonical.slots[index] else { return Ok(None); };
            if record.hash == hash && self.equal(context, record.key)? { return Ok(Some(index)); }
            index = (index + 1) & (self.canonical.slots.len() - 1);
        }
    }
    fn cache_add(&mut self, context: ContextKey<'id>) -> Result<ContextKey<'id>> {
        if context == self.empty() { return Ok(context); }
        if let Some(index) = self.canonical_slot(context)? { return Ok(self.canonical.slots[index].unwrap().value); }
        self.canonical.prepare(self.allocation)?;
        self.canonical.insert(Record { hash: self.hash(context) as u32 as u64, key: context, value: context });
        Ok(context)
    }
    fn visited_get(&self, context: ContextKey<'id>) -> Option<ContextKey<'id>> {
        if self.visited.slots.is_empty() { return None; }
        let mut index = context.index & (self.visited.slots.len() - 1);
        loop {
            let record = self.visited.slots[index]?;
            if record.key == context { return Some(record.value); }
            index = (index + 1) & (self.visited.slots.len() - 1);
        }
    }
    fn visited_put(&mut self, context: ContextKey<'id>, value: ContextKey<'id>) -> Result<()> {
        if !self.visited.slots.is_empty() {
            let mut index = context.index & (self.visited.slots.len() - 1);
            while let Some(record) = self.visited.slots[index] {
                if record.key == context { self.visited.slots[index].as_mut().unwrap().value = value; return Ok(()); }
                index = (index + 1) & (self.visited.slots.len() - 1);
            }
        }
        self.visited.prepare(self.allocation)?;
        self.visited.insert(Record { hash: context.index as u64, key: context, value });
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
                        result = self.canonical.slots[index].unwrap().value;
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

fn update_hash(hash: i32, value: i32) -> i32 {
    let value = (value as u32).wrapping_mul(0xcc9e2d51).rotate_left(15).wrapping_mul(0x1b873593);
    ((hash as u32 ^ value).rotate_left(13).wrapping_mul(5).wrapping_add(0xe6546b64)) as i32
}
fn finish_hash(hash: i32, words: u32) -> i32 {
    let mut hash = hash as u32 ^ words.wrapping_mul(4);
    hash ^= hash >> 16; hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13; hash = hash.wrapping_mul(0xc2b2ae35);
    (hash ^ hash >> 16) as i32
}
fn pair_hash(left: i32, right: i32) -> u64 { (left as u32 as u64).wrapping_mul(0x9e3779b1) ^ right as u32 as u64 }
