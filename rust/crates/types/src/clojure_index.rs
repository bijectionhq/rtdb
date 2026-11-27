use crate::{AllocationScope,Anomaly,Result};
mod hash;

/// Clojure's hasheq for Java strings.
pub fn clojure_string_hasheq(text: &str) -> i32 {
    let hash = java_string_hash(text);
    if hash == 0 { 0 } else { murmur_finish(murmur_block(0, hash), 4) as i32 }
}

/// Clojure's hasheq for optionally qualified symbols.
pub fn clojure_symbol_hasheq(namespace: Option<&str>, name: &str) -> i32 {
    let mut hash = 0;
    let mut length = 0u32;
    let mut units = name.encode_utf16();
    while let Some(low) = units.next() {
        length = length.wrapping_add(2);
        if let Some(high) = units.next() {
            hash = murmur_block(hash, u32::from(low) | (u32::from(high) << 16));
            length = length.wrapping_add(2);
        } else {
            hash ^= murmur_key(u32::from(low));
        }
    }
    let seed = murmur_finish(hash, length) as i32;
    let namespace_hash = namespace.map_or(0, java_string_hash) as i32;
    seed ^ namespace_hash.wrapping_add(0x9e3779b9u32 as i32).wrapping_add(seed.wrapping_shl(6)).wrapping_add(seed >> 2)
}
fn java_string_hash(text: &str) -> u32 { text.encode_utf16().fold(0u32, |hash, unit| hash.wrapping_mul(31).wrapping_add(u32::from(unit))) }
fn murmur_key(value: u32) -> u32 { value.wrapping_mul(0xcc9e2d51).rotate_left(15).wrapping_mul(0x1b873593) }
fn murmur_block(hash: u32, value: u32) -> u32 { (hash ^ murmur_key(value)).rotate_left(13).wrapping_mul(5).wrapping_add(0xe6546b64) }
fn murmur_finish(mut hash: u32, bytes: u32) -> u32 {
    hash ^= bytes;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^ (hash >> 16)
}

/// Semantic key operations for checked Clojure collection construction.
/// Entries may include values; these operations inspect only their keys.
/// Successful hashes and equivalence results must stay stable during construction.
/// The constructor preserves callback order and stops at the first returned error.
pub trait KeySemantics<T> {
    /// Identifies the null key without hashing or comparing it.
    fn is_null(&self,entry: &T) -> bool;
    /// Computes Clojure hasheq; resident keys may be hashed again during trie construction.
    fn hash(&mut self,entry: &T) -> Result<i32>;
    /// Compares keys in the supplied direction, including identity and numeric-category rules.
    fn equivalent(&mut self,left: &T,right: &T) -> Result<bool>;
}

/// Completed checked entries and their Clojure traversal order.
/// Input entries are moved without cloning and remain alive until this owner drops.
/// A supplied allocation scope must outlive destruction of the owned storage.
///
/// ```compile_fail
/// use rtdb_types::{AllocationScope,ClojureKeyIndex,KeySemantics,MemoryBudget};
/// fn invalid<K: KeySemantics<()>>(keys: &mut K) {
///     let memory = MemoryBudget::new(1 << 20);
///     let scope = AllocationScope::new(&memory).unwrap();
///     let index = ClojureKeyIndex::checked_map(Vec::new(),keys,Some(&scope)).unwrap();
///     drop(scope);
///     drop(index);
/// }
/// ```
pub struct ClojureKeyIndex<'scope,T> {
    entries: Vec<T>,layout: Layout,allocation: Option<&'scope AllocationScope>,
}

enum Layout { Array,Hash(hash::Hash) }

/// A duplicate outcome retaining all input entries through diagnostic construction.
/// It borrows the same allocation scope as the attempted index and exposes no owned extraction.
///
/// ```compile_fail
/// use rtdb_types::{AllocationScope,CheckedIndex,ClojureKeyIndex,KeySemantics,MemoryBudget};
/// fn invalid<K: KeySemantics<()>>(keys: &mut K) {
///     let memory = MemoryBudget::new(1 << 20);
///     let scope = AllocationScope::new(&memory).unwrap();
///     let CheckedIndex::Duplicate(duplicate) = ClojureKeyIndex::checked_map(vec![(),()],keys,Some(&scope)).unwrap() else { return; };
///     drop(scope);
///     drop(duplicate);
/// }
/// ```
pub struct DuplicateKey<'scope,T> {
    entries: Vec<T>,earlier: usize,incoming: usize,diagnostic: usize,allocation: Option<&'scope AllocationScope>,
}

/// Construction either completes or identifies the first duplicate in factory order.
pub enum CheckedIndex<'scope,T> {
    /// Unique entries in the selected array-map or HAMT representation.
    Unique(ClojureKeyIndex<'scope,T>),
    /// Inputs retained for the caller's duplicate-key diagnostic.
    Duplicate(DuplicateKey<'scope,T>),
}

impl<T> Drop for ClojureKeyIndex<'_,T> { fn drop(&mut self) { let _ = self.allocation; } }
impl<T> Drop for DuplicateKey<'_,T> { fn drop(&mut self) { let _ = self.allocation; } }

impl<'scope,T> ClojureKeyIndex<'scope,T> {
    /// Checks up to eight map entries in earlier-key/later-key array order; larger maps use a HAMT.
    /// The caller admits the input vector and payloads before allocation and retains their charge in `allocation`.
    /// Index storage is reserved before growth; `None` explicitly selects unscoped construction.
    /// Callback and allocation errors drop partial state and propagate without inspecting later keys.
    pub fn checked_map(entries: Vec<T>,keys: &mut impl KeySemantics<T>,allocation: Option<&'scope AllocationScope>) -> Result<CheckedIndex<'scope,T>> {
        if entries.len() > 8 { return Self::hashed(entries,keys,allocation); }
        for earlier in 0..entries.len() {
            for incoming in earlier + 1..entries.len() {
                if keys.equivalent(&entries[earlier],&entries[incoming])? {
                    return Ok(CheckedIndex::Duplicate(DuplicateKey { entries,earlier,incoming,diagnostic: earlier,allocation }));
                }
            }
        }
        Ok(CheckedIndex::Unique(Self { entries,layout: Layout::Array,allocation }))
    }

    /// Checks set members using transient HAMT insertion, including for empty and small sets.
    /// Input admission, optional scope and early-error contracts are the same as `checked_map`.
    pub fn checked_set(entries: Vec<T>,keys: &mut impl KeySemantics<T>,allocation: Option<&'scope AllocationScope>) -> Result<CheckedIndex<'scope,T>> {
        Self::hashed(entries,keys,allocation)
    }

    fn hashed(entries: Vec<T>,keys: &mut impl KeySemantics<T>,allocation: Option<&'scope AllocationScope>) -> Result<CheckedIndex<'scope,T>> {
        match hash::build(&entries,keys,allocation)? {
            hash::Built::Unique(hash) => Ok(CheckedIndex::Unique(Self { entries,layout: Layout::Hash(hash),allocation })),
            hash::Built::Duplicate { earlier,incoming } => Ok(CheckedIndex::Duplicate(DuplicateKey { entries,earlier,incoming,diagnostic: incoming,allocation })),
        }
    }

    /// Returns the number of admitted entries.
    pub fn len(&self) -> usize { self.entries.len() }
    /// Returns whether construction admitted no entries.
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Borrows entries in semantic order with their original zero-based input ordinals.
    /// Traversal allocates no storage and performs no semantic callbacks.
    pub fn entries(&self) -> impl Iterator<Item=(usize,&T)> {
        let order = match &self.layout { Layout::Array => Order::Array(0..self.entries.len()),Layout::Hash(hash) => Order::Hash(hash.order()) };
        order.map(move |ordinal|(ordinal,&self.entries[ordinal]))
    }

    /// Bounds cumulative scoped index charges for this many input entries, including reservation bookkeeping.
    /// Input vector/payload charges and the scope's initial bookkeeping are excluded.
    /// Construction reserves incrementally; callers must not reserve this bound a second time for the same index.
    pub fn allocation_bound(entries: usize) -> Result<usize> { hash::allocation_bound(entries) }
}

impl<T> DuplicateKey<'_,T> {
    /// Returns the resident or earlier input ordinal found equal to the incoming key.
    pub fn earlier_ordinal(&self) -> usize { self.earlier }
    /// Returns the later input ordinal at the duplicate check.
    pub fn incoming_ordinal(&self) -> usize { self.incoming }
    /// Borrows the earlier key for checked array maps and the incoming key for hashed factories.
    /// The caller owns semantic text conversion and its charged diagnostic handoff.
    pub fn diagnostic_key(&self) -> &T { &self.entries[self.diagnostic] }
}

enum Order<'a> { Array(std::ops::Range<usize>),Hash(hash::Order<'a>) }
impl Iterator for Order<'_> {
    type Item = usize;
    fn next(&mut self) -> Option<usize> { match self { Self::Array(range) => range.next(),Self::Hash(order) => order.next() } }
}

fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.types/clojure-index-size","Clojure collection index exceeds addressable storage").with_kind(crate::AnomalyKind::ProgramLimitExceeded) }

fn grow<T>(values: &mut Vec<T>,required: usize,allocation: Option<&AllocationScope>) -> Result<()> {
    if required <= values.capacity() { return Ok(()); }
    let capacity = required.max(values.capacity().checked_mul(2).ok_or_else(size_error)?).max(4);
    let bytes = capacity.checked_mul(std::mem::size_of::<T>()).ok_or_else(size_error)?;
    if bytes > isize::MAX as usize { return Err(size_error()); }
    if let Some(allocation) = allocation { allocation.reserve(bytes)?; }
    values.reserve_exact(capacity - values.len());Ok(())
}

#[cfg(test)]
mod tests;
