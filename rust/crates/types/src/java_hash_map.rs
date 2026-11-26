use std::{cmp::Ordering, collections::{BTreeMap, btree_map}, mem::{replace, size_of}};
use crate::{Anomaly, Result};

mod tree;
mod removal;

/// Synchronous authority admitting collection storage before allocation.
pub trait Admission {
    /// An owned reservation that remains effective until this lease is dropped.
    type Lease;
    /// Admits additional bytes or returns an error before the collection allocates them.
    fn reserve(&mut self, bytes: usize) -> Result<Self::Lease>;
}
/// Java key operations performed synchronously on the caller, without retaining this operator or its admission authority.
/// Actual source operations and identity witnesses must remain stable while a key is retained, including when native slots move.
/// Every fallible callback must admit native evaluation scratch before allocating it and retain its reservations through scratch destruction.
/// The engine preserves Java's hash guard even for source classes whose ordinary equals and hashCode violate the usual consistency law.
pub trait KeyOps<K, A: Admission> {
    /// The exact screened `Class<C>` for a class declaring `Comparable<C>`; a matching name alone is insufficient.
    type Comparable: Copy;
    /// Identifies Java null without invoking user key code.
    fn is_null(&self, key: &K) -> bool;
    /// Compares Java object identity; the candidate is non-null and the stored key may be null.
    fn same_identity(&self, candidate: &K, stored: &K) -> bool;
    /// Returns the raw Java hashCode for a non-null key.
    fn hash_code(&mut self, key: &K, admission: &mut A) -> Result<i32>;
    /// Invokes the non-null candidate's ordinary equals on a distinct stored object, which may be Java null.
    fn equals(&mut self, candidate: &K, stored: &K, admission: &mut A) -> Result<bool>;
    /// Implements Java HashMap.comparableClassFor, including its exact class/interface screening.
    /// Accepts Java null and must return None for it.
    fn comparable_class(&mut self, key: &K, admission: &mut A) -> Result<Option<Self::Comparable>>;
    /// Returns zero for a stored object outside the screened class, otherwise the candidate's Java compareTo result.
    fn compare(&mut self, class: Self::Comparable, candidate: &K, stored: &K, admission: &mut A) -> Result<i32>;
    /// Returns the non-null object's Java class name; the engine compares its unsigned UTF16 units.
    fn class_name<'a>(&'a self, key: &'a K) -> &'a str;
    /// Returns the stable Java System.identityHashCode witness for a non-null object; native allocation nonces are not equivalent.
    fn identity_hash_code(&mut self, key: &K, admission: &mut A) -> Result<i32>;
}

/// Result of putIfAbsent, preserving the existing non-null value by a borrow rather than cloning it.
pub enum PutOutcome<'a, V> { Inserted, Replaced(V), Present(&'a V) }
/// Passive single-writer construction transient owning collection storage and its admission leases.
/// Keys/values must arrive with their required payload/provenance owners retained; these leases cover collection storage.
/// A failed mutation prevents further lookup, iteration, mutation or publication of this transient.
pub struct Map<K, V, L> {
    slots: Vec<Slot<K, V>>, free: Option<usize>, buckets: BTreeMap<usize, Bucket>,
    capacity: usize, threshold: usize, len: usize, charges: Vec<L>, failed: bool,
}
/// Immutable encounter/lookup geometry retaining its collection leases, first equal key objects and latest values.
pub struct NormalizedMap<K, V, L> { map: Map<K, V, L> }
/// Borrowed source HashMap encounter iteration.
pub struct Iter<'a, K, V> { slots: &'a [Slot<K, V>], buckets: btree_map::Values<'a, usize, Bucket>, next: Option<usize>, remaining: usize }
enum Slot<K, V> { Occupied(Entry<K, V>), Vacant(Option<usize>) }
struct Entry<K, V> { key: K, value: V, hash: i32, links: Links }
#[derive(Clone, Copy, Default)]
struct Links { previous: Option<usize>, next: Option<usize>, parent: Option<usize>, left: Option<usize>, right: Option<usize>, red: bool }
#[derive(Clone, Copy, Default)]
struct Bucket { head: Option<usize>, tail: Option<usize>, len: usize, root: Option<usize> }
struct PutResult<V> { index: usize, previous: Option<V>, preserved: bool }
enum TreePut { Existing(usize), Vacant { parent: usize, left: bool } }
const MAXIMUM_CAPACITY: usize = 1 << 30;
fn invalid(message: &'static str) -> Anomaly { Anomaly::incorrect("rtdb/java-hash-map", message) }
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb/allocation-overflow", "Java HashMap storage overflow").with_kind(crate::AnomalyKind::ProgramLimitExceeded) }
fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(overflow) }
fn mul(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(overflow) }
fn growth<T>(values: &Vec<T>) -> Result<Option<(usize, usize)>> {
    if values.len() < values.capacity() { return Ok(None); }
    let next = if values.capacity() == 0 { 4 } else { mul(values.capacity(), 2)? };
    Ok(Some((next, add(mul(mul(next, size_of::<T>())?, 16)?, 256)?)))
}
fn grow<T>(values: &mut Vec<T>, next: Option<(usize, usize)>) -> Result<()> {
    if let Some((capacity, _)) = next { values.try_reserve_exact(capacity - values.len()).map_err(|cause| Anomaly::incorrect("rtdb/allocation-failed", cause.to_string()).with_kind(crate::AnomalyKind::ResourceExhausted))?; }
    Ok(())
}

impl<K, V, L> Map<K, V, L> {
    /// Creates an allocation-free transient with default capacity (`None`) or an explicit signed32-bit initial capacity.
    /// The first put hashes its key before lazily allocating collection storage; invalid capacity returns an error.
    pub fn new(initial: Option<usize>) -> Result<Self> {
        let threshold = match initial {
            Some(initial) if initial > i32::MAX as usize => return Err(invalid("Initial HashMap capacity exceeds its signed 32-bit domain")),
            Some(initial) => initial.max(1).checked_next_power_of_two().ok_or_else(overflow)?.min(MAXIMUM_CAPACITY),
            None => 0,
        };
        Ok(Self { slots: Vec::new(), free: None, buckets: BTreeMap::new(), capacity: 0, threshold, len: 0, charges: Vec::new(), failed: false })
    }
    fn usable(&self) -> Result<()> { if self.failed { Err(invalid("A failed HashMap transient cannot be used or published")) } else { Ok(()) } }
    fn admit<A: Admission<Lease = L>>(&mut self, bytes: usize, admission: &mut A) -> Result<()> {
        let next = growth(&self.charges)?;
        let lease = admission.reserve(add(bytes, next.map_or(0, |(_, bytes)| bytes))?)?;
        grow(&mut self.charges, next)?; self.charges.push(lease); Ok(())
    }
    fn entry(&self, index: usize) -> &Entry<K, V> { match &self.slots[index] { Slot::Occupied(entry) => entry, Slot::Vacant(_) => unreachable!("linked HashMap slots are occupied") } }
    fn entry_mut(&mut self, index: usize) -> &mut Entry<K, V> { match &mut self.slots[index] { Slot::Occupied(entry) => entry, Slot::Vacant(_) => unreachable!("linked HashMap slots are occupied") } }
    fn spread<A: Admission<Lease = L>, O: KeyOps<K, A>>(key: &K, ops: &mut O, admission: &mut A) -> Result<i32> {
        if ops.is_null(key) { return Ok(0); }
        let hash = ops.hash_code(key, admission)?; Ok(hash ^ ((hash as u32 >> 16) as i32))
    }
    fn key_equal<A: Admission<Lease = L>, O: KeyOps<K, A>>(key: &K, stored: &K, ops: &mut O, admission: &mut A) -> Result<bool> {
        if ops.is_null(key) { Ok(ops.is_null(stored)) } else if ops.same_identity(key, stored) { Ok(true) } else { ops.equals(key, stored, admission) }
    }
    fn find<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<(usize, usize)>> {
        if self.capacity == 0 { return Ok(None); }
        let hash = Self::spread(key, ops, admission)?; self.find_hashed(hash, key, ops, admission)
    }
    fn find_hashed<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, hash: i32, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<(usize, usize)>> {
        if self.capacity == 0 { return Ok(None); }
        let slot = hash as u32 as usize & (self.capacity - 1);
        let Some(bucket) = self.buckets.get(&slot) else { return Ok(None); };
        let Some(head) = bucket.head else { return Ok(None); }; let first = self.entry(head);
        if first.hash == hash && Self::key_equal(key, &first.key, ops, admission)? { return Ok(Some((slot, head))); }
        let Some(mut next) = first.links.next else { return Ok(None); };
        if let Some(root) = bucket.root { return Ok(self.tree_find(root, hash, key, None, ops, admission)?.map(|index| (slot, index))); }
        loop {
            let entry = self.entry(next);
            if entry.hash == hash && Self::key_equal(key, &entry.key, ops, admission)? { return Ok(Some((slot, next))); }
            match entry.links.next { Some(index) => next = index, None => return Ok(None) }
        }
    }
    /// Borrows the matching value, preserving Java lookup callback order; an uninitialized table skips hashing entirely.
    /// Lookup errors leave the collection unchanged.
    pub fn get<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<&V>> { self.usable()?; Ok(self.find(key, ops, admission)?.map(|(_, index)| &self.entry(index).value)) }
    /// Tests membership with get's callback/error order, including omission of hashing before table initialization.
    pub fn contains_key<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<bool> { self.usable()?; Ok(self.find(key, ops, admission)?.is_some()) }
    /// Inserts or replaces a value, retaining the first equal key object and returning the previous owned value when present.
    /// Key callbacks and storage admission can fail; the failed transient must then be discarded.
    pub fn put<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, key: K, value: V, ops: &mut O, admission: &mut A) -> Result<Option<V>> {
        self.usable()?;
        match self.put_inner(key, value, false, |_| false, ops, admission) { Ok(result) => Ok(result.previous), Err(cause) => { self.failed = true; Err(cause) } }
    }
    /// Inserts on absence or Java-null value, using the pure predicate to identify Java null independently of Rust representation.
    /// An existing non-null value is borrowed; errors invalidate only this unpublished transient.
    pub fn put_if_absent<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, key: K, value: V, is_java_null_value: impl Fn(&V) -> bool, ops: &mut O, admission: &mut A) -> Result<PutOutcome<'_, V>> {
        self.usable()?;
        let result = match self.put_inner(key, value, true, is_java_null_value, ops, admission) { Ok(result) => result, Err(cause) => { self.failed = true; return Err(cause); } };
        Ok(if result.preserved { PutOutcome::Present(&self.entry(result.index).value) } else if let Some(previous) = result.previous { PutOutcome::Replaced(previous) } else { PutOutcome::Inserted })
    }
    fn put_inner<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, key: K, value: V, only_if_absent: bool, is_java_null_value: impl Fn(&V) -> bool, ops: &mut O, admission: &mut A) -> Result<PutResult<V>> {
        let hash = Self::spread(&key, ops, admission)?;
        if self.capacity == 0 { self.admit(4096, admission)?; self.resize(ops, admission)?; }
        let slot = hash as u32 as usize & (self.capacity - 1); let mut bucket = self.buckets.get(&slot).copied().unwrap_or_default();
        let mut parent = None; let mut tree_left = false; let mut existing = None;
        if let Some(head) = bucket.head {
            let first = self.entry(head);
            if first.hash == hash && Self::key_equal(&key, &first.key, ops, admission)? { existing = Some(head); }
            else if let Some(root) = bucket.root {
                match self.tree_put(root, hash, &key, ops, admission)? { TreePut::Existing(index) => existing = Some(index), TreePut::Vacant { parent: found, left } => { parent = Some(found); tree_left = left; } }
            } else {
                let mut next = first.links.next;
                while let Some(index) = next { let entry = self.entry(index); if entry.hash == hash && Self::key_equal(&key, &entry.key, ops, admission)? { existing = Some(index); break; } next = entry.links.next; }
            }
        }
        if let Some(index) = existing {
            if only_if_absent && !is_java_null_value(&self.entry(index).value) { return Ok(PutResult { index, previous: None, preserved: true }); }
            let previous = replace(&mut self.entry_mut(index).value, value); return Ok(PutResult { index, previous: Some(previous), preserved: false });
        }
        let index = self.allocate(Entry { key, value, hash, links: Links::default() }, admission)?;
        if let Some(parent) = parent {
            self.entry_mut(index).links.parent = Some(parent);
            if tree_left { self.entry_mut(parent).links.left = Some(index); } else { self.entry_mut(parent).links.right = Some(index); }
            self.link_after(&mut bucket, Some(parent), index); let root = self.balance_insertion(bucket.root.unwrap(), index); self.front(&mut bucket, root);
        } else {
            let tail = bucket.tail; self.link_after(&mut bucket, tail, index);
            if bucket.len >= 9 {
                if self.capacity >= 64 { self.treeify(&mut bucket, ops, admission)?; }
                else { self.buckets.insert(slot, bucket); self.resize(ops, admission)?; bucket = self.buckets.get(&(hash as u32 as usize & (self.capacity - 1))).copied().unwrap_or_default(); }
            }
        }
        self.buckets.insert(hash as u32 as usize & (self.capacity - 1), bucket);
        self.len = add(self.len, 1)?;
        if self.len > self.threshold { self.resize(ops, admission)?; }
        Ok(PutResult { index, previous: None, preserved: false })
    }
    fn allocate<A: Admission<Lease = L>>(&mut self, entry: Entry<K, V>, admission: &mut A) -> Result<usize> {
        if let Some(index) = self.free {
            let next = match &self.slots[index] { Slot::Vacant(next) => *next, Slot::Occupied(_) => unreachable!() }; self.free = next; self.slots[index] = Slot::Occupied(entry); return Ok(index);
        }
        let next = growth(&self.slots)?; self.admit(add(8192, next.map_or(0, |(_, bytes)| bytes))?, admission)?; grow(&mut self.slots, next)?;
        let index = self.slots.len(); self.slots.push(Slot::Occupied(entry)); Ok(index)
    }
    fn link_after(&mut self, bucket: &mut Bucket, previous: Option<usize>, index: usize) {
        let next = match previous { Some(previous) => self.entry(previous).links.next, None => bucket.head };
        self.entry_mut(index).links.previous = previous; self.entry_mut(index).links.next = next;
        if let Some(previous) = previous { self.entry_mut(previous).links.next = Some(index); } else { bucket.head = Some(index); }
        if let Some(next) = next { self.entry_mut(next).links.previous = Some(index); } else { bucket.tail = Some(index); }
        bucket.len += 1;
    }
    fn front(&mut self, bucket: &mut Bucket, root: usize) {
        bucket.root = Some(root); if bucket.head == Some(root) { return; }
        let links = self.entry(root).links;
        if let Some(previous) = links.previous { self.entry_mut(previous).links.next = links.next; }
        if let Some(next) = links.next { self.entry_mut(next).links.previous = links.previous; } else { bucket.tail = links.previous; }
        self.entry_mut(root).links.previous = None; self.entry_mut(root).links.next = bucket.head;
        if let Some(head) = bucket.head { self.entry_mut(head).links.previous = Some(root); } bucket.head = Some(root);
    }
    fn resize<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, ops: &mut O, admission: &mut A) -> Result<()> {
        let old = self.capacity;
        if old >= MAXIMUM_CAPACITY { self.threshold = i32::MAX as usize; return Ok(()); }
        let capacity = if old != 0 { mul(old, 2)? } else if self.threshold != 0 { self.threshold } else { 16 };
        self.capacity = capacity; self.threshold = if capacity >= MAXIMUM_CAPACITY { i32::MAX as usize } else { capacity * 3 / 4 };
        if old == 0 { return Ok(()); }
        for (slot, previous) in std::mem::take(&mut self.buckets) {
            let mut low = Bucket::default(); let mut high = Bucket::default(); let mut next = previous.head;
            while let Some(index) = next {
                let entry = self.entry(index); next = entry.links.next; let high_bit = entry.hash as u32 as usize & old != 0;
                self.entry_mut(index).links.previous = None; self.entry_mut(index).links.next = None;
                let target = if high_bit { &mut high } else { &mut low }; let tail = target.tail; self.link_after(target, tail, index);
            }
            let split = low.len != 0 && high.len != 0;
            for (slot, mut bucket) in [(slot, low), (slot + old, high)] {
                if bucket.len == 0 { continue; }
                if previous.root.is_some() && previous.len == 1 { bucket.root = previous.root; }
                else if previous.root.is_some() && bucket.len > 6 { if split { self.treeify(&mut bucket, ops, admission)?; } else { bucket.root = previous.root; } }
                else { self.clear_tree(&bucket); }
                self.buckets.insert(slot, bucket);
            }
        }
        Ok(())
    }
    fn clear_tree(&mut self, bucket: &Bucket) {
        let mut next = bucket.head;
        while let Some(index) = next { let entry = self.entry_mut(index); next = entry.links.next; entry.links.parent = None; entry.links.left = None; entry.links.right = None; entry.links.red = false; }
    }
    /// Borrows encounter iteration for a successful construction prefix; failed mutations return an error.
    pub fn try_iter(&self) -> Result<Iter<'_, K, V>> { self.usable()?; Ok(self.iter_unchecked()) }
    fn iter_unchecked(&self) -> Iter<'_, K, V> { Iter { slots: &self.slots, buckets: self.buckets.values(), next: None, remaining: self.len } }
    /// Returns the constructed entry count; after mutation failure this is diagnostic prefix state only.
    pub fn len(&self) -> usize { self.len }
    /// Borrows every reservation still owned by the collection, including retained capacity after `clear`.
    /// Failed transients still expose their storage reservations for accounting.
    /// Payload and provenance reservations owned by entries remain separate.
    pub fn leases(&self) -> &[L] { &self.charges }
    /// Consumes a successful construction prefix into an immutable table without allocating or invoking the admission authority.
    /// The admission argument is a type witness only; a failed transient is dropped and returns an error.
    pub fn finish<A: Admission<Lease = L>>(self, _admission: &mut A) -> Result<NormalizedMap<K, V, L>> { self.usable()?; Ok(NormalizedMap { map: self }) }
}

impl<K, V, L> NormalizedMap<K, V, L> {
    /// Returns the number of retained Java hash-filtered mappings.
    pub fn len(&self) -> usize { self.map.len }
    pub fn is_empty(&self) -> bool { self.map.len == 0 }
    /// Borrows retained entries in source HashMap encounter order without allocating.
    pub fn iter(&self) -> Iter<'_, K, V> { self.map.iter_unchecked() }
    /// Borrows collection reservations for a caller retaining cached storage against another admission scope.
    /// Payload/provenance reservations owned by keys or values remain separate.
    pub fn leases(&self) -> &[L] { self.map.leases() }
    /// Borrows a matching value with the original source lookup geometry and callback order; errors leave this map unchanged.
    pub fn get<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<&V>> { self.map.get(key, ops, admission) }
    /// Tests membership using the same lookup callback/error order as get.
    pub fn contains_key<A: Admission<Lease = L>, O: KeyOps<K, A>>(&self, key: &K, ops: &mut O, admission: &mut A) -> Result<bool> { self.map.contains_key(key, ops, admission) }
}
impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(index) = self.next { let Slot::Occupied(entry) = &self.slots[index] else { unreachable!("encounter links point to occupied HashMap slots") }; self.next = entry.links.next; self.remaining -= 1; return Some((&entry.key, &entry.value)); }
            self.next = self.buckets.next()?.head;
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) { (self.remaining, Some(self.remaining)) }
}
impl<K, V> ExactSizeIterator for Iter<'_, K, V> { fn len(&self) -> usize { self.remaining } }

#[cfg(test)]
mod tests;
