use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use std::{collections::{BTreeMap, BTreeSet}, cmp::Ordering, hash::{BuildHasher, Hash, Hasher}};

/// Named fields in the order they were first written, compared as a map.
/// The reference keeps struct children in insertion order, and that order is what it writes; equality, hashing and ordering ignore it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StructFields<V>(Box<IndexMap<String, V, Fixed>>);

/// Iterates a struct's fields in insertion order.
pub type StructFieldsIter<'a, V> = indexmap::map::Iter<'a, String, V>;

/// Members in the order they were first seen, compared as a set.
#[derive(Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderedSet<T: Hash + Eq>(Box<IndexSet<T, Fixed>>);

/// Fixed keys keep hashing deterministic; the collections are boxed so a `LogicalType` or `Value` holding one stays small.
type Fixed = std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

fn stable_hash<T: Hash + ?Sized>(value: &T) -> u64 { Fixed::default().hash_one(value) }

impl<V> StructFields<V> {
    pub fn new() -> Self { Self(Box::default()) }
    pub fn with_capacity(capacity: usize) -> Self { Self(Box::new(IndexMap::with_capacity_and_hasher(capacity, Fixed::default()))) }
    pub fn insert(&mut self, key: String, value: V) -> Option<V> { self.0.insert(key, value) }
    /// Removes a field, keeping the order of the rest.
    pub fn remove<Q: ?Sized + Hash + indexmap::Equivalent<String>>(&mut self, key: &Q) -> Option<V> { self.0.shift_remove(key) }
    pub fn remove_entry<Q: ?Sized + Hash + indexmap::Equivalent<String>>(&mut self, key: &Q) -> Option<(String, V)> { self.0.shift_remove_entry(key) }
    pub fn pop_first(&mut self) -> Option<(String, V)> { self.0.shift_remove_index(0) }
    pub fn first_key_value(&self) -> Option<(&String, &V)> { self.0.first() }
    pub fn last_key_value(&self) -> Option<(&String, &V)> { self.0.last() }
    pub fn append(&mut self, other: &mut Self) { self.0.extend(other.0.drain(..)); }
    pub fn into_keys(self) -> indexmap::map::IntoKeys<String, V> { (*self.0).into_keys() }
    pub fn into_values(self) -> indexmap::map::IntoValues<String, V> { (*self.0).into_values() }
    /// StructFields sorted by name, for callers whose output is defined by name rather than by first appearance.
    pub fn sorted(&self) -> Vec<(&String, &V)> { let mut entries: Vec<_> = self.0.iter().collect(); entries.sort_by(|a, b| a.0.cmp(b.0)); entries }
}

impl<V> Default for StructFields<V> { fn default() -> Self { Self::new() } }
impl<V> std::ops::Deref for StructFields<V> { type Target = IndexMap<String, V, Fixed>; fn deref(&self) -> &Self::Target { &self.0 } }
impl<V> std::ops::DerefMut for StructFields<V> { fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 } }
impl<V: std::fmt::Debug> std::fmt::Debug for StructFields<V> { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_map().entries(self.0.iter()).finish() } }
impl<V: PartialEq> PartialEq for StructFields<V> { fn eq(&self, other: &Self) -> bool { self.0 == other.0 } }
impl<V: Eq> Eq for StructFields<V> {}
impl<V: PartialEq> PartialEq<BTreeMap<String, V>> for StructFields<V> {
    fn eq(&self, other: &BTreeMap<String, V>) -> bool { self.len() == other.len() && other.iter().all(|(name, value)| self.get(name) == Some(value)) }
}
impl<V: PartialEq> PartialEq<StructFields<V>> for BTreeMap<String, V> { fn eq(&self, other: &StructFields<V>) -> bool { other == self } }
impl<V: Hash> Hash for StructFields<V> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut entries: Vec<_> = self.0.iter().map(|entry| stable_hash(&entry)).collect();
        entries.sort_unstable();
        entries.hash(state);
    }
}
impl<V: Ord> PartialOrd for StructFields<V> { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl<V: Ord> Ord for StructFields<V> {
    fn cmp(&self, other: &Self) -> Ordering { self.sorted().cmp(&other.sorted()) }
}
impl<V> FromIterator<(String, V)> for StructFields<V> { fn from_iter<I: IntoIterator<Item = (String, V)>>(iter: I) -> Self { Self(Box::new(iter.into_iter().collect())) } }
impl<V> Extend<(String, V)> for StructFields<V> { fn extend<I: IntoIterator<Item = (String, V)>>(&mut self, iter: I) { self.0.extend(iter) } }
impl<V> IntoIterator for StructFields<V> { type Item = (String, V); type IntoIter = indexmap::map::IntoIter<String, V>; fn into_iter(self) -> Self::IntoIter { (*self.0).into_iter() } }
impl<'a, V> IntoIterator for &'a StructFields<V> { type Item = (&'a String, &'a V); type IntoIter = indexmap::map::Iter<'a, String, V>; fn into_iter(self) -> Self::IntoIter { self.0.iter() } }
impl<'a, V> IntoIterator for &'a mut StructFields<V> { type Item = (&'a String, &'a mut V); type IntoIter = indexmap::map::IterMut<'a, String, V>; fn into_iter(self) -> Self::IntoIter { self.0.iter_mut() } }
impl<V, const N: usize> From<[(String, V); N]> for StructFields<V> { fn from(entries: [(String, V); N]) -> Self { entries.into_iter().collect() } }
impl<V> From<BTreeMap<String, V>> for StructFields<V> { fn from(entries: BTreeMap<String, V>) -> Self { entries.into_iter().collect() } }
impl<V> From<StructFields<V>> for BTreeMap<String, V> { fn from(fields: StructFields<V>) -> Self { fields.into_iter().collect() } }

impl<T: Hash + Eq> OrderedSet<T> {
    pub fn new() -> Self { Self(Box::default()) }
    pub fn with_capacity(capacity: usize) -> Self { Self(Box::new(IndexSet::with_capacity_and_hasher(capacity, Fixed::default()))) }
    /// Removes a member, keeping the order of the rest.
    pub fn remove<Q: ?Sized + Hash + indexmap::Equivalent<T>>(&mut self, value: &Q) -> bool { self.0.shift_remove(value) }
    pub fn take<Q: ?Sized + Hash + indexmap::Equivalent<T>>(&mut self, value: &Q) -> Option<T> { self.0.shift_take(value) }
    pub fn pop_first(&mut self) -> Option<T> { self.0.shift_remove_index(0) }
}
impl<T: Hash + Eq + Ord> OrderedSet<T> {
    /// Members in their total order, for callers whose output is defined by the set rather than by first appearance.
    pub fn sorted(&self) -> Vec<&T> { let mut members: Vec<_> = self.0.iter().collect(); members.sort(); members }
}

impl<T: Hash + Eq> Default for OrderedSet<T> { fn default() -> Self { Self::new() } }
impl<T: Hash + Eq> std::ops::Deref for OrderedSet<T> { type Target = IndexSet<T, Fixed>; fn deref(&self) -> &Self::Target { &self.0 } }
impl<T: Hash + Eq> std::ops::DerefMut for OrderedSet<T> { fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 } }
impl<T: Hash + Eq + std::fmt::Debug> std::fmt::Debug for OrderedSet<T> { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_set().entries(self.0.iter()).finish() } }
impl<T: Hash + Eq> PartialEq for OrderedSet<T> { fn eq(&self, other: &Self) -> bool { self.0 == other.0 } }
impl<T: Hash + Eq> Eq for OrderedSet<T> {}
impl<T: Hash + Eq> Hash for OrderedSet<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut members: Vec<_> = self.0.iter().map(stable_hash).collect();
        members.sort_unstable();
        members.hash(state);
    }
}
impl<T: Hash + Eq + Ord> PartialOrd for OrderedSet<T> { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }
impl<T: Hash + Eq + Ord> Ord for OrderedSet<T> { fn cmp(&self, other: &Self) -> Ordering { self.sorted().cmp(&other.sorted()) } }
impl<T: Hash + Eq> FromIterator<T> for OrderedSet<T> { fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self { Self(Box::new(iter.into_iter().collect())) } }
impl<T: Hash + Eq> Extend<T> for OrderedSet<T> { fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) { self.0.extend(iter) } }
impl<T: Hash + Eq> IntoIterator for OrderedSet<T> { type Item = T; type IntoIter = indexmap::set::IntoIter<T>; fn into_iter(self) -> Self::IntoIter { (*self.0).into_iter() } }
impl<'a, T: Hash + Eq> IntoIterator for &'a OrderedSet<T> { type Item = &'a T; type IntoIter = indexmap::set::Iter<'a, T>; fn into_iter(self) -> Self::IntoIter { self.0.iter() } }
impl<T: Hash + Eq, const N: usize> From<[T; N]> for OrderedSet<T> { fn from(members: [T; N]) -> Self { members.into_iter().collect() } }
impl<T: Hash + Eq> From<BTreeSet<T>> for OrderedSet<T> { fn from(members: BTreeSet<T>) -> Self { members.into_iter().collect() } }
impl<T: Hash + Eq + Ord> From<OrderedSet<T>> for BTreeSet<T> { fn from(members: OrderedSet<T>) -> Self { members.into_iter().collect() } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ordered_collections_stay_pointer_sized() {
        assert_eq!(std::mem::size_of::<StructFields<u8>>(), std::mem::size_of::<usize>());
        assert_eq!(std::mem::size_of::<OrderedSet<u8>>(), std::mem::size_of::<usize>());
    }

    #[test]
    fn fields_keep_insertion_order_and_compare_as_maps() {
        let ab: StructFields<i32> = [("b".into(), 1), ("a".into(), 2)].into();
        let ba: StructFields<i32> = [("a".into(), 2), ("b".into(), 1)].into();
        assert_eq!(ab.keys().collect::<Vec<_>>(), ["b", "a"]);
        assert_eq!(ab, ba);
        assert_eq!(ab.cmp(&ba), Ordering::Equal);
        assert_eq!(stable_hash(&ab), stable_hash(&ba));
        let mut removed = StructFields::from([("c".into(), 0), ("b".into(), 1), ("a".into(), 2)]);
        removed.remove("c");
        assert_eq!(removed.keys().collect::<Vec<_>>(), ["b", "a"]);
    }

    #[test]
    fn ordered_sets_keep_first_seen_order_and_compare_as_sets() {
        let ba: OrderedSet<i32> = [2, 1, 2].into();
        assert_eq!(ba.iter().collect::<Vec<_>>(), [&2, &1]);
        assert_eq!(ba, OrderedSet::from([1, 2]));
        assert_eq!(ba.cmp(&OrderedSet::from([1, 2])), Ordering::Equal);
        assert_eq!(stable_hash(&ba), stable_hash(&OrderedSet::from([1, 2])));
    }
}
