use super::*;
use std::sync::Arc;
use crate::{MemoryBudget, MemoryReservation};

#[derive(Clone, Debug)]
pub(super) struct Key { pub id: i32, pub value: i32, pub hash: i32, pub class: &'static str, pub comparable: bool, pub rank: i32, pub null: bool }
pub(super) fn key(value: i32) -> Key { Key { id: value + 1, value, hash: value, class: "FixtureComparable", comparable: true, rank: value, null: false } }
pub(super) fn collision(value: i32, comparable: bool) -> Key { Key { hash: 1, comparable, ..key(value) } }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Call { Hash(i32), Equals(i32, i32), Class(i32), Compare(i32, i32), Identity(i32) }
#[derive(Default)]
pub(super) struct Ops { pub calls: Vec<Call>, pub reject: Option<Call> }
impl Ops {
    fn call(&mut self, call: Call) -> Result<()> { self.calls.push(call); if self.reject == Some(call) { Err(Anomaly::incorrect("rtdb/keyops-test", "Rejected key operation")) } else { Ok(()) } }
}
pub(super) struct Budget { pub memory: Arc<MemoryBudget> }
impl Admission for Budget {
    type Lease = Arc<MemoryReservation>;
    fn reserve(&mut self, bytes: usize) -> Result<Self::Lease> { self.memory.reserve(bytes) }
}
impl KeyOps<Key, Budget> for Ops {
    type Comparable = &'static str;
    fn is_null(&self, key: &Key) -> bool { key.null }
    fn same_identity(&self, candidate: &Key, stored: &Key) -> bool { candidate.id == stored.id }
    fn hash_code(&mut self, key: &Key, _: &mut Budget) -> Result<i32> { self.call(Call::Hash(key.id))?; Ok(key.hash) }
    fn equals(&mut self, candidate: &Key, stored: &Key, _: &mut Budget) -> Result<bool> { self.call(Call::Equals(candidate.id, stored.id))?; Ok(!stored.null && candidate.class == stored.class && candidate.value == stored.value) }
    fn comparable_class(&mut self, key: &Key, _: &mut Budget) -> Result<Option<Self::Comparable>> { self.call(Call::Class(key.id))?; Ok((!key.null && key.comparable).then_some(key.class)) }
    fn compare(&mut self, class: Self::Comparable, candidate: &Key, stored: &Key, _: &mut Budget) -> Result<i32> { self.call(Call::Compare(candidate.id, stored.id))?; Ok(if !stored.null && class == stored.class { match candidate.rank.cmp(&stored.rank) { Ordering::Less => -1, Ordering::Equal => 0, Ordering::Greater => 1 } } else { 0 }) }
    fn class_name<'a>(&'a self, key: &'a Key) -> &'a str { key.class }
    fn identity_hash_code(&mut self, key: &Key, _: &mut Budget) -> Result<i32> { self.call(Call::Identity(key.id))?; Ok(key.id) }
}
pub(super) type TestMap = Map<Key, i32, Arc<MemoryReservation>>;
pub(super) fn budget() -> Budget { Budget { memory: MemoryBudget::new(usize::MAX) } }
pub(super) fn order(map: &TestMap) -> Vec<i32> { map.try_iter().unwrap().map(|(key, _)| key.value).collect() }
pub(super) fn validate(map: &TestMap) {
    fn tree(map: &TestMap, index: usize, parent: Option<usize>) -> usize {
        let node = map.entry(index); assert_eq!(node.links.parent, parent);
        if node.links.red { assert!(!node.links.left.is_some_and(|child| map.entry(child).links.red)); assert!(!node.links.right.is_some_and(|child| map.entry(child).links.red)); }
        let left = node.links.left.map_or(1, |child| tree(map, child, Some(index))); let right = node.links.right.map_or(1, |child| tree(map, child, Some(index))); assert_eq!(left, right);
        left + usize::from(!node.links.red)
    }
    let mut count = 0;
    for bucket in map.buckets.values() {
        let mut next = bucket.head; let mut previous = None; let mut entries = 0;
        while let Some(index) = next { let entry = map.entry(index); assert_eq!(entry.links.previous, previous); previous = Some(index); next = entry.links.next; entries += 1; assert!(entries <= map.len); }
        assert_eq!(previous, bucket.tail); assert_eq!(entries, bucket.len); count += entries;
        if let Some(root) = bucket.root { assert!(!map.entry(root).links.red); tree(map, root, None); }
    }
    assert_eq!(count, map.len); assert_eq!(map.try_iter().unwrap().len(), map.len);
}

#[test]
fn lazy_lookup_skips_hashing_before_allocation_while_put_hashes_before_admission() {
    let mut map = TestMap::new(None).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    assert!(map.get(&key(7), &mut ops, &mut admission).unwrap().is_none()); assert!(!map.contains_key(&key(7), &mut ops, &mut admission).unwrap()); assert!(ops.calls.is_empty()); assert_eq!(admission.memory.used_bytes(), 0);
    let mut rejected = Budget { memory: MemoryBudget::new(0) }; assert!(map.put(key(7), 9, &mut ops, &mut rejected).is_err()); assert_eq!(ops.calls, vec![Call::Hash(8)]); assert_eq!(rejected.memory.used_bytes(), 0); assert!(map.try_iter().is_err()); assert!(map.finish(&mut rejected).is_err());
}

#[test]
fn null_keys_never_invoke_user_hash_or_equals() {
    let mut map = TestMap::new(None).unwrap(); let mut ops = Ops::default(); let mut admission = budget(); let null = Key { null: true, id: 0, ..key(0) };
    assert_eq!(map.put(null.clone(), 1, &mut ops, &mut admission).unwrap(), None); assert_eq!(map.put(null.clone(), 2, &mut ops, &mut admission).unwrap(), Some(1));
    assert_eq!(map.get(&null, &mut ops, &mut admission).unwrap(), Some(&2)); assert!(ops.calls.is_empty()); validate(&map);
}

#[test]
fn replacement_keeps_the_first_equal_key_object_and_latest_value() {
    let mut map = TestMap::new(Some(2)).unwrap(); let mut ops = Ops::default(); let mut admission = budget(); let first = key(1); let latest = Key { id: 999, ..first.clone() };
    map.put(first.clone(), 1, &mut ops, &mut admission).unwrap(); map.put(key(2), 2, &mut ops, &mut admission).unwrap(); assert_eq!(map.put(latest, 3, &mut ops, &mut admission).unwrap(), Some(1));
    let found = map.try_iter().unwrap().find(|(key, _)| key.value == 1).unwrap(); assert_eq!(found.0.id, first.id); assert_eq!(*found.1, 3); assert_eq!(map.len(), 2); validate(&map);
}

#[test]
fn only_equal_spread_hashes_invoke_key_equality() {
    let mut map = TestMap::new(Some(64)).unwrap(); let mut ops = Ops::default(); let mut admission = budget(); map.put(Key { hash: 1, ..key(1) }, 1, &mut ops, &mut admission).unwrap(); ops.calls.clear();
    assert!(map.get(&Key { id: 99, hash: 65, ..key(2) }, &mut ops, &mut admission).unwrap().is_none()); assert_eq!(ops.calls, vec![Call::Hash(99)]);
}

#[test]
fn explicit_zero_small_and_default_capacities_follow_distinct_lazy_growth() {
    for (initial, capacities) in [(Some(0), vec![2, 4, 4, 8]), (Some(2), vec![2, 4, 4, 8]), (None, vec![16, 16, 16, 16])] {
        let mut map = TestMap::new(initial).unwrap(); let mut ops = Ops::default(); let mut admission = budget(); assert_eq!(map.capacity, 0);
        for (index, capacity) in capacities.into_iter().enumerate() { map.put(key(index as i32), index as i32, &mut ops, &mut admission).unwrap(); assert_eq!(map.capacity, capacity); validate(&map); }
    }
    assert!(TestMap::new(Some(usize::MAX)).is_err());
}

#[test]
fn comparable_collision_tree_retains_source_root_and_encounter_links() {
    let mut map = TestMap::new(Some(64)).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    for value in 0..9 { map.put(collision(value, true), value, &mut ops, &mut admission).unwrap(); validate(&map); }
    assert_eq!(order(&map), vec![3, 0, 1, 2, 4, 5, 6, 7, 8]); assert!(ops.calls.iter().all(|call| !matches!(call, Call::Identity(_))));
}

#[test]
fn noncomparable_tree_lookup_searches_equal_keys_across_identity_tie_directions() {
    let mut map = TestMap::new(Some(64)).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    for value in 0..9 { map.put(collision(value, false), value, &mut ops, &mut admission).unwrap(); }
    assert_eq!(order(&map), vec![3, 0, 1, 2, 4, 5, 6, 7, 8]); let query = Key { id: 10000, ..collision(1, false) }; ops.calls.clear();
    assert_eq!(map.get(&query, &mut ops, &mut admission).unwrap(), Some(&1)); assert!(!ops.calls.iter().any(|call| matches!(call, Call::Identity(_))));
    assert_eq!(map.put(query, 99, &mut ops, &mut admission).unwrap(), Some(1)); assert_eq!(map.len(), 9); assert_eq!(map.try_iter().unwrap().find(|(key, _)| key.value == 1).unwrap().0.id, 2); validate(&map);
}

#[test]
fn comparable_zero_unequal_keys_use_subtree_search_before_identity_insertion() {
    let mut map = TestMap::new(Some(64)).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    for value in 0..9 { map.put(Key { rank: 0, ..collision(value, true) }, value, &mut ops, &mut admission).unwrap(); }
    let query = Key { id: 10000, rank: 0, ..collision(1, true) }; assert_eq!(map.put(query, 77, &mut ops, &mut admission).unwrap(), Some(1)); assert_eq!(map.len(), 9); validate(&map);
}

#[test]
fn class_name_ties_use_unsigned_utf16_and_identity_hashes_compare_as_java_ints() {
    let mut ops = Ops::default(); let mut admission = budget(); let supplementary = Key { class: "A𐀀", ..key(1) }; let bmp = Key { class: "A\u{e000}", ..key(2) };
    assert_eq!(TestMap::tie_break(&supplementary, &bmp, &mut ops, &mut admission).unwrap(), Ordering::Less); assert!(ops.calls.is_empty());
    assert_eq!(TestMap::tie_break(&Key { id: -1, ..key(1) }, &Key { id: 1, ..key(2) }, &mut ops, &mut admission).unwrap(), Ordering::Less); assert_eq!(ops.calls, vec![Call::Identity(-1), Call::Identity(1)]);
}

#[test]
fn put_if_absent_preserves_nonnull_values_and_replaces_existing_java_null() {
    let mut map: Map<Key, Option<i32>, Arc<MemoryReservation>> = Map::new(None).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    assert!(matches!(map.put_if_absent(key(1), None, Option::is_none, &mut ops, &mut admission).unwrap(), PutOutcome::Inserted));
    assert!(matches!(map.put_if_absent(Key { id: 99, ..key(1) }, Some(7), Option::is_none, &mut ops, &mut admission).unwrap(), PutOutcome::Replaced(None)));
    assert!(matches!(map.put_if_absent(Key { id: 100, ..key(1) }, Some(8), Option::is_none, &mut ops, &mut admission).unwrap(), PutOutcome::Present(&Some(7))));
    assert_eq!(map.try_iter().unwrap().next().unwrap().0.id, 2); assert_eq!(map.len(), 1);
}

#[test]
fn normalized_maps_retain_collection_leases_and_original_lookup_geometry() {
    let mut admission = budget(); let memory = Arc::clone(&admission.memory); let mut ops = Ops::default(); let mut map = TestMap::new(Some(64)).unwrap();
    for value in 0..9 { map.put(collision(value, false), value, &mut ops, &mut admission).unwrap(); }
    let map = map.finish(&mut admission).unwrap(); drop(admission); assert!(memory.used_bytes() > 0); assert_eq!(map.iter().map(|(key, _)| key.value).collect::<Vec<_>>(), vec![3, 0, 1, 2, 4, 5, 6, 7, 8]);
    let mut lookup = Budget { memory: Arc::clone(&memory) }; assert_eq!(map.get(&Key { id: 1000, ..collision(1, false) }, &mut ops, &mut lookup).unwrap(), Some(&1)); drop(map); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn callback_failures_preserve_readonly_maps_and_prevent_failed_transient_publication() {
    let mut admission = budget(); let mut ops = Ops::default(); let mut map = TestMap::new(None).unwrap(); map.put(collision(1, true), 1, &mut ops, &mut admission).unwrap();
    let query = Key { id: 99, ..collision(2, true) }; ops.reject = Some(Call::Equals(99, 2)); assert!(map.put(query, 2, &mut ops, &mut admission).unwrap_err().code.ends_with("keyops-test")); assert!(map.finish(&mut admission).is_err());
    let mut map = TestMap::new(None).unwrap(); ops.reject = None; map.put(collision(1, true), 1, &mut ops, &mut admission).unwrap(); let map = map.finish(&mut admission).unwrap();
    ops.reject = Some(Call::Hash(99)); assert!(map.get(&Key { id: 99, ..key(2) }, &mut ops, &mut admission).is_err()); ops.reject = None; assert_eq!(map.get(&collision(1, true), &mut ops, &mut admission).unwrap(), Some(&1));
}

#[test]
fn treeification_waits_for_the_source_minimum_table_capacity() {
    let mut map = TestMap::new(None).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    for value in 0..11 { map.put(collision(value, true), value, &mut ops, &mut admission).unwrap(); validate(&map); if value == 8 { assert_eq!(map.capacity, 32); assert!(map.buckets.values().all(|bucket| bucket.root.is_none())); } if value == 9 { assert_eq!(map.capacity, 64); assert!(map.buckets.values().all(|bucket| bucket.root.is_none())); } }
    assert!(map.buckets.values().any(|bucket| bucket.root.is_some()));
}

#[test]
fn resize_preserves_low_high_chain_order_and_all_tree_lookup_results() {
    let mut map = TestMap::new(Some(64)).unwrap(); let mut ops = Ops::default(); let mut admission = budget();
    for value in 0..64 { map.put(Key { hash: if value < 16 { 1 } else if value < 32 { 65 } else { value * 128 + 3 }, ..key(value) }, value, &mut ops, &mut admission).unwrap(); validate(&map); }
    assert_eq!(map.capacity, 128); for value in 0..64 { let query = Key { id: 10000 + value, hash: if value < 16 { 1 } else if value < 32 { 65 } else { value * 128 + 3 }, ..key(value) }; assert_eq!(map.get(&query, &mut ops, &mut admission).unwrap(), Some(&value)); }
}

#[test]
fn null_keys_share_collision_trees_without_invoking_user_hash_or_equals_on_null() {
    let mut map=TestMap::new(Some(64)).unwrap();let mut ops=Ops::default();let mut admission=budget();
    for value in 0..9 {map.put(Key{hash:0,..key(value)},value,&mut ops,&mut admission).unwrap();}
    let null=Key{null:true,id:0,..key(0)};ops.calls.clear();map.put(null.clone(),99,&mut ops,&mut admission).unwrap();
    assert!(!ops.calls.iter().any(|call|matches!(call,Call::Hash(0)|Call::Equals(0,_))));validate(&map);
    assert_eq!(map.get(&null,&mut ops,&mut admission).unwrap(),Some(&99));assert_eq!(map.get(&Key{hash:0,id:100,..key(0)},&mut ops,&mut admission).unwrap(),Some(&0));assert_eq!(map.len(),10);
}

#[test]
fn source_equal_keys_with_different_hashes_are_not_globally_deduplicated() {
    let mut map=TestMap::new(Some(64)).unwrap();let mut ops=Ops::default();let mut admission=budget();
    map.put(Key{hash:1,..key(1)},1,&mut ops,&mut admission).unwrap();ops.calls.clear();map.put(Key{id:99,hash:65,..key(1)},2,&mut ops,&mut admission).unwrap();
    assert_eq!(map.len(),2);assert_eq!(ops.calls,vec![Call::Hash(99)]);
    assert_eq!(map.get(&Key{id:100,hash:1,..key(1)},&mut ops,&mut admission).unwrap(),Some(&1));assert_eq!(map.get(&Key{id:101,hash:65,..key(1)},&mut ops,&mut admission).unwrap(),Some(&2));validate(&map);
}

#[test]
fn collision_trees_compare_spread_hashes_as_signed_java_ints() {
    fn inorder(map:&TestMap,index:Option<usize>,out:&mut Vec<i32>) {if let Some(index)=index {let entry=map.entry(index);inorder(map,entry.links.left,out);out.push(entry.key.value);inorder(map,entry.links.right,out);}}
    let mut map=TestMap::new(Some(64)).unwrap();let mut ops=Ops::default();let mut admission=budget();
    for value in 0..16 {map.put(Key{hash:if value<8 {1} else {i32::MIN+1},..key(value)},value,&mut ops,&mut admission).unwrap();validate(&map);}
    let bucket=map.buckets.get(&1).unwrap();let mut values=Vec::new();inorder(&map,bucket.root,&mut values);assert_eq!(values,vec![8,9,10,11,12,13,14,15,0,1,2,3,4,5,6,7]);
    assert!(map.try_iter().unwrap().filter(|(key,_)|key.value>=8).all(|(key,_)|key.hash==i32::MIN+1));
}

#[test]
fn resize_preserves_a_single_iterator_retained_tree_node_and_later_tree_put_callbacks() {
    let mut map=TestMap::new(Some(64)).unwrap();let mut ops=Ops::default();let mut admission=budget();
    for value in 0..9 {map.put(Key{hash:0,..key(value)},value,&mut ops,&mut admission).unwrap();}
    assert!(map.remove_if(&mut ops,&mut admission,|key,_,_,_|Ok(key.value!=0)).unwrap());assert!(map.buckets[&0].root.is_some());
    for value in 1..49 {map.put(key(value),value,&mut ops,&mut admission).unwrap();}
    assert_eq!(map.capacity,128);assert_eq!(map.buckets[&0].len,1);assert!(map.buckets[&0].root.is_some());validate(&map);
    ops.calls.clear();map.put(Key{hash:0,..key(99)},99,&mut ops,&mut admission).unwrap();
    assert_eq!(ops.calls,vec![Call::Hash(100),Call::Equals(100,1),Call::Equals(100,1),Call::Class(100),Call::Compare(100,1)]);assert!(map.buckets[&0].root.is_some());validate(&map);
}

#[test]
fn distinct_native_null_wrappers_match_without_calling_nonnull_identity_operations() {
    struct Strict(Ops);
    impl KeyOps<Key,Budget> for Strict {
        type Comparable=&'static str;
        fn is_null(&self,key:&Key)->bool {self.0.is_null(key)}
        fn same_identity(&self,candidate:&Key,stored:&Key)->bool {assert!(!candidate.null);self.0.same_identity(candidate,stored)}
        fn hash_code(&mut self,key:&Key,admission:&mut Budget)->Result<i32> {self.0.hash_code(key,admission)}
        fn equals(&mut self,key:&Key,stored:&Key,admission:&mut Budget)->Result<bool> {self.0.equals(key,stored,admission)}
        fn comparable_class(&mut self,key:&Key,admission:&mut Budget)->Result<Option<Self::Comparable>> {self.0.comparable_class(key,admission)}
        fn compare(&mut self,class:Self::Comparable,key:&Key,stored:&Key,admission:&mut Budget)->Result<i32> {self.0.compare(class,key,stored,admission)}
        fn class_name<'a>(&'a self,key:&'a Key)->&'a str {self.0.class_name(key)}
        fn identity_hash_code(&mut self,key:&Key,admission:&mut Budget)->Result<i32> {self.0.identity_hash_code(key,admission)}
    }
    let mut map=TestMap::new(None).unwrap();let mut ops=Strict(Ops::default());let mut admission=budget();
    let first=Key{null:true,id:100,..key(1)};let replacement=Key{null:true,id:200,..key(2)};
    assert_eq!(map.put(first,1,&mut ops,&mut admission).unwrap(),None);assert_eq!(map.put(replacement,2,&mut ops,&mut admission).unwrap(),Some(1));assert_eq!(map.len(),1);
    assert_eq!(map.get(&Key{null:true,id:300,..key(3)},&mut ops,&mut admission).unwrap(),Some(&2));assert_eq!(map.try_iter().unwrap().next().unwrap().0.id,100);
    let removed=map.remove(&Key{null:true,id:400,..key(4)},&mut ops,&mut admission).unwrap().unwrap();assert_eq!(removed.0.id,100);assert_eq!(removed.1,2);assert!(ops.0.calls.is_empty());validate(&map);
}
