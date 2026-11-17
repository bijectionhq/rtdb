use super::*;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering as AtomicOrdering}};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Key { value: i32, identity: usize, null: bool }
fn key(value: i32, identity: usize) -> Key { Key { value, identity, null: false } }
#[derive(Debug, PartialEq, Eq)]
enum Call { Hash(usize), Equals(usize, usize), Comparable(usize), Compare(usize, usize), Identity(usize), Predicate(usize) }
struct Ops { calls: Vec<Call>, fail_hash: bool, fail_equal: Option<i32>, fail_compare: bool, hash_by_value: bool }
impl Ops { fn new() -> Self { Self { calls: Vec::new(), fail_hash: false, fail_equal: None, fail_compare: false, hash_by_value: false } } }
struct Lease { bytes: usize, live: Arc<AtomicUsize> }
impl Drop for Lease { fn drop(&mut self) { self.live.fetch_sub(self.bytes, AtomicOrdering::SeqCst); } }
struct Budget { live: Arc<AtomicUsize>, limit: usize, requests: usize }
impl Budget { fn new() -> Self { Self { live: Arc::new(AtomicUsize::new(0)), limit: usize::MAX, requests: 0 } } }
impl Admission for Budget {
    type Lease = Lease;
    fn reserve(&mut self, bytes: usize) -> Result<Lease> {
        self.requests += 1;
        let current = self.live.load(AtomicOrdering::SeqCst);
        if current.checked_add(bytes).is_none_or(|bytes| bytes > self.limit) { return Err(invalid("test collection admission denied")); }
        self.live.fetch_add(bytes, AtomicOrdering::SeqCst);
        Ok(Lease { bytes, live: Arc::clone(&self.live) })
    }
}
impl KeyOps<Key, Budget> for Ops {
    type Comparable = ();
    fn is_null(&self, key: &Key) -> bool { key.null }
    fn same_identity(&self, candidate: &Key, stored: &Key) -> bool { candidate.null && stored.null || candidate.identity == stored.identity }
    fn hash_code(&mut self, key: &Key, _: &mut Budget) -> Result<i32> {
        self.calls.push(Call::Hash(key.identity));
        if self.fail_hash { return Err(invalid("test hash failed")); }
        Ok(if self.hash_by_value { key.value } else { 0 })
    }
    fn equals(&mut self, candidate: &Key, stored: &Key, _: &mut Budget) -> Result<bool> {
        self.calls.push(Call::Equals(candidate.identity, stored.identity));
        if self.fail_equal == Some(stored.value) { return Err(invalid("test equality failed")); }
        Ok(!stored.null && candidate.value == stored.value)
    }
    fn comparable_class(&mut self, key: &Key, _: &mut Budget) -> Result<Option<()>> { self.calls.push(Call::Comparable(key.identity)); Ok(Some(())) }
    fn compare(&mut self, _: (), candidate: &Key, stored: &Key, _: &mut Budget) -> Result<i32> {
        self.calls.push(Call::Compare(candidate.identity, stored.identity));
        if self.fail_compare { return Err(invalid("test comparison failed")); }
        Ok(match candidate.value.cmp(&stored.value) { Ordering::Less => -1, Ordering::Equal => 0, Ordering::Greater => 1 })
    }
    fn class_name<'a>(&'a self, _: &'a Key) -> &'a str { "java.lang.Integer" }
    fn identity_hash_code(&mut self, key: &Key, _: &mut Budget) -> Result<i32> { self.calls.push(Call::Identity(key.identity)); Ok(key.identity as i32) }
}
fn values(map: &Map<Key, i32, Lease>) -> Vec<i32> { map.iter_unchecked().map(|(key, _)| key.value).collect() }
fn populated(order: &[i32], budget: &mut Budget, ops: &mut Ops) -> Map<Key, i32, Lease> {
    let mut map = Map::new(Some(64)).unwrap();
    for value in order { map.put(key(*value, *value as usize + 1), *value * 10, ops, budget).unwrap(); }
    map
}

#[test]
fn remove_hashes_an_unallocated_lookup_while_get_skips_it_and_null_never_calls_user_hash() {
    let mut map: Map<Key, i32, Lease> = Map::new(None).unwrap(); let mut budget = Budget::new(); let mut ops = Ops::new();
    assert!(map.get(&key(1, 101), &mut ops, &mut budget).unwrap().is_none()); assert!(ops.calls.is_empty());
    assert!(map.remove(&key(1, 101), &mut ops, &mut budget).unwrap().is_none()); assert_eq!(ops.calls, [Call::Hash(101)]);
    assert_eq!(map.capacity, 0); assert_eq!(budget.requests, 0);
    let null = Key { value: 0, identity: 202, null: true };
    ops.calls.clear(); map.put(null.clone(), 7, &mut ops, &mut budget).unwrap(); assert!(ops.calls.is_empty());
    ops.fail_hash = true; let removed = map.remove(&null, &mut ops, &mut budget).unwrap().unwrap();
    assert_eq!(removed, (null, 7)); assert!(ops.calls.is_empty()); assert_eq!(map.len(), 0);
    assert_ne!(map.capacity, 0); drop(map); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn removal_returns_the_first_stored_equal_key_after_value_replacement() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = Map::new(None).unwrap();
    map.put(key(7, 11), 1, &mut ops, &mut budget).unwrap();
    assert_eq!(map.put(key(7, 22), 2, &mut ops, &mut budget).unwrap(), Some(1));
    let lookup = key(7, 33); ops.calls.clear();
    let removed = map.remove(&lookup, &mut ops, &mut budget).unwrap().unwrap();
    assert_eq!(removed, (key(7, 11), 2)); assert_eq!(lookup.identity, 33);
    assert_eq!(ops.calls, [Call::Hash(33), Call::Equals(33, 11)]);
    assert!(map.buckets.is_empty()); assert_eq!(map.len(), 0);
}

#[test]
fn list_head_middle_and_tail_removal_preserve_links_and_reuse_freed_slots_without_admission() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[0,1,2,3], &mut budget, &mut ops);
    let slots = map.slots.len(); let requests = budget.requests; let live = budget.live.load(AtomicOrdering::SeqCst); budget.limit = live;
    for (removed, expected) in [(0, vec![1,2,3]), (2, vec![1,3]), (3, vec![1]), (1, vec![])] {
        assert_eq!(map.remove(&key(removed, removed as usize + 100), &mut ops, &mut budget).unwrap().unwrap().1, removed * 10);
        assert_eq!(values(&map), expected); check(&map);
    }
    assert_eq!(budget.requests, requests); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), live);
    map.put(key(4, 5), 40, &mut ops, &mut budget).unwrap(); map.put(key(5, 6), 50, &mut ops, &mut budget).unwrap();
    assert_eq!(map.slots.len(), slots); assert_eq!(budget.requests, requests); assert_eq!(values(&map), [4,5]);
    assert!(matches!(&map.slots[1], Slot::Occupied(entry) if entry.key.value == 4));
    assert!(matches!(&map.slots[3], Slot::Occupied(entry) if entry.key.value == 5)); check(&map);
    drop(map); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn absent_lookups_preserve_allocated_empty_tables_and_collection_leases() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[1], &mut budget, &mut ops);
    map.remove(&key(1, 100), &mut ops, &mut budget).unwrap(); let capacity = map.capacity; let live = budget.live.load(AtomicOrdering::SeqCst);
    let requests = budget.requests; ops.calls.clear();
    assert!(map.remove(&key(2, 200), &mut ops, &mut budget).unwrap().is_none());
    assert_eq!(ops.calls, [Call::Hash(200)]); assert_eq!(map.capacity, capacity); assert_eq!(budget.requests, requests);
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), live); check(&map);
    drop(map); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn failed_hash_equality_and_tree_comparison_leave_links_unchanged_and_poison_only_the_transient() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut empty: Map<Key, i32, Lease> = Map::new(None).unwrap(); ops.fail_hash = true;
    assert!(empty.remove(&key(0, 100), &mut ops, &mut budget).is_err()); assert_eq!(ops.calls, [Call::Hash(100)]);
    assert!(empty.failed); assert!(empty.try_iter().is_err()); assert_eq!(empty.capacity, 0);
    ops = Ops::new(); let mut list = populated(&[0,1,2], &mut budget, &mut ops); let before = values(&list); ops.calls.clear(); ops.fail_equal = Some(1);
    assert!(list.remove(&key(9, 100), &mut ops, &mut budget).is_err());
    assert_eq!(ops.calls, [Call::Hash(100), Call::Equals(100, 1), Call::Equals(100, 2)]);
    assert_eq!(values(&list), before); assert!(list.failed); assert_eq!(list.len(), 3); check(&list); drop(list);
    ops = Ops::new(); let mut tree = populated(&[0,1,2,3,4,5,6,7,8], &mut budget, &mut ops); let before = values(&tree); ops.calls.clear(); ops.fail_compare = true;
    assert!(tree.remove(&key(99, 100), &mut ops, &mut budget).is_err());
    assert_eq!(ops.calls, [Call::Hash(100), Call::Equals(100, 4), Call::Equals(100, 4), Call::Comparable(100), Call::Compare(100, 4)]);
    assert_eq!(values(&tree), before); assert!(tree.failed); check(&tree);
    assert!(tree.finish(&mut budget).is_err()); drop(empty); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn interior_root_removal_swaps_tree_links_without_swapping_payloads_and_moves_the_new_root_first() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[0,1,2,3,4,5,6,7,8], &mut budget, &mut ops);
    assert_eq!(values(&map), [3,0,1,2,4,5,6,7,8]); check(&map); ops.calls.clear(); let requests = budget.requests;
    assert_eq!(map.remove(&key(3, 100), &mut ops, &mut budget).unwrap(), Some((key(3, 4), 30)));
    assert_eq!(ops.calls, [Call::Hash(100), Call::Equals(100, 4)]);
    assert_eq!(values(&map), [4,0,1,2,5,6,7,8]); assert_eq!(budget.requests, requests);
    for (stored, value) in map.iter_unchecked() { assert_eq!(stored.identity, stored.value as usize + 1); assert_eq!(*value, stored.value * 10); }
    check(&map);
}

type Row = (i32, Option<i32>, Option<i32>, Option<i32>, bool);
fn shaped(rows: &[Row], root: i32, budget: &mut Budget, ops: &mut Ops) -> Map<Key, i32, Lease> {
    let order = rows.iter().map(|row| row.0).collect::<Vec<_>>(); let mut map = populated(&order, budget, ops);
    let index_of = |value| rows.iter().position(|row| row.0 == value).unwrap();
    for (index, (_, parent, left, right, red)) in rows.iter().enumerate() {
        let links = &mut map.entry_mut(index).links;
        links.parent = parent.map(index_of); links.left = left.map(index_of); links.right = right.map(index_of); links.red = *red;
    }
    let mut bucket = map.buckets[&0]; map.front(&mut bucket, index_of(root)); map.buckets.insert(0, bucket); check(&map); map
}

#[test]
fn direct_successor_removal_keeps_payload_identity_when_rebalancing_selects_another_root() {
    let mut budget = Budget::new(); let mut ops = Ops::new();
    let mut map = shaped(&[
        (0,Some(1),None,None,true), (1,Some(3),Some(0),Some(2),false), (2,Some(1),None,None,true),
        (3,None,Some(1),Some(4),false), (4,Some(3),None,None,false),
    ], 3, &mut budget, &mut ops);
    assert_eq!(values(&map), [3,0,1,2,4]); ops.calls.clear();
    assert_eq!(map.remove(&key(3, 100), &mut ops, &mut budget).unwrap(), Some((key(3, 4), 30)));
    assert_eq!(values(&map), [1,0,2,4]); assert!(map.buckets[&0].root.is_some());
    assert_eq!(ops.calls, [Call::Hash(100), Call::Equals(100, 4)]);
    for (stored, value) in map.iter_unchecked() { assert_eq!(stored.identity, stored.value as usize + 1); assert_eq!(*value, stored.value * 10); }
    check(&map);
}

#[test]
fn untreeification_uses_root_shape_rather_than_the_split_paths_six_node_threshold() {
    let mut budget = Budget::new(); let mut ops = Ops::new();
    let mut retained = shaped(&[
        (0,Some(1),None,None,true), (1,Some(3),Some(0),Some(2),false), (2,Some(1),None,None,true),
        (3,None,Some(1),Some(5),false), (4,Some(5),None,None,true), (5,Some(3),Some(4),Some(6),false), (6,Some(5),None,None,true),
    ], 3, &mut budget, &mut ops);
    retained.remove(&key(6, 100), &mut ops, &mut budget).unwrap(); assert_eq!(retained.len(), 6); assert!(retained.buckets[&0].root.is_some());
    assert_eq!(values(&retained), [3,0,1,2,4,5]); check(&retained);
    let mut flattened = shaped(&[
        (2,Some(4),None,Some(3),false), (3,Some(2),None,None,true), (4,None,Some(2),Some(6),false),
        (5,Some(6),None,None,false), (6,Some(4),Some(5),Some(7),true), (7,Some(6),None,Some(8),false), (8,Some(7),None,None,true),
    ], 4, &mut budget, &mut ops);
    let requests = budget.requests; flattened.remove(&key(8, 200), &mut ops, &mut budget).unwrap(); assert_eq!(flattened.len(), 6);
    assert!(flattened.buckets[&0].root.is_none()); assert_eq!(values(&flattened), [4,2,3,5,6,7]); assert_eq!(budget.requests, requests); check(&flattened);
}

#[test]
fn mirrored_deletion_traces_preserve_black_height_membership_and_original_key_objects() {
    let ascending = (0..32).collect::<Vec<_>>(); let descending = (0..32).rev().collect::<Vec<_>>();
    let alternating = (0..32).step_by(2).chain((1..32).step_by(2)).collect::<Vec<_>>();
    for insertion in [&ascending, &descending] {
        for removal in [&ascending, &descending, &alternating] {
            let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(insertion, &mut budget, &mut ops); check(&map);
            let requests = budget.requests; let live = budget.live.load(AtomicOrdering::SeqCst); budget.limit = live; let mut present = [true;32];
            for value in removal {
                let removed = map.remove(&key(*value, *value as usize + 1000), &mut ops, &mut budget).unwrap().unwrap();
                assert_eq!(removed, (key(*value, *value as usize + 1), *value * 10)); present[*value as usize] = false; check(&map);
                let mut actual = values(&map); actual.sort(); assert_eq!(actual, (0..32).filter(|value| present[*value as usize]).collect::<Vec<_>>());
                for (key, value) in map.iter_unchecked() { assert_eq!(key.identity, key.value as usize + 1); assert_eq!(*value, key.value * 10); }
            }
            assert_eq!(map.len(), 0); assert!(map.buckets.is_empty()); assert_eq!(budget.requests, requests); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), live);
            drop(map); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
        }
    }
}

fn check(map: &Map<Key, i32, Lease>) { check_roots(map, true); }
fn check_roots(map: &Map<Key, i32, Lease>, root_first: bool) {
    fn tree(map: &Map<Key, i32, Lease>, index: Option<usize>, parent: Option<usize>, low: Option<i32>, high: Option<i32>, seen: &mut [bool]) -> usize {
        let Some(index) = index else { return 1; }; assert!(!seen[index]); seen[index] = true;
        let node = map.entry(index); assert_eq!(node.links.parent, parent);
        assert!(low.is_none_or(|low| node.key.value > low)); assert!(high.is_none_or(|high| node.key.value < high));
        if node.links.red { assert!(!map.removal_red(node.links.left)); assert!(!map.removal_red(node.links.right)); }
        let left = tree(map, node.links.left, Some(index), low, Some(node.key.value), seen);
        let right = tree(map, node.links.right, Some(index), Some(node.key.value), high, seen); assert_eq!(left, right);
        left + usize::from(!node.links.red)
    }
    let mut linked = vec![false; map.slots.len()]; let mut count = 0;
    for (slot, bucket) in &map.buckets {
        let mut next = bucket.head; let mut previous = None; let mut members = 0; let mut tree_seen = vec![false; map.slots.len()];
        if let Some(root) = bucket.root { if root_first { assert_eq!(bucket.head, Some(root)); } assert!(!map.entry(root).links.red); tree(map, Some(root), None, None, None, &mut tree_seen); }
        while let Some(index) = next {
            assert!(!linked[index]); linked[index] = true; members += 1; count += 1;
            let entry = map.entry(index); assert_eq!(entry.links.previous, previous); assert_eq!(entry.hash as u32 as usize & (map.capacity - 1), *slot);
            if bucket.root.is_some() { assert!(tree_seen[index]); }
            else { assert!(entry.links.parent.is_none() && entry.links.left.is_none() && entry.links.right.is_none() && !entry.links.red); }
            previous = Some(index); next = entry.links.next;
        }
        assert_eq!(previous, bucket.tail); assert_eq!(members, bucket.len);
        if bucket.root.is_some() { assert_eq!(tree_seen.iter().filter(|seen| **seen).count(), members); }
    }
    assert_eq!(count, map.len);
    let mut free = map.free; let mut vacant = vec![false; map.slots.len()];
    while let Some(index) = free { assert!(!vacant[index]); vacant[index] = true; let Slot::Vacant(next) = &map.slots[index] else { panic!("free slot must be vacant") }; free = *next; }
    for (index, slot) in map.slots.iter().enumerate() { assert_eq!(linked[index], matches!(slot, Slot::Occupied(_))); assert_eq!(vacant[index], matches!(slot, Slot::Vacant(_))); }
}

#[test]
fn iterator_removal_calls_predicates_before_cached_hash_lookup_and_never_calls_user_hash() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[0,1,2,3], &mut budget, &mut ops);
    ops.calls.clear(); ops.fail_hash = true; let requests = budget.requests;
    assert!(map.remove_if(&mut ops, &mut budget, |key, _, ops, _| {
        ops.calls.push(Call::Predicate(key.identity)); Ok(key.value == 1 || key.value == 2)
    }).unwrap());
    assert_eq!(ops.calls, [Call::Predicate(1), Call::Predicate(2), Call::Equals(2,1), Call::Predicate(3), Call::Equals(3,1), Call::Predicate(4)]);
    assert_eq!(values(&map), [0,3]); assert_eq!(budget.requests, requests); check(&map);
    ops.calls.clear(); assert!(map.remove(&key(0, 100), &mut ops, &mut budget).is_err()); assert_eq!(ops.calls, [Call::Hash(100)]);
}

#[test]
fn iterator_tree_removal_preserves_original_next_traversal_when_head_middle_or_root_changes() {
    let selections: &[&[i32]] = &[&[3], &[0], &[5], &[8], &[3,5], &[3,0,4,7]];
    for selected in selections {
        let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[0,1,2,3,4,5,6,7,8], &mut budget, &mut ops);
        let expected = [3,0,1,2,4,5,6,7,8]; assert_eq!(values(&map), expected); let mut visited = Vec::new();
        ops.calls.clear(); ops.fail_hash = true;
        assert!(map.remove_if(&mut ops, &mut budget, |key, _, ops, _| {
            visited.push(key.value); ops.calls.push(Call::Predicate(key.identity)); Ok(selected.contains(&key.value))
        }).unwrap());
        assert_eq!(visited, expected);
        assert_eq!(values(&map), expected.into_iter().filter(|key| !selected.contains(key)).collect::<Vec<_>>());
        assert!(!ops.calls.iter().any(|call| matches!(call, Call::Hash(_)))); check_roots(&map, false);
        if selected.len() == 1 && selected[0] == 3 {
            let bucket = map.buckets[&0]; assert_eq!(map.entry(bucket.head.unwrap()).key.value, 0); assert_eq!(map.entry(bucket.root.unwrap()).key.value, 4);
            ops.fail_hash = false; assert_eq!(map.get(&key(4, 100), &mut ops, &mut budget).unwrap(), Some(&40));
        }
    }
}

#[test]
fn iterator_removal_keeps_a_single_tree_node_until_public_removal_or_growth_demands_other_geometry() {
    let mut budget = Budget::new(); let mut ops = Ops::new();
    let mut map = shaped(&[
        (0,Some(1),None,None,true), (1,Some(3),Some(0),Some(2),false), (2,Some(1),None,None,true),
        (3,None,Some(1),Some(4),false), (4,Some(3),None,None,false),
    ], 3, &mut budget, &mut ops);
    let mut visited = Vec::new(); ops.calls.clear(); ops.fail_hash = true; let requests = budget.requests;
    assert!(map.remove_if(&mut ops, &mut budget, |key, _, _, _| { visited.push(key.value); Ok(key.value != 2) }).unwrap());
    assert_eq!(visited, [3,0,1,2,4]); assert_eq!(values(&map), [2]); assert!(map.buckets[&0].root.is_some());
    assert_eq!(budget.requests, requests); check_roots(&map, false); ops.fail_hash = false;
    map.put(key(9, 10), 90, &mut ops, &mut budget).unwrap(); assert_eq!(values(&map), [2,9]); assert!(map.buckets[&0].root.is_some()); check(&map);
    assert_eq!(map.remove(&key(2, 100), &mut ops, &mut budget).unwrap(), Some((key(2, 3), 20)));
    assert_eq!(values(&map), [9]); assert!(map.buckets[&0].root.is_none()); assert_eq!(budget.requests, requests); check(&map);
}

#[test]
fn iterator_predicate_and_lookup_failures_retain_completed_prefixes_without_visiting_later_entries() {
    for predicate_failure in [true, false] {
        let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[0,1,2,3], &mut budget, &mut ops);
        ops.calls.clear(); ops.fail_hash = true;
        let cause = map.remove_if(&mut ops, &mut budget, |key, _, ops, _| {
            ops.calls.push(Call::Predicate(key.identity));
            if key.value == 2 { if predicate_failure { return Err(invalid("test predicate failed")); } ops.fail_equal = Some(0); }
            Ok(key.value == 1 || key.value == 2)
        }).unwrap_err();
        assert_eq!(cause.message.as_deref(), Some(if predicate_failure { "test predicate failed" } else { "test equality failed" }));
        let mut expected = vec![Call::Predicate(1), Call::Predicate(2), Call::Equals(2,1), Call::Predicate(3)];
        if !predicate_failure { expected.push(Call::Equals(3,1)); }
        assert_eq!(ops.calls, expected); assert_eq!(values(&map), [0,2,3]); assert!(map.failed); assert!(map.try_iter().is_err()); check(&map);
        assert!(map.finish(&mut budget).is_err()); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
    }
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut map = populated(&[0,1,2,3,4,5,6,7,8], &mut budget, &mut ops);
    ops.calls.clear(); ops.fail_hash = true;
    let cause = map.remove_if(&mut ops, &mut budget, |key, _, ops, _| {
        ops.calls.push(Call::Predicate(key.identity)); if key.value == 5 { ops.fail_compare = true; } Ok(key.value == 3 || key.value == 5)
    }).unwrap_err();
    assert_eq!(cause.message.as_deref(), Some("test comparison failed"));
    assert_eq!(ops.calls, [Call::Predicate(4), Call::Predicate(1), Call::Predicate(2), Call::Predicate(3), Call::Predicate(5), Call::Predicate(6), Call::Equals(6,1), Call::Equals(6,5), Call::Comparable(6), Call::Compare(6,5)]);
    assert_eq!(values(&map), [0,1,2,4,5,6,7,8]); assert!(map.failed); check_roots(&map, false);
}

#[test]
fn iterator_empty_rejected_and_sparse_bucket_passes_keep_the_source_cursor_and_change_result() {
    let mut budget = Budget::new(); let mut ops = Ops::new(); let mut empty: Map<Key, i32, Lease> = Map::new(None).unwrap();
    ops.fail_hash = true; let mut calls = 0;
    assert!(!empty.remove_if(&mut ops, &mut budget, |_, _, _, _| { calls += 1; Ok(true) }).unwrap()); assert_eq!(calls, 0); assert!(ops.calls.is_empty());
    ops.fail_hash = false; ops.hash_by_value = true; let mut map = populated(&[65,2,129,70], &mut budget, &mut ops); let expected = [65,129,2,70]; assert_eq!(values(&map), expected);
    ops.calls.clear(); ops.fail_hash = true;
    assert!(!map.remove_if(&mut ops, &mut budget, |key, _, ops, _| { ops.calls.push(Call::Predicate(key.identity)); Ok(false) }).unwrap());
    assert_eq!(values(&map), expected); assert_eq!(ops.calls, [Call::Predicate(66), Call::Predicate(130), Call::Predicate(3), Call::Predicate(71)]);
    ops.calls.clear(); let requests = budget.requests; let live = budget.live.load(AtomicOrdering::SeqCst); budget.limit = live;
    assert!(map.remove_if(&mut ops, &mut budget, |key, _, ops, _| { ops.calls.push(Call::Predicate(key.identity)); Ok(true) }).unwrap());
    assert_eq!(ops.calls, [Call::Predicate(66), Call::Predicate(130), Call::Predicate(3), Call::Predicate(71)]);
    assert!(map.buckets.is_empty()); assert_eq!(map.len(), 0); assert_eq!(budget.requests, requests); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), live); check(&map);
    drop(map); drop(empty); assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn clear_keeps_warm_lookup_demand_and_reuses_tree_slots_under_a_full_budget() {
    let mut budget = Budget::new();
    let mut ops = Ops::new();
    let mut map = populated(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], &mut budget, &mut ops);
    map.remove(&key(9, 101), &mut ops, &mut budget).unwrap();
    assert!(map.buckets[&0].root.is_some());
    let geometry = (map.capacity, map.threshold, map.slots.len());
    let requests = budget.requests;
    let live = budget.live.load(AtomicOrdering::SeqCst);
    budget.limit = live;
    ops.calls.clear();

    map.clear().unwrap();
    map.clear().unwrap();
    assert!(ops.calls.is_empty());
    assert_eq!(map.try_iter().unwrap().len(), 0);
    assert_eq!((map.capacity, map.threshold, map.slots.len()), geometry);
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), live);
    check(&map);

    ops.fail_hash = true;
    assert!(map.contains_key(&key(1, 102), &mut ops, &mut budget).is_err());
    assert_eq!(ops.calls, [Call::Hash(102)]);
    ops.fail_hash = false;
    for value in 0..9 {
        map.put(key(value, value as usize + 20), value, &mut ops, &mut budget).unwrap();
    }
    assert_eq!(values(&map), [3, 0, 1, 2, 4, 5, 6, 7, 8]);
    assert_eq!(budget.requests, requests);
    check(&map);
    drop(map);
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn clear_drops_values_while_their_collection_admission_stays_live() {
    let mut budget = Budget::new();
    let mut ops = Ops::new();
    let mut map = Map::new(None).unwrap();
    let value = Arc::new(42);
    let retained = Arc::downgrade(&value);
    map.put(key(1, 1), value, &mut ops, &mut budget).unwrap();
    assert!(retained.upgrade().is_some());
    let live = budget.live.load(AtomicOrdering::SeqCst);

    map.clear().unwrap();

    assert!(retained.upgrade().is_none());
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), live);
    drop(map);
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn clear_preserves_lazy_initialization_and_rejects_poisoned_maps() {
    let mut budget = Budget::new();
    let mut ops = Ops::new();
    let mut map: Map<Key, i32, Lease> = Map::new(Some(64)).unwrap();
    map.clear().unwrap();
    ops.fail_hash = true;
    assert!(!map.contains_key(&key(1, 1), &mut ops, &mut budget).unwrap());
    assert!(ops.calls.is_empty());
    assert_eq!(budget.requests, 0);

    assert!(map.put(key(1, 1), 42, &mut ops, &mut budget).is_err());
    assert!(map.clear().is_err());
    assert!(map.finish(&mut budget).is_err());
}

#[test]
fn unwinding_during_clear_cannot_publish_partial_removal() {
    struct Value(bool);
    impl Drop for Value {
        fn drop(&mut self) {
            assert!(!self.0, "fixture destructor");
        }
    }

    let mut budget = Budget::new();
    let mut ops = Ops::new();
    let mut map = Map::new(None).unwrap();
    map.put(key(1, 1), Value(true), &mut ops, &mut budget).unwrap();
    map.put(key(2, 2), Value(false), &mut ops, &mut budget).unwrap();
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| map.clear()));

    assert!(unwind.is_err());
    assert!(map.try_iter().is_err());
    assert!(map.clear().is_err());
    assert!(map.finish(&mut budget).is_err());
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}

#[test]
fn transient_leases_remain_visible_after_clear_and_failed_mutation() {
    let mut budget = Budget::new();
    let mut ops = Ops::new();
    let mut map = populated(&[1, 2], &mut budget, &mut ops);
    let retained = map.leases().iter().map(|lease| lease.bytes).sum::<usize>();
    assert!(retained > 0);
    assert_eq!(retained, budget.live.load(AtomicOrdering::SeqCst));
    let requests = budget.requests;

    map.clear().unwrap();
    assert_eq!(map.leases().iter().map(|lease| lease.bytes).sum::<usize>(), retained);
    ops.fail_hash = true;
    assert!(map.put(key(3, 3), 30, &mut ops, &mut budget).is_err());
    assert!(map.try_iter().is_err());
    assert_eq!(map.leases().iter().map(|lease| lease.bytes).sum::<usize>(), retained);
    assert_eq!(budget.requests, requests);
    drop(map);
    assert_eq!(budget.live.load(AtomicOrdering::SeqCst), 0);
}
