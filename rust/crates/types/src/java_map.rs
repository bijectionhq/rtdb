use std::{cmp::Ordering, collections::BTreeMap};

struct Entry<'a, K> { key: &'a K, source: usize, hash: i32, previous: Option<usize>, next: Option<usize>, parent: Option<usize>, left: Option<usize>, right: Option<usize>, red: bool }
#[derive(Default)]
struct Bucket { head: Option<usize>, tail: Option<usize>, len: usize, root: Option<usize> }
struct Replay<'a, K, C> { entries: Vec<Entry<'a, K>>, compare_keys: C, buckets: BTreeMap<usize, Bucket>, capacity: usize, threshold: usize }

#[derive(Clone, Copy, PartialEq, Eq)]
enum Insertion { Put, ComputeIfAbsent }

/// Returns OpenJDK HashMap iteration indices after insertion, replacement and collision-tree ordering.
/// `hash` returns the raw Java `hashCode`; comparable keys share one class and use its `Comparable` ordering.
/// Non-comparable keys must already be deduplicated and use a stable per-object identity tie order.
/// Duplicate keys return the last input index at their retained Java iteration position.
/// The caller owns keys and allocation admission; an overflowing initial capacity returns `None`.
pub fn indices<'a, K: Eq + 'a>(initial: usize, keys: impl IntoIterator<Item = &'a K>, hash: impl Fn(&K) -> i32, compare_keys: impl Fn(&K, &K) -> Ordering) -> Option<Vec<usize>> {
    let capacity = initial.max(1).checked_next_power_of_two()?.min(1 << 30);
    let mut replay = Replay { entries: Vec::new(), compare_keys, buckets: BTreeMap::new(), capacity, threshold: capacity * 3 / 4 };
    for (source, key) in keys.into_iter().enumerate() {
        replay.insert(key, source, hash(key), Insertion::Put);
    }
    Some(replay.into_order())
}

/// Returns OpenJDK default HashMap iteration indices after non-null `computeIfAbsent` calls.
/// Repeated keys retain the first input index and may trigger a resize on lookup.
/// Key comparison and allocation obligations match [`indices`].
pub fn compute_if_absent_indices<'a, K: Eq + 'a>(keys: impl IntoIterator<Item = &'a K>, hash: impl Fn(&K) -> i32, compare_keys: impl Fn(&K, &K) -> Ordering) -> Vec<usize> {
    let mut replay = Replay { entries: Vec::new(), compare_keys, buckets: BTreeMap::new(), capacity: 16, threshold: 12 };
    for (source, key) in keys.into_iter().enumerate() {
        replay.insert(key, source, hash(key), Insertion::ComputeIfAbsent);
    }
    replay.into_order()
}

/// Returns iteration indices after individual insertions followed by OpenJDK `HashMap.putAll`.
/// `initial = None` selects the default constructor; `Some` selects an explicit initial capacity.
/// `additions` contains distinct keys in the source map's iteration order; its length is that map's size.
/// Indices address the concatenation of `keys` and `additions`, retaining the latest replacement index.
/// Key comparison and allocation obligations match `indices`; capacity overflow returns `None`.
pub fn put_all_indices<'a, K: Eq + 'a>(initial: Option<usize>, keys: impl IntoIterator<Item = &'a K>, additions: &[&'a K], hash: impl Fn(&K) -> i32, compare_keys: impl Fn(&K, &K) -> Ordering) -> Option<Vec<usize>> {
    let capacity = initial.unwrap_or(16).max(1).checked_next_power_of_two()?.min(1 << 30);
    let mut replay = Replay { entries: Vec::new(), compare_keys, buckets: BTreeMap::new(), capacity, threshold: if capacity == 1 << 30 { i32::MAX as usize } else { capacity * 3 / 4 } };
    let mut offset = 0;
    for key in keys { replay.insert(key, offset, hash(key), Insertion::Put); offset += 1; }
    if !additions.is_empty() {
        if offset == 0 {
            let requested = if additions.len() >= (1 << 30) * 3 / 4 { 1 << 30 } else { (additions.len() * 4).div_ceil(3) };
            let threshold = initial.map_or(0, |_| capacity).max(requested);
            replay.capacity = threshold.max(1).next_power_of_two().min(1 << 30);
            replay.threshold = if replay.capacity == 1 << 30 { i32::MAX as usize } else { replay.capacity * 3 / 4 };
        } else {
            while additions.len() > replay.threshold && replay.capacity < 1 << 30 { replay.resize(); }
        }
        for (index, key) in additions.iter().enumerate() { replay.insert(*key, offset + index, hash(*key), Insertion::Put); }
    }
    Some(replay.into_order())
}

impl<'a, K: Eq, C: Fn(&K, &K) -> Ordering> Replay<'a, K, C> {
    fn into_order(self) -> Vec<usize> {
        let mut order = Vec::with_capacity(self.entries.len());
        for bucket in self.buckets.values() {
            let mut next = bucket.head;
            while let Some(index) = next {
                order.push(self.entries[index].source);
                next = self.entries[index].next;
            }
        }
        order
    }

    fn insert(&mut self, key: &'a K, source: usize, hash: i32, insertion: Insertion) {
        let hash = hash ^ ((hash as u32 >> 16) as i32);
        if insertion == Insertion::ComputeIfAbsent && self.entries.len() > self.threshold {
            self.resize();
        }
        let slot = hash as u32 as usize & (self.capacity - 1);
        let mut bucket = self.buckets.remove(&slot).unwrap_or_default();
        if let Some(index) = self.find(&bucket, hash, key) {
            if insertion == Insertion::Put {
                self.entries[index].source = source;
            }
            self.buckets.insert(slot, bucket);
            return;
        }
        let index = self.entries.len(); self.entries.push(Entry { key, source, hash, previous: None, next: None, parent: None, left: None, right: None, red: false });
        let treeify_at = match insertion { Insertion::Put => 9, Insertion::ComputeIfAbsent => 8 };
        if bucket.root.is_some() {
            let root = bucket.root.unwrap();
            let parent = self.tree_insert(root, index);
            self.link_after(&mut bucket, Some(parent), index);
            let root = self.balance(root, index); self.front(&mut bucket, root);
        } else {
            let previous = match insertion { Insertion::Put => bucket.tail, Insertion::ComputeIfAbsent => None };
            self.link_after(&mut bucket, previous, index);
            if bucket.len >= treeify_at && self.capacity >= 64 { self.treeify(&mut bucket); }
        }
        let resize_for_chain = bucket.root.is_none() && bucket.len >= treeify_at && self.capacity < 64;
        self.buckets.insert(slot, bucket);
        if resize_for_chain { self.resize(); }
        if insertion == Insertion::Put && self.entries.len() > self.threshold { self.resize(); }
    }
    fn find(&self, bucket: &Bucket, hash: i32, key: &K) -> Option<usize> {
        if let Some(root) = bucket.root {
            let mut next = Some(root);
            while let Some(index) = next {
                let entry = &self.entries[index];
                match self.compare(hash, key, index) {
                    Ordering::Equal => return Some(index),
                    Ordering::Less => next = entry.left,
                    Ordering::Greater => next = entry.right,
                }
            }
        } else {
            let mut next = bucket.head;
            while let Some(index) = next {
                let entry = &self.entries[index];
                if entry.key == key { return Some(index); }
                next = entry.next;
            }
        }
        None
    }
    fn link_after(&mut self, bucket: &mut Bucket, previous: Option<usize>, index: usize) {
        let next = match previous { Some(previous) => self.entries[previous].next, None => bucket.head };
        self.entries[index].previous = previous; self.entries[index].next = next;
        if let Some(previous) = previous { self.entries[previous].next = Some(index); } else { bucket.head = Some(index); }
        if let Some(next) = next { self.entries[next].previous = Some(index); } else { bucket.tail = Some(index); }
        bucket.len += 1;
    }
    fn resize(&mut self) {
        if self.capacity >= 1 << 30 { self.threshold = i32::MAX as usize; return; }
        let old = self.capacity; self.capacity *= 2;
        self.threshold = if self.capacity >= 1 << 30 { i32::MAX as usize } else { self.capacity * 3 / 4 };
        for (slot, bucket) in std::mem::take(&mut self.buckets) {
            let tree = bucket.root.is_some(); let root = bucket.root;
            let mut low = Bucket::default(); let mut high = Bucket::default(); let mut next = bucket.head;
            while let Some(index) = next {
                next = self.entries[index].next;
                let target = if self.entries[index].hash as u32 as usize & old == 0 { &mut low } else { &mut high };
                let tail = target.tail; self.link_after(target, tail, index);
            }
            let split = low.len != 0 && high.len != 0;
            for (slot, mut bucket) in [(slot, low), (slot + old, high)] {
                if bucket.len == 0 { continue; }
                if tree && bucket.len > 6 {
                    if split { self.treeify(&mut bucket); } else { bucket.root = root; }
                }
                self.buckets.insert(slot, bucket);
            }
        }
    }
    fn direction(&self, left: usize, right: usize) -> Ordering {
        let left = &self.entries[left]; self.compare(left.hash, left.key, right)
    }
    fn compare(&self, hash: i32, key: &K, right: usize) -> Ordering {
        let right = &self.entries[right];
        hash.cmp(&right.hash).then_with(|| (self.compare_keys)(key, right.key))
    }
    fn tree_insert(&mut self, mut parent: usize, index: usize) -> usize {
        loop {
            let left = self.direction(index, parent) == Ordering::Less;
            let child = if left { self.entries[parent].left } else { self.entries[parent].right };
            if let Some(child) = child { parent = child; }
            else {
                if left { self.entries[parent].left = Some(index); } else { self.entries[parent].right = Some(index); }
                self.entries[index].parent = Some(parent); return parent;
            }
        }
    }
    fn treeify(&mut self, bucket: &mut Bucket) {
        let Some(mut root) = bucket.head else { return; };
        let mut next = bucket.head;
        while let Some(index) = next { let entry = &mut self.entries[index]; entry.parent = None; entry.left = None; entry.right = None; entry.red = false; next = entry.next; }
        let mut next = self.entries[root].next;
        while let Some(index) = next { self.tree_insert(root, index); root = self.balance(root, index); next = self.entries[index].next; }
        self.front(bucket, root);
    }
    fn front(&mut self, bucket: &mut Bucket, root: usize) {
        bucket.root = Some(root);
        if bucket.head == Some(root) { return; }
        let previous = self.entries[root].previous; let next = self.entries[root].next;
        if let Some(previous) = previous { self.entries[previous].next = next; }
        if let Some(next) = next { self.entries[next].previous = previous; } else { bucket.tail = previous; }
        self.entries[root].previous = None; self.entries[root].next = bucket.head;
        if let Some(first) = bucket.head { self.entries[first].previous = Some(root); }
        bucket.head = Some(root);
    }
    fn rotate(&mut self, mut root: usize, pivot: usize, left: bool) -> usize {
        let child = if left { self.entries[pivot].right } else { self.entries[pivot].left }.unwrap();
        let inner = if left { self.entries[child].left } else { self.entries[child].right };
        if left { self.entries[pivot].right = inner; } else { self.entries[pivot].left = inner; }
        if let Some(inner) = inner { self.entries[inner].parent = Some(pivot); }
        let parent = self.entries[pivot].parent; self.entries[child].parent = parent;
        if let Some(parent) = parent {
            if self.entries[parent].left == Some(pivot) { self.entries[parent].left = Some(child); } else { self.entries[parent].right = Some(child); }
        } else { root = child; self.entries[child].red = false; }
        if left { self.entries[child].left = Some(pivot); } else { self.entries[child].right = Some(pivot); }
        self.entries[pivot].parent = Some(child); root
    }
    fn balance(&mut self, mut root: usize, mut index: usize) -> usize {
        self.entries[index].red = true;
        loop {
            let Some(parent) = self.entries[index].parent else { self.entries[index].red = false; return index; };
            let Some(grand) = self.entries[parent].parent else { return root; };
            if !self.entries[parent].red { return root; }
            let left = self.entries[grand].left == Some(parent);
            let uncle = if left { self.entries[grand].right } else { self.entries[grand].left };
            if uncle.is_some_and(|uncle| self.entries[uncle].red) {
                self.entries[uncle.unwrap()].red = false; self.entries[parent].red = false; self.entries[grand].red = true; index = grand;
            } else {
                let triangle = if left { self.entries[parent].right == Some(index) } else { self.entries[parent].left == Some(index) };
                if triangle { index = parent; root = self.rotate(root, index, left); }
                if let Some(parent) = self.entries[index].parent {
                    self.entries[parent].red = false;
                    if let Some(grand) = self.entries[parent].parent { self.entries[grand].red = true; root = self.rotate(root, grand, !left); }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_if_absent_prepends_collisions_and_keeps_first_index() {
        let keys = [0, 16, 0, 32, -1];
        assert_eq!(compute_if_absent_indices(&keys, |key| *key, Ord::cmp), [4, 3, 1, 0]);
    }

    #[test]
    fn compute_if_absent_resizes_before_an_existing_key_lookup() {
        let keys = [16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 16];
        assert_eq!(compute_if_absent_indices(&keys[..13], |key| *key, Ord::cmp), (0..13).collect::<Vec<_>>());
        assert_eq!(compute_if_absent_indices(&keys, |key| *key, Ord::cmp), (1..13).chain([0]).collect::<Vec<_>>());
    }

    #[test]
    fn compute_if_absent_treeifies_the_head_inserted_chain_after_resizing_twice() {
        let keys: Vec<_> = (0..10).collect();
        assert_eq!(compute_if_absent_indices(&keys[..9], |_| 0, Ord::cmp), (0..9).rev().collect::<Vec<_>>());
        assert_eq!(compute_if_absent_indices(&keys, |_| 0, Ord::cmp), [6, 9, 8, 7, 5, 4, 3, 2, 1, 0]);
    }

    #[test]
    fn bulk_default_constructor_presizes_before_first_insertion() {
        let keys = [9i32, 2];
        let additions = keys.iter().collect::<Vec<_>>();
        assert_eq!(put_all_indices(None, std::iter::empty(), &additions, |key| *key, Ord::cmp), Some(vec![0, 1]));
        assert_eq!(put_all_indices(Some(16), std::iter::empty(), &additions, |key| *key, Ord::cmp), Some(vec![1, 0]));
        assert_eq!(put_all_indices(None, keys[..1].iter(), &additions[1..], |key| *key, Ord::cmp), Some(vec![1, 0]));
    }

    #[test]
    fn bulk_capacity_uses_ceiling_without_extra_bucket_at_exact_load_factor() {
        let keys = [4i32, 1, 2];
        assert_eq!(put_all_indices(None, std::iter::empty(), &keys.iter().collect::<Vec<_>>(), |key| *key, Ord::cmp), Some(vec![0, 1, 2]));
    }

    #[test]
    fn bulk_replacement_indices_include_replaced_first_batch_entries() {
        let first = [1i32, 1, 2];
        let additions = [1i32, 3];
        assert_eq!(put_all_indices(None, first.iter(), &additions.iter().collect::<Vec<_>>(), |key| *key, Ord::cmp), Some(vec![3, 2, 4]));
        assert_eq!(put_all_indices(Some(usize::MAX), first.iter(), &[], |key| *key, Ord::cmp), None);
    }

    #[test]
    fn unique_identity_ranked_keys_retain_collision_tree_root_order() {
        #[derive(PartialEq, Eq)]
        struct Key { identity: u32 }
        let keys = (0..9).map(|identity| Key { identity }).collect::<Vec<_>>();
        let order = indices(64, keys.iter(), |_| 1, |left, right| left.identity.cmp(&right.identity)).unwrap();
        assert_eq!(order[0], 3);
        let mut sorted = order; sorted.sort_unstable(); assert_eq!(sorted, (0..9).collect::<Vec<_>>());
    }

    #[test]
    fn bulk_large_collision_family_retains_every_key_and_replacement() {
        let keys = (0..64).collect::<Vec<u32>>();
        let additions = keys.iter().rev().collect::<Vec<_>>();
        let mut order = put_all_indices(None, keys[..12].iter(), &additions, |_| 1, Ord::cmp).unwrap();
        order.sort_unstable(); assert_eq!(order, (12..76).collect::<Vec<_>>());
    }
}
