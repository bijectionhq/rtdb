use std::{cmp::Ordering, collections::BTreeMap};

struct Entry<'a, K> { key: &'a K, source: usize, hash: i32, previous: Option<usize>, next: Option<usize>, parent: Option<usize>, left: Option<usize>, right: Option<usize>, red: bool }
#[derive(Default)]
struct Bucket { head: Option<usize>, tail: Option<usize>, len: usize, root: Option<usize> }
struct Replay<'a, K, C> { entries: Vec<Entry<'a, K>>, compare_keys: C, buckets: BTreeMap<usize, Bucket>, capacity: usize, threshold: usize }

// OpenJDK HashMap insertion and iteration order, including collision trees (java.util.HashMap).
pub(crate) fn indices<'a, K: Eq + 'a>(initial: usize, keys: impl IntoIterator<Item = &'a K>, hash: impl Fn(&K) -> i32, compare_keys: impl Fn(&K, &K) -> Ordering) -> Option<Vec<usize>> {
    let capacity = initial.max(1).checked_next_power_of_two()?.min(1 << 30);
    let mut replay = Replay { entries: Vec::new(), compare_keys, buckets: BTreeMap::new(), capacity, threshold: capacity * 3 / 4 };
    for (source, key) in keys.into_iter().enumerate() { replay.insert(key, source, hash(key)); }
    let mut order = Vec::with_capacity(replay.entries.len());
    for bucket in replay.buckets.values() {
        let mut next = bucket.head;
        while let Some(index) = next { order.push(replay.entries[index].source); next = replay.entries[index].next; }
    }
    Some(order)
}

impl<'a, K: Eq, C: Fn(&K, &K) -> Ordering> Replay<'a, K, C> {
    fn insert(&mut self, key: &'a K, source: usize, hash: i32) {
        let hash = hash ^ ((hash as u32 >> 16) as i32);
        let slot = hash as u32 as usize & (self.capacity - 1);
        let mut bucket = self.buckets.remove(&slot).unwrap_or_default();
        if let Some(index) = self.find(&bucket, hash, key) { self.entries[index].source = source; self.buckets.insert(slot, bucket); return; }
        let index = self.entries.len(); self.entries.push(Entry { key, source, hash, previous: None, next: None, parent: None, left: None, right: None, red: false });
        if bucket.root.is_some() {
            let root = bucket.root.unwrap();
            let parent = self.tree_insert(root, index);
            self.link_after(&mut bucket, Some(parent), index);
            let root = self.balance(root, index); self.front(&mut bucket, root);
        } else {
            let tail = bucket.tail; self.link_after(&mut bucket, tail, index);
            if bucket.len >= 9 && self.capacity >= 64 { self.treeify(&mut bucket); }
        }
        let resize_for_chain = bucket.root.is_none() && bucket.len >= 9 && self.capacity < 64;
        self.buckets.insert(slot, bucket);
        if resize_for_chain { self.resize(); }
        if self.entries.len() > self.threshold { self.resize(); }
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
