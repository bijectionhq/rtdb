use rtdb_types::Result;
use super::owned::{Budget, Text, Vector, overflow};

mod tree;

#[derive(Clone, Copy, Default)]
struct Links { previous: Option<usize>, next: Option<usize>, parent: Option<usize>, left: Option<usize>, right: Option<usize>, red: bool }
struct Topic { name: Text, expires_ms: i64, hash: usize, links: Links }
#[derive(Clone, Copy, Default)]
enum Bin { #[default] Empty, List(usize), Tree { first: usize, root: usize } }
impl Bin { fn first(self) -> Option<usize> { match self { Self::Empty => None, Self::List(first) | Self::Tree { first, .. } => Some(first) } } }
pub(super) struct ActiveTopics { table: Vector<Bin>, entries: Vector<Option<Topic>>, len: usize }
pub(super) struct Iter<'a> { owner: &'a ActiveTopics, bucket: usize, next: Option<usize>, remaining: usize }

impl ActiveTopics {
    pub(super) fn new() -> Self { Self { table: Vector::new(), entries: Vector::new(), len: 0 } }
    fn node(&self, index: usize) -> &Topic { self.entries.as_slice()[index].as_ref().expect("linked active topic") }
    fn node_mut(&mut self, index: usize) -> &mut Topic { self.entries.as_mut_slice()[index].as_mut().expect("linked active topic") }
    fn hash(name: &Text) -> usize { ((name.hash() as u32 ^ (name.hash() as u32 >> 16)) & 0x7fffffff) as usize }
    fn find(&self, name: &Text) -> Option<usize> {
        if self.table.len() == 0 { return None; }
        let hash = Self::hash(name);
        let bin = self.table.as_slice()[hash & (self.table.len() - 1)];
        if let Bin::Tree { root, .. } = bin { return self.tree_find(root, hash, name); }
        let mut next = bin.first();
        while let Some(index) = next {
            let node = self.node(index);
            if node.hash == hash && node.name == *name { return Some(index); }
            next = node.links.next;
        }
        None
    }
    pub(super) fn refresh_existing(&mut self, name: &Text, expires_ms: i64) -> bool {
        if let Some(index) = self.find(name) { self.node_mut(index).expires_ms = expires_ms; true } else { false }
    }
    pub(super) fn expiry(&self, name: &Text) -> Option<i64> { self.find(name).map(|index| self.node(index).expires_ms) }
    fn allocate(&mut self, value: Topic, budget: &mut Budget) -> Result<usize> {
        if let Some(index) = self.entries.as_slice().iter().position(Option::is_none) {
            self.entries.as_mut_slice()[index] = Some(value); Ok(index)
        } else {
            let index = self.entries.len(); self.entries.push(Some(value), budget)?; Ok(index)
        }
    }
    pub(super) fn put(&mut self, name: Text, expires_ms: i64, budget: &mut Budget) -> Result<bool> {
        if self.table.len() == 0 { self.resize(budget)?; }
        let hash = Self::hash(&name);
        let bucket = hash & (self.table.len() - 1);
        let bin = self.table.as_slice()[bucket];
        if let Bin::Tree { first, root } = bin {
            match self.tree_position(root, hash, &name) {
                Ok(index) => { self.node_mut(index).expires_ms = expires_ms; return Ok(false); }
                Err((parent, left)) => {
                    let index = self.allocate(Topic { name, expires_ms, hash, links: Links { next: Some(first), parent: Some(parent), ..Links::default() } }, budget)?;
                    self.node_mut(first).links.previous = Some(index);
                    if left { self.node_mut(parent).links.left = Some(index); } else { self.node_mut(parent).links.right = Some(index); }
                    let root = self.balance_insertion(root, index);
                    self.table.as_mut_slice()[bucket] = Bin::Tree { first: index, root };
                }
            }
        } else {
            let mut next = bin.first();
            let mut previous = None;
            let mut bin_count = 0;
            while let Some(index) = next {
                bin_count += 1;
                if self.node(index).hash == hash && self.node(index).name == name {
                    self.node_mut(index).expires_ms = expires_ms;
                    if bin_count >= 8 { self.treeify_or_resize(bucket, budget)?; }
                    return Ok(false);
                }
                previous = Some(index);
                next = self.node(index).links.next;
            }
            let index = self.allocate(Topic { name, expires_ms, hash, links: Links { previous, ..Links::default() } }, budget)?;
            if let Some(previous) = previous { self.node_mut(previous).links.next = Some(index); }
            else { self.table.as_mut_slice()[bucket] = Bin::List(index); }
            if bin_count >= 8 { self.treeify_or_resize(bucket, budget)?; }
        }
        self.len = self.len.checked_add(1).ok_or_else(overflow)?;
        while self.len >= self.table.len() - self.table.len() / 4 && self.table.len() < 1 << 30 { self.resize(budget)?; }
        Ok(true)
    }
    fn treeify_or_resize(&mut self, bucket: usize, budget: &mut Budget) -> Result<()> {
        let capacity = self.table.len();
        if capacity >= 64 {
            if let Bin::List(first) = self.table.as_slice()[bucket] {
                let root = self.build_tree(first);
                self.table.as_mut_slice()[bucket] = Bin::Tree { first, root };
            }
            return Ok(());
        }
        let target = capacity.checked_mul(3).and_then(|value| value.checked_add(1)).and_then(usize::checked_next_power_of_two).ok_or_else(overflow)?;
        while target > self.table.len() - self.table.len() / 4 { self.resize(budget)?; }
        Ok(())
    }
    fn list_bin(&mut self, first: Option<usize>) -> Bin {
        let mut next = first;
        let mut previous = None;
        while let Some(index) = next {
            next = self.node(index).links.next;
            self.node_mut(index).links = Links { previous, next, ..Links::default() };
            previous = Some(index);
        }
        first.map_or(Bin::Empty, Bin::List)
    }
    fn resize(&mut self, budget: &mut Budget) -> Result<()> {
        let old = self.table.len();
        let capacity = if old == 0 { 16 } else { old.checked_mul(2).ok_or_else(overflow)? };
        let mut table = Vector::with_capacity(capacity, budget)?;
        for _ in 0..capacity { table.push(Bin::Empty, budget)?; }
        for bucket in 0..old {
            let bin = self.table.as_slice()[bucket];
            let Some(head) = bin.first() else { continue; };
            if let Bin::Tree { root, .. } = bin {
                let mut low = None; let mut low_tail = None; let mut low_count = 0usize;
                let mut high = None; let mut high_tail = None; let mut high_count = 0usize;
                let mut next = Some(head);
                while let Some(index) = next {
                    next = self.node(index).links.next;
                    let is_high = self.node(index).hash & old != 0;
                    let (first, tail, count) = if is_high { (&mut high, &mut high_tail, &mut high_count) } else { (&mut low, &mut low_tail, &mut low_count) };
                    self.node_mut(index).links.previous = *tail;
                    self.node_mut(index).links.next = None;
                    if let Some(tail) = *tail { self.node_mut(tail).links.next = Some(index); } else { *first = Some(index); }
                    *tail = Some(index); *count += 1;
                }
                for (slot, first, count, other_count) in [(bucket, low, low_count, high_count), (bucket + old, high, high_count, low_count)] {
                    table.as_mut_slice()[slot] = match first {
                        None => Bin::Empty,
                        Some(first) if count <= 6 => self.list_bin(Some(first)),
                        Some(first) => Bin::Tree { first, root: if other_count == 0 { root } else { self.build_tree(first) } },
                    };
                }
            } else {
                let mut run_bit = self.node(head).hash & old;
                let mut last_run = head;
                let mut next = self.node(head).links.next;
                while let Some(index) = next {
                    let bit = self.node(index).hash & old;
                    if bit != run_bit { run_bit = bit; last_run = index; }
                    next = self.node(index).links.next;
                }
                let (mut low, mut high) = if run_bit == 0 { (Some(last_run), None) } else { (None, Some(last_run)) };
                let mut current = head;
                while current != last_run {
                    let next = self.node(current).links.next.expect("last run remains reachable");
                    if self.node(current).hash & old == 0 { self.node_mut(current).links.next = low; low = Some(current); }
                    else { self.node_mut(current).links.next = high; high = Some(current); }
                    current = next;
                }
                table.as_mut_slice()[bucket] = self.list_bin(low);
                table.as_mut_slice()[bucket + old] = self.list_bin(high);
            }
        }
        self.table = table;
        Ok(())
    }
    pub(super) fn remove(&mut self, name: &Text) -> bool {
        if self.table.len() == 0 { return false; }
        let bucket = Self::hash(name) & (self.table.len() - 1);
        let Some(index) = self.find(name) else { return false; };
        let bin = self.table.as_slice()[bucket];
        let replacement = match bin {
            Bin::Tree { first, root } => self.remove_tree(first, root, index),
            Bin::List(first) => {
                let links = self.node(index).links;
                if let Some(previous) = links.previous { self.node_mut(previous).links.next = links.next; }
                if let Some(next) = links.next { self.node_mut(next).links.previous = links.previous; }
                if first == index { links.next.map_or(Bin::Empty, Bin::List) } else { Bin::List(first) }
            }
            Bin::Empty => unreachable!("found active topic has a table bin"),
        };
        self.table.as_mut_slice()[bucket] = replacement;
        self.entries.as_mut_slice()[index] = None;
        self.len -= 1;
        true
    }
    pub(super) fn iter(&self) -> Iter<'_> { Iter { owner: self, bucket: 0, next: None, remaining: self.len } }
}
impl<'a> Iterator for Iter<'a> {
    type Item = &'a Text;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(index) = self.next {
                let node = self.owner.node(index); self.next = node.links.next; self.remaining -= 1; return Some(&node.name);
            }
            self.next = self.owner.table.as_slice().get(self.bucket)?.first();
            self.bucket += 1;
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) { (self.remaining, Some(self.remaining)) }
}
impl ExactSizeIterator for Iter<'_> {}

#[cfg(test)]
mod tests;
