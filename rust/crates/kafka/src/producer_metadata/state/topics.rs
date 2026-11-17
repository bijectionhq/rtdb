use rtdb_types::{Anomaly, Result};
use super::owned::{Budget, Text, Vector, overflow};

struct Topic { name: Text, expires_ms: i64, hash: usize, next: Option<usize> }
pub(super) struct ActiveTopics { table: Vector<Option<usize>>, entries: Vector<Option<Topic>>, len: usize }
pub(super) struct Iter<'a> { owner: &'a ActiveTopics, bucket: usize, next: Option<usize>, remaining: usize }

impl ActiveTopics {
    pub(super) fn new() -> Self { Self { table: Vector::new(), entries: Vector::new(), len: 0 } }
    fn node(&self, index: usize) -> &Topic { self.entries.as_slice()[index].as_ref().expect("linked active topic") }
    fn node_mut(&mut self, index: usize) -> &mut Topic { self.entries.as_mut_slice()[index].as_mut().expect("linked active topic") }
    fn hash(name: &Text) -> usize { ((name.hash() as u32 ^ (name.hash() as u32 >> 16)) & 0x7fffffff) as usize }
    fn find(&self, name: &Text) -> Option<usize> {
        if self.table.len() == 0 { return None; }
        let hash = Self::hash(name);
        let mut next = self.table.as_slice()[hash & (self.table.len() - 1)];
        while let Some(index) = next {
            let node = self.node(index);
            if node.hash == hash && node.name == *name { return Some(index); }
            next = node.next;
        }
        None
    }
    pub(super) fn refresh_existing(&mut self, name: &Text, expires_ms: i64) -> bool {
        if let Some(index) = self.find(name) { self.node_mut(index).expires_ms = expires_ms; true } else { false }
    }
    pub(super) fn expiry(&self, name: &Text) -> Option<i64> { self.find(name).map(|index| self.node(index).expires_ms) }
    pub(super) fn put(&mut self, name: Text, expires_ms: i64, budget: &mut Budget) -> Result<bool> {
        if self.table.len() == 0 { self.resize(budget)?; }
        let hash = Self::hash(&name);
        let bucket = hash & (self.table.len() - 1);
        let mut next = self.table.as_slice()[bucket];
        let mut previous = None;
        let mut bin_count = 0;
        while let Some(index) = next {
            bin_count += 1;
            if self.node(index).hash == hash && self.node(index).name == name {
                self.node_mut(index).expires_ms = expires_ms;
                if bin_count >= 8 { self.treeify_or_resize(budget)?; }
                return Ok(false);
            }
            previous = Some(index);
            next = self.node(index).next;
        }
        let value = Topic { name, expires_ms, hash, next: None };
        let index = if let Some(index) = self.entries.as_slice().iter().position(Option::is_none) {
            self.entries.as_mut_slice()[index] = Some(value); index
        } else {
            let index = self.entries.len(); self.entries.push(Some(value), budget)?; index
        };
        if let Some(previous) = previous { self.node_mut(previous).next = Some(index); }
        else { self.table.as_mut_slice()[bucket] = Some(index); }
        if bin_count >= 8 { self.treeify_or_resize(budget)?; }
        self.len = self.len.checked_add(1).ok_or_else(overflow)?;
        while self.len >= self.table.len() - self.table.len() / 4 && self.table.len() < 1 << 30 { self.resize(budget)?; }
        Ok(true)
    }
    fn treeify_or_resize(&mut self, budget: &mut Budget) -> Result<()> {
        let capacity = self.table.len();
        if capacity >= 64 {
            return Err(Anomaly::unsupported("rtdb/kafka-concurrent-map-tree", "ConcurrentHashMap topic tree profile requires its source tree geometry"));
        }
        let target = capacity.checked_mul(3).and_then(|value| value.checked_add(1)).and_then(usize::checked_next_power_of_two).ok_or_else(overflow)?;
        while target > self.table.len() - self.table.len() / 4 { self.resize(budget)?; }
        Ok(())
    }
    fn resize(&mut self, budget: &mut Budget) -> Result<()> {
        let old = self.table.len();
        let capacity = if old == 0 { 16 } else { old.checked_mul(2).ok_or_else(overflow)? };
        let mut table = Vector::with_capacity(capacity, budget)?;
        for _ in 0..capacity { table.push(None, budget)?; }
        for bucket in 0..old {
            let Some(head) = self.table.as_slice()[bucket] else { continue; };
            let mut run_bit = self.node(head).hash & old;
            let mut last_run = head;
            let mut next = self.node(head).next;
            while let Some(index) = next {
                let bit = self.node(index).hash & old;
                if bit != run_bit { run_bit = bit; last_run = index; }
                next = self.node(index).next;
            }
            let (mut low, mut high) = if run_bit == 0 { (Some(last_run), None) } else { (None, Some(last_run)) };
            let mut current = head;
            while current != last_run {
                let next = self.node(current).next.expect("last run remains reachable");
                if self.node(current).hash & old == 0 { self.node_mut(current).next = low; low = Some(current); }
                else { self.node_mut(current).next = high; high = Some(current); }
                current = next;
            }
            table.as_mut_slice()[bucket] = low;
            table.as_mut_slice()[bucket + old] = high;
        }
        self.table = table;
        Ok(())
    }
    pub(super) fn remove(&mut self, name: &Text) -> bool {
        if self.table.len() == 0 { return false; }
        let bucket = Self::hash(name) & (self.table.len() - 1);
        let mut next = self.table.as_slice()[bucket];
        let mut previous = None;
        while let Some(index) = next {
            let node = self.node(index);
            if node.name == *name {
                let after = node.next;
                if let Some(previous) = previous { self.node_mut(previous).next = after; }
                else { self.table.as_mut_slice()[bucket] = after; }
                self.entries.as_mut_slice()[index] = None;
                self.len -= 1;
                return true;
            }
            previous = Some(index); next = node.next;
        }
        false
    }
    pub(super) fn iter(&self) -> Iter<'_> { Iter { owner: self, bucket: 0, next: None, remaining: self.len } }
}
impl<'a> Iterator for Iter<'a> {
    type Item = &'a Text;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(index) = self.next {
                let node = self.owner.node(index); self.next = node.next; self.remaining -= 1; return Some(&node.name);
            }
            self.next = *self.owner.table.as_slice().get(self.bucket)?;
            self.bucket += 1;
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) { (self.remaining, Some(self.remaining)) }
}
impl ExactSizeIterator for Iter<'_> {}
