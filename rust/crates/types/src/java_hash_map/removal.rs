use super::*;
use std::ops::Bound::{Excluded, Unbounded};

impl<K, V, L> Map<K, V, L> {
    /// Removes a mapping using Java key lookup and returns its stored key and value.
    /// A non-null key is hashed even before a table exists; null keys use zero.
    /// Fallible key operations precede link mutation, and their failure poisons this unpublished transient.
    /// Collection leases remain retained; later insertions can reuse the vacant slot.
    pub fn remove<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<(K, V)>> {
        self.usable()?;
        match self.remove_inner(key, ops, admission) {
            Ok(value) => Ok(value),
            Err(cause) => { self.failed = true; Err(cause) }
        }
    }
    fn remove_inner<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, key: &K, ops: &mut O, admission: &mut A) -> Result<Option<(K, V)>> {
        let hash = Self::spread(key, ops, admission)?;
        let Some((slot, index)) = self.find_hashed(hash, key, ops, admission)? else { return Ok(None); };
        Ok(Some(self.remove_found(slot, index, true)))
    }
    /// Calls the predicate in original encounter order and removes selected entries through Java iterator lookup.
    /// The predicate runs before cached-hash lookup; iterator removal never invokes user hash_code.
    /// Tree balancing preserves the saved next cursor without root-front movement or small-tree flattening.
    /// Returns whether any predicate selected removal; a predicate or key-operation error preserves completed prefixes and poisons this transient.
    /// The predicate must admit its scratch before allocation and retain reservations through scratch destruction.
    /// Removed keys and values are dropped while collection leases remain retained.
    pub fn remove_if<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, ops: &mut O, admission: &mut A, predicate: impl FnMut(&K, &V, &mut O, &mut A) -> Result<bool>) -> Result<bool> {
        self.usable()?;
        match self.remove_if_inner(ops, admission, predicate) {
            Ok(changed) => Ok(changed),
            Err(cause) => { self.failed = true; Err(cause) }
        }
    }
    fn remove_if_inner<A: Admission<Lease = L>, O: KeyOps<K, A>>(&mut self, ops: &mut O, admission: &mut A, mut predicate: impl FnMut(&K, &V, &mut O, &mut A) -> Result<bool>) -> Result<bool> {
        let mut current = self.buckets.first_key_value().and_then(|(slot, bucket)| bucket.head.map(|index| (*slot, index)));
        let mut changed = false;
        while let Some((slot, index)) = current {
            let next = self.entry(index).links.next.map(|next| (slot, next)).or_else(|| {
                self.buckets.range((Excluded(slot), Unbounded)).next().and_then(|(slot, bucket)| bucket.head.map(|index| (*slot, index)))
            });
            let selected = { let entry = self.entry(index); predicate(&entry.key, &entry.value, ops, admission)? };
            if selected {
                let found = { let entry = self.entry(index); self.find_hashed(entry.hash, &entry.key, ops, admission)? };
                if let Some((slot, index)) = found { self.remove_found(slot, index, false); }
                changed = true;
            }
            current = next;
        }
        Ok(changed)
    }
    fn remove_found(&mut self, slot: usize, index: usize, movable: bool) -> (K, V) {
        let mut bucket = self.buckets[&slot];
        if bucket.root.is_some() { self.remove_tree_node(&mut bucket, index, movable); }
        else { self.unlink_removed(&mut bucket, index); }
        if bucket.head.is_some() { self.buckets.insert(slot, bucket); }
        else { self.buckets.remove(&slot); }
        self.len -= 1;
        let Slot::Occupied(entry) = replace(&mut self.slots[index], Slot::Vacant(self.free)) else { unreachable!("removed HashMap slot is occupied") };
        self.free = Some(index);
        (entry.key, entry.value)
    }
    fn unlink_removed(&mut self, bucket: &mut Bucket, index: usize) {
        let links = self.entry(index).links;
        if let Some(previous) = links.previous { self.entry_mut(previous).links.next = links.next; }
        else { bucket.head = links.next; }
        if let Some(next) = links.next { self.entry_mut(next).links.previous = links.previous; }
        else { bucket.tail = links.previous; }
        bucket.len -= 1;
    }
    fn remove_tree_node(&mut self, bucket: &mut Bucket, index: usize, movable: bool) {
        let mut root = bucket.head.expect("tree bucket has an encounter head");
        self.unlink_removed(bucket, index);
        if bucket.head.is_none() { bucket.root = None; return; }
        while let Some(parent) = self.entry(root).links.parent { root = parent; }
        let left = self.entry(root).links.left;
        if movable && (self.entry(root).links.right.is_none() || left.is_none() || self.entry(left.unwrap()).links.left.is_none()) {
            self.clear_tree(bucket); bucket.root = None; return;
        }
        let links = self.entry(index).links;
        let replacement = match (links.left, links.right) {
            (Some(left), Some(right)) => {
                let mut successor = right;
                while let Some(left) = self.entry(successor).links.left { successor = left; }
                let successor_links = self.entry(successor).links;
                self.entry_mut(successor).links.red = links.red;
                self.entry_mut(index).links.red = successor_links.red;
                if successor == right {
                    self.entry_mut(index).links.parent = Some(successor);
                    self.entry_mut(successor).links.right = Some(index);
                } else {
                    self.entry_mut(index).links.parent = successor_links.parent;
                    if let Some(parent) = successor_links.parent {
                        if self.entry(parent).links.left == Some(successor) { self.entry_mut(parent).links.left = Some(index); }
                        else { self.entry_mut(parent).links.right = Some(index); }
                    }
                    self.entry_mut(successor).links.right = Some(right);
                    self.entry_mut(right).links.parent = Some(successor);
                }
                self.entry_mut(index).links.left = None;
                self.entry_mut(index).links.right = successor_links.right;
                if let Some(right) = successor_links.right { self.entry_mut(right).links.parent = Some(index); }
                self.entry_mut(successor).links.left = Some(left);
                self.entry_mut(left).links.parent = Some(successor);
                self.entry_mut(successor).links.parent = links.parent;
                if let Some(parent) = links.parent {
                    if self.entry(parent).links.left == Some(index) { self.entry_mut(parent).links.left = Some(successor); }
                    else { self.entry_mut(parent).links.right = Some(successor); }
                } else { root = successor; }
                successor_links.right.unwrap_or(index)
            }
            (Some(child), None) | (None, Some(child)) => child,
            (None, None) => index,
        };
        if replacement != index {
            let parent = self.entry(index).links.parent;
            self.entry_mut(replacement).links.parent = parent;
            if let Some(parent) = parent {
                if self.entry(parent).links.left == Some(index) { self.entry_mut(parent).links.left = Some(replacement); }
                else { self.entry_mut(parent).links.right = Some(replacement); }
            } else { root = replacement; self.entry_mut(replacement).links.red = false; }
            let links = &mut self.entry_mut(index).links;
            links.left = None; links.right = None; links.parent = None;
        }
        let root = if self.entry(index).links.red { root } else { self.balance_deletion(root, Some(replacement)) };
        if replacement == index {
            let parent = self.entry(index).links.parent;
            self.entry_mut(index).links.parent = None;
            if let Some(parent) = parent {
                if self.entry(parent).links.left == Some(index) { self.entry_mut(parent).links.left = None; }
                else if self.entry(parent).links.right == Some(index) { self.entry_mut(parent).links.right = None; }
            }
        }
        if movable { self.front(bucket, root); }
        else { bucket.root = Some(root); }
    }
    pub(super) fn balance_deletion(&mut self, mut root: usize, mut current: Option<usize>) -> usize {
        loop {
            let Some(index) = current else { return root; };
            if index == root { return root; }
            let Some(parent) = self.entry(index).links.parent else { self.entry_mut(index).links.red = false; return index; };
            if self.entry(index).links.red { self.entry_mut(index).links.red = false; return root; }
            let left = self.entry(parent).links.left == Some(index);
            let mut parent = Some(parent);
            let mut sibling = if left { self.entry(parent.unwrap()).links.right } else { self.entry(parent.unwrap()).links.left };
            if self.removal_red(sibling) {
                self.entry_mut(sibling.unwrap()).links.red = false;
                self.entry_mut(parent.unwrap()).links.red = true;
                root = self.rotate(root, parent.unwrap(), left);
                parent = self.entry(index).links.parent;
                sibling = parent.and_then(|parent| if left { self.entry(parent).links.right } else { self.entry(parent).links.left });
            }
            let Some(sibling_index) = sibling else { current = parent; continue; };
            let sibling_links = self.entry(sibling_index).links;
            if !self.removal_red(sibling_links.left) && !self.removal_red(sibling_links.right) {
                self.entry_mut(sibling_index).links.red = true;
                current = parent;
            } else {
                let outer = if left { sibling_links.right } else { sibling_links.left };
                if !self.removal_red(outer) {
                    let inner = if left { sibling_links.left } else { sibling_links.right };
                    if let Some(inner) = inner { self.entry_mut(inner).links.red = false; }
                    self.entry_mut(sibling_index).links.red = true;
                    root = self.rotate(root, sibling_index, !left);
                    parent = self.entry(index).links.parent;
                    sibling = parent.and_then(|parent| if left { self.entry(parent).links.right } else { self.entry(parent).links.left });
                }
                if let Some(sibling) = sibling {
                    let parent_red = parent.is_some_and(|parent| self.entry(parent).links.red);
                    self.entry_mut(sibling).links.red = parent_red;
                    let outer = if left { self.entry(sibling).links.right } else { self.entry(sibling).links.left };
                    if let Some(outer) = outer { self.entry_mut(outer).links.red = false; }
                }
                if let Some(parent) = parent { self.entry_mut(parent).links.red = false; root = self.rotate(root, parent, left); }
                current = Some(root);
            }
        }
    }
    fn removal_red(&self, index: Option<usize>) -> bool { index.is_some_and(|index| self.entry(index).links.red) }
}

#[cfg(test)] mod tests;
