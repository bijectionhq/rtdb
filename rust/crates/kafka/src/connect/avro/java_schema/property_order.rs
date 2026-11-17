use super::*;
use std::cmp::Ordering as Compare;

#[derive(Clone, Copy, Default)]
struct Links {
    previous: Option<usize>, next: Option<usize>, parent: Option<usize>,
    left: Option<usize>, right: Option<usize>, red: bool,
}
struct Entry { hash: usize, links: Links }
#[derive(Clone, Copy, Default)]
enum Bin { #[default] Empty, List(usize), Tree { first: usize, root: usize } }
impl Bin {
    fn first(self) -> Option<usize> {
        match self { Self::Empty => None, Self::List(first) | Self::Tree { first, .. } => Some(first) }
    }
}
struct PropertyOrder<'a> { members: &'a Members, entries: Vec<Entry>, table: Vec<Bin>, len: usize }

pub(super) fn snapshot(members: &Members, context: &mut ConversionContext) -> Result<(Vec<usize>, Arc<MemoryReservation>)> {
    context.reserve(add(mul(mul(members.len(), size_of::<Entry>())?, 16)?, 4096)?)?;
    let mut entries = Vec::new();
    entries.try_reserve_exact(members.len()).map_err(allocation)?;
    entries.resize_with(members.len(), || Entry { hash: 0, links: Links::default() });
    let mut geometry = PropertyOrder { members, entries, table: Vec::new(), len: 0 };
    for index in 0..members.len() { geometry.put(index, context)?; }
    let charge = context.reserve(add(mul(mul(members.len(), size_of::<usize>())?, 16)?, 256)?)?;
    let mut order = Vec::new();
    order.try_reserve_exact(members.len()).map_err(allocation)?;
    for bin in &geometry.table {
        let mut next = bin.first();
        while let Some(index) = next {
            order.push(index);
            next = geometry.node(index).links.next;
        }
    }
    Ok((order, charge))
}
impl PropertyOrder<'_> {
    fn node(&self, index: usize) -> &Entry { &self.entries[index] }
    fn node_mut(&mut self, index: usize) -> &mut Entry { &mut self.entries[index] }
    fn name(&self, index: usize) -> &Text { self.members.entry(index).0 }
    fn direction(&self, hash: usize, name: &Text, stored: usize) -> Compare {
        hash.cmp(&self.node(stored).hash).then_with(|| name.cmp(self.name(stored)))
    }
    fn tree_position(&self, mut current: usize, hash: usize, name: &Text) -> std::result::Result<usize, (usize, bool)> {
        loop {
            let left = match self.direction(hash, name, current) {
                Compare::Equal => return Ok(current), Compare::Less => true, Compare::Greater => false,
            };
            match if left { self.node(current).links.left } else { self.node(current).links.right } {
                Some(next) => current = next,
                None => return Err((current, left)),
            }
        }
    }
    fn put(&mut self, index: usize, context: &mut ConversionContext) -> Result<()> {
        let raw = self.name(index).view().java_hash_code() as u32;
        let hash = ((raw ^ (raw >> 16)) & 0x7fffffff) as usize;
        self.node_mut(index).hash = hash;
        if self.table.is_empty() { self.resize(context)?; }
        let bucket = hash & (self.table.len() - 1);
        let bin = self.table[bucket];
        if let Bin::Tree { first, root } = bin {
            let (parent, left) = match self.tree_position(root, hash, self.name(index)) {
                Ok(_) => return Err(invalid("Duplicate immutable property key")),
                Err(position) => position,
            };
            self.node_mut(index).links = Links { next: Some(first), parent: Some(parent), ..Links::default() };
            self.node_mut(first).links.previous = Some(index);
            if left { self.node_mut(parent).links.left = Some(index); }
            else { self.node_mut(parent).links.right = Some(index); }
            let root = self.balance_insertion(root, index);
            self.table[bucket] = Bin::Tree { first: index, root };
        } else {
            let mut next = bin.first();
            let mut previous = None;
            let mut count = 0;
            while let Some(old) = next {
                count += 1;
                if self.node(old).hash == hash && self.name(old) == self.name(index) {
                    return Err(invalid("Duplicate immutable property key"));
                }
                previous = Some(old);
                next = self.node(old).links.next;
            }
            self.node_mut(index).links = Links { previous, ..Links::default() };
            if let Some(previous) = previous { self.node_mut(previous).links.next = Some(index); }
            else { self.table[bucket] = Bin::List(index); }
            if count >= 8 { self.treeify_or_resize(bucket, context)?; }
        }
        self.len = add(self.len, 1)?;
        while self.len >= self.table.len() - self.table.len() / 4 && self.table.len() < 1 << 30 {
            self.resize(context)?;
        }
        Ok(())
    }
    fn treeify_or_resize(&mut self, bucket: usize, context: &mut ConversionContext) -> Result<()> {
        let capacity = self.table.len();
        if capacity >= 64 {
            if let Bin::List(first) = self.table[bucket] {
                let root = self.build_tree(first);
                self.table[bucket] = Bin::Tree { first, root };
            }
            return Ok(());
        }
        let target = add(mul(capacity, 3)?, 1)?.checked_next_power_of_two().ok_or_else(overflow)?;
        while target > self.table.len() - self.table.len() / 4 { self.resize(context)?; }
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
    fn resize(&mut self, context: &mut ConversionContext) -> Result<()> {
        let old = self.table.len();
        let capacity = if old == 0 { 16 } else { mul(old, 2)? };
        context.reserve(add(mul(mul(capacity, size_of::<Bin>())?, 16)?, 256)?)?;
        let mut table = Vec::new();
        table.try_reserve_exact(capacity).map_err(allocation)?;
        table.resize(capacity, Bin::Empty);
        for bucket in 0..old {
            let bin = self.table[bucket];
            let Some(head) = bin.first() else { continue; };
            if let Bin::Tree { root, .. } = bin {
                let mut low = None;
                let mut low_tail = None;
                let mut low_count = 0;
                let mut high = None;
                let mut high_tail = None;
                let mut high_count = 0;
                let mut next = Some(head);
                while let Some(index) = next {
                    next = self.node(index).links.next;
                    let is_high = self.node(index).hash & old != 0;
                    let (first, tail, count) = if is_high {
                        (&mut high, &mut high_tail, &mut high_count)
                    } else {
                        (&mut low, &mut low_tail, &mut low_count)
                    };
                    self.node_mut(index).links.previous = *tail;
                    self.node_mut(index).links.next = None;
                    if let Some(tail) = *tail { self.node_mut(tail).links.next = Some(index); }
                    else { *first = Some(index); }
                    *tail = Some(index);
                    *count += 1;
                }
                for (slot, first, count, other_count) in [(bucket, low, low_count, high_count), (bucket + old, high, high_count, low_count)] {
                    table[slot] = match first {
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
                    let next = self.node(current).links.next.unwrap();
                    if self.node(current).hash & old == 0 {
                        self.node_mut(current).links.next = low;
                        low = Some(current);
                    } else {
                        self.node_mut(current).links.next = high;
                        high = Some(current);
                    }
                    current = next;
                }
                table[bucket] = self.list_bin(low);
                table[bucket + old] = self.list_bin(high);
            }
        }
        self.table = table;
        Ok(())
    }
    fn build_tree(&mut self, first: usize) -> usize {
        let mut root = first;
        let mut current = Some(first);
        while let Some(index) = current {
            let links = self.node(index).links;
            current = links.next;
            self.node_mut(index).links = Links { previous: links.previous, next: links.next, ..Links::default() };
            if index == first { continue; }
            let mut parent = root;
            loop {
                let left = self.direction(self.node(index).hash, self.name(index), parent) != Compare::Greater;
                let next = if left { self.node(parent).links.left } else { self.node(parent).links.right };
                if let Some(next) = next { parent = next; }
                else {
                    self.node_mut(index).links.parent = Some(parent);
                    if left { self.node_mut(parent).links.left = Some(index); }
                    else { self.node_mut(parent).links.right = Some(index); }
                    root = self.balance_insertion(root, index);
                    break;
                }
            }
        }
        root
    }
    fn rotate(&mut self, mut root: usize, pivot: usize, left: bool) -> usize {
        let child = if left { self.node(pivot).links.right } else { self.node(pivot).links.left }.unwrap();
        let inner = if left { self.node(child).links.left } else { self.node(child).links.right };
        if left { self.node_mut(pivot).links.right = inner; }
        else { self.node_mut(pivot).links.left = inner; }
        if let Some(inner) = inner { self.node_mut(inner).links.parent = Some(pivot); }
        let parent = self.node(pivot).links.parent;
        self.node_mut(child).links.parent = parent;
        if let Some(parent) = parent {
            if self.node(parent).links.left == Some(pivot) { self.node_mut(parent).links.left = Some(child); }
            else { self.node_mut(parent).links.right = Some(child); }
        } else {
            root = child;
            self.node_mut(child).links.red = false;
        }
        if left { self.node_mut(child).links.left = Some(pivot); }
        else { self.node_mut(child).links.right = Some(pivot); }
        self.node_mut(pivot).links.parent = Some(child);
        root
    }
    fn balance_insertion(&mut self, mut root: usize, mut index: usize) -> usize {
        self.node_mut(index).links.red = true;
        loop {
            let Some(parent) = self.node(index).links.parent else { self.node_mut(index).links.red = false; return index; };
            let Some(grand) = self.node(parent).links.parent else { return root; };
            if !self.node(parent).links.red { return root; }
            let left = self.node(grand).links.left == Some(parent);
            let uncle = if left { self.node(grand).links.right } else { self.node(grand).links.left };
            if uncle.is_some_and(|uncle| self.node(uncle).links.red) {
                self.node_mut(uncle.unwrap()).links.red = false;
                self.node_mut(parent).links.red = false;
                self.node_mut(grand).links.red = true;
                index = grand;
            } else {
                let triangle = if left { self.node(parent).links.right == Some(index) } else { self.node(parent).links.left == Some(index) };
                if triangle { index = parent; root = self.rotate(root, index, left); }
                if let Some(parent) = self.node(index).links.parent {
                    self.node_mut(parent).links.red = false;
                    if let Some(grand) = self.node(parent).links.parent {
                        self.node_mut(grand).links.red = true;
                        root = self.rotate(root, grand, !left);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
