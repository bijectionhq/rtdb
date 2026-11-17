use std::collections::VecDeque;
use rtdb_types::Result;
use super::invalid;

pub(super) struct JksEntries<T> { buckets: Vec<VecDeque<(Vec<u16>, T)>>, count: usize }

impl<T> JksEntries<T> {
    pub(super) fn new() -> Self { Self { buckets: (0..11).map(|_| VecDeque::new()).collect(), count: 0 } }

    pub(super) fn insert(&mut self, alias: Vec<u16>, value: T) -> Result<()> {
        let mut index = bucket(&alias, self.buckets.len());
        if let Some((_, previous)) = self.buckets[index].iter_mut().find(|(key, _)| *key == alias) {
            *previous = value;
            return Ok(());
        }
        if self.count >= self.buckets.len() * 3 / 4 {
            let capacity = self.buckets.len().checked_mul(2).and_then(|size| size.checked_add(1)).ok_or_else(|| invalid("JKS alias table exceeds its range"))?;
            let replacement = (0..capacity).map(|_| VecDeque::new()).collect();
            let previous = std::mem::replace(&mut self.buckets, replacement);
            for entries in previous.into_iter().rev() {
                for (alias, value) in entries {
                    let index = bucket(&alias, capacity);
                    self.buckets[index].push_front((alias, value));
                }
            }
            index = bucket(&alias, capacity);
        }
        self.buckets[index].push_front((alias, value));
        self.count += 1;
        Ok(())
    }

    pub(super) fn into_enumeration(self) -> impl Iterator<Item = (Vec<u16>, T)> {
        self.buckets.into_iter().rev().flatten()
    }
}

fn bucket(alias: &[u16], capacity: usize) -> usize {
    let hash = alias.iter().fold(0u32, |hash, unit| hash.wrapping_mul(31).wrapping_add(u32::from(*unit)));
    (hash & 0x7fff_ffff) as usize % capacity
}
