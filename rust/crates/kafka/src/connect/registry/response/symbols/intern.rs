use std::{
    collections::BTreeMap,
    hash::{BuildHasher, Hasher, RandomState},
    mem::size_of,
    sync::{Arc, Mutex, Weak},
};
use rtdb_types::{MemoryReservation, Result, java_text::JavaText};
use super::{ConversionContext, InternAuthority, Profile, SymbolName, poisoned_lock};

#[derive(Clone)]
pub(in crate::connect) struct InternCache(Arc<Cache>);

struct Cache {
    hashes: RandomState,
    capacity: usize,
    entries: Mutex<Entries>,
    charge: Arc<MemoryReservation>,
}

struct Entries {
    strong: Vec<(u64, Arc<SymbolName>)>,
    weak: BTreeMap<(u64, u64), WeakName>,
    next: u64,
}

struct WeakName {
    name: Weak<SymbolName>,
    _charge: Arc<MemoryReservation>,
}

impl InternCache {
    pub(in crate::connect) fn open(profile: Profile, context: &mut ConversionContext) -> Result<Self> {
        let capacity = match profile { Profile::Jackson216 => 180, Profile::Jackson221 => 280 };
        let bytes = capacity * size_of::<(u64, Arc<SymbolName>)>() + size_of::<Cache>() + 512;
        let charge = context.reserve(bytes)?;
        let mut strong = Vec::new();
        strong.try_reserve_exact(capacity).map_err(|_| super::allocation_failed())?;
        let entries = Mutex::new(Entries { strong, weak: BTreeMap::new(), next: 0 });
        Ok(Self(Arc::new(Cache { hashes: RandomState::new(), capacity, entries, charge })))
    }
}

impl InternAuthority for InternCache {
    fn intern(&mut self, candidate: Arc<SymbolName>, context: &mut ConversionContext) -> Result<Arc<SymbolName>> {
        context.retain(Arc::clone(&self.0.charge))?;
        let text = candidate.text().view();
        let mut hash = self.0.hashes.build_hasher();
        for unit in text.units() { hash.write_u16(unit); }
        let hash = hash.finish();
        let mut entries = self.0.entries.lock().map_err(|_| poisoned_lock())?;
        if let Some((_, name)) = entries.strong.iter().find(|(key, name)| *key == hash && equal(name, text)) {
            name.retain(context)?;
            return Ok(Arc::clone(name));
        }
        if entries.strong.len() == self.0.capacity {
            entries.strong.clear();
            entries.weak.retain(|_, entry| entry.name.strong_count() != 0);
        }
        let present = entries.weak.range((hash, 0)..=(hash, u64::MAX))
            .filter_map(|(_, entry)| entry.name.upgrade()).find(|name| equal(name, text));
        let name = if let Some(name) = present {
            name.retain(context)?;
            name
        } else {
            let id = entries.next;
            let next = id.checked_add(1).ok_or_else(super::overflow)?;
            let charge = context.reserve(1024)?;
            entries.weak.insert((hash, id), WeakName { name: Arc::downgrade(&candidate), _charge: charge });
            entries.next = next;
            candidate
        };
        entries.strong.push((hash, Arc::clone(&name)));
        Ok(name)
    }
}

fn equal(name: &SymbolName, text: JavaText<'_>) -> bool {
    name.text().view().units().eq(text.units())
}

#[cfg(test)]
mod tests;
