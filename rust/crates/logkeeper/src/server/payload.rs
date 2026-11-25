//! Where a server keeps an entry's payload: in the journal's queue until its round is written, then in a segment, with the most recently written or read payloads cached.

use super::state::Payload;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::sync::Arc;

/// One framed `Append` record in a segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Location {
    pub segment: u64,
    pub offset: u64,
    /// The whole frame, header included.
    pub length: u32,
    pub payload: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Stored {
    /// Decided, and waiting in the journal's queue.
    Pending(Bytes),
    Written(Location),
}

impl Payload for Stored {
    fn decided(payload: &Bytes) -> Self { Stored::Pending(payload.clone()) }

    fn bytes(&self) -> usize {
        match self { Stored::Pending(payload) => payload.len(), Stored::Written(location) => location.payload as usize }
    }
}

/// Where a reader finds a payload, looked up under its stream's lock and read after.
pub(crate) enum Source {
    Ready(Bytes),
    /// The segment's handle, taken while the location was current, so a compaction that retires the segment meanwhile leaves it readable; none if the segment is gone.
    Disk(Option<Arc<File>>, Location),
}

pub(crate) struct Item {
    pub lsn: i64,
    pub epoch: u64,
    pub timestamp: i64,
    pub guarded: bool,
    pub guarded_before: Option<i64>,
    pub timed_origin: Option<crate::protocol::TimedJournalOrigin>,
    pub source: Source,
}

/// What each cached payload costs beyond its bytes, in the maps that index it.
const ENTRY_OVERHEAD: usize = 96;

/// Least-recently-used payloads by location, within a byte budget.
/// A location's contents never change, so an entry is never stale, only unreachable once nothing points at it.
pub(crate) struct Cache {
    budget: usize,
    bytes: usize,
    tick: u64,
    entries: HashMap<Location, (Bytes, u64)>,
    by_use: BTreeMap<u64, Location>,
}

impl Cache {
    pub fn new(budget: usize) -> Self { Self { budget, bytes: 0, tick: 0, entries: HashMap::new(), by_use: BTreeMap::new() } }

    #[cfg(test)]
    pub fn bytes(&self) -> usize { self.bytes }

    fn cost(payload: &Bytes) -> usize { payload.len() + ENTRY_OVERHEAD }

    fn touch(&mut self, location: Location) -> Option<Bytes> {
        let (payload, used) = self.entries.get_mut(&location)?;
        self.by_use.remove(used);
        self.tick += 1;
        *used = self.tick;
        self.by_use.insert(self.tick, location);
        Some(payload.clone())
    }

    pub fn get(&mut self, location: Location) -> Option<Bytes> { self.touch(location) }

    /// Keeps a copy after admission so a request slice cannot pin its full frame.
    pub fn insert(&mut self, location: Location, payload: &[u8]) {
        if let Some(cost) = self.admit(location, payload.len()) {
            self.store(location, Bytes::copy_from_slice(payload), cost);
        }
    }

    /// Disk reads and the cache can share the admitted compact allocation.
    pub fn insert_read(&mut self, location: Location, payload: Bytes) -> Bytes {
        let Some(cost) = self.admit(location, payload.len()) else { return payload };
        let compact = Bytes::copy_from_slice(&payload);
        self.store(location, compact.clone(), cost);
        compact
    }

    fn admit(&mut self, location: Location, len: usize) -> Option<usize> {
        let cost = len.checked_add(ENTRY_OVERHEAD)?;
        if cost > self.budget || self.touch(location).is_some() { return None; }
        while self.bytes > self.budget - cost {
            let Some((_, oldest)) = self.by_use.pop_first() else { break };
            if let Some((evicted, _)) = self.entries.remove(&oldest) {
                self.bytes -= Self::cost(&evicted);
            }
        }
        Some(cost)
    }

    fn store(&mut self, location: Location, payload: Bytes, cost: usize) {
        self.tick += 1;
        self.entries.insert(location, (payload, self.tick));
        self.by_use.insert(self.tick, location);
        self.bytes += cost;
    }

    /// Follows a payload a compaction copied to a new location.
    pub fn moved(&mut self, from: Location, to: Location) {
        let Some((payload, used)) = self.entries.remove(&from) else { return };
        if self.entries.contains_key(&to) {
            self.by_use.remove(&used);
            self.bytes -= Self::cost(&payload);
            return;
        }
        self.by_use.insert(used, to);
        self.entries.insert(to, (payload, used));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(offset: u64) -> Location { Location { segment: 0, offset, length: 0, payload: 0 } }

    #[test]
    fn the_least_recently_used_payloads_go_first_and_the_budget_holds() {
        let mut cache = Cache::new(3 * (10 + ENTRY_OVERHEAD));
        for offset in 0..3 { cache.insert(at(offset), &[offset as u8; 10]); }
        assert!(cache.get(at(0)).is_some());
        cache.insert(at(3), &[3; 10]);
        assert!(cache.get(at(1)).is_none(), "the least recently used is evicted");
        assert!(cache.get(at(0)).is_some() && cache.get(at(2)).is_some() && cache.get(at(3)).is_some());
        assert_eq!(cache.bytes(), 3 * (10 + ENTRY_OVERHEAD));
        cache.insert(at(4), &[4; 1000]);
        assert!(cache.get(at(4)).is_none(), "a payload larger than the budget is not cached");
        cache.moved(at(3), at(30));
        assert_eq!((cache.get(at(3)), cache.get(at(30)).map(|payload| payload.len())), (None, Some(10)));
    }
}
