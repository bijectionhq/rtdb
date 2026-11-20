use crate::{Error, ObjectKey, ObjectStore, Result, object_store::{ReadReservation, RetainedBytes}};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Weak, atomic::{AtomicU64, AtomicUsize, Ordering}};

/// An allocation ceiling shared by every pool drawing on it.
/// When a reservation does not fit, idle entries are evicted from whichever pool holds the most, so one pool's idle data cannot starve another.
pub struct CacheBudget {
    limit: usize,
    retained: AtomicUsize,
    pools: Mutex<Vec<Weak<PoolShared>>>,
}

struct Reservation { budget: Arc<CacheBudget>, pool: Arc<AtomicUsize>, bytes: usize }

impl CacheBudget {
    pub fn new(limit: usize) -> Arc<Self> { Arc::new(Self { limit, retained: AtomicUsize::new(0), pools: Mutex::new(Vec::new()) }) }
    pub fn retained_bytes(&self) -> usize { self.retained.load(Ordering::Acquire) }
    pub fn limit_bytes(&self) -> usize { self.limit }
    fn reserve(self: &Arc<Self>, pool: &Arc<AtomicUsize>, bytes: usize) -> Result<Reservation> {
        self.retained.fetch_update(Ordering::AcqRel, Ordering::Acquire, |retained| {
            retained.checked_add(bytes).filter(|total| *total <= self.limit)
        }).map_err(|retained| Error::MemoryLimit { requested: bytes, limit: self.limit, retained })?;
        pool.fetch_add(bytes, Ordering::AcqRel);
        Ok(Reservation { budget: self.clone(), pool: Arc::clone(pool), bytes })
    }

    fn register(&self, pool: &Arc<PoolShared>) {
        let mut pools = self.pools.lock();
        pools.retain(|pool| pool.strong_count() > 0);
        pools.push(Arc::downgrade(pool));
    }

    /// Evicts the oldest idle entry of the pool retaining the most bytes that has one; false if no pool has an idle entry.
    fn evict_from_largest(&self) -> bool {
        let mut pools: Vec<Arc<PoolShared>> = self.pools.lock().iter().filter_map(Weak::upgrade).collect();
        pools.sort_by_key(|pool| std::cmp::Reverse(pool.retained.load(Ordering::Acquire)));
        pools.iter().any(|pool| reclaim_oldest_idle(&mut pool.entries.lock()))
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.retained.fetch_sub(self.bytes, Ordering::AcqRel);
        self.pool.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct CacheLease {
    allocation: Arc<RetainedBytes>,
    range: Range<usize>,
}

impl AsRef<[u8]> for CacheLease {
    fn as_ref(&self) -> &[u8] { self.as_slice() }
}

impl CacheLease {
    pub fn as_slice(&self) -> &[u8] { &self.allocation.as_slice()[self.range.clone()] }

    /// The slice retains the entire allocation and its byte reservation.
    pub fn slice(&self, range: Range<usize>) -> Result<Self> {
        if range.start > range.end || range.end > self.range.len() {
            return Err(Error::InvalidRange {
                key: "cache lease".into(), start: range.start as u64,
                end: range.end as u64, length: self.range.len() as u64,
            });
        }
        Ok(Self { allocation: self.allocation.clone(), range: self.range.start + range.start..self.range.start + range.end })
    }
}

struct CacheEntry {
    allocation: tokio::sync::Mutex<Option<Arc<RetainedBytes>>>,
    last_used: AtomicU64,
    _reservation: Option<Arc<MemoryReservation>>,
}

type CacheEntries = BTreeMap<ObjectKey, Arc<CacheEntry>>;

fn remove_entry(entries: &mut CacheEntries, key: &ObjectKey) {
    let removed = entries.remove_entry(key);
    if entries.is_empty() { entries.clear(); }
    drop(removed);
}

fn reclaim_oldest_idle(entries: &mut CacheEntries) -> bool {
    let Some(oldest) = entries.values().filter(|entry| Arc::strong_count(entry) == 1)
        .min_by_key(|entry| entry.last_used.load(Ordering::Relaxed)).map(Arc::as_ptr) else { return false; };
    let removed = entries.extract_if(.., |_, entry| Arc::as_ptr(entry) == oldest).next();
    if entries.is_empty() { entries.clear(); }
    removed.is_some()
}

struct EmptyEntryCleanup<'a> {
    entries: &'a Mutex<CacheEntries>,
    key: &'a ObjectKey,
    armed: bool,
}

impl Drop for EmptyEntryCleanup<'_> {
    fn drop(&mut self) {
        if !self.armed { return; }
        let mut entries = self.entries.lock();
        let empty = entries.get(self.key).is_some_and(|entry| {
            Arc::strong_count(entry) == 1 && entry.allocation.try_lock().is_ok_and(|slot| slot.is_none())
        });
        if empty { remove_entry(&mut entries, self.key); }
    }
}

struct PoolShared { entries: Mutex<CacheEntries>, retained: Arc<AtomicUsize> }

pub struct BufferPool {
    store: Arc<dyn ObjectStore>,
    shared: Arc<PoolShared>,
    budget: Arc<CacheBudget>,
    metadata: Option<Arc<MemoryBudget>>,
    access_clock: AtomicU64,
}

impl BufferPool {
    pub fn new(store: Arc<dyn ObjectStore>, byte_limit: usize) -> Self {
        Self::with_budget(store, CacheBudget::new(byte_limit))
    }

    /// Shares an allocation ceiling across pools; a reservation that does not fit evicts idle entries from the pool retaining the most.
    pub fn with_budget(store: Arc<dyn ObjectStore>, budget: Arc<CacheBudget>) -> Self {
        let shared = Arc::new(PoolShared { entries: Mutex::new(BTreeMap::new()), retained: Arc::new(AtomicUsize::new(0)) });
        budget.register(&shared);
        Self {
            store, shared,
            budget,
            metadata: None,
            access_clock: AtomicU64::new(0),
        }
    }

    /// Charges retained cache entries to `metadata` and fetched object bytes to `budget`.
    /// If entry admission fails, the pool can reclaim one idle entry and retry.
    /// If admission still fails, `get` returns `Error::MemoryLimit` before reaching object storage.
    pub fn with_metadata_budget(store: Arc<dyn ObjectStore>, budget: Arc<CacheBudget>, metadata: Arc<MemoryBudget>) -> Self {
        Self { metadata: Some(metadata), ..Self::with_budget(store, budget) }
    }

    pub fn retained_bytes(&self) -> usize { self.budget.retained.load(Ordering::Acquire) }
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> { &self.store }

    /// An evicted object remains charged while a caller retains any slice of its lease.
    pub fn evict(&self, key: &ObjectKey) -> bool {
        let mut entries = self.shared.entries.lock();
        if entries.get(key).is_some_and(|entry| Arc::strong_count(entry) == 1) {
            remove_entry(&mut entries, key);
            true
        } else { false }
    }

    fn entry(&self, key: &ObjectKey) -> Result<Arc<CacheEntry>> {
        let mut entries = self.shared.entries.lock();
        if let Some(entry) = entries.get(key) {
            return Ok(Arc::clone(entry));
        }

        let reservation = self.metadata.as_ref().map(|memory| {
            let bytes = key.as_str().len().checked_add(std::mem::size_of::<CacheEntry>() + 1024)
                .ok_or_else(|| Error::Task("Cache entry metadata allocation exceeds range".into()))?;
            memory.reserve(bytes).or_else(|error| {
                if reclaim_oldest_idle(&mut entries) { memory.reserve(bytes) } else { Err(error) }
            }).map_err(|_| Error::MemoryLimit { requested: bytes, limit: memory.limit_bytes(), retained: memory.used_bytes() })
        }).transpose()?;
        let entry = Arc::new(CacheEntry {
            allocation: tokio::sync::Mutex::new(None),
            last_used: AtomicU64::new(0),
            _reservation: reservation,
        });
        entries.insert(key.clone(), Arc::clone(&entry));
        Ok(entry)
    }

    fn reserve(&self, bytes: usize) -> Result<Reservation> {
        loop {
            match self.budget.reserve(&self.shared.retained, bytes) {
                Ok(reservation) => return Ok(reservation),
                Err(error) => if !self.budget.evict_from_largest() { return Err(error); },
            }
        }
    }

    /// `range` of an object: a slice of the object where the pool holds all of it, and otherwise those bytes alone,
    /// fetched and charged to the pool's budget for as long as the lease lives, but not kept.
    pub async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<CacheLease> {
        let held = self.shared.entries.lock().get(key).cloned();
        if let Some(entry) = held && let Ok(slot) = entry.allocation.try_lock() && let Some(allocation) = slot.as_ref() {
            let (start, end) = (usize::try_from(range.start), usize::try_from(range.end));
            if let (Ok(start), Ok(end)) = (start, end) && start <= end && end <= allocation.len() {
                return Ok(CacheLease { allocation: allocation.clone(), range: start..end });
            }
        }
        let length = range.end.checked_sub(range.start).and_then(|length| usize::try_from(length).ok())
            .ok_or_else(|| Error::Task("Object range is inverted or exceeds addressable memory".into()))?;
        let reservation = ReadReservation::new(length, self.reserve(length)?);
        let allocation = Arc::new(self.store.get_reserved_range(key, range, reservation).await?);
        Ok(CacheLease { allocation, range: 0..length })
    }

    /// Whether the pool holds all of an object, so that a whole read of it costs nothing more.
    pub fn holds(&self, key: &ObjectKey) -> bool {
        let held = self.shared.entries.lock().get(key).cloned();
        held.is_some_and(|entry| entry.allocation.try_lock().is_ok_and(|slot| slot.is_some()))
    }

    /// Concurrent fetches of a key share one load; cancellation releases that caller's wait.
    /// An empty entry is removed when its last fetcher fails or is cancelled.
    pub async fn get(&self, key: &ObjectKey) -> Result<CacheLease> {
        let mut cleanup = EmptyEntryCleanup { entries: &self.shared.entries, key, armed: true };
        let allocation = {
            let entry = self.entry(key)?;
            entry.last_used.store(self.access_clock.fetch_add(1, Ordering::Relaxed), Ordering::Relaxed);
            let mut slot = entry.allocation.lock().await;
            match slot.as_ref() {
                Some(allocation) => allocation.clone(),
                None => {
                    let size = self.store.head(key).await?.size;
                    let size = usize::try_from(size).map_err(|_| Error::MemoryLimit {
                        requested: usize::MAX, limit: self.budget.limit, retained: self.retained_bytes(),
                    })?;
                    let reservation = ReadReservation::new(size, self.reserve(size)?);
                    let allocation = Arc::new(self.store.get_reserved_range(key, 0..size as u64, reservation).await?);
                    *slot = Some(allocation.clone());
                    allocation
                }
            }
        };
        cleanup.armed = false;
        let length = allocation.len();
        Ok(CacheLease { allocation, range: 0..length })
    }
}

#[cfg(test)]
mod fairness_tests {
    use super::*;
    use crate::MemoryStore;
    use bytes::Bytes;

    #[tokio::test]
    async fn one_pools_idle_entries_cannot_starve_another_pool_sharing_its_budget() {
        let budget = CacheBudget::new(100);
        let (first_store, second_store) = (Arc::new(MemoryStore::default()), Arc::new(MemoryStore::default()));
        for index in 0..4 { first_store.put(&ObjectKey::new(format!("a{index}")).unwrap(), Bytes::from(vec![1u8; 25])).await.unwrap(); }
        second_store.put(&ObjectKey::new("b").unwrap(), Bytes::from(vec![2u8; 60])).await.unwrap();
        let first = BufferPool::with_budget(first_store, Arc::clone(&budget));
        let second = BufferPool::with_budget(second_store, Arc::clone(&budget));
        for index in 0..4 { drop(first.get(&ObjectKey::new(format!("a{index}")).unwrap()).await.unwrap()); }
        assert_eq!(budget.retained_bytes(), 100, "the first pool's idle entries fill the budget");
        let lease = second.get(&ObjectKey::new("b").unwrap()).await.expect("the second pool evicts the first pool's idle entries");
        assert_eq!(lease.as_slice().len(), 60);
        assert_eq!(budget.retained_bytes(), 85, "only as many as it needed");
    }
}
