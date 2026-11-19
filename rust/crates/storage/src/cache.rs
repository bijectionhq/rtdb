use crate::{Error, ObjectKey, ObjectStore, Result, object_store::{ReadReservation, RetainedBytes}};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, atomic::{AtomicU64, AtomicUsize, Ordering}};

pub struct CacheBudget {
    limit: usize,
    retained: AtomicUsize,
}

struct Reservation { budget: Arc<CacheBudget>, bytes: usize }

impl CacheBudget {
    pub fn new(limit: usize) -> Arc<Self> { Arc::new(Self { limit, retained: AtomicUsize::new(0) }) }
    pub fn retained_bytes(&self) -> usize { self.retained.load(Ordering::Acquire) }
    pub fn limit_bytes(&self) -> usize { self.limit }
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Reservation> {
        self.retained.fetch_update(Ordering::AcqRel, Ordering::Acquire, |retained| {
            retained.checked_add(bytes).filter(|total| *total <= self.limit)
        }).map_err(|retained| Error::MemoryLimit { requested: bytes, limit: self.limit, retained })?;
        Ok(Reservation { budget: self.clone(), bytes })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) { self.budget.retained.fetch_sub(self.bytes, Ordering::AcqRel); }
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

pub struct BufferPool {
    store: Arc<dyn ObjectStore>,
    entries: Mutex<CacheEntries>,
    budget: Arc<CacheBudget>,
    metadata: Option<Arc<MemoryBudget>>,
    access_clock: AtomicU64,
}

impl BufferPool {
    pub fn new(store: Arc<dyn ObjectStore>, byte_limit: usize) -> Self {
        Self::with_budget(store, CacheBudget::new(byte_limit))
    }

    /// Shares an allocation ceiling across pools; each pool evicts only its own idle entries.
    pub fn with_budget(store: Arc<dyn ObjectStore>, budget: Arc<CacheBudget>) -> Self {
        Self {
            store, entries: Mutex::new(BTreeMap::new()),
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

    /// Drops this pool's entry and any copy the store holds on this node, even while leased; a lease keeps its bytes, and the next read goes to the store.
    pub async fn invalidate(&self, key: &ObjectKey) -> Result<()> {
        remove_entry(&mut self.entries.lock(), key);
        self.store.forget(key).await
    }

    /// An evicted object remains charged while a caller retains any slice of its lease.
    pub fn evict(&self, key: &ObjectKey) -> bool {
        let mut entries = self.entries.lock();
        if entries.get(key).is_some_and(|entry| Arc::strong_count(entry) == 1) {
            remove_entry(&mut entries, key);
            true
        } else { false }
    }

    fn entry(&self, key: &ObjectKey) -> Result<Arc<CacheEntry>> {
        let mut entries = self.entries.lock();
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
        if let Ok(reservation) = self.budget.reserve(bytes) { return Ok(reservation); }
        let mut entries = self.entries.lock();
        let mut idle: Vec<_> = entries.iter()
            .filter(|(_, entry)| Arc::strong_count(entry) == 1)
            .map(|(key, entry)| (entry.last_used.load(Ordering::Relaxed), key.clone())).collect();
        idle.sort();
        for (_, key) in idle {
            entries.remove(&key);
            if let Ok(reservation) = self.budget.reserve(bytes) { return Ok(reservation); }
        }
        self.budget.reserve(bytes)
    }

    /// Concurrent fetches of a key share one load; cancellation releases that caller's wait.
    /// An empty entry is removed when its last fetcher fails or is cancelled.
    pub async fn get(&self, key: &ObjectKey) -> Result<CacheLease> {
        let mut cleanup = EmptyEntryCleanup { entries: &self.entries, key, armed: true };
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
