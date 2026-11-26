use crate::{Error, ObjectKey, ObjectStore, Result, object_store::{ReadReservation, RetainedBytes, ObjectReadAllocation}};
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::log::proto::PublishedObjectVersion;
use std::any::Any;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Weak, atomic::{AtomicU64, Ordering}};
use std::time::Duration;

/// A decoded form of an object, which a pool holds beside or instead of the object's bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum View { File, Layout, Page(usize) }

/// A decoded view held by a pool; it retains the reservation `BufferPool::admit` gave for it.
pub trait Cached: Any + Send + Sync {
    /// Whether a reader still holds what this value retains, so that evicting it would free nothing.
    fn in_use(&self) -> bool { false }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Slot { Object, View(View) }

enum Value {
    Object(Arc<CacheEntry>),
    View { value: Arc<dyn Any + Send + Sync>, in_use: fn(&(dyn Any + Send + Sync)) -> bool, _overhead: Arc<MemoryReservation> },
}

impl Value {
    /// Whether evicting this entry releases its bytes without interrupting a load.
    fn idle(&self) -> bool {
        match self {
            Self::Object(entry) => Arc::strong_count(entry) == 1
                && entry.allocation.try_lock().is_ok_and(|slot| slot.as_ref().is_none_or(|allocation| Arc::strong_count(allocation) == 1)),
            Self::View { value, in_use, .. } => Arc::strong_count(value) == 1 && !in_use(value.as_ref()),
        }
    }
}

fn in_use_of<T: Cached>(value: &(dyn Any + Send + Sync)) -> bool { value.downcast_ref::<T>().is_some_and(T::in_use) }

struct Entry { pool: u64, key: ObjectKey, slot: Slot, value: Value }

const NIL: usize = usize::MAX;
struct Node { prev: usize, next: usize, entry: Option<Entry> }

/// Every entry of every pool sharing a budget, in one recency list from `head`, used last, to `tail`, used first.
struct Lru {
    index: HashMap<u64, HashMap<ObjectKey, HashMap<Slot, usize>>>,
    nodes: Vec<Node>,
    free: Vec<usize>,
    head: usize,
    tail: usize,
}

impl Lru {
    fn new() -> Self { Self { index: HashMap::new(), nodes: Vec::new(), free: Vec::new(), head: NIL, tail: NIL } }

    fn find(&self, pool: u64, key: &ObjectKey, slot: Slot) -> Option<usize> {
        self.index.get(&pool)?.get(key)?.get(&slot).copied()
    }

    fn value(&self, node: usize) -> Option<&Value> { self.nodes[node].entry.as_ref().map(|entry| &entry.value) }

    fn unlink(&mut self, node: usize) {
        let (prev, next) = (self.nodes[node].prev, self.nodes[node].next);
        if prev == NIL { self.head = next; } else { self.nodes[prev].next = next; }
        if next == NIL { self.tail = prev; } else { self.nodes[next].prev = prev; }
    }

    fn push_front(&mut self, node: usize) {
        self.nodes[node].prev = NIL;
        self.nodes[node].next = self.head;
        if self.head == NIL { self.tail = node; } else { self.nodes[self.head].prev = node; }
        self.head = node;
    }

    fn touch(&mut self, node: usize) {
        if self.head != node { self.unlink(node); self.push_front(node); }
    }

    /// Returns the value this one replaces, for the caller to drop once it has released the lock.
    fn insert(&mut self, pool: u64, key: &ObjectKey, slot: Slot, value: Value) -> Option<Value> {
        if let Some(node) = self.find(pool, key, slot) {
            self.touch(node);
            return self.nodes[node].entry.as_mut().map(|entry| std::mem::replace(&mut entry.value, value));
        }
        let node = Node { prev: NIL, next: NIL, entry: Some(Entry { pool, key: key.clone(), slot, value }) };
        let index = match self.free.pop() {
            Some(index) => { self.nodes[index] = node; index }
            None => { self.nodes.push(node); self.nodes.len() - 1 }
        };
        self.index.entry(pool).or_default().entry(key.clone()).or_default().insert(slot, index);
        self.push_front(index);
        None
    }

    fn remove(&mut self, node: usize) -> Option<Value> {
        let Entry { pool, key, slot, value } = self.nodes[node].entry.take()?;
        self.unlink(node);
        self.free.push(node);
        if let Some(files) = self.index.get_mut(&pool) {
            if let Some(slots) = files.get_mut(&key) {
                slots.remove(&slot);
                if slots.is_empty() { files.remove(&key); }
            }
            if files.is_empty() { self.index.remove(&pool); }
        }
        Some(value)
    }

    /// Removes the least recently used entry that `evictable` accepts.
    fn evict(&mut self, evictable: impl Fn(&Entry) -> bool) -> Option<Value> {
        let mut node = self.tail;
        while node != NIL {
            if self.nodes[node].entry.as_ref().is_some_and(&evictable) { return self.remove(node); }
            node = self.nodes[node].prev;
        }
        None
    }

    fn remove_key(&mut self, pool: u64, key: &ObjectKey) -> Vec<Value> {
        let nodes: Vec<_> = self.index.get(&pool).and_then(|files| files.get(key))
            .into_iter().flat_map(|slots| slots.values().copied()).collect();
        nodes.into_iter().filter_map(|node| self.remove(node)).collect()
    }

    fn remove_pool(&mut self, pool: u64) -> Vec<Value> {
        let nodes: Vec<usize> = self.index.get(&pool).into_iter().flat_map(|files| files.values().flat_map(|slots| slots.values().copied())).collect();
        nodes.into_iter().filter_map(|node| self.remove(node)).collect()
    }
}

/// An allocation ceiling shared by every pool drawing on it, over their objects' bytes and their decoded views alike.
/// A reservation that does not fit evicts the least recently used idle entries of any of those pools until it does.
pub struct CacheBudget {
    memory: Arc<MemoryBudget>,
    lru: Mutex<Lru>,
    pools: AtomicU64,
}

impl CacheBudget {
    pub fn new(limit: usize) -> Arc<Self> { Self::with_memory(MemoryBudget::new(limit)) }
    pub fn with_memory(memory: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self { memory, lru: Mutex::new(Lru::new()), pools: AtomicU64::new(0) })
    }
    pub fn prepaid_by(&self, parent: &Arc<MemoryBudget>) -> bool { self.memory.prepaid_by(parent) }
    pub fn retained_bytes(&self) -> usize { self.memory.used_bytes() }
    pub fn limit_bytes(&self) -> usize { self.memory.limit_bytes() }

    fn limit_error(&self, requested: usize) -> Error {
        Error::MemoryLimit { requested, limit: self.limit_bytes(), retained: self.retained_bytes() }
    }

    fn reserve(&self, scope: &Arc<MemoryBudget>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        // A request no eviction could make fit evicts nothing.
        if bytes > self.limit_bytes() { return Err(self.limit_error(bytes)); }
        loop {
            if let Ok(reservation) = scope.reserve(bytes) { return Ok(reservation); }
            let evicted = self.lru.lock().evict(|entry| entry.value.idle());
            match evicted { Some(evicted) => drop(evicted), None => return Err(self.limit_error(bytes)) }
        }
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
    pub fn is_partial_allocation(&self) -> bool { self.range.len() != self.allocation.len() }

    /// Memoizes the successfully computed SHA-256 of the complete immutable allocation.
    pub fn sha256<E>(&self, compute: impl FnOnce(&[u8]) -> std::result::Result<[u8; 32], E>) -> std::result::Result<[u8; 32], E> {
        if self.is_partial_allocation() { return compute(self.as_slice()); }
        let mut digest = self.allocation.sha256.lock();
        if let Some(digest) = *digest { return Ok(digest); }
        let value = compute(self.as_slice())?;
        *digest = Some(value);
        Ok(value)
    }

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
    _reservation: Option<Arc<MemoryReservation>>,
}

struct EmptyEntryCleanup<'a> {
    pool: &'a BufferPool,
    key: &'a ObjectKey,
    armed: bool,
}

impl Drop for EmptyEntryCleanup<'_> {
    fn drop(&mut self) {
        if !self.armed { return; }
        let mut lru = self.pool.budget.lru.lock();
        let Some(node) = lru.find(self.pool.id, self.key, Slot::Object) else { return };
        let empty = matches!(lru.value(node), Some(Value::Object(entry))
            if Arc::strong_count(entry) == 1 && entry.allocation.try_lock().is_ok_and(|slot| slot.is_none()));
        let removed = if empty { lru.remove(node) } else { None };
        drop(lru);
        drop(removed);
    }
}

pub(crate) fn reserve_read(length: usize, allocation: ObjectReadAllocation, reserve: impl Fn(usize) -> Result<Arc<MemoryReservation>>) -> Result<ReadReservation> {
    let retained = reserve(allocation.retained_bytes)?;
    let scratch = (allocation.scratch_bytes > 0).then(|| reserve(allocation.scratch_bytes)).transpose()?;
    Ok(ReadReservation::funded(length, retained, scratch))
}

fn reserve_from(memory: &Arc<MemoryBudget>, bytes: usize) -> Result<Arc<MemoryReservation>> {
    memory.reserve(bytes).map_err(|_| Error::MemoryLimit { requested: bytes, limit: memory.limit_bytes(), retained: memory.used_bytes() })
}

/// What a view's entry retains beyond the view: its node, two copies of its key and its index slots.
fn view_overhead(key: &ObjectKey) -> usize { key.as_str().len().saturating_mul(2).saturating_add(2 * std::mem::size_of::<Node>() + 256) }

/// A catalog registration issued by the storage owner for one buffer-pool residence.
pub struct HeldCatalogRead {
    residence: Arc<()>,
    cut: Option<rtdb_types::CatalogCut>,
    registration: Option<Arc<dyn Send + Sync>>,
    catalog_file_paths: Option<Arc<[String]>>,
    checkpoint: Option<rtdb_types::CatalogCheckpoint>,
    admitted: std::time::Instant,
}

impl HeldCatalogRead {
    pub fn is_registered(&self) -> bool { self.registration.is_some() }
    pub fn cut(&self) -> Option<rtdb_types::CatalogCut> { self.cut }

    fn covers(&self, cut: Option<rtdb_types::CatalogCut>) -> bool {
        match (self.cut, cut) {
            (Some(held), Some(cut)) => held.storage_version == cut.storage_version
                && held.storage_epoch == cut.storage_epoch && held.replica_epoch == cut.replica_epoch
                && if self.checkpoint.is_some() { held.through == cut.through } else { held.through <= cut.through },
            (None, None) => true,
            _ => false,
        }
    }
}

/// How long after its admission a catalog hold may serve further readers of the same catalog, and outlives its own.
/// Readers arriving within it share one storage registration; later ones are admitted afresh,
/// so the store's admission checks (floors, erasure, residence) see every catalog at least this often.
const CATALOG_HOLD_SHARING: Duration = Duration::from_millis(100);

/// Catalog holds this pool has issued and that a later reader of the same catalog may share.
#[derive(Default)]
struct IssuedHolds {
    live: Vec<Weak<HeldCatalogRead>>,
    lingering: Option<Arc<HeldCatalogRead>>,
}

pub struct BufferPool {
    residence: Arc<()>,
    issued_holds: Arc<Mutex<IssuedHolds>>,
    store: Arc<dyn ObjectStore>,
    budget: Arc<CacheBudget>,
    memory: Arc<MemoryBudget>,
    id: u64,
    metadata: Option<Arc<MemoryBudget>>,
    scan_timing: ScanTiming,
}

struct ScanTiming { latency_micros: AtomicU64, bytes_per_millisecond: AtomicU64 }

impl Default for ScanTiming {
    fn default() -> Self {
        Self { latency_micros: AtomicU64::new(4000), bytes_per_millisecond: AtomicU64::new(32 * 1024) }
    }
}

impl ScanTiming {
    fn observe(&self, len: usize, elapsed: std::time::Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX).max(1);
        let (estimate, sample) = if len <= 64 * 1024 {
            (&self.latency_micros, micros.clamp(50, 200_000))
        } else {
            let latency = self.latency_micros.load(Ordering::Relaxed);
            let rate = (len as u64).saturating_mul(1000) / micros.saturating_sub(latency).max(100);
            (&self.bytes_per_millisecond, rate.clamp(1024, 512 * 1024))
        };
        let _ = estimate.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| Some((old * 7 + sample) / 8));
    }

    fn merge_gap(&self) -> u64 {
        let latency = self.latency_micros.load(Ordering::Relaxed);
        let rate = self.bytes_per_millisecond.load(Ordering::Relaxed);
        (latency * rate / 2000).clamp(8192, 256 * 1024)
    }
}

impl BufferPool {
    pub fn new(store: Arc<dyn ObjectStore>, byte_limit: usize) -> Self {
        Self::with_budget(store, CacheBudget::new(byte_limit))
    }

    /// Shares an allocation ceiling across pools; a reservation that does not fit evicts the least recently used idle entries of any of them.
    pub fn with_budget(store: Arc<dyn ObjectStore>, budget: Arc<CacheBudget>) -> Self {
        Self::open(store, budget, None)
    }

    fn open(store: Arc<dyn ObjectStore>, budget: Arc<CacheBudget>, metadata: Option<Arc<MemoryBudget>>) -> Self {
        let id = budget.pools.fetch_add(1, Ordering::Relaxed);
        let memory = budget.memory.scope();
        Self { residence: Arc::new(()), issued_holds: Arc::default(), store, budget, memory, id, metadata, scan_timing: ScanTiming::default() }
    }

    /// Charges the bookkeeping of retained objects to `metadata`, and their bytes and every view to `budget`.
    /// If an object's admission fails, the pool can reclaim one of its idle objects and retry.
    /// If admission still fails, `get` returns `Error::MemoryLimit` before reaching object storage.
    pub fn with_metadata_budget(store: Arc<dyn ObjectStore>, budget: Arc<CacheBudget>, metadata: Arc<MemoryBudget>) -> Self {
        Self::open(store, budget, Some(metadata))
    }

    /// The reader's prepaid metadata allocation for a scoped cache.
    pub fn prepaid_metadata_budget(&self) -> Option<&Arc<MemoryBudget>> {
        self.metadata.as_ref().filter(|memory| memory.is_prepaid())
    }

    pub fn retained_bytes(&self) -> usize { self.budget.retained_bytes() }
    pub fn budget(&self) -> &Arc<CacheBudget> { &self.budget }
    pub fn scan_merge_gap(&self) -> u64 { self.scan_timing.merge_gap() }
    pub fn scan_byte_limit(&self) -> usize { self.memory.limit_bytes() }

    /// Bytes charged to this pool, including decoded views and outstanding read leases.
    pub fn pool_retained_bytes(&self) -> usize { self.memory.used_bytes() }

    pub fn object_store(&self) -> &Arc<dyn ObjectStore> { &self.store }

    pub async fn hold_catalog(&self, cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        self.hold_catalog_with_paths(cut, None).await
    }

    pub async fn hold_catalog_with_paths(&self, cut: Option<rtdb_types::CatalogCut>, paths: Option<Arc<[String]>>)
        -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        if let Some(held) = self.issued_hold(cut, paths.as_deref(), None) { return Ok(held); }
        let held = self.issue_catalog_hold(cut, paths).await?;
        Ok(self.remember_hold(held))
    }

    async fn issue_catalog_hold(&self, cut: Option<rtdb_types::CatalogCut>, paths: Option<Arc<[String]>>)
        -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        let admitted = std::time::Instant::now();
        let registration = match &paths {
            Some(paths) => self.store.retain_catalog_with_paths(cut, Arc::clone(paths)).await?,
            None => self.store.retain_catalog(cut).await?,
        };
        self.finish_catalog_hold(cut, paths, registration, None, admitted)
    }

    pub async fn hold_anchored_catalog(
        &self, cut: rtdb_types::CatalogCut, base: rtdb_types::CatalogCheckpoint, paths: Option<Arc<[String]>>,
    ) -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        if let Some(held) = self.issued_hold(Some(cut), paths.as_deref(), Some(base)) { return Ok(held); }
        let held = self.issue_anchored_catalog(cut, base, paths).await?;
        Ok(self.remember_hold(held))
    }

    async fn issue_anchored_catalog(
        &self, cut: rtdb_types::CatalogCut, base: rtdb_types::CatalogCheckpoint, paths: Option<Arc<[String]>>,
    ) -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        let admitted = std::time::Instant::now();
        let registration = self.store.retain_anchored_catalog(cut, base, paths.clone()).await?;
        self.finish_catalog_hold(Some(cut), paths, registration, Some(base), admitted)
    }

    fn finish_catalog_hold(
        &self, cut: Option<rtdb_types::CatalogCut>, paths: Option<Arc<[String]>>,
        registration: Option<Arc<dyn Send + Sync>>, checkpoint: Option<rtdb_types::CatalogCheckpoint>,
        admitted: std::time::Instant,
    ) -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        if self.store.defers_trie_releases() && registration.is_none() {
            return Err(rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.query/catalog-not-held",
                "The coordinated storage provider returned no catalog hold"));
        }
        Ok(Arc::new(HeldCatalogRead { residence: Arc::clone(&self.residence), cut, registration, catalog_file_paths: paths, checkpoint, admitted }))
    }

    /// Re-admit the same scope through the current owner. The old cohort retains
    /// its original permit until its readers, provider tasks and deliveries drain.
    pub async fn renew_catalog(&self, held: &HeldCatalogRead) -> rtdb_types::Result<Arc<HeldCatalogRead>> {
        let closed = || rtdb_types::Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.storage/catalog-not-held",
            "The storage residence did not renew the catalog hold");
        if !Arc::ptr_eq(&self.residence, &held.residence) || !held.is_registered() { return Err(closed()); }
        let paths = held.catalog_file_paths.clone();
        let renewed = match held.checkpoint {
            Some(checkpoint) => self.issue_anchored_catalog(held.cut.ok_or_else(closed)?, checkpoint, paths).await?,
            None => self.issue_catalog_hold(held.cut, paths).await?,
        };
        if !renewed.is_registered() { return Err(closed()); }
        Ok(renewed)
    }

    /// A hold this pool admitted for exactly this catalog within the sharing window, held without a gap since.
    fn issued_hold(&self, cut: Option<rtdb_types::CatalogCut>, paths: Option<&[String]>, checkpoint: Option<rtdb_types::CatalogCheckpoint>)
        -> Option<Arc<HeldCatalogRead>> {
        let same_checkpoint = |held: &HeldCatalogRead| match (held.checkpoint, checkpoint) {
            (Some(held), Some(wanted)) => held.boundary == wanted.boundary && held.root == wanted.root,
            (None, None) => true,
            _ => false,
        };
        let candidates: Vec<Arc<HeldCatalogRead>> = {
            let mut issued = self.issued_holds.lock();
            issued.live.retain(|held| held.strong_count() > 0);
            issued.live.iter().rev().filter_map(Weak::upgrade).collect()
        };
        // Dropping a candidate can release its registration, so the choice is made outside the lock.
        let shareable: Vec<_> = candidates.into_iter().filter(|held| {
            held.is_registered() && Arc::ptr_eq(&self.residence, &held.residence)
                && held.catalog_file_paths.as_deref() == paths && same_checkpoint(held)
                && held.admitted.elapsed() < CATALOG_HOLD_SHARING
        }).collect();
        if let Some(exact) = shareable.iter().find(|held| held.cut == cut) { return Some(Arc::clone(exact)); }
        let later = |held: &HeldCatalogRead| match (held.cut, cut) {
            (Some(held), Some(cut)) => held.storage_version == cut.storage_version && held.storage_epoch == cut.storage_epoch
                && held.replica_epoch == cut.replica_epoch && held.through <= cut.through,
            _ => false,
        };
        if !self.store.catalog_registration_covers_later_cuts() { return None; }
        let earlier = shareable.iter().find(|held| later(held))?;
        // The reader keeps its own cut, which renewal re-admits; only the registration is shared.
        let extended = Arc::new(HeldCatalogRead {
            residence: Arc::clone(&earlier.residence), cut, registration: earlier.registration.clone(),
            catalog_file_paths: earlier.catalog_file_paths.clone(), checkpoint: earlier.checkpoint, admitted: earlier.admitted,
        });
        self.issued_holds.lock().live.push(Arc::downgrade(&extended));
        Some(extended)
    }

    fn remember_hold(&self, held: Arc<HeldCatalogRead>) -> Arc<HeldCatalogRead> {
        if !held.is_registered() { return held; }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.issued_holds.lock().live.push(Arc::downgrade(&held));
            return held;
        };
        let replaced = {
            let mut issued = self.issued_holds.lock();
            issued.live.retain(|held| held.strong_count() > 0);
            issued.live.push(Arc::downgrade(&held));
            issued.lingering.replace(Arc::clone(&held))
        };
        // Releasing a registration can remove its storage ticket, so it happens outside the lock.
        drop(replaced);
        let issued = Arc::downgrade(&self.issued_holds);
        let lingering = Arc::downgrade(&held);
        runtime.spawn(async move {
            tokio::time::sleep(CATALOG_HOLD_SHARING).await;
            let Some(issued) = issued.upgrade() else { return; };
            let expired = {
                let mut issued = issued.lock();
                if issued.lingering.as_ref().is_some_and(|held| Weak::ptr_eq(&Arc::downgrade(held), &lingering)) { issued.lingering.take() } else { None }
            };
            drop(expired);
        });
        held
    }

    pub fn holds_catalog(&self, held: &HeldCatalogRead, cut: Option<rtdb_types::CatalogCut>) -> bool {
        self.holds_catalog_with_paths(held, cut, None)
    }

    pub fn holds_catalog_with_paths(&self, held: &HeldCatalogRead, cut: Option<rtdb_types::CatalogCut>, paths: Option<&[String]>) -> bool {
        Arc::ptr_eq(&self.residence, &held.residence) && held.covers(cut)
            && held.catalog_file_paths.as_deref().is_none_or(|held_paths| held.cut == cut && paths == Some(held_paths))
    }

    fn reserve(&self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.budget.reserve(&self.memory, bytes)
    }

    /// Drops the object's bytes, decoded views and disk copy from this pool's caches. Outstanding leases keep their bytes.
    pub async fn invalidate(&self, key: &ObjectKey) -> Result<()> {
        let removed = self.budget.lru.lock().remove_key(self.id, key);
        drop(removed);
        self.store.forget(key).await
    }

    /// An evicted object remains charged while a caller retains any slice of its lease.
    pub fn evict(&self, key: &ObjectKey) -> bool {
        let mut lru = self.budget.lru.lock();
        let Some(node) = lru.find(self.id, key, Slot::Object) else { return false };
        if !matches!(lru.value(node), Some(Value::Object(entry)) if Arc::strong_count(entry) == 1) { return false; }
        let removed = lru.remove(node);
        drop(lru);
        drop(removed);
        true
    }

    fn held(&self, key: &ObjectKey) -> Option<Arc<CacheEntry>> {
        let mut lru = self.budget.lru.lock();
        let node = lru.find(self.id, key, Slot::Object)?;
        lru.touch(node);
        match lru.value(node) { Some(Value::Object(entry)) => Some(Arc::clone(entry)), _ => None }
    }

    fn entry(&self, key: &ObjectKey) -> Result<Arc<CacheEntry>> {
        let mut lru = self.budget.lru.lock();
        if let Some(node) = lru.find(self.id, key, Slot::Object) {
            lru.touch(node);
            if let Some(Value::Object(entry)) = lru.value(node) { return Ok(Arc::clone(entry)); }
        }

        let reservation = self.metadata.as_ref().map(|memory| {
            let bytes = key.as_str().len().checked_add(std::mem::size_of::<CacheEntry>() + 1024)
                .ok_or_else(|| Error::Task("Cache entry metadata allocation exceeds range".into()))?;
            memory.reserve(bytes).or_else(|error| {
                let pool = self.id;
                match lru.evict(|entry| entry.pool == pool && entry.slot == Slot::Object && entry.value.idle()) {
                    Some(evicted) => { drop(evicted); memory.reserve(bytes) }
                    None => Err(error),
                }
            }).map_err(|_| Error::MemoryLimit { requested: bytes, limit: memory.limit_bytes(), retained: memory.used_bytes() })
        }).transpose()?;
        let entry = Arc::new(CacheEntry { allocation: tokio::sync::Mutex::new(None), _reservation: reservation });
        let replaced = lru.insert(self.id, key, Slot::Object, Value::Object(Arc::clone(&entry)));
        drop(lru);
        drop(replaced);
        Ok(entry)
    }

    fn held_range(&self, key: &ObjectKey, range: &Range<u64>) -> Option<CacheLease> {
        let entry = self.held(key)?;
        let slot = entry.allocation.try_lock().ok()?;
        let allocation = slot.as_ref()?;
        let (start, end) = (usize::try_from(range.start).ok()?, usize::try_from(range.end).ok()?);
        (start <= end && end <= allocation.len()).then(|| CacheLease { allocation: allocation.clone(), range: start..end })
    }

    async fn fetch(&self, key: &ObjectKey, range: Range<u64>, reserve: impl Fn(usize) -> Result<Arc<MemoryReservation>>) -> Result<CacheLease> {
        let length = range.end.checked_sub(range.start).and_then(|length| usize::try_from(length).ok())
            .ok_or_else(|| Error::Task("Object range is inverted or exceeds addressable memory".into()))?;
        let allocation = self.store.scan_range_allocation(key, length)?;
        let reservation = reserve_read(length, allocation, reserve)?;
        let allocation = Arc::new(self.store.get_reserved_range(key, range, reservation).await?);
        Ok(CacheLease { allocation, range: 0..length })
    }

    /// `range` of an object: a slice of the object where the pool holds all of it, and otherwise those bytes alone,
    /// fetched and charged to the pool's budget for as long as the lease lives, but not kept.
    pub async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<CacheLease> {
        if let Some(lease) = self.held_range(key, &range) { return Ok(lease); }
        self.fetch(key, range, |length| self.reserve(length)).await
    }

    /// As `get_range`, charging the bytes to `fallback` where the pool's budget cannot hold them.
    pub async fn get_range_or(&self, key: &ObjectKey, range: Range<u64>, fallback: &Arc<MemoryBudget>) -> Result<CacheLease> {
        if let Some(lease) = self.held_range(key, &range) { return Ok(lease); }
        self.fetch(key, range, |length| self.reserve(length).or_else(|_| reserve_from(fallback, length))).await
    }

    pub async fn scan_length(&self, key: &ObjectKey) -> Result<u64> {
        if let Some(entry) = self.held(key) {
            if let Some(allocation) = entry.allocation.lock().await.as_ref() { return Ok(allocation.len() as u64); }
        }
        Ok(self.store.head(key).await?.size)
    }

    pub async fn scan_range(&self, key: &ObjectKey, range: Range<u64>, fallback: &Arc<MemoryBudget>) -> Result<CacheLease> {
        self.fetch_scan_range(key, range, |length| self.reserve(length).or_else(|_| reserve_from(fallback, length))).await
    }

    /// Fetches a range without populating the disk cache with the whole object and charges the shared pool.
    pub async fn get_scan_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<CacheLease> {
        self.fetch_scan_range(key, range, |length| self.reserve(length)).await
    }

    async fn fetch_scan_range(&self, key: &ObjectKey, range: Range<u64>, reserve: impl Fn(usize) -> Result<Arc<MemoryReservation>>) -> Result<CacheLease> {
        if let Some(lease) = self.held_range(key, &range) { return Ok(lease); }
        let length = range.end.checked_sub(range.start).and_then(|length| usize::try_from(length).ok())
            .ok_or_else(|| Error::Task("Scan range is inverted or exceeds addressable memory".into()))?;
        let allocation = self.store.scan_range_allocation(key, length)?;
        let reservation = reserve_read(length, allocation, reserve)?;
        let started = std::time::Instant::now();
        let allocation = Arc::new(self.store.get_reserved_scan_range(key, range, reservation).await?);
        self.scan_timing.observe(length, started.elapsed());
        Ok(CacheLease { allocation, range: 0..length })
    }

    /// Reads an immutable object of the recorded length without filling the pool on a miss.
    pub async fn get_exact_or(&self, key: &ObjectKey, length: usize, fallback: &Arc<MemoryBudget>) -> Result<CacheLease> {
        let end = u64::try_from(length).map_err(|_| Error::Task("Object size exceeds the storage range".into()))?;
        if let Some(lease) = self.held_range(key, &(0..end))
            && !lease.is_partial_allocation()
        {
            return Ok(lease);
        }
        self.get_fresh_exact_or(key, length, fallback).await
    }

    /// Reopens the persisted immutable object without using a resident pool allocation.
    pub async fn get_fresh_exact_or(&self, key: &ObjectKey, length: usize, fallback: &Arc<MemoryBudget>) -> Result<CacheLease> {
        let reservation = self.reserve_exact_read(key, length, fallback)?;
        let bytes = reservation.retaining_io(self.store.get_fresh_exact(key, length)).await?;
        let allocation = Arc::new(reservation.finish(bytes)?);
        Ok(CacheLease { allocation, range: 0..length })
    }

    /// Reopens the recorded publication, checking the provider version on the response that supplies its bytes.
    pub async fn get_published_exact_or(
        &self, key: &ObjectKey, length: usize, publication: Option<&PublishedObjectVersion>, fallback: &Arc<MemoryBudget>,
    ) -> Result<Option<CacheLease>> {
        let reservation = self.reserve_exact_read(key, length, fallback)?;
        let observed = reservation.retaining_io(self.store.observe_fresh_exact(key, length)).await?;
        let observed_publication = observed.version.map(PublishedObjectVersion::from);
        if observed_publication.as_ref() != publication { return Ok(None); }
        let allocation = Arc::new(reservation.finish(observed.value)?);
        Ok(Some(CacheLease { allocation, range: 0..length }))
    }

    fn reserve_exact_read(&self, key: &ObjectKey, length: usize, fallback: &Arc<MemoryBudget>) -> Result<ReadReservation> {
        let allocation = self.store.exact_object_allocation(key, length)?;
        reserve_read(length, allocation, |bytes| self.reserve(bytes).or_else(|_| reserve_from(fallback, bytes)))
    }

    /// Tries to cache an exact immutable object using only the shared pool's budget.
    pub async fn get_exact_cached_or(&self, key: &ObjectKey, length: usize, fallback: &Arc<MemoryBudget>) -> Result<CacheLease> {
        if !self.store.permits_shared_read_cache() || length > self.budget.limit_bytes() {
            return self.get_exact_or(key, length, fallback).await;
        }
        let mut cleanup = EmptyEntryCleanup { pool: self, key, armed: true };
        if let Ok(entry) = self.entry(key) {
            let mut slot = entry.allocation.lock().await;
            match slot.as_ref() {
                Some(allocation) if allocation.len() == length => {
                    cleanup.armed = false;
                    return Ok(CacheLease { allocation: allocation.clone(), range: 0..length });
                }
                None => {
                    let allocation = self.store.exact_object_allocation(key, length)?;
                    if let Ok(reservation) = reserve_read(length, allocation, |bytes| self.reserve(bytes)) {
                        let bytes = reservation.retaining_io(self.store.get_fresh_exact(key, length)).await?;
                        let allocation = Arc::new(reservation.finish(bytes)?);
                        *slot = Some(allocation.clone());
                        cleanup.armed = false;
                        return Ok(CacheLease { allocation, range: 0..length });
                    }
                }
                Some(_) => (),
            }
        }
        self.get_exact_or(key, length, fallback).await
    }

    async fn fetch_whole(&self, key: &ObjectKey, length: usize, reserve: impl Fn(usize) -> Result<Arc<MemoryReservation>>) -> Result<Arc<RetainedBytes>> {
        let allocation = self.store.whole_object_allocation(key, length)?;
        let reservation = reserve_read(length, allocation, reserve)?;
        Ok(Arc::new(self.store.get_reserved_object(key, reservation).await?))
    }

    /// As `get`, except that an object the pool cannot admit is read whole under `fallback`, and not kept.
    pub async fn get_or(&self, key: &ObjectKey, fallback: &Arc<MemoryBudget>) -> Result<CacheLease> {
        match self.get(key).await {
            Err(Error::MemoryLimit { .. }) => {
                let size = self.store.head(key).await?.size;
                let size = usize::try_from(size).map_err(|_| self.budget.limit_error(usize::MAX))?;
                let allocation = self.fetch_whole(key, size, |bytes| reserve_from(fallback, bytes)).await?;
                Ok(CacheLease { allocation, range: 0..size })
            }
            fetched => fetched,
        }
    }

    /// Whether the pool holds all of an object, so that a whole read of it costs nothing more.
    pub fn holds(&self, key: &ObjectKey) -> bool {
        let held = {
            let lru = self.budget.lru.lock();
            match lru.find(self.id, key, Slot::Object).and_then(|node| lru.value(node)) { Some(Value::Object(entry)) => Some(Arc::clone(entry)), _ => None }
        };
        held.is_some_and(|entry| entry.allocation.try_lock().is_ok_and(|slot| slot.is_some()))
    }

    /// Concurrent fetches of a key share one load; cancellation releases that caller's wait.
    /// An empty entry is removed when its last fetcher fails or is cancelled.
    pub async fn get(&self, key: &ObjectKey) -> Result<CacheLease> {
        if !self.store.permits_shared_read_cache() {
            let size = self.store.head(key).await?.size;
            let size = usize::try_from(size).map_err(|_| self.budget.limit_error(usize::MAX))?;
            let allocation = self.fetch_whole(key, size, |bytes| self.reserve(bytes)).await?;
            return Ok(CacheLease { allocation, range: 0..size });
        }
        let mut cleanup = EmptyEntryCleanup { pool: self, key, armed: true };
        let allocation = {
            let entry = self.entry(key)?;
            let mut slot = entry.allocation.lock().await;
            match slot.as_ref() {
                Some(allocation) => allocation.clone(),
                None => {
                    let size = self.store.head(key).await?.size;
                    let size = usize::try_from(size).map_err(|_| self.budget.limit_error(usize::MAX))?;
                    let allocation = self.fetch_whole(key, size, |bytes| self.reserve(bytes)).await?;
                    *slot = Some(allocation.clone());
                    allocation
                }
            }
        };
        cleanup.armed = false;
        let length = allocation.len();
        Ok(CacheLease { allocation, range: 0..length })
    }

    /// Reserves `bytes` from the pool's budget for a view about to be decoded, evicting idle entries to make room.
    /// None where the view would take more than an eighth of the budget, or does not fit once nothing idle is left.
    pub fn admit(&self, bytes: usize) -> Option<Arc<MemoryReservation>> {
        if !self.store.permits_shared_read_cache() || bytes > self.budget.limit_bytes() / 8 { return None; }
        self.reserve(bytes).ok()
    }

    /// Holds `value` as `view` of the object in place of any it held; false, holding nothing, where its bookkeeping does not fit.
    pub fn insert_view<T: Cached>(&self, key: &ObjectKey, view: View, value: Arc<T>) -> bool {
        if !self.store.permits_shared_read_cache() { return false; }
        let Ok(overhead) = self.reserve(view_overhead(key)) else { return false };
        let value = Value::View { value, in_use: in_use_of::<T>, _overhead: overhead };
        let replaced = self.budget.lru.lock().insert(self.id, key, Slot::View(view), value);
        drop(replaced);
        true
    }

    pub fn view<T: Cached>(&self, key: &ObjectKey, view: View) -> Option<Arc<T>> {
        let value = {
            let mut lru = self.budget.lru.lock();
            let node = lru.find(self.id, key, Slot::View(view))?;
            lru.touch(node);
            match lru.value(node) { Some(Value::View { value, .. }) => Arc::clone(value), _ => return None }
        };
        value.downcast::<T>().ok()
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let removed = self.budget.lru.lock().remove_pool(self.id);
        drop(removed);
    }
}

#[cfg(test)]
mod fairness_tests {
    use super::*;
    use crate::MemoryStore;
    use bytes::Bytes;

    #[tokio::test]
    async fn a_held_catalog_cannot_cross_a_pool_or_cover_an_older_cut() {
        use rtdb_types::{CatalogCut, ReplicaPosition, StorageEpoch, StorageVersion};

        let store = Arc::new(MemoryStore::default());
        let budget = CacheBudget::new(100);
        let first = BufferPool::with_budget(store.clone(), budget.clone());
        let second = BufferPool::with_budget(store, budget);
        let cut = CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(1),
            replica_epoch: StorageEpoch(3), through: ReplicaPosition(10) };
        let held = first.hold_catalog(Some(cut)).await.unwrap();
        assert!(first.holds_catalog(&held, Some(cut)));
        assert!(!second.holds_catalog(&held, Some(cut)));
        assert!(!first.holds_catalog(&held, Some(CatalogCut { through: ReplicaPosition(9), ..cut })));
        assert!(!first.holds_catalog(&held, Some(CatalogCut { storage_epoch: StorageEpoch(2), ..cut })));
    }

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

    #[tokio::test]
    async fn a_request_larger_than_the_budget_evicts_nothing() {
        let budget = CacheBudget::new(100);
        let (small, large) = (Arc::new(MemoryStore::default()), Arc::new(MemoryStore::default()));
        small.put(&ObjectKey::new("a").unwrap(), Bytes::from(vec![1u8; 50])).await.unwrap();
        large.put(&ObjectKey::new("b").unwrap(), Bytes::from(vec![2u8; 200])).await.unwrap();
        let first = BufferPool::with_budget(small, Arc::clone(&budget));
        let second = BufferPool::with_budget(large, Arc::clone(&budget));
        drop(first.get(&ObjectKey::new("a").unwrap()).await.unwrap());
        assert!(matches!(second.get(&ObjectKey::new("b").unwrap()).await, Err(Error::MemoryLimit { .. })));
        assert_eq!(budget.retained_bytes(), 50, "the other pool's entry survives");
    }

    #[tokio::test]
    async fn a_prepaid_cache_admits_and_retains_objects_during_ordinary_pressure() {
        let parent = MemoryBudget::new(200);
        let prepaid = CacheBudget::with_memory(parent.prepay(100).unwrap());
        assert!(prepaid.prepaid_by(&parent));
        let store = Arc::new(MemoryStore::default());
        let a = ObjectKey::new("a").unwrap();
        let b = ObjectKey::new("b").unwrap();
        store.put(&a, Bytes::from(vec![1; 60])).await.unwrap();
        store.put(&b, Bytes::from(vec![2; 60])).await.unwrap();
        let ordinary = BufferPool::with_budget(store.clone(), CacheBudget::with_memory(Arc::clone(&parent)));
        let audience = BufferPool::with_budget(store, prepaid);
        let held = ordinary.get(&a).await.unwrap();
        let _pressure = parent.reserve(40).unwrap();
        assert_eq!(parent.used_bytes(), parent.limit_bytes());
        assert!(matches!(ordinary.get(&b).await, Err(Error::MemoryLimit { .. })));
        assert!(!audience.holds(&a));
        drop(audience.get(&a).await.unwrap());
        drop(held);
        drop(ordinary.get(&b).await.unwrap());
        assert!(!ordinary.holds(&a));
        assert!(audience.holds(&a));
    }
}

#[cfg(test)]
mod view_tests {
    use super::*;
    use crate::MemoryStore;
    use bytes::Bytes;

    struct Decoded { _reservation: Arc<MemoryReservation>, readers: Arc<()> }
    impl Cached for Decoded { fn in_use(&self) -> bool { Arc::strong_count(&self.readers) > 1 } }

    fn key(name: &str) -> ObjectKey { ObjectKey::new(name).unwrap() }

    async fn pool_of(budget: &Arc<CacheBudget>, objects: &[(&str, usize)]) -> BufferPool {
        let store = Arc::new(MemoryStore::default());
        for (name, size) in objects { store.put(&key(name), Bytes::from(vec![7u8; *size])).await.unwrap(); }
        BufferPool::with_budget(store, Arc::clone(budget))
    }

    fn cache_view(pool: &BufferPool, name: &str, view: View, bytes: usize) -> bool {
        let Some(reservation) = pool.admit(bytes) else { return false };
        pool.insert_view(&key(name), view, Arc::new(Decoded { _reservation: reservation, readers: Arc::new(()) }))
    }

    #[tokio::test]
    async fn an_exact_read_does_not_cache_decrypted_bytes_against_provider_policy() {
        let sealed = crate::sealing::EncryptingStore::new(Arc::new(MemoryStore::default()), &[9; 32]).unwrap();
        let name = key("test");
        sealed.put(&name, Bytes::from_static(b"sealed")).await.unwrap();
        let pool = BufferPool::new(Arc::new(sealed), 1 << 20);
        let memory = MemoryBudget::new(1 << 20);
        let read = pool.get_exact_cached_or(&name, 6, &memory).await.unwrap();
        assert_eq!(read.as_slice(), b"sealed");
        assert!(!pool.holds(&name));
        drop(read);
        assert_eq!(pool.pool_retained_bytes(), 0);
    }

    #[tokio::test]
    async fn views_and_objects_are_evicted_least_recently_used_first_from_one_budget() {
        let budget = CacheBudget::new(40_000);
        let pool = pool_of(&budget, &[("a", 13_000), ("b", 13_000), ("c", 13_000), ("d", 13_000)]).await;
        for name in ["a", "b", "c"] { drop(pool.get(&key(name)).await.unwrap()); }
        assert!(cache_view(&pool, "a", View::Page(0), 4_000), "admitting a view evicts the least recently used object");
        assert!(!pool.holds(&key("a")));
        assert!(pool.holds(&key("b")) && pool.holds(&key("c")));
        for name in ["b", "c"] { drop(pool.get(&key(name)).await.unwrap()); }
        drop(pool.get(&key("d")).await.unwrap());
        assert!(pool.view::<Decoded>(&key("a"), View::Page(0)).is_none(), "fetching an object evicts the least recently used view");
        assert!(pool.holds(&key("b")) && pool.holds(&key("c")) && pool.holds(&key("d")));
        assert!(budget.retained_bytes() <= budget.limit_bytes());
    }

    #[tokio::test]
    async fn a_view_in_use_is_kept_and_a_view_that_cannot_fit_is_not_cached() {
        let budget = CacheBudget::new(40_000);
        let pool = pool_of(&budget, &[("a", 32_000)]).await;
        assert!(cache_view(&pool, "f", View::File, 4_000));
        let held = pool.view::<Decoded>(&key("f"), View::File).unwrap();
        let reader = Arc::clone(&held.readers);
        drop(held);
        let lease = pool.get(&key("a")).await.unwrap();
        assert!(pool.view::<Decoded>(&key("f"), View::File).is_some(), "a view whose contents a reader holds is not evicted");
        assert!(!cache_view(&pool, "f", View::Page(1), 5_000), "nothing idle is left to evict");
        assert!(!cache_view(&pool, "f", View::Page(2), 5_001), "a view larger than an eighth of the budget is never cached");
        assert!(budget.retained_bytes() <= budget.limit_bytes());
        drop((reader, lease));
        assert!(cache_view(&pool, "f", View::Page(1), 5_000), "once released, the idle entries make room");
        assert!(pool.view::<Decoded>(&key("f"), View::Page(1)).is_some() && pool.view::<Decoded>(&key("f"), View::File).is_some());
        assert!(!pool.holds(&key("a")), "the object went first, as the least recently used");
    }

    #[tokio::test]
    async fn the_budget_is_never_exceeded_across_objects_ranges_and_views() {
        let budget = CacheBudget::new(50_000);
        let objects: Vec<(String, usize)> = (0..12).map(|index| (format!("o{index}"), 3_000 + 1_000 * index)).collect();
        let named: Vec<(&str, usize)> = objects.iter().map(|(name, size)| (name.as_str(), *size)).collect();
        let pool = pool_of(&budget, &named).await;
        let mut held = Vec::new();
        for round in 0..200usize {
            let (name, size) = &objects[round * 7 % objects.len()];
            match round % 4 {
                0 => { let lease = pool.get(&key(name)).await.unwrap(); if round % 3 == 0 { held.push(lease); } }
                1 => drop(pool.get_range(&key(name), 0..(*size as u64 / 2)).await.unwrap()),
                2 => { cache_view(&pool, name, View::Page(round % 5), 500 + round * 13 % 5_000); }
                _ => if !held.is_empty() { held.remove(0); },
            }
            assert!(budget.retained_bytes() <= budget.limit_bytes());
        }
        drop(held);
        drop(pool);
        assert_eq!(budget.retained_bytes(), 0, "dropping the pool releases every entry it held");
    }

    #[tokio::test]
    async fn one_pools_fetch_evicts_another_pools_idle_views() {
        let budget = CacheBudget::new(40_000);
        let first = pool_of(&budget, &[]).await;
        let second = pool_of(&budget, &[("big", 36_000)]).await;
        for page in 0..4 { assert!(cache_view(&first, "f", View::Page(page), 4_000)); }
        assert!(first.pool_retained_bytes() > 16_000);
        assert_eq!(second.pool_retained_bytes(), 0);
        let lease = second.get(&key("big")).await.unwrap();
        let kept = (0..4).filter(|page| first.view::<Decoded>(&key("f"), View::Page(*page)).is_some()).count();
        assert!(kept < 4, "the other pool's fetch evicted some views");
        assert!(first.view::<Decoded>(&key("f"), View::Page(0)).is_none(), "the oldest first");
        assert!(second.view::<Decoded>(&key("f"), View::Page(3)).is_none(), "views belong to the pool that cached them");
        assert_eq!(second.pool_retained_bytes(), 36_000);
        assert_eq!(budget.retained_bytes(), first.pool_retained_bytes() + second.pool_retained_bytes());
        assert!(budget.retained_bytes() <= budget.limit_bytes());
        second.invalidate(&key("big")).await.unwrap();
        assert_eq!(second.pool_retained_bytes(), 36_000);
        drop(lease);
        assert_eq!(second.pool_retained_bytes(), 0);
    }
}

#[cfg(test)]
#[tokio::test]
async fn spatial_arrow_posting_digest_follows_the_object_allocation() {
    let store = Arc::new(crate::MemoryStore::default());
    let key = ObjectKey::new("postings").unwrap();
    store.put(&key, bytes::Bytes::from_static(&[1, 2])).await.unwrap();
    let pool = BufferPool::with_budget(store, CacheBudget::new(1 << 20));
    let fallback = MemoryBudget::new(1 << 20);
    let initial = pool.get_exact_cached_or(&key, 2, &fallback).await.unwrap();
    assert_eq!(initial.sha256(|_| Err::<[u8; 32], _>("cancelled")), Err("cancelled"));
    assert_eq!(initial.sha256(|_| Ok::<_, ()>([23; 32])), Ok([23; 32]));
    let same = pool.get_exact_cached_or(&key, 2, &fallback).await.unwrap();
    assert_eq!(same.sha256(|_| -> std::result::Result<_, ()> { panic!("cached object rehashed") }), Ok([23; 32]));
    let partial = same.slice(0..1).unwrap();
    assert_eq!(partial.sha256(|bytes| { assert_eq!(bytes, &[1]); Ok::<_, ()>([42; 32]) }), Ok([42; 32]));
}
