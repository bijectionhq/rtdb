use std::sync::{Arc, Mutex, Weak, atomic::{AtomicUsize, Ordering}};

use crate::{Anomaly, AnomalyKind, Result};

/// Finite byte admission shared across retained allocation leases.
/// Child scopes track their own retained/peak bytes while charging every ancestor.
#[derive(Debug)]
pub struct MemoryBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    parent: Option<Arc<MemoryBudget>>,
    funding: Option<Arc<MemoryReservation>>,
    /// Bytes this budget and its ancestors have admitted ahead of use, which small reservations draw on per thread.
    credit: Option<Credit>,
    /// On a budget without a parent, the credited budgets beneath it, whose unspent credit a refused admission reclaims.
    credited: Mutex<Vec<Weak<MemoryBudget>>>,
}

/// The largest reservation a credited budget admits from a thread's credit, and the batch in which a thread refills it.
const CREDIT_BATCH: usize = 8 << 10;
/// The most unspent credit one shard keeps after a release; the excess goes back to the budget and its ancestors.
const CREDIT_KEPT: usize = 2 * CREDIT_BATCH;

#[repr(align(128))]
#[derive(Debug, Default)]
struct CreditShard(AtomicUsize);

/// Per-thread shards of admitted but unreserved bytes, each on its own cache line.
#[derive(Debug)]
struct Credit { shards: Box<[CreditShard]> }

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);
thread_local! { static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed); }

impl Credit {
    fn new() -> Self {
        let shards = std::thread::available_parallelism().map_or(8, |threads| threads.get()).clamp(1, 64).next_power_of_two();
        Self { shards: (0..shards).map(|_| CreditShard::default()).collect() }
    }

    fn shard(&self) -> &AtomicUsize {
        let index = SHARD.try_with(|shard| *shard).unwrap_or(0) & (self.shards.len() - 1);
        &self.shards[index].0
    }

    fn take(shard: &AtomicUsize, bytes: usize) -> bool {
        shard.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |credit| credit.checked_sub(bytes)).is_ok()
    }

    fn outstanding(&self) -> usize {
        self.shards.iter().fold(0usize, |total, shard| total.saturating_add(shard.0.load(Ordering::Acquire)))
    }

    fn drain(&self) -> usize {
        self.shards.iter().fold(0usize, |total, shard| total.saturating_add(shard.0.swap(0, Ordering::AcqRel)))
    }
}

impl MemoryBudget {
    fn with_parts(limit_bytes: usize, parent: Option<Arc<Self>>, funding: Option<Arc<MemoryReservation>>, credit: Option<Credit>) -> Arc<Self> {
        Arc::new(Self { limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent, funding, credit, credited: Mutex::new(Vec::new()) })
    }

    /// Creates a root budget with a byte limit; zero permits only zero-byte reservations.
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Self::with_parts(limit_bytes, None, None, None)
    }

    /// Tracks a region's retained and peak bytes while charging every reservation to the shared parent limit.
    /// Retained reservations keep both budgets alive after the region closes.
    pub fn scope(self: &Arc<Self>) -> Arc<Self> {
        self.scope_with_limit(self.limit_bytes)
    }

    /// Opens a child with its own byte ceiling and the parent's existing admission limits.
    /// The effective ceiling cannot exceed this budget's byte limit.
    pub fn scope_with_limit(self: &Arc<Self>, limit_bytes: usize) -> Arc<Self> {
        Self::with_parts(limit_bytes.min(self.limit_bytes), Some(Arc::clone(self)), None, None)
    }

    /// A child for reservations made from many threads at once.
    /// Each thread admits a reservation of at most `CREDIT_BATCH` bytes from credit its shard holds, which this child and its
    /// ancestors admitted `CREDIT_BATCH` bytes at a time, so such a reservation writes no counter other threads write.
    /// Unspent credit counts as used on this child's ancestors and in its `peak_bytes`, but not in its `used_bytes`.
    /// A shard keeps at most `CREDIT_KEPT` bytes once its thread releases, so idle credit is bounded by the shard count, not by
    /// the number of sessions.
    /// A reservation refused anywhere under the same root reclaims all unspent credit there and is tried once more, so credit
    /// refuses nothing that would fit, except the bytes other threads take as credit in between.
    pub fn credited(self: &Arc<Self>) -> Arc<Self> {
        let child = Self::with_parts(self.limit_bytes, Some(Arc::clone(self)), None, Some(Credit::new()));
        if let Ok(mut credited) = self.root().credited.lock() {
            credited.retain(|budget| budget.strong_count() > 0);
            credited.push(Arc::downgrade(&child));
        }
        child
    }

    /// Pays for a separate budget in full before it can admit work.
    /// Retained reservations keep the funding alive, even after the owner closes.
    pub fn prepay(self: &Arc<Self>, bytes: usize) -> Result<Arc<Self>> {
        let funding = self.reserve(bytes)?;
        Ok(Self::with_parts(bytes, None, Some(funding), None))
    }

    pub fn is_prepaid(&self) -> bool { self.funding.is_some() }

    /// Derived snapshot data can share ordinary reservations within the same root budget.
    pub fn snapshot_budget(self: &Arc<Self>) -> Option<&Arc<Self>> {
        let mut root = self;
        while let Some(parent) = &root.parent {
            if root.limit_bytes != parent.limit_bytes { return None; }
            root = parent;
        }
        (!root.is_prepaid()).then_some(root)
    }

    pub fn prepaid_by(&self, budget: &Arc<Self>) -> bool {
        self.funding.as_ref().is_some_and(|funding| Arc::ptr_eq(funding.budget(), budget))
    }

    /// Reserves bytes without waiting; release occurs when the last reservation handle drops.
    /// Exceeding this budget or an ancestor limit, including size overflow, returns an error of kind `ResourceExhausted`.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.admit(bytes)?;
        Ok(Arc::new(MemoryReservation { budget: Arc::clone(self), bytes, backing: Box::default() }))
    }

    /// Admits new storage that also depends on already funded allocations.
    pub fn reserve_with_backing(self: &Arc<Self>, bytes: usize,
        backing: impl ExactSizeIterator<Item = Arc<MemoryReservation>>,
    ) -> Result<Arc<MemoryReservation>> {
        let bytes = backing.len().checked_mul(std::mem::size_of::<Arc<MemoryReservation>>())
            .and_then(|slots| bytes.checked_add(slots))
            .ok_or_else(|| Anomaly::incorrect("rtdb/reservation-size", "Combined reservation exceeds addressable memory"))?;
        self.admit(bytes)?;
        Ok(Arc::new(MemoryReservation { budget: Arc::clone(self), bytes, backing: backing.collect() }))
    }

    fn root(&self) -> &Self {
        let mut root = self;
        while let Some(parent) = &root.parent { root = parent; }
        root
    }

    /// Admits bytes through the chain; if a limit refuses them, reclaims the root's unspent credit and tries once more.
    fn admit(&self, bytes: usize) -> Result<()> {
        match self.acquire_chain(bytes) {
            Ok(()) => Ok(()),
            Err(error) if self.root().reclaim_credit() => self.acquire_chain(bytes).map_err(|_| error),
            Err(error) => Err(error),
        }
    }

    /// Returns every credited descendant's unspent credit to its ancestors; true if there was any.
    fn reclaim_credit(&self) -> bool {
        let credited: Vec<Arc<Self>> = match self.credited.lock() {
            Ok(credited) => credited.iter().filter_map(Weak::upgrade).collect(),
            Err(_) => return false,
        };
        let mut reclaimed = false;
        for budget in &credited {
            let Some(credit) = &budget.credit else { continue };
            let drained = credit.drain();
            if drained > 0 {
                budget.release_direct(drained);
                reclaimed = true;
            }
        }
        reclaimed
    }

    /// Admits bytes at the root first and at this budget last, releasing the ancestors' admission if this one refuses.
    fn acquire_chain(&self, bytes: usize) -> Result<()> {
        if let Some(credit) = &self.credit && bytes <= CREDIT_BATCH {
            let shard = credit.shard();
            if Credit::take(shard, bytes) { return Ok(()); }
            if self.acquire_direct(bytes + CREDIT_BATCH).is_ok() {
                shard.fetch_add(CREDIT_BATCH, Ordering::AcqRel);
                return Ok(());
            }
        }
        self.acquire_direct(bytes)
    }

    fn acquire_direct(&self, bytes: usize) -> Result<()> {
        if let Some(parent) = &self.parent { parent.acquire_chain(bytes)?; }
        if let Err(error) = self.acquire(bytes) {
            if let Some(parent) = &self.parent { parent.release_chain(bytes); }
            return Err(error);
        }
        Ok(())
    }

    fn release_chain(&self, bytes: usize) {
        if let Some(credit) = &self.credit && bytes <= CREDIT_BATCH {
            let shard = credit.shard();
            if shard.fetch_add(bytes, Ordering::AcqRel) + bytes > CREDIT_KEPT
                && let Ok(kept) = shard.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |credit| (credit > CREDIT_BATCH).then_some(CREDIT_BATCH))
            {
                self.release_direct(kept - CREDIT_BATCH);
            }
            return;
        }
        self.release_direct(bytes);
    }

    fn release_direct(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
        if let Some(parent) = &self.parent { parent.release_chain(bytes); }
    }

    /// Counts the reservation controls created by a successful `reserve`, including this budget and every ancestor.
    /// Includes zero-byte reservations; failed reservations may construct fewer controls.
    /// Walks the immutable chain without allocating, reserving or changing counters; the count is stable for this budget's lifetime.
    /// Returns an error if the count exceeds usize.
    pub fn reservation_lease_count(&self) -> Result<usize> {
        let mut count = 1usize;
        let mut budget = self;
        while let Some(parent) = &budget.parent {
            count = count.checked_add(1).ok_or_else(|| Anomaly::incorrect("rtdb/allocation-overflow", "Reservation ancestor count exceeds range"))?;
            budget = parent;
        }
        Ok(count)
    }

    fn acquire(&self, bytes: usize) -> Result<()> {
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes).filter(|next| *next <= self.limit_bytes)
                .ok_or_else(|| Anomaly::incorrect("rtdb/memory-budget-exceeded", format!("Cannot reserve {bytes} bytes from budget {} with {current} retained", self.limit_bytes))
                    .with_kind(AnomalyKind::ResourceExhausted))?;
            match self.used_bytes.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    if next > self.peak_bytes.load(Ordering::Relaxed) { self.peak_bytes.fetch_max(next, Ordering::Relaxed); }
                    return Ok(());
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Bytes retained by reservations on this budget and its descendants, excluding this budget's own unspent credit.
    pub fn used_bytes(&self) -> usize {
        let used = self.used_bytes.load(Ordering::Acquire);
        self.credit.as_ref().map_or(used, |credit| used.saturating_sub(credit.outstanding()))
    }
    pub fn peak_bytes(&self) -> usize { self.peak_bytes.load(Ordering::Relaxed) }
    pub fn limit_bytes(&self) -> usize { self.limit_bytes }

    /// Estimates the bytes available under this limit and all ancestor limits.
    /// A subsequent reservation still checks the shared limits.
    /// Unspent credit counts as available, since a refused reservation reclaims it: on a chain through a credited budget,
    /// that budget's credit, and otherwise, at the root, the credit of every credited budget beneath it.
    pub fn available_bytes(&self) -> usize {
        let mut available = usize::MAX;
        let mut credit: Option<usize> = None;
        let mut budget = Some(self);
        while let Some(current) = budget {
            let reclaimable = match credit {
                Some(credit) => credit,
                None if current.parent.is_none() => current.registered_credit(),
                None => 0,
            };
            available = available.min(current.limit_bytes.saturating_sub(current.used_bytes().saturating_sub(reclaimable)));
            if let Some(own) = &current.credit { credit = Some(credit.unwrap_or(0).saturating_add(own.outstanding())); }
            budget = current.parent.as_deref();
        }
        available
    }

    fn registered_credit(&self) -> usize {
        let Ok(credited) = self.credited.lock() else { return 0 };
        credited.iter().filter_map(Weak::upgrade)
            .fold(0usize, |total, budget| total.saturating_add(budget.credit.as_ref().map_or(0, Credit::outstanding)))
    }
}

impl Drop for MemoryBudget {
    fn drop(&mut self) {
        if let (Some(credit), Some(parent)) = (&self.credit, &self.parent) {
            let unspent = credit.drain();
            if unspent > 0 { parent.release_chain(unspent); }
        }
    }
}

/// Retained byte charge on its budget and every ancestor, keeping the budget chain alive.
#[derive(Debug)]
pub struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: usize,
    backing: Box<[Arc<MemoryReservation>]>,
}

impl MemoryReservation {
    /// The local charge excludes the already funded allocations in `backing`.
    pub fn bytes(&self) -> usize { self.bytes }
    pub fn budget(&self) -> &Arc<MemoryBudget> { &self.budget }
    pub fn backing(&self) -> &[Arc<Self>] { &self.backing }

    /// - Admits additional bytes through this lease and every ancestor before the caller allocates them.
    /// - Requires a total at least as large as the held bytes; allocations are not resized.
    /// - An admission failure preserves retained bytes and lease sizes; ancestor peaks may record temporary admission.
    pub fn grow_to(&mut self, bytes: usize) -> Result<()> {
        let additional = bytes.checked_sub(self.bytes)
            .ok_or_else(|| Anomaly::incorrect("rtdb/reservation-shrink", "Reservation growth requires at least the held bytes"))?;
        self.budget.admit(additional)?;
        self.bytes = bytes;
        Ok(())
    }

    /// Releases excess capacity from this lease and its parent leases without lowering recorded peaks.
    /// The requested bytes must cover every allocation retained under this lease; this method does not resize allocations.
    /// Requesting more than the held bytes returns an error without changing any counters.
    pub fn shrink_to(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.bytes {
            return Err(Anomaly::incorrect("rtdb/reservation-growth", "A memory reservation can only shrink"));
        }
        self.release_to(bytes);
        Ok(())
    }

    /// Transfers part of this grant to an independently released reservation for storage with independent backing.
    pub fn split_off(&mut self, bytes: usize) -> Result<Arc<Self>> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or_else(|| Anomaly::incorrect("rtdb/undersized-reservation", "Allocation exceeds its reservation"))?;
        Ok(Arc::new(Self { budget: Arc::clone(&self.budget), bytes, backing: Box::default() }))
    }

    /// Moves an independent charge between reservations on the same budget without acquiring it again.
    pub fn absorb(&mut self, other: &mut Self) -> Result<()> {
        if !Arc::ptr_eq(&self.budget, &other.budget) {
            return Err(Anomaly::incorrect("rtdb/reservation-budget", "Reservations belong to different budgets"));
        }
        if !other.backing.is_empty() {
            return Err(Anomaly::incorrect("rtdb/reservation-backing", "Reservation depends on other allocations"));
        }
        let bytes = self.bytes.checked_add(other.bytes)
            .ok_or_else(|| Anomaly::incorrect("rtdb/reservation-size", "Combined reservation exceeds addressable memory"))?;
        self.bytes = bytes;
        other.bytes = 0;
        Ok(())
    }

    fn release_to(&mut self, bytes: usize) {
        let released = self.bytes - bytes;
        self.bytes = bytes;
        self.budget.release_chain(released);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) { self.budget.release_chain(self.bytes); }
}


#[test]
fn ordered_index_snapshot_budget_shares_only_ordinary_equal_limit_scopes() {
    let root = MemoryBudget::new(8192);
    let request = root.scope().credited().scope();
    assert!(Arc::ptr_eq(&root, request.snapshot_budget().unwrap()));
    assert!(root.scope_with_limit(4096).scope().snapshot_budget().is_none());
    assert!(root.prepay(4096).unwrap().scope().snapshot_budget().is_none());
}
