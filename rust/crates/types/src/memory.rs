use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

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
}

impl MemoryBudget {
    fn with_parts(limit_bytes: usize, parent: Option<Arc<Self>>, funding: Option<Arc<MemoryReservation>>) -> Arc<Self> {
        Arc::new(Self { limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent, funding })
    }

    /// Creates a root budget with a byte limit; zero permits only zero-byte reservations.
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Self::with_parts(limit_bytes, None, None)
    }

    /// Tracks a region's retained and peak bytes while charging every reservation to the shared parent limit.
    /// Retained reservations keep both budgets alive after the region closes.
    pub fn scope(self: &Arc<Self>) -> Arc<Self> {
        self.scope_with_limit(self.limit_bytes)
    }

    /// Opens a child with its own byte ceiling and the parent's existing admission limits.
    /// The effective ceiling cannot exceed this budget's byte limit.
    pub fn scope_with_limit(self: &Arc<Self>, limit_bytes: usize) -> Arc<Self> {
        Self::with_parts(limit_bytes.min(self.limit_bytes), Some(Arc::clone(self)), None)
    }

    /// Pays for a separate budget in full before it can admit work.
    /// Retained reservations keep the funding alive, even after the owner closes.
    pub fn prepay(self: &Arc<Self>, bytes: usize) -> Result<Arc<Self>> {
        let funding = self.reserve(bytes)?;
        Ok(Self::with_parts(bytes, None, Some(funding)))
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

    /// Admits bytes at the root first and at this budget last, releasing the ancestors' admission if this one refuses.
    fn admit(&self, bytes: usize) -> Result<()> {
        if let Some(parent) = &self.parent { parent.admit(bytes)?; }
        if let Err(error) = self.acquire(bytes) {
            if let Some(parent) = &self.parent { parent.release(bytes); }
            return Err(error);
        }
        Ok(())
    }

    fn release(&self, bytes: usize) {
        self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
        if let Some(parent) = &self.parent { parent.release(bytes); }
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

    /// Bytes retained by reservations on this budget and its descendants.
    pub fn used_bytes(&self) -> usize { self.used_bytes.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> usize { self.peak_bytes.load(Ordering::Relaxed) }
    pub fn limit_bytes(&self) -> usize { self.limit_bytes }

    /// Estimates the bytes available under this limit and all ancestor limits.
    /// A subsequent reservation still checks the shared limits.
    pub fn available_bytes(&self) -> usize {
        let mut available = usize::MAX;
        let mut budget = Some(self);
        while let Some(current) = budget {
            available = available.min(current.limit_bytes.saturating_sub(current.used_bytes()));
            budget = current.parent.as_deref();
        }
        available
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
        self.budget.release(released);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) { self.budget.release(self.bytes); }
}


#[test]
fn ordered_index_snapshot_budget_shares_only_ordinary_equal_limit_scopes() {
    let root = MemoryBudget::new(8192);
    let request = root.scope().scope();
    assert!(Arc::ptr_eq(&root, request.snapshot_budget().unwrap()));
    assert!(root.scope_with_limit(4096).scope().snapshot_budget().is_none());
    assert!(root.prepay(4096).unwrap().scope().snapshot_budget().is_none());
}
