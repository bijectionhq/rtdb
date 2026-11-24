use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

use crate::{Anomaly, Result};

/// Finite byte admission shared across retained allocation leases.
/// Child scopes track their own retained/peak bytes while charging every ancestor.
#[derive(Debug)]
pub struct MemoryBudget {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    parent: Option<Arc<MemoryBudget>>,
    funding: Option<Arc<MemoryReservation>>,
    /// For a quantized budget, the bytes its parent has admitted for it, which cover its used bytes in whole quanta.
    quantized: Option<Quantized>,
}

#[derive(Debug)]
struct Quantized { quantum: usize, granted: std::sync::Mutex<usize> }

impl MemoryBudget {
    /// Creates a root budget with a byte limit; zero permits only zero-byte reservations.
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self { limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: None, funding: None, quantized: None })
    }

    /// Tracks a region's retained and peak bytes while charging every reservation to the shared parent limit.
    /// Retained reservations keep both budgets alive after the region closes.
    pub fn scope(self: &Arc<Self>) -> Arc<Self> {
        self.scope_with_limit(self.limit_bytes)
    }

    /// Opens a child with its own byte ceiling and the parent's existing admission limits.
    /// The effective ceiling cannot exceed this budget's byte limit.
    pub fn scope_with_limit(self: &Arc<Self>, limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self { limit_bytes: limit_bytes.min(self.limit_bytes), used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: Some(Arc::clone(self)), funding: None, quantized: None })
    }

    /// A child that admits its reservations against this budget a `quantum` of bytes at a time, so that its many small
    /// reservations touch this budget, and the ancestors it shares with other children, only once per quantum.
    /// This budget sees whole quanta: up to two quanta more than the child uses, which it returns as it frees them.
    pub fn quantized(self: &Arc<Self>, quantum: usize) -> Arc<Self> {
        Arc::new(Self { limit_bytes: self.limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: Some(Arc::clone(self)), funding: None,
            quantized: Some(Quantized { quantum: quantum.max(1), granted: std::sync::Mutex::new(0) }) })
    }

    /// Pays for a separate budget in full before it can admit work.
    /// Retained reservations keep the funding alive, even after the owner closes.
    pub fn prepay(self: &Arc<Self>, bytes: usize) -> Result<Arc<Self>> {
        let funding = self.reserve(bytes)?;
        Ok(Arc::new(Self {
            limit_bytes: bytes,
            used_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
            parent: None,
            funding: Some(funding),
            quantized: None,
        }))
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
    /// Exceeding this budget or an ancestor limit, including size overflow, returns an error.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.acquire_chain(bytes)?;
        Ok(Arc::new(MemoryReservation { budget: Arc::clone(self), bytes }))
    }

    /// Admits bytes at the root first and at this budget last, releasing the ancestors' admission if this one refuses.
    fn acquire_chain(&self, bytes: usize) -> Result<()> {
        if let (Some(quantized), Some(parent)) = (&self.quantized, &self.parent) {
            let mut granted = quantized.granted.lock().map_err(|_| Anomaly::fault("rtdb/memory-budget-poisoned", "A quantized budget's grant is poisoned"))?;
            self.acquire(bytes)?;
            let used = self.used_bytes.load(Ordering::Acquire);
            if used > *granted {
                let quanta = (used - *granted).div_ceil(quantized.quantum);
                let next_grant = quanta.checked_mul(quantized.quantum).and_then(|more| (*granted).checked_add(more))
                    .ok_or_else(|| Anomaly::incorrect("rtdb/allocation-overflow", "Quantized grant exceeds range"));
                match next_grant.and_then(|next| parent.acquire_chain(next - *granted).map(|()| next)) {
                    Ok(next) => *granted = next,
                    Err(error) => {
                        self.used_bytes.fetch_sub(bytes, Ordering::AcqRel);
                        return Err(error);
                    }
                }
            }
            return Ok(());
        }
        if let Some(parent) = &self.parent { parent.acquire_chain(bytes)?; }
        if let Err(error) = self.acquire(bytes) {
            if let Some(parent) = &self.parent { parent.release_chain(bytes); }
            return Err(error);
        }
        Ok(())
    }

    fn release_chain(&self, bytes: usize) {
        if let (Some(quantized), Some(parent)) = (&self.quantized, &self.parent) {
            let Ok(mut granted) = quantized.granted.lock() else { self.used_bytes.fetch_sub(bytes, Ordering::AcqRel); return };
            let used = self.used_bytes.fetch_sub(bytes, Ordering::AcqRel) - bytes;
            let keep = used.div_ceil(quantized.quantum).saturating_add(1).saturating_mul(quantized.quantum);
            if *granted > keep.saturating_add(quantized.quantum) {
                parent.release_chain(*granted - keep);
                *granted = keep;
            }
            return;
        }
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
                .ok_or_else(|| Anomaly::incorrect("rtdb/memory-budget-exceeded", format!("Cannot reserve {bytes} bytes from budget {} with {current} retained", self.limit_bytes)))?;
            match self.used_bytes.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    self.peak_bytes.fetch_max(next, Ordering::Relaxed);
                    return Ok(());
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn used_bytes(&self) -> usize { self.used_bytes.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> usize { self.peak_bytes.load(Ordering::Relaxed) }
    pub fn limit_bytes(&self) -> usize { self.limit_bytes }

    /// Estimates the bytes available under this limit and all ancestor limits.
    /// A subsequent reservation still checks the shared limits.
    pub fn available_bytes(&self) -> usize {
        let mut available = self.limit_bytes.saturating_sub(self.used_bytes());
        let mut budget = self;
        while let Some(parent) = &budget.parent {
            available = available.min(parent.limit_bytes.saturating_sub(parent.used_bytes()));
            budget = parent;
        }
        available
    }
}

impl Drop for MemoryBudget {
    fn drop(&mut self) {
        if let (Some(quantized), Some(parent)) = (&mut self.quantized, &self.parent) {
            let granted = quantized.granted.get_mut().map(|granted| std::mem::take(granted)).unwrap_or(0);
            if granted > 0 { parent.release_chain(granted); }
        }
    }
}

/// Retained byte charge on its budget and every ancestor, keeping the budget chain alive.
#[derive(Debug)]
pub struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: usize,
}

impl MemoryReservation {
    pub fn bytes(&self) -> usize { self.bytes }
    pub fn budget(&self) -> &Arc<MemoryBudget> { &self.budget }

    /// - Admits additional bytes through this lease and every ancestor before the caller allocates them.
    /// - Requires a total at least as large as the held bytes; allocations are not resized.
    /// - An admission failure preserves retained bytes and lease sizes; ancestor peaks may record temporary admission.
    pub fn grow_to(&mut self, bytes: usize) -> Result<()> {
        let additional = bytes.checked_sub(self.bytes)
            .ok_or_else(|| Anomaly::incorrect("rtdb/reservation-shrink", "Reservation growth requires at least the held bytes"))?;
        self.budget.acquire_chain(additional)?;
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

    /// Transfers part of this grant to an independently released reservation.
    pub fn split_off(&mut self, bytes: usize) -> Result<Arc<Self>> {
        self.bytes = self.bytes.checked_sub(bytes)
            .ok_or_else(|| Anomaly::incorrect("rtdb/undersized-reservation", "Allocation exceeds its reservation"))?;
        Ok(Arc::new(Self { budget: Arc::clone(&self.budget), bytes }))
    }

    /// Moves an existing charge between reservations on the same budget without acquiring it again.
    pub fn absorb(&mut self, other: &mut Self) -> Result<()> {
        if !Arc::ptr_eq(&self.budget, &other.budget) {
            return Err(Anomaly::incorrect("rtdb/reservation-budget", "Reservations belong to different budgets"));
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
    let request = root.scope().quantized(4096).scope();
    assert!(Arc::ptr_eq(&root, request.snapshot_budget().unwrap()));
    assert!(root.scope_with_limit(4096).scope().snapshot_budget().is_none());
    assert!(root.prepay(4096).unwrap().scope().snapshot_budget().is_none());
}
