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
}

impl MemoryBudget {
    /// Creates a root budget with a byte limit; zero permits only zero-byte reservations.
    pub fn new(limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self { limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: None })
    }

    /// Tracks a region's retained and peak bytes while charging every reservation to the shared parent limit.
    /// Retained reservations keep both budgets alive after the region closes.
    pub fn scope(self: &Arc<Self>) -> Arc<Self> {
        self.scope_with_limit(self.limit_bytes)
    }

    /// Opens a child with its own byte ceiling and the parent's existing admission limits.
    /// The effective ceiling cannot exceed this budget's byte limit.
    pub fn scope_with_limit(self: &Arc<Self>, limit_bytes: usize) -> Arc<Self> {
        Arc::new(Self { limit_bytes: limit_bytes.min(self.limit_bytes), used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: Some(Arc::clone(self)) })
    }

    /// Reserves bytes without waiting; release occurs when the last reservation handle drops.
    /// Exceeding this budget or an ancestor limit, including size overflow, returns an error.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.acquire_chain(bytes)?;
        Ok(Arc::new(MemoryReservation { budget: Arc::clone(self), bytes }))
    }

    /// Admits bytes at the root first and at this budget last, releasing the ancestors' admission if this one refuses.
    fn acquire_chain(&self, bytes: usize) -> Result<()> {
        if let Some(parent) = &self.parent { parent.acquire_chain(bytes)?; }
        if let Err(error) = self.acquire(bytes) {
            if let Some(parent) = &self.parent { parent.release_chain(bytes); }
            return Err(error);
        }
        Ok(())
    }

    fn release_chain(&self, bytes: usize) {
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

    fn release_to(&mut self, bytes: usize) {
        let released = self.bytes - bytes;
        self.bytes = bytes;
        self.budget.release_chain(released);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) { self.budget.release_chain(self.bytes); }
}
