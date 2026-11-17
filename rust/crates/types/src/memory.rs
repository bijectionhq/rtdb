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
        Arc::new(Self { limit_bytes: self.limit_bytes, used_bytes: AtomicUsize::new(0), peak_bytes: AtomicUsize::new(0), parent: Some(Arc::clone(self)) })
    }

    /// Reserves bytes without waiting; release occurs when the last reservation handle drops.
    /// Exceeding this budget or an ancestor limit, including size overflow, returns an error.
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Arc<MemoryReservation>> {
        let parent = self.parent.as_ref().map(|parent| parent.reserve(bytes)).transpose()?;
        let mut current = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(bytes).filter(|next| *next <= self.limit_bytes)
                .ok_or_else(|| Anomaly::incorrect("rtdb/memory-budget-exceeded", format!("Cannot reserve {bytes} bytes from budget {} with {current} retained", self.limit_bytes)))?;
            match self.used_bytes.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => {
                    self.peak_bytes.fetch_max(next, Ordering::Relaxed);
                    return Ok(Arc::new(MemoryReservation { budget: Arc::clone(self), bytes, parent }));
                }
                Err(actual) => current = actual,
            }
        }
    }

    pub fn used_bytes(&self) -> usize { self.used_bytes.load(Ordering::Acquire) }
    pub fn peak_bytes(&self) -> usize { self.peak_bytes.load(Ordering::Relaxed) }
    pub fn limit_bytes(&self) -> usize { self.limit_bytes }
}

/// Retained byte charge keeping its budget and ancestor leases alive.
/// Arc clones share one charge; only exclusive access permits shrinking.
#[derive(Debug)]
pub struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: usize,
    parent: Option<Arc<MemoryReservation>>,
}

impl MemoryReservation {
    pub fn bytes(&self) -> usize { self.bytes }
    pub fn budget(&self) -> &Arc<MemoryBudget> { &self.budget }

    /// Releases excess capacity from this lease and its parent leases without lowering recorded peaks.
    /// The requested bytes must cover every allocation retained under this lease; this method does not resize allocations.
    /// Requires exclusive access; requesting more than the held bytes returns an error without changing any counters.
    pub fn shrink_to(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.bytes {
            return Err(Anomaly::incorrect("rtdb/reservation-growth", "A memory reservation can only shrink"));
        }
        self.release_to(bytes);
        Ok(())
    }

    fn release_to(&mut self, bytes: usize) {
        if let Some(parent) = &mut self.parent { Arc::get_mut(parent).expect("reservation exclusively owns its parent lease").release_to(bytes); }
        let released = self.bytes - bytes;
        self.bytes = bytes;
        self.budget.used_bytes.fetch_sub(released, Ordering::AcqRel);
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) { self.budget.used_bytes.fetch_sub(self.bytes, Ordering::AcqRel); }
}
