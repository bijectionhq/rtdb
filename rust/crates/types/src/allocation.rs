use std::sync::{Arc, Mutex};
use crate::{Anomaly, MemoryBudget, MemoryReservation, Result};

/// Call-owned reservations for intermediates and returned values.
/// Keep the scope alive until returned values have been dropped or transferred to another charged owner.
pub struct AllocationScope {
    memory: Arc<MemoryBudget>,
    lease: Mutex<Option<Arc<MemoryReservation>>>,
}

impl AllocationScope {
    /// Borrows the budget for values that acquire and retain their own allocation leases.
    /// This does not transfer the scope's charges; admit owned storage before allocation.
    pub fn memory_budget(&self) -> &Arc<MemoryBudget> { &self.memory }

    /// Transfers the charge for retained allocations to their next owner.
    pub fn into_reservation(self) -> Result<Option<Arc<MemoryReservation>>> {
        self.lease.into_inner().map_err(|_| Anomaly::fault("rtdb.expr/allocation-scope", "Expression allocation scope was poisoned"))
    }

    /// Acquires the scope's bookkeeping reservation from the supplied finite budget.
    pub fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        let initial = memory.reserve(4096)?;
        Ok(Self { memory: Arc::clone(memory), lease: Mutex::new(Some(initial)) })
    }

    /// Opens an empty child scope that charges the same budget and releases its reservations independently.
    pub fn scratch(&self) -> Self {
        Self { memory: Arc::clone(&self.memory), lease: Mutex::new(None) }
    }

    /// Releases every charge but the scope's bookkeeping, as dropping it and opening another would, keeping its lease.
    /// Callers must have charged elsewhere anything still alive that this scope admitted.
    pub fn reset(&self) -> Result<()> {
        let mut lease = self.lease.lock().map_err(|_| Anomaly::fault("rtdb.expr/allocation-scope", "Expression allocation scope was poisoned"))?;
        match lease.as_mut().and_then(Arc::get_mut) {
            Some(held) if held.bytes() > 4096 => held.shrink_to(4096),
            Some(_) => Ok(()),
            None => { *lease = Some(self.memory.reserve(4096)?); Ok(()) }
        }
    }

    /// Reserves bytes plus bookkeeping before allocation; charges last until the scope is dropped.
    /// Calls may share the scope across threads; budget exhaustion returns without granting a reservation.
    pub fn reserve(&self, bytes: usize) -> Result<()> {
        let bytes = bytes.checked_add(256).ok_or_else(|| Anomaly::incorrect("rtdb.expr/allocation-size", "Expression allocation exceeds addressable memory").with_kind(crate::AnomalyKind::ProgramLimitExceeded))?;
        let mut lease = self.lease.lock().map_err(|_| Anomaly::fault("rtdb.expr/allocation-scope", "Expression allocation scope was poisoned"))?;
        match lease.as_mut().and_then(Arc::get_mut) {
            Some(held) => held.grow_to(held.bytes().checked_add(bytes).ok_or_else(|| Anomaly::incorrect("rtdb.expr/allocation-size", "Expression allocation exceeds addressable memory").with_kind(crate::AnomalyKind::ProgramLimitExceeded))?),
            None => { *lease = Some(self.memory.reserve(bytes)?); Ok(()) }
        }
    }
}
