use std::sync::{Arc, Mutex};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};

/// Call-owned reservations for expression intermediates and returned values.
/// Keep the scope alive until returned values have been dropped or transferred to another charged owner.
pub struct EvaluationScope {
    memory: Arc<MemoryBudget>,
    reservations: Mutex<Vec<Arc<MemoryReservation>>>,
}

impl EvaluationScope {
    /// Acquires the scope's bookkeeping reservation from the supplied finite budget.
    pub fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        let initial = memory.reserve(4096)?;
        Ok(Self { memory: Arc::clone(memory), reservations: Mutex::new(vec![initial]) })
    }

    /// Reserves bytes plus bookkeeping before allocation; charges last until the scope is dropped.
    /// Calls may share the scope across threads; budget exhaustion returns without granting a reservation.
    pub fn reserve(&self, bytes: usize) -> Result<()> {
        let bytes = bytes.checked_add(256).ok_or_else(|| Anomaly::incorrect("rtdb.expr/allocation-size", "Expression allocation exceeds addressable memory"))?;
        let reservation = self.memory.reserve(bytes)?;
        self.reservations.lock().map_err(|_| Anomaly::fault("rtdb.expr/allocation-scope", "Expression allocation scope was poisoned"))?.push(reservation);
        Ok(())
    }
}
