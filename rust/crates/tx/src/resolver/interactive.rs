use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_index::{Snapshot, StagedWrite, TransactionBuffer};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, TransactionKey, TxId};

use crate::{ScheduledOperation, SourceTransaction, SubmitOptions, TxOp};
use crate::source::Operation;
use super::{OpenTransaction, ResolveContext};

pub struct InteractiveTransaction {
    buffer: Option<TransactionBuffer>,
    options: SubmitOptions,
    context: Arc<ResolveContext>,
}

impl InteractiveTransaction {
    pub fn open(base: Snapshot, clock: Timestamp, mut options: SubmitOptions, context: Arc<ResolveContext>) -> Result<Self> {
        if options.user_metadata.is_some() { return Err(unsupported()); }
        let id = next_provisional_id(base.tx_basis())?;
        let system_time = match options.system_time {
            Some(requested) if base.tx_basis().is_none_or(|key| requested > key.system_time) => requested,
            Some(_) => return Err(Anomaly::incorrect("invalid-system-time", "Interactive system time must follow the snapshot basis")),
            None => crate::system_time::automatic_time(base.tx_basis().map(|key| key.system_time.0), clock.0)
                .map(Timestamp).ok_or_else(|| Anomaly::incorrect("xtdb/time-overflow", "Timestamp has no following chronon"))?,
        };
        options.system_time = Some(system_time);
        let buffer = TransactionBuffer::new(base, TransactionKey { tx_id: TxId(id), system_time }, Arc::clone(&context.memory))?;
        Ok(Self { buffer: Some(buffer), options, context })
    }

    pub fn belongs_to(&self, context: &Arc<ResolveContext>) -> bool { Arc::ptr_eq(&self.context, context) }

    pub fn snapshot(&self) -> Result<Snapshot> {
        self.buffer.as_ref().ok_or_else(failed)?.snapshot()
    }

    pub fn system_time(&self) -> Result<Timestamp> {
        self.buffer.as_ref().map(|buffer| buffer.tx_key().system_time).ok_or_else(failed)
    }

    pub async fn apply(&mut self, operation: ScheduledOperation) -> Result<()> {
        let operation = operation.into_operation();
        if !matches!(operation, TxOp::Sql { .. } | TxOp::SqlBytes { .. }) { return Err(unsupported()); }
        let buffer = self.buffer.take().ok_or_else(failed)?;
        let mut tx = OpenTransaction::with_buffer(buffer, &self.options, &self.context);
        tx.interactive = true;
        tx.apply(&Operation::Public(operation)).await?;
        self.buffer = Some(tx.buffer);
        Ok(())
    }

    pub fn into_source(self) -> Result<SourceTransaction> {
        let writes = self.buffer.ok_or_else(failed)?.stage()?;
        let overhead = self.options.default_tz.len().saturating_add(self.options.user.as_ref().map_or(0, String::len))
            .checked_add(2048).ok_or_else(super::overflow)?;
        let prepared = PreparedInteractive {
            writes: Arc::new(writes),
            reservation: self.context.memory.reserve(overhead)?,
            rejection: self.context.memory.prepay_reclaimable(1024 * 1024)?,
        };
        let mut source = SourceTransaction::from_operations(Vec::new(), self.options, &self.context.memory)?;
        source.interactive = Some(prepared);
        Ok(source)
    }
}

#[derive(Clone)]
pub(crate) struct PreparedInteractive {
    writes: Arc<StagedWrite>,
    reservation: Arc<MemoryReservation>,
    rejection: Arc<MemoryBudget>,
}

impl PreparedInteractive {
    pub(crate) fn retained_bytes(&self) -> usize {
        usize::try_from(self.writes.retained_batch_bytes()).unwrap_or(usize::MAX)
            .saturating_add(self.reservation.bytes()).saturating_add(self.rejection.limit_bytes())
    }

    pub(crate) fn rejection_budget(&self) -> Arc<MemoryBudget> { Arc::clone(&self.rejection) }

    pub(super) fn validate_basis(&self, base: &Snapshot) -> Result<()> {
        if self.writes.predecessor() != base.tx_basis() || self.writes.block_index() != base.block_index() {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/serialization-failure", "The database advanced during the interactive transaction")
                .with_context("sql-state", "40001".into()));
        }
        Ok(())
    }

    pub(super) fn resolve(self, key: TransactionKey, memory: &Arc<MemoryBudget>) -> Result<StagedWrite> {
        (*self.writes).clone().assign_interactive_position(key, memory)
    }
}

fn next_provisional_id(predecessor: Option<TransactionKey>) -> Result<i64> {
    predecessor.map_or(-1, |key| key.tx_id.0).checked_add(1).ok_or_else(super::overflow)
}

pub(super) fn unsupported() -> Anomaly {
    Anomaly::unsupported("rtdb/interactive-operation", "This operation is unavailable in an interactive write transaction")
}

fn failed() -> Anomaly {
    Anomaly::incorrect("rtdb/interactive-failed", "The interactive transaction failed")
}
