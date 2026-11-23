#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if (after_bootstrap && index == 1) || (!after_bootstrap && index == 0) {
        Some(submitted_at)
    } else {
        None
    },
))]
pub(crate) fn guard_time_at(index: usize, after_bootstrap: bool, submitted_at: i64) -> Option<i64> {
    let guard_index = if after_bootstrap { 1 } else { 0 };
    if index == guard_index { Some(submitted_at) } else { None }
}


#[cfg(not(verus_keep_ghost))]
mod original {
    use rtdb_types::{Anomaly, Result, Timestamp, TransactionKey, TxId};

    const TX_ID: &str = "legacy-guard-original-tx-id";
    const SYSTEM_TIME: &str = "legacy-guard-original-system-time-micros";

    pub(crate) fn with_historical_original(error: Anomaly, original: TransactionKey) -> Anomaly {
        error.with_context(TX_ID, original.tx_id.0.into())
            .with_context(SYSTEM_TIME, original.system_time.0.into())
    }

    pub fn attributed_historical_guard_original(error: &Anomaly) -> Result<Option<TransactionKey>> {
        match (error.context.get(TX_ID), error.context.get(SYSTEM_TIME)) {
            (None, None) => Ok(None),
            (Some(id), Some(time)) => {
                let original = id.as_i64().zip(time.as_i64()).map(|(id, time)| TransactionKey {
                    tx_id: TxId(id), system_time: Timestamp(time),
                });
                original.map(Some).ok_or_else(bad_original)
            }
            _ => Err(bad_original()),
        }
    }

    fn bad_original() -> Anomaly {
        Anomaly::fault("rtdb.tx/legacy-guard-original", "The stored guard attribution has an invalid original coordinate")
    }
}

#[cfg(not(verus_keep_ghost))]
pub use original::attributed_historical_guard_original;
#[cfg(not(verus_keep_ghost))]
pub(crate) use original::with_historical_original;
