//! The clock decision of one `rtdb.store.v1.Append` at its ordering point
//! (T7): exactly Bijection's `fact_store::persistence::judge::admit`, which
//! RTDB vendors byte for byte as `store_judge`.
//!
//! [`judge_store_clock`] is the only place `TransactState::resolve_store`
//! judges the writer's clock sample against the store's last stamp and the
//! append's window. Ties at the last stamp's micros are admitted and ordered
//! by position. The ordering point waits out a regression before calling the
//! judge. Whether an append is outside its window depends on its sample alone.
//!
//! V9′ (Verus area `store_append_judge`) proves this function equal to the
//! vendored `admitted` spec over these exact bytes. EVERY ANNOTATION IS
//! `cfg`-GATED ON `verus_keep_ghost`: an ordinary build gains no dependency
//! and no code.
#![allow(unexpected_cfgs)]

use store_judge::Admitted;
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// The decision `judge::admit` gives an append whose reads passed: `head` is
/// the store's last version, `last` the last stamp's micros (`None` before
/// any commit), `now` the writer's sample, `[not_before, before)` the
/// append's window and `write_free` whether it only validates.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == store_judge::admitted(head, last, now, not_before, before, write_free)
))]
pub(crate) fn judge_store_clock(
    head: u64, last: Option<i64>, now: i64, not_before: Option<i64>, before: Option<i64>, write_free: bool,
) -> Admitted {
    store_judge::admit(head, last, now, not_before, before, write_free)
}

/// Wait within the current ordering turn for the raw clock to reach the
/// last ordered Store reading, including a reading inherited on takeover.
pub async fn sample_store_clock(
    clock: &(dyn Fn() -> rtdb_types::Timestamp + Sync),
    floor: Option<rtdb_types::Timestamp>,
    skew_micros: i64,
) -> rtdb_types::Timestamp {
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    let Some(floor) = floor else { return clock(); };
    let started = Instant::now();
    let skew = Duration::from_micros(skew_micros.max(0) as u64);
    let mut fault_reported = false;
    loop {
        let sample = clock();
        if sample >= floor { return sample; }
        if !fault_reported && started.elapsed() >= skew {
            tracing::error!("Store ordering clock has not caught up within its skew bound");
            fault_reported = true;
        }
        let gap = floor.0.saturating_sub(sample.0) as u64;
        sleep(Duration::from_micros(gap.clamp(1, 10_000))).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use rtdb_types::Timestamp;

    use super::sample_store_clock;

    #[tokio::test]
    async fn the_ordering_turn_resamples_until_it_reaches_the_inherited_reading() {
        let clock = AtomicI64::new(98);
        let read = || Timestamp(clock.fetch_add(1, Ordering::Relaxed));
        assert_eq!(sample_store_clock(&read, Some(Timestamp(100)), 250_000).await, Timestamp(100));
        assert_eq!(clock.load(Ordering::Relaxed), 101);
    }
}
