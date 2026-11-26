//! The clock decision of one `rtdb.store.v1.Append` at its ordering point
//! (T7): exactly Bijection's `fact_store::persistence::judge::admit`, which
//! RTDB vendors byte for byte as `store_judge`.
//!
//! [`judge_store_clock`] is the only place `TransactState::resolve_store`
//! judges the writer's clock sample against the store's last stamp and the
//! append's window. Ties at the last stamp's micros are admitted and ordered
//! by position; a sample below it is the writer clock's regression, never a
//! wait and never raised; and whether an append is outside its window depends
//! on its sample alone.
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
