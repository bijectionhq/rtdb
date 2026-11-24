#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (switch_at <= lower && lower <= upper && upper < deadline),
))]
pub(crate) fn fence_at_switch(lower: u64, upper: u64, switch_at: u64, deadline: u64) -> bool {
    switch_at <= lower && lower <= upper && upper < deadline
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (before > 0 && before as int * 1000 + clock_error as int <= switch_at as int),
))]
pub(crate) fn original_cutoff_before_fence(before: i64, clock_error: u64, switch_at: u64) -> bool {
    if before <= 0 { return false; }
    let Some(upper) = (before as u64).checked_mul(1000) else { return false; };
    let Some(upper) = upper.checked_add(clock_error) else { return false; };
    upper <= switch_at
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (incumbent > 0 && successor as int == incumbent as int + 1),
))]
pub(crate) fn next_writer_epoch(incumbent: u64, successor: u64) -> bool {
    incumbent > 0 && incumbent.checked_add(1) == Some(successor)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (clock_upper < original_drain_until),
))]
pub(crate) fn retained_read_live(clock_upper: u64, original_drain_until: u64) -> bool {
    clock_upper < original_drain_until
}
