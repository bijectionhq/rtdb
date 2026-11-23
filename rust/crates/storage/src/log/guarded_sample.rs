#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (if 0 < clock as int + 1
        && staged_upper as int <= clock as int + 1
        && clock as int + 1 < cutoff as int
        && (journal_before.is_some() ==> cutoff <= journal_before.unwrap())
    { Some(clock) } else { None }),
))]
pub(super) fn guarded_sample(
    clock: i64,
    staged_upper: i64,
    cutoff: i64,
    journal_before: Option<i64>,
) -> Option<i64> {
    let upper = match clock.checked_add(1) {
        Some(upper) => upper,
        None => return None,
    };
    let journal_covers_cutoff = match journal_before {
        Some(before) => before >= cutoff,
        None => true,
    };
    if upper <= 0 || upper < staged_upper || upper >= cutoff || !journal_covers_cutoff {
        None
    } else {
        Some(clock)
    }
}
