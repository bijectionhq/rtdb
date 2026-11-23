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
