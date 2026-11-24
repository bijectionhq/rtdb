#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted == (if kind_six { prepared_action && has_clock_slot } else { !prepared_action }),
))]
pub(super) fn admits(kind_six: bool, prepared_action: bool, has_clock_slot: bool) -> bool {
    kind_six == prepared_action && (!kind_six || has_clock_slot)
}
