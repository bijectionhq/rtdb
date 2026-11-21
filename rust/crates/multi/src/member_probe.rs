#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if control_ok && pgwire_ok {
        0
    } else if !same_endpoints {
        1
    } else if previous == u32::MAX {
        u32::MAX
    } else {
        (previous + 1) as u32
    },
))]
pub(crate) fn failed_rounds(previous: u32, same_endpoints: bool, control_ok: bool, pgwire_ok: bool) -> u32 {
    if control_ok && pgwire_ok {
        0
    } else if same_endpoints {
        previous.saturating_add(1)
    } else {
        1
    }
}
