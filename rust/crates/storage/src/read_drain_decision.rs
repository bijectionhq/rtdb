#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (admission_closed && !live_reads_seen),
))]
pub(crate) fn provider_drain_completed(admission_closed: bool, live_reads_seen: bool) -> bool {
    admission_closed && !live_reads_seen
}
