#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (record_count == 0),
))]
pub(crate) fn replica_read_is_empty(record_count: usize) -> bool {
    record_count == 0
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (!fenced && may_lead && !already_leading && !claim_pending),
))]
pub(crate) fn should_start_claim(fenced: bool, may_lead: bool, already_leading: bool, claim_pending: bool) -> bool {
    !fenced && may_lead && !already_leading && !claim_pending
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if fenced { 0i64 } else { 1i64 },
))]
pub(crate) fn initial_term_sequence(fenced: bool) -> i64 {
    if fenced { 0 } else { 1 }
}
