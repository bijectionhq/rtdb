#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(next =>
    ensures
        current == u64::MAX ==> next.is_none(),
        current < u64::MAX ==> next == Some((current + 1) as u64),
))]
pub(super) fn next_manifest_sequence(current: u64) -> Option<u64> {
    current.checked_add(1)
}

#[cfg_attr(verus_keep_ghost, verus_spec(prune =>
    ensures prune <==> candidate < confirmed,
))]
pub(super) fn may_prune_manifest(candidate: u64, confirmed: u64) -> bool {
    candidate < confirmed
}
