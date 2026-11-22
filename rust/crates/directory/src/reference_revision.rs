#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> parent == expected && can_fork && lineage_matches,
))]
pub(super) fn may_publish_fork(parent: u64, expected: u64, can_fork: bool, lineage_matches: bool) -> bool {
    parent == expected && can_fork && lineage_matches
}

#[cfg_attr(verus_keep_ghost, verus_spec(next =>
    ensures
        current == u64::MAX ==> next.is_none(),
        current < u64::MAX ==> next == Some((current + 1) as u64),
))]
pub(super) fn advance_revision(current: u64) -> Option<u64> {
    if current == u64::MAX { None } else { Some(current + 1) }
}
