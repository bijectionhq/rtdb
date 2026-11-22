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

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> open && prepared_at == Some(current),
))]
pub(super) fn may_publish_reference(current: u64, open: bool, prepared_at: Option<u64>) -> bool {
    open && prepared_at == Some(current)
}

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> current == expected && residence_matches,
))]
pub(super) fn may_settle_reference_disposal(current: u64, expected: u64, residence_matches: bool) -> bool {
    current == expected && residence_matches
}

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> authorized && same_owner && matching_fence,
))]
pub(super) fn may_settle_exact_owned(authorized: bool, same_owner: bool, matching_fence: bool) -> bool {
    authorized && same_owner && matching_fence
}
