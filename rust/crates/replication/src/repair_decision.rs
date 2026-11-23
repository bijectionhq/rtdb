#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (expected == observed && digest_matches),
))]
pub(crate) fn matches_root(expected: i64, observed: i64, digest_matches: bool) -> bool {
    expected == observed && digest_matches
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (after >= 0 && after < through && after >> 48 == through >> 48),
))]
pub(crate) fn admits_repair_interval(after: i64, through: i64) -> bool {
    after >= 0 && after < through && after >> 48 == through >> 48
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (recorded.is_none() || recorded == Some(observed)),
))]
pub(crate) fn matches_retained_boundary(recorded: Option<i64>, observed: i64) -> bool {
    match recorded {
        Some(boundary) => boundary == observed,
        None => true,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_none() == (position == i64::MAX),
        out.is_some() ==> out.unwrap() as int == position as int + 1,
))]
pub(crate) fn exclusive_replay_end(position: i64) -> Option<i64> {
    position.checked_add(1)
}
