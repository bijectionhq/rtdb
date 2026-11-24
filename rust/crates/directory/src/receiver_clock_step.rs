#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (instant <= elapsed && elapsed < expires),
))]
pub(crate) fn sample_is_fresh(instant: i64, elapsed: i64, expires: i64) -> bool {
    instant <= elapsed && elapsed < expires
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (observed > previous && (same_publisher || observed >= expires)),
))]
pub(crate) fn advances_frontier(previous: i64, expires: i64, same_publisher: bool, observed: i64) -> bool {
    observed > previous && (same_publisher || observed >= expires)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (same_publisher && observed < expires),
))]
pub(crate) fn keeps_epoch(expires: i64, same_publisher: bool, observed: i64) -> bool {
    same_publisher && observed < expires
}
