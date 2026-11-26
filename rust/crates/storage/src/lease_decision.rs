#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// A request may begin only strictly before the deadline.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (now < deadline),
))]
pub fn lease_admits(now: i64, deadline: i64) -> bool {
    now < deadline
}
