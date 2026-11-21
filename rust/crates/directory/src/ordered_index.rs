//! Admission and monotone closure for one ordered index's page namespaces.

#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(admitted =>
    ensures admitted == (!dropped && requested >= oldest),
))]
pub(crate) fn admits(dropped: bool, oldest: u64, requested: u64) -> bool {
    !dropped && requested >= oldest
}

#[cfg_attr(verus_keep_ghost, verus_spec(oldest =>
    ensures
        oldest >= previous,
        oldest >= replacement,
        oldest == previous || oldest == replacement,
))]
pub(crate) fn advance(previous: u64, replacement: u64) -> u64 {
    if previous >= replacement { previous } else { replacement }
}
