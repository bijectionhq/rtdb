#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(fresh => ensures fresh == (candidate != previous),))]
pub(crate) fn fresh_survivor_allocation(candidate: u128, previous: u128) -> bool {
    candidate != previous
}

#[cfg_attr(verus_keep_ghost, verus_spec(declared => ensures
    declared == (address_matches && observed_bytes == expected_bytes),
))]
pub(crate) fn declares_written_member(
    address_matches: bool,
    observed_bytes: u64,
    expected_bytes: u64,
) -> bool {
    address_matches && observed_bytes == expected_bytes
}
