#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (required_block >= 0 && required_epoch > 0
        && adopted_block >= required_block && adopted_epoch >= required_epoch && !still_retained),
))]
pub(crate) fn reset_allows_retirement(
    required_block: i64,
    required_epoch: u64,
    adopted_block: i64,
    adopted_epoch: u64,
    still_retained: bool,
) -> bool {
    required_block >= 0 && required_epoch > 0
        && adopted_block >= required_block && adopted_epoch >= required_epoch && !still_retained
}
