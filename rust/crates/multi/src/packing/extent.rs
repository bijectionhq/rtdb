#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(fits =>
    ensures fits <==> (offset as int) + (length as int) <= (limit as int),
))]
pub(crate) fn entry_fits(offset: u64, length: u64, limit: u64) -> bool {
    offset <= limit && length <= limit - offset
}
