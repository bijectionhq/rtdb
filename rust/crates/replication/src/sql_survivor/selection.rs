#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(keep => ensures keep == !origin_selected,))]
pub(super) fn keep_inherited_event(origin_selected: bool) -> bool {
    !origin_selected
}

#[cfg_attr(verus_keep_ghost, verus_spec(keep => ensures
    keep == match before {
        Some((tx, row)) => !(transaction < tx || (transaction == tx && ordinal < row)),
        None => true,
    },
))]
pub(super) fn keep_local_event(transaction: i64, ordinal: u64, before: Option<(i64, u64)>) -> bool {
    match before {
        Some((marker_tx, marker_ordinal)) => {
            marker_tx < transaction || (marker_tx == transaction && marker_ordinal <= ordinal)
        }
        None => true,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(write => ensures write == (rows > 0),))]
pub(super) fn write_survivor_trie(rows: u32) -> bool {
    rows != 0
}
