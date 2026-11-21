#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out as int == time as int - i64::MIN as int,
        out <= u64::MAX as u128,
))]
pub(super) fn offset(time: i64) -> u128 {
    (time as i128 - i64::MIN as i128) as u128
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out as int == if time == i64::MAX {
            u64::MAX as int + 1
        } else {
            time as int - i64::MIN as int
        },
))]
pub(super) fn upper_bound(time: i64) -> u128 {
    if time == i64::MAX {
        u64::MAX as u128 + 1
    } else {
        offset(time)
    }
}
