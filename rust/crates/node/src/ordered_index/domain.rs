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

/// Start of the aligned bucket at this level that contains the point.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires point <= u64::MAX as u128, level <= 64,
    ensures
        out as u128 <= point,
        point < out as int + (1_u128 << level) as int,
        (out as u128 & ((1_u128 << level) - 1) as u128) == 0,
))]
pub(super) fn bucket_start(point: u128, level: u32) -> u64 {
    let aligned = point >> level << level;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(aligned <= point
            && point < add(aligned, 1_u128 << level)
            && aligned & sub(1_u128 << level, 1) == 0) by (bit_vector)
            requires point <= u64::MAX as u128, level <= 64,
                aligned == point >> level << level;
    }
    aligned as u64
}
