#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;
#[cfg(verus_keep_ghost)]
use vstd::std_specs::bits::{axiom_u64_leading_zeros, axiom_u64_trailing_zeros};

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

/// Takes the largest aligned bucket starting here that fits before the end.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires start < end, end <= u64::MAX as u128 + 1,
    ensures
        out.0 <= 64,
        out.1 as int == start as int + (1u128 << out.0) as int,
        start < out.1,
        out.1 <= end,
        start & sub(1u128 << out.0, 1) == 0,
        forall|other: u32| other <= 64
            && start & sub(1u128 << other, 1) == 0
            && start as int + (1u128 << other) as int <= end as int ==> other <= out.0,
))]
pub(super) fn cover_step(start: u128, end: u128) -> (u32, u128) {
    let span = end - start;
    if span == u64::MAX as u128 + 1 {
        #[cfg(verus_keep_ghost)]
        proof! {
            assert(start == 0);
            assert(1u128 << 64 == 18_446_744_073_709_551_616u128) by (bit_vector);
            assert(start & sub(1u128 << 64, 1) == 0) by (bit_vector) requires start == 0;
        }
        return (64, end);
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(span <= u64::MAX as u128);
        assert(start <= u64::MAX as u128);
    }
    let offset = start as u64;
    let remaining = span as u64;
    let aligned = offset.trailing_zeros();
    let zeros = remaining.leading_zeros();
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(remaining > 0);
        axiom_u64_leading_zeros(remaining);
        axiom_u64_trailing_zeros(offset);
    }
    let fits = 63 - zeros;
    let level = aligned.min(fits);
    let width = 1u64 << level;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(width <= remaining) by (bit_vector)
            requires level <= fits, (remaining >> fits) & 1u64 != 0,
                width == 1u64 << level;
        assert(start & sub(1u128 << level, 1) == 0) by (bit_vector)
            requires start == offset as u128, level <= aligned, aligned <= 64,
                offset << sub(64u64, aligned as u64) == 0;
        assert(width as u128 == 1u128 << level && width > 0) by (bit_vector)
            requires level <= 63, width == 1u64 << level;
        assert forall|other: u32| other <= 64
            && start & sub(1u128 << other, 1) == 0
            && start as int + (1u128 << other) as int <= end as int implies other <= level by {
            assert(1u128 << 64 == 18_446_744_073_709_551_616u128) by (bit_vector);
            assert(other < 64);
            assert((1u64 << other) as u128 == 1u128 << other) by (bit_vector)
                requires other < 64;
            assert(1u64 << other <= remaining);
            assert(other <= fits) by (bit_vector)
                requires other < 64, fits < 64, 1u64 << other <= remaining,
                    remaining >> add(fits, 1) == 0;
            assert(other <= aligned) by (bit_vector)
                requires other < 64, aligned <= 64, start == offset as u128,
                    start & sub(1u128 << other, 1) == 0,
                    aligned < 64 ==> (offset >> aligned) & 1u64 != 0;
        }
    }
    (level, start + width as u128)
}
