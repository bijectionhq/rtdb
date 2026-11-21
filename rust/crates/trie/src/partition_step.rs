#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn digit(byte: int, lane: int) -> int {
        let place = if lane == 0 { 64 } else if lane == 1 { 16 } else if lane == 2 { 4 } else { 1 };
        (byte / place) % 4
    }
}

/// Selects the base-four digit at the given depth in the big-endian IID.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Some) == (depth < 64),
        out is Some ==> (out->Some_0) < 4 &&
            (out->Some_0) as int == digit(iid@[(depth as int) / 4] as int, (depth as int) % 4),
))]
#[inline]
pub fn base4_bucket(iid: &[u8; 16], depth: usize) -> Option<u8> {
    if depth >= 64 {
        return None;
    }
    let lane = depth % 4;
    let byte = iid[depth / 4];
    let shift = 6 - 2 * lane;
    let bucket = (byte >> shift) & 3;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(lane < 4);
        if lane == 0 {
            assert(shift == 6);
            assert((byte >> 6usize) & 3u8 == (byte / 64u8) % 4u8) by (bit_vector);
            assert(bucket == (byte / 64u8) % 4u8);
        } else if lane == 1 {
            assert(shift == 4);
            assert((byte >> 4usize) & 3u8 == (byte / 16u8) % 4u8) by (bit_vector);
            assert(bucket == (byte / 16u8) % 4u8);
        } else if lane == 2 {
            assert(shift == 2);
            assert((byte >> 2usize) & 3u8 == (byte / 4u8) % 4u8) by (bit_vector);
            assert(bucket == (byte / 4u8) % 4u8);
        } else {
            assert(shift == 0);
            assert((byte >> 0usize) & 3u8 == byte % 4u8) by (bit_vector);
            assert(bucket == byte % 4u8);
        }
        assert(bucket as int == digit(byte as int, lane as int));
        assert(bucket < 4);
    }
    Some(bucket)
}
