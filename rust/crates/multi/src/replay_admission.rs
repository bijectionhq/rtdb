#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (tag == b'P' || tag == b'B' || tag == b'D' || tag == b'C' || tag == b'H' || tag == b'S'),
))]
pub(crate) fn is_setup_frame(tag: u8) -> bool {
    matches!(tag, b'P' | b'B' | b'D' | b'C' | b'H' | b'S')
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (original_supports_permit && still_before_cutoff),
))]
pub(crate) fn may_send_simple_replay(original_supports_permit: bool, still_before_cutoff: bool) -> bool {
    original_supports_permit && still_before_cutoff
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if window > 0 && issued_at + window < i64::MAX {
        Some((issued_at + window) as i64)
    } else {
        None
    },
))]
pub(crate) fn cutoff_micros(issued_at: i64, window: i64) -> Option<i64> {
    if window <= 0 { return None; }
    let cutoff = issued_at.checked_add(window)?;
    if cutoff == i64::MAX { None } else { Some(cutoff) }
}
