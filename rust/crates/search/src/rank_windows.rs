#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// Consecutive document IDs in a scoring window.
const WINDOW: u32 = 128;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out as int == (doc as int) / 128,
))]
pub(crate) fn window_of(doc: u32) -> u32 {
    doc / WINDOW
}

/// Half-open document bounds, clamped at the largest representable endpoint.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out.0 as int) == if (window as int) * 128 <= u32::MAX as int {
            (window as int) * 128
        } else {
            u32::MAX as int
        },
        (out.1 as int) == if ((window as int) + 1) * 128 <= u32::MAX as int {
            ((window as int) + 1) * 128
        } else {
            u32::MAX as int
        },
))]
pub(crate) fn document_range(window: u32) -> (u32, u32) {
    (
        window.saturating_mul(WINDOW),
        window.saturating_add(1).saturating_mul(WINDOW),
    )
}

/// Preserve a conservative pair of extrema when adding a posting to a window.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.0 == if old_frequency >= frequency { old_frequency } else { frequency },
        out.1 == if old_length <= length { old_length } else { length },
))]
pub(crate) fn extend_impact(
    old_frequency: u32,
    old_length: u32,
    frequency: u32,
    length: u32,
) -> (u32, u32) {
    (old_frequency.max(frequency), old_length.min(length))
}
