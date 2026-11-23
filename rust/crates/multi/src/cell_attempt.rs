#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (staging && current),
))]
pub(crate) fn may_close(staging: bool, current: bool) -> bool {
    staging && current
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (closed && drained && !prepared && fresh
        && match prior {
            Some(prior) => prior < u32::MAX && next == prior + 1,
            None => next == 1,
        }),
))]
pub(crate) fn may_select_next(closed: bool, drained: bool, prepared: bool, fresh: bool, prior: Option<u32>, next: u32) -> bool {
    closed && drained && !prepared && fresh && match prior {
        Some(prior) => prior < u32::MAX && next == prior + 1,
        None => next == 1,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (prepared && !abandoned && receipt == source && source == destination),
))]
pub(crate) fn may_activate(prepared: bool, abandoned: bool, receipt: Option<u32>, source: Option<u32>, destination: Option<u32>) -> bool {
    prepared && !abandoned && receipt == source && source == destination
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (closed && candidate != current
        && (selected.is_none() || selected.unwrap() != candidate)),
))]
pub(crate) fn may_dispose(closed: bool, candidate: u128, current: u128, selected: Option<u128>) -> bool {
    closed && candidate != current && match selected {
        Some(selected) => selected != candidate,
        None => true,
    }
}
