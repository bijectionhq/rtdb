#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (parked && exact_checkpoint && same_selection
        && (prior.is_none() || prior.unwrap() < candidate)),
))]
pub(super) fn may_install(
    parked: bool, exact_checkpoint: bool, same_selection: bool, prior: Option<i64>, candidate: i64,
) -> bool {
    parked && exact_checkpoint && same_selection && match prior {
        Some(prior) => prior < candidate,
        None => true,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(retained =>
    ensures retained <==> (selected.is_some() && selected.unwrap() == capture)
        || (repair.is_some() && repair.unwrap().0 == capture
            && (repair.unwrap().1 || selected.is_none())),
))]
pub(super) fn retains_capture(selected: Option<u128>, repair: Option<(u128, bool)>, capture: u128) -> bool {
    matches!(selected, Some(source) if source == capture)
        || matches!(repair, Some((source, pending)) if source == capture && (pending || selected.is_none()))
}
