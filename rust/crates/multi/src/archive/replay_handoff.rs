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
