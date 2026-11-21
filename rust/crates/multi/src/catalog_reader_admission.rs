#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> serving && open_owner && same_residence && current_epoch == opened_epoch,
))]
pub(crate) fn admits(serving: bool, open_owner: bool, same_residence: bool, current_epoch: i32, opened_epoch: i32) -> bool {
    serving && open_owner && same_residence && current_epoch == opened_epoch
}
