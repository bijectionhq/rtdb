#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(check => ensures check == !preparation_held,))]
pub(super) fn may_check_fork_outcome(preparation_held: bool) -> bool {
    !preparation_held
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    (out == Some(true)) == !has_stage,
    (out == Some(false)) == (has_stage && deleted),
    (out == None) == (has_stage && !deleted),
))]
pub(super) fn settled_fork_outcome(has_stage: bool, deleted: bool) -> Option<bool> {
    if !has_stage {
        Some(true)
    } else if deleted {
        Some(false)
    } else {
        None
    }
}
