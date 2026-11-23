#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> selected_epoch == installed_epoch && selected < installed,
))]
pub(super) fn receipt_follows_selection(selected: i64, selected_epoch: i32, installed: i64, installed_epoch: i32) -> bool {
    selected_epoch == installed_epoch && selected < installed
}
