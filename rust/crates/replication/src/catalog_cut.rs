#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.is_some() <==> tracking && !block_pending && !apply_deferred,
        out.is_some() ==> out.unwrap() == received,
))]
pub(crate) fn completed_prefix(received: i64, tracking: bool, block_pending: bool, apply_deferred: bool) -> Option<i64> {
    if tracking && !block_pending && !apply_deferred {
        Some(received)
    } else {
        None
    }
}

/// A reopened prefix belongs to its original checkpoint and covers its exact end.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (loaded_base == captured_base && captured_base <= captured_end
        && replayed_end == captured_end && (captured_base < captured_end ==> held_suffix)),
))]
pub(crate) fn completes_snapshot_replay(
    loaded_base: i64, captured_base: i64, replayed_end: i64, captured_end: i64, held_suffix: bool,
) -> bool {
    loaded_base == captured_base && captured_base <= captured_end
        && replayed_end == captured_end && (captured_base == captured_end || held_suffix)
}
