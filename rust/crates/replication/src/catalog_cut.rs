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
