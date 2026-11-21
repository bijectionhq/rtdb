#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> (recognized && installed >= removal && !required),
))]
pub(crate) fn confirms_removal(recognized: bool, installed: i64, removal: i64, required: bool) -> bool {
    recognized && installed >= removal && !required
}
