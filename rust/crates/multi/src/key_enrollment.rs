#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out <==> current_writer && original_residence && !observed_history,
))]
pub(crate) fn may_enroll(current_writer: bool, original_residence: bool, observed_history: bool) -> bool {
    current_writer && original_residence && !observed_history
}
