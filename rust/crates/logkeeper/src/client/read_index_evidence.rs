#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(valid =>
    ensures valid == (expected == observed && visible >= -1 && decided >= visible),
))]
pub(super) fn valid_reply(expected: u128, observed: u128, decided: i64, visible: i64) -> bool {
    expected == observed && visible >= -1 && decided >= visible
}
