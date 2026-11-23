#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (system_from == transaction_revision),
))]
pub(crate) fn matches_transaction(system_from: i64, transaction_revision: i64) -> bool {
    system_from == transaction_revision
}
