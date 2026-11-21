#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out == match listed {
            Some(index) if index > attempted => Some(index),
            _ => None,
        },
        out.is_some() ==> (i64::MAX as int) - (out.unwrap() as int)
            < (i64::MAX as int) - (attempted as int),
))]
pub(crate) fn newer_checkpoint(attempted: i64, listed: Option<i64>) -> Option<i64> {
    match listed {
        Some(index) if index > attempted => Some(index),
        _ => None,
    }
}
