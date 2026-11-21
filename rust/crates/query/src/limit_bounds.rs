#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Some) == (0 <= value <= i64::MAX as i128),
        out is Some ==> out.unwrap() as int == value as int,
))]
pub(crate) fn checked_row_count(value: i128) -> Option<u64> {
    if value < 0 || value > i64::MAX as i128 { return None; }
    Some(value as u64)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out <= i64::MAX as u64,
        out as int == match fetch {
            None => i64::MAX as int,
            Some(fetch) => if offset as int + fetch as int <= i64::MAX as int {
                offset as int + fetch as int
            } else {
                i64::MAX as int
            },
        },
))]
pub(crate) fn sort_prefix_len(offset: u64, fetch: Option<u64>) -> u64 {
    let cap = i64::MAX as u64;
    offset.saturating_add(fetch.unwrap_or(cap)).min(cap)
}
