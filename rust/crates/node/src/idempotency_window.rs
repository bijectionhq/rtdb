#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

const HOUR_MICROS: i64 = 3_600_000_000;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if submitted_at + 3_600_000_000 < i64::MAX {
        Some((submitted_at + 3_600_000_000) as i64)
    } else {
        None
    },
))]
pub(crate) fn expiration(submitted_at: i64) -> Option<i64> {
    let expiry = submitted_at.checked_add(HOUR_MICROS)?;
    if expiry == i64::MAX { None } else { Some(expiry) }
}
