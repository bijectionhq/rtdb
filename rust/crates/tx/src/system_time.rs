#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match previous {
        Some(last) => last < candidate,
        None => true,
    },
))]
pub(crate) fn advances(previous: Option<i64>, candidate: i64) -> bool {
    match previous {
        Some(last) => last < candidate,
        None => true,
    }
}

/// A client may name a system time that advances the history and does not run ahead of the log's append time.
/// History time therefore never leads the log's clock, so automatic assignment keeps a following chronon.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (match previous {
        Some(last) => last < candidate,
        None => true,
    } && candidate <= log_time),
))]
pub(crate) fn admits_client_time(previous: Option<i64>, candidate: i64, log_time: i64) -> bool {
    advances(previous, candidate) && candidate <= log_time
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match previous {
        Some(last) if candidate <= last =>
            if last == i64::MAX { None } else { Some((last + 1) as i64) },
        _ => Some(candidate),
    },
))]
pub(crate) fn automatic_time(previous: Option<i64>, candidate: i64) -> Option<i64> {
    match previous {
        Some(last) if !advances(previous, candidate) => last.checked_add(1),
        _ => Some(candidate),
    }
}
