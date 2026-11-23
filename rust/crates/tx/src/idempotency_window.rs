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
pub fn expiration(submitted_at: i64) -> Option<i64> {
    let expiry = submitted_at.checked_add(HOUR_MICROS)?;
    if expiry == i64::MAX { None } else { Some(expiry) }
}

pub(crate) enum ReplayWindow {
    Elapsed,
    Unrepresentable,
    Open(i64),
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Elapsed) == (submitted_at != i64::MIN && not_after != i64::MAX && submitted_at >= not_after),
        (out is Open) == (
            submitted_at != i64::MIN && not_after != i64::MAX &&
            submitted_at < not_after && submitted_at + 3_600_000_000 < i64::MAX
        ),
        match out {
            ReplayWindow::Open(end) => end < i64::MAX && end >= not_after && end > submitted_at &&
                end as int == if submitted_at + 3_600_000_000 >= not_after {
                    submitted_at + 3_600_000_000
                } else {
                    not_after as int
                },
            _ => true,
        },
))]
pub(crate) fn replay_window(submitted_at: i64, not_after: i64) -> ReplayWindow {
    if submitted_at == i64::MIN || not_after == i64::MAX {
        return ReplayWindow::Unrepresentable;
    }
    if submitted_at >= not_after {
        return ReplayWindow::Elapsed;
    }
    match expiration(submitted_at) {
        Some(ordinary_expiry) => ReplayWindow::Open(ordinary_expiry.max(not_after)),
        None => ReplayWindow::Unrepresentable,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if previous < appended { appended } else { previous },
))]
pub(crate) fn advance_retry_clock(previous: i64, appended: i64) -> i64 {
    previous.max(appended)
}

pub(crate) enum BoundedWindow {
    Expired,
    TooLong,
    Open,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Expired) == (not_after <= observed),
        (out is TooLong) == (not_after as int - observed as int > 3_600_000_000),
        (out is Open) == (observed < not_after && not_after as int - observed as int <= 3_600_000_000),
))]
pub(crate) fn bounded_window(observed: i64, not_after: i64) -> BoundedWindow {
    if not_after <= observed {
        BoundedWindow::Expired
    } else if (not_after as i128) - (observed as i128) > HOUR_MICROS as i128 {
        BoundedWindow::TooLong
    } else {
        BoundedWindow::Open
    }
}
