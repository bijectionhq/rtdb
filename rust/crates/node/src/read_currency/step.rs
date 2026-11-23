#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if pending.is_none() || pending.unwrap().1 < tip {
        Some((observed_at, tip))
    } else {
        pending
    },
))]
pub(super) fn observe(pending: Option<(i64, i64)>, observed_at: i64, tip: i64) -> Option<(i64, i64)> {
    match pending {
        Some((_, previous)) if tip <= previous => pending,
        _ => Some((observed_at, tip)),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if pending.is_some() && installed.is_some() && pending.unwrap().1 <= installed.unwrap() {
        let observed = pending.unwrap().0;
        let next = if anchor.is_some() && observed < anchor.unwrap() { anchor.unwrap() } else { observed };
        (Some(next), None)
    } else {
        (anchor, pending)
    },
))]
pub(super) fn confirm(
    anchor: Option<i64>,
    pending: Option<(i64, i64)>,
    installed: Option<i64>,
) -> (Option<i64>, Option<(i64, i64)>) {
    let (Some((observed_at, tip)), Some(installed)) = (pending, installed) else {
        return (anchor, pending);
    };
    if installed < tip {
        return (anchor, pending);
    }
    let observed_at = match anchor {
        Some(previous) if observed_at < previous => previous,
        _ => observed_at,
    };
    (Some(observed_at), None)
}
