use crate::production::{ReplayWindow, expiration, replay_window};
use vstd::prelude::*;

#[verus_spec(out =>
    requires admitted + 3_600_000_000 < i64::MAX,
    ensures out.is_some(), out.unwrap() > admitted, out.unwrap() < i64::MAX,
)]
fn the_window_has_a_strictly_later_finite_endpoint(admitted: i64) -> Option<i64> {
    expiration(admitted)
}

#[verus_spec(out =>
    requires
        i64::MIN < first < cutoff < i64::MAX,
        first + 3_600_000_000 < i64::MAX,
        retry < cutoff,
    ensures match out { ReplayWindow::Open(end) => retry < end, _ => false },
)]
fn admitted_retry_precedes_marker_expiration(first: i64, retry: i64, cutoff: i64) -> ReplayWindow {
    replay_window(first, cutoff)
}
