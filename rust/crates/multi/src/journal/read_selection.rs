#![allow(unexpected_cfgs)]

#[cfg(not(verus_keep_ghost))]
use ordered_logkeeper::ordered::{Cut, Hold, RequestId};
#[cfg(not(verus_keep_ghost))]
use std::time::Instant;
#[cfg(verus_keep_ghost)]
use crate::{Cut, Hold, Instant, RequestId};
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (actual == requested),
))]
pub(super) fn barrier_matches_invocation(actual: RequestId, requested: RequestId) -> bool {
    actual.nonce == requested.nonce && actual.expires_at == requested.expires_at
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == if next_lsn as int - 1 <= i64::MAX {
        Some((next_lsn as int - 1) as i64)
    } else {
        None
    },
))]
pub(super) fn barrier_offset(next_lsn: u64) -> Option<i64> {
    if next_lsn == 0 {
        return Some(-1);
    }
    let offset = next_lsn - 1;
    if offset > i64::MAX as u64 { None } else { Some(offset as i64) }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (expires_at <= position),
))]
pub(super) fn read_release_settled(position: u64, expires_at: u64) -> bool {
    position >= expires_at
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == hold.through,
))]
pub(super) fn barrier_cut(hold: Hold) -> Cut {
    hold.through
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (now < deadline),
))]
pub(super) fn read_lease_admits(now: Instant, deadline: Instant) -> bool {
    now < deadline
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installed_read_selection_keeps_the_invocation_horizon_and_exact_offset() {
        let requested = RequestId { nonce: uuid::Uuid::from_u128(1), expires_at: 10 };
        assert!(barrier_matches_invocation(requested, requested));
        assert!(!barrier_matches_invocation(RequestId { expires_at: 9, ..requested }, requested));
        assert!(!barrier_matches_invocation(RequestId { nonce: uuid::Uuid::from_u128(2), ..requested }, requested));
        assert!(!read_release_settled(9, requested.expires_at));
        assert!(read_release_settled(10, requested.expires_at));
        let now = Instant::now();
        assert!(!read_lease_admits(now, now));
        assert_eq!(barrier_offset(0), Some(-1));
        assert_eq!(barrier_offset(1), Some(0));
        assert_eq!(barrier_offset(i64::MAX as u64 + 1), Some(i64::MAX));
        assert_eq!(barrier_offset(i64::MAX as u64 + 2), None);
    }
}
