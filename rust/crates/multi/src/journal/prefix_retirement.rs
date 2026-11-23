#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (retired < writer_epoch && (grant.is_none() || grant.unwrap() != retired)),
))]
pub(crate) fn superseded(retired: u64, writer_epoch: u64, grant: Option<u64>) -> bool {
    retired < writer_epoch && match grant {
        Some(current) => current != retired,
        None => true,
    }
}

/// Every scoped credential the retired grant obtained passed its grant check
/// before the coordinator first observed the fence.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (horizon_ms >= 0 && observed_at_ms <= i64::MAX - horizon_ms
        && now_ms >= observed_at_ms + horizon_ms),
))]
pub(crate) fn leases_expired(now_ms: i64, observed_at_ms: i64, horizon_ms: i64) -> bool {
    horizon_ms >= 0 && observed_at_ms <= i64::MAX - horizon_ms && now_ms >= observed_at_ms + horizon_ms
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (superseded && exact_binding && independent_checkpoint && trimmed_to_checkpoint
        && !retained_from_grant && !preparing_from_grant && drained && qualified),
))]
pub(crate) fn may_retire(
    superseded: bool,
    exact_binding: bool,
    independent_checkpoint: bool,
    trimmed_to_checkpoint: bool,
    retained_from_grant: bool,
    preparing_from_grant: bool,
    drained: bool,
    qualified: bool,
) -> bool {
    superseded && exact_binding && independent_checkpoint && trimmed_to_checkpoint
        && !retained_from_grant && !preparing_from_grant && drained && qualified
}
