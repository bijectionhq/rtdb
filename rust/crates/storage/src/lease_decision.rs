#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// The last instant at which a reader may begin a provider request.
/// `anchor` is when its catalog was confirmed to include every replica record then published.
/// `margin` covers the clock skew between reader and collector twice over and the longest provider request.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (if i64::MIN as int <= anchor as int + lease as int <= i64::MAX as int
        && i64::MIN as int <= anchor as int + lease as int - margin as int <= i64::MAX as int {
        Some((anchor as int + lease as int - margin as int) as i64)
    } else { None }),
))]
pub fn lease_deadline(anchor: i64, lease: i64, margin: i64) -> Option<i64> {
    match anchor.checked_add(lease) {
        Some(end) => end.checked_sub(margin),
        None => None,
    }
}

/// A request may begin only strictly before the deadline.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (now < deadline),
))]
pub fn lease_admits(now: i64, deadline: i64) -> bool {
    now < deadline
}

/// The collector deletes a superseded file only after a whole lease has passed on its own clock
/// since it first observed the file as garbage.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (i64::MIN as int <= first_observed as int + lease as int <= now as int),
))]
pub fn collection_waited(first_observed: i64, lease: i64, now: i64) -> bool {
    match first_observed.checked_add(lease) {
        Some(end) => end <= now,
        None => false,
    }
}
