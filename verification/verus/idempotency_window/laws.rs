use crate::production::expiration;
use vstd::prelude::*;

#[verus_spec(out =>
    requires admitted + 3_600_000_000 < i64::MAX,
    ensures out.is_some(), out.unwrap() > admitted, out.unwrap() < i64::MAX,
)]
fn the_window_has_a_strictly_later_finite_endpoint(admitted: i64) -> Option<i64> {
    expiration(admitted)
}
