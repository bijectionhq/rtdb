use crate::production::intersect;
use vstd::prelude::*;

verus! {
    spec fn contains(range: (i64, i64), point: i64) -> bool {
        range.0 <= point && point < range.1
    }
}

// LAW RTDB-TIME-06.
#[verus_spec(out =>
    ensures (out is Some && contains(out->Some_0, point)) == (contains(left, point) && contains(right, point)),
)]
fn intersection_preserves_point_membership(left: (i64, i64), right: (i64, i64), point: i64) -> Option<(i64, i64)> {
    intersect(left, right)
}

// LAW RTDB-TIME-07.
#[verus_spec(out =>
    requires left.1 <= right.0,
    ensures out is None,
)]
fn adjacent_validity_ranges_never_intersect(left: (i64, i64), right: (i64, i64)) -> Option<(i64, i64)> {
    intersect(left, right)
}
