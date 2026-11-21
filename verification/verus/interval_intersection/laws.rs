use crate::production::{admit_fragment, intersect};
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

// LAW RTDB-TIME-10.
#[verus_spec(out =>
    requires contains(requested_valid, valid_point), contains(requested_system, system_point),
    ensures
        (out is Some && contains(out->Some_0, valid_point) && contains(system, system_point))
        == (contains(valid, valid_point) && contains(system, system_point)),
)]
fn admission_preserves_the_answer_inside_the_query(
    valid: (i64, i64), system: (i64, i64),
    requested_valid: (i64, i64), requested_system: (i64, i64), clamp: bool,
    valid_point: i64, system_point: i64,
) -> Option<(i64, i64)> {
    admit_fragment(valid, system, requested_valid, requested_system, clamp)
}

// LAW RTDB-TIME-11.
#[verus_spec(out =>
    requires system.1 <= requested_system.0,
    ensures out is None,
)]
fn a_disjoint_system_span_cannot_emit_a_fragment(
    valid: (i64, i64), system: (i64, i64),
    requested_valid: (i64, i64), requested_system: (i64, i64), clamp: bool,
) -> Option<(i64, i64)> {
    admit_fragment(valid, system, requested_valid, requested_system, clamp)
}
