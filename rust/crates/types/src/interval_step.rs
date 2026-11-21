#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn expected_intersection(left: (i64, i64), right: (i64, i64)) -> Option<(i64, i64)> {
        let lower = if left.0 >= right.0 { left.0 } else { right.0 };
        let upper = if left.1 <= right.1 { left.1 } else { right.1 };
        if lower < upper { Some((lower, upper)) } else { None }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == expected_intersection(left, right),
))]
pub(crate) fn intersect(left: (i64, i64), right: (i64, i64)) -> Option<(i64, i64)> {
    let lower = if left.0 > right.0 { left.0 } else { right.0 };
    let upper = if left.1 < right.1 { left.1 } else { right.1 };
    if upper > lower { Some((lower, upper)) } else { None }
}
