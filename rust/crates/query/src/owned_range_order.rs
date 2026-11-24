#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (previous.0 < group.0 && group.0 <= head.0
        && previous.1 <= group.1 && group.1 <= head.1
        && (group.0 == head.0 ==> group.1 == head.1)),
))]
pub(crate) fn ordered_range_group(previous: (i64, i64), group: (i64, i64), head: (i64, i64)) -> bool {
    previous.0 < group.0 && group.0 <= head.0 && previous.1 <= group.1 && group.1 <= head.1
        && (group.0 != head.0 || group.1 == head.1)
}
