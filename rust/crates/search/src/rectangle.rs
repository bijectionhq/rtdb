#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Some) == (left@[1] == right@[0] && left@[2] == right@[2] && left@[3] == right@[3]),
        out is Some ==>
            (out->Some_0)@[0] == left@[0] &&
            (out->Some_0)@[1] == right@[1] &&
            (out->Some_0)@[2] == left@[2] &&
            (out->Some_0)@[3] == left@[3],
))]
pub(crate) fn merge_adjacent_valid_time(left: [i64; 4], right: [i64; 4]) -> Option<[i64; 4]> {
    if left[1] == right[0] && left[2] == right[2] && left[3] == right[3] {
        Some([left[0], right[1], left[2], left[3]])
    } else {
        None
    }
}
