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

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (system_start < event_system && (erase || event_system < system_end)),
))]
pub(crate) fn can_shadow(system_start: i64, system_end: i64, event_system: i64, erase: bool) -> bool {
    system_start < event_system && (erase || event_system < system_end)
}

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn has_point(rectangle: Seq<i64>, valid: i64, system: i64) -> bool {
        rectangle[0] <= valid && valid < rectangle[1]
        && rectangle[2] <= system && system < rectangle[3]
    }

    pub open spec fn part_has_point(piece: Option<[i64; 4]>, valid: i64, system: i64) -> bool {
        match piece {
            Some(rectangle) => has_point(rectangle@, valid, system),
            None => false,
        }
    }

    pub open spec fn cut_has_point(parts: Seq<Option<[i64; 4]>>, valid: i64, system: i64) -> bool {
        part_has_point(parts[0], valid, system)
        || part_has_point(parts[1], valid, system)
        || part_has_point(parts[2], valid, system)
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        forall |valid: i64, system: i64| #[trigger] cut_has_point(out@, valid, system) ==
            (has_point(rectangle@, valid, system)
             && !(rectangle@[2] < event_system
                  && (erase || (event_from <= valid && valid < event_to && event_system <= system)))),
))]
pub(crate) fn cut_rectangle(
    rectangle: [i64; 4],
    event_from: i64,
    event_to: i64,
    event_system: i64,
    erase: bool,
) -> [Option<[i64; 4]>; 3] {
    let [valid_from, valid_to, system_from, system_to] = rectangle;
    if !can_shadow(system_from, system_to, event_system, erase) {
        return [Some(rectangle), None, None];
    }
    if erase {
        return [None, None, None];
    }
    let cut_from = if event_from > valid_from { event_from } else { valid_from };
    let cut_to = if event_to < valid_to { event_to } else { valid_to };
    if cut_from >= cut_to {
        return [Some(rectangle), None, None];
    }
    let left = if valid_from < cut_from { Some([valid_from, cut_from, system_from, system_to]) } else { None };
    let right = if cut_to < valid_to { Some([cut_to, valid_to, system_from, system_to]) } else { None };
    [left, right, Some([cut_from, cut_to, system_from, event_system])]
}
