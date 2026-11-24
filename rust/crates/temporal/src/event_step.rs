#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out == (!already_erased && (erase || (match system_upper {
            Some(upper) => system_from < upper,
            None => true,
        }))),
))]
pub(crate) fn applies(already_erased: bool, erase: bool, system_from: i64, system_upper: Option<i64>) -> bool {
    !already_erased && (erase || match system_upper {
        Some(upper) => system_from < upper,
        None => true,
    })
}

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn last_possible_coordinate(valid_to: i64, system_to: i64) -> i64 {
        if valid_to < system_to { valid_to } else { system_to }
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out.0 >= previous,
        out.0 >= last_possible_coordinate(valid_to, system_to),
        out.0 == previous || out.0 == last_possible_coordinate(valid_to, system_to),
        out.1 == (out.0 >= valid_from),
))]
pub(crate) fn recency_step(previous: i64, valid_from: i64, valid_to: i64, system_to: i64) -> (i64, bool) {
    let bound = if valid_to < system_to { valid_to } else { system_to };
    let best = if previous > bound { previous } else { bound };
    (best, best >= valid_from)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (if erase { Some(true) } else {
        match original {
            Some(position) => Some(position <= through),
            None => None,
        }
    }),
))]
pub(crate) fn position_visible(original: Option<i64>, through: i64, erase: bool) -> Option<bool> {
    if erase { return Some(true); }
    match original {
        Some(event) => Some(event <= through),
        None => None,
    }
}
