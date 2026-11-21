use crate::production::{can_shadow, cut_has_point, cut_rectangle, merge_adjacent_valid_time};
use vstd::prelude::*;

verus! {
    spec fn visible_after_event(
        rectangle: Seq<i64>,
        valid_at: i64,
        system_at: i64,
        event_valid_from: i64,
        event_valid_to: i64,
        event_system: i64,
        erase: bool,
    ) -> bool {
        rectangle[0] <= valid_at && valid_at < rectangle[1]
        && rectangle[2] <= system_at && system_at < rectangle[3]
        && !(rectangle[2] < event_system
             && (erase || (event_valid_from <= valid_at && valid_at < event_valid_to
                          && event_system <= system_at)))
    }
}

// LAW RTDB-FTS-01.
#[verus_spec(out =>
    ensures
        left@[2] != right@[2] ==> out is None,
)]
fn merge_preserves_event_origin(left: [i64; 4], right: [i64; 4]) -> Option<[i64; 4]> {
    merge_adjacent_valid_time(left, right)
}

// LAW RTDB-FTS-02.
#[verus_spec(out =>
    requires left@[0] < left@[1], right@[0] < right@[1],
    ensures
        match out {
            Some(joined) =>
                visible_after_event(joined@, valid_at, system_at, event_valid_from, event_valid_to, event_system, erase)
                == (visible_after_event(left@, valid_at, system_at, event_valid_from, event_valid_to, event_system, erase)
                    || visible_after_event(right@, valid_at, system_at, event_valid_from, event_valid_to, event_system, erase)),
            None => true,
        },
)]
fn merge_preserves_visibility_after_a_cut(
    left: [i64; 4],
    right: [i64; 4],
    valid_at: i64,
    system_at: i64,
    event_valid_from: i64,
    event_valid_to: i64,
    event_system: i64,
    erase: bool,
) -> Option<[i64; 4]> {
    merge_adjacent_valid_time(left, right)
}

// LAW RTDB-FTS-03.
#[verus_spec(out =>
    requires system_start < system_end, system_end <= erase_system,
    ensures out,
)]
fn an_erase_can_shadow_a_closed_historical_rectangle(
    system_start: i64,
    system_end: i64,
    erase_system: i64,
) -> bool {
    can_shadow(system_start, system_end, erase_system, true)
}

// LAW RTDB-FTS-04.
#[verus_spec(out =>
    ensures
        cut_has_point(out@, valid_at, system_at)
        == visible_after_event(rectangle@, valid_at, system_at, event_from, event_to, event_system, erase),
)]
fn a_cut_matches_the_reference_at_every_point(
    rectangle: [i64; 4],
    event_from: i64,
    event_to: i64,
    event_system: i64,
    erase: bool,
    valid_at: i64,
    system_at: i64,
) -> [Option<[i64; 4]>; 3] {
    cut_rectangle(rectangle, event_from, event_to, event_system, erase)
}
