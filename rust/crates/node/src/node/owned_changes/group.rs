//! Decisions over the next entries of the held relation journals.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    out is None ==> forall|i: int| 0 <= i < positions@.len() ==> (#[trigger] positions@[i]) is None,
    out matches Some(selected) ==> {
        &&& selected < positions@.len()
        &&& positions@[selected as int] is Some
        &&& forall|i: int| 0 <= i < positions@.len() && (#[trigger] positions@[i]) is Some ==>
            if reverse { positions@[i].unwrap() <= positions@[selected as int].unwrap() }
            else { positions@[selected as int].unwrap() <= positions@[i].unwrap() }
    },
))]
pub(super) fn next_group(positions: &[Option<i64>], reverse: bool) -> Option<usize> {
    let mut chosen: Option<usize> = None;
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= positions@.len(),
            chosen is None ==> forall|i: int| 0 <= i < index ==> (#[trigger] positions@[i]) is None,
            chosen matches Some(selected) ==> {
                &&& selected < index
                &&& positions@[selected as int] is Some
                &&& forall|i: int| 0 <= i < index && (#[trigger] positions@[i]) is Some ==>
                    if reverse { positions@[i].unwrap() <= positions@[selected as int].unwrap() }
                    else { positions@[selected as int].unwrap() <= positions@[i].unwrap() }
            },
        decreases positions@.len() - index,
    ))]
    while index < positions.len() {
        if let Some(position) = positions[index] {
            if let Some(selected) = chosen {
                if let Some(prior) = positions[selected] {
                    if (reverse && position > prior) || (!reverse && position < prior) {
                        chosen = Some(index);
                    }
                }
            } else {
                chosen = Some(index);
            }
        }
        index += 1;
    }
    chosen
}

pub struct Fragment {
    pub position: i64,
    pub history_time: i64,
    pub through_events: i64,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    out == (events > 0
        && header.position == selected_position && entry.position == selected_position
        && header.history_time == selected_time && entry.history_time == selected_time
        && header.through_events == entry.through_events
        && previous_events as int + events as int == entry.through_events as int),
))]
pub(super) fn admits_fragment(
    selected_position: i64, selected_time: i64, header: Fragment, entry: Fragment,
    previous_events: i64, events: i64,
) -> bool {
    events > 0
        && header.position == selected_position && entry.position == selected_position
        && header.history_time == selected_time && entry.history_time == selected_time
        && header.through_events == entry.through_events
        && previous_events.checked_add(events) == Some(entry.through_events)
}
