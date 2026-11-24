use crate::production::{Fragment, admits_fragment, next_group};
use vstd::prelude::*;

verus! {

pub fn forward_scan_selects_the_earliest_unread_publication(positions: &[Option<i64>])
    -> (out: Option<usize>)
    ensures
        out is None <==> forall|i: int| 0 <= i < positions@.len()
            ==> (#[trigger] positions@[i]) is None,
        out matches Some(index) ==> {
            &&& index < positions@.len()
            &&& positions@[index as int] is Some
            &&& forall|i: int| 0 <= i < positions@.len() && (#[trigger] positions@[i]) is Some
                ==> positions@[index as int].unwrap() <= positions@[i].unwrap()
        },
{
    let out = next_group(positions, false);
    if let Some(index) = out {
        assert(positions@[index as int] is Some);
    }
    out
}

pub fn a_group_fragment_keeps_its_publication_and_neighbors(
    position: i64, time: i64, header: Fragment, entry: Fragment, predecessor: i64, events: i64,
) -> (admitted: bool)
    ensures
        admitted ==> entry.position == position && header.position == position,
        admitted ==> entry.history_time == time && header.history_time == time,
        admitted ==> predecessor < header.through_events
            && header.through_events == entry.through_events
            && predecessor as int + events as int == header.through_events as int,
        header.position == position && entry.position == position
            && header.history_time == time && entry.history_time == time
            && predecessor as int + events as int == header.through_events as int
            && header.through_events == entry.through_events && events > 0 ==> admitted,
{
    admits_fragment(position, time, header, entry, predecessor, events)
}

}
