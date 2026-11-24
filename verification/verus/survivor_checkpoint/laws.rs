use crate::member_production::{declares_written_member, fresh_survivor_allocation};
use crate::production::{keep_inherited_event, keep_local_event, write_survivor_trie};
use vstd::prelude::*;

verus! {

spec fn before_marker(position: (i64, u64), marker: (i64, u64)) -> bool {
    let (tx, row) = position;
    let (marker_tx, marker_row) = marker;
    tx < marker_tx || (tx == marker_tx && row < marker_row)
}

spec fn survivor_events(
    in_exact_head: bool, origin_selected: bool, position: (i64, u64), marker: Option<(i64, u64)>,
) -> bool {
    in_exact_head && !origin_selected && match marker {
        Some(before) => !before_marker(position, before),
        None => true,
    }
}

fn selects_exactly_the_surviving_events(
    in_exact_head: bool, origin_selected: bool, tx: i64, row: u64, marker: Option<(i64, u64)>,
) {
    let kept = in_exact_head && keep_inherited_event(origin_selected) && keep_local_event(tx, row, marker);
    assert(kept == survivor_events(in_exact_head, origin_selected, (tx, row), marker));
}

spec fn survivor_members(
    allocation: u128, written_under: u128, bytes_at_written_path: bool, exact_extent: bool,
) -> bool {
    written_under == allocation && bytes_at_written_path && exact_extent
}

fn declares_exactly_written_members(
    candidate: u128, previous: u128, observed_allocation: u128, same_path: bool,
    observed_size: u64, written_size: u64,
) {
    let fresh = fresh_survivor_allocation(candidate, previous);
    let declared = fresh && declares_written_member(
        observed_allocation == candidate && same_path, observed_size, written_size,
    );
    assert(declared == (candidate != previous && survivor_members(
        candidate, observed_allocation, same_path, observed_size == written_size,
    )));
}

fn writes_a_trie_exactly_for_nonempty_survivors(rows: u32) {
    let writes = write_survivor_trie(rows);
    assert(writes == (rows > 0));
}

}
