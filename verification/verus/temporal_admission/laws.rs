use crate::owned_range_order::ordered_range_group;
use crate::production::{applies, position_visible, recency_step};
use vstd::prelude::*;

// LAW RTDB-TIME-01.
#[verus_spec(out =>
    ensures out,
)]
fn an_erase_in_the_admitted_history_applies_beyond_the_query_time(erase_time: i64, query_upper: i64) -> bool {
    applies(false, true, erase_time, Some(query_upper))
}

// LAW RTDB-TIME-02.
#[verus_spec(out =>
    requires query_upper <= system_from,
    ensures !out,
)]
fn an_ordinary_future_event_is_excluded(system_from: i64, query_upper: i64) -> bool {
    applies(false, false, system_from, Some(query_upper))
}

// LAW RTDB-TIME-03.
#[verus_spec(out =>
    ensures !out,
)]
fn no_older_event_survives_a_preceding_erase(erase: bool, time: i64, upper: Option<i64>) -> bool {
    applies(true, erase, time, upper)
}

// LAW RTDB-TIME-04.
#[verus_spec(out =>
    requires earlier_end <= start,
    ensures out.1 ==> earlier_end <= out.0,
)]
fn an_early_recency_stop_bounds_all_preceding_valid_intervals(
    previous: i64, start: i64, end: i64, system_end: i64, earlier_end: i64,
) -> (i64, bool) {
    recency_step(previous, start, end, system_end)
}

// LAW RTDB-TIME-05.
#[verus_spec(out =>
    requires valid_point < end, system_point < system_end,
    ensures valid_point < out.0 || system_point < out.0,
)]
fn recency_cannot_prune_a_live_point(
    previous: i64, start: i64, end: i64, system_end: i64, valid_point: i64, system_point: i64,
) -> (i64, bool) {
    recency_step(previous, start, end, system_end)
}

#[verus_spec(out =>
    requires selected < later,
    ensures out == (Some(true), Some(false), Some(true)),
)]
fn exact_cut_excludes_a_later_position_and_applies_erasure(selected: i64, later: i64) -> (Option<bool>, Option<bool>, Option<bool>) {
    (position_visible(Some(selected), selected, false),
     position_visible(Some(later), selected, false),
     position_visible(Some(later), selected, true))
}

#[verus_spec(out =>
    requires original < group, group <= applied, applied < beyond,
    ensures out == (true, false, false),
)]
fn a_tied_history_group_must_advance_within_the_covered_prefix(
    original: i64, group: i64, applied: i64, history: i64, beyond: i64,
) -> (bool, bool, bool) {
    (
        ordered_range_group((original, history), (group, history), (applied, history)),
        ordered_range_group((original, history), (original, history), (applied, history)),
        ordered_range_group((original, history), (beyond, history), (applied, history)),
    )
}
