use crate::production::{accepts_clock_lineage, clock_slot_admits, next_grid_reading, owned_deadline_is_live, owned_point_matches, admits_client_time, admits_historical_time, advances, automatic_time, historical_time};
use vstd::prelude::*;

verus! {

// LAW T4-HISTORY-FLOOR.
pub fn sql_history_uses_the_logged_time_or_preceding_floor(previous: i64, logged: i64) {
    let at = historical_time(Some(previous), logged);
    assert(previous <= at && logged <= at);
    assert(at == previous || at == logged);
    if logged <= previous {
        assert(at == previous);
    }
}

// LAW T4-NO-LEAD.
pub fn a_tied_explicit_history_time_still_has_the_original_append_bound(previous: i64, requested: i64, logged: i64) {
    let admitted = admits_historical_time(Some(previous), requested, logged);
    assert(admitted == (previous <= requested && requested <= logged));
    if previous == requested && requested <= logged {
        assert(admitted);
    }
}

// LAW TIME-SYSTEM-FIRST.
pub fn first_uses_the_clock(candidate: i64) {
    let assigned = automatic_time(None, candidate);
    assert(assigned == Some(candidate));
}

// LAW TIME-SYSTEM-CLOCK.
pub fn an_advancing_clock_is_used_unchanged(previous: i64, candidate: i64)
    requires previous < candidate,
{
    let assigned = automatic_time(Some(previous), candidate);
    assert(assigned == Some(candidate));
}

// LAW TIME-SYSTEM-MONOTONIC.
pub fn every_automatic_assignment_advances(previous: i64, candidate: i64) {
    let assigned = automatic_time(Some(previous), candidate);
    if let Some(time) = assigned {
        assert(previous < time);
    }
}

// LAW TIME-SYSTEM-MINIMAL.
pub fn a_stalled_clock_advances_one_tick(previous: i64, candidate: i64)
    requires candidate <= previous, previous < i64::MAX,
{
    let assigned = automatic_time(Some(previous), candidate);
    assert(assigned.is_some() && assigned.unwrap() as int == previous as int + 1);
}

// LAW TIME-SYSTEM-EXHAUSTION.
pub fn the_maximum_cannot_be_advanced(candidate: i64) {
    let assigned = automatic_time(Some(i64::MAX), candidate);
    assert(assigned.is_none());
}

// LAW TIME-SYSTEM-EXPLICIT-REFUSAL.
pub fn nonadvancing_explicit_times_are_refused(previous: i64, candidate: i64)
    requires candidate <= previous,
{
    let admissible = advances(Some(previous), candidate);
    assert(!admissible);
}

// LAW TIME-SYSTEM-BOUNDED-BY-LOG.
pub fn an_admitted_client_time_never_leads_the_log(previous: Option<i64>, candidate: i64, log_time: i64) {
    let admitted = admits_client_time(previous, candidate, log_time);
    if admitted {
        assert(candidate <= log_time);
    }
}

// LAW TIME-SYSTEM-NO-EXHAUSTION.
pub fn an_admitted_client_time_leaves_a_following_chronon(previous: Option<i64>, candidate: i64, log_time: i64, next_log: i64)
    requires log_time < i64::MAX,
{
    let admitted = admits_client_time(previous, candidate, log_time);
    if admitted {
        let next = automatic_time(Some(candidate), next_log);
        assert(next.is_some());
    }
}

pub fn a_proposal_slot_checks_the_observation_independently_of_append() {
    let accepted = clock_slot_admits(3, 5, 1_000_000, 1_000_007, 1_000_000);
    let before_window = clock_slot_admits(3, 5, 1_000_000, 1_000_007, 999_999);
    let at_end = clock_slot_admits(3, 5, 1_000_000, 1_000_007, 2_000_000);
    assert(accepted && !before_window && !at_end);
}

pub fn an_observation_cannot_authorize_a_history_time_ahead_of_append() {
    let accepted = clock_slot_admits(3, 5, 1_000_000, 999_999, 1_000_000);
    assert(!accepted);
}

// V3. A receiver clock and the original deadline suffice for the temporal verdict.
pub fn equal_receiver_observations_keep_the_same_deadline_verdict(
    left: i64, right: i64, deadline: i64,
)
    requires left == right,
{
    assert(owned_deadline_is_live(left, deadline) == owned_deadline_is_live(right, deadline));
    assert(!owned_deadline_is_live(deadline, deadline));
}

// V12. Two different original events cannot spend the same point witness.
pub fn a_tied_history_coordinate_does_not_match_another_assertion(
    before: i64, after: i64, ordinal: u64, history: i64,
)
    requires before < after,
{
    assert(!owned_point_matches(Some((before, ordinal, history)), Some((after, ordinal, history))));
    assert(!owned_point_matches(Some((before, 0, history)), Some((before, 1, history))));
}

// The paired check assumes that both paths reached their original prepared cut
// and admitted the same public receiver observation at the advertised reading.
pub fn the_prepared_clock_verdict_does_not_depend_on_extra_predecessors(
    through: i64, left_predecessor: i64, right_predecessor: i64, reading: i64,
    left_append: i64, right_append: i64, observed: i64,
)
    requires through <= left_predecessor, through <= right_predecessor,
        reading <= left_append, reading <= right_append,
{
    assert(clock_slot_admits(through, left_predecessor, reading, left_append, observed)
        == clock_slot_admits(through, right_predecessor, reading, right_append, observed));
}

pub fn an_advertised_reading_is_on_the_receiver_grid(observed: i64) {
    if let Some(reading) = next_grid_reading(observed) {
        assert(clock_slot_admits(1, 1, reading, reading, reading));
    }
}

pub fn the_offered_reading_requires_the_original_clock_lineage() {
    assert(!accepts_clock_lineage(false, 3, 2, 8, 5, 1_100_000, 200_000));
    assert(!accepts_clock_lineage(true, 1, 2, 8, 5, 1_100_000, 200_000));
    assert(!accepts_clock_lineage(true, 3, 2, 4, 5, 1_100_000, 200_000));
    assert(!accepts_clock_lineage(true, 2, 2, 5, 5, 1_100_000, 200_000));
    assert(accepts_clock_lineage(true, 3, 2, 8, 5, 1_100_000, 200_000));
}

}
