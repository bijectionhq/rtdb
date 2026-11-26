use crate::production::{acceptance_relative_time, accepts_clock_lineage, clock_slot_admits, next_grid_reading, owned_deadline_is_live, owned_point_matches, original_funded_seat, advances, automatic_time, admits_supplied_time, external_time};
use vstd::prelude::*;

verus! {

// LAW TIME-SYSTEM-STRICT. A read basis names a transaction by its system time, so a tie never advances.
pub fn a_tied_time_does_not_advance(previous: i64) {
    let tied = advances(Some(previous), previous);
    assert(!tied);
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

// LAW TIME-SUPPLIED-STRICT. A supplied time that does not follow its predecessor's is refused.
pub fn a_supplied_time_must_follow_its_predecessor(previous: i64, candidate: i64, now: i64)
    requires candidate <= previous,
{
    let admitted = admits_supplied_time(Some(previous), candidate, now);
    assert(!admitted);
}

// LAW TIME-SUPPLIED-FUTURE. A supplied time cannot lead now.
pub fn a_supplied_time_cannot_lead_now(previous: Option<i64>, candidate: i64, now: i64)
    requires now < candidate,
{
    let admitted = admits_supplied_time(previous, candidate, now);
    assert(!admitted);
}

// LAW TIME-SUPPLIED-BACKFILL. A backfill time that advances and does not lead now is used unchanged.
pub fn a_backfill_time_is_used_unchanged(previous: i64, candidate: i64, now: i64)
    requires previous < candidate, candidate <= now,
{
    let admitted = admits_supplied_time(Some(previous), candidate, now);
    let assigned = external_time(Some(previous), Some(candidate), now);
    assert(admitted && assigned == Some(candidate));
}

// LAW TIME-EXTERNAL-MONOTONIC. An external source's time is stepped rather than refused, and always advances.
pub fn every_external_time_advances(previous: i64, supplied: Option<i64>, now: i64) {
    let assigned = external_time(Some(previous), supplied, now);
    if let Some(time) = assigned {
        assert(previous < time);
    }
}

// LAW TIME-EXTERNAL-TIE. A tied external time advances one tick.
pub fn a_tied_external_time_steps_one_tick(previous: i64, now: i64)
    requires previous < now,
{
    let assigned = external_time(Some(previous), Some(previous), now);
    assert(assigned == Some((previous + 1) as i64));
}

// LAW TIME-EXTERNAL-FUTURE. A future external time is brought back to now.
pub fn a_future_external_time_is_brought_back_to_now(previous: i64, candidate: i64, now: i64)
    requires previous < now, now < candidate,
{
    let assigned = external_time(Some(previous), Some(candidate), now);
    assert(assigned == Some(now));
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
    let left_live = owned_deadline_is_live(left, deadline);
    let right_live = owned_deadline_is_live(right, deadline);
    let at_deadline = owned_deadline_is_live(deadline, deadline);
    assert(left_live == right_live);
    assert(!at_deadline);
}

// V4. Hidden appends cannot retime explicitly acceptance-relative effects.
pub fn recorded_receiver_reading_materializes_relative_terms(observed: i64, left_append: i64, right_append: i64) {
    let left = acceptance_relative_time(None, Some(observed), left_append);
    let right = acceptance_relative_time(None, Some(observed), right_append);
    assert(left == observed && left == right);
}

pub fn the_original_advertised_reading_materializes_prepared_terms(
    reading: i64, observed: i64, appended: i64,
) {
    let selected = acceptance_relative_time(Some(reading), Some(observed), appended);
    assert(selected == reading);
}

// V12. Two different original events cannot spend the same point witness.
pub fn a_tied_history_coordinate_does_not_match_another_assertion(
    before: i64, after: i64, ordinal: u64, history: i64,
)
    requires before < after,
{
    let other_position = owned_point_matches(Some((before, ordinal, history)), Some((after, ordinal, history)));
    let other_ordinal = owned_point_matches(Some((before, 0, history)), Some((before, 1, history)));
    assert(!other_position);
    assert(!other_ordinal);
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
    let left = clock_slot_admits(through, left_predecessor, reading, left_append, observed);
    let right = clock_slot_admits(through, right_predecessor, reading, right_append, observed);
    assert(left == right);
}

pub fn an_advertised_reading_is_on_the_receiver_grid(observed: i64) {
    if let Some(reading) = next_grid_reading(observed) {
        let admitted = clock_slot_admits(1, 1, reading, reading, reading);
        assert(admitted);
    }
}

pub fn the_offered_reading_requires_the_original_clock_lineage() {
    let other_domain = accepts_clock_lineage(false, 3, 2, 8, 5, 1_100_000, 200_000);
    let older_epoch = accepts_clock_lineage(true, 1, 2, 8, 5, 1_100_000, 200_000);
    let earlier_frontier = accepts_clock_lineage(true, 3, 2, 4, 5, 1_100_000, 200_000);
    let stale_lineage = accepts_clock_lineage(true, 2, 2, 5, 5, 1_100_000, 200_000);
    let original = accepts_clock_lineage(true, 3, 2, 8, 5, 1_100_000, 200_000);
    assert(!other_domain);
    assert(!older_epoch);
    assert(!earlier_frontier);
    assert(!stale_lineage);
    assert(original);
}

pub fn funded_seat_and_receipt_name_one_advertised_reading() {
    let original = original_funded_seat(1_000_000, 1_000_000, 2_000_000, true, true);
    let other_reading = original_funded_seat(1_000_000, 1_000_001, 2_000_000, true, true);
    let foreign_booking = original_funded_seat(1_000_000, 1_000_000, 2_000_000, true, false);
    let unbooked = original_funded_seat(1_000_000, 1_000_000, 2_000_000, false, true);
    let other_window = original_funded_seat(1_000_000, 1_000_000, 2_000_001, true, true);
    assert(original);
    assert(!other_reading);
    assert(!foreign_booking);
    assert(!unbooked);
    assert(!other_window);
}

pub fn distinct_publications_on_the_same_grid_materialize_the_same_terms(
    left_predecessor: i64, right_predecessor: i64, left_append: i64, right_append: i64,
)
    requires 1 <= left_predecessor < right_predecessor,
        1_000_000 <= left_append, 1_000_000 <= right_append,
{
    let left = clock_slot_admits(1, left_predecessor, 1_000_000, left_append, 1_000_001);
    let right = clock_slot_admits(1, right_predecessor, 1_000_000, right_append, 1_899_999);
    assert(left && right);
    let left_term = acceptance_relative_time(Some(1_000_000), Some(1_000_001), left_append);
    let right_term = acceptance_relative_time(Some(1_000_000), Some(1_899_999), right_append);
    assert(left_term == right_term);
}


}
