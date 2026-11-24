use crate::production::{clock_slot_admits, admits_client_time, admits_historical_time, advances, automatic_time, historical_time};
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
    let accepted = clock_slot_admits(3, 5, 60, 80, 99, 80);
    let before_window = clock_slot_admits(3, 5, 60, 80, 99, 79);
    let at_end = clock_slot_admits(3, 5, 60, 80, 99, 1000080);
    assert(accepted && !before_window && !at_end);
}

pub fn an_observation_cannot_authorize_a_history_time_ahead_of_append() {
    let accepted = clock_slot_admits(3, 5, 60, 80, 79, 80);
    assert(!accepted);
}

}
