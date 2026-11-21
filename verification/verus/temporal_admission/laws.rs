use crate::production::applies;
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
