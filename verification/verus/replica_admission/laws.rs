use crate::production::{SequenceDecision, classify_sequence, is_stale_term};
use vstd::prelude::*;

// LAW RTDB-TX-01.
#[verus_spec(out =>
    requires arriving < adopted,
    ensures out,
)]
fn a_displaced_term_is_always_fenced(adopted: i64, arriving: i64) -> bool {
    is_stale_term(adopted, arriving)
}

// LAW RTDB-TX-02.
#[verus_spec(out =>
    requires received < next,
    ensures out is Discard,
)]
fn a_duplicate_cannot_apply(next: i64, received: i64) -> SequenceDecision {
    classify_sequence(next, Some(received))
}

// LAW RTDB-TX-03.
#[verus_spec(out =>
    requires next < received,
    ensures out is Void,
)]
fn a_missing_predecessor_voids_the_term(next: i64, received: i64) -> SequenceDecision {
    classify_sequence(next, Some(received))
}
