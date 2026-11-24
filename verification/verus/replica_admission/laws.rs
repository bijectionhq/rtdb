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

use crate::production::TermFence;
use crate::promotion::{initial_term_sequence, replica_read_is_empty, should_start_claim};
use crate::recovery_promotion::may_promote;
use crate::term_types::{Term, TermSequence};

verus! {

pub open spec fn fences_superseded(highest: int, arriving: int) -> bool {
    arriving < highest
}

pub open spec fn highest_after_admission(highest: int, arriving: int) -> int {
    if arriving > highest { arriving } else { highest }
}

pub open spec fn fenced_promotion(
    record_count: nat,
    recognized_prefix: bool,
    may_lead: bool,
    already_leading: bool,
    holds_grant: bool,
    highest: int,
) -> Option<(int, int)> {
    if record_count == 0 && recognized_prefix && may_lead && !already_leading && holds_grant && highest < i64::MAX {
        Some((highest + 1, 0))
    } else {
        None
    }
}

}

#[verus_spec(out =>
    requires fences_superseded(old(fence).observed_highest() as int, arriving.0 as int),
    ensures
        out matches Ok(admission) && admission.verdict is Fenced,
        final(fence).observed_highest() == old(fence).observed_highest(),
        final(fence).observed_position() == old(fence).observed_position(),
)]
fn a_superseded_record_is_fenced(
    fence: &mut TermFence,
    arriving: Term,
    sequence: Option<TermSequence>,
) -> crate::Result<crate::production::Admission> {
    fence.admit(arriving, sequence)
}

#[verus_spec(out =>
    ensures match out {
        Ok(_) => final(fence).observed_highest() as int ==
            highest_after_admission(old(fence).observed_highest() as int, arriving.0 as int),
        Err(_) => final(fence).observed_highest() == old(fence).observed_highest(),
    },
)]
fn admission_preserves_the_high_water_mark(
    fence: &mut TermFence,
    arriving: Term,
    sequence: Option<TermSequence>,
) -> crate::Result<crate::production::Admission> {
    fence.admit(arriving, sequence)
}

#[verus_spec(out =>
    ensures
        (match out {
            Some((term, sequence)) => Some((term.0 as int, sequence as int)),
            None => None,
        }) == fenced_promotion(record_count as nat, recognized_prefix, may_lead, already_leading, holds_grant,
                               fence.observed_highest() as int),
)]
fn fenced_promotion_after_empty_read(
    record_count: usize,
    recognized_prefix: bool,
    may_lead: bool,
    already_leading: bool,
    holds_grant: bool,
    fence: &TermFence,
) -> Option<(Term, i64)> {
    if !replica_read_is_empty(record_count) || !may_promote(recognized_prefix, may_lead, already_leading, holds_grant) {
        return None;
    }
    match fence.claim_term() {
        Ok(term) => Some((term, initial_term_sequence(true))),
        Err(_) => None,
    }
}

#[verus_spec(out =>
    ensures !out,
)]
fn fenced_driver_never_starts_a_claim(may_lead: bool, already_leading: bool, claim_pending: bool) -> bool {
    should_start_claim(true, may_lead, already_leading, claim_pending)
}
