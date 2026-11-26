//! Laws over RTDB's shipped `tx::resolver::footprint_grant` sans-IO core.
use vstd::prelude::*;
use crate::footprint_grant::{
    self, GrantHolder, GrantProposalVerdict, TransferVerdict, WaiterEntry, WriterKind,
    age_leq, strictly_older,
};

verus! {

/// On lease loss, a durable grant transfers with its recorded progress,
/// original work age, reopened cut, and unioned footprint to the replacement
/// attempt for the same work key rather than lapsing, and immediately fences
/// delayed proposals from the prior attempt.
pub fn durable_grant_survives_lease_loss_with_progress(
    grant: GrantHolder,
    replacement_attempt: u64,
    next_progress: u64,
    next_footprint_len: u64,
) -> (transferred: GrantHolder)
    requires
        grant.active,
        replacement_attempt > grant.holder_attempt,
        next_progress >= grant.progress_step,
        next_footprint_len >= grant.footprint_len,
    ensures
        transferred.active,
        transferred.work_key_id == grant.work_key_id,
        transferred.work_age == grant.work_age,
        transferred.reopened_cut == grant.reopened_cut,
        transferred.holder_attempt == replacement_attempt,
        transferred.progress_step == next_progress,
        transferred.footprint_len == next_footprint_len,
{
    let verdict = footprint_grant::transfer_on_lease_loss(
        grant,
        grant.work_key_id,
        replacement_attempt,
        next_progress,
        next_footprint_len,
    );
    match verdict {
        TransferVerdict::Transferred(next) => {
            let new_admit = footprint_grant::admit_grant_proposal(
                next,
                grant.work_key_id,
                replacement_attempt,
            );
            let old_admit = footprint_grant::admit_grant_proposal(
                next,
                grant.work_key_id,
                grant.holder_attempt,
            );
            assert(new_admit == GrantProposalVerdict::AdmitAndRelease);
            assert(old_admit == GrantProposalVerdict::FenceStaleAttempt);
            next
        }
        _ => {
            assert(false);
            grant
        }
    }
}

/// Strict work-key age rotation: a younger waiter can never overtake an older
/// waiter, and `select_next_holder` always chooses the oldest eligible waiter
/// in the finite queue.
pub fn strict_age_rotation_bounds_older_waiters(
    waiters: &[WaiterEntry],
    older_idx: usize,
    younger_idx: usize,
) -> (winner: WaiterEntry)
    requires
        older_idx < waiters@.len(),
        younger_idx < waiters@.len(),
        strictly_older(waiters@[older_idx as int], waiters@[younger_idx as int]),
    ensures
        age_leq(winner, waiters@[older_idx as int]),
        strictly_older(winner, waiters@[younger_idx as int]),
        winner != waiters@[younger_idx as int],
{
    let older = waiters[older_idx];
    let younger = waiters[younger_idx];
    let first_pick = footprint_grant::select_older_waiter(older, younger);
    let second_pick = footprint_grant::select_older_waiter(younger, older);
    assert(first_pick == older);
    assert(second_pick == older);
    match footprint_grant::select_next_holder(waiters) {
        Some(selected) => {
            assert(age_leq(selected, waiters@[older_idx as int]));
            assert(strictly_older(selected, waiters@[younger_idx as int]));
            selected
        }
        None => {
            assert(false);
            older
        }
    }
}

/// Every competing writer — both Flight and SQL — is excluded from mutating an
/// overlapping footprint while a durable grant is live, while non-overlapping
/// writes and the active holder attempt are admitted.
pub fn all_writers_fenced_flight_and_sql(writer: WriterKind)
    ensures true,
{
    let competing = footprint_grant::writer_may_apply(writer, true, true, false);
    let holder = footprint_grant::writer_may_apply(writer, true, true, true);
    let disjoint = footprint_grant::writer_may_apply(writer, false, true, false);
    let inactive = footprint_grant::writer_may_apply(writer, true, false, false);
    assert(!competing);
    assert(holder);
    assert(disjoint);
    assert(inactive);
}

/// Definite OCC conflict debits strictly decrement positive credits and stay
/// sticky at zero, forcing escalation to the durable footprint grant.
pub fn durable_work_keyed_debit_monotonic(remaining: u8) -> (next: u8)
    ensures
        remaining > 0 ==> next < remaining,
        remaining == 0 ==> next == 0,
{
    let next = footprint_grant::debit_occ_conflict(remaining);
    if remaining == 0 {
        let escalate = footprint_grant::needs_durable_grant(next);
        assert(escalate);
    }
    next
}

/// Strict monotone footprint union spends finite declared schema capacity and
/// never shrinks the covered footprint.
pub fn footprint_union_strict_expansion_bounded(
    held_len: u64,
    added_new: u64,
    declared_cap: u64,
) -> (next: u64)
    requires
        held_len <= declared_cap,
        added_new > 0,
        held_len as int + added_new as int <= declared_cap as int,
    ensures
        held_len < next <= declared_cap,
        (declared_cap - next) < (declared_cap - held_len),
{
    match footprint_grant::union_footprint_len(held_len, added_new, declared_cap) {
        Some(total) => total,
        None => {
            assert(false);
            held_len
        }
    }
}

} // verus!
