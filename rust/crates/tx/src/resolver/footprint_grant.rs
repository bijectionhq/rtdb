//! Pure Verus-verified sans-IO decision core for RTDB's SS8 durable footprint
//! grant, work-keyed OCC debit, strict work-key age rotation, lease-loss
//! progress transfer, and all-writer (Flight and SQL) fence.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub const DEFAULT_OPTIMISTIC_CONFLICT_LIMIT: u8 = 3;

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub enum WriterKind {
    Flight,
    Sql,
}

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub struct WaiterEntry {
    pub work_key_id: u64,
    pub work_age: u64,
}

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub struct GrantHolder {
    pub work_key_id: u64,
    pub work_age: u64,
    pub holder_attempt: u64,
    pub reopened_cut: i64,
    pub progress_step: u64,
    pub footprint_len: u64,
    pub active: bool,
}

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub enum TransferVerdict {
    Transferred(GrantHolder),
    RejectNotActive,
    RejectWrongWorkKey,
    RejectNonAdvancingAttempt,
    RejectRegressingProgress,
}

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, PartialEq, Eq))]
pub enum GrantProposalVerdict {
    AdmitAndRelease,
    FenceInactiveGrant,
    FenceWrongWorkKey,
    FenceStaleAttempt,
}

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn strictly_older(first: WaiterEntry, second: WaiterEntry) -> bool {
        first.work_age < second.work_age
            || (first.work_age == second.work_age && first.work_key_id < second.work_key_id)
    }

    pub open spec fn age_leq(first: WaiterEntry, second: WaiterEntry) -> bool {
        first.work_age < second.work_age
            || (first.work_age == second.work_age && first.work_key_id <= second.work_key_id)
    }
}

/// Consume one definite OCC conflict credit for a work key. Zero is sticky
/// across restarts until the work item retires.
#[cfg_attr(verus_keep_ghost, verus_spec(next => ensures
    remaining == 0 ==> next == 0,
    remaining > 0 ==> next as int == remaining as int - 1,
))]
pub fn debit_occ_conflict(remaining: u8) -> u8 {
    if remaining == 0 {
        0
    } else {
        remaining - 1
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(needs => ensures needs == (remaining == 0)))]
pub fn needs_durable_grant(remaining: u8) -> bool {
    remaining == 0
}

/// All-writer fence covering both Flight and SQL writers.
/// While a durable grant is active on an overlapping footprint, only the
/// matching active holder attempt may apply a write.
#[cfg_attr(verus_keep_ghost, verus_spec(allowed => ensures
    allowed == (!overlapping || !grant_active || matches_active_holder),
))]
pub fn writer_may_apply(
    writer: WriterKind,
    overlapping: bool,
    grant_active: bool,
    matches_active_holder: bool,
) -> bool {
    match writer {
        WriterKind::Flight => !overlapping || !grant_active || matches_active_holder,
        WriterKind::Sql => !overlapping || !grant_active || matches_active_holder,
    }
}

/// Strict work-key age rotation comparator: older work age always wins,
/// with deterministic tie-breaking by work_key_id.
#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
    selected == if age_leq(first, second) { first } else { second },
    age_leq(selected, first),
    age_leq(selected, second),
))]
pub fn select_older_waiter(first: WaiterEntry, second: WaiterEntry) -> WaiterEntry {
    if first.work_age < second.work_age
        || (first.work_age == second.work_age && first.work_key_id <= second.work_key_id)
    {
        first
    } else {
        second
    }
}

/// Choose the next durable grant holder from eligible waiters strictly by
/// work-key age.
#[cfg_attr(verus_keep_ghost, verus_spec(selected => ensures
    waiters@.len() == 0 ==> selected is None,
    waiters@.len() > 0 ==> (selected matches Some(winner)
        && (exists|k: int| 0 <= k < waiters@.len() && #[trigger] waiters@[k] == winner)
        && (forall|i: int| 0 <= i < waiters@.len() ==> age_leq(winner, #[trigger] waiters@[i]))),
))]
pub fn select_next_holder(waiters: &[WaiterEntry]) -> Option<WaiterEntry> {
    if waiters.is_empty() {
        return None;
    }
    let mut best = waiters[0];
    let mut index: usize = 1;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            1 <= index <= waiters@.len(),
            exists|k: int| 0 <= k < index && #[trigger] waiters@[k] == best,
            forall|i: int| 0 <= i < index ==> age_leq(best, #[trigger] waiters@[i]),
        decreases waiters@.len() - index,
    ))]
    while index < waiters.len() {
        let candidate = waiters[index];
        let next = select_older_waiter(best, candidate);
        #[cfg(verus_keep_ghost)]
        proof! {
            if next == candidate {
                assert(waiters@[index as int] == candidate);
            }
            assert forall|i: int| 0 <= i < index + 1 implies age_leq(next, #[trigger] waiters@[i]) by {
                if i < index {
                    assert(age_leq(best, waiters@[i]));
                    assert(age_leq(next, best));
                } else {
                    assert(waiters@[i] == candidate);
                    assert(age_leq(next, candidate));
                }
            }
        }
        best = next;
        index += 1;
    }
    Some(best)
}

/// Durable grant lease-loss transfer: a held grant never lapses on turnover.
/// On holder lease loss, the durable grant transfers with its recorded
/// progress, original work age, reopened cut, and unioned footprint to the
/// replacement attempt for the same work key while advancing `holder_attempt`
/// so delayed proposals from the prior attempt are fenced.
#[cfg_attr(verus_keep_ghost, verus_spec(verdict => ensures
    match verdict {
        TransferVerdict::Transferred(next) => {
            &&& grant.active
            &&& grant.work_key_id == work_key_id
            &&& replacement_attempt > grant.holder_attempt
            &&& progress_step >= grant.progress_step
            &&& expanded_footprint_len >= grant.footprint_len
            &&& next.active
            &&& next.work_key_id == grant.work_key_id
            &&& next.work_age == grant.work_age
            &&& next.holder_attempt == replacement_attempt
            &&& next.reopened_cut == grant.reopened_cut
            &&& next.progress_step == progress_step
            &&& next.footprint_len == expanded_footprint_len
        }
        TransferVerdict::RejectNotActive => !grant.active,
        TransferVerdict::RejectWrongWorkKey => grant.active && grant.work_key_id != work_key_id,
        TransferVerdict::RejectNonAdvancingAttempt =>
            grant.active && grant.work_key_id == work_key_id && replacement_attempt <= grant.holder_attempt,
        TransferVerdict::RejectRegressingProgress =>
            grant.active && grant.work_key_id == work_key_id && replacement_attempt > grant.holder_attempt
                && (progress_step < grant.progress_step || expanded_footprint_len < grant.footprint_len),
    },
))]
pub fn transfer_on_lease_loss(
    grant: GrantHolder,
    work_key_id: u64,
    replacement_attempt: u64,
    progress_step: u64,
    expanded_footprint_len: u64,
) -> TransferVerdict {
    if !grant.active {
        return TransferVerdict::RejectNotActive;
    }
    if grant.work_key_id != work_key_id {
        return TransferVerdict::RejectWrongWorkKey;
    }
    if replacement_attempt <= grant.holder_attempt {
        return TransferVerdict::RejectNonAdvancingAttempt;
    }
    if progress_step < grant.progress_step || expanded_footprint_len < grant.footprint_len {
        return TransferVerdict::RejectRegressingProgress;
    }
    TransferVerdict::Transferred(GrantHolder {
        work_key_id: grant.work_key_id,
        work_age: grant.work_age,
        holder_attempt: replacement_attempt,
        reopened_cut: grant.reopened_cut,
        progress_step,
        footprint_len: expanded_footprint_len,
        active: true,
    })
}

/// Admit a proposal carrying a durable grant identity. Only the active holder's
/// current `holder_attempt` is admitted; stale attempts fenced by a lease-loss
/// transfer are rejected.
#[cfg_attr(verus_keep_ghost, verus_spec(verdict => ensures
    verdict == if !grant.active {
        GrantProposalVerdict::FenceInactiveGrant
    } else if grant.work_key_id != work_key_id {
        GrantProposalVerdict::FenceWrongWorkKey
    } else if grant.holder_attempt != carried_attempt {
        GrantProposalVerdict::FenceStaleAttempt
    } else {
        GrantProposalVerdict::AdmitAndRelease
    },
))]
pub fn admit_grant_proposal(
    grant: GrantHolder,
    work_key_id: u64,
    carried_attempt: u64,
) -> GrantProposalVerdict {
    if !grant.active {
        GrantProposalVerdict::FenceInactiveGrant
    } else if grant.work_key_id != work_key_id {
        GrantProposalVerdict::FenceWrongWorkKey
    } else if grant.holder_attempt != carried_attempt {
        GrantProposalVerdict::FenceStaleAttempt
    } else {
        GrantProposalVerdict::AdmitAndRelease
    }
}

/// Monotone durable footprint union bounded by the finite declared schema
/// footprint capacity.
#[cfg_attr(verus_keep_ghost, verus_spec(next => ensures
    (next is Some) == (held_len <= declared_cap && held_len as int + added_new as int <= declared_cap as int),
    next matches Some(total) ==> {
        &&& total as int == held_len as int + added_new as int
        &&& held_len <= total <= declared_cap
        &&& (added_new > 0 ==> held_len < total)
    },
))]
pub fn union_footprint_len(held_len: u64, added_new: u64, declared_cap: u64) -> Option<u64> {
    if held_len > declared_cap {
        return None;
    }
    let remaining_cap = declared_cap - held_len;
    if added_new > remaining_cap {
        return None;
    }
    Some(held_len + added_new)
}
