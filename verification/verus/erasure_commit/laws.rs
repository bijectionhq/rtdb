use crate::prefix_retirement::leases_expired;
use crate::production::*;
use vstd::prelude::*;

verus! {

pub fn rotation_evidence_names_a_strict_majority(cohort: usize, evidence: usize) {
    if strict_majority(cohort, evidence) {
        assert((cohort as int) < 2 * evidence as int);
    }
}

pub fn rotation_evidence_requires_both_slots(
    selected: u8, generations: [u64; 2], populated: [bool; 2], distinct: bool,
) {
    let evidence = mirrors_fence(selected, generations, populated, distinct);
    assert(evidence ==> populated@[0] && populated@[1]);
}

pub fn rotation_evidence_mirrors_the_selected_fence(
    selected: u8, generations: [u64; 2], populated: [bool; 2], distinct: bool,
) {
    let evidence = mirrors_fence(selected, generations, populated, distinct);
    assert(evidence ==> selected <= 1 && distinct
        && 0 < generations@[1 - selected as int]
        && generations@[1 - selected as int] as int + 1 == generations@[selected as int]);
}

pub fn rotation_evidence_names_the_clean_successor(
    checkpoint: bool, reported: u64, successor: u64, retired: u64,
)
    requires successor as int == retired as int + 1,
{
    let covered = at_clean_checkpoint(checkpoint, reported, successor);
    assert(covered ==> checkpoint && reported == successor && retired < reported);
}

pub fn a_fixed_reservation_fits_the_requested_deadline(requested: u64, reserved: u64) {
    let admitted = fixed_deadline_admits(requested, reserved);
    assert(admitted ==> 0 < reserved <= requested);
}

pub fn a_fixed_commit_keeps_the_reserved_deadline_and_inventories(
    requested: u64, reserved: u64, committed: u64, committed_at: u64,
    same_retiring_inventory: bool, same_draining_inventory: bool,
) {
    if fixed_deadline_admits(requested, reserved)
        && same_reservation(reserved, committed, same_retiring_inventory, same_draining_inventory)
        && fixed_commit_admits(requested, committed, committed_at)
    {
        assert(committed == reserved && committed_at < reserved <= requested
            && same_retiring_inventory && same_draining_inventory);
    }
}

pub fn a_commit_cannot_mix_attempts(exact_preparation: bool, exact_switch: bool) {
    let accepted = same_attempt(exact_preparation, exact_switch);
    assert(accepted ==> exact_preparation && exact_switch);
}

pub fn fixed_completion_distinguishes_on_time_and_late(due: u64, destroyed_at: u64) {
    let met = completion_status(Some(due), destroyed_at);
    assert(met == Some(destroyed_at <= due));
}

pub fn the_installed_profile_claims_no_fixed_deadline(
    operational: bool, reserved: bool, destroyed_at: u64,
) {
    if installed_admits(operational, reserved) {
        assert(operational && !reserved);
        let met = completion_status(None, destroyed_at);
        assert(met.is_none());
    }
}

pub fn an_operational_target_is_a_checked_offset(committed_at: u64) {
    let target = operational_target_at(committed_at);
    assert(target.is_some() ==> target.unwrap() as int == committed_at as int + 120_000);
    assert(target.is_none() ==> committed_at as int + 120_000 > u64::MAX as int);
}

pub fn outstanding_ordered_holds_block_destruction(outstanding: u64) {
    let drained = ordered_holds_drained(outstanding);
    assert(drained ==> outstanding == 0);
}

pub fn all_scoped_namespace_grants_expire_before_prefix_retirement(
    now: i64, fence_observed_at: i64, horizon: i64, grant_expires_at: i64,
)
    requires grant_expires_at as int <= fence_observed_at as int + horizon as int,
{
    let drained = leases_expired(now, fence_observed_at, horizon);
    assert(drained ==> grant_expires_at <= now);
}

pub fn withdrawal_waits_for_the_latest_join(now: u64, latest_join: u64) {
    let ready = withdrawal_follows_join(now, latest_join);
    assert(ready ==> latest_join <= now);
}

pub fn destruction_evidence_places_each_join_before_destruction(
    committed_at: u64, closed_at: u64, observed_at: u64, destroyed_at: u64,
) {
    if destruction_follows_drain(committed_at, closed_at, destroyed_at)
        && observed_during_drain(closed_at, observed_at, destroyed_at)
    {
        assert(committed_at <= closed_at <= observed_at <= destroyed_at);
    }
}

}
