pub(super) const OPERATIONAL_TARGET_MS: u64 = 120_000;

pub(super) fn strict_majority(cohort: usize, evidence: usize) -> bool {
    cohort >= 3 && cohort % 2 == 1 && evidence == cohort / 2 + 1
}

pub(super) fn mirrors_fence(
    selected: u8,
    generations: [u64; 2],
    populated: [bool; 2],
    distinct: bool,
) -> bool {
    if selected > 1 {
        return false;
    }
    let selected = usize::from(selected);
    let alternate = 1 - selected;
    generations[alternate] != 0
        && generations[alternate].checked_add(1) == Some(generations[selected])
        && populated[alternate]
        && populated[selected]
        && distinct
}

pub(super) fn at_clean_checkpoint(
    exact_checkpoint: bool,
    generation: u64,
    successor_generation: u64,
) -> bool {
    exact_checkpoint && generation == successor_generation && generation > 1
}

pub(super) fn fixed_deadline_admits(requested: u64, reserved: u64) -> bool {
    reserved != 0 && reserved <= requested
}

pub(super) fn same_reservation(
    reserved: u64,
    committed: u64,
    same_retiring_inventory: bool,
    same_draining_inventory: bool,
) -> bool {
    reserved == committed && same_retiring_inventory && same_draining_inventory
}

pub(super) fn same_attempt(exact_preparation: bool, exact_switch: bool) -> bool {
    exact_preparation && exact_switch
}

pub(super) fn fixed_commit_admits(requested: u64, commitment: u64, committed_at: u64) -> bool {
    committed_at < commitment && commitment <= requested
}

pub(super) fn installed_admits(operational: bool, reserved: bool) -> bool {
    operational && !reserved
}

pub(super) fn operational_target_at(committed_at: u64) -> Option<u64> {
    committed_at.checked_add(OPERATIONAL_TARGET_MS)
}

pub(super) fn completion_status(enforced_deadline: Option<u64>, destroyed_at: u64) -> Option<bool> {
    match enforced_deadline {
        Some(by) => Some(destroyed_at <= by),
        None => None,
    }
}

pub(super) fn ordered_holds_drained(outstanding: u64) -> bool {
    outstanding == 0
}

pub(super) fn withdrawal_follows_join(now: u64, latest_join: u64) -> bool {
    now >= latest_join
}

pub(super) fn destruction_follows_drain(committed_at: u64, closed_at: u64, destroyed_at: u64) -> bool {
    committed_at <= closed_at && closed_at <= destroyed_at
}

pub(super) fn observed_during_drain(closed_at: u64, observed_at: u64, destroyed_at: u64) -> bool {
    closed_at <= observed_at && observed_at <= destroyed_at
}
