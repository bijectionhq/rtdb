#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/cell_attempt.rs"]
mod attempt;

use attempt::{may_activate, may_close, may_dispose, may_select_next};
use vstd::prelude::*;

verus! {

fn a_prepared_attempt_cannot_close(current: bool) {
    let closed = may_close(false, current);
    assert(!closed);
}

fn a_fresh_attempt_follows_only_a_drained_closed_predecessor(prior: Option<u32>, next: u32) {
    let undrained = may_select_next(true, false, false, true, prior, next);
    let open = may_select_next(false, true, false, true, prior, next);
    let reused = may_select_next(true, true, false, false, prior, next);
    let prepared = may_select_next(true, true, true, true, prior, next);
    assert(!undrained && !open && !reused && !prepared);
}

fn attempts_advance_by_exactly_one(prior: u32, next: u32)
    requires next != prior + 1,
{
    let accepted = may_select_next(true, true, false, true, Some(prior), next);
    assert(!accepted);
}

fn a_closed_attempts_receipt_cannot_activate(receipt: u32, selected: u32)
    requires receipt != selected,
{
    let stale = may_activate(true, false, Some(receipt), Some(selected), Some(selected));
    let unfollowed = may_activate(true, false, Some(selected), Some(selected), Some(receipt));
    let legacy = may_activate(true, false, None, Some(selected), Some(selected));
    assert(!stale && !unfollowed && !legacy);
}

fn an_abandoned_or_unprepared_generation_cannot_activate(attempt: Option<u32>) {
    let abandoned = may_activate(true, true, attempt, attempt, attempt);
    let staging = may_activate(false, false, attempt, attempt, attempt);
    assert(!abandoned && !staging);
}

fn disposal_never_reaches_the_current_or_selected_generation(generation: u128, other: u128) {
    let current = may_dispose(true, generation, generation, Some(other));
    let selected = may_dispose(true, generation, other, Some(generation));
    let open = may_dispose(false, generation, other, None);
    assert(!current && !selected && !open);
}

}

fn main() {}
