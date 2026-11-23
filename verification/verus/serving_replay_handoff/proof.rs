#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/archive/replay_handoff.rs"]
mod handoff;

use handoff::{may_install, retains_capture};
use vstd::prelude::*;

verus! {

fn active_holder_excludes_new_archive(cut: i64, prior: Option<i64>) {
    let accepted = may_install(false, true, true, prior, cut);
    assert(!accepted);
}

fn verification_cannot_publish_after_the_checkpoint_or_source_changes(cut: i64, prior: Option<i64>) {
    let checkpoint_advanced = may_install(true, false, true, prior, cut);
    let competing_source = may_install(true, true, false, prior, cut);
    assert(!checkpoint_advanced && !competing_source);
}

fn an_installed_source_moves_strictly_forward(prior: i64, candidate: i64)
    requires candidate <= prior,
{
    let accepted = may_install(true, true, true, Some(prior), candidate);
    assert(!accepted);
}

fn pending_restore_retains_its_capture(selected: u128, repair: u128) {
    let retained = retains_capture(Some(selected), Some((repair, true)), repair);
    assert(retained);
}

fn an_explicit_archive_supersedes_a_completed_repair(selected: u128, repair: u128)
    requires selected != repair,
{
    let old = retains_capture(Some(selected), Some((repair, false)), repair);
    let new = retains_capture(Some(selected), Some((repair, false)), selected);
    assert(!old && new);
}

fn a_completed_repair_without_a_successor_retains_its_capture(repair: u128) {
    let retained = retains_capture(None, Some((repair, false)), repair);
    assert(retained);
}

}

fn main() {}
