#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/archive/replay_handoff.rs"]
mod handoff;

use handoff::may_install;
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

}

fn main() {}
