#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/node/src/ordered_index/replay_coverage.rs"]
mod coverage;

use coverage::{classify_replay_block, replay_complete};
use vstd::prelude::*;

verus! {

fn cannot_skip_an_adopted_block(expected_block: i64, observed_block: i64, rows: Option<i64>)
    requires expected_block <= observed_block,
{
    let decision = classify_replay_block(Some((expected_block, rows)), observed_block, 0);
    assert(decision == None);
}

fn nonempty_block_requires_its_adopted_count(next: Option<(i64, Option<i64>)>, block: i64, rows: i64)
    requires rows > 0, next != Some((block, Some(rows))),
{
    let decision = classify_replay_block(next, block, rows);
    assert(decision == None);
}

fn a_matching_total_cannot_substitute_for_the_inventory(rows: i64)
{
    let complete = replay_complete(false, rows, rows);
    assert(!complete);
}

}

fn main() {}
