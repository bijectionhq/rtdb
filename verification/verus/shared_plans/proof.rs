#![feature(proc_macro_hygiene)]

use vstd::prelude::*;

verus! {

/// Stands for `rtdb_query::SharedPlanKey`, whose derived equality compares every planning input.
#[derive(PartialEq, Eq, Structural)]
pub struct SharedPlanKey {
    pub source: u64,
    pub options: u64,
}

}

#[path = "../../../rust/crates/query/src/shared_plan_entries.rs"]
mod production;
#[path = "../../../rust/crates/plan/src/lookup_replay.rs"]
mod replay;

mod laws;

fn main() {}
