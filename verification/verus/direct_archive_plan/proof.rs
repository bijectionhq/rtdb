#![feature(proc_macro_hygiene)]

use vstd::prelude::*;

#[path = "../../../rust/crates/tx/src/hosted_control/direct_plan.rs"]
mod production;

verus! {

fn admitted_direct_ordinals(body: &[u8], inventory: &[u32]) -> (plan: Option<Vec<u32>>)
    ensures
        plan is Some ==> production::admitted(body@, inventory@, plan.unwrap()@),
        body@.len() % 4 != 0 ==> plan is None,
{
    production::decode(body, inventory)
}

} // verus!

fn main() {}
