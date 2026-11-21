#![feature(proc_macro_hygiene)]

use vstd::prelude::*;

#[path = "../../rust/crates/directory/src/ordered_index.rs"]
mod production;

verus! {

fn no_reopen_after_replacement(
    dropped: bool, previous: u64, replacement: u64, requested: u64,
) -> (admitted: bool)
    requires dropped || requested < previous || requested < replacement,
    ensures !admitted,
{
    let oldest = production::advance(previous, replacement);
    production::admits(dropped, oldest, requested)
}

fn stale_retirement_preserves_newer_pages(
    previous: u64, replacement: u64, requested: u64,
) -> (admitted: bool)
    requires replacement <= previous <= requested,
    ensures admitted,
{
    let oldest = production::advance(previous, replacement);
    production::admits(false, oldest, requested)
}

fn new_scope_opens_after_cut(
    previous: u64, replacement: u64,
) -> (admitted: bool)
    requires previous <= replacement,
    ensures admitted,
{
    let oldest = production::advance(previous, replacement);
    production::admits(false, oldest, replacement)
}

fn main() {}

}
