#![feature(proc_macro_hygiene)]

use vstd::prelude::*;

#[path = "../../rust/crates/node/src/ordered_index/seek.rs"]
mod production;

verus! {

pub fn given_boundary(length: usize, boundary: usize) -> (out: usize)
    requires boundary <= length,
    ensures out == boundary,
{
    let before = move |position: usize| -> (answer: bool)
        ensures answer == (position < boundary)
    {
        position < boundary
    };
    assert(forall |position: usize| position < length ==> #[trigger] call_requires(before, (position,)));
    assert(production::splits(length, before, boundary));
    let out = production::partition(length, before);
    // Compare both cuts with the actual same deterministic comparator.
    if out < boundary {
        let answer = before(out);
        assert(answer);
        assert(call_ensures(before, (out,), answer));
    } else if boundary < out {
        let answer = before(boundary);
        assert(!answer);
        assert(call_ensures(before, (boundary,), answer));
    }
    out
}

pub proof fn reversed_slots_do_not_repeat(length: usize, a: usize, b: usize)
    requires a < b < length,
    ensures length as int - 1 - b as int < length as int - 1 - a as int,
{}

fn main() {}

}
