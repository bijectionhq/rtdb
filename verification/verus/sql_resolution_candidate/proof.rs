#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/tx/src/resolver/resolution_candidate.rs"]
mod production;

use vstd::prelude::*;

verus! {
fn failed_assertion_cannot_return_candidate_writes<T, W, E>(
    candidate: T, prepared_writes: Option<W>, original_refusal: E,
) -> (out: Result<(T, Option<W>), E>)
    ensures out == Err(original_refusal),
{
    production::resolve_candidate(candidate, prepared_writes, Err(original_refusal))
}

fn success_preserves_the_complete_candidate<T, W, E>(
    candidate: T, prepared_writes: Option<W>,
) -> (out: Result<(T, Option<W>), E>)
    ensures out == Ok((candidate, prepared_writes)),
{
    production::resolve_candidate(candidate, prepared_writes, Ok(()))
}
}

fn main() {}
