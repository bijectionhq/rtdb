#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(crate) enum SequenceDecision {
    Discard,
    Apply,
    Void,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (observed < highest),
))]
pub(crate) fn is_stale_term(highest: i64, observed: i64) -> bool {
    observed < highest
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out is Apply <==> (observed is Some && observed->Some_0 == expected),
        out is Void <==> (observed is Some && expected < observed->Some_0),
))]
pub(crate) fn classify_sequence(expected: i64, observed: Option<i64>) -> SequenceDecision {
    match observed {
        Some(sequence) if sequence == expected => SequenceDecision::Apply,
        Some(sequence) if sequence > expected => SequenceDecision::Void,
        _ => SequenceDecision::Discard,
    }
}
