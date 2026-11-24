#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(exact =>
    ensures exact == (observed@ == expected@),
))]
pub(super) fn matches_bytes(observed: &[u8], expected: &[u8]) -> bool {
    if observed.len() != expected.len() {
        return false;
    }
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= observed@.len(),
        observed@.len() == expected@.len(),
        forall|j: int| 0 <= j < i ==> observed@[j] == expected@[j],
        decreases observed@.len() - i,
    ))]
    while i < observed.len() {
        if observed[i] != expected[i] {
            return false;
        }
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(observed@ =~= expected@); }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(exact =>
    ensures exact == (recovered_id >= 0 && recovered_at == checkpoint_at
        && snapshot == Some((recovered_id, recovered_at))),
))]
pub(super) fn stage_transaction_matches(
    checkpoint_at: i64,
    recovered_id: i64,
    recovered_at: i64,
    snapshot: Option<(i64, i64)>,
) -> bool {
    recovered_id >= 0 && recovered_at == checkpoint_at && match snapshot {
        Some((id, at)) => id == recovered_id && at == recovered_at,
        None => false,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(entry =>
    ensures
        entry is Some <==> index < tries@.len(),
        entry matches Some((trie, next)) ==> *trie == tries@[index as int]
            && next == index + 1,
))]
pub(super) fn next_owned_file<T>(tries: &[T], index: usize) -> Option<(&T, usize)> {
    if index >= tries.len() {
        return None;
    }
    Some((&tries[index], index + 1))
}
