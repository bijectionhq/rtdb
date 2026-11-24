#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matched =>
    ensures matched == (observed == Some(expected)),
))]
pub(crate) fn matching_present_integer(observed: Option<i64>, expected: i64) -> bool {
    match observed {
        Some(observed) => observed == expected,
        None => false,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(own =>
    ensures own == (leader == Some(observed)),
))]
pub(crate) fn own_readback_term(leader: Option<i64>, observed: i64) -> bool {
    match leader {
        Some(term) => term == observed,
        _ => false,
    }
}

pub(crate) type ReplicaFingerprint = [u8; 32];

#[cfg_attr(verus_keep_ghost, verus_spec(same =>
    ensures same == (appended@ == observed@),
))]
fn same_fingerprint(appended: &ReplicaFingerprint, observed: &ReplicaFingerprint) -> bool {
    let mut index = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= 32,
            forall|i: int| 0 <= i < index ==> #[trigger] appended@[i] == observed@[i],
        decreases 32 - index,
    ))]
    while index < 32 {
        if appended[index] != observed[index] {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(appended@[index as int] != observed@[index as int]);
                assert(appended@ != observed@);
            }
            return false;
        }
        index += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(appended@ =~= observed@); }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(matched =>
    ensures matched == (match (resolved, appended, observed) {
        (true, Some(expected), Some(actual)) => expected@ == actual@,
        _ => false,
    }),
))]
pub(crate) fn readback_matches_transaction(
    resolved: bool, appended: Option<&ReplicaFingerprint>, observed: Option<&ReplicaFingerprint>,
) -> bool {
    match (resolved, appended, observed) {
        (true, Some(appended), Some(observed)) => same_fingerprint(appended, observed),
        _ => false,
    }
}

pub(crate) enum TransactionEvent<T, E> {
    Enqueued(T),
    ReadbackFailed(E),
    ApplyFinished(Result<T, E>),
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (match event {
        TransactionEvent::Enqueued(..) => None,
        TransactionEvent::ReadbackFailed(error) => Some(Err(error)),
        TransactionEvent::ApplyFinished(outcome) => Some(outcome),
    }),
))]
pub(crate) fn transaction_completion<T, E>(event: TransactionEvent<T, E>) -> Option<Result<T, E>> {
    match event {
        TransactionEvent::Enqueued(_) => None,
        TransactionEvent::ReadbackFailed(error) => Some(Err(error)),
        TransactionEvent::ApplyFinished(outcome) => Some(outcome),
    }
}

#[cfg(not(verus_keep_ghost))]
pub(crate) fn fingerprint(frame: &[u8]) -> ReplicaFingerprint {
    use sha2::{Digest, Sha256};
    Sha256::digest(frame).into()
}
