use crate::production::{
    ReplicaFingerprint, TransactionEvent, matching_present_integer, own_readback_term,
    readback_matches_transaction, transaction_completion,
};
use vstd::prelude::*;

verus! {

fn acknowledgement_requires_exact_term(leader: Option<i64>, observed: i64) {
    let own = own_readback_term(leader, observed);
    assert(own == (leader == Some(observed)));
}

fn acknowledgement_requires_exact_sequence(appended: i64, observed: Option<i64>) {
    let matches = matching_present_integer(observed, appended);
    assert(matches == (observed == Some(appended)));
}

fn acknowledgement_requires_exact_payload(
    resolved: bool, appended: Option<&ReplicaFingerprint>, observed: Option<&ReplicaFingerprint>,
) {
    let matches = readback_matches_transaction(resolved, appended, observed);
    assert(matches == (resolved && (match (appended, observed) {
        (Some(expected), Some(actual)) => expected@ == actual@,
        _ => false,
    })));
}

fn enqueued_does_not_acknowledge<Result, Error>(staged: Result) {
    let completion = transaction_completion::<Result, Error>(TransactionEvent::Enqueued(staged));
    assert(completion is None);
}

fn mismatch_fails_pending_write<Result, Error>(error: Error) {
    let completion = transaction_completion::<Result, Error>(TransactionEvent::ReadbackFailed(error));
    assert(completion == Some(Err(error)));
}

fn acknowledged_result_equals_applied_result<Applied, Error>(applied: Applied) {
    let completion = transaction_completion::<Applied, Error>(TransactionEvent::ApplyFinished(Ok(applied)));
    assert(completion == Some(Ok(applied)));
}

fn failed_apply_fails_pending_write<Applied, Error>(error: Error) {
    let completion = transaction_completion::<Applied, Error>(TransactionEvent::ApplyFinished(Err(error)));
    assert(completion == Some(Err(error)));
}

}
