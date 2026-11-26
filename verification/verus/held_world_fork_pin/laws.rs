use crate::production::{
    CertifiedSourceImage, ForkDecisionOutcome, ForkDecisionRecord, ForkDisposition, ForkPin,
    ForkPinWrite, record_matches_pin, recover_fork_disposition, recover_pin_write, same_reservation,
    select_pin_write,
};
use vstd::prelude::*;

verus! {

fn unconfirmed_decision_preserves_pin(original: ForkPin, current: ForkPin, registered: bool) {
    let write = select_pin_write(original, current, true, ForkDisposition::Ordered { registered, certified: false });
    assert(write.is_none());
}

fn replacement_reservation_is_preserved(original: ForkPin, current: ForkPin, disposition: ForkDisposition)
    requires !same_reservation(original, current),
{
    let write = select_pin_write(original, current, true, disposition);
    assert(write.is_none());
}

fn certified_refusal_releases_the_original_pin(original: ForkPin) {
    let write = select_pin_write(original, original, true, ForkDisposition::Ordered { registered: false, certified: true });
    assert(matches!(write, Some(ForkPinWrite::Release(pin)) if pin == original.pin));
}

/// A replacement process recovers the durable V8 refusal record linked to the
/// original directory reservation and retries the exact pin release once source
/// confirmation and complete applied image hold.
fn replacement_recovers_v8_refusal_and_releases_pin(
    original: ForkPin,
    request_key: [u8; 32],
    request_digest: [u8; 32],
    tx_id: i64,
    revision_micros: i64,
    confirmed_tx_id: i64,
    retain_until_micros: i64,
)
    requires confirmed_tx_id >= tx_id,
{
    let record = ForkDecisionRecord {
        incarnation: original.incarnation,
        log_epoch: original.log_epoch,
        pin: original.pin,
        reservation: original.reservation,
        request_key,
        request_digest,
        outcome: ForkDecisionOutcome::Refused { tx_id, revision_micros },
        retain_until_micros,
    };
    let source = CertifiedSourceImage { confirmed_tx_id, image_complete: true };
    let write = recover_pin_write(
        original, original, true, &request_key, &request_digest, Some(&record), source,
    );
    assert(matches!(write, Some(ForkPinWrite::Release(pin)) if pin == original.pin));
}

/// An incomplete applied image (omitted control row or receipt) cannot certify
/// a recovered V8 refusal record or release the provisional pin.
fn incomplete_applied_image_withholds_recovered_release(
    original: ForkPin,
    request_key: [u8; 32],
    request_digest: [u8; 32],
    record: ForkDecisionRecord,
    confirmed_tx_id: i64,
) {
    let source = CertifiedSourceImage { confirmed_tx_id, image_complete: false };
    let disposition = recover_fork_disposition(original, &request_key, &request_digest, Some(&record), source);
    assert(matches!(disposition, ForkDisposition::Ordered { registered: false, certified: false }));
    let write = recover_pin_write(
        original, original, true, &request_key, &request_digest, Some(&record), source,
    );
    assert(write.is_none());
}

/// A V8 decision record with a different request digest or reservation token
/// cannot settle the original pin.
fn mismatched_v8_record_cannot_settle_pin(
    original: ForkPin,
    request_key: [u8; 32],
    request_digest: [u8; 32],
    record: ForkDecisionRecord,
    source: CertifiedSourceImage,
)
    requires !record_matches_pin(original, request_key@, request_digest@, record),
{
    let write = recover_pin_write(
        original, original, true, &request_key, &request_digest, Some(&record), source,
    );
    assert(write.is_none());
}

}
