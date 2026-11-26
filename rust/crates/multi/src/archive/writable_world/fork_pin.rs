#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[derive(Clone, Copy)]
pub(super) struct ForkPin {
    pub incarnation: u64,
    pub log_epoch: i32,
    pub pin: u128,
    pub reservation: u128,
}

#[derive(Clone, Copy)]
pub(super) enum ForkDisposition {
    NeverSubmitted,
    Ordered { registered: bool, certified: bool },
}

pub(super) enum ForkPinWrite {
    Publish(u128),
    Release(u128),
}

/// V8 decision record outcome stored at the reservation's deterministic
/// request slot so a replacement process can recover an original refusal
/// (or applied registration) after process loss and retry directory settlement.
#[derive(Clone, Copy)]
pub(super) enum ForkDecisionOutcome {
    Applied { world: u128, tx_id: i64 },
    Refused { tx_id: i64, revision_micros: i64 },
}

/// Durable V8 refusal/decision record linking an ordered fork outcome to the
/// exact directory reservation that must be published or released.
#[derive(Clone, Copy)]
pub(super) struct ForkDecisionRecord {
    pub incarnation: u64,
    pub log_epoch: i32,
    pub pin: u128,
    pub reservation: u128,
    pub request_key: [u8; 32],
    pub request_digest: [u8; 32],
    pub outcome: ForkDecisionOutcome,
    pub retain_until_micros: i64,
}

/// Certified source and applied-image observation required before a recovered
/// V8 decision record can settle a directory reservation.
#[derive(Clone, Copy)]
pub(super) struct CertifiedSourceImage {
    pub confirmed_tx_id: i64,
    pub image_complete: bool,
}

#[cfg_attr(verus_keep_ghost, verus_spec(same => ensures same == (left@ == right@)))]
pub(super) fn same_bytes32(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut i: usize = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 32,
        forall|k: int| 0 <= k < i ==> #[trigger] left@[k] == right@[k],
        decreases 32 - i,
    ))]
    while i < 32 {
        if left[i] != right[i] {
            return false;
        }
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(left@ =~= right@);
    }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(write => ensures
    match write {
        Some(ForkPinWrite::Publish(pin)) =>
            binding_matches && same_reservation(original, current) && pin == original.pin
                && matches!(disposition, ForkDisposition::Ordered { registered: true, certified: true }),
        Some(ForkPinWrite::Release(pin)) =>
            binding_matches && same_reservation(original, current) && pin == original.pin
                && matches!(disposition, ForkDisposition::NeverSubmitted
                    | ForkDisposition::Ordered { registered: false, certified: true }),
        None => !binding_matches || !same_reservation(original, current)
            || matches!(disposition, ForkDisposition::Ordered { certified: false, .. }),
    },
))]
pub(super) fn select_pin_write(
    original: ForkPin, current: ForkPin, binding_matches: bool, disposition: ForkDisposition,
) -> Option<ForkPinWrite> {
    if !binding_matches || original.incarnation != current.incarnation
        || original.log_epoch != current.log_epoch || original.pin != current.pin
        || original.reservation != current.reservation
    {
        return None;
    }
    match disposition {
        ForkDisposition::NeverSubmitted => Some(ForkPinWrite::Release(original.pin)),
        ForkDisposition::Ordered { registered: true, certified: true } => Some(ForkPinWrite::Publish(original.pin)),
        ForkDisposition::Ordered { registered: false, certified: true } => Some(ForkPinWrite::Release(original.pin)),
        ForkDisposition::Ordered { certified: false, .. } => None,
    }
}

/// Certify a durable V8 decision record recovered by a replacement process
/// against the reservation's exact coordinates, request link, confirmed source
/// prefix, and content-complete applied image.
#[cfg_attr(verus_keep_ghost, verus_spec(disposition => ensures
    match record {
        Some(rec) => {
            let matches_link = rec.incarnation == original.incarnation
                && rec.log_epoch == original.log_epoch
                && rec.pin == original.pin
                && rec.reservation == original.reservation
                && rec.request_key@ == expected_key@
                && rec.request_digest@ == expected_digest@
                && source.image_complete;
            match rec.outcome {
                ForkDecisionOutcome::Applied { tx_id, .. } =>
                    disposition == ForkDisposition::Ordered {
                        registered: matches_link && source.confirmed_tx_id >= tx_id,
                        certified: matches_link && source.confirmed_tx_id >= tx_id,
                    },
                ForkDecisionOutcome::Refused { tx_id, .. } =>
                    disposition == ForkDisposition::Ordered {
                        registered: false,
                        certified: matches_link && source.confirmed_tx_id >= tx_id,
                    },
            }
        },
        None => disposition == ForkDisposition::Ordered { registered: false, certified: false },
    },
))]
pub(super) fn recover_fork_disposition(
    original: ForkPin,
    expected_key: &[u8; 32],
    expected_digest: &[u8; 32],
    record: Option<&ForkDecisionRecord>,
    source: CertifiedSourceImage,
) -> ForkDisposition {
    let Some(rec) = record else {
        return ForkDisposition::Ordered { registered: false, certified: false };
    };
    if !source.image_complete
        || rec.incarnation != original.incarnation
        || rec.log_epoch != original.log_epoch
        || rec.pin != original.pin
        || rec.reservation != original.reservation
        || !same_bytes32(&rec.request_key, expected_key)
        || !same_bytes32(&rec.request_digest, expected_digest)
    {
        return ForkDisposition::Ordered { registered: false, certified: false };
    }
    match rec.outcome {
        ForkDecisionOutcome::Applied { tx_id, .. } => {
            let certified = source.confirmed_tx_id >= tx_id;
            ForkDisposition::Ordered { registered: certified, certified }
        }
        ForkDecisionOutcome::Refused { tx_id, .. } => {
            let certified = source.confirmed_tx_id >= tx_id;
            ForkDisposition::Ordered { registered: false, certified }
        }
    }
}

/// Replacement-process settlement decision: recovers the durable V8 decision
/// record and emits the exact directory Publish or Release compare-and-set.
#[cfg_attr(verus_keep_ghost, verus_spec(write => ensures
    match write {
        Some(ForkPinWrite::Publish(pin)) =>
            binding_matches && same_reservation(original, current) && pin == original.pin
                && (match record {
                    Some(rec) => record_matches_pin(original, expected_key@, expected_digest@, *rec)
                        && source.image_complete
                        && (match rec.outcome {
                            ForkDecisionOutcome::Applied { tx_id, .. } => source.confirmed_tx_id >= tx_id,
                            ForkDecisionOutcome::Refused { .. } => false,
                        }),
                    None => false,
                }),
        Some(ForkPinWrite::Release(pin)) =>
            binding_matches && same_reservation(original, current) && pin == original.pin
                && (match record {
                    Some(rec) => record_matches_pin(original, expected_key@, expected_digest@, *rec)
                        && source.image_complete
                        && (match rec.outcome {
                            ForkDecisionOutcome::Applied { .. } => false,
                            ForkDecisionOutcome::Refused { tx_id, .. } => source.confirmed_tx_id >= tx_id,
                        }),
                    None => false,
                }),
        None => !binding_matches || !same_reservation(original, current)
            || !source.image_complete
            || match record {
                None => true,
                Some(rec) => !record_matches_pin(original, expected_key@, expected_digest@, *rec)
                    || match rec.outcome {
                        ForkDecisionOutcome::Applied { tx_id, .. } => source.confirmed_tx_id < tx_id,
                        ForkDecisionOutcome::Refused { tx_id, .. } => source.confirmed_tx_id < tx_id,
                    },
            },
    },
))]
pub(super) fn recover_pin_write(
    original: ForkPin,
    current: ForkPin,
    binding_matches: bool,
    expected_key: &[u8; 32],
    expected_digest: &[u8; 32],
    record: Option<&ForkDecisionRecord>,
    source: CertifiedSourceImage,
) -> Option<ForkPinWrite> {
    let disposition = recover_fork_disposition(original, expected_key, expected_digest, record, source);
    select_pin_write(original, current, binding_matches, disposition)
}

#[cfg(verus_keep_ghost)]
verus! {
pub(super) open spec fn same_reservation(original: ForkPin, current: ForkPin) -> bool {
    original.incarnation == current.incarnation && original.log_epoch == current.log_epoch
        && original.pin == current.pin && original.reservation == current.reservation
}

pub(super) open spec fn record_matches_pin(
    original: ForkPin,
    expected_key: Seq<u8>,
    expected_digest: Seq<u8>,
    rec: ForkDecisionRecord,
) -> bool {
    rec.incarnation == original.incarnation
        && rec.log_epoch == original.log_epoch
        && rec.pin == original.pin
        && rec.reservation == original.reservation
        && rec.request_key@ == expected_key
        && rec.request_digest@ == expected_digest
}
}
