#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (observed == expected),
))]
pub(super) fn revision_matches(observed: Option<i64>, expected: Option<i64>) -> bool {
    match (observed, expected) {
        (Some(observed), Some(expected)) => observed == expected,
        (None, None) => true,
        _ => false,
    }
}

pub(super) struct HostedStamp {
    pub observed: Option<i64>,
    pub expected: Option<i64>,
}

#[cfg_attr(verus_keep_ghost, verus_spec(current =>
    ensures current == (forall|i: int| 0 <= i < stamps@.len() ==>
        (#[trigger] stamps@[i]).observed == stamps@[i].expected),
))]
pub(super) fn hosted_stamps_current(stamps: &[HostedStamp]) -> bool {
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= stamps@.len(),
            forall|i: int| 0 <= i < index ==> (#[trigger] stamps@[i]).observed == stamps@[i].expected,
        decreases stamps@.len() - index,
    ))]
    while index < stamps.len() {
        let stamp = &stamps[index];
        if !revision_matches(stamp.observed, stamp.expected) {
            #[cfg(verus_keep_ghost)]
            proof! { assert(stamps@[index as int].observed != stamps@[index as int].expected); }
            return false;
        }
        index += 1;
    }
    true
}

pub(super) struct LandingMarker<'a> {
    pub context: &'a str,
    pub fingerprint: &'a [u8],
    pub source: Option<i64>,
    pub revision: i64,
}

/// Check the event selected for publication against the original submission.
#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (marker.context@ == context@ && marker.fingerprint@ == fingerprint@
        && marker.source == Some(source) && marker.revision == revision),
))]
pub(super) fn landing_marker_matches(
    marker: LandingMarker<'_>, context: &str, fingerprint: &[u8; 32], source: i64, revision: i64,
) -> bool {
    marker.context == context && fingerprint_matches(marker.fingerprint, fingerprint)
        && marker.source == Some(source) && marker.revision == revision
}

#[cfg_attr(verus_keep_ghost, verus_spec(selected =>
    ensures selected == if check_only { Some(original) } else { None },
))]
pub(super) fn hosted_check_for_refusal<T>(original: &T, check_only: bool) -> Option<&T> {
    if check_only { Some(original) } else { None }
}

#[cfg_attr(verus_keep_ghost, verus_spec(bytes =>
    ensures bytes@ == submitted@,
))]
pub(super) fn fingerprint_bytes(submitted: &[u8; 32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(32);
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= 32,
            bytes@ == submitted@.take(index as int),
        decreases 32 - index,
    ))]
    while index < 32 {
        bytes.push(submitted[index]);
        index += 1;
        #[cfg(verus_keep_ghost)]
        proof! { assert(bytes@ =~= submitted@.take(index as int)); }
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(bytes@ =~= submitted@); }
    bytes
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (observed@ == submitted@),
))]
pub(super) fn fingerprint_matches(observed: &[u8], submitted: &[u8; 32]) -> bool {
    if observed.len() != submitted.len() {
        return false;
    }
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= 32,
            observed@.len() == 32,
            forall|i: int| 0 <= i < index ==> #[trigger] observed@[i] == submitted@[i],
        decreases 32 - index,
    ))]
    while index < 32 {
        if observed[index] != submitted[index] {
            return false;
        }
        index += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(observed@ =~= submitted@); }
    true
}

pub(super) enum SourceOrder {
    PlacedPositionMismatch,
    Nonadvancing,
    Admitted,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures match out {
        SourceOrder::PlacedPositionMismatch => placed && source != transaction,
        SourceOrder::Nonadvancing => (!placed || source == transaction)
            && match predecessor { Some(previous) => transaction <= previous, None => false },
        SourceOrder::Admitted => (!placed || source == transaction)
            && match predecessor { Some(previous) => previous < transaction, None => true },
    },
))]
pub(super) fn source_order(
    placed: bool, source: i64, predecessor: Option<i64>, transaction: i64,
) -> SourceOrder {
    if placed && source != transaction {
        return SourceOrder::PlacedPositionMismatch;
    }
    match predecessor {
        Some(previous) if transaction <= previous => SourceOrder::Nonadvancing,
        _ => SourceOrder::Admitted,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(upper =>
    ensures upper == if 0 < sample + 1 < until { Some((sample + 1) as i64) } else { None },
))]
pub(super) fn staging_clock_upper(sample: i64, until: i64) -> Option<i64> {
    match sample.checked_add(1) {
        Some(upper) if 0 < upper && upper < until => Some(upper),
        _ => None,
    }
}
