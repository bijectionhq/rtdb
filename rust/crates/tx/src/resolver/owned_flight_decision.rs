//! Ordered decisions over observations from one resolver predecessor.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

use super::control_order::{HostedStamp, hosted_stamps_current};

pub(super) enum ControlDecision {
    StaleAuthority,
    StaleControl,
    Current,
}

#[cfg_attr(verus_keep_ghost, verus_spec(decision => ensures
    decision == if !authority_current { ControlDecision::StaleAuthority }
        else if forall|i: int| 0 <= i < stamps@.len() ==>
            (#[trigger] stamps@[i]).observed == stamps@[i].expected
        { ControlDecision::Current } else { ControlDecision::StaleControl },
))]
pub(super) fn check_controls(
    authority_current: bool, stamps: &[HostedStamp],
) -> ControlDecision {
    if !authority_current {
        ControlDecision::StaleAuthority
    } else if !hosted_stamps_current(stamps) {
        ControlDecision::StaleControl
    } else {
        ControlDecision::Current
    }
}

pub(super) enum PrimaryResult<T, E> {
    Staged(T),
    Constraint(E),
    Failed(E),
}

pub(super) enum Publish<T, R, E> {
    Primary(T),
    StageRefusal(R),
    Abort(E),
}

/// An explicitly selected constraint outcome can stage its registered
/// alternative. A failed predecessor check always enters as Failed.
#[cfg_attr(verus_keep_ghost, verus_spec(command => ensures
    command == match result {
        PrimaryResult::Staged(staged) => Publish::Primary(staged),
        PrimaryResult::Constraint(error) => match registered {
            Some(refusal) => Publish::StageRefusal(refusal),
            None => Publish::Abort(error),
        },
        PrimaryResult::Failed(error) => Publish::Abort(error),
    },
))]
pub(super) fn select_publication<T, R, E>(
    result: PrimaryResult<T, E>, registered: Option<R>,
) -> Publish<T, R, E> {
    match result {
        PrimaryResult::Staged(staged) => Publish::Primary(staged),
        PrimaryResult::Constraint(error) => match registered {
            Some(refusal) => Publish::StageRefusal(refusal),
            None => Publish::Abort(error),
        },
        PrimaryResult::Failed(error) => Publish::Abort(error),
    }
}

pub(super) struct MarkerWrite<'a> {
    pub context: &'a str,
    pub fingerprint: [u8; 32],
    pub revision: i64,
}

/// A create-only command for the exact context at the accepting revision.
#[cfg_attr(verus_keep_ghost, verus_spec(command => ensures
    (command is Some) == (observed is None),
    command matches Some(marker) ==> marker.context == context
        && marker.fingerprint == fingerprint && marker.revision == revision,
))]
pub(super) fn stage_marker(
    context: &str, fingerprint: [u8; 32], revision: i64, observed: Option<i64>,
) -> Option<MarkerWrite<'_>> {
    if observed.is_some() {
        return None;
    }
    Some(MarkerWrite { context, fingerprint, revision })
}
