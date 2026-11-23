#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(super) enum SeatRevision {
    Missing,
    Free(u128),
    Charged { revision: u128, stopped: bool },
}

pub(super) enum SeatPredecessor {
    Missing,
    Exact(u128),
}

pub(super) enum SeatCondition {
    Missing,
    Exact(u128),
    MissingOr(u128),
    Either(u128, u128),
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match revision {
        SeatRevision::Missing => Some(SeatPredecessor::Missing),
        SeatRevision::Free(version) | SeatRevision::Charged {revision: version, stopped: true} =>
            Some(SeatPredecessor::Exact(version)),
        SeatRevision::Charged {stopped: false, ..} => None,
    },
))]
pub(super) fn claim_predecessor(revision: SeatRevision) -> Option<SeatPredecessor> {
    match revision {
        SeatRevision::Missing => Some(SeatPredecessor::Missing),
        SeatRevision::Free(version) | SeatRevision::Charged { revision: version, stopped: true } =>
            Some(SeatPredecessor::Exact(version)),
        SeatRevision::Charged { stopped: false, .. } => None,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match *predecessor {
        SeatPredecessor::Missing => SeatCondition::Missing,
        SeatPredecessor::Exact(version) => SeatCondition::Exact(version),
    },
))]
pub(super) fn acquire_condition(predecessor: &SeatPredecessor) -> SeatCondition {
    match *predecessor {
        SeatPredecessor::Missing => SeatCondition::Missing,
        SeatPredecessor::Exact(version) => SeatCondition::Exact(version),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match *predecessor {
        SeatPredecessor::Missing => SeatCondition::MissingOr(claimed),
        SeatPredecessor::Exact(version) => SeatCondition::Either(version, claimed),
    },
))]
pub(super) fn release_condition(predecessor: &SeatPredecessor, claimed: u128) -> SeatCondition {
    match *predecessor {
        SeatPredecessor::Missing => SeatCondition::MissingOr(claimed),
        SeatPredecessor::Exact(version) => SeatCondition::Either(version, claimed),
    }
}
