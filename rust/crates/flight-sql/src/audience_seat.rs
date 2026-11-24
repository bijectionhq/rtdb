#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(crate) enum SeatState<P, S> {
    Vacant(P),
    Live {
        session: S,
        same_credential: bool,
        same_attempt: bool,
    },
    Draining,
}

pub(crate) enum SeatDecision<P, S> {
    Mint(P),
    Resume(S),
    Refuse,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match seat {
        SeatState::Vacant(permit) => SeatDecision::Mint(permit),
        SeatState::Live { session, same_credential: true, same_attempt: true } => SeatDecision::Resume(session),
        _ => SeatDecision::Refuse,
    },
))]
pub(crate) fn admit_audience_seat<P, S>(seat: SeatState<P, S>) -> SeatDecision<P, S> {
    match seat {
        SeatState::Vacant(permit) => SeatDecision::Mint(permit),
        SeatState::Live { session, same_credential, same_attempt } if same_credential && same_attempt =>
            SeatDecision::Resume(session),
        SeatState::Live { .. } | SeatState::Draining => SeatDecision::Refuse,
    }
}

pub(crate) enum SeatAttachment {
    ForeignDeployment,
    Displaced,
    Current { unexpired: bool },
}

pub(crate) enum OccupiedSeatAction {
    RejectStaleCandidate,
    RejectForeignDeployment,
    CheckRetry,
    Retire,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match (candidate_is_current, original) {
        (false, _) => OccupiedSeatAction::RejectStaleCandidate,
        (true, SeatAttachment::ForeignDeployment) => OccupiedSeatAction::RejectForeignDeployment,
        (true, SeatAttachment::Current { unexpired: true }) => OccupiedSeatAction::CheckRetry,
        _ => OccupiedSeatAction::Retire,
    },
))]
pub(crate) fn occupied_seat_action(
    candidate_is_current: bool, original: SeatAttachment,
) -> OccupiedSeatAction {
    if !candidate_is_current {
        return OccupiedSeatAction::RejectStaleCandidate;
    }
    match original {
        SeatAttachment::ForeignDeployment => OccupiedSeatAction::RejectForeignDeployment,
        SeatAttachment::Current { unexpired: true } => OccupiedSeatAction::CheckRetry,
        SeatAttachment::Displaced | SeatAttachment::Current { unexpired: false } => OccupiedSeatAction::Retire,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures out == (open || cleanup)))]
pub(crate) fn audience_handoff_permitted(open: bool, cleanup: bool) -> bool {
    open || cleanup
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out is Some <==> (turn < turns && (digit == b'0' || digit == b'1')),
        out is Some ==> out->Some_0 == (digit == b'1'),
))]
pub(crate) fn parse_seat_alternate(turns: usize, turn: usize, digit: u8) -> Option<bool> {
    if turn >= turns { return None; }
    match digit {
        b'0' => Some(false),
        b'1' => Some(true),
        _ => None,
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires turns <= 64,
    ensures out == 2 * turns,
))]
pub(crate) fn session_seat_count(turns: usize) -> usize {
    2 * turns
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires turn < 64,
    ensures out == 2 * turn + if alternate { 1int } else { 0int },
))]
pub(crate) fn session_seat_index(turn: usize, alternate: bool) -> usize {
    2 * turn + if alternate { 1 } else { 0 }
}
