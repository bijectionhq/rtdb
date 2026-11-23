use crate::production::{SeatDecision, SeatState, admit_audience_seat, session_seat_count, session_seat_index};
use vstd::prelude::*;

#[verus_spec(out =>
    requires turns <= 64, turn < turns, other_turn < turns,
    ensures out,
)]
fn prepaid_seats_are_distinct_and_in_range(
    turns: usize, turn: usize, alternate: bool, other_turn: usize, other_alternate: bool,
) -> bool {
    let count = session_seat_count(turns);
    let index = session_seat_index(turn, alternate);
    let other_index = session_seat_index(other_turn, other_alternate);
    index < count && other_index < count
        && ((index == other_index) == (turn == other_turn && alternate == other_alternate))
}

#[verus_spec(out =>
    requires turn < 64, !close_arrived || retained_io,
    ensures !(out is Mint),
)]
fn a_lost_close_cannot_admit_a_third_session(
    turn: usize, alternate: bool, closed_alternate: bool, close_arrived: bool, retained_io: bool,
    same_credential: bool, same_attempt: bool,
) -> SeatDecision<usize, usize> {
    let selected = session_seat_index(turn, alternate);
    let close_target = session_seat_index(turn, closed_alternate);
    let seat = if selected != close_target || !close_arrived {
        SeatState::Live { session: selected, same_credential, same_attempt }
    } else if retained_io {
        SeatState::Draining
    } else {
        SeatState::Vacant(selected)
    };
    admit_audience_seat(seat)
}
