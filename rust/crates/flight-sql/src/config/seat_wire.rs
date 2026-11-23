use super::seat_uuid::{Uuid, parse_attempt};

#[cfg(verus_keep_ghost)]
use super::seat_uuid::{canonical_uuid, non_nil};

use crate::audience_seat::parse_seat_alternate;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub const AUDIENCE_FLIGHT_HEADER: &str = "x-rtdb-audience-flight-v19";
pub const AUDIENCE_SEAT_HEADER: &str = "x-rtdb-audience-reader-seat";

#[cfg(verus_keep_ghost)]
verus! {
pub closed spec fn decimal_digit(byte: u8) -> bool {
    b'0' <= byte <= b'9'
}

pub closed spec fn turn_text(bytes: Seq<u8>, turn: int, split: int) -> bool {
    (bytes.len() == 40 && split == 1 && decimal_digit(bytes[0]) && turn == bytes[0] - b'0')
    || (bytes.len() == 41 && split == 2 && b'1' <= bytes[0] <= b'9' && decimal_digit(bytes[1])
        && turn == 10 * (bytes[0] - b'0') + bytes[1] - b'0')
}
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        (out is Some) <==> (exists|split: int, turn: int| #[trigger] turn_text(bytes@, turn, split)
            && turn < turns && bytes@[split] == b'/' && bytes@[split + 2] == b'/'
            && (bytes@[split + 1] == b'0' || bytes@[split + 1] == b'1')),
        out matches Some((turn, alternate, start)) ==>
            turn < turns && start + 36 == bytes@.len() && start >= 4
            && turn_text(bytes@, turn as int, start - 3)
            && bytes@[start - 3] == b'/' && bytes@[start - 1] == b'/'
            && bytes@[start - 2] == (if alternate { b'1' } else { b'0' }),
))]
pub(super) fn seat_prefix(turns: usize, bytes: &[u8]) -> Option<(usize, bool, usize)> {
    let (turn, split) = match bytes.len() {
        40 if bytes[0] >= b'0' && bytes[0] <= b'9' => ((bytes[0] - b'0') as usize, 1),
        41 if bytes[0] >= b'1' && bytes[0] <= b'9' && bytes[1] >= b'0' && bytes[1] <= b'9' => (
            10 * (bytes[0] - b'0') as usize + (bytes[1] - b'0') as usize, 2
        ),
        _ => return None,
    };
    if bytes[split] != b'/' || bytes[split + 2] != b'/' { return None; }
    let alternate = parse_seat_alternate(turns, turn, bytes[split + 1])?;
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(turn_text(bytes@, turn as int, split as int));
    }
    Some((turn, alternate, split + 3))
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (audience && handshake && first && !second),
))]
pub(crate) fn accepts_seat_header(audience: bool, handshake: bool, first: bool, second: bool) -> bool {
    audience && handshake && first && !second
}

#[cfg(verus_keep_ghost)]
verus! {

pub open spec fn seat_text(bytes: Seq<u8>, turns: int, turn: int, alternate: bool, attempt: Uuid) -> bool {
    let split = bytes.len() - 39;
    turn_text(bytes, turn, split)
        && turn < turns
        && bytes[split] == b'/'
        && bytes[split + 1] == (if alternate { b'1' } else { b'0' })
        && bytes[split + 2] == b'/'
        && non_nil(attempt)
        && bytes.skip(split + 3) == canonical_uuid(attempt)
}

}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (start + 36 == bytes@.len() && bytes@.skip(start as int) == canonical@),
))]
fn canonical_attempt_text(bytes: &[u8], start: usize, canonical: &[u8; 36]) -> bool {
    if start > bytes.len() || bytes.len() - start != 36 { return false; }
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            start + 36 == bytes@.len(), 0 <= index <= 36,
            forall|i: int| 0 <= i < index ==> #[trigger] canonical@[i] == bytes@[start + i],
        decreases 36 - index,
    ))]
    while index < 36 {
        if bytes[start + index] != canonical[index] {
            #[cfg(verus_keep_ghost)]
            proof! { assert(bytes@.skip(start as int)[index as int] == bytes@[start + index]); }
            return false;
        }
        index += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(bytes@.skip(start as int) =~= canonical@); }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        out matches Some((turn, alternate, attempt)) ==> seat_text(bytes@, turns as int, turn as int, alternate, attempt),
        forall|turn: usize, alternate: bool, attempt: Uuid|
            #[trigger] seat_text(bytes@, turns as int, turn as int, alternate, attempt)
            ==> out == Some((turn, alternate, attempt)),
))]
pub(super) fn parse_seat_fields(turns: usize, bytes: &[u8]) -> Option<(usize, bool, Uuid)> {
    let (turn, alternate, start) = seat_prefix(turns, bytes)?;
    let (attempt, canonical) = parse_attempt(bytes, start)?;
    if !canonical_attempt_text(bytes, start, &canonical) { return None; }
    Some((turn, alternate, attempt))
}

#[cfg(verus_keep_ghost)]
verus! {
proof fn audience_header_names_keep_their_protocol_spelling()
    ensures
        AUDIENCE_FLIGHT_HEADER@ == "x-rtdb-audience-flight-v19"@,
        AUDIENCE_SEAT_HEADER@ == "x-rtdb-audience-reader-seat"@,
        AUDIENCE_FLIGHT_HEADER@ != AUDIENCE_SEAT_HEADER@,
{
    reveal_strlit("x-rtdb-audience-flight-v19");
    reveal_strlit("x-rtdb-audience-reader-seat");
}
}
