use crate::recovery_promotion::may_promote;
use crate::replay_cursor::{next_in_hold, recovery_coordinates_match, select_record, starts_at};
use vstd::prelude::*;

verus! {

pub fn opened_log_has_certified_coordinates(
    opened: i64, through: u64, base: u64, genesis: bool, checkpoint: Option<i64>, exact_root: bool,
) -> (admitted: bool)
    ensures admitted ==> opened as int + 1 == through as int && base <= through
        && match checkpoint {
            Some(boundary) => exact_root && !genesis && boundary as int + 1 == base as int,
            None => genesis && base == 0,
        },
{
    recovery_coordinates_match(opened, through, base, genesis, checkpoint, exact_root)
}

pub fn checked_replay_delivers_exact_next_record<T>(
    original: T,
    from: u64,
    offered: u64,
    through: u64,
    terminal_matches: bool,
    checkpoint_matches: bool,
    through_matches: bool,
    record_cut_matches: bool,
    recorded_time: i64,
    prepared_time: i64,
    recorded_bytes: usize,
    sealed_bytes: usize,
) -> (record: Option<T>)
    ensures record is Some ==> record == Some(original)
        && from as int + 1 == offered && offered <= through
        && (offered == through ==> terminal_matches)
        && checkpoint_matches && through_matches && record_cut_matches
        && recorded_time == prepared_time && recorded_bytes == sealed_bytes,
{
    if !next_in_hold(from, offered, through, terminal_matches) {
        return None;
    }
    select_record(original, checkpoint_matches, through_matches, record_cut_matches,
        recorded_time, prepared_time, recorded_bytes, sealed_bytes)
}

pub fn renewal_preserves_exact_cursor(next: u64, start: u64, exact_previous_cut: bool) -> (accepted: bool)
    ensures accepted == (start == next && exact_previous_cut),
{
    starts_at(next, start, true, exact_previous_cut)
}

pub fn unknown_prefix_cannot_promote(may_lead: bool, already_leading: bool, holds_grant: bool) {
    let accepted = may_promote(false, may_lead, already_leading, holds_grant);
    assert(!accepted);
}

}
