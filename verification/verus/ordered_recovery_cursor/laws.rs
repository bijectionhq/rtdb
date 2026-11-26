use crate::recovery_promotion::{may_promote, publish_installed_prefix, writer_activation};
use crate::replay_cursor::{new_replay_cursor, next_in_hold, recovery_coordinates_match, select_record, starts_at};
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

pub fn unknown_prefix_cannot_promote(
    prefix_applied: bool, may_lead: bool, already_leading: bool, holds_grant: bool,
) {
    let accepted = may_promote(false, prefix_applied, may_lead, already_leading, holds_grant);
    assert(!accepted);
}

pub fn writer_waits_for_the_recovered_prefix(received: i64, previous: i64)
    requires previous < received,
{
    let initial = writer_activation(true);
    assert(initial is Recovering);
    let appended = initial.installed(true, true, true, received, Some(previous));
    assert(appended is Recovering);
    let unconfirmed = appended.installed(false, true, true, received, Some(received));
    assert(unconfirmed is Recovering);
    let unknown = unconfirmed.installed(true, false, true, received, Some(received));
    assert(unknown is Recovering);
    let incomplete = unknown.installed(true, true, false, received, Some(received));
    assert(incomplete is Recovering);
    let recovered = incomplete.installed(true, true, true, received, Some(received));
    assert(recovered is Active);
}

/// A record read but not yet applied (held behind a pending block or an
/// in-flight resolution) blocks promotion, writer activation and
/// installed-prefix publication.
pub fn unapplied_prefix_blocks_promotion_and_activation(next_pos: i64) {
    let can_promote = may_promote(true, false, true, false, true);
    assert(!can_promote);
    let published = publish_installed_prefix(true, Some(next_pos), false);
    assert(published is None);
    let activation = writer_activation(true).installed(true, true, false, next_pos, Some(next_pos));
    assert(activation is Recovering);
}

/// A fully applied prefix at the exact cut enables promotion, writer
/// activation and installed-prefix publication.
pub fn applied_prefix_enables_promotion_and_serving(next_pos: i64) {
    let can_promote = may_promote(true, true, true, false, true);
    assert(can_promote);
    let published = publish_installed_prefix(true, Some(next_pos), true);
    assert(published == Some(next_pos));
    let activation = writer_activation(true).installed(true, true, true, next_pos, Some(next_pos));
    assert(activation is Active);
}

pub fn exact_delivery_retries_same_cut<C: Copy, T>(
    start_position: u64, start: C, end: C, through: C, through_position: u64,
    original: T, delivered: T,
) -> (output: T)
    requires start_position as int + 1 < through_position,
    ensures output == delivered,
{
    let mut cursor = new_replay_cursor(start_position);
    let bound = cursor.bind(start, start_position, through_position, through_position, false);
    assert(bound);
    let head = cursor.prepare(end, start_position + 1, through, through_position, false, 5, 17, 33);
    assert(head is Some);
    let head = head.unwrap();
    let (cut, record) = head.select(original, true, true, true, 5, 17).unwrap();
    let selected_from = cut.from();
    assert(record == original && selected_from == start);

    let refused: Result<Option<T>, ()> = cursor.finish(cut, true, Err(()));
    assert(refused == Err(()));
    let unchanged = cursor.next();
    let predecessor = cursor.from();
    assert(unchanged == start_position && predecessor == Some(start));

    let (retry, _) = head.select(record, true, true, true, 5, 17).unwrap();
    let result: Result<Option<T>, ()> = cursor.finish(retry, true, Ok(delivered));
    let advanced = cursor.next();
    let current = cursor.from();
    assert(advanced == start_position + 1 && current == Some(end));
    result.unwrap().unwrap()
}

}
