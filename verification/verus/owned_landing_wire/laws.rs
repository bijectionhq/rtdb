use vstd::prelude::*;

use crate::owned_landing_wire::{
    ControlCommand, group_u16, group_u32, hosted_check_columns, hosted_control_command,
    point_revision_metadata, world_fingerprint,
};

verus! {

/// The group count and both lengths decode the big-endian sender fields.
pub(crate) fn group_lengths_round_trip(count_or_relation_length: u16, ipc_length: u32)
    -> (decoded: (u16, u32))
    ensures decoded == (count_or_relation_length, ipc_length)
{
    let n = count_or_relation_length;
    let m = ipc_length;
    let decoded_n = group_u16(&[(n >> 8) as u8, n as u8]);
    let decoded_m = group_u32(&[(m >> 24) as u8, (m >> 16) as u8, (m >> 8) as u8, m as u8]);
    proof {
        assert((((n >> 8) as u8 as u16) << 8) | (n as u8 as u16) == n) by (bit_vector);
        assert(
            ((((m >> 24) as u8 as u32) << 24) | (((m >> 16) as u8 as u32) << 16)
                | (((m >> 8) as u8 as u32) << 8) | (m as u8 as u32)) == m
        ) by (bit_vector);
    }
    (decoded_n, decoded_m)
}

/// The typed Arrow lookup of the admitted row gives the original stamp and
/// check flag. The action uses this selection for the ordered control input.
pub(crate) fn hosted_writer_control_round_trips(stamp: i64, require_checks: bool)
    -> (decoded: ControlCommand)
    ensures decoded == ControlCommand::Check(Some(stamp))
{
    let (id, expected, mode) = hosted_check_columns();
    proof {
        assert(id@ == "_id"@ && expected@ == "_expected_micros"@ && mode@ == "_check_only"@);
    }
    hosted_control_command(Some(stamp), true, require_checks)
}

/// The caller positions the action cursor at the encoded reservation.
pub(crate) fn marker_fingerprint_slot_round_trips<'a>(
    body_at_slot: &'a [u8], Ghost(encoded): Ghost<Seq<u8>>, Ghost(suffix): Ghost<Seq<u8>>,
) -> (decoded: Option<([u8; 32], &'a [u8])>)
    requires encoded.len() == 32, body_at_slot@ == encoded + suffix,
    ensures
        decoded matches Some((fingerprint, rest)) && fingerprint@ == encoded && rest@ == suffix,
{
    let result = world_fingerprint(body_at_slot);
    proof {
        assert(body_at_slot@.take(32) =~= encoded);
        assert(body_at_slot@.skip(32) =~= suffix);
    }
    result
}

/// At the held-response boundary the supplied row revision is emitted under
/// the exact metadata key. Canonical decimal serialization follows selection.
pub(crate) fn held_revision_slot_round_trips(revision: i64) -> (metadata: Option<(&'static str, i64)>)
    ensures metadata matches Some((key, at)) && key@ == "rtdb.owned.revision_micros"@ && at == revision
{
    point_revision_metadata(Some(revision))
}

}
