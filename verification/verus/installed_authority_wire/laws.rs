use crate::authority_wire::{identity_matches, revision_matches, split_fields, writer_byte_matches};
use vstd::prelude::*;

verus! {

pub fn decoded_wire_fields_are_original(bytes: &[u8; 40]) -> (fields: ([u8; 8], [u8; 16], [u8; 16]))
    ensures fields.0@ == bytes@.subrange(0, 8),
        fields.1@ == bytes@.subrange(8, 24),
        fields.2@ == bytes@.subrange(24, 40),
{
    split_fields(bytes)
}

pub fn bound_authority_matches_predecessor(
    stamp: i64,
    expected: Option<i64>,
    observed: i64,
    incarnation: &[u8; 16],
    writer: &[u8; 16],
    kind: &str,
    observed_incarnation: &[u8],
    observed_writer: &[u8],
) -> (admitted: bool)
    ensures admitted <==> expected == Some(stamp) && stamp == observed
        && (kind@ == "hosted"@ || kind@ == "standalone"@)
        && observed_incarnation@ == incarnation@ && observed_writer@.len() == 32
        && (forall|i: int| 0 <= i < 16 ==> #[trigger] writer_byte_matches(writer@, observed_writer@, i)),
{
    revision_matches(expected, stamp) && revision_matches(Some(observed), stamp)
        && identity_matches(incarnation, writer, kind, observed_incarnation, observed_writer)
}

}
