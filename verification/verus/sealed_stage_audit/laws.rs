use crate::journal_evidence::{journal_frame_matches, valid_journal_cutoff};
use crate::stage_journal::checked_journal_fields;
use vstd::prelude::*;

verus! {

fn the_audit_accepts_the_writers_sealed_pair(stage_cutoff: i64, before: i64, digest: &[u8; 32])
    requires
        before >= stage_cutoff,
        before > 0,
        digest@ != Seq::new(32, |i: int| 0u8),
{
    let bytes: &[u8] = digest;
    let checked = checked_journal_fields(stage_cutoff, Some(before), Some(bytes));
    let Ok(Some((sealed_cutoff, sealed_digest))) = checked else {
        assert(false);
        return;
    };
    let valid = valid_journal_cutoff(sealed_cutoff, digest);
    assert(valid);
    assert(sealed_digest@ == digest@);
    let accepted = journal_frame_matches(Some((sealed_cutoff, *digest)), Some((before, bytes)));
    assert(accepted);
}

}
