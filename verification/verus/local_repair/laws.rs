use crate::anchor::{AnchorInstallation, RecoveryCollection, collect_recovery_file, install_anchor};
use crate::digest::same_publication_digest;
use crate::replay::{admits_repair_interval, exclusive_replay_end, matches_retained_boundary, matches_root};
use vstd::prelude::*;

verus! {

pub fn only_a_same_journal_successor_replaces_an_anchor(
    old_block: i64, old_boundary: i64, new_block: i64, new_boundary: i64, same_digest: bool,
)
    requires old_block >= 0, old_boundary >= 0,
{
    let decision = install_anchor(Some((old_block, old_boundary)), new_block, new_boundary, same_digest);
    if matches!(decision, AnchorInstallation::Replace) {
        assert(new_block > old_block);
        assert(new_boundary > old_boundary);
        assert(new_boundary >> 48 == old_boundary >> 48);
    }
}

pub fn confirming_an_anchor_requires_its_exact_receipt(
    block: i64, boundary: i64, same_digest: bool,
)
    requires block >= 0, boundary >= 0,
{
    let decision = install_anchor(Some((block, boundary)), block, boundary, same_digest);
    assert(matches!(decision, AnchorInstallation::Confirm) == same_digest);
    assert(!matches!(decision, AnchorInstallation::Replace));
}

pub fn files_at_and_after_an_anchor_stay(first: i64, file: i64)
    requires file >= first,
{
    let decision = collect_recovery_file(Some(first), file);
    assert(matches!(decision, RecoveryCollection::Keep));
}

pub fn an_anchored_removal_requires_synchronization(first: i64, file: i64)
    requires file < first,
{
    let decision = collect_recovery_file(Some(first), file);
    assert(matches!(decision, RecoveryCollection::SynchronizeAndRemove));
}

pub fn every_digest_byte_must_match(expected: [u8; 32], observed: [u8; 32], byte: usize)
    requires byte < 32, expected@[byte as int] != observed@[byte as int],
{
    let matches = same_publication_digest(expected, observed);
    assert(!matches);
}

pub fn a_root_requires_its_exact_block_and_digest(expected: i64, observed: i64, digest_matches: bool)
{
    let accepted = matches_root(expected, observed, digest_matches);
    if accepted {
        assert(expected == observed);
        assert(digest_matches);
    }
}

pub fn a_repair_interval_starts_and_ends_in_one_journal(after: i64, through: i64)
{
    let accepted = admits_repair_interval(after, through);
    if accepted {
        assert(0 <= after && after < through);
        assert(after >> 48 == through >> 48);
    }
}

pub fn an_export_cannot_substitute_another_retained_boundary(recorded: i64, observed: i64)
    requires recorded != observed,
{
    let accepted = matches_retained_boundary(Some(recorded), observed);
    assert(!accepted);
}

pub fn replay_end_includes_the_target_but_excludes_the_next_byte(through: i64, later: i64)
    requires 0 <= through < i64::MAX, through < later,
{
    let end = exclusive_replay_end(through);
    assert(end.is_some());
    assert(through < end.unwrap());
    assert(end.unwrap() <= later);
}

}
