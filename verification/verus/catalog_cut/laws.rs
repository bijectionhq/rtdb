use crate::production::{completed_prefix, completes_snapshot_replay, matches_snapshot_root};
use vstd::prelude::*;

verus! {

// LAW RET-CUT-BLOCK.
pub fn a_pending_boundary_blocks_publication(received: i64) {
    let published = completed_prefix(received, true, true, false);
    assert(published.is_none());
}

// LAW RET-CUT-DEFERRED.
pub fn an_undrained_resolution_blocks_publication(received: i64) {
    let published = completed_prefix(received, true, false, true);
    assert(published.is_none());
}

// LAW RET-CUT-UNTRACKED.
pub fn an_untracked_history_cannot_publish(received: i64) {
    let published = completed_prefix(received, false, false, false);
    assert(published.is_none());
}

// LAW RET-CUT-DRAINED.
pub fn a_drained_supported_history_can_publish_its_prefix(received: i64) {
    let published = completed_prefix(received, true, false, false);
    assert(published == Some(received));
}

// A later installed prefix cannot answer for the original selected end.
pub fn replay_cannot_advance_past_the_selected_cut(base: i64, observed: i64, through: i64)
    requires base <= through, through < observed,
{
    assert(!completes_snapshot_replay(base, base, observed, through, true));
}

// Replaying the selected suffix requires the provider owner throughout.
pub fn suffix_replay_requires_custody(base: i64, through: i64)
    requires base < through,
{
    assert(!completes_snapshot_replay(base, base, through, through, false));
    assert(completes_snapshot_replay(base, base, through, through, true));
}

pub fn another_checkpoint_cannot_anchor_the_original_suffix(base: i64, loaded: i64, through: i64)
    requires base <= through, loaded != base,
{
    assert(!completes_snapshot_replay(loaded, base, through, through, true));
    assert(completes_snapshot_replay(base, base, base, base, false));
}

pub fn resume_rejects_a_substituted_root_even_at_the_same_boundary(
    expected: [u8; 32], loaded: [u8; 32], byte: usize,
)
    requires byte < 32, expected@[byte as int] != loaded@[byte as int],
{
    assert(!matches_snapshot_root(Some(loaded), Some(expected)));
}

pub fn genesis_credentials_cannot_resume_a_published_root(expected: [u8; 32]) {
    assert(!matches_snapshot_root(None, Some(expected)));
    assert(!matches_snapshot_root(Some(expected), None));
    assert(matches_snapshot_root(None, None));
}

}
