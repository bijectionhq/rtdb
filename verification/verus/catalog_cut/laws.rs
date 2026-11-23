use crate::production::completed_prefix;
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

}
