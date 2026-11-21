use crate::production::{can_stop, score_key};
use vstd::prelude::*;

verus! {

// LAW FTS-2-PENDING-TIE.
pub fn pending_tie_blocks_cutoff(score: u64)
    requires score_key(score).is_some(),
{
    let stop = can_stop(score, Some(score), None);
    assert(!stop);
}

// LAW FTS-2-UNREAD-TIE.
pub fn unread_tie_blocks_cutoff(score: u64)
    requires score_key(score).is_some(),
{
    let stop = can_stop(score, None, Some(score));
    assert(!stop);
}

// LAW FTS-2-EXHAUSTED.
pub fn exhausted_frontiers_permit_cutoff(score: u64)
    requires score_key(score).is_some(),
{
    let stop = can_stop(score, None, None);
    assert(stop);
}

}
