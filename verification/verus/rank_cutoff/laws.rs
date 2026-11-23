use crate::production::{can_stop, can_stop_with_uuid, score_key, uuid_bound_loses};
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

pub fn known_uuid_ties_can_stop(score: u64, chosen: u128, later: u128)
    requires score_key(score).is_some(), chosen < later,
{
    let stop = can_stop_with_uuid(score, chosen, Some((score, Some(later))), Some((score, Some(later))));
    assert(stop);
}

pub fn unknown_or_earlier_uuid_ties_block_cutoff(score: u64, earlier: u128, chosen: u128)
    requires score_key(score).is_some(), earlier <= chosen,
{
    let unknown = can_stop_with_uuid(score, chosen, None, Some((score, None)));
    let eligible = can_stop_with_uuid(score, chosen, Some((score, Some(earlier))), None);
    assert(!unknown && !eligible);
}

pub proof fn a_conservative_uuid_floor_excludes_every_row_in_the_range(
    kth: u64, chosen: u128, ceiling: u64, first: u128, score: u64, iid: u128,
)
    requires
        score_key(kth).is_some(), score_key(ceiling).is_some(), score_key(score).is_some(),
        score_key(score).unwrap() <= score_key(ceiling).unwrap(),
        first <= iid,
        uuid_bound_loses(Some((ceiling, Some(first))), score_key(kth).unwrap(), chosen),
    ensures uuid_bound_loses(Some((score, Some(iid))), score_key(kth).unwrap(), chosen),
{
}

// LAW FTS-2-EXHAUSTED.
pub fn exhausted_frontiers_permit_cutoff(score: u64)
    requires score_key(score).is_some(),
{
    let stop = can_stop(score, None, None);
    assert(stop);
}

}
