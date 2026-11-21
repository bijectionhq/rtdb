use crate::production::{all_held_members_scheduled, block_is_eligible};
use vstd::prelude::*;

verus! {

// LAW GC-2-LATEST.
pub fn latest_block_stays(latest: i64, keep: usize)
{
    let eligible = block_is_eligible(latest, latest, keep);
    assert(!eligible);
}

// LAW GC-1-GROUP.
pub fn a_retained_sibling_keeps_the_group(members: [Option<bool>; 4], bucket: usize)
    requires bucket < 4, members@[bucket as int] == Some(false),
{
    let eligible = all_held_members_scheduled(members);
    assert(!eligible);
}

}
