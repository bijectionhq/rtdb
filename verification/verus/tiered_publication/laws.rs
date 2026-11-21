use crate::production::can_publish;
use vstd::prelude::*;

verus! {

// LAW COMPACT-1-EXACT-GROUP.
pub fn an_absent_bucket_keeps_the_parent(
    members: [Option<(i64, bool)>; 4], block: i64, bucket: usize,
)
    requires bucket < 4, members@[bucket as int] == None,
{
    let publish = can_publish(members, block);
    assert(!publish);
}

// LAW COMPACT-1-BLOCK.
pub fn another_generation_keeps_the_parent(target: i64, other: i64)
    requires target < other,
{
    let good = Some((target, true));
    let publish = can_publish([good, good, Some((other, true)), good], target);
    assert(!publish);
}

// LAW COMPACT-1-LIVE.
pub fn a_garbage_member_keeps_the_parent(block: i64)
{
    let good = Some((block, true));
    let publish = can_publish([good, good, Some((block, false)), good], block);
    assert(!publish);
}

// LAW COMPACT-1-PROMOTION.
pub fn all_four_usable_members_can_replace_the_parent(block: i64)
{
    let good = Some((block, true));
    let publish = can_publish([good; 4], block);
    assert(publish);
}

}
