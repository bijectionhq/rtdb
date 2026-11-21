#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (block < latest && (block as int) + (keep as int) <= (latest as int)),
))]
pub(crate) fn block_is_eligible(block: i64, latest: i64, keep: usize) -> bool {
    block != latest && (block as i128) <= (latest as i128) - (keep as i128)
}

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn has_survivor(members: Seq<Option<bool>>) -> bool {
        exists |i: int| 0 <= i < 4 && #[trigger] members[i] == Some(false)
    }
}

/// Each slot is absent, held and scheduled for collection, or held and still retained.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == !has_survivor(members@),
))]
pub(crate) fn all_held_members_scheduled(members: [Option<bool>; 4]) -> bool {
    // A missing member imposes no obligation. A held member must be in this batch.
    members[0] != Some(false)
        && members[1] != Some(false)
        && members[2] != Some(false)
        && members[3] != Some(false)
}
