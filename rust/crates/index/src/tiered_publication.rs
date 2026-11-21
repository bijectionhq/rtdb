#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(verus_keep_ghost)]
verus! {
    pub open spec fn member_at(candidate: Option<(i64, bool)>, block: i64) -> bool {
        candidate == Some((block, true))
    }

    pub open spec fn exact_group(members: Seq<Option<(i64, bool)>>, block: i64) -> bool {
        forall |bucket: int| 0 <= bucket < 4 ==> member_at(#[trigger] members[bucket], block)
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == member_at(candidate, block),
))]
fn is_usable(candidate: Option<(i64, bool)>, block: i64) -> bool {
    match candidate {
        Some((actual, usable)) => actual == block && usable,
        None => false,
    }
}

/// Each bucket supplies the actual block and whether the trie is nascent or live.
#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == exact_group(members@, block),
))]
pub(crate) fn can_publish(members: [Option<(i64, bool)>; 4], block: i64) -> bool {
    is_usable(members[0], block)
        && is_usable(members[1], block)
        && is_usable(members[2], block)
        && is_usable(members[3], block)
}
