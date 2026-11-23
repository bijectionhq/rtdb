use vstd::prelude::*;

use crate::production::{MemberStart, document_member, ordered};

verus! {

// LAW SEARCH-MEMBER-1.
pub(crate) fn document_uses_the_member_covering_its_offset(starts: &[MemberStart], doc: u32, expected: usize) -> (owner: usize)
    requires
        starts@.len() > 0, starts@[0].doc == 0, ordered(starts@),
        expected < starts@.len(),
        starts@[expected as int].doc <= doc,
        forall |i: int| (expected as int) < i < starts@.len() ==> (#[trigger] starts@[i]).doc > doc,
    ensures owner == expected,
{
    document_member(starts, doc)
}

} // verus!
