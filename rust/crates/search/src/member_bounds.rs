#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

/// A member's starting offsets in the shared page and document sequences.
#[derive(Clone, Copy)]
#[cfg_attr(verus_keep_ghost, verus_verify)]
pub(crate) struct MemberStart {
    pub(crate) page: u32,
    pub(crate) doc: u32,
}

#[cfg(verus_keep_ghost)]
verus! {
    pub(crate) open spec fn ordered(starts: Seq<MemberStart>) -> bool {
        forall |i: int, j: int| 0 <= i <= j < starts.len() ==>
            (#[trigger] starts[i]).doc <= (#[trigger] starts[j]).doc
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(owner =>
    requires starts@.len() > 0, starts@[0].doc == 0, ordered(starts@),
    ensures
        owner < starts@.len(),
        starts@[owner as int].doc <= doc,
        forall |i: int| (owner as int) < i < starts@.len() ==> (#[trigger] starts@[i]).doc > doc,
))]
pub(crate) fn document_member(starts: &[MemberStart], doc: u32) -> usize {
    let mut lower = 1;
    let mut upper = starts.len();
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            1 <= lower <= upper <= starts@.len(),
            ordered(starts@),
            forall |i: int| 0 <= i < lower ==> (#[trigger] starts@[i]).doc <= doc,
            forall |i: int| upper <= i < starts@.len() ==> (#[trigger] starts@[i]).doc > doc,
        decreases upper - lower,
    ))]
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        if starts[middle].doc <= doc {
            lower = middle + 1;
        } else {
            upper = middle;
        }
    }
    lower - 1
}
