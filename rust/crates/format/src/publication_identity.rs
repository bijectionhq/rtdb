#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (left@ == right@),
))]
pub fn same_publication_digest(left: [u8; 32], right: [u8; 32]) -> bool {
    let mut index = 0usize;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= 32,
            forall|j: int| 0 <= j < index ==> #[trigger] left@[j] == right@[j],
        decreases 32 - index,
    ))]
    while index < 32 {
        if left[index] != right[index] {
            #[cfg(verus_keep_ghost)]
            proof! {
                assert(left@[index as int] != right@[index as int]);
                assert(left@ != right@);
            }
            return false;
        }
        index += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(left@ =~= right@); }
    true
}
