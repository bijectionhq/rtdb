#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (sealed@ == framed@),
))]
fn same_digest(sealed: &[u8; 32], framed: &[u8]) -> bool {
    if framed.len() != 32 {
        return false;
    }
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(
        invariant
            index <= 32,
            framed@.len() == 32,
            forall|i: int| 0 <= i < index ==> #[trigger] sealed@[i] == framed@[i],
        decreases 32 - index,
    ))]
    while index < 32 {
        if sealed[index] != framed[index] {
            #[cfg(verus_keep_ghost)]
            proof! { assert(sealed@[index as int] != framed@[index as int]); }
            return false;
        }
        index += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! { assert(sealed@ =~= framed@); }
    true
}

#[cfg_attr(verus_keep_ghost, verus_spec(valid =>
    ensures valid == (before > 0 && certificate@ != Seq::new(32, |i: int| 0u8)),
))]
pub(crate) fn valid_journal_cutoff(before: i64, certificate: &[u8; 32]) -> bool {
    let zero = [0u8; 32];
    #[cfg(verus_keep_ghost)]
    proof! { assert(zero@ =~= Seq::new(32, |i: int| 0u8)); }
    before > 0 && !same_digest(certificate, &zero)
}

#[cfg_attr(verus_keep_ghost, verus_spec(matches =>
    ensures matches == (match (sealed, framed) {
        (None, None) => true,
        (Some((before, digest)), Some((other_before, other_digest))) =>
            before == other_before && digest@ == other_digest@,
        _ => false,
    }),
))]
pub fn journal_frame_matches(
    sealed: Option<(i64, [u8; 32])>, framed: Option<(i64, &[u8])>,
) -> bool {
    match (sealed, framed) {
        (None, None) => true,
        (Some((before, digest)), Some((other_before, other_digest))) =>
            before == other_before && same_digest(&digest, other_digest),
        _ => false,
    }
}
