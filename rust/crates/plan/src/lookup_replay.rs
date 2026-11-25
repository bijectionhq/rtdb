#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires forall|i: int| 0 <= i < lookups@.len() ==> same.requires((&lookups@[i],)),
    ensures out matches Ok(true) ==> forall|i: int| 0 <= i < lookups@.len() ==> same.ensures((&lookups@[i],), Ok(true)),
))]
pub(crate) fn replays_every<L, E>(lookups: &[L], same: impl Fn(&L) -> Result<bool, E>) -> Result<bool, E> {
    let mut index = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        index <= lookups@.len(),
        forall|i: int| 0 <= i < lookups@.len() ==> same.requires((&lookups@[i],)),
        forall|i: int| 0 <= i < index ==> same.ensures((&lookups@[i],), Ok(true)),
        decreases lookups@.len() - index,
    ))]
    while index < lookups.len() {
        match same(&lookups[index]) {
            Ok(true) => {}
            other => return other,
        }
        index += 1;
    }
    Ok(true)
}
