#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures
        previous.is_some() ==> out == previous.unwrap(),
        previous.is_none() ==> out == proposed,
))]
pub(crate) fn replay_cutoff<T: Copy>(previous: Option<T>, proposed: T) -> T {
    match previous {
        Some(original) => original,
        None => proposed,
    }
}
