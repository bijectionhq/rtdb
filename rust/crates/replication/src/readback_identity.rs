#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(matched =>
    ensures matched == (observed == Some(expected)),
))]
pub(crate) fn matching_present_integer(observed: Option<i64>, expected: i64) -> bool {
    match observed {
        Some(observed) => observed == expected,
        None => false,
    }
}

#[cfg(not(verus_keep_ghost))]
pub(crate) type ReplicaFingerprint = [u8; 32];

#[cfg(not(verus_keep_ghost))]
pub(crate) fn fingerprint(frame: &[u8]) -> ReplicaFingerprint {
    use sha2::{Digest, Sha256};
    Sha256::digest(frame).into()
}
