#![allow(unexpected_cfgs)]

use crate::publication_identity::same_publication_digest;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> (!requires_manifest || has_manifest) && match stored {
        Some(digest) => digest@ == chosen@,
        None => false,
    },
))]
pub(crate) fn matches_catalog(
    chosen: [u8; 32],
    stored: Option<[u8; 32]>,
    requires_manifest: bool,
    has_manifest: bool,
) -> bool {
    if requires_manifest && !has_manifest {
        return false;
    }
    matches_table(stored, chosen)
}

#[cfg_attr(verus_keep_ghost, verus_spec(accepted =>
    ensures accepted <==> match expected {
        Some(digest) => digest@ == loaded@,
        None => false,
    },
))]
pub(crate) fn matches_table(expected: Option<[u8; 32]>, loaded: [u8; 32]) -> bool {
    match expected {
        Some(digest) => same_publication_digest(digest, loaded),
        None => false,
    }
}
