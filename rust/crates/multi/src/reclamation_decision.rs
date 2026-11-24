#![allow(unexpected_cfgs)]

use rtdb_format::catalog::same_publication_digest;

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (fenced_name@ == name@ && fenced_version@ == version@),
))]
pub(crate) fn matches_object_fence(
    fenced_name: [u8; 32], fenced_version: [u8; 32], name: [u8; 32], version: [u8; 32],
) -> bool {
    same_publication_digest(fenced_name, name) && same_publication_digest(fenced_version, version)
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (0 <= through as int && (through as int) < trim_floor as int && trim_floor <= selected_cut),
))]
pub(crate) fn release_is_covered(through: i64, trim_floor: u64, selected_cut: u64) -> bool {
    through >= 0 && (through as u64) < trim_floor && trim_floor <= selected_cut
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (erasure_floor != 0),
))]
pub(crate) fn erasure_requires_custody_join(erasure_floor: u64) -> bool {
    erasure_floor != 0
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (same_origin && same_attempt),
))]
pub(crate) fn retains_serving_slot(same_origin: bool, same_attempt: bool) -> bool {
    same_origin && same_attempt
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (!deleted || !disposed || !regions_empty),
))]
pub(crate) fn retains_publication_scope(deleted: bool, disposed: bool, regions_empty: bool) -> bool {
    !deleted || !disposed || !regions_empty
}
