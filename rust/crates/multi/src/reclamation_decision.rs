use rtdb_format::catalog::same_publication_digest;

pub(crate) fn matches_object_fence(
    fenced_name: [u8; 32], fenced_version: [u8; 32], name: [u8; 32], version: [u8; 32],
) -> bool {
    same_publication_digest(fenced_name, name) && same_publication_digest(fenced_version, version)
}

pub(crate) fn release_is_covered(through: i64, trim_floor: u64, selected_cut: u64) -> bool {
    through >= 0 && (through as u64) < trim_floor && trim_floor <= selected_cut
}

pub(crate) fn erasure_requires_custody_join(erasure_floor: u64) -> bool {
    erasure_floor != 0
}

pub(crate) fn retains_serving_slot(same_origin: bool, same_attempt: bool) -> bool {
    same_origin && same_attempt
}

pub(crate) fn retains_publication_scope(deleted: bool, disposed: bool, regions_empty: bool) -> bool {
    !deleted || !disposed || !regions_empty
}
