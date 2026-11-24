pub(super) fn keep_inherited_event(origin_selected: bool) -> bool {
    !origin_selected
}

pub(super) fn keep_local_event(transaction: i64, ordinal: u64, before: Option<(i64, u64)>) -> bool {
    match before {
        Some((marker_tx, marker_ordinal)) => {
            marker_tx < transaction || (marker_tx == transaction && marker_ordinal <= ordinal)
        }
        None => true,
    }
}

pub(super) fn write_survivor_trie(rows: u32) -> bool {
    rows != 0
}
