#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    requires current.requires(()),
    ensures match capture {
        Some(snapshot) => out == snapshot,
        None => current.ensures((), out),
    },
))]
pub(super) fn query_snapshot<T>(capture: Option<T>, current: impl FnOnce() -> T) -> T {
    match capture {
        Some(snapshot) => snapshot,
        None => current(),
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (catalog_cut is None && tx_basis is None),
))]
pub(super) fn empty_catalog_matches_cut<C, T>(catalog_cut: Option<C>, tx_basis: Option<T>) -> bool {
    catalog_cut.is_none() && tx_basis.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installed_read_selection_refuses_an_empty_cut_after_publication() {
        assert!(empty_catalog_matches_cut(None::<u64>, None::<u64>));
        assert!(!empty_catalog_matches_cut(Some(0), None::<u64>));
        assert!(!empty_catalog_matches_cut(None::<u64>, Some(0)));
    }

    #[test]
    fn installed_read_selection_passes_the_capture_by_value_without_resampling() {
        assert_eq!(query_snapshot(Some(7), || panic!("a captured query resampled")), 7);
        assert_eq!(query_snapshot(None, || 8), 8);
    }
}
