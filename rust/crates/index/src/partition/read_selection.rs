#![allow(unexpected_cfgs)]

#[cfg(not(verus_keep_ghost))]
use rtdb_types::CatalogCut;
#[cfg(verus_keep_ghost)]
use crate::CatalogCut;
#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == (published == requested),
))]
pub(super) fn catalog_matches_cut(published: CatalogCut, requested: CatalogCut) -> bool {
    published.storage_version.0 == requested.storage_version.0
        && published.storage_epoch.0 == requested.storage_epoch.0
        && published.replica_epoch.0 == requested.replica_epoch.0
        && published.through.0 == requested.through.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{ReplicaPosition, StorageEpoch, StorageVersion};

    #[test]
    fn installed_read_selection_rejects_ahead_and_foreign_catalogs() {
        let selected = CatalogCut {
            storage_version: StorageVersion(1), storage_epoch: StorageEpoch(2),
            replica_epoch: StorageEpoch(3), through: ReplicaPosition(4),
        };
        assert!(catalog_matches_cut(selected, selected));
        assert!(!catalog_matches_cut(CatalogCut { through: ReplicaPosition(5), ..selected }, selected));
        assert!(!catalog_matches_cut(CatalogCut { storage_version: StorageVersion(2), ..selected }, selected));
        assert!(!catalog_matches_cut(CatalogCut { storage_epoch: StorageEpoch(3), ..selected }, selected));
        assert!(!catalog_matches_cut(CatalogCut { replica_epoch: StorageEpoch(4), ..selected }, selected));
    }
}
