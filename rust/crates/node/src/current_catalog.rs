use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

use parking_lot::Mutex;
use rtdb_index::{CatalogPublicationObserver, PartitionState};
use rtdb_storage::cache::HeldCatalogRead;
use rtdb_types::CatalogCut;

/// A database's own share of the storage hold on the catalog version its partition publishes now.
/// Readers of that version share an already registered hold rather than registering one each; the share ends as soon as
/// the partition publishes another cut, after which the version lives on only while readers hold it.
pub(crate) struct CurrentCatalog {
    state: Mutex<Current>,
    /// The address of the held share, so that a reader already sharing it confirms that without taking the lock.
    held: AtomicUsize,
}

struct Current { cut: Option<CatalogCut>, share: Option<Arc<HeldCatalogRead>> }

impl CurrentCatalog {
    pub(crate) fn observe(partition: &PartitionState) -> Arc<Self> {
        let current = Arc::new(Self { state: Mutex::new(Current { cut: None, share: None }), held: AtomicUsize::new(0) });
        let observer: Arc<dyn CatalogPublicationObserver> = current.clone();
        partition.observe_publication(&observer);
        current
    }

    /// Keeps `hold` as the database's share if it is a registered hold of exactly the published cut.
    /// Ordinary reads hold their cut anchored at its checkpoint, so an anchored hold is a share like any other.
    pub(crate) fn offer(&self, hold: &Arc<HeldCatalogRead>) {
        let address = Arc::as_ptr(hold) as usize;
        if self.held.load(Ordering::Acquire) == address || !hold.is_registered() || hold.cut().is_none() {
            return;
        }
        let replaced = {
            let mut state = self.state.lock();
            if state.cut != hold.cut() { return; }
            self.held.store(address, Ordering::Release);
            state.share.replace(Arc::clone(hold))
        };
        // Releasing a registration can remove its storage ticket, so it happens outside the lock.
        drop(replaced);
    }

    #[cfg(test)]
    fn share(&self) -> Option<Arc<HeldCatalogRead>> { self.state.lock().share.clone() }
}

impl CatalogPublicationObserver for CurrentCatalog {
    fn published(&self, cut: Option<CatalogCut>) -> Option<Box<dyn Send>> {
        let mut state = self.state.lock();
        state.cut = cut;
        if state.share.as_ref().is_none_or(|share| share.cut() == cut) { return None; }
        self.held.store(0, Ordering::Release);
        state.share.take().map(|share| Box::new(share) as Box<dyn Send>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rtdb_storage::{LocalStore, ObjectStore, cache::BufferPool};
    use rtdb_types::{ReplicaPosition, StorageEpoch, StorageVersion};

    /// The ledger's reader tickets, one per held registration, by file name.
    fn tickets(root: &std::path::Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(root.join(".tmp/catalog-ledger")).map_or(Vec::new(), |entries| {
            entries.filter_map(|entry| entry.ok()).map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| name.starts_with("reader-")).collect()
        });
        names.sort();
        names
    }

    fn cut(through: i64) -> CatalogCut {
        CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0), through: ReplicaPosition(through) }
    }

    #[tokio::test]
    async fn sequential_readers_of_the_current_version_register_once_and_a_superseded_version_ends_with_its_last_reader() {
        let location = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(location.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
        let collector = LocalStore::open(location.path()).unwrap().with_catalog_ledger(StorageEpoch(0));
        let pool = BufferPool::new(store, 1 << 20);
        let partition = PartitionState::empty(Default::default()).unwrap();
        partition.publish_catalog_cut(cut(0)).unwrap();
        let current = CurrentCatalog::observe(&partition);

        let first = pool.hold_catalog(Some(cut(0))).await.unwrap();
        current.offer(&first);
        drop(first);
        let registration = tickets(location.path());
        assert_eq!(registration.len(), 1, "the database keeps the current version registered between readers");
        for _ in 0..10 {
            let hold = pool.hold_catalog(Some(cut(0))).await.unwrap();
            current.offer(&hold);
            drop(hold);
            assert_eq!(tickets(location.path()), registration, "a later reader of the current version shares its registration");
        }

        let reader = pool.hold_catalog(Some(cut(0))).await.unwrap();
        partition.publish_catalog_cut(cut(1)).unwrap();
        assert!(current.share().is_none(), "a newer cut supersedes the database's share");
        assert_eq!(tickets(location.path()), registration, "the superseded version lives on while a reader holds it");
        assert!(!collector.advance_catalog_floor(cut(1)).await.unwrap(), "its reader still blocks the floor");
        current.offer(&reader);
        assert!(current.share().is_none(), "a superseded version does not become current again");
        drop(reader);
        assert!(tickets(location.path()).is_empty(), "the superseded version ends exactly with its last reader");
        assert!(collector.advance_catalog_floor(cut(1)).await.unwrap(), "the floor advances as soon as it ends");

        let next = pool.hold_catalog(Some(cut(1))).await.unwrap();
        current.offer(&next);
        drop(next);
        assert_eq!(tickets(location.path()).len(), 1);
        assert!(collector.advance_catalog_floor(cut(1)).await.unwrap(), "a share of the current version never blocks a floor it covers");
        drop(current);
        assert!(tickets(location.path()).is_empty(), "a closed database holds nothing");
    }

    #[tokio::test]
    async fn sequential_readers_holding_the_current_version_anchored_register_once() {
        let location = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(location.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
        let pool = BufferPool::new(store, 1 << 20);
        let partition = PartitionState::empty(Default::default()).unwrap();
        partition.publish_catalog_cut(cut(0)).unwrap();
        let current = CurrentCatalog::observe(&partition);
        let anchor = rtdb_types::CatalogCheckpoint { boundary: ReplicaPosition(-1), root: [0; 32] };
        let mut registration = None;
        for _ in 0..10 {
            let hold = pool.hold_anchored_catalog(cut(0), anchor, None).await.unwrap();
            current.offer(&hold);
            drop(hold);
            let now = tickets(location.path());
            assert_eq!(now.len(), 1, "the database keeps the anchored current version registered between readers");
            assert_eq!(registration.get_or_insert_with(|| now.clone()), &now, "later readers share the first registration");
        }
    }
}
