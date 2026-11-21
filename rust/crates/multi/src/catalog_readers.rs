use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Cas, CatalogReader, DeploymentRecord, Directory, Lifecycle, StorageNamespaces, StorageRegion};
use rtdb_format::keys::StorageRoot;
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, ReplicaPosition, Result, StorageEpoch};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::admin::update_installation;
use crate::catalog_readers::admission::admits;

#[path = "catalog_reader_admission.rs"]
mod admission;

#[derive(Clone)]
pub(crate) struct CatalogReaders {
    directory: Arc<dyn Directory>,
    tasks: TaskTracker,
    deployment: String,
    residence: String,
    namespaces: StorageNamespaces,
    storage_root: String,
    replica_epoch: StorageEpoch,
    owner: Uuid,
}

impl CatalogReaders {
    pub fn new(directory: Arc<dyn Directory>, tasks: TaskTracker, record: &DeploymentRecord,
        storage_root: String, namespaces: StorageNamespaces, owner: Uuid) -> Self {
        Self { directory, tasks, deployment: record.id.clone(), residence: record.storage_prefix(),
            storage_root, namespaces, replica_epoch: StorageEpoch(record.log_epoch), owner }
    }

    pub async fn retain(&self, cut: Option<CatalogCut>) -> Result<Arc<dyn Send + Sync>> {
        let matches_cut = match cut {
            Some(cut) => self.matches_cut(cut)?,
            None => true,
        };
        if !matches_cut { return Err(closed()); }
        let reader = Uuid::new_v4();
        let (ready, admitted) = oneshot::channel();
        let (release, drained) = oneshot::channel::<()>();
        let worker = self.clone();
        self.tasks.spawn(async move {
            let result = worker.register(reader, cut).await;
            let opened = result.is_ok();
            let uncertain = result.as_ref().err().is_some_and(|error| !matches!(
                error.code.as_str(), CLOSED | "rtdb.multi/unknown-installation"
            ));
            if ready.send(result.map(|()| Arc::new(release) as Arc<dyn Send + Sync>)).is_ok() && opened {
                let _ = drained.await;
            }
            if opened || uncertain {
                loop {
                    match worker.release(reader, uncertain).await {
                        Ok(()) => break,
                        Err(error) => eprintln!("Catalog reader {reader} for {} is pending release: {}", worker.deployment, error.code),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });
        admitted.await.map_err(|_| closed())?
    }

    fn matches_cut(&self, cut: CatalogCut) -> Result<bool> {
        let root = StorageRoot::new(cut.storage_version, cut.storage_epoch)?;
        let floor = ReplicaPosition::from_epoch_offset(self.replica_epoch, -1)?;
        Ok(root.key() == self.storage_root && cut.replica_epoch == self.replica_epoch
            && (cut.through == floor || cut.through > floor && cut.through.epoch() == self.replica_epoch))
    }

    async fn register(&self, reader: Uuid, cut: Option<CatalogCut>) -> Result<()> {
        update_installation(self.directory.as_ref(), &self.deployment, |record| {
            let open_owner = matches!(record.storage_regions.get(&self.owner),
                Some(StorageRegion::Local { .. } | StorageRegion::Remote { archive: None, closed: false, .. }));
            if !admits(
                matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined),
                open_owner,
                record.storage_prefix() == self.residence
                    && record.storage_namespaces.as_ref().is_none_or(|namespaces| *namespaces == self.namespaces),
                record.log_epoch,
                self.replica_epoch.0,
            ) {
                return Err(closed());
            }
            record.catalog_readers.insert(reader, CatalogReader {
                owner: self.owner, storage_root: self.storage_root.clone(), replica_epoch: self.replica_epoch.0, cut,
            });
            Ok(())
        }).await.map(|_| ())
    }

    async fn release(&self, reader: Uuid, uncertain: bool) -> Result<()> {
        for _ in 0..8 {
            let Some(mut record) = self.directory.deployment(&self.deployment).await? else { return Ok(()); };
            if record.catalog_readers.remove(&reader).is_none() && !uncertain { return Ok(()); }
            // Even an absent reader needs a versioned fence after an uncertain admission.
            let version = record.version;
            if let Cas::Applied(_) = self.directory.replace_deployment(record, version).await? {
                return Ok(());
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/catalog-reader-release", "The installation changed while closing a catalog reader"))
    }
}

const CLOSED: &str = "rtdb.multi/catalog-reader-closed";

fn closed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, CLOSED, "The captured catalog does not belong to an open serving residence")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::MemoryDirectory;
    use rtdb_format::keys::CURRENT_STORAGE_VERSION;
    use rtdb_storage::IoRegion;

    async fn serving() -> (CatalogReaders, Arc<MemoryDirectory>, DeploymentRecord) {
        let directory = MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.log_epoch = 3;
        let owner = Uuid::new_v4();
        record.storage_regions.insert(owner, StorageRegion::Local {
            archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None,
        });
        let record = directory.create_deployment(record).await.unwrap();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
        let readers = CatalogReaders::new(directory.clone(), TaskTracker::new(), &record, root, namespaces, owner);
        (readers, directory, record)
    }

    #[tokio::test]
    async fn retained_provider_io_keeps_an_opaque_capture_registered_after_cancellation() {
        let (readers, directory, origin) = serving().await;
        let permit = readers.retain(None).await.unwrap();
        let region = IoRegion::new();
        let work_region = Arc::clone(&region);
        let (entered, running) = oneshot::channel();
        let (release, finish) = std::sync::mpsc::channel();
        let waiter = tokio::spawn(IoRegion::retaining(permit, async move {
            work_region.run_blocking(move || {
                entered.send(()).unwrap();
                finish.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(())
            }).await
        }));
        tokio::time::timeout(Duration::from_secs(5), running).await.unwrap().unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let record = directory.deployment(&origin.id).await.unwrap().unwrap();
        assert_eq!(record.catalog_readers.values().map(|reader| reader.cut).collect::<Vec<_>>(), [None]);
        release.send(()).unwrap();
        region.close_and_wait().await;
        readers.tasks.close();
        tokio::time::timeout(Duration::from_secs(5), readers.tasks.wait()).await.unwrap();
        assert!(directory.deployment(&origin.id).await.unwrap().unwrap().catalog_readers.is_empty());
    }

    #[tokio::test]
    async fn a_new_journal_or_closed_owner_refuses_the_old_capture() {
        let (readers, directory, origin) = serving().await;
        let mut cut = CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0), replica_epoch: readers.replica_epoch,
            through: ReplicaPosition::from_epoch_offset(readers.replica_epoch, -1).unwrap(),
        };
        let permit = readers.retain(Some(cut)).await.unwrap();
        cut.replica_epoch = StorageEpoch(2);
        assert_eq!(readers.retain(Some(cut)).await.err().unwrap().code, CLOSED);
        update_installation(directory.as_ref(), &origin.id, |record| { record.log_epoch += 1; Ok(()) }).await.unwrap();
        assert_eq!(readers.retain(None).await.err().unwrap().code, CLOSED);
        crate::store::release_region(directory.as_ref(), &origin.id, readers.owner, false).await.unwrap();
        assert!(directory.deployment(&origin.id).await.unwrap().unwrap().catalog_readers.is_empty());
        drop(permit);
        readers.tasks.close();
        tokio::time::timeout(Duration::from_secs(5), readers.tasks.wait()).await.unwrap();
    }
}
