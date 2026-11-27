use std::sync::Arc;

use rtdb_directory::{CatalogAdmissionFloor, DeploymentRecord, Directory, Lifecycle, StorageNamespaces, StorageRegion};
use rtdb_format::keys::StorageRoot;
use rtdb_format::log::proto::PublishedObjectVersion;
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCut, ReplicaPosition, Result, StorageEpoch};
use uuid::Uuid;

use crate::admin::update_installation;
use crate::catalog_readers::admission::admits;

#[path = "catalog_reader_admission.rs"]
mod admission;

/// The serving residence's retirement floor in the deployment directory.
/// Readers register nothing: a superseded file stays readable for the garbage-collection window, so an installed
/// retirement is released as soon as the floor records it, as in XTDB. Reader claims an older node left in the record
/// are ignored and leave with their storage region.
#[derive(Clone)]
pub(crate) struct CatalogReaders {
    directory: Arc<dyn Directory>,
    deployment: String,
    residence: String,
    namespaces: StorageNamespaces,
    storage_root: String,
    replica_epoch: StorageEpoch,
    owner: Uuid,
    writer_epoch: Option<u64>,
}

impl CatalogReaders {
    pub fn new(directory: Arc<dyn Directory>, record: &DeploymentRecord,
        storage_root: String, namespaces: StorageNamespaces, owner: Uuid, writer_epoch: Option<u64>) -> Self {
        Self { directory, deployment: record.id.clone(), residence: record.storage_prefix(),
            storage_root, namespaces, replica_epoch: StorageEpoch(record.log_epoch), owner, writer_epoch }
    }

    fn matches_cut(&self, cut: CatalogCut) -> Result<bool> {
        let root = StorageRoot::new(cut.storage_version, cut.storage_epoch)?;
        let floor = ReplicaPosition::from_epoch_offset(self.replica_epoch, -1)?;
        Ok(root.key() == self.storage_root && cut.replica_epoch == self.replica_epoch
            && (cut.through == floor || cut.through > floor && cut.through.epoch() == self.replica_epoch))
    }

    fn residence_is_open(&self, record: &DeploymentRecord) -> bool {
        let open_owner = matches!(record.storage_regions.get(&self.owner),
            Some(StorageRegion::Local { .. } | StorageRegion::Remote { archive: None, closed: false, .. }));
        admits(
            matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined),
            open_owner,
            record.storage_prefix() == self.residence
                && record.storage_namespaces.as_ref().is_none_or(|namespaces| *namespaces == self.namespaces),
            record.log_epoch,
            self.replica_epoch.0,
        )
    }

    fn floor(&self, record: &DeploymentRecord) -> Option<ReplicaPosition> {
        record.catalog_admission_floors.get(&self.storage_root).filter(|floor| {
            floor.residence == self.residence && floor.object_namespace == self.namespaces.objects
                && floor.cut.replica_epoch == self.replica_epoch
        }).map(|floor| floor.cut.through)
    }

    pub fn search_staging_owner(&self) -> Option<rtdb_format::search_manifest::SearchStagingOwner> {
        self.writer_epoch.map(|writer_epoch| rtdb_format::search_manifest::SearchStagingOwner::Directory {
            region: self.owner,
            writer_epoch,
        })
    }

    pub async fn search_staging_drained(&self, staged: rtdb_format::search_manifest::SearchStagingOwner) -> Result<bool> {
        let rtdb_format::search_manifest::SearchStagingOwner::Directory { region, writer_epoch } = staged
            else { return Ok(false); };
        Ok(self.directory.deployment(&self.deployment).await?.is_some_and(|record| {
            self.residence_is_open(&record) && self.writer_epoch == Some(record.epoch)
                && record.holder.is_some() && writer_epoch < record.epoch
                && !record.storage_regions.contains_key(&region)
        }))
    }

    async fn search_release_owner(&self, cut: CatalogCut) -> Result<DeploymentRecord> {
        if !self.matches_cut(cut)? { return Err(closed()); }
        let Some(record) = self.directory.deployment(&self.deployment).await? else { return Err(closed()); };
        if !self.residence_is_open(&record) || self.writer_epoch != Some(record.epoch) || record.holder.is_none() {
            return Err(closed());
        }
        Ok(record)
    }

    pub async fn permits_search_release(&self, bucket: &dyn ObjectStore, cut: CatalogCut, key: &ObjectKey,
        publication: Option<&PublishedObjectVersion>) -> Result<bool> {
        let record = self.search_release_owner(cut).await?;
        let store = format!("{}/{}", self.residence, self.storage_root);
        crate::store::exact_retirement::permits_search_release(self.directory.as_ref(), bucket, &record, &store, key, publication).await
    }

    pub async fn permits_search_releases(&self, bucket: &dyn ObjectStore, cut: CatalogCut,
        items: &[(&ObjectKey, Option<&PublishedObjectVersion>)]) -> Result<Vec<bool>> {
        let record = self.search_release_owner(cut).await?;
        let store = format!("{}/{}", self.residence, self.storage_root);
        crate::store::exact_retirement::permits_search_releases(self.directory.as_ref(), bucket, &record, &store, items, None).await
    }

    pub async fn permits_search_tombstone_prune(&self, bucket: &dyn ObjectStore, cut: CatalogCut, checkpoint: BlockIndex,
        key: &ObjectKey, publication: Option<&PublishedObjectVersion>) -> Result<bool> {
        let (floor, allowed) = self.close_admission(cut, &[None]).await?;
        if !allowed[0] { return Ok(false); }
        let owner = self.search_release_owner(cut).await?;
        if !crate::store::recovery_covers_search_checkpoint(&owner, &floor, checkpoint) {
            return Ok(false);
        }
        let store = format!("{}/{}", self.residence, self.storage_root);
        Ok(crate::store::exact_retirement::permits_search_releases(self.directory.as_ref(), bucket, &owner, &store,
            &[(key, publication)], Some(checkpoint)).await?[0])
    }

    pub async fn advance(&self, installed: CatalogCut) -> Result<bool> {
        Ok(self.close_admission(installed, &[None]).await?.1[0])
    }

    /// Records the installed retirement as this residence's floor and releases every candidate it names.
    /// No reader holds a catalog back; only a closed residence or a superseded writer refuses.
    pub async fn close_admission(&self, installed: CatalogCut, paths: &[Option<&str>]) -> Result<(CatalogAdmissionFloor, Vec<bool>)> {
        if !self.matches_cut(installed)? { return Err(closed()); }
        let floor = CatalogAdmissionFloor {
            residence: self.residence.clone(), object_namespace: self.namespaces.objects, cut: installed,
        };
        let record = update_installation(self.directory.as_ref(), &self.deployment, |record| {
            if !self.residence_is_open(record) || self.writer_epoch != Some(record.epoch) || record.holder.is_none() {
                return Err(closed());
            }
            if self.floor(record).is_none_or(|existing| existing < installed.through) {
                record.catalog_admission_floors.insert(self.storage_root.clone(), floor.clone());
            }
            Ok(())
        }).await?;
        let closed = self.floor(&record).is_some_and(|existing| existing >= installed.through);
        let allowed = vec![closed; paths.len()];
        if self.floor(&record) == Some(installed.through) {
            let event = if paths.iter().any(Option::is_some) { "close-catalog-admission" } else { "advance-floor" };
            crate::residence_trace::record(event, || serde_json::json!({
                "lineage": record.lineage, "installation": self.deployment, "residence": self.residence, "namespace": self.namespaces.objects,
                "root": self.storage_root, "revision": record.version, "epoch": self.replica_epoch,
                "through": installed.through, "eligible_paths": paths.iter().zip(&allowed)
                    .filter_map(|(path, allowed)| allowed.then_some(*path).flatten()).collect::<Vec<_>>(),
            }));
        }
        Ok((floor, allowed))
    }

    pub fn matches_release(&self, floor: &CatalogAdmissionFloor) -> bool {
        floor.residence == self.residence && floor.object_namespace == self.namespaces.objects
            && self.matches_cut(floor.cut).unwrap_or(false)
    }
}

const CLOSED: &str = "rtdb.multi/catalog-reader-closed";

fn closed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, CLOSED, "The captured catalog does not belong to an open serving residence")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::{CatalogReader, MemoryDirectory, ReplaceOutcome};
    use rtdb_format::keys::CURRENT_STORAGE_VERSION;

    async fn serving() -> (CatalogReaders, Arc<MemoryDirectory>, DeploymentRecord) {
        let directory = MemoryDirectory::new();
        let (readers, record) = serving_on(directory.clone()).await;
        (readers, directory, record)
    }

    async fn serving_on(directory: Arc<dyn Directory>) -> (CatalogReaders, DeploymentRecord) {
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.log_epoch = 3;
        record.holder = Some(rtdb_directory::MemberId("writer".into()));
        let owner = Uuid::new_v4();
        record.storage_regions.insert(owner, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let record = directory.create_deployment(record).await.unwrap();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
        let readers = CatalogReaders::new(directory, &record, root, namespaces, owner, Some(record.epoch));
        (readers, record)
    }

    #[tokio::test]
    async fn search_staging_requires_both_writer_turnover_and_region_drain() {
        let (mut readers, directory, record) = serving().await;
        let region = Uuid::new_v4();
        let writer_epoch = 1;
        let staged = rtdb_format::search_manifest::SearchStagingOwner::Directory { writer_epoch, region };
        update_installation(directory.as_ref(), &record.id, |current| {
            current.epoch = writer_epoch;
            let held = current.storage_regions[&readers.owner].clone();
            current.storage_regions.insert(region, held);
            Ok(())
        }).await.unwrap();
        readers.writer_epoch = Some(writer_epoch);
        assert!(!readers.search_staging_drained(staged).await.unwrap());
        update_installation(directory.as_ref(), &record.id, |current| {
            current.epoch += 1;
            Ok(())
        }).await.unwrap();
        readers.writer_epoch = Some(writer_epoch + 1);
        assert!(!readers.search_staging_drained(staged).await.unwrap());
        update_installation(directory.as_ref(), &record.id, |current| {
            current.storage_regions.remove(&region);
            Ok(())
        }).await.unwrap();
        assert!(readers.search_staging_drained(staged).await.unwrap());
    }

    #[tokio::test]
    async fn a_reader_claim_left_by_an_older_node_never_blocks_the_floor_or_a_file() {
        let (readers, directory, origin) = serving().await;
        let old = CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0), replica_epoch: readers.replica_epoch,
            through: ReplicaPosition::from_epoch_offset(readers.replica_epoch, 0).unwrap(),
        };
        let installed = CatalogCut {
            through: ReplicaPosition::from_epoch_offset(readers.replica_epoch, 1).unwrap(),
            ..old
        };
        let held_path = rtdb_format::keys::TableSlug::recorded("public$held").unwrap()
            .data_key(&"l01-rc-b00".parse().unwrap());
        update_installation(directory.as_ref(), &origin.id, |record| {
            record.catalog_readers.insert(Uuid::new_v4(), CatalogReader {
                owner: readers.owner, storage_root: readers.storage_root.clone(), replica_epoch: readers.replica_epoch.0,
                cut: Some(old), catalog_file_paths: Some(rtdb_types::CatalogFilePaths { members: vec![held_path.clone()] }),
            });
            record.catalog_readers.insert(Uuid::new_v4(), CatalogReader {
                owner: readers.owner, storage_root: readers.storage_root.clone(), replica_epoch: readers.replica_epoch.0,
                cut: None, catalog_file_paths: None,
            });
            Ok(())
        }).await.unwrap();

        let (floor, allowed) = readers.close_admission(installed, &[Some(&held_path), None]).await.unwrap();
        assert_eq!(allowed, [true, true], "the GC window, not a reader claim, bounds what a retirement may release");
        assert_eq!(floor.cut, installed);
        let current = directory.deployment(&origin.id).await.unwrap().unwrap();
        assert_eq!(current.catalog_admission_floors[&readers.storage_root].cut, installed);
        assert!(readers.advance(old).await.unwrap(), "replaying an earlier receipt is already covered");
        update_installation(directory.as_ref(), &origin.id, |record| { record.epoch += 1; Ok(()) }).await.unwrap();
        assert_eq!(readers.advance(installed).await.unwrap_err().code, CLOSED, "a superseded writer cannot move the floor");
    }

    #[tokio::test]
    async fn a_stale_floor_write_loses_its_compare_and_replace() {
        let (readers, directory, origin) = serving().await;
        let newer = CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0), replica_epoch: readers.replica_epoch,
            through: ReplicaPosition::from_epoch_offset(readers.replica_epoch, 1).unwrap(),
        };
        let mut stale = directory.deployment(&origin.id).await.unwrap().unwrap();
        let stale_version = stale.version;
        assert!(readers.advance(newer).await.unwrap());
        stale.catalog_admission_floors.clear();
        assert!(!matches!(directory.replace_deployment(stale, stale_version).await, ReplaceOutcome::Applied(_)));
    }
}
