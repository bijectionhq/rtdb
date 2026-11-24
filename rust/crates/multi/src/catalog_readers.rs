use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rtdb_directory::{Cas, CatalogAdmissionFloor, CatalogReader, DeploymentRecord, Directory, Lifecycle, StorageNamespaces, StorageRegion};
use rtdb_format::keys::StorageRoot;
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, ReplicaPosition, Result, StorageEpoch};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::admin::update_installation;
use crate::catalog_readers::admission::{admits, admits_after_floor};

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
    writer_epoch: Option<u64>,
    pending: Arc<Mutex<PendingReaders>>,
}

#[derive(Default)]
struct PendingReaders {
    draining: bool,
    registrations: VecDeque<ReaderRequest>,
    releases: VecDeque<ReaderRequest>,
}

struct ReaderRequest {
    change: ReaderChange,
    answer: oneshot::Sender<Result<()>>,
}

enum ReaderChange {
    Register { reader: Uuid, cut: Option<CatalogCut>, paths: Option<Arc<[String]>> },
    Release { reader: Uuid, uncertain: bool },
}

impl CatalogReaders {
    pub fn new(directory: Arc<dyn Directory>, tasks: TaskTracker, record: &DeploymentRecord,
        storage_root: String, namespaces: StorageNamespaces, owner: Uuid, writer_epoch: Option<u64>) -> Self {
        Self { directory, tasks, deployment: record.id.clone(), residence: record.storage_prefix(),
            storage_root, namespaces, replica_epoch: StorageEpoch(record.log_epoch), owner, writer_epoch, pending: Arc::default() }
    }

    pub async fn retain(&self, cut: Option<CatalogCut>) -> Result<Arc<dyn Send + Sync>> {
        self.retain_with_paths(cut, None).await
    }

    pub async fn retain_with_paths(&self, cut: Option<CatalogCut>, paths: Option<Arc<[String]>>) -> Result<Arc<dyn Send + Sync>> {
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
            let result = worker.change(ReaderChange::Register { reader, cut, paths }).await;
            let opened = result.is_ok();
            let uncertain = result.as_ref().err().is_some_and(|error| !matches!(
                error.code.as_str(), CLOSED | CATALOG_NOT_CURRENT | "rtdb.multi/unknown-installation"
            ));
            if ready.send(result.map(|()| Arc::new(release) as Arc<dyn Send + Sync>)).is_ok() && opened {
                let _ = drained.await;
            }
            if opened || uncertain {
                loop {
                    match worker.change(ReaderChange::Release { reader, uncertain }).await {
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

    pub async fn advance(&self, installed: CatalogCut) -> Result<bool> {
        Ok(self.close_admission(installed, &[None]).await?.1[0])
    }

    /// Serializes closing old admissions with testing the current holders for each file.
    pub async fn close_admission(&self, installed: CatalogCut, paths: &[Option<&str>]) -> Result<(CatalogAdmissionFloor, Vec<bool>)> {
        if !self.matches_cut(installed)? { return Err(closed()); }
        let floor = CatalogAdmissionFloor {
            residence: self.residence.clone(), object_namespace: self.namespaces.objects, cut: installed,
        };
        let has_exact_witness = paths.iter().any(Option::is_some);
        let record = update_installation(self.directory.as_ref(), &self.deployment, |record| {
            if !self.residence_is_open(record) || self.writer_epoch != Some(record.epoch) || record.holder.is_none() {
                return Err(closed());
            }
            if self.floor(record).is_none_or(|existing| existing < installed.through)
                && (has_exact_witness || readers_allow(record, &self.storage_root, installed, None))
            {
                record.catalog_admission_floors.insert(self.storage_root.clone(), floor.clone());
            }
            Ok(())
        }).await?;
        let closed = self.floor(&record).is_some_and(|existing| existing >= installed.through);
        let allowed: Vec<_> = paths.iter().map(|path| closed && readers_allow(&record, &self.storage_root, installed, *path)).collect();
        if self.floor(&record) == Some(installed.through)
            && (has_exact_witness || readers_allow(&record, &self.storage_root, installed, None))
        {
            let event = if has_exact_witness { "close-catalog-admission" } else { "advance-floor" };
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

    async fn change(&self, change: ReaderChange) -> Result<()> {
        let (answer, result) = oneshot::channel();
        let start = {
            let mut pending = self.pending.lock();
            let queue = match &change {
                ReaderChange::Register { .. } => &mut pending.registrations,
                ReaderChange::Release { .. } => &mut pending.releases,
            };
            queue.push_back(ReaderRequest { change, answer });
            !std::mem::replace(&mut pending.draining, true)
        };
        if start {
            let readers = self.clone();
            self.tasks.spawn(async move { readers.drain_changes().await });
        }
        result.await.map_err(|_| closed())?
    }

    async fn drain_changes(&self) {
        loop {
            let batch = {
                let mut pending = self.pending.lock();
                let queue = if !pending.releases.is_empty() {
                    &mut pending.releases
                } else if !pending.registrations.is_empty() {
                    &mut pending.registrations
                } else {
                    pending.draining = false;
                    return;
                };
                let count = queue.len().min(MAX_READER_CHANGES);
                queue.drain(..count).collect::<Vec<_>>()
            };
            let answers = self.apply_changes(&batch).await;
            for (index, request) in batch.into_iter().enumerate() {
                let result = match &answers {
                    Ok(answers) => answers[index].clone(),
                    Err(error) => Err(error.clone()),
                };
                let _ = request.answer.send(result);
            }
        }
    }

    async fn apply_changes(&self, batch: &[ReaderRequest]) -> Result<Vec<Result<()>>> {
        let mut current = self.directory.deployment(&self.deployment).await?;
        for _ in 0..8 {
            let Some(mut record) = current else {
                return Ok(batch.iter().map(|request| match &request.change {
                    ReaderChange::Register { .. } => Err(closed()),
                    ReaderChange::Release { .. } => Ok(()),
                }).collect());
            };
            let mut needs_cas = false;
            let mut answers = Vec::with_capacity(batch.len());
            for request in batch {
                match &request.change {
                    ReaderChange::Register { reader, cut, paths } => {
                        let result = self.register(&mut record, *reader, *cut, paths.as_deref());
                        needs_cas |= result.is_ok();
                        answers.push(result);
                    }
                    ReaderChange::Release { reader, uncertain } => {
                        // An absent reader can still have an in-flight admission against this version.
                        needs_cas |= record.catalog_readers.remove(reader).is_some() || *uncertain;
                        answers.push(Ok(()));
                    }
                }
            }
            if !needs_cas { return Ok(answers); }
            let version = record.version;
            match self.directory.replace_deployment(record, version).await? {
                Cas::Applied(stored) => {
                    for (request, _) in batch.iter().zip(&answers).filter(|(_, answer)| answer.is_ok()) {
                        let (event, hold, cut) = match &request.change {
                            ReaderChange::Register { reader, cut, .. } => ("catalog-hold", reader, *cut),
                            ReaderChange::Release { reader, .. } => ("catalog-release", reader, None),
                        };
                        crate::residence_trace::record(event, || serde_json::json!({
                            "lineage": stored.lineage, "installation": self.deployment, "residence": self.residence,
                            "namespace": self.namespaces.objects, "root": self.storage_root, "revision": stored.version,
                            "epoch": self.replica_epoch, "hold": hold, "through": cut.map(|cut| cut.through),
                            "paths": stored.catalog_readers.get(hold)
                                .and_then(|registration| registration.catalog_file_paths.as_deref()),
                        }));
                    }
                    return Ok(answers);
                }
                Cas::Conflict(fresh) => current = fresh,
            }
        }
        Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/catalog-readers-contended", "The installation changed while updating its catalog readers"))
    }

    fn register(&self, record: &mut DeploymentRecord, reader: Uuid, cut: Option<CatalogCut>, paths: Option<&[String]>) -> Result<()> {
        if !self.residence_is_open(record) { return Err(closed()); }
        if !admits_after_floor(cut.map(|cut| cut.through.0), self.floor(record).map(|floor| floor.0)) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, CATALOG_NOT_CURRENT,
                "The captured catalog precedes this residence's installed admission floor"));
        }
        let paths = paths.filter(|paths| cut.is_some()
            && paths.windows(2).all(|window| window[0] < window[1])
            && paths.iter().all(|path| rtdb_format::keys::is_catalog_retirement_file(path))
            && primary_path_charge(paths) <= rtdb_types::CATALOG_FILE_PATH_BYTES
            && record.catalog_readers.values().filter_map(|reader| reader.catalog_file_paths.as_deref())
                .map(primary_path_charge).fold(primary_path_charge(paths), usize::saturating_add) <= PRIMARY_PATH_LEDGER_BYTES);
        let store = format!("{}/{}/", self.residence, self.storage_root);
        if record.storage_regions.values().any(|region| match region {
            StorageRegion::ExactRetirement { object, .. } => object.strip_prefix(&store)
                .is_some_and(|path| rtdb_format::keys::is_catalog_retirement_file(path)
                    && paths.is_none_or(|paths| contains_path(paths, path))),
            _ => false,
        }) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, CATALOG_NOT_CURRENT,
                "The captured catalog intersects a physical retirement in progress"));
        }
        record.catalog_readers.insert(reader, CatalogReader {
            owner: self.owner, storage_root: self.storage_root.clone(), replica_epoch: self.replica_epoch.0, cut,
            catalog_file_paths: paths.map(<[String]>::to_vec),
        });
        Ok(())
    }

}

pub(crate) fn readers_allow(record: &DeploymentRecord, storage_root: &str, installed: CatalogCut, path: Option<&str>) -> bool {
    record.catalog_readers.values().filter(|reader| {
        reader.storage_root == storage_root && reader.replica_epoch == installed.replica_epoch.0
    }).all(|reader| match path {
        Some(path) => reader.catalog_file_paths.as_ref().is_some_and(|paths| !contains_path(paths, path)),
        None => admits_after_floor(reader.cut.map(|cut| cut.through.0), Some(installed.through.0)),
    })
}

fn contains_path(paths: &[String], path: &str) -> bool {
    paths.binary_search_by(|member| member.as_str().cmp(path)).is_ok()
}

fn primary_path_charge(paths: &[String]) -> usize {
    paths.iter().fold(0usize, |charge, path| charge.saturating_add(path.len().saturating_mul(6).saturating_add(16)))
}

const PRIMARY_PATH_LEDGER_BYTES: usize = 64 * 1024;
const MAX_READER_CHANGES: usize = 64;
const CLOSED: &str = "rtdb.multi/catalog-reader-closed";
const CATALOG_NOT_CURRENT: &str = "rtdb.node/catalog-not-current";

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
        record.holder = Some(rtdb_directory::MemberId("writer".into()));
        let owner = Uuid::new_v4();
        record.storage_regions.insert(owner, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let record = directory.create_deployment(record).await.unwrap();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
        let readers = CatalogReaders::new(directory.clone(), TaskTracker::new(), &record, root, namespaces, owner, Some(record.epoch));
        (readers, directory, record)
    }

    #[tokio::test]
    async fn concurrent_admissions_and_releases_share_directory_updates() {
        use rtdb_directory::RemoteDirectory;
        use tokio::net::TcpListener;
        use tokio_util::sync::CancellationToken;

        let data = tempfile::tempdir().unwrap();
        MemoryDirectory::initialize(data.path()).unwrap();
        let durable = MemoryDirectory::open(data.path()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let serving = tokio::spawn(rtdb_directory::serve(listener, durable.clone(), stop.clone()));
        let directory: Arc<dyn Directory> = RemoteDirectory::connect(&address.to_string()).await.unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        let owner = Uuid::new_v4();
        record.storage_regions.insert(owner, StorageRegion::Local {
            archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let record = directory.create_deployment(record).await.unwrap();
        let namespaces = StorageNamespaces { objects: Uuid::new_v4(), keys: [0; 32] };
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().key();
        let readers = CatalogReaders::new(directory, TaskTracker::new(), &record, root, namespaces, owner, Some(record.epoch));

        let permits = futures::future::join_all((0..64).map(|_| readers.retain(None))).await
            .into_iter().collect::<Result<Vec<_>>>().unwrap();
        let opened = durable.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(opened.catalog_readers.len(), 64);
        assert!(opened.version < record.version + 64);
        drop(permits);

        readers.tasks.close();
        tokio::time::timeout(Duration::from_secs(5), readers.tasks.wait()).await.unwrap();
        let closed = durable.deployment(&record.id).await.unwrap().unwrap();
        assert!(closed.catalog_readers.is_empty());
        assert!(closed.version < opened.version + 64);
        stop.cancel();
        serving.await.unwrap().unwrap();
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
    async fn floor_installation_and_reader_admission_share_one_cas() {
        let (readers, directory, origin) = serving().await;
        let old = CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0), replica_epoch: readers.replica_epoch,
            through: ReplicaPosition::from_epoch_offset(readers.replica_epoch, 0).unwrap(),
        };
        let newer = CatalogCut {
            through: ReplicaPosition::from_epoch_offset(readers.replica_epoch, 1).unwrap(),
            ..old
        };
        let mut stale = directory.deployment(&origin.id).await.unwrap().unwrap();
        let stale_version = stale.version;
        let held = readers.retain(Some(old)).await.unwrap();
        stale.catalog_admission_floors.insert(readers.storage_root.clone(), CatalogAdmissionFloor {
            residence: readers.residence.clone(), object_namespace: readers.namespaces.objects, cut: newer,
        });
        assert!(!matches!(directory.replace_deployment(stale, stale_version).await.unwrap(), Cas::Applied(_)));
        let opaque = readers.retain(None).await.unwrap();
        assert!(!readers.advance(newer).await.unwrap(), "an older held catalog must block the floor");
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let record = directory.deployment(&origin.id).await.unwrap().unwrap();
                if record.catalog_readers.len() == 1 { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        assert!(!readers.advance(newer).await.unwrap(), "an opaque held catalog must block the floor");
        drop(opaque);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !readers.advance(newer).await.unwrap() { tokio::task::yield_now().await; }
        }).await.unwrap();

        let (stale, opaque, current) = tokio::join!(readers.retain(Some(old)), readers.retain(None), readers.retain(Some(newer)));
        assert_eq!(stale.err().expect("a capture older than the installed floor must refuse").code, CATALOG_NOT_CURRENT);
        assert_eq!(opaque.err().unwrap().code, CATALOG_NOT_CURRENT);
        let current = current.unwrap();
        assert!(readers.advance(old).await.unwrap(), "replaying an earlier receipt is already covered");
        let installed = directory.deployment(&origin.id).await.unwrap().unwrap();
        assert_eq!(installed.catalog_admission_floors[&readers.storage_root].cut, newer);
        update_installation(directory.as_ref(), &origin.id, |record| { record.epoch += 1; Ok(()) }).await.unwrap();
        assert_eq!(readers.advance(newer).await.unwrap_err().code, CLOSED);
        drop(current);
        readers.tasks.close();
        tokio::time::timeout(Duration::from_secs(5), readers.tasks.wait()).await.unwrap();
    }

    #[tokio::test]
    async fn an_old_catalog_holds_only_the_files_it_can_open() {
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
        let unrelated_path = rtdb_format::keys::TableSlug::recorded("public$unrelated").unwrap()
            .data_key(&"l01-rc-b00".parse().unwrap());
        let search_prefix = rtdb_format::search_manifest::run_prefix(&rtdb_format::keys::TableSlug::recorded("public$held").unwrap());
        let held_run = format!("{search_prefix}/{}.f16.arrow", "aa".repeat(32));
        let unrelated_run = format!("{search_prefix}/{}.f16.arrow", "bb".repeat(32));
        let mut paths = vec![held_path.clone(), held_run.clone()];
        paths.sort_unstable();
        let hold = readers.retain_with_paths(Some(old), Some(paths.into())).await.unwrap();

        let (_, allowed) = readers.close_admission(installed, &[Some(&held_path)]).await.unwrap();
        assert_eq!(allowed, [false]);
        assert_eq!(readers.retain(Some(old)).await.err().unwrap().code, CATALOG_NOT_CURRENT);
        let (_, allowed) = readers.close_admission(installed,
            &[Some(&held_path), Some(&unrelated_path), Some(&held_run), Some(&unrelated_run), None],
        ).await.unwrap();
        assert_eq!(allowed, [false, true, false, true, false]);
        assert!(!readers.advance(installed).await.unwrap());

        let current = directory.deployment(&origin.id).await.unwrap().unwrap();
        assert_eq!(current.catalog_readers.len(), 1);
        assert_eq!(current.catalog_admission_floors[&readers.storage_root].cut, installed);
        drop(hold);
        readers.tasks.close();
        readers.tasks.wait().await;
        let (_, allowed) = readers.close_admission(installed, &[Some(&held_path)]).await.unwrap();
        assert_eq!(allowed, [true]);
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
