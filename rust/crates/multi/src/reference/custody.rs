use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, ReplayCustody, StorageNamespaces, StorageRegion};
use rtdb_storage::Error;
use rtdb_types::{Anomaly, AnomalyCategory};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::keys::KeyService;

mod admission;

/// Registers inherited provider reads on the record that owns their bytes.
#[derive(Clone)]
pub(crate) struct ReferenceReads {
    directory: Arc<dyn Directory>,
    keys: Arc<dyn KeyService>,
    tasks: TaskTracker,
    org: String,
    lineage: String,
    root: String,
    namespaces: StorageNamespaces,
    custody: ReplayCustody,
}

const CLOSED: &str = "rtdb.multi/reference-source-closed";

impl ReferenceReads {
    pub(crate) fn new(
        directory: Arc<dyn Directory>, keys: Arc<dyn KeyService>, tasks: TaskTracker,
        fork: &DeploymentRecord, root: &str, namespaces: StorageNamespaces, custody: ReplayCustody,
    ) -> Self {
        Self { directory, keys, tasks, org: fork.org.clone(), lineage: fork.lineage.clone(),
            root: root.to_owned(), namespaces, custody }
    }

    /// The returned owner is retained by each admitted provider task, even if
    /// the async caller goes away. The worker owns registration and cleanup.
    pub(super) async fn retain(&self, source: &str, object: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<Arc<dyn Send + Sync>> {
        let closed = || Error::Closed(format!("reference source {source} is closed"));
        let (residence, root) = source.rsplit_once('/').ok_or_else(closed)?;
        let (owner, generation) = residence.rsplit_once('/').ok_or_else(closed)?;
        let (_, deployment) = owner.rsplit_once('/').ok_or_else(closed)?;
        if deployment.is_empty() || generation.is_empty() || root != self.root
            || !object.is_under(&rtdb_storage::ObjectKey::new(source)?) || object.as_str() == source
        {
            return Err(closed());
        }
        let worker = self.clone();
        let deployment = deployment.to_owned();
        let residence = residence.to_owned();
        let object = object.to_string();
        let (ready, admitted) = oneshot::channel();
        let (release, drained) = oneshot::channel::<()>();
        self.tasks.spawn(async move {
            let id = Uuid::new_v4();
            let witness = if worker.custody == ReplayCustody::Local {
                match worker.keys.hold_local_storage_region(id) {
                    Ok(witness) => witness,
                    Err(error) => { let _ = ready.send(Err(error)); return; }
                }
            } else { None };
            let accepted = crate::admin::update_installation(worker.directory.as_ref(), &deployment, |owner| {
                if !admission::may_admit_reference_read(
                    matches!(owner.lifecycle, Lifecycle::Live | Lifecycle::Quarantined),
                    owner.storage_prefix() == residence && owner.org == worker.org && owner.lineage == worker.lineage,
                    owner.storage_namespaces == Some(worker.namespaces),
                ) || owner.storage_regions.values().any(|region| region.retires_reference(&object)) {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, CLOSED, "The reference's physical residence no longer admits reads"));
                }
                owner.storage_regions.insert(id, StorageRegion::ReferenceRead { custody: worker.custody.clone(), object: Some(object.clone()), archive_source: None });
                Ok(())
            }).await;
            let opened = accepted.is_ok();
            let uncertain = accepted.as_ref().err().is_some_and(|error| !matches!(
                error.code.as_str(), CLOSED | "rtdb.multi/unknown-installation"
            ));
            if ready.send(accepted.map(|_| Arc::new(release) as Arc<dyn Send + Sync>)).is_ok() && opened {
                let _ = drained.await;
            }
            if opened || uncertain {
                loop {
                    match crate::store::release_region(worker.directory.as_ref(), &deployment, id, uncertain).await {
                        Ok(()) => break,
                        Err(error) => eprintln!("Reference read {id} for {deployment} is pending release: {}", error.code),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            if let Some(witness) = witness {
                drop(witness);
                if let Err(error) = worker.keys.forget_local_storage_region(id) {
                    eprintln!("Could not remove reference read witness {id}: {}", error.code);
                }
            }
        });
        admitted.await.map_err(|_| closed())?.map_err(|error| match error.code.as_str() {
            CLOSED | "rtdb.multi/unknown-installation" => closed(),
            _ => Error::Remote { key: source.to_owned(), message: error.code.into() },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rtdb_directory::MemoryDirectory;
    use rtdb_storage::{IoRegion, ObjectStore};

    use crate::deployment::{CellStorage, storage_namespaces};
    use crate::keys::LocalKeyService;
    use crate::reference::{Base, PrefixedStore, ReferencingStore, key, simulate_fork};

    #[tokio::test]
    async fn inherited_io_holds_its_physical_residence_through_cancellation_and_disposal() {
        let files = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: files.path().join("objects") };
        let keys = Arc::new(LocalKeyService::new(files.path().join("keys")).unwrap());
        let directory = MemoryDirectory::new();
        let io = IoRegion::new();
        let bucket = storage.bucket(io.clone()).await.unwrap();
        let namespaces = storage_namespaces(bucket.as_ref(), keys.as_ref()).await.unwrap();
        let mut parent = DeploymentRecord::new("parent", "org", Vec::new(), 0);
        parent.storage_namespaces = Some(namespaces);
        let parent = directory.create_deployment(parent).await.unwrap();

        let mut child = DeploymentRecord::new("child", "org", Vec::new(), 0);
        child.parent = Some(parent.id.clone());
        child.lineage = parent.lineage.clone();
        child.storage_namespaces = Some(namespaces);
        let child = directory.create_deployment(child).await.unwrap();
        let child_store = format!("{}/v06", child.storage_prefix());
        let parent_store = format!("{}/v06", parent.storage_prefix());
        let parent_object = key(&format!("{parent_store}/payload")).unwrap();
        let object = key("payload").unwrap();
        let bytes = Bytes::from_static(b"payload");
        bucket.put(&parent_object, bytes.clone()).await.unwrap();
        simulate_fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        let base = Base::load(bucket.as_ref(), &child_store).await.unwrap();

        let tasks = TaskTracker::new();
        let custody = ReferenceReads::new(directory.clone(), keys.clone(), tasks.clone(), &child, "v06", namespaces, ReplayCustody::Local);
        let own = Arc::new(PrefixedStore::new(bucket.clone(), child_store));
        let store = Arc::new(ReferencingStore::new(own, bucket.clone(), base).with_custody(custody.clone()));
        assert_eq!(store.get_range(&object, 1..4).await.unwrap(), bytes.slice(1..4));

        let unrelated = DeploymentRecord::new("unrelated", "org", Vec::new(), 0);
        let other_custody = ReferenceReads::new(directory.clone(), keys.clone(), tasks.clone(), &unrelated, "v06", namespaces, ReplayCustody::Local);
        assert!(matches!(other_custody.retain(&parent_store, &parent_object).await, Err(Error::Closed(_))));
        assert!(matches!(custody.retain(&format!("{}/v05", parent.storage_prefix()), &parent_object).await, Err(Error::Closed(_))));

        let retirement = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &parent.id, |owner| {
            owner.storage_regions.insert(retirement, StorageRegion::ExactRetirement { object: parent_object.to_string(), release: None });
            Ok(())
        }).await.unwrap();
        assert!(matches!(store.get(&object).await, Err(Error::Closed(_))), "the owner fences the retiring data");
        crate::admin::update_installation(directory.as_ref(), &parent.id, |owner| {
            owner.storage_regions.insert(retirement, StorageRegion::ExactRetirement {
                object: format!("{parent_store}/another-object"), release: Some(parent_object.to_string()),
            });
            Ok(())
        }).await.unwrap();
        assert!(matches!(store.get(&object).await, Err(Error::Closed(_))), "the owner also fences the release marker");
        crate::store::release_region(directory.as_ref(), &parent.id, retirement, false).await.unwrap();

        let (entered, running) = oneshot::channel();
        let (release, finish) = std::sync::mpsc::channel();
        let work = parking_lot::Mutex::new(Some((entered, finish)));
        let reader = store.clone();
        let requested = object.clone();
        let source = parent_object.clone();
        let waiter = tokio::spawn(async move {
            reader.read(&requested, |store, key| {
                let pause = (key == &source).then(|| work.lock().take().unwrap());
                let io = io.clone();
                Box::pin(async move {
                    if let Some((entered, finish)) = pause {
                        io.run_blocking(move || {
                            entered.send(()).unwrap();
                            finish.recv_timeout(Duration::from_secs(10)).unwrap();
                            Ok(())
                        }).await?;
                    }
                    store.get(key).await
                })
            }).await
        });
        tokio::time::timeout(Duration::from_secs(5), running).await.unwrap().unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());

        let owner = directory.deployment(&parent.id).await.unwrap().unwrap();
        assert!(owner.storage_regions.values().any(|region| matches!(region, StorageRegion::ReferenceRead { .. })),
            "cancelled provider work still needs its physical owner");
        assert!(directory.deployment(&child.id).await.unwrap().unwrap().storage_regions.is_empty());
        crate::admin::begin_disposal(directory.as_ref(), &parent.id).await.unwrap();
        let pending = crate::admin::finish_disposal(directory.as_ref(), &storage, keys.as_ref(), &parent.id, 4 << 20).await.unwrap_err();
        assert_eq!(pending.code, "rtdb.multi/storage-not-drained");
        assert!(matches!(store.get(&object).await, Err(Error::Closed(_))),
            "a closed physical owner refuses a new provider read");
        assert_eq!(bucket.get(&parent_object).await.unwrap(), bytes);

        release.send(()).unwrap();
        tasks.close();
        tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await.unwrap();
        crate::admin::finish_disposal(directory.as_ref(), &storage, keys.as_ref(), &parent.id, 4 << 20).await.unwrap();
        assert!(matches!(bucket.get(&parent_object).await, Err(Error::MissingObject(_))));
        assert_eq!(store.get(&object).await.unwrap(), bytes);
    }
}
