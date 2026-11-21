//! Physical custody of an ordered index's independently compacted pages.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_directory::{Cas, DeploymentRecord, Directory, LegacyIndexPages, Lifecycle, OrderedIndexStorage, StoragePublication, StorageRegion};
use rtdb_format::keys::ORDERED_INDEX_ROOT;
use rtdb_storage::{
    Error, ObjectKey, ObjectStore,
    object_store::{IndexPageLifecycle, IndexPageScope},
};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio::sync::{OnceCell, oneshot, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use crate::admin::update_installation;
use crate::deployment::CellStorage;

const WATCH_INTERVAL: Duration = Duration::from_millis(250);

type Permit = Arc<dyn Send + Sync>;
type PermitSlot = Arc<Mutex<Option<Permit>>>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Scope {
    index: String,
    generation: u64,
}

pub(crate) struct HostedIndexPages {
    directory: Arc<dyn Directory>,
    deployment: String,
    store: String,
    owner: Uuid,
    writer_epoch: Option<u64>,
    tasks: TaskTracker,
    directory_watch: OnceLock<watch::Receiver<Option<DeploymentRecord>>>,
    cancelled: CancellationToken,
    permits: Mutex<BTreeMap<Scope, Arc<OnceCell<PermitSlot>>>>,
}

impl HostedIndexPages {
    pub(crate) fn new(
        directory: Arc<dyn Directory>,
        deployment: String,
        store: String,
        owner: Uuid,
        writer_epoch: Option<u64>,
        tasks: TaskTracker,
    ) -> Self {
        Self { directory, deployment, store, owner, writer_epoch, tasks, directory_watch: OnceLock::new(),
            cancelled: CancellationToken::new(), permits: Mutex::default() }
    }

    fn scope(&self, scope: IndexPageScope<'_>) -> rtdb_storage::Result<Scope> {
        definition(scope.index.as_str()).ok_or_else(|| Error::InvalidKey(scope.index.to_string()))?;
        Ok(Scope { index: format!("{}/{}", self.store, scope.index), generation: scope.generation })
    }

    async fn open(&self, scope: &Scope) -> Result<PermitSlot> {
        let (ready, admitted) = oneshot::channel();
        let (release, drained) = oneshot::channel::<()>();
        let permit_id = Uuid::new_v4();
        let mut watched = self.directory_watch.get_or_init(|| {
            rtdb_directory::watch_deployment(self.directory.clone(), self.deployment.clone(), WATCH_INTERVAL)
        }).clone();
        let cancelled = self.cancelled.clone();
        let directory = self.directory.clone();
        let deployment = self.deployment.clone();
        let owner = self.owner;
        let scope = scope.clone();
        self.tasks.spawn(async move {
            let answer = update_installation(directory.as_ref(), &deployment, |record| {
                require_owner(record, owner, &scope.index, None)?;
                let index = record.ordered_indexes.entry(scope.index.clone()).or_default();
                if !index.admits(scope.generation) {
                    return Err(closed_scope());
                }
                index.generations.entry(scope.generation).or_default().insert(permit_id, owner);
                Ok(())
            }).await;
            let slot = Arc::new(Mutex::new(Some(Arc::new(release) as Permit)));
            let opened = answer.as_ref().ok().map(|record| record.version);
            let delivered = ready.send(answer.map(|_| slot.clone())).is_ok();
            if let Some(version) = opened.filter(|_| delivered) {
                tokio::select! {
                    _ = cancelled.cancelled() => {}
                    _ = watched.wait_for(|record| record.as_ref().is_some_and(|record| {
                        record.version >= version
                            && (require_owner(record, owner, &scope.index, None).is_err()
                                || record.ordered_indexes.get(&scope.index)
                                    .is_some_and(|index| !index.admits(scope.generation)))
                    })) => {}
                }
            }
            slot.lock().take();
            let _ = drained.await;
            // A lost CAS response may have recorded the permit. Fence an
            // ambiguous admission even if the first read sees it absent.
            loop {
                match release_permit(directory.as_ref(), &deployment, &scope, permit_id).await {
                    Ok(()) => break,
                    Err(error) => eprintln!("Ordered index permit {permit_id} is retained: {}", error.code),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        admitted.await.map_err(|_| closed_scope())?
    }
}

impl Drop for HostedIndexPages {
    fn drop(&mut self) { self.cancelled.cancel(); }
}

#[async_trait]
impl IndexPageLifecycle for HostedIndexPages {
    async fn retain(&self, scope: IndexPageScope<'_>) -> rtdb_storage::Result<Permit> {
        let scope = self.scope(scope)?;
        if self.directory_watch.get().is_some_and(|watch| watch.borrow().as_ref().is_some_and(|record| {
            record.ordered_indexes.get(&scope.index).is_some_and(|index| !index.admits(scope.generation))
        })) {
            return Err(storage_error(closed_scope()));
        }
        let slot = {
            let mut permits = self.permits.lock();
            permits.retain(|_, permit| permit.get().is_none_or(|slot| slot.lock().is_some()));
            permits.entry(scope.clone()).or_default().clone()
        };
        let permit = slot.get_or_try_init(|| self.open(&scope)).await.map_err(storage_error)?;
        let outstanding = permit.lock().clone();
        outstanding.ok_or_else(|| storage_error(closed_scope()))
    }

    async fn retire_before(&self, scope: IndexPageScope<'_>) -> rtdb_storage::Result<()> {
        let scope = self.scope(scope)?;
        let epoch = self.writer_epoch.ok_or_else(|| storage_error(closed_scope()))?;
        update_installation(self.directory.as_ref(), &self.deployment, |record| {
            require_owner(record, self.owner, &scope.index, Some(epoch))?;
            let index = record.ordered_indexes.entry(scope.index.clone()).or_default();
            index.retire_earlier(scope.generation);
            retire_legacy(index);
            Ok(())
        }).await.map_err(storage_error)?;
        Ok(())
    }

    async fn retire_dropped(&self, active: &BTreeSet<ObjectKey>, through: i64) -> rtdb_storage::Result<()> {
        let epoch = self.writer_epoch.ok_or_else(|| storage_error(closed_scope()))?;
        update_installation(self.directory.as_ref(), &self.deployment, |record| {
            require_owner(record, self.owner, &self.store, Some(epoch))?;
            for (index, state) in &mut record.ordered_indexes {
                let Some(relative) = below(index, &self.store) else { continue };
                if definition(relative).is_some_and(|version| version <= through)
                    && !active.iter().any(|index| index.as_str() == relative)
                {
                    state.dropped = true;
                    retire_legacy(state);
                }
            }
            Ok(())
        }).await.map_err(storage_error)?;
        Ok(())
    }
}

fn closed_scope() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/index-pages-closed", "The ordered index page scope is closed")
}

fn storage_error(error: Anomaly) -> Error {
    Error::Closed(error.code.into())
}

fn require_owner(record: &DeploymentRecord, owner: Uuid, path: &str, epoch: Option<u64>) -> Result<()> {
    if !region_open(record.storage_regions.get(&owner)) || below(path, &record.storage_prefix()).is_none()
        || !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
        || epoch.is_some_and(|epoch| record.epoch != epoch || record.holder.is_none())
    {
        return Err(closed_scope());
    }
    Ok(())
}

fn below<'a>(name: &'a str, parent: &str) -> Option<&'a str> {
    name.strip_prefix(parent)?.strip_prefix('/')
}

fn definition(relative: &str) -> Option<i64> {
    let component = below(relative, ORDERED_INDEX_ROOT)?;
    let (version, digest) = component.split_once('-')?;
    if version.len() != 16 || digest.len() != 64
        || !version.bytes().chain(digest.bytes()).all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    i64::from_str_radix(version, 16).ok()
}

fn page_key(name: &str) -> Option<(&str, Option<u64>)> {
    let (store, index) = name.rsplit_once(&format!("/{ORDERED_INDEX_ROOT}/"))?;
    let (id, rest) = index.split_once('/')?;
    let index = &name[..store.len() + ORDERED_INDEX_ROOT.len() + id.len() + 2];
    definition(&name[store.len() + 1..index.len()])?;
    if rest.starts_with("pages/") {
        return Some((index, None));
    }
    let (generation, page) = rest.strip_prefix("generations/")?.split_once('/')?;
    if generation.len() != 16 || !page.starts_with("pages/") {
        return None;
    }
    Some((index, Some(u64::from_str_radix(generation, 16).ok()?)))
}

pub(crate) fn require_page_publication(
    record: &DeploymentRecord, region: Uuid, key: &ObjectKey,
) -> Result<()> {
    let Some((index, generation)) = page_key(key.as_str()) else { return Ok(()) };
    let generation = generation.ok_or_else(closed_scope)?;
    if record.ordered_indexes.get(index).is_some_and(|index| {
        index.admits(generation) && index.generations.get(&generation)
            .is_some_and(|permits| permits.values().any(|owner| *owner == region))
    }) {
        Ok(())
    } else {
        Err(closed_scope())
    }
}

async fn release_permit(
    directory: &dyn Directory, deployment: &str, scope: &Scope, permit: Uuid,
) -> Result<()> {
    for _ in 0..8 {
        let Some(mut record) = directory.deployment(deployment).await? else { return Ok(()) };
        if let Some(permits) = record.ordered_indexes.get_mut(&scope.index)
            .and_then(|index| index.generations.get_mut(&scope.generation))
        {
            permits.remove(&permit);
        }
        let version = record.version;
        if let Cas::Applied(_) = directory.replace_deployment(record, version).await? {
            return Ok(());
        }
    }
    Err(closed_scope())
}

fn retire_legacy(index: &mut OrderedIndexStorage) {
    if index.legacy_pages == LegacyIndexPages::Retained {
        index.legacy_pages = LegacyIndexPages::Retiring;
    }
}

fn region_open(region: Option<&StorageRegion>) -> bool {
    matches!(region, Some(StorageRegion::Remote { archive: None, closed: false, .. } | StorageRegion::Local { .. }))
}

fn untracked_regions_open(record: &DeploymentRecord) -> bool {
    record.storage_regions.values().any(|region| {
        region_open(Some(region)) && !region.tracks_ordered_index_pages()
    })
}

fn ready_generations(record: &DeploymentRecord) -> Vec<(String, Option<u64>)> {
    if untracked_regions_open(record) {
        return Vec::new();
    }
    let mut result = Vec::new();
    let residence = record.storage_prefix();
    for (name, index) in &record.ordered_indexes {
        if below(name, &residence).is_none() {
            continue;
        }
        if index.legacy_pages == LegacyIndexPages::Retiring {
            result.push((name.clone(), None));
        }
        for (&generation, users) in &index.generations {
            if !index.admits(generation) && users.values().all(|owner| !region_open(record.storage_regions.get(owner))) {
                result.push((name.clone(), Some(generation)));
            }
        }
    }
    result
}

pub(crate) async fn reap(
    directory: &dyn Directory, storage: &CellStorage, bucket: &dyn ObjectStore,
    record: &DeploymentRecord,
) -> Result<()> {
    if untracked_regions_open(record) {
        return Ok(());
    }
    let residence = record.storage_prefix();
    let discovery: Vec<_> = record.ordered_indexes.iter().filter_map(|(name, index)| {
        let before = if index.dropped { u64::MAX } else { index.oldest_generation };
        (before > index.audited_before && below(name, &residence).is_some()).then_some((name, before))
    }).collect();
    if discovery.is_empty() && ready_generations(record).is_empty() {
        return Ok(());
    }
    let inventory = match storage {
        CellStorage::S3 { config } => Some(CellStorage::s3_inventory(config).await?),
        CellStorage::Local { .. } => None,
    };
    let mut refreshed = record.clone();
    for (index, before) in discovery {
        let prefix = ObjectKey::new(format!("{index}/generations"))?;
        let mut generations = if let Some(inventory) = &inventory {
            inventory.child_directories(&prefix).await?
        } else {
            bucket.list(Some(&prefix)).await?.iter().filter_map(|object| {
                below(object.key.as_str(), prefix.as_str())?.split_once('/').map(|(part, _)| part.to_owned())
            }).collect()
        };
        for (_, _, publication) in publications_under(record, &prefix)? {
            if let Some((generation, _)) = below(&publication.key, prefix.as_str()).and_then(|relative| relative.split_once('/')) {
                generations.insert(generation.to_owned());
            }
        }
        refreshed = update_installation(directory, &record.id, |current| {
            if current.storage_prefix() != residence || untracked_regions_open(current) {
                return Ok(());
            }
            let Some(state) = current.ordered_indexes.get_mut(index) else { return Ok(()) };
            for generation in &generations {
                if generation.len() == 16 && let Ok(generation) = u64::from_str_radix(generation, 16)
                    && generation < before && !state.admits(generation)
                {
                    state.generations.entry(generation).or_default();
                }
            }
            state.audited_before = state.audited_before.max(before);
            Ok(())
        }).await?;
    }
    let record = &refreshed;
    for (index, generation) in ready_generations(record) {
        let prefix = match generation {
            Some(generation) => ObjectKey::new(format!("{index}/generations/{generation:016x}"))?,
            None => ObjectKey::new(format!("{index}/pages"))?,
        };
        let publications = publications_under(record, &prefix)?;
        if let Some(inventory) = &inventory {
            for (_, _, publication) in &publications {
                inventory.abort_publication(&ObjectKey::new(publication.key.clone())?, &publication.upload_id).await?;
            }
            inventory.purge(&prefix).await?;
        } else {
            for object in bucket.list(Some(&prefix)).await? {
                bucket.delete(&object.key).await?;
            }
            if !bucket.list(Some(&prefix)).await?.is_empty() {
                return Err(closed_scope());
            }
        }
        update_installation(directory, &record.id, |current| {
            if current.storage_prefix() != record.storage_prefix() {
                return Ok(());
            }
            for (region, attempt, publication) in &publications {
                if let Some(StorageRegion::Remote { publications, .. }) = current.storage_regions.get_mut(region)
                    && publications.get(attempt) == Some(publication)
                {
                    publications.remove(attempt);
                }
            }
            let Some(state) = current.ordered_indexes.get_mut(&index) else { return Ok(()) };
            match generation {
                Some(generation) if !state.admits(generation) => { state.generations.remove(&generation); }
                None if state.legacy_pages == LegacyIndexPages::Retiring => { state.legacy_pages = LegacyIndexPages::Removed; }
                _ => {}
            }
            Ok(())
        }).await?;
    }
    Ok(())
}

fn publications_under(record: &DeploymentRecord, prefix: &ObjectKey) -> Result<Vec<(Uuid, Uuid, StoragePublication)>> {
    let mut matched = Vec::new();
    for (&region, state) in &record.storage_regions {
        if let StorageRegion::Remote { archive: None, publications, .. } = state {
            for (&attempt, publication) in publications {
                if ObjectKey::new(publication.key.clone())?.is_under(prefix) {
                    matched.push((region, attempt, publication.clone()));
                }
            }
        }
    }
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use rtdb_directory::{MemberId, MemoryDirectory};
    use rtdb_storage::{IoRegion, MemoryStore};

    use super::*;

    #[tokio::test]
    async fn a_closed_generation_rejects_uploads_and_waits_for_a_cancelled_read() {
        let directory = MemoryDirectory::new();
        let owner = Uuid::new_v4();
        let mut record = DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        record.epoch = 1;
        record.holder = Some(MemberId("writer".into()));
        record.storage_regions.insert(owner, crate::store::remote_storage_region(None, None));
        let record = directory.create_deployment(record).await.unwrap();
        let store = format!("{}/v06", record.storage_prefix());
        let tasks = TaskTracker::new();
        let lifecycle = HostedIndexPages::new(
            directory.clone(), record.id.clone(), store.clone(), owner, Some(1), tasks.clone(),
        );
        let index = ObjectKey::new(format!("{ORDERED_INDEX_ROOT}/0000000000000001-{}", "a".repeat(64))).unwrap();
        let old = IndexPageScope { index: &index, generation: 0 };
        let next = IndexPageScope { index: &index, generation: 2 };
        let old_key = ObjectKey::new(format!("{store}/{index}/generations/{:016x}/pages/old", old.generation)).unwrap();
        let new_key = ObjectKey::new(format!("{store}/{index}/generations/{:016x}/pages/new", next.generation)).unwrap();
        let unregistered = ObjectKey::new(format!("{store}/{index}/generations/0000000000000001/pages/orphan")).unwrap();
        let bucket = MemoryStore::default();
        let storage = CellStorage::Local { root: std::path::PathBuf::new() };

        let permit = lifecycle.retain(old).await.unwrap();
        let _replacement = lifecycle.retain(next).await.unwrap();
        let mut watched = rtdb_directory::watch_deployment(directory.clone(), record.id.clone(), WATCH_INTERVAL);
        let opened = directory.deployment(&record.id).await.unwrap().unwrap();
        require_page_publication(&opened, owner, &old_key).unwrap();
        require_page_publication(&opened, owner, &new_key).unwrap();
        bucket.put(&old_key, "erased".as_bytes().into()).await.unwrap();
        bucket.put(&new_key, "clear".as_bytes().into()).await.unwrap();
        bucket.put(&unregistered, "orphan".as_bytes().into()).await.unwrap();

        let (entered, running) = oneshot::channel();
        let (complete, wait) = std::sync::mpsc::channel();
        let request = tokio::spawn(IoRegion::retaining(permit, async move {
            IoRegion::new().run_blocking(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(())
            }).await
        }));
        running.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());

        lifecycle.retire_before(next).await.unwrap();
        let closed = directory.deployment(&record.id).await.unwrap().unwrap();
        assert!(require_page_publication(&closed, owner, &old_key).is_err());
        assert!(require_page_publication(&closed, Uuid::new_v4(), &new_key).is_err());
        require_page_publication(&closed, owner, &new_key).unwrap();
        let mut before_cutover = closed.clone();
        before_cutover.storage_regions.insert(Uuid::new_v4(), StorageRegion::Local {
            archive: None, tracks_ordered_index_pages: false,
        });
        reap(directory.as_ref(), &storage, &bucket, &before_cutover).await.unwrap();
        assert!(bucket.head(&unregistered).await.is_ok());

        reap(directory.as_ref(), &storage, &bucket, &closed).await.unwrap();
        assert!(bucket.head(&old_key).await.is_ok());
        assert!(matches!(bucket.head(&unregistered).await, Err(Error::MissingObject(_))));

        complete.send(()).unwrap();
        let reaped = {
            let ready = tokio::time::timeout(Duration::from_secs(5), watched.wait_for(|version| {
                version.as_ref().is_some_and(|record| {
                    ready_generations(record).contains(&(format!("{store}/{index}"), Some(0)))
                })
            })).await.unwrap().unwrap();
            ready.as_ref().unwrap().clone()
        };
        assert!(lifecycle.retain(old).await.is_err());
        reap(directory.as_ref(), &storage, &bucket, &reaped).await.unwrap();
        assert!(matches!(bucket.head(&old_key).await, Err(Error::MissingObject(_))));
        assert!(bucket.head(&new_key).await.is_ok());
        let acknowledged = directory.deployment(&record.id).await.unwrap().unwrap();
        let state = &acknowledged.ordered_indexes[&format!("{store}/{index}")];
        assert!(!state.admits(0));
        assert!(!state.generations.contains_key(&0));

        drop(_replacement);
        drop(lifecycle);
        tasks.close();
        tokio::time::timeout(Duration::from_secs(5), tasks.wait()).await.unwrap();
    }
}
