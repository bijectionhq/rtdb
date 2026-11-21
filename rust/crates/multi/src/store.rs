//! A deployment's object store as a node sees it (contract `ObjectStorage`): fenced by the holder's epoch, encrypted under the deployment's key, and unable to delete.
//!
//! - `FencedStore` checks current directory authority before admitting each write (`ZombiesCannotDamageStorage`).
//! - `EncryptingStore` seals objects with AES-256-GCM under the deployment's key, bound to the object name.
//! - `GarbageStore` turns a delete into a garbage record; only the reaper erases an object, once it has been garbage for the grace period (`OnlyTheReaperDeletes`, `ReapUnreachable`).

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use prost_types::Any;
use rtdb_directory::{Cas, DeploymentRecord, Directory, HostedTask, Lifecycle, StoragePublication, StorageRegion, now_ms};
use rtdb_node::RemoteStorageFactory;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, object_store::StoredObject, remote::PublicationJournal};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use crate::cipher::{Cipher, SEALED_OVERHEAD as OVERHEAD};
use crate::deployment::CellStorage;
use crate::keys::KeyService;

/// The protobuf type name the cell's storage factory is registered under.
pub const CELL_STORAGE_TYPE: &str = "rtdb.multi.CellStorage";
pub const GARBAGE_DIRECTORY: &str = "~garbage";
const SEALED_OVERHEAD: u64 = OVERHEAD as u64;

/// The deployment store a node opens, carried in its `DatabaseConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellStoreConfig {
    /// The cell's object storage, and the deployment's prefix within it.
    pub storage: crate::deployment::CellStorage,
    pub prefix: String,
    pub deployment: String,
    /// The epoch a holder opened the store at; absent for a reader, which never writes.
    pub writer_epoch: Option<u64>,
    /// A compaction worker's store, which may write compaction outputs and nothing else.
    #[serde(default)]
    pub compactor: bool,
    /// A snapshot reader's store, which recovers exactly this block.
    #[serde(default)]
    pub snapshot_block: Option<i64>,
}

pub struct CellStoreFactory {
    pub directory: Arc<dyn Directory>,
    pub keys: Arc<dyn KeyService>,
    pub(crate) hosted_task: Option<HostedTask>,
    pub(crate) storage_regions: TaskTracker,
}

#[async_trait]
impl RemoteStorageFactory for CellStoreFactory {
    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        let (record, region, release) = self.admit_region(&config, io).await?;
        let recovery = if storage_root.as_str() == crate::archive::checkpoint_storage_root()? {
            crate::archive::ArchivedCheckpoint::load(&record, region.clone()).await?
        } else {
            None
        };
        let bucket = config.storage.bucket(region).await?;
        let store = format!("{}/{}", config.prefix, storage_root.as_str());
        let root = FencedStore {
            inner: Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), store.clone())),
            directory: self.directory.clone(),
            deployment: config.deployment.clone(),
            fence: Fence::from(&config),
            _release: Some(release),
        };
        confirm_root(&root, &config.deployment, config.writer_epoch.is_some()).await?;
        let discovery = match record.checkpoint { Some(checkpoint) => crate::packing::PackDiscovery::From(checkpoint.block), None => crate::packing::PackDiscovery::List };
        Ok(open_with_scoped_store(bucket, &store, Arc::new(root), discovery, wall_clock(), recovery).await?)
    }

    /// A deployment with a published checkpoint recovers from it without listing its store.
    async fn block_discovery(&self, configuration: &Any) -> Result<rtdb_replication::BlockDiscovery> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        if let Some(block) = config.snapshot_block { return Ok(rtdb_replication::BlockDiscovery::At(rtdb_types::BlockIndex(block))); }
        Ok(match self.checkpoint(&config).await? { Some(block) => rtdb_replication::BlockDiscovery::From(rtdb_types::BlockIndex(block)), None => rtdb_replication::BlockDiscovery::List })
    }

    /// Encryption sits above the node's disk cache, which therefore holds ciphertext.
    async fn wrap_cached(&self, configuration: &Any, cached: Arc<dyn ObjectStore>) -> Result<Arc<dyn ObjectStore>> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        let record = self.directory.deployment(&config.deployment).await?
            .filter(|record| record.storage_prefix() == config.prefix)
            .ok_or_else(|| storage_region_error("The cached store belongs to another residence"))?;
        Ok(Arc::new(EncryptingStore::new(cached, &self.keys.key(&record.key_id()).await?)?))
    }
}

impl CellStoreFactory {
    async fn admit_region(
        &self,
        config: &CellStoreConfig,
        parent: Arc<IoRegion>,
    ) -> Result<(DeploymentRecord, Arc<IoRegion>, oneshot::Sender<()>)> {
        let directory = self.directory.clone();
        let keys = self.keys.clone();
        let config = config.clone();
        let hosted_task = self.hosted_task.clone();
        let id = Uuid::new_v4();
        let (release, dropped) = oneshot::channel();
        let (ready, admitted) = oneshot::channel();
        // The worker owns both the admission and its release. Dropping the
        // opening future cannot strand a request that has reached the directory.
        self.storage_regions.spawn(async move {
            let local = matches!(config.storage, crate::deployment::CellStorage::Local { .. });
            let witness = if local {
                match keys.hold_local_storage_region(id) {
                    Ok(witness) => witness,
                    Err(error) => {
                        let _ = ready.send(Err(error));
                        return;
                    }
                }
            } else {
                None
            };
            let witnessed = witness.is_some();
            let mut io = parent.child_holding(witness.map(|file| Arc::new(file) as Arc<dyn Send + Sync>));
            if !local && (config.writer_epoch.is_some() || config.compactor) {
                track_publications(&mut io, directory.clone(), config.deployment.clone(), id, config.writer_epoch);
            }
            let accepted = crate::admin::update_installation(directory.as_ref(), &config.deployment, |record| {
                if !matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
                    || record.storage_prefix() != config.prefix
                    || config.writer_epoch.is_some_and(|epoch| epoch != record.epoch)
                {
                    return Err(Anomaly::incorrect("rtdb.multi/storage-authority", format!("Installation {} no longer authorizes this store", config.deployment)));
                }
                record.storage_regions.insert(id, if local {
                    StorageRegion::Local
                } else {
                    remote_storage_region(None, hosted_task.clone())
                });
                Ok(())
            }).await;
            let opened = accepted.is_ok();
            // The admission predicate and missing installation are local
            // refusals. Other directory errors may hide an applied CAS.
            let uncertain = accepted.as_ref().err().is_some_and(|error| !matches!(
                error.code.as_str(), "rtdb.multi/storage-authority" | "rtdb.multi/unknown-installation"
            ));
            if ready.send(accepted.map(|record| (record, io.clone()))).is_ok() && opened {
                let _ = dropped.await;
            }
            io.close_and_wait().await;
            drop(io);
            // A failed registration may already be committed. Compare and
            // replace even if absent to fence that attempt's old version.
            if opened || uncertain {
                loop {
                    match release_region(directory.as_ref(), &config.deployment, id, uncertain).await {
                        Ok(()) => break,
                        Err(error) => eprintln!("Storage region {id} still open in {}: {}", config.deployment, error.code),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            if witnessed && let Err(error) = keys.forget_local_storage_region(id) {
                eprintln!("Could not remove storage region witness {id}: {}", error.code);
            }
        });
        let (record, region) = admitted.await.map_err(|_| Anomaly::new(
            rtdb_types::AnomalyCategory::Unavailable,
            "rtdb.multi/storage-admission",
            "The serving storage region could not be admitted",
        ))??;
        Ok((record, region, release))
    }

    async fn checkpoint(&self, config: &CellStoreConfig) -> Result<Option<i64>> {
        Ok(self.directory.deployment(&config.deployment).await?.and_then(|record| record.checkpoint).map(|checkpoint| checkpoint.block))
    }
}

pub(crate) fn remote_storage_region(archive: Option<Uuid>, owner: Option<HostedTask>) -> StorageRegion {
    StorageRegion::Remote { archive, owner, closed: false, publications: BTreeMap::new() }
}

pub(crate) fn track_publications(
    io: &mut Arc<IoRegion>,
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    writer_epoch: Option<u64>,
) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch, transfer: false },
    ));
}

pub(crate) fn track_transfer_publications(
    io: &mut Arc<IoRegion>,
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    epoch: u64,
) {
    Arc::get_mut(io).expect("the region has not been exposed").track_publications(Arc::new(
        DirectoryPublications { directory, deployment, region, writer_epoch: Some(epoch), transfer: true },
    ));
}

struct DirectoryPublications {
    directory: Arc<dyn Directory>,
    deployment: String,
    region: Uuid,
    writer_epoch: Option<u64>,
    transfer: bool,
}

#[async_trait]
impl PublicationJournal for DirectoryPublications {
    async fn admit(&self, key: &ObjectKey, attempt: Uuid, upload: &str) -> rtdb_storage::Result<()> {
        if attempt.is_nil() || upload.is_empty() {
            return Err(publication_error(key, storage_region_error("The provider upload has no identity")));
        }
        let publication = StoragePublication { key: key.to_string(), upload_id: upload.to_owned() };
        crate::admin::update_installation(self.directory.as_ref(), &self.deployment, |record| {
            let accepts_writes = if self.transfer {
                record.lifecycle == Lifecycle::Moving && record.move_from.is_some()
            } else {
                matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
            };
            if !accepts_writes || self.writer_epoch.is_some_and(|epoch| record.epoch != epoch)
            {
                return Err(storage_region_error("The installation no longer admits provider publications"));
            }
            let Some(StorageRegion::Remote { archive, closed: false, .. }) = record.storage_regions.get(&self.region) else {
                return Err(storage_region_error("The remote storage region is closed or absent"));
            };
            require_publication_key(record, *archive, key)?;
            let Some(StorageRegion::Remote { publications, .. }) = record.storage_regions.get_mut(&self.region) else {
                unreachable!("the publication region was just checked");
            };
            match publications.entry(attempt) {
                std::collections::btree_map::Entry::Vacant(entry) => { entry.insert(publication.clone()); }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &publication => {}
                _ => return Err(storage_region_error("The publication identifies another object")),
            }
            Ok(())
        }).await.map_err(|error| publication_error(key, error))?;
        Ok(())
    }

    async fn settle(&self, key: &ObjectKey, attempt: Uuid, upload: &str, submitted: bool) -> rtdb_storage::Result<()> {
        for _ in 0..8 {
            let Some(mut record) = self.directory.deployment(&self.deployment).await
                .map_err(|error| publication_error(key, error))? else { return Ok(()); };
            let Some(StorageRegion::Remote { publications, .. }) = record.storage_regions.get_mut(&self.region)
            else { return Ok(()); };
            match publications.get(&attempt) {
                Some(retained) if retained.key != key.as_str() || retained.upload_id != upload => {
                    return Err(publication_error(key, storage_region_error("The publication identifies another object")));
                }
                None if submitted => return Ok(()),
                _ => {}
            }
            publications.remove(&attempt);
            // A cancellation can race a committed admission whose reply was
            // lost. Even when absent, bump this record's version so that
            // admission cannot land after we confirm it was never sent.
            let version = record.version;
            if let Cas::Applied(_) = self.directory.replace_deployment(record, version).await
                .map_err(|error| publication_error(key, error))?
            {
                return Ok(());
            }
        }
        Err(publication_error(key, storage_region_error("The publication inventory kept changing")))
    }
}

fn publication_error(key: &ObjectKey, error: Anomaly) -> Error {
    Error::Remote { key: key.to_string(), message: error.code.into() }
}

pub(crate) async fn release_region(directory: &dyn Directory, deployment: &str, region: Uuid, uncertain: bool) -> Result<()> {
    for _ in 0..8 {
        let Some(mut record) = directory.deployment(deployment).await? else { return Ok(()); };
        match record.storage_regions.get_mut(&region) {
            Some(StorageRegion::Remote { closed, .. }) if *closed => return Ok(()),
            Some(StorageRegion::Remote { closed, publications, .. }) if !publications.is_empty() => *closed = true,
            None if !uncertain => return Ok(()),
            _ => { record.storage_regions.remove(&region); }
        }
        let version = record.version;
        if let Cas::Applied(_) = directory.replace_deployment(record, version).await? {
            return Ok(());
        }
    }
    Err(Anomaly::new(
        rtdb_types::AnomalyCategory::Conflict,
        "rtdb.multi/storage-release-conflict",
        "The installation changed while closing a storage region",
    ))
}

/// A local lock or ECS stop record can witness process death. Before
/// releasing a remote admission, retire its exact provider upload IDs.
pub(crate) async fn reconcile_storage_regions(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
) -> Result<()> {
    for (&region, state) in &record.storage_regions {
        match state {
            StorageRegion::Local if matches!(storage, CellStorage::Local { .. }) && keys.local_storage_region_drained(region)? => {
                release_region(directory, &record.id, region, false).await?;
                keys.forget_local_storage_region(region)?;
            }
            StorageRegion::Remote { archive, owner, publications, closed } => {
                if !*closed {
                    let Some(owner) = owner else { continue; };
                    if !directory.hosted_task_stopped(owner).await? {
                        continue;
                    }
                }
                if !publications.is_empty() {
                    abort_remote_publications(storage, record, *archive, publications).await?;
                }
                crate::admin::update_installation(directory, &record.id, |current| {
                    if current.storage_regions.get(&region) == Some(state) {
                        current.storage_regions.remove(&region);
                    }
                    Ok(())
                }).await?;
            }
            _ => {}
        }
    }
    Ok(())
}

async fn abort_remote_publications(
    serving: &CellStorage,
    record: &DeploymentRecord,
    operation: Option<Uuid>,
    publications: &BTreeMap<Uuid, StoragePublication>,
) -> Result<()> {
    let unrecognized = || storage_region_error("A remote region has no matching storage owner");
    let archive = operation.map(|operation| -> Result<CellStorage> {
        let registration = record.checkpoint_captures.get(&operation).ok_or_else(unrecognized)?;
        serde_json::from_str(&registration.archive).map_err(|_| unrecognized())
    }).transpose()?;
    let CellStorage::S3 { config } = archive.as_ref().unwrap_or(serving) else {
        return Err(unrecognized());
    };
    let inventory = CellStorage::s3_inventory(config).await?;
    for (&attempt, publication) in publications {
        let key = ObjectKey::new(publication.key.clone())?;
        if attempt.is_nil() || publication.upload_id.is_empty() {
            return Err(unrecognized());
        }
        require_publication_key(record, operation, &key)?;
        inventory.abort_publication(&key, &publication.upload_id).await?;
    }
    Ok(())
}

fn require_publication_key(record: &DeploymentRecord, operation: Option<Uuid>, key: &ObjectKey) -> Result<()> {
    let allowed = if let Some(operation) = operation {
        let prefix = ObjectKey::new(format!("checkpoint-captures/{}/{operation}", record.id))?;
        let witness = format!("checkpoint-authorities/{}/{operation}.json", record.id);
        record.checkpoint_captures.contains_key(&operation)
            && (key.is_under(&prefix) || key.as_str() == witness)
    } else {
        key.is_under(&ObjectKey::new(record.storage_prefix())?)
    };
    if allowed { return Ok(()); }
    Err(storage_region_error("The publication key is outside its storage scope"))
}

fn storage_region_error(message: &str) -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/storage-region", message)
}

#[derive(Clone, Copy)]
enum Fence {
    Writer { epoch: u64 },
    /// May write a compaction output, whose name its job fixes and whose effect waits on the holder's adoption.
    Compactor,
    Reader,
}

impl From<&CellStoreConfig> for Fence {
    fn from(config: &CellStoreConfig) -> Self {
        match (config.writer_epoch, config.compactor) {
            (Some(epoch), _) => Self::Writer { epoch },
            (None, true) => Self::Compactor,
            (None, false) => Self::Reader,
        }
    }
}

/// Whether `key` is a compaction output: a trie data or metadata file above level 0.
pub fn compaction_output(key: &str) -> bool {
    key.starts_with("tables/") && (key.contains("/data/l") || key.contains("/meta/l")) && !key.contains("/data/l00-") && !key.contains("/meta/l00-")
}

/// Milliseconds since the epoch, as a store stamps the releases and packs it writes.
pub type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// `time`, made strictly increasing: a fork at a past block keeps the releases stamped before the block's pack, so a release and a pack must never share a stamp.
pub fn stamps(time: impl Fn() -> i64 + Send + Sync + 'static) -> Clock {
    let last = std::sync::atomic::AtomicI64::new(i64::MIN);
    Arc::new(move || {
        let now = time();
        let previous = last.fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |previous| Some(previous.saturating_add(1).max(now))).expect("the update always applies");
        previous.saturating_add(1).max(now)
    })
}

/// The wall clock's stamps, one sequence for the whole process.
/// Stamps run ahead of the wall clock through a burst of releases, which only defers their reaping; the reaper reads its time from them too, so that in one process nothing is stamped after the time it reaps as of.
pub fn wall_clock() -> Clock {
    static WALL: std::sync::OnceLock<Clock> = std::sync::OnceLock::new();
    Arc::clone(WALL.get_or_init(|| stamps(now_ms)))
}

/// One store of a deployment in the cell's `bucket` (a prefix and a storage version), as a node opens it below its disk cache: its own objects and those its manifest references, packed, with deletes as releases.
pub async fn open_deployment_store(bucket: crate::reference::Bucket, store: &str, discovery: crate::packing::PackDiscovery, clock: Clock) -> Result<Arc<GarbageStore>> {
    open_deployment_store_with_archive(bucket, store, discovery, clock, None).await
}

pub(crate) async fn open_deployment_store_with_archive(
    bucket: crate::reference::Bucket,
    store: &str,
    discovery: crate::packing::PackDiscovery,
    clock: Clock,
    archive: Option<crate::archive::ArchivedCheckpoint>,
) -> Result<Arc<GarbageStore>> {
    let local: Arc<dyn ObjectStore> = Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), store.to_owned()));
    open_with_scoped_store(bucket, store, local, discovery, clock, archive).await
}

async fn open_with_scoped_store(
    bucket: crate::reference::Bucket,
    store: &str,
    local: Arc<dyn ObjectStore>,
    discovery: crate::packing::PackDiscovery,
    clock: Clock,
    archive: Option<crate::archive::ArchivedCheckpoint>,
) -> Result<Arc<GarbageStore>> {
    let base = crate::reference::Base::load(bucket.as_ref(), store).await?;
    let own: Arc<dyn ObjectStore> = if base.objects.is_empty() { local } else { Arc::new(crate::reference::ReferencingStore::new(local, bucket, base)) };
    let packed = crate::packing::PackingStore::open_with_clock(own, discovery, Arc::clone(&clock)).await?;
    let store = match archive {
        Some(archive) => archive.with_serving(packed),
        None => packed,
    };
    GarbageStore::open_with_clock(store, clock).await
}

struct FencedStore {
    inner: Arc<dyn ObjectStore>,
    directory: Arc<dyn Directory>,
    deployment: String,
    fence: Fence,
    _release: Option<oneshot::Sender<()>>,
}

impl FencedStore {
    async fn check(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        let read_only = || Error::ReadOnlyLog(PathBuf::from(key.as_str()));
        if matches!(self.fence, Fence::Reader)
            || matches!(self.fence, Fence::Compactor) && !compaction_output(key.as_str())
        {
            return Err(read_only());
        }
        let current = self.directory.deployment(&self.deployment).await
            .map_err(|error| Error::Remote { key: key.to_string(), message: error.code.into() })?
            .ok_or_else(|| Error::Remote { key: key.to_string(), message: "Deployment authority is missing".into() })?;
        match self.fence {
            Fence::Writer { epoch } if current.epoch == epoch
                && matches!(current.lifecycle, Lifecycle::Live | Lifecycle::Quarantined) => Ok(()),
            Fence::Writer { epoch } => Err(Error::Deposed { epoch, promised: current.epoch }),
            Fence::Compactor if current.lifecycle == Lifecycle::Live => Ok(()),
            Fence::Reader | Fence::Compactor => Err(read_only()),
        }
    }
}

#[async_trait]
impl ObjectStore for FencedStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.check(key).await?; self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.check(key).await?; self.inner.put_file(key, source).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(Error::ReadOnlyLog(PathBuf::from(key.as_str())))
    }
}

pub struct EncryptingStore { inner: Arc<dyn ObjectStore>, cipher: Cipher }

impl EncryptingStore {
    pub fn new(inner: Arc<dyn ObjectStore>, key: &[u8]) -> Result<Self> { Ok(Self { inner, cipher: Cipher::new(key)? }) }
    fn seal(&self, key: &ObjectKey, plaintext: &[u8]) -> rtdb_storage::Result<Bytes> { self.cipher.seal_deterministic(key.as_str(), plaintext) }
    fn open(&self, key: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes> { self.cipher.open(key.as_str(), &sealed) }
}

fn plain(object: StoredObject) -> StoredObject { StoredObject { size: object.size.saturating_sub(SEALED_OVERHEAD), ..object } }

#[async_trait]
impl ObjectStore for EncryptingStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await.map(plain) }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await.map(plain) }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.open(key, self.inner.get(key).await?) }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let plaintext = self.get(key).await?;
        let length = plaintext.len() as u64;
        if range.start > range.end || range.end > length { return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length }); }
        Ok(plaintext.slice(range.start as usize..range.end as usize))
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        // An immutable put retried with identical content is a completed retry; sealing twice gives different bytes, so the retry is recognised before sealing.
        if let Ok(existing) = self.get(key).await { return if existing == bytes { Ok(()) } else { Err(Error::Task(format!("{key} already exists with different content"))) }; }
        self.inner.put(key, self.seal(key, &bytes)?).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        let bytes = tokio::fs::read(source).await.map_err(|error| Error::Task(format!("cannot read {}: {error}", source.display())))?;
        self.put(key, Bytes::from(bytes)).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        Ok(self.inner.list(directory).await?.into_iter().map(plain).collect())
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
}

/// A garbage record: an object released by its deployment, which the reaper may erase once `released_at_ms` is older than its grace period.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GarbageRecord { pub key: String, pub released_at_ms: i64 }

/// Only a listing needs to know what has been released, so the released set is read on the first one, not on open.
pub struct GarbageStore {
    inner: Arc<dyn ObjectStore>,
    loaded_releases: tokio::sync::OnceCell<()>,
    released: Mutex<BTreeSet<ObjectKey>>,
    clock: Clock,
}

impl GarbageStore {
    pub async fn open(inner: Arc<dyn ObjectStore>) -> Result<Arc<Self>> { Self::open_with_clock(inner, wall_clock()).await }

    pub async fn open_with_clock(inner: Arc<dyn ObjectStore>, clock: Clock) -> Result<Arc<Self>> { Ok(Arc::new(Self { inner, loaded_releases: tokio::sync::OnceCell::new(), released: Mutex::new(BTreeSet::new()), clock })) }

    async fn load_releases(&self) -> rtdb_storage::Result<()> {
        self.loaded_releases.get_or_try_init(|| async {
            let mut released = BTreeSet::new();
            for object in self.inner.list(Some(&ObjectKey::new(GARBAGE_DIRECTORY).map_err(|error| Error::Task(error.to_string()))?)).await? {
                if let Ok(record) = serde_json::from_slice::<GarbageRecord>(&self.inner.get(&object.key).await?) {
                    released.insert(ObjectKey::new(record.key).map_err(|error| Error::Task(error.to_string()))?);
                }
            }
            self.released.lock().extend(released);
            Ok(())
        }).await?;
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for GarbageStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        self.load_releases().await?;
        let objects = self.inner.list(directory).await?;
        let released = self.released.lock();
        Ok(objects.into_iter().filter(|object| !object.key.as_str().starts_with(GARBAGE_DIRECTORY) && !released.contains(&object.key)).collect())
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        if self.released.lock().contains(key) { return Ok(()); }
        let record = GarbageRecord { key: key.as_str().to_owned(), released_at_ms: (self.clock)() };
        let name = ObjectKey::new(format!("{GARBAGE_DIRECTORY}/{:020}-{}", record.released_at_ms, uuid::Uuid::new_v4().simple()))
            .map_err(|error| Error::Task(error.to_string()))?;
        self.inner.put(&name, Bytes::from(serde_json::to_vec(&record).expect("a record serializes"))).await?;
        self.released.lock().insert(key.clone());
        Ok(())
    }
}

pub const ROOT_MARKER: &str = "~root";

/// Only a writer can create the ownership marker for a new store.
async fn confirm_root(store: &dyn ObjectStore, deployment: &str, writer: bool) -> Result<()> {
    let marker = ObjectKey::new(ROOT_MARKER)?;
    let owner = match store.get(&marker).await {
        Ok(owner) => owner,
        Err(Error::MissingObject(_)) if writer => match store.put(&marker, Bytes::copy_from_slice(deployment.as_bytes())).await {
            Ok(()) => return Ok(()),
            Err(Error::ConflictingObject(_)) => store.get(&marker).await?,
            Err(error) => return Err(error.into()),
        },
        Err(error) => return Err(error.into()),
    };
    if owner.as_ref() != deployment.as_bytes() {
        return Err(Anomaly::incorrect("rtdb.multi/foreign-root", format!("This store belongs to deployment {}, not {deployment}", String::from_utf8_lossy(&owner))));
    }
    Ok(())
}

/// What a reaper pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reaped { pub erased: usize, pub waiting: usize }

/// Erases every object a deployment released more than `grace` ago from the cell's bucket.
/// An object other deployments still reference is copied into them first; an object this deployment only references is dropped from its manifest.
pub async fn reap(bucket: &dyn ObjectStore, record: &DeploymentRecord, grace: Duration, references: &crate::reference::References) -> Result<Reaped> {
    reap_as_of(bucket, record, wall_clock()(), grace, references).await
}

/// `reap` as of `now_ms`, in milliseconds since the epoch.
pub async fn reap_as_of(bucket: &dyn ObjectStore, record: &DeploymentRecord, now_ms: i64, grace: Duration, references: &crate::reference::References) -> Result<Reaped> {
    use crate::reference::{Base, physical_objects};
    let mut reaped = Reaped::default();
    let threshold = now_ms - grace.as_millis() as i64;
    for store in crate::reference::stores(bucket, &record.storage_prefix()).await? {
        let mut base = Base::load(bucket, &store).await?;
        let before = base.clone();
        let mut own: BTreeSet<String> = physical_objects(bucket, &store).await?.into_iter().collect();
        // Where an object physically is: in this store, or in the one its manifest names.
        let physical = |own: &BTreeSet<String>, name: &str| -> Option<String> {
            if own.contains(name) { return Some(format!("{store}/{name}")); }
            before.objects.get(name).map(|owner| format!("{owner}/{name}"))
        };
        let garbage = format!("{GARBAGE_DIRECTORY}/");
        let records: BTreeSet<String> = own.iter().chain(before.objects.keys()).filter(|name| name.starts_with(&garbage)).cloned().collect();
        // A released object is either its own object or one entry of a pack; a pack goes once every entry in it has been released for the grace period.
        let mut due: BTreeMap<String, String> = BTreeMap::new();
        for name in records {
            let Some(location) = physical(&own, &name) else { continue };
            let Ok(bytes) = bucket.get(&crate::reference::key(&location)?).await else { continue };
            let Ok(released) = serde_json::from_slice::<GarbageRecord>(&bytes) else { continue };
            if released.released_at_ms > threshold { reaped.waiting += 1; continue; }
            due.insert(released.key, name);
        }
        let mut forgotten = BTreeSet::new();
        let packs: BTreeSet<String> = own.iter().chain(before.objects.keys()).filter(|name| name.starts_with(&format!("{}/", crate::packing::PACK_DIRECTORY))).cloned().collect();
        // The latest block's pack names every other pack, so a cold start finds them all through it: it stays.
        let latest = packs.iter().filter(|pack| pack.starts_with(&format!("{}/b", crate::packing::PACK_DIRECTORY))).max().cloned();
        // A cold start walks block by block from the published checkpoint's pack to the latest, stopping at the first one missing: from the checkpoint on, every block's pack stays.
        let walked = |pack: &str| record.checkpoint.is_some_and(|checkpoint| crate::packing::pack_block(pack).is_some_and(|block| block >= checkpoint.block));
        let mut packed = BTreeSet::new();
        let mut obsolete_packs = Vec::new();
        for pack in packs {
            let Some(location) = physical(&own, &pack) else { continue };
            let index = match crate::packing::read_index(bucket, &crate::reference::key(&location)?).await {
                Err(Error::MissingObject(_)) if !own.contains(&pack) => {
                    // The owner can copy this pack into the fork after its inventory was read.
                    match crate::packing::read_index(bucket, &crate::reference::key(&format!("{store}/{pack}"))?).await {
                        Ok(index) => { own.insert(pack.clone()); index }
                        Err(Error::MissingObject(_)) => { forgotten.insert(pack); continue; }
                        Err(error) => return Err(error.into()),
                    }
                }
                result => result?,
            };
            if Some(&pack) != latest.as_ref() && !walked(&pack) && index.entries.keys().all(|name| due.contains_key(name)) {
                obsolete_packs.push(pack);
            } else {
                packed.extend(index.entries.into_keys());
            }
        }
        for pack in obsolete_packs {
            if own.contains(&pack) { references.erase(bucket, &store, &pack).await?; }
            if before.objects.contains_key(&pack) { forgotten.insert(pack); }
        }
        for name in due.keys() {
            if own.contains(name) { references.erase(bucket, &store, name).await?; }
            if before.objects.contains_key(name) { forgotten.insert(name.clone()); }
        }
        let mut local_records = Vec::new();
        for (name, garbage_record) in &due {
            if !packed.contains(name) {
                if own.contains(garbage_record) { local_records.push(garbage_record); } else { forgotten.insert(garbage_record.clone()); }
                reaped.erased += 1;
            } else { reaped.waiting += 1; }
        }
        for name in &forgotten { base.objects.remove(name); }
        if base != before { base.save(bucket, &store).await?; }
        for garbage_record in local_records { references.erase(bucket, &store, garbage_record).await?; }
    }
    Ok(reaped)
}

/// When a garbage record's name says it was released, in milliseconds since the epoch.
pub fn garbage_released_at(name: &str) -> Option<i64> {
    name.strip_prefix(&format!("{GARBAGE_DIRECTORY}/"))?.split('-').next()?.parse().ok()
}

pub async fn run_reaper(
    directory: Arc<dyn Directory>,
    storage: crate::deployment::CellStorage,
    keys: Option<Arc<dyn KeyService>>,
    grace: Duration,
    interval: Duration,
    cancellation: tokio_util::sync::CancellationToken,
) {
    let Ok(bucket) = storage.bucket(IoRegion::new()).await else { return };
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(interval) => {
                if let Ok(deployments) = directory.deployments().await {
                    if let Some(keys) = keys.as_deref() {
                        for record in deployments.iter().filter(|record| record.storage_regions.values()
                            .any(|region| matches!(region, StorageRegion::Remote { closed: true, .. })))
                        {
                            if cancellation.is_cancelled() { return; }
                            if let Err(error) = reconcile_storage_regions(directory.as_ref(), &storage, keys, record).await {
                                eprintln!("Storage publications for {} are pending: {}", record.id, error.code);
                            }
                        }
                    }
                    let now = wall_clock()();
                    let _ = reaper_pass(bucket.as_ref(), &deployments, now, grace).await;
                    if let Some(keys) = keys.as_deref() {
                        for record in deployments.iter().filter(|record| {
                            record.lifecycle == Lifecycle::Deleting || move_cleanup_due(record, now, grace)
                        }) {
                            let disposal = crate::admin::finish_disposal_at(
                                directory.as_ref(), &storage, keys, &record.id, Some(record.incarnation), 4 << 20,
                            );
                            match cancellation.run_until_cancelled(disposal).await {
                                None => return,
                                Some(Err(error)) => eprintln!("Disposal of {} is pending: {}", record.id, error.code),
                                Some(Ok(_)) => {}
                            }
                        }
                    }
                    for record in deployments.iter().filter(|record| record.archive_retirement.as_ref()
                        .is_some_and(|retirement| retirement.observed_empty_at_ms.is_none()))
                    {
                        let Ok(id) = record.id.parse::<Uuid>() else { continue; };
                        match cancellation.run_until_cancelled(crate::archive::finish_retirement(directory.as_ref(), id)).await {
                            None => return,
                            Some(Err(error)) => eprintln!("Archive retirement of {id} is pending: {}", error.code),
                            Some(Ok(_)) => {},
                        }
                    }
                }
            }
        }
    }
}

/// A moved source retains its serving media until the route is published
/// to a prepared destination and the grace has elapsed.
pub fn move_cleanup_due(record: &DeploymentRecord, now_ms: i64, grace: Duration) -> bool {
    record.lifecycle == Lifecycle::Deleted && record.disposal.is_none()
        && record.moved_at_ms.is_some_and(|moved| now_ms.saturating_sub(moved) >= grace.as_millis() as i64)
}

/// Reaps live and quarantined deployments against the references of the cell.
/// Transfer and disposal fences keep their inventories stable.
/// A deployment that fails to reap does not stop the others'.
pub async fn reaper_pass(bucket: &dyn ObjectStore, deployments: &[DeploymentRecord], now_ms: i64, grace: Duration) -> Result<Vec<Result<Reaped>>> {
    let references = crate::reference::References::scan(bucket, deployments).await?;
    let mut reaped = Vec::new();
    for record in deployments.iter().filter(|record| matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)) {
        reaped.push(reap_as_of(bucket, record, now_ms, grace, &references).await);
    }
    Ok(reaped)
}


#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_storage::LocalStore;
    use rtdb_storage::MemoryStore;

    fn key(name: &str) -> ObjectKey { ObjectKey::new(name).unwrap() }

    struct PutFailure {
        inner: MemoryStore,
        fail: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ObjectStore for PutFailure {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            if self.fail.swap(false, std::sync::atomic::Ordering::Relaxed) {
                return Err(Error::Task("injected object-store write failure".into()));
            }
            self.inner.put(key, bytes).await
        }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn a_failed_root_marker_write_is_returned_to_activation() {
        let inner = PutFailure { inner: MemoryStore::default(), fail: true.into() };
        assert_eq!(confirm_root(&inner, "shop", true).await.unwrap_err().code, "rtdb.storage/task");
        assert!(matches!(inner.get(&key(ROOT_MARKER)).await, Err(Error::MissingObject(_))));
        confirm_root(&inner, "shop", true).await.unwrap();
    }

    #[tokio::test]
    async fn a_failed_release_is_visible_and_can_be_retried() {
        let inner = Arc::new(PutFailure { inner: MemoryStore::default(), fail: false.into() });
        let store = GarbageStore::open(inner.clone()).await.unwrap();
        let object = key("tables/t/data/a.arrow");
        store.put(&object, Bytes::from_static(b"rows")).await.unwrap();
        assert_eq!(store.list(None).await.unwrap().len(), 1);
        inner.fail.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(store.delete(&object).await.is_err());
        assert_eq!(store.list(None).await.unwrap().len(), 1);
        store.delete(&object).await.unwrap();
        assert!(GarbageStore::open(inner).await.unwrap().list(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn objects_are_unreadable_without_their_deployments_key() {
        let raw: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let store = EncryptingStore::new(Arc::clone(&raw), &[7; 32]).unwrap();
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"secret rows")).await.unwrap();
        assert_eq!(store.get(&key("blocks/b00.binpb")).await.unwrap().as_ref(), b"secret rows");
        assert_eq!(store.get_range(&key("blocks/b00.binpb"), 7..11).await.unwrap().as_ref(), b"rows");
        assert_eq!(store.head(&key("blocks/b00.binpb")).await.unwrap().size, 11, "sizes are the plaintext's");
        assert!(!raw.get(&key("blocks/b00.binpb")).await.unwrap().windows(6).any(|window| window == b"secret"), "nothing is stored in the clear");
        assert!(EncryptingStore::new(Arc::clone(&raw), &[8; 32]).unwrap().get(&key("blocks/b00.binpb")).await.is_err(), "another key cannot read it");
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"secret rows")).await.unwrap();
    }

    #[tokio::test]
    async fn a_node_delete_releases_an_object_and_only_the_reaper_erases_it() {
        let directory = tempfile::tempdir().unwrap();
        let record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let root = directory.path().join(record.storage_prefix()).join("v06");
        let store = GarbageStore::open(Arc::new(LocalStore::open(&root).unwrap())).await.unwrap();
        store.put(&key("tables/t/data/a.arrow"), Bytes::from_static(b"a")).await.unwrap();
        store.delete(&key("tables/t/data/a.arrow")).await.unwrap();
        assert!(root.join("tables/t/data/a.arrow").exists(), "a node's delete erases nothing");
        assert!(store.list(Some(&key("tables"))).await.unwrap().is_empty(), "a released object is no longer listed");
        assert_eq!(GarbageStore::open(Arc::new(LocalStore::open(&root).unwrap())).await.unwrap().list(Some(&key("tables"))).await.unwrap().len(), 0, "and stays unlisted across a reopen");
        let bucket = LocalStore::open(directory.path()).unwrap();
        assert_eq!(reap(&bucket, &record, Duration::from_secs(3600), &Default::default()).await.unwrap(), Reaped { erased: 0, waiting: 1 }, "within the grace period it waits");
        assert_eq!(reap(&bucket, &record, Duration::ZERO, &Default::default()).await.unwrap(), Reaped { erased: 1, waiting: 0 });
        assert!(!root.join("tables/t/data/a.arrow").exists());
    }

    /// Opens the store of `record`'s storage version under `root` as a node would, references and all.
    async fn open_store(root: &Path, record: &DeploymentRecord) -> (PathBuf, crate::reference::ReferencingStore) {
        let bucket: crate::reference::Bucket = Arc::new(LocalStore::open(root).unwrap());
        let store = format!("{}/v06", record.storage_prefix());
        let own: Arc<dyn ObjectStore> = Arc::new(crate::reference::PrefixedStore::new(Arc::clone(&bucket), store.clone()));
        let base = crate::reference::Base::load(bucket.as_ref(), &store).await.unwrap();
        (root.join(&store), crate::reference::ReferencingStore::new(own, bucket, base))
    }

    #[tokio::test]
    async fn a_fork_reads_what_it_references_until_it_releases_it_and_keeps_it_when_its_parent_does() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let child = DeploymentRecord::new("shop-preview", "acme", vec![], 0);
        let (parent_store, raw) = open_store(root, &parent).await;
        let parent_view = GarbageStore::open(Arc::new(raw)).await.unwrap();
        for name in ["tables/t/data/kept.arrow", "tables/t/data/parent-drops.arrow", "tables/t/data/child-drops.arrow"] { parent_view.put(&key(name), Bytes::from_static(b"rows")).await.unwrap(); }
        let bucket = LocalStore::open(root).unwrap();
        crate::reference::fork(&bucket, &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        let (child_store, raw) = open_store(root, &child).await;
        let child_view = GarbageStore::open(Arc::new(raw)).await.unwrap();
        assert_eq!(child_view.list(Some(&key("tables"))).await.unwrap().len(), 3, "a fork lists what it references");
        assert_eq!(child_view.get(&key("tables/t/data/kept.arrow")).await.unwrap().as_ref(), b"rows");
        child_view.put(&key("tables/t/data/kept.arrow"), Bytes::from_static(b"rows")).await.unwrap();
        assert!(child_view.put(&key("tables/t/data/kept.arrow"), Bytes::from_static(b"other")).await.is_err(), "put-if-absent holds across a reference");

        parent_view.delete(&key("tables/t/data/parent-drops.arrow")).await.unwrap();
        child_view.delete(&key("tables/t/data/child-drops.arrow")).await.unwrap();
        let records = [parent.clone(), child.clone()];
        let references = crate::reference::References::scan(&bucket, &records).await.unwrap();
        reap(&bucket, &parent, Duration::ZERO, &references).await.unwrap();
        reap(&bucket, &child, Duration::ZERO, &references).await.unwrap();
        assert!(!parent_store.join("tables/t/data/parent-drops.arrow").exists(), "the parent's release erases its object");
        assert!(child_store.join("tables/t/data/parent-drops.arrow").exists(), "after copying it into the fork that still uses it");
        assert!(parent_store.join("tables/t/data/child-drops.arrow").exists(), "the fork's release leaves its parent's object alone");
        let (_, raw) = open_store(root, &child).await;
        let reopened = GarbageStore::open(Arc::new(raw)).await.unwrap();
        let mut listed: Vec<String> = reopened.list(Some(&key("tables"))).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        listed.sort();
        assert_eq!(listed, ["tables/t/data/kept.arrow", "tables/t/data/parent-drops.arrow"], "and the fork no longer references it");
    }

    #[tokio::test]
    async fn a_holders_store_refuses_writes_once_the_directory_moves_past_its_epoch() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let store = FencedStore {
            inner: Arc::new(MemoryStore::default()),
            directory: directory.clone(),
            deployment: record.id.clone(),
            fence: Fence::Writer { epoch: 0 },
            _release: None,
        };
        store.put(&key("blocks/a"), Bytes::from_static(b"a")).await.unwrap();
        record.epoch = 1;
        directory.replace_deployment(record.clone(), record.version).await.unwrap();
        assert!(matches!(store.put(&key("blocks/b"), Bytes::from_static(b"b")).await, Err(Error::Deposed { epoch: 0, promised: 1 })), "a deposed holder writes nothing");
        assert!(store.delete(&key("blocks/a")).await.is_err(), "and releases nothing");
        assert_eq!(store.get(&key("blocks/a")).await.unwrap().as_ref(), b"a", "reads still serve its snapshots");
    }

    #[tokio::test]
    async fn a_compactors_store_refuses_outputs_after_disposal_is_ordered() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let installation = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let store = FencedStore {
            inner: Arc::new(MemoryStore::default()),
            directory: directory.clone(),
            deployment: installation.id.clone(),
            fence: Fence::Compactor,
            _release: None,
        };
        let output = key("tables/t/data/l01-first.arrow");
        store.put(&output, Bytes::from_static(b"first")).await.unwrap();

        crate::admin::begin_disposal(directory.as_ref(), &installation.id).await.unwrap();
        let late_output = key("tables/t/data/l01-late.arrow");
        assert!(matches!(
            store.put(&late_output, Bytes::from_static(b"late")).await,
            Err(Error::ReadOnlyLog(_))
        ));
    }

    #[tokio::test]
    async fn retirement_waits_for_storage_work_after_its_caller_is_cancelled() {
        let files = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap());
        let storage = crate::deployment::CellStorage::Local { root: files.path().join("objects") };
        let directory = rtdb_directory::MemoryDirectory::new();
        let record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let key = keys.key(&record.key_id()).await.unwrap();
        let factory = CellStoreFactory {
            directory: directory.clone(),
            keys: keys.clone(),
            hosted_task: None,
            storage_regions: TaskTracker::new(),
        };
        let config = CellStoreConfig {
            storage: storage.clone(),
            prefix: record.storage_prefix(),
            deployment: record.id.clone(),
            writer_epoch: Some(record.epoch),
            compactor: false,
            snapshot_block: None,
        };
        let (admitted, region, attachment) = factory.admit_region(&config, IoRegion::new()).await.unwrap();
        assert_eq!(admitted.storage_regions.len(), 1);

        let (entered, active) = oneshot::channel();
        let (complete, wait) = std::sync::mpsc::channel();
        let caller = tokio::spawn(async move {
            region.run_blocking(move || {
                entered.send(()).unwrap();
                wait.recv().unwrap();
                Ok(())
            }).await
        });
        active.await.unwrap();
        caller.abort();
        let _ = caller.await;
        drop(attachment);
        crate::admin::begin_disposal(directory.as_ref(), &record.id).await.unwrap();
        let pending = crate::admin::finish_disposal(directory.as_ref(), &storage, keys.as_ref(), &record.id, 4 << 20)
            .await.unwrap_err();
        assert_eq!(pending.code, "rtdb.multi/storage-not-drained");
        assert_eq!(keys.key(&record.key_id()).await.unwrap(), key);
        assert!(factory.admit_region(&config, IoRegion::new()).await.is_err());

        complete.send(()).unwrap();
        factory.storage_regions.close();
        factory.storage_regions.wait().await;
        let receipt = crate::admin::finish_disposal(directory.as_ref(), &storage, keys.as_ref(), &record.id, 4 << 20)
            .await.unwrap();
        assert_eq!(receipt.deployment, record.id);
        assert!(keys.key(&record.key_id()).await.is_err());
    }

    #[tokio::test]
    async fn a_remote_region_records_the_write_before_submission_and_keeps_it_after_close() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let region = Uuid::new_v4();
        let attempt = Uuid::new_v4();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_regions.insert(region, remote_storage_region(None, None));
        let record = directory.create_deployment(record).await.unwrap();
        let object = key(&format!("{}/v06/data", record.storage_prefix()));
        let journal = DirectoryPublications {
            directory: directory.clone(), deployment: record.id.clone(), region, writer_epoch: Some(record.epoch), transfer: false,
        };
        assert!(journal.admit(&key("another-installation/data"), attempt, "upload").await.is_err());
        journal.admit(&object, attempt, "upload").await.unwrap();
        let before = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(before.storage_regions[&region], StorageRegion::Remote {
            archive: None, owner: None, closed: false, publications: BTreeMap::from([(attempt, StoragePublication { key: object.to_string(), upload_id: "upload".to_owned() })]),
        });
        release_region(directory.as_ref(), &record.id, region, false).await.unwrap();
        release_region(directory.as_ref(), &record.id, region, false).await.unwrap();
        assert!(journal.admit(&object, Uuid::new_v4(), "upload").await.is_err());
        let retained = directory.deployment(&record.id).await.unwrap().unwrap();
        assert_eq!(retained.storage_regions[&region], StorageRegion::Remote {
            archive: None, owner: None, closed: true, publications: BTreeMap::from([(attempt, StoragePublication { key: object.to_string(), upload_id: "upload".to_owned() })]),
        });
    }

    #[tokio::test]
    async fn the_local_reaper_recovers_a_region_after_its_owner_drops_the_witness() {
        let files = tempfile::tempdir().unwrap();
        let keys = crate::keys::LocalKeyService::new(files.path().join("keys")).unwrap();
        let storage = crate::deployment::CellStorage::Local { root: files.path().join("objects") };
        let directory = rtdb_directory::MemoryDirectory::new();
        let region = Uuid::new_v4();
        let witness = keys.hold_local_storage_region(region).unwrap().unwrap();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.storage_regions.insert(region, StorageRegion::Local);
        let record = directory.create_deployment(record).await.unwrap();
        crate::admin::begin_disposal(directory.as_ref(), &record.id).await.unwrap();

        let pending = crate::admin::finish_disposal(directory.as_ref(), &storage, &keys, &record.id, 4 << 20)
            .await.unwrap_err();
        assert_eq!(pending.code, "rtdb.multi/storage-not-drained");
        drop(witness);
        let receipt = crate::admin::finish_disposal(directory.as_ref(), &storage, &keys, &record.id, 4 << 20)
            .await.unwrap();
        assert_eq!(receipt.deployment, record.id);
        assert!(!keys.local_storage_region_drained(region).unwrap(), "the consumed witness is removed");
    }

    #[tokio::test]
    async fn the_reaper_erases_a_pack_once_every_object_in_it_is_released() {
        let directory = tempfile::tempdir().unwrap();
        let record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let root = directory.path().join(record.storage_prefix()).join("v06");
        let local: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(&root).unwrap());
        let store = GarbageStore::open(crate::packing::PackingStore::open(local, crate::packing::PackDiscovery::List).await.unwrap()).await.unwrap();
        for name in ["tables/t/data/l00-rc-b00.arrow", "tables/t/meta/l00-rc-b00.arrow"] { store.put(&key(name), Bytes::from_static(b"x")).await.unwrap(); }
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        store.put(&key("blocks/b01.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        let packs = || std::fs::read_dir(root.join(crate::packing::PACK_DIRECTORY)).map(|entries| entries.count()).unwrap_or(0);
        assert_eq!(packs(), 2, "each block has its pack");
        store.delete(&key("tables/t/data/l00-rc-b00.arrow")).await.unwrap();
        let bucket = LocalStore::open(directory.path()).unwrap();
        assert_eq!(reap(&bucket, &record, Duration::ZERO, &Default::default()).await.unwrap(), Reaped { erased: 0, waiting: 1 }, "half a pack released keeps the pack");
        assert_eq!(packs(), 2);
        store.delete(&key("tables/t/meta/l00-rc-b00.arrow")).await.unwrap();
        store.delete(&key("blocks/b00.binpb")).await.unwrap();
        assert_eq!(reap(&bucket, &record, Duration::ZERO, &Default::default()).await.unwrap().erased, 3);
        assert_eq!(packs(), 1, "a fully released pack is erased, and the latest block's, which names every pack, stays");
    }

    #[tokio::test]
    async fn a_fork_at_a_past_block_keeps_an_object_released_in_the_same_millisecond_after_the_block() {
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let clock = stamps(|| 1_000);
        let store = open_deployment_store(Arc::clone(&bucket), "p/v06", crate::packing::PackDiscovery::List, Arc::clone(&clock)).await.unwrap();
        store.put(&key("tables/t/data/l01-rc-b00.arrow"), Bytes::from_static(b"compacted")).await.unwrap();
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        store.delete(&key("tables/t/data/l01-rc-b00.arrow")).await.unwrap();
        crate::reference::fork(bucket.as_ref(), "p", "c", Some(0)).await.unwrap();
        let fork = open_deployment_store(Arc::clone(&bucket), "c/v06", crate::packing::PackDiscovery::List, clock).await.unwrap();
        let listed: Vec<String> = fork.list(Some(&key("tables"))).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(listed, ["tables/t/data/l01-rc-b00.arrow"], "the release came after the block, though within its millisecond");
    }

    #[test]
    fn stamps_rise_strictly_though_the_clock_stands_still_or_goes_back() {
        let time = Arc::new(std::sync::atomic::AtomicI64::new(5));
        let source = Arc::clone(&time);
        let clock = stamps(move || source.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!([clock(), clock()], [5, 6]);
        time.store(3, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(clock(), 7);
        time.store(20, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(clock(), 20);
    }

    #[tokio::test]
    async fn a_store_opened_from_a_checkpoint_behind_its_latest_block_still_finds_it_after_the_reaper_passes() {
        let bucket: crate::reference::Bucket = Arc::new(MemoryStore::default());
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let store = format!("{}/v06", record.storage_prefix());
        let open = |discovery| open_deployment_store(Arc::clone(&bucket), &store, discovery, wall_clock());
        let writer = open(crate::packing::PackDiscovery::List).await.unwrap();
        for block in 0..3i64 {
            writer.put(&key(&format!("tables/t/data/l00-rc-b0{block}.arrow")), Bytes::from(format!("rows {block}"))).await.unwrap();
            writer.put(&key(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block)).unwrap()), Bytes::from_static(b"catalog")).await.unwrap();
        }
        record.checkpoint = Some(rtdb_directory::Checkpoint { block: 0, boundary: 0, source: 0, system_time: 0 });
        for name in ["tables/t/data/l00-rc-b01.arrow", "blocks/b01.binpb"] { writer.delete(&key(name)).await.unwrap(); }
        reap(bucket.as_ref(), &record, Duration::ZERO, &Default::default()).await.unwrap();
        let cold = open(crate::packing::PackDiscovery::From(0)).await.unwrap();
        assert_eq!(cold.get(&key("tables/t/data/l00-rc-b02.arrow")).await.unwrap().as_ref(), b"rows 2", "a block after a fully released one is found from the checkpoint");
    }

    #[tokio::test]
    async fn a_store_refuses_a_deployment_it_was_not_created_for() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalStore::open(directory.path()).unwrap();
        assert!(confirm_root(&store, "one", false).await.is_err());
        assert!(matches!(store.get(&key(ROOT_MARKER)).await, Err(Error::MissingObject(_))));
        confirm_root(&store, "one", true).await.unwrap();
        confirm_root(&store, "one", false).await.unwrap();
        assert_eq!(confirm_root(&store, "two", true).await.unwrap_err().code, "rtdb.multi/foreign-root");
    }
}
