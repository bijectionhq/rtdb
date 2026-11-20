//! A deployment's object store as a node sees it (contract `ObjectStorage`): fenced by the holder's epoch, encrypted under the deployment's key, and unable to delete.
//!
//! - `FencedStore` refuses every write once the directory has moved the deployment past the epoch the store was opened at (`ZombiesCannotDamageStorage`).
//! - `EncryptingStore` seals every object with AES-256-GCM under the deployment's key, bound to the object's key, so destroying the key makes every copy unreadable.
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
use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, now_ms};
use rtdb_node::RemoteStorageFactory;
use rtdb_storage::{Error, IoRegion, LocalStore, ObjectKey, ObjectStore, object_store::StoredObject};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::cipher::{Cipher, SEALED_OVERHEAD as OVERHEAD};
use crate::keys::KeyService;

/// The protobuf type name the cell's storage factory is registered under.
pub const CELL_STORAGE_TYPE: &str = "rtdb.multi.CellStorage";
pub const GARBAGE_DIRECTORY: &str = "~garbage";
const SEALED_OVERHEAD: u64 = OVERHEAD as u64;

/// The deployment store a node opens, carried in its `DatabaseConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellStoreConfig {
    pub root: PathBuf,
    pub prefix: String,
    pub deployment: String,
    /// The epoch a holder opened the store at; absent for a reader, which never writes.
    pub writer_epoch: Option<u64>,
    /// A compaction worker's store, which may write compaction outputs and nothing else.
    #[serde(default)]
    pub compactor: bool,
}

pub struct CellStoreFactory {
    pub directory: Arc<dyn Directory>,
    pub keys: Arc<dyn KeyService>,
    pub fence_interval: Duration,
}

#[async_trait]
impl RemoteStorageFactory for CellStoreFactory {
    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        let local = LocalStore::open_in(config.root.join(&config.prefix).join(storage_root.as_str()), io)?;
        let packed = crate::packing::PackingStore::open(Arc::new(local)).await?;
        Ok(GarbageStore::open(packed).await?)
    }

    /// Encryption sits above the node's disk cache, which therefore holds ciphertext only: destroying the key leaves no readable copy on any node.
    async fn wrap_cached(&self, configuration: &Any, cached: Arc<dyn ObjectStore>) -> Result<Arc<dyn ObjectStore>> {
        let config: CellStoreConfig = serde_json::from_slice(&configuration.value)
            .map_err(|error| Anomaly::incorrect("rtdb.multi/storage-config", format!("Cell storage configuration is malformed: {error}")))?;
        let encrypted = EncryptingStore::new(cached, &self.keys.key(&config.deployment)?)?;
        let fence = match (config.writer_epoch, config.compactor) {
            (Some(epoch), _) => Fence::Writer { epoch, record: rtdb_directory::watch_deployment(Arc::clone(&self.directory), config.deployment.clone(), self.fence_interval) },
            (None, true) => Fence::Compactor,
            (None, false) => Fence::Reader,
        };
        Ok(Arc::new(FencedStore { inner: Arc::new(encrypted), fence }))
    }
}

enum Fence {
    Writer { epoch: u64, record: watch::Receiver<Option<DeploymentRecord>> },
    /// May write a compaction output, whose name its job fixes and whose effect waits on the holder's adoption.
    Compactor,
    Reader,
}

/// Whether `key` is a compaction output: a trie data or metadata file above level 0.
pub fn compaction_output(key: &str) -> bool {
    key.starts_with("tables/") && (key.contains("/data/l") || key.contains("/meta/l")) && !key.contains("/data/l00-") && !key.contains("/meta/l00-")
}

pub struct FencedStore { inner: Arc<dyn ObjectStore>, fence: Fence }

impl FencedStore {
    fn check(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        match &self.fence {
            Fence::Reader => Err(Error::ReadOnlyLog(PathBuf::from(key.as_str()))),
            Fence::Compactor if compaction_output(key.as_str()) => Ok(()),
            Fence::Compactor => Err(Error::ReadOnlyLog(PathBuf::from(key.as_str()))),
            Fence::Writer { epoch, record } => match &*record.borrow() {
                Some(current) if current.epoch != *epoch || current.lifecycle != Lifecycle::Live && current.lifecycle != Lifecycle::Quarantined =>
                    Err(Error::Deposed { epoch: *epoch, promised: current.epoch }),
                _ => Ok(()),
            },
        }
    }
}

#[async_trait]
impl ObjectStore for FencedStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.check(key)?; self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.check(key)?; self.inner.put_file(key, source).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        if matches!(self.fence, Fence::Compactor) { return Err(Error::ReadOnlyLog(PathBuf::from(key.as_str()))); }
        self.check(key)?;
        self.inner.delete(key).await
    }
}

pub struct EncryptingStore { inner: Arc<dyn ObjectStore>, cipher: Cipher }

impl EncryptingStore {
    pub fn new(inner: Arc<dyn ObjectStore>, key: &[u8]) -> Result<Self> { Ok(Self { inner, cipher: Cipher::new(key)? }) }
    fn seal(&self, key: &ObjectKey, plaintext: &[u8]) -> rtdb_storage::Result<Bytes> { self.cipher.seal(key.as_str(), plaintext) }
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

pub struct GarbageStore { inner: Arc<dyn ObjectStore>, released: Mutex<BTreeSet<ObjectKey>> }

impl GarbageStore {
    pub async fn open(inner: Arc<dyn ObjectStore>) -> Result<Arc<Self>> {
        let mut released = BTreeSet::new();
        for object in inner.list(Some(&ObjectKey::new(GARBAGE_DIRECTORY)?)).await? {
            if let Ok(record) = serde_json::from_slice::<GarbageRecord>(&inner.get(&object.key).await?) { released.insert(ObjectKey::new(record.key)?); }
        }
        Ok(Arc::new(Self { inner, released: Mutex::new(released) }))
    }
    fn visible(&self, object: &StoredObject) -> bool {
        !object.key.as_str().starts_with(GARBAGE_DIRECTORY) && !self.released.lock().contains(&object.key)
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
        Ok(self.inner.list(directory).await?.into_iter().filter(|object| self.visible(object)).collect())
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        if !self.released.lock().insert(key.clone()) { return Ok(()); }
        let record = GarbageRecord { key: key.as_str().to_owned(), released_at_ms: now_ms() };
        let name = ObjectKey::new(format!("{GARBAGE_DIRECTORY}/{:020}-{}", record.released_at_ms, uuid::Uuid::new_v4().simple()))
            .map_err(|error| Error::Task(error.to_string()))?;
        self.inner.put(&name, Bytes::from(serde_json::to_vec(&record).expect("a record serializes"))).await
    }
}

/// What a reaper pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reaped { pub erased: usize, pub waiting: usize }

/// Erases every object a deployment released more than `grace` ago, under a cell's local storage root.
pub fn reap_local(root: &Path, record: &DeploymentRecord, grace: Duration) -> Result<Reaped> {
    let prefix = root.join(record.storage_prefix());
    let mut reaped = Reaped::default();
    let Ok(versions) = std::fs::read_dir(&prefix) else { return Ok(reaped); };
    let threshold = now_ms() - grace.as_millis() as i64;
    for version in versions.flatten() {
        let Ok(records) = std::fs::read_dir(version.path().join(GARBAGE_DIRECTORY)) else { continue };
        // A released object is either its own file or one entry of a pack; a pack goes once every entry in it has been released for the grace period.
        let mut due: BTreeMap<String, PathBuf> = BTreeMap::new();
        for entry in records.flatten() {
            let Ok(bytes) = std::fs::read(entry.path()) else { continue };
            let Ok(released) = serde_json::from_slice::<GarbageRecord>(&bytes) else { continue };
            if released.released_at_ms > threshold { reaped.waiting += 1; continue; }
            due.insert(released.key, entry.path());
        }
        let mut erased: BTreeSet<String> = BTreeSet::new();
        for (key, _) in &due {
            let object = version.path().join(key);
            if object.exists() { std::fs::remove_file(&object).map_err(io)?; erased.insert(key.clone()); }
        }
        let mut packed: BTreeSet<String> = BTreeSet::new();
        if let Ok(packs) = std::fs::read_dir(version.path().join(crate::packing::PACK_DIRECTORY)) {
            for pack in packs.flatten() {
                let Some(index) = read_pack_index(&pack.path()) else { continue };
                if index.entries.keys().all(|key| due.contains_key(key)) {
                    std::fs::remove_file(pack.path()).map_err(io)?;
                    erased.extend(index.entries.into_keys());
                } else {
                    packed.extend(index.entries.into_keys());
                }
            }
        }
        for (key, record) in &due {
            if erased.contains(key) || !packed.contains(key) { std::fs::remove_file(record).map_err(io)?; reaped.erased += 1; } else { reaped.waiting += 1; }
        }
    }
    Ok(reaped)
}

fn read_pack_index(path: &Path) -> Option<crate::packing::PackIndex> {
    let bytes = std::fs::read(path).ok()?;
    let length = u64::from_be_bytes(bytes.get(bytes.len().checked_sub(8)?..)?.try_into().ok()?) as usize;
    let start = bytes.len().checked_sub(8 + length)?;
    serde_json::from_slice(&bytes[start..bytes.len() - 8]).ok()
}

pub async fn run_reaper(directory: Arc<dyn Directory>, root: PathBuf, grace: Duration, interval: Duration, cancellation: tokio_util::sync::CancellationToken) {
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(interval) => {
                if let Ok(deployments) = directory.deployments().await {
                    for record in deployments.iter().filter(|record| record.lifecycle != Lifecycle::Deleted) { let _ = reap_local(&root, record, grace); }
                }
            }
        }
    }
}

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/reaper", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_storage::MemoryStore;

    fn key(name: &str) -> ObjectKey { ObjectKey::new(name).unwrap() }

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
        assert_eq!(reap_local(directory.path(), &record, Duration::from_secs(3600)).unwrap(), Reaped { erased: 0, waiting: 1 }, "within the grace period it waits");
        assert_eq!(reap_local(directory.path(), &record, Duration::ZERO).unwrap(), Reaped { erased: 1, waiting: 0 });
        assert!(!root.join("tables/t/data/a.arrow").exists());
    }

    #[tokio::test]
    async fn a_holders_store_refuses_writes_once_the_directory_moves_past_its_epoch() {
        let directory = rtdb_directory::MemoryDirectory::new();
        let mut record = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let watched = rtdb_directory::watch_deployment(directory.clone(), record.id.clone(), Duration::from_millis(5));
        let store = FencedStore { inner: Arc::new(MemoryStore::default()), fence: Fence::Writer { epoch: 0, record: watched } };
        store.put(&key("blocks/a"), Bytes::from_static(b"a")).await.unwrap();
        record.epoch = 1;
        directory.replace_deployment(record.clone(), record.version).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(store.put(&key("blocks/b"), Bytes::from_static(b"b")).await, Err(Error::Deposed { epoch: 0, promised: 1 })), "a deposed holder writes nothing");
        assert!(store.delete(&key("blocks/a")).await.is_err(), "and releases nothing");
        assert_eq!(store.get(&key("blocks/a")).await.unwrap().as_ref(), b"a", "reads still serve its snapshots");
    }

    #[tokio::test]
    async fn the_reaper_erases_a_pack_once_every_object_in_it_is_released() {
        let directory = tempfile::tempdir().unwrap();
        let record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let root = directory.path().join(record.storage_prefix()).join("v06");
        let local: Arc<dyn ObjectStore> = Arc::new(LocalStore::open(&root).unwrap());
        let store = GarbageStore::open(crate::packing::PackingStore::open(local).await.unwrap()).await.unwrap();
        for name in ["tables/t/data/l00-rc-b00.arrow", "tables/t/meta/l00-rc-b00.arrow"] { store.put(&key(name), Bytes::from_static(b"x")).await.unwrap(); }
        store.put(&key("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        let packs = || std::fs::read_dir(root.join(crate::packing::PACK_DIRECTORY)).map(|entries| entries.count()).unwrap_or(0);
        assert_eq!(packs(), 1);
        store.delete(&key("tables/t/data/l00-rc-b00.arrow")).await.unwrap();
        assert_eq!(reap_local(directory.path(), &record, Duration::ZERO).unwrap(), Reaped { erased: 0, waiting: 1 }, "half a pack released keeps the pack");
        assert_eq!(packs(), 1);
        store.delete(&key("tables/t/meta/l00-rc-b00.arrow")).await.unwrap();
        assert_eq!(reap_local(directory.path(), &record, Duration::ZERO).unwrap().erased, 2);
        assert_eq!(packs(), 0, "a fully released pack is erased");
    }
}
