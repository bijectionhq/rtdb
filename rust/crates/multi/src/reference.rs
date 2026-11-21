//! Forks by reference (`ForkByReference`): a fork's store names, in a `~base` manifest, the objects it shares with its parent and where each one physically lives, instead of copying them.
//!
//! Everything here works on the cell's bucket, one object store holding every deployment's stores, whose keys are relative to the cell's storage root.
//! - `ReferencingStore` reads a store's own object first and its referenced one otherwise, and lists both.
//! - Whatever erases a physical object (the reaper, a deletion) first copies it into every store that still references it, so a reference never dangles.
//! - A fork releasing an object it only references drops the reference; the object itself is its owner's.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use rtdb_directory::{DeploymentRecord, Lifecycle};
use rtdb_storage::{Error, ObjectKey, ObjectStore, object_store::StoredObject};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

/// A cell's object storage: every deployment's stores, by keys relative to the cell's storage root.
pub type Bucket = Arc<dyn ObjectStore>;

/// Where a store's manifests are: each is written once, under the next sequence number, and the highest is the store's.
pub const BASE_MANIFEST: &str = "~base";

/// A store's references: for each object it shares, the store, relative to the cell's storage root, that physically holds it under the same key.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Base { pub objects: BTreeMap<String, String> }

pub(crate) fn key(name: &str) -> rtdb_storage::Result<ObjectKey> { ObjectKey::new(name.to_owned()).map_err(|error| Error::InvalidKey(error.to_string())) }

fn storage(error: Error) -> Anomaly { error.into() }

/// Whether `key`, within a store, is bookkeeping rather than one of its objects.
fn bookkeeping(key: &str) -> bool { key == crate::store::ROOT_MARKER || key.starts_with(&format!("{BASE_MANIFEST}/")) || key.ends_with(".tmp") }

impl Base {
    /// The latest manifest of `store`, or none.
    pub async fn load(bucket: &dyn ObjectStore, store: &str) -> Result<Base> {
        for _ in 0..4 {
            let Some((_, name)) = latest_manifest(bucket, store).await? else { return Ok(Base::default()); };
            match bucket.get(&key(&name)?).await {
                Ok(bytes) => return serde_json::from_slice(&bytes)
                    .map_err(|error| Anomaly::fault("rtdb.multi/base-manifest", format!("{name}: {error}"))),
                Err(Error::MissingObject(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        }
        Err(Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/base-manifest-changed", format!("The reference manifest for {store} kept changing")))
    }

    /// Writes this as `store`'s manifest, after its latest, and forgets the earlier ones.
    pub async fn save(&self, bucket: &dyn ObjectStore, store: &str) -> Result<()> {
        let previous = latest_manifest(bucket, store).await?;
        let sequence = previous.as_ref().map_or(0, |(sequence, _)| sequence + 1);
        bucket.put(&key(&format!("{store}/{BASE_MANIFEST}/{sequence:020}"))?, Bytes::from(serde_json::to_vec(self).expect("a manifest serializes"))).await.map_err(storage)?;
        for object in bucket.list(Some(&key(&format!("{store}/{BASE_MANIFEST}"))?)).await.map_err(storage)? {
            if manifest_sequence(object.key.as_str()).is_some_and(|earlier| earlier < sequence) { bucket.delete(&object.key).await.map_err(storage)?; }
        }
        Ok(())
    }
}

fn manifest_sequence(name: &str) -> Option<u64> { name.rsplit('/').next()?.parse().ok() }

async fn latest_manifest(bucket: &dyn ObjectStore, store: &str) -> Result<Option<(u64, String)>> {
    let listed = bucket.list(Some(&key(&format!("{store}/{BASE_MANIFEST}"))?)).await.map_err(storage)?;
    Ok(listed.into_iter().filter_map(|object| manifest_sequence(object.key.as_str()).map(|sequence| (sequence, object.key.as_str().to_owned()))).max())
}

/// One store within the cell's bucket: its keys, under `prefix`, seen without it.
pub struct PrefixedStore { bucket: Bucket, prefix: String, scoped_writer: Option<Bucket> }

impl PrefixedStore {
    pub fn new(bucket: Bucket, prefix: impl Into<String>) -> Self { Self { bucket, prefix: prefix.into(), scoped_writer: None } }
    pub(crate) fn with_writer(mut self, writer: Option<Bucket>) -> Self {
        self.scoped_writer = writer;
        self
    }
    fn full(&self, key: &ObjectKey) -> rtdb_storage::Result<ObjectKey> { self::key(&format!("{}/{}", self.prefix, key.as_str())) }
    fn mutation(&self, key: &ObjectKey) -> rtdb_storage::Result<(&dyn ObjectStore, ObjectKey)> {
        match &self.scoped_writer {
            Some(writer) => Ok((writer.as_ref(), self.full(key)?)),
            None => Ok((self.bucket.as_ref(), self.full(key)?)),
        }
    }
}

#[async_trait]
impl ObjectStore for PrefixedStore {
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.full(key).is_ok_and(|key| self.bucket.supports_scan_ranges(&key)) }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { Ok(StoredObject { key: key.clone(), size: self.bucket.head(&self.full(key)?).await?.size }) }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { Ok(StoredObject { key: key.clone(), size: self.bucket.head_fresh(&self.full(key)?).await?.size }) }
    async fn get_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.bucket.get_fresh(&self.full(key)?).await }
    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<StoredObject>> {
        let mut observed = self.bucket.observe_head(&self.full(key)?).await?;
        observed.value.key = key.clone();
        Ok(observed)
    }
    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        self.bucket.retirement_version(&self.full(key)?).await
    }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.bucket.get(&self.full(key)?).await }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> { self.bucket.get_fresh_exact(&self.full(key)?, length).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.bucket.get_range(&self.full(key)?, range).await }
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<Bytes>> {
        self.bucket.observe_range(&self.full(key)?, range).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        let (writer, key) = self.mutation(key)?;
        writer.put(&key, bytes).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        let (writer, key) = self.mutation(key)?;
        writer.put_file(&key, source).await
    }
    async fn put_with_version(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        let (writer, key) = self.mutation(key)?;
        writer.put_with_version(&key, bytes).await
    }
    async fn put_file_with_version(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        let (writer, key) = self.mutation(key)?;
        writer.put_file_with_version(&key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let under = match directory { Some(directory) => self.full(directory)?, None => self::key(&self.prefix)? };
        let strip = format!("{}/", self.prefix);
        self.bucket.list(Some(&under)).await?.into_iter().filter_map(|object| {
            let relative = object.key.as_str().strip_prefix(&strip)?.to_owned();
            Some(self::key(&relative).map(|key| StoredObject { key, size: object.size }))
        }).collect()
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        let (writer, key) = self.mutation(key)?;
        writer.delete(&key).await
    }
}

/// A store's own objects and the objects its manifest references, read own-first.
pub struct ReferencingStore { own: Arc<dyn ObjectStore>, bucket: Bucket, base: Base }

impl ReferencingStore {
    /// Opens `own`, a store in the cell's `bucket` whose manifest is `base`.
    pub fn new(own: Arc<dyn ObjectStore>, bucket: Bucket, base: Base) -> Self { Self { own, bucket, base } }

    async fn read<T>(&self, key: &ObjectKey, read: impl for<'a> Fn(&'a dyn ObjectStore, &'a ObjectKey) -> BoxFuture<'a, rtdb_storage::Result<T>>) -> rtdb_storage::Result<T> {
        let own = read(self.own.as_ref(), key).await;
        if !matches!(own, Err(Error::MissingObject(_))) { return own; }
        let Some(owner) = self.base.objects.get(key.as_str()) else { return own; };
        let inherited = self::key(&format!("{owner}/{key}"))?;
        match read(self.bucket.as_ref(), &inherited).await {
            // Erasure can copy the object here and remove it from its owner between the two reads.
            Err(Error::MissingObject(_)) => read(self.own.as_ref(), key).await,
            result => result,
        }
    }
}

#[async_trait]
impl ObjectStore for ReferencingStore {
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.own.supports_scan_ranges(key) && self.base.objects.get(key.as_str()).is_none_or(|owner| {
            self::key(&format!("{owner}/{key}")).is_ok_and(|key| self.bucket.supports_scan_ranges(&key))
        }) }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        let object = self.read(key, |store, key| store.head(key)).await?;
        Ok(StoredObject { key: key.clone(), size: object.size })
    }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        let object = self.read(key, |store, key| store.head_fresh(key)).await?;
        Ok(StoredObject { key: key.clone(), size: object.size })
    }
    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<StoredObject>> {
        let mut observed = self.read(key, |store, key| store.observe_head(key)).await?;
        observed.value.key = key.clone();
        Ok(observed)
    }
    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        if self.base.objects.contains_key(key.as_str()) { return Ok(None); }
        self.own.retirement_version(key).await
    }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store, key| store.get(key)).await
    }
    async fn get_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store, key| store.get_fresh(key)).await
    }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> { self.read(key, |store, key| store.get_fresh_exact(key, length)).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        self.read(key, |store, key| store.get_range(key, range.clone())).await
    }
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<Bytes>> {
        self.read(key, |store, key| store.observe_range(key, range.clone())).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        if self.base.objects.contains_key(key.as_str()) {
            return if self.get(key).await? == bytes { Ok(()) } else { Err(Error::ConflictingObject(key.to_string())) };
        }
        self.own.put(key, bytes).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        if self.base.objects.contains_key(key.as_str()) { return self.put(key, Bytes::from(std::fs::read(source).map_err(|error| Error::Task(format!("{}: {error}", source.display())))?)).await; }
        self.own.put_file(key, source).await
    }
    async fn put_with_version(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        if self.base.objects.contains_key(key.as_str()) {
            self.put(key, bytes).await?;
            return Ok(None);
        }
        self.own.put_with_version(key, bytes).await
    }
    async fn put_file_with_version(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectVersion>> {
        if self.base.objects.contains_key(key.as_str()) {
            self.put_file(key, source).await?;
            return Ok(None);
        }
        self.own.put_file_with_version(key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let mut listed: BTreeMap<String, StoredObject> = self.own.list(directory).await?.into_iter()
            .filter(|object| !bookkeeping(object.key.as_str())).map(|object| (object.key.as_str().to_owned(), object)).collect();
        for name in self.base.objects.keys() {
            let key = self::key(name)?;
            if listed.contains_key(name) || directory.is_some_and(|directory| !key.is_under(directory)) { continue; }
            match self.head(&key).await {
                Ok(object) => { listed.insert(name.clone(), object); }
                Err(Error::MissingObject(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(listed.into_values().collect())
    }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.own.delete(key).await }
}

/// The objects one store holds itself, as keys within it: everything but its bookkeeping.
pub async fn physical_objects(bucket: &dyn ObjectStore, store: &str) -> Result<Vec<String>> {
    let strip = format!("{store}/");
    Ok(bucket.list(Some(&key(store)?)).await.map_err(storage)?.into_iter()
        .filter_map(|object| object.key.as_str().strip_prefix(&strip).map(str::to_owned))
        .filter(|key| !bookkeeping(key)).collect())
}

/// Whether a store holds `key` itself.
pub async fn holds(bucket: &dyn ObjectStore, store: &str, name: &str) -> Result<bool> {
    match bucket.head(&key(&format!("{store}/{name}"))?).await { Ok(_) => Ok(true), Err(Error::MissingObject(_)) => Ok(false), Err(error) => Err(error.into()) }
}

/// The stores (one per storage version) under a deployment's prefix.
pub async fn stores(bucket: &dyn ObjectStore, prefix: &str) -> Result<Vec<String>> {
    let strip = format!("{prefix}/");
    let versions: BTreeSet<String> = bucket.list(Some(&key(prefix)?)).await.map_err(storage)?.into_iter()
        .filter_map(|object| object.key.as_str().strip_prefix(&strip).and_then(|rest| rest.split_once('/')).map(|(version, _)| version.to_owned())).collect();
    Ok(versions.into_iter().map(|version| format!("{prefix}/{version}")).collect())
}

/// Where `name`, an object of `store`, physically is: in the store, or in the one its manifest names.
async fn located(bucket: &dyn ObjectStore, store: &str, base: &Base, name: &str) -> Result<Option<String>> {
    if holds(bucket, store, name).await? { return Ok(Some(format!("{store}/{name}"))); }
    let Some(owner) = base.objects.get(name) else { return Ok(None) };
    Ok(holds(bucket, owner, name).await?.then(|| format!("{owner}/{name}")))
}

/// The block a trie file's name ends in.
fn trie_block(name: &str) -> Option<i64> {
    if !name.starts_with("tables/") || !(name.contains("/data/") || name.contains("/meta/")) { return None; }
    let stem = name.rsplit('/').next()?.strip_suffix(".arrow")?;
    stem.parse::<rtdb_format::keys::TrieKey>().ok().map(|trie| trie.block_index().0)
}

/// Forks the deployment at `parent` (a prefix in the cell's bucket) into `child`, referencing every object the parent's stores hold or reference, or, from a past block, those it may need.
/// A past block's packs are the ones its own pack names, its releases those before it was written, and its trie files those of blocks up to it; every other object is referenced too, since sealing makes anything the fork writes again the same bytes.
/// A trie file of a later block is the parent's history after the fork point, under a name the fork's own compaction writes with its own rows.
/// Derived ordered-index objects and retirement journal records stay with their original residence.
pub async fn fork(bucket: &dyn ObjectStore, parent: &str, child: &str, at_block: Option<i64>) -> Result<()> {
    let past = match at_block { Some(block) => Some((block, block_packs(bucket, parent, block).await?, written_at_ms(bucket, parent, block).await?)), None => None };
    let wanted = |name: &str| {
        if crate::store::stays_in_source_residence(name) { return false; }
        match &past {
            None => true,
            Some((_, packs, _)) if name.starts_with(&format!("{}/", crate::packing::PACK_DIRECTORY)) => packs.contains(name),
            Some((block, _, written)) => trie_block(name).is_none_or(|trie| trie <= *block) && crate::store::garbage_released_at(name).is_none_or(|released| released <= *written),
        }
    };
    for store in stores(bucket, parent).await? {
        let version = store.rsplit('/').next().expect("a store has a version").to_owned();
        let mut base = Base::load(bucket, &store).await?;
        base.objects.retain(|name, _| wanted(name));
        for name in physical_objects(bucket, &store).await? { if wanted(&name) { base.objects.insert(name, store.clone()); } }
        base.save(bucket, &format!("{child}/{version}")).await?;
    }
    Ok(())
}

/// Where `block`'s pack, the one object a flush of it writes, is among `parent`'s stores.
async fn block_pack_location(bucket: &dyn ObjectStore, parent: &str, block: i64) -> Result<String> {
    let pack = crate::packing::block_pack(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block))?);
    for store in stores(bucket, parent).await? {
        let base = Base::load(bucket, &store).await?;
        if let Some(location) = located(bucket, &store, &base, &pack).await? { return Ok(location); }
    }
    Err(Anomaly::incorrect("rtdb.multi/no-such-block", format!("Block {block} has no pack in the deployment's store")))
}

pub(crate) async fn verify_block_pack(bucket: &dyn ObjectStore, prefix: &str, block: i64) -> Result<()> {
    let location = block_pack_location(bucket, prefix, block).await?;
    crate::packing::read_index(bucket, &key(&location)?).await?;
    Ok(())
}

/// When `block`'s pack was written in `parent`'s stores, in milliseconds since the epoch, as its index records.
pub async fn written_at_ms(bucket: &dyn ObjectStore, parent: &str, block: i64) -> Result<i64> {
    let location = block_pack_location(bucket, parent, block).await?;
    let index = crate::packing::read_index(bucket, &key(&location)?).await.map_err(storage)?;
    index.written_at_ms.ok_or_else(|| Anomaly::incorrect("rtdb.multi/pack-unstamped", format!("Block {block}'s pack records no write time")))
}

/// The packs that existed when `block` was written: its own, and every one its index names.
async fn block_packs(bucket: &dyn ObjectStore, parent: &str, block: i64) -> Result<BTreeSet<String>> {
    let location = block_pack_location(bucket, parent, block).await?;
    let index = crate::packing::read_index(bucket, &key(&location)?).await.map_err(storage)?;
    let own = crate::packing::block_pack(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block))?);
    Ok(index.previous.into_iter().chain([own]).collect())
}

/// Every reference in a cell: for each physical object, the stores that reference it.
#[derive(Default)]
pub struct References { by_object: BTreeMap<String, BTreeSet<String>> }

impl References {
    /// The references of every deployment in `records` that is not deleted.
    pub async fn scan(bucket: &dyn ObjectStore, records: &[DeploymentRecord]) -> Result<Self> {
        let mut references = Self::default();
        for record in records {
            if record.lifecycle == Lifecycle::Deleted { continue; }
            for store in stores(bucket, &record.storage_prefix()).await? {
                // One listing per store, rather than a request per reference, on every reaper pass.
                let own: BTreeSet<String> = physical_objects(bucket, &store).await?.into_iter().collect();
                for (name, owner) in Base::load(bucket, &store).await?.objects {
                    if own.contains(&name) { continue; }
                    references.by_object.entry(format!("{owner}/{name}")).or_default().insert(store.clone());
                }
            }
        }
        Ok(references)
    }

    /// Erases `name` from `store`, first copying it into every store that still references it there.
    pub async fn erase(&self, bucket: &dyn ObjectStore, store: &str, name: &str) -> Result<()> {
        let physical = format!("{store}/{name}");
        for referrer in self.by_object.get(&physical).into_iter().flatten() { copy_in(bucket, &physical, bucket, &format!("{referrer}/{name}")).await?; }
        bucket.delete(&key(&physical)?).await.map_err(storage)
    }

    /// Erases everything a store holds, copying the referenced objects out first.
    pub async fn erase_store(&self, bucket: &dyn ObjectStore, store: &str) -> Result<()> {
        for name in physical_objects(bucket, store).await? { self.erase(bucket, store, &name).await?; }
        for object in bucket.list(Some(&key(store)?)).await.map_err(storage)? { bucket.delete(&object.key).await.map_err(storage)?; }
        Ok(())
    }
}

/// Erases every object a deployment's `prefix` holds, first copying what the other deployments of the cell (`others`) reference into them, and returns whether the prefix is then empty.
pub async fn erase_deployment(bucket: &dyn ObjectStore, prefix: &str, others: &[DeploymentRecord]) -> Result<bool> {
    require_disjoint_prefix(&key(prefix)?, others.iter())?;
    let references = References::scan(bucket, others).await?;
    for store in stores(bucket, prefix).await? { references.erase_store(bucket, &store).await?; }
    for object in bucket.list(Some(&key(prefix)?)).await.map_err(storage)? { bucket.delete(&object.key).await.map_err(storage)?; }
    Ok(bucket.list(Some(&key(prefix)?)).await.map_err(storage)?.is_empty())
}

pub(crate) fn require_disjoint_prefix<'a>(prefix: &ObjectKey, others: impl Iterator<Item = &'a DeploymentRecord>) -> Result<()> {
    for other in others {
        let other_prefix = key(&other.storage_prefix())?;
        if other_prefix.is_under(prefix) || prefix.is_under(&other_prefix) {
            return Err(Anomaly::incorrect("rtdb.multi/overlapping-deployment-prefix", format!("Storage prefix {prefix} overlaps the storage of {}", other.name)));
        }
    }
    Ok(())
}

async fn copy_in(from_bucket: &dyn ObjectStore, from: &str, to_bucket: &dyn ObjectStore, to: &str) -> rtdb_storage::Result<()> {
    let target = key(to)?;
    match to_bucket.head(&target).await { Ok(_) => return Ok(()), Err(Error::MissingObject(_)) => {}, Err(error) => return Err(error) }
    to_bucket.put(&target, from_bucket.get(&key(from)?).await?).await
}

/// An earlier attempt can leave objects in the target. Only identical bytes
/// satisfy a retry, and a concurrent create remains conditional in the store.
async fn stage_object(bucket: &dyn ObjectStore, key: &ObjectKey, bytes: Bytes) -> Result<()> {
    match bucket.get(key).await {
        Ok(existing) if existing == bytes => Ok(()),
        Ok(_) => Err(Error::ConflictingObject(key.to_string()).into()),
        Err(Error::MissingObject(_)) => bucket.put(key, bytes).await.map_err(storage),
        Err(error) => Err(error.into()),
    }
}

/// Copies a deployment's stores from `from` in one cell's bucket to `to` in another's, resolving retained references into objects of its own.
/// The destination builds its ordered index and retirement journal from its authoritative catalog.
pub async fn materialize(from_bucket: &dyn ObjectStore, from: &str, to_bucket: &dyn ObjectStore, to: &str) -> Result<()> {
    for store in stores(from_bucket, from).await? {
        let version = store.rsplit('/').next().expect("a store has a version").to_owned();
        let target = format!("{to}/{version}");
        let base = Base::load(from_bucket, &store).await?;
        for name in physical_objects(from_bucket, &store).await? {
            if crate::store::stays_in_source_residence(&name) { continue; }
            let source = key(&format!("{store}/{name}"))?;
            let destination = key(&format!("{target}/{name}"))?;
            stage_object(to_bucket, &destination, from_bucket.get(&source).await?).await?;
        }
        for (name, owner) in base.objects {
            if crate::store::stays_in_source_residence(&name) { continue; }
            let destination = key(&format!("{target}/{name}"))?;
            let own = key(&format!("{store}/{name}"))?;
            let bytes = match from_bucket.get(&own).await {
                Err(Error::MissingObject(_)) => match from_bucket.get(&key(&format!("{owner}/{name}"))?).await {
                    // Concurrent retirement materializes the reference before
                    // removing the original, so look again in the fork.
                    Err(Error::MissingObject(_)) => match from_bucket.get(&own).await {
                        Err(Error::MissingObject(_)) if crate::store::garbage_released_at(&name).is_some() => continue,
                        value => value?,
                    },
                    value => value?,
                },
                value => value?,
            };
            stage_object(to_bucket, &destination, bytes).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket() -> Bucket { Arc::new(rtdb_storage::MemoryStore::default()) }
    async fn put(bucket: &Bucket, name: &str, bytes: &[u8]) { bucket.put(&key(name).unwrap(), Bytes::copy_from_slice(bytes)).await.unwrap(); }

    #[tokio::test]
    async fn deleting_an_overlapping_prefix_leaves_the_other_deployments_objects_intact() {
        let bucket = bucket();
        let other = DeploymentRecord::new("other", "acme", Vec::new(), 0);
        let other_prefix = other.storage_prefix();
        let retained = format!("{other_prefix}/v06/rows");
        put(&bucket, &retained, b"other deployment").await;
        let (outer, _) = other_prefix.rsplit_once('/').unwrap();
        let error = erase_deployment(bucket.as_ref(), outer, &[other]).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/overlapping-deployment-prefix");
        assert_eq!(bucket.get(&key(&retained).unwrap()).await.unwrap(), b"other deployment".as_slice());
    }

    struct TransferringStore {
        inner: Bucket,
        original: ObjectKey,
        copy: ObjectKey,
        transferred: std::sync::atomic::AtomicBool,
    }

    impl TransferringStore {
        async fn transfer_if_read(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
            if key == &self.original && !self.transferred.swap(true, std::sync::atomic::Ordering::Relaxed) {
                self.inner.copy(&self.original, &self.copy).await?;
                self.inner.delete(&self.original).await?;
            }
            Ok(())
        }
    }

    #[async_trait]
    impl ObjectStore for TransferringStore {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
            self.transfer_if_read(key).await?;
            self.inner.head(key).await
        }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
            self.transfer_if_read(key).await?;
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
            self.transfer_if_read(key).await?;
            self.inner.get_range(key, range).await
        }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn a_fork_reads_an_object_transferred_during_the_read() {
        for read in ["get", "get_range", "head", "head_fresh", "list", "put"] {
            let bucket = bucket();
            put(&bucket, "parent/v06/row", b"parent").await;
            fork(bucket.as_ref(), "parent", "child", None).await.unwrap();
            let base = Base::load(bucket.as_ref(), "child/v06").await.unwrap();
            let bucket: Bucket = Arc::new(TransferringStore {
                inner: bucket,
                original: key("parent/v06/row").unwrap(),
                copy: key("child/v06/row").unwrap(),
                transferred: Default::default(),
            });
            let store = ReferencingStore::new(Arc::new(PrefixedStore::new(bucket.clone(), "child/v06")), bucket.clone(), base);
            let row = key("row").unwrap();
            match read {
                "get" => assert_eq!(store.get(&row).await.unwrap(), b"parent".as_slice()),
                "get_range" => assert_eq!(store.get_range(&row, 2..6).await.unwrap(), b"rent".as_slice()),
                "head" => assert_eq!(store.head(&row).await.unwrap().size, 6),
                "head_fresh" => assert_eq!(store.head_fresh(&row).await.unwrap().size, 6),
                "list" => assert_eq!(store.list(None).await.unwrap(), [StoredObject { key: row, size: 6 }]),
                "put" => {
                    store.put(&row, Bytes::from_static(b"parent")).await.unwrap();
                    assert!(matches!(store.put(&row, Bytes::from_static(b"overwrite")).await, Err(Error::ConflictingObject(_))));
                }
                _ => unreachable!(),
            }
            assert!(!holds(bucket.as_ref(), "parent/v06", "row").await.unwrap());
        }
    }

    #[tokio::test]
    async fn reading_a_manifest_retries_when_its_listed_revision_was_replaced() {
        let bucket = bucket();
        let base = Base { objects: BTreeMap::from([("row".into(), "parent/v06".into())]) };
        base.save(bucket.as_ref(), "child/v06").await.unwrap();
        let changing = TransferringStore {
            inner: bucket,
            original: key(&format!("child/v06/{BASE_MANIFEST}/{:020}", 0)).unwrap(),
            copy: key(&format!("child/v06/{BASE_MANIFEST}/{:020}", 1)).unwrap(),
            transferred: Default::default(),
        };
        assert_eq!(Base::load(&changing, "child/v06").await.unwrap(), base);
    }

    #[tokio::test]
    async fn a_cell_copy_recovers_a_reference_that_changes_owner_during_the_read() {
        let (here, there) = (bucket(), bucket());
        put(&here, "parent/v06/row", b"parent").await;
        fork(here.as_ref(), "parent", "child", None).await.unwrap();
        let transferring = TransferringStore {
            inner: here.clone(),
            original: key("parent/v06/row").unwrap(),
            copy: key("child/v06/row").unwrap(),
            transferred: Default::default(),
        };
        materialize(&transferring, "child", there.as_ref(), "arrived").await.unwrap();
        assert_eq!(there.get(&key("arrived/v06/row").unwrap()).await.unwrap().as_ref(), b"parent");
        assert!(!holds(here.as_ref(), "parent/v06", "row").await.unwrap());
    }

    #[tokio::test]
    async fn a_cell_copy_fails_when_an_inherited_object_is_missing_at_both_owners() {
        let (here, there) = (bucket(), bucket());
        put(&here, "parent/v06/row", b"parent").await;
        fork(here.as_ref(), "parent", "child", None).await.unwrap();
        here.delete(&key("parent/v06/row").unwrap()).await.unwrap();
        assert!(materialize(here.as_ref(), "child", there.as_ref(), "arrived").await.is_err());
    }

    #[tokio::test]
    async fn a_transfer_retries_only_identical_staged_objects() {
        let (here, there) = (bucket(), bucket());
        put(&here, "source/v06/blocks/b00", b"accepted").await;
        put(&there, "target/v06/blocks/b00", b"substitute").await;
        assert!(materialize(here.as_ref(), "source", there.as_ref(), "target").await.is_err());
        there.delete(&key("target/v06/blocks/b00").unwrap()).await.unwrap();
        materialize(here.as_ref(), "source", there.as_ref(), "target").await.unwrap();
        materialize(here.as_ref(), "source", there.as_ref(), "target").await.unwrap();
    }

    #[tokio::test]
    async fn derived_indexes_and_retirement_records_stay_in_their_source_residence() {
        let (here, there) = (bucket(), bucket());
        let prefix = rtdb_format::keys::ORDERED_INDEX_ROOT;
        let page = format!("{prefix}/idx/pages/block/hash");
        let manifest = format!("{prefix}/idx/manifests/block");
        let release = "~garbage/00000000000000001000-debt-ordered-index";
        let journal = rtdb_format::keys::TRIE_RETIREMENT_DIRECTORY;
        let intent = format!("{journal}/6-0-0/pending/id");
        let receipt = format!("{journal}/6-0-0/installed/id");
        put(&here, "source/v06/blocks/b00", b"catalog").await;
        for name in [page.as_str(), manifest.as_str(), release, intent.as_str(), receipt.as_str()] {
            put(&here, &format!("source/v06/{name}"), b"only the original store owns these bytes").await;
        }
        let source = Arc::new(PrefixedStore::new(here.clone(), "source/v06"));
        let source = crate::store::GarbageStore::open(source).await.unwrap();
        source.delete(&key(&intent).unwrap()).await.unwrap();
        fork(here.as_ref(), "source", "child", None).await.unwrap();
        let base = Base::load(here.as_ref(), "child/v06").await.unwrap();
        assert_eq!(base.objects.into_keys().collect::<Vec<_>>(), ["blocks/b00"]);
        materialize(here.as_ref(), "source", there.as_ref(), "target").await.unwrap();
        assert_eq!(physical_objects(there.as_ref(), "target/v06").await.unwrap(), ["blocks/b00"]);
    }

    #[tokio::test]
    async fn a_fork_moved_to_another_cell_owns_everything_it_referenced() {
        let (here, there) = (bucket(), bucket());
        put(&here, "p/v06/blocks/b00", b"parent").await;
        put(&here, &format!("p/v06/{}", crate::store::ROOT_MARKER), b"p").await;
        fork(here.as_ref(), "p", "c", None).await.unwrap();
        put(&here, "c/v06/blocks/b01", b"child").await;
        materialize(here.as_ref(), "c", there.as_ref(), "c").await.unwrap();
        let mut moved = physical_objects(there.as_ref(), "c/v06").await.unwrap();
        moved.sort();
        assert_eq!(moved, ["blocks/b00", "blocks/b01"]);
        assert_eq!(Base::load(there.as_ref(), "c/v06").await.unwrap(), Base::default(), "the moved store references nothing");
        assert_eq!(there.get(&key("c/v06/blocks/b00").unwrap()).await.unwrap().as_ref(), b"parent");
    }

    #[tokio::test]
    async fn a_fork_at_a_past_block_takes_the_packs_that_blocks_index_names_and_the_releases_before_it() {
        let bucket = bucket();
        let pack = |block: i64| crate::packing::block_pack(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block)).unwrap());
        let (first, _) = crate::packing::encode_pack(&BTreeMap::from([("blocks/b00.binpb".to_owned(), Bytes::from_static(b"zero"))]), Vec::new(), 1_000);
        bucket.put(&key(&format!("p/v06/{}", pack(0))).unwrap(), first).await.unwrap();
        let (second, _) = crate::packing::encode_pack(&BTreeMap::from([("blocks/b01.binpb".to_owned(), Bytes::from_static(b"one"))]), vec![pack(0)], 2_000);
        bucket.put(&key(&format!("p/v06/{}", pack(1))).unwrap(), second).await.unwrap();
        put(&bucket, &format!("p/v06/{}/{:020}-early", crate::store::GARBAGE_DIRECTORY, 500), b"{}").await;
        put(&bucket, &format!("p/v06/{}/{:020}-late", crate::store::GARBAGE_DIRECTORY, 1_500), b"{}").await;
        put(&bucket, "p/v06/tables/t/data/l01-compacted.arrow", b"compacted").await;
        fork(bucket.as_ref(), "p", "c", Some(0)).await.unwrap();
        let referenced: Vec<String> = Base::load(bucket.as_ref(), "c/v06").await.unwrap().objects.into_keys().collect();
        assert_eq!(referenced, ["tables/t/data/l01-compacted.arrow".to_owned(), format!("{}/{:020}-early", crate::store::GARBAGE_DIRECTORY, 500), pack(0)],
            "a later block's pack and a release after the block are left out; everything else is referenced");
    }

    #[tokio::test]
    async fn a_fork_at_a_past_block_leaves_out_the_trie_files_of_later_blocks_and_writes_its_own() {
        let bucket = bucket();
        let clock = crate::store::stamps(|| 1_000);
        let parent = crate::store::open_deployment_store(Arc::clone(&bucket), "p/v06", crate::packing::PackDiscovery::List, Arc::clone(&clock)).await.unwrap();
        for block in 0..2 { parent.put(&key(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block)).unwrap()).unwrap(), Bytes::from_static(b"catalog")).await.unwrap(); }
        for (name, bytes) in [("tables/t/data/l01-rc-b00.arrow", b"parent's b00"), ("tables/t/data/l01-rc-b01.arrow", b"parent's b01")] { parent.put(&key(name).unwrap(), Bytes::from_static(bytes)).await.unwrap(); }
        fork(bucket.as_ref(), "p", "c", Some(0)).await.unwrap();
        let child = crate::store::open_deployment_store(Arc::clone(&bucket), "c/v06", crate::packing::PackDiscovery::List, clock).await.unwrap();
        let listed: Vec<String> = child.list(Some(&key("tables").unwrap())).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(listed, ["tables/t/data/l01-rc-b00.arrow"], "a compaction output of a block after the fork point is not the fork's");
        child.put(&key("tables/t/data/l01-rc-b01.arrow").unwrap(), Bytes::from_static(b"child's b01")).await.unwrap();
        assert_eq!(child.get(&key("tables/t/data/l01-rc-b01.arrow").unwrap()).await.unwrap().as_ref(), b"child's b01", "and the fork's own compaction writes its own under that name");
    }

    #[tokio::test]
    async fn a_store_keeps_one_manifest_whose_latest_is_read() {
        let bucket = bucket();
        Base { objects: BTreeMap::from([("a".into(), "p/v06".into())]) }.save(bucket.as_ref(), "c/v06").await.unwrap();
        let latest = Base { objects: BTreeMap::from([("b".into(), "p/v06".into())]) };
        latest.save(bucket.as_ref(), "c/v06").await.unwrap();
        assert_eq!(Base::load(bucket.as_ref(), "c/v06").await.unwrap(), latest);
        assert_eq!(bucket.list(Some(&key("c/v06/~base").unwrap())).await.unwrap().len(), 1, "an older manifest is forgotten once a newer one is written");
    }
}
