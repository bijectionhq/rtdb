//! Packs (`WritePackAndManifest`): a block flush costs one object, its block's pack, which is also the deployment's boot object.
//!
//! The objects a flush writes before its block catalog (level-0 trie data and metadata, per-table block files) are held back and written, with the catalog, as one pack whose trailer indexes them.
//! A cold start reads the latest block's pack and nothing else: its index, its catalog and its table blocks are ranges of one object.
//! Every other write (compaction outputs, oversized payloads) goes straight through.
//! The pack written with a block catalog is named after that block, and its index lists every pack written before it, so a store finds every pack by reading the latest block's, never listing.
//! A store probes for newer block catalogs when asked for an object it does not know, so a reader finds packs its writer published after it opened.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use rtdb_storage::{Error, ObjectKey, ObjectStore, object_store::StoredObject};
use serde::{Deserialize, Serialize};

pub const PACK_DIRECTORY: &str = "~packs";
/// Held-back bytes beyond which a pack is written without waiting for the catalog.
const MAXIMUM_HELD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackEntry { pub offset: u64, pub length: u64 }

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackIndex {
    pub entries: BTreeMap<String, PackEntry>,
    /// Every pack its writer knew of when writing this one; set on a block's pack.
    #[serde(default)]
    pub previous: Vec<String>,
    /// When the pack was written, in milliseconds since the epoch: by it a fork at a past block tells which releases came after the block.
    #[serde(default)]
    pub written_at_ms: Option<i64>,
}

/// How a store finds its packs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackDiscovery {
    /// Lists them: for a store with no published block to start from.
    List,
    /// Reads the pack of the latest block at or after this one, which was published, falling back to listing if the hint is gone.
    From(i64),
}

/// The pack written with `catalog`, a block catalog's key: `blocks/b0a.binpb` is packed as `~packs/b0a.pack`.
pub fn block_pack(catalog: &str) -> String {
    let stem = catalog.rsplit('/').next().unwrap_or(catalog).trim_end_matches(".binpb");
    format!("{PACK_DIRECTORY}/{stem}.pack")
}

/// The block whose pack `pack` is, if it is a block's: `~packs/b0a.pack` is block 10's.
pub fn pack_block(pack: &str) -> Option<i64> {
    let stem = pack.strip_prefix(&format!("{PACK_DIRECTORY}/"))?.strip_suffix(".pack")?;
    rtdb_format::keys::block_index_from_key(&format!("{stem}.binpb")).ok().map(|block| block.0)
}

/// Whether a flush writes `key` before its block catalog: a level-0 trie file or a per-table block file.
pub fn held_back(key: &str) -> bool {
    key.starts_with("tables/") && (key.contains("/data/l00-") || key.contains("/meta/l00-") || key.contains("/blocks/b"))
}

fn is_catalog(key: &str) -> bool { key.starts_with("blocks/b") && key.ends_with(".binpb") }

/// Whether `key` is found only in a pack: what a flush holds back, and the catalog it is packed with.
fn packed(key: &str) -> bool { held_back(key) || is_catalog(key) }

/// Encodes a pack: the objects back to back, the index as JSON, and the index's length as eight big-endian bytes.
pub fn encode_pack(objects: &BTreeMap<String, Bytes>, previous: Vec<String>, written_at_ms: i64) -> (Bytes, PackIndex) {
    let mut bytes = BytesMut::new();
    let mut index = PackIndex { entries: BTreeMap::new(), previous, written_at_ms: Some(written_at_ms) };
    for (key, object) in objects {
        index.entries.insert(key.clone(), PackEntry { offset: bytes.len() as u64, length: object.len() as u64 });
        bytes.extend_from_slice(object);
    }
    let encoded = serde_json::to_vec(&index).expect("an index serializes");
    bytes.extend_from_slice(&encoded);
    bytes.put_u64(encoded.len() as u64);
    (bytes.freeze(), index)
}

pub async fn read_index(store: &dyn ObjectStore, pack: &ObjectKey) -> rtdb_storage::Result<PackIndex> {
    let size = store.head(pack).await?.size;
    if size < 8 { return Err(Error::Task(format!("{pack} is too short to be a pack"))); }
    let length = u64::from_be_bytes(store.get_range(pack, size - 8..size).await?.as_ref().try_into().expect("eight bytes"));
    if length + 8 > size { return Err(Error::Task(format!("{pack} has a malformed trailer"))); }
    let encoded = store.get_range(pack, size - 8 - length..size - 8).await?;
    serde_json::from_slice(&encoded).map_err(|error| Error::Task(format!("{pack} has a malformed index: {error}")))
}

struct State {
    held: BTreeMap<String, Bytes>,
    held_bytes: usize,
    /// Which pack holds each packed object.
    located: BTreeMap<String, (ObjectKey, PackEntry)>,
    packs: Vec<ObjectKey>,
    discovery: PackDiscovery,
    /// The latest block whose pack this store has read.
    latest_block: Option<i64>,
}

pub struct PackingStore { inner: Arc<dyn ObjectStore>, state: Mutex<State>, writing: tokio::sync::Mutex<()>, clock: crate::store::Clock }

impl PackingStore {
    /// Opens a store, reading nothing until an object it holds back is asked for.
    pub async fn open(inner: Arc<dyn ObjectStore>, discovery: PackDiscovery) -> rtdb_storage::Result<Arc<Self>> { Self::open_with_clock(inner, discovery, crate::store::wall_clock()).await }

    pub async fn open_with_clock(inner: Arc<dyn ObjectStore>, discovery: PackDiscovery, clock: crate::store::Clock) -> rtdb_storage::Result<Arc<Self>> {
        Ok(Arc::new(Self { inner, state: Mutex::new(State { discovery, held: BTreeMap::new(), held_bytes: 0, located: BTreeMap::new(), packs: Vec::new(), latest_block: None }), writing: tokio::sync::Mutex::new(()), clock }))
    }

    async fn exists(&self, object: &ObjectKey) -> rtdb_storage::Result<bool> {
        match self.inner.head(object).await { Ok(_) => Ok(true), Err(Error::MissingObject(_)) => Ok(false), Err(error) => Err(error) }
    }

    /// Reads the index of `pack` into the store, unless it already knows it; a pack named by a later one's index that has since been erased is skipped.
    async fn adopt(&self, pack: ObjectKey) -> rtdb_storage::Result<Option<PackIndex>> {
        if self.state.lock().packs.contains(&pack) { return Ok(None); }
        let index = match read_index(self.inner.as_ref(), &pack).await { Ok(index) => index, Err(Error::MissingObject(_)) => return Ok(None), Err(error) => return Err(error) };
        let mut state = self.state.lock();
        for (object, entry) in &index.entries { state.located.insert(object.clone(), (pack.clone(), entry.clone())); }
        state.packs.push(pack);
        Ok(Some(index))
    }

    async fn refresh(&self) -> rtdb_storage::Result<()> {
        let (discovery, latest) = { let state = self.state.lock(); (state.discovery, state.latest_block) };
        let start = match (discovery, latest) {
            (_, Some(latest)) => latest + 1,
            (PackDiscovery::From(hint), None) if self.exists(&pack_key(hint)?).await? => hint,
            _ => return self.refresh_by_listing().await,
        };
        let mut next = start;
        while self.exists(&pack_key(next)?).await? { next += 1; }
        let Some(found) = (next > start).then(|| next - 1).or(latest) else { return Ok(()) };
        if latest == Some(found) { return Ok(()); }
        if let Some(index) = self.adopt(pack_key(found)?).await? {
            for previous in index.previous { self.adopt(key(&previous)?).await?; }
        }
        self.state.lock().latest_block = Some(found);
        Ok(())
    }

    async fn refresh_by_listing(&self) -> rtdb_storage::Result<()> {
        for pack in self.inner.list(Some(&key(PACK_DIRECTORY)?)).await? { self.adopt(pack.key).await?; }
        Ok(())
    }

    /// Writes everything held back as one pack: the block's own, naming every earlier pack, when written with `catalog`.
    async fn write_pack(&self, catalog: Option<&ObjectKey>) -> rtdb_storage::Result<()> {
        let _writing = self.writing.lock().await;
        // A block's pack names every earlier one, so the store must know them all first.
        if catalog.is_some() { self.refresh().await?; }
        let held = std::mem::take(&mut self.state.lock().held);
        if held.is_empty() && catalog.is_none() { return Ok(()); }
        let (name, previous) = match catalog {
            Some(catalog) => (key(&block_pack(catalog.as_str()))?, self.state.lock().packs.iter().map(|pack| pack.as_str().to_owned()).collect()),
            None => (key(&format!("{PACK_DIRECTORY}/p{:020}-{}.pack", (self.clock)(), uuid::Uuid::new_v4().simple()))?, Vec::new()),
        };
        let (bytes, index) = encode_pack(&held, previous, (self.clock)());
        if let Err(error) = self.inner.put(&name, bytes).await {
            // Another store packed the block first: its pack serves, if it holds the same objects.
            if matches!(error, Error::ConflictingObject(_)) && catalog.is_some() && self.same_pack(&name, &held).await? {
                self.adopt(name).await?;
                return Ok(());
            }
            let mut state = self.state.lock();
            for (object, bytes) in held { state.held.entry(object).or_insert(bytes); }
            return Err(error);
        }
        let mut state = self.state.lock();
        state.held_bytes = state.held.values().map(Bytes::len).sum();
        for (object, entry) in index.entries { state.located.insert(object, (name.clone(), entry)); }
        state.packs.push(name);
        Ok(())
    }

    /// Whether the stored pack `name` holds exactly `objects`.
    async fn same_pack(&self, name: &ObjectKey, objects: &BTreeMap<String, Bytes>) -> rtdb_storage::Result<bool> {
        let index = read_index(self.inner.as_ref(), name).await?;
        if index.entries.len() != objects.len() { return Ok(false); }
        for (key, entry) in &index.entries {
            let Some(bytes) = objects.get(key) else { return Ok(false) };
            if self.inner.get_range(name, entry.offset..entry.offset + entry.length).await? != *bytes { return Ok(false); }
        }
        Ok(true)
    }

    fn held(&self, object: &ObjectKey) -> Option<Bytes> { self.state.lock().held.get(object.as_str()).cloned() }
    fn located(&self, object: &ObjectKey) -> Option<(ObjectKey, PackEntry)> { self.state.lock().located.get(object.as_str()).cloned() }

    async fn locate(&self, object: &ObjectKey) -> rtdb_storage::Result<Option<(ObjectKey, PackEntry)>> {
        if let Some(found) = self.located(object) { return Ok(Some(found)); }
        if packed(object.as_str()) { self.refresh().await?; }
        Ok(self.located(object))
    }
}

fn catalog_key(block: i64) -> rtdb_storage::Result<ObjectKey> {
    key(&rtdb_format::keys::block_key(rtdb_types::BlockIndex(block)).map_err(|error| Error::Task(error.to_string()))?)
}

fn pack_key(block: i64) -> rtdb_storage::Result<ObjectKey> { key(&block_pack(catalog_key(block)?.as_str())) }

fn key(name: &str) -> rtdb_storage::Result<ObjectKey> { ObjectKey::new(name).map_err(|error| Error::Task(error.to_string())) }

#[async_trait]
impl ObjectStore for PackingStore {
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.inner.supports_scan_ranges(key) }
    async fn head(&self, object: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        if let Some(bytes) = self.held(object) { return Ok(StoredObject { key: object.clone(), size: bytes.len() as u64 }); }
        match self.inner.head(object).await {
            Err(Error::MissingObject(missing)) => match self.locate(object).await? {
                Some((_, entry)) => Ok(StoredObject { key: object.clone(), size: entry.length }),
                None => Err(Error::MissingObject(missing)),
            },
            other => other,
        }
    }
    async fn get(&self, object: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        if let Some(bytes) = self.held(object) { return Ok(bytes); }
        if let Some((pack, entry)) = self.locate(object).await? { return self.inner.get_range(&pack, entry.offset..entry.offset + entry.length).await; }
        self.inner.get(object).await
    }
    async fn get_range(&self, object: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        if let Some(bytes) = self.held(object) {
            if range.start > range.end || range.end > bytes.len() as u64 { return Err(Error::InvalidRange { key: object.to_string(), start: range.start, end: range.end, length: bytes.len() as u64 }); }
            return Ok(bytes.slice(range.start as usize..range.end as usize));
        }
        if let Some((pack, entry)) = self.locate(object).await? {
            if range.start > range.end || range.end > entry.length { return Err(Error::InvalidRange { key: object.to_string(), start: range.start, end: range.end, length: entry.length }); }
            return self.inner.get_range(&pack, entry.offset + range.start..entry.offset + range.end).await;
        }
        self.inner.get_range(object, range).await
    }
    async fn put(&self, object: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        if held_back(object.as_str()) {
            let full = {
                let mut state = self.state.lock();
                state.held_bytes += bytes.len();
                state.held.insert(object.as_str().to_owned(), bytes);
                state.held_bytes > MAXIMUM_HELD_BYTES
            };
            return if full { self.write_pack(None).await } else { Ok(()) };
        }
        if is_catalog(object.as_str()) {
            // Put-if-absent holds for a packed catalog as for any object: the same bytes again succeed, others conflict.
            if self.located(object).is_some() || self.locate(object).await?.is_some() {
                if self.get(object).await? != bytes { return Err(Error::ConflictingObject(object.to_string())); }
                // The block was packed already, by a deposed holder say: what this store held back for it is in that pack.
                let mut state = self.state.lock();
                let packed: Vec<String> = state.held.keys().filter(|key| state.located.contains_key(key.as_str())).cloned().collect();
                for key in packed { state.held.remove(&key); }
                state.held_bytes = state.held.values().map(Bytes::len).sum();
                return Ok(());
            }
            self.state.lock().held.insert(object.as_str().to_owned(), bytes);
            return self.write_pack(Some(object)).await;
        }
        self.inner.put(object, bytes).await
    }
    async fn put_file(&self, object: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        if held_back(object.as_str()) || is_catalog(object.as_str()) {
            let bytes = tokio::fs::read(source).await.map_err(|error| Error::Task(format!("cannot read {}: {error}", source.display())))?;
            return self.put(object, Bytes::from(bytes)).await;
        }
        self.inner.put_file(object, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let prefix = directory.map(|directory| format!("{}/", directory.as_str()));
        let under = |name: &str| prefix.as_deref().is_none_or(|prefix| name.starts_with(prefix));
        self.refresh().await?;
        let mut listed: BTreeMap<String, StoredObject> = self.inner.list(directory).await?.into_iter()
            .filter(|object| !object.key.as_str().starts_with(PACK_DIRECTORY)).map(|object| (object.key.as_str().to_owned(), object)).collect();
        let state = self.state.lock();
        for (name, (_, entry)) in &state.located { if under(name) { listed.entry(name.clone()).or_insert(StoredObject { key: key(name)?, size: entry.length }); } }
        for (name, bytes) in &state.held { if under(name) { listed.entry(name.clone()).or_insert(StoredObject { key: key(name)?, size: bytes.len() as u64 }); } }
        Ok(listed.into_values().collect())
    }
    async fn delete(&self, object: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(object).await }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_storage::MemoryStore;

    fn object(name: &str) -> ObjectKey { ObjectKey::new(name).unwrap() }

    #[tokio::test]
    async fn a_flush_costs_one_object_and_every_object_still_reads() {
        let raw = Arc::new(MemoryStore::default());
        let store = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        for name in ["tables/public$orders/data/l00-rc-b00.arrow", "tables/public$orders/meta/l00-rc-b00.arrow", "tables/public$orders/blocks/b00.binpb", "tables/public$items/data/l00-rc-b00.arrow"] {
            store.put(&object(name), Bytes::from(name.as_bytes().to_vec())).await.unwrap();
        }
        assert!(raw.list(None).await.unwrap().is_empty(), "nothing is written before the catalog");
        assert_eq!(store.get(&object("tables/public$orders/blocks/b00.binpb")).await.unwrap().as_ref(), b"tables/public$orders/blocks/b00.binpb", "held objects read back");
        store.put(&object("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        let written: Vec<String> = raw.list(None).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(written, ["~packs/b00.pack"], "one pack, holding the catalog too");
        assert_eq!(store.get(&object("blocks/b00.binpb")).await.unwrap().as_ref(), b"catalog");
        store.put(&object("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        assert!(store.put(&object("blocks/b00.binpb"), Bytes::from_static(b"other")).await.is_err(), "a packed catalog is put-if-absent like any object");

        let reader = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        assert_eq!(reader.get_range(&object("tables/public$orders/meta/l00-rc-b00.arrow"), 7..13).await.unwrap().as_ref(), b"public");
        assert_eq!(reader.head(&object("tables/public$items/data/l00-rc-b00.arrow")).await.unwrap().size, 41);
        let listed: Vec<String> = reader.list(Some(&object("tables/public$orders"))).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(listed.len(), 3, "packed objects are listed where they belong: {listed:?}");
        store.put(&object("tables/public$orders/data/l01-rc-b00.arrow"), Bytes::from_static(b"compacted")).await.unwrap();
        assert_eq!(raw.get(&object("tables/public$orders/data/l01-rc-b00.arrow")).await.unwrap().as_ref(), b"compacted", "compaction outputs go straight through");
    }

    #[tokio::test]
    async fn a_reader_finds_packs_published_after_it_opened() {
        let raw = Arc::new(MemoryStore::default());
        let reader = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        let writer = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        writer.put(&object("tables/t/data/l00-rc-b01.arrow"), Bytes::from_static(b"late")).await.unwrap();
        writer.put(&object("blocks/b01.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        assert_eq!(reader.get(&object("tables/t/data/l00-rc-b01.arrow")).await.unwrap().as_ref(), b"late");
    }

    /// A store that counts the listings made of it.
    struct Counting { inner: MemoryStore, lists: std::sync::atomic::AtomicUsize }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.list(directory).await
        }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn a_store_opened_from_a_published_block_finds_every_pack_without_listing() {
        let raw = Arc::new(Counting { inner: MemoryStore::default(), lists: Default::default() });
        let writer = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        for block in 0..3i64 {
            writer.put(&object(&format!("tables/t/data/l00-rc-b{block:02}.arrow")), Bytes::from(format!("rows {block}"))).await.unwrap();
            writer.put(&catalog_key(block).unwrap(), Bytes::from_static(b"catalog")).await.unwrap();
        }
        let lists = raw.lists.load(std::sync::atomic::Ordering::SeqCst);
        for hint in [0, 2] {
            let reader = PackingStore::open(raw.clone(), PackDiscovery::From(hint)).await.unwrap();
            for block in 0..3 { assert_eq!(reader.get(&object(&format!("tables/t/data/l00-rc-b{block:02}.arrow"))).await.unwrap(), Bytes::from(format!("rows {block}"))); }
        }
        assert_eq!(raw.lists.load(std::sync::atomic::Ordering::SeqCst), lists, "from a published block, nothing is listed");
        let reader = PackingStore::open(raw.clone(), PackDiscovery::From(9)).await.unwrap();
        assert_eq!(reader.get(&object("tables/t/data/l00-rc-b01.arrow")).await.unwrap().as_ref(), b"rows 1", "a hint that is gone falls back to listing");
    }

    #[tokio::test]
    async fn a_block_packed_already_by_a_deposed_writer_is_taken_as_its_successors() {
        let raw = Arc::new(MemoryStore::default());
        let (deposed, successor) = (PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap(), PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap());
        successor.put(&object("tables/t/data/l00-rc-b00.arrow"), Bytes::from_static(b"rows")).await.unwrap();
        deposed.put(&object("tables/t/data/l00-rc-b00.arrow"), Bytes::from_static(b"rows")).await.unwrap();
        deposed.put(&catalog_key(0).unwrap(), Bytes::from_static(b"catalog")).await.unwrap();
        successor.put(&catalog_key(0).unwrap(), Bytes::from_static(b"catalog")).await.unwrap();
        assert_eq!(raw.list(None).await.unwrap().len(), 1, "the block is one pack still");
        assert_eq!(successor.get(&object("tables/t/data/l00-rc-b00.arrow")).await.unwrap().as_ref(), b"rows");
        assert!(successor.put(&catalog_key(0).unwrap(), Bytes::from_static(b"a different catalog")).await.is_err(), "a different block under the same name is still refused");
        successor.put(&object("tables/t/data/l00-rc-b01.arrow"), Bytes::from_static(b"next")).await.unwrap();
        successor.put(&catalog_key(1).unwrap(), Bytes::from_static(b"catalog 1")).await.unwrap();
        let index = read_index(raw.as_ref(), &pack_key(1).unwrap()).await.unwrap();
        assert_eq!(index.entries.keys().cloned().collect::<Vec<_>>(), ["blocks/b01.binpb", "tables/t/data/l00-rc-b01.arrow"], "and the next block's pack holds only its own objects");
    }
}
