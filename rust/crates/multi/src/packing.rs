//! Packs (`WritePackAndManifest`): a block flush costs one object for all its level-0 files, plus the block catalog.
//!
//! The objects a flush writes before its block catalog (level-0 trie data and metadata, per-table block files) are held back and written, when the catalog is published, as one pack whose trailer indexes them.
//! The catalog itself stays its own object, so recovery still finds blocks where it looks for them.
//! Every other write (compaction outputs, oversized payloads) goes straight through.
//! A store re-reads the pack indexes when asked for an object it does not know, so a reader finds packs its writer published after it opened.

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
pub struct PackIndex { pub entries: BTreeMap<String, PackEntry> }

/// Whether a flush writes `key` before its block catalog: a level-0 trie file or a per-table block file.
pub fn held_back(key: &str) -> bool {
    key.starts_with("tables/") && (key.contains("/data/l00-") || key.contains("/meta/l00-") || key.contains("/blocks/b"))
}

fn is_catalog(key: &str) -> bool { key.starts_with("blocks/b") && key.ends_with(".binpb") }

/// Encodes a pack: the objects back to back, the index as JSON, and the index's length as eight big-endian bytes.
pub fn encode_pack(objects: &BTreeMap<String, Bytes>) -> (Bytes, PackIndex) {
    let mut bytes = BytesMut::new();
    let mut index = PackIndex::default();
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

#[derive(Default)]
struct State {
    held: BTreeMap<String, Bytes>,
    held_bytes: usize,
    /// Which pack holds each packed object.
    located: BTreeMap<String, (ObjectKey, PackEntry)>,
    packs: Vec<ObjectKey>,
}

pub struct PackingStore { inner: Arc<dyn ObjectStore>, state: Mutex<State>, writing: tokio::sync::Mutex<()> }

impl PackingStore {
    pub async fn open(inner: Arc<dyn ObjectStore>) -> rtdb_storage::Result<Arc<Self>> {
        let store = Arc::new(Self { inner, state: Mutex::new(State::default()), writing: tokio::sync::Mutex::new(()) });
        store.refresh().await?;
        Ok(store)
    }

    async fn refresh(&self) -> rtdb_storage::Result<()> {
        let listed = self.inner.list(Some(&key(PACK_DIRECTORY)?)).await?;
        for pack in listed {
            if self.state.lock().packs.contains(&pack.key) { continue; }
            let index = read_index(self.inner.as_ref(), &pack.key).await?;
            let mut state = self.state.lock();
            for (object, entry) in index.entries { state.located.insert(object, (pack.key.clone(), entry)); }
            state.packs.push(pack.key);
        }
        Ok(())
    }

    /// Writes everything held back as one pack.
    async fn write_pack(&self) -> rtdb_storage::Result<()> {
        let _writing = self.writing.lock().await;
        let held = std::mem::take(&mut self.state.lock().held);
        if held.is_empty() { return Ok(()); }
        let (bytes, index) = encode_pack(&held);
        let name = key(&format!("{PACK_DIRECTORY}/p{:020}-{}.pack", rtdb_directory::now_ms(), uuid::Uuid::new_v4().simple()))?;
        if let Err(error) = self.inner.put(&name, bytes).await {
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

    fn held(&self, object: &ObjectKey) -> Option<Bytes> { self.state.lock().held.get(object.as_str()).cloned() }
    fn located(&self, object: &ObjectKey) -> Option<(ObjectKey, PackEntry)> { self.state.lock().located.get(object.as_str()).cloned() }

    async fn locate(&self, object: &ObjectKey) -> rtdb_storage::Result<Option<(ObjectKey, PackEntry)>> {
        if let Some(found) = self.located(object) { return Ok(Some(found)); }
        if held_back(object.as_str()) { self.refresh().await?; }
        Ok(self.located(object))
    }
}

fn key(name: &str) -> rtdb_storage::Result<ObjectKey> { ObjectKey::new(name).map_err(|error| Error::Task(error.to_string())) }

#[async_trait]
impl ObjectStore for PackingStore {
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
            return if full { self.write_pack().await } else { Ok(()) };
        }
        if is_catalog(object.as_str()) { self.write_pack().await?; }
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
    async fn a_flush_costs_one_pack_and_its_catalog_and_every_object_still_reads() {
        let raw = Arc::new(MemoryStore::default());
        let store = PackingStore::open(raw.clone()).await.unwrap();
        for name in ["tables/public$orders/data/l00-rc-b00.arrow", "tables/public$orders/meta/l00-rc-b00.arrow", "tables/public$orders/blocks/b00.binpb", "tables/public$items/data/l00-rc-b00.arrow"] {
            store.put(&object(name), Bytes::from(name.as_bytes().to_vec())).await.unwrap();
        }
        assert!(raw.list(None).await.unwrap().is_empty(), "nothing is written before the catalog");
        assert_eq!(store.get(&object("tables/public$orders/blocks/b00.binpb")).await.unwrap().as_ref(), b"tables/public$orders/blocks/b00.binpb", "held objects read back");
        store.put(&object("blocks/b00.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        let written: Vec<String> = raw.list(None).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(written.len(), 2, "one pack and the catalog: {written:?}");
        assert!(written.iter().any(|name| name == "blocks/b00.binpb"));

        let reader = PackingStore::open(raw.clone()).await.unwrap();
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
        let reader = PackingStore::open(raw.clone()).await.unwrap();
        let writer = PackingStore::open(raw.clone()).await.unwrap();
        writer.put(&object("tables/t/data/l00-rc-b01.arrow"), Bytes::from_static(b"late")).await.unwrap();
        writer.put(&object("blocks/b01.binpb"), Bytes::from_static(b"catalog")).await.unwrap();
        assert_eq!(reader.get(&object("tables/t/data/l00-rc-b01.arrow")).await.unwrap().as_ref(), b"late");
    }
}
