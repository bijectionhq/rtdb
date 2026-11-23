//! Packs (`WritePackAndManifest`): a block flush costs one object, its block's pack, which is also the deployment's boot object.
//!
//! The objects a flush writes before its block catalog (level-0 trie data and metadata, per-table block files) are held back and written, with the catalog, as one pack whose trailer indexes them.
//! A cold start reads the latest block's pack and nothing else: its index, its catalog and its table blocks are ranges of one object.
//! Every other write (compaction outputs, oversized payloads) goes straight through.
//! The pack written with a block catalog is named after that block, and its index lists every pack written before it, so a store finds every pack by reading the latest block's, never listing.
//! A store probes for newer block catalogs when asked for an object it does not know, so a reader finds packs its writer published after it opened.
//! An audience writer loads locations before attachment and publishes new locations with the blocks its readers may capture.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectObservation, ObjectStore, StoredObject};
use rtdb_types::PhysicalObjectVersion;
use serde::{Deserialize, Serialize};

mod extent;
use extent::entry_fits;

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

pub(crate) fn is_pack_key(key: &str) -> bool {
    key.strip_prefix(PACK_DIRECTORY).is_some_and(|suffix| suffix.starts_with('/'))
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
    Ok(observe_pack_index(store, pack).await?.value)
}

pub(crate) async fn observe_pack_index(store: &dyn ObjectStore, pack: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<PackIndex>> {
    read_pack_index(store, pack, store.observe_head(pack).await?).await
}

async fn read_pack_index(store: &dyn ObjectStore, pack: &ObjectKey, head: ObjectObservation<StoredObject>) -> rtdb_storage::Result<ObjectObservation<PackIndex>> {
    let expected = head.version.as_ref();
    let read = |range| async move {
        let observed = store.observe_range(pack, range).await?;
        require_same_version(pack, expected, observed.version.as_ref())?;
        Ok::<_, Error>(observed.value)
    };
    let value = read_pack_index_with(pack, head.value.size, usize::MAX, read).await?;
    Ok(ObjectObservation { value, version: head.version })
}

pub(crate) async fn read_pack_index_with<F, E>(
    pack: &ObjectKey, size: u64, maximum_index_bytes: usize, read: impl Fn(Range<u64>) -> F,
) -> std::result::Result<PackIndex, E>
where
    F: std::future::Future<Output = std::result::Result<Bytes, E>>,
    E: From<Error>,
{
    if size < 8 { return Err(Error::Task(format!("{pack} is too short to be a pack")).into()); }
    let trailer = read(size - 8..size).await?;
    let trailer = trailer.as_ref().try_into().map_err(|_| Error::Task(format!("{pack} has an incomplete trailer")))?;
    let length = u64::from_be_bytes(trailer);
    let payload_end = (size - 8).checked_sub(length).ok_or_else(|| Error::Task(format!("{pack} has a malformed trailer")))?;
    if length > maximum_index_bytes as u64 {
        return Err(Error::MessageTooLarge { size: usize::try_from(length).unwrap_or(usize::MAX), maximum: maximum_index_bytes }.into());
    }
    let encoded = read(payload_end..size - 8).await?;
    let index: PackIndex = serde_json::from_slice(&encoded).map_err(|error| Error::Task(format!("{pack} has a malformed index: {error}")))?;
    if index.entries.values().any(|entry| !entry_fits(entry.offset, entry.length, payload_end)) {
        return Err(Error::Task(format!("{pack} has an entry outside its payload")).into());
    }
    Ok(index)
}

fn require_same_version(pack: &ObjectKey, expected: Option<&PhysicalObjectVersion>, observed: Option<&PhysicalObjectVersion>) -> rtdb_storage::Result<()> {
    if expected != observed {
        return Err(Error::Remote { key: pack.to_string(), message: "The pack publication changed during observation".into() });
    }
    Ok(())
}

fn require_pack_relocation(pack: &ObjectKey, expected: Option<&PhysicalObjectVersion>, observed: Option<&PhysicalObjectVersion>) -> rtdb_storage::Result<()> {
    let copied = expected.zip(observed).is_some_and(|(prior, current)| {
        prior.namespace == current.namespace && prior.key != current.key
    });
    if copied { Ok(()) } else { require_same_version(pack, expected, observed) }
}

#[derive(Clone)]
struct LocatedEntry {
    pack: ObjectKey,
    entry: PackEntry,
    version: Option<Arc<PhysicalObjectVersion>>,
}

enum HeadRequest {
    MemberPack(ObjectKey, Vec<(ObjectKey, LocatedEntry)>),
    Loose(ObjectKey),
}

enum PackRead { Full, Exact(usize), Range(Range<u64>) }

impl PackRead {
    fn extent(&self, object: &ObjectKey, entry: &PackEntry) -> rtdb_storage::Result<Range<u64>> {
        let range = match self {
            Self::Full => 0..entry.length,
            Self::Exact(length) if *length as u64 == entry.length => 0..entry.length,
            Self::Exact(_) => return Err(Error::Task(format!("Immutable object {object} has the wrong length"))),
            Self::Range(range) if range.start <= range.end && range.end <= entry.length => range.clone(),
            Self::Range(range) => return Err(Error::InvalidRange { key: object.to_string(), start: range.start, end: range.end, length: entry.length }),
        };
        Ok(entry.offset + range.start..entry.offset + range.end)
    }
}

struct State {
    held: BTreeMap<String, Bytes>,
    held_bytes: usize,
    /// Which pack holds each packed object.
    located: BTreeMap<String, LocatedEntry>,
    packs: Vec<ObjectKey>,
    discovery: PackDiscovery,
    /// The latest block whose pack this store has read.
    latest_block: Option<i64>,
}

impl State {
    fn register(&mut self, pack: ObjectKey, publication: &ObjectObservation<PackIndex>) {
        let version = publication.version.clone().map(Arc::new);
        for (object, entry) in &publication.value.entries {
            self.located.insert(object.clone(), LocatedEntry { pack: pack.clone(), entry: entry.clone(), version: version.clone() });
        }
        if !self.packs.contains(&pack) { self.packs.push(pack); }
    }

    fn hold(&mut self, object: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        match self.held.entry(object.to_string()) {
            std::collections::btree_map::Entry::Occupied(entry) if entry.get() != &bytes => {
                Err(Error::ConflictingObject(object.to_string()))
            }
            std::collections::btree_map::Entry::Occupied(_) => Ok(()),
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.held_bytes += bytes.len();
                entry.insert(bytes);
                Ok(())
            }
        }
    }
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
        let publication = match self.observe_index(&pack).await {
            Ok(publication) => publication,
            Err(Error::MissingObject(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        self.state.lock().register(pack, &publication);
        Ok(Some(publication.value))
    }

    /// Loads the serving writer's pack locations before audience reads can start.
    /// Later publications made by this writer register their locations before applying the new catalog.
    pub(crate) async fn prepare_audience_reads(&self) -> rtdb_storage::Result<()> {
        IoRegion::audience_object_read(self.refresh()).await
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
        let held = {
            let mut state = self.state.lock();
            state.held_bytes = 0;
            std::mem::take(&mut state.held)
        };
        if held.is_empty() && catalog.is_none() { return Ok(()); }
        let (name, previous) = match catalog {
            Some(catalog) => (key(&block_pack(catalog.as_str()))?, self.state.lock().packs.iter().map(|pack| pack.as_str().to_owned()).collect()),
            None => (key(&format!("{PACK_DIRECTORY}/p{:020}-{}.pack", (self.clock)(), uuid::Uuid::new_v4().simple()))?, Vec::new()),
        };
        let (bytes, index) = encode_pack(&held, previous, (self.clock)());
        let publication = match self.inner.put_with_publication(&name, bytes).await {
            Ok(publication) => Ok(ObjectObservation { value: index, version: publication.and_then(rtdb_types::PhysicalObjectPublication::into_version) }),
            Err(error @ Error::ConflictingObject(_)) if catalog.is_some() => {
                self.compare_pack(&name, &held).await.and_then(|same| same.ok_or(error))
            }
            Err(error) => Err(error),
        };
        let mut state = self.state.lock();
        match publication {
            Ok(publication) => {
                state.register(name, &publication);
                Ok(())
            }
            Err(error) => {
                for (object, bytes) in held { state.held.entry(object).or_insert(bytes); }
                state.held_bytes = state.held.values().map(Bytes::len).sum();
                Err(error)
            }
        }
    }

    async fn observe_index(&self, pack: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<PackIndex>> {
        observe_pack_index(self.inner.as_ref(), pack).await
    }

    /// Verifies an existing publication against all the bytes prepared for the block.
    async fn compare_pack(&self, name: &ObjectKey, objects: &BTreeMap<String, Bytes>) -> rtdb_storage::Result<Option<ObjectObservation<PackIndex>>> {
        let index = self.observe_index(name).await?;
        if index.value.entries.len() != objects.len() { return Ok(None); }
        for (key, entry) in &index.value.entries {
            let Some(bytes) = objects.get(key) else { return Ok(None); };
            let observed = self.inner.observe_range(name, entry.offset..entry.offset + entry.length).await?;
            require_same_version(name, index.version.as_ref(), observed.version.as_ref())?;
            if observed.value != *bytes { return Ok(None); }
        }
        Ok(Some(index))
    }

    async fn observe_location(&self, object: &ObjectKey, location: LocatedEntry) -> rtdb_storage::Result<LocatedEntry> {
        let head = self.inner.observe_head(&location.pack).await?;
        if location.version.as_deref() == head.version.as_ref() {
            if !entry_fits(location.entry.offset, location.entry.length, head.value.size) {
                return Err(Error::Task(format!("The stored pack {} does not cover {object}", location.pack)));
            }
            return Ok(location);
        }
        if IoRegion::requires_audience_transport() {
            require_same_version(&location.pack, location.version.as_deref(), head.version.as_ref())?;
        }
        // A fork's reference can move to its own copy. A new version at the same physical path cannot inherit the admitted index.
        require_pack_relocation(&location.pack, location.version.as_deref(), head.version.as_ref())?;
        let mut publication = read_pack_index(self.inner.as_ref(), &location.pack, head).await?;
        let entry = publication.value.entries.remove(object.as_str()).ok_or_else(|| Error::MissingObject(object.to_string()))?;
        Ok(LocatedEntry { pack: location.pack.clone(), entry, version: publication.version.map(Arc::new) })
    }

    async fn observe_members(&self, pack: &ObjectKey, members: Vec<(ObjectKey, LocatedEntry)>)
    -> rtdb_storage::Result<Vec<(ObjectKey, ObjectObservation<StoredObject>)>> {
        let head = self.inner.observe_head(pack).await?;
        let mut relocated = false;
        for (_, location) in &members {
            if location.version.as_deref() == head.version.as_ref() { continue; }
            if IoRegion::requires_audience_transport() {
                require_same_version(pack, location.version.as_deref(), head.version.as_ref())?;
            }
            require_pack_relocation(pack, location.version.as_deref(), head.version.as_ref())?;
            relocated = true;
        }
        let size = head.value.size;
        let version = head.version.clone();
        let index = if relocated { Some(read_pack_index(self.inner.as_ref(), pack, head).await?.value) } else { None };
        members.into_iter().map(|(object, location)| {
            let entry = match &index {
                Some(index) => index.entries.get(object.as_str()).ok_or_else(|| Error::MissingObject(object.to_string()))?,
                None => &location.entry,
            };
            if !entry_fits(entry.offset, entry.length, size) {
                return Err(Error::Task(format!("The stored pack {pack} does not cover {object}")));
            }
            Ok((object.clone(), ObjectObservation {
                value: StoredObject { key: object.clone(), size: entry.length },
                version: version.clone(),
            }))
        }).collect()
    }

    /// Observes the owned publication named by this member's admitted index.
    pub(crate) async fn retirement_pack(&self, member: &ObjectKey) -> rtdb_storage::Result<Option<(ObjectKey, PhysicalObjectVersion)>> {
        let Some(location) = self.locate(member).await? else { return Ok(None); };
        if pack_block(location.pack.as_str()).is_none() { return Ok(None); }
        let location = match self.observe_location(member, location).await {
            Ok(location) => location,
            Err(Error::MissingObject(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(version) = location.version else { return Ok(None); };
        let owned = self.inner.retirement_version(&location.pack).await?;
        Ok((owned.as_ref() == Some(version.as_ref())).then(|| (location.pack, (*version).clone())))
    }

    async fn read_packed(&self, object: &ObjectKey, location: LocatedEntry, read: PackRead) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let location = self.observe_location(object, location).await?;
        let range = read.extent(object, &location.entry)?;
        let observed = self.inner.observe_range(&location.pack, range).await?;
        require_same_version(&location.pack, location.version.as_deref(), observed.version.as_ref())?;
        Ok(observed)
    }

    fn held(&self, object: &ObjectKey) -> Option<Bytes> { self.state.lock().held.get(object.as_str()).cloned() }
    fn located(&self, object: &ObjectKey) -> Option<LocatedEntry> { self.state.lock().located.get(object.as_str()).cloned() }

    async fn locate(&self, object: &ObjectKey) -> rtdb_storage::Result<Option<LocatedEntry>> {
        if let Some(found) = self.located(object) { return Ok(Some(found)); }
        if packed(object.as_str()) && !IoRegion::requires_audience_transport() { self.refresh().await?; }
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
        if IoRegion::requires_audience_transport() { return Ok(self.observe_head(object).await?.value); }
        if let Some(bytes) = self.held(object) { return Ok(StoredObject { key: object.clone(), size: bytes.len() as u64 }); }
        match self.inner.head(object).await {
            Err(Error::MissingObject(missing)) => match self.locate(object).await? {
                Some(location) => Ok(StoredObject { key: object.clone(), size: location.entry.length }),
                None => Err(Error::MissingObject(missing)),
            },
            other => other,
        }
    }
    async fn head_fresh(&self, object: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        Ok(self.observe_head(object).await?.value)
    }
    async fn observe_head(&self, object: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        let Some(location) = self.locate(object).await? else {
            return self.inner.observe_head(object).await;
        };
        let location = self.observe_location(object, location).await?;
        Ok(ObjectObservation {
            value: StoredObject { key: object.clone(), size: location.entry.length },
            version: location.version.as_deref().cloned(),
        })
    }
    async fn observe_heads(&self, keys: &[ObjectKey]) -> rtdb_storage::Result<BTreeMap<ObjectKey, ObjectObservation<StoredObject>>> {
        let mut requests = Vec::new();
        let mut packs: BTreeMap<ObjectKey, Vec<_>> = BTreeMap::new();
        for object in keys {
            if let Some(location) = self.locate(object).await? {
                packs.entry(location.pack.clone()).or_default().push((object.clone(), location));
            } else {
                requests.push(HeadRequest::Loose(object.clone()));
            }
        }
        requests.extend(packs.into_iter().map(|(pack, members)| HeadRequest::MemberPack(pack, members)));
        let batches = futures::stream::iter(requests).map(|request| async move {
            match request {
                HeadRequest::MemberPack(pack, members) => self.observe_members(&pack, members).await,
                HeadRequest::Loose(key) => {
                    let head = self.inner.observe_head(&key).await?;
                    Ok(vec![(key, head)])
                }
            }
        }).buffer_unordered(16);
        futures::pin_mut!(batches);
        let mut observed = BTreeMap::new();
        while let Some(batch) = batches.try_next().await? { observed.extend(batch); }
        Ok(observed)
    }
    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<PhysicalObjectVersion>> {
        if packed(key.as_str()) { return Ok(None); }
        self.inner.retirement_version(key).await
    }
    async fn retirement_publication(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        if packed(key.as_str()) { return Ok(None); }
        self.inner.retirement_publication(key).await
    }
    async fn get(&self, object: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        if IoRegion::requires_audience_transport() { return self.get_fresh(object).await; }
        if let Some(bytes) = self.held(object) { return Ok(bytes); }
        if let Some(location) = self.locate(object).await? { return self.read_packed(object, location, PackRead::Full).await.map(|read| read.value); }
        self.inner.get(object).await
    }
    async fn get_fresh(&self, object: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        if let Some(location) = self.locate(object).await? { return self.read_packed(object, location, PackRead::Full).await.map(|read| read.value); }
        self.inner.get_fresh(object).await
    }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> {
        Ok(self.observe_fresh_exact(key, length).await?.value)
    }
    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        if let Some(location) = self.locate(key).await? {
            return self.read_packed(key, location, PackRead::Exact(length)).await;
        }
        self.inner.observe_fresh_exact(key, length).await
    }
    async fn get_range(&self, object: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        if !IoRegion::requires_audience_transport() && let Some(bytes) = self.held(object) {
            if range.start > range.end || range.end > bytes.len() as u64 { return Err(Error::InvalidRange { key: object.to_string(), start: range.start, end: range.end, length: bytes.len() as u64 }); }
            return Ok(bytes.slice(range.start as usize..range.end as usize));
        }
        if let Some(location) = self.locate(object).await? { return self.read_packed(object, location, PackRead::Range(range)).await.map(|read| read.value); }
        self.inner.get_range(object, range).await
    }
    async fn observe_range(&self, object: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        if let Some(location) = self.locate(object).await? {
            return self.read_packed(object, location, PackRead::Range(range)).await;
        }
        self.inner.observe_range(object, range).await
    }

    async fn put(&self, object: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        if held_back(object.as_str()) {
            let full = {
                let mut state = self.state.lock();
                state.hold(object, bytes)?;
                state.held_bytes > MAXIMUM_HELD_BYTES
            };
            return if full { self.write_pack(None).await } else { Ok(()) };
        }
        if is_catalog(object.as_str()) {
            // Put-if-absent holds for a packed catalog as for any object: the same bytes again succeed, others conflict.
            if self.located(object).is_some() || self.locate(object).await?.is_some() {
                if self.get_fresh(object).await? != bytes { return Err(Error::ConflictingObject(object.to_string())); }
                // The block was packed already, by a deposed holder say: what this store held back for it is in that pack.
                let mut state = self.state.lock();
                let packed: Vec<String> = state.held.keys().filter(|key| state.located.contains_key(key.as_str())).cloned().collect();
                for key in packed { state.held.remove(&key); }
                state.held_bytes = state.held.values().map(Bytes::len).sum();
                return Ok(());
            }
            self.state.lock().hold(object, bytes)?;
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
    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        if packed(key.as_str()) {
            self.put(key, bytes).await?;
            return Ok(None);
        }
        self.inner.put_with_publication(key, bytes).await
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
        if packed(key.as_str()) {
            self.put_file(key, source).await?;
            return Ok(None);
        }
        self.inner.put_file_with_publication(key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let prefix = directory.map(|directory| format!("{}/", directory.as_str()));
        let under = |name: &str| prefix.as_deref().is_none_or(|prefix| name.starts_with(prefix));
        self.refresh().await?;
        let mut listed: BTreeMap<String, StoredObject> = self.inner.list(directory).await?.into_iter()
            .filter(|object| !object.key.as_str().starts_with(PACK_DIRECTORY)).map(|object| (object.key.as_str().to_owned(), object)).collect();
        let state = self.state.lock();
        for (name, location) in &state.located { if under(name) { listed.entry(name.clone()).or_insert(StoredObject { key: key(name)?, size: location.entry.length }); } }
        for (name, bytes) in &state.held { if under(name) { listed.entry(name.clone()).or_insert(StoredObject { key: key(name)?, size: bytes.len() as u64 }); } }
        Ok(listed.into_values().collect())
    }
    async fn delete(&self, object: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(object).await }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rtdb_storage::MemoryStore;

    fn object(name: &str) -> ObjectKey { ObjectKey::new(name).unwrap() }

    #[tokio::test]
    async fn audience_reads_use_the_installed_pack_locations() {
        let raw: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let earlier = object("tables/orders/data/l00-rc-b00.arrow");
        let later = object("tables/orders/data/l00-rc-b01.arrow");
        let bytes = Bytes::from_static(b"row");
        let writer = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        writer.put(&earlier, bytes.clone()).await.unwrap();
        writer.put(&catalog_key(0).unwrap(), Bytes::from_static(b"block zero")).await.unwrap();

        let holder = PackingStore::open(raw, PackDiscovery::List).await.unwrap();
        holder.prepare_audience_reads().await.unwrap();
        assert_eq!(IoRegion::audience_object_read(holder.get_fresh_exact(&earlier, bytes.len())).await.unwrap(), bytes);

        writer.put(&later, bytes.clone()).await.unwrap();
        writer.put(&catalog_key(1).unwrap(), Bytes::from_static(b"block one")).await.unwrap();
        assert!(matches!(IoRegion::audience_object_read(holder.get_fresh_exact(&later, bytes.len())).await,
            Err(Error::MissingObject(_))));
        assert_eq!(holder.get_fresh_exact(&later, bytes.len()).await.unwrap(), bytes);
    }

    #[derive(Default)]
    pub(crate) struct VersionedPacks {
        inner: MemoryStore,
        versions: Mutex<BTreeMap<ObjectKey, usize>>,
        ranges: std::sync::atomic::AtomicUsize,
        change_on_range: std::sync::atomic::AtomicUsize,
        heads: std::sync::atomic::AtomicUsize,
        maximum_read: std::sync::atomic::AtomicU64,
        pub(crate) change_on_exact: std::sync::atomic::AtomicBool,
    }

    impl VersionedPacks {
        pub(crate) fn limit_read_size(&self, bytes: u64) {
            self.maximum_read.store(bytes, std::sync::atomic::Ordering::Relaxed);
        }

        fn require_read_size(&self, key: &ObjectKey, bytes: u64) -> rtdb_storage::Result<()> {
            let maximum = self.maximum_read.load(std::sync::atomic::Ordering::Relaxed);
            if maximum > 0 && bytes > maximum {
                return Err(Error::PermissionDenied { key: key.to_string(), message: "test provider refused a larger read".into() });
            }
            Ok(())
        }

        pub(crate) fn head_observations(&self) -> usize {
            self.heads.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn version(&self, key: &ObjectKey) -> PhysicalObjectVersion {
            let version = self.versions.lock()[key];
            PhysicalObjectVersion {
                namespace: rtdb_types::S3ObjectNamespace { bucket: "test-cell".into(), endpoint: None, region: None },
                key: key.to_string(), version: version.to_string(),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for VersionedPacks {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
            self.heads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(ObjectObservation { value: self.head(key).await?, version: Some(self.version(key)) })
        }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
            if self.maximum_read.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                self.require_read_size(key, self.inner.head(key).await?.size)?;
            }
            self.inner.get(key).await
        }
        async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
            self.require_read_size(key, length as u64)?;
            let value = self.inner.get_fresh_exact(key, length).await?;
            let mut version = self.version(key);
            if self.change_on_exact.load(std::sync::atomic::Ordering::SeqCst) {
                version.version = "different".into();
            }
            Ok(ObjectObservation { value, version: Some(version) })
        }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
            self.require_read_size(key, range.end.saturating_sub(range.start))?;
            self.inner.get_range(key, range).await
        }
        async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
            use std::sync::atomic::Ordering::SeqCst;
            let value = self.get_range(key, range).await?;
            let mut version = self.version(key);
            if self.ranges.fetch_add(1, SeqCst) + 1 == self.change_on_range.load(SeqCst) {
                version.version = "changed".into();
            }
            Ok(ObjectObservation { value, version: Some(version) })
        }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            self.put_with_publication(key, bytes).await.map(drop)
        }
        async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<Option<rtdb_types::PhysicalObjectPublication>> {
            let existed = self.inner.head(key).await.is_ok();
            self.inner.put(key, bytes).await?;
            if !existed { *self.versions.lock().entry(key.clone()).or_default() += 1; }
            Ok(Some(rtdb_types::PhysicalObjectPublication::Version(self.version(key))))
        }
        async fn put_file(&self, _: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> { unreachable!("tests publish bytes") }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn fresh_heads_share_one_pack_observation_and_refuse_a_replaced_version() {
        let raw = Arc::new(VersionedPacks::default());
        let store = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        let root = catalog_key(0).unwrap();
        let table = object("tables/t/blocks/b00.binpb");
        let loose = object("tables/t/data/l01-rc-b00.arrow");
        store.put(&table, Bytes::from_static(b"table")).await.unwrap();
        store.put(&root, Bytes::from_static(b"root")).await.unwrap();
        store.put(&loose, Bytes::from_static(b"loose")).await.unwrap();
        let requests = [table.clone(), root.clone(), loose.clone()];
        let prior = raw.heads.load(std::sync::atomic::Ordering::Relaxed);
        let heads = store.observe_heads(&requests).await.unwrap();
        assert_eq!(raw.heads.load(std::sync::atomic::Ordering::Relaxed) - prior, 2);
        assert_eq!(heads[&table].value.size, 5);
        assert_eq!(heads[&root].value.size, 4);
        assert_eq!(heads[&table].version, heads[&root].version);
        assert_ne!(heads[&root].version, heads[&loose].version);

        let pack = object(&block_pack(root.as_str()));
        let bytes = raw.get(&pack).await.unwrap();
        raw.delete(&pack).await.unwrap();
        assert!(matches!(store.observe_heads(&requests).await, Err(Error::MissingObject(_))));
        raw.put(&pack, bytes).await.unwrap();
        assert!(matches!(store.observe_heads(&requests).await, Err(Error::Remote { .. })));
    }

    #[tokio::test]
    async fn checkpoint_member_observations_name_the_inherited_publication() -> rtdb_types::Result<()> {
        let raw: crate::reference::Bucket = Arc::new(VersionedPacks::default());
        let root = catalog_key(0)?;
        let table = object("tables/t/blocks/b00.binpb");
        let loose = object("tables/t/search/value.bin");
        let plaintext = Bytes::from_static(b"source bytes");
        let material = [5; crate::keys::KEY_BYTES];
        let parent = crate::store::open_deployment_store(
            raw.clone(), "parent/v06", PackDiscovery::List, crate::store::wall_clock(),
        ).await?;
        let writer = crate::store::EncryptingStore::new(parent, &material)?;
        for name in [&loose, &table, &root] {
            writer.put(name, plaintext.clone()).await?;
        }
        crate::reference::fork(raw.as_ref(), "parent", "child", None).await?;
        let child = crate::store::open_deployment_store(
            raw, "child/v06", PackDiscovery::List, crate::store::wall_clock(),
        ).await?;
        let reader = crate::store::EncryptingStore::new(child, &material)?;
        let expected_pack = format!("parent/v06/{}", pack_key(0)?.as_str());
        for (name, physical) in [(&root, expected_pack.as_str()), (&table, expected_pack.as_str()),
                                 (&loose, "parent/v06/tables/t/search/value.bin")] {
            let head = reader.observe_head(name).await?;
            let body = reader.observe_fresh_exact(name, plaintext.len()).await?;
            assert_eq!(head.value.key, *name);
            assert_eq!(body.value, plaintext);
            assert_eq!(body.version, head.version, "the exact body and head name the same publication");
            assert_eq!(body.version.as_ref().map(|version| version.key.as_str()), Some(physical),
                "copy observations must preserve the inherited physical publication");
            assert_eq!(reader.retirement_version(name).await?, None);
        }
        Ok(())
    }

    #[tokio::test]
    async fn pack_index_requires_one_physical_publication() {
        use std::sync::atomic::Ordering::SeqCst;
        let raw = Arc::new(VersionedPacks::default());
        let store = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        let root = catalog_key(0).unwrap();
        store.put(&root, Bytes::from_static(b"catalog")).await.unwrap();
        let pack = pack_key(0).unwrap();
        for (read, message) in [(1, "the trailer must match the provider metadata"), (2, "the index must match the provider metadata")] {
            raw.ranges.store(0, SeqCst);
            raw.change_on_range.store(read, SeqCst);
            assert!(matches!(read_index(raw.as_ref(), &pack).await, Err(Error::Remote { .. })), "{message}");
        }
        raw.change_on_range.store(0, SeqCst);
        assert!(read_index(raw.as_ref(), &pack).await.unwrap().entries.contains_key(root.as_str()));
    }

    #[tokio::test]
    async fn cached_pack_offsets_require_the_observed_physical_version() {
        let raw = Arc::new(VersionedPacks::default());
        let writer = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        let root = catalog_key(0).unwrap();
        writer.put(&root, Bytes::from_static(b"catalog")).await.unwrap();
        let reader = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        assert_eq!(reader.get_fresh(&root).await.unwrap().as_ref(), b"catalog");

        let pack = pack_key(0).unwrap();
        let bytes = raw.get(&pack).await.unwrap();
        raw.delete(&pack).await.unwrap();
        raw.put(&pack, bytes).await.unwrap();
        for store in [writer, reader] {
            assert!(matches!(store.head_fresh(&root).await, Err(Error::Remote { .. })), "the same pack path cannot inherit a cached index across versions");
            assert!(matches!(store.get_range(&root, 0..7).await, Err(Error::Remote { .. })));
        }
    }

    #[tokio::test]
    async fn packed_member_bytes_match_the_admitted_pack_version() {
        use std::sync::atomic::Ordering::SeqCst;
        let raw = Arc::new(VersionedPacks::default());
        let writer = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        let root = catalog_key(0).unwrap();
        writer.put(&root, Bytes::from_static(b"catalog")).await.unwrap();
        raw.change_on_range.store(1, SeqCst);
        assert!(matches!(writer.get_fresh_exact(&root, 7).await, Err(Error::Remote { .. })), "the returned bytes must come from the admitted pack");
        assert_eq!(writer.get_fresh_exact(&root, 7).await.unwrap().as_ref(), b"catalog");
    }

    #[tokio::test]
    async fn a_fork_reads_the_copy_after_checking_the_destination_index() {
        use crate::reference::{Base, Bucket, PrefixedStore, ReferencingStore};
        use std::sync::atomic::Ordering::SeqCst;
        let raw = Arc::new(VersionedPacks::default());
        let bucket: Bucket = raw.clone();
        let root = catalog_key(0).unwrap();
        let pack = pack_key(0).unwrap();
        let entries = [(root.to_string(), Bytes::from_static(b"catalog"))].into_iter().collect();
        let (contents, _) = encode_pack(&entries, Vec::new(), 0);
        let parent = object(&format!("parent/{pack}"));
        raw.put(&parent, contents.clone()).await.unwrap();
        let own = Arc::new(PrefixedStore::new(bucket.clone(), "child"));
        let base = Base { objects: BTreeMap::from([(pack.to_string(), "parent".into())]) };
        let reader = PackingStore::open(Arc::new(ReferencingStore::new(own, bucket, base)), PackDiscovery::List).await.unwrap();
        assert_eq!(reader.get_fresh(&root).await.unwrap().as_ref(), b"catalog");

        raw.put(&object(&format!("child/{pack}")), contents).await.unwrap();
        raw.delete(&parent).await.unwrap();
        raw.ranges.store(0, SeqCst);
        raw.change_on_range.store(2, SeqCst);
        assert!(matches!(reader.get_fresh(&root).await, Err(Error::Remote { .. })), "a moved reference must validate the destination index");
        assert_eq!(reader.get_fresh(&root).await.unwrap().as_ref(), b"catalog");
    }

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
    async fn pack_index_admission_requires_entries_within_the_payload() {
        let raw = MemoryStore::default();
        let pack = pack_key(0).unwrap();
        let mut bytes = BytesMut::from(&b"rows"[..]);
        let index = br#"{"entries":{"row":{"offset":3,"length":2}},"previous":[]}"#;
        bytes.extend_from_slice(index);
        bytes.put_u64(index.len() as u64);
        raw.put(&pack, bytes.freeze()).await.unwrap();
        let error = read_index(&raw, &pack).await.unwrap_err();
        assert!(error.to_string().contains("outside its payload"));

        raw.delete(&pack).await.unwrap();
        raw.put(&pack, Bytes::copy_from_slice(&u64::MAX.to_be_bytes())).await.unwrap();
        let error = read_index(&raw, &pack).await.unwrap_err();
        assert!(error.to_string().contains("malformed trailer"));
    }

    #[tokio::test]
    async fn an_oversized_pack_index_is_refused_before_the_index_read() {
        let pack = pack_key(0).unwrap();
        let read = |range| async move {
            assert_eq!(range, 101..109);
            Ok::<_, Error>(Bytes::copy_from_slice(&101u64.to_be_bytes()))
        };
        let error = read_pack_index_with(&pack, 109, 100, read).await.unwrap_err();
        assert!(matches!(error, Error::MessageTooLarge { size: 101, maximum: 100 }));
    }

    #[tokio::test]
    async fn fresh_metadata_requires_the_containing_pack_in_storage() {
        let raw = Arc::new(MemoryStore::default());
        let store = PackingStore::open(raw.clone(), PackDiscovery::List).await.unwrap();
        let rows = object("tables/t/data/l00-rc-b00.arrow");
        let root = catalog_key(0).unwrap();
        store.put(&rows, Bytes::from_static(b"rows")).await.unwrap();
        assert!(store.head(&rows).await.is_ok());
        assert!(matches!(store.head_fresh(&rows).await, Err(Error::MissingObject(_))),
            "staged bytes cannot supply fresh storage evidence");

        store.put(&root, Bytes::from_static(b"catalog")).await.unwrap();
        assert_eq!(store.head_fresh(&rows).await.unwrap().size, 4);
        assert_eq!(store.head_fresh(&root).await.unwrap().size, 7);

        let pack = pack_key(0).unwrap();
        raw.delete(&pack).await.unwrap();
        assert!(store.head(&root).await.is_ok(), "the cached location survives removal");
        assert!(matches!(store.head_fresh(&root).await, Err(Error::MissingObject(_))),
            "a cached pack entry cannot supply fresh storage evidence");

        raw.put(&pack, Bytes::from_static(b"short")).await.unwrap();
        assert!(store.head_fresh(&root).await.is_err(), "a short pack cannot cover the entry");
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
    struct Counting {
        inner: MemoryStore,
        lists: std::sync::atomic::AtomicUsize,
        fail_next_put: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ObjectStore for Counting {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            if self.fail_next_put.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(Error::Task("pack write was interrupted".into()));
            }
            self.inner.put(key, bytes).await
        }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
            self.lists.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.list(directory).await
        }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    #[tokio::test]
    async fn a_store_opened_from_a_published_block_finds_every_pack_without_listing() {
        let raw = Arc::new(Counting { inner: MemoryStore::default(), lists: Default::default(), fail_next_put: false.into() });
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
    async fn an_encrypted_pack_retry_requires_durable_bytes_before_reporting_success() {
        let raw = Arc::new(Counting { inner: MemoryStore::default(), lists: Default::default(), fail_next_put: true.into() });
        let serving = crate::store::open_deployment_store(raw.clone(), "db/v06", PackDiscovery::List, crate::store::wall_clock()).await.unwrap();
        let store = crate::store::EncryptingStore::new(serving, &[7; 32]).unwrap();
        let rows = object("tables/t/data/l00-rc-b00.arrow");
        let root = catalog_key(0).unwrap();
        store.put(&rows, Bytes::from_static(b"rows")).await.unwrap();
        assert!(store.put(&root, Bytes::from_static(b"catalog")).await.is_err());
        assert_eq!(store.get(&root).await.unwrap().as_ref(), b"catalog");
        assert!(matches!(store.head_fresh(&root).await, Err(Error::MissingObject(_))));
        assert!(matches!(store.get_fresh_exact(&root, 7).await, Err(Error::MissingObject(_))));
        let conflicting_retry = store.put(&rows, Bytes::from_static(b"other")).await;
        store.put(&root, Bytes::from_static(b"catalog")).await.unwrap();
        assert!(raw.head(&object("db/v06/~packs/b00.pack")).await.is_ok(), "an encrypted retry must publish the pack before returning");
        assert!(matches!(conflicting_retry, Err(Error::ConflictingObject(_))));
        assert_eq!(store.get_fresh(&root).await.unwrap().as_ref(), b"catalog");
        assert_eq!(store.get_fresh_exact(&rows, 4).await.unwrap().as_ref(), b"rows");
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
