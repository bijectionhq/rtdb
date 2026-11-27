use crate::{AppleSync, Error, IoRegion, Result};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if key.is_empty() || key.contains('\\') || key.contains('\0')
            || key.split('/').any(|part| part.is_empty() || part == "." || part == "..")
            || Path::new(&key).components().any(|component| !matches!(component, Component::Normal(_)))
            || key.split('/').any(|part| part == ".tmp")
        {
            return Err(Error::InvalidKey(key));
        }
        Ok(Self(key))
    }

    pub fn as_str(&self) -> &str { &self.0 }

    pub fn is_under(&self, directory: &ObjectKey) -> bool {
        self == directory || self.0.strip_prefix(&directory.0).is_some_and(|suffix| suffix.starts_with('/'))
    }
}

impl std::fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    pub key: ObjectKey,
    pub size: u64,
}

pub enum DrainedSearchUpload {
    Present(StoredObject),
    Absent,
}

pub fn strong_entity_tag(tag: &str) -> bool {
    tag.strip_prefix('"').and_then(|tag| tag.strip_suffix('"')).is_some_and(|value| {
        !value.is_empty() && value != "*"
            && value.bytes().all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\' | b','))
    })
}

/// Metadata or bytes and the physical version named by the same provider response.
pub struct ObjectObservation<T> {
    pub value: T,
    pub version: Option<rtdb_types::PhysicalObjectVersion>,
}

/// Reads the provider's current hold on a specific physical version.
#[async_trait]
pub trait ObjectVersionHold: Send + Sync {
    async fn require_held(&self, key: &ObjectKey, version: &rtdb_types::PhysicalObjectVersion)
        -> rtdb_types::Result<()>;
}

/// The retained and temporary buffers of an object read, excluding provider transport metadata.
pub struct ObjectReadAllocation {
    pub retained_bytes: usize,
    pub scratch_bytes: usize,
}

pub struct ReadReservation {
    length: usize,
    retention: Arc<dyn Send + Sync>,
    retained_bytes: usize,
    scratch: Option<(usize, Arc<rtdb_types::MemoryReservation>)>,
}

impl ReadReservation {
    /// `retention` must hold the memory charge for `length` bytes until it is dropped.
    pub fn new<T: Send + Sync + 'static>(length: usize, retention: T) -> Self {
        Self { length, retention: Arc::new(retention), retained_bytes: 0, scratch: None }
    }

    /// Holds separate budget reservations for the returned bytes and temporary buffers.
    pub fn funded(length: usize, retained: Arc<rtdb_types::MemoryReservation>, scratch: Option<Arc<rtdb_types::MemoryReservation>>) -> Self {
        let retained_bytes = retained.bytes();
        let scratch = scratch.map(|charge| (charge.bytes(), charge));
        Self { length, retention: retained, retained_bytes, scratch }
    }

    pub fn covers(&self, allocation: &ObjectReadAllocation) -> bool {
        self.retained_bytes >= allocation.retained_bytes
            && self.scratch.as_ref().map_or(0, |(bytes, _)| *bytes) >= allocation.scratch_bytes
    }

    /// Transfers scratch capacity to a nested read whose bytes are temporary to this read.
    pub fn take_scratch_read(&mut self, length: usize, allocation: ObjectReadAllocation) -> Result<Self> {
        let required = allocation.retained_bytes.checked_add(allocation.scratch_bytes)
            .filter(|_| allocation.retained_bytes >= length);
        if required.is_none_or(|required| self.scratch.as_ref().is_none_or(|(bytes, _)| *bytes < required)) {
            return Err(Error::Closed("Nested read exceeds its scratch reservation".into()));
        }
        let (_, charge) = self.scratch.take().ok_or_else(|| Error::Closed("The read has no scratch reservation".into()))?;
        let scratch = (allocation.scratch_bytes > 0).then(|| (allocation.scratch_bytes, Arc::clone(&charge)));
        Ok(Self { length, retention: charge, retained_bytes: allocation.retained_bytes, scratch })
    }

    pub fn length(&self) -> usize { self.length }

    pub fn check_range(&self, range: &Range<u64>) -> Result<()> {
        if range.end.checked_sub(range.start) != Some(self.length as u64) {
            return Err(Error::Task("reserved read range does not match its allocation reservation".into()));
        }
        Ok(())
    }

    /// Keeps output and temporary charges alive in provider tasks admitted while polling this read.
    pub async fn retaining_io<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        let owner: Arc<dyn Send + Sync> = match &self.scratch {
            Some((_, scratch)) => Arc::new((Arc::clone(&self.retention), Arc::clone(scratch))),
            None => Arc::clone(&self.retention),
        };
        crate::IoRegion::retaining(owner, work).await
    }

    /// Transfers the charge to the completed allocation; mismatched byte counts are rejected.
    pub fn finish(self, bytes: Bytes) -> Result<RetainedBytes> {
        if bytes.len() != self.length { return Err(Error::Task("reserved read returned a different byte count".into())); }
        Ok(RetainedBytes { bytes, _retention: self.retention })
    }
}

pub struct RetainedBytes {
    bytes: Bytes,
    _retention: Arc<dyn Send + Sync>,
}

impl RetainedBytes {
    pub fn as_slice(&self) -> &[u8] { &self.bytes }
    pub fn len(&self) -> usize { self.bytes.len() }
    pub fn is_empty(&self) -> bool { self.bytes.is_empty() }
    pub(crate) fn discard_prefix(mut self, bytes: usize) -> Self {
        self.bytes = self.bytes.slice(bytes..);
        self
    }
}

/// A derived ordered index's published page namespace.
#[derive(Clone, Copy)]
pub struct IndexPageScope<'a> {
    pub index: &'a ObjectKey,
    pub generation: u64,
}

/// The hosted store's authority over indexed page copies and their retirement.
/// A retained permit covers decoded pages and provider requests spawned while
/// holding it. The retirement operation records the obligation before returning.
#[async_trait]
pub trait IndexPageLifecycle: Send + Sync {
    async fn retain(&self, scope: IndexPageScope<'_>) -> Result<Arc<dyn Send + Sync>>;
    async fn retire_before(&self, scope: IndexPageScope<'_>) -> Result<()>;
    async fn retire_dropped(&self, active: &BTreeSet<ObjectKey>, through: i64) -> Result<()>;
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Identifies a loose allocation by the S3 address this owner will use,
    /// including for names reserved before the first provider write.
    fn s3_publication_address(&self, _key: &ObjectKey) -> Option<(rtdb_types::S3ObjectNamespace, String)> { None }
    /// Resolves the provider address selected by the current held storage view.
    async fn replica_publication_address(&self, key: &ObjectKey) -> Result<Option<(rtdb_types::S3ObjectNamespace, String)>> {
        let observed = self.observe_head(key).await?;
        Ok(observed.version.map(|version| (version.namespace, version.key))
            .or_else(|| self.s3_publication_address(key)))
    }
    /// Whether returned bytes may be reused across reads without returning to this owner.
    fn permits_shared_read_cache(&self) -> bool { false }
    /// The durability policy used for local filesystem syncs on Apple platforms.
    fn apple_sync(&self) -> AppleSync {
        self.local_maintenance_store().map_or(AppleSync::Full, LocalStore::apple_sync)
    }
    fn local_maintenance_store(&self) -> Option<&LocalStore> { None }
    fn local_erasure_custody(&self) -> Option<&Arc<crate::sealing::LocalErasureCustody>> { None }
    fn local_classic_key_slots(&self) -> Option<&dyn crate::sealing::ObjectCipher> { None }
    async fn require_erasure_catalog(&self, _cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<()> { Ok(()) }
    /// Resolves the single payload named by an authenticated replica delivery.
    async fn read_replica_payload(&self, _custody: &crate::log::ReplicaObjectCustody,
        _memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<crate::log::ReplicaPayload> {
        Err(Error::Task("The store has no installed replica payload broker".into()))
    }
    /// Published checkpoint files survive a process restart on the same storage.
    fn syncs_checkpoints(&self) -> bool { false }
    /// Selects the private Arrow-only ordered-index layout for the lifetime of this attachment.
    fn ordered_index_arrow_only(&self) -> bool { false }
    fn index_page_lifecycle(&self) -> Option<&dyn IndexPageLifecycle> { None }
    /// Opens the storage projection owned by an exact journal snapshot hold.
    async fn open_log_snapshot(
        &self, _authority: Arc<dyn std::any::Any + Send + Sync>,
    ) -> rtdb_types::Result<Arc<dyn ObjectStore>> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.storage/snapshot-custody",
            "The storage provider cannot open the journal's exact snapshot"))
    }
    /// Bounds SQL execution and transport even if the answer uses only in-memory rows.
    async fn catalog_delivery_lease(&self) -> rtdb_types::Result<Option<Arc<crate::ReadLease>>> {
        Ok(None)
    }
    /// Admits one read of a captured catalog where the storage owner grants per-read custody: a hosted journal's
    /// protected snapshot and the erasure custody its plaintext delivery holds. The returned value keeps that custody
    /// while it lives.
    /// Every other store returns `None` and keeps no reader state: a superseded file stays readable for the
    /// garbage-collection window (the newest blocks to keep and the garbage lifetime), and a read of a catalog older than
    /// that window may find its files gone, as in XTDB.
    async fn admit_catalog(
        &self, _cut: Option<rtdb_types::CatalogCut>, _checkpoint: Option<rtdb_types::CatalogCheckpoint>,
    ) -> rtdb_types::Result<Option<Arc<dyn Send + Sync>>> {
        Ok(None)
    }
    /// Whether physical trie retirement belongs to a coordinated storage authority.
    fn defers_trie_releases(&self) -> bool { false }
    /// The custodian can settle covered receipt L0 after releasing its recovery dependencies.
    fn releases_receipt_l0(&self) -> bool { false }
    /// Records an installed retirement with its storage authority before its files are released.
    /// Readers never hold it back: the retirement was selected from tries superseded before the garbage-as-of horizon.
    /// Returns false only while the authority cannot record it yet and expects the caller to retry.
    async fn advance_catalog_floor(&self, _installed: rtdb_types::CatalogCut) -> rtdb_types::Result<bool> {
        Ok(true)
    }
    /// Submits the files selected by an installed trie-removal receipt to their storage authority.
    /// Returns false while the authority retains the files and expects the caller to retry.
    async fn release_trie_files(&self, _receipt: &ObjectKey, _contents: Bytes) -> rtdb_types::Result<bool> {
        Err(rtdb_types::Anomaly::unsupported("rtdb.storage/trie-release", "This store has no coordinated trie release"))
    }
    /// Records the installed retirement and closes the publication of each exact Classic candidate, so a retired name is
    /// never published again. A path is supplied only when the installed receipt identifies an immutable file of a
    /// compacted trie.
    async fn close_classic_trie_admission(&self, cut: rtdb_types::CatalogCut, paths: &[Option<&str>]) -> rtdb_types::Result<Vec<bool>> {
        Ok(vec![self.advance_catalog_floor(cut).await?; paths.len()])
    }
    /// Provider preflight for an exact publication selected by the Classic grant.
    async fn classic_trie_retirement_ready(&self, _key: &ObjectKey, version: Option<&rtdb_types::PhysicalObjectVersion>) -> rtdb_types::Result<bool> {
        Ok(version.is_none())
    }
    /// Physical settlement, called by the Classic retirement owner after admission closes.
    async fn settle_classic_trie(&self, key: &ObjectKey, version: Option<&rtdb_types::PhysicalObjectVersion>) -> rtdb_types::Result<()> {
        if version.is_some() {
            return Err(rtdb_types::Anomaly::unsupported("rtdb.storage/trie-release", "The provider cannot settle an exact object version"));
        }
        self.delete(key).await?;
        Ok(())
    }
    /// Returns the writer registration that owns a staged search run in this residence.
    /// `Ok(None)` is reserved for volatile storage whose runs disappear with the store.
    /// Stores without a staging owner or volatile lifetime return `Unsupported`.
    async fn search_staging_owner(&self) -> rtdb_types::Result<Option<rtdb_format::search_manifest::SearchStagingOwner>> {
        Err(unsupported_search_staging())
    }
    /// Checks that the staged writer was superseded and its provider work drained.
    async fn search_staging_drained(&self, _owner: rtdb_format::search_manifest::SearchStagingOwner)
        -> rtdb_types::Result<bool> { Ok(false) }
    async fn head_drained_search_upload(&self, key: &ObjectKey, owner: rtdb_format::search_manifest::SearchStagingOwner)
        -> rtdb_types::Result<Option<DrainedSearchUpload>> {
        if !self.search_staging_drained(owner).await? { return Ok(None); }
        let body = match self.head_fresh(key).await {
            Ok(object) => DrainedSearchUpload::Present(object),
            Err(Error::MissingObject(_)) => DrainedSearchUpload::Absent,
            Err(error) => return Err(error.into()),
        };
        Ok(Some(body))
    }
    fn requires_search_publication(&self) -> bool { false }
    /// Whether installed search-run retirement uses the exact publication grant.
    fn releases_exact_search_runs(&self) -> bool { false }
    /// Checks storage-side dependencies before attempting to release a retired search run.
    async fn permits_search_release(&self, _key: &ObjectKey, _publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        _cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> { Ok(true) }
    /// Checks that the retirement is recorded before a search tombstone is pruned.
    /// On Classic local storage the caller holds the exclusive recovery gate during this check and durable publication.
    async fn permits_search_tombstone_prune(&self, key: &ObjectKey, publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        _checkpoint: rtdb_types::BlockIndex, cut: rtdb_types::CatalogCut) -> rtdb_types::Result<bool> {
        if self.defers_trie_releases() {
            if self.local_maintenance_store().is_none() || !self.advance_catalog_floor(cut).await? { return Ok(false); }
        }
        self.permits_search_release(key, publication, Some(cut)).await
    }
    async fn release_search_run(&self, key: &ObjectKey, _publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> {
        if self.defers_trie_releases() {
            let Some(cut) = cut else { return Ok(false); };
            if !self.advance_catalog_floor(cut).await? { return Ok(false); }
        }
        self.delete(key).await?;
        Ok(true)
    }
    fn supports_directory_page_release(&self) -> bool { !self.defers_trie_releases() }
    async fn release_entity_directory_page(&self, key: &ObjectKey, _cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> {
        if self.defers_trie_releases() { return Ok(false); }
        self.delete(key).await?;
        Ok(true)
    }
    /// Releases `keys` in order and stops at the first the store refuses, returning how many it released.
    async fn release_entity_directory_pages(&self, keys: &[ObjectKey], cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<usize> {
        for (released, key) in keys.iter().enumerate() {
            if !self.release_entity_directory_page(key, cut).await? { return Ok(released); }
        }
        Ok(keys.len())
    }
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject>;
    /// Reads current metadata for the key from the backing provider.
    /// The default is suitable when `head` already obtains current provider metadata.
    /// Store wrappers must forward this request to the wrapped store's `head_fresh`.
    /// A missing key returns `Error::MissingObject`.
    async fn head_fresh(&self, key: &ObjectKey) -> Result<StoredObject> { self.head(key).await }
    /// A fresh metadata read, with a provider version when the store can observe one.
    async fn observe_head(&self, key: &ObjectKey) -> Result<ObjectObservation<StoredObject>> {
        Ok(ObjectObservation { value: self.head_fresh(key).await?, version: None })
    }
    /// Observes every logical key from storage, sharing evidence for members of one physical publication.
    async fn observe_heads(&self, keys: &[ObjectKey]) -> Result<BTreeMap<ObjectKey, ObjectObservation<StoredObject>>> {
        let mut observed = BTreeMap::new();
        for key in keys { observed.insert(key.clone(), self.observe_head(key).await?); }
        Ok(observed)
    }
    /// Observes the current provider version of a loose object owned by this store.
    /// Missing, inherited, packed or unversioned objects return no evidence.
    async fn retirement_version(&self, _key: &ObjectKey) -> Result<Option<rtdb_types::PhysicalObjectVersion>> {
        Ok(None)
    }
    async fn retirement_publication(&self, key: &ObjectKey) -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        Ok(self.retirement_version(key).await?.map(rtdb_types::PhysicalObjectPublication::Version))
    }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes>;
    /// Reads without creating or trusting a node-local durable cache copy.
    /// Store wrappers must forward this request across any cache boundary.
    async fn get_fresh(&self, key: &ObjectKey) -> Result<Bytes> { self.get(key).await }
    /// Reads an immutable object of the expected size without creating a durable cache entry.
    /// Providers with response metadata can validate the size and read the body in one operation.
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<Bytes> {
        if self.head_fresh(key).await?.size != length as u64 {
            return Err(wrong_size(key, length));
        }
        let bytes = self.get_fresh(key).await?;
        if bytes.len() != length { return Err(wrong_size(key, length)); }
        Ok(bytes)
    }
    /// Reads exactly the expected full object length and returns the physical version from
    /// the response supplying the bytes when the provider can identify that version.
    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<ObjectObservation<Bytes>> {
        Ok(ObjectObservation { value: self.get_fresh_exact(key, length).await?, version: None })
    }
    /// Returns exactly the half-open byte range; an unavailable range is an error.
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes>;
    /// Reads the exact range and carries the physical version from this read, when available.
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<ObjectObservation<Bytes>> {
        Ok(ObjectObservation { value: self.get_range(key, range).await?, version: None })
    }
    /// Whether a scan can read a slice without repeatedly loading the whole object.
    fn supports_scan_ranges(&self, _key: &ObjectKey) -> bool { true }
    /// Whether a codec can read bounded, format-aligned ranges through `observe_range`.
    fn supports_observed_scan_ranges(&self, key: &ObjectKey) -> bool { self.supports_scan_ranges(key) }
    /// Copies one complete immutable response into a private cache file under the supplied memory budget.
    /// Returning `None` leaves the file untouched and selects the existing reserved read path.
    /// Dropping the future stops writing to the destination.
    async fn try_fill_disk_cache(
        &self, _key: &ObjectKey, _destination: &mut tempfile::NamedTempFile, _memory: &Arc<rtdb_types::MemoryBudget>,
    ) -> Result<Option<u64>> {
        Ok(None)
    }
    /// Retains the allocation charge during the read and with the returned bytes.
    /// Implementations that offload work beyond this future must move the reservation into that work.
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        reservation.finish(self.get_range(key, range).await?)
    }
    /// How much storage the pool must admit before reading the object body.
    fn whole_object_allocation(&self, _key: &ObjectKey, length: usize) -> Result<ObjectReadAllocation> {
        Ok(ObjectReadAllocation { retained_bytes: length, scratch_bytes: 0 })
    }
    /// How much storage an exact whole-object read must admit before reading the body.
    fn exact_object_allocation(&self, key: &ObjectKey, length: usize) -> Result<ObjectReadAllocation> {
        self.whole_object_allocation(key, length)
    }
    /// Reads a whole immutable object of the reserved length.
    async fn get_reserved_object(&self, key: &ObjectKey, reservation: ReadReservation) -> Result<RetainedBytes> {
        self.get_reserved_range(key, 0..reservation.length() as u64, reservation).await
    }
    /// Charges any expanded storage window needed for a scan's requested plaintext bytes.
    fn scan_range_allocation(&self, _key: &ObjectKey, length: usize) -> Result<ObjectReadAllocation> {
        Ok(ObjectReadAllocation { retained_bytes: length, scratch_bytes: 0 })
    }
    /// Reads part of an object without filling a whole-object disk cache on a miss.
    async fn get_reserved_scan_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        self.get_reserved_range(key, range, reservation).await
    }
    /// Publishes an immutable object, accepting an identical existing object as a completed retry.
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()>;
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()>;
    /// Publishes from a private file descriptor without requiring a plaintext path.
    async fn put_private_file(&self, _key: &ObjectKey, _source: File, _with_publication: bool)
        -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        Err(Error::Closed("The store has no private file publication".into()))
    }
    /// Returns the identity from the successful write or from the read that verified an identical existing object.
    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.put(key, bytes).await?;
        Ok(None)
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &Path) -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        self.put_file(key, source).await?;
        Ok(None)
    }
    /// Returns the object names reported under a directory, ordered lexicographically.
    /// A listing may observe concurrent writes or deletions while enumerating the names.
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>>;
    async fn delete(&self, key: &ObjectKey) -> Result<()>;
    /// Whether block collection may submit offloaded payloads for deferred reclamation.
    fn queues_offloaded_payload_reclamation(&self) -> bool { false }
    /// Collects an eligible checkpoint or offloaded replica file unless recovery still retains its block.
    async fn collect_recovery_file(&self, key: &ObjectKey) -> rtdb_types::Result<()> {
        self.delete(key).await.map_err(Into::into)
    }
    /// Drops any copy of the object held on this node, leaving the remote object untouched.
    async fn forget(&self, _key: &ObjectKey) -> Result<()> { Ok(()) }
    /// The object as a file on this node's disk, for a reader that maps it rather than holding its bytes; `None` where the store keeps no local file.
    async fn local_file(&self, _key: &ObjectKey) -> Result<Option<LocalFile>> { Ok(None) }
    /// Opens an existing local copy while leaving storage untouched on a cache miss.
    async fn cached_local_file(&self, key: &ObjectKey) -> Result<Option<LocalFile>> { self.local_file(key).await }

    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> {
        self.put(destination, self.get(source).await?).await
    }
}

pub(crate) fn wrong_size(key: &ObjectKey, length: usize) -> Error {
    Error::ImmutableLength { key: key.to_string(), length }
}

/// Returns the unsupported anomaly for a store attachment with no recoverable search staging owner.
pub fn unsupported_search_staging() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::unsupported("rtdb.storage/search-staging-unsupported", "This store has no recoverable search staging owner")
}

#[derive(Default)]
pub struct MemoryStore {
    objects: RwLock<BTreeMap<ObjectKey, Bytes>>,
}

fn checked_range(key: &ObjectKey, range: Range<u64>, length: u64) -> Result<Range<usize>> {
    if range.start > range.end || range.end > length || range.end > usize::MAX as u64 {
        return Err(Error::InvalidRange {
            key: key.to_string(), start: range.start, end: range.end, length,
        });
    }
    Ok(range.start as usize..range.end as usize)
}

#[async_trait]
impl ObjectStore for MemoryStore {
    fn permits_shared_read_cache(&self) -> bool { true }
    async fn search_staging_owner(&self) -> rtdb_types::Result<Option<rtdb_format::search_manifest::SearchStagingOwner>> { Ok(None) }
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let objects = self.objects.read();
        let bytes = objects.get(key).ok_or_else(|| Error::MissingObject(key.to_string()))?;
        Ok(StoredObject { key: key.clone(), size: bytes.len() as u64 })
    }

    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.objects.read().get(key).cloned().ok_or_else(|| Error::MissingObject(key.to_string()))
    }

    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<Bytes> {
        let bytes = self.get(key).await?;
        if bytes.len() != length { return Err(wrong_size(key, length)); }
        Ok(bytes)
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let bytes = self.get(key).await?;
        Ok(bytes.slice(checked_range(key, range, bytes.len() as u64)?))
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> {
        let mut objects = self.objects.write();
        match objects.get(key) {
            Some(existing) if existing != &bytes => Err(Error::ConflictingObject(key.to_string())),
            Some(_) => Ok(()),
            None => { objects.insert(key.clone(), bytes); Ok(()) }
        }
    }

    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()> {
        let bytes = tokio::fs::read(source).await.map_err(|error| Error::io(source, error))?;
        self.put(key, Bytes::from(bytes)).await
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        Ok(self.objects.read().iter()
            .filter(|(key, _)| directory.is_none_or(|directory| key.is_under(directory)))
            .map(|(key, value)| StoredObject { key: key.clone(), size: value.len() as u64 }).collect())
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        self.objects.write().remove(key);
        Ok(())
    }
}

/// An object opened from this node's disk, kept there for as long as the value lives.
pub struct LocalFile { file: std::fs::File, _lease: Option<crate::disk_cache::DiskLease> }

impl LocalFile {
    pub(crate) fn new(file: std::fs::File, lease: Option<crate::disk_cache::DiskLease>) -> Self { Self { file, _lease: lease } }
    pub fn file(&self) -> &std::fs::File { &self.file }
}

pub(crate) fn map_range(file: LocalFile, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
    reservation.check_range(&range)?;
    let length = file.file().metadata().map_err(|error| Error::io(Path::new(key.as_str()), error))?.len();
    let range = checked_range(key, range, length)?;
    if range.is_empty() { return reservation.finish(Bytes::new()); }

    struct MappedRange { map: memmap2::Mmap, _lease: Option<crate::disk_cache::DiskLease> }
    impl AsRef<[u8]> for MappedRange { fn as_ref(&self) -> &[u8] { &self.map } }
    // SAFETY: object files are published atomically and never modified in place. The mapping retains any disk-cache lease.
    let map = unsafe { memmap2::MmapOptions::new().offset(range.start as u64).len(range.len()).map(file.file()) }
        .map_err(|error| Error::io(Path::new(key.as_str()), error))?;
    reservation.finish(Bytes::from_owner(MappedRange { map, _lease: file._lease }))
}

pub struct LocalStore {
    root: Arc<PathBuf>,
    io: Arc<IoRegion>,
    catalog: Option<Arc<crate::local_catalog::LocalCatalogLedger>>,
    mapped: Arc<parking_lot::Mutex<MappedObjects>>,
    apple_sync: AppleSync,
}

/// The mappings of every local store on one root, so that removing an object through any of them releases its mapping.
fn root_mappings(root: &Path) -> Arc<parking_lot::Mutex<MappedObjects>> {
    static ROOTS: std::sync::LazyLock<parking_lot::Mutex<HashMap<PathBuf, std::sync::Weak<parking_lot::Mutex<MappedObjects>>>>> =
        std::sync::LazyLock::new(Default::default);
    let mut roots = ROOTS.lock();
    roots.retain(|_, mapped| mapped.strong_count() > 0);
    if let Some(mapped) = roots.get(root).and_then(std::sync::Weak::upgrade) { return mapped; }
    let mapped = Arc::default();
    roots.insert(root.to_path_buf(), Arc::downgrade(&mapped));
    mapped
}

/// How many local objects stay mapped for scan reads at once.
const MAPPED_OBJECT_LIMIT: usize = 512;

/// Read-only mappings of local objects, shared between scan reads so that a read does not open and map its file again.
/// Every use compares the file's identity with the mapped one, so a removed or replaced object is never read through an older mapping.
/// The mappings are not charged to a memory budget: they are read-only and file-backed, so their resident pages are clean
/// page cache that the kernel reclaims under pressure without writeback and shares with every other reader of the file,
/// whereas a budget bounds the allocations the kernel cannot take back.
/// What a mapping does hold is address space and, once its file is removed, that file's disk blocks, which is what
/// `MAPPED_OBJECT_LIMIT` and the sweep bound.
#[derive(Default)]
struct MappedObjects {
    entries: HashMap<PathBuf, MappedObject>,
    clock: u64,
    swept: Option<std::time::Instant>,
}

/// The least time between two checks of every mapped object's file.
const MAPPED_OBJECT_SWEEP: std::time::Duration = std::time::Duration::from_secs(1);

struct MappedObject {
    map: Arc<memmap2::Mmap>,
    identity: FileIdentity,
    used: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified: Option<std::time::SystemTime>,
}

impl FileIdentity {
    fn of(metadata: &fs::Metadata) -> Self {
        #[cfg(unix)]
        let (device, inode) = { use std::os::unix::fs::MetadataExt; (metadata.dev(), metadata.ino()) };
        #[cfg(not(unix))]
        let (device, inode) = (0, 0);
        Self { device, inode, length: metadata.len(), modified: metadata.modified().ok() }
    }
}

impl MappedObjects {
    fn evict(&mut self, path: &Path) -> Option<MappedObject> { self.entries.remove(path) }

    fn get(&mut self, path: &Path, identity: FileIdentity) -> Option<Arc<memmap2::Mmap>> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(path)?;
        if entry.identity != identity { return None; }
        entry.used = clock;
        Some(Arc::clone(&entry.map))
    }
}

/// A slice of a shared object mapping.
struct MappedSlice {
    map: Arc<memmap2::Mmap>,
    range: Range<usize>,
}

impl AsRef<[u8]> for MappedSlice {
    fn as_ref(&self) -> &[u8] { &self.map[self.range.clone()] }
}

/// The object's current mapping and length; an empty object has no mapping.
fn mapped_object(mapped: &parking_lot::Mutex<MappedObjects>, path: &Path, key: &ObjectKey)
    -> Result<(Option<Arc<memmap2::Mmap>>, u64)> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            let evicted = mapped.lock().entries.remove(path);
            drop(evicted);
            return Err(if error.kind() == std::io::ErrorKind::NotFound { Error::MissingObject(key.to_string()) } else { Error::io(path, error) });
        }
    };
    let identity = FileIdentity::of(&metadata);
    if identity.length == 0 { return Ok((None, 0)); }
    let hit = mapped.lock().get(path, identity);
    sweep_mappings(mapped);
    if let Some(map) = hit { return Ok((Some(map), identity.length)); }

    let file = open_object(path, key)?;
    let opened = FileIdentity::of(&file.metadata().map_err(|error| Error::io(path, error))?);
    if opened.length == 0 { return Ok((None, 0)); }
    // SAFETY: object files are published atomically and never modified in place; a replacement is a new file with its own identity.
    let map = Arc::new(unsafe { memmap2::Mmap::map(&file) }.map_err(|error| Error::io(path, error))?);
    remember_mapping(mapped, path, opened, Arc::clone(&map));
    Ok((Some(map), opened.length))
}

/// Releases, at most once per sweep interval, the mappings whose files were removed or replaced outside this store.
fn sweep_mappings(mapped: &parking_lot::Mutex<MappedObjects>) {
    let held: Vec<(PathBuf, FileIdentity)> = {
        let mut objects = mapped.lock();
        let due = objects.swept.is_none_or(|swept| swept.elapsed() >= MAPPED_OBJECT_SWEEP);
        if due { objects.swept = Some(std::time::Instant::now()); }
        if due { objects.entries.iter().map(|(path, entry)| (path.clone(), entry.identity)).collect() } else { Vec::new() }
    };
    if held.is_empty() { return; }
    let stale: Vec<PathBuf> = held.into_iter()
        .filter(|(path, identity)| fs::metadata(path).map_or(true, |metadata| FileIdentity::of(&metadata) != *identity))
        .map(|(path, _)| path).collect();
    let evicted: Vec<MappedObject> = {
        let mut objects = mapped.lock();
        stale.iter().filter_map(|path| objects.evict(path)).collect()
    };
    drop(evicted);
}

fn remember_mapping(mapped: &parking_lot::Mutex<MappedObjects>, path: &Path, identity: FileIdentity, map: Arc<memmap2::Mmap>) {
    let mut evicted = Vec::new();
    {
        let mut objects = mapped.lock();
        while objects.entries.len() >= MAPPED_OBJECT_LIMIT {
            let Some(oldest) = objects.entries.iter().min_by_key(|(_, entry)| entry.used).map(|(path, _)| path.clone()) else { break; };
            if let Some(entry) = objects.entries.remove(&oldest) { evicted.push(entry); }
        }
        objects.clock += 1;
        let used = objects.clock;
        evicted.extend(objects.entries.insert(path.to_path_buf(), MappedObject { map, identity, used }));
    }
    // Unmapping happens outside the lock.
    drop(evicted);
}

struct AdmittedFile {
    file: File,
    _admission: crate::io_region::IoAdmission,
}

impl LocalStore {
    pub fn root(&self) -> &Path { &self.root }

    pub(crate) fn io_region(&self) -> &Arc<IoRegion> { &self.io }

    /// Settles the prefixes selected by the verified offline repair anchor.
    pub async fn retire_classic_log_prefixes(&self, anchor: crate::LocalRepairAnchor,
        replica: &Path, maximum: usize, vault: Option<&crate::sealing::ClassicKeyVault>,
    ) -> rtdb_types::Result<()> {
        crate::classic_retirement::retire_log_prefixes(self, anchor, replica, maximum, vault).await
    }
    /// Opens a store with an independent I/O region; its owner calls `close_and_wait` before runtime shutdown.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_in(root, IoRegion::new())
    }
    /// Shares a caller-owned I/O region with sibling stores; closing the region closes admission for every sibling.
    pub fn open_in(root: impl AsRef<Path>, io: Arc<IoRegion>) -> Result<Self> {
        let selected = Arc::new(root.as_ref().to_path_buf());
        let _operation = io.admit_read(selected.clone(), ())?;
        let root = selected.as_path();
        fs::create_dir_all(root).map_err(|err| Error::io(root, err))?;
        let root = fs::canonicalize(root).map_err(|err| Error::io(root, err))?;
        let mapped = root_mappings(&root);
        Ok(Self { root: Arc::new(root), io, catalog: None, mapped, apple_sync: AppleSync::Full })
    }
    /// Selects the Apple platform sync policy for this store's durable file and directory flushes.
    pub fn with_apple_sync(mut self, apple_sync: AppleSync) -> Self {
        self.apple_sync = apple_sync;
        if let Some(catalog) = &self.catalog {
            self.catalog = Some(Arc::new(catalog.with_apple_sync(apple_sync)));
        }
        self
    }
    /// The durability policy used for local filesystem syncs on Apple platforms.
    pub fn apple_sync(&self) -> AppleSync { self.apple_sync }
    /// Joins the retirement and erasure ledger on a shared local filesystem.
    pub fn with_catalog_ledger(mut self, replica_epoch: rtdb_types::StorageEpoch) -> Self {
        self.catalog = Some(Arc::new(crate::local_catalog::LocalCatalogLedger::new(self.root.clone(), replica_epoch, self.apple_sync)));
        self
    }
    /// Synchronizes an existing immutable object and its directory entries through the storage root.
    pub async fn sync_existing(&self, key: &ObjectKey) -> Result<()> {
        let apple_sync = self.apple_sync;
        self.io.read(self.root.clone(), key.clone(), move |root, key| {
            let path = root.join(key.as_str());
            let file = open_object(&path, key)?;
            crate::sync_file(&file, apple_sync).map_err(|error| Error::io(&path, error))?;
            sync_parents(&path, root, apple_sync)
        }).await
    }

    /// Retains local recovery media while startup opens its checkpoint and replica cursor.
    pub async fn hold_recovery_open(&self) -> rtdb_types::Result<Arc<dyn Send + Sync>> {
        let ledger = self.catalog.as_ref().ok_or_else(|| rtdb_types::Anomaly::unsupported(
            "rtdb.storage/recovery-hold", "This store has no local catalog ledger"))?.clone();
        Ok(Arc::new(self.io.read(ledger, "recovery-open", |ledger, _| Ok(ledger.hold_recovery_open())).await??))
    }

    /// Acquires exclusive recovery custody, or returns `None` while the gate is held.
    /// The last guard clone releases custody on drop.
    pub async fn try_lock_recovery_deletion(&self) -> rtdb_types::Result<Option<Arc<dyn Send + Sync>>> {
        let ledger = self.catalog.as_ref().ok_or_else(|| rtdb_types::Anomaly::unsupported(
            "rtdb.storage/recovery-hold", "This store has no local catalog ledger"))?.clone();
        let gate = self.io.read(ledger, "recovery-open", |ledger, _| Ok(ledger.try_lock_recovery_deletion())).await??;
        Ok(gate.map(|gate| Arc::new(gate) as Arc<dyn Send + Sync>))
    }

    /// Installs the erasure floor at `cut`; a catalog captured before it is refused from then on.
    pub async fn close_erasure_catalog(&self, cut: rtdb_types::CatalogCut) -> rtdb_types::Result<()> {
        let ledger = self.catalog.clone().ok_or_else(|| rtdb_types::Anomaly::unsupported(
            "rtdb.storage/erasure-catalog", "This store has no local catalog ledger"))?;
        self.io.read(ledger, cut, |ledger, &cut| Ok(ledger.close_erasure_admission(cut))).await?
    }

    pub async fn require_erasure_catalog(&self, cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<()> {
        let ledger = self.catalog.clone().ok_or_else(|| rtdb_types::Anomaly::unsupported(
            "rtdb.storage/erasure-catalog", "This store has no local catalog ledger"))?;
        self.io.read(ledger, cut, |ledger, &cut| Ok(ledger.require_erasure_admission(cut))).await?
    }

    pub async fn verifies_erasure_catalog(&self, cut: rtdb_types::CatalogCut) -> rtdb_types::Result<bool> {
        let ledger = self.catalog.clone().ok_or_else(|| rtdb_types::Anomaly::unsupported(
            "rtdb.storage/erasure-catalog", "This store has no local catalog ledger"))?;
        self.io.read(ledger, cut, |ledger, &cut| Ok(ledger.verifies_erasure_admission(cut))).await?
    }

    /// Persists a verified repair anchor selected at the current checkpoint under exclusive maintenance.
    pub async fn install_repair_anchor(&self, anchor: crate::LocalRepairAnchor) -> rtdb_types::Result<()> {
        let ledger = Arc::new(crate::local_catalog::LocalCatalogLedger::new(self.root.clone(), anchor.boundary.epoch(), self.apple_sync));
        self.io.read(ledger, "repair-anchor", move |ledger, _| Ok(ledger.install_repair_anchor(anchor))).await?
    }

    /// Loads the durable repair anchor at this local storage root.
    pub async fn retained_repair_anchor(&self) -> rtdb_types::Result<Option<crate::LocalRepairAnchor>> {
        let ledger = self.catalog.clone().unwrap_or_else(|| {
            Arc::new(crate::local_catalog::LocalCatalogLedger::new(self.root.clone(), rtdb_types::StorageEpoch(0), self.apple_sync))
        });
        self.io.read(ledger, "repair-anchor", |ledger, _| Ok(ledger.retained_repair_anchor())).await?
    }

    /// Rejects new work and joins this store's I/O region, including cancelled callers' blocking work.
    pub async fn close_and_wait(&self) { self.io.close_and_wait().await; }

    fn path(&self, key: &ObjectKey) -> PathBuf { self.root.join(key.as_str()) }

    /// Copies to an explicit destination store, including a different filesystem root.
    /// Publication is immutable and accepts identical retries; both I/O regions retain admitted work through cancellation.
    /// Missing sources return MissingObject, different destination bytes return ConflictingObject, and either closed region returns Closed.
    /// An I/O or synchronization error can occur after the destination has been published.
    pub async fn copy_to(&self, source: &ObjectKey, destination: &LocalStore, key: &ObjectKey) -> Result<()> {
        let admission = self.io.admit_read(self.root.clone(), source.clone())?;
        let file = open_object(&self.path(source), source)?;
        destination.publish_file(key, AdmittedFile { file, _admission: admission }).await
    }

    async fn publish_file(&self, key: &ObjectKey, input: AdmittedFile) -> Result<()> {
        let (root, path, key) = (Arc::clone(&self.root), self.path(key), key.clone());
        let catalog = self.catalog.clone();
        let apple_sync = self.apple_sync;
        self.io.run(move || {
            let mut input = input;
            let parent = path.parent().ok_or_else(|| Error::InvalidKey(key.to_string()))?;
            fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
            let temp_dir = root.join(".tmp");
            fs::create_dir_all(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            let mut upload = tempfile::NamedTempFile::new_in(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            std::io::copy(&mut input.file, &mut upload).map_err(|error| Error::io(upload.path(), error))?;
            crate::sync_file(upload.as_file(), apple_sync).map_err(|error| Error::io(upload.path(), error))?;
            let _gate = match catalog.as_ref() {
                Some(catalog) => catalog.publication_gate(&key)?,
                None => None,
            };
            match upload.persist_noclobber(&path) {
                Ok(_) => (),
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if !equal_local_files(&path, error.file.path())? { return Err(Error::ConflictingObject(key.to_string())); }
                }
                Err(error) => return Err(Error::io(&path, error.error)),
            }
            sync_publication_parents(&path, &root, apple_sync)
        }).await
    }
}

fn open_object(path: &Path, key: &ObjectKey) -> Result<File> {
    File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound { Error::MissingObject(key.to_string()) }
        else { Error::io(path, error) }
    })
}

fn sync_directory(path: &Path, apple_sync: AppleSync) -> Result<()> {
    crate::sync_directory(path, apple_sync).map_err(|error| Error::io(path, error))
}

fn sync_parents(path: &Path, root: &Path, apple_sync: AppleSync) -> Result<()> {
    let mut current = path.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
    loop {
        sync_directory(current, apple_sync)?;
        if current == root { return Ok(()); }
        current = current.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
    }
}

fn sync_publication_parents(path: &Path, root: &Path, apple_sync: AppleSync) -> Result<()> {
    #[cfg(target_vendor = "apple")]
    if apple_sync == AppleSync::Full && try_sync_publication_volume(path, root)? {
        return Ok(());
    }
    sync_parents(path, root, apple_sync)
}

#[cfg(target_vendor = "apple")]
fn try_sync_publication_volume(path: &Path, root: &Path) -> Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let root_directory = rtdb_types::directory_io::open_for_sync(root).map_err(|error| Error::io(root, error))?;
    let device = root_directory.metadata().map_err(|error| Error::io(root, error))?.dev();
    for parent in path.ancestors().skip(1).take_while(|parent| *parent != root) {
        let directory = rtdb_types::directory_io::open_for_sync(parent).map_err(|error| Error::io(parent, error))?;
        if directory.metadata().map_err(|error| Error::io(parent, error))?.dev() != device {
            return Ok(false);
        }
        crate::sync_file(&directory, AppleSync::Fsync).map_err(|error| Error::io(parent, error))?;
    }
    // F_FULLFSYNC flushes the device cache, including the preceding fsyncs on this device.
    Ok(crate::durability::unsupported_full_sync(&root_directory).map_err(|error| Error::io(root, error))?.is_none())
}

fn equal_local_files(left: &Path, right: &Path) -> Result<bool> {
    let mut left_file = File::open(left).map_err(|error| Error::io(left, error))?;
    let mut right_file = File::open(right).map_err(|error| Error::io(right, error))?;
    let length = left_file.metadata().map_err(|error| Error::io(left, error))?.len();
    if length != right_file.metadata().map_err(|error| Error::io(right, error))?.len() { return Ok(false); }
    let mut left_bytes = [0u8; 64 * 1024];
    let mut right_bytes = [0u8; 64 * 1024];
    let mut remaining = length;
    while remaining != 0 {
        let count = remaining.min(left_bytes.len() as u64) as usize;
        left_file.read_exact(&mut left_bytes[..count]).map_err(|error| Error::io(left, error))?;
        right_file.read_exact(&mut right_bytes[..count]).map_err(|error| Error::io(right, error))?;
        if left_bytes[..count] != right_bytes[..count] { return Ok(false); }
        remaining -= count as u64;
    }
    Ok(true)
}

fn read_local_range(path: &Path, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
    let mut file = open_object(path, key)?;
    let length = file.metadata().map_err(|error| Error::io(path, error))?.len();
    let slice = checked_range(key, range.clone(), length)?;
    file.seek(SeekFrom::Start(range.start)).map_err(|error| Error::io(path, error))?;
    let mut bytes = vec![0; slice.len()];
    file.read_exact(&mut bytes).map_err(|error| Error::io(path, error))?;
    Ok(Bytes::from(bytes))
}

#[async_trait]
impl ObjectStore for LocalStore {
    fn permits_shared_read_cache(&self) -> bool { true }
    fn apple_sync(&self) -> AppleSync { self.apple_sync }
    fn local_maintenance_store(&self) -> Option<&LocalStore> { Some(self) }
    fn syncs_checkpoints(&self) -> bool { true }

    fn defers_trie_releases(&self) -> bool { self.catalog.is_some() }
    fn releases_receipt_l0(&self) -> bool { self.catalog.is_some() }

    async fn advance_catalog_floor(&self, installed: rtdb_types::CatalogCut) -> rtdb_types::Result<bool> {
        if self.catalog.is_none() { return Ok(true); }
        Ok(self.close_classic_trie_admission(installed, &[None]).await?[0])
    }

    async fn close_classic_trie_admission(&self, cut: rtdb_types::CatalogCut, paths: &[Option<&str>]) -> rtdb_types::Result<Vec<bool>> {
        let Some(ledger) = self.catalog.clone() else { return Ok(vec![false; paths.len()]); };
        let paths = paths.iter().map(|path| path.map(str::to_owned)).collect::<Vec<_>>();
        self.io.read(ledger, (cut, paths), |ledger, (cut, paths)| {
            let paths = paths.iter().map(Option::as_deref).collect::<Vec<_>>();
            Ok(ledger.close_admission(*cut, &paths))
        }).await?
    }

    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> rtdb_types::Result<bool> {
        crate::classic_retirement::retire(self, receipt, contents).await
    }

    async fn search_staging_owner(&self) -> rtdb_types::Result<Option<rtdb_format::search_manifest::SearchStagingOwner>> {
        use rtdb_format::search_manifest::SearchStagingOwner;
        let ledger = self.catalog.clone().ok_or_else(unsupported_search_staging)?;
        let attachment = self.io.read(ledger, (), |ledger, _| ledger.search_attachment()).await?;
        Ok(Some(SearchStagingOwner::Local { attachment }))
    }

    async fn search_staging_drained(&self, owner: rtdb_format::search_manifest::SearchStagingOwner)
        -> rtdb_types::Result<bool> {
        let (Some(ledger), rtdb_format::search_manifest::SearchStagingOwner::Local { attachment }) = (self.catalog.clone(), owner)
            else { return Ok(false); };
        Ok(self.io.read(ledger, attachment, |ledger, &attachment| ledger.search_drained(attachment)).await?)
    }

    fn releases_exact_search_runs(&self) -> bool { self.catalog.is_some() }

    async fn release_search_run(&self, key: &ObjectKey, publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> {
        crate::classic_retirement::retire_search_run(self, key, publication, cut, None).await
    }

    fn supports_directory_page_release(&self) -> bool { true }

    async fn release_entity_directory_page(&self, key: &ObjectKey, cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> {
        crate::classic_retirement::retire_search_run(self, key, None, cut, None).await
    }

    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        self.io.read(self.root.clone(), key.clone(), move |root, key| {
            let path = root.join(key.as_str());
            let size = fs::metadata(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound { Error::MissingObject(key.to_string()) } else { Error::io(&path, error) }
            })?.len();
            Ok(StoredObject { key: key.clone(), size })
        }).await
    }

    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.io.read(self.root.clone(), key.clone(), move |root, key| {
            let path = root.join(key.as_str());
            let mut file = open_object(&path, key)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(|error| Error::io(&path, error))?;
            Ok(Bytes::from(bytes))
        }).await
    }

    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<Bytes> {
        self.io.read(self.root.clone(), key.clone(), move |root, key| {
            let path = root.join(key.as_str());
            let mut file = open_object(&path, key)?;
            if file.metadata().map_err(|error| Error::io(&path, error))?.len() != length as u64 {
                return Err(wrong_size(key, length));
            }
            let mut bytes = vec![0; length];
            file.read_exact(&mut bytes).map_err(|error| Error::io(&path, error))?;
            if file.read(&mut [0]).map_err(|error| Error::io(&path, error))? != 0 {
                return Err(wrong_size(key, length));
            }
            Ok(Bytes::from(bytes))
        }).await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        self.io.read(self.root.clone(), (key.clone(), range), |root, (key, range)| {
            read_local_range(&root.join(key.as_str()), key, range.clone())
        }).await
    }

    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        self.io.read(self.root.clone(), (key.clone(), range), move |root, (key, range)| {
            reservation.finish(read_local_range(&root.join(key.as_str()), key, range.clone())?)
        }).await
    }

    async fn get_reserved_scan_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        let mapped = Arc::clone(&self.mapped);
        self.io.read(self.root.clone(), (key.clone(), range), move |root, (key, range)| {
            let (map, length) = mapped_object(&mapped, &root.join(key.as_str()), key)?;
            let range = checked_range(key, range.clone(), length)?;
            match map {
                Some(map) if !range.is_empty() => reservation.finish(Bytes::from_owner(MappedSlice { map, range })),
                _ => reservation.finish(Bytes::new()),
            }
        }).await
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> {
        let (root, path, key) = (self.root.clone(), self.path(key), key.clone());
        let catalog = self.catalog.clone();
        let apple_sync = self.apple_sync;
        self.io.run(move || {
            let parent = path.parent().ok_or_else(|| Error::InvalidKey(key.to_string()))?;
            fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
            let temp_dir = root.join(".tmp");
            fs::create_dir_all(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            let mut upload = tempfile::NamedTempFile::new_in(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            upload.write_all(&bytes).map_err(|error| Error::io(upload.path(), error))?;
            crate::sync_file(upload.as_file(), apple_sync).map_err(|error| Error::io(upload.path(), error))?;
            let _gate = match catalog.as_ref() {
                Some(catalog) => catalog.publication_gate(&key)?,
                None => None,
            };
            match upload.persist_noclobber(&path) {
                Ok(_) => (),
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let existing = fs::read(&path).map_err(|error| Error::io(&path, error))?;
                    if existing.as_slice() != bytes.as_ref() {
                        return Err(Error::ConflictingObject(key.to_string()));
                    }
                }
                Err(error) => return Err(Error::io(&path, error.error)),
            }
            sync_publication_parents(&path, &root, apple_sync)
        }).await
    }

    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()> {
        let admission = self.io.admit_read(self.root.clone(), source.to_path_buf())?;
        let file = File::open(source).map_err(|error| Error::io(source, error))?;
        self.publish_file(key, AdmittedFile { file, _admission: admission }).await
    }

    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> {
        self.copy_to(source, self, destination).await
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        self.io.read(self.root.clone(), directory.cloned(), |root, directory| {
            fn visit(root: &Path, path: &Path, result: &mut Vec<StoredObject>) -> Result<()> {
                let metadata = match fs::symlink_metadata(path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                    Err(error) => return Err(Error::io(path, error)),
                };
                if metadata.is_symlink() {
                    return Err(Error::InvalidKey(path.display().to_string()));
                }
                if metadata.is_file() {
                    let relative = path.strip_prefix(root).map_err(|_| Error::InvalidKey(path.display().to_string()))?;
                    let key = ObjectKey::new(relative.to_str().ok_or_else(|| Error::InvalidKey(relative.display().to_string()))?)?;
                    result.push(StoredObject { key, size: metadata.len() });
                } else if metadata.is_dir() {
                    for entry in fs::read_dir(path).map_err(|error| Error::io(path, error))? {
                        let entry = entry.map_err(|error| Error::io(path, error))?;
                        if entry.file_name() != ".tmp" { visit(root, &entry.path(), result)?; }
                    }
                }
                Ok(())
            }
            let start = directory.as_ref().map_or_else(|| root.clone(), |key| root.join(key.as_str()));
            let mut result = Vec::new();
            visit(root, &start, &mut result)?;
            result.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(result)
        }).await
    }

    /// A local object is never rewritten under its key, and an open file outlives a delete, so the file is the object for as long as it is held.
    async fn local_file(&self, key: &ObjectKey) -> Result<Option<LocalFile>> {
        self.io.read(self.root.clone(), key.clone(), |root, key| {
            Ok(Some(LocalFile::new(open_object(&root.join(key.as_str()), key)?, None)))
        }).await
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        let path = self.path(key);
        let root = self.root.clone();
        let mapped = Arc::clone(&self.mapped);
        let apple_sync = self.apple_sync;
        self.io.run(move || remove_local_object(&root, &path, &mapped, apple_sync)).await
    }

    async fn forget(&self, key: &ObjectKey) -> Result<()> {
        let evicted = self.mapped.lock().evict(&self.path(key));
        drop(evicted);
        Ok(())
    }

    async fn collect_recovery_file(&self, key: &ObjectKey) -> rtdb_types::Result<()> {
        use rtdb_format::keys::{block_index_from_key, oversized_block_index};
        let block = if key.as_str().starts_with("oversized/") {
            oversized_block_index(key.as_str()).ok_or_else(|| Error::InvalidKey(key.to_string()))?
        } else {
            block_index_from_key(key.as_str())?
        };
        let path = self.path(key);
        let root = self.root.clone();
        let apple_sync = self.apple_sync;
        let ledger = Arc::new(crate::local_catalog::LocalCatalogLedger::new(root.clone(), rtdb_types::StorageEpoch(0), apple_sync));
        let mapped = Arc::clone(&self.mapped);
        self.io.read(ledger, ("repair-anchor", key.clone(), block), move |ledger, (_, _, block)| {
            Ok(ledger.collect_recovery_file(*block, || remove_local_object(&root, &path, &mapped, apple_sync)))
        }).await?
    }
}

fn remove_local_object(root: &Path, path: &Path, mapped: &parking_lot::Mutex<MappedObjects>, apple_sync: AppleSync) -> Result<()> {
    // A mapping keeps a removed file's space allocated, so it goes with the file;
    // the second eviction catches a read that mapped the file again before the unlink.
    let evicted = mapped.lock().evict(path);
    drop(evicted);
    let removed = fs::remove_file(path);
    let evicted = mapped.lock().evict(path);
    drop(evicted);
    match removed {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(Error::io(path, error)),
    }
    // A retry can observe an unlink whose parent was not flushed.
    // If a containing directory is also absent, confirm its removal.
    let mut parent = path.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
    loop {
        match crate::sync_directory(parent, apple_sync) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && parent != root => {
                parent = parent.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
            }
            Err(error) => return Err(Error::io(parent, error)),
        }
    }
}

#[cfg(test)]
mod mapping_tests {
    use super::*;

    #[tokio::test]
    async fn local_stores_carry_independent_apple_sync_policies() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let full = LocalStore::open(first_dir.path()).unwrap();
        let fsync = LocalStore::open(second_dir.path()).unwrap().with_apple_sync(AppleSync::Fsync);
        assert_eq!(full.apple_sync(), AppleSync::Full);
        assert_eq!(fsync.apple_sync(), AppleSync::Fsync);
        let key = ObjectKey::new("tables/t/data/entry.arrow").unwrap();
        full.put(&key, Bytes::from_static(b"full")).await.unwrap();
        fsync.put(&key, Bytes::from_static(b"fsync")).await.unwrap();
        assert_eq!(full.get(&key).await.unwrap().as_ref(), b"full");
        assert_eq!(fsync.get(&key).await.unwrap().as_ref(), b"fsync");
        full.close_and_wait().await;
        fsync.close_and_wait().await;
    }

    #[tokio::test]
    async fn removing_an_object_releases_its_mapping() {
        let location = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalStore::open(location.path()).unwrap());
        let pool = crate::cache::BufferPool::new(store.clone(), 1 << 20);
        let (kept, removed) = (ObjectKey::new("tables/t/data/kept.arrow").unwrap(), ObjectKey::new("tables/t/data/removed.arrow").unwrap());
        for key in [&kept, &removed] {
            store.put(key, Bytes::from_static(b"page bytes")).await.unwrap();
            drop(pool.get_scan_range(key, 0..4).await.unwrap());
        }
        assert_eq!(store.mapped.lock().entries.len(), 2);
        store.delete(&removed).await.unwrap();
        assert!(store.mapped.lock().entries.keys().all(|path| path.ends_with("kept.arrow")));
        let sibling = LocalStore::open(location.path()).unwrap();
        sibling.delete(&kept).await.unwrap();
        assert!(store.mapped.lock().entries.is_empty(), "a store on the same root releases the mapping");
    }
}
