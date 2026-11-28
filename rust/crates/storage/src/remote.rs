use crate::{Error, HeldView, IoRegion, ObjectKey, ObjectObservation, ObjectStore, ReadReservation, Result, RetainedBytes, StoredObject};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{Attribute, GetOptions, GetResult, ObjectMeta, ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use rtdb_types::{MemoryBudget, PhysicalObjectPublication, PhysicalObjectVersion, S3ObjectNamespace, TerminalObjectPublication};
use std::{io::Write, ops::Range, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub use crate::{object_store::strong_entity_tag, valid_object_version};

#[path = "remote_catalog.rs"]
mod catalog;
#[path = "remote_read_drain.rs"]
mod read_drain;
#[path = "remote_ranked_admission.rs"]
mod ranked_admission;
pub use ranked_admission::{ClosedRankedAdmission, RankedAdmission, RankedAdmissionPage, RankedAdmissionScope, RankedStagingUpload};
#[cfg(feature = "remote-cache-http")]
pub mod cache_http;

pub const S3_RETIREMENT_ATTRIBUTE: &str = "rtdb-retirement";

// The Reqwest transport uses a 2 MiB HTTP/2 stream window and sub-MiB HTTP/1 read buffers.
const CACHE_DOWNLOAD_BYTES: usize = 3 * 1024 * 1024;

/// A provider may still be copying when its completion can no longer be established.
#[derive(Debug, thiserror::Error)]
#[error("copy outcome is unknown: {0}")]
pub struct CopyOutcomeUnknown(pub &'static str);

fn copy_outcome_unknown(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error.is::<CopyOutcomeUnknown>() { return true; }
        let Some(source) = error.source() else { return false };
        error = source;
    }
}

/// The storage-region owner records an empty provider upload before bytes
/// may be submitted. The identity is retained until completion or abort.
#[async_trait]
pub trait PublicationJournal: Send + Sync {
    async fn allocate_unit(&self, _key: &ObjectKey, _logical: &ObjectKey, _unit: rtdb_types::ServingPublication) -> Result<()> {
        Ok(())
    }
    async fn unit_attempt(&self, _key: &ObjectKey, _body: &[u8]) -> Result<Option<uuid::Uuid>> {
        Ok(None)
    }
    async fn active_unit_upload(&self, _key: &ObjectKey, _attempt: uuid::Uuid) -> Result<Option<String>> {
        Ok(None)
    }
    async fn unit_outcome(&self, _key: &ObjectKey, _attempt: uuid::Uuid, _upload: &str, _published: bool) -> Result<()> {
        Ok(())
    }

    async fn admit(&self, key: &ObjectKey, attempt: uuid::Uuid, upload: &str) -> Result<()>;
    /// A lost admit reply needs cancellation even if no bytes were sent.
    async fn settle(&self, key: &ObjectKey, attempt: uuid::Uuid, upload: &str, submitted: bool) -> Result<()>;
}

/// Converts the provider's relative, percent-encoded path to an object key.
pub fn decode_provider_key(relative: &str) -> Result<ObjectKey> {
    let decoded = relative.split('/')
        .map(|part| percent_encoding::percent_decode_str(part).decode_utf8().map(|part| part.into_owned()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::InvalidKey(relative.into()))?.join("/");
    ObjectKey::new(decoded)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RemoteKeyLayout {
    #[default]
    Escaped,
    LiteralS3,
}

impl RemoteKeyLayout {
    pub fn provider_path(self, key: &str) -> Result<Path> {
        match self {
            Self::Escaped => Ok(Path::from(key)),
            Self::LiteralS3 if key.len() <= 1024 =>
                Path::parse(key).map_err(|_| Error::InvalidKey(key.to_owned())),
            Self::LiteralS3 => Err(Error::InvalidKey(key.to_owned())),
        }
    }

    fn logical_key(self, relative: &str) -> Result<ObjectKey> {
        match self {
            Self::Escaped => decode_provider_key(relative),
            Self::LiteralS3 => ObjectKey::new(relative),
        }
    }
}

/// The result of this specific conditional publication.
#[derive(Debug, PartialEq, Eq)]
pub enum PublicationResult {
    Published { version: Option<String>, etag: Option<String> },
    Conflict,
}

/// A provider with a durable, independently abortable upload capability.
#[async_trait]
pub trait PublicationUploads: Send + Sync {
    /// Confirms that completed uploads will have immutable provider version IDs.
    async fn require_versioned(&self) -> Result<()> {
        Err(Error::Closed("The publication provider does not confirm immutable versions".into()))
    }
    /// Reserves an empty upload for a provider-encoded key.
    async fn reserve(&self, path: &Path, attempt: uuid::Uuid) -> Result<String>;
    /// Makes the upload visible only if no object already holds that key.
    async fn complete(&self, path: &Path, upload: &str, bytes: Bytes) -> Result<PublicationResult>;
    /// Cancels this upload and confirms that it retains no parts.
    async fn abort(&self, path: &Path, upload: &str) -> Result<()>;
    /// Resolves an exact attempt's completed version after the caller drains its upload.
    /// `None` confirms absence. An indeterminate outcome returns an error.
    async fn completed_version(&self, _path: &Path, _attempt: uuid::Uuid) -> Result<Option<String>> {
        Err(Error::Closed("The publication provider does not identify completed attempts".into()))
    }
}

/// Provider inventory used to settle exactly the Classic publication selected by a grant.
#[async_trait]
pub trait ClassicVersionRetirement: Send + Sync {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool>;
    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()>;
    async fn require_never_versioned(&self, key: &ObjectKey) -> Result<()>;
    async fn settle_staging_upload(&self, upload: uuid::Uuid, version: Option<&str>) -> Result<()>;
}

#[derive(Clone)]
pub struct RemoteStore {
    objects: Arc<RemoteObjects>,
    cache_downloads: Option<Arc<dyn ProviderStore>>,
    io: Arc<IoRegion>,
    cell: bool,
    version_namespace: Option<Arc<S3ObjectNamespace>>,
    terminal_allocation: Option<uuid::Uuid>,
    catalog: Option<Arc<catalog::CatalogLedger>>,
    classic_versions: Option<Arc<dyn ClassicVersionRetirement>>,
}

impl RemoteStore {
    /// Opens an independent I/O region; the owner calls `close_and_wait` before runtime shutdown.
    /// The prefix scopes keys to one configured database partition and storage generation.
    pub fn new(client: Arc<dyn ProviderStore>, prefix: Option<ObjectKey>) -> Self {
        Self::new_in(client, prefix, IoRegion::new())
    }

    /// Uses the owner's I/O region, which must be closed and joined after all store consumers finish.
    /// Every admitted operation remains owned through caller cancellation, including publication cleanup.
    pub fn new_in(client: Arc<dyn ProviderStore>, prefix: Option<ObjectKey>, io: Arc<IoRegion>) -> Self {
        Self { objects: Arc::new(RemoteObjects { client, prefix, key_layout: RemoteKeyLayout::Escaped, uploads: None, hide_tombstones: false, read_drain: None }), cache_downloads: None, io, cell: false, version_namespace: None, terminal_allocation: None, catalog: None, classic_versions: None }
    }

    pub fn new_classic_s3(client: Arc<dyn ProviderStore>, prefix: ObjectKey, io: Arc<IoRegion>, key_layout: RemoteKeyLayout) -> Self {
        let mut store = Self::new_in(client, Some(prefix), io);
        Arc::make_mut(&mut store.objects).key_layout = key_layout;
        store
    }

    /// Cell storage publishes a final object with one conditional provider
    /// write for untracked cell operations.
    pub fn new_single_put(client: Arc<dyn ProviderStore>, prefix: ObjectKey, io: Arc<IoRegion>) -> Self {
        Self { objects: Arc::new(RemoteObjects { client, prefix: Some(prefix), key_layout: RemoteKeyLayout::Escaped, uploads: None, hide_tombstones: true, read_drain: None }), cache_downloads: None, io, cell: true, version_namespace: None, terminal_allocation: None, catalog: None, classic_versions: None }
    }

    /// Opens a cell store with abortable, journaled publication.
    pub fn new_managed(
        client: Arc<dyn ProviderStore>,
        prefix: ObjectKey,
        io: Arc<IoRegion>,
        uploads: Arc<dyn PublicationUploads>,
    ) -> Self {
        let objects = Arc::new(RemoteObjects { client, prefix: Some(prefix), key_layout: RemoteKeyLayout::Escaped, uploads: Some(uploads), hide_tombstones: true, read_drain: None });
        Self { objects, cache_downloads: None, io, cell: true, version_namespace: None, terminal_allocation: None, catalog: None, classic_versions: None }
    }

    /// Configure the cache GET client with the same namespace and authorization as the ordinary client.
    /// It must deliver one response without resuming and bound each in-flight body by `CACHE_DOWNLOAD_BYTES`.
    pub fn with_cache_downloads(mut self, client: Arc<dyn ProviderStore>) -> Self {
        self.cache_downloads = Some(client);
        self
    }

    pub fn with_catalog_ledger(mut self) -> Result<Self> {
        let key = ObjectKey::new("catalog-ledger")?;
        let path = self.objects.path_for(&format!(".tmp/{key}"))?;
        self.catalog = Some(catalog::CatalogLedger::new(Arc::clone(&self.objects.client), key, path));
        Ok(self)
    }

    pub fn with_read_drain(mut self, scope: fn(&ObjectKey) -> Option<&str>) -> Self {
        let client = self.objects.client.clone();
        Arc::make_mut(&mut self.objects).read_drain = Some(ReadDrainOwner { client, scope });
        self
    }

    pub fn inherit_read_drain(mut self, owner: &Self) -> Result<Self> {
        if let Some(drain) = &owner.objects.read_drain {
            if self.version_namespace != owner.version_namespace || self.objects.prefix != owner.objects.prefix {
                return Err(Error::Closed("The provider read census belongs to a different storage namespace".into()));
            }
            Arc::make_mut(&mut self.objects).read_drain = Some(drain.clone());
        }
        Ok(self)
    }

    /// Close point-read admission and observe the durable census for this key.
    pub async fn close_read_admission(&self, key: &ObjectKey) -> Result<bool> {
        let Some(drain) = self.objects.read_drain(key) else { return Ok(false); };
        drain.close_and_observe().await
    }

    /// Enables version observations for a store backed by the specified S3 namespace.
    pub fn with_s3_versioning(mut self, namespace: S3ObjectNamespace) -> Self {
        self.version_namespace = Some(Arc::new(namespace));
        self
    }

    pub fn with_classic_versions(mut self, inventory: Arc<dyn ClassicVersionRetirement>) -> Self {
        self.classic_versions = Some(inventory);
        self
    }

    pub fn s3_namespace(&self) -> Option<&S3ObjectNamespace> { self.version_namespace.as_deref() }

    pub fn s3_object_key(&self, key: &ObjectKey) -> Option<String> {
        self.s3_namespace()?;
        Some(self.objects.path(key).ok()?.to_string())
    }

    pub fn with_terminal_allocation(mut self, allocation: uuid::Uuid) -> Self {
        self.terminal_allocation = Some(allocation);
        self
    }

    /// Rejects new requests and waits for admitted operations, including requests whose callers were cancelled.
    /// This closes the complete shared region when the store was opened with `new_in`.
    pub async fn close_and_wait(&self) { self.io.close_and_wait().await; }

    /// Copies between explicit roots backed by the same provider client.
    /// Distinct client instances are rejected before I/O, even when they name the same bucket.
    /// The provider must support conditional copy; existing destinations are accepted only when their bytes match.
    /// Missing sources return MissingObject, different destination bytes return ConflictingObject, and a closed region returns Closed.
    /// Provider authorization failures return PermissionDenied; a panicking provider task returns Task.
    /// Incompatible clients, unsupported copy operations and provider failures return Remote; a failed request may have published the destination.
    /// Both regions retain admitted work until provider completion, even if the caller cancels.
    pub async fn copy_to(&self, source: &ObjectKey, destination: &RemoteStore, key: &ObjectKey) -> Result<()> {
        if !Arc::ptr_eq(&self.objects.client, &destination.objects.client) {
            return Err(Error::Remote { key: key.to_string(), message: "object copy requires a shared provider client".into() });
        }
        let admission = self.io.admit_read(self.objects.clone(), source.clone())?
            .and(destination.io.admit()?);
        let (objects, destination_objects, source, key) = (Arc::clone(&self.objects), Arc::clone(&destination.objects), source.clone(), key.clone());
        if destination.cell {
            let io = destination.io.clone();
            return IoRegion::run_async_admitted(admission, async move {
                let bytes = objects.get(&source).await?;
                destination_objects.put(&key, bytes, Some(&io)).await.map(drop)
            }).await;
        }
        IoRegion::run_async_admitted(admission, async move { objects.copy_to(&source, &destination_objects, &key).await }).await
    }
}

#[async_trait]
impl ObjectStore for RemoteStore {
    fn s3_publication_address(&self, key: &ObjectKey) -> Option<(S3ObjectNamespace, String)> {
        Some((self.s3_namespace()?.clone(), self.objects.path(key).ok()?.to_string()))
    }

    fn permits_shared_read_cache(&self) -> bool { true }
    fn defers_trie_releases(&self) -> bool { self.catalog.is_some() }

    async fn advance_catalog_floor(&self, cut: rtdb_types::CatalogCut) -> rtdb_types::Result<bool> {
        let Some(ledger) = self.catalog.clone() else { return Ok(true); };
        self.io.run_async(async move { Ok(ledger.advance(cut).await) }).await?
    }

    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> rtdb_types::Result<bool> {
        crate::classic_retirement::retire(self, receipt, contents).await
    }

    fn requires_search_publication(&self) -> bool { self.releases_exact_search_runs() }
    fn releases_exact_search_runs(&self) -> bool { self.catalog.is_some() && self.classic_versions.is_some() }

    async fn release_search_run(&self, key: &ObjectKey, publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> {
        if rtdb_format::search_manifest::is_ranged_run(key.as_str()) && self.releases_exact_search_runs() {
            return crate::classic_retirement::retire_search_run(self, key, publication, cut, None).await;
        }
        if self.catalog.is_some() {
            let Some(cut) = cut else { return Ok(false); };
            if !self.advance_catalog_floor(cut).await? { return Ok(false); }
        }
        self.delete(key).await?;
        Ok(true)
    }

    async fn close_classic_trie_admission(&self, cut: rtdb_types::CatalogCut, paths: &[Option<&str>]) -> rtdb_types::Result<Vec<bool>> {
        let Some(ledger) = self.catalog.clone().filter(|_| self.classic_versions.is_some()) else {
            return Ok(vec![self.advance_catalog_floor(cut).await?; paths.len()]);
        };
        let paths = paths.iter().map(|path| path.map(str::to_owned)).collect::<Vec<_>>();
        self.io.run_async(async move {
            let paths = paths.iter().map(Option::as_deref).collect::<Vec<_>>();
            Ok(ledger.close_admission(cut, &paths).await)
        }).await?
    }

    async fn classic_trie_retirement_ready(&self, key: &ObjectKey, version: Option<&PhysicalObjectVersion>) -> rtdb_types::Result<bool> {
        let Some(inventory) = self.classic_versions.clone() else { return Ok(version.is_none()); };
        // Key authority in a sealed Classic root is scoped to the logical
        // path. Refuse to withdraw that slot if another version is visible.
        if let Some(version) = version {
            match self.observe_head(key).await {
                Ok(current) if current.version.as_ref() == Some(version) => (),
                Ok(_) => return Ok(false),
                Err(Error::MissingObject(_)) => (),
                Err(error) => return Err(error.into()),
            }
        }
        let (key, version) = (key.clone(), version.cloned());
        Ok(self.io.run_async(async move {
            match version {
                Some(version) => inventory.available(&key, &version).await,
                None => inventory.require_never_versioned(&key).await.map(|()| true),
            }
        }).await?)
    }

    async fn settle_classic_trie(&self, key: &ObjectKey, version: Option<&PhysicalObjectVersion>) -> rtdb_types::Result<()> {
        let Some(inventory) = self.classic_versions.clone() else {
            if version.is_some() {
                return Err(rtdb_types::Anomaly::unsupported("rtdb.storage/trie-release", "The provider cannot settle an exact object version"));
            }
            return self.delete(key).await.map_err(Into::into);
        };
        let (objects, key, version) = (Arc::clone(&self.objects), key.clone(), version.cloned());
        Ok(self.io.run_async(async move {
            if let Some(version) = version {
                return inventory.erase(&key, &version).await;
            }
            inventory.require_never_versioned(&key).await?;
            objects.delete(&key).await?;
            match objects.head(&key).await {
                Err(Error::MissingObject(_)) => Ok(()),
                Err(error) => Err(error),
                Ok(_) => Err(Error::ConflictingObject(key.to_string())),
            }
        }).await?)
    }

    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        self.io.read_async(self.objects.clone(), key.clone(), move |objects, key| async move { objects.head(&key).await }).await
    }
    async fn observe_head(&self, key: &ObjectKey) -> Result<ObjectObservation<StoredObject>> {
        let namespace = self.version_namespace.clone();
        self.io.read_async(self.objects.clone(), key.clone(), move |objects, key| async move {
            let path = objects.path(&key)?;
            let metadata = objects.metadata(&key).await?;
            Ok(ObjectObservation {
                version: physical_version(namespace.as_deref(), &path, metadata.version),
                value: StoredObject { key, size: metadata.size },
            })
        }).await
    }
    async fn retirement_version(&self, key: &ObjectKey) -> Result<Option<PhysicalObjectVersion>> {
        let Some(namespace) = self.version_namespace.clone() else { return Ok(None); };
        self.io.read_async(self.objects.clone(), key.clone(), move |objects, key| async move {
            let path = objects.path(&key)?;
            let metadata = match objects.metadata(&key).await {
                Ok(metadata) => metadata,
                Err(Error::MissingObject(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            Ok(physical_version(Some(namespace.as_ref()), &path, metadata.version))
        }).await
    }
    async fn retirement_publication(&self, key: &ObjectKey) -> Result<Option<PhysicalObjectPublication>> {
        let Some(allocation) = self.terminal_allocation else {
            return Ok(self.retirement_version(key).await?.map(PhysicalObjectPublication::Version));
        };
        let Some(namespace) = self.version_namespace.clone() else { return Ok(None); };
        self.io.read_async(self.objects.clone(), key.clone(), move |objects, key| async move {
            let metadata = match objects.metadata(&key).await {
                Ok(metadata) => metadata,
                Err(Error::MissingObject(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            Ok(observed_terminal(&namespace, &objects.path(&key)?, allocation, metadata.version.as_deref(), metadata.e_tag))
        }).await
    }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.io.read_async(self.objects.clone(), key.clone(), move |objects, key| async move { objects.get(&key).await }).await
    }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<Bytes> {
        Ok(self.observe_fresh_exact(key, length).await?.value)
    }
    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<ObjectObservation<Bytes>> {
        let namespace = self.version_namespace.clone();
        self.io.read_async(self.objects.clone(), (key.clone(), length), move |objects, (key, length)| async move { objects.observe_exact(&key, length, namespace.as_deref()).await }).await
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        self.io.read_async(self.objects.clone(), (key.clone(), range), |objects, (key, range)| async move { objects.get_range(&key, range).await }).await
    }
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<ObjectObservation<Bytes>> {
        let Some(namespace) = self.version_namespace.clone() else {
            return Ok(ObjectObservation { value: self.get_range(key, range).await?, version: None });
        };
        if range.start >= range.end {
            let observed = self.observe_head(key).await?;
            if range.start == range.end && range.end <= observed.value.size {
                return Ok(ObjectObservation { value: Bytes::new(), version: observed.version });
            }
            return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length: observed.value.size });
        }
        self.io.read_async(self.objects.clone(), (key.clone(), range), move |objects, (key, range)| async move {
            let path = objects.path(&key)?;
            let length = usize::try_from(range.end - range.start)
                .map_err(|_| Error::Task("The reserved provider range exceeds addressable memory".into()))?;
            let mut check_tombstone = false;
            let response = objects.with_read(&key, async {
                let response = objects.read(&key, GetOptions::new().with_range(Some(range.clone()))).await
                    .inspect_err(|error| check_tombstone = objects.hide_tombstones && !matches!(error, Error::MissingObject(_)))?;
                if response.range != range || response.meta.size < range.end {
                    return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length: response.meta.size });
                }
                let version = physical_version(Some(&namespace), &path, response.meta.version.clone());
                let value = collect_exact_body(response, &key, length, || Error::Remote {
                    key: key.to_string(), message: "provider returned a different byte range length".into(),
                }).await?;
                Ok(ObjectObservation { value, version })
            }).await;
            if check_tombstone && matches!(objects.metadata(&key).await, Err(Error::MissingObject(_))) {
                return Err(Error::MissingObject(key.to_string()));
            }
            response
        }).await
    }
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        self.io.read_async(self.objects.clone(), (key.clone(), range), move |objects, (key, range)| async move { reservation.finish(objects.get_range(&key, range).await?) }).await
    }
    async fn try_fill_disk_cache(
        &self, key: &ObjectKey, destination: &mut tempfile::NamedTempFile, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<u64>> {
        let Some(client) = &self.cache_downloads else { return Ok(None); };
        if self.cell || self.objects.read_drain.is_some() { return Ok(None); }
        let Ok(charge) = memory.reserve(CACHE_DOWNLOAD_BYTES) else { return Ok(None); };
        let selection = Arc::new((Arc::clone(client), self.objects.path(key)?));
        let view = HeldView::recovery(selection.clone());
        let lease = IoRegion::current_read_lease();
        let result = IoRegion::reading(view, async {
            let _admission = self.io.admit()?;
            let response = selection.0.get_opts(&selection.1, GetOptions::default())
                .await.map_err(|error| provider_error(key, error))?;
            let size = response.meta.size;
            let wrong_size = || Error::Remote { key: key.to_string(), message: "provider returned an incomplete or oversized cache body".into() };
            if response.range != (0..size) { return Err(wrong_size()); }
            let mut body = response.into_stream();
            let mut remaining = size;
            while let Some(chunk) = body.try_next().await.map_err(|error| provider_error(key, error))? {
                if chunk.len() > charge.bytes() {
                    return Err(Error::Remote { key: key.to_string(), message: "cache download chunk exceeded the reserved transport window".into() });
                }
                remaining = remaining.checked_sub(chunk.len() as u64).ok_or_else(wrong_size)?;
                destination.write_all(&chunk).map_err(|error| Error::io(destination.path(), error))?;
                drop(chunk);
                tokio::task::consume_budget().await;
            }
            if remaining != 0 { return Err(wrong_size()); }
            Ok(Some(size))
        }).await;
        match lease { Some(lease) => lease.classify(result), None => result }
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> {
        self.put_with_publication(key, bytes).await.map(drop)
    }
    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> Result<Option<PhysicalObjectPublication>> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        let io = self.io.clone();
        let cell = self.cell;
        let namespace = self.version_namespace.clone();
        let terminal_allocation = self.terminal_allocation;
        self.io.run_async(async move {
            let published = objects.put(&key, bytes, cell.then_some(io.as_ref())).await?;
            let path = objects.path(&key)?;
            let Some(namespace) = namespace.as_deref() else { return Ok(None); };
            if let Some(allocation) = terminal_allocation {
                if published.version.as_deref().is_some_and(|version| version != "null") {
                    return Err(Error::Remote { key: key.to_string(), message: "A terminal publication returned a provider version".into() });
                }
                let etag = published.etag.filter(|etag| strong_entity_tag(etag)).ok_or_else(|| Error::Remote {
                    key: key.to_string(), message: "A terminal publication requires a strong entity tag".into(),
                })?;
                return Ok(Some(PhysicalObjectPublication::Terminal(TerminalObjectPublication {
                    namespace: namespace.clone(), key: path.to_string(), allocation, etag,
                })));
            }
            Ok(physical_version(Some(namespace), &path, published.version).map(PhysicalObjectPublication::Version))
        }).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &std::path::Path) -> Result<()> {
        self.put_file_with_publication(key, source).await.map(drop)
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &std::path::Path) -> Result<Option<PhysicalObjectPublication>> {
        if self.cell {
            return Err(Error::Remote { key: key.to_string(), message: "Cell writes must supply the final sealed bytes".into() });
        }
        let admission = self.io.admit()?;
        let input = std::fs::File::open(source).map_err(|error| Error::io(source, error))?;
        let (objects, key, source, namespace) = (Arc::clone(&self.objects), key.clone(), source.to_owned(), self.version_namespace.clone());
        let inventory = self.classic_versions.clone();
        IoRegion::run_async_admitted(admission, async move {
            let publication = objects.put_opened_file(
                &key, tokio::fs::File::from_std(input), &source, inventory.as_deref(), namespace.is_some(),
            ).await?;
            Ok(physical_version(namespace.as_deref(), &objects.path(&key)?, publication.and_then(|object| object.version))
                .map(PhysicalObjectPublication::Version))
        }).await
    }
    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> { self.copy_to(source, self, destination).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        self.io.read_async(self.objects.clone(), directory.cloned(), |objects, directory| async move { objects.list(directory.as_ref()).await }).await
    }
    async fn reclaim_unreachable_checkpoint(&self, key: &ObjectKey) -> Result<bool> {
        if self.cell || !self.close_read_admission(key).await? {
            return Ok(false);
        }
        self.delete(key).await?;
        Ok(true)
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        if self.cell {
            return Err(Error::Remote { key: key.to_string(), message: "Cell storage requires provider retirement authority".into() });
        }
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { objects.delete(&key).await }).await
    }
}

#[derive(Clone)]
struct RemoteObjects {
    client: Arc<dyn ProviderStore>,
    prefix: Option<ObjectKey>,
    key_layout: RemoteKeyLayout,
    uploads: Option<Arc<dyn PublicationUploads>>,
    hide_tombstones: bool,
    read_drain: Option<ReadDrainOwner>,
}

#[derive(Clone)]
struct ReadDrainOwner {
    client: Arc<dyn ProviderStore>,
    scope: fn(&ObjectKey) -> Option<&str>,
}

impl RemoteObjects {
    async fn copy_to(&self, source: &ObjectKey, destination: &RemoteObjects, key: &ObjectKey) -> Result<()> {
        match self.client.copy_if_not_exists(&self.path(source)?, &destination.path(key)?).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                let length = self.head(source).await?.size;
                if destination.head(key).await?.size != length { return Err(Error::ConflictingObject(key.to_string())); }
                let mut offset = 0;
                while offset < length {
                    let end = offset.saturating_add(1024 * 1024).min(length);
                    let source_bytes = self.get_range(source, offset..end).await?;
                    let destination_bytes = destination.get_range(key, offset..end).await?;
                    if source_bytes != destination_bytes { return Err(Error::ConflictingObject(key.to_string())); }
                    offset = end;
                }
                Ok(())
            }
            Err(error @ object_store::Error::NotFound { .. }) => Err(provider_error(source, error)),
            Err(error) => Err(provider_error(key, error)),
        }
    }

    fn path(&self, key: &ObjectKey) -> Result<Path> {
        self.path_for(key.as_str())
    }

    fn path_for(&self, relative: &str) -> Result<Path> {
        match &self.prefix {
            Some(prefix) => self.key_layout.provider_path(&format!("{prefix}/{relative}")),
            None => self.key_layout.provider_path(relative),
        }
    }

    fn read_drain<'a>(&'a self, key: &'a ObjectKey) -> Option<read_drain::ReadDrain<'a>> {
        let owner = self.read_drain.as_ref()?;
        let scope = (owner.scope)(key)?;
        let control = format!("{scope}.lifecycle/.tmp/installed-read-drain");
        let root = Path::from(match &self.prefix {
            Some(prefix) => format!("{prefix}/{control}"),
            None => control,
        });
        Some(read_drain::ReadDrain::new(owner.client.as_ref(), root, key))
    }

    async fn with_read<T>(&self, key: &ObjectKey, work: impl Future<Output = Result<T>>) -> Result<T> {
        match self.read_drain(key) {
            Some(drain) => drain.read(work).await,
            None => work.await,
        }
    }

    async fn compare_file(&self, key: &ObjectKey, file: &mut tokio::fs::File, source: &std::path::Path,
        length: u64) -> Result<ProviderPublication> {
        self.with_read(key, async {
            let conflict = || Error::ConflictingObject(key.to_string());
            let object = self.read(key, GetOptions::default()).await?;
            if object.meta.size != length || object.range != (0..length) { return Err(conflict()); }
            let publication = ProviderPublication { version: object.meta.version.clone(), etag: object.meta.e_tag.clone() };
            file.rewind().await.map_err(|error| Error::io(source, error))?;
            let mut stream = object.into_stream();
            let mut remaining = length;
            let mut buffer = vec![0; length.clamp(1, 1024 * 1024) as usize];
            while let Some(bytes) = stream.try_next().await.map_err(|error| provider_error(key, error))? {
                if bytes.len() as u64 > remaining { return Err(conflict()); }
                for chunk in bytes.chunks(buffer.len()) {
                    file.read_exact(&mut buffer[..chunk.len()]).await.map_err(|error| Error::io(source, error))?;
                    if chunk != &buffer[..chunk.len()] { return Err(conflict()); }
                }
                remaining -= bytes.len() as u64;
            }
            if remaining != 0 { return Err(conflict()); }
            Ok(publication)
        }).await
    }
}

fn observed_terminal(namespace: &S3ObjectNamespace, path: &Path, allocation: uuid::Uuid,
    version: Option<&str>, etag: Option<String>) -> Option<PhysicalObjectPublication> {
    if version.is_some_and(|version| version != "null") { return None; }
    let etag = etag.filter(|etag| strong_entity_tag(etag))?;
    Some(PhysicalObjectPublication::Terminal(TerminalObjectPublication {
        namespace: namespace.clone(), key: path.to_string(), allocation, etag,
    }))
}

fn physical_version(namespace: Option<&S3ObjectNamespace>, path: &Path, version: Option<String>) -> Option<PhysicalObjectVersion> {
    let namespace = namespace?;
    let version = version.filter(|version| valid_object_version(version.as_bytes()))?;
    Some(PhysicalObjectVersion { namespace: namespace.clone(), key: path.to_string(), version })
}

fn publication_attribute() -> Attribute { Attribute::Metadata("rtdb-publication".into()) }

struct ProviderPublication {
    version: Option<String>,
    etag: Option<String>,
}

fn provider_error(key: &ObjectKey, error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::MissingObject(key.to_string()),
        error @ object_store::Error::PermissionDenied { .. } => Error::PermissionDenied { key: key.to_string(), message: error.to_string() },
        error => Error::Remote { key: key.to_string(), message: error.to_string() },
    }
}

/// Reject an oversized response before copying it into the reserved body buffer.
async fn collect_exact_body(object: GetResult, key: &ObjectKey, length: usize, wrong_size: impl Fn() -> Error) -> Result<Bytes> {
    let mut stream = object.into_stream();
    let first = stream.try_next().await.map_err(|error| provider_error(key, error))?.unwrap_or_default();
    if first.len() > length { return Err(wrong_size()); }
    if first.len() == length {
        while let Some(chunk) = stream.try_next().await.map_err(|error| provider_error(key, error))? {
            if !chunk.is_empty() { return Err(wrong_size()); }
        }
        return Ok(first);
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&first);
    drop(first);
    while let Some(chunk) = stream.try_next().await.map_err(|error| provider_error(key, error))? {
        if chunk.len() > length - bytes.len() { return Err(wrong_size()); }
        bytes.extend_from_slice(&chunk);
    }
    if bytes.len() != length { return Err(wrong_size()); }
    Ok(bytes.into())
}

impl RemoteObjects {
    async fn read(&self, key: &ObjectKey, options: GetOptions) -> Result<GetResult> {
        let response = self.client.get_opts(&self.path(key)?, options).await.map_err(|error| provider_error(key, error))?;
        if self.is_tombstone(&response) {
            return Err(Error::MissingObject(key.to_string()));
        }
        Ok(response)
    }

    fn is_tombstone(&self, response: &GetResult) -> bool {
        self.hide_tombstones && response.meta.size == 0
            && response.attributes.get(&Attribute::Metadata(S3_RETIREMENT_ATTRIBUTE.into()))
                .and_then(|value| uuid::Uuid::parse_str(value.as_ref()).ok()).is_some_and(|token| !token.is_nil())
    }

    async fn metadata(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        self.with_read(key, self.read_metadata(key)).await
    }

    async fn read_metadata(&self, key: &ObjectKey) -> Result<ObjectMeta> {
        if self.hide_tombstones {
            return Ok(self.read(key, GetOptions { head: true, ..GetOptions::default() }).await?.meta);
        }
        self.client.head(&self.path(key)?).await.map_err(|error| provider_error(key, error))
    }

    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let metadata = self.metadata(key).await?;
        Ok(StoredObject { key: key.clone(), size: metadata.size })
    }

    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.with_read(key, async {
            self.read(key, GetOptions::default()).await?.bytes().await.map_err(|error| provider_error(key, error))
        }).await
    }

    async fn observe_exact(&self, key: &ObjectKey, length: usize, namespace: Option<&S3ObjectNamespace>) -> Result<ObjectObservation<Bytes>> {
        self.with_read(key, self.read_exact(key, length, namespace)).await
    }

    async fn read_exact(&self, key: &ObjectKey, length: usize, namespace: Option<&S3ObjectNamespace>) -> Result<ObjectObservation<Bytes>> {
        let path = self.path(key)?;
        let object = self.read(key, GetOptions::default()).await?;
        let wrong_size = || crate::object_store::wrong_size(key, length);
        if object.meta.size != length as u64 || object.range != (0..length as u64) {
            return Err(wrong_size());
        }
        let version = physical_version(namespace, &path, object.meta.version.clone());
        let value = collect_exact_body(object, key, length, wrong_size).await?;
        Ok(ObjectObservation { value, version })
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let length = self.head(key).await?.size;
        if range.start > range.end || range.end > length {
            return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length });
        }
        if range.is_empty() { return Ok(Bytes::new()); }
        self.with_read(key, self.read_range(key, range)).await
    }

    async fn read_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let bytes = self.client.get_range(&self.path(key)?, range.clone()).await.map_err(|error| provider_error(key, error))?;
        if bytes.len() as u64 != range.end - range.start {
            return Err(Error::Remote { key: key.to_string(), message: "provider returned an incomplete byte range".into() });
        }
        Ok(bytes)
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes, authority: Option<&IoRegion>) -> Result<ProviderPublication> {
        if let Some(journal) = authority.and_then(IoRegion::publication_journal) {
            return self.put_journaled(key, bytes, journal).await;
        }
        let options = PutOptions { mode: PutMode::Create, ..Default::default() };
        match self.client.put_opts(&self.path(key)?, bytes.clone().into(), options).await {
            Ok(written) => Ok(ProviderPublication { version: written.version, etag: written.e_tag }),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                self.compare_existing(key, bytes).await
            }
            Err(error) => Err(provider_error(key, error)),
        }
    }

    async fn put_journaled(&self, key: &ObjectKey, bytes: Bytes, journal: &dyn PublicationJournal) -> Result<ProviderPublication> {
        let uploads = self.uploads.as_ref().ok_or_else(|| Error::Remote {
            key: key.to_string(), message: "The cell store has no abortable publication capability".into(),
        })?;
        let unit = journal.unit_attempt(key, &bytes).await?;
        let attempt = unit.unwrap_or_else(uuid::Uuid::new_v4);
        let path = self.path(key)?;
        if let Some(unit) = unit
            && let Some(chosen) = journal.active_unit_upload(key, unit).await?
        {
            return match self.publication_confirmed(key, unit).await? {
                PublicationResult::Published { version, etag } => {
                    journal.unit_outcome(key, unit, &chosen, true).await?;
                    Ok(ProviderPublication { version, etag })
                }
                PublicationResult::Conflict => Err(Error::Closed("The original serving upload still owns this attempt".into())),
            };
        }
        let upload = uploads.reserve(&path, attempt).await?;
        if let Err(error) = journal.admit(key, attempt, &upload).await {
            uploads.abort(&path, &upload).await?;
            journal.settle(key, attempt, &upload, false).await?;
            return Err(error);
        }

        let result = uploads.complete(&path, &upload, bytes.clone()).await;
        if !matches!(&result, Ok(PublicationResult::Published { .. })) {
            // A conflict leaves uploaded parts behind. An uncertain
            // completion must also be fenced before clearing the admission.
            uploads.abort(&path, &upload).await?;
        }
        let rejected = matches!(result, Ok(PublicationResult::Conflict));
        let selected = match result {
            Ok(PublicationResult::Published { version, etag }) => Ok(ProviderPublication { version, etag }),
            Ok(PublicationResult::Conflict) => self.compare_existing(key, bytes).await,
            Err(error) => {
                match self.publication_confirmed(key, attempt).await {
                    Ok(PublicationResult::Published { version, etag }) => Ok(ProviderPublication { version, etag }),
                    _ => Err(error),
                }
            }
        };
        if let Some(unit) = unit {
            if selected.is_ok() || rejected {
                if matches!(self.publication_confirmed(key, unit).await?, PublicationResult::Published { .. }) {
                    journal.unit_outcome(key, unit, &upload, true).await?;
                } else if rejected {
                    journal.unit_outcome(key, unit, &upload, false).await?;
                }
            }
        } else {
            journal.settle(key, attempt, &upload, true).await?;
        }
        selected
    }

    async fn compare_existing(&self, key: &ObjectKey, bytes: Bytes) -> Result<ProviderPublication> {
        self.with_read(key, self.read_existing(key, bytes)).await
    }

    async fn read_existing(&self, key: &ObjectKey, bytes: Bytes) -> Result<ProviderPublication> {
        let object = self.client.get(&self.path(key)?).await.map_err(|error| provider_error(key, error))?;
        let version = object.meta.version.clone();
        let etag = object.meta.e_tag.clone();
        if self.is_tombstone(&object) || object.bytes().await.map_err(|error| provider_error(key, error))? != bytes {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        Ok(ProviderPublication { version, etag })
    }

    async fn publication_confirmed(&self, key: &ObjectKey, attempt: uuid::Uuid) -> Result<PublicationResult> {
        self.with_read(key, self.read_publication(key, attempt)).await
    }

    async fn read_publication(&self, key: &ObjectKey, attempt: uuid::Uuid) -> Result<PublicationResult> {
        match self.client.get_opts(&self.path(key)?, GetOptions::new().with_head(true)).await {
            Ok(object) => {
                let expected = attempt.to_string();
                if object.attributes.get(&publication_attribute())
                    .is_some_and(|receipt| receipt.as_ref() == expected.as_str())
                {
                    Ok(PublicationResult::Published { version: object.meta.version, etag: object.meta.e_tag })
                } else {
                    Ok(PublicationResult::Conflict)
                }
            }
            Err(object_store::Error::NotFound { .. }) => Ok(PublicationResult::Conflict),
            Err(error) => Err(provider_error(key, error)),
        }
    }

    async fn put_opened_file(&self, key: &ObjectKey, mut input: tokio::fs::File, source: &std::path::Path,
        inventory: Option<&dyn ClassicVersionRetirement>, bind_publication: bool) -> Result<Option<ProviderPublication>> {
        let length = input.metadata().await.map_err(|error| Error::io(source, error))?.len();
        match self.head(key).await {
            Ok(_) => return self.compare_file(key, &mut input, source, length).await.map(Some),
            Err(Error::MissingObject(_)) => (),
            Err(error) => return Err(error),
        }
        if length == 0 { return self.put(key, Bytes::new(), None).await.map(Some); }
        let upload_id = uuid::Uuid::new_v4();
        let temporary = format!(".tmp/upload-{upload_id}");
        let temporary = self.path_for(&temporary)?;
        let mut upload = self.client.put_multipart(&temporary).await.map_err(|error| provider_error(key, error))?;
        let uploaded: Result<Option<String>> = async {
            let mut remaining = length;
            while remaining != 0 {
                let count = remaining.min(5 * 1024 * 1024) as usize;
                let mut bytes = vec![0u8; count];
                input.read_exact(&mut bytes).await.map_err(|error| Error::io(source, error))?;
                upload.put_part(Bytes::from(bytes).into()).await.map_err(|error| provider_error(key, error))?;
                remaining -= count as u64;
            }
            let published = upload.complete().await.map_err(|error| provider_error(key, error))?;
            Ok(published.version)
        }.await;
        let version = match uploaded {
            Ok(version) => version,
            Err(error) => {
                if let Err(cleanup) = upload.abort().await {
                    return Err(Error::MultipartAbort { upload: Box::new(error), abort: Box::new(provider_error(key, cleanup)) });
                }
                return Err(error);
            }
        };
        let published = match self.client.copy_if_not_exists(&temporary, &self.path(key)?).await {
            Ok(()) if !bind_publication => Ok(None),
            Ok(()) | Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                // The conditional copy returns no destination version.
                self.compare_file(key, &mut input, source, length).await.map(Some)
            }
            // A pending copy still depends on its private source.
            Err(error) if copy_outcome_unknown(&error) => return Err(provider_error(key, error)),
            Err(error) => Err(provider_error(key, error)),
        };
        let cleanup = match inventory {
            Some(inventory) => inventory.settle_staging_upload(upload_id, version.as_deref()).await,
            None => self.client.delete(&temporary).await.map_err(|error| provider_error(key, error)),
        };
        let publication = published?;
        cleanup?;
        Ok(publication)
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        let namespace = self.prefix.as_ref().map(|key| self.key_layout.provider_path(key.as_str())).transpose()?;
        let path = directory.map(|key| self.path(key)).transpose()?.or_else(|| namespace.clone());
        let prefix = namespace.map(|path| format!("{path}/"));
        let mut stream = self.client.list(path.as_ref());
        let mut result = Vec::new();
        while let Some(metadata) = stream.try_next().await.map_err(|error| {
            let key = directory.map(ToString::to_string).unwrap_or_default();
            match error {
                error @ object_store::Error::PermissionDenied { .. } => Error::PermissionDenied { key, message: error.to_string() },
                error => Error::Remote { key, message: error.to_string() },
            }
        })? {
            let relative = match &prefix {
                Some(prefix) => {
                    let Some(relative) = metadata.location.as_ref().strip_prefix(prefix) else { continue };
                    relative
                }
                None => metadata.location.as_ref(),
            };
            if relative.split('/').any(|part| part == ".tmp") { continue; }
            let key = self.key_layout.logical_key(relative)?;
            if directory.is_some_and(|directory| !key.is_under(directory)) { continue; }
            if self.hide_tombstones && metadata.size == 0 {
                match self.metadata(&key).await {
                    Err(Error::MissingObject(_)) => continue,
                    Err(error) => return Err(error),
                    Ok(current) => {
                        result.push(StoredObject { key, size: current.size });
                        continue;
                    }
                }
            }
            result.push(StoredObject { key, size: metadata.size });
        }
        result.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(result)
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        match self.client.delete(&self.path(key)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(provider_error(key, error)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn terminal_retirement_observes_only_unversioned_objects_with_a_strong_tag() {
        let namespace = S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None };
        let path = Path::from("partitions/docs/search-runs/file");
        let allocation = uuid::Uuid::from_u128(4);
        let tag = || Some(r#""etag""#.to_owned());
        assert!(observed_terminal(&namespace, &path, allocation, Some("version"), tag()).is_none());
        assert!(observed_terminal(&namespace, &path, allocation, None, Some(r#"W/"etag""#.into())).is_none());
        let Some(PhysicalObjectPublication::Terminal(publication)) = observed_terminal(&namespace, &path, allocation, Some("null"), tag()) else {
            panic!("expected terminal publication evidence");
        };
        assert_eq!(publication.allocation, allocation);
        assert_eq!(publication.key, path.as_ref());
        assert_eq!(publication.etag, r#""etag""#);
    }

    use super::*;

    #[test]
    fn provider_permission_denial_retains_the_requested_object_identity() {
        let key = ObjectKey::new("tables/foo/meta/l01-rc-b00.arrow").unwrap();
        let error = object_store::Error::PermissionDenied {
            path: "configured-prefix/tables/foo/meta/l01-rc-b00.arrow".into(),
            source: Box::new(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected provider denial")),
        };
        let error = provider_error(&key, error);
        assert!(matches!(&error, Error::PermissionDenied { key: actual, .. } if actual == key.as_str()));
        let anomaly = rtdb_types::Anomaly::from(error);
        assert_eq!(anomaly.category, rtdb_types::AnomalyCategory::Forbidden);
        assert_eq!(anomaly.code, "rtdb.storage/permission-denied");
        assert_eq!(anomaly.context["key"], key.as_str());
        assert!(anomaly.message.as_deref().is_some_and(|message| message.contains("injected provider denial")));
    }

    #[test]
    fn permission_words_in_generic_provider_errors_are_not_denials() {
        let key = ObjectKey::new("object").unwrap();
        let error = object_store::Error::Generic {
            store: "test", source: Box::new(std::io::Error::other("403 AccessDenied PermissionDenied")),
        };
        assert!(matches!(provider_error(&key, error), Error::Remote { .. }));
    }

    #[tokio::test]
    async fn classic_literal_s3_writes_and_copies_to_literal_keys() {
        let provider = Arc::new(object_store::memory::InMemory::new());
        let root = ObjectKey::new("root%3F").unwrap();
        let store = RemoteStore::new_classic_s3(provider.clone(), root, IoRegion::new(), RemoteKeyLayout::LiteralS3);
        let question = ObjectKey::new("data/t?").unwrap();
        let percent = ObjectKey::new("data/t%3F").unwrap();
        let copied = ObjectKey::new("data/key#2").unwrap();
        let input = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(input.path(), b"percent").unwrap();

        store.put(&question, Bytes::from_static(b"question")).await.unwrap();
        store.put_file(&percent, input.path()).await.unwrap();
        store.copy(&percent, &copied).await.unwrap();
        for (physical, expected) in [
            ("root%3F/data/t?", b"question".as_slice()),
            ("root%3F/data/t%3F", b"percent".as_slice()),
            ("root%3F/data/key#2", b"percent".as_slice()),
        ] {
            let path = Path::parse(physical).unwrap();
            assert_eq!(provider.get(&path).await.unwrap().bytes().await.unwrap().as_ref(), expected);
        }
        assert!(matches!(provider.head(&Path::parse("root%253F/data/t%3F").unwrap()).await,
            Err(object_store::Error::NotFound { .. })));
        store.close_and_wait().await;
    }

    #[tokio::test]
    async fn a_listed_key_is_the_key_that_was_put_whatever_the_provider_encodes() {
        let store = RemoteStore::new_in(Arc::new(object_store::memory::InMemory::new()), Some(ObjectKey::new("cell").unwrap()), crate::IoRegion::new());
        for name in ["~packs/b00.pack", "tables/public$orders/data/l00-rc-b00.arrow", "notes/50%/a b"] {
            store.put(&ObjectKey::new(name).unwrap(), bytes::Bytes::from_static(b"x")).await.unwrap();
        }
        let listed: Vec<String> = store.list(None).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(listed, ["notes/50%/a b", "tables/public$orders/data/l00-rc-b00.arrow", "~packs/b00.pack"]);
        for name in &listed { store.get(&ObjectKey::new(name.clone()).unwrap()).await.unwrap(); }
        assert_eq!(store.list(Some(&ObjectKey::new("~packs").unwrap())).await.unwrap().len(), 1, "and a listing under one finds it");
    }
    #[tokio::test]
    async fn a_cell_store_refuses_path_deletion() {
        let provider = Arc::new(object_store::memory::InMemory::new());
        let root = ObjectKey::new("cell").unwrap();
        let cell = RemoteStore::new_single_put(provider.clone(), root.clone(), IoRegion::new());
        let ordinary = RemoteStore::new(provider, Some(root));
        let key = ObjectKey::new("data/retained").unwrap();
        cell.put(&key, Bytes::from_static(b"owned")).await.unwrap();

        assert!(matches!(cell.delete(&key).await, Err(Error::Remote { .. })));
        assert_eq!(ordinary.get(&key).await.unwrap(), Bytes::from_static(b"owned"));
        ordinary.delete(&key).await.unwrap();
        assert!(matches!(cell.head(&key).await, Err(Error::MissingObject(_))));
    }

    #[tokio::test]
    async fn only_serving_reads_hide_retirement_tombstones() {
        let provider = Arc::new(object_store::memory::InMemory::new());
        let root = ObjectKey::new("cell").unwrap();
        let serving = RemoteStore::new_single_put(provider.clone(), root.clone(), IoRegion::new())
            .with_s3_versioning(S3ObjectNamespace { bucket: "bucket".into(), endpoint: None, region: None });
        let ordinary = RemoteStore::new(provider.clone(), Some(root));
        let retired = ObjectKey::new("~packs/b01.pack").unwrap();
        let empty = ObjectKey::new("data/empty").unwrap();
        let mut attrs = object_store::Attributes::new();
        attrs.insert(Attribute::Metadata(S3_RETIREMENT_ATTRIBUTE.into()), uuid::Uuid::new_v4().simple().to_string().into());
        provider.put_opts(&Path::from(format!("cell/{retired}")), Bytes::new().into(),
            PutOptions { attributes: attrs, ..PutOptions::default() }).await.unwrap();
        serving.put(&empty, Bytes::new()).await.unwrap();
        assert!(matches!(serving.head(&retired).await, Err(Error::MissingObject(_))));
        assert!(matches!(serving.get(&retired).await, Err(Error::MissingObject(_))));
        assert!(matches!(serving.get_fresh_exact(&retired, 0).await, Err(Error::MissingObject(_))));
        assert!(matches!(serving.observe_range(&retired, 0..1).await, Err(Error::MissingObject(_))));
        assert_eq!(serving.list(None).await.unwrap(), [StoredObject { key: empty, size: 0 }]);
        assert_eq!(ordinary.head(&retired).await.unwrap().size, 0);
        for bytes in [Bytes::from_static(b"late write"), Bytes::new()] {
            assert!(matches!(serving.put(&retired, bytes).await, Err(Error::ConflictingObject(_))));
        }
    }

}
