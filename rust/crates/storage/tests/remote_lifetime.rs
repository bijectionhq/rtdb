#![cfg(feature = "remote")]

use std::{fmt, future::{Future, poll_fn}, pin::Pin, sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::Poll, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore as ProviderStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart, memory::InMemory, path::Path};
use rtdb_columnar::MemoryBudget;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, ReadReservation, disk_cache::DiskCache, remote::{PublicationJournal, PublicationResult, PublicationUploads, RemoteStore}};
use tokio::sync::Semaphore;

#[derive(Debug)]
struct Gate { entered: Semaphore, release: Semaphore }

impl Gate {
    fn new() -> Arc<Self> { Arc::new(Self { entered: Semaphore::new(0), release: Semaphore::new(0) }) }
    async fn enter(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
    async fn arrived(&self) { bounded(self.entered.acquire()).await.unwrap().forget(); }
    fn release(&self) { self.release.add_permits(1); }
}

#[derive(Debug, Default)]
struct Hooks {
    multipart_start: Option<Arc<Gate>>,
    copy: Option<Arc<Gate>>,
    abort: Option<Arc<Gate>>,
    delete: Option<Arc<Gate>>,
    read: Option<Arc<Gate>>,
    read_body: Option<Arc<Gate>>,
    cache_body: Option<Arc<CacheBody>>,
    fail_part: Option<usize>,
    fail_abort: bool,
    fail_delete: bool,
    fail_copy: bool,
    lose_put_reply: bool,
    fail_before_put: bool,
    forget_put_witness: bool,
    fail_first_publication_abort: bool,
    deny_list: bool,
    put_version: Option<&'static str>,
    read_version: Option<&'static str>,
    head_version: Option<&'static str>,
    put_etag: Option<&'static str>,
    read_etag: Option<&'static str>,
    head_etag: Option<&'static str>,
}

#[derive(Debug)]
struct CacheBody {
    frame_size: usize,
    pause_after_first_frame: Option<Arc<Gate>>,
    fail_at_eof: bool,
    declared_size: Option<u64>,
}

#[derive(Debug, Default)]
struct Observed {
    calls: AtomicUsize,
    publication_aborts: AtomicUsize,
    parts: AtomicUsize,
    completed: AtomicUsize,
    abort_calls: AtomicUsize,
    aborts_completed: AtomicUsize,
    deletes_completed: AtomicUsize,
    copies_completed: AtomicUsize,
}

#[derive(Debug)]
struct ControlledStore {
    inner: Arc<InMemory>,
    hooks: Arc<Hooks>,
    observed: Arc<Observed>,
    uploads: std::sync::Mutex<std::collections::BTreeMap<String, (Path, uuid::Uuid)>>,
}

impl ControlledStore {
    fn new(hooks: Hooks) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemory::new()), hooks: Arc::new(hooks), observed: Arc::new(Observed::default()),
            uploads: Default::default(),
        })
    }
    fn called(&self) { self.observed.calls.fetch_add(1, Ordering::SeqCst); }
}

impl fmt::Display for ControlledStore {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result { output.write_str("controlled in-memory provider") }
}

#[async_trait]
impl ProviderStore for ControlledStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, mut options: PutOptions) -> object_store::Result<PutResult> {
        self.called();
        if self.hooks.fail_before_put {
            return Err(injected("provider publication is unconfirmed"));
        }
        if self.hooks.forget_put_witness {
            options.attributes = Default::default();
        }
        let mut result = self.inner.put_opts(location, payload, options).await?;
        if let Some(version) = self.hooks.put_version { result.version = Some(version.into()); }
        if let Some(etag) = self.hooks.put_etag { result.e_tag = Some(etag.into()); }
        if self.hooks.lose_put_reply {
            return Err(injected("lost publication reply"));
        }
        Ok(result)
    }
    async fn put_multipart_opts(&self, location: &Path, options: PutMultipartOptions) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.called();
        let inner = self.inner.put_multipart_opts(location, options).await?;
        if let Some(gate) = &self.hooks.multipart_start { gate.enter().await; }
        Ok(Box::new(ControlledUpload { inner, hooks: Arc::clone(&self.hooks), observed: Arc::clone(&self.observed), next_part: 1 }))
    }
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        self.called();
        if !options.head { if let Some(gate) = &self.hooks.read { gate.enter().await; } }
        let version = if options.head { self.hooks.head_version } else { self.hooks.read_version };
        let etag = if options.head { self.hooks.head_etag } else { self.hooks.read_etag };
        let gate = if options.head { None } else { self.hooks.read_body.clone() };
        let cache_body = self.hooks.cache_body.clone();
        if cache_body.is_some() {
            assert!(!options.head);
            assert!(options.range.is_none());
            assert!(options.version.is_none());
        }
        let mut result = self.inner.get_opts(location, options).await?;
        if let Some(gate) = gate {
            let GetResultPayload::Stream(body) = result.payload else { unreachable!("in-memory provider returns a stream") };
            result.payload = GetResultPayload::Stream(futures::stream::once(async move {
                gate.enter().await;
                Ok::<_, object_store::Error>(body)
            }).try_flatten().boxed());
        }
        if let Some(version) = version { result.meta.version = Some(version.into()); }
        if let Some(etag) = etag { result.meta.e_tag = Some(etag.into()); }
        if let Some(shape) = cache_body {
            let GetResultPayload::Stream(body) = result.payload else { unreachable!("in-memory provider returns a stream") };
            if let Some(size) = shape.declared_size {
                result.meta.size = size;
                result.range = 0..size;
            }
            result.payload = GetResultPayload::Stream(body.map_ok(move |bytes| {
                let shape = Arc::clone(&shape);
                futures::stream::try_unfold((0, bytes, shape), |(offset, bytes, shape)| async move {
                    if offset == shape.frame_size {
                        if let Some(gate) = &shape.pause_after_first_frame { gate.enter().await; }
                    }
                    if offset == bytes.len() {
                        return if shape.fail_at_eof { Err(injected("cache body fault at EOF")) } else { Ok(None) };
                    }
                    let end = (offset + shape.frame_size).min(bytes.len());
                    Ok(Some((bytes.slice(offset..end), (end, bytes, shape))))
                })
            }).try_flatten().boxed());
        }
        Ok(result)
    }
    fn delete_stream(&self, locations: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        let (inner, hooks, observed) = (Arc::clone(&self.inner), Arc::clone(&self.hooks), Arc::clone(&self.observed));
        locations.then(move |location| {
            let (inner, hooks, observed) = (Arc::clone(&inner), Arc::clone(&hooks), Arc::clone(&observed));
            async move {
                let location = location?;
                observed.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(gate) = &hooks.delete { gate.enter().await; }
                if hooks.fail_delete { return Err(injected("injected delete failure")); }
                inner.delete(&location).await?;
                observed.deletes_completed.fetch_add(1, Ordering::SeqCst);
                Ok(location)
            }
        }).boxed()
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.called();
        if self.hooks.deny_list {
            let path = prefix.map(ToString::to_string).unwrap_or_default();
            return futures::stream::once(async move { Err(object_store::Error::PermissionDenied {
                path, source: Box::new(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected list denial")),
            }) }).boxed();
        }
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.called(); self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
        self.called();
        if let Some(gate) = &self.hooks.copy { gate.enter().await; }
        if self.hooks.fail_copy { return Err(injected("injected copy failure")); }
        self.inner.copy_opts(from, to, options).await?;
        self.observed.copies_completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl PublicationUploads for ControlledStore {
    async fn reserve(&self, path: &Path, attempt: uuid::Uuid) -> rtdb_storage::Result<String> {
        let upload = format!("upload-{attempt}");
        assert!(self.uploads.lock().unwrap().insert(upload.clone(), (path.clone(), attempt)).is_none());
        Ok(upload)
    }

    async fn complete(&self, path: &Path, upload: &str, bytes: Bytes) -> rtdb_storage::Result<PublicationResult> {
        let (owner, attempt) = self.uploads.lock().unwrap().get(upload).unwrap().clone();
        assert_eq!(owner, *path);
        let mut options = PutOptions { mode: object_store::PutMode::Create, ..Default::default() };
        options.attributes.insert(object_store::Attribute::Metadata("rtdb-publication".into()), attempt.to_string().into());
        match self.put_opts(path, bytes.into(), options).await {
            Ok(written) => {
                self.uploads.lock().unwrap().remove(upload);
                Ok(PublicationResult::Published { version: written.version, etag: written.e_tag })
            }
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => Ok(PublicationResult::Conflict),
            Err(error) => Err(Error::Remote { key: path.to_string(), message: error.to_string() }),
        }
    }

    async fn abort(&self, path: &Path, upload: &str) -> rtdb_storage::Result<()> {
        let aborts = self.observed.publication_aborts.fetch_add(1, Ordering::SeqCst);
        if self.hooks.fail_first_publication_abort && aborts == 0 {
            return Err(Error::Remote { key: path.to_string(), message: "provider did not confirm abort".into() });
        }
        if let Some((owner, _)) = self.uploads.lock().unwrap().remove(upload) {
            assert_eq!(owner, *path);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ControlledUpload { inner: Box<dyn MultipartUpload>, hooks: Arc<Hooks>, observed: Arc<Observed>, next_part: usize }

#[async_trait]
impl MultipartUpload for ControlledUpload {
    fn put_part(&mut self, payload: PutPayload) -> UploadPart {
        let part = self.next_part; self.next_part += 1;
        self.observed.parts.fetch_add(1, Ordering::SeqCst);
        if self.hooks.fail_part == Some(part) { return Box::pin(async { Err(injected("injected upload part failure")) }); }
        self.inner.put_part(payload)
    }
    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = self.inner.complete().await?;
        self.observed.completed.fetch_add(1, Ordering::SeqCst);
        Ok(result)
    }
    async fn abort(&mut self) -> object_store::Result<()> {
        self.observed.abort_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.hooks.abort { gate.enter().await; }
        if self.hooks.fail_abort { return Err(injected("injected abort failure")); }
        self.inner.abort().await?;
        self.observed.aborts_completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn injected(message: &'static str) -> object_store::Error {
    object_store::Error::Generic { store: "remote-lifetime-test", source: Box::new(std::io::Error::other(message)) }
}

async fn bounded<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future).await.expect("controlled I/O did not finish")
}

fn staged_cache_lengths(root: &std::path::Path) -> Vec<u64> {
    std::fs::read_dir(root.join(".tmp")).unwrap()
        .map(|entry| entry.unwrap())
        .filter(|entry| entry.file_name() != ".owner-lock")
        .map(|entry| entry.metadata().unwrap().len())
        .collect()
}

#[tokio::test]
async fn cache_download_streams_an_object_larger_than_its_budget_and_restarts_after_cancellation() {
    use rtdb_types::S3ObjectNamespace;

    let gate = Gate::new();
    let frame_size = 256 << 10;
    let download = ControlledStore::new(Hooks { cache_body: Some(Arc::new(CacheBody {
        frame_size, pause_after_first_frame: Some(gate.clone()), fail_at_eof: false, declared_size: None,
    })), ..Default::default() });
    let ordinary = ControlledStore::new(Hooks { read_version: Some("observed"), ..Default::default() });
    let object = Path::from("part/trie");
    let length = 6 << 20;
    let initial = Bytes::from(vec![11; length]);
    download.inner.put(&object, initial.into()).await.unwrap();
    ordinary.inner.put(&object, Bytes::from_static(b"exact grant").into()).await.unwrap();
    let namespace = S3ObjectNamespace { bucket: "bucket".into(), endpoint: None, region: None };
    let remote = RemoteStore::new(ordinary.clone(), Some(ObjectKey::new("part").unwrap()))
        .with_s3_versioning(namespace.clone())
        .with_cache_downloads(download.clone());

    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(4 << 20);
    let cache = DiskCache::open(directory.path(), length as u64, memory.clone()).unwrap();
    let store = cache.scope(ObjectKey::new("db").unwrap(), Arc::new(remote)).unwrap();
    let key = ObjectKey::new("trie").unwrap();
    let result = directory.path().join("db/trie");
    let pending = tokio::spawn({
        let (store, key) = (store.clone(), key.clone());
        async move { store.local_file(&key).await.map(drop) }
    });
    gate.arrived().await;
    assert!(memory.used_bytes() >= 3 << 20);
    assert_eq!(staged_cache_lengths(directory.path()), [frame_size as u64]);
    assert!(!result.exists());
    pending.abort();
    assert!(bounded(pending).await.unwrap_err().is_cancelled());
    assert!(staged_cache_lengths(directory.path()).is_empty());
    assert_eq!(memory.used_bytes(), 0);

    let replacement = Bytes::from(vec![29; length]);
    download.inner.put(&object, replacement.clone().into()).await.unwrap();
    gate.release();
    drop(bounded(store.local_file(&key)).await.unwrap().unwrap());
    assert_eq!(std::fs::read(&result).unwrap().as_slice(), replacement.as_ref());
    assert!(memory.used_bytes() < frame_size);
    assert_eq!(cache.traffic().network_read_bytes, length as u64);
    assert_eq!(cache.traffic().misses, 1);
    assert_eq!(download.observed.calls.load(Ordering::SeqCst), 2);
    assert_eq!(ordinary.observed.calls.load(Ordering::SeqCst), 0);

    let exact = store.observe_fresh_exact(&key, b"exact grant".len()).await.unwrap();
    assert_eq!(exact.value.as_ref(), b"exact grant");
    let version = exact.version.unwrap();
    assert_eq!(version.namespace, namespace);
    assert_eq!(version.key, "part/trie");
    assert_eq!(version.version, "observed");
}

#[tokio::test]
async fn cache_download_rejects_out_of_budget_frames_wrong_lengths_and_faulted_eof() {
    for (frame_size, declared_size, fail_at_eof, expected) in [
        (4 << 20, None, false, "exceeded the reserved transport window"),
        (256 << 10, Some((4 << 20) + 1), false, "incomplete or oversized cache body"),
        (256 << 10, Some((4 << 20) - 1), false, "incomplete or oversized cache body"),
        (256 << 10, None, true, "cache body fault at EOF"),
    ] {
        let provider = ControlledStore::new(Hooks { cache_body: Some(Arc::new(CacheBody {
            frame_size, pause_after_first_frame: None, fail_at_eof, declared_size,
        })), ..Default::default() });
        provider.inner.put(&Path::from("trie"), Bytes::from(vec![9; 4 << 20]).into()).await.unwrap();
        let remote = RemoteStore::new(Arc::new(InMemory::new()), None)
            .with_cache_downloads(provider);
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryBudget::new(4 << 20);
        let cache = DiskCache::open(directory.path(), 8 << 20, memory.clone()).unwrap();
        let key = ObjectKey::new("trie").unwrap();
        assert!(matches!(bounded(cache.get(&key, &remote, &key)).await,
            Err(Error::Remote { message, .. }) if message.contains(expected)));
        assert!(!directory.path().join("trie").exists());
        assert!(staged_cache_lengths(directory.path()).is_empty());
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(cache.traffic().misses, 0);
    }
}

#[tokio::test]
async fn an_unfunded_cache_stream_leaves_the_reserved_range_fill_available() {
    let ordinary = ControlledStore::new(Hooks::default());
    let download = ControlledStore::new(Hooks::default());
    let remote = RemoteStore::new(ordinary.clone(), None)
        .with_cache_downloads(download.clone());
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 4 << 20, MemoryBudget::new(1 << 20)).unwrap();
    let key = ObjectKey::new("trie").unwrap();
    let bytes = Bytes::from(vec![7; 2 << 20]);
    ordinary.inner.put(&Path::from("trie"), bytes.clone().into()).await.unwrap();
    let lease = bounded(cache.get(&key, &remote, &key)).await.unwrap();
    assert_eq!(std::fs::read(lease.path()).unwrap().as_slice(), bytes.as_ref());
    assert_eq!(download.observed.calls.load(Ordering::SeqCst), 0);
}

#[derive(Default)]
struct PublicationInventory {
    pending: std::sync::Mutex<std::collections::BTreeMap<uuid::Uuid, (ObjectKey, String)>>,
}

#[async_trait]
impl PublicationJournal for PublicationInventory {
    async fn admit(&self, key: &ObjectKey, attempt: uuid::Uuid, upload: &str) -> rtdb_storage::Result<()> {
        assert!(self.pending.lock().unwrap().insert(attempt, (key.clone(), upload.to_owned())).is_none());
        Ok(())
    }

    async fn settle(&self, key: &ObjectKey, attempt: uuid::Uuid, upload: &str, submitted: bool) -> rtdb_storage::Result<()> {
        assert!(submitted);
        assert_eq!(self.pending.lock().unwrap().remove(&attempt), Some((key.clone(), upload.to_owned())));
        Ok(())
    }
}

fn journaled_region() -> (Arc<IoRegion>, Arc<PublicationInventory>) {
    let mut io = IoRegion::new();
    let journal = Arc::new(PublicationInventory::default());
    Arc::get_mut(&mut io).unwrap().track_publications(journal.clone());
    (io, journal)
}

#[tokio::test]
async fn publication_versions_come_from_the_acknowledgement_or_verified_existing_bytes() {
    use rtdb_types::S3ObjectNamespace;

    let path = ObjectKey::new("data.arrow").unwrap();
    let contents = Bytes::from_static(b"sealed");
    for managed in [false, true] {
        let provider = ControlledStore::new(Hooks {
            put_version: Some("write-version"),
            read_version: Some("retried-version"),
            head_version: Some("unbound-current-version"),
            ..Default::default()
        });
        let prefix = ObjectKey::new("cell").unwrap();
        let (io, journal) = journaled_region();
        let store = if managed {
            RemoteStore::new_managed(provider.clone(), prefix, io.clone(), provider)
        } else {
            RemoteStore::new_in(provider, Some(prefix), io.clone())
        }.with_s3_versioning(S3ObjectNamespace { bucket: "objects".into(), endpoint: None, region: None });

        let written = store.put_with_publication(&path, contents.clone()).await.unwrap().unwrap().into_version().unwrap();
        assert_eq!(written.key, "cell/data.arrow");
        assert_eq!(written.version, "write-version", "the provider write acknowledgement supplies the version");
        let retried = store.put_with_publication(&path, contents.clone()).await.unwrap().unwrap().into_version().unwrap();
        assert_eq!(retried.version, "retried-version", "the same read must supply the body and version");
        assert_eq!(retried.namespace, written.namespace);
        assert_eq!(store.retirement_version(&path).await.unwrap().unwrap().version, "unbound-current-version");
        assert!(matches!(store.put_with_publication(&path, Bytes::from_static(b"other")).await, Err(Error::ConflictingObject(_))));
        io.close_and_wait().await;
        assert!(journal.pending.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn multipart_file_publication_reports_the_version_whose_bytes_were_verified() {
    let provider = ControlledStore::new(Hooks {
        put_version: Some("write-version"),
        read_version: Some("verified-version"),
        head_version: Some("unverified-head-version"),
        ..Default::default()
    });
    let store = RemoteStore::new(provider.clone(), None)
        .with_s3_versioning(rtdb_types::S3ObjectNamespace { bucket: "objects".into(), endpoint: None, region: None });
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("data.arrow");
    let key = ObjectKey::new("data.arrow").unwrap();
    let mut contents = vec![17u8; 1024 * 1024 + 1];
    std::fs::write(&file, &contents).unwrap();

    for _ in 0..2 {
        let publication = store.put_file_with_publication(&key, &file).await.unwrap().unwrap().into_version().unwrap();
        assert_eq!(publication.key, key.as_str());
        assert_eq!(publication.version, "verified-version");
    }
    assert_eq!(store.retirement_version(&key).await.unwrap().unwrap().version, "unverified-head-version");
    contents[1024 * 1024] = 29;
    std::fs::write(&file, &contents).unwrap();
    assert!(matches!(store.put_file_with_publication(&key, &file).await, Err(Error::ConflictingObject(_))));
    assert_eq!(paths(&provider.inner).await, ["data.arrow"]);

    let empty = ObjectKey::new("empty.arrow").unwrap();
    std::fs::write(&file, []).unwrap();
    let publication = store.put_file_with_publication(&empty, &file).await.unwrap().unwrap().into_version().unwrap();
    assert_eq!(publication.version, "write-version");
}

#[tokio::test]
async fn terminal_publication_carries_the_tag_from_its_acknowledgement_or_confirmed_retry() {
    let path = ObjectKey::new("data.arrow").unwrap();
    let prefix = ObjectKey::new("terminal-root").unwrap();
    let contents = Bytes::from_static(b"sealed");
    let allocation = uuid::Uuid::new_v4();
    let namespace = rtdb_types::S3ObjectNamespace { bucket: "objects".into(), endpoint: None, region: None };
    for lose_put_reply in [false, true] {
        let (io, journal) = journaled_region();
        let provider = ControlledStore::new(Hooks {
            put_etag: Some("\"acknowledged\""), head_etag: Some("\"confirmed\""), read_etag: Some("\"retried\""),
            lose_put_reply, ..Default::default()
        });
        let store = RemoteStore::new_managed(provider.clone(), prefix.clone(), io.clone(), provider)
            .with_s3_versioning(namespace.clone()).with_terminal_allocation(allocation);
        let receipt = |etag: &str| Some(rtdb_types::PhysicalObjectPublication::Terminal(rtdb_types::TerminalObjectPublication {
            namespace: namespace.clone(), key: "terminal-root/data.arrow".into(), allocation, etag: etag.into(),
        }));
        let committed = store.put_with_publication(&path, contents.clone()).await.unwrap();
        assert_eq!(committed, receipt(if lose_put_reply { "\"confirmed\"" } else { "\"acknowledged\"" }));
        assert_eq!(store.put_with_publication(&path, contents.clone()).await.unwrap(), receipt("\"retried\""));
        io.close_and_wait().await;
        assert!(journal.pending.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn managed_completion_recovers_exact_bytes_or_retires_the_upload() {
    let key = ObjectKey::new("data.arrow").unwrap();
    let contents = Bytes::from_static(b"sealed");
    let prefix = ObjectKey::new("cell").unwrap();
    let (io, journal) = journaled_region();
    let provider = ControlledStore::new(Hooks::default());
    let store = RemoteStore::new_managed(provider.clone(), prefix.clone(), io.clone(), provider);
    store.put(&key, contents.clone()).await.unwrap();
    assert!(matches!(
        store.put(&key, Bytes::from_static(b"other")).await,
        Err(Error::ConflictingObject(_))
    ));
    io.close_and_wait().await;
    assert!(journal.pending.lock().unwrap().is_empty());

    let (io, journal) = journaled_region();
    let provider = ControlledStore::new(Hooks { lose_put_reply: true, head_version: Some("confirmed-attempt-version"), ..Default::default() });
    let store = RemoteStore::new_managed(provider.clone(), prefix.clone(), io.clone(), provider.clone())
        .with_s3_versioning(rtdb_types::S3ObjectNamespace { bucket: "objects".into(), endpoint: None, region: None });
    let recovered = store.put_with_publication(&key, contents.clone()).await.unwrap().unwrap().into_version().unwrap();
    assert_eq!(recovered.version, "confirmed-attempt-version");
    io.close_and_wait().await;
    assert!(journal.pending.lock().unwrap().is_empty());
    assert_eq!(paths(&provider.inner).await, ["cell/data.arrow"]);

    for (hooks, objects) in [
        (Hooks { fail_before_put: true, ..Default::default() }, Vec::<&str>::new()),
        (Hooks { lose_put_reply: true, forget_put_witness: true, ..Default::default() }, vec!["cell/data.arrow"]),
    ] {
        let (io, journal) = journaled_region();
        let provider = ControlledStore::new(hooks);
        let store = RemoteStore::new_managed(provider.clone(), prefix.clone(), io.clone(), provider.clone());
        assert!(matches!(store.put(&key, contents.clone()).await, Err(Error::Remote { .. })));
        io.close_and_wait().await;
        assert!(journal.pending.lock().unwrap().is_empty());
        assert!(provider.uploads.lock().unwrap().is_empty());
        assert_eq!(paths(&provider.inner).await, objects);
    }
}

#[tokio::test]
async fn a_replacement_can_abort_the_closed_writers_exact_upload() {
    let key = ObjectKey::new("data.arrow").unwrap();
    let prefix = ObjectKey::new("cell").unwrap();
    let (io, journal) = journaled_region();
    let provider = ControlledStore::new(Hooks {
        fail_before_put: true,
        fail_first_publication_abort: true,
        ..Default::default()
    });
    let old = RemoteStore::new_managed(provider.clone(), prefix.clone(), io.clone(), provider.clone());
    assert!(matches!(old.put(&key, Bytes::from_static(b"sealed")).await, Err(Error::Remote { .. })));
    io.close_and_wait().await;
    let pending = journal.pending.lock().unwrap().clone();
    assert_eq!(pending.len(), 1);
    let (_, (retained_key, upload)) = pending.first_key_value().unwrap();
    assert_eq!(retained_key, &key);
    drop(old);
    drop(io);

    let path = Path::from("cell/data.arrow");
    let other = provider.reserve(&path, uuid::Uuid::new_v4()).await.unwrap();
    provider.abort(&path, upload).await.unwrap();
    let open = provider.uploads.lock().unwrap();
    assert!(!open.contains_key(upload));
    assert!(open.contains_key(&other));
}

struct PublicationGate {
    admission: Arc<Gate>,
    refused: bool,
    settlements: AtomicUsize,
}

#[async_trait]
impl PublicationJournal for PublicationGate {
    async fn admit(&self, _key: &ObjectKey, _attempt: uuid::Uuid, _upload: &str) -> rtdb_storage::Result<()> {
        self.admission.enter().await;
        if self.refused {
            return Err(Error::Task("directory refused the publication".into()));
        }
        Ok(())
    }

    async fn settle(&self, _key: &ObjectKey, _attempt: uuid::Uuid, _upload: &str, submitted: bool) -> rtdb_storage::Result<()> {
        assert_eq!(submitted, !self.refused);
        self.settlements.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn a_tracked_remote_put_waits_for_durable_admission_before_uploading_payload() {
    for refused in [false, true] {
        let gate = Gate::new();
        let journal = Arc::new(PublicationGate { admission: gate.clone(), refused, settlements: AtomicUsize::new(0) });
        let mut io = IoRegion::new();
        Arc::get_mut(&mut io).unwrap().track_publications(journal.clone());
        let provider = ControlledStore::new(Hooks::default());
        let store = RemoteStore::new_managed(provider.clone(), ObjectKey::new("cell").unwrap(), io.clone(), provider.clone());
        let key = ObjectKey::new("data.arrow").unwrap();
        let mut write = Box::pin(store.put(&key, Bytes::from_static(b"sealed")));
        assert_pending(write.as_mut()).await;
        gate.arrived().await;
        assert_eq!(provider.observed.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.uploads.lock().unwrap().len(), 1);
        gate.release();
        assert_eq!(bounded(write).await.is_err(), refused);
        io.close_and_wait().await;
        assert_eq!(journal.settlements.load(Ordering::SeqCst), 1);
        assert!(provider.uploads.lock().unwrap().is_empty());
        assert_eq!(provider.observed.calls.load(Ordering::SeqCst), if refused { 0 } else { 1 });
    }
}

async fn assert_pending<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    poll_fn(|context| { assert!(future.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
}

async fn paths(provider: &InMemory) -> Vec<String> {
    let mut paths = provider.list(None).map_ok(|object| object.location.to_string()).try_collect::<Vec<_>>().await.unwrap();
    paths.sort(); paths
}

fn source_file(bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("data.arrow");
    std::fs::write(&path, bytes).unwrap();
    (directory, path)
}

#[tokio::test]
async fn list_denial_keeps_the_relative_requested_directory_under_a_provider_prefix() {
    let provider = ControlledStore::new(Hooks { deny_list: true, ..Default::default() });
    let store = RemoteStore::new(provider, Some(ObjectKey::new("provider/root").unwrap()));
    let directory = ObjectKey::new("tables/foo").unwrap();
    for requested in [Some(&directory), None] {
        let error = store.list(requested).await.unwrap_err();
        let expected = requested.map(ObjectKey::as_str).unwrap_or_default();
        assert!(matches!(&error, Error::PermissionDenied { key, .. } if key == expected));
        let anomaly = rtdb_types::Anomaly::from(error);
        assert_eq!(anomaly.category, rtdb_types::AnomalyCategory::Forbidden);
        assert_eq!(anomaly.context["key"], expected);
    }
    store.close_and_wait().await;
}

#[cfg(unix)]
#[tokio::test]
async fn cancelled_multipart_waiter_keeps_unlinked_source_and_drains_private_staging_cleanup() {
    let start = Gate::new(); let cleanup = Gate::new();
    let provider = ControlledStore::new(Hooks { multipart_start: Some(Arc::clone(&start)), delete: Some(Arc::clone(&cleanup)), ..Default::default() });
    let store = RemoteStore::new(provider.clone(), Some(ObjectKey::new("db").unwrap()));
    let key = ObjectKey::new("tables/a/data.arrow").unwrap();
    let mut data = vec![17; 5 * 1024 * 1024 + 31]; data[5 * 1024 * 1024] = 29;
    let (_directory, path) = source_file(&data);
    let mut request = Box::pin(store.put_file(&key, &path));
    assert_pending(request.as_mut()).await; start.arrived().await;
    drop(request); std::fs::remove_file(&path).unwrap(); assert!(!path.exists());
    let mut close = Box::pin(store.close_and_wait());
    assert_pending(close.as_mut()).await;
    start.release(); cleanup.arrived().await;
    assert_pending(close.as_mut()).await;
    assert_eq!(provider.observed.parts.load(Ordering::SeqCst), 2);
    assert_eq!(provider.observed.completed.load(Ordering::SeqCst), 1);
    let staged = paths(&provider.inner).await;
    assert_eq!(staged.len(), 2);
    assert!(staged.iter().any(|path| path.starts_with("db/.tmp/upload-")));
    assert_eq!(provider.inner.get(&Path::from("db/tables/a/data.arrow")).await.unwrap().bytes().await.unwrap().as_ref(), data.as_slice());
    cleanup.release(); bounded(close).await;
    assert_eq!(paths(&provider.inner).await, ["db/tables/a/data.arrow"]);
    assert_eq!(provider.observed.deletes_completed.load(Ordering::SeqCst), 1);
    assert_eq!(provider.observed.abort_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn cancelled_failed_multipart_upload_is_joined_through_abort() {
    let start = Gate::new(); let abort = Gate::new();
    let provider = ControlledStore::new(Hooks { multipart_start: Some(Arc::clone(&start)), abort: Some(Arc::clone(&abort)), fail_part: Some(2), ..Default::default() });
    let store = RemoteStore::new(provider.clone(), Some(ObjectKey::new("db").unwrap()));
    let key = ObjectKey::new("data.arrow").unwrap();
    let (_directory, path) = source_file(&vec![7; 5 * 1024 * 1024 + 1]);
    let mut request = Box::pin(store.put_file(&key, &path));
    assert_pending(request.as_mut()).await; start.arrived().await; drop(request);
    let mut close = Box::pin(store.close_and_wait());
    assert_pending(close.as_mut()).await;
    start.release(); abort.arrived().await;
    assert_pending(close.as_mut()).await;
    assert_eq!(provider.observed.parts.load(Ordering::SeqCst), 2);
    assert_eq!(provider.observed.completed.load(Ordering::SeqCst), 0);
    assert_eq!(provider.observed.abort_calls.load(Ordering::SeqCst), 1);
    assert_eq!(provider.observed.aborts_completed.load(Ordering::SeqCst), 0);
    assert!(paths(&provider.inner).await.is_empty());
    abort.release(); bounded(close).await;
    assert_eq!(provider.observed.aborts_completed.load(Ordering::SeqCst), 1);
    assert!(paths(&provider.inner).await.is_empty());
}

#[tokio::test]
async fn cancelled_provider_copy_is_joined_by_both_root_regions() {
    let gate = Gate::new();
    let provider = ControlledStore::new(Hooks { copy: Some(Arc::clone(&gate)), ..Default::default() });
    provider.inner.put(&Path::from("source/data.arrow"), Bytes::from_static(b"copied bytes").into()).await.unwrap();
    let source_io = IoRegion::new(); let destination_io = IoRegion::new();
    let source = RemoteStore::new_in(provider.clone(), Some(ObjectKey::new("source").unwrap()), Arc::clone(&source_io));
    let destination = RemoteStore::new_in(provider.clone(), Some(ObjectKey::new("destination").unwrap()), Arc::clone(&destination_io));
    let key = ObjectKey::new("data.arrow").unwrap();
    let mut request = Box::pin(source.copy_to(&key, &destination, &key));
    assert_pending(request.as_mut()).await; gate.arrived().await; drop(request);
    let mut source_close = Box::pin(source_io.close_and_wait());
    let mut destination_close = Box::pin(destination_io.close_and_wait());
    assert_pending(source_close.as_mut()).await; assert_pending(destination_close.as_mut()).await;
    assert!(provider.inner.head(&Path::from("destination/data.arrow")).await.is_err());
    gate.release(); bounded(async { tokio::join!(source_close, destination_close); }).await;
    assert_eq!(provider.observed.copies_completed.load(Ordering::SeqCst), 1);
    assert_eq!(provider.inner.get(&Path::from("destination/data.arrow")).await.unwrap().bytes().await.unwrap().as_ref(), b"copied bytes");
    assert_eq!(paths(&provider.inner).await, ["destination/data.arrow", "source/data.arrow"]);
}

#[tokio::test]
async fn closed_regions_reject_all_requests_without_touching_the_provider() {
    let provider = ControlledStore::new(Hooks::default());
    let io = IoRegion::new(); io.close_and_wait().await;
    let store = RemoteStore::new_in(provider.clone(), None, Arc::clone(&io));
    let key = ObjectKey::new("data.arrow").unwrap();
    let directory = tempfile::tempdir().unwrap(); let missing = directory.path().join("never-opened.arrow");
    assert!(matches!(store.head(&key).await, Err(Error::Closed(_))));
    assert!(matches!(store.get(&key).await, Err(Error::Closed(_))));
    assert!(matches!(store.get_range(&key, 0..1).await, Err(Error::Closed(_))));
    assert!(matches!(store.put(&key, Bytes::from_static(b"x")).await, Err(Error::Closed(_))));
    assert!(matches!(store.put_file(&key, &missing).await, Err(Error::Closed(_))));
    assert!(matches!(store.copy(&key, &key).await, Err(Error::Closed(_))));
    assert!(matches!(store.list(None).await, Err(Error::Closed(_))));
    assert!(matches!(store.delete(&key).await, Err(Error::Closed(_))));
    let memory = MemoryBudget::new(8);
    let reservation = ReadReservation::new(1, memory.reserve(1).unwrap());
    assert!(matches!(store.get_reserved_range(&key, 0..1, reservation).await, Err(Error::Closed(_))));
    assert_eq!(memory.used_bytes(), 0);
    let open_io = IoRegion::new();
    let open = RemoteStore::new_in(provider.clone(), None, Arc::clone(&open_io));
    assert!(matches!(store.copy_to(&key, &open, &key).await, Err(Error::Closed(_))));
    assert!(matches!(open.copy_to(&key, &store, &key).await, Err(Error::Closed(_))));
    bounded(open_io.close_and_wait()).await;
    assert_eq!(provider.observed.calls.load(Ordering::SeqCst), 0);
    assert!(!missing.exists());
}

#[tokio::test]
async fn cancelled_reserved_range_keeps_its_charge_until_the_provider_finishes() {
    let gate = Gate::new();
    let provider = ControlledStore::new(Hooks { read: Some(Arc::clone(&gate)), ..Default::default() });
    provider.inner.put(&Path::from("data.arrow"), Bytes::from_static(b"0123456789").into()).await.unwrap();
    let store = RemoteStore::new(provider, None);
    let key = ObjectKey::new("data.arrow").unwrap(); let memory = MemoryBudget::new(8);
    let reservation = ReadReservation::new(4, memory.reserve(4).unwrap());
    let mut request = Box::pin(store.get_reserved_range(&key, 2..6, reservation));
    assert_pending(request.as_mut()).await; gate.arrived().await; drop(request);
    assert_eq!(memory.used_bytes(), 4);
    let mut close = Box::pin(store.close_and_wait());
    assert_pending(close.as_mut()).await; assert_eq!(memory.used_bytes(), 4);
    gate.release(); bounded(close).await;
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn cancelled_remote_body_keeps_its_catalog_owner_until_the_provider_drains() {
    let gate = Gate::new();
    let provider = ControlledStore::new(Hooks { read_body: Some(Arc::clone(&gate)), ..Default::default() });
    provider.inner.put(&Path::from("data.arrow"), Bytes::from_static(b"catalog data").into()).await.unwrap();
    let root = IoRegion::new();
    let store = RemoteStore::new_in(provider, None, root.child());
    let key = ObjectKey::new("data.arrow").unwrap();
    let owner = Arc::new(());
    let held = Arc::downgrade(&owner);
    let mut request = Box::pin(IoRegion::retaining(owner, store.get_fresh_exact(&key, 12)));
    assert_pending(request.as_mut()).await;
    gate.arrived().await;

    drop(request);
    let mut drain = Box::pin(root.close_and_wait());
    assert_pending(drain.as_mut()).await;
    assert!(held.upgrade().is_some(), "the response body still needs the catalog owner");
    assert!(matches!(store.head(&key).await, Err(Error::Closed(_))));

    gate.release();
    bounded(drain).await;
    assert!(held.upgrade().is_none(), "provider completion must release the last catalog owner");
}

#[tokio::test]
async fn successful_reserved_range_retains_the_charge_after_the_region_drains() {
    let gate = Gate::new();
    let provider = ControlledStore::new(Hooks { read: Some(Arc::clone(&gate)), ..Default::default() });
    provider.inner.put(&Path::from("data.arrow"), Bytes::from_static(b"0123456789").into()).await.unwrap();
    let store = RemoteStore::new(provider, None);
    let key = ObjectKey::new("data.arrow").unwrap(); let memory = MemoryBudget::new(8);
    let reservation = ReadReservation::new(4, memory.reserve(4).unwrap());
    let mut request = Box::pin(store.get_reserved_range(&key, 2..6, reservation));
    assert_pending(request.as_mut()).await; gate.arrived().await;
    assert_eq!(memory.used_bytes(), 4);
    gate.release(); let result = bounded(request).await.unwrap();
    bounded(store.close_and_wait()).await;
    assert_eq!(result.as_slice(), b"2345"); assert_eq!(memory.used_bytes(), 4);
    drop(result); assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn publication_reports_staging_cleanup_failure_after_destination_is_visible() {
    let provider = ControlledStore::new(Hooks { fail_delete: true, ..Default::default() });
    let store = RemoteStore::new(provider.clone(), Some(ObjectKey::new("db").unwrap()));
    let key = ObjectKey::new("data.arrow").unwrap(); let (_directory, path) = source_file(b"published");
    let error = store.put_file(&key, &path).await.unwrap_err();
    assert!(matches!(error, Error::Remote { message, .. } if message.contains("injected delete failure")));
    assert_eq!(provider.inner.get(&Path::from("db/data.arrow")).await.unwrap().bytes().await.unwrap().as_ref(), b"published");
    let retained = paths(&provider.inner).await;
    assert_eq!(retained.len(), 2); assert!(retained.iter().any(|path| path.starts_with("db/.tmp/upload-")));
    assert_eq!(store.list(None).await.unwrap().iter().map(|object| object.key.as_str()).collect::<Vec<_>>(), ["data.arrow"]);
    bounded(store.close_and_wait()).await;
}

#[tokio::test]
async fn multipart_error_reports_the_abort_failure_and_publication_failure_still_cleans_staging() {
    let provider = ControlledStore::new(Hooks { fail_part: Some(1), fail_abort: true, ..Default::default() });
    let store = RemoteStore::new(provider.clone(), None);
    let key = ObjectKey::new("data.arrow").unwrap(); let (_directory, path) = source_file(b"payload");
    let error = store.put_file(&key, &path).await.unwrap_err();
    assert!(matches!(&error, Error::MultipartAbort { .. }));
    let message = error.to_string();
    assert!(message.contains("injected upload part failure") && message.contains("multipart abort also failed") && message.contains("injected abort failure"));
    assert_eq!(provider.observed.abort_calls.load(Ordering::SeqCst), 1);
    assert!(paths(&provider.inner).await.is_empty());
    bounded(store.close_and_wait()).await;
    let provider = ControlledStore::new(Hooks { fail_copy: true, ..Default::default() });
    let store = RemoteStore::new(provider.clone(), None);
    let error = store.put_file(&key, &path).await.unwrap_err();
    assert!(matches!(error, Error::Remote { message, .. } if message.contains("injected copy failure")));
    assert_eq!(provider.observed.completed.load(Ordering::SeqCst), 1);
    assert_eq!(provider.observed.deletes_completed.load(Ordering::SeqCst), 1);
    assert!(paths(&provider.inner).await.is_empty());
    bounded(store.close_and_wait()).await;
}
