#![cfg(feature = "remote")]

use std::{fmt, future::{Future, poll_fn}, pin::Pin, sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::Poll, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore as ProviderStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, UploadPart, memory::InMemory, path::Path};
use rtdb_columnar::MemoryBudget;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, ReadReservation, remote::RemoteStore};
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
    fail_part: Option<usize>,
    fail_abort: bool,
    fail_delete: bool,
    fail_copy: bool,
    lose_put_reply: bool,
    fail_before_put: bool,
    forget_put_witness: bool,
    defer_publication_head: bool,
    deny_list: bool,
}

#[derive(Debug, Default)]
struct Observed {
    calls: AtomicUsize,
    heads: AtomicUsize,
    parts: AtomicUsize,
    completed: AtomicUsize,
    abort_calls: AtomicUsize,
    aborts_completed: AtomicUsize,
    deletes_completed: AtomicUsize,
    copies_completed: AtomicUsize,
}

#[derive(Debug)]
struct ControlledStore { inner: Arc<InMemory>, hooks: Arc<Hooks>, observed: Arc<Observed> }

impl ControlledStore {
    fn new(hooks: Hooks) -> Arc<Self> {
        Arc::new(Self { inner: Arc::new(InMemory::new()), hooks: Arc::new(hooks), observed: Arc::new(Observed::default()) })
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
        let result = self.inner.put_opts(location, payload, options).await?;
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
        if options.head && self.hooks.defer_publication_head && self.observed.heads.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(injected("the first publication observation was unavailable"));
        }
        if !options.head { if let Some(gate) = &self.hooks.read { gate.enter().await; } }
        self.inner.get_opts(location, options).await
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

#[tokio::test]
async fn a_drained_single_put_region_recovers_only_its_exact_committed_write() {
    let key = ObjectKey::new("data.arrow").unwrap();
    let contents = Bytes::from_static(b"sealed");
    let prefix = ObjectKey::new("cell").unwrap();
    let io = IoRegion::new();
    let store = RemoteStore::new_single_put(ControlledStore::new(Hooks::default()), prefix.clone(), io.clone());
    store.put(&key, contents.clone()).await.unwrap();
    assert!(matches!(
        store.put(&key, Bytes::from_static(b"other")).await,
        Err(Error::ConflictingObject(_))
    ));
    io.close_and_wait().await;
    assert!(!io.has_unknown_publication());

    let io = IoRegion::new();
    let provider = ControlledStore::new(Hooks { lose_put_reply: true, ..Default::default() });
    let store = RemoteStore::new_single_put(provider.clone(), prefix.clone(), io.clone());
    store.put(&key, contents.clone()).await.unwrap();
    io.close_and_wait().await;
    assert!(!io.has_unknown_publication());
    assert_eq!(paths(&provider.inner).await, ["cell/data.arrow"]);

    for (hooks, objects) in [
        (Hooks { fail_before_put: true, ..Default::default() }, Vec::<&str>::new()),
        (Hooks { lose_put_reply: true, forget_put_witness: true, ..Default::default() }, vec!["cell/data.arrow"]),
    ] {
        let io = IoRegion::new();
        let provider = ControlledStore::new(hooks);
        let store = RemoteStore::new_single_put(provider.clone(), prefix.clone(), io.clone());
        assert!(matches!(store.put(&key, contents.clone()).await, Err(Error::Remote { .. })));
        io.close_and_wait().await;
        assert!(io.has_unknown_publication());
        assert_eq!(paths(&provider.inner).await, objects);
    }
}

#[tokio::test]
async fn a_replacement_can_observe_a_closed_writers_exact_publication() {
    let key = ObjectKey::new("data.arrow").unwrap();
    let prefix = ObjectKey::new("cell").unwrap();
    let io = IoRegion::new();
    let provider = ControlledStore::new(Hooks {
        lose_put_reply: true,
        defer_publication_head: true,
        ..Default::default()
    });
    let old = RemoteStore::new_single_put(provider.clone(), prefix.clone(), io.clone());
    assert!(matches!(old.put(&key, Bytes::from_static(b"sealed")).await, Err(Error::Remote { .. })));
    io.close_and_wait().await;
    let pending = io.pending_publications();
    assert_eq!(pending.len(), 1);
    let (&attempt, retained_key) = pending.first_key_value().unwrap();
    assert_eq!(retained_key, &key);
    drop(old);
    drop(io);

    let reaper = RemoteStore::new_single_put(provider, prefix, IoRegion::new());
    assert!(reaper.publication_confirmed(retained_key, attempt).await.unwrap());
    assert!(!reaper.publication_confirmed(retained_key, uuid::Uuid::new_v4()).await.unwrap());
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
