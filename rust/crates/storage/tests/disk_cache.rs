use std::{future::{Future, poll_fn}, ops::Range, path::Path, pin::Pin, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}, task::Poll};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_storage::{Error, MemoryStore, ObjectKey, ObjectStore, ReadReservation, Result, StoredObject, cache::{BufferPool, CacheBudget}, disk_cache::DiskCache};
use tokio::sync::Semaphore;

struct ObservedStore {
    inner: MemoryStore,
    heads: AtomicUsize,
    fresh_heads: AtomicUsize,
    reads: AtomicUsize,
    fail: AtomicBool,
    hold_first: bool,
    entered: Semaphore,
    release: Semaphore,
}
impl ObservedStore {
    fn new(hold_first: bool) -> Self {
        Self { inner: MemoryStore::default(), heads: AtomicUsize::new(0), fresh_heads: AtomicUsize::new(0), reads: AtomicUsize::new(0), fail: AtomicBool::new(false), hold_first, entered: Semaphore::new(0), release: Semaphore::new(0) }
    }
}
#[async_trait]
impl ObjectStore for ObservedStore {
    fn permits_shared_read_cache(&self) -> bool { true }
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.head(key).await
    }
    async fn head_fresh(&self, key: &ObjectKey) -> Result<StoredObject> {
        self.fresh_heads.fetch_add(1, Ordering::SeqCst);
        self.inner.head_fresh(key).await
    }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes> { self.get_range(key, 0..self.head(key).await?.size).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 && self.hold_first {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        if self.fail.swap(false, Ordering::SeqCst) { return Err(Error::Task("injected download failure".into())); }
        self.inner.get_range(key, range).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, path: &Path) -> Result<()> {
        let bytes = std::fs::read(path).map_err(|source| Error::Io { path: path.to_owned(), source })?;
        self.inner.put(key, bytes.into()).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> Result<()> { self.inner.delete(key).await }
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|context| { assert!(future.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
}

#[tokio::test]
async fn buffer_pool_charges_entry_metadata_before_head_and_releases_failed_entries() {
    let store = Arc::new(ObservedStore::new(false));
    let key = ObjectKey::new("present").unwrap();
    store.put(&key, Bytes::from_static(b"data")).await.unwrap();
    let memory = MemoryBudget::new(4096);
    let cache = BufferPool::with_metadata_budget(store.clone(), CacheBudget::new(4), Arc::clone(&memory));
    let pressure = memory.reserve(memory.limit_bytes()).unwrap();
    assert!(matches!(cache.get(&key).await, Err(Error::MemoryLimit { requested, limit: 4096, retained: 4096 }) if requested > key.as_str().len()));
    assert_eq!(store.heads.load(Ordering::SeqCst), 0);
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    assert_eq!(cache.retained_bytes(), 0);
    assert!(!cache.evict(&key));
    drop(pressure);

    for path in ["missing/one", "missing/two"] {
        let absent = ObjectKey::new(path).unwrap();
        assert!(matches!(cache.get(&absent).await, Err(Error::MissingObject(_))));
        assert!(!cache.evict(&absent));
        assert_eq!(memory.used_bytes(), 0);
    }
    store.fail.store(true, Ordering::SeqCst);
    assert!(matches!(cache.get(&key).await, Err(Error::Task(message)) if message == "injected download failure"));
    assert!(!cache.evict(&key));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(cache.retained_bytes(), 0);

    let lease = cache.get(&key).await.unwrap();
    let entry_bytes = memory.used_bytes();
    assert!(entry_bytes > key.as_str().len());
    let pressure = memory.reserve(memory.limit_bytes() - entry_bytes).unwrap();
    assert_eq!(cache.get(&key).await.unwrap().as_slice(), b"data");
    assert_eq!(memory.used_bytes(), entry_bytes + pressure.bytes());
    assert_eq!(store.heads.load(Ordering::SeqCst), 4);
    assert_eq!(store.reads.load(Ordering::SeqCst), 2);
    let slice = lease.slice(1..2).unwrap();
    drop(lease);
    assert!(cache.evict(&key));
    assert_eq!(memory.used_bytes(), pressure.bytes());
    drop(pressure);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(cache.retained_bytes(), 4);
    assert_eq!(slice.as_slice(), b"a");
    drop(slice);
    assert_eq!(cache.retained_bytes(), 0);
}

#[tokio::test]
async fn buffer_pool_shares_one_admitted_active_entry_between_fetchers() {
    let store = Arc::new(ObservedStore::new(true));
    let key = ObjectKey::new("data").unwrap();
    store.put(&key, Bytes::from_static(b"data")).await.unwrap();
    let memory = MemoryBudget::new(4096);
    let cache = BufferPool::with_metadata_budget(store.clone(), CacheBudget::new(4), Arc::clone(&memory));
    let mut loader = Box::pin(cache.get(&key));
    assert_pending(loader.as_mut()).await;
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    let entry_bytes = memory.used_bytes();
    assert!(entry_bytes > 0);
    let pressure = memory.reserve(memory.limit_bytes() - entry_bytes).unwrap();

    let mut waiter = Box::pin(cache.get(&key));
    assert_pending(waiter.as_mut()).await;
    assert_eq!(memory.used_bytes(), pressure.bytes() + entry_bytes);
    assert_eq!(cache.retained_bytes(), 4);
    assert!(!cache.evict(&key));
    let other = ObjectKey::new("next").unwrap();
    assert!(matches!(cache.get(&other).await, Err(Error::MemoryLimit { limit: 4096, .. })));
    assert!(!cache.evict(&other));
    assert_eq!(store.heads.load(Ordering::SeqCst), 1);
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);

    store.release.add_permits(1);
    let (first, second) = tokio::join!(loader, waiter);
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.as_slice(), b"data");
    assert_eq!(second.as_slice(), b"data");
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(memory.used_bytes(), pressure.bytes() + entry_bytes);
    assert!(cache.evict(&key));
    assert_eq!(memory.used_bytes(), pressure.bytes());
    drop(pressure);
    assert_eq!(memory.used_bytes(), 0);
    drop(first);
    assert_eq!(cache.retained_bytes(), 4);
    drop(second);
    assert_eq!(cache.retained_bytes(), 0);
}

#[tokio::test]
async fn buffer_pool_retires_a_cancelled_empty_entry_after_its_last_fetcher() {
    for loader_first in [false, true] {
        let store = Arc::new(ObservedStore::new(true));
        let key = ObjectKey::new("data").unwrap();
        store.put(&key, Bytes::from_static(b"data")).await.unwrap();
        let memory = MemoryBudget::new(4096);
        let cache = BufferPool::with_metadata_budget(store.clone(), CacheBudget::new(4), Arc::clone(&memory));
        let mut loader = Box::pin(cache.get(&key));
        assert_pending(loader.as_mut()).await;
        assert_eq!(store.reads.load(Ordering::SeqCst), 1);
        let entry_bytes = memory.used_bytes();
        assert!(entry_bytes > 0);
        let mut waiter = Box::pin(cache.get(&key));
        assert_pending(waiter.as_mut()).await;
        assert_eq!(cache.retained_bytes(), 4);

        let (first, second) = if loader_first { (loader, waiter) } else { (waiter, loader) };
        drop(first);
        assert_eq!(memory.used_bytes(), entry_bytes);
        assert_eq!(cache.retained_bytes(), if loader_first { 0 } else { 4 });
        assert!(!cache.evict(&key));
        drop(second);
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(cache.retained_bytes(), 0);
        assert!(!cache.evict(&key));

        assert_eq!(cache.get(&key).await.unwrap().as_slice(), b"data");
        assert_eq!(store.reads.load(Ordering::SeqCst), 2);
        assert_eq!(memory.used_bytes(), entry_bytes);
        drop(cache);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn buffer_pool_reclaims_only_the_oldest_idle_entry_under_metadata_pressure() {
    let store = Arc::new(MemoryStore::default());
    let keys = ["000", "111", "222"].map(|name| ObjectKey::new(name).unwrap());
    for key in &keys { store.put(key, Bytes::new()).await.unwrap(); }
    let [first, second, third] = &keys;

    let memory = MemoryBudget::new(4096);
    let probe = BufferPool::with_metadata_budget(store.clone(), CacheBudget::new(0), Arc::clone(&memory));
    assert!(probe.get(first).await.unwrap().as_slice().is_empty());
    let entry_bytes = memory.used_bytes();
    assert!(entry_bytes > 0);
    drop(probe);
    assert_eq!(memory.used_bytes(), 0);

    let memory = MemoryBudget::new(entry_bytes);
    let cache = BufferPool::with_metadata_budget(store.clone(), CacheBudget::new(0), Arc::clone(&memory));
    assert!(cache.get(first).await.unwrap().as_slice().is_empty());
    assert!(cache.get(second).await.unwrap().as_slice().is_empty());
    assert!(!cache.evict(first));
    assert_eq!(cache.retained_bytes(), 0);
    assert_eq!(memory.used_bytes(), entry_bytes);
    assert!(cache.evict(second));
    assert_eq!(memory.used_bytes(), 0);

    let memory = MemoryBudget::new(entry_bytes * 2);
    let cache = BufferPool::with_metadata_budget(store, CacheBudget::new(0), Arc::clone(&memory));
    for key in [first, second, first, third] {
        assert!(cache.get(key).await.unwrap().as_slice().is_empty());
    }
    assert!(!cache.evict(second));
    assert_eq!(memory.used_bytes(), entry_bytes * 2);
    assert_eq!(cache.retained_bytes(), 0);
    assert!(cache.evict(first));
    assert!(cache.evict(third));
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn pinned_files_survive_over_capacity_and_release_evicts_them() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 4, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let store = MemoryStore::default();
    let first = ObjectKey::new("db/0/first").unwrap();
    let second = ObjectKey::new("db/0/second").unwrap();
    store.put(&first, Bytes::from_static(b"123456")).await.unwrap();
    store.put(&second, Bytes::from_static(b"abc")).await.unwrap();
    let a = cache.get(&first, &store, &first).await.unwrap();
    let b = cache.get(&second, &store, &second).await.unwrap();
    assert_eq!(cache.stats().pinned_bytes, 9);
    assert_eq!(std::fs::read(a.path()).unwrap(), b"123456");
    let first_path = a.path().to_owned();
    let second_path = b.path().to_owned();
    drop(b);
    assert!(!second_path.exists());
    assert!(first_path.exists());
    drop(a);
    assert!(!first_path.exists());
    assert_eq!(cache.stats().pinned_bytes + cache.stats().evictable_bytes, 0);
}

#[tokio::test]
async fn concurrent_readers_share_the_download_and_each_keeps_the_file_pinned() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 1, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let store = Arc::new(ObservedStore::new(true));
    let key = ObjectKey::new("object").unwrap();
    store.put(&key, Bytes::from_static(b"data")).await.unwrap();
    let first = { let (cache, store, key) = (cache.clone(), store.clone(), key.clone()); tokio::spawn(async move { cache.get(&key, store.as_ref(), &key).await }) };
    store.entered.acquire().await.unwrap().forget();
    let second = cache.get(&key, store.as_ref(), &key);
    let release = async { store.release.add_permits(1); };
    let (second, ()) = tokio::join!(second, release);
    let a = first.await.unwrap().unwrap();
    let b = second.unwrap();
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    assert_eq!(a.path(), b.path());
    let path = a.path().to_owned();
    drop(a);
    assert!(path.exists());
    drop(b);
    assert!(!path.exists());
}

#[tokio::test]
async fn failed_and_cancelled_downloads_release_metadata_and_can_retry() {
    use std::{future::{Future, poll_fn}, task::Poll};
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let cache = DiskCache::open(directory.path(), 100, memory.clone()).unwrap();
    let store = ObservedStore::new(true);
    let key = ObjectKey::new("object").unwrap();
    store.put(&key, Bytes::from_static(b"data")).await.unwrap();
    let mut pending = Box::pin(cache.get(&key, &store, &key));
    poll_fn(|context| { assert!(pending.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    store.entered.try_acquire().unwrap().forget();
    assert!(memory.used_bytes() > 0);
    drop(pending);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(std::fs::read_dir(directory.path().join(".tmp")).unwrap().filter(|entry| entry.as_ref().unwrap().file_name() != ".owner-lock").count(), 0);
    store.fail.store(true, Ordering::SeqCst);
    assert!(cache.get(&key, &store, &key).await.is_err());
    assert_eq!(memory.used_bytes(), 0);
    let lease = cache.get(&key, &store, &key).await.unwrap();
    assert_eq!(std::fs::read(lease.path()).unwrap(), b"data");
    assert_eq!(store.reads.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn complete_files_are_reused_after_reopen_without_remote_access() {
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let store = ObservedStore::new(false);
    let key = ObjectKey::new("db/0/table/file").unwrap();
    store.put(&key, Bytes::from_static(b"persistent cache")).await.unwrap();
    {
        let cache = DiskCache::open(directory.path(), 100, memory.clone()).unwrap();
        drop(cache.get(&key, &store, &key).await.unwrap());
    }
    assert_eq!(memory.used_bytes(), 0);
    store.delete(&key).await.unwrap();
    let cache = DiskCache::open(directory.path(), 100, memory).unwrap();
    let lease = cache.get(&key, &store, &key).await.unwrap();
    assert_eq!(std::fs::read(lease.path()).unwrap(), b"persistent cache");
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn database_scopes_keep_equal_remote_keys_separate_and_return_charged_ranges() {
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let cache = DiskCache::open(directory.path(), 100, memory.clone()).unwrap();
    let first = Arc::new(MemoryStore::default());
    let second = Arc::new(MemoryStore::default());
    let key = ObjectKey::new("same").unwrap();
    first.put(&key, Bytes::from_static(b"first")).await.unwrap();
    second.put(&key, Bytes::from_static(b"other")).await.unwrap();
    let a = cache.scope(ObjectKey::new("first/0").unwrap(), first).unwrap();
    let b = cache.scope(ObjectKey::new("second/0").unwrap(), second).unwrap();
    let first = a.get_range(&key, 1..4).await.unwrap();
    assert_eq!(first.as_ref(), b"irs");
    assert_eq!(b.get(&key).await.unwrap().as_ref(), b"other");
    let retained = memory.used_bytes();
    let cloned = first.clone();
    drop(first);
    assert_eq!(memory.used_bytes(), retained);
    drop(cloned);
    assert_eq!(memory.used_bytes(), retained - 3);
    assert!(matches!(a.get_range(&key, 0..6).await, Err(Error::InvalidRange { .. })));
    assert!(a.get_reserved_range(&key, 0..4, ReadReservation::new(3, ())).await.is_err());
}

#[tokio::test]
async fn a_fresh_read_has_no_durable_cache_dependency() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let remote = Arc::new(MemoryStore::default());
    let scope = ObjectKey::new("db/0").unwrap();
    let store = cache.scope(scope, remote.clone()).unwrap();
    let key = ObjectKey::new("derived/page").unwrap();
    let durable_copy = directory.path().join("db/0/derived/page");
    remote.put(&key, Bytes::from_static(b"page")).await.unwrap();

    assert_eq!(store.get_fresh(&key).await.unwrap().as_ref(), b"page");
    assert!(!durable_copy.exists());

    assert_eq!(store.get(&key).await.unwrap().as_ref(), b"page");
    assert!(durable_copy.exists());
    remote.delete(&key).await.unwrap();
    assert!(matches!(store.get_fresh(&key).await, Err(Error::MissingObject(_))));
}

#[tokio::test]
async fn successful_file_upload_warms_cache_and_deleting_through_the_store_drops_the_cached_copy() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let remote = Arc::new(ObservedStore::new(false));
    let store = cache.scope(ObjectKey::new("db/0").unwrap(), remote.clone()).unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"arrow file").unwrap();
    let key = ObjectKey::new("table/data.arrow").unwrap();
    store.put_file(&key, source.path()).await.unwrap();
    assert!(source.path().exists());
    assert_eq!(store.get(&key).await.unwrap().as_ref(), b"arrow file");
    assert_eq!(remote.reads.load(Ordering::SeqCst), 0, "the upload warmed the cache");
    store.delete(&key).await.unwrap();
    assert!(store.list(None).await.unwrap().is_empty());
    assert!(store.get(&key).await.is_err(), "a deleted object is no longer served from the cache");
    assert!(!directory.path().join("db/0/table/data.arrow").exists());
}

#[tokio::test]
async fn an_invalidated_file_outlives_its_leases_and_nothing_else() {
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let store = ObservedStore::new(false);
    let key = ObjectKey::new("db/0/table/file").unwrap();
    store.put(&key, Bytes::from_static(b"erased later")).await.unwrap();
    {
        let cache = DiskCache::open(directory.path(), 100, memory.clone()).unwrap();
        let lease = cache.get(&key, &store, &key).await.unwrap();
        cache.invalidate(&key).unwrap();
        assert!(!directory.path().join("db/0/table/file").exists());
        assert!(lease.path().starts_with(directory.path().canonicalize().unwrap().join(".tmp")));
        assert_eq!(std::fs::read(lease.path()).unwrap(), b"erased later", "a reader holding the lease keeps reading");
        let retired = lease.path();
        drop(lease);
        assert!(!retired.exists(), "the last lease takes the file with it");
        drop(cache.get(&key, &store, &key).await.unwrap());
        assert_eq!(store.reads.load(Ordering::SeqCst), 2, "the next read goes to the store");
        cache.invalidate(&key).unwrap();
    }
    store.delete(&key).await.unwrap();
    let cache = DiskCache::open(directory.path(), 100, memory).unwrap();
    assert!(cache.get(&key, &store, &key).await.is_err(), "an invalidated file is not rediscovered after a restart");
}

#[tokio::test]
async fn an_invalidated_loader_cannot_unlink_the_replacement_file() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let store = ObservedStore::new(true);
    let key = ObjectKey::new("db/0/table/file").unwrap();
    store.put(&key, Bytes::from_static(b"data")).await.unwrap();
    let mut old = Box::pin(cache.get(&key, &store, &key));
    assert_pending(old.as_mut()).await;
    cache.invalidate(&key).unwrap();
    let fresh = cache.get(&key, &store, &key).await.unwrap();
    store.release.add_permits(1);
    let retired = old.await.unwrap();
    let retired_path = retired.path();
    assert_ne!(retired_path, fresh.path());
    drop(retired);
    assert!(!retired_path.exists());
    assert_eq!(std::fs::read(fresh.path()).unwrap(), b"data");
}

#[tokio::test]
async fn invalidating_a_buffer_pool_entry_keeps_open_leases_and_misses_afterwards() {
    let remote = Arc::new(MemoryStore::default());
    let pool = BufferPool::new(remote.clone(), 1 << 20);
    let key = ObjectKey::new("tables/docs/data/l01-rc-b00.arrow").unwrap();
    remote.put(&key, Bytes::from_static(b"data")).await.unwrap();
    let lease = pool.get(&key).await.unwrap();
    remote.delete(&key).await.unwrap();
    assert_eq!(pool.get(&key).await.unwrap().as_slice(), b"data", "until invalidated, the pool serves its copy");
    pool.invalidate(&key).await.unwrap();
    assert_eq!(lease.as_slice(), b"data");
    assert!(pool.get(&key).await.is_err());
}

#[tokio::test]
async fn fresh_head_observes_provider_deletion_while_cached_reads_survive() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let database = rtdb_storage::IoRegion::new();
    let remote = Arc::new(ObservedStore::new(false));
    let store = cache.scope_in(ObjectKey::new("db/0").unwrap(), remote.clone(), database.clone()).unwrap();
    let key = ObjectKey::new("table/data.arrow").unwrap();
    remote.put(&key, Bytes::from_static(b"data")).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap().as_ref(), b"data");
    let heads = remote.heads.load(Ordering::SeqCst);
    assert_eq!(remote.reads.load(Ordering::SeqCst), 1);

    let cached = store.head(&key).await.unwrap();
    assert_eq!(store.head_fresh(&key).await.unwrap(), cached);
    remote.delete(&key).await.unwrap();
    assert!(matches!(store.head_fresh(&key).await, Err(Error::MissingObject(missing)) if missing == key.as_str()));
    assert_eq!(store.head(&key).await.unwrap(), cached);
    assert_eq!(store.get(&key).await.unwrap().as_ref(), b"data");
    assert_eq!(remote.heads.load(Ordering::SeqCst), heads);
    assert_eq!(remote.reads.load(Ordering::SeqCst), 1);

    database.close_and_wait().await;
    assert!(matches!(store.head_fresh(&key).await, Err(Error::Closed(_))));
    assert_eq!(remote.fresh_heads.load(Ordering::SeqCst), 2);
    cache.close_and_wait().await;
}

#[test]
fn cancelled_blocking_read_holds_disk_lease_and_read_charge_until_io_finishes() {
    use std::{future::{Future, poll_fn}, task::Poll};
    struct Charge { reservation: Option<Arc<rtdb_columnar::MemoryReservation>>, released: Option<tokio::sync::oneshot::Sender<()>> }
    impl Drop for Charge {
        fn drop(&mut self) { drop(self.reservation.take()); if let Some(released) = self.released.take() { let _ = released.send(()); } }
    }
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(directory.path().join("db/0")).unwrap();
    std::fs::write(directory.path().join("db/0/object"), b"retained").unwrap();
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let cache = DiskCache::open(directory.path(), 8, memory.clone()).unwrap();
    let database = rtdb_storage::IoRegion::new();
    let store = cache.scope_in(ObjectKey::new("db/0").unwrap(), Arc::new(MemoryStore::default()), database.clone()).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || { started.send(()).unwrap(); paused.recv().unwrap(); });
        waiting.await.unwrap();
        let key = ObjectKey::new("object").unwrap();
        let retained = memory.used_bytes();
        let (released, mut release) = tokio::sync::oneshot::channel();
        let charge = Charge { reservation: Some(memory.reserve(8).unwrap()), released: Some(released) };
        let mut read = Box::pin(store.get_reserved_range(&key, 0..8, ReadReservation::new(8, charge)));
        poll_fn(|context| { assert!(read.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        drop(read);
        assert_eq!(cache.stats().pinned_bytes, 8);
        assert_eq!(memory.used_bytes(), retained + 8);
        let mut database_close = Box::pin(database.close_and_wait());
        poll_fn(|context| { assert!(database_close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(store.head(&key).await, Err(Error::Closed(_))));
        let mut close = Box::pin(cache.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(store.get(&key).await, Err(Error::Closed(_))));
        resume.send(()).unwrap();
        blocker.await.unwrap();
        database_close.await;
        close.await;
        assert_eq!(release.try_recv(), Ok(()));
        assert_eq!(cache.stats().pinned_bytes, 0);
        assert_eq!(memory.used_bytes(), retained);
    });
}

#[cfg(unix)]
#[test]
fn database_close_joins_cancelled_cache_publication_after_provider_completion() {
    use std::{future::{Future, poll_fn}, task::Poll};
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let database = rtdb_storage::IoRegion::new();
    let sibling = rtdb_storage::IoRegion::new();
    let remote = Arc::new(ObservedStore::new(false));
    let store = cache.scope_in(ObjectKey::new("db/0").unwrap(), remote.clone(), database.clone()).unwrap();
    let other = cache.scope_in(ObjectKey::new("other/0").unwrap(), Arc::new(MemoryStore::default()), sibling.clone()).unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"published").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, finish) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || { entered.send(()).unwrap(); finish.recv().unwrap(); });
        started.await.unwrap();
        let key = ObjectKey::new("object").unwrap();
        let mut upload = Box::pin(store.put_file(&key, source.path()));
        poll_fn(|context| { assert!(upload.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert_eq!(remote.head(&key).await.unwrap().size, 9);
        assert!(!directory.path().join("db/0/object").exists());
        drop(upload);
        drop(source);
        let mut close = Box::pin(database.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        other.put(&key, Bytes::from_static(b"sibling")).await.unwrap();
        assert_eq!(other.head(&key).await.unwrap().size, 7);
        release.send(()).unwrap();
        blocker.await.unwrap();
        close.await;
        assert_eq!(std::fs::read(directory.path().join("db/0/object")).unwrap(), b"published");
        assert!(matches!(store.head(&key).await, Err(Error::Closed(_))));
        assert_eq!(other.get(&key).await.unwrap().as_ref(), b"sibling");
        sibling.close_and_wait().await;
        cache.close_and_wait().await;
    });
}

#[tokio::test]
async fn close_cancels_the_loading_caller_and_joins_temporary_cleanup() {
    use std::{future::{Future, poll_fn}, task::Poll};
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(4 * 1024 * 1024);
    let cache = DiskCache::open(directory.path(), 100, memory.clone()).unwrap(); let store = ObservedStore::new(true);
    let key = ObjectKey::new("object").unwrap(); store.put(&key, Bytes::from_static(b"held")).await.unwrap();
    let mut read = Box::pin(cache.get(&key, &store, &key));
    poll_fn(|context| { assert!(read.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
    store.entered.try_acquire().unwrap().forget();
    let ((), read) = tokio::join!(cache.close_and_wait(), read);
    assert!(matches!(read, Err(Error::Closed(_)))); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(std::fs::read_dir(directory.path().join(".tmp")).unwrap().count(), 1);
}

#[tokio::test]
async fn directory_ownership_precedes_crash_staging_cleanup() {
    let directory = tempfile::tempdir().unwrap(); let staging = directory.path().join(".tmp");
    std::fs::create_dir_all(staging.join("abandoned-directory")).unwrap();
    std::fs::write(staging.join("abandoned-directory/part"), b"incomplete").unwrap(); std::fs::write(staging.join("abandoned-file"), b"incomplete").unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).unwrap();
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 1);
    std::fs::write(staging.join("active-file"), b"active").unwrap();
    assert!(DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).is_err());
    assert!(staging.join("active-file").exists());
    cache.close_and_wait().await; drop(cache);
    let reopened = DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).unwrap();
    assert!(!staging.join("active-file").exists()); reopened.close_and_wait().await;
}

#[tokio::test]
async fn download_chunks_leave_room_for_metadata_and_requested_range_under_small_budgets() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(64 * 1024);
    let cache = DiskCache::open(directory.path(), 1 << 20, memory.clone()).unwrap(); let remote = Arc::new(MemoryStore::default());
    let key = ObjectKey::new("large-object").unwrap(); remote.put(&key, Bytes::from(vec![7; 256 * 1024])).await.unwrap();
    let store = cache.scope(ObjectKey::new("db/0").unwrap(), remote).unwrap();
    assert_eq!(store.get_range(&key, 0..1).await.unwrap().as_ref(), &[7]);
    assert!(memory.used_bytes() < memory.limit_bytes()); cache.close_and_wait().await;
}

#[test]
fn staging_directory_and_owner_lock_reject_non_directory_and_non_file_paths() {
    let directory = tempfile::tempdir().unwrap();
    let temporary = directory.path().join(".tmp");
    std::fs::write(&temporary, b"preserved").unwrap();
    assert!(DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).is_err());
    assert_eq!(std::fs::read(&temporary).unwrap(), b"preserved");
    std::fs::remove_file(&temporary).unwrap();
    std::fs::create_dir_all(temporary.join(".owner-lock")).unwrap();
    std::fs::write(temporary.join("unclaimed"), b"preserved").unwrap();
    assert!(DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).is_err());
    assert!(temporary.join(".owner-lock").is_dir());
    assert_eq!(std::fs::read(temporary.join("unclaimed")).unwrap(), b"preserved");
}

#[cfg(unix)]
#[test]
fn staging_symlink_cannot_redirect_startup_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("preserved"), b"outside cache").unwrap();
    std::os::unix::fs::symlink(outside.path(), directory.path().join(".tmp")).unwrap();
    assert!(DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).is_err());
    assert_eq!(std::fs::read(outside.path().join("preserved")).unwrap(), b"outside cache");
    assert!(!outside.path().join(".owner-lock").exists());
}

#[cfg(unix)]
#[test]
fn lock_symlink_is_rejected_before_orphan_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path().join("lock");
    std::fs::write(&target, b"outside cache").unwrap();
    let temporary = directory.path().join(".tmp");
    std::fs::create_dir(&temporary).unwrap();
    std::fs::write(temporary.join("unclaimed"), b"preserved").unwrap();
    std::os::unix::fs::symlink(&target, temporary.join(".owner-lock")).unwrap();
    assert!(DiskCache::open(directory.path(), 100, MemoryBudget::new(1 << 20)).is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"outside cache");
    assert_eq!(std::fs::read(temporary.join("unclaimed")).unwrap(), b"preserved");
}

#[tokio::test]
async fn a_local_file_of_a_cached_object_is_pinned_while_held() {
    use std::io::Read;
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 4, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let remote = Arc::new(MemoryStore::default());
    let key = ObjectKey::new("tables/docs/first").unwrap();
    remote.put(&key, Bytes::from_static(b"123456")).await.unwrap();
    let store = cache.scope(ObjectKey::new("db").unwrap(), remote).unwrap();
    let local = store.local_file(&key).await.unwrap().expect("a disk-cached store keeps a local file");
    let mut bytes = Vec::new();
    local.file().try_clone().unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"123456");
    assert_eq!(cache.stats().pinned_bytes, 6, "held over capacity, as any lease is");
    drop(local);
    assert_eq!(cache.stats().pinned_bytes + cache.stats().evictable_bytes, 0);
    assert!(matches!(store.local_file(&ObjectKey::new("tables/docs/absent").unwrap()).await, Err(Error::MissingObject(_))));
}

#[tokio::test]
async fn a_cold_scan_range_does_not_fill_the_disk_cache() {
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(8 << 20);
    let cache = DiskCache::open(directory.path(), 8 << 20, Arc::clone(&memory)).unwrap();
    let remote = Arc::new(ObservedStore::new(false));
    let key = ObjectKey::new("tables/docs/data/file.arrow").unwrap();
    let mut bytes = vec![0; 2 << 20];
    bytes[123..127].copy_from_slice(b"page");
    remote.put(&key, bytes.into()).await.unwrap();
    let store = cache.scope(ObjectKey::new("db").unwrap(), remote.clone()).unwrap();
    let pool = BufferPool::new(Arc::clone(&store), 1 << 20);

    let page = pool.scan_range(&key, 123..127, &memory).await.unwrap();
    assert_eq!(page.as_slice(), b"page");
    assert_eq!(remote.reads.load(Ordering::SeqCst), 1);
    assert_eq!(cache.stats().pinned_bytes + cache.stats().evictable_bytes, 0);

    drop(store.local_file(&key).await.unwrap());
    let previously_read = remote.reads.load(Ordering::SeqCst);
    let page = pool.scan_range(&key, 123..127, &memory).await.unwrap();
    assert_eq!(remote.reads.load(Ordering::SeqCst), previously_read);
    assert_eq!(cache.stats().pinned_bytes, 2 << 20);
    store.forget(&key).await.unwrap();
    assert_eq!(page.as_slice(), b"page");
}
