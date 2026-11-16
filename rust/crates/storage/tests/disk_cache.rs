use std::{ops::Range, path::Path, sync::{Arc, atomic::{AtomicBool, AtomicUsize, Ordering}}};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_storage::{Error, MemoryStore, ObjectKey, ObjectStore, ReadReservation, Result, StoredObject, disk_cache::DiskCache};
use tokio::sync::Semaphore;

struct ObservedStore {
    inner: MemoryStore,
    reads: AtomicUsize,
    fail: AtomicBool,
    hold_first: bool,
    entered: Semaphore,
    release: Semaphore,
}
impl ObservedStore {
    fn new(hold_first: bool) -> Self {
        Self { inner: MemoryStore::default(), reads: AtomicUsize::new(0), fail: AtomicBool::new(false), hold_first, entered: Semaphore::new(0), release: Semaphore::new(0) }
    }
}
#[async_trait]
impl ObjectStore for ObservedStore {
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> { self.inner.head(key).await }
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
    async fn put_file(&self, key: &ObjectKey, path: &Path) -> Result<()> { self.inner.put_file(key, path).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> Result<()> { self.inner.delete(key).await }
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
async fn successful_file_upload_warms_cache_and_remote_deletion_preserves_cached_reads() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 100, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let remote = Arc::new(ObservedStore::new(false));
    let store = cache.scope(ObjectKey::new("db/0").unwrap(), remote.clone()).unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), b"arrow file").unwrap();
    let key = ObjectKey::new("table/data.arrow").unwrap();
    store.put_file(&key, source.path()).await.unwrap();
    assert!(source.path().exists());
    store.delete(&key).await.unwrap();
    assert!(store.list(None).await.unwrap().is_empty());
    assert_eq!(store.get(&key).await.unwrap().as_ref(), b"arrow file");
    assert_eq!(remote.reads.load(Ordering::SeqCst), 0);
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
    let store = cache.scope(ObjectKey::new("db/0").unwrap(), Arc::new(MemoryStore::default())).unwrap();
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
        let mut close = Box::pin(cache.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(store.get(&key).await, Err(Error::Closed(_))));
        resume.send(()).unwrap();
        blocker.await.unwrap();
        close.await;
        assert_eq!(release.try_recv(), Ok(()));
        assert_eq!(cache.stats().pinned_bytes, 0);
        assert_eq!(memory.used_bytes(), retained);
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
