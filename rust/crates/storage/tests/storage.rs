use bytes::Bytes;
use rtdb_storage::{Error, LocalStore, MemoryStore, ObjectKey, ObjectStore, cache::{BufferPool, CacheBudget}};
use std::sync::Arc;

async fn immutable_objects(store: Arc<dyn ObjectStore>) {
    let key = ObjectKey::new("tables/products/data/l00-r00.arrow").unwrap();
    store.put(&key, Bytes::from_static(b"abcdef")).await.unwrap();
    store.put(&key, Bytes::from_static(b"abcdef")).await.unwrap();
    assert!(matches!(store.put(&key, Bytes::from_static(b"wrong")).await, Err(Error::ConflictingObject(_))));
    assert_eq!(store.get(&key).await.unwrap().as_ref(), b"abcdef");
    assert_eq!(store.get_range(&key, 1..4).await.unwrap().as_ref(), b"bcd");
    assert!(matches!(store.get_range(&key, 0..7).await, Err(Error::InvalidRange { .. })));
    let copied = ObjectKey::new("tables/products/data/copy.arrow").unwrap();
    store.copy(&key, &copied).await.unwrap();
    assert_eq!(store.get(&copied).await.unwrap(), store.get(&key).await.unwrap());
    store.delete(&key).await.unwrap();
    store.delete(&key).await.unwrap();
    assert!(matches!(store.get(&key).await, Err(Error::MissingObject(_))));
    assert_eq!(store.list(None).await.unwrap()[0].key, copied);
}

#[tokio::test]
async fn memory_objects_are_immutable_and_ranges_are_exact() {
    immutable_objects(Arc::new(MemoryStore::default())).await;
}

#[tokio::test]
async fn local_objects_are_immutable_and_ranges_are_exact() {
    let directory = tempfile::tempdir().unwrap();
    immutable_objects(Arc::new(LocalStore::open(directory.path()).unwrap())).await;
}

async fn immutable_file_upload(store: Arc<dyn ObjectStore>) {
    let source = tempfile::tempdir().unwrap();
    let path = source.path().join("source.arrow");
    let key = ObjectKey::new("tables/products/data/file.arrow").unwrap();
    let mut data = vec![73u8; 2 * 1024 * 1024 + 19];
    data[65_536] = 4;
    std::fs::write(&path, &data).unwrap();
    store.put_file(&key, &path).await.unwrap();
    store.put_file(&key, &path).await.unwrap();
    assert_eq!(store.get(&key).await.unwrap().as_ref(), data.as_slice());
    data[65_536] = 5;
    std::fs::write(&path, &data).unwrap();
    assert!(matches!(store.put_file(&key, &path).await, Err(Error::ConflictingObject(_))));
    assert_eq!(store.get_range(&key, 65_536..65_537).await.unwrap().as_ref(), &[4]);
    let empty = ObjectKey::new("empty").unwrap();
    std::fs::write(&path, []).unwrap();
    store.put_file(&empty, &path).await.unwrap();
    store.put_file(&empty, &path).await.unwrap();
    assert_eq!(store.head(&empty).await.unwrap().size, 0);
    assert_eq!(store.list(None).await.unwrap().len(), 2);
}

#[tokio::test]
async fn local_streamed_publication_accepts_exact_retries_and_rejects_same_size_conflicts() {
    let directory = tempfile::tempdir().unwrap();
    immutable_file_upload(Arc::new(LocalStore::open(directory.path()).unwrap())).await;
}

#[tokio::test]
async fn memory_file_publication_has_the_same_immutable_contract() {
    immutable_file_upload(Arc::new(MemoryStore::default())).await;
}

#[tokio::test]
async fn concurrent_local_file_publishers_never_replace_the_winner() {
    let source = tempfile::tempdir().unwrap();
    let first = source.path().join("first");
    let second = source.path().join("second");
    std::fs::write(&first, vec![1u8; 1 << 20]).unwrap();
    std::fs::write(&second, vec![2u8; 1 << 20]).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let store = LocalStore::open(directory.path()).unwrap();
    let key = ObjectKey::new("winner").unwrap();
    let (a, b) = tokio::join!(store.put_file(&key, &first), store.put_file(&key, &second));
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let winner = if a.is_ok() { 1 } else { 2 };
    assert!(store.get(&key).await.unwrap().iter().all(|byte| *byte == winner));
    let error = if let Err(error) = a { error } else { b.unwrap_err() };
    assert!(matches!(error, Error::ConflictingObject(_)));
}

#[tokio::test]
async fn directory_listing_does_not_match_a_sibling_prefix() {
    let store = MemoryStore::default();
    store.put(&ObjectKey::new("a/b").unwrap(), Bytes::new()).await.unwrap();
    store.put(&ObjectKey::new("ab/c").unwrap(), Bytes::new()).await.unwrap();
    let objects = store.list(Some(&ObjectKey::new("a").unwrap())).await.unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key.as_str(), "a/b");
}

#[test]
fn keys_are_relative_canonical_paths() {
    for key in ["", "/absolute", "../outside", "a/../b", "a//b", "a/./b", "a\\b", ".tmp/a", "a\0b"] {
        assert!(ObjectKey::new(key).is_err(), "{key:?}");
    }
}

#[tokio::test]
async fn retained_slices_keep_their_charge_after_eviction() {
    let store = Arc::new(MemoryStore::default());
    let key = ObjectKey::new("one").unwrap();
    let second = ObjectKey::new("two").unwrap();
    store.put(&key, Bytes::from_static(b"1234")).await.unwrap();
    store.put(&second, Bytes::from_static(b"5678")).await.unwrap();
    let cache = BufferPool::new(store, 4);
    let lease = cache.get(&key).await.unwrap();
    let slice = lease.slice(1..2).unwrap();
    drop(lease);
    assert!(cache.evict(&key));
    assert_eq!(cache.retained_bytes(), 4);
    assert!(matches!(cache.get(&second).await, Err(Error::MemoryLimit { .. })));
    assert_eq!(slice.as_slice(), b"2");
    drop(slice);
    assert_eq!(cache.retained_bytes(), 0);
    assert_eq!(cache.get(&second).await.unwrap().as_slice(), b"5678");
}

#[tokio::test]
async fn cache_failure_does_not_poison_later_fetches() {
    let store = Arc::new(MemoryStore::default());
    let key = ObjectKey::new("later").unwrap();
    let cache = BufferPool::new(store.clone(), 8);
    assert!(matches!(cache.get(&key).await, Err(Error::MissingObject(_))));
    store.put(&key, Bytes::from_static(b"present")).await.unwrap();
    assert_eq!(cache.get(&key).await.unwrap().as_slice(), b"present");
}

#[tokio::test]
async fn local_objects_survive_reopening() {
    let directory = tempfile::tempdir().unwrap();
    let key = ObjectKey::new("deep/tree/object").unwrap();
    {
        let store = LocalStore::open(directory.path()).unwrap();
        store.put(&key, Bytes::from_static(b"persisted")).await.unwrap();
    }
    let reopened = LocalStore::open(directory.path()).unwrap();
    assert_eq!(reopened.get(&key).await.unwrap().as_ref(), b"persisted");
}

#[test]
fn cancelled_local_reads_retain_the_charge_until_the_blocking_read_finishes() {
    use rtdb_storage::ReadReservation;
    use std::{future::{Future, poll_fn}, sync::atomic::{AtomicBool, Ordering}, task::Poll};
    use tokio::sync::oneshot;

    struct Charge {
        retained: Arc<AtomicBool>,
        released: Option<oneshot::Sender<()>>,
    }

    impl Drop for Charge {
        fn drop(&mut self) {
            self.retained.store(false, Ordering::Release);
            if let Some(released) = self.released.take() { let _ = released.send(()); }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("object"), b"retained").unwrap();
    let store = LocalStore::open(directory.path()).unwrap();
    let key = ObjectKey::new("object").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let (started, waiting) = oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            paused.recv().unwrap();
        });
        waiting.await.unwrap();
        let retained = Arc::new(AtomicBool::new(true));
        let (released, release) = oneshot::channel();
        let reservation = ReadReservation::new(8, Charge { retained: retained.clone(), released: Some(released) });
        let mut read = Box::pin(store.get_reserved_range(&key, 0..8, reservation));
        poll_fn(|context| {
            assert!(read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        drop(read);
        assert!(retained.load(Ordering::Acquire));
        resume.send(()).unwrap();
        blocker.await.unwrap();
        release.await.unwrap();
        assert!(!retained.load(Ordering::Acquire));
    });
}

#[tokio::test]
async fn reserved_reads_reject_a_different_range_length() {
    use rtdb_storage::ReadReservation;
    let store = MemoryStore::default();
    let key = ObjectKey::new("object").unwrap();
    store.put(&key, Bytes::from_static(b"abc")).await.unwrap();
    assert!(store.get_reserved_range(&key, 0..3, ReadReservation::new(2, ())).await.is_err());
}

#[tokio::test]
async fn database_pools_share_one_ceiling_while_a_detached_pool_lease_remains_live() {
    let first_store = Arc::new(MemoryStore::default());
    let second_store = Arc::new(MemoryStore::default());
    let key = ObjectKey::new("object").unwrap();
    first_store.put(&key, Bytes::from_static(b"first")).await.unwrap();
    second_store.put(&key, Bytes::from_static(b"other")).await.unwrap();
    let budget = CacheBudget::new(5);
    let first = BufferPool::with_budget(first_store, budget.clone());
    let second = BufferPool::with_budget(second_store, budget.clone());
    let retained = first.get(&key).await.unwrap();
    drop(first);
    assert_eq!(budget.retained_bytes(), 5);
    assert!(matches!(second.get(&key).await, Err(Error::MemoryLimit { limit: 5, .. })));
    assert_eq!(retained.as_slice(), b"first");
    drop(retained);
    assert_eq!(budget.retained_bytes(), 0);
    assert_eq!(second.get(&key).await.unwrap().as_slice(), b"other");
}
