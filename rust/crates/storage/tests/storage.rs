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
    store.copy(&key, &copied).await.unwrap();
    store.copy(&key, &key).await.unwrap();
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
    let store = Arc::new(LocalStore::open(directory.path()).unwrap());
    immutable_objects(store.clone()).await; store.close_and_wait().await;
}

#[tokio::test]
async fn local_delete_requires_a_retained_namespace_even_for_an_absent_object() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("objects");
    let store = LocalStore::open(&root).unwrap();
    let key = ObjectKey::new("new/partition/absent").unwrap();
    store.delete(&key).await.unwrap();
    std::fs::remove_dir(&root).unwrap();
    assert!(store.delete(&key).await.is_err());
    store.close_and_wait().await;
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
    let store = Arc::new(LocalStore::open(directory.path()).unwrap());
    immutable_file_upload(store.clone()).await; store.close_and_wait().await;
}

#[tokio::test]
async fn local_nested_publication_survives_reopen_and_accepts_exact_retries() {
    let directory = tempfile::tempdir().unwrap();
    let storage = directory.path().join("objects");
    let source = directory.path().join("source.arrow");
    let data_key = ObjectKey::new("tables/products/data/l00-r00.arrow").unwrap();
    let table_key = ObjectKey::new("tables/products/blocks/b00.binpb").unwrap();
    let root_key = ObjectKey::new("blocks/b00.binpb").unwrap();
    let data = b"trie data";
    let table = Bytes::from_static(b"table block");
    let root = Bytes::from_static(b"root block");
    std::fs::write(&source, data).unwrap();

    let store = LocalStore::open(&storage).unwrap();
    store.put_file(&data_key, &source).await.unwrap();
    store.put(&table_key, table.clone()).await.unwrap();
    store.put(&root_key, root.clone()).await.unwrap();
    store.close_and_wait().await;

    let reopened = LocalStore::open(&storage).unwrap();
    assert_eq!(reopened.get(&data_key).await.unwrap().as_ref(), data);
    assert_eq!(reopened.get(&table_key).await.unwrap(), table);
    assert_eq!(reopened.get(&root_key).await.unwrap(), root);
    reopened.put_file(&data_key, &source).await.unwrap();
    reopened.put(&root_key, root).await.unwrap();
    assert!(matches!(reopened.put(&root_key, Bytes::from_static(b"conflict")).await, Err(Error::ConflictingObject(_))));
    reopened.close_and_wait().await;
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
    store.close_and_wait().await;
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
        store.close_and_wait().await;
    }
    let reopened = LocalStore::open(directory.path()).unwrap();
    assert_eq!(reopened.get(&key).await.unwrap().as_ref(), b"persisted");
    reopened.close_and_wait().await;
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
        let mut close = Box::pin(store.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(store.head(&key).await, Err(Error::Closed(_))));
        resume.send(()).unwrap();
        blocker.await.unwrap();
        close.await;
        release.await.unwrap();
        assert!(!retained.load(Ordering::Acquire));
    });
}

#[tokio::test]
async fn sibling_local_stores_close_through_their_shared_region() {
    let first = tempfile::tempdir().unwrap(); let second = tempfile::tempdir().unwrap();
    let region = rtdb_storage::IoRegion::new(); let a = LocalStore::open_in(first.path(), region.clone()).unwrap(); let b = LocalStore::open_in(second.path(), region.clone()).unwrap();
    let key = ObjectKey::new("object").unwrap();
    a.put(&key, Bytes::from_static(b"first")).await.unwrap(); b.put(&key, Bytes::from_static(b"second")).await.unwrap();
    region.close_and_wait().await;
    assert!(matches!(a.get(&key).await, Err(Error::Closed(_)))); assert!(matches!(b.get(&key).await, Err(Error::Closed(_))));
    assert!(LocalStore::open_in(first.path(), region).is_err());
}

#[tokio::test]
async fn local_copies_publish_into_an_explicit_sibling_root_and_preserve_conflicts() {
    let directory = tempfile::tempdir().unwrap();
    let source = LocalStore::open(directory.path().join("source")).unwrap();
    let destination = LocalStore::open(directory.path().join("outside")).unwrap();
    let key = ObjectKey::new("original.arrow").unwrap();
    let target = ObjectKey::new("subdir/copy.arrow").unwrap();
    let mut data = vec![7u8; 2 * 1024 * 1024 + 17];
    source.put(&key, Bytes::copy_from_slice(&data)).await.unwrap();
    source.copy_to(&key, &destination, &target).await.unwrap();
    source.copy_to(&key, &destination, &target).await.unwrap();
    assert_eq!(std::fs::read(directory.path().join("outside/subdir/copy.arrow")).unwrap(), data);
    assert_eq!(source.list(None).await.unwrap().len(), 1);
    data[1024 * 1024] = 8;
    let different = ObjectKey::new("different.arrow").unwrap();
    source.put(&different, Bytes::from(data)).await.unwrap();
    assert!(matches!(source.copy_to(&different, &destination, &target).await, Err(Error::ConflictingObject(_))));
    assert_eq!(destination.get_range(&target, 1024 * 1024..1024 * 1024 + 1).await.unwrap().as_ref(), &[7]);
    assert!(matches!(source.copy_to(&ObjectKey::new("missing").unwrap(), &destination, &target).await, Err(Error::MissingObject(_))));
    source.close_and_wait().await;
    destination.close_and_wait().await;
}

#[test]
fn cancelled_cross_root_copy_is_retained_by_both_regions_until_publication() {
    use std::{future::{Future, poll_fn}, task::Poll};
    let directory = tempfile::tempdir().unwrap();
    let source = LocalStore::open(directory.path().join("source")).unwrap();
    let destination = LocalStore::open(directory.path().join("destination")).unwrap();
    let key = ObjectKey::new("object").unwrap();
    std::fs::write(directory.path().join("source/object"), b"copied after unlink").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || { started.send(()).unwrap(); paused.recv().unwrap(); });
        waiting.await.unwrap();
        let mut copy = Box::pin(source.copy_to(&key, &destination, &key));
        poll_fn(|context| { assert!(copy.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        drop(copy);
        std::fs::remove_file(directory.path().join("source/object")).unwrap();
        let mut close_source = Box::pin(source.close_and_wait());
        let mut close_destination = Box::pin(destination.close_and_wait());
        poll_fn(|context| {
            assert!(close_source.as_mut().poll(context).is_pending());
            assert!(close_destination.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        resume.send(()).unwrap();
        blocker.await.unwrap();
        close_source.await;
        close_destination.await;
        assert_eq!(std::fs::read(directory.path().join("destination/object")).unwrap(), b"copied after unlink");
    });
}

#[test]
fn cancelled_file_publication_keeps_its_open_source_until_region_drain() {
    use std::{future::{Future, poll_fn}, task::Poll};
    let directory = tempfile::tempdir().unwrap(); let store = LocalStore::open(directory.path()).unwrap();
    let source = tempfile::NamedTempFile::new().unwrap(); std::fs::write(source.path(), b"published after unlink").unwrap();
    let key = ObjectKey::new("object").unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(1).max_blocking_threads(1).enable_all().build().unwrap();
    runtime.block_on(async {
        let (started, waiting) = tokio::sync::oneshot::channel(); let (resume, paused) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || { started.send(()).unwrap(); paused.recv().unwrap(); }); waiting.await.unwrap();
        let mut upload = Box::pin(store.put_file(&key, source.path()));
        poll_fn(|context| { assert!(upload.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        drop(upload); std::fs::remove_file(source.path()).unwrap();
        let mut close = Box::pin(store.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        resume.send(()).unwrap(); blocker.await.unwrap(); close.await;
        assert_eq!(std::fs::read(directory.path().join("object")).unwrap(), b"published after unlink");
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

#[tokio::test]
async fn a_local_objects_file_reads_its_bytes_even_after_a_delete() {
    use std::io::Read;
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(LocalStore::open(directory.path()).unwrap());
    let key = ObjectKey::new("tables/docs/artefacts/file.bin").unwrap();
    store.put(&key, bytes::Bytes::from_static(b"search file")).await.unwrap();
    let local = store.local_file(&key).await.unwrap().expect("a local store's objects are files");
    store.delete(&key).await.unwrap();
    let mut bytes = Vec::new();
    local.file().try_clone().unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"search file");
    assert!(matches!(store.local_file(&key).await, Err(rtdb_storage::Error::MissingObject(_))));
    assert!(rtdb_storage::MemoryStore::default().local_file(&key).await.unwrap().is_none(), "a memory store has no local file");
    store.close_and_wait().await;
}

#[tokio::test]
async fn local_scan_reads_follow_removed_and_republished_objects() {
    let location = tempfile::tempdir().unwrap();
    let store = std::sync::Arc::new(rtdb_storage::LocalStore::open(location.path()).unwrap());
    let pool = rtdb_storage::cache::BufferPool::new(store.clone(), 1 << 20);
    let key = rtdb_storage::ObjectKey::new("tables/orders/data/l00-rc-b00.arrow").unwrap();
    store.put(&key, bytes::Bytes::from_static(b"first version of the page")).await.unwrap();

    assert_eq!(pool.get_scan_range(&key, 0..5).await.unwrap().as_slice(), b"first");
    assert_eq!(pool.get_scan_range(&key, 6..13).await.unwrap().as_slice(), b"version");
    let held = pool.get_scan_range(&key, 0..5).await.unwrap();

    store.delete(&key).await.unwrap();
    assert!(matches!(pool.get_scan_range(&key, 0..5).await, Err(rtdb_storage::Error::MissingObject(_))));
    assert_eq!(held.as_slice(), b"first", "a lease keeps the bytes it was given");

    store.put(&key, bytes::Bytes::from_static(b"second version, longer than the first")).await.unwrap();
    assert_eq!(pool.get_scan_range(&key, 0..6).await.unwrap().as_slice(), b"second");
    assert_eq!(pool.get_scan_range(&key, 32..37).await.unwrap().as_slice(), b"first");
    assert_eq!(pool.scan_length(&key).await.unwrap(), 37);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_checkpoint_publication_batch_keeps_staged_names_private_and_retries() {
    use std::io::Write;

    let root = tempfile::tempdir().unwrap();
    let store = LocalStore::open(root.path()).unwrap();
    let first = ObjectKey::new("tables/public$test/data/l00-rc-b00.arrow").unwrap();
    let second = ObjectKey::new("tables/public$test/meta/l00-rc-b00.arrow").unwrap();
    let mut abandoned = store.begin_checkpoint_publication().await.unwrap().unwrap();
    let staged = abandoned.prepare_file(first.clone()).await.unwrap();
    staged.file().try_clone().unwrap().write_all(b"arrow data").unwrap();
    staged.stage();
    abandoned.stage_bytes(second.clone(), Bytes::from_static(b"arrow metadata")).await.unwrap();
    assert!(matches!(store.head(&first).await, Err(Error::MissingObject(_))));
    assert!(matches!(store.head(&second).await, Err(Error::MissingObject(_))));
    drop(abandoned);

    for _ in 0..2 {
        let mut batch = store.begin_checkpoint_publication().await.unwrap().unwrap();
        let staged = batch.prepare_file(first.clone()).await.unwrap();
        staged.file().try_clone().unwrap().write_all(b"arrow data").unwrap();
        staged.stage();
        batch.stage_bytes(second.clone(), Bytes::from_static(b"arrow metadata")).await.unwrap();
        batch.finish().await.unwrap();
    }
    store.close_and_wait().await;
    let reopened = LocalStore::open(root.path()).unwrap();
    assert_eq!(reopened.get(&first).await.unwrap().as_ref(), b"arrow data");
    assert_eq!(reopened.get(&second).await.unwrap().as_ref(), b"arrow metadata");

    let mut wrong = reopened.begin_checkpoint_publication().await.unwrap().unwrap();
    wrong.stage_bytes(first.clone(), Bytes::from_static(b"new contents")).await.unwrap();
    assert!(matches!(wrong.flush().await, Err(Error::ConflictingObject(_))));
    assert!(matches!(wrong.finish().await, Err(Error::Task(_))));
    assert_eq!(reopened.get(&first).await.unwrap().as_ref(), b"arrow data");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn local_checkpoint_publication_batch_close_drains_the_staging_owner() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalStore::open(root.path()).unwrap();
    let mut batch = store.begin_checkpoint_publication().await.unwrap().unwrap();
    batch.stage_bytes(ObjectKey::new("tables/test/catalog").unwrap(), Bytes::from_static(b"payload")).await.unwrap();
    let close = store.close_and_wait();
    tokio::pin!(close);
    tokio::select! {
        biased;
        _ = &mut close => panic!("close completed with a live staging owner"),
        _ = std::future::ready(()) => {}
    }
    assert!(matches!(batch.finish().await, Err(Error::Closed(_))));
    close.await;
}
