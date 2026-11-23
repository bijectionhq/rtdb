use std::{collections::BTreeMap, fs::{self, File, OpenOptions}, io::{Read, Seek, SeekFrom, Write}, ops::Range, path::{Path, PathBuf}, sync::{Arc, OnceLock, atomic::{AtomicU64, Ordering}}, time::SystemTime};
use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt;
use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use tokio_util::sync::CancellationToken;

use crate::{Error, IoRegion, ObjectKey, ObjectStore, ReadReservation, Result, RetainedBytes, StoredObject};

/// The path moves when a leased file is invalidated; readers hold the lock while they open it.
struct CachedFile { path: parking_lot::RwLock<PathBuf>, size: u64 }
struct Entry {
    file: OnceLock<CachedFile>,
    loading: Arc<tokio::sync::Mutex<()>>,
    accessed: AtomicU64,
    /// Set once the entry has left the cache, so its file goes with the last lease.
    retired: std::sync::atomic::AtomicBool,
    _reservation: Arc<MemoryReservation>,
}
impl Drop for Entry {
    fn drop(&mut self) {
        if self.retired.load(Ordering::Acquire) { if let Some(file) = self.file.get() { let _ = fs::remove_file(&*file.path.read()); } }
    }
}
struct State { entries: BTreeMap<ObjectKey, Arc<Entry>>, clock: u64 }

pub struct DiskCache {
    root: PathBuf,
    maximum_bytes: u64,
    memory: Arc<MemoryBudget>,
    state: Mutex<State>,
    operations: Arc<IoRegion>,
    cancellation: CancellationToken,
    _directory_lock: File,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskCacheStats { pub maximum_bytes: u64, pub pinned_bytes: u64, pub evictable_bytes: u64 }

pub struct DiskLease { cache: Arc<DiskCache>, entry: Arc<Entry> }
impl DiskLease {
    pub fn path(&self) -> PathBuf { self.entry.file.get().expect("published disk lease").path.read().clone() }
    fn open(&self) -> std::io::Result<File> { File::open(&*self.entry.file.get().expect("published disk lease").path.read()) }
    pub fn size(&self) -> u64 { self.entry.file.get().expect("published disk lease").size }
}
impl Drop for DiskLease { fn drop(&mut self) { self.cache.trim(Some(&self.entry)); } }

impl DiskCache {
    /// Reuses complete files and evicts the oldest idle entries to fit the configured cache capacity.
    /// Pinned files may exceed capacity until their final lease is released; metadata and download chunks use `memory`.
    /// Holds an exclusive directory lock until all cache owners and leases are released.
    /// The region owner must call `close_and_wait` before dropping the cache or shutting down its Tokio runtime.
    pub fn open(root: impl AsRef<Path>, maximum_bytes: u64, memory: Arc<MemoryBudget>) -> Result<Arc<Self>> {
        if maximum_bytes == 0 { return Err(Error::Task("Disk cache capacity must be positive".into())); }
        fs::create_dir_all(root.as_ref()).map_err(|error| Error::io(root.as_ref(), error))?;
        let root = fs::canonicalize(root.as_ref()).map_err(|error| Error::io(root.as_ref(), error))?;
        let temporary = root.join(".tmp");
        match fs::create_dir(&temporary) {
            Ok(()) => {},
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {},
            Err(error) => return Err(Error::io(&temporary, error)),
        }
        let temporary_metadata = fs::symlink_metadata(&temporary).map_err(|error| Error::io(&temporary, error))?;
        if !temporary_metadata.is_dir() || temporary_metadata.is_symlink() { return Err(Error::InvalidKey(temporary.display().to_string())); }
        let lock_path = temporary.join(".owner-lock");
        let create_lock = match fs::symlink_metadata(&lock_path) {
            Ok(metadata) if metadata.is_file() && !metadata.is_symlink() => false,
            Ok(_) => return Err(Error::InvalidKey(lock_path.display().to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => true,
            Err(error) => return Err(Error::io(&lock_path, error)),
        };
        let directory_lock = OpenOptions::new().create_new(create_lock).read(true).write(true).open(&lock_path).map_err(|error| Error::io(&lock_path, error))?;
        directory_lock.try_lock_exclusive().map_err(|error| Error::io(&lock_path, error))?;
        for item in fs::read_dir(&temporary).map_err(|error| Error::io(&temporary, error))? {
            let item = item.map_err(|error| Error::io(&temporary, error))?;
            if item.file_name() == ".owner-lock" { continue; }
            let path = item.path();
            if item.file_type().map_err(|error| Error::io(&path, error))?.is_dir() { fs::remove_dir_all(&path) } else { fs::remove_file(&path) }.map_err(|error| Error::io(&path, error))?;
        }
        let cache = Arc::new(Self { root, maximum_bytes, memory, state: Mutex::new(State { entries: BTreeMap::new(), clock: 0 }),
            operations: IoRegion::new(), cancellation: CancellationToken::new(), _directory_lock: directory_lock });
        cache.discover(&cache.root)?;
        cache.trim(None);
        Ok(cache)
    }
    fn begin_operation(&self) -> Result<crate::io_region::IoAdmission> {
        self.operations.admit()
    }
    /// Rejects new cache work, cancels downloads, and joins all admitted work, including blocking reads whose callers were cancelled.
    /// Existing leases remain pinned and keep the directory lock until released.
    pub async fn close_and_wait(&self) {
        self.operations.close();
        self.cancellation.cancel();
        self.operations.wait().await;
    }
    fn discover(&self, directory: &Path) -> Result<()> {
        for item in fs::read_dir(directory).map_err(|error| Error::io(directory, error))? {
            let item = item.map_err(|error| Error::io(directory, error))?;
            let path = item.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| Error::io(&path, error))?;
            if metadata.file_type().is_symlink() { continue; }
            if item.file_name() == ".tmp" { continue; }
            if metadata.is_dir() { self.discover(&path)?; continue; }
            if !metadata.is_file() { continue; }
            let relative = path.strip_prefix(&self.root).map_err(|error| Error::Task(error.to_string()))?;
            let key = ObjectKey::new(relative.to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/"))?;
            let entry = self.new_entry(&key)?;
            let accessed = metadata.accessed().unwrap_or(SystemTime::UNIX_EPOCH).max(metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH))
                .duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros().min(u64::MAX as u128) as u64;
            entry.accessed.store(accessed, Ordering::Relaxed);
            let _ = entry.file.set(CachedFile { path: parking_lot::RwLock::new(path), size: metadata.len() });
            let mut state = self.state.lock();
            state.clock = state.clock.max(accessed);
            state.entries.insert(key, Arc::new(entry));
        }
        Ok(())
    }
    fn reserve(&self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        self.memory.reserve(bytes).map_err(|_| Error::MemoryLimit { requested: bytes, limit: self.memory.limit_bytes(), retained: self.memory.used_bytes() })
    }
    fn new_entry(&self, key: &ObjectKey) -> Result<Entry> {
        let bytes = key.as_str().len().checked_mul(3).and_then(|bytes| bytes.checked_add(self.root.as_os_str().len())).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(|| Error::Task("Disk cache metadata allocation exceeds range".into()))?;
        Ok(Entry { file: OnceLock::new(), loading: Arc::new(tokio::sync::Mutex::new(())), accessed: AtomicU64::new(0), retired: Default::default(), _reservation: self.reserve(bytes)? })
    }
    fn entry(&self, key: &ObjectKey) -> Result<Arc<Entry>> {
        let mut state = self.state.lock();
        let entry = match state.entries.get(key) { Some(entry) => Arc::clone(entry), None => { let entry = Arc::new(self.new_entry(key)?); state.entries.insert(key.clone(), Arc::clone(&entry)); entry } };
        state.clock = state.clock.saturating_add(1);
        entry.accessed.store(state.clock, Ordering::Relaxed);
        Ok(entry)
    }
    pub fn stats(&self) -> DiskCacheStats {
        let state = self.state.lock();
        let mut result = DiskCacheStats { maximum_bytes: self.maximum_bytes, pinned_bytes: 0, evictable_bytes: 0 };
        for entry in state.entries.values() {
            let size = entry.file.get().map_or(0, |file| file.size);
            if Arc::strong_count(entry) > 1 { result.pinned_bytes = result.pinned_bytes.saturating_add(size); } else { result.evictable_bytes = result.evictable_bytes.saturating_add(size); }
        }
        result
    }
    fn trim(&self, releasing: Option<&Arc<Entry>>) {
        let mut state = self.state.lock();
        state.entries.retain(|_, entry| entry.file.get().is_some() || !idle(entry, releasing));
        let mut bytes = state.entries.values().filter_map(|entry| entry.file.get()).fold(0u64, |bytes, file| bytes.saturating_add(file.size));
        while bytes > self.maximum_bytes {
            let candidate = state.entries.iter().filter(|(_, entry)| {
                entry.file.get().is_some() && idle(entry, releasing)
            }).min_by_key(|(_, entry)| entry.accessed.load(Ordering::Relaxed)).map(|(key, _)| key.clone());
            let Some(key) = candidate else { break; };
            let Some(entry) = state.entries.remove(&key) else { break; };
            if let Some(file) = entry.file.get() {
                let path = file.path.read().clone();
                match fs::remove_file(&path) {
                    Ok(()) => bytes = bytes.saturating_sub(file.size),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => bytes = bytes.saturating_sub(file.size),
                    Err(error) => { eprintln!("RTDB disk cache eviction failed at {}: {error}", path.display()); state.entries.insert(key, entry); break; }
                }
            }
        }
    }
    fn cached_file(self: &Arc<Self>, key: &ObjectKey) -> Option<DiskLease> {
        let mut state = self.state.lock();
        let entry = state.entries.get(key).filter(|entry| entry.file.get().is_some()).cloned()?;
        state.clock = state.clock.saturating_add(1);
        entry.accessed.store(state.clock, Ordering::Relaxed);
        Some(DiskLease { cache: Arc::clone(self), entry })
    }
    fn cached_size(&self, key: &ObjectKey) -> Option<u64> { self.state.lock().entries.get(key).and_then(|entry| entry.file.get()).map(|file| file.size) }

    /// Concurrent readers share a load; cancellation discards its temporary file and leaves the next caller able to retry.
    pub async fn get(self: &Arc<Self>, cache_key: &ObjectKey, store: &dyn ObjectStore, remote_key: &ObjectKey) -> Result<DiskLease> {
        let _operation = self.begin_operation()?;
        let entry = self.entry(cache_key)?;
        let lease = DiskLease { cache: Arc::clone(self), entry };
        let guard = self.cancellation.run_until_cancelled(lease.entry.loading.lock()).await.ok_or_else(cache_closed)?;
        if lease.entry.file.get().is_none() {
            let object = self.cancellation.run_until_cancelled(store.head(remote_key)).await.ok_or_else(cache_closed)??;
            let mut temporary = tempfile::NamedTempFile::new_in(self.root.join(".tmp")).map_err(|error| Error::io(&self.root, error))?;
            let ranges = store.supports_scan_ranges(remote_key);
            let chunk_size = if ranges {
                (1024 * 1024).min(self.memory.limit_bytes().saturating_sub(self.memory.used_bytes()) / 2).max(1) as u64
            } else {
                object.size.max(1)
            };
            let mut offset = 0;
            while offset < object.size {
                let end = object.size.min(offset.saturating_add(chunk_size));
                let length = usize::try_from(end - offset).map_err(|error| Error::Task(error.to_string()))?;
                let bytes = self.cancellation.run_until_cancelled(async {
                    if ranges {
                        let allocation = store.scan_range_allocation(remote_key, length)?;
                        let reservation = crate::cache::reserve_read(length, allocation, |bytes| self.reserve(bytes))?;
                        store.get_reserved_range(remote_key, offset..end, reservation).await
                    } else {
                        let allocation = store.whole_object_allocation(remote_key, length)?;
                        let reservation = crate::cache::reserve_read(length, allocation, |bytes| self.reserve(bytes))?;
                        store.get_reserved_object(remote_key, reservation).await
                    }
                }).await.ok_or_else(cache_closed)??;
                temporary.write_all(bytes.as_slice()).map_err(|error| Error::io(temporary.path(), error))?;
                offset = end;
            }
            self.publish(cache_key, &lease.entry, temporary, object.size)?;
        }
        drop(guard);
        self.trim(None);
        Ok(lease)
    }

    fn publish(&self, key: &ObjectKey, entry: &Arc<Entry>, temporary: tempfile::NamedTempFile, size: u64) -> Result<()> {
        let state = self.state.lock();
        let path = if state.entries.get(key).is_some_and(|current| Arc::ptr_eq(current, entry)) {
            let path = self.root.join(key.as_str());
            let parent = path.parent().ok_or_else(|| Error::InvalidKey(key.to_string()))?;
            fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
            temporary.persist(&path).map_err(|error| Error::io(&path, error.error))?;
            path
        } else {
            debug_assert!(entry.retired.load(Ordering::Acquire));
            temporary.keep().map_err(|error| Error::io(error.file.path(), error.error))?.1
        };
        entry.file.set(CachedFile { path: parking_lot::RwLock::new(path), size })
            .map_err(|_| Error::Task("Disk cache entry published twice".into()))
    }

    /// Drops the cached copy of a key, so the next read goes to the remote store.
    /// An idle file is removed now. A leased one moves into the staging directory, where its readers keep it until the last lease ends and a restart clears it.
    pub fn invalidate(&self, cache_key: &ObjectKey) -> Result<()> {
        let mut state = self.state.lock();
        let Some(entry) = state.entries.get(cache_key) else { return Ok(()) };
        if let Some(file) = entry.file.get() {
            let staging = self.root.join(".tmp");
            let retired = tempfile::Builder::new().prefix("retired-").tempfile_in(&staging).map_err(|error| Error::io(&staging, error))?
                .into_temp_path().keep().map_err(|error| Error::io(&staging, error.error))?;
            let mut path = file.path.write();
            match fs::rename(&*path, &retired) {
                Ok(()) => *path = retired,
                Err(error) => {
                    let _ = fs::remove_file(&retired);
                    if error.kind() != std::io::ErrorKind::NotFound { return Err(Error::io(&*path, error)); }
                    *path = retired;
                }
            }
        }
        entry.retired.store(true, Ordering::Release);
        state.entries.remove(cache_key);
        Ok(())
    }
    pub fn scope(self: &Arc<Self>, namespace: ObjectKey, remote: Arc<dyn ObjectStore>) -> Result<Arc<dyn ObjectStore>> {
        self.scope_in(namespace, remote, Arc::clone(&self.operations))
    }
    /// Gates this store's requests with `io`, including cache hits and retained blocking work.
    /// Its owner closes and joins that region after consumers finish; other cache scopes remain usable.
    /// The remote capability must retain any offloaded provider work in the same region.
    pub fn scope_in(self: &Arc<Self>, namespace: ObjectKey, remote: Arc<dyn ObjectStore>, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        if !remote.permits_shared_read_cache() {
            return Err(Error::Closed("The object owner requires a fresh read for each use".into()));
        }
        Ok(Arc::new(DiskCachedStore { cache: Arc::clone(self), namespace, remote, io }))
    }

    /// Caches a completed immutable upload; the caller keeps `source` unchanged until this returns.
    /// The source file remains at its original path.
    pub async fn put_file(self: &Arc<Self>, key: &ObjectKey, source: &Path) -> Result<()> {
        self.put_file_admitted(key, source, None).await
    }
    async fn put_file_admitted(self: &Arc<Self>, key: &ObjectKey, source: &Path, scope: Option<crate::io_region::IoAdmission>) -> Result<()> {
        let operation = self.begin_operation()?;
        let operation = match scope { Some(scope) => operation.and(scope), None => operation };
        let lease = DiskLease { cache: Arc::clone(self), entry: self.entry(key)? };
        let guard = self.cancellation.run_until_cancelled(Arc::clone(&lease.entry.loading).lock_owned()).await.ok_or_else(cache_closed)?;
        if lease.entry.file.get().is_some() { return Ok(()); }
        let input = File::open(source).map_err(|error| Error::io(source, error))?;
        let paths = self.reserve(source.as_os_str().len().checked_add(key.as_str().len()).and_then(|size| size.checked_add(1024)).ok_or_else(|| Error::Task("Cache copy path allocation exceeds range".into()))?)?;
        let (cache, source, key) = (Arc::clone(self), source.to_owned(), key.clone());
        IoRegion::run_admitted(operation, move || {
            let (cache, source, key, lease, _guard, mut input, _paths) = (cache, source, key, lease, guard, input, paths);
            let size = input.metadata().map_err(|error| Error::io(&source, error))?.len();
            let chunk_size = (64 * 1024).min(cache.memory.limit_bytes().saturating_sub(cache.memory.used_bytes()) / 2).max(1);
            let _reservation = cache.reserve(chunk_size)?;
            let mut chunk = vec![0u8; chunk_size];
            let mut temporary = tempfile::NamedTempFile::new_in(cache.root.join(".tmp")).map_err(|error| Error::io(&cache.root, error))?;
            let mut remaining = size;
            while remaining > 0 {
                if cache.cancellation.is_cancelled() { return Err(cache_closed()); }
                let length = remaining.min(chunk_size as u64) as usize;
                input.read_exact(&mut chunk[..length]).map_err(|error| Error::io(&source, error))?;
                temporary.write_all(&chunk[..length]).map_err(|error| Error::io(temporary.path(), error))?;
                remaining -= length as u64;
            }
            cache.publish(&key, &lease.entry, temporary, size)
        }).await
    }
}

fn idle(entry: &Arc<Entry>, releasing: Option<&Arc<Entry>>) -> bool {
    let count = Arc::strong_count(entry);
    count == 1 || count == 2 && releasing.is_some_and(|released| Arc::ptr_eq(released, entry))
}
fn cache_closed() -> Error { Error::Closed("disk cache is closed".into()) }

struct DiskCachedStore { cache: Arc<DiskCache>, namespace: ObjectKey, remote: Arc<dyn ObjectStore>, io: Arc<IoRegion> }
struct OwnedRead(RetainedBytes);
impl AsRef<[u8]> for OwnedRead { fn as_ref(&self) -> &[u8] { self.0.as_slice() } }
impl DiskCachedStore {
    fn key(&self, key: &ObjectKey) -> Result<ObjectKey> { ObjectKey::new(format!("{}/{key}", self.namespace)) }
}
#[async_trait]
impl ObjectStore for DiskCachedStore {
    fn permits_shared_read_cache(&self) -> bool { self.remote.permits_shared_read_cache() }
    fn index_page_lifecycle(&self) -> Option<&dyn crate::object_store::IndexPageLifecycle> {
        self.remote.index_page_lifecycle()
    }
    async fn retain_catalog(&self, cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<Option<Arc<dyn Send + Sync>>> {
        self.remote.retain_catalog(cut).await
    }
    async fn retain_catalog_with_primary(&self, cut: Option<rtdb_types::CatalogCut>, paths: Arc<[String]>)
        -> rtdb_types::Result<Option<Arc<dyn Send + Sync>>> {
        self.remote.retain_catalog_with_primary(cut, paths).await
    }
    fn defers_trie_releases(&self) -> bool { self.remote.defers_trie_releases() }
    fn releases_receipt_l0(&self) -> bool { self.remote.releases_receipt_l0() }
    async fn advance_catalog_floor(&self, installed: rtdb_types::CatalogCut) -> rtdb_types::Result<bool> {
        self.remote.advance_catalog_floor(installed).await
    }
    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> rtdb_types::Result<bool> {
        self.remote.release_trie_files(receipt, contents).await
    }
    async fn release_search_run(&self, key: &ObjectKey, publication: Option<&rtdb_format::log::proto::PublishedObjectVersion>,
        cut: Option<rtdb_types::CatalogCut>) -> rtdb_types::Result<bool> {
        let _scope = self.io.admit()?;
        let _operation = self.cache.begin_operation()?;
        if !self.remote.release_search_run(key, publication, cut).await? { return Ok(false); }
        self.cache.invalidate(&self.key(key)?)?;
        Ok(true)
    }

    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let _scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let _operation = self.cache.begin_operation()?;
        match self.cache.cached_size(&self.key(key)?) { Some(size) => Ok(StoredObject { key: key.clone(), size }), None => self.cache.cancellation.run_until_cancelled(self.remote.head(key)).await.ok_or_else(cache_closed)? }
    }
    async fn head_fresh(&self, key: &ObjectKey) -> Result<StoredObject> {
        let _scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let _operation = self.cache.begin_operation()?;
        self.cache.cancellation.run_until_cancelled(self.remote.head_fresh(key)).await.ok_or_else(cache_closed)?
    }
    async fn observe_head(&self, key: &ObjectKey) -> Result<crate::ObjectObservation<StoredObject>> {
        self.remote.observe_head(key).await
    }
    async fn observe_heads(&self, keys: &[ObjectKey]) -> Result<std::collections::BTreeMap<ObjectKey, crate::ObjectObservation<StoredObject>>> {
        self.remote.observe_heads(keys).await
    }
    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<crate::ObjectObservation<Bytes>> {
        self.remote.observe_range(key, range).await
    }
    async fn retirement_version(&self, key: &ObjectKey) -> Result<Option<rtdb_types::PhysicalObjectVersion>> {
        let _scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let _operation = self.cache.begin_operation()?;
        self.cache.cancellation.run_until_cancelled(self.remote.retirement_version(key)).await.ok_or_else(cache_closed)?
    }
    async fn retirement_publication(&self, key: &ObjectKey) -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        let _scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let _operation = self.cache.begin_operation()?;
        self.cache.cancellation.run_until_cancelled(self.remote.retirement_publication(key)).await.ok_or_else(cache_closed)?
    }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.get_range(key, 0..self.head(key).await?.size).await
    }
    async fn get_fresh(&self, key: &ObjectKey) -> Result<Bytes> {
        let _scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let _operation = self.cache.begin_operation()?;
        self.cache.cancellation.run_until_cancelled(self.remote.get_fresh(key)).await.ok_or_else(cache_closed)?
    }
    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<Bytes> { self.remote.get_fresh_exact(key, length).await }
    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> Result<crate::ObjectObservation<Bytes>> {
        let _scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let _operation = self.cache.begin_operation()?;
        self.cache.cancellation.run_until_cancelled(self.remote.observe_fresh_exact(key, length)).await.ok_or_else(cache_closed)?
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let length = usize::try_from(range.end.checked_sub(range.start).ok_or_else(|| Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length: 0 })?).map_err(|error| Error::Task(error.to_string()))?;
        let reservation = ReadReservation::new(length, self.cache.reserve(length)?);
        self.get_reserved_range(key, range, reservation).await.map(|bytes| Bytes::from_owner(OwnedRead(bytes)))
    }
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        let scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let cache_key = self.key(key)?;
        let lease = self.cache.get(&cache_key, self.remote.as_ref(), key).await?;
        if range.end.checked_sub(range.start) != Some(reservation.length() as u64) || range.end > lease.size() {
            return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length: lease.size() });
        }
        let operation = self.cache.begin_operation()?.and(scope);
        IoRegion::run_admitted(operation, move || {
            let lease = lease;
            let mut file = lease.open().map_err(|error| Error::io(lease.path(), error))?;
            file.seek(SeekFrom::Start(range.start)).map_err(|error| Error::io(lease.path(), error))?;
            let mut bytes = vec![0; reservation.length()];
            file.read_exact(&mut bytes).map_err(|error| Error::io(lease.path(), error))?;
            reservation.finish(Bytes::from(bytes))
        }).await
    }
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.remote.supports_scan_ranges(key) }
    fn supports_observed_scan_ranges(&self, key: &ObjectKey) -> bool { self.remote.supports_observed_scan_ranges(key) }

    async fn get_reserved_scan_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        let scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let operation = self.cache.begin_operation()?.and(scope);
        let Some(lease) = self.cache.cached_file(&self.key(key)?) else {
            return self.cache.cancellation.run_until_cancelled(self.remote.get_reserved_scan_range(key, range, reservation))
                .await.ok_or_else(cache_closed)?;
        };
        let key = key.clone();
        IoRegion::run_admitted(operation, move || {
            let file = lease.open().map_err(|error| Error::io(lease.path(), error))?;
            crate::object_store::map_range(crate::LocalFile::new(file, Some(lease)), &key, range, reservation)
        }).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> { let _scope = self.io.admit()?; let _operation = self.cache.begin_operation()?; self.remote.put(key, bytes).await }
    /// The lease pins the cached file, so the cache neither evicts nor counts as free what a reader still maps.
    async fn local_file(&self, key: &ObjectKey) -> Result<Option<crate::LocalFile>> {
        let scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let lease = self.cache.get(&self.key(key)?, self.remote.as_ref(), key).await?;
        let operation = self.cache.begin_operation()?.and(scope);
        IoRegion::run_admitted(operation, move || {
            let file = lease.open().map_err(|error| Error::io(lease.path(), error))?;
            Ok(Some(crate::LocalFile::new(file, Some(lease))))
        }).await
    }
    async fn cached_local_file(&self, key: &ObjectKey) -> Result<Option<crate::LocalFile>> {
        let scope = self.io.admit_read(self.cache.clone(), self.key(key)?).await?;
        let operation = self.cache.begin_operation()?.and(scope);
        let Some(lease) = self.cache.cached_file(&self.key(key)?) else { return Ok(None) };
        IoRegion::run_admitted(operation, move || {
            let file = lease.open().map_err(|error| Error::io(lease.path(), error))?;
            Ok(Some(crate::LocalFile::new(file, Some(lease))))
        }).await
    }
    async fn put_with_publication(&self, key: &ObjectKey, bytes: Bytes) -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        let _scope = self.io.admit()?;
        let _operation = self.cache.begin_operation()?;
        self.remote.put_with_publication(key, bytes).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()> {
        self.put_file_with_publication(key, source).await.map(drop)
    }
    async fn put_file_with_publication(&self, key: &ObjectKey, source: &Path) -> Result<Option<rtdb_types::PhysicalObjectPublication>> {
        let scope = self.io.admit()?;
        let _operation = self.cache.begin_operation()?;
        let written = self.remote.put_file_with_publication(key, source).await?;
        self.cache.put_file_admitted(&self.key(key)?, source, Some(scope)).await?;
        Ok(written)
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> { let _scope = self.io.admit_read(self.cache.clone(), (self.namespace.clone(), directory.cloned())).await?; let _operation = self.cache.begin_operation()?; self.cache.cancellation.run_until_cancelled(self.remote.list(directory)).await.ok_or_else(cache_closed)? }
    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        let _scope = self.io.admit()?;
        let _operation = self.cache.begin_operation()?;
        self.remote.delete(key).await?;
        self.cache.invalidate(&self.key(key)?)
    }
    fn queues_offloaded_payload_reclamation(&self) -> bool { self.remote.queues_offloaded_payload_reclamation() }
    async fn collect_recovery_file(&self, key: &ObjectKey) -> rtdb_types::Result<()> {
        let _scope = self.io.admit()?;
        let _operation = self.cache.begin_operation()?;
        self.remote.collect_recovery_file(key).await?;
        self.cache.invalidate(&self.key(key)?).map_err(Into::into)
    }
    async fn forget(&self, key: &ObjectKey) -> Result<()> { let _scope = self.io.admit()?; self.cache.invalidate(&self.key(key)?) }
    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> { let _scope = self.io.admit()?; let _operation = self.cache.begin_operation()?; self.remote.copy(source, destination).await }
}
