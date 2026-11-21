use crate::{Error, IoRegion, Result};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};
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

pub struct ReadReservation {
    length: usize,
    retention: Box<dyn Send + Sync>,
}

impl ReadReservation {
    /// `retention` must hold the memory charge for `length` bytes until it is dropped.
    pub fn new<T: Send + Sync + 'static>(length: usize, retention: T) -> Self {
        Self { length, retention: Box::new(retention) }
    }

    pub fn length(&self) -> usize { self.length }

    pub(crate) fn check_range(&self, range: &Range<u64>) -> Result<()> {
        if range.end.checked_sub(range.start) != Some(self.length as u64) {
            return Err(Error::Task("reserved read range does not match its allocation reservation".into()));
        }
        Ok(())
    }

    /// Transfers the charge to the completed allocation; mismatched byte counts are rejected.
    pub fn finish(self, bytes: Bytes) -> Result<RetainedBytes> {
        if bytes.len() != self.length { return Err(Error::Task("reserved read returned a different byte count".into())); }
        Ok(RetainedBytes { bytes, _retention: self.retention })
    }
}

pub struct RetainedBytes {
    bytes: Bytes,
    _retention: Box<dyn Send + Sync>,
}

impl RetainedBytes {
    pub fn as_slice(&self) -> &[u8] { &self.bytes }
    pub fn len(&self) -> usize { self.bytes.len() }
    pub fn is_empty(&self) -> bool { self.bytes.is_empty() }
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
    fn index_page_lifecycle(&self) -> Option<&dyn IndexPageLifecycle> { None }
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject>;
    /// Reads current metadata for the key from the backing provider.
    /// The default is suitable when `head` already obtains current provider metadata.
    /// Store wrappers must forward this request to the wrapped store's `head_fresh`.
    /// A missing key returns `Error::MissingObject`.
    async fn head_fresh(&self, key: &ObjectKey) -> Result<StoredObject> { self.head(key).await }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes>;
    /// Reads without creating or trusting a node-local durable cache copy.
    /// Store wrappers must forward this request across any cache boundary.
    async fn get_fresh(&self, key: &ObjectKey) -> Result<Bytes> { self.get(key).await }
    /// Returns exactly the half-open byte range; an unavailable range is an error.
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes>;
    /// Whether a scan can read a slice without repeatedly loading the whole object.
    fn supports_scan_ranges(&self, _key: &ObjectKey) -> bool { true }
    /// Retains the allocation charge during the read and with the returned bytes.
    /// Implementations that offload work beyond this future must move the reservation into that work.
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        reservation.finish(self.get_range(key, range).await?)
    }
    /// Reads part of an object without filling a whole-object disk cache on a miss.
    async fn get_reserved_scan_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        self.get_reserved_range(key, range, reservation).await
    }
    /// Publishes an immutable object, accepting an identical existing object as a completed retry.
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()>;
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()>;
    /// Returns the object names reported under a directory, ordered lexicographically.
    /// A listing may observe concurrent writes or deletions while enumerating the names.
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>>;
    async fn delete(&self, key: &ObjectKey) -> Result<()>;
    /// Drops any copy of the object held on this node, leaving the remote object untouched.
    async fn forget(&self, _key: &ObjectKey) -> Result<()> { Ok(()) }
    /// The object as a file on this node's disk, for a reader that maps it rather than holding its bytes; `None` where the store keeps no local file.
    async fn local_file(&self, _key: &ObjectKey) -> Result<Option<LocalFile>> { Ok(None) }

    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> {
        self.put(destination, self.get(source).await?).await
    }
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
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let objects = self.objects.read();
        let bytes = objects.get(key).ok_or_else(|| Error::MissingObject(key.to_string()))?;
        Ok(StoredObject { key: key.clone(), size: bytes.len() as u64 })
    }

    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.objects.read().get(key).cloned().ok_or_else(|| Error::MissingObject(key.to_string()))
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
}

struct AdmittedFile {
    file: File,
    _admission: crate::io_region::IoAdmission,
}

impl LocalStore {
    /// Opens a store with an independent I/O region; its owner calls `close_and_wait` before runtime shutdown.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_in(root, IoRegion::new())
    }
    /// Shares a caller-owned I/O region with sibling stores; closing the region closes admission for every sibling.
    pub fn open_in(root: impl AsRef<Path>, io: Arc<IoRegion>) -> Result<Self> {
        let _operation = io.admit()?;
        let root = root.as_ref();
        fs::create_dir_all(root).map_err(|err| Error::io(root, err))?;
        let root = fs::canonicalize(root).map_err(|err| Error::io(root, err))?;
        Ok(Self { root: Arc::new(root), io })
    }
    /// Rejects new work and joins this store's I/O region, including cancelled callers' blocking work.
    pub async fn close_and_wait(&self) { self.io.close_and_wait().await; }

    fn path(&self, key: &ObjectKey) -> PathBuf { self.root.join(key.as_str()) }

    /// Copies to an explicit destination store, including a different filesystem root.
    /// Publication is immutable and accepts identical retries; both I/O regions retain admitted work through cancellation.
    /// Missing sources return MissingObject, different destination bytes return ConflictingObject, and either closed region returns Closed.
    /// An I/O or synchronization error can occur after the destination has been published.
    pub async fn copy_to(&self, source: &ObjectKey, destination: &LocalStore, key: &ObjectKey) -> Result<()> {
        let admission = self.io.admit()?;
        let file = open_object(&self.path(source), source)?;
        destination.publish_file(key, AdmittedFile { file, _admission: admission }).await
    }

    async fn publish_file(&self, key: &ObjectKey, input: AdmittedFile) -> Result<()> {
        let (root, path, key) = (Arc::clone(&self.root), self.path(key), key.clone());
        self.io.run(move || {
            let mut input = input;
            let parent = path.parent().ok_or_else(|| Error::InvalidKey(key.to_string()))?;
            fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
            let temp_dir = root.join(".tmp");
            fs::create_dir_all(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            let mut upload = tempfile::NamedTempFile::new_in(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            std::io::copy(&mut input.file, &mut upload).map_err(|error| Error::io(upload.path(), error))?;
            crate::sync_file(upload.as_file()).map_err(|error| Error::io(upload.path(), error))?;
            match upload.persist_noclobber(&path) {
                Ok(_) => (),
                Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if !equal_local_files(&path, error.file.path())? { return Err(Error::ConflictingObject(key.to_string())); }
                }
                Err(error) => return Err(Error::io(&path, error.error)),
            }
            sync_parents(&path, &root)
        }).await
    }
}

fn open_object(path: &Path, key: &ObjectKey) -> Result<File> {
    File::open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound { Error::MissingObject(key.to_string()) }
        else { Error::io(path, error) }
    })
}

fn sync_directory(path: &Path) -> Result<()> {
    crate::sync_directory(path).map_err(|error| Error::io(path, error))
}

fn sync_parents(path: &Path, root: &Path) -> Result<()> {
    let mut current = path.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
    loop {
        sync_directory(current)?;
        if current == root { return Ok(()); }
        current = current.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
    }
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
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let (path, key) = (self.path(key), key.clone());
        self.io.run(move || {
            let file = open_object(&path, &key)?;
            let size = file.metadata().map_err(|error| Error::io(&path, error))?.len();
            Ok(StoredObject { key, size })
        }).await
    }

    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        let (path, key) = (self.path(key), key.clone());
        self.io.run(move || {
            let mut file = open_object(&path, &key)?;
            let mut bytes = Vec::new();
            file.read_to_end(&mut bytes).map_err(|error| Error::io(&path, error))?;
            Ok(Bytes::from(bytes))
        }).await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let (path, key) = (self.path(key), key.clone());
        self.io.run(move || read_local_range(&path, &key, range)).await
    }

    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        let (path, key) = (self.path(key), key.clone());
        self.io.run(move || reservation.finish(read_local_range(&path, &key, range)?)).await
    }

    async fn get_reserved_scan_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        let (path, key) = (self.path(key), key.clone());
        self.io.run(move || map_range(LocalFile::new(open_object(&path, &key)?, None), &key, range, reservation)).await
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> {
        let (root, path, key) = (self.root.clone(), self.path(key), key.clone());
        self.io.run(move || {
            let parent = path.parent().ok_or_else(|| Error::InvalidKey(key.to_string()))?;
            fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
            let temp_dir = root.join(".tmp");
            fs::create_dir_all(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            let mut upload = tempfile::NamedTempFile::new_in(&temp_dir).map_err(|error| Error::io(&temp_dir, error))?;
            upload.write_all(&bytes).map_err(|error| Error::io(upload.path(), error))?;
            crate::sync_file(upload.as_file()).map_err(|error| Error::io(upload.path(), error))?;
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
            sync_parents(&path, &root)
        }).await
    }

    async fn put_file(&self, key: &ObjectKey, source: &Path) -> Result<()> {
        let admission = self.io.admit()?;
        let file = File::open(source).map_err(|error| Error::io(source, error))?;
        self.publish_file(key, AdmittedFile { file, _admission: admission }).await
    }

    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> {
        self.copy_to(source, self, destination).await
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        let root = self.root.clone();
        let directory = directory.cloned();
        self.io.run(move || {
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
            let start = directory.as_ref().map_or_else(|| root.as_ref().clone(), |key| root.join(key.as_str()));
            let mut result = Vec::new();
            visit(&root, &start, &mut result)?;
            result.sort_by(|a, b| a.key.cmp(&b.key));
            Ok(result)
        }).await
    }

    /// A local object is never rewritten under its key, and an open file outlives a delete, so the file is the object for as long as it is held.
    async fn local_file(&self, key: &ObjectKey) -> Result<Option<LocalFile>> {
        let (path, key) = (self.path(key), key.clone());
        self.io.run(move || Ok(Some(LocalFile::new(open_object(&path, &key)?, None)))).await
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        let path = self.path(key);
        let root = self.root.clone();
        self.io.run(move || {
            match fs::remove_file(&path) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(Error::io(&path, error)),
            }
            // A retry can observe an unlink whose parent was not flushed.
            // If a containing directory is also absent, confirm its removal.
            let mut parent = path.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
            loop {
                match crate::sync_directory(parent) {
                    Ok(()) => return Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound && parent != root.as_path() => {
                        parent = parent.parent().ok_or_else(|| Error::InvalidKey(path.display().to_string()))?;
                    }
                    Err(error) => return Err(Error::io(parent, error)),
                }
            }
        }).await
    }
}
