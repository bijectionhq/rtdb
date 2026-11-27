use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;

use crate::{Error, LocalStore, ObjectKey, Result};

#[cfg(not(target_os = "linux"))]
use crate::{AppleSync, sync_file};

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
const MAX_STAGED_FILES: usize = 128;

/// A checkpoint's private staging files and unfinished publication groups.
pub struct LocalPublicationBatch<'a> {
    store: &'a LocalStore,
    root: Arc<Directory>,
    scratch: PathBuf,
    pending: Option<Vec<StagedObject>>,
    _admission: crate::io_region::IoAdmission,
    capacity: usize,
}

/// A writable staging file that can be registered after its writer finishes.
pub struct PreparedLocalObject<'batch, 'store> {
    batch: &'batch mut LocalPublicationBatch<'store>,
    object: StagedObject,
}

impl PreparedLocalObject<'_, '_> {
    /// Borrows the staging handle while it belongs to this writer.
    pub fn file(&self) -> &File {
        self.object.upload.as_file()
    }

    /// Registers the completed bytes after the caller closes all duplicate handles.
    pub fn stage(self) {
        self.batch.pending.as_mut().expect("writer belongs to an open batch").push(self.object);
    }
}

struct StagedObject {
    key: ObjectKey,
    path: PathBuf,
    upload: tempfile::NamedTempFile,
}

struct Directory {
    path: PathBuf,
    file: File,
}

impl Directory {
    fn open(path: PathBuf) -> Result<Self> {
        let file = rtdb_types::directory_io::open_for_sync(&path).map_err(|error| Error::io(&path, error))?;
        Ok(Self { path, file })
    }
}

impl<'a> LocalPublicationBatch<'a> {
    pub(crate) async fn open(store: &'a LocalStore) -> Result<Option<Self>> {
        let Some(capacity) = staging_capacity() else { return Ok(None); };
        let admission = store.io_region().admit()?;
        let path = store.root().to_path_buf();
        let (root, scratch) = store.io_region().run(move || {
            let root = Directory::open(path)?;
            let scratch = root.path.join(".tmp");
            fs::create_dir_all(&scratch).map_err(|error| Error::io(&scratch, error))?;
            Ok((Arc::new(root), scratch))
        }).await?;
        Ok(Some(Self {
            store, root, scratch, capacity, pending: Some(Vec::new()), _admission: admission,
        }))
    }

    /// Allocates a private file, publishing earlier staged objects first when the group is full.
    pub async fn prepare_file(&mut self, key: ObjectKey) -> Result<PreparedLocalObject<'_, 'a>> {
        let object = self.new_staged_file(key).await?;
        Ok(PreparedLocalObject { batch: self, object })
    }

    /// Stages a complete byte object under an immutable checkpoint dependency key.
    pub async fn stage_bytes(&mut self, key: ObjectKey, bytes: Bytes) -> Result<()> {
        let mut object = self.new_staged_file(key).await?;
        object.upload.write_all(&bytes).map_err(|error| Error::io(object.upload.path(), error))?;
        self.pending.as_mut().expect("staged file belongs to an open batch").push(object);
        Ok(())
    }

    async fn new_staged_file(&mut self, key: ObjectKey) -> Result<StagedObject> {
        if crate::local_catalog::terminal_name(key.as_str()) {
            return Err(Error::InvalidKey(key.to_string()));
        }
        if self.pending_files()?.len() == self.capacity { self.flush().await?; }
        let _admission = self.store.io_region().admit()?;
        let upload = tempfile::NamedTempFile::new_in(&self.scratch)
            .map_err(|error| Error::io(&self.scratch, error))?;
        Ok(StagedObject { path: self.root.path.join(key.as_str()), key, upload })
    }

    /// Makes the last group durable and releases its staging admission.
    pub async fn finish(mut self) -> Result<()> {
        self.flush().await
    }

    /// Makes staged contents durable before exposing their names, then synchronizes those names.
    /// A caller that receives an error must abandon its checkpoint attempt.
    pub async fn flush(&mut self) -> Result<()> {
        if self.pending_files()?.is_empty() { return Ok(()); }
        let pending = self.pending.take().expect("checked that the batch is open");
        let root = Arc::clone(&self.root);
        let policy = self.store.apple_sync();
        self.store.io_region().run(move || {
            let directories = open_destination_directories(&root.path, &pending)?;
            #[cfg(target_os = "linux")]
            linux::sync_payloads(&pending, policy)?;
            #[cfg(not(target_os = "linux"))]
            sync_each_payload(&pending, policy)?;

            for staged in pending {
                crate::object_store::install_local_file(staged.upload, &staged.path, &staged.key)?;
            }

            #[cfg(target_os = "linux")]
            return linux::sync_directories(&directories, policy);
            #[cfg(not(target_os = "linux"))]
            sync_each_directory(&directories, policy)
        }).await?;
        self.pending = Some(Vec::new());
        Ok(())
    }

    fn pending_files(&self) -> Result<&[StagedObject]> {
        self.pending.as_deref().ok_or_else(|| Error::Task("Checkpoint publication group failed or was cancelled".into()))
    }
}

fn open_destination_directories(root: &Path, pending: &[StagedObject]) -> Result<Vec<Directory>> {
    let mut paths = BTreeSet::new();
    for object in pending {
        let parent = object.path.parent().expect("staged object is under the store root");
        fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
        for dir in parent.ancestors() {
            paths.insert(dir);
            if dir == root { break; }
        }
    }
    let mut paths: Vec<_> = paths.into_iter().collect();
    paths.sort_by_cached_key(|path| std::cmp::Reverse(path.components().count()));
    paths.into_iter().map(|path| Directory::open(path.to_path_buf())).collect()
}

#[cfg(not(target_os = "linux"))]
fn sync_each_payload(pending: &[StagedObject], policy: AppleSync) -> Result<()> {
    for staged in pending {
        sync_file(staged.upload.as_file(), policy).map_err(|error| Error::io(staged.upload.path(), error))?;
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn sync_each_directory(directories: &[Directory], policy: AppleSync) -> Result<()> {
    for dir in directories {
        sync_file(&dir.file, policy).map_err(|error| Error::io(&dir.path, error))?;
    }
    Ok(())
}

fn staging_capacity() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        let mut limits = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: successful getrlimit writes the two limits to the allocated structure.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limits.as_mut_ptr()) } != 0 { return None; }
        let soft = (unsafe { limits.assume_init() }).rlim_cur;
        let capacity = (soft.saturating_sub(64) / 6).min(MAX_STAGED_FILES as libc::rlim_t) as usize;
        return (capacity >= 8).then_some(capacity);
    }
    #[cfg(not(target_os = "linux"))]
    None
}
