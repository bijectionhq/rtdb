//! Exclusive lifetime for locally staged and retired checkpoint allocations.

use crate::cipher::Cipher;
use bytes::Bytes;
use rtdb_storage::{Error, IoRegion, LocalStore, ObjectKey, ObjectStore, sync_directory};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use std::fs::{self, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Fingerprint, invalid};

pub(super) struct LocalArchive {
    root: PathBuf,
    cipher: Cipher,
    key: Vec<u8>,
}

pub(super) struct Allocation<'a> {
    archive: &'a LocalArchive,
    scope: &'a str,
    data: PathBuf,
    state: LocalStore,
    io: Arc<IoRegion>,
}

impl LocalArchive {
    pub(super) fn new(root: &Path, key: &[u8]) -> Result<Self> {
        let root = root.canonicalize().map_err(local_error)?;
        Ok(Self {
            root,
            cipher: Cipher::new(key)?,
            key: key.to_vec(),
        })
    }

    pub(super) async fn acquire<'a>(
        &'a self,
        scope: &'a str,
        exclusive: bool,
    ) -> Result<Allocation<'a>> {
        let relative = ObjectKey::new(scope)?;
        let state_path = self.root.join(format!("{relative}.lifecycle"));
        let lock_path = state_path.join("lock");
        let io = IoRegion::new();
        let lock = io
            .run_blocking(move || {
                let io_error = |source| Error::Io {
                    path: lock_path.clone(),
                    source,
                };
                fs::create_dir_all(&state_path).map_err(io_error)?;
                let lock = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .write(true)
                    .open(&lock_path)
                    .map_err(io_error)?;
                let acquired = if exclusive {
                    lock.try_lock()
                } else {
                    lock.try_lock_shared()
                };
                match acquired {
                    Ok(()) => Ok(lock),
                    Err(TryLockError::WouldBlock) => Err(Error::Closed(
                        "The local checkpoint allocation is active".into(),
                    )),
                    Err(TryLockError::Error(error)) => Err(io_error(error)),
                }
            })
            .await?;
        let lifetime: Arc<dyn Send + Sync> = Arc::new(lock);
        let io = io.child_holding(Some(lifetime));
        let state = LocalStore::open_in(
            self.root.join(format!("{relative}.lifecycle")),
            Arc::clone(&io),
        )?;
        Ok(Allocation {
            archive: self,
            scope,
            data: self.root.join(relative.as_str()),
            state,
            io,
        })
    }
}

impl Allocation<'_> {
    pub(super) async fn closed(&self) -> rtdb_storage::Result<Option<Bytes>> {
        match self.state.get_fresh(&ObjectKey::new("closed")?).await {
            Ok(marker) => Ok(Some(marker)),
            Err(Error::MissingObject(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) fn read_store(&self) -> Result<Arc<dyn ObjectStore>> {
        let raw = LocalStore::open_in(&self.archive.root, Arc::clone(&self.io))?;
        Ok(Arc::new(crate::store::EncryptingStore::new(
            Arc::new(raw),
            &self.archive.key,
        )?))
    }

    async fn sync_scope(&self) -> rtdb_storage::Result<()> {
        let mut directory = self
            .data
            .parent()
            .ok_or_else(|| Error::InvalidKey(self.scope.into()))?
            .to_path_buf();
        let root = self.archive.root.clone();
        self.io
            .run_blocking(move || {
                loop {
                    sync_directory(&directory).map_err(|source| Error::Io {
                        path: directory.clone(),
                        source,
                    })?;
                    if directory == root {
                        return Ok(());
                    }
                    if !directory.pop() {
                        return Err(Error::InvalidKey(directory.display().to_string()));
                    }
                }
            })
            .await
    }

    pub(super) async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        if self.closed().await?.is_some() {
            return Err(Error::Closed(
                "The local checkpoint allocation is permanently closed".into(),
            ));
        }
        let relative = key
            .as_str()
            .strip_prefix(self.scope)
            .and_then(|name| name.strip_prefix('/'))
            .ok_or_else(|| Error::InvalidKey(key.to_string()))?;
        let relative = ObjectKey::new(relative)?;
        // Puts use the allocation's own scratch directory. The I/O region
        // holds the file lock through cancellation of any blocking write.
        let data = LocalStore::open_in(&self.data, Arc::clone(&self.io))?;
        match data.get_fresh(&relative).await {
            Ok(sealed) if self.archive.cipher.open_object(key, &sealed)? == bytes => {
                return self.sync_scope().await;
            }
            Ok(_) => return Err(Error::ConflictingObject(key.to_string())),
            Err(Error::MissingObject(_)) => {}
            Err(error) => return Err(error),
        }
        data.put(&relative, self.archive.cipher.seal_object(key, &bytes)?)
            .await?;
        self.sync_scope().await
    }

    pub(super) async fn retire(&self, manifest: Fingerprint) -> Result<()> {
        self.state
            .put(
                &ObjectKey::new("closed")?,
                Bytes::copy_from_slice(&manifest),
            )
            .await?;
        self.sync_scope().await?;
        let path = self.data.clone();
        self.io
            .run_blocking(move || {
                match fs::remove_dir_all(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(source) => return Err(Error::Io { path, source }),
                }
                Ok(())
            })
            .await?;
        self.sync_scope().await?;
        Ok(())
    }

    pub(super) async fn require_disposition(&self, manifest: Fingerprint) -> Result<bool> {
        match self.closed().await? {
            Some(marker) if marker.as_ref() == manifest.as_slice() => Ok(true),
            Some(_) => Err(invalid(
                "Another manifest closed the local checkpoint allocation",
            )),
            None => Ok(false),
        }
    }
}

fn local_error(error: io::Error) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/local-checkpoint",
        error.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_checkpoint_reader_delays_disposal_and_closed_scope_cannot_reopen() {
        let volume = tempfile::tempdir().unwrap();
        let archive = LocalArchive::new(volume.path(), &[7; 32]).unwrap();
        let scope = "archive/placement/revision/allocation";
        let key = ObjectKey::new(format!("{scope}/manifest")).unwrap();
        let digest = [23; 32];

        let writer = archive.acquire(scope, true).await.unwrap();
        let body = Bytes::from_static(b"exact archive manifest");
        writer.put(&key, body.clone()).await.unwrap();
        drop(writer);
        let allocation = archive.acquire(scope, false).await.unwrap();
        let reader = allocation.read_store().unwrap();
        drop(allocation);
        assert_eq!(reader.get(&key).await.unwrap(), body);
        assert!(archive.acquire(scope, true).await.is_err());
        drop(reader);

        let retirement = archive.acquire(scope, true).await.unwrap();
        assert!(!retirement.require_disposition(digest).await.unwrap());
        retirement.retire(digest).await.unwrap();
        assert!(!volume.path().join(scope).exists());
        assert!(retirement.require_disposition(digest).await.unwrap());
        assert!(matches!(
            retirement.put(&key, body).await,
            Err(Error::Closed(_))
        ));
        retirement.retire(digest).await.unwrap();
    }
}
