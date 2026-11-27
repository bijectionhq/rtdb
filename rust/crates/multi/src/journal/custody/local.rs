//! Shared lifetimes for immutable checkpoint I/O, fenced by exclusive retirement.

use crate::keys::{UnitLedger, UnitOperation};
use bytes::Bytes;
use rtdb_storage::{AppleSync, Error, IoRegion, LocalStore, ObjectKey, ObjectStore, sync_directory};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use std::fs::{self, OpenOptions, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{Fingerprint, invalid};

pub(super) struct LocalArchive {
    root: PathBuf,
    apple_sync: AppleSync,
}

pub(super) struct Allocation<'a> {
    archive: &'a LocalArchive,
    scope: &'a str,
    data: PathBuf,
    state: LocalStore,
    io: Arc<IoRegion>,
}

impl LocalArchive {
    pub(super) fn new(root: &Path) -> Result<Self> {
        let root = root.canonicalize().map_err(local_error)?;
        Ok(Self { root, apple_sync: AppleSync::Full })
    }

    pub(super) fn with_apple_sync(mut self, apple_sync: AppleSync) -> Self {
        self.apple_sync = apple_sync;
        self
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
        )?.with_apple_sync(self.apple_sync);
        Ok(Allocation {
            archive: self,
            scope,
            data: self.root.join(relative.as_str()),
            state,
            io,
        })
    }

    pub(super) async fn readable<'a>(&'a self, scope: &'a str) -> Result<Allocation<'a>> {
        let allocation = self.acquire(scope, false).await?;
        if allocation.closed().await?.is_some() {
            return Err(invalid("The local checkpoint archive is closed"));
        }
        Ok(allocation)
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

    pub(super) fn region(&self) -> Arc<IoRegion> {
        Arc::clone(&self.io)
    }

    pub(super) fn read_store(&self, ledger: UnitLedger) -> Result<Arc<dyn ObjectStore>> {
        let raw = LocalStore::open_in(&self.archive.root, self.region())?.with_apple_sync(self.archive.apple_sync);
        Ok(Arc::new(crate::store::EncryptingStore::with_units(
            Arc::new(raw),
            Arc::new(ledger),
        )))
    }

    pub(super) async fn sync_existing(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        LocalStore::open_in(&self.archive.root, Arc::clone(&self.io))?
            .with_apple_sync(self.archive.apple_sync)
            .sync_existing(key)
            .await
    }

    async fn sync_scope(&self) -> rtdb_storage::Result<()> {
        let mut directory = self
            .data
            .parent()
            .ok_or_else(|| Error::InvalidKey(self.scope.into()))?
            .to_path_buf();
        let root = self.archive.root.clone();
        let apple_sync = self.archive.apple_sync;
        self.io
            .run_blocking(move || {
                loop {
                    sync_directory(&directory, apple_sync).map_err(|source| Error::Io {
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

    pub(super) async fn put(
        &self,
        ledger: &UnitLedger,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> rtdb_storage::Result<()> {
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
        let data = LocalStore::open_in(&self.data, Arc::clone(&self.io))?.with_apple_sync(self.archive.apple_sync);
        let existing = match data.get_fresh(&relative).await {
            Ok(sealed) => sealed,
            Err(Error::MissingObject(_)) => {
                let sealed = ledger
                    .operate(key, UnitOperation::Seal(bytes.clone()))
                    .await?
                    .bytes()?;
                match data.put(&relative, sealed).await {
                    Ok(()) => return self.sync_scope().await,
                    Err(Error::ConflictingObject(_)) => data.get_fresh(&relative).await?,
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        };
        let plaintext = ledger
            .operate(key, UnitOperation::Open(existing))
            .await?
            .bytes()?;
        if plaintext != bytes {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        data.sync_existing(&relative).await?;
        self.sync_scope().await
    }

    pub(super) async fn close(&self, manifest: Fingerprint) -> Result<()> {
        self.mark("closed", manifest).await
    }

    async fn mark(&self, name: &str, manifest: Fingerprint) -> Result<()> {
        self.state
            .put(&ObjectKey::new(name)?, Bytes::copy_from_slice(&manifest))
            .await?;
        self.sync_scope().await?;
        Ok(())
    }

    pub(super) async fn retire(&self) -> Result<()> {
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
        use crate::keys::{LocalKeyService, UnitScope};
        use ordered_logkeeper::ordered::Placement;
        use uuid::Uuid;

        let vault = tempfile::tempdir().unwrap();
        let keys = Arc::new(LocalKeyService::new(vault.path()).unwrap());
        let archive = LocalArchive::new(volume.path()).unwrap();
        let scope = "archive/placement/revision/allocation";
        let key = ObjectKey::new(format!("{scope}/manifest")).unwrap();
        let digest = [23; 32];
        let placement = Placement {
            deployment: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            group: Uuid::new_v4(),
            generation: 1,
        };
        let key_scope = UnitScope::new(placement, b"checkpoint", scope.as_bytes());

        let writer = archive.acquire(scope, true).await.unwrap();
        let body = Bytes::from_static(b"exact archive manifest");
        writer
            .put(
                &UnitLedger::new(keys.clone(), key_scope, writer.region()),
                &key,
                body.clone(),
            )
            .await
            .unwrap();
        drop(writer);
        let allocation = archive.acquire(scope, false).await.unwrap();
        let reader = allocation
            .read_store(UnitLedger::new(
                keys.clone(),
                key_scope,
                allocation.region(),
            ))
            .unwrap();
        drop(allocation);
        assert_eq!(reader.get(&key).await.unwrap(), body);
        assert!(archive.acquire(scope, true).await.is_err());
        drop(reader);

        let retirement = archive.acquire(scope, true).await.unwrap();
        assert!(!retirement.require_disposition(digest).await.unwrap());
        retirement.close(digest).await.unwrap();
        let ledger = UnitLedger::new(keys, key_scope, retirement.region());
        ledger.withdraw_scope().await.unwrap();
        retirement.retire().await.unwrap();
        assert!(!volume.path().join(scope).exists());
        assert!(retirement.require_disposition(digest).await.unwrap());
        assert!(matches!(
            retirement.put(&ledger, &key, body).await,
            Err(Error::Closed(_))
        ));
        retirement.retire().await.unwrap();
    }
}
