//! Operation-only custody for a checkpoint archive and its dependent suffix.

use std::{fs, io::ErrorKind, ops::Range, path::Path, sync::Arc};

use bytes::Bytes;
use ring::hmac;
use rtdb_storage::{Error, ObjectKey};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use crate::cipher::Cipher;

use super::{DESTROYED, FramedObject, KEY_BYTES, LocalKeyService, io, is_destroyed, malformed, read_key};

#[async_trait::async_trait]
pub trait ArchiveKey: Send + Sync {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open_frame_header(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<FramedObject>;
    async fn open_frames(&self, name: &ObjectKey, layout: FramedObject, frames: Range<usize>, sealed: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool>;
}

const KEY_COMPARISON: &[u8] = b"rtdb.archive.key-comparison.v1";

struct LegacyArchiveKey {
    cipher: Cipher,
    comparison: hmac::Tag,
}

pub(crate) fn legacy_archive_key(key: &[u8; KEY_BYTES]) -> Result<Arc<dyn ArchiveKey>> {
    let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
    Ok(Arc::new(LegacyArchiveKey { cipher: Cipher::new(key)?, comparison }))
}

#[async_trait::async_trait]
impl ArchiveKey for LegacyArchiveKey {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.cipher.seal_deterministic(associated, &plaintext)
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.cipher.open(associated, &ciphertext)
    }

    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.cipher.seal_object(name, &plaintext)
    }

    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        self.cipher.open_object(name, &sealed)
    }

    async fn open_frame_header(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<FramedObject> {
        FramedObject::open_header(&self.cipher, name, &sealed)
    }

    async fn open_frames(&self, name: &ObjectKey, layout: FramedObject, frames: Range<usize>, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        layout.open_frames(&self.cipher, name, frames, &sealed)
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
        Ok(hmac::verify(&candidate, KEY_COMPARISON, self.comparison.as_ref()).is_ok())
    }
}

pub(super) struct LocalArchiveKey {
    pub vault: LocalKeyService,
    pub id: String,
}

impl LocalArchiveKey {
    async fn operate<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&[u8; KEY_BYTES]) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let vault = self.vault.clone();
        let id = self.id.clone();
        tokio::task::spawn_blocking(move || {
            let path = vault.path(&id)?;
            let _lock = vault.lock_key(&path)?;
            if expire_locked(&vault, &path)?.is_some() {
                return Err(revoked());
            }
            let key = read_key(&path, fs::read(&path).map_err(io)?)?;
            let output = operation(&key)?;
            if expire_locked(&vault, &path)?.is_some() {
                return Err(revoked());
            }
            Ok(output)
        })
        .await
        .map_err(|error| Anomaly::fault("rtdb.multi/archive-vault-task", error.to_string()))?
    }
}

fn schedule_conflict() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/archive-vault", "The archive has another revocation schedule")
}

pub(super) fn revoked() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.multi/archive-revoked", "The archive key has been revoked")
}

fn read_schedule(path: &Path) -> Result<Option<i64>> {
    match fs::read(path.with_extension("expires")) {
        Ok(bytes) => {
            let due_at_ms = i64::from_be_bytes(bytes.try_into().map_err(|_| malformed())?);
            if due_at_ms <= 0 { return Err(malformed()); }
            Ok(Some(due_at_ms))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

pub(super) fn schedule_locked(vault: &LocalKeyService, path: &Path, due_at_ms: i64) -> Result<bool> {
    if due_at_ms <= 0 {
        return Err(Anomaly::incorrect("rtdb.multi/archive-vault", "The archive revocation date must be positive"));
    }
    match read_schedule(path)? {
        Some(prior) if prior != due_at_ms => return Err(schedule_conflict()),
        Some(_) => vault.sync_directory()?,
        None => {
            read_key(path, fs::read(path).map_err(io)?)?;
            vault.publish(&path.with_extension("expires"), &due_at_ms.to_be_bytes())?;
        }
    }
    Ok(expire_locked(vault, path)?.is_some())
}

pub(super) fn withdraw_locked(vault: &LocalKeyService, path: &Path) -> Result<()> {
    if read_schedule(path)?.is_some() {
        if expire_locked(vault, path)?.is_some() {
            return Ok(());
        }
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/archive-vault",
            "The archive authority is waiting for its scheduled revocation"));
    }
    vault.publish(path, DESTROYED)
}

pub(super) fn expire_locked(vault: &LocalKeyService, path: &Path) -> Result<Option<i64>> {
    let Some(due_at_ms) = read_schedule(path)? else { return Ok(None); };
    let material = fs::read(path).map_err(io)?;
    if is_destroyed(&material) {
        vault.sync_directory()?;
        return Ok(Some(due_at_ms));
    }
    if rtdb_directory::now_ms() < due_at_ms {
        return Ok(None);
    }
    vault.publish(path, DESTROYED)?;
    Ok(Some(due_at_ms))
}

fn archive_io(error: Anomaly) -> Error {
    Error::PermissionDenied { key: "checkpoint archive".into(), message: error.code.into() }
}

#[async_trait::async_trait]
impl ArchiveKey for LocalArchiveKey {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        let associated = associated.to_owned();
        self.operate(move |key| Ok(Cipher::new(key)?.seal_deterministic(&associated, &plaintext)?))
            .await.map_err(archive_io)
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        let associated = associated.to_owned();
        self.operate(move |key| Ok(Cipher::new(key)?.open(&associated, &ciphertext)?))
            .await.map_err(archive_io)
    }

    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        let name = name.clone();
        self.operate(move |key| Ok(Cipher::new(key)?.seal_object(&name, &plaintext)?)).await.map_err(archive_io)
    }

    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        let name = name.clone();
        self.operate(move |key| Ok(Cipher::new(key)?.open_object(&name, &sealed)?)).await.map_err(archive_io)
    }

    async fn open_frame_header(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<FramedObject> {
        let name = name.clone();
        self.operate(move |key| Ok(FramedObject::open_header(&Cipher::new(key)?, &name, &sealed)?))
            .await.map_err(archive_io)
    }

    async fn open_frames(&self, name: &ObjectKey, layout: FramedObject, frames: Range<usize>, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        let name = name.clone();
        self.operate(move |key| Ok(layout.open_frames(&Cipher::new(key)?, &name, frames, &sealed)?))
            .await.map_err(archive_io)
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        self.operate(move |key| {
            let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
            let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
            Ok(hmac::verify(&candidate, KEY_COMPARISON, comparison.as_ref()).is_ok())
        }).await
    }
}
