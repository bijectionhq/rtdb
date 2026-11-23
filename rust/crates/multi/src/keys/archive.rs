//! Operation-only custody for a checkpoint archive and its dependent suffix.

use std::{fs, io::ErrorKind, ops::Range, path::Path, sync::Arc};

use bytes::Bytes;
use ring::hmac;
use rtdb_storage::{Error, IoRegion, ObjectKey};
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
    pub region: Arc<IoRegion>,
}

impl LocalArchiveKey {
    async fn operate<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&[u8; KEY_BYTES]) -> Result<T> + Send + 'static,
    ) -> rtdb_storage::Result<T> {
        let vault = self.vault.clone();
        let id = self.id.clone();
        let locked = move || -> Result<T> {
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
        };
        self.region.run_blocking(move || locked().map_err(archive_io)).await
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
            .await
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        let associated = associated.to_owned();
        self.operate(move |key| Ok(Cipher::new(key)?.open(&associated, &ciphertext)?))
            .await
    }

    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        let name = name.clone();
        self.operate(move |key| Ok(Cipher::new(key)?.seal_object(&name, &plaintext)?)).await
    }

    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        let name = name.clone();
        self.operate(move |key| Ok(Cipher::new(key)?.open_object(&name, &sealed)?)).await
    }

    async fn open_frame_header(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<FramedObject> {
        let name = name.clone();
        self.operate(move |key| Ok(FramedObject::open_header(&Cipher::new(key)?, &name, &sealed)?))
            .await
    }

    async fn open_frames(&self, name: &ObjectKey, layout: FramedObject, frames: Range<usize>, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        let name = name.clone();
        self.operate(move |key| Ok(layout.open_frames(&Cipher::new(key)?, &name, frames, &sealed)?))
            .await
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        Ok(self.operate(move |key| {
            let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
            let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
            Ok(hmac::verify(&candidate, KEY_COMPARISON, comparison.as_ref()).is_ok())
        }).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::{Future, poll_fn}, task::Poll};

    use crate::keys::KeyService;
    use rtdb_storage::ReadReservation;
    use rtdb_types::MemoryBudget;

    #[tokio::test]
    async fn cancelled_archive_frame_keeps_its_reservation_until_the_region_drains() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(temp.path())?;
        let region = IoRegion::new();
        let id = "frame";
        let key = vault.archive_key(id, true, Arc::clone(&region)).await?;
        let name = ObjectKey::new("tables/t/data/l00-rc-b00.arrow")?;
        let plaintext = Bytes::from_static(b"archived data");
        let length = plaintext.len();
        let sealed = key.seal_object(&name, plaintext).await?;
        let header_bytes = crate::cipher::object::SEALED_HEADER_BYTES;
        let layout = key.open_frame_header(&name, sealed.slice(..header_bytes)).await?;
        let ciphertext = sealed.slice(header_bytes..);

        let files = vault.archive_vault()?;
        let held = files.lock_key(&files.path(id)?)?;
        let memory = MemoryBudget::new(1024);
        let mut read = Box::pin(async {
            let reservation = ReadReservation::new(length, memory.reserve(length).unwrap());
            reservation.retaining_io(key.open_frames(&name, layout, 0..1, ciphertext)).await
        });
        poll_fn(|context| {
            assert!(read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        drop(read);
        assert_eq!(memory.used_bytes(), length);

        let mut drain = Box::pin(region.close_and_wait());
        poll_fn(|context| {
            assert!(drain.as_mut().poll(context).is_pending());
            Poll::Ready(())
        }).await;
        drop(held);
        drain.await;
        assert_eq!(memory.used_bytes(), 0);
        assert!(matches!(key.open_frame_header(&name, sealed.slice(..header_bytes)).await, Err(Error::Closed(_))));
        Ok(())
    }
}
