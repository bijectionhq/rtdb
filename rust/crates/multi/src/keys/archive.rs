//! Operation-only custody for a checkpoint archive and its dependent suffix.

use std::{fs, io::ErrorKind, ops::Range, path::Path, sync::Arc, time::Instant};

use bytes::Bytes;
use ring::{
    digest::{SHA256, digest},
    hmac,
};
use rtdb_storage::{Error, IoRegion, ObjectKey};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use crate::cipher::Cipher;

use super::{
    DESTROYED, FramedObject, KEY_BYTES, LocalKeyService, UnitOperation, UnitOutput, UnitScope, io,
    is_destroyed, malformed, read_key,
    unit::{self, OPERATION_LIMIT},
};

#[async_trait::async_trait]
pub trait ArchiveKey: Send + Sync {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes>;
    async fn open_frame_header(
        &self,
        name: &ObjectKey,
        sealed: Bytes,
    ) -> rtdb_storage::Result<FramedObject>;
    async fn open_frames(
        &self,
        name: &ObjectKey,
        layout: FramedObject,
        frames: Range<usize>,
        sealed: Bytes,
    ) -> rtdb_storage::Result<Bytes>;
    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool>;
}

const KEY_COMPARISON: &[u8] = b"rtdb.archive.key-comparison.v1";

struct LegacyArchiveKey {
    cipher: Cipher,
    comparison: hmac::Tag,
}

pub(crate) fn legacy_archive_key(key: &[u8; KEY_BYTES]) -> Result<Arc<dyn ArchiveKey>> {
    let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
    Ok(Arc::new(LegacyArchiveKey {
        cipher: Cipher::new(key)?,
        comparison,
    }))
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

    async fn open_frame_header(
        &self,
        name: &ObjectKey,
        sealed: Bytes,
    ) -> rtdb_storage::Result<FramedObject> {
        FramedObject::open_header(&self.cipher, name, &sealed)
    }

    async fn open_frames(
        &self,
        name: &ObjectKey,
        layout: FramedObject,
        frames: Range<usize>,
        sealed: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        layout.open_frames(&self.cipher, name, frames, &sealed)
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
        Ok(hmac::verify(&candidate, KEY_COMPARISON, self.comparison.as_ref()).is_ok())
    }
}

pub(super) struct LocalArchiveKey {
    pub vault: LocalKeyService,
    pub unit_vault: LocalKeyService,
    pub id: String,
    pub region: Arc<IoRegion>,
}

impl LocalArchiveKey {
    async fn operate<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&[u8; KEY_BYTES]) -> Result<T> + Send + 'static,
    ) -> rtdb_storage::Result<T> {
        self.operate_with(operation, archive_io).await
    }

    pub(super) async fn operate_with<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&[u8; KEY_BYTES]) -> Result<T> + Send + 'static,
        map_error: fn(Anomaly) -> Error,
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
        self.region
            .run_blocking(move || locked().map_err(map_error))
            .await
    }
}

fn schedule_conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.multi/archive-vault",
        "The archive has another revocation schedule",
    )
}

pub(super) fn revoked() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.multi/archive-revoked",
        "The archive key has been revoked",
    )
}

fn read_schedule(path: &Path) -> Result<Option<i64>> {
    match fs::read(path.with_extension("expires")) {
        Ok(bytes) => {
            let due_at_ms = i64::from_be_bytes(bytes.try_into().map_err(|_| malformed())?);
            if due_at_ms <= 0 {
                return Err(malformed());
            }
            Ok(Some(due_at_ms))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io(error)),
    }
}

pub(super) fn schedule_locked(
    vault: &LocalKeyService,
    path: &Path,
    due_at_ms: i64,
) -> Result<bool> {
    if due_at_ms <= 0 {
        return Err(Anomaly::incorrect(
            "rtdb.multi/archive-vault",
            "The archive revocation date must be positive",
        ));
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
        return Err(Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb.multi/archive-vault",
            "The archive authority is waiting for its scheduled revocation",
        ));
    }
    vault.publish(path, DESTROYED)
}

pub(super) fn expire_locked(vault: &LocalKeyService, path: &Path) -> Result<Option<i64>> {
    let Some(due_at_ms) = read_schedule(path)? else {
        return Ok(None);
    };
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
    Error::PermissionDenied {
        key: "checkpoint archive".into(),
        message: error.code.into(),
    }
}

#[async_trait::async_trait]
pub(super) trait CaptureCryptography: Send + Sync {
    async fn crypt(
        &self,
        name: ObjectKey,
        operation: UnitOperation,
    ) -> rtdb_storage::Result<UnitOutput>;
    async fn matches_authority_key(&self, candidate: [u8; KEY_BYTES]) -> Result<bool>;
}

fn associated_unit(associated: &str) -> rtdb_storage::Result<ObjectKey> {
    use std::fmt::Write;
    let mut name = String::from("archive-records/");
    for byte in digest(&SHA256, associated.as_bytes()).as_ref() {
        write!(name, "{byte:02x}").expect("write to String");
    }
    ObjectKey::new(name)
}

pub(super) fn with_archive_unit(
    authority: &[u8; KEY_BYTES],
    mut material: [u8; KEY_BYTES],
    deadline: Instant,
    name: &ObjectKey,
    operation: UnitOperation,
) -> rtdb_storage::Result<UnitOutput> {
    let key = hmac::Key::new(hmac::HMAC_SHA256, authority);
    let combined = hmac::sign(&key, &material);
    material.fill(0);
    unit::with_material(
        combined.as_ref().try_into().expect("HMAC-SHA256 width"),
        deadline,
        |cipher| operation.apply(cipher, name),
    )
}

#[async_trait::async_trait]
impl<T: CaptureCryptography> ArchiveKey for T {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.crypt(
            associated_unit(associated)?,
            UnitOperation::SealValue(associated.to_owned(), plaintext),
        )
        .await?
        .bytes()
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.crypt(
            associated_unit(associated)?,
            UnitOperation::OpenValue(associated.to_owned(), ciphertext),
        )
        .await?
        .bytes()
    }

    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.crypt(name.clone(), UnitOperation::Seal(plaintext))
            .await?
            .bytes()
    }

    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        self.crypt(name.clone(), UnitOperation::Open(sealed))
            .await?
            .bytes()
    }

    async fn open_frame_header(
        &self,
        name: &ObjectKey,
        sealed: Bytes,
    ) -> rtdb_storage::Result<FramedObject> {
        self.crypt(name.clone(), UnitOperation::Header(sealed))
            .await?
            .header()
    }

    async fn open_frames(
        &self,
        name: &ObjectKey,
        layout: FramedObject,
        frames: Range<usize>,
        sealed: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        self.crypt(name.clone(), UnitOperation::Frames(layout, frames, sealed))
            .await?
            .bytes()
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        self.matches_authority_key(candidate).await
    }
}

pub(super) fn matches_authority_key(key: &[u8; KEY_BYTES], candidate: [u8; KEY_BYTES]) -> bool {
    let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
    let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
    hmac::verify(&candidate, KEY_COMPARISON, comparison.as_ref()).is_ok()
}

#[async_trait::async_trait]
impl CaptureCryptography for LocalArchiveKey {
    async fn crypt(
        &self,
        name: ObjectKey,
        operation: UnitOperation,
    ) -> rtdb_storage::Result<UnitOutput> {
        let vault = self.unit_vault.clone();
        let slot = UnitScope::archive(&self.id).slot(&name);
        let deadline = Instant::now() + OPERATION_LIMIT;
        let work = self.operate_with(
            move |authority| {
                Ok(
                    vault.with_local_slot(&slot, operation.provisions(), deadline, |material| {
                        with_archive_unit(authority, material, deadline, &name, operation)
                    })?,
                )
            },
            super::journal::key_error,
        );
        let result = tokio::time::timeout_at(deadline.into(), work)
            .await
            .map_err(|_| unit::expired())?;
        if Instant::now() >= deadline {
            return Err(unit::expired());
        }
        result
    }

    async fn matches_authority_key(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        Ok(self
            .operate(move |key| Ok(matches_authority_key(key, candidate)))
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

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
        let layout = key
            .open_frame_header(&name, sealed.slice(..header_bytes))
            .await?;
        let ciphertext = sealed.slice(header_bytes..);

        let files = vault.archive_vault()?;
        let held = files.lock_key(&files.path(id)?)?;
        let memory = MemoryBudget::new(1024);
        let mut read = Box::pin(async {
            let reservation = ReadReservation::new(length, memory.reserve(length).unwrap());
            reservation
                .retaining_io(key.open_frames(&name, layout, 0..1, ciphertext))
                .await
        });
        poll_fn(|context| {
            assert!(read.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(read);
        assert_eq!(memory.used_bytes(), length);

        let mut drain = Box::pin(region.close_and_wait());
        poll_fn(|context| {
            assert!(drain.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(held);
        drain.await;
        assert_eq!(memory.used_bytes(), 0);
        assert!(matches!(
            key.open_frame_header(&name, sealed.slice(..header_bytes))
                .await,
            Err(Error::Closed(_))
        ));
        Ok(())
    }
}
