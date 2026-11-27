//! Operation-only custody for a checkpoint archive and its dependent suffix.

use std::{fs, io::ErrorKind, ops::Range, path::Path, sync::Arc, time::{Duration, Instant}};

use bytes::Bytes;
use ring::{
    digest::{SHA256, digest},
    hmac,
};
use rtdb_storage::{Error, IoRegion, ObjectKey, sealing::{ObjectCipher, PlaintextView}};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use crate::cipher::Cipher;

mod revocations;

use super::{
    DESTROYED, FramedObject, KEY_BYTES, LocalKeyService, UnitOperation, UnitOutput,
    UnitScope, io, is_destroyed, malformed, read_key, require_general_key_namespace,
    unit::{self, OPERATION_LIMIT},
};

#[async_trait::async_trait]
pub trait ArchiveKey: ObjectCipher {
    async fn seal(&self, associated: &str, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.seal_prefixed(associated, Bytes::new(), plaintext).await
    }
    async fn seal_prefixed(
        &self, associated: &str, prefix: Bytes, plaintext: Bytes,
    ) -> rtdb_storage::Result<Bytes>;
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
impl ObjectCipher for LegacyArchiveKey {
    async fn operate(&self, name: &ObjectKey, operation: UnitOperation) -> rtdb_storage::Result<UnitOutput> {
        if matches!(operation, UnitOperation::Grant) {
            return Err(Error::Closed("Raw archive material cannot confirm delivery".into()));
        }
        operation.apply_async(&self.cipher, name).await
    }
}

#[async_trait::async_trait]
impl ArchiveKey for LegacyArchiveKey {
    async fn seal_prefixed(
        &self, associated: &str, prefix: Bytes, plaintext: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        self.cipher.seal_deterministic_prefixed(associated, &prefix, &plaintext)
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
    if revocations::read_revocation(path)?.is_some() {
        return Err(schedule_conflict());
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
    if revocations::revocation_deadline(path)?.is_some() {
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
    let Some(due_at_ms) = revocations::revocation_deadline(path)? else {
        return Ok(None);
    };
    let material = zeroize::Zeroizing::new(fs::read(path).map_err(io)?);
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

    async fn grant(&self, name: &ObjectKey) -> rtdb_storage::Result<UnitOutput> {
        self.crypt(name.clone(), UnitOperation::Grant).await
    }
}

struct CaptureKey<T>(T);

#[async_trait::async_trait]
impl<T: CaptureCryptography> ObjectCipher for CaptureKey<T> {
    async fn operate(&self, name: &ObjectKey, operation: UnitOperation) -> rtdb_storage::Result<UnitOutput> {
        match operation {
            UnitOperation::Grant => self.0.grant(name).await,
            operation => self.0.crypt(name.clone(), operation).await,
        }
    }
}

pub(super) fn tracked_archive_key(key: impl CaptureCryptography + 'static) -> Arc<dyn ArchiveKey> {
    let key = Arc::new(CaptureKey(key));
    Arc::new(TrackedArchiveKey { inner: key.clone(), delivery: key })
}

struct TrackedArchiveKey {
    inner: Arc<dyn ArchiveKey>,
    delivery: Arc<dyn ObjectCipher>,
}

impl TrackedArchiveKey {
    fn track(&self, name: &ObjectKey) -> rtdb_storage::Result<()> {
        PlaintextView::track(&self.delivery, name)
    }
}

#[async_trait::async_trait]
impl ObjectCipher for TrackedArchiveKey {
    async fn operate(&self, name: &ObjectKey, operation: UnitOperation) -> rtdb_storage::Result<UnitOutput> {
        if matches!(operation, UnitOperation::Grant) {
            self.delivery.operate(name, operation).await
        } else {
            self.inner.operate(name, operation).await
        }
    }
}

#[async_trait::async_trait]
impl ArchiveKey for TrackedArchiveKey {
    async fn seal_prefixed(
        &self, associated: &str, prefix: Bytes, plaintext: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        self.inner.seal_prefixed(associated, prefix, plaintext).await
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        let plaintext = self.inner.open(associated, ciphertext).await?;
        self.track(&associated_unit(associated)?)?;
        Ok(plaintext)
    }

    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.inner.seal_object(name, plaintext).await
    }

    async fn open_object(&self, name: &ObjectKey, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        let plaintext = self.inner.open_object(name, ciphertext).await?;
        self.track(name)?;
        Ok(plaintext)
    }

    async fn open_frame_header(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<FramedObject> {
        let header = self.inner.open_frame_header(name, sealed).await?;
        self.track(name)?;
        Ok(header)
    }

    async fn open_frames(
        &self, name: &ObjectKey, layout: FramedObject, frames: Range<usize>, sealed: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        let plaintext = self.inner.open_frames(name, layout, frames, sealed).await?;
        self.track(name)?;
        Ok(plaintext)
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        self.inner.matches(candidate).await
    }
}

pub(super) fn associated_unit(associated: &str) -> rtdb_storage::Result<ObjectKey> {
    use std::fmt::Write;
    let mut name = String::from("archive-records/");
    for byte in digest(&SHA256, associated.as_bytes()).as_ref() {
        write!(name, "{byte:02x}").expect("write to String");
    }
    ObjectKey::new(name)
}

fn with_archive_unit(
    authority: &[u8; KEY_BYTES],
    mut material: [u8; KEY_BYTES],
    deadline: Instant,
    name: &ObjectKey,
    operation: UnitOperation,
) -> rtdb_storage::Result<UnitOutput> {
    unit::with_material(archive_material(authority, &mut material), deadline, |cipher| {
        operation.apply(cipher, name)
    })
}

#[cfg(any(feature = "dynamodb", feature = "s3-key-vault"))]
pub(super) fn apply_archive_unit<'a>(
    authority: &mut [u8; KEY_BYTES],
    material: &mut [u8; KEY_BYTES],
    deadline: Instant,
    name: &'a ObjectKey,
    operation: UnitOperation,
) -> impl std::future::Future<Output = rtdb_storage::Result<UnitOutput>> + use<'a> {
    let mut combined = archive_material(authority, material);
    authority.fill(0);
    unit::apply_with_material(&mut combined, deadline, name, operation)
}

fn archive_material(authority: &[u8; KEY_BYTES], material: &mut [u8; KEY_BYTES]) -> [u8; KEY_BYTES] {
    let key = hmac::Key::new(hmac::HMAC_SHA256, authority);
    let combined = hmac::sign(&key, material);
    material.fill(0);
    combined.as_ref().try_into().expect("HMAC-SHA256 width")
}

#[async_trait::async_trait]
impl<T: CaptureCryptography> ArchiveKey for CaptureKey<T> {
    async fn seal_prefixed(
        &self, associated: &str, prefix: Bytes, plaintext: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        self.0.crypt(
            associated_unit(associated)?,
            UnitOperation::SealPrefixedValue {
                associated: associated.to_owned(),
                prefix,
                body: plaintext,
            },
        )
        .await?
        .bytes()
    }

    async fn open(&self, associated: &str, ciphertext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.0.crypt(
            associated_unit(associated)?,
            UnitOperation::OpenValue(associated.to_owned(), ciphertext),
        )
        .await?
        .bytes()
    }

    async fn seal_object(&self, name: &ObjectKey, plaintext: Bytes) -> rtdb_storage::Result<Bytes> {
        self.0.crypt(name.clone(), UnitOperation::Seal(plaintext))
            .await?
            .bytes()
    }

    async fn open_object(&self, name: &ObjectKey, sealed: Bytes) -> rtdb_storage::Result<Bytes> {
        self.0.crypt(name.clone(), UnitOperation::Open(sealed))
            .await?
            .bytes()
    }

    async fn open_frame_header(
        &self,
        name: &ObjectKey,
        sealed: Bytes,
    ) -> rtdb_storage::Result<FramedObject> {
        self.0.crypt(name.clone(), UnitOperation::Header(sealed))
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
        self.0.crypt(name.clone(), UnitOperation::Frames(layout, frames, sealed))
            .await?
            .bytes()
    }

    async fn matches(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        self.0.matches_authority_key(candidate).await
    }
}

pub(super) fn matches_authority_key(key: &[u8; KEY_BYTES], candidate: [u8; KEY_BYTES]) -> bool {
    let candidate = hmac::Key::new(hmac::HMAC_SHA256, &candidate);
    let comparison = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, key), KEY_COMPARISON);
    hmac::verify(&candidate, KEY_COMPARISON, comparison.as_ref()).is_ok()
}

#[async_trait::async_trait]
impl CaptureCryptography for LocalArchiveKey {
    async fn grant(&self, name: &ObjectKey) -> rtdb_storage::Result<UnitOutput> {
        let vault = self.vault.clone();
        let unit_vault = self.unit_vault.clone();
        let id = self.id.clone();
        let slot = UnitScope::archive(&id).slot(name);
        // A grant can be confirmed after the archive's object region has drained.
        IoRegion::new().run_blocking(move || {
            let map = super::journal::key_error;
            let path = vault.path(&id).map_err(map)?;
            let authority = vault.lock_key_shared(&path).map_err(map)?;
            let mut secret = read_key(&path, fs::read(&path).map_err(io).map_err(map)?).map_err(map)?;
            secret.fill(0);
            let until = read_schedule(&path).map_err(map)?.map(|due| {
                let remaining = u64::try_from(due.saturating_sub(rtdb_directory::now_ms())).unwrap_or_default();
                if remaining == 0 { return Err(Error::ErasedUnderView); }
                Instant::now().checked_add(Duration::from_millis(remaining)).ok_or_else(unit::expired)
            }).transpose()?;
            let unit = unit_vault.hold_local_unit_delivery(&slot)?;
            let custody = Arc::new((authority, unit));
            Ok(UnitOutput::HeldGrant { until, custody })
        }).await
    }

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
    use super::revocations::RevocationStep;
    use super::*;
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };

    use crate::keys::{ArchiveRevocation, KeyService};
    use rtdb_storage::ReadReservation;
    use rtdb_types::MemoryBudget;

    #[tokio::test]
    async fn capture_revocation_retries_the_same_order_after_partial_withdrawal() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(directory.path())?;
        let key = vault.archive_key("capture", true, IoRegion::new()).await?;
        let body = key.seal("member", Bytes::from_static(b"checkpoint")).await?;
        let request = ArchiveRevocation {
            operation_id: uuid::Uuid::new_v4(),
            request_sha256: [1; 32],
            not_before_ms: rtdb_directory::now_ms(),
        };
        assert_eq!(vault.update_archive_revocation("capture", request, RevocationStep::WithdrawRoot).await?, None);
        assert!(key.open("member", body).await.is_err());
        drop(vault);

        let reopened = LocalKeyService::new(directory.path())?;
        assert!(reopened.reconcile_archive_revocation("capture", ArchiveRevocation {
            request_sha256: [2; 32], ..request
        }).await.is_err());
        let (first, retry) = tokio::join!(
            reopened.reconcile_archive_revocation("capture", request),
            reopened.reconcile_archive_revocation("capture", request),
        );
        let terminal = first?;
        assert_eq!(retry?, terminal);
        drop(reopened);
        assert_eq!(LocalKeyService::new(directory.path())?
            .reconcile_archive_revocation("capture", request).await?, terminal);
        Ok(())
    }

    #[tokio::test]
    async fn archive_delivery_owns_the_capture_and_refuses_expired_plaintext() -> Result<()> {
        let temp = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::with_archive_broker(temp.path())?;
        let io = IoRegion::new();
        let id = "answer";
        let key = vault.archive_key(id, true, io.clone()).await?;
        let name = ObjectKey::new("tables/t/data/l00-rc-b00.arrow")?;
        let sealed = key.seal_object(&name, Bytes::from_static(b"held answer")).await?;
        let view = PlaintextView::new(MemoryBudget::new(64 << 10));
        let retained = view.reading(key.open_object(&name, sealed)).await?;
        io.close_and_wait().await;
        assert_eq!(retained.as_ref(), b"held answer");

        let broker = vault.archive_vault()?;
        let lock = broker.open_key_lock(&broker.path(id)?)?;
        let grant = view.grant().await?;
        assert!(matches!(lock.try_lock(), Err(std::fs::TryLockError::WouldBlock)));
        drop(grant);
        assert!(vault.schedule_archive_key(id, rtdb_directory::now_ms() - 1).await?);
        assert_eq!(view.grant().await.err().unwrap().code, "rtdb/erased-under-view");
        Ok(())
    }

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
