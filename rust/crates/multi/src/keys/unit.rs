//! Key slots for immutable units in installed storage.

use std::{
    ops::Range,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use ring::digest::{Context, SHA256, digest};
use rtdb_storage::{Error, IoRegion, ObjectKey, RetainedBytes};
use rtdb_types::{Anomaly, Result};

use crate::cipher::Cipher;

use super::{FramedObject, KEY_BYTES, KeyService, LocalKeyService, journal::key_error};

pub(super) const SLOT_PREFIX: &str = "unit-";
pub(super) const OPERATION_LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(crate) struct UnitScope([u8; 32]);

impl UnitScope {
    pub(crate) fn new(
        placement: ordered_logkeeper::ordered::Placement,
        purpose: &'static [u8],
        residence: &[u8],
    ) -> Self {
        let mut digest = Context::new(&SHA256);
        digest.update(b"rtdb.multi/unit-scope/v1\0");
        digest.update(placement.deployment.as_bytes());
        digest.update(placement.incarnation.as_bytes());
        digest.update(placement.group.as_bytes());
        digest.update(&placement.generation.to_be_bytes());
        digest.update(purpose);
        digest.update(b"\0");
        digest.update(residence);
        Self(digest.finish().as_ref().try_into().expect("SHA-256 width"))
    }

    fn slot(self, object: &ObjectKey) -> UnitSlot {
        let name = digest(&SHA256, object.as_str().as_bytes());
        UnitSlot(format!(
            "{SLOT_PREFIX}{}-{}",
            hex(&self.0),
            hex(name.as_ref())
        ))
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("write to String");
    }
    out
}

pub struct UnitSlot(String);

impl UnitSlot {
    pub(super) fn id(&self) -> &str {
        &self.0
    }
}

pub enum UnitOperation {
    Seal(Bytes),
    Open(Bytes),
    OpenRetained(RetainedBytes),
    Header(Bytes),
    Frames(FramedObject, Range<usize>, Bytes),
}

impl UnitOperation {
    pub(super) fn provisions(&self) -> bool {
        matches!(self, Self::Seal(_))
    }

    pub(crate) fn apply(self, key: &Cipher, name: &ObjectKey) -> rtdb_storage::Result<UnitOutput> {
        match self {
            Self::Seal(body) => key.seal_object(name, &body).map(UnitOutput::Bytes),
            Self::Open(sealed) => key.open_object(name, &sealed).map(UnitOutput::Bytes),
            Self::OpenRetained(sealed) => key
                .open_object(name, sealed.as_slice())
                .map(UnitOutput::Bytes),
            Self::Header(sealed) => {
                FramedObject::open_header(key, name, &sealed).map(UnitOutput::Header)
            }
            Self::Frames(layout, frames, sealed) => layout
                .open_frames(key, name, frames, &sealed)
                .map(UnitOutput::Bytes),
        }
    }
}

pub enum UnitOutput {
    Bytes(Bytes),
    Header(FramedObject),
}

impl UnitOutput {
    pub(crate) fn bytes(self) -> rtdb_storage::Result<Bytes> {
        match self {
            Self::Bytes(bytes) => Ok(bytes),
            Self::Header(_) => Err(Error::Task("Expected unit bytes".into())),
        }
    }

    pub(crate) fn header(self) -> rtdb_storage::Result<FramedObject> {
        match self {
            Self::Header(header) => Ok(header),
            Self::Bytes(_) => Err(Error::Task("Expected a sealed unit header".into())),
        }
    }
}

/// The immutable object namespace selects the slot. The key service retains
/// material and tombstones separately from object storage and its backups.
#[derive(Clone)]
pub(crate) struct UnitLedger {
    keys: Arc<dyn KeyService>,
    scope: UnitScope,
    io: Arc<IoRegion>,
    deadline: Option<Instant>,
}

impl UnitLedger {
    pub(crate) fn new(keys: Arc<dyn KeyService>, scope: UnitScope, io: Arc<IoRegion>) -> Self {
        Self {
            keys,
            scope,
            io,
            deadline: None,
        }
    }

    pub(crate) fn bounded(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    pub(crate) async fn operate(
        &self,
        name: &ObjectKey,
        operation: UnitOperation,
    ) -> rtdb_storage::Result<UnitOutput> {
        let work = self
            .keys
            .use_unit_key(self.scope.slot(name), name.clone(), operation, &self.io);
        let Some(deadline) = self.deadline else {
            return work.await;
        };
        let now = Instant::now();
        if now >= deadline {
            return Err(expired());
        }
        let budget = deadline
            .duration_since(now)
            .as_micros()
            .min(i64::MAX as u128) as i64;
        let clock = Arc::new(move || now.elapsed().as_micros().min(i64::MAX as u128) as i64);
        let lease = Arc::new(rtdb_storage::ReadLease::new(Some(budget), clock));
        let result = tokio::time::timeout_at(deadline.into(), IoRegion::leased(Some(lease), work))
            .await
            .map_err(|_| expired())?;
        if Instant::now() >= deadline {
            return Err(expired());
        }
        result
    }

    pub(crate) async fn withdraw(&self, name: &ObjectKey) -> Result<()> {
        self.keys
            .withdraw_unit_key(self.scope.slot(name), &self.io)
            .await
    }
}

pub(super) fn unsupported() -> Anomaly {
    Anomaly::unsupported(
        "rtdb.multi/unit-key-authority",
        "The key provider has no immutable-unit custody ledger",
    )
}

pub(super) fn expired() -> Error {
    Error::Closed("The unit cryptography operation expired".into())
}

pub(super) fn with_material<T>(
    mut material: [u8; KEY_BYTES],
    deadline: Instant,
    operation: impl FnOnce(&Cipher) -> rtdb_storage::Result<T>,
) -> rtdb_storage::Result<T> {
    let cipher = if Instant::now() < deadline {
        Cipher::new(&material).map_err(key_error)
    } else {
        Err(expired())
    };
    material.fill(0);
    let result = operation(&cipher?);
    if Instant::now() >= deadline {
        return Err(expired());
    }
    result
}

impl LocalKeyService {
    pub(super) async fn use_local_unit(
        &self,
        slot: UnitSlot,
        name: ObjectKey,
        operation: UnitOperation,
        io: &Arc<IoRegion>,
    ) -> rtdb_storage::Result<UnitOutput> {
        let service = self.clone();
        let deadline = Instant::now() + OPERATION_LIMIT;
        let result = io
            .run_blocking(move || {
                let vault = service.archive_vault().map_err(key_error)?;
                let path = vault.path(slot.id()).map_err(key_error)?;
                let _lock = vault.lock_key(&path).map_err(key_error)?;
                if Instant::now() >= deadline {
                    return Err(expired());
                }
                let material = vault
                    .read_or_create_key(&path, operation.provisions())
                    .map_err(key_error)?;
                with_material(material, deadline, |key| operation.apply(key, &name))
            })
            .await;
        if Instant::now() >= deadline {
            return Err(expired());
        }
        result
    }

    pub(super) async fn withdraw_local_unit(
        &self,
        slot: UnitSlot,
        io: &Arc<IoRegion>,
    ) -> Result<()> {
        let service = self.clone();
        io.run_blocking(move || {
            let complete = || {
                let vault = service.archive_vault()?;
                let path = vault.path(slot.id())?;
                let _lock = vault.lock_key(&path)?;
                super::archive::withdraw_locked(&vault, &path)
            };
            Ok(complete())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::EncryptingStore;
    use rtdb_storage::{MemoryStore, ObjectStore};
    use uuid::Uuid;

    #[tokio::test]
    async fn withdrawal_survives_reopen_and_leaves_the_resealed_successor_readable() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let io = IoRegion::new();
        let keys: Arc<dyn KeyService> = Arc::new(LocalKeyService::new(directory.path())?);
        let placement = ordered_logkeeper::ordered::Placement {
            deployment: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            group: Uuid::new_v4(),
            generation: 1,
        };
        let original_scope = UnitScope::new(placement, b"serving", b"epoch-1");
        let successor_scope = UnitScope::new(placement, b"checkpoint", b"revision-2");
        let original_ledger = UnitLedger::new(Arc::clone(&keys), original_scope, Arc::clone(&io));
        let successor_ledger = UnitLedger::new(keys, successor_scope, Arc::clone(&io));
        let original_media: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let successor_media: Arc<dyn ObjectStore> = Arc::new(MemoryStore::default());
        let original =
            EncryptingStore::with_units(Arc::clone(&original_media), original_ledger.clone());
        let successor = EncryptingStore::with_units(Arc::clone(&successor_media), successor_ledger);
        let erased = ObjectKey::new("tables/docs/data/l00-rc-b00.arrow")?;
        let survivor = ObjectKey::new("tables/docs/data/l00-rc-b01.arrow")?;
        let body = Bytes::from_static(b"retained document");
        original.put(&erased, body.clone()).await?;
        original.put(&survivor, body.clone()).await?;
        successor.put(&erased, body.clone()).await?;

        let retained_ciphertext = original_media.get(&erased).await?;
        assert_ne!(retained_ciphertext, successor_media.get(&erased).await?);
        assert_eq!(original.get(&erased).await?, body);

        original_ledger.withdraw(&erased).await?;
        let reopened = UnitLedger::new(
            Arc::new(LocalKeyService::new(directory.path())?),
            original_scope,
            Arc::clone(&io),
        );
        let reopened = EncryptingStore::with_units(original_media, reopened);
        assert!(matches!(
            reopened.get(&erased).await,
            Err(Error::ErasedUnderView)
        ));
        assert!(matches!(
            reopened.put(&erased, body.clone()).await,
            Err(Error::ErasedUnderView)
        ));
        assert_eq!(reopened.get(&survivor).await?, body);
        assert_eq!(successor.get(&erased).await?, body);
        Ok(())
    }
}
