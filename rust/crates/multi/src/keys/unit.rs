//! Key slots for immutable units in installed storage.

use std::{
    fs::{self, File},
    io::ErrorKind,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use ring::digest::{Context, SHA256, digest};
use rtdb_storage::{Error, IoRegion, ObjectKey, sealing::ObjectCipher};
use rtdb_types::{Anomaly, Result};

use crate::cipher::Cipher;

use super::{KEY_BYTES, KeyService, LocalKeyService, UnitOperation, UnitOutput, journal::key_error};

pub(super) const SLOT_PREFIX: &str = "unit-";
const NAMESPACE_CLOSED: &[u8] = b"RTDB-unit-namespace-closed-v1";
pub(super) const OPERATION_LIMIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub struct UnitScope([u8; 32]);

impl UnitScope {
    pub(crate) fn new(
        placement: ordered_logkeeper::ordered::Placement,
        purpose: &'static [u8],
        residence: &[u8],
    ) -> Self {
        let mut digest = Context::new(&SHA256);
        digest.update(b"rtdb.multi/unit-scope/v2\0");
        digest.update(placement.deployment.as_bytes());
        digest.update(placement.incarnation.as_bytes());
        digest.update(placement.group.as_bytes());
        digest.update(&placement.generation.to_be_bytes());
        digest.update(purpose);
        digest.update(b"\0");
        digest.update(residence);
        Self(digest.finish().as_ref().try_into().expect("SHA-256 width"))
    }

    pub(super) fn namespace(self) -> String {
        format!("{SLOT_PREFIX}{}", hex(&self.0))
    }

    pub(super) fn archive(id: &str) -> Self {
        let mut digest = Context::new(&SHA256);
        digest.update(b"rtdb.multi/archive-unit-scope/v1\0");
        digest.update(id.as_bytes());
        Self(digest.finish().as_ref().try_into().expect("SHA-256 width"))
    }

    pub(super) fn slot(self, object: &ObjectKey) -> UnitSlot {
        let name = digest(&SHA256, object.as_str().as_bytes());
        UnitSlot {
            scope: self,
            digest: name.as_ref().try_into().expect("SHA-256 width"),
        }
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

pub struct UnitSlot {
    pub(super) scope: UnitScope,
    pub(super) digest: [u8; 32],
}

impl UnitSlot {
    pub(super) fn id(&self) -> String {
        format!("{}-{}", self.scope.namespace(), hex(&self.digest))
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

    pub(crate) async fn withdraw_scope(&self) -> Result<()> {
        self.keys.withdraw_unit_scope(self.scope, &self.io).await
    }

    pub(crate) async fn withdraw(&self, name: &ObjectKey) -> Result<()> {
        self.keys
            .withdraw_unit_key(self.scope.slot(name), &self.io)
            .await
    }
}

#[async_trait::async_trait]
impl ObjectCipher for UnitLedger {
    async fn operate(&self, name: &ObjectKey, operation: UnitOperation) -> rtdb_storage::Result<UnitOutput> {
        UnitLedger::operate(self, name, operation).await
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

fn local_scope_closed(path: &Path) -> Result<bool> {
    match fs::read(path) {
        Ok(marker) if marker == NAMESPACE_CLOSED => Ok(true),
        Ok(_) => Err(super::malformed()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(error) => Err(super::io(error)),
    }
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
                vault.with_local_slot(&slot, operation.provisions(), deadline, |material| {
                    with_material(material, deadline, |key| operation.apply(key, &name))
                })
            })
            .await;
        if Instant::now() >= deadline {
            return Err(expired());
        }
        result
    }

    pub(super) fn with_local_slot<T>(
        &self,
        slot: &UnitSlot,
        provision: bool,
        deadline: Instant,
        operation: impl FnOnce([u8; KEY_BYTES]) -> rtdb_storage::Result<T>,
    ) -> rtdb_storage::Result<T> {
        let scope = self.path(&slot.scope.namespace()).map_err(key_error)?;
        let gate = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(scope.with_extension("lock"))
            .map_err(super::io)
            .map_err(key_error)?;
        gate.lock_shared().map_err(super::io).map_err(key_error)?;
        if local_scope_closed(&scope).map_err(key_error)? {
            return Err(Error::ErasedUnderView);
        }
        let path = self.path(&slot.id()).map_err(key_error)?;
        let _lock = self.lock_key(&path).map_err(key_error)?;
        if Instant::now() >= deadline {
            return Err(expired());
        }
        let material = self
            .read_or_create_key(&path, provision)
            .map_err(key_error)?;
        operation(material)
    }

    pub(super) async fn withdraw_local_scope(
        &self,
        scope: UnitScope,
        io: &Arc<IoRegion>,
    ) -> Result<()> {
        let service = self.clone();
        io.run_blocking(move || {
            let complete = || {
                let vault = service.archive_vault()?;
                let namespace = scope.namespace();
                let marker = vault.path(&namespace)?;
                let _gate = vault.lock_key(&marker)?;
                if !local_scope_closed(&marker)? {
                    vault.publish(&marker, NAMESPACE_CLOSED)?;
                }
                let prefix = format!("{namespace}-");
                let mut slots = std::collections::BTreeSet::new();
                for entry in fs::read_dir(&vault.directory).map_err(super::io)? {
                    let entry = entry.map_err(super::io)?;
                    let path = entry.path();
                    let Some(id) = path.file_stem().and_then(|name| name.to_str()) else {
                        continue;
                    };
                    let Some(unit) = id.strip_prefix(&prefix) else {
                        continue;
                    };
                    if unit.len() == 64 && unit.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        slots.insert(vault.path(id)?);
                    }
                }
                // The namespace lock excludes provisioning while the vault
                // reconciles slots, including slots with only a pending write.
                for path in slots {
                    let _lock = vault.lock_key(&path)?;
                    super::archive::withdraw_locked(&vault, &path)?;
                }
                vault.sync_directory()
            };
            Ok(complete())
        })
        .await?
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
                let scope = vault.path(&slot.scope.namespace())?;
                let _gate = vault.lock_key(&scope)?;
                let path = vault.path(&slot.id())?;
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
    use bytes::Bytes;
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
            EncryptingStore::with_units(Arc::clone(&original_media), Arc::new(original_ledger.clone()));
        let successor = EncryptingStore::with_units(Arc::clone(&successor_media), Arc::new(successor_ledger));
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
        let reopened = EncryptingStore::with_units(original_media, Arc::new(reopened));
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

        let orphan = ObjectKey::new("unpublished")?;
        original_ledger
            .operate(&orphan, UnitOperation::Seal(body.clone()))
            .await?;
        original_ledger.withdraw_scope().await?;
        original_ledger.withdraw_scope().await?;
        assert!(matches!(
            reopened.get(&survivor).await,
            Err(Error::ErasedUnderView)
        ));
        assert!(matches!(
            reopened.put(&orphan, body.clone()).await,
            Err(Error::ErasedUnderView)
        ));
        assert_eq!(successor.get(&erased).await?, body);
        Ok(())
    }
}
