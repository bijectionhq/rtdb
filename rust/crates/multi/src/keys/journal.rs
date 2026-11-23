use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use ordered_logkeeper::ordered::Placement;
use rtdb_storage::{Error, IoRegion};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use crate::cipher::Cipher;

use super::{
    KeyService, LocalKeyService, archive::{self, LocalArchiveKey}, unit::{self, OPERATION_LIMIT},
};

/// The payload key survives writer replacement and changes only on rotation.
#[derive(Clone, Copy)]
pub struct JournalKey {
    deployment: Uuid,
    incarnation: Uuid,
    generation: u64,
}

impl JournalKey {
    pub fn new(placement: Placement, generation: u64) -> Self {
        Self {
            deployment: placement.deployment,
            incarnation: placement.incarnation,
            generation,
        }
    }

    pub(crate) fn id(self) -> String {
        format!("journal-{}-{}-{}", self.deployment, self.incarnation, self.generation)
    }

    pub(crate) async fn open(
        self,
        keys: &dyn KeyService,
        io: &Arc<IoRegion>,
        ciphertext: Bytes,
    ) -> rtdb_storage::Result<Bytes> {
        keys.open_journal_payload(self, io, ciphertext).await
    }
}

pub(super) fn key_error(error: Anomaly) -> Error {
    match error.code.as_ref() {
        "rtdb.multi/key-destroyed" | "rtdb.multi/archive-revoked" => Error::ErasedUnderView,
        _ => Error::Remote { key: "journal payload key".into(), message: error.to_string() },
    }
}

impl LocalKeyService {
    pub(super) fn provision_journal(&self, key: JournalKey) -> Result<()> {
        self.require_service_root()?;
        let id = key.id();
        let raw_path = self.path(&id)?;
        let _raw_lock = self.lock_key(&raw_path)?;
        if raw_path.try_exists().map_err(super::io)? {
            return Err(Anomaly::incorrect(
                "rtdb.multi/journal-key-exported",
                "The journal key identity already has exportable material",
            ));
        }
        let vault = self.archive_vault()?;
        let path = vault.path(&id)?;
        let _lock = vault.lock_key(&path)?;
        if archive::expire_locked(&vault, &path)?.is_some() {
            return Err(archive::revoked());
        }
        vault.read_or_create_key(&path, true)?;
        Ok(())
    }

    pub(super) async fn journal_payload(
        &self,
        key: JournalKey,
        io: &Arc<IoRegion>,
        operation: impl FnOnce(&Cipher, &str) -> rtdb_storage::Result<Bytes> + Send + 'static,
    ) -> rtdb_storage::Result<Bytes> {
        let deadline = Instant::now() + OPERATION_LIMIT;
        let service = self.clone();
        tokio::time::timeout_at(deadline.into(), async {
            let vault = io.run_blocking(move || service.archive_vault().map_err(key_error)).await?;
            let id = key.id();
            let handle = LocalArchiveKey { vault, id: id.clone(), region: Arc::clone(io) };
            let result = handle.operate_with(
                move |material| {
                    if Instant::now() >= deadline { return Err(unit::expired().into()); }
                    Ok(operation(&Cipher::new(material)?, &id)?)
                },
                key_error,
            ).await;
            if Instant::now() >= deadline { return Err(unit::expired()); }
            result
        }).await.map_err(|_| unit::expired())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_retained_record_uses_its_own_key_until_that_generation_expires() -> Result<()> {
        let directory = tempfile::tempdir().unwrap();
        let vault = LocalKeyService::new(directory.path())?;
        let io = IoRegion::new();
        let placement = Placement {
            deployment: Uuid::new_v4(),
            incarnation: Uuid::new_v4(),
            group: Uuid::new_v4(),
            generation: 1,
        };
        let old = JournalKey::new(placement, 1);
        let new = JournalKey::new(placement, 2);
        let unknown = JournalKey::new(placement, 3);
        let other = JournalKey::new(Placement { incarnation: Uuid::new_v4(), ..placement }, 1);
        for key in [old, new, other] {
            vault.provision_journal_key(key).await?;
        }
        let plaintext = Bytes::from_static(b"chosen journal payload");
        let retained = vault.seal_journal_payload(old, &io, plaintext.clone()).await?;
        let suffix = vault.seal_journal_payload(new, &io, plaintext.clone()).await?;
        assert_eq!(old.open(&vault, &io, retained.clone()).await?, plaintext);
        assert!(new.open(&vault, &io, retained.clone()).await.is_err());
        assert!(other.open(&vault, &io, retained.clone()).await.is_err());
        let missing = unknown.open(&vault, &io, retained.clone()).await.unwrap_err();
        assert!(!matches!(missing, Error::ErasedUnderView));
        assert!(vault.existing(&old.id()).await.is_err());

        assert!(vault.schedule_archive_key(&old.id(), rtdb_directory::now_ms() - 1).await?);
        let reopened = LocalKeyService::new(directory.path())?;
        let erased = old.open(&reopened, &io, retained).await.unwrap_err();
        assert_eq!(Anomaly::from(erased).code, "rtdb/erased-under-view");
        assert!(reopened.provision_journal_key(old).await.is_err());
        assert_eq!(new.open(&reopened, &io, suffix).await?, plaintext);
        Ok(())
    }
}
