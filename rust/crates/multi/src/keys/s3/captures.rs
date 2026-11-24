use std::{sync::Arc, time::Instant};

use rtdb_storage::{IoRegion, ObjectKey};
use rtdb_types::Result;

use super::{
    KEY_BYTES, KeyService, OPERATION_LIMIT, S3KeyService, UnitOperation, UnitOutput, UnitScope,
    key_error, unit,
};
use crate::keys::archive::{
    ArchiveKey, CaptureCryptography, matches_authority_key, tracked_archive_key, with_archive_unit,
};

pub(super) struct CaptureAuthority {
    pub(super) vault: S3KeyService,
    pub(super) id: String,
}

#[async_trait::async_trait]
impl CaptureCryptography for CaptureAuthority {
    async fn crypt(
        &self,
        name: ObjectKey,
        operation: UnitOperation,
    ) -> rtdb_storage::Result<UnitOutput> {
        let slot = UnitScope::archive(&self.id).slot(&name);
        let deadline = Instant::now() + OPERATION_LIMIT;
        tokio::time::timeout_at(deadline.into(), async {
            self.vault
                .require_never_versioned()
                .await
                .map_err(key_error)?;
            let authority = self.vault.existing(&self.id).await.map_err(key_error)?;
            let material = self
                .vault
                .load_unit_slot(&slot, operation.provisions())
                .await?;
            with_archive_unit(&authority, material, deadline, &name, operation)
        })
        .await
        .map_err(|_| unit::expired())?
    }

    async fn matches_authority_key(&self, candidate: [u8; KEY_BYTES]) -> Result<bool> {
        let key = self.vault.existing(&self.id).await?;
        Ok(matches_authority_key(&key, candidate))
    }
}

impl S3KeyService {
    pub(super) async fn open_capture(
        &self,
        id: &str,
        provision: bool,
        _region: Arc<IoRegion>,
    ) -> Result<Arc<dyn ArchiveKey>> {
        if provision {
            self.key(id).await?;
        } else {
            self.existing(id).await?;
        }
        Ok(tracked_archive_key(CaptureAuthority {
            vault: self.clone(),
            id: id.to_owned(),
        }))
    }
}
