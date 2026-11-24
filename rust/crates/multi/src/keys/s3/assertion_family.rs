use std::sync::Arc;

use bytes::Bytes;
use rtdb_storage::IoRegion;
use rtdb_types::Result;
use uuid::Uuid;

use super::{Precondition, S3KeyService};
use crate::keys::assertion::family::{FamilyJournal, MAX_RECORD_BYTES};

fn path(family: Uuid) -> String {
    format!("assertion-families/{family}")
}

#[async_trait::async_trait]
impl FamilyJournal for S3KeyService {
    async fn read_family(&self, family: Uuid, _region: &Arc<IoRegion>) -> Result<Option<(Bytes, String)>> {
        self.require_never_versioned().await?;
        self.read_vault_object(&path(family), MAX_RECORD_BYTES).await
    }

    async fn write_family(
        &self, family: Uuid, _region: &Arc<IoRegion>, previous: Option<&str>, bytes: Bytes,
    ) -> Result<bool> {
        self.require_never_versioned().await?;
        let condition = previous.map_or(Precondition::Absent, Precondition::Matches);
        self.put(&path(family), &bytes, condition).await
    }
}
