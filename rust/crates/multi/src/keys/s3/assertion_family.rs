use std::sync::Arc;

use bytes::Bytes;
use rtdb_storage::IoRegion;
use rtdb_types::Result;

use super::{Precondition, S3KeyService};
use crate::keys::assertion::family::{FamilyJournal, FamilyRecord, MAX_RECORD_BYTES};

fn path(record: FamilyRecord) -> String {
    format!("assertion-families/{record}")
}

#[async_trait::async_trait]
impl FamilyJournal for S3KeyService {
    async fn read_family(&self, record: FamilyRecord, _region: &Arc<IoRegion>) -> Result<Option<(Bytes, String)>> {
        self.require_never_versioned().await?;
        self.read_vault_object(&path(record), MAX_RECORD_BYTES).await
    }

    async fn write_family(
        &self, record: FamilyRecord, _region: &Arc<IoRegion>, previous: Option<&str>, bytes: Bytes,
    ) -> Result<bool> {
        self.require_never_versioned().await?;
        let condition = previous.map_or(Precondition::Absent, Precondition::Matches);
        self.put(&path(record), &bytes, condition).await
    }
}
