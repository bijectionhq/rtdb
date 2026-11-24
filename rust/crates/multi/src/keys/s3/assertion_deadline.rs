use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use super::{Precondition, S3KeyService, malformed, unavailable};
use crate::keys::assertion_deadline::DeadlineJournal;

const PREFIX: &str = "assertion-erasure-journal/";
const MAX_RECORD_BYTES: usize = 1024 * 1024;
const PAGE_SIZE: i32 = 100;

fn path(order: Uuid) -> String {
    format!("{PREFIX}{order}")
}

#[async_trait::async_trait]
impl DeadlineJournal for S3KeyService {
    async fn read(&self, order: Uuid) -> Result<Option<(Vec<u8>, String)>> {
        let object = self.read_vault_object(&path(order), MAX_RECORD_BYTES).await?;
        Ok(object.map(|(bytes, etag)| (bytes.to_vec(), etag)))
    }

    async fn compare_and_set(&self, order: Uuid, previous: Option<&str>, bytes: Vec<u8>) -> Result<bool> {
        if bytes.is_empty() || bytes.len() > MAX_RECORD_BYTES {
            return Err(Anomaly::incorrect(
                "rtdb.multi/assertion-deadline-record", "The assertion erasure ledger exceeds its record limit",
            ));
        }
        self.require_never_versioned().await?;
        let condition = previous.map_or(Precondition::Absent, Precondition::Matches);
        self.put(&path(order), &bytes, condition).await
    }

    async fn page(&self, after: Option<Uuid>) -> Result<Vec<Uuid>> {
        let page = self.client.list_objects_v2().bucket(&self.bucket).prefix(PREFIX)
            .set_start_after(after.map(path)).max_keys(PAGE_SIZE).send().await
            .map_err(|_| unavailable("scan assertion erasure journal"))?;
        page.contents().iter().map(|entry| {
            let key = entry.key().ok_or_else(malformed)?;
            let order = key.strip_prefix(PREFIX).ok_or_else(malformed)?
                .parse::<Uuid>().map_err(|_| malformed())?;
            if path(order) != key {
                return Err(malformed());
            }
            Ok(order)
        }).collect()
    }
}
