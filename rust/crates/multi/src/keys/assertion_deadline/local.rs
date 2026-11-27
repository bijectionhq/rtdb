use std::{collections::BTreeSet, fs, io::ErrorKind};

use rtdb_types::Result;
use uuid::Uuid;

use crate::keys::{LocalKeyService, assertion::family::{LOCAL_PREFIX, MAX_RECORD_BYTES, local_record}, io};

use super::{DeadlineJournal, invalid};

const PREFIX: &str = "erasure-";
const PAGE_SIZE: usize = 100;

#[async_trait::async_trait]
impl DeadlineJournal for LocalKeyService {
    async fn read(&self, order: Uuid) -> Result<Option<(Vec<u8>, String)>> {
        let owner = self.clone();
        in_vault(owner, move |vault| {
            let path = vault.path(&format!("{LOCAL_PREFIX}{PREFIX}{order}"))?;
            let _lock = vault.lock_key_shared(&path)?;
            Ok(local_record(vault, &path)?.map(|(bytes, version)| (bytes.to_vec(), version)))
        }).await
    }

    async fn compare_and_set(&self, order: Uuid, previous: Option<&str>, bytes: Vec<u8>) -> Result<bool> {
        if bytes.is_empty() || bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("The assertion erasure ledger exceeds its record limit"));
        }
        let previous = previous.map(str::to_owned);
        in_vault(self.clone(), move |vault| {
            let path = vault.path(&format!("{LOCAL_PREFIX}{PREFIX}{order}"))?;
            let _lock = vault.lock_key(&path)?;
            if local_record(vault, &path)?.as_ref().map(|(_, version)| version) != previous.as_ref() {
                return Ok(false);
            }
            vault.publish(&path, &bytes)?;
            Ok(true)
        }).await
    }

    async fn page(&self, after: Option<Uuid>) -> Result<Vec<Uuid>> {
        in_vault(self.clone(), move |vault| {
            let prefix = format!("{LOCAL_PREFIX}{PREFIX}");
            let mut orders = BTreeSet::new();
            for entry in fs::read_dir(&vault.directory).map_err(io)? {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(error) if error.kind() == ErrorKind::NotFound => continue,
                    Err(error) => return Err(io(error)),
                };
                let name = entry.file_name();
                let Some(id) = name.to_str().and_then(|name| name.strip_prefix(&prefix))
                    .and_then(|name| name.strip_suffix(".key")) else { continue; };
                let order = id.parse::<Uuid>().map_err(|_| invalid("Invalid assertion erasure order address"))?;
                if order.to_string() != id || order.is_nil() {
                    return Err(invalid("Invalid assertion erasure order address"));
                }
                if after.is_none_or(|after| order > after) {
                    orders.insert(order);
                    if orders.len() > PAGE_SIZE { orders.pop_last(); }
                }
            }
            Ok(orders.into_iter().collect())
        }).await
    }
}

async fn in_vault<T: Send + 'static>(
    owner: LocalKeyService, operation: impl FnOnce(&LocalKeyService) -> Result<T> + Send + 'static,
) -> Result<T> {
    owner.require_assertion_broker()?;
    tokio::task::spawn_blocking(move || operation(&owner.archive_vault()?))
        .await.map_err(|_| invalid("The local assertion erasure journal is unavailable"))?
}
