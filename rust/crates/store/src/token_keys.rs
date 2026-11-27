use std::sync::{Arc, atomic::{AtomicU64, Ordering}};

use async_trait::async_trait;
use rtdb_crypt::{Generation, Kms, tokens::{TokenKey, TokenKeys}};
use rtdb_storage::{Error, ObjectKey, ObjectStore, ObjectVersionHold};
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result};
use uuid::Uuid;

use crate::objects::read_held_bytes;

pub struct StoredTokenKeys {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    incarnation: Uuid,
    hint: AtomicU64,
}

impl StoredTokenKeys {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String, incarnation: Uuid, initial_generation: u64) -> Self {
        Self { store, prefix, incarnation, hint: AtomicU64::new(initial_generation) }
    }

    pub async fn rotate(&self, kms: &dyn Kms, next: u64, unreadable_by: i64) -> Result<()> {
        let previous = next.checked_sub(1).ok_or_else(invalid_metadata)?;
        let previous = self.get(previous).await?.ok_or_else(invalid_metadata)?;
        kms.retire(previous.generation(), unreadable_by).await?;
        self.install(kms, next).await?;
        Ok(())
    }

    pub async fn destroy_retired(&self, kms: &dyn Kms, number: u64) -> Result<()> {
        let key = self.get(number).await?.ok_or_else(invalid_metadata)?;
        kms.destroy(key.generation()).await
    }

    fn path(&self, number: u64) -> Result<ObjectKey> {
        ObjectKey::new(format!("{}/keys/{number:016x}", self.prefix)).map_err(Into::into)
    }

    /// Checks the held provider version and wrapping of the selected token key envelope.
    pub async fn check_object_hold(
        &self, number: u64, kms: &dyn Kms, hold: &dyn ObjectVersionHold,
    ) -> Result<PhysicalObjectVersion> {
        let (bytes, version) = read_held_bytes(self.store.as_ref(), &self.path(number)?, TokenKey::MAX_ENCODED_BYTES, hold).await?;
        self.decode(number, &bytes)?.verify_wrapping(kms).await?;
        Ok(version)
    }

    fn decode(&self, number: u64, bytes: &[u8]) -> Result<TokenKey> {
        let key = TokenKey::decode(bytes)?;
        if key.generation() != (Generation { database: self.incarnation, number }) {
            return Err(invalid_metadata());
        }
        Ok(key)
    }

    pub async fn install(&self, kms: &dyn Kms, number: u64) -> Result<TokenKey> {
        if let Some(key) = self.get(number).await? {
            return Ok(key);
        }
        let generation = Generation {
            database: self.incarnation,
            number,
        };
        let proposed = TokenKey::create(kms, generation).await?;
        let written = self
            .store
            .put(&self.path(number)?, proposed.encode().into())
            .await;
        if let Some(installed) = self.get(number).await? {
            return Ok(installed);
        }
        written?;
        Err(unavailable())
    }
}

#[async_trait]
impl TokenKeys for StoredTokenKeys {
    async fn current(&self) -> Result<TokenKey> {
        let mut number = self.hint.load(Ordering::Relaxed);
        let mut key = self.get(number).await?.ok_or_else(unavailable)?;
        while let Some(next) = number.checked_add(1) {
            let Some(found) = self.get(next).await? else {
                break;
            };
            number = next;
            key = found;
        }
        self.hint.fetch_max(number, Ordering::Relaxed);
        Ok(key)
    }

    async fn get(&self, number: u64) -> Result<Option<TokenKey>> {
        match self.store.get_fresh(&self.path(number)?).await {
            Ok(bytes) => self.decode(number, &bytes).map(Some),
            Err(Error::MissingObject(_)) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/token-authority-unavailable",
        "The database token authority is unavailable",
    )
}

fn invalid_metadata() -> Anomaly {
    Anomaly::fault(
        "rtdb/token-authority-metadata",
        "Invalid database token authority metadata",
    )
}
