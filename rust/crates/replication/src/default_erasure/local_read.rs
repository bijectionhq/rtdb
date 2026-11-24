use std::{fs::File, sync::Arc, time::Instant};

use async_trait::async_trait;
use rtdb_crypt::{Generation, Kms};
use rtdb_storage::{IoRegion, ObjectStore, sealing::LocalErasureCustody};
use rtdb_types::{CatalogCut, ReplicaPosition, Result, StorageEpoch, StorageVersion};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::PartitionLogs;

use super::{DefaultErasureReadCut, DefaultErasureReadFence, invalid, unsupported};

/// The admitted Local residence and its original checkpoint generation authority.
pub struct LocalDefaultErasureReadFence {
    database: Uuid,
    store: Arc<dyn ObjectStore>,
    custody: Arc<LocalErasureCustody>,
    kms: Arc<dyn Kms>,
    io: Arc<IoRegion>,
    storage_version: StorageVersion,
    storage_epoch: StorageEpoch,
    replica_epoch: StorageEpoch,
}

pub struct LocalErasureRead {
    _admission: File,
    deadline: Instant,
}

impl LocalErasureRead {
    pub fn deadline(&self) -> Instant { self.deadline }
}

impl LocalDefaultErasureReadFence {
    pub async fn open(logs: &PartitionLogs, kms: Arc<dyn Kms>, io: Arc<IoRegion>, database: Uuid) -> Result<Self> {
        let store = Arc::clone(logs.store());
        let custody = store.local_erasure_custody().cloned().ok_or_else(unsupported)?;
        let [source_lineage, replica_lineage] = if custody.kafka_owner().is_some() {
            logs.default_erasure_kafka_topics().ok_or_else(unsupported)?
        } else {
            if !logs.retains_recovery_history() { return Err(unsupported()); }
            [logs.source_journal_lineage().ok_or_else(unsupported)?, logs.replica_journal_lineage().ok_or_else(unsupported)?]
        };
        if !kms.supports_durable_generation_retirement() || !store.syncs_checkpoints() || !store.defers_trie_releases()
            || logs.checkpoint_log_keys().is_none() || store.local_maintenance_store().is_none()
            || store.local_classic_key_slots().is_none() || store.permits_shared_read_cache()
        { return Err(unsupported()); }
        io.run_blocking({
            let custody = Arc::clone(&custody);
            move || custody.bind(database, source_lineage, replica_lineage)
        }).await?;
        Ok(Self { database, store, custody, kms, io, storage_version: logs.storage_version,
            storage_epoch: logs.storage_epoch, replica_epoch: logs.replica_epoch })
    }

    pub async fn admit(&self, generation: u64) -> Result<Arc<LocalErasureRead>> {
        let deadline = Instant::now() + rtdb_storage::sealing::PLAINTEXT_DELIVERY_WINDOW;
        let custody = Arc::clone(&self.custody);
        let database = self.database;
        let held = self.io.run_blocking(move || custody.admit(database, generation)).await
            .map_err(|error| match error {
                rtdb_storage::Error::Closed(_) => rtdb_crypt::erased_under_view(),
                error => error.into(),
            })?;
        Ok(Arc::new(LocalErasureRead { _admission: held, deadline }))
    }

    fn catalog_cut(&self, boundary: ReplicaPosition) -> Result<CatalogCut> {
        if boundary.after_offset(self.replica_epoch) < 0 { return Err(invalid()); }
        Ok(CatalogCut { storage_version: self.storage_version, storage_epoch: self.storage_epoch,
            replica_epoch: self.replica_epoch, through: boundary })
    }

    async fn has_retired_ancestors(&self, generation: u64) -> Result<bool> {
        let Some(previous) = generation.checked_sub(1) else { return Ok(false); };
        if previous == 0 { return Ok(true); }
        let custody = Arc::clone(&self.custody);
        let database = self.database;
        Ok(self.io.run_blocking(move || custody.had_drain(database, previous)).await?
            && self.kms.generation_destroyed(Generation { database, number: previous }).await?)
    }
}

#[async_trait]
impl DefaultErasureReadFence for LocalDefaultErasureReadFence {
    async fn seal_prior(&self, database: Uuid, generation: u64, boundary: ReplicaPosition) -> Result<()> {
        if database != self.database { return Err(invalid()); }
        let cut = self.catalog_cut(boundary)?;
        let custody = Arc::clone(&self.custody);
        self.io.run_blocking(move || custody.seal_admission(database, generation)).await?;
        self.store.local_maintenance_store().ok_or_else(unsupported)?.close_erasure_catalog(cut).await?;
        Ok(())
    }

    async fn close(&self, cut: DefaultErasureReadCut) -> Result<Option<[u8; 32]>> {
        if cut.database != self.database || !self.has_retired_ancestors(cut.predecessor).await? { return Err(invalid()); }
        self.seal_prior(cut.database, cut.predecessor, cut.boundary).await?;
        let commitment = commitment(cut)?;
        let custody = Arc::clone(&self.custody);
        self.io.run_blocking(move || custody.bind_cut(cut.database, cut.predecessor, commitment)).await?;
        let catalog = self.catalog_cut(cut.boundary)?;
        if !self.store.local_maintenance_store().ok_or_else(unsupported)?.verifies_erasure_catalog(catalog).await? {
            return Ok(None);
        }
        let custody = Arc::clone(&self.custody);
        Ok(self.io.run_blocking(move || custody.finish_drain(cut.database, cut.predecessor, commitment))
            .await?.then_some(commitment))
    }

    async fn verify(&self, cut: DefaultErasureReadCut, witness: [u8; 32]) -> Result<bool> {
        if cut.database != self.database || commitment(cut)? != witness || !self.has_retired_ancestors(cut.predecessor).await? {
            return Ok(false);
        }
        let catalog = self.catalog_cut(cut.boundary)?;
        let custody = Arc::clone(&self.custody);
        Ok(self.store.local_maintenance_store().ok_or_else(unsupported)?.verifies_erasure_catalog(catalog).await?
            && self.io.run_blocking(move || custody.verifies_drain(cut.database, cut.predecessor, witness)).await?)
    }
}

fn commitment(cut: DefaultErasureReadCut) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    hash.update(b"rtdb.default.local-read-drain/v1\0");
    hash.update(serde_json::to_vec(&cut).map_err(|_| invalid())?);
    Ok(hash.finalize().into())
}
