use rtdb_format::journal::{Checkpoint, Manifest, Record};
use rtdb_index::branches::BranchCut;
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, AnomalyCategory, Result, TransactionKey, TxId, rtdb_v1::Basis};
use sha2::{Digest, Sha256};

use crate::replay::{Committed, JournalCoordinates, RestoreOrigin};

use super::{MultiDatabase, next, no_writer};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClassicInstallation {
    pub basis: Basis,
    pub system_time: rtdb_types::Timestamp,
    pub checkpoint_lsn: u64,
    pub checkpoint_writer_epoch: u64,
    pub key_generation: u64,
    pub checkpoint_sha256: [u8; 32],
    pub manifest: String,
    pub manifest_ciphertext_sha256: [u8; 32],
}

impl MultiDatabase {
    /// Installs a verified Classic projection on a private stream reserved by the native catalog.
    pub async fn install_classic_checkpoint(
        &mut self,
        import: [u8; 32],
        transactions: &TransactState,
    ) -> Result<ClassicInstallation> {
        self.refresh().await?;
        if let Some(installed) = self.observe_classic_checkpoint(import).await? {
            if transactions.index().head()
                != (BranchCut {
                    basis: installed.basis,
                    system_time: installed.system_time,
                })
                || installed.checkpoint_sha256
                    != <[u8; 32]>::from(Sha256::digest(transactions.encode_checkpoint()?))
            {
                return Err(conflict());
            }
            return Ok(installed);
        }
        let cut = transactions.index().head();
        if import == [0; 32]
            || cut.basis.incarnation() != self.incarnation()
            || cut.basis.position() == 0
        {
            return Err(conflict());
        }
        let lease = self.authority()?;
        let state = self.state();
        let through = state.head;
        let generation = state.generation;
        let entry = next(through)?;
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if writer.pipeline.next_position() != entry.0 {
            return Err(conflict());
        }
        let coordinates = JournalCoordinates {
            offset: cut.basis.position().saturating_sub(through.0),
            origin: Some(RestoreOrigin {
                digest: import,
                through: cut.basis.position(),
                source: None,
            }),
        };
        coordinates.position(next(entry)?)?;
        let manifest = Manifest {
            files: Vec::new(),
            pins: Vec::new(),
            payload: Committed::checkpoint_payload(transactions, lease.until, false, coordinates)?,
        };
        let image = self
            .writer
            .as_mut()
            .ok_or_else(no_writer)?
            .objects
            .manifest(self.stream.kms.as_ref(), generation, &manifest)
            .await?;
        image.put(self.store.as_ref()).await?;
        self.require_authority(lease)?;
        self.append_control(
            lease,
            Record::Checkpoint(Checkpoint {
                manifest: image.id(),
                through: through.0,
                key_generation: generation,
                restore: true,
            }),
        )
        .await?;
        self.observe_classic_checkpoint(import)
            .await?
            .ok_or_else(conflict)
    }

    /// Reopens the committed manifest at the exact private candidate terminal.
    pub async fn classic_checkpoint_status(
        &mut self,
        import: [u8; 32],
    ) -> Result<Option<ClassicInstallation>> {
        self.refresh().await?;
        self.observe_classic_checkpoint(import).await
    }

    /// Confirms that the original Classic cut is among the exact installed cuts.
    pub async fn classic_checkpoint_cut(
        &mut self,
        import: [u8; 32],
        original: TransactionKey,
    ) -> Result<BranchCut> {
        let installed = self
            .classic_checkpoint_status(import)
            .await?
            .ok_or_else(conflict)?;
        let basis = classic_import_basis(self.incarnation(), original.tx_id)?;
        let cut = self.state().transactions.index().exact(basis)?;
        if basis > installed.basis || cut.system_time != original.system_time {
            return Err(conflict());
        }
        Ok(cut)
    }

    async fn observe_classic_checkpoint(
        &self,
        import: [u8; 32],
    ) -> Result<Option<ClassicInstallation>> {
        let state = self.state();
        let Some(origin) = state.coordinates.origin else {
            if state.transactions.index().head().basis.position() != 0
                || state.records.iter().any(|record| {
                    !matches!(record, Record::Lease { .. } | Record::WriterRelease { .. })
                })
            {
                return Err(conflict());
            }
            return Ok(None);
        };
        if origin.digest != import || origin.source.is_some() {
            return Err(conflict());
        }
        let cut = state
            .transactions
            .index()
            .exact(Basis::new(self.incarnation(), origin.through))?;
        let mut checkpoints = state.records.iter().filter_map(|record| match record {
            Record::Checkpoint(checkpoint) if checkpoint.restore => Some(checkpoint),
            _ => None,
        });
        let checkpoint = checkpoints.next().ok_or_else(conflict)?;
        if checkpoints.next().is_some()
            || state.transactions.index().head() != cut
            || state.records.iter().any(|record| {
                !matches!(
                    record,
                    Record::Lease { .. }
                        | Record::WriterRelease { .. }
                        | Record::Checkpoint(Checkpoint { restore: true, .. })
                )
            })
        {
            return Err(conflict());
        }
        self.check_generation(checkpoint.key_generation).await?;
        let opened = rtdb_store::read_manifest(
            self.store.as_ref(),
            self.stream.kms.as_ref(),
            checkpoint.manifest,
        )
        .await?;
        if opened.generation.number != checkpoint.key_generation {
            return Err(conflict());
        }
        Ok(Some(ClassicInstallation {
            basis: cut.basis,
            system_time: cut.system_time,
            checkpoint_lsn: checkpoint.through.checked_add(1).ok_or_else(conflict)?,
            checkpoint_writer_epoch: checkpoint.manifest.epoch,
            key_generation: checkpoint.key_generation,
            checkpoint_sha256: Sha256::digest(state.transactions.encode_checkpoint()?).into(),
            manifest: checkpoint.manifest.to_string(),
            manifest_ciphertext_sha256: checkpoint.manifest.digest,
        }))
    }
}

/// Maps an authenticated Classic transaction ID into the native import prefix's basis domain.
pub fn classic_import_basis(incarnation: [u8; 16], tx: TxId) -> Result<Basis> {
    let position = u64::try_from(tx.0)
        .ok()
        .and_then(|id| id.checked_add(1))
        .filter(|position| *position < (1 << 63))
        .ok_or_else(conflict)?;
    Ok(Basis::new(incarnation, position))
}

fn conflict() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/classic-installation",
        "The native stream differs from the exact Classic import",
    )
}
