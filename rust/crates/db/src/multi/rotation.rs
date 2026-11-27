use bytes::Bytes;
use rtdb_store::{ErasurePlan, Retirement};
use rtdb_tx::transact::{Erasure, Receipt, ScopedTransact};

use super::{
    Anomaly, AnomalyCategory, Checkpoint, Committed, Generation, Lease, Lsn, Manifest, MultiDatabase,
    Record, Result, Timestamp, erasure_pending, next, no_writer,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RotationReceipt {
    pub generation: u64,
    pub checkpoint: Lsn,
    pub unreadable_by: Timestamp,
}

#[derive(Clone, Copy)]
pub(super) struct ArmedRotation {
    cut: Lsn,
    retirement: Retirement,
    destroyed: bool,
}

impl ArmedRotation {
    pub(super) fn pending_destruction_at(&self) -> Option<i64> {
        (!self.destroyed).then(|| self.retirement.unreadable_by())
    }
}

impl MultiDatabase {
    pub(super) async fn check_erase_storage(&self, lease: Lease) -> Result<()> {
        self.require_destroyed_generation()?;
        let reachable = self.reachability(lease).await?;
        if reachable.pins().next().is_some() {
            return Err(rotation_busy("Release drained pins before erasing rows"));
        }
        if reachable.requires_file_rewrite() {
            return Err(Anomaly::unsupported("rtdb/erasure-files", "Erasure needs a rewrite of the referenced files"));
        }
        Ok(())
    }

    pub(super) async fn finish_pending_erasure(&mut self) -> Result<()> {
        Box::pin(async move {
        if self.state().rotation.is_none() && self.state().transactions.pending_erasure().is_some() {
            self.check_erase_storage(self.authority()?).await?;
            self.rotate_clean_checkpoint(&[]).await?;
        } else {
            self.finish_key_rotation().await?;
        }
        Ok(())
        }).await
    }

    pub(super) async fn complete_erase_receipt(&mut self, mut receipt: Receipt, scope: Option<ScopedTransact<'_>>) -> Result<Receipt> {
        if receipt.erasure == Some(Erasure::Pending) {
            self.finish_pending_erasure().await?;
            let transactions = &self.state().transactions;
            let stored = match scope {
                Some(scope) => transactions.tx_status_scoped(&receipt.key, scope)?,
                None => transactions.tx_status(&receipt.key)?,
            };
            if !matches!(stored.erasure, Some(Erasure::Armed { .. })) {
                return Err(erasure_pending());
            }
            receipt.erasure = stored.erasure;
        }
        Ok(receipt)
    }

    /// Publishes the current clean fold with its complete rewritten file set.
    pub async fn rotate_clean_checkpoint(
        &mut self,
        clean_files: &[Bytes],
    ) -> Result<RotationReceipt> {
        if self.activation_required() {
            return Err(activation_effects_unavailable());
        }
        Box::pin(async move {
        self.refresh().await?;
        let lease = self.authority()?;
        if self.state().rotation.is_some() {
            return self
                .finish_key_rotation()
                .await?
                .ok_or_else(missing_rotation);
        }
        if self.reachability(lease).await?.pins().next().is_some() {
            return Err(rotation_busy("Release drained pins before rotating keys"));
        }
        let cut = next(self.state().head)?;
        let plan = self.rotation_plan(self.state().generation, cut)?;
        let generation = plan.next_generation();
        self.stream.kms.create(generation).await?;
        let payload =
            Committed::checkpoint_payload_in(self.durable_transactions(), lease.until, true, self.state().coordinates, self.binary_checkpoints)?;
        let kms = self.stream.kms.as_ref();
        let store = self.store.as_ref();
        let objects = &mut self.writer.as_mut().ok_or_else(no_writer)?.objects;
        let mut files = Vec::with_capacity(clean_files.len());
        for data in clean_files {
            let file = objects.file(kms, generation.number, data).await?;
            file.put(store).await?;
            files.push(file.id());
        }
        let image = objects
            .manifest(
                kms,
                generation.number,
                &Manifest {
                    files,
                    pins: Vec::new(),
                    payload,
                },
            )
            .await?;
        image.put(store).await?;
        self.append_control_in_generation(
            lease,
            generation.number,
            Record::Checkpoint(Checkpoint {
                manifest: image.id(),
                through: cut.0 - 1,
                key_generation: generation.number,
                restore: false,
            }),
        )
        .await?;
        self.finish_key_rotation()
            .await?
            .ok_or_else(missing_rotation)
        }).await
    }

    pub(super) async fn finish_key_rotation(&mut self) -> Result<Option<RotationReceipt>> {
        Box::pin(async move {
        let Some(rotation) = self.state().rotation else {
            return Ok(None);
        };
        if self.activation_required() {
            return Err(activation_effects_unavailable());
        }
        let lease = self.authority()?;
        let plan = self.rotation_plan(
            rotation
                .generation
                .checked_sub(1)
                .ok_or_else(missing_rotation)?,
            rotation.cut,
        )?;
        if !rotation.committed {
            self.append_control(
                lease,
                Record::KeyRotate {
                    generation: rotation.generation,
                    at: rotation.cut.0,
                },
            )
            .await?;
        }
        let kms = self.stream.kms.clone();
        let mut armed = match self
            .writer
            .as_ref()
            .and_then(|writer| writer.rotation)
            .filter(|armed| armed.cut == rotation.cut)
        {
            Some(armed) => armed,
            None => {
                let (observed_at, retirement) = match rotation.retirement {
                    Some(observation) => {
                        let retirement = match observation.unreadable_by {
                            Some(deadline) => plan.retirement_by(deadline),
                            None => plan.retirement(observation.observed_at)?,
                        };
                        (observation.observed_at, retirement)
                    }
                    None => {
                        // Sample only after seeing KeyRotate commit. Persist the observation
                        // before arming the KMS so successors use the original deadline.
                        let observed_at = self.upper();
                        let retirement = plan.retirement(observed_at)?;
                        self.append_control(lease, Record::KeyRetirement {
                            generation: rotation.generation,
                            rotation_lsn: plan.rotation_lsn(),
                            observed_at,
                            unreadable_by: Some(retirement.unreadable_by()),
                            armed: false,
                        }).await?;
                        (observed_at, retirement)
                    }
                };
                self.require_authority(lease)?;
                self.token_keys.install(kms.as_ref(), rotation.generation).await?;
                retirement.arm(kms.as_ref()).await?;
                if !rotation.retirement_armed {
                    self.append_control(lease, Record::KeyRetirement {
                        generation: rotation.generation,
                        rotation_lsn: plan.rotation_lsn(),
                        observed_at,
                        unreadable_by: Some(retirement.unreadable_by()),
                        armed: true,
                    }).await?;
                }
                self.trim_committed_checkpoint(lease, rotation.cut, Lsn(plan.trim_through())).await?;
                let armed = ArmedRotation {
                    cut: rotation.cut,
                    retirement,
                    destroyed: false,
                };
                self.writer.as_mut().ok_or_else(no_writer)?.rotation = Some(armed);
                armed
            }
        };
        if !armed.destroyed && (self.clock)().0 >= armed.retirement.unreadable_by() {
            armed.retirement.destroy(kms.as_ref()).await?;
            armed.destroyed = true;
            self.writer.as_mut().ok_or_else(no_writer)?.rotation = Some(armed);
        }
        Ok(Some(RotationReceipt {
            generation: rotation.generation,
            checkpoint: rotation.cut,
            unreadable_by: Timestamp(armed.retirement.unreadable_by()),
        }))
        }).await
    }

    pub(super) fn require_armed_rotation(&self) -> Result<()> {
        if self.state().transactions.pending_erasure().is_some() {
            return Err(erasure_pending());
        }
        if let Some(rotation) = self.state().rotation
            && !self
                .writer
                .as_ref()
                .and_then(|writer| writer.rotation)
                .is_some_and(|armed| armed.cut == rotation.cut)
        {
            return Err(rotation_busy(
                "Recover the key rotation before admitting writes",
            ));
        }
        Ok(())
    }

    pub(super) fn require_destroyed_generation(&self) -> Result<()> {
        if self.state().rotation.is_some() && !self.rotation_destroyed() {
            return Err(rotation_busy(
                "The retiring generation still has a recovery obligation",
            ));
        }
        Ok(())
    }

    pub(super) fn rotation_destroyed(&self) -> bool {
        self.state().rotation.is_some_and(|rotation| {
            self.writer.as_ref().and_then(|writer| writer.rotation)
                .is_some_and(|armed| armed.cut == rotation.cut && armed.destroyed)
        })
    }

    fn rotation_plan(&self, old: u64, cut: Lsn) -> Result<ErasurePlan> {
        ErasurePlan::new(
            Generation {
                database: self.options.database,
                number: old,
            },
            cut.0,
            self.options.lease_micros as u64,
            self.options.drift_micros as u64,
        )
    }
}

fn rotation_busy(message: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb/key-rotation-pending", message)
}

fn missing_rotation() -> Anomaly {
    Anomaly::fault(
        "rtdb/key-rotation",
        "The expected key rotation is absent from the applied prefix",
    )
}

fn activation_effects_unavailable() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/activation-effects",
        "Key rotation requires an effect provider commissioned for database activation",
    )
}
