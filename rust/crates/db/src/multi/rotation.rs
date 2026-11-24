use bytes::Bytes;
use journal_contract::BoxFuture;
use rtdb_store::{ErasurePlan, Retirement};

use super::{
    Anomaly, AnomalyCategory, Checkpoint, Committed, Generation, Lsn, Manifest, MultiDatabase,
    Record, Result, Timestamp, Uuid, next, no_writer,
};

/// An adapter's durable timestamp for the original successful rotation append.
pub trait RotationTiming: Send + Sync {
    fn committed_at(&self, database: Uuid, rotation: Lsn) -> BoxFuture<'_, Result<Timestamp>>;
}

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

impl MultiDatabase {
    pub fn with_rotation_timing(mut self, timing: std::sync::Arc<dyn RotationTiming>) -> Self {
        self.rotation_timing = Some(timing);
        self
    }

    /// Publishes the current clean fold with its complete rewritten file set.
    pub async fn rotate_clean_checkpoint(
        &mut self,
        clean_files: &[Bytes],
    ) -> Result<RotationReceipt> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.rotation_timing
            .as_ref()
            .ok_or_else(timing_unavailable)?;
        if self.state().rotation.is_some() {
            return self
                .finish_key_rotation()
                .await?
                .ok_or_else(timing_unavailable);
        }
        if self.reachability(lease).await?.pins().next().is_some() {
            return Err(rotation_busy("Release drained pins before rotating keys"));
        }
        let cut = next(self.state().head)?;
        let plan = self.rotation_plan(self.state().generation, cut)?;
        let generation = plan.next_generation();
        self.stream.kms.create(generation).await?;
        let payload =
            Committed::checkpoint_payload(self.durable_transactions(), lease.until, true)?;
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
            }),
        )
        .await?;
        self.finish_key_rotation()
            .await?
            .ok_or_else(timing_unavailable)
    }

    pub(super) async fn finish_key_rotation(&mut self) -> Result<Option<RotationReceipt>> {
        let Some(rotation) = self.state().rotation else {
            return Ok(None);
        };
        let lease = self.authority()?;
        let timing = self
            .rotation_timing
            .clone()
            .ok_or_else(timing_unavailable)?;
        let plan = self.rotation_plan(
            rotation
                .generation
                .checked_sub(1)
                .ok_or_else(timing_unavailable)?,
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
                let committed = timing
                    .committed_at(self.options.database, Lsn(plan.rotation_lsn()))
                    .await?;
                self.require_authority(lease)?;
                let this = &mut *self;
                let retirement = plan
                    .finish_rotation(kms.as_ref(), committed.0, move |through| async move {
                        this.trim(lease, Lsn(through)).await
                    })
                    .await?;
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
    }

    pub(super) fn require_armed_rotation(&self) -> Result<()> {
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
        if let Some(rotation) = self.state().rotation
            && !self
                .writer
                .as_ref()
                .and_then(|writer| writer.rotation)
                .is_some_and(|armed| armed.cut == rotation.cut && armed.destroyed)
        {
            return Err(rotation_busy(
                "The retiring generation still has a recovery obligation",
            ));
        }
        Ok(())
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

fn timing_unavailable() -> Anomaly {
    Anomaly::unsupported(
        "rtdb/key-rotation-clock",
        "Key rotation needs a durable journal commit timestamp",
    )
}
