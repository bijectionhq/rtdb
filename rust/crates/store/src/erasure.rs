use std::future::Future;

use bytes::Bytes;
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::{Checkpoint, Manifest, Pin, Record};
use rtdb_types::{Anomaly, Result};

use crate::{ObjectStore, PreparedObject, WriterObjects};

/// A generation change at a checkpoint record's LSN.
/// The clean checkpoint image covers the preceding LSN.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ErasurePlan {
    old: Generation,
    next: Generation,
    cut: u64,
    rotate_at: u64,
    trim_through: u64,
    grant_micros: u64,
}

impl ErasurePlan {
    pub fn new(old: Generation, cut: u64, lease_micros: u64, drift_micros: u64) -> Result<Self> {
        let next = old.number.checked_add(1).ok_or_else(invalid_plan)?;
        Ok(Self {
            old,
            next: Generation {
                number: next,
                ..old
            },
            cut,
            rotate_at: cut.checked_add(1).ok_or_else(invalid_plan)?,
            trim_through: cut.checked_sub(1).ok_or_else(invalid_plan)?,
            grant_micros: lease_micros
                .checked_add(drift_micros)
                .ok_or_else(invalid_plan)?,
        })
    }

    pub fn next_generation(self) -> Generation {
        self.next
    }

    pub fn checkpoint_lsn(self) -> u64 {
        self.cut
    }

    pub fn rotation_lsn(self) -> u64 {
        self.rotate_at
    }

    pub fn trim_through(self) -> u64 {
        self.trim_through
    }

    pub fn retirement(self, rotation_committed_at: i64) -> Result<Retirement> {
        let unreadable_by = rotation_committed_at
            .checked_add_unsigned(self.grant_micros)
            .ok_or_else(invalid_plan)?;
        Ok(self.retirement_by(unreadable_by))
    }

    pub fn retirement_by(self, unreadable_by: i64) -> Retirement {
        Retirement {
            generation: self.old,
            unreadable_by,
        }
    }

    /// Stage a complete clean image from the database fold.
    /// File order is preserved for checkpoint metadata that indexes into the manifest.
    /// The caller holds publication at the cut until the staged rotation commits.
    pub async fn prepare(
        self,
        writer: &mut WriterObjects,
        kms: &dyn Kms,
        clean_files: &[Bytes],
        pins: Vec<Pin>,
        checkpoint_payload: Vec<u8>,
    ) -> Result<PreparedErasure> {
        if writer.database() != self.old.database {
            return Err(invalid_plan());
        }
        kms.create(self.next).await?;
        let mut files = Vec::with_capacity(clean_files.len());
        for file in clean_files {
            files.push(writer.file(kms, self.next.number, file).await?);
        }
        let manifest = writer
            .manifest(
                kms,
                self.next.number,
                &Manifest {
                    files: files.iter().map(PreparedObject::id).collect(),
                    pins,
                    payload: checkpoint_payload,
                },
            )
            .await?;

        let checkpoint = Checkpoint {
            manifest: manifest.id(),
            through: self.trim_through,
            key_generation: self.next.number,
            restore: false,
            recovery_epoch: 0,
        };
        let checkpoint_record = self
            .seal_record(kms, self.cut, Record::Checkpoint(checkpoint.clone()))
            .await?;
        let rotation_record = self
            .seal_record(
                kms,
                self.rotate_at,
                Record::KeyRotate {
                    generation: self.next.number,
                    at: self.cut,
                },
            )
            .await?;
        Ok(PreparedErasure {
            plan: self,
            files,
            manifest,
            checkpoint,
            checkpoint_record,
            rotation_record,
        })
    }

    async fn seal_record(self, kms: &dyn Kms, lsn: u64, record: Record) -> Result<Bytes> {
        Ok(Bytes::from(
            rtdb_crypt::seal(kms, self.next, Context::Journal { lsn }, &record.encode()?).await?,
        ))
    }

    /// Complete the journal and KMS work after observing an already committed rotation.
    /// A successor resumes this before admitting old-generation delivery.
    pub async fn finish_rotation<T>(
        self,
        kms: &dyn Kms,
        rotation_committed_at: i64,
        trim: impl FnOnce(u64) -> T,
    ) -> Result<Retirement>
    where
        T: Future<Output = Result<()>> + Send,
    {
        let retirement = self.retirement(rotation_committed_at)?;
        retirement.arm(kms).await?;
        trim(self.trim_through).await?;
        Ok(retirement)
    }
}

/// Clean objects and exact journal bytes for a single erasure attempt.
pub struct PreparedErasure {
    plan: ErasurePlan,
    files: Vec<PreparedObject>,
    manifest: PreparedObject,
    checkpoint: Checkpoint,
    checkpoint_record: Bytes,
    rotation_record: Bytes,
}

impl PreparedErasure {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Retry the same attempt while its writer still owns the cut.
    /// A replacement writer recovers the committed prefix before resuming publication.
    /// Append returns the original durable commit time in UTC microseconds on an identical retry.
    pub async fn execute<A, T>(
        &self,
        store: &dyn ObjectStore,
        kms: &dyn Kms,
        mut append: impl FnMut(u64, Bytes) -> A,
        trim: impl FnOnce(u64) -> T,
    ) -> Result<Retirement>
    where
        A: Future<Output = Result<i64>> + Send,
        T: Future<Output = Result<()>> + Send,
    {
        for file in &self.files {
            file.put(store).await?;
        }
        self.manifest.put(store).await?;
        append(self.plan.cut, self.checkpoint_record.clone()).await?;
        let committed_at = append(self.plan.rotate_at, self.rotation_record.clone()).await?;
        self.plan.finish_rotation(kms, committed_at, trim).await
    }
}

/// The proof of an armed old-generation deadline. Destruction is retryable after that deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Retirement {
    generation: Generation,
    unreadable_by: i64,
}

impl Retirement {
    pub fn unreadable_by(self) -> i64 {
        self.unreadable_by
    }

    pub async fn arm(self, kms: &dyn Kms) -> Result<()> {
        kms.retire(self.generation, self.unreadable_by).await
    }

    pub async fn destroy(self, kms: &dyn Kms) -> Result<()> {
        kms.destroy(self.generation).await
    }
}

fn invalid_plan() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.store/invalid-erasure-plan",
        "Invalid erasure scope, cut or deadline",
    )
}

#[cfg(test)]
mod tests;
