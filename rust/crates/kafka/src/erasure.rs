use async_trait::async_trait;
use rtdb_storage::log::{
    CheckpointKeyRetirement, CheckpointPrefix, Log, LogOffset, PriorCheckpointReferences,
};
use rtdb_storage::object_store::ReadReservation;
use rtdb_storage::sealing::{KafkaFrameKeys, KafkaPrefixRetirement};
use rtdb_types::{Anomaly, Result};

use crate::{Access, KafkaLog};

impl KafkaLog {
    /// Arms the cut ordered by the database writer against the exact published checkpoint.
    /// The writer must retain a recovery path at or above the cut before calling this method.
    pub async fn arm_native_prefix_retirement(
        &self,
        retirement: KafkaPrefixRetirement,
    ) -> Result<()> {
        let (keys, broker_end) = self.verified_retirement_cut(retirement).await?;
        Ok(keys
            .arm_kafka_prefix_retirement(retirement, broker_end)
            .await?)
    }

    /// Withdraws a batch of keys for this cut. Success with `true` confirms the entire prefix.
    pub async fn settle_native_prefix_retirement(
        &self,
        retirement: KafkaPrefixRetirement,
    ) -> Result<bool> {
        let (keys, topic) = self.native_prefix_custody()?;
        self.verified_watermarks(topic).await?;
        Ok(keys.settle_kafka_prefix_retirement(retirement).await?)
    }

    async fn verified_retirement_cut(
        &self,
        retirement: KafkaPrefixRetirement,
    ) -> Result<(&KafkaFrameKeys, i64)> {
        let (keys, topic) = self.native_prefix_custody()?;
        let (broker_floor, broker_end) = self.verified_watermarks(topic).await?;
        if broker_end < retirement.before
            || broker_floor > retirement.before
                && keys.kafka_classic_retirement_floor()? < retirement.before
                && !keys.has_kafka_prefix_retirement(retirement).await?
        {
            return Err(Anomaly::incorrect(
                "rtdb.kafka/native-retirement",
                "The authorized recovery cut is outside the verified broker interval",
            ));
        }
        if self.default_erasure {
            if broker_floor > retirement.before { return Err(incomplete_history()); }
            if !keys.has_kafka_prefix_retirement(retirement).await? {
                self.backfill_checkpoint_suffix(keys, retirement.before, broker_end).await?;
            }
        }
        Ok((keys, broker_end))
    }

    async fn backfill_checkpoint_suffix(&self, keys: &KafkaFrameKeys, before: i64, broker_end: i64) -> Result<()> {
        let Some(missing) = keys.first_unobserved_kafka_position(before, broker_end).await? else { return Ok(()); };
        let mut range = self.range(LogOffset(missing), LogOffset(broker_end)).await?;
        let bound = range.allocation_bound();
        // A broker acknowledgement can commit before the delivery callback records its offset.
        for offset in missing..broker_end {
            let record = range.next(ReadReservation::new(bound, self.memory.reserve(bound)?)).await?
                .ok_or_else(incomplete_history)?;
            if record.metadata.offset.0 != offset { return Err(incomplete_history()); }
        }
        Ok(())
    }

    pub(super) fn native_prefix_custody(&self) -> Result<(&KafkaFrameKeys, [u8; 16])> {
        match (&self.access, self.keys.as_deref(), self.topic_id) {
            (Access::Writable { .. }, Some(keys), Some(topic)) => Ok((keys, topic)),
            _ => Err(Anomaly::unsupported(
                "rtdb.kafka/native-retirement",
                "Native prefix retirement requires a writable sealed topic",
            )),
        }
    }
}

#[async_trait]
impl CheckpointKeyRetirement for KafkaLog {
    fn default_erasure_topic(&self) -> Option<[u8; 16]> {
        self.topic_id.filter(|_| self.default_erasure)
    }

    async fn validate_default_erasure_history(&self, through: LogOffset, recovery_after: LogOffset) -> Result<()> {
        let (keys, topic) = self.native_prefix_custody()?;
        let before = through.0.checked_add(1).ok_or_else(incomplete_history)?;
        let recovery_next = recovery_after.0.checked_add(1).ok_or_else(incomplete_history)?;
        let (broker_floor, broker_end) = self.verified_watermarks(topic).await?;
        if !self.default_erasure || broker_floor < 0 || broker_floor > recovery_next
            || keys.kafka_erasure_floor()? != recovery_next || recovery_next > before || before > broker_end
        {
            return Err(incomplete_history());
        }
        self.backfill_checkpoint_suffix(keys, before, broker_end).await?;
        keys.validate_kafka_position_history(recovery_next, before, broker_end).await?;
        if self.replica_references {
            keys.prior_replica_references(before).await?;
        }
        Ok(())
    }

    async fn verifies_checkpoint_prefix(&self, cut: CheckpointPrefix) -> Result<bool> {
        let (keys, topic) = self.native_prefix_custody()?;
        let retirement = broker_cut(cut)?;
        let (broker_floor, broker_end) = self.verified_watermarks(topic).await?;
        Ok(self.default_erasure && broker_floor <= retirement.before && retirement.before <= broker_end
            && keys.verifies_kafka_prefix_retirement(retirement).await?)
    }

    fn checkpoint_offsets_are_dense(&self) -> bool {
        true
    }

    async fn prior_checkpoint_references(
        &self,
        through: LogOffset,
    ) -> Result<Option<PriorCheckpointReferences>> {
        if !self.replica_references {
            return Ok(None);
        }
        let (keys, topic) = self.native_prefix_custody()?;
        let (broker_floor, broker_end) = self.verified_watermarks(topic).await?;
        let before = through
            .0
            .checked_add(1)
            .filter(|before| *before > 0 && *before <= broker_end)
            .ok_or_else(|| {
                Anomaly::incorrect(
                    "rtdb.kafka/native-retirement",
                    "The replica inventory exceeds the observed broker prefix",
                )
            })?;
        let first_live = broker_floor.max(keys.kafka_erasure_floor()?).min(before);
        let detached = keys.prior_replica_references(first_live).await?;
        Ok(Some(PriorCheckpointReferences {
            first_live: LogOffset(first_live),
            detached,
        }))
    }

    async fn validate_checkpoint_prefix(&self, cut: CheckpointPrefix) -> Result<()> {
        let retirement = broker_cut(cut)?;
        let (keys, broker_end) = self.verified_retirement_cut(retirement).await?;
        Ok(keys
            .validate_kafka_prefix_retirement(retirement, broker_end)
            .await?)
    }

    async fn settle_checkpoint_prefix(&self, cut: CheckpointPrefix) -> Result<bool> {
        let retirement = broker_cut(cut)?;
        self.arm_native_prefix_retirement(retirement).await?;
        self.settle_native_prefix_retirement(retirement).await
    }
}

pub(super) fn incomplete_history() -> Anomaly {
    Anomaly::unsupported("rtdb.kafka/erasure-history", "Default erasure requires the original sealed Kafka history and recovery suffix")
}

fn broker_cut(cut: CheckpointPrefix) -> Result<KafkaPrefixRetirement> {
    let before = cut
        .through
        .checked_add(1)
        .filter(|before| *before > 0)
        .ok_or_else(|| {
            Anomaly::incorrect(
                "rtdb.kafka/native-retirement",
                "The checkpoint has no inclusive record in this partition",
            )
        })?;
    Ok(KafkaPrefixRetirement {
        before,
        checkpoint_sha256: cut.checkpoint_sha256,
        unreadable_by: cut.unreadable_by,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kafka_checkpoint_through_the_first_record_closes_only_that_offset() -> Result<()> {
        let cut = CheckpointPrefix {
            through: 0,
            checkpoint_sha256: [4; 32],
            unreadable_by: rtdb_types::Timestamp(5),
        };
        let selected = broker_cut(cut)?;
        assert_eq!(selected.before, 1);
        assert_eq!(selected.checkpoint_sha256, cut.checkpoint_sha256);
        assert_eq!(selected.unreadable_by, cut.unreadable_by);
        assert!(broker_cut(CheckpointPrefix { through: -1, ..cut }).is_err());
        assert!(
            broker_cut(CheckpointPrefix {
                through: i64::MAX,
                ..cut
            })
            .is_err()
        );
        Ok(())
    }
}
