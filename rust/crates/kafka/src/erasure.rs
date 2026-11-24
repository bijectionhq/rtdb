use async_trait::async_trait;
use rtdb_storage::log::{CheckpointKeyRetirement, CheckpointPrefix};
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
        if broker_floor > retirement.before || broker_end < retirement.before {
            return Err(Anomaly::incorrect(
                "rtdb.kafka/native-retirement",
                "The authorized recovery cut is outside the verified broker interval",
            ));
        }
        Ok((keys, broker_end))
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
