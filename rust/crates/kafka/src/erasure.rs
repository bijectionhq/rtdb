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
        let (keys, topic) = self.native_prefix_custody()?;
        let (broker_floor, broker_end) = self.verified_watermarks(topic).await?;
        if broker_floor > retirement.before {
            return Err(Anomaly::incorrect(
                "rtdb.kafka/native-retirement",
                "The broker floor has advanced beyond the authorized recovery cut",
            ));
        }
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

    fn native_prefix_custody(&self) -> Result<(&KafkaFrameKeys, [u8; 16])> {
        match (&self.access, self.keys.as_deref(), self.topic_id) {
            (Access::Writable { .. }, Some(keys), Some(topic)) => Ok((keys, topic)),
            _ => Err(Anomaly::unsupported(
                "rtdb.kafka/native-retirement",
                "Native prefix retirement requires a writable sealed topic",
            )),
        }
    }
}
