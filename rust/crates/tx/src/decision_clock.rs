//! The live receiver's independently ordered clock, separate from history time and source append time.
use rtdb_types::{Anomaly, Result, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecisionObservation {
    pub domain: [u8; 16],
    pub epoch: u64,
    pub sequence: u64,
    pub instant: Timestamp,
}

impl DecisionObservation {
    pub const WIDTH: i32 = 40;

    pub fn encode(self) -> [u8; Self::WIDTH as usize] {
        let mut bytes = [0; Self::WIDTH as usize];
        bytes[..16].copy_from_slice(&self.domain);
        bytes[16..24].copy_from_slice(&self.epoch.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.sequence.to_be_bytes());
        bytes[32..].copy_from_slice(&self.instant.0.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes: &[u8; Self::WIDTH as usize] = bytes.try_into().map_err(|_| invalid_observation())?;
        let domain = bytes[..16].try_into().map_err(|_| invalid_observation())?;
        let epoch = u64::from_be_bytes(bytes[16..24].try_into().map_err(|_| invalid_observation())?);
        let sequence = u64::from_be_bytes(bytes[24..32].try_into().map_err(|_| invalid_observation())?);
        let instant = Timestamp(i64::from_be_bytes(bytes[32..].try_into().map_err(|_| invalid_observation())?));
        if domain == [0; 16] || epoch == 0 || sequence < epoch {
            return Err(invalid_observation());
        }
        Ok(Self { domain, epoch, sequence, instant })
    }
}

fn invalid_observation() -> Anomaly {
    Anomaly::fault("rtdb.clock/invalid-observation", "The stored receiver clock observation is invalid")
}

#[async_trait::async_trait]
pub trait DecisionClock: Send + Sync {
    async fn observe(&self) -> Result<DecisionObservation>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receiver_evidence_preserves_the_original_domain_and_time() {
        let original = DecisionObservation { domain: [7; 16], epoch: 3, sequence: 8, instant: Timestamp(900) };
        let bytes = original.encode();
        assert_eq!(DecisionObservation::decode(&bytes).unwrap(), original);
        assert!(DecisionObservation::decode(&bytes[..32]).is_err());
        let mut wrong_epoch = bytes;
        wrong_epoch[16..24].fill(0);
        assert!(DecisionObservation::decode(&wrong_epoch).is_err());
    }
}
