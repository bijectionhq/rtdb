//! The live receiver's independently ordered clock, separate from history time and source append time.
use rtdb_types::{Anomaly, Result, TimedJournalCutoff, Timestamp, TxId, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};

use crate::OwnedClockSlot;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionObservation {
    pub domain: [u8; 16],
    pub epoch: u64,
    pub sequence: u64,
    pub instant: Timestamp,
}

impl DecisionObservation {
    pub const WIDTH: i32 = 40;

    pub(crate) fn covers(self, earlier: Self) -> bool {
        crate::system_time::accepts_clock_lineage(
            self.domain == earlier.domain, self.epoch, earlier.epoch, self.sequence, earlier.sequence,
            self.instant.0, earlier.instant.0,
        )
    }

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

/// Retained evidence of the receiver observation and any exact reading it used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderedClockEvidence {
    pub observation: DecisionObservation,
    pub slot: Option<OwnedClockSlot>,
}

impl OrderedClockEvidence {
    pub const WIDTH: i32 = 2 * DecisionObservation::WIDTH + 17;

    pub fn encode(self) -> [u8; Self::WIDTH as usize] {
        let mut bytes = [0; Self::WIDTH as usize];
        bytes[..40].copy_from_slice(&self.observation.encode());
        if let Some(slot) = self.slot {
            bytes[40] = if slot.funded { 2 } else { 1 };
            bytes[41..49].copy_from_slice(&slot.through.0.to_be_bytes());
            bytes[49..57].copy_from_slice(&slot.reading.0.to_be_bytes());
            bytes[57..].copy_from_slice(&slot.issued.encode());
        }
        bytes
    }

    pub fn decode(bytes: &[u8], accepted: TxId) -> Result<Self> {
        let bytes: &[u8; Self::WIDTH as usize] = bytes.try_into().map_err(|_| invalid_observation())?;
        let observation = DecisionObservation::decode(&bytes[..40])?;
        let slot = match bytes[40] {
            0 if bytes[41..].iter().all(|byte| *byte == 0) => None,
            flag @ (1 | 2) => {
                let through = TxId(i64::from_be_bytes(bytes[41..49].try_into().map_err(|_| invalid_observation())?));
                let reading = Timestamp(i64::from_be_bytes(bytes[49..57].try_into().map_err(|_| invalid_observation())?));
                let issued = DecisionObservation::decode(&bytes[57..])?;
                if through.0 < 0 || through >= accepted || !observation.covers(issued)
                    || crate::system_time::clock_slot_reading(issued.instant) != Some(reading)
                    || !crate::system_time::clock_slot_contains(reading.0, observation.instant.0)
                {
                    return Err(invalid_observation());
                }
                Some(OwnedClockSlot { through, reading, issued, funded: flag == 2 })
            }
            _ => return Err(invalid_observation()),
        };
        Ok(Self { observation, slot })
    }
}

fn invalid_observation() -> Anomaly {
    Anomaly::fault("rtdb.clock/invalid-observation", "The stored receiver clock observation is invalid")
}

#[async_trait::async_trait]
pub trait DecisionClock: Send + Sync {
    async fn observe(&self) -> Result<DecisionObservation>;

    fn supports_funded_offers(&self) -> bool { false }

    async fn funded_offers_for(&self, _incarnation: [u8; 16]) -> bool { false }

    fn funded_certificate(&self, _predecessor: Basis, _receiver: DecisionObservation) -> Option<[u8; 32]> { None }

    /// An installed verifier supplies the original receiver certificate's cutoffs.
    async fn authorize_offer(
        &self, _predecessor: Basis, _receiver: DecisionObservation, _before: Timestamp,
    ) -> Result<FundedCutoff> {
        Err(crate::transact::funding_unsupported())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FundedCutoff {
    pub decision_before: Timestamp,
    pub journal: TimedJournalCutoff,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_slot_keeps_the_original_target_and_cut() {
        let observation = DecisionObservation { domain: [7; 16], epoch: 3, sequence: 8, instant: Timestamp(1_200_000) };
        let issued = DecisionObservation { sequence: 6, instant: Timestamp(100), ..observation };
        let slot = OwnedClockSlot { through: TxId(5), reading: Timestamp(1_000_000), issued, funded: true };
        let original = OrderedClockEvidence { observation, slot: Some(slot) };
        let bytes = original.encode();
        assert_eq!(OrderedClockEvidence::decode(&bytes, TxId(6)).unwrap(), original);
        assert!(OrderedClockEvidence::decode(&bytes, slot.through).is_err());
        assert!(OrderedClockEvidence::decode(&bytes[..40], TxId(6)).is_err());
        let mut other_domain = bytes;
        other_domain[57..73].fill(9);
        assert!(OrderedClockEvidence::decode(&other_domain, TxId(6)).is_err());
    }

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
