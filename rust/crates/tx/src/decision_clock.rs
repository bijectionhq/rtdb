//! The live receiver's independently ordered clock, separate from history time and source append time.
use rtdb_types::{Result, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecisionObservation {
    pub domain: [u8; 16],
    pub epoch: u64,
    pub sequence: u64,
    pub instant: Timestamp,
}

#[async_trait::async_trait]
pub trait DecisionClock: Send + Sync {
    async fn observe(&self) -> Result<DecisionObservation>;
}
