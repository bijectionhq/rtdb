use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

use crate::branches::BranchIndex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreWriterFence {
    pub epoch: u64,
    pub writer: Option<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreWriterClaim {
    pub fence: StoreWriterFence,
    pub generation: u64,
    pub claim: [u8; 16],
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub(in crate::branches) struct StoreWriterState {
    pub epoch: u64,
    pub writer: Option<[u8; 16]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim: Option<[u8; 16]>,
}

impl StoreWriterState {
    fn fence(self) -> StoreWriterFence {
        StoreWriterFence { epoch: self.epoch, writer: self.writer }
    }
}

const STALE_WRITER: &str = "rtdb.store/stale-writer";

fn stale_writer() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, STALE_WRITER, "The store writer fence has changed")
}

impl BranchIndex {
    /// The ordered position of the current byte-store writer claim or fence.
    pub fn store_writer_fence_position(&self) -> Option<u64> {
        self.store_writer_fences.keys().next_back().copied()
    }

    fn store_writer_fence(&self) -> Option<StoreWriterFence> {
        self.store_writer_fences.values().next_back().map(|state| state.fence())
    }

    pub fn require_store_writer(&self, writer: Option<([u8; 16], StoreWriterFence, u64, [u8; 16])>) -> Result<()> {
        match (self.store_writer_fences.iter().next_back(), writer) {
            (None, None) => Ok(()),
            (Some((&current_generation, current)), Some((incarnation, requested, generation, claim)))
                if incarnation == self.head().basis.incarnation() && current.writer.is_some()
                    && current.fence() == requested && generation == current_generation && current.claim == Some(claim) => Ok(()),
            _ => Err(stale_writer()),
        }
    }

    pub fn check_store_writer_claim(&self, incarnation: [u8; 16], fence: StoreWriterFence) -> Result<()> {
        if fence.writer.is_none_or(|assignment| assignment == [0; 16]) {
            return Err(Anomaly::incorrect("rtdb.store/writer-assignment", "A store writer claim names its placement assignment"));
        }
        self.check_store_writer_fence(incarnation, fence).map(|_| ())
    }

    pub(in crate::branches) fn apply_store_writer_claim(
        &mut self, incarnation: [u8; 16], fence: StoreWriterFence, claim: [u8; 16],
    ) -> Result<()> {
        self.check_store_writer_claim(incarnation, fence)?;
        self.store_writer_fences.insert(self.head().basis.position(),
            StoreWriterState { epoch: fence.epoch, writer: fence.writer, claim: Some(claim) });
        Ok(())
    }

    pub fn check_store_writer_fence(&self, incarnation: [u8; 16], requested: StoreWriterFence) -> Result<bool> {
        if requested.epoch == 0 {
            return Err(Anomaly::incorrect("rtdb.store/writer-epoch", "A store writer epoch must be positive"));
        }
        if incarnation != self.head().basis.incarnation() {
            return Err(stale_writer());
        }
        let Some(current) = self.store_writer_fence() else { return Ok(true); };
        if current == requested { return Ok(false); }
        let closes_later_epoch = requested.writer.is_none() && requested.epoch > current.epoch;
        let claims_closed_epoch = current.writer.is_none() && requested.writer.is_some()
            && requested.epoch == current.epoch;
        if closes_later_epoch || claims_closed_epoch { Ok(true) } else { Err(stale_writer()) }
    }

    pub(in crate::branches) fn apply_store_writer_fence(
        &mut self, incarnation: [u8; 16], fence: StoreWriterFence,
    ) -> Result<()> {
        if !self.check_store_writer_fence(incarnation, fence)? {
            return Err(Anomaly::fault("rtdb.store/writer-fence-record", "A resolved writer transition must change the fence"));
        }
        self.store_writer_fences.insert(self.head().basis.position(),
            StoreWriterState { epoch: fence.epoch, writer: fence.writer, claim: None });
        Ok(())
    }
}
