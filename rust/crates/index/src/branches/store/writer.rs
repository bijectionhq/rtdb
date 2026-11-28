use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

use crate::branches::BranchIndex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreWriterFence {
    pub epoch: u64,
    pub writer: Option<[u8; 16]>,
}

const STALE_WRITER: &str = "rtdb.store/stale-writer";

fn stale_writer() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, STALE_WRITER, "The store writer fence has changed")
}

impl BranchIndex {
    fn store_writer_fence(&self) -> Option<StoreWriterFence> {
        self.store_writer_fences.values().next_back().copied()
    }

    pub fn require_store_writer(&self, writer: Option<([u8; 16], StoreWriterFence)>) -> Result<()> {
        match (self.store_writer_fence(), writer) {
            (None, None) => Ok(()),
            (Some(current), Some((incarnation, requested)))
                if incarnation == self.head().basis.incarnation() && current.writer.is_some() && current == requested => Ok(()),
            _ => Err(stale_writer()),
        }
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
        self.store_writer_fences.insert(self.head().basis.position(), fence);
        Ok(())
    }
}
