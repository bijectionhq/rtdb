use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use rtdb_format::envelope::ReplicaBody;
use rtdb_index::BlockPreparation;
use rtdb_types::{Anomaly, BlockIndex, Result, StorageEpoch, StorageVersion, Timestamp, TxId};

use crate::ReplicaRecord;
use crate::admission::AdmittedReplica;

pub struct PendingBlock {
    pub boundary: ReplicaRecord,
    pub prepared: BlockPreparation,
    buffered: VecDeque<AdmittedReplica>,
    buffered_bytes: usize,
    max_records: usize,
    max_bytes: usize,
    /// When the boundary opened the block, for the reference's `xtdb.replica.block.buffer.timer`.
    opened: std::time::Instant,
}
impl PendingBlock {
    pub fn new(boundary: ReplicaRecord, prepared: BlockPreparation, max_records: usize, max_bytes: usize) -> Result<Self> {
        let ReplicaBody::BlockBoundary(message) = &boundary.envelope.message else { return Err(invalid("Pending block requires a boundary")); };
        if BlockIndex(message.block_index.unwrap_or_default()) != prepared.block_index() { return Err(invalid("Boundary does not match the open block")); }
        Ok(Self { boundary, prepared, buffered: VecDeque::new(), buffered_bytes: 0, max_records, max_bytes, opened: std::time::Instant::now() })
    }
    pub fn block_index(&self) -> BlockIndex { self.prepared.block_index() }
    pub fn matches_upload(&self, message: &rtdb_format::log::proto::BlockUploaded, version: StorageVersion, epoch: StorageEpoch) -> bool {
        BlockIndex(message.block_index.unwrap_or_default()) == self.block_index()
            && StorageVersion(message.storage_version.unwrap_or_default()) == version
            && StorageEpoch(message.storage_epoch.unwrap_or_default()) == epoch
    }
    /// Records have already passed term admission and oversized-payload resolution.
    pub(crate) fn hold(&mut self, record: AdmittedReplica) -> Result<()> {
        let bytes = self.buffered_bytes.checked_add(record.retained_bytes()).ok_or_else(|| invalid("Held replica byte size overflow"))?;
        if self.buffered.len() >= self.max_records || bytes > self.max_bytes {
            return Err(Anomaly::fault("xtdb.indexer/follower-buffer-overflow", "Pending block exceeded the retained replica record limit"));
        }
        self.buffered.push_back(record);
        self.buffered_bytes = bytes;
        Ok(())
    }
    pub fn buffered_len(&self) -> usize { self.buffered.len() }
    pub fn opened(&self) -> std::time::Instant { self.opened }
    pub(crate) fn into_records(self) -> VecDeque<AdmittedReplica> { self.buffered }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockCycle { Filling { rows: u64 }, Cut }
impl BlockCycle {
    pub fn accepting(self) -> bool { matches!(self, Self::Filling { .. }) }
    pub fn is_full(self, threshold: u64) -> bool { matches!(self, Self::Filling { rows } if rows > 0 && rows >= threshold) }
    pub fn resolved(&mut self, added: u64) -> Result<()> {
        let Self::Filling { rows } = self else { return Err(invalid("Resolution attempted while a block cut is pending")); };
        *rows = rows.checked_add(added).ok_or_else(|| invalid("Resolved row gauge overflow"))?;
        Ok(())
    }
    pub fn cut(&mut self) -> Result<()> { if !self.accepting() { return Err(invalid("Block was already cut")); } *self = Self::Cut; Ok(()) }
    pub fn adopted(&mut self) { *self = Self::Filling { rows: 0 }; }
}

pub struct FlushTimer { last_check: Timestamp, previous_persisted_tx: Option<TxId>, timeout_micros: i64 }
impl FlushTimer {
    pub fn new(now: Timestamp, persisted: Option<TxId>, timeout: Duration) -> Result<Self> {
        Ok(Self { last_check: now, previous_persisted_tx: persisted, timeout_micros: i64::try_from(timeout.as_micros()).map_err(|_| invalid("Flush interval exceeds timestamp range"))? })
    }
    pub fn should_flush(&mut self, now: Timestamp, persisted: Option<TxId>) -> bool {
        if now.0.saturating_sub(self.last_check.0) <= self.timeout_micros { return false; }
        self.last_check = now;
        if persisted != self.previous_persisted_tx { self.previous_persisted_tx = persisted; false } else { true }
    }
}

pub(crate) fn fail_pending(pending: &mut VecDeque<Arc<crate::appender::PendingWrite>>, error: Anomaly) {
    for write in pending.drain(..) { write.finish(Err(error.clone())); }
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::fault("rtdb.replication/block-state", message) }
