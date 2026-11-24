use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_format::{catalog::legacy_table_ref, envelope::{ReplicaBody, decode_replica_outcome_attempt}};
use rtdb_storage::object_store::ReadReservation;
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, PlacedReservation, PlacedTerminal, ReplicaPosition, Result, SourcePosition, Term, Timestamp};
use tokio_util::sync::CancellationToken;

use crate::{PartitionLogs, RecoveredPartition, blocks::{matches_confirmation, validate_uploaded_root}, driver::{additions, artefact_entries}};

/// Owns the original checkpoint boundary and protected replica interval.
pub struct SnapshotReplay {
    cut: CatalogCut,
    after: ReplicaPosition,
    protected: Option<rtdb_storage::log::ProtectedLogRange>,
}

impl SnapshotReplay {
    pub fn custody(&self) -> Option<Arc<dyn Send + Sync>> {
        self.protected.as_ref().map(|range| Arc::clone(&range.hold))
    }

    pub async fn protect(
        logs: &PartitionLogs, cut: CatalogCut, checkpoint: Option<crate::PublishedCheckpoint>,
    ) -> Result<Self> {
        if cut.storage_version != logs.storage_version || cut.storage_epoch != logs.storage_epoch
            || cut.replica_epoch != logs.replica_epoch
        {
            return Err(unretained());
        }
        let after = match checkpoint {
            Some(checkpoint) if checkpoint.boundary.epoch() == cut.replica_epoch && checkpoint.root_sha256.is_some() => checkpoint.boundary,
            Some(_) => return Err(unretained()),
            None => logs.replica_floor()?,
        };
        let protected = if after < cut.through {
            let next = |position: ReplicaPosition| position.0.checked_add(1).map(ReplicaPosition).ok_or_else(unretained);
            Some(logs.protected_snapshot_range(next(after)?, next(cut.through)?).await?)
        } else if after == cut.through {
            None
        } else {
            return Err(unretained());
        };
        Ok(Self { cut, after, protected })
    }
}

impl RecoveredPartition {
    /// Rebuild an installed SQL cut in a private partition using its protected replica interval.
    /// The caller retains the interval owner through result delivery.
    pub async fn replay_snapshot_suffix(
        &mut self, logs: &PartitionLogs, replay: SnapshotReplay, cancellation: &CancellationToken,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        let SnapshotReplay { cut, after, protected } = replay;
        let base = self.replica_start(logs)?;
        if base != after { return Err(unretained()); }
        let Some(protected) = protected else {
            return if crate::catalog_cut::completes_snapshot_replay(base.0, after.0, base.0, cut.through.0, false) {
                Ok(None)
            } else {
                Err(unretained())
            };
        };
        let mut range = protected.range;
        let bound = range.allocation_bound();
        let memory = logs.memory();
        let mut reached = after;
        let mut slot: Option<(PlacedReservation, Term)> = None;
        while let Some(raw) = cancellation.run_until_cancelled(range.next(ReadReservation::new(bound, memory.reserve(bound)?)))
            .await.ok_or_else(crate::progress::cancelled)??
        {
            let record = logs.decode_replica(raw)?.ok_or_else(unretained)?;
            let position = record.metadata.position;
            if position <= reached || position > cut.through { return Err(unretained()); }
            reached = position;
            let (_, admitted) = self.fence.admit_record(record)?;
            let Some(admitted) = admitted else { continue; };
            let record = cancellation.run_until_cancelled(admitted.resolve_payload_fresh(logs))
                .await.ok_or_else(crate::progress::cancelled)??;
            let term = record.envelope.term;
            let at = record.metadata.timestamp;
            let block = self.block.as_ref();
            if let ReplicaBody::BlockUploaded(message) = &record.envelope.message
                && let Some(block) = block.filter(|block| matches_confirmation(block, message, cut.storage_version, cut.storage_epoch))
            {
                validate_uploaded_root(block, message, logs.requires_checkpoint_digest())?;
            }
            let block = block.map_or(-1, |block| block.block_index().0);
            if crate::driver::stale_replica_body(
                &record.envelope.message, self.progress.tx_id().0, self.progress.source_position.0, block, false,
            ) { continue; }
            match &record.envelope.message {
                ReplicaBody::ResolvedTx(message) => {
                    let attempt = message.placed_attempt.as_deref().map(decode_replica_outcome_attempt)
                        .transpose()?.map(|(attempt, _)| attempt);
                    let terminal = message.placed_slot_terminal.as_deref().map(PlacedTerminal::decode).transpose()?;
                    match (slot, attempt, terminal) {
                        (None, None, None) => {}
                        (Some((original, owner)), Some(attempt), Some(terminal))
                            if original.attempt == attempt && original.commitment == terminal.commitment
                                && message.tx_id == Some(attempt.position().0) && message.src_msg_id == message.tx_id
                                && ((!terminal.fenced && owner == term)
                                    || (terminal.fenced && owner < term && message.committed == Some(false))) =>
                        {
                            slot = None;
                        }
                        _ => return Err(unretained()),
                    }
                    self.apply_snapshot_transaction(message, memory)?;
                }
                ReplicaBody::NoOp(message) => {
                    if let Some(wire) = message.placed_slot_reservation.as_deref() {
                        let reservation = PlacedReservation::decode(wire)?;
                        if slot.is_some() || message.src_msg_id.is_some()
                            || reservation.attempt.position() <= self.progress.source_position
                        {
                            return Err(unretained());
                        }
                        slot = Some((reservation, term));
                    } else {
                        self.advance_snapshot_source(message.src_msg_id, slot.is_some())?;
                    }
                }
                ReplicaBody::TriesAdded(message) => {
                    if same_storage(message.storage_version, message.storage_epoch, cut) {
                        self.partition.add_tries(&additions(&message.tries)?, at)?;
                    }
                    self.advance_snapshot_source(message.source_msg_id, slot.is_some())?;
                }
                ReplicaBody::TriesDeleted(message) => {
                    let table = legacy_table_ref(message.table_name.as_deref().unwrap_or_default())?;
                    let keys = message.trie_keys.iter().map(|key| key.parse()).collect::<Result<BTreeSet<_>>>()?;
                    self.partition.delete_tries(&BTreeMap::from([(table, keys)]));
                }
                ReplicaBody::TablesRetired(message) => {
                    let tables = message.table_names.iter().map(|name| legacy_table_ref(name)).collect::<Result<Vec<_>>>()?;
                    self.partition.retire_tables(tables, Timestamp(message.as_of_micros.unwrap_or_default()));
                }
                ReplicaBody::ArtefactsAdded(message) => {
                    if same_storage(message.storage_version, message.storage_epoch, cut)
                        && let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default())
                    {
                        self.partition.add_artefacts_at(&table, artefact_entries(&message.entries), at);
                    }
                    self.advance_snapshot_source(message.source_msg_id, slot.is_some())?;
                }
                ReplicaBody::ArtefactsRemoved(message) => {
                    if same_storage(message.storage_version, message.storage_epoch, cut)
                        && let Ok(table) = legacy_table_ref(message.table_name.as_deref().unwrap_or_default())
                    {
                        self.partition.remove_artefacts(&table, artefact_entries(&message.entries));
                    }
                    self.advance_snapshot_source(message.source_msg_id, slot.is_some())?;
                }
                ReplicaBody::BlockBoundary(_) | ReplicaBody::BlockUploaded(_) | ReplicaBody::OversizedMessage(_) =>
                    return Err(unretained()),
            }
        }
        if !crate::catalog_cut::completes_snapshot_replay(base.0, after.0, reached.0, cut.through.0, true) {
            return Err(unretained());
        }
        Ok(Some(protected.hold))
    }

    fn apply_snapshot_transaction(
        &mut self, message: &rtdb_format::log::proto::ResolvedTx, memory: &Arc<rtdb_columnar::MemoryBudget>,
    ) -> Result<()> {
        let state = self.partition.value();
        let rtdb_tx::ReplayedTransaction { writes, result, .. } = rtdb_tx::decode_replica(
            message, state.live().latest_completed_tx(), state.live().block_index(), memory,
        )?;
        self.advance_snapshot_source(message.src_msg_id, false)?;
        // This workspace installs resolved row effects only. External and database
        // callbacks already ran at the original publication and do not run on queries.
        self.partition.apply(&writes)?;
        self.progress.tx_key = Some(result.tx_key());
        self.progress.external_token = message.external_source_token.clone().map(Arc::from);
        self.progress.latest_result = None;
        Ok(())
    }

    fn advance_snapshot_source(&mut self, position: Option<i64>, slot: bool) -> Result<()> {
        if let Some(position) = position.map(SourcePosition) {
            if slot || position < self.progress.source_position { return Err(unretained()); }
            self.progress.source_position = position;
        }
        Ok(())
    }
}

fn same_storage(version: Option<i32>, epoch: Option<i32>, cut: CatalogCut) -> bool {
    version == Some(cut.storage_version.0) && epoch.unwrap_or_default() == cut.storage_epoch.0
}

fn unretained() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/snapshot-unretained", "The original snapshot suffix cannot be reconstructed")
}


#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::MemoryBudget;
    use rtdb_format::log::proto;
    use rtdb_storage::{cache::BufferPool, MemoryStore};
    use rtdb_types::TxId;

    #[test]
    fn snapshot_replay_keeps_order_across_tied_history_times() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let store = Arc::new(MemoryStore::default());
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let blocks = crate::BlockStore::new(store, storage, Arc::clone(&memory), 1 << 20)?;
        let mut recovered = blocks.empty_snapshot_base(Default::default(), SourcePosition(-1), Timestamp(22))?;
        let record = |position| proto::ResolvedTx {
            tx_id: Some(position), src_msg_id: Some(position), system_time_micros: Some(22),
            committed: Some(true), ..Default::default()
        };
        recovered.apply_snapshot_transaction(&record(4), &memory)?;
        let original = recovered.partition.snapshot();
        recovered.apply_snapshot_transaction(&record(6), &memory)?;
        let later = recovered.partition.snapshot();
        assert_eq!(original.tx_basis().unwrap().tx_id, TxId(4));
        assert_eq!(later.tx_basis().unwrap().tx_id, TxId(6));
        assert_eq!(original.tx_basis().unwrap().system_time, later.tx_basis().unwrap().system_time);
        assert!(recovered.apply_snapshot_transaction(&record(5), &memory).is_err());
        Ok(())
    }
}
