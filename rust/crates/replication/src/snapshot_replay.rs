use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use rtdb_format::{catalog::legacy_table_ref, envelope::{ReplicaBody, decode_replica_outcome_attempt}};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_index::{Snapshot, StagedWrite};
use rtdb_storage::{ObjectStore, object_store::ReadReservation};
use rtdb_types::{Anomaly, AnomalyCategory, CatalogCut, EventOp, EventPosition, Iid, PlacedReservation, PlacedTerminal, ReplicaPosition, Result, SourcePosition, TableRef, Term, Timestamp, TransactionKey};
use tokio_util::sync::CancellationToken;

use crate::{PartitionLogs, RecoveredPartition, blocks::{matches_confirmation, validate_uploaded_root}, driver::{additions, artefact_entries}};

/// Owns the original checkpoint boundary and protected replica interval.
pub struct SnapshotReplay {
    cut: CatalogCut,
    after: ReplicaPosition,
    checkpoint: Option<crate::PublishedCheckpoint>,
    protected: rtdb_storage::log::ProtectedLogSnapshot,
}

impl SnapshotReplay {
    pub fn custody(&self) -> Option<Arc<dyn Send + Sync>> {
        self.protected.provider.as_ref().map(|owner| Arc::clone(owner) as Arc<dyn Send + Sync>)
            .or_else(|| self.protected.suffix.as_ref().map(|range| Arc::clone(&range.hold)))
    }

    pub fn provider(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        self.protected.provider.clone()
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
        let root = checkpoint.and_then(|checkpoint| checkpoint.root_sha256).unwrap_or([0; 32]);
        let protected = logs.protected_snapshot_range(after, cut.through, root).await?;
        Ok(Self { cut, after, checkpoint, protected })
    }
}

/// A target comes from the authenticated original transaction, including
/// any transaction metadata erased in that decision.
pub struct ErasureTarget {
    pub table: TableRef,
    pub iid: Iid,
    pub entity_key: Option<Box<[u8]>>,
    pub before: EventPosition,
    _reservation: Arc<MemoryReservation>,
}

/// An exact source for the survivor rewrite. The original provider hold
/// stays live while the snapshot and targets are being consumed.
pub struct SqlErasureSource {
    authenticated_head: [u8; 32],
    cut: CatalogCut,
    checkpoint: Option<crate::PublishedCheckpoint>,
    source_position: SourcePosition,
    transaction: TransactionKey,
    targets: Vec<ErasureTarget>,
    pub(crate) custody: Arc<dyn Send + Sync>,
    snapshot: Snapshot,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) memory: Arc<MemoryBudget>,
    pub(crate) fence: crate::TermFence,
    pub(crate) external_token: Option<Arc<[u8]>>,
}

impl SqlErasureSource {
    pub fn authenticated_head(&self) -> [u8; 32] { self.authenticated_head }
    pub fn cut(&self) -> CatalogCut { self.cut }
    pub fn checkpoint(&self) -> Option<crate::PublishedCheckpoint> { self.checkpoint }
    pub fn source_position(&self) -> SourcePosition { self.source_position }
    pub fn transaction(&self) -> TransactionKey { self.transaction }
    pub fn targets(&self) -> &[ErasureTarget] { &self.targets }
    pub fn snapshot(&self) -> &Snapshot { &self.snapshot }
}

impl RecoveredPartition {
    /// Rebuild an installed SQL cut in a private partition using its protected replica interval.
    /// The caller retains the interval owner through result delivery.
    pub async fn replay_snapshot_suffix(
        &mut self, logs: &PartitionLogs, replay: SnapshotReplay, cancellation: &CancellationToken,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        self.replay_snapshot_suffix_with(logs, replay, cancellation, |_, _, _| Ok(()), false).await
    }

    /// Resolve the exact erasing transaction in the protected replica interval,
    /// then apply the remaining certified effects through the selected head.
    pub async fn replay_sql_erasure(
        &mut self,
        logs: &PartitionLogs,
        replay: SnapshotReplay,
        transaction: TransactionKey,
        position: ReplicaPosition,
        store: Arc<dyn ObjectStore>,
        cancellation: &CancellationToken,
    ) -> Result<SqlErasureSource> {
        if position <= replay.after || position > replay.cut.through {
            return Err(missing_erasure());
        }
        let authenticated_head = replay.protected.suffix.as_ref().and_then(|range| range.authenticated_head)
            .ok_or_else(missing_erasure)?;
        let cut = replay.cut;
        let checkpoint = replay.checkpoint;
        let mut selected = None;
        let memory = logs.memory();
        let custody = self.replay_snapshot_suffix_with(logs, replay, cancellation, |original, write, committed| {
            if original == position {
                if !committed || write.tx_key() != transaction { return Err(missing_erasure()); }
                selected = Some(erasure_targets(write, memory)?);
            }
            Ok(())
        }, true).await?.ok_or_else(missing_erasure)?;
        let targets = selected.filter(|targets| !targets.is_empty()).ok_or_else(missing_erasure)?;
        Ok(SqlErasureSource {
            authenticated_head, cut, checkpoint, source_position: self.progress.source_position, transaction,
            targets, custody, snapshot: self.partition.snapshot(),
            store, memory: Arc::clone(memory),
            fence: self.fence.clone(), external_token: self.progress.external_token.clone(),
        })
    }

    async fn replay_snapshot_suffix_with(
        &mut self, logs: &PartitionLogs, replay: SnapshotReplay, cancellation: &CancellationToken,
        mut observe: impl FnMut(ReplicaPosition, &StagedWrite, bool) -> Result<()> + Send,
        require_complete_transactions: bool,
    ) -> Result<Option<Arc<dyn Send + Sync>>> {
        let SnapshotReplay { cut, after, checkpoint, protected } = replay;
        let base = self.replica_start(logs)?;
        let loaded = self.block.as_ref().and_then(|block| Some(crate::PublishedCheckpoint {
            block: block.block_index(),
            boundary: block.boundary_replica_position()?,
            root_sha256: Some(block.publication_sha256()?),
        }));
        let coordinates = |checkpoint: crate::PublishedCheckpoint| (checkpoint.block, checkpoint.boundary);
        if base != after || loaded.map(coordinates) != checkpoint.map(coordinates)
            || !crate::catalog_cut::matches_snapshot_root(
                loaded.and_then(|loaded| loaded.root_sha256),
                checkpoint.and_then(|issued| issued.root_sha256),
            )
            || (loaded.is_none() && self.block.is_some())
        {
            return Err(unretained());
        }
        let Some(protected) = protected.suffix else {
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
            let offset = raw.metadata.offset;
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
                    self.apply_snapshot_transaction(message, memory, |write, committed| observe(position, write, committed))?;
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
                        range.admit_snapshot_files(offset)?;
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
                        range.admit_snapshot_files(offset)?;
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
        if (require_complete_transactions && slot.is_some())
            || !crate::catalog_cut::completes_snapshot_replay(base.0, after.0, reached.0, cut.through.0, true)
        {
            return Err(unretained());
        }
        Ok(Some(protected.hold))
    }

    fn apply_snapshot_transaction(
        &mut self, message: &rtdb_format::log::proto::ResolvedTx, memory: &Arc<MemoryBudget>,
        observe: impl FnOnce(&StagedWrite, bool) -> Result<()>,
    ) -> Result<()> {
        let state = self.partition.value();
        let rtdb_tx::ReplayedTransaction { writes, result, .. } = rtdb_tx::decode_replica(
            message, state.live().latest_completed_tx(), state.live().block_index(), memory,
        )?;
        observe(&writes, result.committed())?;
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

fn erasure_targets(write: &StagedWrite, memory: &Arc<MemoryBudget>) -> Result<Vec<ErasureTarget>> {
    let mut targets = Vec::new();
    for (table, write_table) in write.tables() {
        let Some(batch) = write_table.batch() else { continue; };
        for row in 0..batch.len() {
            let header = batch.header(row)?;
            if header.op != EventOp::Erase { continue; }
            let before = header.position.filter(|position| position.transaction == write.tx_key().tx_id)
                .ok_or_else(missing_erasure)?;
            let entity_key = batch.view().entity_key(row)?;
            let bytes = table.schema.len().checked_add(table.table.len())
                .and_then(|size| size.checked_add(entity_key.map_or(0, |key| key.len())))
                .and_then(|size| size.checked_add(2 * std::mem::size_of::<ErasureTarget>()))
                .ok_or_else(missing_erasure)?;
            let reservation = memory.reserve(bytes)?;
            targets.push(ErasureTarget {
                table: table.clone(),
                iid: header.iid,
                entity_key: entity_key.map(Box::<[u8]>::from),
                before,
                _reservation: reservation,
            });
        }
    }
    Ok(targets)
}

fn missing_erasure() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/erasure-transaction-unretained",
        "The exact committed erasure is outside the protected replay interval or has no positioned effects")
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
        recovered.apply_snapshot_transaction(&record(4), &memory, |_, _| Ok(()))?;
        let original = recovered.partition.snapshot();
        recovered.apply_snapshot_transaction(&record(6), &memory, |_, _| Ok(()))?;
        let later = recovered.partition.snapshot();
        assert_eq!(original.tx_basis().unwrap().tx_id, TxId(4));
        assert_eq!(later.tx_basis().unwrap().tx_id, TxId(6));
        assert_eq!(original.tx_basis().unwrap().system_time, later.tx_basis().unwrap().system_time);
        assert!(recovered.apply_snapshot_transaction(&record(5), &memory, |_, _| Ok(())).is_err());
        Ok(())
    }
}
