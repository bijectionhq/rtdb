use rtdb_format::{catalog::BlockCatalog, data::EventBatch, envelope::ReplicaBody};
use rtdb_index::LiveTable;
use rtdb_storage::{log::LogRange, object_store::ReadReservation};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, TableRef, Term, TransactionKey};
use tokio_util::sync::CancellationToken;

use crate::{PartitionLogs, TermFence, blocks::matches_catalog_boundary, repair_decision::exclusive_replay_end};

pub struct RetainedTableReplay<'a> {
    logs: &'a PartitionLogs,
    target: &'a BlockCatalog,
    table: &'a TableRef,
    through: ReplicaPosition,
    range: Box<dyn LogRange>,
    fence: TermFence,
    block: BlockIndex,
    source: SourcePosition,
    latest_tx: Option<TransactionKey>,
    awaiting_upload: Option<BlockIndex>,
    finished: bool,
}

impl<'a> RetainedTableReplay<'a> {
    pub async fn open(logs: &'a PartitionLogs, target: &'a BlockCatalog, table: &'a TableRef) -> Result<Option<Self>> {
        if logs.local_recovery_store().is_none()
            || (logs.storage_epoch, logs.replica_epoch, logs.source_epoch) != (StorageEpoch(0), StorageEpoch(0), StorageEpoch(0))
            || target.external_source_token().is_some()
        {
            return Ok(None);
        }
        let Some(through) = target.boundary_replica_position().filter(|position| position.0 >= 0 && position.epoch() == logs.replica_epoch) else {
            return Ok(None);
        };
        let end = exclusive_replay_end(through.0).map(ReplicaPosition).ok_or_else(|| invalid("The replay boundary overflows"))?;
        let start = ReplicaPosition::from_epoch_offset(logs.replica_epoch, 0)?;
        Ok(Some(Self {
            logs, target, table, through,
            range: logs.replica_range(start, end).await?,
            fence: TermFence::from_boundary(Term(0), None)?,
            block: BlockIndex(0),
            source: logs.source_floor()?,
            latest_tx: None,
            awaiting_upload: None,
            finished: false,
        }))
    }

    pub async fn next_block(
        &mut self, cancellation: &CancellationToken,
        mut visit: impl FnMut(BlockIndex, &EventBatch) -> Result<()> + Send,
    ) -> Result<Option<(BlockIndex, i64)>> {
        if self.finished { return Ok(None); }
        let memory = self.logs.memory();
        let bound = self.range.allocation_bound();
        let mut live = LiveTable::new(self.table.clone(), self.block, TrieConfig::default());
        loop {
            let record = cancellation.run_until_cancelled(self.range.next(ReadReservation::new(bound, memory.reserve(bound)?)))
                .await.ok_or_else(crate::progress::cancelled)??
                .ok_or_else(|| invalid("The retained journal ended before the captured checkpoint"))?;
            let record = self.logs.decode_replica(record)?.ok_or_else(|| invalid("The retained journal contains an unknown record"))?;
            let terminal = record.metadata.position == self.through;
            if terminal && !matches_catalog_boundary(self.target, &record) {
                return Err(invalid("The retained journal does not contain the captured boundary"));
            }
            let (_, admitted) = self.fence.admit_record(record)?;
            let Some(admitted) = admitted else {
                if terminal { return Err(invalid("The captured boundary was fenced")); }
                continue;
            };
            let record = cancellation.run_until_cancelled(admitted.resolve_payload_fresh(self.logs))
                .await.ok_or_else(crate::progress::cancelled)??;
            match &record.envelope.message {
                ReplicaBody::ResolvedTx(message) => {
                    let source = message.src_msg_id.map(SourcePosition).filter(|source| *source > self.source)
                        .ok_or_else(|| invalid("Resolved transaction source order is discontinuous"))?;
                    if message.db_op.is_some() || message.external_source_token.is_some()
                        || message.table_data.keys().any(|name| TableRef::is_owned_lifetime_replica_table(name))
                    {
                        return Err(invalid("The retained journal requires additional replay authority"));
                    }
                    let transaction = rtdb_tx::decode_replica(message, self.latest_tx, self.block, memory)?;
                    let key = transaction.writes.tx_key();
                    if self.latest_tx.is_some_and(|last| key.tx_id <= last.tx_id || key.system_time <= last.system_time) {
                        return Err(invalid("Resolved transaction basis regressed"));
                    }
                    if let Some(write) = transaction.writes.tables().get(self.table) {
                        live = live.append(write, key.system_time)?;
                    }
                    self.source = source;
                    self.latest_tx = Some(key);
                }
                ReplicaBody::BlockBoundary(boundary) => {
                    let block = boundary.block_index.map(BlockIndex).ok_or_else(|| invalid("The replay boundary has no block"))?;
                    if block < self.block && !terminal { continue; }
                    let source = boundary.latest_processed_msg_id.map(SourcePosition).filter(|source| *source >= self.source)
                        .ok_or_else(|| invalid("The boundary does not cover the retained source prefix"))?;
                    if self.awaiting_upload.is_some() || boundary.external_source_token.is_some()
                        || block != self.block || block > self.target.block_index()
                        || terminal != (block == self.target.block_index())
                        || (terminal && self.latest_tx != self.target.latest_completed_tx())
                    {
                        return Err(invalid("The retained journal does not cover the captured checkpoint"));
                    }
                    self.source = source;
                    self.finished = terminal;
                    self.awaiting_upload = (!terminal).then_some(block);
                    self.block = BlockIndex(block.0.checked_add(1).ok_or_else(|| invalid("The replay block index overflows"))?);
                    if live.row_count() > 0 {
                        let pages = live.block_pages(memory)?;
                        for leaf in live.trie().leaves() {
                            if cancellation.is_cancelled() { return Err(crate::progress::cancelled()); }
                            let batch = pages.copy(&leaf.sorted_rows(&live)?)?;
                            visit(block, &EventBatch::new(batch.batch().clone())?)?;
                            tokio::task::yield_now().await;
                        }
                    }
                    return Ok(Some((block, i64::from(live.row_count()))));
                }
                ReplicaBody::BlockUploaded(upload) => {
                    if self.awaiting_upload.is_some_and(|block| upload.block_index == Some(block.0))
                        && upload.storage_version == Some(self.logs.storage_version.0)
                        && upload.storage_epoch.unwrap_or_default() == self.logs.storage_epoch.0
                    {
                        self.awaiting_upload = None;
                    }
                }
                ReplicaBody::NoOp(_) | ReplicaBody::TriesAdded(_)
                | ReplicaBody::TriesDeleted(_) | ReplicaBody::TablesRetired(_) | ReplicaBody::ArtefactsAdded(_) | ReplicaBody::ArtefactsRemoved(_) => (),
                ReplicaBody::OversizedMessage(_) => return Err(invalid("An oversized replica record was not resolved")),
            }
        }
    }
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::fault("rtdb.replication/retained-table-replay", message)
}
