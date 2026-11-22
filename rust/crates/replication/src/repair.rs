use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use fs2::FileExt;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{
    catalog::{BlockCatalog, CheckpointContents, publication_digest},
    envelope::ReplicaBody,
    keys::{CURRENT_STORAGE_VERSION, StorageRoot, block_key},
};
use rtdb_storage::{
    IoRegion, LocalStore, ObjectKey, ObjectStore,
    cache::{BufferPool, CacheBudget},
    log::{MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions},
    object_store::ReadReservation,
};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, Timestamp, TransactionKey};
use tokio_util::sync::CancellationToken;

use crate::{BlockDiscovery, BlockStore, PartitionLogs, RecoveredPartition, ReplicaRecord, blocks::matches_catalog_boundary};

/// Selects a trusted source publication by its block number and SHA-256.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairCheckpoint {
    pub block: BlockIndex,
    pub sha256: [u8; 32],
}

/// Rebuilds a separate epoch-zero local store from one stopped native replica journal.
#[derive(Clone, Debug)]
pub struct LocalRepair {
    pub source_storage: PathBuf,
    pub base_storage: Option<PathBuf>,
    pub replica_log: PathBuf,
    pub output_parent: PathBuf,
    pub base: RepairCheckpoint,
    pub through: RepairCheckpoint,
}

#[derive(Debug)]
pub struct RepairedStore {
    pub path: PathBuf,
    pub checkpoint: RepairCheckpoint,
    pub latest_transaction: TransactionKey,
    pub source_replica_epoch: StorageEpoch,
}

impl LocalRepair {
    /// An exclusive source lock covers the replay. The returned directory contains only complete checkpoints.
    pub async fn rebuild(
        &self, memory: Arc<MemoryBudget>, cancellation: &CancellationToken,
    ) -> Result<RepairedStore> {
        if self.base.block >= self.through.block {
            return Err(invalid("The target checkpoint must advance the trusted base"));
        }
        cancelled(cancellation)?;
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
        let source_root = fs::canonicalize(self.source_storage.join(&root)).map_err(io)?;
        let base_root = match &self.base_storage {
            Some(storage) => fs::canonicalize(storage.join(&root)).map_err(io)?,
            None => source_root.clone(),
        };
        let output_parent = fs::canonicalize(&self.output_parent).map_err(io)?;
        if output_parent.starts_with(&source_root) || output_parent.starts_with(&base_root) || !output_parent.is_dir() {
            return Err(invalid("The output parent must be a directory outside both input storage roots"));
        }
        let journal = fs::canonicalize(&self.replica_log).map_err(io)?;
        let journal_lock = File::open(&journal).map_err(io)?;
        FileExt::try_lock_exclusive(&journal_lock).map_err(|error| unavailable(format!("Cannot exclusively open the source journal: {error}")))?;

        let region = IoRegion::new().child_holding(Some(Arc::new(journal_lock)));
        let built = self.build((&source_root, &base_root), &journal, &output_parent, &root, &memory, &region, cancellation).await;
        region.close_and_wait().await;
        built
    }

    async fn build(
        &self, (source_root, base_root): (&Path, &Path), journal: &Path, output_parent: &Path, root: &str,
        memory: &Arc<MemoryBudget>, io_region: &Arc<IoRegion>, cancellation: &CancellationToken,
    ) -> Result<RepairedStore> {
        const CACHE_BYTES: usize = 32 << 20;
        let _cache_charge = memory.reserve(CACHE_BYTES)?;
        let cache = CacheBudget::new(CACHE_BYTES);
        let source = Arc::new(LocalStore::open_in(source_root, Arc::clone(io_region))?);
        let source_pool = pool(source.clone(), &cache, memory);
        let file_size_target = rtdb_index::TrieCatalog::default().file_size_target();
        let source_blocks = BlockStore::new(source.clone(), source_pool.clone(), Arc::clone(memory), file_size_target)?;
        let (base_storage, base_blocks) = if base_root == source_root {
            (source.clone(), source_blocks.clone())
        } else {
            let store = Arc::new(LocalStore::open_in(base_root, Arc::clone(io_region))?);
            let blocks = BlockStore::new(store.clone(), pool(store.clone(), &cache, memory), Arc::clone(memory), file_size_target)?;
            (store, blocks)
        };
        let (base, tables) = base_blocks.load(self.base.block).await?;
        require_root(&base, self.base)?;
        let target = source_blocks.load_fresh_catalog(self.through.block).await?;
        require_root(&target, self.through)?;
        let after = base.boundary_replica_position().ok_or_else(|| invalid("The base has no replica boundary"))?;
        let through = target.boundary_replica_position().ok_or_else(|| invalid("The target has no replica boundary"))?;
        let epoch = after.epoch();
        if after.0 < 0 || through.epoch() != epoch || through <= after {
            return Err(invalid("Repair requires a strictly advancing interval in one replica journal"));
        }
        let latest_transaction = target.latest_completed_tx().ok_or_else(|| invalid("The target has no completed transaction"))?;
        let options = ReadOnlyLocalLogOptions::default();
        let maximum_message_bytes = options.maximum_message_bytes;
        let replica = ReadOnlyLocalLog::open_in(journal, options, Arc::clone(io_region))?;
        let logs = PartitionLogs::new(
            Arc::new(MemoryLog::new(maximum_message_bytes)), replica, epoch, epoch, CURRENT_STORAGE_VERSION,
            StorageEpoch(0), source.clone(), source_pool, Arc::clone(memory),
        )?;

        let directory = Arc::new(tempfile::Builder::new().prefix("repair-").tempdir_in(output_parent).map_err(io)?);
        let staged = directory.path().join(".staging");
        let stage_io = io_region.child_holding(Some(directory.clone()));
        let destination = Arc::new(LocalStore::open_in(staged.join(root), Arc::clone(&stage_io))?);
        let mut base_wire = base.as_proto().clone();
        let mut digests = BTreeMap::new();
        for entry in base.table_entries()? {
            cancelled(cancellation)?;
            let table = tables.get(&entry.table).ok_or_else(|| invalid("The base has no bound table block"))?;
            if base.table_block_digest(entry.oid).is_none() {
                return Err(invalid("The repair base must bind every table block"));
            }
            let _scratch = memory.reserve(table.encoded_len().checked_mul(4).ok_or_else(|| invalid("Table block exceeds addressable memory"))?)?;
            for file in table.checkpoint_trie_files(&entry.slug, CheckpointContents::Primary)? {
                cancelled(cancellation)?;
                let file = ObjectKey::new(file)?;
                base_storage.copy_to(&file, &destination, &file).await?;
            }
            let primary = table.for_primary_repair();
            let bytes = Bytes::from(primary.encode());
            digests.insert(entry.oid, publication_digest(&bytes).to_vec());
            destination.put(&ObjectKey::new(entry.slug.block_key(self.base.block)?)?, bytes).await?;
        }
        drop(tables);
        base_wire.table_block_manifest = Some(rtdb_format::block::proto::TableBlockManifest { sha256_by_oid: digests });
        let mut staged_base = BlockCatalog::from_proto(base_wire)?;
        destination.put(&ObjectKey::new(block_key(self.base.block)?)?, Bytes::from(staged_base.encode_for_publication())).await?;

        let stage_blocks = BlockStore::new(destination.clone(), pool(destination.clone(), &cache, memory), Arc::clone(memory), file_size_target)?;
        let mut repaired = stage_blocks.recover(Default::default(), logs.source_floor()?, Timestamp(0), BlockDiscovery::At(self.base.block)).await?;
        let sha256 = replay(&logs, &stage_blocks, &destination, &mut repaired, &base, &target, memory, cancellation).await?;
        let checkpoint = RepairCheckpoint { block: self.through.block, sha256 };
        drop(stage_blocks);
        drop(destination);
        stage_io.close_and_wait().await;
        drop(stage_io);
        let directory = Arc::try_unwrap(directory).map_err(|_| invalid("Repair staging still has active I/O"))?;
        cancelled(cancellation)?;
        let ready = directory.path().join("storage");
        fs::rename(staged, &ready).map_err(io)?;
        rtdb_storage::sync_directory(directory.path()).map_err(io)?;
        rtdb_storage::sync_directory(output_parent).map_err(io)?;
        let path = directory.keep().join("storage");
        Ok(RepairedStore { path, checkpoint, latest_transaction, source_replica_epoch: epoch })
    }
}

async fn replay(
    logs: &PartitionLogs, blocks: &BlockStore, destination: &LocalStore, repair: &mut RecoveredPartition,
    base: &BlockCatalog, target: &BlockCatalog, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<[u8; 32]> {
    let after = base.boundary_replica_position().ok_or_else(|| invalid("The base has no replica boundary"))?;
    let through = target.boundary_replica_position().ok_or_else(|| invalid("The target has no replica boundary"))?;
    let exclusive = ReplicaPosition(through.0.checked_add(1).ok_or_else(|| invalid("Replica interval overflows"))?);
    let mut range = logs.replica_range(after, exclusive).await?;
    let bound = range.allocation_bound();
    let first = range.next(ReadReservation::new(bound, memory.reserve(bound)?)).await?
        .ok_or_else(|| unavailable("The source journal does not contain the trusted base boundary"))?;
    let first = logs.decode_replica(first)?.ok_or_else(|| invalid("The trusted base has an unknown replica record"))?;
    require_boundary(&first, base)?;
    drop(first);
    let mut previous = base.clone();
    while let Some(raw) = cancellation.run_until_cancelled(range.next(ReadReservation::new(bound, memory.reserve(bound)?)))
        .await.ok_or_else(interrupted)??
    {
        let record = logs.decode_replica(raw)?.ok_or_else(|| invalid("The repair interval contains an unknown replica record"))?;
        let position = record.metadata.position;
        let (_, admitted) = repair.fence.admit_record(record)?;
        let Some(admitted) = admitted else {
            if position == through { return Err(invalid("The target checkpoint belongs to a fenced replica record")); }
            continue;
        };
        let record = admitted.resolve_payload(logs).await?;
        if position == through { require_boundary(&record, target)?; }
        let installed = repair.partition.value();
        match &record.envelope.message {
            ReplicaBody::ResolvedTx(message) => {
                repair.apply_archived_transaction(message, memory)?;
            }
            ReplicaBody::BlockBoundary(message) => {
                let index = BlockIndex(message.block_index.ok_or_else(|| invalid("A replay boundary has no block index"))?);
                if installed.table_catalog().block_index().is_some_and(|prior| index <= prior) { continue; }
                if index != installed.live().block_index() || message.external_source_token.is_some() {
                    return Err(invalid("The replica journal has a discontinuous or external-source block boundary"));
                }
                let source = message.latest_processed_msg_id.map(SourcePosition)
                    .filter(|source| *source >= repair.progress.source_position)
                    .ok_or_else(|| invalid("The replica boundary does not cover the replayed source prefix"))?;
                let prepared = repair.partition.prepare_block()?;
                if position == through && installed.live().latest_completed_tx() != target.latest_completed_tx() {
                    return Err(invalid("The replayed transaction head differs from the target checkpoint"));
                }
                let output = blocks.produce_cancellable(&prepared, &record, Default::default(), cancellation).await?;
                if position == through && output.catalog.table_entries()? != target.table_entries()? {
                    return Err(invalid("The rebuilt table registry differs from the target checkpoint"));
                }
                let (stored, table_blocks) = blocks.load(index).await?;
                let digest = stored.publication_sha256().filter(|digest| Some(*digest) == output.catalog.publication_sha256())
                    .ok_or_else(|| invalid("The staged checkpoint differs from its durable read-back"))?;
                repair.partition.adopt_block(prepared, stored.clone(), &table_blocks, &output.tries, record.metadata.timestamp)?;
                repair.progress.source_position = source;
                discard_staged_checkpoint(destination, &previous, cancellation).await?;
                previous = stored;
                if position == through { return Ok(digest); }
            }
            ReplicaBody::NoOp(_) | ReplicaBody::BlockUploaded(_) | ReplicaBody::TriesAdded(_)
            | ReplicaBody::TriesDeleted(_) | ReplicaBody::ArtefactsAdded(_) | ReplicaBody::ArtefactsRemoved(_) => (),
            ReplicaBody::OversizedMessage(_) => return Err(invalid("The source journal has an unresolved oversized record")),
        }
    }
    Err(unavailable("The source journal ended before the target checkpoint"))
}

async fn discard_staged_checkpoint(
    destination: &LocalStore, checkpoint: &BlockCatalog, cancellation: &CancellationToken,
) -> Result<()> {
    let index = checkpoint.block_index();
    destination.delete(&ObjectKey::new(block_key(index)?)?).await?;
    for entry in checkpoint.table_entries()? {
        cancelled(cancellation)?;
        destination.delete(&ObjectKey::new(entry.slug.block_key(index)?)?).await?;
    }
    Ok(())
}

fn require_boundary(record: &ReplicaRecord, catalog: &BlockCatalog) -> Result<()> {
    if !matches_catalog_boundary(catalog, record) {
        return Err(invalid("The checkpoint does not match its journal boundary"));
    }
    Ok(())
}

fn require_root(root: &BlockCatalog, checkpoint: RepairCheckpoint) -> Result<()> {
    if root.block_index() != checkpoint.block || root.publication_sha256() != Some(checkpoint.sha256) {
        return Err(invalid("The source checkpoint does not match the selected publication"));
    }
    if root.external_source_token().is_some() || !root.secondary_databases().is_empty() {
        return Err(invalid("Local repair requires one database without an external source"));
    }
    Ok(())
}

fn pool(store: Arc<dyn ObjectStore>, cache: &Arc<CacheBudget>, memory: &Arc<MemoryBudget>) -> Arc<BufferPool> {
    Arc::new(BufferPool::with_metadata_budget(store, Arc::clone(cache), Arc::clone(memory)))
}

fn cancelled(cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() { Err(interrupted()) } else { Ok(()) }
}

fn interrupted() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "rtdb.repair/cancelled", "Local repair was cancelled") }
fn invalid(reason: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.repair/invalid-checkpoint", reason) }
fn unavailable(reason: impl Into<String>) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.repair/source-unavailable", reason) }
fn io(error: std::io::Error) -> Anomaly { Anomaly::fault("rtdb.repair/io", error.to_string()) }
