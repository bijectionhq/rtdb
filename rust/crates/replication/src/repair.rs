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
    catalog::{BlockCatalog, CheckpointContents, TableBlock, legacy_table_ref, publication_digest, same_publication_digest},
    envelope::ReplicaBody,
    keys::{CURRENT_STORAGE_VERSION, StorageRoot, block_key},
};
use rtdb_index::{TableCatalog, TrieCatalog, TrieState};
use rtdb_storage::{
    HeldView, IoRegion, LocalRepairAnchor, LocalStore, ObjectKey, ObjectStore,
    cache::{BufferPool, CacheBudget},
    log::{MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions},
    object_store::ReadReservation,
    sealing::ClassicKeyVault,
};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCut, ReplicaPosition, Result, SourcePosition, StorageEpoch, TableRef, Timestamp, TransactionKey};
use rtdb_types::residence_trace::{self, RepairInventory, RepairInventoryTarget};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{BlockDiscovery, BlockStore, PartitionLogs, RecoveredPartition, ReplicaRecord, blocks::matches_catalog_boundary, repair_decision};

const CACHE_BYTES: usize = 32 << 20;

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

/// Rebuilds from the latest anchor retained by the original local storage root.
#[derive(Clone, Debug)]
pub struct RetainedLocalRepair {
    pub source_storage: PathBuf,
    pub base_storage: PathBuf,
    pub replica_log: PathBuf,
    pub output_parent: PathBuf,
    pub through: RepairCheckpoint,
}

#[derive(Debug)]
pub struct RepairedStore {
    pub path: PathBuf,
    pub checkpoint: RepairCheckpoint,
    pub latest_transaction: TransactionKey,
    pub source_replica_epoch: StorageEpoch,
}

/// Protects replay from an independently stored copy of the stopped database's current local checkpoint.
#[derive(Clone, Debug)]
pub struct RetainRepairBase {
    pub source_storage: PathBuf,
    pub base_storage: PathBuf,
    pub replica_log: PathBuf,
    pub base: RepairCheckpoint,
}

impl RetainRepairBase {
    /// Installs a durable block-collection bound after verifying the detached base and its journal boundary.
    pub async fn install(&self, memory: Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<ReplicaPosition> {
        self.install_with_retention(memory, cancellation, false).await
    }

    pub async fn install_and_reclaim(&self, memory: Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<ReplicaPosition> {
        self.install_with_retention(memory, cancellation, true).await
    }

    async fn install_with_retention(&self, memory: Arc<MemoryBudget>, cancellation: &CancellationToken, reclaim: bool) -> Result<ReplicaPosition> {
        cancelled(cancellation)?;
        let vault = std::env::var_os("RTDB_CLASSIC_KEY_VAULT")
            .map(|path| ClassicKeyVault::open(Path::new(&path))).transpose()?;
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
        let source_root = fs::canonicalize(self.source_storage.join(&root)).map_err(io)?;
        let base_root = fs::canonicalize(self.base_storage.join(&root)).map_err(io)?;
        require_detached_base(&source_root, &base_root)?;
        let (journal, region) = exclusive_journal(&self.replica_log)?;
        let result = self.install_locked(source_root.clone(), base_root, &journal, &region, &memory, cancellation, vault.as_ref()).await;
        region.close_and_wait().await;
        drop(region);
        let anchor = result?;
        if reclaim {
            cancelled(cancellation)?;
            let source = LocalStore::open(source_root)?.with_catalog_ledger(anchor.boundary.epoch());
            let result = source.retire_classic_log_prefixes(anchor, &journal, ReadOnlyLocalLogOptions::default().maximum_message_bytes, vault.as_ref()).await;
            source.close_and_wait().await;
            result?;
        }
        Ok(anchor.boundary)
    }

    async fn install_locked(
        &self, source_root: PathBuf, base_root: PathBuf, journal: &Path,
        region: &Arc<IoRegion>, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
        vault: Option<&ClassicKeyVault>,
    ) -> Result<LocalRepairAnchor> {
        let _cache_charge = memory.reserve(CACHE_BYTES)?;
        let cache = CacheBudget::new(CACHE_BYTES);
        let source = Arc::new(LocalStore::open_in(&source_root, region.clone())?);
        let base_store = Arc::new(LocalStore::open_in(&base_root, region.clone())?);
        let source_body = repair_body(source.clone(), vault, region).await?;
        let base_body = repair_body(base_store.clone(), vault, region).await?;
        let source_pool = pool(source_body.clone(), &cache, memory);
        let file_size_target = rtdb_index::TrieCatalog::default().file_size_target();
        let source_blocks = BlockStore::new(source_body.clone(), source_pool.clone(), memory.clone(), file_size_target)?;
        if source_blocks.latest_listed_block().await? != Some(self.base.block) {
            return Err(invalid("The repair anchor must begin at the current serving checkpoint"));
        }
        let serving = source_blocks.load_fresh_catalog(self.base.block).await?;
        require_root(&serving, self.base)?;
        require_offline_lifetime_admission(&serving)?;
        let base_blocks = BlockStore::new(base_body.clone(), pool(base_body, &cache, memory), memory.clone(), file_size_target)?;
        let (base, tables) = base_blocks.load(self.base.block).await?;
        require_root(&base, self.base)?;
        require_offline_lifetime_admission(&base)?;
        base_blocks.verify_primary_inventory(&base, &tables).await?;
        let primary_inventory_sha256 = primary_inventory_digest(base_blocks.object_store(), &base, &tables, memory, cancellation).await?;
        let serving_inventory = primary_inventory_digest(source_body.as_ref(), &serving, &tables, memory, cancellation).await?;
        if primary_inventory_sha256 != serving_inventory {
            return Err(invalid("The detached repair inventory differs from the serving checkpoint"));
        }
        for entry in base.table_entries()? {
            cancelled(cancellation)?;
            let table = tables.get(&entry.table).ok_or_else(|| invalid("The base has no bound table block"))?;
            if base.table_block_digest(entry.oid).is_none() {
                return Err(invalid("The repair base must bind every table block"));
            }
            let _scratch = memory.reserve(table_manifest_reservation(table.encoded_len())?)?;
            for file in table.checkpoint_trie_files(&entry, CheckpointContents::Primary)? {
                cancelled(cancellation)?;
                base_store.sync_existing(&ObjectKey::new(file)?).await?;
            }
            base_store.sync_existing(&ObjectKey::new(entry.slug.block_key(self.base.block)?)?).await?;
        }
        base_store.sync_existing(&ObjectKey::new(block_key(self.base.block)?)?).await?;
        for parent in base_root.ancestors().skip(1) {
            cancelled(cancellation)?;
            rtdb_storage::sync_directory(parent).map_err(io)?;
        }
        let boundary = base.boundary_replica_position().filter(|position| position.0 >= 0)
            .ok_or_else(|| invalid("The base has no usable replica boundary"))?;
        let after = next_replica_byte(boundary)?;
        let logs = open_repair_logs(journal, region, boundary.epoch(), source_body, source_pool, memory, vault)?;
        let mut range = logs.replica_range(boundary, after).await?;
        let bound = range.allocation_bound();
        let record = range.next(ReadReservation::new(bound, memory.reserve(bound)?)).await?
            .ok_or_else(|| unavailable("The source journal does not contain the trusted base boundary"))?;
        let record = logs.decode_replica(record)?.ok_or_else(|| invalid("The trusted base has an unknown replica record"))?;
        require_boundary(&record, &base)?;
        cancelled(cancellation)?;
        let anchor = LocalRepairAnchor { block: self.base.block, boundary, sha256: self.base.sha256, primary_inventory_sha256 };
        source.install_repair_anchor(anchor).await?;
        repair_inventory_observation(&source_root.to_string_lossy(), anchor, serving_inventory, None)
            .observe("repair-inventory-enrolled");
        Ok(anchor)
    }
}

fn repair_inventory_observation(
    residence: &str, anchor: LocalRepairAnchor, observed_inventory: [u8; 32], target: Option<RepairInventoryTarget>,
) -> RepairInventory<'_> {
    RepairInventory {
        residence, anchor_block: anchor.block.0, anchor_boundary: anchor.boundary.0,
        anchor_root: anchor.sha256, inventory: anchor.primary_inventory_sha256, observed_inventory, target,
    }
}

fn trace_repair_target(target: RepairCheckpoint, through: ReplicaPosition) -> RepairInventoryTarget {
    RepairInventoryTarget { invocation: uuid::Uuid::new_v4(), block: target.block.0, boundary: through.0, root: target.sha256 }
}

impl LocalRepair {
    /// An exclusive source lock covers the replay. The returned directory contains only complete checkpoints.
    pub async fn rebuild(
        &self, memory: Arc<MemoryBudget>, cancellation: &CancellationToken,
    ) -> Result<RepairedStore> {
        require_advancing_cut(self.base, self.through)?;
        cancelled(cancellation)?;
        let paths = RepairPaths::resolve(&self.source_storage, self.base_storage.as_deref(), &self.output_parent)?;
        RepairRange { base: BaseSelection::Explicit(self.base), through: self.through }
            .rebuild(paths, &self.replica_log, &memory, cancellation).await
    }
}

impl RetainedLocalRepair {
    /// Reads the anchor under the original journal lock and checks it against the detached export.
    pub async fn rebuild(
        &self, memory: Arc<MemoryBudget>, cancellation: &CancellationToken,
    ) -> Result<RepairedStore> {
        cancelled(cancellation)?;
        let paths = RepairPaths::resolve(&self.source_storage, Some(&self.base_storage), &self.output_parent)?;
        require_detached_base(&paths.source_root, &paths.base_root)?;
        RepairRange { base: BaseSelection::Retained, through: self.through }
            .rebuild(paths, &self.replica_log, &memory, cancellation).await
    }
}

pub struct PrimaryReset {
    source: Arc<PartitionLogs>,
    staged: Option<Arc<LocalStore>>,
    store: Arc<dyn ObjectStore>,
    hold: Arc<HeldView>,
    catalog: Arc<TrieCatalog>,
    tables: Arc<TableCatalog>,
    target: TransactionKey,
}

pub struct PublishedPrimaryReset {
    pub(crate) source: Arc<PartitionLogs>,
    pub(crate) expected: Arc<TrieCatalog>,
    pub(crate) block: BlockIndex,
    pub(crate) catalog: TrieCatalog,
    pub(crate) target: TransactionKey,
}

impl PrimaryReset {
    pub async fn from_installed(
        source: Arc<PartitionLogs>, installed: &BlockCatalog,
        cut: Option<CatalogCut>, file_size_target: i64, cancellation: &CancellationToken,
    ) -> Result<Self> {
        let target = installed.latest_completed_tx().ok_or_else(no_reset_transactions)?;
        let store = Arc::clone(source.storage());
        // Open the authenticated root under a conservative claim, then narrow
        // custody to the immutable files this reset can open.
        let selection = HeldView::catalog(store.as_ref(), Arc::new(installed.clone()), cut, None).await?;
        let load = async {
            let blocks = source.recovery_block_store(file_size_target)?;
            let (root, tables) = blocks.load(installed.block_index()).await?;
            if installed.publication_sha256().is_none() || root.publication_sha256() != installed.publication_sha256() {
                return Err(reset_inventory("The checkpoint root differs from the installed publication"));
            }
            if root.table_entries()?.iter().any(|entry| root.table_block_digest(entry.oid).is_none()) {
                return Err(reset_inventory("The checkpoint has no bound table manifest"));
            }
            let paths: Arc<[String]> = crate::replay_closure::checkpoint_files(&root, &tables, CheckpointContents::Primary)?
                .into_iter().filter(|key| rtdb_format::keys::is_catalog_retirement_file(key.as_str()))
                .map(|key| key.to_string()).collect();
            let hold = HeldView::catalog(store.as_ref(), Arc::new(root.clone()), cut, Some(paths)).await?;
            IoRegion::reading(Arc::clone(&hold), cancellation.run_until_cancelled(blocks.verify_primary_inventory(&root, &tables)))
                .await.ok_or_else(crate::progress::cancelled)?
                .map_err(|error| match error.code.as_str() {
                    "rtdb.storage/missing-object" | "rtdb.replication/checkpoint-primary-extent" =>
                        reset_inventory(error.message.unwrap_or_default()),
                    _ => error,
                })?;
            let table_catalog = TableCatalog::load(Some(root), &tables)?;
            let primary = tables.into_iter().map(|(table, block)| (table, block.for_primary_repair())).collect();
            let catalog = TrieCatalog::load(&primary, file_size_target, target.system_time)?;
            Ok((Arc::new(table_catalog), Arc::new(catalog), hold))
        };
        let (tables, catalog, hold) = IoRegion::reading(selection, load).await?;
        Ok(Self { source, staged: None, store, hold, catalog, tables, target })
    }

    /// Rebuilds the adopted cut from a detached anchor and the journal held by this maintenance node.
    pub async fn from_retained(source: Arc<PartitionLogs>, installed: &BlockCatalog, export: &Path,
        file_size_target: i64, cancellation: &CancellationToken,
    ) -> Result<Self> {
        cancelled(cancellation)?;
        let local = source.local_recovery_store().ok_or_else(|| invalid("Retained reset requires a durable local store and journal"))?;
        if source.storage_version != CURRENT_STORAGE_VERSION || source.storage_epoch != StorageEpoch(0) || !local.defers_trie_releases() {
            return Err(invalid("Retained reset requires coordinated epoch-zero local storage"));
        }
        let memory = source.memory();
        let _cache_charge = memory.reserve(CACHE_BYTES)?;
        let cache = CacheBudget::new(CACHE_BYTES);
        let recovery = HeldView::recovery(local.hold_recovery_open().await?);
        let anchor = local.retained_repair_anchor().await?.ok_or_else(|| invalid("The source storage has no retained repair base"))?;
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
        let export = fs::canonicalize(export.join(root)).map_err(io)?;
        require_detached_base(local.root(), &export)?;
        let base_receipt = RepairCheckpoint { block: anchor.block, sha256: anchor.sha256 };
        let target_receipt = RepairCheckpoint { block: installed.block_index(), sha256: installed.publication_sha256()
            .ok_or_else(|| invalid("The adopted checkpoint has no publication digest"))? };
        require_advancing_cut(base_receipt, target_receipt)?;
        let current = source.recovery_block_store(file_size_target)?;
        let target = IoRegion::reading(Arc::clone(&recovery), current.load_fresh_catalog(target_receipt.block)).await?;
        require_root(&target, target_receipt)?;
        require_offline_lifetime_admission(&target)?;
        let target_tx = target.latest_completed_tx().ok_or_else(|| invalid("The target has no completed transaction"))?;
        let through = target.boundary_replica_position().ok_or_else(|| invalid("The target has no replica boundary"))?;
        if through.epoch() != source.replica_epoch || !repair_decision::admits_repair_interval(anchor.boundary.0, through.0) {
            return Err(invalid("The repair anchor and target must be in the held replica journal"));
        }

        let directory = Arc::new(tempfile::Builder::new().prefix("rtdb-reset-").tempdir().map_err(io)?);
        let region = IoRegion::new().child_holding(Some(directory.clone()));
        let archive = Arc::new(LocalStore::open_in(export, region.clone())?);
        let base_blocks = BlockStore::new(archive.clone(), pool(archive.clone(), &cache, memory), Arc::clone(memory), file_size_target)?;
        let (base, tables) = base_blocks.load(anchor.block).await?;
        require_root(&base, base_receipt)?;
        require_offline_lifetime_admission(&base)?;
        if base.boundary_replica_position() != Some(anchor.boundary) {
            return Err(invalid("The retained repair boundary differs from its exported checkpoint"));
        }
        let staged = Arc::new(LocalStore::open_in(directory.path(), region)?);
        stage_primary_base(&archive, &staged, &base, &tables, memory, cancellation).await?;
        let observed_inventory = primary_inventory_digest(staged.as_ref(), &base, &tables, memory, cancellation).await?;
        if observed_inventory != anchor.primary_inventory_sha256 {
            return Err(invalid("The retained primary inventory differs from its enrolled checkpoint"));
        }
        let residence = local.root().to_string_lossy();
        let observation = residence_trace::enabled().then(|| repair_inventory_observation(
            &residence, anchor, observed_inventory, Some(trace_repair_target(target_receipt, through)),
        ));
        if let Some(observation) = &observation { observation.observe("repair-inventory-verified"); }
        let staged_blocks = BlockStore::new(staged.clone(), pool(staged.clone(), &cache, memory), Arc::clone(memory), file_size_target)?;
        let mut rebuilt = staged_blocks.recover(Default::default(), source.source_floor()?, Timestamp(0), BlockDiscovery::At(base.block_index())).await?;
        IoRegion::reading(recovery,
            replay(&source, &staged_blocks, &staged, &mut rebuilt, &base, &target, memory, cancellation)).await?;
        if let Some(observation) = observation { observation.observe("repair-replayed"); }
        let value = rebuilt.partition.value();
        let catalog = Arc::clone(value.trie_catalog());
        let tables = Arc::clone(value.table_catalog());
        let store = Arc::clone(source.storage());
        let hold = HeldView::recovery(staged.clone());
        Ok(Self { source, staged: Some(staged), store, hold, catalog, tables, target: target_tx })
    }

    pub fn catalog(&self) -> &TrieCatalog { &self.catalog }

    /// Copies replacement pairs before the leader can publish a catalog that names them.
    pub async fn publish_files(self, expected: Arc<TrieCatalog>, block: BlockIndex,
        cancellation: &CancellationToken,
    ) -> Result<PublishedPrimaryReset> {
        let (catalog, files_epoch) = expected.reset_from_primary((*self.catalog).clone(), block, self.target.system_time)?;
        let publish = async {
            for entry in self.tables.entries() {
                for trie in self.catalog.iter_tries(&entry.table).filter(|trie| !matches!(trie.state, TrieState::Garbage { .. })) {
                    cancelled(cancellation)?;
                    let relocated = if trie.key.level() > 0 { trie.key.clone().with_epoch(files_epoch) } else { trie.key.clone() };
                    let objects = [(entry.slug.data_key(&trie.key), entry.slug.data_key(&relocated)),
                        (entry.slug.metadata_key(&trie.key), entry.slug.metadata_key(&relocated))];
                    for (from, to) in objects {
                        if let Some(staged) = &self.staged {
                            let destination = self.source.local_recovery_store()
                                .ok_or_else(|| invalid("The local recovery store is unavailable"))?;
                            staged.copy_to(&ObjectKey::new(from)?, destination, &ObjectKey::new(to)?).await?;
                        } else if from != to {
                            self.store.copy(&ObjectKey::new(from)?, &ObjectKey::new(to)?).await?;
                        }
                    }
                }
            }
            Ok::<_, Anomaly>(())
        };
        IoRegion::reading(Arc::clone(&self.hold), publish).await?;
        Ok(PublishedPrimaryReset { source: self.source, expected, block, catalog, target: self.target })
    }
}

fn no_reset_transactions() -> Anomaly {
    Anomaly::incorrect("rtdb.compactor-reset/no-transactions", "Compactor reset requires a completed transaction")
}

fn reset_inventory(message: impl Into<String>) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.compactor-reset/incomplete-inventory", message)
}

struct RepairPaths {
    source_root: PathBuf,
    base_root: PathBuf,
    output_parent: PathBuf,
    root: String,
}

impl RepairPaths {
    fn resolve(source: &Path, base: Option<&Path>, output_parent: &Path) -> Result<Self> {
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
        let source_root = fs::canonicalize(source.join(&root)).map_err(io)?;
        let base_root = match base {
            Some(storage) => fs::canonicalize(storage.join(&root)).map_err(io)?,
            None => source_root.clone(),
        };
        let output_parent = fs::canonicalize(output_parent).map_err(io)?;
        if output_parent.starts_with(&source_root) || output_parent.starts_with(&base_root) || !output_parent.is_dir() {
            return Err(invalid("The output parent must be a directory outside both input storage roots"));
        }
        Ok(Self { source_root, base_root, output_parent, root })
    }
}

enum BaseSelection {
    Explicit(RepairCheckpoint),
    Retained,
}

struct RepairRange {
    base: BaseSelection,
    through: RepairCheckpoint,
}

impl RepairRange {
    async fn rebuild(
        &self, paths: RepairPaths, replica_log: &Path, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
    ) -> Result<RepairedStore> {
        let (journal, region) = exclusive_journal(replica_log)?;
        let built = self.build(&paths, &journal, memory, &region, cancellation).await;
        region.close_and_wait().await;
        built
    }

    async fn build(
        &self, paths: &RepairPaths, journal: &Path,
        memory: &Arc<MemoryBudget>, io_region: &Arc<IoRegion>, cancellation: &CancellationToken,
    ) -> Result<RepairedStore> {
        let RepairPaths { source_root, base_root, output_parent, root } = paths;
        let _cache_charge = memory.reserve(CACHE_BYTES)?;
        let cache = CacheBudget::new(CACHE_BYTES);
        let source = Arc::new(LocalStore::open_in(source_root, Arc::clone(io_region))?);
        let (base_receipt, retained_anchor) = match self.base {
            BaseSelection::Explicit(base) => (base, None),
            BaseSelection::Retained => {
                let anchor = source.retained_repair_anchor().await?
                    .ok_or_else(|| invalid("The source storage has no retained repair base"))?;
                (RepairCheckpoint { block: anchor.block, sha256: anchor.sha256 }, Some(anchor))
            }
        };
        require_advancing_cut(base_receipt, self.through)?;
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
        let (base, tables) = base_blocks.load(base_receipt.block).await?;
        require_root(&base, base_receipt)?;
        require_offline_lifetime_admission(&base)?;
        let target = source_blocks.load_fresh_catalog(self.through.block).await?;
        require_root(&target, self.through)?;
        require_offline_lifetime_admission(&target)?;
        let after = base.boundary_replica_position().ok_or_else(|| invalid("The base has no replica boundary"))?;
        if !repair_decision::matches_retained_boundary(retained_anchor.map(|anchor| anchor.boundary.0), after.0) {
            return Err(invalid("The retained repair boundary differs from its exported checkpoint"));
        }
        let through = target.boundary_replica_position().ok_or_else(|| invalid("The target has no replica boundary"))?;
        let epoch = after.epoch();
        if !repair_decision::admits_repair_interval(after.0, through.0) {
            return Err(invalid("Repair requires a strictly advancing interval in one replica journal"));
        }
        let latest_transaction = target.latest_completed_tx().ok_or_else(|| invalid("The target has no completed transaction"))?;
        let logs = open_repair_logs(journal, io_region, epoch, source, source_pool, memory, None)?;

        let directory = Arc::new(tempfile::Builder::new().prefix("repair-").tempdir_in(output_parent).map_err(io)?);
        let staged = directory.path().join(".staging");
        let stage_io = io_region.child_holding(Some(directory.clone()));
        let destination = Arc::new(LocalStore::open_in(staged.join(root), Arc::clone(&stage_io))?);
        stage_primary_base(&base_storage, &destination, &base, &tables, memory, cancellation).await?;
        let residence = source_root.to_string_lossy();
        let observation = if let Some(anchor) = retained_anchor {
            let observed_inventory = primary_inventory_digest(destination.as_ref(), &base, &tables, memory, cancellation).await?;
            if observed_inventory != anchor.primary_inventory_sha256 {
                return Err(invalid("The retained primary inventory differs from its enrolled checkpoint"));
            }
            residence_trace::enabled().then(|| repair_inventory_observation(
                &residence, anchor, observed_inventory, Some(trace_repair_target(self.through, through)),
            ))
        } else {
            None
        };
        if let Some(observation) = &observation { observation.observe("repair-inventory-verified"); }

        let stage_blocks = BlockStore::new(destination.clone(), pool(destination.clone(), &cache, memory), Arc::clone(memory), file_size_target)?;
        let mut repaired = stage_blocks.recover(Default::default(), logs.source_floor()?, Timestamp(0), BlockDiscovery::At(base_receipt.block)).await?;
        let sha256 = replay(&logs, &stage_blocks, &destination, &mut repaired, &base, &target, memory, cancellation).await?;
        if let Some(observation) = observation { observation.observe("repair-replayed"); }
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

async fn stage_primary_base(
    base_storage: &LocalStore,
    destination: &LocalStore,
    base: &BlockCatalog,
    tables: &BTreeMap<TableRef, TableBlock>,
    memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken,
) -> Result<()> {
    let mut base_wire = base.as_proto().clone();
    let mut digests = BTreeMap::new();
    for entry in base.table_entries()? {
        cancelled(cancellation)?;
        let table = tables.get(&entry.table).ok_or_else(|| invalid("The base has no bound table block"))?;
        if base.table_block_digest(entry.oid).is_none() {
            return Err(invalid("The repair base must bind every table block"));
        }
        let _scratch = memory.reserve(table_manifest_reservation(table.encoded_len())?)?;
        for file in table.checkpoint_trie_files(&entry, CheckpointContents::Primary)? {
            cancelled(cancellation)?;
            let file = ObjectKey::new(file)?;
            base_storage.copy_to(&file, &destination, &file).await?;
        }
        let primary = table.for_primary_repair();
        let bytes = Bytes::from(primary.encode());
        digests.insert(entry.oid, publication_digest(&bytes).to_vec());
        destination.put(&ObjectKey::new(entry.slug.block_key(base.block_index())?)?, bytes).await?;
    }
    base_wire.table_block_manifest = Some(rtdb_format::block::proto::TableBlockManifest { sha256_by_oid: digests });
    let mut staged_base = BlockCatalog::from_proto(base_wire)?;
    destination.put(&ObjectKey::new(block_key(base.block_index())?)?, Bytes::from(staged_base.encode_for_publication())).await?;

    Ok(())
}

async fn primary_inventory_digest(
    store: &dyn ObjectStore,
    catalog: &BlockCatalog,
    tables: &BTreeMap<TableRef, TableBlock>,
    memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken,
) -> Result<[u8; 32]> {
    const PAGE_BYTES: u64 = 256 << 10;
    let mut digest = Sha256::new();
    digest.update(b"rtdb/local-repair-primary-inventory/v1\0");
    for entry in catalog.table_entries()? {
        let table = tables.get(&entry.table).ok_or_else(|| invalid("The base has no bound table block"))?;
        let _scratch = memory.reserve(table_manifest_reservation(table.encoded_len())?)?;
        for name in table.checkpoint_trie_files(&entry, CheckpointContents::Primary)? {
            cancelled(cancellation)?;
            let key = ObjectKey::new(name)?;
            let extent = store.head_fresh(&key).await?.size;
            digest.update((key.as_str().len() as u64).to_be_bytes());
            digest.update(key.as_str().as_bytes());
            digest.update(extent.to_be_bytes());
            if store.supports_scan_ranges(&key) {
                let mut offset = 0;
                while offset < extent {
                    cancelled(cancellation)?;
                    let end = offset.saturating_add(PAGE_BYTES).min(extent);
                    let length = (end - offset) as usize;
                    let allocation = store.scan_range_allocation(&key, length)?;
                    let retained = memory.reserve(allocation.retained_bytes)?;
                    let scratch = (allocation.scratch_bytes != 0).then(|| memory.reserve(allocation.scratch_bytes)).transpose()?;
                    let body = store.get_reserved_scan_range(&key, offset..end, ReadReservation::funded(length, retained, scratch)).await?;
                    digest.update(body.as_slice());
                    offset = end;
                }
            } else {
                let length = usize::try_from(extent).map_err(|_| invalid("A primary object exceeds addressable memory"))?;
                let allocation = store.whole_object_allocation(&key, length)?;
                let retained = memory.reserve(allocation.retained_bytes)?;
                let scratch = (allocation.scratch_bytes != 0).then(|| memory.reserve(allocation.scratch_bytes)).transpose()?;
                let body = store.get_reserved_object(&key, ReadReservation::funded(length, retained, scratch)).await?;
                digest.update(body.as_slice());
            }
        }
    }
    Ok(digest.finalize().into())
}

fn require_offline_lifetime_admission(catalog: &BlockCatalog) -> Result<()> {
    if catalog.table_entries()?.iter().any(|entry| entry.table.owned_lifetime_relation().is_some()) {
        return Err(unavailable("Offline repair requires the original placed authority for lifetime replay"));
    }
    Ok(())
}

async fn replay(
    logs: &PartitionLogs, blocks: &BlockStore, destination: &LocalStore, repair: &mut RecoveredPartition,
    base: &BlockCatalog, target: &BlockCatalog, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken,
) -> Result<[u8; 32]> {
    let after = base.boundary_replica_position().ok_or_else(|| invalid("The base has no replica boundary"))?;
    let through = target.boundary_replica_position().ok_or_else(|| invalid("The target has no replica boundary"))?;
    let mut range = logs.replica_range(after, next_replica_byte(through)?).await?;
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
                if message.table_data.keys().any(|name| rtdb_types::TableRef::is_owned_lifetime_replica_table(name)) {
                    return Err(unavailable("Offline replay cannot apply a placed lifetime floor"));
                }
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
            ReplicaBody::TablesRetired(message) => {
                let tables = message.table_names.iter().map(|name| legacy_table_ref(name)).collect::<Result<Vec<_>>>()?;
                repair.partition.retire_tables(tables, Timestamp(message.as_of_micros.unwrap_or_default()));
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
    let digest_matches = root.publication_sha256().is_some_and(|digest| same_publication_digest(digest, checkpoint.sha256));
    if !repair_decision::matches_root(checkpoint.block.0, root.block_index().0, digest_matches) {
        return Err(invalid("The source checkpoint does not match the selected publication"));
    }
    if root.external_source_token().is_some() || !root.secondary_databases().is_empty() {
        return Err(invalid("Local repair requires one database without an external source"));
    }
    Ok(())
}

fn require_detached_base(source: &Path, base: &Path) -> Result<()> {
    if base.starts_with(source) || source.starts_with(base) {
        return Err(invalid("The repair base must be detached from the serving storage root"));
    }
    Ok(())
}

fn require_advancing_cut(base: RepairCheckpoint, through: RepairCheckpoint) -> Result<()> {
    if base.block >= through.block {
        return Err(invalid("The target checkpoint must advance the trusted base"));
    }
    Ok(())
}

fn exclusive_journal(path: &Path) -> Result<(PathBuf, Arc<IoRegion>)> {
    let path = fs::canonicalize(path).map_err(io)?;
    let file = File::open(&path).map_err(io)?;
    FileExt::try_lock_exclusive(&file)
        .map_err(|error| unavailable(format!("Cannot exclusively open the source journal: {error}")))?;
    Ok((path, IoRegion::new().child_holding(Some(Arc::new(file)))))
}

async fn repair_body(
    store: Arc<LocalStore>, vault: Option<&ClassicKeyVault>, region: &Arc<IoRegion>,
) -> Result<Arc<dyn ObjectStore>> {
    match vault {
        Some(vault) => {
            vault.require_separate_path(store.root())?;
            let keys = vault.units(store.as_ref(), Arc::clone(region), false).await?;
            Ok(Arc::new(ClassicKeyVault::wrap(store, keys)?))
        }
        None => {
            ClassicKeyVault::require_unenrolled(store.as_ref()).await?;
            Ok(store)
        }
    }
}

fn open_repair_logs(
    journal: &Path, region: &Arc<IoRegion>, epoch: StorageEpoch, store: Arc<dyn ObjectStore>,
    cache: Arc<BufferPool>, memory: &Arc<MemoryBudget>, vault: Option<&ClassicKeyVault>,
) -> Result<Arc<PartitionLogs>> {
    let options = ReadOnlyLocalLogOptions::default();
    let source = Arc::new(MemoryLog::new(options.maximum_message_bytes));
    let replica = match vault {
        Some(vault) => {
            let keys = vault.log_keys(journal, false, Arc::clone(region))?;
            ReadOnlyLocalLog::open_with_key_slots(journal, options, Arc::clone(region), keys)?
        }
        None => ReadOnlyLocalLog::open_in(journal, options, Arc::clone(region))?,
    };
    PartitionLogs::new(source, replica, epoch, epoch, CURRENT_STORAGE_VERSION, StorageEpoch(0), store, cache, memory.clone())
}

fn next_replica_byte(position: ReplicaPosition) -> Result<ReplicaPosition> {
    repair_decision::exclusive_replay_end(position.0).map(ReplicaPosition).ok_or_else(|| invalid("Replica interval overflows"))
}

fn table_manifest_reservation(encoded: usize) -> Result<usize> {
    encoded.checked_mul(4).ok_or_else(|| invalid("Table block exceeds addressable memory"))
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
