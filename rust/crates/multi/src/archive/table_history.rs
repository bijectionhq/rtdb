use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use rtdb_directory::{Checkpoint, DeploymentRecord, Directory, HostedTask};
use rtdb_format::{
    catalog::{BlockCatalog, TableBlock, TableEntry, publication_digest},
    log::proto::EntityDirectoryRoot,
};
use rtdb_maintenance::{
    HistoryCoverage, HistoryRead, HistoryRow, HistoryVisitor, TableHistorySource,
};
use rtdb_query::entity_directory::{DirectoryRead, table_events};
use rtdb_storage::{IoRegion, ObjectStore};
use rtdb_types::{
    Anomaly, AnomalyCategory, BlockIndex, EventHeader, EventOp, EventOrigin, MemoryBudget, Result,
    StorageEpoch, TableRef,
};

use crate::{keys::KeyService, store::CellStoreConfig};

use super::{
    replica_suffix::{read_published_catalog, visit_table},
    stage::with_region,
};

pub(crate) struct HostedTableReplay {
    directory: Arc<dyn Directory>,
    keys: Arc<dyn KeyService>,
    hosted_task: Option<HostedTask>,
    config: CellStoreConfig,
}

impl HostedTableReplay {
    pub(crate) fn new(
        directory: Arc<dyn Directory>,
        keys: Arc<dyn KeyService>,
        hosted_task: Option<HostedTask>,
        config: CellStoreConfig,
    ) -> Self {
        Self {
            directory,
            keys,
            hosted_task,
            config,
        }
    }

    fn checkpoint(&self, record: &DeploymentRecord, target: &BlockCatalog) -> Option<Checkpoint> {
        let checkpoint = record.checkpoint?;
        let offset = target
            .boundary_replica_position()?
            .after_offset(StorageEpoch(record.log_epoch));
        (Some(record.epoch) == self.config.writer_epoch
            && record.storage_prefix() == self.config.prefix
            && checkpoint.block <= target.block_index().0
            && checkpoint.boundary <= offset
            && checkpoint.root_sha256.is_some()
            && record.recovery_manifest_sha256.is_some())
        .then_some(checkpoint)
    }

    async fn replay(
        &self,
        record: &DeploymentRecord,
        checkpoint: Checkpoint,
        target: &BlockCatalog,
        read: HistoryRead<'_>,
        visit: &mut HistoryVisitor<'_>,
        io: Arc<IoRegion>,
    ) -> Result<Option<HistoryCoverage>> {
        let store = read.storage.object_store();
        let base = read_published_catalog(
            store.as_ref(),
            checkpoint,
            StorageEpoch(record.log_epoch),
            read.memory,
        )
        .await?;
        let Some(base_rows) =
            checkpoint_rows(store.as_ref(), &base, read.table, read.memory).await?
        else {
            return Ok(None);
        };
        let Some(directory) = read.snapshot.trie_catalog().entity_directory(read.table) else {
            return Ok(None);
        };
        let Some(through) = target.boundary_replica_position() else {
            return Ok(None);
        };
        let mut admitted = BTreeMap::<EventOrigin, EventHeader>::new();
        let mut charge = read.memory.reserve(4096)?;
        let mut admit = |origin: EventOrigin, header: EventHeader| -> Result<()> {
            let charge = Arc::get_mut(&mut charge).expect("The history reader owns its scratch");
            charge.grow_to(charge.bytes().saturating_add(128))?;
            if admitted.insert(origin, header).is_some() {
                return Err(unavailable());
            }
            Ok(())
        };
        if base_rows != 0 && base.boundary_replica_position() != Some(through) {
            let snapshot = open_base_snapshot(&read, &base).await?;
            let base_read = DirectoryRead {
                snapshot: &snapshot,
                storage: read.storage,
                memory: read.memory,
                cancellation: read.cancellation,
            };
            table_events(&base_read, read.table, |event| {
                admit(event.origin, event.header)
            })
            .await?;
        }
        let mut suffix_rows = 0i64;
        let mut suffix_blocks = BTreeMap::<BlockIndex, i64>::new();
        visit_table(
            self.directory.as_ref(),
            record,
            &self.config.storage,
            self.keys.as_ref(),
            checkpoint,
            &base,
            target,
            read.table,
            io,
            read.memory,
            read.cancellation,
            |block, batch| {
                let rows = i64::try_from(batch.len()).map_err(|_| unavailable())?;
                if rows == 0 {
                    return Ok(());
                }
                suffix_rows = suffix_rows.checked_add(rows).ok_or_else(unavailable)?;
                let count = suffix_blocks.entry(block).or_default();
                *count = count.checked_add(rows).ok_or_else(unavailable)?;
                for row in 0..batch.len() {
                    let origin = batch
                        .origin(row)?
                        .filter(|origin| origin.source_block() == block)
                        .ok_or_else(unavailable)?;
                    admit(origin, batch.header(row)?)?;
                }
                Ok(())
            },
        )
        .await?;
        let durable = read
            .snapshot
            .table_catalog()
            .metadata(read.table)
            .map_or(0, |meta| meta.row_count);
        if base_rows.checked_add(suffix_rows) != Some(durable)
            || !complete_directory(directory, target.block_index(), durable)
        {
            return Ok(None);
        }
        let entry = read
            .snapshot
            .table_entry(read.table)
            .ok_or_else(unavailable)?;
        let directory_read = DirectoryRead {
            snapshot: read.snapshot,
            storage: read.storage,
            memory: read.memory,
            cancellation: read.cancellation,
        };
        let mut payloads = BTreeMap::new();
        let reconstructing = base.boundary_replica_position() != Some(through);
        let mut retained_suffix = BTreeMap::<BlockIndex, i64>::new();
        table_events(&directory_read, read.table, |event| {
            let block = event.origin.source_block();
            if block > BlockIndex(directory.through_block()) || block.0 < 0 {
                return Err(unavailable());
            }
            if reconstructing && admitted.remove(&event.origin) != Some(event.header) {
                return Err(unavailable());
            }
            if block > base.block_index() {
                let rows = retained_suffix.entry(block).or_default();
                *rows = rows.checked_add(1).ok_or_else(unavailable)?;
                if *rows > suffix_blocks.get(&block).copied().unwrap_or(0) {
                    return Err(unavailable());
                }
            }
            if event.header.op == EventOp::Put && read.blocks.binary_search(&block.0).is_ok() {
                let charge =
                    Arc::get_mut(&mut charge).expect("The history reader owns its scratch");
                charge.grow_to(
                    charge
                        .bytes()
                        .saturating_add(256 + event.entity_key.as_ref().map_or(0, Vec::len)),
                )?;
                if payloads
                    .insert(event.origin, (event.header, event.entity_key))
                    .is_some()
                {
                    return Err(unavailable());
                }
            } else {
                visit(
                    block,
                    HistoryRow::Header {
                        header: event.header,
                        origin: event.origin,
                    },
                )?;
            }
            Ok(())
        })
        .await?;
        if !directory.has_erases() && (suffix_blocks != retained_suffix || !admitted.is_empty()) {
            return Err(unavailable());
        }
        for trie in read.snapshot.trie_catalog().current_tries(read.table) {
            if payloads.is_empty() {
                break;
            }
            rtdb_query::visit_trie_events(
                read.storage,
                read.memory,
                read.cancellation,
                &entry.slug,
                &trie.key,
                |batch| {
                    for row in 0..batch.len() {
                        let Some(origin) = batch.origin(row)? else {
                            continue;
                        };
                        let Some((header, entity_key)) = payloads.remove(&origin) else {
                            continue;
                        };
                        if batch.header(row)? != header
                            || !matches_entity_key(batch, row, entity_key.as_deref())?
                        {
                            return Err(unavailable());
                        }
                        visit(origin.source_block(), HistoryRow::Stored { batch, row })?;
                    }
                    Ok(())
                },
            )
            .await?;
        }
        if !payloads.is_empty() {
            return Err(unavailable());
        }
        Ok(Some(HistoryCoverage::Semantic {
            lineage: format!("hosted/{}", record.lineage),
            checkpoint_boundary: base.boundary_replica_position().ok_or_else(unavailable)?,
            through,
            checkpoint_catalog_sha256: base.publication_sha256().ok_or_else(unavailable)?,
            directory: directory.clone(),
            suffix_rows,
        }))
    }
}

fn matches_entity_key(
    batch: &rtdb_format::data::EventBatch,
    row: usize,
    expected: Option<&[u8]>,
) -> Result<bool> {
    let Some(expected) = expected else {
        return Ok(true);
    };
    if let Some(recorded) = batch.entity_key(row)? {
        return Ok(recorded == expected);
    }
    Ok(batch
        .borrow_event(row)?
        .put_identity()?
        .is_some_and(|id| id.into_owned().handle_bytes() == expected))
}

async fn open_base_snapshot(
    read: &HistoryRead<'_>,
    base: &BlockCatalog,
) -> Result<rtdb_index::Snapshot> {
    let store = rtdb_replication::BlockStore::new(
        Arc::clone(read.storage.object_store()),
        Arc::clone(read.storage),
        Arc::clone(read.memory),
        read.snapshot.trie_catalog().file_size_target(),
    )?;
    let (catalog, tables) = store.load(base.block_index()).await?;
    if catalog.publication_sha256() != base.publication_sha256() {
        return Err(unavailable());
    }
    let tries = rtdb_index::TrieCatalog::load(
        &tables,
        read.snapshot.trie_catalog().file_size_target(),
        rtdb_types::Timestamp(0),
    )?;
    let tables = rtdb_index::TableCatalog::load(Some(catalog), &tables)?;
    Ok(
        rtdb_index::PartitionValue::loaded(tries, tables, rtdb_trie::TrieConfig::default())?
            .snapshot(),
    )
}

#[async_trait]
impl TableHistorySource for HostedTableReplay {
    async fn visit_table(
        &self,
        read: HistoryRead<'_>,
        visit: &mut HistoryVisitor<'_>,
    ) -> Result<Option<HistoryCoverage>> {
        let Some(target) = read.snapshot.table_catalog().block() else {
            return Ok(None);
        };
        let Some(record) = self.directory.deployment(&self.config.deployment).await? else {
            return Ok(None);
        };
        let Some(checkpoint) = self.checkpoint(&record, target) else {
            return Ok(None);
        };
        let store = read.storage.object_store();
        let hold = store.retain_catalog(read.snapshot.catalog_cut()).await?;
        let replay = with_region(
            self.directory.as_ref(),
            self.keys.as_ref(),
            &record.id,
            &self.config.storage,
            self.hosted_task.as_ref(),
            None,
            None,
            |current| {
                if self.checkpoint(current, target) != Some(checkpoint)
                    || current.incarnation != record.incarnation
                    || current.log_epoch != record.log_epoch
                {
                    return Err(unavailable());
                }
                let through = target
                    .boundary_replica_position()
                    .ok_or_else(unavailable)?
                    .after_offset(StorageEpoch(current.log_epoch));
                if through == checkpoint.boundary {
                    return Ok(None);
                }
                Ok(Some(crate::hosted_recovery::replay_start(
                    current,
                    checkpoint.boundary,
                )?))
            },
            |record, _, io| async move {
                self.replay(&record, checkpoint, target, read, visit, io)
                    .await
            },
        );
        match hold {
            Some(hold) => IoRegion::retaining(hold, replay).await,
            None => replay.await,
        }
    }
}

pub(crate) fn complete_directory(
    directory: &EntityDirectoryRoot,
    block: BlockIndex,
    rows: i64,
) -> bool {
    directory
        .through_block
        .is_some_and(|through| (0..=block.0).contains(&through))
        && directory.source_rows == Some(rows)
        && rows > 0
        && directory.erase_frontier_digest().len() == 32
        && rtdb_format::cow_tree::RootRef::decode(directory.root()).is_ok()
}

async fn checkpoint_rows(
    store: &dyn ObjectStore,
    catalog: &BlockCatalog,
    table: &TableRef,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<i64>> {
    let Some(entry) = catalog
        .table_entries()?
        .into_iter()
        .find(|entry| &entry.table == table)
    else {
        return Ok(Some(0));
    };
    let metadata = read_table_block(store, catalog, &entry, memory).await?;
    let rows = metadata.row_count();
    Ok((rows == 0
        || metadata
            .as_proto()
            .entity_directory
            .as_ref()
            .is_some_and(|root| complete_directory(root, catalog.block_index(), rows)))
    .then_some(rows))
}

pub(crate) async fn read_table_block(
    store: &dyn ObjectStore,
    catalog: &BlockCatalog,
    entry: &TableEntry,
    memory: &Arc<MemoryBudget>,
) -> Result<TableBlock> {
    let path = super::key(&entry.slug.block_key(catalog.block_index())?)?;
    let size = usize::try_from(store.head_fresh(&path).await?.size).map_err(|_| unavailable())?;
    let charge = memory.reserve(size.saturating_mul(8).saturating_add(4096))?;
    let bytes = IoRegion::retaining(Arc::clone(&charge) as Arc<dyn Send + Sync>, store.observe_fresh_exact(&path, size))
        .await?
        .value;
    if catalog.table_block_digest(entry.oid) != Some(publication_digest(&bytes).as_slice()) {
        return Err(unavailable());
    }
    TableBlock::decode_reserved(&bytes, charge)
}

fn unavailable() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/search-history-unavailable",
        "The hosted table history does not verify at the requested cut",
    )
}
