use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use rtdb_columnar::MemoryReservation;
use rtdb_format::{catalog::CheckpointContents, keys::{CURRENT_STORAGE_VERSION, StorageRoot, block_key, lex_hex}};
use rtdb_index::{CatalogTrie, PartitionValue};
use rtdb_maintenance::ResetRetirements;
use rtdb_replication::PrimaryReset;
use rtdb_storage::{HeldView, IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Result, StorageEpoch, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{Node, database::PublishDeletions};

pub struct CompactorReset {
    database: Arc<crate::Database>,
    term: rtdb_replication::TermHandle,
    expected: Arc<PartitionValue>,
    primary: PrimaryReset,
    preview: BTreeMap<TableRef, Vec<CatalogTrie>>,
    files: Vec<ObjectKey>,
    reservations: Arc<[Arc<MemoryReservation>]>,
}
impl CompactorReset {
    pub fn files_to_delete(&self) -> &[ObjectKey] { &self.files }
    pub fn replacement_tries(&self) -> &BTreeMap<TableRef, Vec<CatalogTrie>> {
        &self.preview
    }

    /// Publishes the replacement primary catalog in a new block and submits the superseded files for retirement.
    /// Every competing writer and maintenance worker must remain stopped until this finishes.
    /// A coordinated store can defer physical retirement while readers hold the previous catalog.
    /// An uncoordinated store requires exclusive access for both readers and writers.
    pub async fn apply(self, cancellation: &CancellationToken) -> Result<BlockIndex> {
        if cancellation.is_cancelled() { return Err(reset_cancelled()); }
        self.database.require_writable()?;
        let store = self.database.storage.object_store();
        let retirements = if store.defers_trie_releases() {
            Some(ResetRetirements::stage(store.as_ref(), &self.expected, cancellation).await?)
        } else { None };
        let retained = self.reservations;
        let expected = Arc::clone(self.expected.trie_catalog());
        let open_block = self.expected.live().block_index();
        let publish = async {
            let published = self.primary.publish_files(expected, open_block, cancellation).await?;
            self.term.reset_compactor(published, Arc::clone(&retained)).await
        };
        let block = cancellation.run_until_cancelled(publish).await.ok_or_else(reset_cancelled)??;
        if let Some(retirements) = retirements {
            retirements.publish(store.as_ref(), &PublishDeletions(self.term), &self.database.partition.value(), block, cancellation).await?;
            return Ok(block);
        }
        for key in self.files {
            if cancellation.is_cancelled() { return Err(reset_cancelled()); }
            match store.delete(&key).await {
                Ok(()) | Err(rtdb_storage::Error::MissingObject(_)) => {},
                Err(error) => return Err(error.into()),
            }
        }
        drop(retained);
        Ok(block)
    }
}

fn reset_inventory(reason: impl Into<String>) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.compactor-reset/incomplete-inventory", reason)
}

fn reset_cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "rtdb.compactor-reset/cancelled", "Compactor reset was cancelled") }

pub struct SnapshotExport {
    store: Arc<dyn ObjectStore>,
    hold: Arc<HeldView>,
    block_index: BlockIndex,
    export_time: Timestamp,
    directory: ObjectKey,
    storage_root: ObjectKey,
    table_count: usize,
    files: Vec<ObjectKey>,
    _reservation: Arc<MemoryReservation>,
}
impl SnapshotExport {
    pub fn block_index(&self) -> BlockIndex { self.block_index }
    pub fn export_time(&self) -> Timestamp { self.export_time }
    pub fn directory(&self) -> &ObjectKey { &self.directory }
    pub fn table_count(&self) -> usize { self.table_count }
    pub fn files(&self) -> &[ObjectKey] { &self.files }
    pub fn destination(&self, source: &ObjectKey) -> Result<ObjectKey> { ObjectKey::new(format!("{}/{source}", self.storage_root)).map_err(Into::into) }

    /// Copies the captured files into an epoch-zero, single-partition store below `directory`.
    /// Completed copies remain on failure and identical retries are accepted.
    /// Cancellation is checked between copies; an admitted object copy finishes before returning.
    pub async fn copy(&self, cancellation: &CancellationToken) -> Result<()> {
        IoRegion::reading(Arc::clone(&self.hold), async {
            // The root makes the destination visible to checkpoint discovery.
            for key in self.files.iter().skip(1).chain(self.files.first()) {
                if cancellation.is_cancelled() { return Err(Anomaly::new(AnomalyCategory::Interrupted, "rtdb.export/cancelled", "Snapshot export was cancelled")); }
                self.store.copy(key, &self.destination(key)?).await?;
            }
            Ok(())
        }).await
    }
}

impl Node {
    /// Flushes pending input and prepares a reset from the adopted checkpoint's primary inventory.
    /// Indexing must be enabled, with compaction and automatic GC disabled; all other users of the database must be stopped.
    /// A plan alone performs no reset or deletion; its initial catch-up flush is durable even for a dry run.
    pub async fn prepare_compactor_reset(&self, database: &str, cancellation: &CancellationToken) -> Result<CompactorReset> {
        self.prepare_reset(database, None, cancellation).await
    }

    /// Rebuilds the catch-up checkpoint from a detached retained export and the maintenance node's durable local journal.
    /// Preparation reads from the detached export and stages only temporary replacement objects.
    /// The initial catch-up flush is durable even for a dry run.
    pub async fn prepare_compactor_reset_from_retained_base(&self, database: &str, export: &Path,
        cancellation: &CancellationToken,
    ) -> Result<CompactorReset> {
        self.prepare_reset(database, Some(export), cancellation).await
    }

    async fn prepare_reset(&self, database: &str, export: Option<&Path>, cancellation: &CancellationToken) -> Result<CompactorReset> {
        self.require_compactor_reset()?;
        let database = self.catalog().database(database)?;
        if database.object_store().local_erasure_custody().is_some() {
            return Err(Anomaly::unsupported("rtdb.compactor-reset/custody",
                "The enrolled Local erasure residence retains its original publication authority"));
        }
        database.require_writable()?;
        let source = cancellation.run_until_cancelled(database.flush()).await.ok_or_else(reset_cancelled)??;
        tokio::time::timeout(Duration::from_secs(300), database.replication.await_source(source, cancellation)).await
            .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.compactor-reset/catch-up-timeout", "Database did not catch up within five minutes"))??;
        let value = database.partition.value();
        let term = database.replication.term().ok_or_else(|| Anomaly::new(AnomalyCategory::Conflict, "rtdb.compactor-reset/not-leader", "Compactor reset requires exclusive leadership"))?;
        let expected = value.trie_catalog();
        let mut bound = 8192usize;
        for table in expected.tables() {
            let entry = value.table_catalog().entry(table).ok_or_else(|| invalid("Trie table has no registered slug"))?;
            for trie in expected.iter_tries(table).filter(|trie| trie.key.level() > 0) {
                bound = bound.checked_add(entry.slug.as_str().len().checked_add(trie.key.part().len()).and_then(|size| size.checked_add(512)).and_then(|size| size.checked_mul(4)).ok_or_else(overflow)?).ok_or_else(overflow)?;
            }
        }
        let mut reservations = vec![self.memory().reserve(bound)?];
        let mut files = Vec::new();
        for table in expected.tables() {
            if cancellation.is_cancelled() { return Err(reset_cancelled()); }
            let entry = value.table_catalog().entry(table).ok_or_else(|| invalid("Trie table has no registered slug"))?;
            for trie in expected.all_tries(table).into_iter().filter(|trie| trie.key.level() > 0) {
                files.push(ObjectKey::new(entry.slug.metadata_key(&trie.key))?);
                files.push(ObjectKey::new(entry.slug.data_key(&trie.key))?);
            }
        }
        if value.live().latest_completed_tx().is_none() {
            return Err(Anomaly::incorrect("rtdb.compactor-reset/no-transactions", "Compactor reset requires a completed transaction"));
        }
        let installed = value.table_catalog().block().ok_or_else(|| reset_inventory("Reset requires an adopted checkpoint"))?;
        let primary = match export {
            Some(export) => PrimaryReset::from_retained(Arc::clone(&database.logs), installed,
                export, expected.file_size_target(), cancellation).await?,
            None => PrimaryReset::from_installed(Arc::clone(&database.logs), installed,
                value.snapshot().catalog_cut(), expected.file_size_target(), cancellation).await?,
        };
        let mut reserve = 4096usize;
        for catalog in [&**expected, primary.catalog()] {
            for table in catalog.tables() {
                for trie in catalog.iter_tries(table) {
                    let size = (trie.key.part().len() + 1024).checked_mul(8).ok_or_else(overflow)?;
                    reserve = reserve.checked_add(size).ok_or_else(overflow)?;
                }
            }
        }
        reservations.push(self.memory().reserve(reserve)?);
        let preview = primary.catalog().tables().map(|table| {
            (table.clone(), primary.catalog().live_and_nascent_tries(table).into_iter().cloned().collect())
        }).collect();
        Ok(CompactorReset { database, term, expected: value, primary, preview, files, reservations: reservations.into() })
    }
    /// Plans an offline export from the loaded block's immutable dependency closure.
    /// Indexing and compaction must be disabled on this node. The plan retains its source through provider drain on coordinated stores.
    /// Returns None when the database has no completed block.
    pub async fn snapshot_export(&self, database: &str, now: Timestamp) -> Result<Option<SnapshotExport>> {
        self.require_offline_export()?;
        let database = self.catalog().database(database)?;
        if database.object_store().local_classic_key_slots().is_some()
            || database.native_serving_authority.as_ref().is_some_and(|authority| authority.checkpoints.is_some())
        {
            return Err(Anomaly::unsupported("rtdb.export/custody-binding",
                "Snapshot export requires an independently admitted destination for sealed state"));
        }
        let value = database.partition.value();
        let Some(captured) = value.table_catalog().block() else { return Ok(None); };
        let block_index = captured.block_index();
        let expected = captured.publication_sha256();
        let store = Arc::clone(database.storage.object_store());
        let cut = value.snapshot().catalog_cut();
        let hold = HeldView::catalog(store.as_ref(), Arc::clone(&value) as Arc<dyn Send + Sync>, cut).await?;
        let blocks = rtdb_replication::BlockStore::new(
            Arc::clone(&store),
            Arc::clone(&database.storage),
            Arc::clone(self.memory()),
            value.trie_catalog().file_size_target(),
        )?;
        let (block, tables) = IoRegion::reading(Arc::clone(&hold), async {
            let (block, tables) = blocks.load(block_index).await?;
            if expected.is_some_and(|digest| block.publication_sha256() != Some(digest)) {
                return Err(invalid("Loaded checkpoint root changed while planning the export"));
            }
            blocks.verify_primary_inventory(&block, &tables).await?;
            Ok::<_, Anomaly>((block, tables))
        }).await?;
        let date = rtdb_types::CivilDateTime::from_epoch_second(now.epoch_parts().0, 0)?;
        let (year, month, day) = date.date().ymd();
        let year_of_era = if year <= 0 { 1 - year } else { year };
        let year = if year_of_era > 9999 { format!("+{year_of_era}") } else { format!("{year_of_era:04}") };
        let second = date.second_of_day();
        let date = format!("{year}{month:02}{day:02}T{:02}{:02}{:02}Z", second / 3600, second / 60 % 60, second % 60);
        let directory = ObjectKey::new(format!("exports/b{}_{}", lex_hex(u64::try_from(block_index.0).map_err(invalid)?), date))?;
        let root = ObjectKey::new(format!("{directory}/{}", StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key()))?;
        let entries = block.table_entries()?;
        let mut bound = 4096usize;
        for entry in &entries {
            let slug_bytes = entry.slug.as_str().len();
            bound = bound.checked_add(slug_bytes.checked_mul(4).and_then(|size| size.checked_add(1024)).ok_or_else(overflow)?).ok_or_else(overflow)?;
            let table = tables.get(&entry.table).ok_or_else(|| invalid("Checkpoint table manifest is unavailable"))?;
            for trie in table.trie_details() {
                let bytes = trie.trie_key.as_ref().map_or(0, String::len).checked_add(slug_bytes).ok_or_else(overflow)?;
                bound = bound.checked_add(bytes.checked_mul(8).and_then(|size| size.checked_add(1024)).ok_or_else(overflow)?).ok_or_else(overflow)?;
            }
        }
        let reservation = self.memory().reserve(bound)?;
        let table_count = entries.len();
        let mut files = vec![ObjectKey::new(block_key(block_index)?)?];
        for entry in entries {
            let table = tables.get(&entry.table).ok_or_else(|| invalid("Checkpoint table manifest is unavailable"))?;
            files.push(ObjectKey::new(entry.slug.block_key(block_index)?)?);
            for file in table.checkpoint_trie_files(&entry, CheckpointContents::Primary)? {
                files.push(ObjectKey::new(file)?);
            }
        }
        Ok(Some(SnapshotExport { store, hold, block_index, export_time: now, directory, storage_root: root, table_count, files, _reservation: reservation }))
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.export/invalid-state", error.to_string()) }
fn overflow() -> Anomaly { invalid("Snapshot file manifest exceeds addressable memory") }
