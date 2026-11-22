use std::{collections::BTreeMap, sync::Arc, time::Duration};
use rtdb_columnar::MemoryReservation;
use rtdb_format::{catalog::CheckpointContents, keys::{CURRENT_STORAGE_VERSION, StorageRoot, TrieKey, block_key, lex_hex}};
use rtdb_index::{CatalogTrie, TrieCatalog};
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Result, StorageEpoch, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;
use crate::Node;

pub struct CompactorReset {
    database: Arc<crate::Database>,
    term: rtdb_replication::TermHandle,
    expected: Arc<TrieCatalog>,
    expected_block: BlockIndex,
    replacements: BTreeMap<TableRef, Vec<CatalogTrie>>,
    files: Vec<ObjectKey>,
    reservations: Arc<[Arc<MemoryReservation>]>,
}
impl CompactorReset {
    pub fn files_to_delete(&self) -> &[ObjectKey] { &self.files }
    pub fn replacement_tries(&self) -> &BTreeMap<TableRef, Vec<CatalogTrie>> { &self.replacements }

    /// Publishes the captured L0 catalog in a new block, then removes compacted metadata before data.
    /// All other nodes, clients and upstream writers must remain stopped until this finishes.
    /// Cancellation before publication leaves every compacted file; cancellation afterwards leaves a usable L0 catalog.
    pub async fn apply(self, cancellation: &CancellationToken) -> Result<BlockIndex> {
        if cancellation.is_cancelled() { return Err(reset_cancelled()); }
        self.database.require_writable()?;
        let retained = self.reservations;
        let block = cancellation.run_until_cancelled(self.term.reset_compactor(self.expected, self.expected_block, self.replacements, Arc::clone(&retained))).await.ok_or_else(reset_cancelled)??;
        for key in self.files {
            if cancellation.is_cancelled() { return Err(reset_cancelled()); }
            match self.database.storage.object_store().delete(&key).await {
                Ok(()) | Err(rtdb_storage::Error::MissingObject(_)) => {},
                Err(error) => return Err(error.into()),
            }
        }
        drop(retained);
        Ok(block)
    }
}

fn reset_cancelled() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "rtdb.compactor-reset/cancelled", "Compactor reset was cancelled") }

pub struct SnapshotExport {
    store: Arc<dyn ObjectStore>,
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
        // The root makes the destination visible to checkpoint discovery.
        for key in self.files.iter().skip(1).chain(self.files.first()) {
            if cancellation.is_cancelled() { return Err(Anomaly::new(AnomalyCategory::Interrupted, "rtdb.export/cancelled", "Snapshot export was cancelled")); }
            self.store.copy(key, &self.destination(key)?).await?;
        }
        Ok(())
    }
}

impl Node {
    /// Flushes pending input and captures surviving L0 files and compacted files for an exclusive maintenance reset.
    /// Indexing must be enabled, with compaction and automatic GC disabled; all other users of the database must be stopped.
    /// A plan alone performs no reset or deletion; its initial catch-up flush is durable even for a dry run.
    pub async fn prepare_compactor_reset(&self, database: &str, cancellation: &CancellationToken) -> Result<CompactorReset> {
        self.require_compactor_reset()?;
        let database = self.catalog().database(database)?;
        database.require_writable()?;
        let source = cancellation.run_until_cancelled(database.flush()).await.ok_or_else(reset_cancelled)??;
        tokio::time::timeout(Duration::from_secs(300), database.replication.await_source(source, cancellation)).await
            .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.compactor-reset/catch-up-timeout", "Database did not catch up within five minutes"))??;
        let value = database.partition.value();
        let term = database.replication.term().ok_or_else(|| Anomaly::new(AnomalyCategory::Conflict, "rtdb.compactor-reset/not-leader", "Compactor reset requires exclusive leadership"))?;
        let expected = Arc::clone(value.trie_catalog());
        let mut bound = 8192usize;
        for table in expected.tables() {
            let entry = value.table_catalog().entry(table).ok_or_else(|| invalid("Trie table has no registered slug"))?;
            for trie in expected.iter_tries(table).filter(|trie| trie.key.level() > 0) {
                bound = bound.checked_add(entry.slug.as_str().len().checked_add(trie.key.part().len()).and_then(|size| size.checked_add(512)).and_then(|size| size.checked_mul(4)).ok_or_else(overflow)?).ok_or_else(overflow)?;
            }
        }
        let mut reservations = vec![self.memory().reserve(bound)?];
        let mut files = Vec::new();
        let mut replacements = BTreeMap::new();
        for table in expected.tables() {
            if cancellation.is_cancelled() { return Err(reset_cancelled()); }
            let entry = value.table_catalog().entry(table).ok_or_else(|| invalid("Trie table has no registered slug"))?;
            for trie in expected.all_tries(table).into_iter().filter(|trie| trie.key.level() > 0) {
                files.push(ObjectKey::new(entry.slug.metadata_key(&trie.key))?);
                files.push(ObjectKey::new(entry.slug.data_key(&trie.key))?);
            }
            let prefix = ObjectKey::new(format!("{}/meta", entry.slug.table_prefix()))?;
            let listed = cancellation.run_until_cancelled(database.storage.object_store().list(Some(&prefix))).await.ok_or_else(reset_cancelled)??;
            let bound = listed.iter().try_fold(4096usize, |bound, object| bound.checked_add(object.key.as_str().len().checked_mul(8)?.checked_add(2048)?)).ok_or_else(overflow)?;
            reservations.push(self.memory().reserve(bound)?);
            let mut tries = Vec::new();
            for metadata in listed {
                if cancellation.is_cancelled() { return Err(reset_cancelled()); }
                let Some(name) = metadata.key.as_str().rsplit('/').next().and_then(|name| name.strip_suffix(".arrow")) else { continue; };
                let Ok(key) = name.parse::<TrieKey>() else { continue; };
                if key.level() != 0 { continue; }
                let data = ObjectKey::new(entry.slug.data_key(&key))?;
                match cancellation.run_until_cancelled(database.storage.object_store().head(&data)).await.ok_or_else(reset_cancelled)? {
                    Ok(data) => tries.push(CatalogTrie::notification(key, i64::try_from(data.size).map_err(invalid)?, None)?),
                    Err(rtdb_storage::Error::MissingObject(_)) => {},
                    Err(error) => return Err(error.into()),
                }
            }
            tries.sort_by(|left, right| left.key.cmp(&right.key));
            replacements.insert(table.clone(), tries);
        }
        Ok(CompactorReset { database, term, expected, expected_block: value.live().block_index(), replacements, files, reservations: reservations.into() })
    }
    /// Plans an offline export from the loaded block's immutable dependency closure.
    /// Indexing and compaction must be disabled on this node; source files must remain available until copying finishes.
    /// Returns None when the database has no completed block.
    pub async fn snapshot_export(&self, database: &str, now: Timestamp) -> Result<Option<SnapshotExport>> {
        self.require_offline_export()?;
        let database = self.catalog().database(database)?;
        let value = database.partition.value();
        let Some(captured) = value.table_catalog().block() else { return Ok(None); };
        let block_index = captured.block_index();
        let expected = captured.publication_sha256();
        let blocks = rtdb_replication::BlockStore::new(
            Arc::clone(database.storage.object_store()),
            Arc::clone(&database.storage),
            Arc::clone(self.memory()),
            value.trie_catalog().file_size_target(),
        )?;
        let (block, tables) = blocks.load(block_index).await?;
        if expected.is_some_and(|digest| block.publication_sha256() != Some(digest)) {
            return Err(invalid("Loaded checkpoint root changed while planning the export"));
        }
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
            for file in table.checkpoint_trie_files(&entry.slug, CheckpointContents::Primary)? {
                files.push(ObjectKey::new(file)?);
            }
        }
        Ok(Some(SnapshotExport { store: Arc::clone(database.storage.object_store()), block_index, export_time: now, directory, storage_root: root, table_count, files, _reservation: reservation }))
    }
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.export/invalid-state", error.to_string()) }
fn overflow() -> Anomaly { invalid("Snapshot file manifest exceeds addressable memory") }
