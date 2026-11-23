use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use rtdb_columnar::MemoryReservation;
use rtdb_format::{catalog::CheckpointContents, keys::{CURRENT_STORAGE_VERSION, StorageRoot, TrieKey, block_key, lex_hex}};
use rtdb_index::{CatalogTrie, PartitionValue};
use rtdb_maintenance::ResetRetirements;
use rtdb_replication::ReplayedPrimary;
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Result, StorageEpoch, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;
use crate::{Node, database::PublishDeletions};

enum ResetPrimary {
    L0(BTreeMap<TableRef, Vec<CatalogTrie>>),
    Replayed { recovery: ReplayedPrimary, preview: BTreeMap<TableRef, Vec<CatalogTrie>> },
}

pub struct CompactorReset {
    database: Arc<crate::Database>,
    term: rtdb_replication::TermHandle,
    expected: Arc<PartitionValue>,
    primary: ResetPrimary,
    files: Vec<ObjectKey>,
    reservations: Arc<[Arc<MemoryReservation>]>,
}
impl CompactorReset {
    pub fn files_to_delete(&self) -> &[ObjectKey] { &self.files }
    pub fn replacement_tries(&self) -> &BTreeMap<TableRef, Vec<CatalogTrie>> {
        match &self.primary { ResetPrimary::L0(tries) | ResetPrimary::Replayed { preview: tries, .. } => tries }
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
            match self.primary {
                ResetPrimary::L0(tries) => self.term.reset_compactor(expected, open_block, tries, Arc::clone(&retained)).await,
                ResetPrimary::Replayed { recovery, .. } => {
                    let published = recovery.publish_files(expected, open_block, cancellation).await?;
                    self.term.reset_replayed_compactor(published, Arc::clone(&retained)).await
                }
            }
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
    /// Flushes pending input and verifies the installed checkpoint's L0 inventory for an exclusive maintenance reset.
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
        database.require_writable()?;
        let source = cancellation.run_until_cancelled(database.flush()).await.ok_or_else(reset_cancelled)??;
        tokio::time::timeout(Duration::from_secs(300), database.replication.await_source(source, cancellation)).await
            .map_err(|_| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.compactor-reset/catch-up-timeout", "Database did not catch up within five minutes"))??;
        let value = database.partition.value();
        let term = database.replication.term().ok_or_else(|| Anomaly::new(AnomalyCategory::Conflict, "rtdb.compactor-reset/not-leader", "Compactor reset requires exclusive leadership"))?;
        let expected = Arc::clone(value.trie_catalog());
        let store = database.storage.object_store();
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
        if let Some(export) = export {
            let installed = value.table_catalog().block().ok_or_else(|| reset_inventory("Retained reset requires an adopted checkpoint"))?;
            let recovery = ReplayedPrimary::prepare(Arc::clone(&database.logs), installed, export, expected.file_size_target(), cancellation).await?;
            let replay = recovery.catalog();
            let mut reserve = 4096usize;
            for catalog in [&*expected, replay] {
                for table in catalog.tables() {
                    for trie in catalog.iter_tries(table) {
                        let size = (trie.key.part().len() + 1024).checked_mul(8).ok_or_else(overflow)?;
                        reserve = reserve.checked_add(size).ok_or_else(overflow)?;
                    }
                }
            }
            reservations.push(self.memory().reserve(reserve)?);
            let preview = replay.tables().map(|table| {
                (table.clone(), replay.live_and_nascent_tries(table).into_iter().cloned().collect())
            }).collect();
            let primary = ResetPrimary::Replayed { recovery, preview };
            return Ok(CompactorReset { database, term, expected: value, primary, files, reservations: reservations.into() });
        }
        let mut replacements = BTreeMap::new();
        if let Some(installed) = value.table_catalog().block() {
            let blocks = rtdb_replication::BlockStore::new(
                Arc::clone(store), Arc::clone(&database.storage), Arc::clone(self.memory()), expected.file_size_target(),
            )?;
            let (root, tables) = cancellation.run_until_cancelled(blocks.load(installed.block_index())).await.ok_or_else(reset_cancelled)??;
            if installed.publication_sha256().is_none() || root.publication_sha256() != installed.publication_sha256() {
                return Err(reset_inventory("The checkpoint root differs from the installed publication"));
            }
            for entry in root.table_entries()? {
                if root.table_block_digest(entry.oid).is_none() {
                    return Err(reset_inventory("The checkpoint has no bound table inventory"));
                }
                let table = tables.get(&entry.table).ok_or_else(|| reset_inventory("The checkpoint has no table block"))?;
                let bound = table.trie_details().try_fold(4096usize, |bound, detail| {
                    bound.checked_add(detail.trie_key.as_ref()?.len().checked_add(512)?.checked_mul(4)?)
                }).ok_or_else(overflow)?;
                reservations.push(self.memory().reserve(bound)?);
                let mut tries = BTreeMap::new();
                for detail in table.trie_details() {
                    let key: TrieKey = detail.trie_key.as_deref().ok_or_else(|| reset_inventory("The checkpoint has an unnamed trie"))?.parse()?;
                    if key.level() != 0 { continue; }
                    let keys = [ObjectKey::new(entry.slug.data_key(&key))?, ObjectKey::new(entry.slug.metadata_key(&key))?];
                    let heads = cancellation.run_until_cancelled(store.observe_heads(&keys)).await.ok_or_else(reset_cancelled)?
                        .map_err(|error| match error {
                            rtdb_storage::Error::MissingObject(name) => reset_inventory(format!("Required L0 object is missing: {name}")),
                            error => error.into(),
                        })?;
                    let size = heads.get(&keys[0]).map(|head| head.value.size).filter(|size| *size > 0)
                        .ok_or_else(|| reset_inventory(format!("The required L0 data file is empty: {}", keys[0])))?;
                    if heads.get(&keys[1]).is_none_or(|head| head.value.size == 0) {
                        return Err(reset_inventory(format!("The required L0 metadata file is empty: {}", keys[1])));
                    }
                    let size = i64::try_from(size).map_err(|_| overflow())?;
                    if detail.data_file_size.is_some_and(|expected| expected != size) {
                        return Err(reset_inventory(format!("The required L0 data file has the wrong length: {}", keys[0])));
                    }
                    let trie = CatalogTrie::notification(key.clone(), size, None)?;
                    if tries.insert(key, trie).is_some() { return Err(reset_inventory("The checkpoint repeats an L0 trie")); }
                }
                replacements.insert(entry.table, tries.into_values().collect());
            }
        }
        let primary = ResetPrimary::L0(replacements);
        Ok(CompactorReset { database, term, expected: value, primary, files, reservations: reservations.into() })
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
