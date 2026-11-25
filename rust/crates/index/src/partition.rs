use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};
use rtdb_format::{catalog::{BlockCatalog, TableBlock}, keys::TrieKey};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, TableRef, Timestamp, TransactionKey, TxId};

use crate::{CatalogCut, CatalogTrie, LiveIndex, ServingSearch, Snapshot, StagedWrite, TableCatalog, TableMeta, TrieCatalog, TrieState};
use crate::reader_retention::any_reference_requires;

mod read_selection;

#[derive(Clone, Copy, Debug)]
enum CatalogPublication {
    Untracked,
    Installed(CatalogCut),
    Suspended,
}

#[derive(Clone, Debug)]
pub struct PartitionValue {
    trie_catalog: Arc<TrieCatalog>,
    checkpoint_tries: Option<Arc<CheckpointTries>>,
    table_catalog: Arc<TableCatalog>,
    live: Arc<LiveIndex>,
    live_snapshot: Option<Arc<LiveIndex>>,
    catalog_publication: CatalogPublication,
}

impl PartitionValue {
    pub fn loaded(trie_catalog: TrieCatalog, table_catalog: TableCatalog, config: TrieConfig) -> Result<Self> {
        let trie_catalog = trie_catalog.with_naming_epoch(table_catalog.block().map_or(0, BlockCatalog::naming_epoch));
        let block_index = BlockIndex(table_catalog.block_index().map_or(-1, |index| index.0).checked_add(1).ok_or_else(|| invalid("Block index exhausted"))?);
        let latest_completed_tx = table_catalog.block().and_then(BlockCatalog::latest_completed_tx);
        let checkpoint_tries = table_catalog.block_index().map(|_| Arc::new(CheckpointTries::capture(&trie_catalog)));
        Ok(Self { trie_catalog: Arc::new(trie_catalog), checkpoint_tries, table_catalog: Arc::new(table_catalog),
            live: Arc::new(LiveIndex::new(block_index, latest_completed_tx, config)), live_snapshot: None, catalog_publication: CatalogPublication::Untracked })
    }
    pub fn trie_catalog(&self) -> &Arc<TrieCatalog> { &self.trie_catalog }
    pub fn table_catalog(&self) -> &Arc<TableCatalog> { &self.table_catalog }
    pub fn live(&self) -> &Arc<LiveIndex> { &self.live }
    pub fn snapshot(&self) -> Snapshot { self.snapshot_with_live(&self.live) }

    pub fn retains_trie(&self, table: &TableRef, key: &TrieKey) -> bool {
        self.trie_catalog.trie(table, key).is_some()
            || self.checkpoint_tries.as_ref().is_some_and(|checkpoint| checkpoint.requires(table, key))
    }

    fn snapshot_with_live(&self, live: &Arc<LiveIndex>) -> Snapshot {
        let cut = match self.catalog_publication {
            CatalogPublication::Installed(cut) => Some(cut),
            CatalogPublication::Untracked | CatalogPublication::Suspended => None,
        };
        Snapshot::new(Arc::clone(&self.trie_catalog), Arc::clone(&self.table_catalog), Arc::clone(live)).with_catalog_cut(cut)
    }
}

#[derive(Debug)]
struct CheckpointTries {
    tries: BTreeMap<TableRef, BTreeSet<TrieKey>>,
    search: BTreeMap<TableRef, BTreeSet<[u8; 32]>>,
    entity_directories: BTreeMap<TableRef, rtdb_format::log::proto::EntityDirectoryRoot>,
    term_directories: BTreeMap<TableRef, Vec<rtdb_format::log::proto::TermDirectoryRoot>>,
}

impl CheckpointTries {
    fn capture(catalog: &TrieCatalog) -> Self {
        let tries = catalog.tables().map(|table| {
            let keys = catalog.iter_tries(table)
                .filter(|trie| matches!(trie.state, TrieState::Live | TrieState::Nascent))
                .map(|trie| trie.key.clone()).collect();
            (table.clone(), keys)
        }).collect();
        let search = catalog.tables().map(|table| {
            (table.clone(), catalog.search_runs(table).filter_map(|run| run.identity().ok()).collect())
        }).collect();
        let entity_directories = catalog.tables().filter_map(|table| Some((table.clone(), catalog.entity_directory(table)?.clone()))).collect();
        let term_directories = catalog.tables().map(|table| (table.clone(), catalog.term_directories(table).cloned().collect())).collect();
        Self { tries, search, entity_directories, term_directories }
    }

    fn requires(&self, table: &TableRef, key: &TrieKey) -> bool {
        self.tries.get(table).is_some_and(|keys| keys.contains(key))
    }
}

#[derive(Clone, Debug)]
pub struct BlockPreparation {
    captured: Arc<PartitionValue>,
    metadata: Arc<PreparedBlockMetadata>,
}

#[derive(Debug, Default)]
struct PreparedBlockMetadata {
    tables: BTreeMap<TableRef, TableMeta>,
    hll_reservations: Vec<Arc<rtdb_columnar::MemoryReservation>>,
}

impl BlockPreparation {
    pub fn block_index(&self) -> BlockIndex { self.captured.live.block_index() }
    pub fn value(&self) -> &Arc<PartitionValue> { &self.captured }
    pub fn metadata(&self) -> &BTreeMap<TableRef, TableMeta> { &self.metadata.tables }
    pub fn table_catalog(&self) -> Result<TableCatalog> { self.captured.table_catalog.fold(self.metadata()) }
    pub fn table_blocks(&self, tries: &BTreeMap<TableRef, Vec<CatalogTrie>>, as_of: Timestamp) -> Result<BTreeMap<TableRef, TableBlock>> {
        let mut trie_catalog = self.captured.trie_catalog.as_ref().clone();
        for (table, tries) in tries { trie_catalog = trie_catalog.with_tries(table.clone(), tries.clone(), as_of)?; }
        let catalog = self.table_catalog()?;
        catalog.tables().iter().map(|(table, meta)| {
            let mut partitions = trie_catalog.partitions(table)?;
            preserve_sealed_term_directories(tries.get(table).into_iter().flatten(), &mut partitions);
            let block = meta.to_block(partitions)?;
            let block = trie_catalog.with_entity_directory(table, block);
            let block = trie_catalog.with_search_manifest(table, block);
            Ok((table.clone(), trie_catalog.with_retirement(table, trie_catalog.with_term_directories(table, block))))
        }).collect()
    }
}

fn preserve_sealed_term_directories<'a>(tries: impl Iterator<Item = &'a CatalogTrie>,
    partitions: &mut [rtdb_format::block::proto::Partition]) {
    for trie in tries {
        let key = trie.key.to_string();
        let Some(detail) = partitions.iter_mut().flat_map(|partition| &mut partition.tries)
            .find(|detail| detail.trie_key() == key) else { continue; };
        // A retry needs the original CAS, which the checkpoint's resulting root cannot reconstruct.
        detail.artefacts.extend(trie.artefacts.iter()
            .filter(|record| rtdb_format::term_directory::sealed_change(record).is_some()).cloned());
    }
}

#[derive(Debug, Default)]
struct Retention {
    readers: Vec<Weak<TrieCatalog>>,
}

pub struct TrieCollection {
    current: Arc<PartitionValue>,
    readers: Vec<Arc<TrieCatalog>>,
}

impl TrieCollection {
    pub fn current(&self) -> &PartitionValue { &self.current }

    pub fn eligible(&self, table: &TableRef, as_of: Timestamp) -> Vec<TrieKey> {
        self.current.trie_catalog().garbage_tries(table, as_of).into_iter()
            .filter(|candidate| !self.protected(table, &candidate.key))
            .map(|trie| trie.key.clone()).collect()
    }

    pub fn eligible_receipt_l0(&self, table: &TableRef, as_of: Timestamp) -> impl Iterator<Item = &TrieKey> {
        self.current.trie_catalog().iter_tries(table).filter(move |trie| {
            table.stores_expiring_transaction_metadata() && trie.key.level() == 0
                && matches!(trie.state, TrieState::Garbage { as_of: time } if time <= as_of)
                && !self.protected(table, &trie.key)
        }).map(|trie| &trie.key)
    }

    pub fn eligible_l0_artefacts(&self, table: &TableRef, as_of: Timestamp) -> impl Iterator<Item = &TrieKey> {
        self.current.trie_catalog().iter_tries(table).filter(move |trie| {
            trie.key.level() == 0 && matches!(trie.state, TrieState::Garbage { as_of: time } if time <= as_of)
                && !trie.artefacts.is_empty()
                && !self.protected(table, &trie.key)
        }).map(|trie| &trie.key)
    }

    pub fn eligible_search_runs(&self, table: &TableRef, as_of: Timestamp) -> impl Iterator<Item = &rtdb_format::log::proto::SearchRun> {
        self.current.trie_catalog().retired_search_runs(table).filter_map(move |retired| {
            if retired.released() || retired.superseded_at() > as_of.0 { return None; }
            let run = retired.run.as_ref()?;
            let id = run.identity().ok()?;
            let checkpoint = self.current.checkpoint_tries.as_ref().and_then(|checkpoint| checkpoint.search.get(table));
            let protected = any_reference_requires(checkpoint.is_some_and(|keys| keys.contains(&id)), self.readers.len(), |reader| {
                self.readers[reader].search_runs(table).any(|run| run.identity().is_ok_and(|key| key == id))
            });
            (!protected).then_some(run)
        })
    }

    pub fn eligible_search_tombstones(&self, table: &TableRef) -> impl Iterator<Item = &rtdb_format::log::proto::RetiredSearchRun> {
        self.current.trie_catalog().retired_search_runs(table).filter(move |retired| {
            if !retired.abandoned() || !retired.released() { return false; }
            let Some(settled_at) = retired.settled_at_replica_msg_id else { return true; };
            let Some(id) = retired.run.as_ref().and_then(|run| run.identity().ok()) else { return false; };
            self.readers.iter().all(|reader| reader.search_tombstone_settled_at(table, &id) == Some(settled_at))
        })
    }

    pub fn held_entity_directories<'a>(&'a self, table: &'a TableRef) -> impl Iterator<Item = &'a rtdb_format::log::proto::EntityDirectoryRoot> + 'a {
        self.current.trie_catalog().entity_directory(table).into_iter()
            .chain(self.current.checkpoint_tries.as_ref().and_then(|checkpoint| checkpoint.entity_directories.get(table)))
            .chain(self.readers.iter().filter_map(move |reader| reader.entity_directory(table)))
    }

    pub fn held_term_directories<'a>(&'a self, table: &'a TableRef) -> impl Iterator<Item = &'a rtdb_format::log::proto::TermDirectoryRoot> + 'a {
        self.current.trie_catalog().term_directories(table)
            .chain(self.current.checkpoint_tries.as_ref().and_then(|checkpoint| checkpoint.term_directories.get(table)).into_iter().flatten())
            .chain(self.readers.iter().flat_map(move |reader| reader.term_directories(table)))
    }

    fn protected(&self, table: &TableRef, key: &TrieKey) -> bool {
        let checkpoint_requires = self.current.checkpoint_tries.as_ref().is_some_and(|checkpoint| checkpoint.requires(table, key));
        any_reference_requires(checkpoint_requires, self.readers.len(), |index| {
            self.readers[index].trie(table, key).is_some_and(|trie| trie.state == TrieState::Live)
        })
    }
}

#[derive(Debug)]
pub struct PartitionState {
    value: RwLock<Arc<PartitionValue>>,
    published: Mutex<Option<Arc<PartitionValue>>>,
    retention: Mutex<Retention>,
}

pub struct PreparedWrite {
    predecessor: Arc<LiveIndex>,
    next: PartitionValue,
    written: Vec<TableRef>,
}

impl PreparedWrite {
    pub fn snapshot(&self) -> Snapshot { self.next.snapshot() }
}

impl PartitionState {
    pub fn new(initial: PartitionValue) -> Self {
        Self {
            value: RwLock::new(Arc::new(initial)),
            published: Mutex::new(None),
            retention: Mutex::new(Retention::default()),
        }
    }
    pub fn empty(config: TrieConfig) -> Result<Self> {
        Ok(Self::new(PartitionValue::loaded(TrieCatalog::default(), TableCatalog::default().with_system_tables()?, config)?))
    }
    pub fn value(&self) -> Arc<PartitionValue> { Arc::clone(&self.value.read()) }

    fn retain_catalog(&self, value: &PartitionValue) {
        let mut retention = self.retention.lock();
        let identity = Arc::as_ptr(value.trie_catalog());
        if retention.readers.last().is_some_and(|reader| reader.as_ptr() == identity) {
            return;
        }
        retention.readers.retain(|reader| reader.strong_count() != 0);
        retention.readers.push(Arc::downgrade(value.trie_catalog()));
    }

    /// Captures the current catalog and open read references under the same state lock as snapshot admission.
    pub fn trie_collection(&self) -> TrieCollection {
        let value = self.value.read();
        let retention = self.retention.lock();
        let readers = retention.readers.iter().filter_map(Weak::upgrade).collect();
        TrieCollection { current: Arc::clone(&value), readers }
    }

    /// Captures the latest applied state for a reader, including transaction resolution.
    pub fn snapshot(&self) -> Snapshot {
        let value = self.value.read();
        self.retain_catalog(&value);
        value.snapshot()
    }

    /// Captures the version published by the driver at this exact cut.
    /// Mutable state between replica application and publication carries no capture authority.
    pub fn published_snapshot(&self, cut: CatalogCut) -> Option<Snapshot> {
        let value = self.value.read();
        if !matches!(value.catalog_publication, CatalogPublication::Installed(current) if read_selection::catalog_matches_cut(current, cut)) {
            return None;
        }
        let published = self.published.lock();
        let published = published.as_ref()?;
        self.retain_catalog(published);
        Some(published.snapshot())
    }

    /// Captures the physical catalog and its replica coordinate from one installation.
    pub fn current_published_snapshot(&self) -> Option<Snapshot> {
        let value = self.value.read();
        let CatalogPublication::Installed(_) = value.catalog_publication else { return None; };
        let published = self.published.lock();
        let published = published.as_ref()?;
        self.retain_catalog(published);
        Some(published.snapshot())
    }

    /// Publishes a prefix after the replica driver has finished installing it.
    /// A reset suspends publication until adoption of its new naming epoch.
    pub fn publish_catalog_cut(&self, cut: CatalogCut) -> Result<()> {
        let mut value = self.value.write();
        match value.catalog_publication {
            CatalogPublication::Suspended => return Ok(()),
            CatalogPublication::Installed(previous) => {
                if cut.storage_version != previous.storage_version || cut.storage_epoch != previous.storage_epoch
                    || cut.replica_epoch != previous.replica_epoch || cut.through < previous.through
                {
                    return Err(invalid("Installed catalog prefix changed history or moved backwards"));
                }
                if cut == previous { return Ok(()); }
            }
            CatalogPublication::Untracked => {}
        }
        let mut next = value.as_ref().clone();
        next.catalog_publication = CatalogPublication::Installed(cut);
        let next = Arc::new(next);
        self.retain_catalog(&next);
        *self.published.lock() = Some(Arc::clone(&next));
        *value = next;
        Ok(())
    }

    /// The caller's basis barrier runs before this capture; this passive object never waits for log progress.
    pub fn open_snapshot(&self, minimum: Option<TxId>) -> Snapshot {
        let fresh = |live: &LiveIndex| minimum.is_none_or(|minimum| {
            live.latest_completed_tx().is_some_and(|basis| basis.tx_id >= minimum)
        });
        {
            let value = self.value.read();
            if let Some(live) = &value.live_snapshot {
                if fresh(live) {
                    self.retain_catalog(&value);
                    return value.snapshot_with_live(live);
                }
            }
        }
        let mut value = self.value.write();
        self.retain_catalog(&value);
        if let Some(live) = &value.live_snapshot {
            if fresh(live) { return value.snapshot_with_live(live); }
        }
        let mut next = value.as_ref().clone();
        next.live_snapshot = Some(Arc::clone(&next.live));
        let snapshot = next.snapshot();
        *value = Arc::new(next);
        snapshot
    }

    pub fn resolving_snapshot(&self, predecessors: &[Arc<StagedWrite>]) -> Result<Snapshot> { self.snapshot().with_staged(predecessors) }

    pub fn apply(&self, write: &StagedWrite) -> Result<()> {
        let mut value = self.value.write();
        if write.tables().values().any(crate::TableWrite::is_adopted_history) {
            value.snapshot().require_virgin_adoption(write)?;
        }
        let mut live = value.live.apply(write)?;
        live.search = None;
        let table_catalog = value.table_catalog.with_entries_for(write.tables().keys().cloned())?;
        let mut next = value.as_ref().clone();
        next.live = Arc::new(live);
        next.table_catalog = Arc::new(table_catalog);
        *value = Arc::new(next);
        Ok(())
    }

    pub fn prepare_write(&self, write: &StagedWrite) -> Result<PreparedWrite> {
        let captured = self.value();
        if write.tables().values().any(crate::TableWrite::is_adopted_history) {
            captured.snapshot().require_virgin_adoption(write)?;
        }
        let written: Vec<_> = write.tables().keys().cloned().collect();
        let mut next = captured.as_ref().clone();
        next.live = Arc::new(captured.live.apply(write)?);
        next.table_catalog = Arc::new(captured.table_catalog.with_entries_for(written.iter().cloned())?);
        Ok(PreparedWrite { predecessor: Arc::clone(&captured.live), next, written })
    }

    pub fn install_write(&self, mut prepared: PreparedWrite, search: Option<ServingSearch>) -> Result<()> {
        let mut value = self.value.write();
        if !Arc::ptr_eq(&prepared.predecessor, &value.live) {
            return Err(invalid("Prepared search data no longer follows the live index"));
        }
        Arc::make_mut(&mut prepared.next.live).search = search;
        let mut next = value.as_ref().clone();
        next.table_catalog = Arc::new(value.table_catalog.with_entries_for(prepared.written)?);
        next.live = prepared.next.live;
        *value = Arc::new(next);
        Ok(())
    }

    pub fn add_tries(&self, additions: &BTreeMap<TableRef, Vec<CatalogTrie>>, as_of: Timestamp) -> Result<()> {
        let mut value = self.value.write();
        let mut trie_catalog = value.trie_catalog.as_ref().clone();
        let epoch = trie_catalog.naming_epoch();
        for (table, tries) in additions {
            // A completed job can reach the replica journal after a compactor reset.
            let current = tries.iter().filter(|trie| trie.key.level() == 0 || trie.key.epoch() == epoch);
            trie_catalog = trie_catalog.with_tries(table.clone(), current.cloned(), as_of)?;
        }
        let mut next = value.as_ref().clone();
        next.trie_catalog = Arc::new(trie_catalog);
        *value = Arc::new(next);
        Ok(())
    }
    pub fn add_artefacts(&self, table: &TableRef, entries: Vec<(TrieKey, rtdb_format::log::proto::ArtefactRecord)>) {
        self.add_artefacts_at(table, entries, Timestamp(0));
    }

    pub fn add_artefacts_at(&self, table: &TableRef, entries: Vec<(TrieKey, rtdb_format::log::proto::ArtefactRecord)>, as_of: Timestamp) {
        let mut value = self.value.write();
        let trie_catalog = value.trie_catalog.with_artefacts_at(table, entries, as_of);
        let mut next = value.as_ref().clone();
        next.trie_catalog = Arc::new(trie_catalog);
        *value = Arc::new(next);
    }
    pub fn remove_artefacts(&self, table: &TableRef, entries: Vec<(TrieKey, rtdb_format::log::proto::ArtefactRecord)>) {
        self.remove_artefacts_at(table, entries, None);
    }

    pub fn remove_artefacts_at(&self, table: &TableRef, entries: Vec<(TrieKey, rtdb_format::log::proto::ArtefactRecord)>,
        position: Option<ReplicaPosition>) {
        let mut value = self.value.write();
        let trie_catalog = value.trie_catalog.without_artefacts_at(table, entries, position);
        let mut next = value.as_ref().clone();
        next.trie_catalog = Arc::new(trie_catalog);
        *value = Arc::new(next);
    }
    /// Applies a replicated table retirement (trie-cat.allium: RetireTable).
    pub fn retire_tables(&self, tables: impl IntoIterator<Item = TableRef>, as_of: Timestamp) {
        let mut value = self.value.write();
        let trie_catalog = value.trie_catalog.with_retired_tables(tables, as_of);
        let mut next = value.as_ref().clone();
        next.trie_catalog = Arc::new(trie_catalog);
        *value = Arc::new(next);
    }
    pub fn delete_tries(&self, deletions: &BTreeMap<TableRef, BTreeSet<TrieKey>>) {
        let mut value = self.value.write();
        let mut trie_catalog = value.trie_catalog.as_ref().clone();
        for (table, keys) in deletions { trie_catalog = trie_catalog.without_tries(table, keys); }
        let mut next = value.as_ref().clone();
        next.trie_catalog = Arc::new(trie_catalog);
        *value = Arc::new(next);
    }
    pub fn install_reset_primary(&self, catalog: TrieCatalog, target: TransactionKey) -> Result<()> {
        let mut value = self.value.write();
        if value.table_catalog.block().and_then(BlockCatalog::latest_completed_tx) != Some(target)
            || value.live.latest_completed_tx() != Some(target) || value.live.block_row_count() != 0
            || value.trie_catalog.naming_epoch().checked_add(1) != Some(catalog.naming_epoch())
        {
            return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.compactor-reset/catalog-moved", "The replacement primary no longer covers the installed reset cut"));
        }
        let mut next = value.as_ref().clone();
        next.trie_catalog = Arc::new(catalog);
        next.catalog_publication = CatalogPublication::Suspended;
        *self.published.lock() = None;
        *value = Arc::new(next);
        Ok(())
    }
    pub fn prepare_block(&self) -> Result<BlockPreparation> {
        let captured = self.value();
        let mut metadata = PreparedBlockMetadata::default();
        for (table, live) in captured.live.tables() {
            let (table_metadata, hll_reservation) = live.block_metadata()?;
            metadata.tables.insert(table.clone(), table_metadata);
            metadata.hll_reservations.extend(hll_reservation);
        }
        Ok(BlockPreparation { captured, metadata: Arc::new(metadata) })
    }

    /// Installs the full catalog from an independently rewritten recovery block.
    pub fn adopt_rewritten_block(&self, prepared: BlockPreparation, block: BlockCatalog,
        table_blocks: &BTreeMap<TableRef, TableBlock>, as_of: Timestamp,
    ) -> Result<()> {
        let naming_epoch = block.naming_epoch();
        let durable = TrieCatalog::load(table_blocks, prepared.captured.trie_catalog.file_size_target(), as_of)?
            .with_naming_epoch(naming_epoch);
        let checkpoint_tries = Some(Arc::new(CheckpointTries::capture(&durable)));
        let tables = TableCatalog::load(Some(block.clone()), table_blocks)?.with_system_tables()?;
        let mut value = self.value.write();
        if !Arc::ptr_eq(&value.live, &prepared.captured.live)
            || block.block_index() != value.live.block_index()
            || block.latest_completed_tx() != value.live.latest_completed_tx()
            || value.trie_catalog.naming_epoch().checked_add(1) != Some(naming_epoch)
        {
            return Err(invalid("The rewrite no longer covers the installed partition boundary"));
        }
        let live = value.live.next_block()?;
        *self.published.lock() = None;
        *value = Arc::new(PartitionValue { trie_catalog: Arc::new(durable), checkpoint_tries,
            table_catalog: Arc::new(tables), live: Arc::new(live), live_snapshot: None,
            catalog_publication: CatalogPublication::Untracked });
        Ok(())
    }

    /// Storage must publish the root and its table blocks before this atomic adoption.
    /// Collection protects the tries used to restore the recorded checkpoint.
    pub fn adopt_block(&self, prepared: BlockPreparation, block: BlockCatalog, table_blocks: &BTreeMap<TableRef, TableBlock>,
        tries: &BTreeMap<TableRef, Vec<CatalogTrie>>, as_of: Timestamp) -> Result<()> {
        let durable_tries = TrieCatalog::load(table_blocks, prepared.captured.trie_catalog.file_size_target(), as_of)?;
        let checkpoint_tries = Some(Arc::new(CheckpointTries::capture(&durable_tries)));
        let mut value = self.value.write();
        if !Arc::ptr_eq(&value.live, &prepared.captured.live) { return Err(invalid("Live data moved while the block was being prepared")); }
        if block.block_index() != value.live.block_index() || block.latest_completed_tx() != value.live.latest_completed_tx() {
            return Err(invalid("Durable block does not describe the prepared live boundary"));
        }
        let epoch = block.naming_epoch();
        let trie_catalog = if value.trie_catalog.naming_epoch() == epoch {
            let mut current = value.trie_catalog.as_ref().clone();
            for (table, tries) in tries { current = current.with_tries(table.clone(), tries.clone(), as_of)?; }
            current
        } else {
            // A reset checkpoint replaces the catalog, including for a node that
            // did not perform the reset or that is recovering an interrupted reset.
            durable_tries.with_naming_epoch(epoch)
        };
        let old_epoch = value.table_catalog.block().map_or(0, BlockCatalog::naming_epoch);
        let catalog_publication = match value.catalog_publication {
            CatalogPublication::Suspended if epoch > old_epoch && epoch == value.trie_catalog.naming_epoch() => CatalogPublication::Untracked,
            publication => publication,
        };
        let table_catalog = value.table_catalog.adopt(block, prepared.metadata())?;
        if !table_catalog.tables().keys().eq(table_blocks.keys()) {
            return Err(invalid("Table blocks do not match the adopted checkpoint registry"));
        }
        let live = value.live.next_block()?;
        *value = Arc::new(PartitionValue { trie_catalog: Arc::new(trie_catalog), checkpoint_tries,
            table_catalog: Arc::new(table_catalog), live: Arc::new(live), live_snapshot: None,
            catalog_publication });
        Ok(())
    }
}

fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-partition-transition", message.to_string()) }
