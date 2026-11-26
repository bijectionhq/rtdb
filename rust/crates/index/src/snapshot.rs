use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::any::{Any, TypeId};

use rtdb_types::{Anomaly, BlockIndex, CatalogCheckpoint, CatalogCut, LogicalType, OwnedRelation, ReplicaPosition, Result, TableRef, Timestamp, TransactionKey};

use crate::{CatalogTrie, LiveIndex, LiveTable, StagedWrite, TableCatalog, TableEntry, TrieCatalog, TrieState, absent_type};

#[derive(Clone)]
struct CatalogRead { permit: Arc<dyn std::any::Any + Send + Sync> }

impl std::fmt::Debug for CatalogRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("CatalogRead").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct SnapshotMemos(parking_lot::Mutex<BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl std::fmt::Debug for SnapshotMemos {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("SnapshotMemos").finish_non_exhaustive()
    }
}

/// Each table's column types joined with the table catalog's, for the snapshot whose memos hold them.
#[derive(Default)]
struct JoinedColumnTypes(parking_lot::Mutex<BTreeMap<TableRef, StructFields<LogicalType>>>);

#[derive(Clone, Debug)]
pub struct Snapshot {
    trie_catalog: Arc<TrieCatalog>,
    table_catalog: Arc<TableCatalog>,
    durable: Arc<LiveIndex>,
    layered: BTreeMap<TableRef, Vec<Arc<LiveTable>>>,
    basis: Option<TransactionKey>,
    system_time_cap: Option<Timestamp>,
    catalog_cut: Option<CatalogCut>,
    catalog_read: Option<CatalogRead>,
    read_custody: Option<(CatalogRead, std::time::Instant)>,
    memos: Arc<SnapshotMemos>,
    certified_read: Option<Arc<rtdb_types::residence_trace::CertifiedRead>>,
}

/// Routing coordinates carried by the captured catalog and its applied event
/// inventories. A query may lower the system cap, so these physical counts
/// are upper bounds at a lowered cut. Zero still proves positive exhaustion.
#[derive(Clone, Copy, Debug)]
pub struct OwnedRoutingInventory {
    pub data_events: i64,
    pub journal_events: i64,
    pub mask_events: i64,
    has_data_tries: bool,
    has_journal_tries: bool,
}

impl OwnedRoutingInventory {
    pub fn data_exhausted(self) -> bool {
        self.data_events == 0 && self.mask_events == 0 && !self.has_data_tries
    }

    pub fn journal_exhausted(self) -> bool {
        self.data_exhausted() && self.journal_events == 0 && !self.has_journal_tries
    }
}

impl Snapshot {
    pub fn new(trie_catalog: Arc<TrieCatalog>, table_catalog: Arc<TableCatalog>, live: Arc<LiveIndex>) -> Self {
        Self { basis: live.latest_completed_tx(), system_time_cap: live.latest_completed_tx().map(|key| key.system_time), durable: live, trie_catalog, table_catalog, layered: BTreeMap::new(), catalog_cut: None, catalog_read: None, read_custody: None, certified_read: None, memos: Default::default() }
    }
    pub fn with_catalog_cut(mut self, catalog_cut: Option<CatalogCut>) -> Self {
        self.catalog_cut = catalog_cut;
        self
    }
    /// Returns the conservative installed prefix captured with this physical catalog.
    /// Callers must also bind the opened database and residence before comparing cuts across nodes.
    pub fn catalog_cut(&self) -> Option<CatalogCut> { self.catalog_cut }
    /// The exact checkpoint that supplies the catalog's admitted replica prefix.
    pub fn catalog_checkpoint(&self) -> Result<Option<CatalogCheckpoint>> {
        let Some(cut) = self.catalog_cut else { return Ok(None); };
        Ok(match self.table_catalog.block() {
            Some(block) => block.boundary_replica_position().zip(block.publication_sha256())
                .map(|(boundary, root)| CatalogCheckpoint { boundary, root }),
            None => Some(CatalogCheckpoint { boundary: ReplicaPosition::from_epoch_offset(cut.replica_epoch, -1)?, root: [0; 32] }),
        })
    }
    /// Complete paths for compacted primaries and their recorded artefacts, and published search runs.
    /// A claim exceeding the bound retains the conservative scope.
    /// The collectable file paths the captured catalogs name, computed once per pair of catalogs, which are replaced rather than changed.
    pub fn catalog_file_paths(&self) -> Option<Arc<[String]>> {
        type Inventories = Vec<(std::sync::Weak<TrieCatalog>, std::sync::Weak<TableCatalog>, Option<Arc<[String]>>)>;
        static INVENTORIES: std::sync::LazyLock<parking_lot::Mutex<Inventories>> = std::sync::LazyLock::new(Default::default);
        const KEPT: usize = 8;
        let held = |trie: &std::sync::Weak<TrieCatalog>, table: &std::sync::Weak<TableCatalog>| {
            std::ptr::eq(trie.as_ptr(), Arc::as_ptr(&self.trie_catalog)) && std::ptr::eq(table.as_ptr(), Arc::as_ptr(&self.table_catalog))
        };
        {
            let mut inventories = INVENTORIES.lock();
            inventories.retain(|(trie, table, _)| trie.strong_count() > 0 && table.strong_count() > 0);
            if let Some((_, _, paths)) = inventories.iter().find(|(trie, table, _)| held(trie, table)) { return paths.clone(); }
        }
        let paths = self.inventory_file_paths();
        let mut inventories = INVENTORIES.lock();
        if !inventories.iter().any(|(trie, table, _)| held(trie, table)) {
            if inventories.len() >= KEPT { inventories.remove(0); }
            inventories.push((Arc::downgrade(&self.trie_catalog), Arc::downgrade(&self.table_catalog), paths.clone()));
        }
        paths
    }

    fn inventory_file_paths(&self) -> Option<Arc<[String]>> {
        let mut paths = Vec::new();
        let mut bytes = 0usize;
        let mut retain = |path: String| {
            bytes = bytes.checked_add(path.len().checked_mul(6)?.checked_add(16)?)?;
            if bytes > rtdb_types::CATALOG_FILE_PATH_BYTES { return None; }
            paths.push(path);
            Some(())
        };
        for table in self.trie_catalog.tables() {
            if self.trie_catalog.entity_directory(table).is_some() || self.trie_catalog.term_directories(table).next().is_some() {
                return None;
            }
            let entry = self.table_catalog.entry(table)?;
            for trie in self.trie_catalog.iter_tries(table).filter(|trie| trie.key.level() > 0) {
                if trie.state == TrieState::Live {
                    retain(entry.slug.data_key(&trie.key))?;
                    retain(entry.slug.metadata_key(&trie.key))?;
                }
                for record in &trie.artefacts {
                    if !matches!(rtdb_format::keys::ArtefactOwner::of(&trie.key, record).ok()?,
                        rtdb_format::keys::ArtefactOwner::Trie(_))
                    {
                        continue;
                    }
                    retain(entry.slug.recorded_artefact_key(&trie.key, record).ok()?)?;
                }
            }
            for run in self.trie_catalog.search_runs(table) {
                retain(run.object_key(&entry.slug).ok()?)?;
            }
        }
        paths.sort_unstable();
        paths.dedup();
        Some(paths.into())
    }

    /// Transfers the store's captured catalog registration into this snapshot and its clones.
    pub fn with_catalog_read(mut self, permit: Arc<dyn std::any::Any + Send + Sync>) -> Self {
        self.catalog_read = Some(CatalogRead { permit });
        self
    }
    /// Retains an in-memory serving selection through evaluation and delivery.
    pub fn with_read_custody(mut self, permit: Arc<dyn std::any::Any + Send + Sync>, deadline: std::time::Instant) -> Self {
        self.read_custody = Some((CatalogRead { permit }, deadline));
        self
    }

    pub fn read_custody_deadline(&self) -> Option<std::time::Instant> {
        self.read_custody.as_ref().map(|(_, deadline)| *deadline)
    }

    /// Clones the storage registration that was attached when the catalog was captured.
    pub fn catalog_read_permit<T: std::any::Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(&self.catalog_read.as_ref()?.permit).downcast().ok()
    }
    pub fn with_certified_read(mut self, read: Arc<rtdb_types::residence_trace::CertifiedRead>) -> Self {
        self.certified_read = Some(read);
        self
    }
    pub fn certified_read(&self) -> Option<&Arc<rtdb_types::residence_trace::CertifiedRead>> {
        self.certified_read.as_ref()
    }
    pub fn tx_basis(&self) -> Option<TransactionKey> { self.basis }
    /// Owns derived state for this immutable snapshot and its clones.
    pub fn memo<T: Any + Default + Send + Sync>(&self) -> Arc<T> {
        let mut memos = self.memos.0.lock();
        Arc::clone(memos.entry(TypeId::of::<T>()).or_insert_with(|| Arc::new(T::default())))
            .downcast().expect("memo type matches its key")
    }
    pub fn serving_search<T: std::any::Any>(&self) -> Option<&T> { self.durable.search.as_ref()?.get() }
    /// Whether a transaction's own uncommitted writes are layered over this snapshot.
    pub fn has_overlays(&self) -> bool { !self.layered.is_empty() }
    pub fn system_time_cap(&self) -> Option<Timestamp> { self.system_time_cap }
    /// Bounds a clone's owned overlay map, table names and segment references; immutable index buffers stay shared.
    pub fn clone_allocation_bound(&self) -> Result<usize> {
        let overflow = || Anomaly::incorrect("rtdb.index/snapshot-size", "Snapshot clone metadata exceeds addressable memory");
        self.layered.iter().try_fold(std::mem::size_of::<Self>(), |bytes, (table, segments)| {
            bytes.checked_add(table.schema.len()).and_then(|bytes| bytes.checked_add(table.table.len()))
                .and_then(|bytes| segments.len().checked_mul(std::mem::size_of::<Arc<LiveTable>>()).and_then(|segments| bytes.checked_add(segments)))
                .and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)
        })
    }
    /// Keeps this generation's catalog and live buffers; `None` leaves system time uncapped.
    /// A supplied cap beyond the captured transaction head returns an error.
    pub fn with_system_time_cap(mut self, cap: Option<Timestamp>) -> Result<Self> {
        if cap.is_some_and(|cap| self.basis.is_none_or(|basis| cap > basis.system_time)) {
            return Err(Anomaly::incorrect("rtdb.index/future-read-basis", "Query basis exceeds the captured partition head"));
        }
        if self.system_time_cap != cap {
            self.memos = Default::default();
            self.system_time_cap = cap;
        }
        Ok(self)
    }
    pub fn block_index(&self) -> BlockIndex { self.durable.block_index() }
    pub fn trie_catalog(&self) -> &TrieCatalog { &self.trie_catalog }
    pub fn trie_catalog_ref(&self) -> &Arc<TrieCatalog> { &self.trie_catalog }
    pub fn table_catalog(&self) -> &TableCatalog { &self.table_catalog }
    pub fn table_entry(&self, table: &TableRef) -> Option<&TableEntry> { self.table_catalog.entry(table) }
    pub fn trie_entries(&self, table: &TableRef) -> Vec<&CatalogTrie> { self.trie_catalog.current_tries(table) }
    pub fn tables(&self) -> BTreeSet<TableRef> {
        self.table_names().cloned().collect()
    }
    /// Borrows each table identity source. A name can occur in more than one source.
    pub fn table_names(&self) -> impl Iterator<Item = &TableRef> {
        self.table_catalog.tables().keys().chain(self.durable.tables().keys()).chain(self.layered.keys())
    }
    /// Tests this snapshot's table names without allocating, including empty declarations and superseded live tables.
    pub fn contains_table(&self, table: &TableRef) -> bool {
        self.table_catalog.tables().contains_key(table) || self.durable.tables().contains_key(table) || self.layered.contains_key(table)
    }
    /// Conservative allocation allowance for enumerating and merging this snapshot's table schemas.
    /// It borrows the schema sources and allocates nothing; event buffers and HLLs are excluded.
    pub fn schema_allocation_bound(&self, database: &str) -> Result<usize> {
        let catalog = self.table_catalog.tables().iter().map(|(table, metadata)| (table, &metadata.column_types));
        let applied = self.durable.tables().iter().map(|(table, live)| (table, live.column_types()));
        let overlays = self.layered.iter().flat_map(|(table, segments)| {
            segments.iter().map(move |segment| (table, segment.column_types()))
        });
        schema_sources_bound(database, catalog.chain(applied).chain(overlays))
    }
    /// Bounds the schema merge for just one table in this snapshot.
    pub fn table_schema_allocation_bound(&self, database: &str, table: &TableRef) -> Result<usize> {
        let catalog = self.table_catalog.metadata(table).map(|metadata| &metadata.column_types);
        let live = self.durable.tables().get(table).into_iter()
            .chain(self.layered.get(table).into_iter().flatten())
            .map(|segment| segment.column_types());
        schema_sources_bound(database, catalog.into_iter().chain(live).map(|types| (table, types)))
    }
    pub fn column_names(&self, table: &TableRef) -> BTreeSet<String> {
        let mut names = BTreeSet::new();
        if let Some(metadata) = self.table_catalog.metadata(table) { names.extend(metadata.column_types.keys().cloned()); }
        if let Some(durable) = self.durable.tables().get(table) { names.extend(durable.column_types().keys().cloned()); }
        for segment in self.layered.get(table).into_iter().flatten() { names.extend(segment.column_types().keys().cloned()); }
        names
    }

    /// Borrows segments from newest overlay to oldest, followed by the applied segment unless L0 already covers it.
    pub fn iter_segments<'a>(&'a self, table: &'a TableRef) -> impl Iterator<Item = &'a Arc<LiveTable>> {
        let overlays = self.layered.get(table).into_iter().flat_map(|layers| layers.iter().rev());
        let applied = self.durable.tables().get(table)
            .filter(|segment| segment.block_index() > self.trie_catalog.l0_max_block_index(table));
        overlays.chain(applied)
    }

    /// Counts all events admitted for this relation, including live and staged
    /// events. A flush transfers those events into the catalog's cumulative count.
    pub fn relation_event_frontier(&self, table: &TableRef) -> Result<i64> {
        let durable = self.table_catalog.metadata(table).map_or(0, |metadata| metadata.row_count);
        if durable < 0 { return Err(Anomaly::fault("rtdb.index/relation-frontier", "Invalid durable relation event frontier")); }
        self.iter_segments(table).try_fold(durable, |count, segment| {
            count.checked_add(i64::from(segment.row_count())).ok_or_else(|| {
                Anomaly::fault("rtdb.index/relation-frontier", "Relation event frontier overflow")
            })
        })
    }

    /// Capture one admitted physical relation. Live resolution, compaction
    /// and replay all contribute through the same relation event frontiers.
    pub fn owned_routing_inventory(&self, owner: &str, relation: &OwnedRelation) -> Result<OwnedRoutingInventory> {
        let data = relation.table(owner);
        let journal = relation.change_journal(owner)?;
        Ok(OwnedRoutingInventory {
            data_events: self.relation_event_frontier(&data)?,
            journal_events: self.relation_event_frontier(&journal)?,
            mask_events: self.relation_event_frontier(&relation.purge_table(owner))?,
            has_data_tries: self.trie_catalog.has_current_tries(&data),
            has_journal_tries: self.trie_catalog.has_current_tries(&journal),
        })
    }

    /// Newer delta segments precede older ones, preserving operation order at equal system times.
    pub fn segments(&self, table: &TableRef) -> Vec<Arc<LiveTable>> { self.iter_segments(table).cloned().collect() }

    /// Confirms that every put in this table's captured scan inputs uses the IID derived from its document ID.
    /// A serving trie, a mismatched ID, or an unverified live segment leaves the relationship unproved.
    pub fn has_canonical_entity_iids(&self, table: &TableRef) -> bool {
        !self.trie_catalog.has_current_tries(table) && self.live_has_canonical_entity_iids(table)
    }

    pub fn live_has_canonical_entity_iids(&self, table: &TableRef) -> bool {
        self.iter_segments(table).all(|segment| segment.has_canonical_entity_iids())
    }

    pub fn declared_columns(&self, table: &TableRef) -> StructFields<LogicalType> {
        let mut declarations = self.table_catalog.metadata(table)
            .map_or_else(StructFields::new, |metadata| metadata.declared_columns.clone());
        if let Some(live) = self.durable.tables().get(table) {
            crate::catalog::add_declarations(&mut declarations, live.declared_columns());
        }
        declarations
    }

    /// A table's column types, joined once per snapshot.
    pub fn column_types(&self, table: &TableRef) -> StructFields<LogicalType> {
        let memo = self.memo::<JoinedColumnTypes>();
        if let Some(types) = memo.0.lock().get(table) { return types.clone(); }
        let types = self.column_types_for(table, self.column_names(table));
        memo.0.lock().insert(table.clone(), types.clone());
        types
    }
    pub fn column_types_for(&self, table: &TableRef, columns: impl IntoIterator<Item = String>) -> StructFields<LogicalType> {
        let sources: Vec<_> = self.durable.tables().get(table).into_iter().chain(self.layered.get(table).into_iter().flatten()).map(|segment| segment.column_types()).collect();
        let absent: Vec<_> = sources.iter().map(|types| absent_type(types)).collect();
        columns.into_iter().map(|name| {
            let mut ty = self.table_catalog.contributed_type(table, &name);
            for (types, absent) in sources.iter().zip(&absent) { ty = ty.join(types.get(&name).unwrap_or(absent)); }
            (name, ty)
        }).collect()
    }

    /// Layers a transaction's own segments over this snapshot, as staging its whole write would.
    pub fn with_transaction_segments(&self, tx_key: rtdb_types::TransactionKey, segments: &std::collections::BTreeMap<TableRef, Arc<LiveTable>>) -> Result<Self> {
        let mut next = self.clone();
        next.memos = Default::default();
        for (table, segment) in segments { next.layered.entry(table.clone()).or_default().push(Arc::clone(segment)); }
        if segments.keys().any(|table| next.table_catalog.entry(table).is_none()) {
            next.table_catalog = Arc::new(next.table_catalog.with_entries_for(segments.keys().cloned())?);
        }
        next.basis = Some(tx_key);
        next.system_time_cap = Some(tx_key.system_time);
        Ok(next)
    }
    pub(crate) fn require_virgin_adoption(&self, write: &StagedWrite) -> Result<()> {
        for (table, effect) in write.tables() {
            if effect.is_adopted_history()
                && (self.contains_table(table) || self.trie_catalog.iter_tries(table).next().is_some())
            {
                return Err(Anomaly::incorrect("rtdb.index/adoption-destination", "Adopted history requires a virgin destination"));
            }
        }
        Ok(())
    }

    pub fn with_staged(&self, staged: &[Arc<StagedWrite>]) -> Result<Self> {
        let mut next = self.clone();
        if !staged.is_empty() { next.memos = Default::default(); }
        for write in staged {
            if write.predecessor() != next.basis || write.block_index() != self.block_index() {
                return Err(Anomaly::incorrect("rtdb.index/overlay-order", "Staged overlay does not follow the snapshot basis and block"));
            }
            next.require_virgin_adoption(write)?;
            for (table, segment) in write.staged_segments()? {
                next.layered.entry(table.clone()).or_default().push(Arc::clone(segment));
            }
            if write.tables().keys().any(|table| next.table_catalog.entry(table).is_none()) {
                next.table_catalog = Arc::new(next.table_catalog.with_entries_for(write.tables().keys().cloned())?);
            }
            next.basis = Some(write.tx_key());
            next.system_time_cap = Some(write.tx_key().system_time);
        }
        Ok(next)
    }
}

fn schema_sources_bound<'a>(
    database: &str,
    sources: impl Iterator<Item = (&'a TableRef, &'a StructFields<LogicalType>)>,
) -> Result<usize> {
    let overflow = || Anomaly::incorrect("rtdb.index/schema-size", "Schema allocation bound exceeds addressable memory");
    let mut bytes = 4096usize;
    for (table, types) in sources {
        bytes = bytes.checked_add(database.len()).and_then(|n| n.checked_add(table.schema.len()))
            .and_then(|n| n.checked_add(table.table.len())).and_then(|n| n.checked_add(1024)).ok_or_else(overflow)?;
        for (name, ty) in types {
            let retained = ty.retained_bytes()?;
            bytes = bytes.checked_add(name.len()).and_then(|n| n.checked_add(512))
                .and_then(|n| n.checked_add(retained)).ok_or_else(overflow)?;
        }
    }
    bytes.checked_mul(16).ok_or_else(overflow)
}

#[cfg(test)]
mod owned_routing_tests {
    use super::OwnedRoutingInventory;

    #[test]
    fn an_empty_inventory_has_no_data_journal_tries_or_masks() {
        let empty = OwnedRoutingInventory {
            data_events: 0,
            journal_events: 0,
            mask_events: 0,
            has_data_tries: false,
            has_journal_tries: false,
        };
        assert!(empty.journal_exhausted());
        assert!(!(OwnedRoutingInventory { mask_events: 1, ..empty }).data_exhausted());
        assert!(!(OwnedRoutingInventory { has_data_tries: true, ..empty }).data_exhausted());
        assert!(!(OwnedRoutingInventory { has_journal_tries: true, ..empty }).journal_exhausted());
        assert!(!(OwnedRoutingInventory { journal_events: 1, ..empty }).journal_exhausted());
    }
}

#[test]
fn column_types_are_joined_again_under_another_table_catalog() {
    let state = crate::PartitionState::empty(Default::default()).unwrap();
    let key = TransactionKey { tx_id: rtdb_types::TxId(0), system_time: Timestamp(10) };
    let mut transaction = crate::live::TransactionBuffer::new(state.value().snapshot(), key, rtdb_columnar::MemoryBudget::new(1 << 24)).unwrap();
    let table = TableRef::new("public", "docs");
    transaction.put(table.clone(), StructFields::from([("_id".into(), rtdb_types::Value::I64(1)), ("v".into(), rtdb_types::Value::I64(2))]), rtdb_types::Interval::default()).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let applied = state.value().snapshot();
    let types = applied.column_types(&table);
    assert_eq!(types, applied.column_types_for(&table, applied.column_names(&table)));
    assert!(types.get("v").is_some());
    assert_eq!(state.value().snapshot().column_types(&table), types, "a later snapshot of the same segment reuses the join");
    let other = Snapshot::new(Arc::new(TrieCatalog::default()), Arc::new(TableCatalog::default()), Arc::clone(&applied.durable));
    assert_eq!(other.column_types(&table), other.column_types_for(&table, other.column_names(&table)));
}

#[test]
fn catalog_file_paths_are_computed_once_per_pair_of_catalogs() {
    let live = Arc::new(LiveIndex::new(BlockIndex(0), None, rtdb_trie::TrieConfig::default()));
    let (trie, table) = (Arc::new(TrieCatalog::default()), Arc::new(TableCatalog::default()));
    let first = Snapshot::new(Arc::clone(&trie), Arc::clone(&table), Arc::clone(&live)).catalog_file_paths().unwrap();
    let again = Snapshot::new(Arc::clone(&trie), Arc::clone(&table), Arc::clone(&live)).catalog_file_paths().unwrap();
    assert!(Arc::ptr_eq(&first, &again));
    let replaced = Snapshot::new(Arc::new(TrieCatalog::default()), table, live).catalog_file_paths().unwrap();
    assert!(!Arc::ptr_eq(&first, &replaced), "a replaced catalog has its own inventory");
    assert_eq!(first, replaced);
}

#[test]
fn ordered_index_snapshot_memos_follow_the_captured_basis() {
    #[derive(Default)]
    struct Memo;
    let transaction = TransactionKey { tx_id: rtdb_types::TxId(1), system_time: Timestamp(10) };
    let live = LiveIndex::new(BlockIndex(0), Some(transaction), rtdb_trie::TrieConfig::default());
    let snapshot = Snapshot::new(Arc::new(TrieCatalog::default()), Arc::new(TableCatalog::default()), Arc::new(live));
    let memo = snapshot.memo::<Memo>();
    assert!(Arc::ptr_eq(&memo, &snapshot.clone().memo::<Memo>()));
    assert!(Arc::ptr_eq(&memo, &snapshot.clone().with_system_time_cap(Some(Timestamp(10))).unwrap().memo::<Memo>()));
    assert!(!Arc::ptr_eq(&memo, &snapshot.clone().with_system_time_cap(Some(Timestamp(9))).unwrap().memo::<Memo>()));
    assert!(!Arc::ptr_eq(&memo, &snapshot.with_transaction_segments(transaction, &BTreeMap::new()).unwrap().memo::<Memo>()));
}
