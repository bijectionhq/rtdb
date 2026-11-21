use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use rtdb_format::{catalog::{BlockCatalog, TableBlock}, keys::TrieKey};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, BlockIndex, Result, TableRef, Timestamp};

use crate::{CatalogTrie, LiveIndex, Snapshot, StagedWrite, TableCatalog, TableMeta, TrieCatalog};

#[derive(Clone, Debug)]
pub struct PartitionValue {
    trie_catalog: Arc<TrieCatalog>,
    table_catalog: Arc<TableCatalog>,
    live: Arc<LiveIndex>,
    live_snapshot: Option<Arc<LiveIndex>>,
}

impl PartitionValue {
    pub fn loaded(trie_catalog: TrieCatalog, table_catalog: TableCatalog, config: TrieConfig) -> Result<Self> {
        let block_index = BlockIndex(table_catalog.block_index().map_or(-1, |index| index.0).checked_add(1).ok_or_else(|| invalid("Block index exhausted"))?);
        let latest_completed_tx = table_catalog.block().and_then(BlockCatalog::latest_completed_tx);
        Ok(Self { trie_catalog: Arc::new(trie_catalog), table_catalog: Arc::new(table_catalog),
            live: Arc::new(LiveIndex::new(block_index, latest_completed_tx, config)), live_snapshot: None })
    }
    pub fn trie_catalog(&self) -> &Arc<TrieCatalog> { &self.trie_catalog }
    pub fn table_catalog(&self) -> &Arc<TableCatalog> { &self.table_catalog }
    pub fn live(&self) -> &Arc<LiveIndex> { &self.live }
    pub fn snapshot(&self) -> Snapshot { Snapshot::new(Arc::clone(&self.trie_catalog), Arc::clone(&self.table_catalog), Arc::clone(&self.live)) }
}

#[derive(Clone, Debug)]
pub struct BlockPreparation {
    captured: Arc<PartitionValue>,
    metadata: BTreeMap<TableRef, TableMeta>,
}

impl BlockPreparation {
    pub fn block_index(&self) -> BlockIndex { self.captured.live.block_index() }
    pub fn value(&self) -> &Arc<PartitionValue> { &self.captured }
    pub fn metadata(&self) -> &BTreeMap<TableRef, TableMeta> { &self.metadata }
    pub fn table_catalog(&self) -> Result<TableCatalog> { self.captured.table_catalog.fold(&self.metadata) }
    pub fn table_blocks(&self, tries: &BTreeMap<TableRef, Vec<CatalogTrie>>, as_of: Timestamp) -> Result<BTreeMap<TableRef, TableBlock>> {
        let mut trie_catalog = self.captured.trie_catalog.as_ref().clone();
        for (table, tries) in tries { trie_catalog = trie_catalog.with_tries(table.clone(), tries.clone(), as_of)?; }
        let catalog = self.table_catalog()?;
        catalog.tables().iter().map(|(table, meta)| Ok((table.clone(), meta.to_block(trie_catalog.partitions(table)?)?))).collect()
    }
}

#[derive(Debug)]
pub struct PartitionState {
    value: ArcSwap<PartitionValue>,
    writer: Mutex<()>,
}

impl PartitionState {
    pub fn new(initial: PartitionValue) -> Self { Self { value: ArcSwap::from_pointee(initial), writer: Mutex::new(()) } }
    pub fn empty(config: TrieConfig) -> Result<Self> {
        Ok(Self::new(PartitionValue::loaded(TrieCatalog::default(), TableCatalog::default().with_system_tables()?, config)?))
    }
    pub fn value(&self) -> Arc<PartitionValue> { self.value.load_full() }

    /// The caller's basis barrier runs before this capture; this passive object never waits for log progress.
    pub fn open_snapshot(&self, min_system_time: Option<Timestamp>) -> Snapshot {
        let fresh = |live: &LiveIndex| min_system_time.is_none_or(|minimum| live.latest_completed_tx().is_some_and(|basis| basis.system_time >= minimum));
        loop {
            let value = self.value.load();
            if let Some(live) = &value.live_snapshot {
                if fresh(live) { return Snapshot::new(Arc::clone(&value.trie_catalog), Arc::clone(&value.table_catalog), Arc::clone(live)); }
            }
            let mut next = value.as_ref().clone();
            next.live_snapshot = Some(Arc::clone(&next.live));
            let snapshot = next.snapshot();
            let previous = self.value.compare_and_swap(&value, Arc::new(next));
            if Arc::ptr_eq(&previous, &value) { return snapshot; }
        }
    }

    pub fn resolving_snapshot(&self, predecessors: &[Arc<StagedWrite>]) -> Result<Snapshot> { self.value().snapshot().with_staged(predecessors) }

    fn publish(&self, next: PartitionValue) {
        self.value.rcu(|current| Arc::new(PartitionValue {
            live_snapshot: current.live_snapshot.clone(),
            ..next.clone()
        }));
    }

    pub fn apply(&self, write: &StagedWrite) -> Result<()> {
        let _writer = self.writer.lock();
        let mut next = self.value.load().as_ref().clone();
        next.live = Arc::new(next.live.apply(write)?);
        next.table_catalog = Arc::new(next.table_catalog.with_entries_for(write.tables().keys().cloned())?);
        self.publish(next);
        Ok(())
    }

    pub fn add_tries(&self, additions: &BTreeMap<TableRef, Vec<CatalogTrie>>, as_of: Timestamp) -> Result<()> {
        let _writer = self.writer.lock();
        let mut next = self.value.load().as_ref().clone();
        let mut trie_catalog = next.trie_catalog.as_ref().clone();
        for (table, tries) in additions { trie_catalog = trie_catalog.with_tries(table.clone(), tries.clone(), as_of)?; }
        next.trie_catalog = Arc::new(trie_catalog);
        self.publish(next);
        Ok(())
    }
    pub fn add_artefacts(&self, table: &TableRef, entries: Vec<(TrieKey, rtdb_format::log::proto::ArtefactRecord)>) {
        let _writer = self.writer.lock();
        let mut next = self.value.load().as_ref().clone();
        next.trie_catalog = Arc::new(next.trie_catalog.with_artefacts(table, entries));
        self.publish(next);
    }
    pub fn remove_artefacts(&self, table: &TableRef, entries: Vec<(TrieKey, rtdb_format::log::proto::ArtefactRecord)>) {
        let _writer = self.writer.lock();
        let mut next = self.value.load().as_ref().clone();
        next.trie_catalog = Arc::new(next.trie_catalog.without_artefacts(table, entries));
        self.publish(next);
    }
    pub fn delete_tries(&self, deletions: &BTreeMap<TableRef, BTreeSet<TrieKey>>) {
        let _writer = self.writer.lock();
        let mut next = self.value.load().as_ref().clone();
        let mut trie_catalog = next.trie_catalog.as_ref().clone();
        for (table, keys) in deletions { trie_catalog = trie_catalog.without_tries(table, keys); }
        next.trie_catalog = Arc::new(trie_catalog);
        self.publish(next);
    }
    pub fn reset_to_l0(&self, replacements: BTreeMap<TableRef, Vec<CatalogTrie>>) -> Result<()> {
        let _writer = self.writer.lock();
        let mut next = self.value.load().as_ref().clone();
        next.trie_catalog = Arc::new(next.trie_catalog.reset_to_l0(replacements)?);
        self.publish(next);
        Ok(())
    }
    pub fn prepare_block(&self) -> Result<BlockPreparation> {
        let captured = self.value();
        let metadata = captured.live.block_metadata()?;
        Ok(BlockPreparation { captured, metadata })
    }

    /// Storage must publish the block file before this atomic adoption.
    pub fn adopt_block(&self, prepared: BlockPreparation, block: BlockCatalog, tries: &BTreeMap<TableRef, Vec<CatalogTrie>>, as_of: Timestamp) -> Result<()> {
        let _writer = self.writer.lock();
        let value = self.value.load();
        if !Arc::ptr_eq(&value.live, &prepared.captured.live) { return Err(invalid("Live data moved while the block was being prepared")); }
        if block.block_index() != value.live.block_index() || block.latest_completed_tx() != value.live.latest_completed_tx() {
            return Err(invalid("Durable block does not describe the prepared live boundary"));
        }
        let mut trie_catalog = value.trie_catalog.as_ref().clone();
        for (table, tries) in tries { trie_catalog = trie_catalog.with_tries(table.clone(), tries.clone(), as_of)?; }
        let table_catalog = value.table_catalog.adopt(block, &prepared.metadata)?;
        let live = value.live.next_block()?;
        self.value.store(Arc::new(PartitionValue { trie_catalog: Arc::new(trie_catalog), table_catalog: Arc::new(table_catalog), live: Arc::new(live), live_snapshot: None }));
        Ok(())
    }
}

fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-partition-transition", message.to_string()) }

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread, time::Duration};

    use super::{PartitionState, TrieConfig};

    #[test]
    fn a_snapshot_refresh_finishes_during_a_write_and_survives_publication() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let captured = thread::scope(|threads| {
            let writer = state.writer.lock();
            let next = state.value().as_ref().clone();
            let (opened, capture) = mpsc::channel();
            let reading = &state;
            threads.spawn(move || { let _ = opened.send(reading.open_snapshot(None)); });
            let captured = capture.recv_timeout(Duration::from_secs(5));
            if captured.is_ok() { state.publish(next); }
            drop(writer);
            captured
        });

        assert!(captured.is_ok());
        assert!(state.value().live_snapshot.is_some());
    }
}
