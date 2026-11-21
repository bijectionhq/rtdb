use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use rtdb_format::{catalog::{BlockCatalog, TableEntry}, keys::{PartGroup, TrieKey, block_index_from_key, oversized_block_index}};
use rtdb_index::PartitionState;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, BlockIndex, Result, TableRef, Timestamp};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use crate::{CycleWorker, SnapshotSource, TrieDeletionPublisher};

#[derive(Clone, Debug)]
pub struct GarbageConfig {
    pub blocks_to_keep: usize,
    pub garbage_lifetime: Duration,
    pub table_parallelism: usize,
    pub delete_parallelism: usize,
    pub chunk_size: usize,
}

impl Default for GarbageConfig {
    fn default() -> Self { Self { blocks_to_keep: 10, garbage_lifetime: Duration::from_secs(24 * 60 * 60), table_parallelism: 8, delete_parallelism: 64, chunk_size: 1024 } }
}

impl GarbageConfig {
    fn validate(&self) -> Result<()> {
        if self.blocks_to_keep == 0 || self.table_parallelism == 0 || self.delete_parallelism == 0 || self.chunk_size == 0 || self.chunk_size > 1024 {
            return Err(crate::invalid("Garbage collection requires positive retention and concurrency, with at most 1024 trie keys per chunk"));
        }
        Ok(())
    }
}

pub trait CollectionObserver: Send + Sync {
    fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly);
}

pub fn block_is_garbage(block: BlockIndex, latest: BlockIndex, keep: usize) -> bool {
    crate::retention::block_is_eligible(block.0, latest.0, keep)
}

pub struct TrieCollector {
    store: Arc<dyn ObjectStore>,
    partition: Arc<PartitionState>,
    publisher: Arc<dyn TrieDeletionPublisher>,
    observer: Arc<dyn CollectionObserver>,
    config: GarbageConfig,
}

impl TrieCollector {
    pub fn new(store: Arc<dyn ObjectStore>, partition: Arc<PartitionState>, publisher: Arc<dyn TrieDeletionPublisher>,
        observer: Arc<dyn CollectionObserver>, config: GarbageConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { store, partition, publisher, observer, config })
    }

    pub async fn default_garbage_as_of(&self) -> Result<Option<Timestamp>> {
        let prefix = ObjectKey::new("blocks").map_err(crate::invalid)?;
        let blocks: Vec<_> = self.store.list(Some(&prefix)).await.map_err(crate::invalid)?.into_iter().filter(|block| block.key.as_str().ends_with(".binpb")).collect();
        let Some(index) = blocks.len().checked_sub(self.config.blocks_to_keep) else { return Ok(None); };
        let block = BlockCatalog::decode(&self.store.get(&blocks[index].key).await.map_err(crate::invalid)?)?;
        let Some(tx) = block.latest_completed_tx() else { return Ok(None); };
        let duration = i64::try_from(self.config.garbage_lifetime.as_nanos().div_ceil(1000)).map_err(crate::invalid)?;
        Ok(Some(Timestamp(tx.system_time.0.checked_sub(duration).ok_or_else(|| crate::invalid("Garbage lifetime exceeds timestamp range"))?)))
    }

    pub async fn collect_as_of(&self, as_of: Timestamp, cancellation: &CancellationToken) -> Result<()> {
        let collection = Arc::new(self.partition.trie_collection());
        let tables: Vec<_> = collection.current().table_catalog().entries().cloned().collect();
        let deletes = Arc::new(Semaphore::new(self.config.delete_parallelism));
        let tasks = stream::iter(tables).map(|entry| {
            let deletes = Arc::clone(&deletes);
            let collection = Arc::clone(&collection);
            async move {
                let keys = collection.eligible(&entry.table, as_of);
                let groups = finished_groups(collection.current().trie_catalog(), &entry.table, &keys);
                let result = IoRegion::retaining(collection, self.collect_table(&entry, &keys, &groups, &deletes, cancellation)).await;
                if let Err(error) = &result { if !cancellation.is_cancelled() { self.observer.table_failed(Some(&entry.table), error); } }
            }
        });
        tasks.buffer_unordered(self.config.table_parallelism).for_each(|_| async {}).await;
        if cancellation.is_cancelled() { Err(crate::closed()) } else { Ok(()) }
    }

    async fn collect_table(&self, entry: &TableEntry, keys: &[TrieKey], groups: &BTreeSet<PartGroup>, deletes: &Semaphore, cancellation: &CancellationToken) -> Result<()> {
        // A group's artefacts are derived from every member's data file, so they go before any member's.
        for group in groups {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let artefacts = ObjectKey::new(entry.slug.group_artefact_prefix(group)).map_err(crate::invalid)?;
            for artefact in self.store.list(Some(&artefacts)).await.map_err(crate::invalid)? {
                self.store.delete(&artefact.key).await.map_err(crate::invalid)?;
            }
        }
        for chunk in keys.chunks(self.config.chunk_size) {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            if chunk.iter().any(|key| key.level() == 0) { return Err(crate::invalid("L0 recovery files cannot be garbage collected")); }
            let results = stream::iter(0..chunk.len()).map(|index| async move {
                let key = &chunk[index];
                let _permit = deletes.acquire().await.map_err(crate::invalid)?;
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                // Artefacts carry the data file's content, so they go first; listing catches any whose record has not reached this catalog.
                let artefacts = ObjectKey::new(entry.slug.artefact_prefix(key)).map_err(crate::invalid)?;
                for artefact in self.store.list(Some(&artefacts)).await.map_err(crate::invalid)? {
                    self.store.delete(&artefact.key).await.map_err(crate::invalid)?;
                }
                self.store.delete(&ObjectKey::new(entry.slug.data_key(key)).map_err(crate::invalid)?).await.map_err(crate::invalid)?;
                self.store.delete(&ObjectKey::new(entry.slug.metadata_key(key)).map_err(crate::invalid)?).await.map_err(crate::invalid)
            }).buffer_unordered(self.config.delete_parallelism).collect::<Vec<Result<()>>>().await;
            for result in results { result?; }
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.publisher.publish(entry.table.clone(), chunk.to_vec()).await?;
        }
        Ok(())
    }
}

/// The partition groups of tries being collected none of whose members the catalog holds besides them: their artefacts serve no trie once these go.
fn finished_groups(catalog: &rtdb_index::TrieCatalog, table: &TableRef, keys: &[TrieKey]) -> BTreeSet<PartGroup> {
    use rtdb_format::keys::BRANCH_FACTOR;

    let collected: BTreeSet<&TrieKey> = keys.iter().collect();
    keys.iter().filter_map(TrieKey::part_group)
        .filter(|group| {
            let members = group.members();
            let held: [Option<bool>; BRANCH_FACTOR as usize] = std::array::from_fn(|bucket| {
                let key = &members[bucket];
                catalog.trie(table, key).map(|_| collected.contains(key))
            });
            crate::retention::all_held_members_scheduled(held)
        })
        .collect()
}

#[async_trait]
impl CycleWorker for TrieCollector {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        if let Some(as_of) = self.default_garbage_as_of().await? { self.collect_as_of(as_of, cancellation).await?; }
        Ok(())
    }
}

pub struct BlockCollector {
    store: Arc<dyn ObjectStore>,
    snapshots: Arc<dyn SnapshotSource>,
    observer: Arc<dyn CollectionObserver>,
    config: GarbageConfig,
}

impl BlockCollector {
    pub fn new(store: Arc<dyn ObjectStore>, snapshots: Arc<dyn SnapshotSource>, observer: Arc<dyn CollectionObserver>, config: GarbageConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { store, snapshots, observer, config })
    }

    async fn sweep(&self, prefix: &str, latest: BlockIndex, oversized: bool, cancellation: &CancellationToken, deletes: &Semaphore) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let prefix = ObjectKey::new(prefix).map_err(crate::invalid)?;
        let objects = self.store.list(Some(&prefix)).await.map_err(crate::invalid)?;
        let objects = objects.into_iter().filter(|object| {
            let index = if oversized { oversized_block_index(object.key.as_str()) } else { block_index_from_key(object.key.as_str()).ok() };
            index.is_some_and(|index| block_is_garbage(index, latest, self.config.blocks_to_keep))
        });
        let results = stream::iter(objects).map(|object| async move {
            let _permit = deletes.acquire().await.map_err(crate::invalid)?;
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.store.delete(&object.key).await.map_err(crate::invalid)
        }).buffer_unordered(self.config.delete_parallelism).collect::<Vec<Result<()>>>().await;
        for result in results { result?; }
        Ok(())
    }
}

#[async_trait]
impl CycleWorker for BlockCollector {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        let snapshot = self.snapshots.snapshot();
        let Some(latest) = snapshot.table_catalog().block_index() else { return Ok(()); };
        let deletes = Arc::new(Semaphore::new(self.config.delete_parallelism));
        if let Err(error) = self.sweep("blocks", latest, false, cancellation, &deletes).await { self.observer.table_failed(None, &error); }
        let entries: Vec<_> = snapshot.table_catalog().entries().cloned().collect();
        stream::iter(entries).map(|entry| {
            let deletes = Arc::clone(&deletes);
            async move {
                if let Err(error) = self.sweep(&format!("{}/blocks", entry.slug.table_prefix()), latest, false, cancellation, &deletes).await {
                    self.observer.table_failed(Some(&entry.table), &error);
                }
            }
        }).buffer_unordered(self.config.table_parallelism).for_each(|_| async {}).await;
        if let Err(error) = self.sweep("oversized", latest, true, cancellation, &deletes).await { self.observer.table_failed(None, &error); }
        if cancellation.is_cancelled() { Err(crate::closed()) } else { Ok(()) }
    }
}
