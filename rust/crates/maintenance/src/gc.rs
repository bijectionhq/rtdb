use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use async_trait::async_trait;
use futures::{StreamExt, stream};
use rtdb_format::{
    catalog::{BlockCatalog, TableEntry},
    keys::{ArtefactOwner, PartGroup, TrieKey, block_index_from_key, oversized_block_index},
    log::proto::{ArtefactEntry, ArtefactsAdded, ArtefactsRemoved, PublishedObjectVersion, SearchRunChange},
};
use rtdb_index::{PartitionState, TrieCatalog};
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, BlockIndex, CatalogCut, ReplicaPosition, Result, TableRef, Timestamp};
use tokio::sync::{OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;
use crate::{ArtefactPublisher, CycleWorker, SnapshotSource, TableRetirements, TrieDeletionPublisher};
use crate::retirement_intent::{self, FileRelease, PendingRetirement};

#[derive(Clone, Debug)]
pub struct GarbageConfig {
    pub blocks_to_keep: usize,
    pub garbage_lifetime: Duration,
    pub table_parallelism: usize,
    pub delete_parallelism: usize,
    pub chunk_size: usize,
    /// How long a reader on any node may begin provider requests after its catalog was confirmed current.
    /// The collector waits this long on its own clock after first observing a trie as garbage.
    pub reader_lease: Duration,
    /// Subtracted from a reader's lease: twice the bound on clock skew between nodes, plus the longest provider request.
    pub reader_lease_margin: Duration,
}

impl Default for GarbageConfig {
    fn default() -> Self { Self { blocks_to_keep: 10, garbage_lifetime: Duration::from_secs(24 * 60 * 60), table_parallelism: 8, delete_parallelism: 64, chunk_size: 1024,
        reader_lease: Duration::from_secs(60 * 60), reader_lease_margin: Duration::from_secs(60) } }
}

impl GarbageConfig {
    fn validate(&self) -> Result<()> {
        if self.blocks_to_keep == 0 || self.table_parallelism == 0 || self.delete_parallelism == 0 || self.chunk_size == 0 || self.chunk_size > 1024 {
            return Err(crate::invalid("Garbage collection requires positive retention and concurrency, with at most 1024 trie keys per chunk"));
        }
        if self.reader_lease_margin >= self.reader_lease && !(self.reader_lease.is_zero() && self.reader_lease_margin.is_zero()) {
            return Err(crate::invalid("The reader lease must exceed its margin; a zero lease admits no provider request from another node"));
        }
        self.reader_lease_micros()?;
        self.reader_lease_margin_micros()?;
        Ok(())
    }
    pub fn reader_lease_micros(&self) -> Result<i64> { micros(self.reader_lease) }
    pub fn reader_lease_margin_micros(&self) -> Result<i64> { micros(self.reader_lease_margin) }
}

fn micros(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_micros()).map_err(|_| crate::invalid("Reader lease exceeds the timestamp range"))
}

pub trait CollectionObserver: Send + Sync {
    fn table_failed(&self, table: Option<&TableRef>, error: &Anomaly);
}

pub fn block_is_garbage(block: BlockIndex, latest: BlockIndex, keep: usize) -> bool {
    crate::retention::block_is_eligible(block.0, latest.0, keep)
}

/// Monotonic microseconds; the collector measures only the time elapsed since it first observed a trie.
pub type CollectorClock = Arc<dyn Fn() -> i64 + Send + Sync>;

pub struct TrieCollector {
    clock: CollectorClock,
    /// When this collector first saw each trie as eligible; a new collector starts empty and waits afresh.
    first_observed: Mutex<BTreeMap<(TableRef, TrieKey), i64>>,
    first_directory_observed: Mutex<BTreeMap<(TableRef, rtdb_format::cow_tree::PageDigest), i64>>,
    first_term_page_observed: Mutex<BTreeMap<(TableRef, rtdb_format::cow_tree::PageDigest), i64>>,
    directory: Option<(Arc<dyn ArtefactPublisher>, rtdb_types::StorageVersion, rtdb_types::StorageEpoch)>,
    first_search_observed: Mutex<BTreeMap<(TableRef, [u8; 32]), i64>>,
    search: Option<(Arc<dyn ArtefactPublisher>, rtdb_types::StorageVersion, rtdb_types::StorageEpoch)>,
    retirements: Option<Arc<dyn TableRetirements>>,
    store: Arc<dyn ObjectStore>,
    partition: Arc<PartitionState>,
    publisher: Arc<dyn TrieDeletionPublisher>,
    observer: Arc<dyn CollectionObserver>,
    config: GarbageConfig,
}

impl TrieCollector {
    pub fn new(store: Arc<dyn ObjectStore>, partition: Arc<PartitionState>, publisher: Arc<dyn TrieDeletionPublisher>,
        observer: Arc<dyn CollectionObserver>, config: GarbageConfig, clock: CollectorClock) -> Result<Self> {
        config.validate()?;
        Ok(Self { clock, first_observed: Mutex::default(), first_search_observed: Mutex::default(), search: None,
            first_directory_observed: Mutex::default(), first_term_page_observed: Mutex::default(), directory: None, retirements: None, store, partition, publisher, observer, config })
    }

    pub fn with_search(mut self, publisher: Arc<dyn ArtefactPublisher>, version: rtdb_types::StorageVersion, epoch: rtdb_types::StorageEpoch) -> Self {
        self.search = Some((publisher, version, epoch));
        self
    }

    pub fn with_retirements(mut self, retirements: Arc<dyn TableRetirements>) -> Self {
        self.retirements = Some(retirements);
        self
    }

    /// Publishes the retirement of every due table, in chunks; their tries become garbage for later cycles to collect.
    async fn retire_due_tables(&self, retirements: &dyn TableRetirements, cancellation: &CancellationToken) -> Result<()> {
        let Some((tables, as_of)) = retirements.due(cancellation).await? else { return Ok(()); };
        for chunk in tables.chunks(self.config.chunk_size) {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            retirements.retire(chunk.to_vec(), as_of).await?;
        }
        Ok(())
    }

    pub fn with_entity_directories(mut self, publisher: Arc<dyn ArtefactPublisher>, version: rtdb_types::StorageVersion,
        epoch: rtdb_types::StorageEpoch) -> Self {
        self.directory = Some((publisher, version, epoch));
        self
    }

    /// Keeps the eligible tries whose reader lease has elapsed since this collector first observed them.
    fn leased_out(&self, table: &TableRef, eligible: Vec<TrieKey>) -> Result<Vec<TrieKey>> {
        let lease = self.config.reader_lease_micros()?;
        let now = (self.clock)();
        let mut first_observed = self.first_observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let current: BTreeSet<&TrieKey> = eligible.iter().collect();
        first_observed.retain(|(observed, key), _| observed != table || current.contains(key));
        drop(current);
        Ok(eligible.into_iter().filter(|key| {
            let first = *first_observed.entry((table.clone(), key.clone())).or_insert(now);
            rtdb_storage::read_lease::collection_waited(first, lease, now)
        }).collect())
    }

    pub async fn default_garbage_as_of(&self) -> Result<Option<Timestamp>> {
        IoRegion::reading_recovery(self.store.as_ref(), async {
            let prefix = ObjectKey::new("blocks").map_err(crate::invalid)?;
            let blocks: Vec<_> = self.store.list(Some(&prefix)).await.map_err(crate::invalid)?.into_iter().filter(|block| block.key.as_str().ends_with(".binpb")).collect();
            let Some(index) = blocks.len().checked_sub(self.config.blocks_to_keep) else { return Ok(None); };
            let block = BlockCatalog::decode(&self.store.get(&blocks[index].key).await.map_err(crate::invalid)?)?;
            let Some(tx) = block.latest_completed_tx() else { return Ok(None); };
            let duration = i64::try_from(self.config.garbage_lifetime.as_nanos().div_ceil(1000)).map_err(crate::invalid)?;
            Ok(Some(Timestamp(tx.system_time.0.checked_sub(duration).ok_or_else(|| crate::invalid("Garbage lifetime exceeds timestamp range"))?)))
        }).await
    }

    pub async fn collect_as_of(&self, as_of: Timestamp, cancellation: &CancellationToken) -> Result<()> {
        self.collect_cycle(Some(as_of), cancellation).await
    }

    async fn collect_cycle(&self, as_of: Option<Timestamp>, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let collection = Arc::new(self.partition.trie_collection());
        let cut = collection.current().snapshot().catalog_cut();
        if self.store.defers_trie_releases() && cut.is_none() { return Ok(()); }
        let release = FileRelease {
            slots: Arc::new(Semaphore::new(self.config.delete_parallelism)),
            cancellation: cancellation.clone(),
        };
        if let Some(cut) = cut
            && let Err(error) = IoRegion::retaining(collection.clone(), retirement_intent::resume(
                self.store.as_ref(), self.publisher.as_ref(), collection.current(), cut, &release,
            )).await
        {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.observer.table_failed(None, &error);
            return Ok(());
        }
        if let Some(retirements) = &self.retirements
            && let Err(error) = self.retire_due_tables(retirements.as_ref(), cancellation).await
        {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.observer.table_failed(None, &error);
        }
        let Some(as_of) = as_of else { return Ok(()); };
        let oldest_checkpoint = OnceCell::new();
        let tables: Vec<_> = collection.current().table_catalog().entries().cloned().collect();
        let tasks = stream::iter(tables).map(|entry| {
            let release = release.clone();
            let collection = Arc::clone(&collection);
            let oldest_checkpoint = &oldest_checkpoint;
            async move {
                let mut eligible = collection.eligible(&entry.table, as_of);
                let receipt_l0 = self.store.releases_receipt_l0() && entry.table.stores_expiring_transaction_metadata();
                if receipt_l0 {
                    eligible.extend(collection.eligible_receipt_l0(&entry.table, as_of).cloned());
                } else {
                    eligible.extend(collection.eligible_l0_artefacts(&entry.table, as_of).cloned());
                }
                let coordinated = self.store.defers_trie_releases();
                let (due, timed): (Vec<_>, Vec<_>) = eligible.into_iter()
                    .partition(|key| coordinated && (key.level() > 0 || receipt_l0));
                let timed = match self.leased_out(&entry.table, timed) {
                    Ok(ready) => ready,
                    Err(error) => { self.observer.table_failed(Some(&entry.table), &error); return; }
                };
                let (seals, keys): (Vec<_>, Vec<_>) = due.into_iter().chain(timed)
                    .partition(|key| key.level() == 0 && !receipt_l0);
                let groups = finished_groups(collection.current().trie_catalog(), &entry.table, &keys);
                let result = IoRegion::retaining(collection.clone(), async {
                    self.collect_table(&entry, &keys, &groups, cut, collection.current().trie_catalog(), &release).await?;
                    self.collect_search_seals(&entry, &seals, cut, collection.current().trie_catalog(), cancellation).await?;
                    self.collect_pending_search(&entry, collection.current().trie_catalog(), collection.current().table_catalog().block_index(), cancellation).await?;
                    self.collect_search_runs(&entry, &collection, as_of, cut, cancellation).await?;
                    self.prune_search_tombstones(&entry, &collection, cut, oldest_checkpoint, cancellation).await?;
                    self.collect_entity_directory_pages(&entry, &collection, cut, cancellation).await?;
                    self.collect_term_directory_pages(&entry, &collection, cut, cancellation).await
                }).await;
                if let Err(error) = &result { if !cancellation.is_cancelled() { self.observer.table_failed(Some(&entry.table), error); } }
            }
        });
        tasks.buffer_unordered(self.config.table_parallelism).for_each(|_| async {}).await;
        if cancellation.is_cancelled() { Err(crate::closed()) } else { Ok(()) }
    }

    async fn collect_entity_directory_pages(&self, entry: &TableEntry, collection: &rtdb_index::TrieCollection,
        cut: Option<CatalogCut>, cancellation: &CancellationToken) -> Result<()> {
        use rtdb_format::{cow_tree, entity_directory, log::proto::EntityDirectoryChange};
        let Some((publisher, version, epoch)) = &self.directory else { return Ok(()); };
        if !self.store.supports_directory_page_release() { return Ok(()); }
        let table = &entry.table;
        let catalog = collection.current().trie_catalog();
        let retired = catalog.retired_entity_directories(table).map(|receipt| Ok((receipt, receipt.reference()?)))
            .collect::<Result<Vec<_>>>()?;
        if retired.is_empty() { return Ok(()); }
        let mut held = collection.held_entity_directories(table).map(|root| root.reference()).collect::<Result<Vec<_>>>()?;
        if let Some(latest) = collection.current().table_catalog().block_index() {
            let directory = ObjectKey::new(format!("{}/blocks", entry.slug.table_prefix()))?;
            for checkpoint in self.store.list(Some(&directory)).await? {
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                let Ok(block) = block_index_from_key(checkpoint.key.as_str()) else { continue; };
                if block >= latest { continue; }
                let size = usize::try_from(checkpoint.size).map_err(crate::invalid)?;
                let bytes = match self.store.get_fresh_exact(&checkpoint.key, size).await {
                    Ok(bytes) => bytes,
                    Err(rtdb_storage::Error::MissingObject(_)) => continue,
                    Err(error) => return Err(error.into()),
                };
                let checkpoint = rtdb_format::catalog::TableBlock::decode(&bytes)?;
                if let Some(root) = &checkpoint.as_proto().entity_directory {
                    held.push(root.reference()?);
                }
            }
        }
        let roots: BTreeSet<_> = held.iter().map(|root| root.digest).collect();
        let now = (self.clock)();
        let lease = self.config.reader_lease_micros()?;
        let eligible = {
            let candidates: BTreeSet<_> = retired.iter().filter(|(_, root)| !roots.contains(&root.digest)).map(|(_, root)| root.digest).collect();
            let mut first = self.first_directory_observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            first.retain(|(recorded, digest), _| recorded != table || candidates.contains(digest));
            candidates.into_iter().filter(|&digest| {
                let observed = *first.entry((table.clone(), digest)).or_insert(now);
                rtdb_storage::read_lease::collection_waited(observed, lease, now)
            }).collect::<BTreeSet<_>>()
        };
        if eligible.is_empty() { return Ok(()); }
        let prefix = entity_directory::prefix(&entry.slug);
        let pages = rtdb_storage::cow_pages::FreshPages { store: self.store.as_ref(), prefix: &prefix };
        let mut reachable = BTreeSet::new();
        let leased = retired.iter().map(|(_, root)| root).filter(|root| !eligible.contains(&root.digest));
        for (root, retired) in held.iter().map(|root| (root, false)).chain(leased.map(|root| (root, true))) {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            if !reachable.contains(&root.digest) {
                // A collector that restarted sees a partly released root as newly retired.
                let pages = if retired { cow_tree::reachable_present(&pages, root).await? } else { cow_tree::reachable(&pages, root).await? };
                reachable.extend(pages);
            }
        }
        let mut entries = Vec::new();
        for (receipt, root) in retired.into_iter().filter(|(_, root)| eligible.contains(&root.digest)) {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let keys = cow_tree::retirement_order(&pages, &root, &reachable).await?.into_iter()
                .map(|digest| ObjectKey::new(cow_tree::page_key(&prefix, digest))).collect::<std::result::Result<Vec<_>, _>>()?;
            if self.store.release_entity_directory_pages(&keys, cut).await? < keys.len() { continue; }
            let key = TrieKey::l0(BlockIndex(receipt.through_block()))?;
            let record = EntityDirectoryChange { root: Some(receipt.clone()), replaces_root: None }.to_artefact()?;
            entries.push(ArtefactEntry { trie_key: Some(key.to_string()), record: Some(record) });
            if entries.len() >= self.config.chunk_size { break; }
        }
        if !entries.is_empty() {
            publisher.remove(ArtefactsRemoved { storage_version: Some(version.0), storage_epoch: Some(epoch.0),
                table_name: Some(table.schema_and_table()), entries, source_msg_id: None }).await?;
        }
        Ok(())
    }

    // Each build writes fresh digests, so a sweep candidate cannot acquire a new reference through a rebuild of the same rows.
    // The reader lease gives newly uploaded pages time to become reachable through the published root.
    async fn collect_term_directory_pages(&self, entry: &TableEntry, collection: &rtdb_index::TrieCollection,
        cut: Option<CatalogCut>, cancellation: &CancellationToken) -> Result<()> {
        use rtdb_format::{cow_tree, term_directory};
        if self.search.is_none() { return Ok(()); }
        let table = &entry.table;
        let prefix = term_directory::page_prefix(&entry.slug);
        let listed = self.store.list(Some(&ObjectKey::new(format!("{prefix}/pages"))?)).await?;
        let stored: BTreeSet<_> = listed.iter().filter_map(|object| term_directory::page_digest(object.key.as_str())).collect();
        if stored.is_empty() { return Ok(()); }
        let Ok(mut held) = collection.held_term_directories(table).filter_map(|root| root.page_root().transpose()).collect::<Result<Vec<_>>>() else { return Ok(()); };
        if let Some(latest) = collection.current().table_catalog().block_index() {
            let directory = ObjectKey::new(format!("{}/blocks", entry.slug.table_prefix()))?;
            for checkpoint in self.store.list(Some(&directory)).await? {
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                let Ok(block) = block_index_from_key(checkpoint.key.as_str()) else { continue; };
                if block >= latest { continue; }
                let size = usize::try_from(checkpoint.size).map_err(crate::invalid)?;
                let bytes = match self.store.get_fresh_exact(&checkpoint.key, size).await {
                    Ok(bytes) => bytes,
                    Err(rtdb_storage::Error::MissingObject(_)) => continue,
                    Err(error) => return Err(error.into()),
                };
                let checkpoint = rtdb_format::catalog::TableBlock::decode(&bytes)?;
                for root in &checkpoint.as_proto().term_directories {
                    match root.page_root() {
                        Ok(Some(root)) => held.push(root),
                        Ok(None) => {}
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
        let pages = rtdb_storage::cow_pages::FreshPages { store: self.store.as_ref(), prefix: &prefix };
        let mut reachable = BTreeSet::new();
        for root in &held {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            if !reachable.contains(&root.digest) { reachable.extend(cow_tree::reachable(&pages, root).await?); }
        }
        let now = (self.clock)();
        let lease = self.config.reader_lease_micros()?;
        let unreachable = {
            let mut first = self.first_term_page_observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            first.retain(|(recorded, digest), _| recorded != table || (stored.contains(digest) && !reachable.contains(digest)));
            stored.into_iter().filter(|digest| !reachable.contains(digest)).filter(|&digest| {
                let observed = *first.entry((table.clone(), digest)).or_insert(now);
                rtdb_storage::read_lease::collection_waited(observed, lease, now)
            }).collect::<Vec<_>>()
        };
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let keys = unreachable.into_iter().take(self.config.chunk_size)
            .map(|digest| ObjectKey::new(cow_tree::page_key(&prefix, digest))).collect::<std::result::Result<Vec<_>, _>>()?;
        self.store.release_entity_directory_pages(&keys, cut).await?;
        Ok(())
    }

    async fn collect_search_seals(&self, entry: &TableEntry, seals: &[TrieKey], cut: Option<CatalogCut>, catalog: &TrieCatalog,
        cancellation: &CancellationToken) -> Result<()> {
        if seals.is_empty() { return Ok(()); }
        if self.store.defers_trie_releases() {
            let Some(cut) = cut else { return Ok(()); };
            if !self.store.advance_catalog_floor(cut).await? { return Ok(()); }
        }
        let prefix = ObjectKey::new(entry.slug.artefacts_prefix())?;
        let seals: BTreeSet<_> = seals.iter().collect();
        for object in self.store.list(Some(&prefix)).await? {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let Some((ArtefactOwner::Trie(owner), kind, id, generation)) = entry.slug.parse_artefact_key(object.key.as_str()) else { continue; };
            if !seals.contains(&owner) { continue; }
            if kind == rtdb_format::entity_history::KIND || (kind == "search" && id.ends_with("f16w")) {
                if let Some(record) = catalog.trie(&entry.table, &owner).and_then(|trie| trie.artefact(&kind, &id, generation))
                    && self.store.permits_search_release(&object.key, record.published_version.as_ref(), cut).await?
                {
                    self.store.release_search_run(&object.key, record.published_version.as_ref(), cut).await?;
                }
            } else if kind == "search" && (id.ends_with("f15") || id.ends_with("f16h")) {
                self.store.delete(&object.key).await?;
            }
        }
        Ok(())
    }

    async fn collect_pending_search(&self, entry: &TableEntry, catalog: &TrieCatalog, durable: Option<BlockIndex>,
        cancellation: &CancellationToken) -> Result<()> {
        use rtdb_format::search_manifest::{MAX_PENDING_BYTES, SearchStagingOwner, StagedSearchRun, pending_prefix};
        use rtdb_storage::object_store::DrainedSearchUpload;
        let Some((publisher, version, epoch)) = &self.search else { return Ok(()); };
        let prefix = ObjectKey::new(pending_prefix(&entry.slug))?;
        let objects = self.store.list(Some(&prefix)).await?;
        let known = catalog.search_runs(&entry.table).chain(catalog.retired_search_runs(&entry.table).filter_map(|item| item.run.as_ref()))
            .map(|run| ObjectKey::new(run.pending_key(&entry.slug)?).map_err(Anomaly::from)).collect::<Result<BTreeSet<_>>>()?;
        let mut abandoned = Vec::new();
        for intent in objects {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            if known.contains(&intent.key) {
                self.store.delete(&intent.key).await?;
                continue;
            }
            let Ok(size) = usize::try_from(intent.size) else { continue; };
            if size > MAX_PENDING_BYTES { continue; }
            let bytes = match self.store.get_fresh_exact(&intent.key, size).await {
                Ok(bytes) => bytes,
                Err(rtdb_storage::Error::MissingObject(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            let Ok(StagedSearchRun { mut run, owner: Some(owner) }) = StagedSearchRun::decode(&bytes) else { continue; };
            if run.pending_key(&entry.slug)? != intent.key.as_str()
                || durable.is_none_or(|boundary| run.blocks.last().is_none_or(|last| *last > boundary.0)) {
                continue;
            }
            let body = ObjectKey::new(run.object_key(&entry.slug)?)?;
            match self.store.head_drained_search_upload(&body, owner).await? {
                Some(DrainedSearchUpload::Present(object)) if object.size == run.file_size() as u64 => {}
                Some(DrainedSearchUpload::Absent) => {
                    self.store.delete(&intent.key).await?;
                    continue;
                }
                _ => continue,
            }
            run.publication = self.store.retirement_publication(&body).await?.map(Into::into);
            let local = matches!(owner, SearchStagingOwner::Local { .. }) && self.store.local_maintenance_store().is_some();
            if run.publication.is_none() && !local { continue; }
            let trie = TrieKey::l0(BlockIndex(run.blocks[0]))?.to_string();
            let record = SearchRunChange { run: Some(run), retire_only: Some(true), abandon: Some(true), ..Default::default() }.to_artefact()?;
            abandoned.push(ArtefactEntry { trie_key: Some(trie), record: Some(record) });
            if abandoned.len() >= self.config.chunk_size { break; }
        }
        if !abandoned.is_empty() {
            publisher.publish(ArtefactsAdded { storage_version: Some(version.0), storage_epoch: Some(epoch.0),
                table_name: Some(entry.table.schema_and_table()), entries: abandoned, source_msg_id: None }).await?;
        }
        Ok(())
    }

    async fn collect_search_runs(&self, entry: &TableEntry, collection: &rtdb_index::TrieCollection, as_of: Timestamp,
        cut: Option<CatalogCut>, cancellation: &CancellationToken) -> Result<()> {
        let Some((publisher, version, epoch)) = &self.search else { return Ok(()); };
        let eligible: BTreeMap<_, _> = collection.eligible_search_runs(&entry.table, as_of)
            .filter_map(|run| run.identity().ok().map(|id| (id, run))).collect();
        let ready: Vec<_> = if self.store.releases_exact_search_runs() {
            eligible.into_values().collect()
        } else {
            let lease = self.config.reader_lease_micros()?;
            let now = (self.clock)();
            let mut first = self.first_search_observed.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            first.retain(|(table, digest), _| table != &entry.table || eligible.contains_key(digest));
            eligible.into_iter().filter_map(|(digest, run)| {
                let first = *first.entry((entry.table.clone(), digest)).or_insert(now);
                rtdb_storage::read_lease::collection_waited(first, lease, now).then_some(run)
            }).collect()
        };
        let mut entries = Vec::new();
        for run in ready {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let key = ObjectKey::new(run.object_key(&entry.slug)?)?;
            if !self.store.permits_search_release(&key, run.publication.as_ref(), cut).await? { continue; }
            if !self.store.release_search_run(&key, run.publication.as_ref(), cut).await? { continue; }
            let trie_key = TrieKey::l0(rtdb_types::BlockIndex(run.blocks[0]))?.to_string();
            let record = SearchRunChange { run: Some(run.clone()), retire_only: Some(true), ..Default::default() }.to_artefact()?;
            entries.push(ArtefactEntry { trie_key: Some(trie_key), record: Some(record) });
            if entries.len() >= self.config.chunk_size { break; }
        }
        if !entries.is_empty() {
            publisher.remove(ArtefactsRemoved { storage_version: Some(version.0), storage_epoch: Some(epoch.0),
                table_name: Some(entry.table.schema_and_table()), entries, source_msg_id: None }).await?;
        }
        Ok(())
    }

    async fn oldest_search_checkpoint(&self, latest: BlockIndex, cut: CatalogCut) -> Result<Option<(BlockIndex, CatalogCut)>> {
        IoRegion::reading_recovery(self.store.as_ref(), async {
            let directory = ObjectKey::new("blocks")?;
            let oldest = self.store.list(Some(&directory)).await?.into_iter().filter_map(|object| {
                let block = block_index_from_key(object.key.as_str()).ok()?;
                (block <= latest).then_some((block, object.key))
            }).min_by_key(|(block, _)| *block);
            let Some((oldest, key)) = oldest else { return Ok(None); };
            let bytes = match self.store.get_fresh(&key).await {
                Ok(bytes) => bytes,
                Err(rtdb_storage::Error::MissingObject(_)) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            let catalog = BlockCatalog::decode(&bytes)?;
            let Some(through) = catalog.boundary_replica_position() else { return Ok(None); };
            Ok((catalog.block_index() == oldest && through.0 >= 0 && through.epoch() == cut.replica_epoch && through <= cut.through)
                .then_some((oldest, CatalogCut { through, ..cut })))
        }).await
    }

    async fn prune_search_tombstones(&self, entry: &TableEntry, collection: &rtdb_index::TrieCollection, cut: Option<CatalogCut>,
        oldest_checkpoint: &OnceCell<Option<(BlockIndex, CatalogCut)>>, cancellation: &CancellationToken) -> Result<()> {
        let Some((publisher, version, epoch)) = &self.search else { return Ok(()); };
        let Some((cut, latest)) = cut.zip(collection.current().table_catalog().block_index()) else { return Ok(()); };
        let mut entries = Vec::new();
        for retired in collection.eligible_search_tombstones(&entry.table) {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let Some(run) = retired.run.as_ref().filter(|run| run.allocation.is_some()) else { continue; };
            if let Some(settled_at) = retired.settled_at_replica_msg_id {
                let Some((block, checkpoint)) = *oldest_checkpoint.get_or_try_init(|| self.oldest_search_checkpoint(latest, cut)).await?
                    else { continue; };
                if checkpoint.through.0 <= settled_at { continue; }
                let first_in_journal = ReplicaPosition::from_epoch_offset(checkpoint.replica_epoch, 0)?;
                let floor = CatalogCut { through: ReplicaPosition(settled_at + 1).max(first_in_journal), ..checkpoint };
                let key = ObjectKey::new(run.object_key(&entry.slug)?)?;
                if !self.store.permits_search_tombstone_prune(&key, run.publication.as_ref(), block, floor).await? { continue; }
            }
            let intent = ObjectKey::new(run.pending_key(&entry.slug)?)?;
            match self.store.head_fresh(&intent).await {
                Err(rtdb_storage::Error::MissingObject(_)) => (),
                Ok(_) => continue,
                Err(error) => return Err(error.into()),
            }
            let trie_key = TrieKey::l0(BlockIndex(run.blocks[0]))?.to_string();
            let record = SearchRunChange { run: Some(run.clone()), retire_only: Some(true),
                prune_settled_at: retired.settled_at_replica_msg_id, ..Default::default() }.to_artefact()?;
            entries.push(ArtefactEntry { trie_key: Some(trie_key), record: Some(record) });
            if entries.len() >= self.config.chunk_size { break; }
        }
        if !entries.is_empty() {
            publisher.remove(ArtefactsRemoved { storage_version: Some(version.0), storage_epoch: Some(epoch.0),
                table_name: Some(entry.table.schema_and_table()), entries, source_msg_id: None }).await?;
        }
        Ok(())
    }

    async fn collect_table(&self, entry: &TableEntry, keys: &[TrieKey], groups: &BTreeSet<PartGroup>, cut: Option<CatalogCut>, catalog: &TrieCatalog, release: &FileRelease) -> Result<()> {
        let deletes = release.slots.as_ref();
        let cancellation = &release.cancellation;
        if keys.iter().any(|key| key.level() == 0)
            && !(self.store.releases_receipt_l0() && entry.table.stores_expiring_transaction_metadata())
        {
            return Err(crate::invalid("This store cannot release L0 recovery files for the table"));
        }
        let chunk_size = self.config.chunk_size;
        // A shared file belongs in the receipt of the last batch removing members
        // of its group. Keep group releases before releases of their data files.
        let last_group_batch: BTreeMap<_, _> = keys.iter().enumerate()
            .filter_map(|(index, key)| key.part_group().filter(|group| groups.contains(group)).map(|group| (group, index / chunk_size)))
            .collect();
        let mut group_files: BTreeMap<usize, Vec<ObjectKey>> = BTreeMap::new();
        for (group, batch) in last_group_batch {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let prefix = ObjectKey::new(entry.slug.group_artefact_prefix(&group))?;
            group_files.entry(batch).or_default().extend(self.store.list(Some(&prefix)).await?.into_iter().map(|file| file.key));
        }
        if !keys.is_empty() {
            let scheduled: BTreeMap<_, _> = keys.iter().enumerate().map(|(index, key)| (key, index / chunk_size)).collect();
            let prefix = ObjectKey::new(entry.slug.packs_prefix())?;
            for object in self.store.list(Some(&prefix)).await? {
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                let Some((ArtefactOwner::Pack(pack), _, _, _)) = entry.slug.parse_artefact_key(object.key.as_str()) else { continue; };
                let held = pack.members().iter().filter(|key| catalog.trie(&entry.table, key).is_some());
                let Some(batches) = held.map(|key| scheduled.get(key).copied()).collect::<Option<Vec<_>>>() else { continue; };
                let batch = batches.into_iter().max().unwrap_or((keys.len() - 1) / chunk_size);
                group_files.entry(batch).or_default().push(object.key);
            }
        }

        let mut batches = Vec::new();
        for (index, chunk) in keys.chunks(chunk_size).enumerate() {
            let trie_files = stream::iter(0..chunk.len()).map(|index| async move {
                let key = &chunk[index];
                let _permit = deletes.acquire().await.map_err(crate::invalid)?;
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                // Listing also catches artefacts whose records have yet to reach this catalog.
                let prefix = ObjectKey::new(entry.slug.artefact_prefix(key))?;
                let mut files: Vec<_> = self.store.list(Some(&prefix)).await?.into_iter().map(|object| object.key).collect();
                files.push(ObjectKey::new(entry.slug.data_key(key))?);
                files.push(ObjectKey::new(entry.slug.metadata_key(key))?);
                Ok::<_, Anomaly>(files)
            }).buffered(self.config.delete_parallelism).collect::<Vec<_>>().await;
            let trie_files = trie_files.into_iter().collect::<Result<Vec<_>>>()?;
            let intent = if let Some(cut) = cut {
                let keys: Vec<_> = group_files.get(&index).into_iter().flatten().chain(trie_files.iter().flatten()).cloned().collect();
                let mut published = BTreeMap::new();
                for key in chunk {
                    let Some(files) = catalog.trie(&entry.table, key).and_then(|trie| trie.published_files.as_ref()) else { continue };
                    if let Some(data) = &files.data { published.insert(ObjectKey::new(entry.slug.data_key(key))?, data); }
                    if let Some(metadata) = &files.metadata { published.insert(ObjectKey::new(entry.slug.metadata_key(key))?, metadata); }
                }
                let files = stream::iter(keys).map(|key| {
                    let version = published.get(&key).copied()
                        .or_else(|| published_artefact(catalog, entry, &key)).cloned();
                    async move {
                        let _permit = deletes.acquire().await.map_err(crate::invalid)?;
                        if cancellation.is_cancelled() { return Err(crate::closed()); }
                        retirement_intent::observe_file(self.store.as_ref(), &key, version).await
                    }
                }).buffered(self.config.delete_parallelism).collect::<Vec<_>>().await;
                let files = files.into_iter().collect::<Result<Vec<_>>>()?;
                Some(PendingRetirement::stage(self.store.as_ref(), cut, &entry.table, chunk, files).await?)
            } else { None };
            batches.push((chunk, trie_files, intent));
        }

        let deferred = self.store.defers_trie_releases();
        if !deferred {
            for key in group_files.values().flatten() {
                if cancellation.is_cancelled() { return Err(crate::closed()); }
                self.store.delete(key).await?;
            }
        }
        for (chunk, trie_files, intent) in batches {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            if !deferred {
                let results = stream::iter(trie_files).map(|files| async move {
                    let _permit = deletes.acquire().await.map_err(crate::invalid)?;
                    if cancellation.is_cancelled() { return Err(crate::closed()); }
                    for file in files { self.store.delete(&file).await?; }
                    Ok::<_, Anomaly>(())
                }).buffer_unordered(self.config.delete_parallelism).collect::<Vec<_>>().await;
                for result in results { result?; }
            }
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            let position = self.publisher.publish(entry.table.clone(), chunk.to_vec()).await?;
            if let Some(intent) = intent { intent.finish(self.store.as_ref(), position, release).await?; }
        }
        Ok(())
    }
}

fn published_artefact<'a>(catalog: &'a TrieCatalog, entry: &TableEntry, key: &ObjectKey) -> Option<&'a PublishedObjectVersion> {
    let (owner, kind, id, version) = entry.slug.parse_artefact_key(key.as_str())?;
    let mut versions = owner.tries().into_iter()
        .filter_map(|key| {
            let record = catalog.trie(&entry.table, &key)?.artefact(&kind, &id, version)?;
            (ArtefactOwner::of(&key, record).ok().as_ref() == Some(&owner)).then_some(record)
        })
        .map(|record| record.published_version.as_ref());
    let first = versions.next().flatten()?;
    versions.all(|version| version == Some(first)).then_some(first)
}

/// The partition groups of tries being collected none of whose members the catalog holds besides them: their artefacts serve no trie once these go.
fn finished_groups(catalog: &TrieCatalog, table: &TableRef, keys: &[TrieKey]) -> BTreeSet<PartGroup> {
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
        self.collect_cycle(self.default_garbage_as_of().await?, cancellation).await
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

    async fn sweep(&self, prefix: &str, block_index: fn(&str) -> Option<BlockIndex>, latest: BlockIndex,
        cancellation: &CancellationToken, deletes: &Semaphore) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let prefix = ObjectKey::new(prefix).map_err(crate::invalid)?;
        let objects = self.store.list(Some(&prefix)).await.map_err(crate::invalid)?;
        let objects = objects.into_iter().filter(|object| {
            block_index(object.key.as_str()).is_some_and(|index| block_is_garbage(index, latest, self.config.blocks_to_keep))
        });
        let results = stream::iter(objects).map(|object| async move {
            let _permit = deletes.acquire().await.map_err(crate::invalid)?;
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.store.collect_recovery_file(&object.key).await
        }).buffer_unordered(self.config.delete_parallelism).collect::<Vec<Result<()>>>().await;
        for result in results { result?; }
        Ok(())
    }
}

#[async_trait]
impl CycleWorker for BlockCollector {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let snapshot = self.snapshots.snapshot();
        let Some(latest) = snapshot.table_catalog().block_index() else { return Ok(()); };
        let deletes = Arc::new(Semaphore::new(self.config.delete_parallelism));
        let catalog_block = |key: &str| block_index_from_key(key).ok();
        if let Err(error) = self.sweep("blocks", catalog_block, latest, cancellation, &deletes).await {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.observer.table_failed(None, &error);
        }
        if self.store.queues_offloaded_payload_reclamation()
            && let Err(error) = self.sweep("oversized", oversized_block_index, latest, cancellation, &deletes).await
        {
            if cancellation.is_cancelled() { return Err(crate::closed()); }
            self.observer.table_failed(None, &error);
        }
        let entries: Vec<_> = snapshot.table_catalog().entries().cloned().collect();
        stream::iter(entries).map(|entry| {
            let deletes = Arc::clone(&deletes);
            async move {
            if let Err(error) = self.sweep(&format!("{}/blocks", entry.slug.table_prefix()), catalog_block, latest, cancellation, &deletes).await
                    && !cancellation.is_cancelled() {
                    self.observer.table_failed(Some(&entry.table), &error);
                }
            }
        }).buffer_unordered(self.config.table_parallelism).for_each(|_| async {}).await;
        if cancellation.is_cancelled() { Err(crate::closed()) } else { Ok(()) }
    }
}
