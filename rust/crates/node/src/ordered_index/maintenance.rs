use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    io::Cursor,
    sync::Arc,
};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::data::EventBatch;
use rtdb_index::Snapshot;
use rtdb_maintenance::{
    CycleWorker, SnapshotSource,
    input::read_page,
};
use rtdb_replication::{PartitionLogs, RetainedTableReplay};
use rtdb_storage::{HeldView, IoRegion, ObjectKey, cache::BufferPool};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Result, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

use super::{
    Definition, INDEX_ROOT, checkpoint, definitions, format_error,
    projection::{Changes, EventOrder, KeyLayout, RevisionRoots},
    replay_coverage::{classify_replay_block, replay_complete},
    tree::{Address, AddressRef, Root, Tree},
};

pub(crate) struct OrderedIndexMaintenance {
    database: String,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    clock: Arc<dyn crate::Clock>,
    snapshots: Arc<dyn SnapshotSource>,
    logs: Arc<PartitionLogs>,
    arrow_references: bool,
}

impl OrderedIndexMaintenance {
    pub(crate) fn new(
        database: String,
        storage: Arc<BufferPool>,
        memory: Arc<MemoryBudget>,
        clock: Arc<dyn crate::Clock>,
        snapshots: Arc<dyn SnapshotSource>,
        logs: Arc<PartitionLogs>,
        arrow_references: bool,
    ) -> Self {
        Self {
            database,
            storage,
            memory,
            clock,
            snapshots,
            logs,
            arrow_references,
        }
    }

    async fn refresh(&self, snapshot: &Snapshot, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let now = snapshot
            .tx_basis()
            .map_or_else(|| self.clock.now(), |basis| basis.system_time);
        let definitions = definitions(
            &self.database,
            snapshot,
            &self.storage,
            &self.memory,
            now.into(),
            cancellation,
        )
        .await?;
        let owned = super::owned_definitions(
            &self.database, snapshot, &self.storage, &self.memory, now.into(), cancellation,
        ).await?;
        let active = definitions.iter().chain(&owned);
        let directories = active.clone()
            .map(Definition::directory)
            .collect::<Result<BTreeSet<_>>>()?;
        if let Some(basis) = snapshot
            .tx_basis()
            .filter(|_| snapshot.contains_table(&TableRef::new(super::CATALOG.0, super::CATALOG.1)))
        {
            prune_dropped(&self.storage, &directories, basis.tx_id.0, cancellation).await?;
        }
        for definition in active {
            let directory = definition.directory()?;
            self.refresh_index(snapshot, definition, &directory, cancellation)
                .await?;
        }
        Ok(())
    }

    async fn refresh_index(
        &self,
        snapshot: &Snapshot,
        definition: &Definition,
        directory: &ObjectKey,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let result = self.refresh_from_files(snapshot, definition, directory, cancellation).await;
        match result {
            Err(error) if error.code == "rtdb.ordered-index/missing-source" => {
                self.refresh_from_journal(snapshot, definition, directory, cancellation).await?.ok_or(error)
            }
            result => result,
        }
    }

    async fn refresh_from_files(
        &self,
        snapshot: &Snapshot,
        definition: &Definition,
        directory: &ObjectKey,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let through = source_frontier(snapshot, &definition.table);
        if through < 0 {
            return Ok(());
        }
        let expected = source_rows(snapshot, &definition.table);
        if let Ok(Some(checkpoint)) =
            checkpoint::latest(&self.storage, directory, &self.memory, through).await
            && checkpoint.source_rows <= expected
            && let Ok(mut tree) = Tree::new(&self.storage, directory, &self.memory, checkpoint.scope).await
        {
            tree.arrow_references = self.arrow_references;
            if checkpoint.through == through && checkpoint.source_rows == expected {
                match checkpoint.collect(&tree, cancellation).await {
                    Ok(()) => return Ok(()),
                    Err(error) if error.category == AnomalyCategory::Interrupted => {
                        return Err(error);
                    }
                    Err(_) => (),
                }
            } else {
                let sources =
                    source_files(snapshot, &definition.table, checkpoint.through, through)?;
                let cuts = erase_cuts(&self.storage, &self.memory, &sources, cancellation).await?;
                if cuts.cuts.scope == 0 {
                    let mut roots = checkpoint.roots();
                    let advanced =
                        replay_flushed(&tree, definition, &sources, &cuts, &mut roots, cancellation).await;
                    match advanced {
                        Ok(rows) if checkpoint.source_rows.checked_add(rows) == Some(expected) => {
                            let next = checkpoint::publish(
                                &tree,
                                through,
                                expected,
                                roots.archived,
                                cancellation,
                            )
                            .await?;
                            return next.finish(&tree, cancellation).await;
                        }
                        Err(error) if error.category == AnomalyCategory::Interrupted => {
                            return Err(error);
                        }
                        Ok(_) | Err(_) => (),
                    }
                }
            }
        }
        let sources = source_files(snapshot, &definition.table, -1, through)?;
        let cuts = erase_cuts(&self.storage, &self.memory, &sources, cancellation).await?;
        let mut tree = Tree::new(&self.storage, directory, &self.memory, cuts.cuts.scope).await?;
        tree.arrow_references = self.arrow_references;
        checkpoint::request_collection(&tree, through).await?;
        let mut roots = RevisionRoots::default();
        let rows =
            replay_flushed(&tree, definition, &sources, &cuts, &mut roots, cancellation).await?;
        if rows != expected {
            return Err(incomplete_source());
        }
        let checkpoint =
            checkpoint::publish(&tree, through, expected, roots.archived, cancellation).await?;
        checkpoint.collect(&tree, cancellation).await
    }

    async fn refresh_from_journal(
        &self,
        snapshot: &Snapshot,
        definition: &Definition,
        directory: &ObjectKey,
        cancellation: &CancellationToken,
    ) -> Result<Option<()>> {
        let Some(target) = snapshot.table_catalog().block() else { return Ok(None); };
        let Some(mut replay) = RetainedTableReplay::open(&self.logs, target, &definition.table).await? else { return Ok(None); };
        let through = source_frontier(snapshot, &definition.table);
        let expected = source_rows(snapshot, &definition.table);
        let sources = source_files(snapshot, &definition.table, -1, through)?;
        let mut cuts = ErasureCuts::default();
        let mut coverage = ReplayCoverage::new(&sources);
        while let Some((block, rows)) = replay.next_block(cancellation, |block, batch| {
            cuts.record(block.0, batch, &self.memory, cancellation)
        }).await? {
            coverage.observe(block.0, rows)?;
        }
        coverage.finish(expected)?;

        let mut tree = Tree::new(&self.storage, directory, &self.memory, cuts.scope).await?;
        tree.arrow_references = self.arrow_references;
        checkpoint::request_collection(&tree, through).await?;
        let mut replay = RetainedTableReplay::open(&self.logs, target, &definition.table).await?.ok_or_else(incomplete_source)?;
        let mut coverage = ReplayCoverage::new(&sources);
        let mut roots = RevisionRoots::default();
        let mut changes = Changes::default();
        while let Some((block, rows)) = replay.next_block(cancellation, |_, batch| {
            changes.add_batch(batch, 0, &definition.columns, &tree.memory, cancellation, KeyLayout::for_table(&definition.table),
                |header, address| cuts.retains(header, address))
        }).await? {
            if coverage.observe(block.0, rows)? {
                apply_block(&tree, block.0, &mut roots, std::mem::take(&mut changes), cancellation).await?;
            }
        }
        coverage.finish(expected)?;
        let checkpoint = checkpoint::publish(&tree, through, expected, roots.archived, cancellation).await?;
        checkpoint.collect(&tree, cancellation).await?;
        Ok(Some(()))
    }
}

#[async_trait::async_trait]
impl CycleWorker for OrderedIndexMaintenance {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        let partition = self.snapshots.snapshot();
        let snapshot = partition.snapshot();
        let cut = snapshot.catalog_cut();
        let paths = cut.and_then(|_| snapshot.catalog_file_paths());
        let view = HeldView::catalog(self.storage.object_store().as_ref(), partition, cut, paths).await?;
        IoRegion::reading(view, self.refresh(&snapshot, cancellation)).await
    }
}

async fn read_flushed(
    storage: &BufferPool,
    memory: &Arc<MemoryBudget>,
    source: &SourceFile,
    cancellation: &CancellationToken,
    mut visit: impl FnMut(EventBatch, &Arc<MemoryReservation>, &Arc<MemoryReservation>) -> Result<()> + Send,
) -> Result<i64> {
    if cancellation.is_cancelled() {
        return Err(cancelled());
    }
    let store = storage.object_store();
    if store.head_fresh(&source.object).await.map_err(source_read_error)?.size != source.size as u64 {
        return Err(format_error("An ordered index source differs from the adopted file"));
    }
    let input_bytes = memory.reserve(source.size)?;
    let bytes = store.get_fresh(&source.object).await.map_err(source_read_error)?;
    if bytes.len() != source.size {
        return Err(format_error("An ordered index source file changed length"));
    }
    let layout = rtdb_format::ipc::inspect_file_reserved(&mut Cursor::new(&bytes), |bytes| memory.reserve(bytes))?;
    let _footer = memory.reserve(layout.opening_allocation_bound()?)?;
    let buffer = rtdb_columnar::retain_buffer_with_owner(&arrow_buffer::Buffer::from(bytes), input_bytes);
    let mut input = rtdb_format::ipc::ArrowFile::from_inspected_buffer(buffer, None, layout)?;
    let mut rows = 0_i64;
    for page in 0..input.page_count() {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let page = read_page(&mut input, page, memory)?;
        let batch = EventBatch::new(page.batch().clone())?;
        rows = rows
            .checked_add(i64::try_from(batch.len()).map_err(format_error)?)
            .ok_or_else(incomplete_source)?;
        visit(batch, page.reservation(), &_footer)?;
    }
    Ok(rows)
}

fn source_read_error(error: rtdb_storage::Error) -> Anomaly {
    if matches!(error, rtdb_storage::Error::MissingObject(_)) {
        return Anomaly::new(AnomalyCategory::NotFound, "rtdb.ordered-index/missing-source", error.to_string());
    }
    error.into()
}

#[derive(Default)]
struct ErasureCuts {
    scope: u64,
    by_address: BTreeMap<Address, EraseCut>,
}

type EraseCut = ((Timestamp, i128), Arc<MemoryReservation>);

impl ErasureCuts {
    fn record(&mut self, block: i64, batch: &EventBatch, memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<()> {
        batch.visit_erasures(|| {
            if cancellation.is_cancelled() { Err(cancelled()) } else { Ok(()) }
        }, |row, erased| {
            if erased.is_some() {
                let header = batch.header(row)?;
                self.scope = (block as u64 + 1).max(self.scope);
                let key = batch.entity_key(row)?.unwrap_or_default();
                let _scratch = memory.reserve(key.len().saturating_add(128))?;
                if !key.is_empty() { rtdb_columnar::EntityIdRef::from_handle_bytes(key)?; }
                let address = AddressRef { iid: header.iid, key };
                match self.by_address.entry(address.owned()) {
                    Entry::Occupied(mut entry) => {
                        let cut = &mut entry.get_mut().0;
                        *cut = (*cut).max(header.precedence());
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((header.precedence(), memory.reserve(key.len().saturating_add(128))?));
                    }
                }
            }
            Ok(())
        })
    }

    fn retains(&self, header: rtdb_types::EventHeader, address: AddressRef<'_>) -> bool {
        self.by_address.get(&address.owned()).is_none_or(|(cut, _)| header.precedence() >= *cut)
    }
}

struct ReplayInputs {
    cuts: ErasureCuts,
    decoded: Option<Vec<(i64, Vec<EventBatch>)>>,
    _charges: Vec<Arc<MemoryReservation>>,
}

async fn erase_cuts(
    storage: &BufferPool,
    memory: &Arc<MemoryBudget>,
    sources: &[SourceFile],
    cancellation: &CancellationToken,
) -> Result<ReplayInputs> {
    let mut inputs = ReplayInputs { cuts: ErasureCuts::default(), decoded: Some(Vec::new()), _charges: Vec::new() };
    let budget = (memory.available_bytes() / 8).min(32 << 20);
    let mut retained = 0usize;
    for source in sources {
        let mut pages = Vec::new();
        let mut footer = None;
        retained = retained.saturating_add(source.size);
        if retained > budget {
            inputs.decoded = None;
            inputs._charges.clear();
        }
        let mut visit = |batch, page: &Arc<MemoryReservation>, opening: &Arc<MemoryReservation>| {
            inputs.cuts.record(source.block, &batch, memory, cancellation)?;
            if inputs.decoded.is_some() {
                retained = retained.saturating_add(page.bytes());
                if footer.is_none() {
                    retained = retained.saturating_add(opening.bytes());
                    footer = Some(Arc::clone(opening));
                }
                if retained <= budget {
                    pages.push(batch);
                } else {
                    inputs.decoded = None;
                    inputs._charges.clear();
                    pages.clear();
                    footer = None;
                }
            }
            Ok(())
        };
        let rows = read_flushed(storage, memory, source, cancellation, &mut visit).await?;
        if let Some(decoded) = &mut inputs.decoded {
            decoded.push((rows, pages));
            inputs._charges.extend(footer);
        }
    }
    Ok(inputs)
}

async fn flushed_changes(
    tree: &Tree,
    definition: &Definition,
    source: &SourceFile,
    cancellation: &CancellationToken,
    retain: impl Fn(rtdb_types::EventHeader, AddressRef<'_>) -> bool + Send + Sync,
) -> Result<(i64, Changes)> {
    let mut changes = Changes::default();
    let rows = read_flushed(&tree.storage, &tree.memory, source, cancellation, |batch, _, _| {
        changes.add_batch(
            &batch,
            0,
            &definition.columns,
            &tree.memory,
            cancellation,
            KeyLayout::for_table(&definition.table),
            &retain,
        )
    })
    .await?;
    Ok((rows, changes))
}

async fn replay_flushed(
    tree: &Tree,
    definition: &Definition,
    sources: &[SourceFile],
    inputs: &ReplayInputs,
    roots: &mut RevisionRoots,
    cancellation: &CancellationToken,
) -> Result<i64> {
    let mut rows = 0_i64;
    let retain = |header: rtdb_types::EventHeader, address: AddressRef<'_>| inputs.cuts.retains(header, address);
    for (file, source) in sources.iter().enumerate() {
        let (count, changes) = if let Some(decoded) = &inputs.decoded {
            let (count, pages) = &decoded[file];
            let mut changes = Changes::default();
            for batch in pages {
                changes.add_batch(batch, 0, &definition.columns, &tree.memory, cancellation,
                    KeyLayout::for_table(&definition.table), retain)?;
            }
            (*count, changes)
        } else {
            flushed_changes(tree, definition, source, cancellation, retain).await?
        };
        rows = rows.checked_add(count).ok_or_else(incomplete_source)?;
        apply_block(tree, source.block, roots, changes, cancellation).await?;
    }
    Ok(rows)
}

async fn apply_block(tree: &Tree, block: i64, roots: &mut RevisionRoots, changes: Changes, cancellation: &CancellationToken) -> Result<()> {
    roots.apply(tree, changes, EventOrder::NewestFirst, cancellation).await?;
    // A rebuild and incremental replay must write identical addresses for the same block.
    roots.archived = checkpoint::store_roots(tree, block, roots, cancellation).await?;
    roots.changes.clear();
    Ok(())
}

const MAX_TAIL_FILES: usize = 8;

type TailKey = (Definition, usize, Option<i64>);
type TailEntry = Arc<tokio::sync::OnceCell<(i64, Arc<Changes>)>>;

#[derive(Default)]
struct TailCache(parking_lot::Mutex<BTreeMap<TailKey, TailEntry>>);

async fn tail_changes(
    tree: &Tree, definition: &Definition, snapshot: &Snapshot, source: Option<&SourceFile>,
    cancellation: &CancellationToken,
) -> Result<(i64, Arc<Changes>)> {
    let load = || async {
        if let Some(source) = source {
            let (rows, changes) = flushed_changes(tree, definition, source, cancellation, |_, _| true).await?;
            return Ok((rows, Arc::new(changes)));
        }
        let mut changes = Changes::default();
        for segment in snapshot.segments(&definition.table).iter().rev() {
            for (batch, skip) in segment.batches_from(0) {
                changes.add_batch(batch.view(), skip, &definition.columns, &tree.memory, cancellation,
                    KeyLayout::for_table(&definition.table), |_, _| true)?;
            }
        }
        Ok((0, Arc::new(changes)))
    };
    if cancellation.is_cancelled() { return Err(cancelled()); }
    let Some(budget) = tree.memory.snapshot_budget() else { return load().await; };
    let cache = snapshot.memo::<TailCache>();
    let key = (definition.clone(), Arc::as_ptr(budget) as usize, source.map(|source| source.block));
    let entry = {
        let mut entries = cache.0.lock();
        if !entries.contains_key(&key) && entries.len() >= MAX_TAIL_FILES + 1 {
            entries.pop_first();
        }
        Arc::clone(entries.entry(key).or_default())
    };
    let result = cancellation.run_until_cancelled(entry.get_or_try_init(load)).await.ok_or_else(cancelled)?;
    let (rows, changes) = result?;
    if budget.available_bytes() < budget.limit_bytes() / 2 || changes.is_empty() {
        cache.0.lock().retain(|_, cached| !Arc::ptr_eq(cached, &entry));
    }
    Ok((*rows, Arc::clone(changes)))
}

pub(super) async fn read_root(
    tree: &Tree,
    definition: &Definition,
    snapshot: &Snapshot,
    checkpoint: Option<checkpoint::Checkpoint>,
    system_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Option<Root>> {
    let through = match snapshot.tx_basis() {
        Some(head) if head.system_time <= system_at =>
            super::tree::IndexRevision { time: head.system_time, transaction: head.tx_id.0 },
        _ => super::tree::IndexRevision::through_time(system_at),
    };
    let Some(root) =
        read_flushed_root(tree, definition, snapshot, checkpoint, through, cancellation).await?
    else {
        return Ok(None);
    };
    let (_, live) = tail_changes(tree, definition, snapshot, None, cancellation).await?;
    live.read(tree, root, through, EventOrder::OldestFirst, cancellation)
        .await
        .map(Some)
}

async fn read_flushed_root(
    tree: &Tree,
    definition: &Definition,
    snapshot: &Snapshot,
    checkpoint: Option<checkpoint::Checkpoint>,
    revision: super::tree::IndexRevision,
    cancellation: &CancellationToken,
) -> Result<Option<Root>> {
    let through = source_frontier(snapshot, &definition.table);
    let (after, mut rows, mut root) = if let Some(checkpoint) = checkpoint {
        let root = checkpoint
            .roots()
            .at(tree, revision)
            .await?
            .and_then(|(_, root)| root);
        (checkpoint.through, checkpoint.source_rows, root)
    } else {
        (-1, 0, None)
    };
    let file_count = snapshot
        .trie_catalog()
        .l0_source(&definition.table, BlockIndex(after), BlockIndex(through))
        .take(MAX_TAIL_FILES + 1)
        .count();
    if file_count > MAX_TAIL_FILES {
        return Ok(None);
    }
    let files = source_files(snapshot, &definition.table, after, through)?;
    for source in files {
        let (count, changes) =
            tail_changes(tree, definition, snapshot, Some(&source), cancellation).await?;
        rows = rows.checked_add(count).ok_or_else(incomplete_source)?;
        root = changes
            .read(tree, root, revision, EventOrder::NewestFirst, cancellation)
            .await?;
    }
    if rows != source_rows(snapshot, &definition.table) {
        return Err(incomplete_source());
    }

    Ok(Some(root))
}

pub(super) fn source_rows(snapshot: &Snapshot, table: &TableRef) -> i64 {
    snapshot
        .table_catalog()
        .metadata(table)
        .map_or(0, |metadata| metadata.row_count)
}

fn incomplete_source() -> Anomaly {
    format_error("Ordered index inputs do not cover the captured table rows")
}

pub(super) fn source_frontier(snapshot: &Snapshot, table: &TableRef) -> i64 {
    snapshot.trie_catalog().l0_max_block_index(table).0
}

struct SourceFile {
    block: i64,
    object: ObjectKey,
    size: usize,
    rows: Option<i64>,
}

struct ReplayCoverage<'a> {
    pending: &'a [SourceFile],
    rows: i64,
}

impl<'a> ReplayCoverage<'a> {
    fn new(sources: &'a [SourceFile]) -> Self { Self { pending: sources, rows: 0 } }

    fn observe(&mut self, block: i64, rows: i64) -> Result<bool> {
        let next = self.pending.first().map(|source| (source.block, source.rows));
        if !classify_replay_block(next, block, rows).ok_or_else(incomplete_source)? {
            return Ok(false);
        }
        let Some((_, remaining)) = self.pending.split_first() else { return Err(incomplete_source()); };
        self.rows = self.rows.checked_add(rows).ok_or_else(incomplete_source)?;
        self.pending = remaining;
        Ok(true)
    }

    fn finish(self, expected: i64) -> Result<()> {
        if replay_complete(self.pending.is_empty(), self.rows, expected) { Ok(()) } else { Err(incomplete_source()) }
    }
}

fn source_files(
    snapshot: &Snapshot,
    table: &TableRef,
    after: i64,
    through: i64,
) -> Result<Vec<SourceFile>> {
    if after >= through {
        return Ok(Vec::new());
    }
    let storage = snapshot
        .table_entry(table)
        .ok_or_else(|| format_error("An index source table has no recorded storage identity"))?;
    let files = snapshot
        .trie_catalog()
        .l0_source(table, BlockIndex(after), BlockIndex(through))
        .map(|trie| {
            Ok(SourceFile {
                block: trie.key.block_index().0,
                object: ObjectKey::new(storage.slug.data_key(&trie.key))?,
                size: usize::try_from(trie.data_file_size).map_err(format_error)?,
                rows: trie.metadata.as_ref().and_then(|metadata| metadata.row_count),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if files.last().map(|source| source.block) != Some(through) {
        return Err(incomplete_source());
    }
    Ok(files)
}

async fn prune_dropped(
    storage: &BufferPool,
    active: &std::collections::BTreeSet<ObjectKey>,
    through: i64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let store = storage.object_store();
    let managed = store.index_page_lifecycle();
    if let Some(lifecycle) = managed {
        lifecycle.retire_dropped(active, through).await?;
    }
    let root = ObjectKey::new(INDEX_ROOT)?;
    for object in store.list(Some(&root)).await? {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let generation = object
            .key
            .as_str()
            .strip_prefix(INDEX_ROOT)
            .and_then(|path| path.strip_prefix('/'))
            .and_then(|path| path.split_once('-'))
            .and_then(|(part, _)| i64::from_str_radix(part, 16).ok());
        if generation.is_some_and(|generation| generation <= through)
            && !active.iter().any(|index| object.key.is_under(index))
        {
            if managed.is_some() && object.key.as_str().contains("/pages/") {
                continue;
            }
            store.delete(&object.key).await?;
            storage.invalidate(&object.key).await?;
        }
    }
    Ok(())
}

pub(super) fn cancelled() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        "rtdb.ordered-index/cancelled",
        "Ordered index work was cancelled",
    )
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn erase_discovery_retains_the_decoded_pages_for_small_replays() -> Result<()> {
        use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
        use rtdb_types::{Iid, Interval};

        let memory = MemoryBudget::new(32 << 20);
        let storage = BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20);
        let event = StoredEvent { iid: Iid([4; 16]), operation: StoredOperation::Erase,
            system_from: Timestamp(123), commit_position: 0, entity_key: None, valid_time: Interval::ALL,
            origin: None, position: None, recency: None };
        let batch = build_event_batch(std::slice::from_ref(&event), None, None)?;
        let bytes = rtdb_format::ipc::write_file(batch.schema_ref(), &[batch.clone()])?;
        let object = ObjectKey::new("test/order-index/input")?;
        let source = SourceFile { block: 4, object, size: bytes.len(), rows: Some(1) };
        storage.object_store().put(&source.object, bytes.into()).await?;
        let inputs = erase_cuts(&storage, &memory, &[source], &CancellationToken::new()).await?;
        assert_eq!(inputs.cuts.scope, 5);
        let address = AddressRef { iid: event.iid, key: &[] }.owned();
        assert_eq!(inputs.cuts.by_address[&address].0, event.header().precedence());
        let decoded = inputs.decoded.as_ref().expect("the selected replay fits the retention budget");
        assert_eq!(decoded[0].0, 1);
        assert_eq!(decoded[0].1[0].arrow(), &batch);
        assert!(memory.used_bytes() > 128);
        drop(inputs);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }
}
