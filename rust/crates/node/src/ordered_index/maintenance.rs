use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    ops::Range,
    sync::Arc,
};

use arrow_buffer::Buffer;
use rtdb_columnar::{EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::{data::EventBatch, ipc::PageDecoder};
use rtdb_index::Snapshot;
use rtdb_maintenance::{
    CycleWorker, SnapshotSource,
    input::read_page,
};
use rtdb_replication::{PartitionLogs, RetainedTableReplay};
use rtdb_storage::{HeldView, IoRegion, ObjectKey, ObjectStore, ReadReservation, cache::BufferPool};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, EventHeader, Iid, PhysicalObjectVersion, Result, TableRef, Timestamp, WorldShadow};
use tokio_util::sync::CancellationToken;

use super::{
    Definition, DefinitionSet, INDEX_ROOT, checkpoint, definitions, format_error,
    projection::{Changes, EventOrder, KeyLayout, RevisionRoots},
    replay_coverage::{classify_replay_block, replay_complete},
    tree::{AddressRef, Root, Tree},
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
        let active = definitions.as_deref().into_iter()
            .chain(owned.as_ref())
            .flat_map(DefinitionSet::iter);
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
                let cuts = erase_cuts(&self.storage, &self.memory, &sources, &source_columns(definition), cancellation).await?;
                if cuts.cuts.scope == 0 {
                    let mut roots = checkpoint.roots();
                    let advanced =
                        replay_flushed(&tree, definition, &sources, cuts, &mut roots, cancellation).await;
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
        let cuts = erase_cuts(&self.storage, &self.memory, &sources, &source_columns(definition), cancellation).await?;
        let mut tree = Tree::new(&self.storage, directory, &self.memory, cuts.cuts.scope).await?;
        tree.arrow_references = self.arrow_references;
        checkpoint::request_collection(&tree, through).await?;
        let mut roots = RevisionRoots::default();
        let rows =
            replay_flushed(&tree, definition, &sources, cuts, &mut roots, cancellation).await?;
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

fn source_columns(definition: &Definition) -> Cow<'_, [String]> {
    if matches!(KeyLayout::for_table(&definition.table), KeyLayout::World)
        && !definition.columns.iter().any(|column| column == WorldShadow::FIELDS)
    {
        let mut columns = definition.columns.clone();
        columns.push(WorldShadow::FIELDS.to_owned());
        Cow::Owned(columns)
    } else {
        Cow::Borrowed(&definition.columns)
    }
}

async fn read_flushed(
    storage: &BufferPool,
    memory: &Arc<MemoryBudget>,
    source: &SourceFile,
    columns: &[String],
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
    input.project_event_payload(columns);
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

struct ReplayRangeReader<'a> {
    store: &'a dyn ObjectStore,
    source: &'a SourceFile,
    version: PhysicalObjectVersion,
    memory: &'a Arc<MemoryBudget>,
    cancellation: &'a CancellationToken,
}

impl ReplayRangeReader<'_> {
    async fn read(&self, range: Range<u64>) -> Result<Buffer> {
        let length = range.end.checked_sub(range.start)
            .filter(|_| range.end <= self.source.size as u64)
            .and_then(|length| usize::try_from(length).ok())
            .ok_or_else(|| format_error("An ordered source range exceeds its adopted file"))?;
        let key = &self.source.object;
        let allocation = self.store.scan_range_allocation(key, length).map_err(source_read_error)?;
        let retained = self.memory.reserve(allocation.retained_bytes.max(length))?;
        let scratch = (allocation.scratch_bytes > 0).then(|| self.memory.reserve(allocation.scratch_bytes)).transpose()?;
        let reservation = ReadReservation::funded(length, Arc::clone(&retained), scratch);
        let observed = self.cancellation.run_until_cancelled(reservation.retaining_io(self.store.observe_range(key, range)))
            .await.ok_or_else(cancelled)?.map_err(source_read_error)?;
        if observed.version.as_ref() != Some(&self.version) || observed.value.len() != length {
            return Err(format_error("An ordered source range differs from its observed file"));
        }
        Ok(rtdb_columnar::retain_buffer_with_owner(&Buffer::from(observed.value), retained))
    }
}

async fn read_replay_ranges(
    store: &dyn ObjectStore,
    memory: &Arc<MemoryBudget>,
    source: &SourceFile,
    columns: &[String],
    cancellation: &CancellationToken,
    mut visit: impl FnMut(EventBatch) -> Result<()> + Send,
) -> Result<Option<i64>> {
    let head = cancellation.run_until_cancelled(store.observe_head(&source.object))
        .await.ok_or_else(cancelled)?.map_err(source_read_error)?;
    let size = head.value.size;
    if head.value.key != source.object || size != source.size as u64 {
        return Err(format_error("An ordered index source differs from the adopted file"));
    }
    let Some(version) = head.version else {
        return Ok(None);
    };
    let reader = ReplayRangeReader { store, source, version, memory, cancellation };
    let trailer = size.checked_sub(10).ok_or_else(|| format_error("Arrow file is shorter than its footer"))?;
    let length = {
        let bytes = reader.read(trailer..size).await?;
        arrow_ipc::reader::read_footer_length(bytes.as_slice().try_into().map_err(format_error)?).map_err(format_error)?
    };
    let footer = trailer.checked_sub(length as u64).ok_or_else(|| format_error("Arrow footer exceeds its file"))?;
    let opening = length.checked_add(10).and_then(|length| length.checked_mul(16))
        .and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| format_error("Arrow footer allocation overflow"))?;
    let _opening = memory.reserve(opening)?;
    let mut decoder = {
        let bytes = reader.read(footer..trailer).await?;
        PageDecoder::from_footer(bytes.as_slice(), footer, None)?
    };
    if decoder.dictionary_count() != 0 {
        return Ok(None);
    }
    decoder.project_event_payload(columns);
    let mut rows = 0_i64;
    for page in 0..decoder.page_count() {
        let wire = reader.read(decoder.page_range(page)?).await?;
        let charge = memory.reserve(decoder.page_bound(page, wire.as_slice())?)?;
        let page = OwnedBatch::with_reservation(decoder.decode_file_page(page, wire)?, charge)?;
        let batch = EventBatch::new(page.batch().clone())?;
        rows = rows
            .checked_add(i64::try_from(batch.len()).map_err(format_error)?)
            .ok_or_else(incomplete_source)?;
        visit(batch)?;
    }
    Ok(Some(rows))
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
    by_key: BTreeMap<Box<[u8]>, EraseCut>,
    by_iid: BTreeMap<Iid, EraseCut>,
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
                let existing = if key.is_empty() {
                    self.by_iid.get_mut(&header.iid)
                } else {
                    EntityIdRef::from_handle_bytes(key)?;
                    self.by_key.get_mut(key)
                };
                let precedence = header.precedence();
                if let Some((cut, _)) = existing {
                    *cut = (*cut).max(precedence);
                } else {
                    let cut = (precedence, memory.reserve(key.len().saturating_add(128))?);
                    if key.is_empty() {
                        self.by_iid.insert(header.iid, cut);
                    } else {
                        self.by_key.insert(Box::from(key), cut);
                    }
                }
            }
            Ok(())
        })
    }

    fn retains(&self, header: EventHeader, address: AddressRef<'_>) -> bool {
        let cut = if address.key.is_empty() {
            self.by_iid.get(&address.iid)
        } else {
            self.by_key.get(address.key)
        };
        cut.is_none_or(|(precedence, _)| header.precedence() >= *precedence)
    }
}

struct ReplayInputs {
    cuts: ErasureCuts,
    decoded: Option<Vec<RetainedSource>>,
}

struct RetainedSource {
    rows: i64,
    pages: Vec<(EventBatch, Arc<MemoryReservation>)>,
    _opening: Option<Arc<MemoryReservation>>,
}

async fn erase_cuts(
    storage: &BufferPool,
    memory: &Arc<MemoryBudget>,
    sources: &[SourceFile],
    retained_columns: &[String],
    cancellation: &CancellationToken,
) -> Result<ReplayInputs> {
    let mut inputs = ReplayInputs { cuts: ErasureCuts::default(), decoded: Some(Vec::new()) };
    let budget = (memory.available_bytes() / 8).min(32 << 20);
    let mut retained = 0usize;
    for source in sources {
        let mut pages = Vec::new();
        let mut footer = None;
        retained = retained.saturating_add(source.size);
        if retained > budget {
            inputs.decoded = None;
        }
        // A retained page will also supply the later ordered-key replay.
        let columns = if inputs.decoded.is_some() { retained_columns } else { &[] };
        let mut visit = |batch, page: &Arc<MemoryReservation>, opening: &Arc<MemoryReservation>| {
            inputs.cuts.record(source.block, &batch, memory, cancellation)?;
            if inputs.decoded.is_some() {
                retained = retained.saturating_add(page.bytes());
                if footer.is_none() {
                    retained = retained.saturating_add(opening.bytes());
                    footer = Some(Arc::clone(opening));
                }
                if retained <= budget {
                    pages.push((batch, Arc::clone(page)));
                } else {
                    inputs.decoded = None;
                    pages.clear();
                    footer = None;
                }
            }
            Ok(())
        };
        let rows = read_flushed(storage, memory, source, columns, cancellation, &mut visit).await?;
        if let Some(decoded) = &mut inputs.decoded {
            decoded.push(RetainedSource { rows, pages, _opening: footer });
        }
    }
    Ok(inputs)
}

async fn flushed_changes(
    tree: &Tree,
    definition: &Definition,
    source: &SourceFile,
    cancellation: &CancellationToken,
    cuts: Option<&ErasureCuts>,
) -> Result<(i64, Changes)> {
    let columns = source_columns(definition);
    let add = |changes: &mut Changes, batch: EventBatch| {
        changes.add_batch(
            &batch,
            0,
            &definition.columns,
            &tree.memory,
            cancellation,
            KeyLayout::for_table(&definition.table),
            |header, address| cuts.is_none_or(|cuts| cuts.retains(header, address)),
        )
    };
    let store = tree.storage.object_store().as_ref();
    if cuts.is_some() && store.supports_observed_scan_ranges(&source.object) {
        let mut changes = Changes::default();
        match read_replay_ranges(store, &tree.memory, source, &columns, cancellation, |batch| add(&mut changes, batch)).await {
            Ok(Some(rows)) => return Ok((rows, changes)),
            Ok(None) => (),
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => (),
            Err(error) => return Err(error),
        }
    }
    let mut changes = Changes::default();
    let rows = read_flushed(&tree.storage, &tree.memory, source, &columns, cancellation, |batch, _, _| add(&mut changes, batch)).await?;
    Ok((rows, changes))
}

async fn replay_flushed(
    tree: &Tree,
    definition: &Definition,
    sources: &[SourceFile],
    inputs: ReplayInputs,
    roots: &mut RevisionRoots,
    cancellation: &CancellationToken,
) -> Result<i64> {
    let mut rows = 0_i64;
    let ReplayInputs { cuts, decoded } = inputs;
    let mut decoded = decoded.map(Vec::into_iter);
    for source in sources {
        let (count, changes) = if let Some(decoded) = &mut decoded {
            let held = decoded.next().ok_or_else(incomplete_source)?;
            let mut changes = Changes::default();
            for (batch, _page) in held.pages {
                changes.add_batch(&batch, 0, &definition.columns, &tree.memory, cancellation,
                    KeyLayout::for_table(&definition.table), |header, address| cuts.retains(header, address))?;
            }
            (held.rows, changes)
        } else {
            flushed_changes(tree, definition, source, cancellation, Some(&cuts)).await?
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
            let (rows, changes) = flushed_changes(tree, definition, source, cancellation, None).await?;
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
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::Interval;

    #[test]
    fn erasure_cuts_share_complete_keys_and_keep_keyless_iids_separate() -> Result<()> {
        let key = b"\0order";
        let plain = StoredEvent {
            iid: Iid([4; 16]),
            operation: StoredOperation::Erase,
            system_from: Timestamp(20),
            commit_position: 0,
            entity_key: None,
            valid_time: Interval::ALL,
            origin: None,
            position: None,
            recency: None,
        };
        let keyed = StoredEvent {
            system_from: Timestamp(10),
            commit_position: 1,
            entity_key: Some(Arc::from(&key[..])),
            ..plain.clone()
        };
        let duplicate = StoredEvent {
            iid: Iid([5; 16]),
            commit_position: 2,
            ..keyed.clone()
        };
        let memory = MemoryBudget::new(256 + key.len());
        let cancellation = CancellationToken::new();
        let mut cuts = ErasureCuts::default();
        let mut record = |block, events: &[StoredEvent]| {
            let batch = EventBatch::new(build_event_batch(events, None, None)?)?;
            cuts.record(block, &batch, &memory, &cancellation)
        };
        record(4, std::slice::from_ref(&plain))?;
        record(5, &[keyed.clone(), duplicate.clone(), keyed.clone()])?;

        let retains = |header: EventHeader, key: &[u8]| {
            cuts.retains(header, AddressRef { iid: header.iid, key })
        };
        assert!(!retains(keyed.header(), key));
        assert!(retains(duplicate.header(), key));
        assert!(retains(keyed.header(), b"\0other"));
        assert!(retains(plain.header(), &[]));
        assert!(!retains(keyed.header(), &[]));
        assert!(retains(duplicate.header(), &[]));
        assert_eq!(cuts.scope, 6);
        assert_eq!(memory.used_bytes(), 256 + key.len());
        drop(cuts);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn replay_projects_keys_and_keeps_the_original_event_headers() -> Result<()> {
        use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
        use rtdb_types::{LogicalType, ScalarType, StructFields, Value};

        let memory = MemoryBudget::new(32 << 20);
        let storage = BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20);
        let event = StoredEvent {
            iid: Iid([7; 16]), system_from: Timestamp(124), commit_position: 0, entity_key: None,
            valid_time: Interval::ALL, origin: None, position: None, recency: None,
            operation: StoredOperation::Put(StructFields::from([
                ("key".to_owned(), Value::I64(7)), ("payload".to_owned(), Value::Utf8("wide".repeat(512))),
            ])),
        };
        let fields = StructFields::from([
            ("key".to_owned(), LogicalType::Scalar(ScalarType::I64)),
            ("payload".to_owned(), LogicalType::Scalar(ScalarType::Utf8)),
        ]);
        let batch = build_event_batch(std::slice::from_ref(&event), Some(fields), None)?;
        let bytes = rtdb_format::ipc::write_file(batch.schema_ref(), std::slice::from_ref(&batch))?;
        let source = SourceFile { block: 4, object: ObjectKey::new("test/order-index/projected")?, size: bytes.len(), rows: Some(1) };
        storage.object_store().put(&source.object, bytes.into()).await?;

        let read = read_flushed(&storage, &memory, &source, &["key".to_owned()], &CancellationToken::new(), |batch, _, _| {
            let put = batch.put_document()?.expect("original put");
            assert!(put.column_by_name("key").is_some());
            assert!(put.column_by_name("payload").is_none());
            assert_eq!(batch.header(0)?, event.header());
            Ok(())
        }).await?;
        assert_eq!(read, 1);
        Ok(())
    }

    #[tokio::test]
    async fn erase_discovery_retains_the_decoded_pages_for_small_replays() -> Result<()> {
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
        let inputs = erase_cuts(&storage, &memory, &[source], &[], &CancellationToken::new()).await?;
        assert_eq!(inputs.cuts.scope, 5);
        assert_eq!(inputs.cuts.by_iid[&event.iid].0, event.header().precedence());
        let decoded = inputs.decoded.as_ref().expect("the selected replay fits the retention budget");
        assert_eq!(decoded[0].rows, 1);
        assert_eq!(decoded[0].pages[0].0.arrow(), &batch);
        assert!(memory.used_bytes() > 128);
        drop(inputs);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    struct ReplayStore {
        inner: rtdb_storage::MemoryStore,
        source: ObjectKey,
        ranges: std::sync::atomic::AtomicBool,
        reads: parking_lot::Mutex<Vec<Option<Range<u64>>>>,
        changed_at: Option<u64>,
    }

    #[async_trait::async_trait]
    impl ObjectStore for ReplayStore {
        fn supports_scan_ranges(&self, _: &ObjectKey) -> bool {
            self.ranges.load(std::sync::atomic::Ordering::Relaxed)
        }

        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::StoredObject> {
            self.inner.head(key).await
        }

        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<bytes::Bytes> {
            if key == &self.source {
                self.reads.lock().push(None);
            }
            self.inner.get(key).await
        }

        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<bytes::Bytes> {
            if key == &self.source {
                self.reads.lock().push(Some(range.clone()));
            }
            Ok(bytes::Bytes::copy_from_slice(&self.inner.get_range(key, range).await?))
        }

        async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<rtdb_storage::StoredObject>> {
            Ok(rtdb_storage::ObjectObservation { value: self.head(key).await?, version: Some(replay_version(key, "original")) })
        }

        async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<rtdb_storage::ObjectObservation<bytes::Bytes>> {
            let version = if self.changed_at == Some(range.start) { "replacement" } else { "original" };
            Ok(rtdb_storage::ObjectObservation { value: self.get_range(key, range).await?, version: Some(replay_version(key, version)) })
        }

        async fn put(&self, key: &ObjectKey, bytes: bytes::Bytes) -> rtdb_storage::Result<()> {
            self.inner.put(key, bytes).await
        }

        async fn put_file(&self, key: &ObjectKey, source: &std::path::Path) -> rtdb_storage::Result<()> {
            self.inner.put_file(key, source).await
        }

        async fn list(&self, prefix: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<rtdb_storage::StoredObject>> {
            self.inner.list(prefix).await
        }

        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
            self.inner.delete(key).await
        }
    }

    fn replay_version(key: &ObjectKey, version: &str) -> PhysicalObjectVersion {
        PhysicalObjectVersion {
            namespace: rtdb_types::S3ObjectNamespace { bucket: "test".into(), endpoint: None, region: None },
            key: key.to_string(), version: version.into(),
        }
    }

    #[tokio::test]
    async fn declined_retention_replays_exact_page_ranges_with_the_same_erasure_cut() -> Result<()> {
        use crate::ordered_index::{key::AtomRef, tree::{Address, IndexRevision, Link}};
        use rtdb_format::ipc::ArrowFile;
        use rtdb_types::{LogicalType, ScalarType, StructFields, Value};
        use std::sync::atomic::{AtomicBool, Ordering};

        let put = |key| StoredOperation::Put(StructFields::from([
            ("key".to_owned(), Value::I64(key)), ("payload".to_owned(), Value::Utf8("wide".repeat(512))),
        ]));
        let erased = StoredEvent {
            iid: Iid([7; 16]), operation: StoredOperation::Erase, system_from: Timestamp(124),
            commit_position: 3, entity_key: None, valid_time: Interval::ALL, origin: None, position: None, recency: None,
        };
        let old = StoredEvent { operation: put(99), commit_position: 1, system_from: Timestamp(123), ..erased.clone() };
        let earlier = StoredEvent { operation: put(98), commit_position: 2, ..erased.clone() };
        let winner = StoredEvent { operation: put(7), commit_position: 4, ..erased.clone() };
        let fields = StructFields::from([
            ("key".to_owned(), LogicalType::Scalar(ScalarType::I64)),
            ("payload".to_owned(), LogicalType::Scalar(ScalarType::Utf8)),
        ]);
        let pages = [
            build_event_batch(&[earlier, old], Some(fields.clone()), None)?,
            build_event_batch(&[winner.clone(), erased], Some(fields), None)?,
        ];
        let bytes = rtdb_format::ipc::write_file(pages[0].schema_ref(), &pages)?;
        let source = SourceFile { block: 4, object: ObjectKey::new("test/order-index/source")?, size: bytes.len(), rows: Some(4) };
        let input = ArrowFile::from_buffer(Buffer::from(bytes.clone()), None)?;
        let size = source.size as u64;
        let trailer = size - 10;
        let footer = trailer - arrow_ipc::reader::read_footer_length(bytes[trailer as usize..].try_into().unwrap()).unwrap() as u64;
        let expected_ranges = vec![Some(trailer..size), Some(footer..trailer), Some(input.page_range(0)?), Some(input.page_range(1)?)];

        let store = Arc::new(ReplayStore {
            inner: rtdb_storage::MemoryStore::default(), source: source.object.clone(),
            ranges: AtomicBool::new(true), reads: parking_lot::Mutex::new(Vec::new()), changed_at: None,
        });
        store.put(&source.object, bytes.into()).await?;
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let cancellation = CancellationToken::new();
        let definition = Definition { name: "ordered".into(), table: TableRef::new("public", "orders"), columns: vec!["key".into()], version: 0 };
        let sources = [source];
        let mut root_digests = Vec::new();
        for ranged in [true, false] {
            store.ranges.store(ranged, Ordering::Relaxed);
            let other_work = memory.reserve(memory.limit_bytes() - (512 << 10))?;
            let inputs = erase_cuts(&storage, &memory, &sources, &definition.columns, &cancellation).await?;
            assert!(inputs.decoded.is_none());
            drop(other_work);
            assert_eq!(std::mem::take(&mut *store.reads.lock()), vec![None]);

            let tree = Tree::new(&storage, &definition.directory()?, &memory, inputs.cuts.scope).await?;
            let mut roots = RevisionRoots::default();
            assert_eq!(replay_flushed(&tree, &definition, &sources, inputs, &mut roots, &cancellation).await?, 4);
            let reads = std::mem::take(&mut *store.reads.lock());
            if ranged {
                assert_eq!(reads, expected_ranges);
            } else {
                assert_eq!(reads, vec![None]);
            }

            assert!(roots.at(&tree, IndexRevision::through_time(Timestamp(123))).await?.is_none());
            let (_, root) = roots.at(&tree, IndexRevision::MAX).await?.expect("the reinsert is visible");
            let spans = tree.spans(&root, &Address::new(winner.iid, bytes::Bytes::new())).await?;
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].tuple.view().cell(0), AtomRef::Integer(7));
            let Some(Link::Stored(pointer)) = roots.archived else { panic!("expected stored revisions"); };
            root_digests.push(pointer.digest.clone());
        }
        assert_eq!(root_digests[0], root_digests[1]);
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn unversioned_replay_uses_the_complete_file_reader() -> Result<()> {
        let store = rtdb_storage::MemoryStore::default();
        let source = SourceFile {
            block: 4, object: ObjectKey::new("test/order-index/unversioned")?, size: 1, rows: None,
        };
        store.put(&source.object, bytes::Bytes::from_static(&[0])).await?;
        let memory = MemoryBudget::new(1 << 20);
        let observed = read_replay_ranges(&store, &memory, &source, &[],
            &CancellationToken::new(), |_| panic!("unversioned range published a row")).await?;
        assert!(observed.is_none());
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn a_range_from_another_observed_version_refuses_replay_without_a_whole_read() -> Result<()> {
        let event = StoredEvent {
            iid: Iid([4; 16]), operation: StoredOperation::Erase, system_from: Timestamp(123),
            commit_position: 0, entity_key: None, valid_time: Interval::ALL, origin: None, position: None, recency: None,
        };
        let batch = build_event_batch(&[event], None, None)?;
        let pages = [batch.clone(), batch];
        let bytes = rtdb_format::ipc::write_file(pages[0].schema_ref(), &pages)?;
        let input = rtdb_format::ipc::ArrowFile::from_buffer(Buffer::from(bytes.clone()), None)?;
        let source = SourceFile { block: 4, object: ObjectKey::new("test/order-index/version")?, size: bytes.len(), rows: Some(2) };
        let store = Arc::new(ReplayStore {
            inner: rtdb_storage::MemoryStore::default(), source: source.object.clone(),
            ranges: std::sync::atomic::AtomicBool::new(true), reads: parking_lot::Mutex::new(Vec::new()),
            changed_at: Some(input.page_range(1)?.start),
        });
        store.put(&source.object, bytes.into()).await?;
        let storage = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let memory = MemoryBudget::new(32 << 20);
        let cancellation = CancellationToken::new();
        let definition = Definition { name: "ordered".into(), table: TableRef::new("public", "orders"), columns: vec!["key".into()], version: 0 };
        let tree = Tree::new(&storage, &definition.directory()?, &memory, 5).await?;
        let cuts = ErasureCuts::default();
        let result = flushed_changes(&tree, &definition, &source, &cancellation, Some(&cuts)).await;
        let error = result.err().expect("the replacement range must be refused");
        assert_eq!(error.message.as_deref(), Some("An ordered source range differs from its observed file"));
        assert_eq!(store.reads.lock().len(), 4);
        assert!(store.reads.lock().iter().all(Option::is_some));
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }
}
