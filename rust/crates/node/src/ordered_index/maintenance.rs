use std::{collections::BTreeSet, str::FromStr, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::EventBatch, keys::TrieKey};
use rtdb_index::Snapshot;
use rtdb_maintenance::{
    CycleWorker, SnapshotSource,
    input::{download, open_input, read_page},
};
use rtdb_storage::{
    ObjectKey, ObjectStore,
    cache::{BufferPool, Cached, View},
};
use rtdb_types::{
    Anomaly, AnomalyCategory, BlockIndex, Result, TableRef, Timestamp, TransactionKey,
};
use tokio_util::sync::CancellationToken;

use super::{
    Definition, INDEX_ROOT,
    checkpoint::{self, Checkpoint},
    definitions, format_error,
    projection::{Changes, EventOrder, RevisionRoots},
    tree::{Root, Tree},
};

pub(crate) struct OrderedIndexMaintenance {
    database: String,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    clock: Arc<dyn crate::Clock>,
    snapshots: Arc<dyn SnapshotSource>,
}

impl OrderedIndexMaintenance {
    pub(crate) fn new(
        database: String,
        storage: Arc<BufferPool>,
        memory: Arc<MemoryBudget>,
        clock: Arc<dyn crate::Clock>,
        snapshots: Arc<dyn SnapshotSource>,
    ) -> Self {
        Self {
            database,
            storage,
            memory,
            clock,
            snapshots,
        }
    }

    async fn refresh(&self, snapshot: &Snapshot, cancellation: &CancellationToken) -> Result<()> {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let now = snapshot
            .tx_basis()
            .map_or_else(|| self.clock.now(), |basis| basis.system_time);
        let active = definitions(
            &self.database,
            snapshot,
            &self.storage,
            &self.memory,
            now.into(),
            cancellation,
        )
        .await?;
        let mut directories = BTreeSet::new();
        for definition in active {
            let directory = definition.directory()?;
            let tree = Tree {
                storage: &self.storage,
                directory: &directory,
                memory: &self.memory,
            };
            self.refresh_index(snapshot, &definition, &tree, cancellation)
                .await?;
            directories.insert(directory);
        }
        if let Some(basis) = snapshot
            .tx_basis()
            .filter(|_| snapshot.contains_table(&TableRef::new(super::CATALOG.0, super::CATALOG.1)))
        {
            prune_dropped(&self.storage, &directories, basis.tx_id.0, cancellation).await?;
        }
        Ok(())
    }

    async fn refresh_index(
        &self,
        snapshot: &Snapshot,
        definition: &Definition,
        tree: &Tree<'_>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let through = source_frontier(snapshot, &definition.table);
        if through < 0 {
            return Ok(());
        }
        let expected = source_rows(snapshot, &definition.table);
        if let Ok(Some(checkpoint)) = checkpoint::latest(tree, through).await
            && checkpoint.source_rows <= expected
        {
            if checkpoint.through == through && checkpoint.source_rows == expected {
                match checkpoint.collect(tree, cancellation).await {
                    Ok(()) => return Ok(()),
                    Err(error) if error.category == AnomalyCategory::Interrupted => {
                        return Err(error);
                    }
                    Err(_) => (),
                }
            } else {
                let mut roots = checkpoint.roots();
                let advanced = replay_flushed(
                    tree,
                    definition,
                    snapshot,
                    checkpoint.through,
                    through,
                    &mut roots,
                    cancellation,
                )
                .await;
                match advanced {
                    Ok((rows, erased))
                        if checkpoint.source_rows.checked_add(rows) == Some(expected) =>
                    {
                        let next = checkpoint::publish(
                            tree,
                            through,
                            expected,
                            roots.archived,
                            cancellation,
                        )
                        .await?;
                        if erased {
                            next.collect(tree, cancellation).await?;
                        } else {
                            next.retire_manifests(tree, cancellation).await?;
                        }
                        return Ok(());
                    }
                    Err(error) if error.category == AnomalyCategory::Interrupted => {
                        return Err(error);
                    }
                    Ok(_) | Err(_) => (),
                }
            }
        }
        let mut roots = RevisionRoots::default();
        let (rows, _) = replay_flushed(
            tree,
            definition,
            snapshot,
            -1,
            through,
            &mut roots,
            cancellation,
        )
        .await?;
        if rows != expected {
            return Err(incomplete_source());
        }
        let checkpoint =
            checkpoint::publish(tree, through, expected, roots.archived, cancellation).await?;
        checkpoint.collect(tree, cancellation).await
    }
}

#[async_trait::async_trait]
impl CycleWorker for OrderedIndexMaintenance {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        self.refresh(&self.snapshots.snapshot().snapshot(), cancellation)
            .await
    }
}

async fn flushed_changes(
    tree: &Tree<'_>,
    definition: &Definition,
    object: &ObjectKey,
    cancellation: &CancellationToken,
) -> Result<(i64, Changes)> {
    let temp = tempfile::Builder::new()
        .prefix("rtdb-ordered-index")
        .tempdir()
        .map_err(format_error)?;
    let file = download(
        tree.storage.object_store().as_ref(),
        tree.memory,
        object,
        &temp.path().join("data.arrow"),
        cancellation,
    )
    .await?;
    let (mut input, _footer) = open_input(file, tree.memory)?;
    let mut changes = Changes::default();
    let mut rows = 0_i64;
    for page in 0..input.page_count() {
        let page = read_page(&mut input, page, tree.memory)?;
        let batch = EventBatch::new(page.batch().clone())?;
        rows = rows
            .checked_add(i64::try_from(batch.len()).map_err(format_error)?)
            .ok_or_else(incomplete_source)?;
        changes.add_batch(&batch, 0, &definition.columns, tree.memory, cancellation)?;
    }
    Ok((rows, changes))
}

async fn replay_flushed(
    tree: &Tree<'_>,
    definition: &Definition,
    snapshot: &Snapshot,
    after: i64,
    through: i64,
    roots: &mut RevisionRoots,
    cancellation: &CancellationToken,
) -> Result<(i64, bool)> {
    let mut erased = false;
    let mut rows = 0_i64;
    for (block, object) in source_files(
        snapshot,
        &definition.table,
        tree.storage.object_store().as_ref(),
        after,
        through,
    )
    .await?
    {
        let (count, changes) = flushed_changes(tree, definition, &object, cancellation).await?;
        rows = rows.checked_add(count).ok_or_else(incomplete_source)?;
        erased |= roots
            .apply(tree, changes, EventOrder::NewestFirst, cancellation)
            .await?;
        // A rebuild and incremental replay must write identical addresses for the same block.
        roots.archived = checkpoint::store_roots(tree, block, roots, cancellation).await?;
        roots.changes.clear();
    }
    Ok((rows, erased))
}

const MAX_TAIL_FILES: usize = 8;

pub(super) async fn read_root(
    tree: &Tree<'_>,
    definition: &Definition,
    snapshot: &Snapshot,
    system_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Option<Root>> {
    let segments = snapshot.segments(&definition.table);
    let reusable = !snapshot.has_overlays()
        && snapshot
            .tx_basis()
            .filter(|basis| basis.system_time == system_at)
            .is_some();
    let key = tip_key(tree.directory)?;
    let tip = if reusable {
        tree.storage.view::<Tip>(&key, View::Layout)
    } else {
        None
    };
    let row_count = segments.first().map_or(0, |segment| segment.row_count());
    let cached = tip.filter(|tip| {
        tip.block == snapshot.block_index()
            && tip.rows <= row_count
            && snapshot
                .tx_basis()
                .is_some_and(|basis| tip.basis.tx_id <= basis.tx_id)
    });
    if let Some(tip) = &cached
        && tip.rows == row_count
    {
        return Ok(Some(tip.root.clone()));
    }
    let mut root = if let Some(tip) = &cached {
        tip.root.clone()
    } else {
        let Some(root) =
            read_flushed_root(tree, definition, snapshot, system_at, cancellation).await?
        else {
            return Ok(None);
        };
        root
    };
    let mut live = Changes::default();
    for segment in segments.iter().rev() {
        let from = cached.as_ref().map_or(0, |tip| tip.rows);
        for (batch, skip) in segment.batches_from(from) {
            live.add_batch(
                batch.view(),
                skip,
                &definition.columns,
                tree.memory,
                cancellation,
            )?;
        }
    }
    root = live
        .read(tree, root, system_at, EventOrder::OldestFirst, cancellation)
        .await?;
    if reusable
        && let Some(basis) = snapshot.tx_basis()
        && let Some(memory) = tree.storage.admit(
            root.as_ref()
                .map_or(0, |root| root.dirty_bytes())
                .saturating_add(1024),
        )
    {
        tree.storage.insert_view(
            &key,
            View::Layout,
            Arc::new(Tip {
                block: snapshot.block_index(),
                basis,
                rows: row_count,
                root: root.clone(),
                _memory: memory,
            }),
        );
    }
    Ok(Some(root))
}

struct Tip {
    block: BlockIndex,
    basis: TransactionKey,
    rows: u32,
    root: Root,
    _memory: Arc<MemoryReservation>,
}

impl Cached for Tip {}

pub(super) fn tip_key(directory: &ObjectKey) -> Result<ObjectKey> {
    Ok(ObjectKey::new(format!("{directory}/live"))?)
}

async fn read_flushed_root(
    tree: &Tree<'_>,
    definition: &Definition,
    snapshot: &Snapshot,
    system_at: Timestamp,
    cancellation: &CancellationToken,
) -> Result<Option<Root>> {
    let through = source_frontier(snapshot, &definition.table);
    let checkpoint = checkpoint::latest(tree, through).await?;
    if checkpoint.is_none() && through >= 0 {
        return Ok(None);
    }
    let (after, mut rows, mut root) = if let Some(checkpoint) = checkpoint {
        let root = checkpoint
            .roots()
            .at(tree, system_at)
            .await?
            .and_then(|(_, root)| root);
        (checkpoint.through, checkpoint.source_rows, root)
    } else {
        (-1, 0, None)
    };
    let files = source_files(
        snapshot,
        &definition.table,
        tree.storage.object_store().as_ref(),
        after,
        through,
    )
    .await?;
    if files.len() > MAX_TAIL_FILES {
        return Ok(None);
    }
    for (_, object) in files {
        let (count, changes) = flushed_changes(tree, definition, &object, cancellation).await?;
        rows = rows.checked_add(count).ok_or_else(incomplete_source)?;
        root = changes
            .read(tree, root, system_at, EventOrder::NewestFirst, cancellation)
            .await?;
    }
    if rows != source_rows(snapshot, &definition.table) {
        return Err(incomplete_source());
    }

    Ok(Some(root))
}

fn source_rows(snapshot: &Snapshot, table: &TableRef) -> i64 {
    snapshot
        .table_catalog()
        .metadata(table)
        .map_or(0, |metadata| metadata.row_count)
}

fn incomplete_source() -> Anomaly {
    format_error("Ordered index inputs do not cover the captured table rows")
}

fn source_frontier(snapshot: &Snapshot, table: &TableRef) -> i64 {
    snapshot.trie_catalog().l0_max_block_index(table).0
}

async fn source_files(
    snapshot: &Snapshot,
    table: &TableRef,
    store: &dyn ObjectStore,
    after: i64,
    through: i64,
) -> Result<Vec<(i64, ObjectKey)>> {
    if after >= through {
        return Ok(Vec::new());
    }
    let table = snapshot
        .table_entry(table)
        .ok_or_else(|| format_error("An index source table has no recorded storage identity"))?;
    let directory = ObjectKey::new(format!("{}/data", table.slug.table_prefix()))?;
    let mut files = Vec::new();
    for object in store.list(Some(&directory)).await? {
        let Some(file) = object
            .key
            .as_str()
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".arrow"))
        else {
            continue;
        };
        let Ok(trie) = TrieKey::from_str(file) else {
            continue;
        };
        let block = trie.block_index().0;
        if trie == TrieKey::l0(trie.block_index())?
            && object.key.as_str() == table.slug.data_key(&trie)
            && block > after
            && block <= through
        {
            files.push((block, object.key));
        }
    }
    files.sort_unstable_by_key(|(block, _)| *block);
    if files.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(incomplete_source());
    }
    if files.last().map(|(block, _)| *block) != Some(through) {
        return Err(format_error(
            "The index source's latest flushed block is unavailable",
        ));
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
