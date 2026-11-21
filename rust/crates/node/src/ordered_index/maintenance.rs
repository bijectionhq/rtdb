use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{
    data::{EventBatch, StoredOperation},
    keys::TrieKey,
};
use rtdb_index::Snapshot;
use rtdb_maintenance::{
    CycleWorker, SnapshotSource,
    input::{download, open_input, read_page},
};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{Anomaly, AnomalyCategory, EventOp, Iid, Result, TableRef, Value};
use tokio_util::sync::CancellationToken;

use super::{
    Definition, definitions, format_error,
    key::Atom,
    run::{self, Entry},
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

    pub(crate) async fn refresh(
        &self,
        snapshot: &Snapshot,
        cancellation: &CancellationToken,
    ) -> Result<()> {
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
        let store = self.storage.object_store().as_ref();
        let mut directories = std::collections::BTreeSet::new();
        for definition in active {
            let directory = definition.directory()?;
            self.refresh_index(snapshot, &definition, &directory, cancellation)
                .await?;
            directories.insert(directory);
        }
        if let Some(basis) = snapshot
            .tx_basis()
            .filter(|_| snapshot.contains_table(&TableRef::new(super::CATALOG.0, super::CATALOG.1)))
        {
            prune_dropped(store, &directories, basis.tx_id.0, cancellation).await?;
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
        let store = self.storage.object_store().as_ref();
        let through = source_frontier(snapshot, &definition.table);
        if through < 0 {
            return Ok(());
        }
        let mut manifest = run::latest(store, directory, through, &self.memory).await?;
        if manifest.through >= 0 && !run::prune(store, directory, &manifest, cancellation).await? {
            manifest = run::Manifest::default();
        }
        if manifest.through == through {
            return Ok(());
        }
        for (block, data) in source_files(
            snapshot,
            &definition.table,
            store,
            manifest.through,
            through,
        )
        .await?
        {
            let work = Entries::from_file(
                store,
                &data,
                &definition.columns,
                &self.memory,
                cancellation,
            )
            .await?;
            let Entries {
                mut rows,
                erased,
                charges: _charges,
                ..
            } = work;
            rows.sort_unstable();
            rows.dedup();
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            run::append(
                &mut manifest,
                block,
                rows,
                &erased,
                store,
                directory,
                &self.memory,
            )
            .await?;
        }
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        run::publish(store, directory, &manifest).await?;
        if !run::prune(store, directory, &manifest, cancellation).await? {
            return Err(format_error(
                "An ordered index page disappeared during publication",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl CycleWorker for OrderedIndexMaintenance {
    async fn collect(&self, cancellation: &CancellationToken) -> Result<()> {
        self.refresh(&self.snapshots.snapshot().snapshot(), cancellation)
            .await
    }
}

pub(super) fn source_frontier(snapshot: &Snapshot, table: &TableRef) -> i64 {
    snapshot.trie_catalog().l0_max_block_index(table).0
}

pub(super) async fn source_files(
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
        if trie.level() == 0 && block > after && block <= through {
            files.push((block, object.key));
        }
    }
    files.sort_unstable_by_key(|(block, _)| *block);
    if files.last().map(|(block, _)| *block) != Some(through) {
        return Err(format_error(
            "The index source's latest flushed block is unavailable",
        ));
    }
    Ok(files)
}

pub(super) type Erased = BTreeMap<Iid, (i64, std::cmp::Reverse<u64>)>;

pub(super) fn was_erased(entry: &Entry, erased: &Erased) -> bool {
    erased
        .get(&entry.iid)
        .is_some_and(|cut| (entry.system_from, std::cmp::Reverse(entry.source_row)) < *cut)
}

pub(super) struct Entries {
    pub rows: Vec<Entry>,
    charges: Vec<Arc<MemoryReservation>>,
    erased: Erased,
    source_row: u64,
}

impl Entries {
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            charges: Vec::new(),
            erased: Erased::new(),
            source_row: 0,
        }
    }

    fn push(&mut self, entry: Entry, memory: &Arc<MemoryBudget>) -> Result<()> {
        self.charges
            .push(memory.reserve(entry.bytes().saturating_mul(2))?);
        self.rows.push(entry);
        Ok(())
    }

    pub fn add_batch(
        &mut self,
        batch: &EventBatch,
        columns: &[String],
        memory: &Arc<MemoryBudget>,
        mut keep: impl FnMut(&[Atom]) -> bool,
    ) -> Result<()> {
        for row in 0..batch.len() {
            let header = batch.header(row)?;
            let source_row = self.source_row;
            self.source_row += 1;
            if header.op == EventOp::Erase {
                let order = (header.system_from.0, std::cmp::Reverse(source_row));
                if let Some(current) = self.erased.get_mut(&header.iid) {
                    *current = (*current).max(order);
                } else {
                    self.charges.push(memory.reserve(96)?);
                    self.erased.insert(header.iid, order);
                }
            }
            if header.op != EventOp::Put {
                continue;
            }
            let StoredOperation::Put(document) = batch.read_event(row)?.operation else {
                continue;
            };
            let key = columns
                .iter()
                .map(|name| Atom::of(document.get(name).unwrap_or(&Value::Null)))
                .collect::<Option<Vec<_>>>();
            if let Some(key) = key.filter(|key| keep(key)) {
                self.push(
                    Entry {
                        key,
                        iid: header.iid,
                        system_from: header.system_from.0,
                        source_row,
                    },
                    memory,
                )?;
            }
        }
        Ok(())
    }

    pub async fn from_file(
        store: &dyn ObjectStore,
        object: &ObjectKey,
        columns: &[String],
        memory: &Arc<MemoryBudget>,
        cancellation: &CancellationToken,
    ) -> Result<Self> {
        let temp = tempfile::Builder::new()
            .prefix("rtdb-ordered-index")
            .tempdir()
            .map_err(format_error)?;
        let file = download(
            store,
            memory,
            object,
            &temp.path().join("data.arrow"),
            cancellation,
        )
        .await?;
        let (mut input, _footer) = open_input(file, memory)?;
        let mut entries = Self::new();
        for page in 0..input.page_count() {
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }
            let page = read_page(&mut input, page, memory)?;
            entries.add_batch(
                &EventBatch::new(page.batch().clone())?,
                columns,
                memory,
                |_| true,
            )?;
        }
        Ok(entries)
    }

    pub fn merge(&mut self, mut other: Self) {
        self.rows.append(&mut other.rows);
        self.charges.append(&mut other.charges);
    }
}

async fn prune_dropped(
    store: &dyn ObjectStore,
    active: &std::collections::BTreeSet<ObjectKey>,
    through: i64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let root = ObjectKey::new("ordered-index/v1")?;
    for object in store.list(Some(&root)).await? {
        if cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let generation = object
            .key
            .as_str()
            .strip_prefix("ordered-index/v1/")
            .and_then(|path| path.split_once('-'))
            .and_then(|(part, _)| i64::from_str_radix(part, 16).ok());
        if generation.is_some_and(|generation| generation <= through)
            && !active.iter().any(|index| object.key.is_under(index))
        {
            store.delete(&object.key).await?;
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
