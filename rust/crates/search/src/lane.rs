use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::EventBatch, ipc::ArrowFile, keys::ArtefactOwner, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactJob};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{Anomaly, Result, Value};
use tokio_util::sync::CancellationToken;

use crate::{SearchRow, file::write_group_search_file, definitions::SearchDefinition};
use rtdb_maintenance::input::{download, open_input, read_page};

pub const KIND: &str = "search";

/// A partition group's members share one search file while their data files together hold at most this many bytes.
pub const GROUP_BYTES: i64 = 512 << 20;

/// The bytes of data a partition group's shared search file may cover: within GROUP_BYTES and a quarter of the memory budget, which its build holds while it runs (indexing_allowance).
pub(crate) fn group_bytes(memory: &MemoryBudget) -> i64 { GROUP_BYTES.min(i64::try_from(memory.limit_bytes() / 4).unwrap_or(i64::MAX)) }

/// The search artefact kind: install it in a node's services to build search files.
pub struct SearchArtefacts { runtime: Arc<crate::SearchRuntime> }

impl SearchArtefacts {
    pub fn with_runtime(runtime: Arc<crate::SearchRuntime>) -> Self { Self { runtime } }
}

impl rtdb_maintenance::ArtefactKind for SearchArtefacts {
    fn name(&self) -> &str { KIND }
    fn builder(&self, database: rtdb_maintenance::ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>> {
        let clock = Arc::clone(&database.clock);
        Ok(Arc::new(SearchBuilder::new(database.name, database.store, database.storage, database.memory, Arc::new(move || clock().into()), Arc::clone(&self.runtime))))
    }
}

/// Builds the search files of one database, for every definition in `xt.search_indexes`.
pub struct SearchBuilder {
    database: String,
    store: Arc<dyn ObjectStore>,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    clock: Arc<dyn Fn() -> rtdb_types::Instant + Send + Sync>,
    runtime: Arc<crate::SearchRuntime>,
    columns: Mutex<BTreeMap<(String, i64), Vec<String>>>,
}

impl SearchBuilder {
    pub fn new(database: String, store: Arc<dyn ObjectStore>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, clock: Arc<dyn Fn() -> rtdb_types::Instant + Send + Sync>, runtime: Arc<crate::SearchRuntime>) -> Self {
        Self { database, store, storage, memory, clock, runtime, columns: Mutex::new(BTreeMap::new()) }
    }
    async fn search_definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Arc<Vec<SearchDefinition>>> {
        self.runtime.definitions(&self.database, &[snapshot.snapshot()], &self.storage, &self.memory, (self.clock)(), cancellation).await
    }
}

#[async_trait]
impl ArtefactBuilder for SearchBuilder {
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        let definitions = self.search_definitions(snapshot, cancellation).await?;
        let mut columns = self.columns.lock();
        Ok(definitions.iter().map(|definition| {
            let id = definition.artefact_id();
            columns.insert((id.clone(), definition.version), definition.columns.clone());
            ArtefactDefinition { table: definition.table.clone(), kind: KIND.into(), id, version: definition.version, historical: definition.history, group_bytes: Some(group_bytes(&self.memory)) }
        }).collect())
    }

    async fn repairs(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactJob>> {
        let suspects = self.runtime.suspects(&self.database);
        if suspects.is_empty() { return Ok(Vec::new()); }
        let definitions = self.definitions(snapshot, cancellation).await?;
        let mut jobs = Vec::new();
        for suspect in suspects {
            let Some(definition) = definitions.iter().find(|definition| definition.table == suspect.table && definition.id == suspect.artefact_id && definition.version == suspect.version) else {
                self.runtime.clear(&suspect);
                continue;
            };
            let job = ArtefactJob { definition: definition.clone(), owner: suspect.owner.clone() };
            let scope = job.scope();
            let catalog = snapshot.trie_catalog();
            let recorded = job.tries().iter().filter_map(|key| catalog.trie(&suspect.table, key))
                .filter(|trie| trie.state == rtdb_index::TrieState::Live)
                .any(|trie| trie.artefact(KIND, &suspect.artefact_id, suspect.version).is_some_and(|record| record.scope() == scope));
            if recorded { jobs.push(job); }
            else { self.runtime.clear(&suspect); }
        }
        Ok(jobs)
    }

    async fn wait_for_work(&self) { self.runtime.reported(&self.database).await }

    async fn build(&self, job: &ArtefactJob, source: rtdb_maintenance::ArtefactSource<'_>, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let definition = &job.definition;
        let suspect = crate::runtime::Suspect { database: self.database.clone(), table: definition.table.clone(), owner: job.owner.clone(), artefact_id: definition.id.clone(), version: definition.version };
        let repairing = matches!(source, rtdb_maintenance::ArtefactSource::Stored) && self.runtime.is_suspect(&suspect);
        let record = |sections: &[crate::file::Section]| ArtefactRecord { kind: Some(KIND.into()), definition_id: Some(definition.id.clone()), definition_version: Some(definition.version), stats: Some(crate::file::encode_sections(sections)), scope: None };
        let known = self.columns.lock().get(&(definition.id.clone(), definition.version)).cloned();
        let columns = match known { Some(columns) => columns, None => {
            self.definitions(snapshot, cancellation).await?;
            self.columns.lock().get(&(definition.id.clone(), definition.version)).cloned().ok_or_else(|| Anomaly::incorrect("rtdb.search/definition-dropped", "The search index definition is no longer in force"))?
        } };
        let entry = snapshot.table_catalog().entry(&definition.table).ok_or_else(|| Anomaly::fault("rtdb.search/unknown-table", "Search job targets a table the catalog does not hold"))?;
        let identity = format!("{}/{}/{}-v{}", entry.slug.as_str(), job.owner, definition.id, definition.version);
        let key = ObjectKey::new(entry.slug.owner_artefact_key(&job.owner, KIND, &definition.id, definition.version)?)?;
        if repairing {
            // Often only this node's cached copy was bad, so the object is read afresh before anything is rebuilt.
            self.runtime.forget_file(&self.database, &key);
            self.storage.invalidate(&key).await?;
            match self.runtime.file(&self.database, &self.storage, &key, &columns).await {
                Ok(file) => { self.runtime.clear(&suspect); return Ok(record(file.sections())); }
                Err(crate::runtime::FileError::Unavailable(error)) => return Err(error),
                Err(crate::runtime::FileError::Missing) => {}
                Err(crate::runtime::FileError::Corrupt(_)) => {
                    self.runtime.forget_file(&self.database, &key);
                    self.storage.invalidate(&key).await?;
                    // The store refuses different bytes under an existing key; a reader treats the missing file as absent meanwhile.
                    self.store.delete(&key).await?;
                }
            }
        }
        let workdir = tempfile::Builder::new().prefix("rtdb-search-").tempdir().map_err(fault)?;
        let (inputs, size) = match source {
            rtdb_maintenance::ArtefactSource::Local { data, size } => {
                if !matches!(job.owner, ArtefactOwner::Trie(_)) {
                    return Err(fault("A local compaction output covers one trie"));
                }
                (vec![std::fs::File::open(data).map_err(fault)?], i64::try_from(size).unwrap_or(i64::MAX))
            }
            rtdb_maintenance::ArtefactSource::Stored => {
                let tries = job.tries();
                let catalog = snapshot.trie_catalog();
                let mut inputs = Vec::with_capacity(tries.len());
                let mut size = 0i64;
                for (member, trie) in tries.iter().enumerate() {
                    inputs.push(download(self.store.as_ref(), &self.memory, &ObjectKey::new(entry.slug.data_key(trie))?, &workdir.path().join(format!("input-{member}.arrow")), cancellation).await?);
                    if let Some(entry) = catalog.trie(&definition.table, trie) { size = size.saturating_add(entry.data_file_size); }
                }
                (inputs, size)
            }
        };
        let indexing = self.memory.reserve(indexing_allowance(size))?;
        let (memory, token, bundle) = (Arc::clone(&self.memory), cancellation.clone(), workdir.path().join("bundle.bin"));
        let (segment, output) = (workdir.path().join("segment"), bundle.clone());
        let recency = match &job.owner {
            ArtefactOwner::Trie(trie) => trie.recency(),
            ArtefactOwner::Group(group) => group.recency(),
        };
        let ceiling = crate::file::ceiling_of(recency);
        let sections = tokio::task::spawn_blocking(move || -> Result<Vec<crate::file::Section>> {
            let _indexing = indexing;
            let opened = inputs.into_iter().map(|input| open_input(input, &memory)).collect::<Result<Vec<_>>>()?;
            let (readers, _reservations): (Vec<_>, Vec<_>) = opened.into_iter().unzip();
            std::fs::create_dir(&segment).map_err(fault)?;
            let directory = tantivy::directory::MmapDirectory::open(&segment).map_err(fault)?;
            let output = std::io::BufWriter::new(std::fs::File::create(&output).map_err(fault)?);
            let members = readers.into_iter().map(|reader| PageRows::new(reader, &columns, &memory, &token));
            write_group_search_file(&columns, members, &identity, ceiling, directory, output)
        }).await.map_err(|error| Anomaly::fault("rtdb.search/build", error.to_string()))??;
        if cancellation.is_cancelled() { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled")); }
        self.store.put_file(&key, &bundle).await?;
        if repairing { self.runtime.clear(&suspect); }
        Ok(record(&sections))
    }
}

/// The heap a segment's indexing may take, besides the page being read: tantivy holds a segment's postings in memory until it serializes them.
pub(crate) fn indexing_allowance(trie_size: i64) -> usize { usize::try_from(trie_size.max(0)).unwrap_or(usize::MAX).saturating_add(32 << 20) }

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/build", error.to_string()) }

fn search_row(batch: &EventBatch, row: usize, columns: &[String]) -> Result<SearchRow> {
    let header = batch.header(row)?;
    let values = columns.iter().map(|column| Ok(match batch.read_put_field(row, column)? {
        Value::Null | Value::Undefined => None,
        Value::Utf8(text) => Some(rtdb_text::tokens(&text)),
        _ => Some(Vec::new()),
    })).collect::<Result<_>>()?;
    Ok(SearchRow { iid: header.iid.0, system_from: header.system_from.0, valid_from: header.valid_time.lower().0,
        valid_to: header.valid_time.upper().0, op: header.op, values })
}

/// A data file's pages of rows in order, each read under its memory reservation.
struct PageRows<'a> { reader: ArrowFile, columns: &'a [String], memory: &'a Arc<MemoryBudget>, cancellation: &'a CancellationToken, page: usize }

impl<'a> PageRows<'a> {
    fn new(reader: ArrowFile, columns: &'a [String], memory: &'a Arc<MemoryBudget>, cancellation: &'a CancellationToken) -> Self {
        Self { reader, columns, memory, cancellation, page: 0 }
    }
}

impl Iterator for PageRows<'_> {
    type Item = Result<Vec<SearchRow>>;
    fn next(&mut self) -> Option<Result<Vec<SearchRow>>> {
        if self.page == self.reader.page_count() { return None; }
        if self.cancellation.is_cancelled() { return Some(Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled"))); }
        let page = self.page;
        self.page += 1;
        Some(read_page(&mut self.reader, page, self.memory).and_then(|owned| {
            let batch = EventBatch::new(owned.batch().clone())?;
            (0..batch.len()).map(|row| search_row(&batch, row, self.columns)).collect()
        }))
    }
}

/// A data file's pages of rows in order, read one at a time, with the tokens of each column's value.
pub fn search_pages<'a>(data: &[u8], columns: &'a [String], cancellation: &'a CancellationToken) -> Result<impl Iterator<Item = Result<Vec<SearchRow>>> + 'a> {
    let mut file = ArrowFile::open(Arc::from(data), None)?;
    let mut page = 0;
    Ok(std::iter::from_fn(move || {
        if page == file.page_count() { return None; }
        if cancellation.is_cancelled() { return Some(Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled"))); }
        page += 1;
        Some(file.read_page(page - 1).and_then(|batch| {
            let batch = EventBatch::new(batch)?;
            (0..batch.len()).map(|row| search_row(&batch, row, columns)).collect()
        }))
    }))
}
