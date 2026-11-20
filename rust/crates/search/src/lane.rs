use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::{EventBatch, StoredOperation}, ipc::ArrowFile, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactJob};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{Anomaly, Result, Value};
use tokio_util::sync::CancellationToken;

use crate::{SearchRow, file::write_search_file, definitions::SearchDefinition};
use rtdb_maintenance::input::{download, open_input, read_page};

pub const KIND: &str = "search";

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
            ArtefactDefinition { table: definition.table.clone(), kind: KIND.into(), id, version: definition.version }
        }).collect())
    }

    async fn repairs(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactJob>> {
        let suspects = self.runtime.suspects(&self.database);
        if suspects.is_empty() { return Ok(Vec::new()); }
        let definitions = self.definitions(snapshot, cancellation).await?;
        let mut jobs = Vec::new();
        for suspect in suspects {
            let definition = definitions.iter().find(|definition| definition.table == suspect.table && definition.id == suspect.artefact_id && definition.version == suspect.version);
            let recorded = snapshot.trie_catalog().iter_tries(&suspect.table)
                .any(|trie| trie.key == suspect.trie && trie.state == rtdb_index::TrieState::Live && trie.artefact(KIND, &suspect.artefact_id, suspect.version).is_some());
            match definition {
                Some(definition) if recorded => jobs.push(ArtefactJob { definition: definition.clone(), trie: suspect.trie.clone() }),
                _ => self.runtime.clear(&suspect),
            }
        }
        Ok(jobs)
    }

    async fn wait_for_work(&self) { self.runtime.reported(&self.database).await }

    async fn build(&self, job: &ArtefactJob, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let definition = &job.definition;
        let suspect = crate::runtime::Suspect { database: self.database.clone(), table: definition.table.clone(), trie: job.trie.clone(), artefact_id: definition.id.clone(), version: definition.version };
        let repairing = self.runtime.is_suspect(&suspect);
        let record = || ArtefactRecord { kind: Some(KIND.into()), definition_id: Some(definition.id.clone()), definition_version: Some(definition.version), stats: None };
        let known = self.columns.lock().get(&(definition.id.clone(), definition.version)).cloned();
        let columns = match known { Some(columns) => columns, None => {
            self.definitions(snapshot, cancellation).await?;
            self.columns.lock().get(&(definition.id.clone(), definition.version)).cloned().ok_or_else(|| Anomaly::incorrect("rtdb.search/definition-dropped", "The search index definition is no longer in force"))?
        } };
        let entry = snapshot.table_catalog().entry(&definition.table).ok_or_else(|| Anomaly::fault("rtdb.search/unknown-table", "Search job targets a table the catalog does not hold"))?;
        let trie_size = snapshot.trie_catalog().iter_tries(&definition.table).find(|trie| trie.key == job.trie).map_or(0, |trie| trie.data_file_size);
        let identity = format!("{}/{}/{}-v{}", entry.slug.as_str(), job.trie, definition.id, definition.version);
        let key = ObjectKey::new(entry.slug.artefact_key(&job.trie, KIND, &definition.id, definition.version)?)?;
        if repairing {
            // Often only this node's cached copy was bad, so the object is read afresh before anything is rebuilt.
            self.runtime.forget_file(&self.database, &key);
            self.storage.invalidate(&key).await?;
            match self.runtime.file(&self.database, &self.storage, &key, &columns).await {
                Ok(_) => { self.runtime.clear(&suspect); return Ok(record()); }
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
        let input = download(self.store.as_ref(), &self.memory, &ObjectKey::new(entry.slug.data_key(&job.trie))?, &workdir.path().join("input.arrow"), cancellation).await?;
        let indexing = self.memory.reserve(indexing_allowance(trie_size))?;
        let (memory, token, bundle) = (Arc::clone(&self.memory), cancellation.clone(), workdir.path().join("bundle.bin"));
        let (segment, output) = (workdir.path().join("segment"), bundle.clone());
        tokio::task::spawn_blocking(move || -> Result<()> {
            let _indexing = indexing;
            let (reader, _opened) = open_input(input, &memory)?;
            std::fs::create_dir(&segment).map_err(fault)?;
            let directory = tantivy::directory::MmapDirectory::open(&segment).map_err(fault)?;
            let output = std::io::BufWriter::new(std::fs::File::create(&output).map_err(fault)?);
            write_search_file(&columns, PageRows::new(reader, &columns, &memory, &token), &identity, directory, output)
        }).await.map_err(|error| Anomaly::fault("rtdb.search/build", error.to_string()))??;
        if cancellation.is_cancelled() { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled")); }
        self.store.put_file(&key, &bundle).await?;
        if repairing { self.runtime.clear(&suspect); }
        Ok(record())
    }
}

/// The heap a segment's indexing may take, besides the page being read: tantivy holds a segment's postings in memory until it serializes them.
pub(crate) fn indexing_allowance(trie_size: i64) -> usize { usize::try_from(trie_size.max(0)).unwrap_or(usize::MAX).saturating_add(32 << 20) }

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/build", error.to_string()) }

fn search_row(event: &rtdb_format::data::StoredEvent, columns: &[String]) -> SearchRow {
    let values = columns.iter().map(|column| match &event.operation {
        StoredOperation::Put(document) => match document.get(column) {
            None | Some(Value::Null | Value::Undefined) => None,
            Some(Value::Utf8(text)) => Some(rtdb_text::tokens(text)),
            Some(_) => Some(Vec::new()),
        },
        _ => None,
    }).collect();
    SearchRow { iid: event.iid.0, system_from: event.system_from.0, valid_from: event.valid_time.lower().0, valid_to: event.valid_time.upper().0,
        recency: event.recency.map(|recency| recency.0), values }
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
            (0..batch.len()).map(|row| Ok(search_row(&batch.read_event(row)?, self.columns))).collect()
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
            (0..batch.len()).map(|row| Ok(search_row(&batch.read_event(row)?, columns))).collect()
        }))
    }))
}
