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
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{SearchRow, build_search_file, definitions::{SearchDefinition, definitions_in}};

pub const KIND: &str = "search";

/// Per-column corpus totals, carried on the artefact record so a query can sum them without opening the file.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchStats { pub documents: u64, pub tokens: BTreeMap<String, u64> }

impl SearchStats {
    pub fn decode(bytes: &[u8]) -> Result<Self> { serde_json::from_slice(bytes).map_err(|error| Anomaly::fault("rtdb.search/stats", error.to_string())) }
}

/// The search artefact kind: install it in a node's services to build search files.
pub struct SearchArtefacts;

impl rtdb_maintenance::ArtefactKind for SearchArtefacts {
    fn name(&self) -> &str { KIND }
    fn builder(&self, database: rtdb_maintenance::ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>> {
        let clock = Arc::clone(&database.clock);
        Ok(Arc::new(SearchBuilder::new(database.name, database.store, database.storage, database.memory, Arc::new(move || clock().into()))))
    }
}

/// Builds the search files of one database, for every definition in `xt.search_indexes`.
pub struct SearchBuilder {
    database: String,
    store: Arc<dyn ObjectStore>,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    clock: Arc<dyn Fn() -> rtdb_types::Instant + Send + Sync>,
    columns: Mutex<BTreeMap<(String, i64), Vec<String>>>,
}

impl SearchBuilder {
    pub fn new(database: String, store: Arc<dyn ObjectStore>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, clock: Arc<dyn Fn() -> rtdb_types::Instant + Send + Sync>) -> Self {
        Self { database, store, storage, memory, clock, columns: Mutex::new(BTreeMap::new()) }
    }
    async fn search_definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<SearchDefinition>> {
        definitions_in(&self.database, vec![snapshot.snapshot()], Arc::clone(&self.storage), Arc::clone(&self.memory), (self.clock)(), cancellation).await
    }
}

#[async_trait]
impl ArtefactBuilder for SearchBuilder {
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        let definitions = self.search_definitions(snapshot, cancellation).await?;
        let mut columns = self.columns.lock();
        Ok(definitions.into_iter().map(|definition| {
            let id = definition.artefact_id();
            columns.insert((id.clone(), definition.version), definition.columns);
            ArtefactDefinition { table: definition.table, kind: KIND.into(), id, version: definition.version }
        }).collect())
    }

    async fn build(&self, job: &ArtefactJob, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let definition = &job.definition;
        let known = self.columns.lock().get(&(definition.id.clone(), definition.version)).cloned();
        let columns = match known { Some(columns) => columns, None => {
            self.definitions(snapshot, cancellation).await?;
            self.columns.lock().get(&(definition.id.clone(), definition.version)).cloned().ok_or_else(|| Anomaly::incorrect("rtdb.search/definition-dropped", "The search index definition is no longer in force"))?
        } };
        let entry = snapshot.table_catalog().entry(&definition.table).ok_or_else(|| Anomaly::fault("rtdb.search/unknown-table", "Search job targets a table the catalog does not hold"))?;
        let data = self.store.get(&ObjectKey::new(entry.slug.data_key(&job.trie))?).await?;
        let identity = format!("{}/{}/{}-v{}", entry.slug.as_str(), job.trie, definition.id, definition.version);
        let key = ObjectKey::new(entry.slug.artefact_key(&job.trie, KIND, &definition.id, definition.version)?)?;
        let build_columns = columns.clone();
        let (bytes, stats) = tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, SearchStats)> {
            let rows = search_rows(&data, &build_columns)?;
            let mut stats = SearchStats { documents: rows.len() as u64, tokens: BTreeMap::new() };
            for (index, column) in build_columns.iter().enumerate() {
                stats.tokens.insert(column.clone(), rows.iter().map(|row| row.tokens[index].len() as u64).sum());
            }
            Ok((build_search_file(&build_columns, &rows, &identity)?, stats))
        }).await.map_err(|error| Anomaly::fault("rtdb.search/build", error.to_string()))??;
        if cancellation.is_cancelled() { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled")); }
        self.store.put(&key, bytes::Bytes::from(bytes)).await?;
        let stats = serde_json::to_vec(&stats).map_err(|error| Anomaly::fault("rtdb.search/stats", error.to_string()))?;
        Ok(ArtefactRecord { kind: Some(KIND.into()), definition_id: Some(definition.id.clone()), definition_version: Some(definition.version), stats: Some(stats) })
    }
}

/// Every row of a data file in order, with the tokens of each column's UTF-8 value.
pub fn search_rows(data: &[u8], columns: &[String]) -> Result<Vec<SearchRow>> {
    let mut file = ArrowFile::open(Arc::from(data), None)?;
    let mut rows = Vec::new();
    for page in 0..file.page_count() {
        let batch = EventBatch::new(file.read_page(page)?)?;
        for row in 0..batch.len() {
            let event = batch.read_event(row)?;
            let tokens = columns.iter().map(|column| match &event.operation {
                StoredOperation::Put(document) => match document.get(column) { Some(Value::Utf8(text)) => rtdb_text::tokens(text), _ => Vec::new() },
                _ => Vec::new(),
            }).collect();
            rows.push(SearchRow { iid: event.iid.0, system_from: event.system_from.0, valid_from: event.valid_time.lower().0, valid_to: event.valid_time.upper().0,
                recency: event.recency.map(|recency| recency.0), tokens });
        }
    }
    Ok(rows)
}
