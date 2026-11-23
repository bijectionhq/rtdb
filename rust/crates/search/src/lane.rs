use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::{EncodedEvent, EventBatch, EventRef, PutColumn, PutText}, ipc::ArrowFile, keys::ArtefactOwner, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactJob};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{Anomaly, EventOp, LogicalType, Result};
use tokio_util::sync::CancellationToken;

use crate::{SearchRow, file::{artefact_identity, write_versioned_group}, definitions::SearchDefinition};
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

/// Builds search files from the public and protected search catalogs of one database.
pub struct SearchBuilder {
    database: String,
    store: Arc<dyn ObjectStore>,
    storage: Arc<BufferPool>,
    memory: Arc<MemoryBudget>,
    clock: Arc<dyn Fn() -> rtdb_types::Instant + Send + Sync>,
    runtime: Arc<crate::SearchRuntime>,
    columns: Mutex<BTreeMap<(rtdb_types::TableRef, String, i64), Vec<String>>>,
}

impl SearchBuilder {
    pub fn new(database: String, store: Arc<dyn ObjectStore>, storage: Arc<BufferPool>, memory: Arc<MemoryBudget>, clock: Arc<dyn Fn() -> rtdb_types::Instant + Send + Sync>, runtime: Arc<crate::SearchRuntime>) -> Self {
        Self { database, store, storage, memory, clock, runtime, columns: Mutex::new(BTreeMap::new()) }
    }
    async fn search_definitions(&self, partition: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<SearchDefinition>> {
        let now = (self.clock)();
        let snapshot = partition.snapshot();
        let public = self.runtime.definitions(&self.database, std::slice::from_ref(&snapshot), &self.storage, &self.memory, now, cancellation).await?;
        let owned = crate::definitions::owned_definitions_in(&self.database, snapshot, &self.storage, &self.memory, now, cancellation).await?;
        Ok(public.iter().cloned().chain(owned).collect())
    }
}

#[async_trait]
impl ArtefactBuilder for SearchBuilder {
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        let definitions = self.search_definitions(snapshot, cancellation).await?;
        let mut columns = self.columns.lock();
        Ok(definitions.iter().map(|definition| {
            let id = definition.artefact_id();
            columns.insert((definition.table.clone(), id.clone(), definition.version), definition.columns.clone());
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
        let record = |sections: &[crate::file::Section], size: u64| -> Result<ArtefactRecord> {
            Ok(ArtefactRecord {
                kind: Some(KIND.into()),
                definition_id: Some(definition.id.clone()),
                definition_version: Some(definition.version),
                stats: Some(crate::file::encode_sections(sections)),
                scope: None,
                file_size: Some(i64::try_from(size).map_err(fault)?),
                published_version: None,
            })
        };
        let known = self.columns.lock().get(&(definition.table.clone(), definition.id.clone(), definition.version)).cloned();
        let columns = match known { Some(columns) => columns, None => {
            self.definitions(snapshot, cancellation).await?;
            self.columns.lock().get(&(definition.table.clone(), definition.id.clone(), definition.version)).cloned().ok_or_else(|| Anomaly::incorrect("rtdb.search/definition-dropped", "The search index definition is no longer in force"))?
        } };
        let entry = snapshot.table_catalog().entry(&definition.table).ok_or_else(|| Anomaly::fault("rtdb.search/unknown-table", "Search job targets a table the catalog does not hold"))?;
        let identity = artefact_identity(entry.slug.as_str(), &job.owner, &definition.id, definition.version);
        let key = ObjectKey::new(entry.slug.owner_artefact_key(&job.owner, KIND, &definition.id, definition.version)?)?;
        if repairing {
            // Often only this node's cached copy was bad, so the object is read afresh before anything is rebuilt.
            self.runtime.forget_file(&self.database, &key);
            self.storage.invalidate(&key).await?;
            match self.runtime.file(&self.database, &self.storage, &key, &columns).await {
                Ok(file) if file.matches_artefact(&identity, definition.version) => {
                    let size = self.store.head_fresh(&key).await?.size;
                    let record = record(file.sections(), size)?;
                    self.runtime.clear(&suspect);
                    return Ok(record);
                }
                Err(crate::runtime::FileError::Unavailable(error)) => return Err(error),
                Err(crate::runtime::FileError::Missing) => {}
                Ok(_) | Err(crate::runtime::FileError::Corrupt(_)) => {
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
        let version = definition.version;
        let owned = rtdb_types::TableRef::owned_schema(&definition.table.schema);
        let sections = tokio::task::spawn_blocking(move || -> Result<Vec<crate::file::Section>> {
            let _indexing = indexing;
            let opened = inputs.into_iter().map(|input| open_input(input, &memory)).collect::<Result<Vec<_>>>()?;
            let (readers, _reservations): (Vec<_>, Vec<_>) = opened.into_iter().unzip();
            std::fs::create_dir(&segment).map_err(fault)?;
            let directory = tantivy::directory::MmapDirectory::open(&segment).map_err(fault)?;
            let output = std::io::BufWriter::new(std::fs::File::create(&output).map_err(fault)?);
            let members = readers.into_iter().map(|reader| PageRows::new(reader, &columns, &memory, &token, owned));
            write_versioned_group(&columns, members, &identity, version, ceiling, directory, output)
        }).await.map_err(|error| Anomaly::fault("rtdb.search/build", error.to_string()))??;
        if cancellation.is_cancelled() { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled")); }
        let size = tokio::fs::metadata(&bundle).await.map_err(fault)?.len();
        let mut record = record(&sections, size)?;
        record.published_version = self.store.put_file_with_publication(&key, &bundle).await?.map(Into::into);
        if repairing { self.runtime.clear(&suspect); }
        Ok(record)
    }
}

/// The heap a segment's indexing may take, besides the page being read: tantivy holds a segment's postings in memory until it serializes them.
pub(crate) fn indexing_allowance(trie_size: i64) -> usize { usize::try_from(trie_size.max(0)).unwrap_or(usize::MAX).saturating_add(32 << 20) }

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/build", error.to_string()) }

fn search_row(event: EventRef<'_>, columns: &[String], owned: bool, memory: &Arc<MemoryBudget>) -> Result<SearchRow> {
    event.validate_document(memory)?;
    let values = columns.iter().map(|column| Ok(match event.put_text(column)? {
        PutText::Missing => None,
        PutText::Utf8(text) => Some(rtdb_text::tokens(text)),
        PutText::Other => Some(Vec::new()),
    })).collect::<Result<_>>()?;
    let header = event.header();
    let identity = if owned {
        let key = event.entity_key().ok_or_else(|| fault("Owned search event is missing its physical identity"))?;
        let _key = memory.reserve(key.len())?;
        let id = rtdb_types::EntityId::from_handle_bytes(key)?;
        if header.op == EventOp::Put {
            let Some(PutColumn::Stored(field, array, row)) = event.put_column("_id") else {
                return Err(fault("Owned search put is missing its identity"));
            };
            let _scratch = memory.reserve(rtdb_columnar::estimate_copy_cell_bytes(field, array, row)?)?;
            if event.put_cell("_id", &LogicalType::Null)?.entity_id()? != id {
                return Err(fault("Owned search document identity differs from its physical key"));
            }
        }
        Some(id)
    } else {
        None
    };
    Ok(SearchRow { identity, iid: header.iid.0, system_from: header.system_from.0, valid_from: header.valid_time.lower().0,
        valid_to: header.valid_time.upper().0, op: header.op, values })
}

fn search_batch(batch: &EventBatch, columns: &[String], owned: bool, memory: &Arc<MemoryBudget>) -> Result<Vec<SearchRow>> {
    (0..batch.len()).map(|row| search_row(batch.borrow_event(row)?, columns, owned, memory)).collect()
}

/// A data file's pages of rows in order, each read under its memory reservation.
struct PageRows<'a> { reader: ArrowFile, columns: &'a [String], memory: &'a Arc<MemoryBudget>, cancellation: &'a CancellationToken, page: usize, owned: bool }

impl<'a> PageRows<'a> {
    fn new(reader: ArrowFile, columns: &'a [String], memory: &'a Arc<MemoryBudget>, cancellation: &'a CancellationToken, owned: bool) -> Self {
        Self { reader, columns, memory, cancellation, page: 0, owned }
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
            search_batch(&batch, self.columns, self.owned, self.memory)
        }))
    }
}

/// A data file's pages of rows in order, read one at a time, with the tokens of each column's value.
pub fn search_pages<'a>(data: &[u8], columns: &'a [String], cancellation: &'a CancellationToken) -> Result<impl Iterator<Item = Result<Vec<SearchRow>>> + 'a> {
    search_pages_with_budget(Arc::<[u8]>::from(data), columns, cancellation, MemoryBudget::new(usize::MAX))
}

pub(crate) fn search_pages_with_budget<'a>(
    data: impl AsRef<[u8]> + Send + 'static, columns: &'a [String], cancellation: &'a CancellationToken, memory: Arc<MemoryBudget>,
) -> Result<impl Iterator<Item = Result<Vec<SearchRow>>> + 'a> {
    let mut file = ArrowFile::from_reader(std::io::Cursor::new(data), None)?;
    let mut page = 0;
    Ok(std::iter::from_fn(move || {
        if page == file.page_count() { return None; }
        if cancellation.is_cancelled() { return Some(Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled"))); }
        page += 1;
        Some(file.read_page(page - 1).and_then(|batch| {
            let batch = EventBatch::new(batch)?;
            search_batch(&batch, columns, false, &memory)
        }))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{EntityId, Iid, Interval, RunEndWidth, ScalarType, StructFields, Timestamp, Value};

    #[test]
    fn arrow_search_rows_keep_token_presence_and_owned_identity() {
        let identities = ["a", "b", "c", "d"].map(|id| EntityId::String(id.into()));
        let integer = LogicalType::Scalar(ScalarType::I64);
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let fields = StructFields::from([
            ("_id".into(), text.clone()),
            ("body".into(), text.clone().join(&integer).maybe()),
            ("title".into(), text.maybe()),
            ("encoded".into(), LogicalType::RunEndEncoded { width: RunEndWidth::I16, value: Box::new(LogicalType::Scalar(ScalarType::Utf8).maybe()) }),
            ("unindexed".into(), LogicalType::list(integer).maybe()),
        ]);
        let events = [
            StoredOperation::Put(StructFields::from([
                ("_id".into(), Value::Utf8("a".into())),
                ("body".into(), Value::Utf8("One TWO".into())),
                ("title".into(), Value::Utf8("visible title".into())),
                ("encoded".into(), Value::Utf8("one run".into())),
                ("unindexed".into(), Value::List(vec![Value::I64(1); 32])),
            ])),
            StoredOperation::Put(StructFields::from([
                ("_id".into(), Value::Utf8("b".into())),
                ("body".into(), Value::I64(42)),
                ("title".into(), Value::Null),
            ])),
            StoredOperation::Delete,
            StoredOperation::Erase,
        ].into_iter().enumerate().map(|(index, operation)| StoredEvent {
            commit_position: 0,
            entity_key: Some(Arc::from(identities[index].handle_bytes())),
            iid: Iid([index as u8; 16]), system_from: Timestamp(10), valid_time: Interval::ALL, recency: None, operation,
        }).collect::<Vec<_>>();
        let batch = EventBatch::new(build_event_batch(&events, Some(fields), None).unwrap()).unwrap();
        let first = batch.borrow_event(0).unwrap();
        assert!(matches!(first.put_text("title").unwrap(), PutText::Utf8("visible title")));
        assert!(matches!(first.put_text("body").unwrap(), PutText::Utf8("One TWO")));
        assert!(matches!(first.put_text("encoded").unwrap(), PutText::Utf8("one run")));
        assert!(matches!(batch.borrow_event(1).unwrap().put_text("encoded").unwrap(), PutText::Missing));
        let columns = ["body".into(), "title".into(), "missing".into(), "encoded".into()];
        let memory = MemoryBudget::new(1 << 20);
        let result = search_batch(&batch, &columns, true, &memory).unwrap();
        let expected = (0..batch.len()).map(|row| {
            let event = batch.read_event(row).unwrap();
            let header = event.header();
            let document = match &event.operation { StoredOperation::Put(document) => Some(document), _ => None };
            let values = columns.iter().map(|name| match document.and_then(|document| document.get(name)) {
                None | Some(Value::Null | Value::Undefined) => None,
                Some(Value::Utf8(text)) => Some(rtdb_text::tokens(text)),
                Some(_) => Some(vec![]),
            }).collect();
            SearchRow {
                identity: Some(identities[row].clone()),
                iid: header.iid.0, system_from: header.system_from.0, valid_from: header.valid_time.lower().0,
                valid_to: header.valid_time.upper().0, op: header.op, values,
            }
        }).collect::<Vec<_>>();
        assert_eq!(result, expected);
        assert_eq!(memory.used_bytes(), 0);
    }

}
