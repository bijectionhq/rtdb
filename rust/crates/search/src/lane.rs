use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_columnar::{EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_format::{data::{EncodedEvent, EventBatch, EventRef, PutText}, ipc::ArrowFile, keys::ArtefactOwner, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactJob};
use rtdb_storage::{ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_types::{Anomaly, EventOp, Result};
use tokio_util::sync::CancellationToken;

use crate::{SearchRow, file::{IndexedRow, SearchPage, artefact_identity, write_versioned_group}, definitions::SearchDefinition};
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
    fn builds_live_pages(&self) -> bool { true }

    async fn build_live_pages(&self, partition: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<()> {
        let snapshot = partition.snapshot();
        let definitions = self.runtime.definitions(&self.database, std::slice::from_ref(&snapshot), &self.storage, &self.memory,
            (self.clock)(), cancellation).await?;
        let (runtime, database, cancellation) = (Arc::clone(&self.runtime), self.database.clone(), cancellation.clone());
        tokio::task::spawn_blocking(move || {
            for definition in definitions.iter() {
                for segment in snapshot.iter_segments(&definition.table) {
                    for column in &definition.columns {
                        if cancellation.is_cancelled() { return Ok(()); }
                        runtime.publish_live(&database, segment, column, &cancellation)?;
                    }
                }
            }
            Ok(())
        }).await.map_err(fault)?
    }

    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        let definitions = self.search_definitions(snapshot, cancellation).await?;
        let mut columns = self.columns.lock();
        Ok(definitions.iter().map(|definition| {
            let id = definition.artefact_id_for(self.runtime.write_format());
            for format in [crate::file::TANTIVY_FORMAT_VERSION, crate::file::LEGACY_FORMAT_VERSION, crate::file::FORMAT_VERSION, crate::file::TREE_FORMAT_VERSION] {
                columns.insert((definition.table.clone(), definition.artefact_id_for(format), definition.version), definition.columns.clone());
            }
            ArtefactDefinition { table: definition.table.clone(), kind: KIND.into(), id, version: definition.version, historical: definition.history, group_bytes: Some(group_bytes(&self.memory)) }
        }).collect())
    }

    fn retained_definitions(&self, definitions: &[ArtefactDefinition]) -> Vec<ArtefactDefinition> {
        definitions.iter().flat_map(|definition| {
            let prefix = definition.id.rsplit_once('f').map_or(definition.id.as_str(), |(prefix, _)| prefix);
            [crate::file::TANTIVY_FORMAT_VERSION, crate::file::LEGACY_FORMAT_VERSION, crate::file::FORMAT_VERSION, crate::file::TREE_FORMAT_VERSION]
                .map(|format| ArtefactDefinition { id: format!("{prefix}f{format}"), ..definition.clone() })
        }).collect()
    }

    async fn repairs(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactJob>> {
        let suspects = self.runtime.suspects(&self.database);
        if suspects.is_empty() { return Ok(Vec::new()); }
        let definitions = self.retained_definitions(&self.definitions(snapshot, cancellation).await?);
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
        let format = definition.id.rsplit_once('f').and_then(|(_, format)| format.parse::<i32>().ok())
            .filter(|format| matches!(*format, crate::file::TANTIVY_FORMAT_VERSION | crate::file::LEGACY_FORMAT_VERSION | crate::file::FORMAT_VERSION | crate::file::TREE_FORMAT_VERSION))
            .ok_or_else(|| fault("Search artefact definition has an unsupported format"))?;
        let record = |stats: Vec<u8>, size: u64| -> Result<ArtefactRecord> {
            Ok(ArtefactRecord {
                kind: Some(KIND.into()),
                definition_id: Some(definition.id.clone()),
                definition_version: Some(definition.version),
                stats: Some(stats),
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
            let stats = job.tries().iter().filter_map(|trie| snapshot.trie_catalog().trie(&definition.table, trie))
                .filter_map(|trie| trie.artefact(KIND, &definition.id, definition.version))
                .find_map(|record| record.stats.as_deref());
            if format == crate::file::TREE_FORMAT_VERSION {
                let locator = stats.and_then(crate::file::decode_tree_locator)
                    .ok_or_else(|| fault("Recorded search tree has no root locator"))?;
                match self.runtime.tree_file(&self.storage, &key, &locator, &columns, &identity, definition.version, true).await {
                    Ok(file) if file.verify().await.is_ok() => {
                        self.runtime.clear(&suspect);
                        return record(crate::file::encode_tree_stats(&file.sections()?, &locator)?, locator.length);
                    }
                    Err(crate::runtime::FileError::Unavailable(error)) => return Err(error),
                    Err(crate::runtime::FileError::Missing) => {}
                    Ok(_) | Err(crate::runtime::FileError::Corrupt(_)) => {
                        self.storage.invalidate(&key).await?;
                        self.store.delete(&key).await?;
                    }
                }
            } else {
            let locator = stats.and_then(crate::file::decode_footer_locator);
            match self.runtime.file_for_repair(&self.database, &self.storage, &key, &columns, locator.as_ref()).await {
                Ok(file) if file.matches_artefact(&identity, definition.version) && file.verify().is_ok() => {
                    let size = self.store.head_fresh(&key).await?.size;
                    let stats = if format == crate::file::TANTIVY_FORMAT_VERSION {
                        crate::file::encode_tantivy_stats(file.sections())
                    } else {
                        crate::file::encode_catalog_stats(file.sections(), locator.as_ref())?
                    };
                    let record = record(stats, size)?;
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
        let (memory, token, bundle) = (Arc::clone(&self.memory), cancellation.clone(), workdir.path().join("search.arrow"));
        let output = bundle.clone();
        let recency = match &job.owner {
            ArtefactOwner::Trie(trie) => trie.recency(),
            ArtefactOwner::Group(group) => group.recency(),
        };
        let ceiling = crate::file::ceiling_of(recency);
        let version = definition.version;
        let owned = rtdb_types::TableRef::owned_schema(&definition.table.schema);
        let recorded_stats = tokio::task::spawn_blocking(move || -> Result<_> {
            let opened = inputs.into_iter().map(|input| open_input(input, &memory)).collect::<Result<Vec<_>>>()?;
            let file = std::io::BufWriter::new(std::fs::File::create(&output).map_err(fault)?);
            let members = opened.into_iter().map(|(reader, opening)| PageRows::new(reader, opening, Arc::clone(&memory), token.clone(), owned));
            let sections = write_versioned_group(&columns, members, &identity, version, ceiling, file, Some(indexing), format)?;
            let mut file = std::fs::File::open(output).map_err(fault)?;
            match format {
                crate::file::TREE_FORMAT_VERSION => crate::file::encode_tree_stats(&sections, &crate::file::TreeLocator::read(&mut file)?),
                crate::file::FORMAT_VERSION => crate::file::encode_catalog_stats(&sections, Some(&crate::file::FooterLocator::read(&mut file)?)),
                crate::file::TANTIVY_FORMAT_VERSION => Ok(crate::file::encode_tantivy_stats(&sections)),
                _ => crate::file::encode_catalog_stats(&sections, None),
            }
        }).await.map_err(|error| Anomaly::fault("rtdb.search/build", error.to_string()))??;
        if cancellation.is_cancelled() { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled")); }
        let size = tokio::fs::metadata(&bundle).await.map_err(fault)?.len();
        let mut record = record(recorded_stats, size)?;
        record.published_version = self.store.put_file_with_publication(&key, &bundle).await?.map(Into::into);
        if repairing { self.runtime.clear(&suspect); }
        Ok(record)
    }
}

pub(crate) fn indexing_allowance(trie_size: i64) -> usize { usize::try_from(trie_size.max(0)).unwrap_or(usize::MAX).saturating_add(32 << 20) }

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.search/build", error.to_string()) }

fn search_row<'a>(event: EventRef<'a>, columns: &[String], with_identity: bool) -> Result<IndexedRow<'a>> {
    let values = columns.iter().map(|column| Ok(match event.put_text(column)? {
        PutText::Missing => None,
        PutText::Utf8(text) => Some(rtdb_text::analyze(text)),
        PutText::Other => Some(arrow_array::StringArray::from(Vec::<&str>::new())),
    })).collect::<Result<_>>()?;
    let header = event.header();
    let key = if with_identity {
        let key = event.physical_entity_key().ok_or_else(|| fault("Owned search event is missing its complete identity"))?;
        let identity = EntityIdRef::from_handle_bytes(key)?;
        if header.op == EventOp::Put && event.put_identity()? != Some(identity) {
            return Err(fault("Owned search document identity differs from its physical key"));
        }
        Some(Cow::Borrowed(key))
    } else {
        None
    };
    let sql_id = if with_identity { None } else { integer_sql_id(&event) };
    Ok(IndexedRow { header, key, sql_id, values })
}

pub(crate) fn integer_sql_id(event: &EventRef<'_>) -> Option<i64> {
    match event.put_identity().ok().flatten() {
        Some(EntityIdRef::Integer(id)) if rtdb_types::EntityId::Integer(id).iid() == event.header().iid => Some(id),
        _ => None,
    }
}

pub(crate) fn test_row(row: IndexedRow<'_>) -> Result<SearchRow> {
    let IndexedRow { header, key, values, .. } = row;
    Ok(SearchRow {
        identity: key.as_deref().map(EntityIdRef::from_handle_bytes).transpose()?.map(EntityIdRef::into_owned),
        iid: header.iid.0, system_from: header.system_from.0, valid_from: header.valid_time.lower().0,
        valid_to: header.valid_time.upper().0, op: header.op,
        values: values.into_iter().map(|tokens| tokens.map(|tokens| tokens.iter().flatten().map(str::to_owned).collect())).collect(),
    })
}

#[cfg(test)]
fn search_batch(batch: &EventBatch, columns: &[String], owned: bool) -> Result<Vec<SearchRow>> {
    (0..batch.len()).map(|row| search_row(batch.borrow_event(row)?, columns, owned).and_then(test_row)).collect()
}

pub(crate) struct ArrowSearchPage {
    events: EventBatch,
    _owned: OwnedBatch,
    with_identity: bool,
}

impl SearchPage for ArrowSearchPage {
    fn len(&self) -> usize { self.events.len() }

    fn row(&self, row: usize, columns: &[String]) -> Result<IndexedRow<'_>> {
        search_row(self.events.borrow_event(row)?, columns, self.with_identity)
    }
}

pub(crate) struct PageRows {
    reader: ArrowFile,
    _opening: Arc<MemoryReservation>,
    memory: Arc<MemoryBudget>,
    cancellation: CancellationToken,
    page: usize,
    with_identity: bool,
}

impl PageRows {
    fn new(reader: ArrowFile, opening: Arc<MemoryReservation>, memory: Arc<MemoryBudget>,
           cancellation: CancellationToken, with_identity: bool) -> Self {
        Self { reader, _opening: opening, memory, cancellation, page: 0, with_identity }
    }
}

impl Iterator for PageRows {
    type Item = Result<ArrowSearchPage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.page == self.reader.page_count() { return None; }
        if self.cancellation.is_cancelled() {
            return Some(Err(Anomaly::new(rtdb_types::AnomalyCategory::Interrupted, "rtdb.search/cancelled", "Search build cancelled")));
        }
        let page = self.page;
        self.page += 1;
        Some(read_page(&mut self.reader, page, &self.memory).and_then(|owned| {
            let events = EventBatch::new(owned.batch().clone())?;
            Ok(ArrowSearchPage { events, _owned: owned, with_identity: self.with_identity })
        }))
    }
}

/// Reads a data file's indexed rows for diagnostics and fixtures.
pub fn search_pages<'a>(data: &[u8], columns: &'a [String], cancellation: &CancellationToken) -> Result<impl Iterator<Item = Result<Vec<SearchRow>>> + 'a> {
    let pages = search_pages_from_reader(std::io::Cursor::new(Arc::<[u8]>::from(data)), cancellation.clone(), MemoryBudget::new(usize::MAX))?;
    Ok(pages.map(move |page| {
        let page = page?;
        (0..page.len()).map(|row| page.row(row, columns).and_then(test_row)).collect()
    }))
}

pub(crate) fn search_pages_from_reader(
    reader: impl std::io::Read + std::io::Seek + Send + 'static, cancellation: CancellationToken, memory: Arc<MemoryBudget>,
) -> Result<PageRows> {
    let (reader, opening) = open_input(reader, &memory)?;
    Ok(PageRows::new(reader, opening, memory, cancellation, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::data::{StoredEvent, StoredOperation, build_event_batch};
    use rtdb_types::{EntityId, Iid, Interval, LogicalType, RunEndWidth, ScalarType, StructFields, Timestamp, Value};

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
                ("unindexed".into(), Value::list(vec![Value::I64(1); 32])),
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
        let result = search_batch(&batch, &columns, true).unwrap();
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
    }

    #[test]
    fn indexing_borrows_only_the_selected_cells() {
        use arrow_array::{StringArray, StructArray};
        let text = rtdb_columnar::to_field("body", &LogicalType::Scalar(ScalarType::Utf8)).unwrap();
        let uri = rtdb_columnar::to_field("uri", &LogicalType::Scalar(ScalarType::Uri)).unwrap();
        let document = StructArray::new(vec![text, uri].into(), vec![
            Arc::new(StringArray::from(vec!["indexed text"])),
            Arc::new(StringArray::from(vec!["%"])),
        ], None);
        let header = rtdb_types::EventHeader {
            iid: Iid([0; 16]), system_from: Timestamp(10), commit_position: 0, valid_time: Interval::ALL, op: EventOp::Put,
        };
        let memory = MemoryBudget::new(1 << 20);
        let owned = rtdb_format::data::owned_put_batch(Arc::new(document), &[header], None, &memory).unwrap();
        let batch = EventBatch::new(owned.batch().clone()).unwrap();
        assert!(batch.read_event(0).is_err());
        let rows = search_batch(&batch, &["body".into()], false).unwrap();
        assert_eq!(rows[0].values, [Some(vec!["indexed".into(), "text".into()])]);
    }

}
