use std::{fs::File, io::BufWriter, path::Path, sync::Arc};

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::EventBatch, entity_history::{self, HistoryWriter}, keys::{ArtefactOwner, TrieKey},
    log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Result, TableRef};
use tokio_util::sync::CancellationToken;

use crate::{ArtefactBuilder, ArtefactDatabase, ArtefactDefinition, ArtefactJob, ArtefactKind, ArtefactSource,
    input::read_page, merge::{PrimaryData, primary_data_budget}};

pub struct EntityHistoryArtefacts;

impl ArtefactKind for EntityHistoryArtefacts {
    fn name(&self) -> &str { entity_history::KIND }
    fn builder(&self, database: ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>> {
        let directory = crate::entity_directory::EntityDirectoryBuilder::new(&database);
        Ok(Arc::new(EntityHistoryBuilder { store: database.store, memory: database.memory, directory }))
    }
}

struct EntityHistoryBuilder {
    store: Arc<dyn ObjectStore>,
    memory: Arc<MemoryBudget>,
    directory: crate::entity_directory::EntityDirectoryBuilder,
}

fn definition(table: TableRef) -> ArtefactDefinition {
    ArtefactDefinition { table, kind: entity_history::KIND.into(), id: entity_history::ID.into(), version: entity_history::VERSION,
        historical: true, group_bytes: None, packed: true }
}

#[async_trait]
impl ArtefactBuilder for EntityHistoryBuilder {
    fn retire_with_owner(&self, _: &ArtefactRecord) -> bool { true }

    async fn definitions(&self, value: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        Ok(value.table_catalog().entries().map(|entry| definition(entry.table.clone())).collect())
    }

    async fn seal_serving_search(&self, value: &Arc<PartitionValue>, table: &TableRef, trie: &TrieKey, path: &Path,
        size: u64, cancellation: &CancellationToken) -> Result<Vec<ArtefactRecord>> {
        let job = ArtefactJob { definition: definition(table.clone()), owner: ArtefactOwner::Trie(trie.clone()) };
        let mut records = self.directory.seal_serving_search(value, table, trie, path, size, cancellation).await?;
        match self.build(&job, ArtefactSource::Local { data: path, size }, value, cancellation).await {
            Ok(record) => records.push(record),
            Err(error) if cancellation.is_cancelled() => return Err(error),
            Err(_) => {}
        }
        Ok(records)
    }

    async fn build(&self, job: &ArtefactJob, source: ArtefactSource<'_>, value: &Arc<PartitionValue>,
        cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let ArtefactOwner::Trie(trie) = &job.owner else { return Err(crate::invalid("Entity history covers one primary trie")); };
        let table = value.table_catalog().entry(&job.definition.table).ok_or_else(|| crate::invalid("History table has no storage slug"))?;
        let temporary = Arc::new(tempfile::tempdir().map_err(crate::invalid)?);
        let input = match source {
            ArtefactSource::Local { data, .. } => PrimaryData::File(File::open(data).map_err(crate::invalid)?),
            ArtefactSource::Stored => {
                let key = ObjectKey::new(table.slug.data_key(trie))?;
                let size = self.store.head(&key).await?.size;
                PrimaryData::stored(self.store.as_ref(), &key, temporary.path().join("source.arrow"), size,
                    &self.memory, &primary_data_budget(&self.memory), cancellation).await?
            }
        };
        let output = temporary.path().join("history.arrow");
        let owner = entity_history::owner(&table.slug, trie);
        let memory = Arc::clone(&self.memory);
        let token = cancellation.clone();
        let writer_path = output.clone();
        let worker_directory = temporary.clone();
        let (root, size) = tokio::task::spawn_blocking(move || {
            let _directory = worker_directory;
            input.read(&memory, &token, |mut input| {
                let file = File::create(&writer_path).map_err(crate::invalid)?;
                let mut writer = HistoryWriter::new(BufWriter::new(file), owner, &memory)?;
                for page in 0..input.reader.page_count() {
                    if token.is_cancelled() { return Err(crate::closed()); }
                    let batch = read_page(&mut input.reader, page, &memory)?;
                    writer.append(u32::try_from(page).map_err(crate::invalid)?, &EventBatch::new(batch.batch().clone())?)?;
                }
                let (sink, root) = writer.finish()?;
                let size = i64::try_from(sink.get_ref().metadata().map_err(crate::invalid)?.len()).map_err(crate::invalid)?;
                Ok((root, size))
            })
        }).await.map_err(crate::invalid)??;
        if cancellation.is_cancelled() { return Err(crate::closed()); }
        let mut record = ArtefactRecord { kind: Some(entity_history::KIND.into()), definition_id: Some(entity_history::ID.into()),
            definition_version: Some(entity_history::VERSION), stats: Some(root.encode()), file_size: Some(size), ..Default::default() };
        let key = ObjectKey::new(table.slug.recorded_artefact_key(trie, &record)?)?;
        record.published_version = self.store.put_file_with_publication(&key, &output).await?.map(Into::into);
        Ok(record)
    }
}
