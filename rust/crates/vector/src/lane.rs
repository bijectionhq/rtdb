use std::io::{Read, Seek};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::EventBatch, keys::ArtefactOwner, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDatabase, ArtefactDefinition, ArtefactJob, ArtefactKind, ArtefactSource, input};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;

use crate::{definition::{self, VectorDefinition}, file, kmeans, rows::IndexedRows};

/// Builds one immutable generation per current trie for each approximate vector index (vector.allium: BuildGeneration).
/// The artefact lane publishes it with the trie, and the collector deletes it with the trie or its retired definition.
pub struct VectorArtefacts;

impl ArtefactKind for VectorArtefacts {
    fn name(&self) -> &str { definition::KIND }

    fn builder(&self, database: ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>> { Ok(Arc::new(Builder(database))) }
}

struct Builder(ArtefactDatabase);

impl Builder {
    async fn read_definitions(&self, snapshot: &PartitionValue, cancellation: &CancellationToken) -> Result<Vec<VectorDefinition>> {
        let database = &self.0;
        let mut definitions = definition::definitions_in(&database.name, vec![snapshot.snapshot()], Arc::clone(&database.storage),
            Arc::clone(&database.memory), (database.clock)(), cancellation).await?;
        definitions.extend(definition::owned_definitions_in(&database.name, snapshot.snapshot(), Arc::clone(&database.storage),
            Arc::clone(&database.memory), (database.clock)(), cancellation).await?);
        Ok(definitions)
    }
}

#[async_trait]
impl ArtefactBuilder for Builder {
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        Ok(self.read_definitions(snapshot, cancellation).await?.iter().map(VectorDefinition::artefact).collect())
    }

    async fn build(&self, job: &ArtefactJob, source: ArtefactSource<'_>, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let definition = self.read_definitions(snapshot, cancellation).await?.into_iter()
            .find(|definition| definition.artefact() == job.definition)
            .ok_or_else(|| fault("Vector definition left force before its build"))?;
        let ArtefactOwner::Trie(trie) = &job.owner else { return Err(fault("A vector generation covers one trie")) };
        let table = snapshot.table_catalog().entry(&definition.table).ok_or_else(|| fault("Unknown vector index relation"))?;
        let key = ObjectKey::new(table.slug.owner_artefact_key(&job.owner, definition::KIND, &job.definition.id, definition.generation)?)?;
        let dir = tempfile::Builder::new().prefix("rtdb-vector-").tempdir().map_err(fault)?;
        let source = match source {
            ArtefactSource::Local { data, .. } => std::fs::File::open(data).map_err(fault)?,
            ArtefactSource::Stored => input::download(self.0.store.as_ref(), &self.0.memory, &ObjectKey::new(table.slug.data_key(trie))?,
                &dir.path().join("source.arrow"), cancellation).await?,
        };
        let output = dir.path().join("generation.arrow");
        let target = output.clone();
        let memory = Arc::clone(&self.0.memory);
        let cancel = cancellation.clone();
        let written = tokio::task::spawn_blocking(move || {
            let mut rows = IndexedRows::new(definition.dimensions);
            read_indexed_rows(source, &definition, &memory, &cancel, &mut rows)?;
            let _vectors = memory.reserve(rows.rows().len().saturating_mul(definition.dimensions.saturating_mul(12)))?;
            let mut writer = std::io::BufWriter::new(std::fs::File::options().write(true).create_new(true).open(target).map_err(fault)?);
            let written = file::write_generation(rows, &mut writer, &cancel)?;
            std::io::Write::flush(&mut writer).map_err(fault)?;
            Ok::<_, Anomaly>(written)
        }).await.map_err(fault)??;
        if cancellation.is_cancelled() { return Err(kmeans::cancelled()); }
        let published_version = self.0.store.put_file_with_publication(&key, &output).await?.map(Into::into);
        Ok(ArtefactRecord {
            kind: Some(definition::KIND.into()),
            definition_id: Some(job.definition.id.clone()),
            definition_version: Some(job.definition.version),
            stats: Some(written.locator.encode()),
            scope: None,
            file_size: Some(i64::try_from(written.size).map_err(fault)?),
            published_version,
            pack_members: Vec::new(),
        })
    }
}

/// Appends the indexable puts of a stored trie file in page order.
pub(crate) fn read_indexed_rows(source: impl Read + Seek + Send + 'static, definition: &VectorDefinition, memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken, output: &mut IndexedRows) -> Result<()> {
    let (mut reader, _metadata) = input::open_input(source, memory)?;
    let columns = definition.put_columns();
    for page in 0..reader.page_count() {
        if cancellation.is_cancelled() { return Err(kmeans::cancelled()); }
        let _page = memory.reserve(reader.page_allocation_bound(page)?)?;
        let events = EventBatch::new(reader.read_event_page(page, &columns)?)?;
        for row in 0..events.len() {
            output.push_event(&events.borrow_event(row)?, definition)?;
        }
    }
    Ok(())
}

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.vector/build", error.to_string()) }
