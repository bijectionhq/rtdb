use std::io::{Read, Seek, Write};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::{data::EventBatch, keys::ArtefactOwner, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDatabase, ArtefactDefinition, ArtefactJob, ArtefactKind, ArtefactSource, input};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;

use crate::{definition::{self, Definitions, VectorDefinition}, file, kmeans, rows::IndexedRows};

/// Builds one immutable generation per current trie for each approximate vector index (vector.allium: BuildGeneration).
/// The artefact lane publishes it with the trie, and the collector deletes it with the trie or its retired definition.
pub struct VectorArtefacts;

impl ArtefactKind for VectorArtefacts {
    fn name(&self) -> &str { definition::KIND }

    fn builder(&self, database: ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>> { Ok(Arc::new(Builder(database))) }
}

struct Builder(ArtefactDatabase);

impl Builder {
    async fn read_definitions(&self, snapshot: &PartitionValue, cancellation: &CancellationToken) -> Result<[Definitions; 3]> {
        let database = &self.0;
        let sql = definition::definitions_in(&database.name, vec![snapshot.snapshot()], Arc::clone(&database.storage),
            Arc::clone(&database.memory), (database.clock)(), cancellation).await?;
        let owned = definition::owned_definitions_in(&database.name, snapshot.snapshot(), Arc::clone(&database.storage),
            Arc::clone(&database.memory), (database.clock)(), cancellation).await?;
        let native = definition::native_definitions_in(&database.name, snapshot.snapshot(), Arc::clone(&database.storage),
            Arc::clone(&database.memory), (database.clock)(), cancellation).await?;
        Ok([sql, owned, native])
    }
}

#[async_trait]
impl ArtefactBuilder for Builder {
    async fn definitions(&self, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
        let definitions = self.read_definitions(snapshot, cancellation).await?;
        Ok(definitions.iter().flat_map(Definitions::rows).map(|definition| definition.artefact()).collect())
    }

    async fn build(&self, job: &ArtefactJob, source: ArtefactSource<'_>, snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let mut definitions = self.read_definitions(snapshot, cancellation).await?;
        let matches = |definition: &VectorDefinition| definition.artefact() == job.definition;
        let definition = definitions.iter_mut().find_map(|catalog| catalog.take(matches))
            .ok_or_else(|| fault("Vector definition left force before its build"))?;
        drop(definitions);
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
            let writer = std::fs::File::options().write(true).create_new(true).open(target).map_err(fault)?;
            write_generation(source, &definition, &memory, &cancel, writer)
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

fn write_generation(source: impl Read + Seek + Send + 'static, definition: &VectorDefinition,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken, writer: impl Write) -> Result<file::Written> {
    let mut staging = memory.reserve(0)?;
    let mut rows = IndexedRows::new(definition.dimensions);
    read_indexed_rows(source, definition, memory, cancellation, &mut rows,
        Arc::get_mut(&mut staging).expect("generation owns its staging reservation"))?;
    const WRITE_BUFFER_BYTES: usize = 8192;
    let _write_buffer = memory.reserve(WRITE_BUFFER_BYTES)?;
    let mut writer = std::io::BufWriter::with_capacity(WRITE_BUFFER_BYTES, writer);
    let written = file::write_generation(rows, staging, &mut writer, memory, cancellation)?;
    writer.flush().map_err(fault)?;
    Ok(written)
}

pub fn stage_native_generation(source: std::fs::File, definition: &VectorDefinition,
    memory: &Arc<MemoryBudget>, cancellation: &CancellationToken) -> Result<(std::fs::File, ArtefactRecord)> {
    let mut source = source;
    source.rewind().map_err(fault)?;
    let mut target = tempfile::tempfile().map_err(fault)?;
    let written = write_generation(source, definition, memory, cancellation, &mut target)?;
    let artefact = definition.artefact();
    let record = ArtefactRecord {
        kind: Some(artefact.kind),
        definition_id: Some(artefact.id),
        definition_version: Some(artefact.version),
        stats: Some(written.locator.encode()),
        file_size: Some(i64::try_from(written.size).map_err(fault)?),
        ..Default::default()
    };
    Ok((target, record))
}

/// Appends the indexable puts of a stored trie file in page order.
fn read_indexed_rows(source: impl Read + Seek + Send + 'static, definition: &VectorDefinition, memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken, output: &mut IndexedRows, staging: &mut MemoryReservation) -> Result<()> {
    let (mut reader, _metadata) = input::open_input(source, memory)?;
    let columns = definition.put_columns();
    for page in 0..reader.page_count() {
        if cancellation.is_cancelled() { return Err(kmeans::cancelled()); }
        let (batch, _page) = reader.read_event_page_reserved(page, &columns, memory)?;
        let events = EventBatch::new(batch)?;
        for row in 0..events.len() {
            output.push_event(&events.borrow_event(row)?, definition, staging)?;
        }
    }
    Ok(())
}

fn fault(error: impl std::fmt::Display) -> Anomaly { Anomaly::fault("rtdb.vector/build", error.to_string()) }
