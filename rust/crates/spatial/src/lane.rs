use std::sync::Arc;

use async_trait::async_trait;
use geometry::GeometryWork;
use rtdb_format::{data::EventBatch, keys::ArtefactOwner, log::proto::ArtefactRecord};
use rtdb_index::PartitionValue;
use rtdb_maintenance::{
    ArtefactBuilder, ArtefactDatabase, ArtefactDefinition, ArtefactJob, ArtefactKind,
    ArtefactSource, input,
};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, Result};
use tokio_util::sync::CancellationToken;

use crate::{definition, kernel, postings};

pub struct SpatialArtefacts;

impl ArtefactKind for SpatialArtefacts {
    fn name(&self) -> &str {
        definition::KIND
    }

    fn builder(&self, database: ArtefactDatabase) -> Result<Arc<dyn ArtefactBuilder>> {
        Ok(Arc::new(Builder(database)))
    }
}

struct Builder(ArtefactDatabase);

impl Builder {
    async fn read_definitions(
        &self,
        snapshot: &PartitionValue,
        cancellation: &CancellationToken,
    ) -> Result<Vec<definition::Definition>> {
        let database = &self.0;
        definition::definitions(
            &database.name,
            snapshot.snapshot(),
            Arc::clone(&database.storage),
            Arc::clone(&database.memory),
            (database.clock)(),
            cancellation,
        )
        .await
    }
}

#[async_trait]
impl ArtefactBuilder for Builder {
    async fn definitions(
        &self,
        snapshot: &Arc<PartitionValue>,
        cancellation: &CancellationToken,
    ) -> Result<Vec<ArtefactDefinition>> {
        Ok(self
            .read_definitions(snapshot, cancellation)
            .await?
            .iter()
            .map(definition::Definition::artefact)
            .collect())
    }

    async fn build(
        &self,
        job: &ArtefactJob,
        source: ArtefactSource<'_>,
        snapshot: &Arc<PartitionValue>,
        cancellation: &CancellationToken,
    ) -> Result<ArtefactRecord> {
        let definition = self
            .read_definitions(snapshot, cancellation)
            .await?
            .into_iter()
            .find(|definition| definition.artefact() == job.definition)
            .ok_or_else(|| fault("Spatial definition left force before its build"))?;
        let ArtefactOwner::Trie(trie) = &job.owner else {
            return Err(fault("A spatial posting file covers one trie"));
        };
        let table = snapshot
            .table_catalog()
            .entry(&definition.table)
            .ok_or_else(|| fault("Unknown spatial relation"))?;
        let key = ObjectKey::new(table.slug.owner_artefact_key(
            &job.owner,
            definition::KIND,
            &job.definition.id,
            definition.generation,
        )?)?;
        let dir = tempfile::Builder::new()
            .prefix("rtdb-spatial-")
            .tempdir()
            .map_err(fault)?;
        let source = match source {
            ArtefactSource::Local { data, .. } => std::fs::File::open(data).map_err(fault)?,
            ArtefactSource::Stored => {
                input::download(
                    self.0.store.as_ref(),
                    &self.0.memory,
                    &ObjectKey::new(table.slug.data_key(trie))?,
                    &dir.path().join("source.arrow"),
                    cancellation,
                )
                .await?
            }
        };
        let output = dir.path().join("postings.arrow");
        let bundle = output.clone();
        let memory = Arc::clone(&self.0.memory);
        let cancel = cancellation.clone();
        let (size, root) = tokio::task::spawn_blocking(move || {
            let _geometry = memory.reserve(kernel::WORK_BYTES)?;
            let (mut reader, _metadata) = input::open_input(source, &memory)?;
            let mut postings = postings::Postings::new(&memory)?;
            let mut put_columns = definition.partition.clone();
            put_columns.push(definition.geometry.clone());
            for page in 0..reader.page_count() {
                if cancel.is_cancelled() {
                    return Err(postings::cancelled());
                }
                let (batch, _page) = reader.read_event_page_reserved(page, &put_columns, &memory)?;
                let events = EventBatch::new(batch)?;
                for row in 0..events.len() {
                    let mut work =
                        GeometryWork::with_limits(kernel::WORK_STEPS, kernel::WORK_BYTES);
                    if let Some((partition, iid, keys)) =
                        postings::event_keys(&events.borrow_event(row)?, &definition, &mut work)?
                    {
                        postings.append(partition, iid, keys.as_slice())?;
                    }
                }
            }
            let writer = std::fs::File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(bundle)
                .map_err(fault)?;
            postings.write(writer, &cancel)
        })
        .await
        .map_err(fault)??;
        if cancellation.is_cancelled() {
            return Err(postings::cancelled());
        }
        let published_version = self
            .0
            .store
            .put_file_with_publication(&key, &output)
            .await?
            .map(Into::into);
        Ok(ArtefactRecord {
            kind: Some(definition::KIND.into()),
            definition_id: Some(job.definition.id.clone()),
            definition_version: Some(job.definition.version),
            stats: Some(root),
            scope: None,
            file_size: Some(i64::try_from(size).map_err(fault)?),
            published_version,
            pack_members: Vec::new(),
        })
    }
}

fn fault(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.spatial/build", error.to_string())
}
