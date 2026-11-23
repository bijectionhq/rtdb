use std::{fs::File, io::BufWriter, sync::Arc};

use rtdb_format::entity_history::{HistoryRoot, HistoryWriter, forward_owner};
use rtdb_format::keys::{TableSlug, TrieKey};
use rtdb_maintenance::{ArtefactDefinition, ArtefactJob, ArtefactSource};

use super::*;

pub(crate) fn forward_id(definition: &SearchDefinition) -> String {
    format!("{}w", definition.artefact_id_for(crate::file::LOGICAL_FORMAT_VERSION))
}

pub(super) struct ForwardOutput {
    writer: HistoryWriter<BufWriter<File>>,
    directory: tempfile::TempDir,
}

pub(super) struct ForwardFile {
    directory: tempfile::TempDir,
    root: HistoryRoot,
    size: i64,
}

impl ForwardOutput {
    pub(super) fn new(owner: [u8; 32], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let directory = tempfile::tempdir().map_err(fault)?;
        let sink = BufWriter::new(File::create(directory.path().join("forward.arrow")).map_err(fault)?);
        Ok(Self { writer: HistoryWriter::new(sink, owner, memory)?, directory })
    }

    pub(super) fn append_revisions(&mut self, page: usize, events: &EventBatch, columns: usize,
        revisions: &[Vec<Option<crate::file::TextRevision>>]) -> Result<()> {
        self.writer.append_forward_revisions(u32::try_from(page).map_err(fault)?, events, columns,
            |row, _, column| Ok(revisions[row][column]))
    }

    pub(super) fn finish(self) -> Result<ForwardFile> {
        let (sink, root) = self.writer.finish()?;
        let size = i64::try_from(sink.get_ref().metadata().map_err(fault)?.len()).map_err(fault)?;
        Ok(ForwardFile { directory: self.directory, root, size })
    }
}

impl ForwardFile {
    pub(super) fn path(&self) -> std::path::PathBuf { self.directory.path().join("forward.arrow") }

    #[cfg(test)]
    pub(super) fn root_bytes(&self) -> Vec<u8> { self.root.encode() }

    pub(super) async fn publish(self, store: &dyn ObjectStore, slug: &TableSlug, trie: &TrieKey,
        definition: &ArtefactDefinition, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut record = ArtefactRecord { kind: Some(KIND.into()), definition_id: Some(definition.id.clone()),
            definition_version: Some(definition.version), stats: Some(self.root.encode()), file_size: Some(self.size), ..Default::default() };
        let key = ObjectKey::new(slug.recorded_artefact_key(trie, &record)?)?;
        record.published_version = store.put_file_with_publication(&key, &self.path()).await?.map(Into::into);
        Ok(record)
    }
}

impl super::SearchBuilder {
    pub(super) async fn build_forward(&self, job: &ArtefactJob, source: ArtefactSource<'_>,
        snapshot: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let definition = &job.definition;
        let ArtefactOwner::Trie(trie) = &job.owner else { return Err(fault("Forward witnesses cover one primary trie")); };
        let entry = snapshot.table_catalog().entry(&definition.table).ok_or_else(|| fault("Forward witness table has no slug"))?;
        let columns = self.columns.lock().get(&(definition.table.clone(), definition.id.clone(), definition.version)).cloned()
            .ok_or_else(|| fault("Forward witness definition has no fields"))?;
        let directory = tempfile::tempdir().map_err(fault)?;
        let input = match source {
            ArtefactSource::Local { data, .. } => File::open(data).map_err(fault)?,
            ArtefactSource::Stored => {
                let key = ObjectKey::new(entry.slug.data_key(trie))?;
                download(self.store.as_ref(), &self.memory, &key, &directory.path().join("source.arrow"), cancellation).await?
            }
        };
        let owner = forward_owner(&entry.slug, trie, &definition.id, definition.version);
        let token = cancellation.clone();
        let memory = Arc::clone(&self.memory);
        let forward = tokio::task::spawn_blocking(move || {
            let (mut reader, _opening) = open_search_input(input, &columns, &memory)?;
            let mut output = ForwardOutput::new(owner, &memory)?;
            for page in 0..reader.page_count() {
                if token.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
                let batch = read_page(&mut reader, page, &memory)?;
                output.writer.append_forward(u32::try_from(page).map_err(fault)?, &EventBatch::new(batch.batch().clone())?, &columns,
                    |text| Ok(match text {
                        PutText::Utf8(text) => {
                            let _scratch = memory.reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                            Some(super::logical::text_revision(text))
                        }
                        _ => None,
                    }))?;
            }
            output.finish()
        }).await.map_err(fault)??;
        forward.publish(self.store.as_ref(), &entry.slug, trie, definition, cancellation).await
    }
}
