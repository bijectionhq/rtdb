use std::{fs::File, io::BufWriter, sync::Arc};

use rtdb_format::entity_history::{HistoryWriter, forward_owner};
use rtdb_maintenance::{ArtefactJob, ArtefactSource};

use super::*;

pub(crate) fn forward_id(definition: &SearchDefinition) -> String {
    format!("{}w", definition.artefact_id_for(crate::file::LOGICAL_FORMAT_VERSION))
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
        let output = directory.path().join("forward.arrow");
        let path = output.clone();
        let owner = forward_owner(&entry.slug, trie, &definition.id, definition.version);
        let token = cancellation.clone();
        let memory = Arc::clone(&self.memory);
        let (root, size) = tokio::task::spawn_blocking(move || {
            let (mut reader, _opening) = open_input(input, &memory)?;
            let mut writer = HistoryWriter::new(BufWriter::new(File::create(path).map_err(fault)?), owner, &memory)?;
            for page in 0..reader.page_count() {
                if token.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
                let batch = read_page(&mut reader, page, &memory)?;
                writer.append_forward(u32::try_from(page).map_err(fault)?, &EventBatch::new(batch.batch().clone())?, &columns,
                    |text| Ok(match text {
                        PutText::Utf8(text) => {
                            let _scratch = memory.reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                            Some(super::logical::text_revision(text))
                        }
                        _ => None,
                    }))?;
            }
            let (sink, root) = writer.finish()?;
            Ok::<_, Anomaly>((root, i64::try_from(sink.get_ref().metadata().map_err(fault)?.len()).map_err(fault)?))
        }).await.map_err(fault)??;
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut record = ArtefactRecord { kind: Some(KIND.into()), definition_id: Some(definition.id.clone()),
            definition_version: Some(definition.version), stats: Some(root.encode()), file_size: Some(size), ..Default::default() };
        let key = ObjectKey::new(entry.slug.recorded_artefact_key(trie, &record)?)?;
        record.published_version = self.store.put_file_with_publication(&key, &output).await?.map(Into::into);
        Ok(record)
    }
}
