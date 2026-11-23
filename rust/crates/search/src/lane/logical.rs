use std::{collections::BTreeMap, fs::File, io::{BufWriter, Read}, path::Path, sync::Arc};

use arrow_array::Array;
use rtdb_format::{keys::{TableSlug, TrieKey}, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactsAdded, SearchRun, SearchRunChange}};
use rtdb_index::{PartitionValue, Snapshot};
use rtdb_maintenance::{ArtefactDefinition, ArtefactPublisher, SnapshotSource, input::{download, open_input}};
use rtdb_storage::ObjectKey;
use rtdb_types::{AnomalyCategory, BlockIndex, EventHeader, EventOp, Iid, Interval, Result, StorageEpoch, StorageVersion, TableRef, Timestamp};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{SearchDefinition, file::{BuildMode, IndexedRow, LOGICAL_FORMAT_VERSION, LogicalDocument, SearchPage, TextRevision, TreeLocator, write_group_as}};
use super::{PageRows, SearchBuilder, fault, indexing_allowance};

type Erases = BTreeMap<Iid, (Timestamp, u32)>;

#[derive(Default)]
struct RunContents {
    documents: Vec<LogicalDocument>,
    erases: Erases,
}

fn extend_erases(erases: &mut Erases, other: Erases) {
    for (iid, position) in other {
        erases.entry(iid).and_modify(|old| *old = (*old).max(position)).or_insert(position);
    }
}

fn scrub_documents(documents: &mut Vec<LogicalDocument>, erases: &Erases) -> bool {
    let before = documents.len();
    documents.retain(|document| erases.get(&document.iid).is_none_or(|erase| document.position >= *erase));
    documents.len() < before
}

pub(crate) fn text_revision(value: &str) -> TextRevision {
    let mut digest = Sha256::new();
    digest.update(rtdb_text::ANALYZER_VERSION.to_le_bytes());
    digest.update(value.as_bytes());
    digest.finalize().into()
}

pub(crate) fn history_id(definition: &SearchDefinition) -> String {
    format!("{}h", definition.artefact_id_for(LOGICAL_FORMAT_VERSION))
}

pub(crate) fn run_identity(slug: &TableSlug, definition: &str, version: i64, blocks: &[i64], rebuild: i64) -> String {
    format!("logical-search/{slug}/{definition}/{version}/{blocks:?}/{rebuild}", slug = slug.as_str())
}

pub(crate) struct LogicalIndex {
    table: TableRef,
    id: String,
    version: i64,
}

impl LogicalIndex {
    pub fn supports(definition: &SearchDefinition) -> bool {
        definition.history && !definition.table.is_owned_event_data() && !TableRef::owned_schema(&definition.table.schema)
    }

    fn of(definition: &SearchDefinition) -> Self {
        Self { table: definition.table.clone(), id: definition.artefact_id_for(LOGICAL_FORMAT_VERSION), version: definition.version }
    }

    pub fn from_history(definition: &ArtefactDefinition) -> Self {
        Self { table: definition.table.clone(), id: definition.id.trim_end_matches('h').to_owned(), version: definition.version }
    }

    fn records<'a>(&self, snapshot: &'a Snapshot) -> Vec<&'a SearchRun> {
        snapshot.trie_catalog().search_runs(&self.table)
            .filter(|run| run.definition_id() == self.id && run.definition_version() == self.version).collect()
    }
}

impl SearchBuilder {
    pub(super) async fn seal_logical(&self, index: &LogicalIndex, snapshot: &Arc<PartitionValue>, trie: &TrieKey, path: &Path, size: u64,
        cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let view = snapshot.snapshot();
        let seal = view.iter_segments(&index.table).next().cloned().map(|segment| (view.clone(), segment));
        let history_id = format!("{}h", index.id);
        let columns = self.columns.lock().get(&(index.table.clone(), history_id, index.version)).cloned()
            .ok_or_else(|| fault("Logical search definition has no fields"))?;
        let run = self.build_logical(index, &columns, &view, trie.block_index(), File::open(path).map_err(fault)?,
            size.min(i64::MAX as u64) as i64, seal, cancellation).await?;
        SearchRunChange { run: Some(run), ..Default::default() }.to_artefact()
    }

    async fn build_logical(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, block: BlockIndex, input: File,
        size: i64, seal: Option<(Snapshot, Arc<rtdb_index::LiveTable>)>, cancellation: &CancellationToken) -> Result<SearchRun> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Logical search table has no slug"))?;
        let directory = tempfile::tempdir().map_err(fault)?;
        let output = directory.path().join("postings.arrow");
        let file = output.clone();
        let blocks = vec![block.0];
        let identity = run_identity(&entry.slug, &index.id, index.version, &blocks, 0);
        let (budget, token, columns, version) = (Arc::clone(&self.memory), cancellation.clone(), columns.to_vec(), index.version);
        let indexing = self.memory.reserve(indexing_allowance(size))?;
        let ((locator, digest), source_rows, has_erases) = tokio::task::spawn_blocking(move || {
            let (reader, opening) = open_input(input, &budget)?;
            let mut pages = PageRows::new(reader, opening, budget, token, false);
            pages.mode = BuildMode::Postings;
            pages.seal = seal;
            let output = BufWriter::new(File::create(&file).map_err(fault)?);
            let mut source_rows = 0i64;
            let mut has_erases = false;
            let counted = pages.map(|page| {
                let page = page?;
                source_rows += page.len() as i64;
                if !has_erases {
                    for row in 0..page.len() {
                        if page.events.header(row)?.op == EventOp::Erase { has_erases = true; break; }
                    }
                }
                Ok(page)
            });
            write_group_as(&columns, [counted], &identity, Some(version), None, output, Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
            Ok::<_, rtdb_types::Anomaly>((read_locator(&file)?, source_rows, has_erases))
        }).await.map_err(fault)??;
        self.publish_run(index, &entry.slug, &output, blocks, 0, source_rows, has_erases, locator, digest, cancellation).await
    }

    async fn publish_run(&self, index: &LogicalIndex, slug: &TableSlug, output: &Path, blocks: Vec<i64>, rebuild: i64, source_rows: i64,
        has_erases: bool, locator: TreeLocator,
        digest: [u8; 32], cancellation: &CancellationToken) -> Result<SearchRun> {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let key = ObjectKey::new(rtdb_format::search_manifest::run_key(slug, digest))?;
        let publication = self.store.put_file_with_publication(&key, output).await?.map(Into::into);
        Ok(SearchRun { definition_id: Some(index.id.clone()), definition_version: Some(index.version), blocks,
            digest: Some(digest.to_vec()), locator: Some(locator.encode().into_bytes()), file_size: Some(locator.length as i64), publication,
            rebuild: (rebuild > 0).then_some(rebuild), source_rows: Some(source_rows), has_erases: has_erases.then_some(true) })
    }

    pub(super) async fn refresh_logical(&self, snapshots: &dyn SnapshotSource, publisher: &dyn ArtefactPublisher,
        storage: (StorageVersion, StorageEpoch), cancellation: &CancellationToken) -> Result<()> {
        let definitions = self.search_definitions(&snapshots.snapshot(), cancellation).await?;
        self.repair_logical(snapshots, publisher, storage, &definitions, cancellation).await?;
        let snapshot = snapshots.snapshot().snapshot();
        for table in snapshot.trie_catalog().tables() {
            for run in snapshot.trie_catalog().search_runs(table) {
                if definitions.iter().any(|definition| definition.table == *table && LogicalIndex::supports(definition)
                    && definition.artefact_id_for(LOGICAL_FORMAT_VERSION) == run.definition_id() && definition.version == run.definition_version()) {
                    continue;
                }
                let index = LogicalIndex { table: table.clone(), id: run.definition_id().to_owned(), version: run.definition_version() };
                publish_change(publisher, storage, &index, SearchRunChange {
                    run: Some(run.clone()), retire_only: Some(true), ..Default::default()
                }).await?;
            }
        }
        for definition in definitions.iter().filter(|definition| LogicalIndex::supports(definition)) {
            let index = LogicalIndex::of(definition);
            let snapshot = snapshots.snapshot().snapshot();
            let Some(last) = snapshot.table_catalog().block_index() else { continue; };
            let Some(entry) = snapshot.table_entry(&index.table) else { continue; };
            let covered = index.records(&snapshot).into_iter().flat_map(SearchRun::blocks).collect::<std::collections::BTreeSet<_>>();
            for trie in snapshot.trie_catalog().l0_source(&index.table, BlockIndex(-1), last)
                .filter(|trie| !covered.contains(&trie.key.block_index())) {
                if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
                let directory = tempfile::tempdir().map_err(fault)?;
                let key = ObjectKey::new(entry.slug.data_key(&trie.key))?;
                let input = match download(self.store.as_ref(), &self.memory, &key, &directory.path().join("source.arrow"), cancellation).await {
                    Ok(file) => file,
                    Err(error) if error.code == "rtdb.storage/missing-object" => continue,
                    Err(error) => return Err(error),
                };
                let run = match self.build_logical(&index, &definition.columns, &snapshot, trie.key.block_index(), input,
                    trie.data_file_size, None, cancellation).await {
                    Ok(run) => run,
                    Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); break; }
                    Err(error) => return Err(error),
                };
                publish_change(publisher, storage, &index, SearchRunChange { run: Some(run), ..Default::default() }).await?;
            }
            loop {
                let snapshot = snapshots.snapshot().snapshot();
                let mut runs = index.records(&snapshot);
                runs.sort_unstable_by_key(|run| (run.blocks.len(), run.blocks[0]));
                let limit = (self.memory.limit_bytes() / 8).min(128 << 20) as i64;
                let Some(pair) = runs.windows(2).find(|pair| {
                    pair[0].blocks.len() == pair[1].blocks.len()
                        && pair[0].blocks.len() * 2 <= rtdb_format::search_manifest::MAX_RUN_BLOCKS
                        && pair[0].file_size().saturating_add(pair[1].file_size()) <= limit
                }) else { break; };
                let input = pair.iter().map(|run| run.digest().to_vec()).collect::<Vec<_>>();
                let run = match self.merge_logical(&index, &definition.columns, &snapshot, pair, cancellation).await {
                    Ok(run) => run,
                    Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); break; }
                    Err(error) => return Err(error),
                };
                let identity = run.identity()?;
                publish_change(publisher, storage, &index, SearchRunChange { run: Some(run), replaces: input, ..Default::default() }).await?;
                if !index.records(&snapshots.snapshot().snapshot()).iter().any(|run| run.identity().is_ok_and(|found| found == identity)) { break; }
            }
            match self.scrub_logical(&index, &definition.columns, snapshots, publisher, storage, cancellation).await {
                Err(error) if super::is_search_gap(&error, cancellation) => self.runtime.record_serving_gap(),
                result => result?,
            }
        }
        Ok(())
    }

    async fn scrub_logical(&self, index: &LogicalIndex, columns: &[String], snapshots: &dyn SnapshotSource,
        publisher: &dyn ArtefactPublisher, storage: (StorageVersion, StorageEpoch), cancellation: &CancellationToken) -> Result<()> {
        let snapshot = snapshots.snapshot().snapshot();
        let Some(entry) = snapshot.table_entry(&index.table) else { return Ok(()); };
        let runs = index.records(&snapshot);
        if !runs.iter().any(|run| run.has_erases()) { return Ok(()); }
        let cache_key = (index.table.clone(), index.id.clone(), index.version);
        let mut digest = Sha256::new();
        for run in &runs { digest.update(run.digest()); }
        let fingerprint = digest.finalize().into();
        if self.scrubbed.lock().get(&cache_key) == Some(&fingerprint) { return Ok(()); }
        let bound = runs.iter().filter(|run| run.has_erases()).try_fold(4096usize, |bound, run| {
            bound.checked_add(usize::try_from(run.source_rows()).ok()?.checked_mul(128)?)
        }).ok_or_else(|| fault("Erase frontier exceeds addressable memory"))?;
        let _frontier = self.memory.reserve(bound)?;
        let mut erases = Erases::new();
        for run in runs.iter().filter(|run| run.has_erases()) {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            let file = self.runtime.logical_file(&self.storage, &entry.slug, run, columns).await?;
            extend_erases(&mut erases, file.erases().await?);
        }
        for run in runs {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            let indexing = self.memory.reserve(indexing_allowance(run.file_size().saturating_mul(4)))?;
            let file = self.runtime.logical_file(&self.storage, &entry.slug, run, columns).await?;
            let mut documents = file.documents().await?;
            if !scrub_documents(&mut documents, &erases) { continue; }
            let local = if run.has_erases() { file.erases().await? } else { Erases::new() };
            let rebuild = run.rebuild().checked_add(1).ok_or_else(|| fault("Search rebuild generation exhausted"))?;
            let replacement = self.store_documents(index, columns, &entry.slug, run.blocks.clone(), rebuild, run.source_rows(),
                RunContents { documents, erases: local }, indexing, cancellation).await?;
            publish_change(publisher, storage, index, SearchRunChange {
                run: Some(replacement), replaces: vec![run.digest().to_vec()], ..Default::default()
            }).await?;
        }
        let mut scrubbed = self.scrubbed.lock();
        if scrubbed.len() >= 1024 { scrubbed.clear(); }
        scrubbed.insert(cache_key, fingerprint);
        Ok(())
    }

    async fn repair_logical(&self, snapshots: &dyn SnapshotSource, publisher: &dyn ArtefactPublisher,
        storage: (StorageVersion, StorageEpoch), definitions: &[SearchDefinition], cancellation: &CancellationToken) -> Result<()> {
        for (table, id) in self.runtime.suspect_runs(&self.database) {
            let snapshot = snapshots.snapshot().snapshot();
            let target = snapshot.trie_catalog().search_runs(&table).find(|run| run.identity().is_ok_and(|digest| digest == id));
            let Some(target) = target else { self.runtime.clear_run(&self.database, &table, id); continue; };
            let Some(definition) = definitions.iter().find(|definition| definition.table == table
                && definition.version == target.definition_version() && definition.artefact_id_for(LOGICAL_FORMAT_VERSION) == target.definition_id()) else {
                self.runtime.clear_run(&self.database, &table, id);
                continue;
            };
            let index = LogicalIndex::of(definition);
            let attempt = async {
                let entry = snapshot.table_entry(&table).ok_or_else(|| fault("Logical repair has no table slug"))?;
                let checked = async {
                    self.runtime.open_logical(&self.storage, &entry.slug, target, &definition.columns, true).await?.verify().await
                }.await;
                match checked {
                    Ok(()) => return Ok(()),
                    Err(error) if !crate::runtime::FileError::from_anomaly(error.clone()).needs_repair() => return Err(error),
                    Err(_) => {}
                }
                let rebuild = target.rebuild().checked_add(1).ok_or_else(|| fault("Search rebuild generation exhausted"))?;
                let blocks = target.blocks.clone();
                let mut contents = RunContents::default();
                let mut charge = self.memory.reserve(4096)?;
                let mut source_rows = 0i64;
                for block in target.blocks() {
                    let trie = TrieKey::l0(block)?;
                    let key = ObjectKey::new(entry.slug.data_key(&trie))?;
                    let directory = tempfile::tempdir().map_err(fault)?;
                    let input = download(self.store.as_ref(), &self.memory, &key, &directory.path().join("repair.arrow"), cancellation).await?;
                    let memory = Arc::clone(&self.memory);
                    let columns = definition.columns.clone();
                    let token = cancellation.clone();
                    (contents, charge, source_rows) = tokio::task::spawn_blocking(move || {
                        let (reader, opening) = open_input(input, &memory)?;
                        let mut pages = PageRows::new(reader, opening, memory, token, false);
                        pages.mode = BuildMode::Postings;
                        for page in pages {
                            let page = page?;
                            source_rows += page.len() as i64;
                            for row in 0..page.len() {
                                let IndexedRow { header, sql_id, values, revisions, .. } = page.row(row, &columns)?;
                                if header.op == EventOp::Erase {
                                    contents.erases.entry(header.iid).and_modify(|old| *old = (*old).max(header.system_position()))
                                        .or_insert_with(|| header.system_position());
                                }
                                for (field, (tokens, revision)) in values.into_iter().zip(revisions).enumerate() {
                                    let (Some(tokens), Some(revision)) = (tokens, revision) else { continue; };
                                    let retained = tokens.get_array_memory_size().saturating_add(256);
                                    let reservation = Arc::get_mut(&mut charge).expect("Logical repair owns its scratch");
                                    let bytes = reservation.bytes().checked_add(retained).ok_or_else(|| fault("Search repair exceeds addressable memory"))?;
                                    reservation.grow_to(bytes)?;
                                    contents.documents.push(LogicalDocument { iid: header.iid, field, revision, sql_id, tokens,
                                        position: header.system_position() });
                                }
                            }
                        }
                        Ok::<_, rtdb_types::Anomaly>((contents, charge, source_rows))
                    }).await.map_err(fault)??;
                }
                if source_rows != target.source_rows() { return Err(fault("Search repair covers a different number of source events")); }
                for run in index.records(&snapshot).into_iter().filter(|run| run.has_erases() && run.digest() != target.digest()) {
                    let file = self.runtime.logical_file(&self.storage, &entry.slug, run, &definition.columns).await?;
                    scrub_documents(&mut contents.documents, &file.erases().await?);
                }
                let run = self.store_documents(&index, &definition.columns, &entry.slug, blocks, rebuild, source_rows, contents, charge, cancellation).await?;
                publish_change(publisher, storage, &index, SearchRunChange {
                    run: Some(run), replaces: vec![id.to_vec()], ..Default::default()
                }).await
            }.await;
            match attempt {
                Ok(()) => self.runtime.clear_run(&self.database, &table, id),
                Err(error) if super::is_search_gap(&error, cancellation) => self.runtime.record_serving_gap(),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn merge_logical(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, inputs: &[&SearchRun],
        cancellation: &CancellationToken) -> Result<SearchRun> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Logical merge has no table slug"))?;
        let mut blocks = Vec::new();
        let mut contents = RunContents::default();
        let bytes = inputs.iter().map(|run| run.file_size()).sum::<i64>().saturating_mul(4);
        let indexing = self.memory.reserve(indexing_allowance(bytes))?;
        for run in inputs {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            let file = self.runtime.logical_file(&self.storage, &entry.slug, run, columns).await?;
            contents.documents.extend(file.documents().await?);
            if run.has_erases() { extend_erases(&mut contents.erases, file.erases().await?); }
            blocks.extend(run.blocks.iter().copied());
        }
        let source_rows = inputs.iter().try_fold(0i64, |count, run| count.checked_add(run.source_rows()))
            .ok_or_else(|| fault("Search merge source row count overflow"))?;
        self.store_documents(index, columns, &entry.slug, blocks, 0, source_rows, contents, indexing, cancellation).await
    }

    async fn store_documents(&self, index: &LogicalIndex, columns: &[String], slug: &TableSlug, mut blocks: Vec<i64>, rebuild: i64, source_rows: i64,
        mut contents: RunContents, indexing: Arc<rtdb_columnar::MemoryReservation>, cancellation: &CancellationToken) -> Result<SearchRun> {
        blocks.sort_unstable();
        let has_erases = !contents.erases.is_empty();
        scrub_documents(&mut contents.documents, &contents.erases);
        let id = |doc: &LogicalDocument| (doc.iid, doc.field, doc.revision);
        contents.documents.sort_unstable_by_key(|doc| (id(doc), std::cmp::Reverse(doc.position)));
        for pair in contents.documents.windows(2) {
            let (a, b) = (&pair[0], &pair[1]);
            if (a.iid, a.field, a.revision) == (b.iid, b.field, b.revision)
                && (a.tokens != b.tokens || a.sql_id != b.sql_id) { return Err(fault("A logical text revision has conflicting witnesses")); }
        }
        contents.documents.dedup_by_key(|doc| id(doc));
        let mut rows: Vec<_> = contents.documents.into_iter().map(MergedRow::Document)
            .chain(contents.erases.into_iter().map(|(iid, position)| MergedRow::Erase(iid, position))).collect();
        rows.sort_unstable_by_key(|row| std::cmp::Reverse(row.position()));
        rows.sort_by_key(|row| row.iid());
        let directory = tempfile::tempdir().map_err(fault)?;
        let output = directory.path().join("postings.arrow");
        let file = output.clone();
        let (identity, columns, version) = (run_identity(slug, &index.id, index.version, &blocks, rebuild), columns.to_vec(), index.version);
        let (locator, digest) = tokio::task::spawn_blocking(move || {
            let output = BufWriter::new(File::create(&file).map_err(fault)?);
            write_group_as(&columns, [[Ok(MergedRows(rows))]], &identity, Some(version), None, output,
                Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
            read_locator(&file)
        }).await.map_err(fault)??;
        self.publish_run(index, slug, &output, blocks, rebuild, source_rows, has_erases, locator, digest, cancellation).await
    }
}

enum MergedRow { Document(LogicalDocument), Erase(Iid, (Timestamp, u32)) }

impl MergedRow {
    fn iid(&self) -> Iid { match self { Self::Document(doc) => doc.iid, Self::Erase(iid, _) => *iid } }
    fn position(&self) -> (Timestamp, u32) { match self { Self::Document(doc) => doc.position, Self::Erase(_, position) => *position } }
}

struct MergedRows(Vec<MergedRow>);

impl SearchPage for MergedRows {
    fn len(&self) -> usize { self.0.len() }
    fn row(&self, row: usize, columns: &[String]) -> Result<IndexedRow<'_>> {
        let mut values = vec![None; columns.len()];
        let mut revisions = vec![None; columns.len()];
        let row = &self.0[row];
        let (op, sql_id) = match row {
            MergedRow::Document(document) => {
                *values.get_mut(document.field).ok_or_else(|| fault("Search document references an unknown field"))? = Some(document.tokens.clone());
                revisions[document.field] = Some(document.revision);
                (EventOp::Put, document.sql_id)
            }
            MergedRow::Erase(..) => (EventOp::Erase, None),
        };
        let (system_from, commit_position) = row.position();
        Ok(IndexedRow { header: EventHeader { iid: row.iid(), system_from, commit_position,
            valid_time: Interval::ALL, op }, key: None, sql_id, values, revisions })
    }
}

fn read_locator(path: &Path) -> Result<(TreeLocator, [u8; 32])> {
    let mut file = File::open(path).map_err(fault)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let size = file.read(&mut buffer).map_err(fault)?;
        if size == 0 { break; }
        digest.update(&buffer[..size]);
    }
    Ok((TreeLocator::read(&mut file, LOGICAL_FORMAT_VERSION)?, digest.finalize().into()))
}

async fn publish_change(publisher: &dyn ArtefactPublisher, storage: (StorageVersion, StorageEpoch), index: &LogicalIndex,
    change: SearchRunChange) -> Result<()> {
    let run = change.run.as_ref().ok_or_else(|| fault("Search publication has no run"))?;
    let anchor = TrieKey::l0(BlockIndex(run.blocks[0]))?.to_string();
    let record = change.to_artefact()?;
    let response = publisher.publish(ArtefactsAdded { storage_version: Some(storage.0.0), storage_epoch: Some(storage.1.0),
        table_name: Some(index.table.schema_and_table()), entries: vec![ArtefactEntry { trie_key: Some(anchor), record: Some(record) }],
        source_msg_id: None }).await;
    match response {
        Err(error) if error.category == AnomalyCategory::Interrupted => Err(crate::accelerate::search_cancelled()),
        result => result.map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_erase_scrubs_older_text_and_keeps_later_reuse_at_the_same_system_time() {
        let iid = Iid([1; 16]);
        let document = |text: &str, position| LogicalDocument {
            iid, sql_id: None, field: 0, revision: text_revision(text), tokens: rtdb_text::analyze(text), position
        };
        let mut documents = vec![document("shared text", (Timestamp(1), 0)),
            document("old text", (Timestamp(2), 2)), document("shared text", (Timestamp(2), 4))];
        assert!(scrub_documents(&mut documents, &Erases::from([(iid, (Timestamp(2), 3))])));
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].revision, text_revision("shared text"));
        assert_eq!(documents[0].position, (Timestamp(2), 4));
    }
}
