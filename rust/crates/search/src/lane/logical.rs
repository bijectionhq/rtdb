use std::{collections::BTreeMap, fs::File, io::{BufWriter, Read}, path::Path, sync::Arc};

use arrow_array::Array;
use rtdb_format::{keys::{TableSlug, TrieKey}, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactsAdded, SearchRun, SearchRunChange}};
use rtdb_index::{PartitionValue, Snapshot};
use rtdb_maintenance::{ArtefactDefinition, ArtefactPublisher, SnapshotSource, input::{download, open_input}};
use rtdb_storage::ObjectKey;
use rtdb_types::{AnomalyCategory, BlockIndex, EventHeader, EventOp, Iid, Interval, Result, StorageEpoch, StorageVersion, TableRef, Timestamp};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{SearchDefinition, file::{BuildMode, IndexedRow, LOGICAL_FORMAT_VERSION, LogicalDocument, SearchPage, SearchPosition, search_position, TextRevision, TreeLocator, write_group_as}};
use super::{PageRows, SearchBuilder, fault, indexing_allowance};

type Erases = BTreeMap<Iid, SearchPosition>;

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

fn needs_order_replay(run: &SearchRun) -> bool { run.allocation.is_none() && run.has_erases() }

pub(crate) fn text_revision(value: &str) -> TextRevision {
    revision_of(&rtdb_text::analyze(value))
}

pub(super) fn revision_of(tokens: &arrow_array::StringArray) -> TextRevision {
    let mut digest = Sha256::new();
    digest.update(b"rtdb/search/token-revision");
    digest.update(rtdb_text::ANALYZER_VERSION.to_le_bytes());
    for term in tokens.iter().flatten() {
        digest.update((term.len() as u64).to_le_bytes());
        digest.update(term.as_bytes());
    }
    digest.finalize().into()
}

pub(crate) fn history_id(definition: &SearchDefinition) -> String {
    format!("{}h", definition.artefact_id_for(LOGICAL_FORMAT_VERSION))
}

pub(crate) fn run_identity(slug: &TableSlug, definition: &str, version: i64, blocks: &[i64], rebuild: i64, allocation: &[u8]) -> String {
    let base = format!("logical-search/{slug}/{definition}/{version}/{blocks:?}/{rebuild}", slug = slug.as_str());
    if allocation.is_empty() { base } else { format!("{base}/{allocation:02x?}") }
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
        Self { table: definition.table.clone(), id: definition.id.trim_end_matches('w').to_owned(), version: definition.version }
    }

    fn records<'a>(&self, snapshot: &'a Snapshot) -> Vec<&'a SearchRun> {
        snapshot.trie_catalog().search_runs(&self.table)
            .filter(|run| run.definition_id() == self.id && run.definition_version() == self.version).collect()
    }
}

pub(super) fn posting_position(header: EventHeader, origin: Option<rtdb_types::EventOrigin>, page_row: usize) -> Result<u32> {
    if header.commit_position != 0 { return Ok(header.commit_position); }
    match origin {
        Some(origin) => u32::try_from(origin.ordinal()).map_err(fault),
        None => Ok(u32::MAX - u32::try_from(page_row).map_err(fault)?),
    }
}

fn record_source(batch: &rtdb_format::data::EventBatch, columns: &[String], include_documents: bool,
    contents: &mut RunContents, charge: &mut Arc<rtdb_types::MemoryReservation>, cancellation: &CancellationToken) -> Result<bool> {
    let mut has_erases = false;
    for row in 0..batch.len() {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut header = batch.header(row)?;
        if header.op == EventOp::Redacted {
            return Err(fault("Search repair requires assertion-level redaction coverage"));
        }
        if header.position.is_none() {
            header.commit_position = posting_position(header, batch.origin(row)?, row)?;
        }
        if header.op == EventOp::Erase {
            let reservation = Arc::get_mut(charge).expect("Search repair owns its scratch");
            reservation.grow_to(reservation.bytes().saturating_add(128))?;
            contents.erases.entry(header.iid).and_modify(|old| *old = (*old).max(search_position(header))).or_insert(search_position(header));
            has_erases = true;
        }
        if include_documents && header.op == EventOp::Put {
            let event = batch.borrow_event(row)?;
            let sql_id = super::integer_sql_id(&event);
            for (field, column) in columns.iter().enumerate() {
                if let rtdb_format::data::PutText::Utf8(text) = event.put_text(column)? {
                    let reservation = Arc::get_mut(charge).expect("Search repair owns its scratch");
                    let _scratch = reservation.budget().reserve(256usize.saturating_add(text.len().saturating_mul(32)))?;
                    let tokens = rtdb_text::analyze(text);
                    let retained = tokens.get_array_memory_size().saturating_add(256);
                    reservation.grow_to(reservation.bytes().saturating_add(retained))?;
                    contents.documents.push(LogicalDocument { iid: header.iid, field, revision: revision_of(&tokens),
                        sql_id, tokens, position: search_position(header) });
                }
            }
        }
    }
    Ok(has_erases)
}

impl SearchBuilder {
    pub(super) async fn seal_logical(&self, index: &LogicalIndex, snapshot: &Arc<PartitionValue>, trie: &TrieKey, path: &Path, size: u64,
        cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let view = snapshot.snapshot();
        let seal = view.iter_segments(&index.table).next().cloned().map(|segment| (view.clone(), segment));
        let history_id = format!("{}w", index.id);
        let columns = self.columns.lock().get(&(index.table.clone(), history_id, index.version)).cloned()
            .ok_or_else(|| fault("Logical search definition has no fields"))?;
        let run = self.build_logical(index, &columns, &view, trie.block_index(), File::open(path).map_err(fault)?,
            size.min(i64::MAX as u64) as i64, seal, cancellation).await?;
        SearchRunChange { run: Some(run), ..Default::default() }.to_artefact()
    }

    async fn recover_from_journal(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot,
        blocks: &[i64], expected: Option<i64>, cancellation: &CancellationToken)
        -> Result<Option<(RunContents, Arc<rtdb_types::MemoryReservation>, i64)>> {
        let Some(source) = &self.retained else { return Ok(None); };
        let Some(boundary) = snapshot.table_catalog().block() else { return Ok(None); };
        let mut all_erases = RunContents::default();
        let mut contents = RunContents::default();
        let mut charge = self.memory.reserve(4096)?;
        let mut total = 0i64;
        let mut selected = 0i64;
        let mut observed = std::collections::BTreeSet::new();
        let complete = source.replay_table(boundary, &index.table, &mut |block, batch| {
            let rows = i64::try_from(batch.len()).map_err(fault)?;
            total = total.checked_add(rows).ok_or_else(|| fault("Search replay row count overflow"))?;
            if blocks.binary_search(&block.0).is_ok() {
                observed.insert(block.0);
                selected = selected.checked_add(rows).ok_or_else(|| fault("Search extent row count overflow"))?;
                record_source(batch, columns, true, &mut contents, &mut charge, cancellation)?;
            } else {
                record_source(batch, columns, false, &mut all_erases, &mut charge, cancellation)?;
            }
            Ok(())
        }, cancellation).await?;
        let durable = snapshot.table_catalog().metadata(&index.table).map_or(0, |meta| meta.row_count);
        if !complete || total != durable || !observed.into_iter().eq(blocks.iter().copied())
            || expected.is_some_and(|expected| expected != selected) { return Ok(None); }
        scrub_documents(&mut contents.documents, &contents.erases);
        scrub_documents(&mut contents.documents, &all_erases.erases);
        Ok(Some((contents, charge, selected)))
    }

    async fn build_logical(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, block: BlockIndex, input: File,
        size: i64, seal: Option<(Snapshot, Arc<rtdb_index::LiveTable>)>, cancellation: &CancellationToken) -> Result<SearchRun> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Logical search table has no slug"))?;
        let directory = tempfile::tempdir().map_err(fault)?;
        let output = directory.path().join("postings.arrow");
        let file = output.clone();
        let blocks = vec![block.0];
        let allocation = *uuid::Uuid::new_v4().as_bytes();
        let identity = run_identity(&entry.slug, &index.id, index.version, &blocks, 0, &allocation);
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
        self.publish_run(index, &entry.slug, &output, blocks, 0, source_rows, has_erases, locator, digest, allocation, cancellation).await
    }

    async fn publish_run(&self, index: &LogicalIndex, slug: &TableSlug, output: &Path, blocks: Vec<i64>, rebuild: i64, source_rows: i64,
        has_erases: bool, locator: TreeLocator,
        digest: [u8; 32], allocation: [u8; 16], cancellation: &CancellationToken) -> Result<SearchRun> {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut run = SearchRun { definition_id: Some(index.id.clone()), definition_version: Some(index.version), blocks,
            digest: Some(digest.to_vec()), locator: Some(locator.encode().into_bytes()), file_size: Some(locator.length as i64),
            publication: None, rebuild: (rebuild > 0).then_some(rebuild), source_rows: Some(source_rows),
            has_erases: has_erases.then_some(true), allocation: Some(allocation.to_vec()) };
        let intent = ObjectKey::new(run.pending_key(slug)?)?;
        self.store.put(&intent, run.pending_bytes()?.into()).await?;
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let key = ObjectKey::new(run.object_key(slug)?)?;
        run.publication = self.store.put_file_with_publication(&key, output).await?.map(Into::into);
        Ok(run)
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
                    Err(error) if error.code == "rtdb.storage/missing-object" => {
                        let blocks = vec![trie.key.block_index().0];
                        let recovered = self.recover_from_journal(&index, &definition.columns, &snapshot, &blocks, None, cancellation).await;
                        let (contents, charge, rows) = match recovered {
                            Ok(Some(recovered)) => recovered,
                            Ok(None) => continue,
                            Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); continue; }
                            Err(error) => return Err(error),
                        };
                        let run = self.store_documents(&index, &definition.columns, &entry.slug, blocks, 0, rows, contents, charge, cancellation).await?;
                        publish_change(publisher, storage, &index, SearchRunChange { run: Some(run), ..Default::default() }).await?;
                        continue;
                    }
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
                        && pair.iter().all(|run| !needs_order_replay(run))
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
        let mut frontier_charge = self.memory.reserve(4096)?;
        let mut erases = Erases::new();
        for run in runs.iter().filter(|run| run.has_erases()) {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            let file = self.runtime.logical_file(&self.storage, &entry.slug, run, columns).await?;
            let (witnesses, _charge) = file.erases(&self.memory).await?;
            let frontier = Arc::get_mut(&mut frontier_charge).expect("unpublished erase frontier owns its charge");
            let need = 4096usize.saturating_add(erases.len().saturating_add(witnesses.len()).saturating_mul(128));
            if need > frontier.bytes() { frontier.grow_to(need)?; }
            extend_erases(&mut erases, witnesses);
        }
        for run in runs.into_iter().filter(|run| !needs_order_replay(run)) {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            let indexing = self.memory.reserve(indexing_allowance(run.file_size().saturating_mul(4)))?;
            let file = self.runtime.logical_file(&self.storage, &entry.slug, run, columns).await?;
            let mut documents = file.documents().await?;
            if !scrub_documents(&mut documents, &erases) { continue; }
            let (local, _erase_charge) = if run.has_erases() { file.erases(&self.memory).await? }
                else { (Erases::new(), self.memory.reserve(0)?) };
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

    async fn read_l0_contents(&self, slug: &TableSlug, blocks: &[i64], columns: &[String], documents: bool,
        cancellation: &CancellationToken) -> Result<(RunContents, Arc<rtdb_types::MemoryReservation>, i64)> {
        let mut contents = RunContents::default();
        let mut charge = self.memory.reserve(4096)?;
        let mut count = 0i64;
        for &block in blocks {
            let trie = TrieKey::l0(BlockIndex(block))?;
            let key = ObjectKey::new(slug.data_key(&trie))?;
            let directory = tempfile::tempdir().map_err(fault)?;
            let input = download(self.store.as_ref(), &self.memory, &key, &directory.path().join("repair.arrow"), cancellation).await?;
            let memory = Arc::clone(&self.memory);
            let columns = columns.to_vec();
            let token = cancellation.clone();
            (contents, charge, count) = tokio::task::spawn_blocking(move || {
                let (mut reader, _opening) = open_input(input, &memory)?;
                for page in 0..reader.page_count() {
                    let batch = rtdb_maintenance::input::read_page(&mut reader, page, &memory)?;
                    let batch = rtdb_format::data::EventBatch::new(batch.batch().clone())?;
                    count = count.checked_add(batch.len() as i64).ok_or_else(|| fault("Search repair row count overflow"))?;
                    record_source(&batch, &columns, documents, &mut contents, &mut charge, &token)?;
                }
                Ok::<_, rtdb_types::Anomaly>((contents, charge, count))
            }).await.map_err(fault)??;
        }
        Ok((contents, charge, count))
    }

    async fn repair_logical(&self, snapshots: &dyn SnapshotSource, publisher: &dyn ArtefactPublisher,
        storage: (StorageVersion, StorageEpoch), definitions: &[SearchDefinition], cancellation: &CancellationToken) -> Result<()> {
        let captured = snapshots.snapshot().snapshot();
        let mut repairs: std::collections::BTreeSet<_> = self.runtime.suspect_runs(&self.database).into_iter().collect();
        for table in captured.trie_catalog().tables() {
            for run in captured.trie_catalog().search_runs(table).filter(|run| needs_order_replay(run)) {
                repairs.insert((table.clone(), run.identity()?));
            }
        }
        drop(captured);
        for (table, id) in repairs {
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
                if !needs_order_replay(target) {
                    let checked = async {
                        self.runtime.open_logical(&self.storage, &entry.slug, target, &definition.columns, true).await?.verify().await
                    }.await;
                    match checked {
                        Ok(()) => return Ok(()),
                        Err(error) if !crate::runtime::FileError::from_anomaly(error.clone()).needs_repair() => return Err(error),
                        Err(_) => {}
                    }
                }
                let rebuild = target.rebuild().checked_add(1).ok_or_else(|| fault("Search rebuild generation exhausted"))?;
                let blocks = target.blocks.clone();
                let l0 = self.read_l0_contents(&entry.slug, &blocks, &definition.columns, true, cancellation).await;
                let (mut contents, charge, source_rows) = match l0 {
                    Ok((mut contents, charge, source_rows)) if source_rows == target.source_rows() => {
                        let frontier = async {
                            for run in index.records(&snapshot).into_iter().filter(|run| run.has_erases() && run.digest() != target.digest()) {
                                let erases = async {
                                    let file = self.runtime.logical_file(&self.storage, &entry.slug, run, &definition.columns).await?;
                                    file.erases(&self.memory).await
                                }.await;
                                match erases {
                                    Ok((erases, _charge)) => { scrub_documents(&mut contents.documents, &erases); }
                                    Err(error) if super::is_search_gap(&error, cancellation) => {
                                        let (restored, _charge, rows) = self.read_l0_contents(&entry.slug, &run.blocks, &[], false, cancellation).await?;
                                        if rows != run.source_rows() { return Err(fault("Search erase replay covers a different number of source events")); }
                                        scrub_documents(&mut contents.documents, &restored.erases);
                                    }
                                    Err(error) => return Err(error),
                                }
                            }
                            Ok(())
                        }.await;
                        match frontier {
                            Ok(()) => (contents, charge, source_rows),
                            Err(error) if super::is_search_gap(&error, cancellation) => {
                                drop((contents, charge));
                                self.recover_from_journal(&index, &definition.columns, &snapshot, &blocks, Some(target.source_rows()), cancellation)
                                    .await?.ok_or(error)?
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Ok(_) => return Err(fault("Search repair covers a different number of source events")),
                    Err(error) if super::is_search_gap(&error, cancellation) => {
                        self.recover_from_journal(&index, &definition.columns, &snapshot, &blocks, Some(target.source_rows()), cancellation)
                            .await?.ok_or(error)?
                    }
                    Err(error) => return Err(error),
                };
                scrub_documents(&mut contents.documents, &contents.erases);
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
            if run.has_erases() {
                let (erases, _charge) = file.erases(&self.memory).await?;
                extend_erases(&mut contents.erases, erases);
            }
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
        let allocation = *uuid::Uuid::new_v4().as_bytes();
        let (identity, columns, version) = (run_identity(slug, &index.id, index.version, &blocks, rebuild, &allocation), columns.to_vec(), index.version);
        let (locator, digest) = tokio::task::spawn_blocking(move || {
            let output = BufWriter::new(File::create(&file).map_err(fault)?);
            write_group_as(&columns, [[Ok(MergedRows(rows))]], &identity, Some(version), None, output,
                Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
            read_locator(&file)
        }).await.map_err(fault)??;
        self.publish_run(index, slug, &output, blocks, rebuild, source_rows, has_erases, locator, digest, allocation, cancellation).await
    }
}

enum MergedRow { Document(LogicalDocument), Erase(Iid, SearchPosition) }

impl MergedRow {
    fn iid(&self) -> Iid { match self { Self::Document(doc) => doc.iid, Self::Erase(iid, _) => *iid } }
    fn position(&self) -> SearchPosition { match self { Self::Document(doc) => doc.position, Self::Erase(_, position) => *position } }
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
        let (system_from, position, commit_position) = row.position();
        Ok(IndexedRow { header: EventHeader { position, iid: row.iid(), system_from, commit_position,
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
#[test]
fn an_intact_legacy_erase_run_requires_reconstruction() {
    let mut run = SearchRun { has_erases: Some(true), ..Default::default() };
    assert!(needs_order_replay(&run));
    run.allocation = Some(vec![42; 16]);
    assert!(!needs_order_replay(&run));
    run.allocation = None;
    run.has_erases = None;
    assert!(!needs_order_replay(&run));
}

#[cfg(test)]
#[test]
fn normalized_revisions_preserve_order_and_frequency() {
    assert_eq!(text_revision("Quarterly Audit—REPORT"), text_revision("quarterly audit report"));
    assert_ne!(text_revision("report audit"), text_revision("audit report"));
    assert_ne!(text_revision("audit report"), text_revision("audit audit report"));
    assert_ne!(text_revision("a bc"), text_revision("ab c"));
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
        let mut documents = vec![document("shared text", (Timestamp(1), None, 0)),
            document("old text", (Timestamp(2), None, 2)), document("shared text", (Timestamp(2), None, 4))];
        assert!(scrub_documents(&mut documents, &Erases::from([(iid, (Timestamp(2), None, 3))])));
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].revision, text_revision("shared text"));
        assert_eq!(documents[0].position, (Timestamp(2), None, 4));
    }
}
