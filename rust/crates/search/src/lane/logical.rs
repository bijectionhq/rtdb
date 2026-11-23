use std::{collections::BTreeMap, fs::File, io::{BufWriter, Read}, path::Path, sync::Arc};

use arrow_array::Array;
use rtdb_format::cow_tree::page_key;
use rtdb_format::term_directory;
use rtdb_format::log::proto::{SourceReceipt, TermDirectoryRoot};
use rtdb_query::ObjectPageStore;
use rtdb_format::{keys::{TableSlug, TrieKey}, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactsAdded, SearchRun, SearchRunChange}};
use rtdb_index::{PartitionValue, Snapshot};
use rtdb_maintenance::{ArtefactDefinition, ArtefactPublisher, HistoryCoverage, HistoryRead, HistoryRow, SnapshotSource, input::{download, open_input}};
use rtdb_storage::{HeldView, IoRegion, ObjectKey};
use rtdb_types::{AnomalyCategory, BlockIndex, EventHeader, EventOp, Iid, Interval, Result, StorageEpoch, StorageVersion, TableRef, Timestamp};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{SearchDefinition, file::{BuildMode, IndexedRow, LOGICAL_FORMAT_VERSION, LogicalDocument, SearchPage, SearchPosition, search_position, TextRevision, TreeLocator, write_group_as}};
use super::{PageRows, SearchBuilder, fault, indexing_allowance};
use crate::term_directory::{TermEdits, TermSummaries};

type Erases = BTreeMap<Iid, SearchPosition>;

struct PreparedLogicalRun {
    run: SearchRun,
    summaries: TermSummaries,
}

enum DirectoryUpdate<'a> {
    Adopt { run: &'a SearchRun, summaries: &'a TermSummaries, replaces: &'a [Vec<u8>] },
    Rebuild,
}

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

    fn term_directory<'a>(&self, snapshot: &'a Snapshot) -> Option<&'a TermDirectoryRoot> {
        snapshot.trie_catalog().term_directory(&self.table, &self.id, self.version)
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
    contents: &mut RunContents, charge: &mut Arc<rtdb_types::MemoryReservation>, cancellation: &CancellationToken) -> Result<()> {
    for row in 0..batch.len() {
        record_history_row(HistoryRow::Stored { batch, row }, columns, include_documents, contents, charge, cancellation)?;
    }
    Ok(())
}

fn record_history_row(source: HistoryRow<'_>, columns: &[String], include_documents: bool, contents: &mut RunContents,
    charge: &mut Arc<rtdb_types::MemoryReservation>, cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
    let (mut header, origin, row) = match source {
        HistoryRow::Stored { batch, row } => (batch.header(row)?, batch.origin(row)?, row),
        HistoryRow::Header { header, origin } => (header, Some(origin), 0),
    };
    if header.op == EventOp::Redacted {
        return Err(fault("Search repair requires assertion-level redaction coverage"));
    }
    if header.position.is_none() {
        header.commit_position = posting_position(header, origin, row)?;
    }
    if header.op == EventOp::Erase {
        let reservation = Arc::get_mut(charge).expect("Search repair owns its scratch");
        reservation.grow_to(reservation.bytes().saturating_add(128))?;
        contents.erases.entry(header.iid).and_modify(|old| *old = (*old).max(search_position(header))).or_insert(search_position(header));
    } else if include_documents && header.op == EventOp::Put {
        let HistoryRow::Stored { batch, row } = source else { return Err(fault("A retained search event has no verified payload")); };
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
    Ok(())
}

struct RecoveredRun {
    contents: RunContents,
    charge: Arc<rtdb_types::MemoryReservation>,
    receipt: SourceReceipt,
}

impl SearchBuilder {
    async fn hold_logical_input(&self, snapshot: &Snapshot) -> Result<Arc<HeldView>> {
        let cut = snapshot.catalog_cut();
        let paths = cut.and_then(|_| snapshot.catalog_file_paths());
        HeldView::catalog(self.store.as_ref(), Arc::new(snapshot.clone()), cut, paths).await
    }

    pub(super) async fn seal_logical(&self, index: &LogicalIndex, snapshot: &Arc<PartitionValue>, trie: &TrieKey, path: &Path, size: u64,
        cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        let view = snapshot.snapshot();
        let seal = view.iter_segments(&index.table).next().cloned().map(|segment| (view.clone(), segment));
        let history_id = format!("{}w", index.id);
        let columns = self.columns.lock().get(&(index.table.clone(), history_id, index.version)).cloned()
            .ok_or_else(|| fault("Logical search definition has no fields"))?;
        let run = self.build_logical(index, &columns, &view, trie.block_index(), File::open(path).map_err(fault)?,
            size.min(i64::MAX as u64) as i64, seal, cancellation).await?;
        SearchRunChange { run: Some(run.run), ..Default::default() }.to_artefact()
    }

    async fn recover_from_history(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot,
        blocks: &[i64], expected: Option<&SearchRun>, cancellation: &CancellationToken) -> Result<Option<RecoveredRun>> {
        let Some(source) = &self.history else { return Ok(None); };
        let Some(boundary) = snapshot.table_catalog().block() else { return Ok(None); };
        let Some(through) = boundary.boundary_replica_position() else { return Ok(None); };
        let mut outside = RunContents::default();
        let mut contents = RunContents::default();
        let mut charge = self.memory.reserve(4096)?;
        let mut total = 0i64;
        let mut selected = 0i64;
        let mut observed = std::collections::BTreeSet::new();
        let read = HistoryRead { snapshot, storage: &self.storage, memory: &self.memory, cancellation, table: &index.table, blocks };
        let Some(coverage) = source.visit_table(read, &mut |block, row| {
            if block.0 < 0 || block > boundary.block_index() { return Err(fault("Search history has an invalid source block")); }
            total = total.checked_add(1).ok_or_else(|| fault("Search replay row count overflow"))?;
            if blocks.binary_search(&block.0).is_ok() {
                observed.insert(block.0);
                selected = selected.checked_add(1).ok_or_else(|| fault("Search extent row count overflow"))?;
                record_history_row(row, columns, true, &mut contents, &mut charge, cancellation)?;
            } else {
                record_history_row(row, columns, false, &mut outside, &mut charge, cancellation)?;
            }
            Ok(())
        }).await? else { return Ok(None); };
        let durable = snapshot.table_catalog().metadata(&index.table).map_or(0, |meta| meta.row_count);
        let exact = total == durable && observed.iter().copied().eq(blocks.iter().copied());
        let mut frontier = outside.erases;
        extend_erases(&mut frontier, contents.erases.clone());
        let (lineage, checkpoint, checkpoint_catalog_sha256, directory_root, erase_frontier_digest, source_rows) = match coverage {
            HistoryCoverage::Native { lineage, through: end } if end == through && exact
                && expected.is_none_or(|run| run.source_rows() == selected && run.has_erases() == !contents.erases.is_empty()) =>
            {
                let mut digest = Sha256::new();
                for (iid, (time, origin_position, position)) in &frontier {
                    digest.update(iid.0);
                    digest.update(time.0.to_be_bytes());
                    digest.update([u8::from(origin_position.is_some())]);
                    if let Some(origin_position) = origin_position { digest.update(origin_position.to_bytes()); }
                    digest.update(position.to_be_bytes());
                }
                (lineage, -1, None, None, digest.finalize().to_vec(), selected)
            }
            HistoryCoverage::Semantic { lineage, checkpoint_boundary, checkpoint_catalog_sha256, through: end, directory, suffix_rows }
                if end == through && checkpoint_boundary <= end && directory.through_block() <= boundary.block_index().0
                    && directory.source_rows() == durable && suffix_rows <= durable && total <= durable
                    && directory.erase_frontier_digest().len() == 32 =>
            {
                let Some(current) = snapshot.trie_catalog().entity_directory(&index.table) else { return Ok(None); };
                if current != &directory || directory.root().is_empty() { return Ok(None); }
                let source_rows = match expected {
                    Some(run) if exact && run.source_rows() == selected => selected,
                    Some(run) if directory.has_erases() && total < durable && run.source_rows() >= selected
                        && run.source_rows() - selected <= durable - total => run.source_rows(),
                    None if exact => selected,
                    _ => return Ok(None),
                };
                if let Some(run) = expected {
                    if run.has_erases() && contents.erases.is_empty() {
                        if frontier.is_empty() { return Ok(None); }
                        contents.erases.clone_from(&frontier);
                    }
                    if !run.has_erases() && !contents.erases.is_empty() { return Ok(None); }
                }
                (lineage, checkpoint_boundary.0, Some(checkpoint_catalog_sha256.to_vec()), directory.root,
                    directory.erase_frontier_digest.unwrap_or_default(), source_rows)
            }
            _ => return Ok(None),
        };
        if source_rows <= 0 { return Ok(None); }
        scrub_documents(&mut contents.documents, &frontier);
        let receipt = SourceReceipt {
            lineage: Some(lineage), checkpoint_boundary: Some(checkpoint), suffix_through: Some(through.0),
            table: Some(index.table.schema_and_table()), shard_range: Some(rtdb_format::search_manifest::FULL_SHARD_RANGE.to_vec()),
            source_rows: Some(source_rows), erase_frontier_digest: Some(erase_frontier_digest),
            directory_root, blocks: blocks.to_vec(), checkpoint_catalog_sha256,
        };
        Ok(Some(RecoveredRun { contents, charge, receipt }))
    }

    async fn build_logical(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, block: BlockIndex, input: File,
        size: i64, seal: Option<(Snapshot, Arc<rtdb_index::LiveTable>)>, cancellation: &CancellationToken) -> Result<PreparedLogicalRun> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Logical search table has no slug"))?;
        let directory = tempfile::tempdir().map_err(fault)?;
        let output = directory.path().join("postings.arrow");
        let file = output.clone();
        let blocks = vec![block.0];
        let allocation = *uuid::Uuid::new_v4().as_bytes();
        let identity = run_identity(&entry.slug, &index.id, index.version, &blocks, 0, &allocation);
        let (budget, token, columns, version) = (Arc::clone(&self.memory), cancellation.clone(), columns.to_vec(), index.version);
        let indexing = self.memory.reserve(indexing_allowance(size))?;
        let ((locator, digest), source_rows, has_erases, summaries) = tokio::task::spawn_blocking(move || {
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
            let (_, summaries) = write_group_as(&columns, [counted], &identity, Some(version), None, output, Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
            Ok::<_, rtdb_types::Anomaly>((read_locator(&file)?, source_rows, has_erases, summaries))
        }).await.map_err(fault)??;
        let run = self.publish_run(index, &entry.slug, &output, blocks, 0, source_rows, has_erases, locator, digest, allocation, None, cancellation).await?;
        Ok(PreparedLogicalRun { run, summaries })
    }

    async fn publish_run(&self, index: &LogicalIndex, slug: &TableSlug, output: &Path, blocks: Vec<i64>, rebuild: i64, source_rows: i64,
        has_erases: bool, locator: TreeLocator,
        digest: [u8; 32], allocation: [u8; 16], source_receipt: Option<SourceReceipt>, cancellation: &CancellationToken) -> Result<SearchRun> {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut run = SearchRun { definition_id: Some(index.id.clone()), definition_version: Some(index.version), blocks,
            digest: Some(digest.to_vec()), locator: Some(locator.encode().into_bytes()), file_size: Some(locator.length as i64),
            publication: None, rebuild: (rebuild > 0).then_some(rebuild), source_rows: Some(source_rows),
            has_erases: has_erases.then_some(true), allocation: Some(allocation.to_vec()), source_receipt };
        let intent = ObjectKey::new(run.pending_key(slug)?)?;
        let staged = rtdb_format::search_manifest::StagedSearchRun { run: run.clone(), owner: self.store.search_staging_owner().await? };
        self.store.put(&intent, staged.encode()?.into()).await?;
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let key = ObjectKey::new(run.object_key(slug)?)?;
        // A provider can commit a failed request; leave its intent for the collector to settle after the writer drains.
        run.publication = self.store.put_file_with_publication(&key, output).await?.map(Into::into);
        if self.store.releases_exact_search_runs() && run.publication.is_none() {
            run.publication = self.store.retirement_publication(&key).await?.map(Into::into);
            if run.publication.is_none() { return Err(fault("Search upload returned no immutable provider publication")); }
        }
        Ok(run)
    }

    async fn directory_change(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, prepared: PreparedLogicalRun,
        replaces: Vec<Vec<u8>>, cancellation: &CancellationToken) -> Result<SearchRunChange> {
        let PreparedLogicalRun { run, summaries } = prepared;
        let update = self.prepare_term_directory(index, columns, snapshot,
            DirectoryUpdate::Adopt { run: &run, summaries: &summaries, replaces: &replaces }, cancellation).await;
        let mut change = SearchRunChange { run: Some(run), replaces, ..Default::default() };
        match update {
            Ok((root, basis)) => {
                change.term_directory = Some(root);
                change.replaces_directory = Some(basis.to_vec());
            }
            Err(error) if super::is_search_gap(&error, cancellation) => {
                self.runtime.report_term_directory(&self.database, &index.table, &index.id, index.version);
            }
            Err(error) => return Err(error),
        }
        Ok(change)
    }

    async fn prepare_term_directory(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot,
        update: DirectoryUpdate<'_>, cancellation: &CancellationToken)
        -> Result<(TermDirectoryRoot, [u8; 32])> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Term directory has no table slug"))?;
        let pages = ObjectPageStore { store: Arc::clone(&self.store), storage: Arc::clone(&self.storage), prefix: term_directory::page_prefix(&entry.slug) };
        let before = index.records(snapshot);
        let current = index.term_directory(snapshot);
        let basis = term_directory::publication_basis(current, before.iter().map(|run| run.identity()).collect::<Result<Vec<_>>>()?);
        let (added, replaces) = match update {
            DirectoryUpdate::Adopt { run, summaries, replaces } => (Some((run, summaries)), replaces),
            DirectoryUpdate::Rebuild => (None, &[][..]),
        };
        let is_replaced = |run: &SearchRun| replaces.iter().any(|id| id.as_slice() == run.digest());
        let removed = before.iter().filter(|run| is_replaced(run)).count();
        if removed != replaces.len() { return Err(fault("Term directory replacement inputs are no longer active")); }
        let runs = before.len() - removed + usize::from(added.is_some());
        let record_added = |edits: &mut TermEdits| -> Result<()> {
            if let Some((run, summaries)) = added {
                let id = run.identity()?;
                summaries.visit(|field, term, stats| edits.record(field, term, id, Some(stats)))?;
            }
            Ok(())
        };
        let updated = if let Some(current) = current.filter(|_| added.is_some()) {
            let incremental = async {
                let base = current.page_root()?;
                let mut edits = TermEdits::new(&self.memory)?;
                record_added(&mut edits)?;
                for run in before.iter().copied().filter(|run| is_replaced(run)) {
                    self.collect_term_edits(&entry.slug, columns, run, &mut edits, true, cancellation).await?;
                }
                edits.apply(&pages, base.as_ref(), &self.memory).await
            };
            match incremental.await {
                Ok((root, _)) => Some(root),
                Err(error) if super::is_search_gap(&error, cancellation) => None,
                Err(error) => return Err(error),
            }
        } else { None };
        let root = match updated {
            Some(root) => root,
            None => {
                let mut edits = TermEdits::new(&self.memory)?;
                record_added(&mut edits)?;
                for run in before.iter().copied().filter(|run| !is_replaced(run)) {
                    self.collect_term_edits(&entry.slug, columns, run, &mut edits, false, cancellation).await?;
                }
                let (root, written) = edits.apply(&pages, None, &self.memory).await?;
                if current.is_some() {
                    for digest in written {
                        self.storage.invalidate(&ObjectKey::new(page_key(&pages.prefix, digest))?).await?;
                    }
                }
                root
            }
        };
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        Ok((TermDirectoryRoot { definition_id: Some(index.id.clone()), definition_version: Some(index.version),
            root: Some(root.as_ref().map_or_else(Vec::new, |root| root.encode())), runs: Some(runs as i64) }, basis))
    }

    async fn collect_term_edits(&self, slug: &TableSlug, columns: &[String], run: &SearchRun, edits: &mut TermEdits,
        deleting: bool, cancellation: &CancellationToken) -> Result<()> {
        let id = run.identity()?;
        let file = self.runtime.logical_file(&self.storage, slug, run, columns).await?;
        file.visit_term_summaries(|field, term, stats| {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            edits.record(field, term, id, (!deleting).then_some(stats))
        }).await
    }

    async fn repair_term_directory(&self, index: &LogicalIndex, columns: &[String], snapshots: &dyn SnapshotSource, publisher: &dyn ArtefactPublisher,
        storage: (StorageVersion, StorageEpoch), cancellation: &CancellationToken) -> Result<()> {
        let snapshot = snapshots.snapshot().snapshot();
        let Some(run) = index.records(&snapshot).into_iter().next() else { return Ok(()); };
        if index.term_directory(&snapshot).is_some() && !self.runtime.term_directory_suspect(&self.database, &index.table, &index.id, index.version) {
            return Ok(());
        }
        let hold = self.hold_logical_input(&snapshot).await?;
        let (root, basis) = IoRegion::reading(hold, self.prepare_term_directory(index, columns, &snapshot, DirectoryUpdate::Rebuild, cancellation)).await?;
        let change = SearchRunChange { run: Some(run.clone()), term_directory: Some(root.clone()), replaces_directory: Some(basis.to_vec()),
            ..Default::default() };
        publish_change(publisher, storage, index, change).await?;
        if index.term_directory(&snapshots.snapshot().snapshot()) == Some(&root) {
            self.runtime.clear_term_directory(&self.database, &index.table, &index.id, index.version);
        }
        Ok(())
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
                        let recovered = self.recover_from_history(&index, &definition.columns, &snapshot, &blocks, None, cancellation).await;
                        let RecoveredRun { contents, charge, receipt } = match recovered {
                            Ok(Some(recovered)) => recovered,
                            Ok(None) => continue,
                            Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); continue; }
                            Err(error) => return Err(error),
                        };
                        let prepared = self.store_documents(&index, &definition.columns, &entry.slug, blocks, 0, receipt.source_rows(), contents, charge, Some(receipt), cancellation).await?;
                        let current = snapshots.snapshot().snapshot();
                        let change = self.directory_change(&index, &definition.columns, &current, prepared, Vec::new(), cancellation).await?;
                        publish_change(publisher, storage, &index, change).await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let prepared = match self.build_logical(&index, &definition.columns, &snapshot, trie.key.block_index(), input,
                    trie.data_file_size, None, cancellation).await {
                    Ok(run) => run,
                    Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); break; }
                    Err(error) => return Err(error),
                };
                let current = snapshots.snapshot().snapshot();
                let change = self.directory_change(&index, &definition.columns, &current, prepared, Vec::new(), cancellation).await?;
                publish_change(publisher, storage, &index, change).await?;
            }
            loop {
                let snapshot = snapshots.snapshot().snapshot();
                let mut runs = index.records(&snapshot);
                runs.sort_unstable_by_key(|run| (run.blocks.len(), run.blocks[0]));
                let limit = (self.memory.limit_bytes() / 8).min(128 << 20) as i64;
                let Some(pair) = runs.windows(2).find(|pair| {
                    pair[0].blocks.len() == pair[1].blocks.len()
                        && pair.iter().all(|run| !needs_order_replay(run))
                        && same_history_cut(pair[0], pair[1])
                        && pair[0].blocks.len() * 2 <= rtdb_format::search_manifest::MAX_RUN_BLOCKS
                        && pair[0].file_size().saturating_add(pair[1].file_size()) <= limit
                }) else { break; };
                let input = pair.iter().map(|run| run.digest().to_vec()).collect::<Vec<_>>();
                let hold = self.hold_logical_input(&snapshot).await?;
                let merged = IoRegion::reading(hold,
                    self.merge_logical(&index, &definition.columns, &snapshot, pair, cancellation),
                ).await;
                let prepared = match merged {
                    Ok(run) => run,
                    Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); break; }
                    Err(error) => return Err(error),
                };
                let identity = prepared.run.identity()?;
                let change = self.directory_change(&index, &definition.columns, &snapshot, prepared, input, cancellation).await?;
                publish_change(publisher, storage, &index, change).await?;
                if !index.records(&snapshots.snapshot().snapshot()).iter().any(|run| run.identity().is_ok_and(|found| found == identity)) { break; }
            }
            match self.repair_term_directory(&index, &definition.columns, snapshots, publisher, storage, cancellation).await {
                Err(error) if super::is_search_gap(&error, cancellation) => self.runtime.record_serving_gap(),
                result => result?,
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
        let hold = self.hold_logical_input(&snapshot).await?;
        IoRegion::reading(hold, async {
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
                    RunContents { documents, erases: local }, indexing, run.source_receipt.clone(), cancellation).await?;
                let current = snapshots.snapshot().snapshot();
                let change = self.directory_change(index, columns, &current, replacement, vec![run.digest().to_vec()], cancellation).await?;
                publish_change(publisher, storage, index, change).await?;
            }
            Ok::<_, rtdb_types::Anomaly>(())
        }).await?;
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
            let hold = self.hold_logical_input(&snapshot).await?;
            let attempt = IoRegion::reading(hold, async {
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
                let recovered = self.recover_from_history(
                    &index, &definition.columns, &snapshot, &blocks, Some(target), cancellation,
                ).await;
                let (mut contents, charge, source_rows, receipt) = match recovered {
                    Ok(Some(RecoveredRun { contents, charge, receipt })) => (contents, charge, receipt.source_rows(), Some(receipt)),
                    Err(error) if !super::is_search_gap(&error, cancellation) => return Err(error),
                    Ok(None) | Err(_) => {
                        let (mut contents, charge, source_rows) = self.read_l0_contents(&entry.slug, &blocks, &definition.columns, true, cancellation).await?;
                        if source_rows != target.source_rows() { return Err(fault("Search repair covers a different number of source events")); }
                        for run in index.records(&snapshot).into_iter().filter(|run| run.has_erases() && run.digest() != target.digest()) {
                            let erases = async {
                                let file = self.runtime.logical_file(&self.storage, &entry.slug, run, &definition.columns).await?;
                                file.erases(&self.memory).await
                            }.await;
                            match erases {
                                Ok((erases, _charge)) => scrub_documents(&mut contents.documents, &erases),
                                Err(error) if super::is_search_gap(&error, cancellation) => {
                                    let (restored, _charge, rows) = self.read_l0_contents(&entry.slug, &run.blocks, &[], false, cancellation).await?;
                                    if rows != run.source_rows() { return Err(fault("Search erase replay covers a different number of source events")); }
                                    scrub_documents(&mut contents.documents, &restored.erases)
                                }
                                Err(error) => return Err(error),
                            };
                        }
                        (contents, charge, source_rows, target.source_receipt.clone())
                    }
                };
                scrub_documents(&mut contents.documents, &contents.erases);
                let prepared = self.store_documents(&index, &definition.columns, &entry.slug, blocks, rebuild, source_rows, contents, charge, receipt, cancellation).await?;
                let change = self.directory_change(&index, &definition.columns, &snapshot, prepared, vec![id.to_vec()], cancellation).await?;
                publish_change(publisher, storage, &index, change).await?;
                if snapshots.snapshot().trie_catalog().search_runs(&table).any(|held| held.identity().is_ok_and(|held| held == id)) {
                    return Err(fault("The search repair input is still active after publication"));
                }
                Ok(())
            }).await;
            match attempt {
                Ok(()) => self.runtime.clear_run(&self.database, &table, id),
                Err(error) if super::is_search_gap(&error, cancellation) => self.runtime.record_serving_gap(),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn merge_logical(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, inputs: &[&SearchRun],
        cancellation: &CancellationToken) -> Result<PreparedLogicalRun> {
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
        blocks.sort_unstable();
        let receipt = inputs[0].source_receipt.clone().map(|mut receipt| {
            receipt.blocks.clone_from(&blocks);
            receipt.source_rows = Some(source_rows);
            receipt
        });
        self.store_documents(index, columns, &entry.slug, blocks, 0, source_rows, contents, indexing, receipt, cancellation).await
    }

    async fn store_documents(&self, index: &LogicalIndex, columns: &[String], slug: &TableSlug, mut blocks: Vec<i64>, rebuild: i64, source_rows: i64,
        mut contents: RunContents, indexing: Arc<rtdb_columnar::MemoryReservation>, source_receipt: Option<SourceReceipt>,
        cancellation: &CancellationToken) -> Result<PreparedLogicalRun> {
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
        let ((locator, digest), summaries) = tokio::task::spawn_blocking(move || {
            let output = BufWriter::new(File::create(&file).map_err(fault)?);
            let (_, summaries) = write_group_as(&columns, [[Ok(MergedRows(rows))]], &identity, Some(version), None, output,
                Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
            Ok::<_, rtdb_types::Anomaly>((read_locator(&file)?, summaries))
        }).await.map_err(fault)??;
        let run = self.publish_run(index, slug, &output, blocks, rebuild, source_rows, has_erases, locator, digest, allocation, source_receipt, cancellation).await?;
        Ok(PreparedLogicalRun { run, summaries })
    }
}

fn same_history_cut(left: &SearchRun, right: &SearchRun) -> bool {
    match (&left.source_receipt, &right.source_receipt) {
        (None, None) => true,
        (Some(left), Some(right)) => left.suffix_through == right.suffix_through && left.extends(right),
        _ => false,
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
