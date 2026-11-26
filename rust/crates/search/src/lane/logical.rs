use std::{cell::RefCell, collections::{BTreeMap, BTreeSet}, fs::File, io::BufWriter, path::Path, sync::Arc};

use arrow_array::Array;
use rtdb_format::term_directory;
use rtdb_format::log::proto::{SourceReceipt, TermDirectoryRoot};
use rtdb_query::ObjectPageStore;
use rtdb_format::{entity_history::forward_owner, keys::{TableSlug, TrieKey}, log::proto::{ArtefactEntry, ArtefactRecord, ArtefactsAdded, SearchRun, SearchRunChange}};
use rtdb_index::{PartitionValue, Snapshot, TrieCatalog};
use rtdb_maintenance::{ArtefactDefinition, ArtefactPublisher, HistoryCoverage, HistoryRead, HistoryRow, SnapshotSource, input::download};
use rtdb_storage::{HeldView, IoRegion, ObjectKey};
use rtdb_types::{AnomalyCategory, BlockIndex, EventHeader, EventOp, Iid, Interval, Result, StorageEpoch, StorageVersion, TableRef, Timestamp};
use rtdb_types::AnomalyKind;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

use crate::{SearchDefinition, file::{BuildMode, EncodedTreeLocator, FileDigestWriter, IndexedRow, LOGICAL_FORMAT_VERSION, LogicalDocument, SearchPage, SearchPosition, search_position, TextRevision, write_group_as}};
use super::{ArrowSearchPage, PageRows, SearchBuilder, fault, indexing_allowance, open_search_input};
use super::forward::{ForwardFile, ForwardOutput};
use crate::term_directory::{TermEdits, TermSummaries};

type Erases = BTreeMap<Iid, SearchPosition>;
type RunPair = [[u8; 32]; 2];
type UnmergeablePair = (RunPair, LogicalIndex);

const MAX_UNMERGEABLE_PAIRS: usize = 1024;

#[derive(Default)]
pub(super) struct UnmergeablePairs {
    earlier: BTreeSet<UnmergeablePair>,
    most_recent: Option<UnmergeablePair>,
}

impl UnmergeablePairs {
    fn touch(&mut self, index: &LogicalIndex, pair: RunPair) -> bool {
        let key = (pair, index.clone());
        if self.most_recent.as_ref() == Some(&key) { return true; }
        let Some(remembered) = self.earlier.take(&key) else { return false; };
        self.remember(remembered);
        true
    }

    fn insert(&mut self, index: &LogicalIndex, pair: RunPair) {
        let key = (pair, index.clone());
        if self.most_recent.as_ref() == Some(&key) { return; }
        self.earlier.remove(&key);
        self.remember(key);
    }

    fn remember(&mut self, key: UnmergeablePair) {
        if let Some(previous) = self.most_recent.replace(key)
            && self.earlier.len() < MAX_UNMERGEABLE_PAIRS - 1 {
            self.earlier.insert(previous);
        }
    }

    pub(super) fn retain_active(&mut self, catalog: &TrieCatalog) {
        let active = |(pair, index): &UnmergeablePair| pair.iter().all(|digest| {
            catalog.active_search_run(&index.table, digest).is_some_and(|run| index.owns(run))
        });
        self.earlier.retain(active);
        self.most_recent = self.most_recent.take().filter(active);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.earlier.len() + usize::from(self.most_recent.is_some())
    }
}

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

fn keep_merge_document(
    documents: &mut BTreeMap<(Iid, usize, TextRevision), LogicalDocument>, erases: &Erases, document: LogicalDocument,
) -> Result<Option<(Iid, usize, TextRevision)>> {
    use std::collections::btree_map::Entry;

    if erases.get(&document.iid).is_some_and(|erase| document.position < *erase) {
        return Ok(None);
    }
    let key = (document.iid, document.field, document.revision);
    match documents.entry(key) {
        Entry::Vacant(entry) => { entry.insert(document); },
        Entry::Occupied(mut entry) => {
            let prior = entry.get();
            require_matching_revision(prior, &document)?;
            if document.position <= prior.position { return Ok(None); }
            entry.insert(document);
        },
    }
    Ok(Some(key))
}

fn keep_merge_page(
    documents: &mut BTreeMap<(Iid, usize, TextRevision), LogicalDocument>, erases: &Erases,
    page: &mut dyn ExactSizeIterator<Item = Result<LogicalDocument>>,
    cancellation: &CancellationToken,
) -> Result<()> {
    let token_bytes = |doc: &LogicalDocument| {
        let offsets = doc.tokens.value_offsets();
        (offsets[doc.tokens.len()] - offsets[0]) as usize + doc.tokens.len() * size_of::<i32>()
    };
    let mut page_bytes = 0usize;
    let mut selected = Vec::with_capacity(page.len());
    for document in page {
        let document = document?;
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        page_bytes = page_bytes.saturating_add(token_bytes(&document));
        if let Some(key) = keep_merge_document(documents, erases, document)? { selected.push(key); }
    }
    selected.sort_unstable();
    selected.dedup();
    let retained = selected.iter().map(|key| token_bytes(&documents[key])).sum::<usize>();
    // A sparse winner must not pin the token buffer for all discarded rows.
    if retained.saturating_mul(2) < page_bytes {
        for key in selected {
            let document = documents.get_mut(&key).expect("admitted revision");
            document.tokens = document.tokens.iter().collect();
        }
    }
    Ok(())
}

fn require_matching_revision(left: &LogicalDocument, right: &LogicalDocument) -> Result<()> {
    if left.tokens != right.tokens || left.sql_id != right.sql_id {
        return Err(fault("A logical text revision has conflicting witnesses"));
    }
    Ok(())
}

fn needs_order_replay(run: &SearchRun) -> bool { run.allocation.is_none() && run.has_erases() }

fn revision_hasher(analyzer: rtdb_text::Analyzer) -> Sha256 {
    let mut digest = Sha256::new();
    digest.update(b"rtdb/search/token-revision");
    digest.update(rtdb_text::ANALYZER_VERSION.to_le_bytes());
    hash_term(&mut digest, analyzer.name());
    digest
}

fn hash_term(digest: &mut Sha256, term: &str) {
    digest.update((term.len() as u64).to_le_bytes());
    digest.update(term.as_bytes());
}

pub(crate) fn text_revision(analyzer: rtdb_text::Analyzer, value: &str) -> TextRevision {
    let mut digest = revision_hasher(analyzer);
    analyzer.visit_tokens(value, |term| hash_term(&mut digest, term));
    digest.finalize().into()
}

/// The revision of `tokens` that `analyzer` produced.
pub(super) fn revision_of(analyzer: rtdb_text::Analyzer, tokens: &arrow_array::StringArray) -> TextRevision {
    let mut digest = revision_hasher(analyzer);
    for term in tokens.iter().flatten() { hash_term(&mut digest, term); }
    digest.finalize().into()
}

pub(crate) fn history_id(definition: &SearchDefinition) -> String {
    format!("{}h", definition.artefact_id_for(LOGICAL_FORMAT_VERSION))
}

pub(crate) fn run_identity(slug: &TableSlug, definition: &str, version: i64, blocks: &[i64], rebuild: i64, allocation: &[u8]) -> String {
    let base = format!("logical-search/{slug}/{definition}/{version}/{blocks:?}/{rebuild}", slug = slug.as_str());
    if allocation.is_empty() { base } else { format!("{base}/{allocation:02x?}") }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LogicalIndex {
    table: TableRef,
    id: String,
    version: i64,
    analyzer: rtdb_text::Analyzer,
}

impl LogicalIndex {
    pub fn supports(definition: &SearchDefinition) -> bool {
        definition.history && !definition.table.is_owned_event_data() && !TableRef::owned_schema(&definition.table.schema)
    }

    fn of(definition: &SearchDefinition) -> Self {
        Self { table: definition.table.clone(), id: definition.artefact_id_for(LOGICAL_FORMAT_VERSION), version: definition.version,
            analyzer: definition.analyzer }
    }

    pub fn from_history(definition: &ArtefactDefinition, analyzer: rtdb_text::Analyzer) -> Self {
        Self { table: definition.table.clone(), id: definition.id.trim_end_matches('w').to_owned(), version: definition.version, analyzer }
    }

    fn owns(&self, run: &SearchRun) -> bool {
        run.definition_id() == self.id && run.definition_version() == self.version
    }

    fn records<'a>(&self, snapshot: &'a Snapshot) -> Vec<&'a SearchRun> {
        snapshot.trie_catalog().search_runs(&self.table).filter(|run| self.owns(run)).collect()
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

fn record_source(batch: &rtdb_format::data::EventBatch, columns: &[String], analyzer: rtdb_text::Analyzer, include_documents: bool,
    contents: &mut RunContents, charge: &mut Arc<rtdb_types::MemoryReservation>, cancellation: &CancellationToken) -> Result<()> {
    for row in 0..batch.len() {
        record_history_row(HistoryRow::Stored { batch, row }, columns, analyzer, include_documents, contents, charge, cancellation)?;
    }
    Ok(())
}

fn record_history_row(source: HistoryRow<'_>, columns: &[String], analyzer: rtdb_text::Analyzer, include_documents: bool, contents: &mut RunContents,
    charge: &mut Arc<rtdb_types::MemoryReservation>, cancellation: &CancellationToken) -> Result<()> {
    if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
    let (mut header, origin, row) = match source {
        HistoryRow::Stored { batch, row } => (batch.header(row)?, batch.origin(row)?, row),
        HistoryRow::Header { header, origin, page_row } => (header, origin, page_row),
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
                let tokens = analyzer.analyze(text);
                let retained = tokens.get_array_memory_size().saturating_add(256);
                reservation.grow_to(reservation.bytes().saturating_add(retained))?;
                contents.documents.push(LogicalDocument { iid: header.iid, field, revision: revision_of(analyzer, &tokens),
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

struct PostingPage<'a> {
    page: ArrowSearchPage,
    number: usize,
    columns: usize,
    forward: &'a RefCell<Option<ForwardOutput>>,
    revisions: Vec<Vec<Option<TextRevision>>>,
    _scratch: Option<Arc<rtdb_columnar::MemoryReservation>>,
}

impl SearchPage for PostingPage<'_> {
    fn len(&self) -> usize { self.page.len() }

    fn row(&mut self, row: usize, columns: &[String]) -> Result<IndexedRow<'_>> {
        let indexed = self.page.row(row, columns)?;
        if self._scratch.is_some() { self.revisions.push(indexed.revisions.clone()); }
        Ok(indexed)
    }

    fn complete(self) {
        let mut forward = self.forward.borrow_mut();
        if let Some(output) = forward.as_mut()
            && output.append_revisions(self.number, &self.page.events, self.columns, &self.revisions).is_err() {
            *forward = None;
        }
    }
}

impl SearchBuilder {
    async fn can_stage_logical(&self) -> Result<bool> {
        match self.store.search_staging_owner().await {
            Ok(_) => Ok(true),
            Err(error) if error.category == AnomalyCategory::Unsupported => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn hold_logical_input(&self, snapshot: &Snapshot) -> Result<Arc<HeldView>> {
        HeldView::catalog(self.store.as_ref(), Arc::new(snapshot.clone()), snapshot.catalog_cut()).await
    }

    pub(super) async fn seal_logical(&self, index: &LogicalIndex, definition: &ArtefactDefinition, snapshot: &Arc<PartitionValue>,
        trie: &TrieKey, path: &Path, size: u64, cancellation: &CancellationToken) -> Result<Option<(ArtefactRecord, Option<ArtefactRecord>)>> {
        if !self.can_stage_logical().await? { return Ok(None); }
        let view = snapshot.snapshot();
        let seal = view.iter_segments(&index.table).next().cloned().map(|segment| (view.clone(), segment));
        let history_id = format!("{}w", index.id);
        let (columns, _) = self.columns.lock().get(&(index.table.clone(), history_id, index.version)).cloned()
            .ok_or_else(|| fault("Logical search definition has no fields"))?;
        let entry = view.table_entry(&index.table).ok_or_else(|| fault("Logical search table has no slug"))?;
        let owner = forward_owner(&entry.slug, trie, &definition.id, definition.version);
        let (run, forward) = self.build_logical(index, &columns, &view, trie.block_index(), File::open(path).map_err(fault)?,
            size.min(i64::MAX as u64) as i64, seal, Some(owner), cancellation).await?;
        let record = SearchRunChange { run: Some(run.run), ..Default::default() }.to_artefact()?;
        let forward = if let Some(forward) = forward {
            match forward.publish(self.store.as_ref(), &entry.slug, trie, definition, cancellation).await {
                Ok(record) => Some(record),
                Err(error) if super::is_search_gap(&error, cancellation) => None,
                Err(error) => return Err(error),
            }
        } else { None };
        Ok(Some((record, forward)))
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
        let read = HistoryRead { snapshot, storage: &self.storage, memory: &self.memory, cancellation, table: &index.table, blocks, put_columns: columns };
        let Some(coverage) = source.visit_table(read, &mut |block, row| {
            if block.0 < 0 || block > boundary.block_index() { return Err(fault("Search history has an invalid source block")); }
            total = total.checked_add(1).ok_or_else(|| fault("Search replay row count overflow"))?;
            if blocks.binary_search(&block.0).is_ok() {
                observed.insert(block.0);
                selected = selected.checked_add(1).ok_or_else(|| fault("Search extent row count overflow"))?;
                record_history_row(row, columns, index.analyzer, true, &mut contents, &mut charge, cancellation)?;
            } else {
                record_history_row(row, columns, index.analyzer, false, &mut outside, &mut charge, cancellation)?;
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
        size: i64, seal: Option<(Snapshot, Arc<rtdb_index::LiveTable>)>, forward_owner: Option<[u8; 32]>,
        cancellation: &CancellationToken) -> Result<(PreparedLogicalRun, Option<ForwardFile>)> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Logical search table has no slug"))?;
        let directory = tempfile::tempdir().map_err(fault)?;
        let output = directory.path().join("postings.arrow");
        let file = output.clone();
        let blocks = vec![block.0];
        let allocation = *uuid::Uuid::new_v4().as_bytes();
        let identity = run_identity(&entry.slug, &index.id, index.version, &blocks, 0, &allocation);
        let (budget, token, columns, version, analyzer) = (Arc::clone(&self.memory), cancellation.clone(), columns.to_vec(), index.version, index.analyzer);
        let indexing = self.memory.reserve(indexing_allowance(size))?;
        let (locator, digest, source_rows, has_erases, summaries, forward) = tokio::task::spawn_blocking(move || {
            let build = |forward_owner: Option<[u8; 32]>, indexing| {
                let forward = RefCell::new(forward_owner.and_then(|owner| ForwardOutput::new(owner, &budget).ok()));
                let (reader, opening) = open_search_input(input.try_clone().map_err(fault)?, &columns, &budget)?;
                let mut pages = PageRows::new(reader, opening, Arc::clone(&budget), token.clone(), analyzer, false);
                pages.mode = BuildMode::Postings;
                pages.seal = seal.clone();
                let mut output = FileDigestWriter::new(BufWriter::new(File::create(&file).map_err(fault)?));
                let mut source_rows = 0i64;
                let mut has_erases = false;
                let counted = pages.enumerate().map(|(number, page)| {
                    let page = page?;
                    source_rows += page.len() as i64;
                    if !has_erases {
                        for row in 0..page.len() {
                            if page.events.header(row)?.op == EventOp::Erase { has_erases = true; break; }
                        }
                    }
                    let mut forward_output = forward.borrow_mut();
                    let scratch = forward_output.as_ref().and_then(|_| budget.reserve(page.len().saturating_mul(
                        64usize.saturating_add(columns.len().saturating_mul(64)))).ok());
                    if scratch.is_none() { *forward_output = None; }
                    let capacity = if scratch.is_some() { page.len() } else { 0 };
                    Ok(PostingPage { page, number, columns: columns.len(), forward: &forward,
                        revisions: Vec::with_capacity(capacity), _scratch: scratch })
                });
                let (_, summaries, tree) = write_group_as(&columns, [counted], &identity, Some(version), None, &mut output,
                    Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
                let digest = output.into_digest();
                let forward = forward.into_inner().and_then(|output| output.finish().ok());
                let tree = tree.ok_or_else(|| fault("Logical search run has no written root"))?;
                Ok::<_, rtdb_types::Anomaly>((tree.encode(false), digest, source_rows, has_erases, summaries, forward))
            };
            match build(forward_owner, indexing) {
                Err(error) if forward_owner.is_some() && error.is(AnomalyKind::ResourceExhausted) && !token.is_cancelled() => {
                    build(None, budget.reserve(indexing_allowance(size))?)
                }
                result => result,
            }
        }).await.map_err(fault)??;
        let run = self.publish_run(index, &entry.slug, &output, blocks, 0, source_rows, has_erases, locator, digest, allocation, None, cancellation).await?;
        Ok((PreparedLogicalRun { run, summaries }, forward))
    }

    async fn publish_run(&self, index: &LogicalIndex, slug: &TableSlug, output: &Path, blocks: Vec<i64>, rebuild: i64, source_rows: i64,
        has_erases: bool, locator: EncodedTreeLocator,
        digest: [u8; 32], allocation: [u8; 16], source_receipt: Option<SourceReceipt>, cancellation: &CancellationToken) -> Result<SearchRun> {
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut run = SearchRun { definition_id: Some(index.id.clone()), definition_version: Some(index.version), blocks,
            digest: Some(digest.to_vec()), locator: Some(locator.text.into_bytes()), file_size: Some(locator.length as i64),
            publication: None, rebuild: (rebuild > 0).then_some(rebuild), source_rows: Some(source_rows),
            has_erases: has_erases.then_some(true), allocation: Some(allocation.to_vec()), source_receipt };
        let intent = ObjectKey::new(run.pending_key(slug)?)?;
        let staged = rtdb_format::search_manifest::StagedSearchRun { run: run.clone(), owner: self.store.search_staging_owner().await? };
        self.store.put(&intent, staged.encode()?.into()).await?;
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let key = ObjectKey::new(run.object_key(slug)?)?;
        // A provider can commit a failed request, so the collector needs its intent after the writer drains.
        run.publication = self.store.put_file_with_publication(&key, output).await?.map(Into::into);
        if self.store.releases_exact_search_runs() && run.publication.is_none() {
            run.publication = self.store.retirement_publication(&key).await?.map(Into::into);
        }
        if self.store.requires_search_publication() && run.publication.is_none() {
            return Err(fault("Search upload returned no immutable provider publication"));
        }
        Ok(run)
    }

    async fn directory_change(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, prepared: PreparedLogicalRun,
        replaces: Vec<Vec<u8>>, cancellation: &CancellationToken) -> Result<SearchRunChange> {
        let PreparedLogicalRun { run, summaries } = prepared;
        let hold = self.hold_logical_input(snapshot).await?;
        let update = IoRegion::reading(hold, self.prepare_term_directory(index, columns, snapshot,
            DirectoryUpdate::Adopt { run: &run, summaries: &summaries, replaces: &replaces }, cancellation)).await;
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
                edits.apply(&pages, base.as_ref()).await
            };
            match incremental.await {
                Ok(root) => Some(root),
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
                edits.apply(&pages, None).await?
            }
        };
        if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
        let mut covered = before.iter().copied().filter(|run| !is_replaced(run))
            .map(|run| run.identity()).collect::<Result<Vec<_>>>()?;
        if let Some((run, _)) = added { covered.push(run.identity()?); }
        Ok((TermDirectoryRoot { definition_id: Some(index.id.clone()), definition_version: Some(index.version),
            root: Some(root.as_ref().map_or_else(Vec::new, |root| root.encode())), runs: Some(runs as i64),
            run_set: Some(term_directory::run_set(covered).to_vec()) }, basis))
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
        let can_stage = definitions.iter().any(LogicalIndex::supports) && self.can_stage_logical().await?;
        if can_stage {
            self.repair_logical(snapshots, publisher, storage, &definitions, cancellation).await?;
        }
        let snapshot = snapshots.snapshot().snapshot();
        for table in snapshot.trie_catalog().tables() {
            for run in snapshot.trie_catalog().search_runs(table) {
                if definitions.iter().any(|definition| definition.table == *table && LogicalIndex::supports(definition)
                    && definition.artefact_id_for(LOGICAL_FORMAT_VERSION) == run.definition_id() && definition.version == run.definition_version()) {
                    continue;
                }
                // Retiring a run reads none of its tokens, so its analyzer plays no part.
                let index = LogicalIndex { table: table.clone(), id: run.definition_id().to_owned(), version: run.definition_version(),
                    analyzer: rtdb_text::Analyzer::Simple };
                publish_change(publisher, storage, &index, SearchRunChange {
                    run: Some(run.clone()), retire_only: Some(true), ..Default::default()
                }).await?;
            }
        }
        if !can_stage { return Ok(()); }
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
                    trie.data_file_size, None, None, cancellation).await {
                    Ok((run, _)) => run,
                    Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); break; }
                    Err(error) => return Err(error),
                };
                let current = snapshots.snapshot().snapshot();
                let change = self.directory_change(&index, &definition.columns, &current, prepared, Vec::new(), cancellation).await?;
                publish_change(publisher, storage, &index, change).await?;
            }
            self.merge_logical_runs(&index, &definition.columns, snapshots, publisher, storage, cancellation).await?;
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

    async fn merge_logical_runs(&self, index: &LogicalIndex, columns: &[String], snapshots: &dyn SnapshotSource,
        publisher: &dyn ArtefactPublisher, storage: (StorageVersion, StorageEpoch), cancellation: &CancellationToken) -> Result<()> {
        let limit = (self.memory.limit_bytes() / 8).min(128 << 20) as i64;
        loop {
            let snapshot = snapshots.snapshot().snapshot();
            let mut runs = index.records(&snapshot);
            runs.sort_unstable_by_key(|run| (run.blocks.len(), run.blocks[0]));
            let candidates = runs.array_windows::<2>().copied().filter(|[left, right]| {
                left.blocks.len() == right.blocks.len() && !needs_order_replay(left) && !needs_order_replay(right)
                    && left.blocks.len() * 2 <= rtdb_format::search_manifest::MAX_RUN_BLOCKS
                    && left.file_size().saturating_add(right.file_size()) <= limit
                    && compatible_merge_inputs(left, right, self.history.is_some())
            });
            let mut adopted = false;
            for pair in candidates {
                let [left, right] = pair.map(SearchRun::identity);
                let digests = [left?, right?];
                if self.unmergeable.lock().touch(index, digests) { continue; }
                let hold = self.hold_logical_input(&snapshot).await?;
                let merged = IoRegion::reading(hold, self.merge_logical(index, columns, &snapshot, pair, cancellation)).await;
                let prepared = match merged {
                    Ok(Some(run)) => run,
                    Ok(None) => { self.unmergeable.lock().insert(index, digests); continue; }
                    Err(error) if super::is_search_gap(&error, cancellation) => { self.runtime.record_serving_gap(); break; }
                    Err(error) => return Err(error),
                };
                let inputs = digests.into_iter().map(|digest| digest.to_vec()).collect();
                let identity = prepared.run.identity()?;
                let change = self.directory_change(index, columns, &snapshot, prepared, inputs, cancellation).await?;
                publish_change(publisher, storage, index, change).await?;
                adopted = index.records(&snapshots.snapshot().snapshot()).iter().any(|run| run.identity().is_ok_and(|found| found == identity));
                break;
            }
            if !adopted { return Ok(()); }
        }
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

    async fn read_l0_contents(&self, slug: &TableSlug, blocks: &[i64], columns: &[String], analyzer: rtdb_text::Analyzer, documents: bool,
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
                let (mut reader, _opening) = open_search_input(input, if documents { &columns } else { &[] }, &memory)?;
                for page in 0..reader.page_count() {
                    let batch = rtdb_maintenance::input::read_page(&mut reader, page, &memory)?;
                    let batch = rtdb_format::data::EventBatch::new(batch.batch().clone())?;
                    count = count.checked_add(batch.len() as i64).ok_or_else(|| fault("Search repair row count overflow"))?;
                    record_source(&batch, &columns, analyzer, documents, &mut contents, &mut charge, &token)?;
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
                        Ok(()) => return Ok(true),
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
                        let (mut contents, charge, source_rows) = self.read_l0_contents(&entry.slug, &blocks, &definition.columns, definition.analyzer, true, cancellation).await?;
                        if source_rows != target.source_rows() { return Err(fault("Search repair covers a different number of source events")); }
                        for run in index.records(&snapshot).into_iter().filter(|run| run.has_erases() && run.digest() != target.digest()) {
                            let erases = async {
                                let file = self.runtime.logical_file(&self.storage, &entry.slug, run, &definition.columns).await?;
                                file.erases(&self.memory).await
                            }.await;
                            match erases {
                                Ok((erases, _charge)) => scrub_documents(&mut contents.documents, &erases),
                                Err(error) if super::is_search_gap(&error, cancellation) => {
                                    let (restored, _charge, rows) = self.read_l0_contents(&entry.slug, &run.blocks, &[], definition.analyzer, false, cancellation).await?;
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
                Ok(!snapshots.snapshot().trie_catalog().search_runs(&table).any(|held| held.identity().is_ok_and(|held| held == id)))
            }).await;
            match attempt {
                Ok(true) => self.runtime.clear_run(&self.database, &table, id),
                Ok(false) => {
                    self.runtime.mark_run_suspect(&self.database, &table, id);
                    self.runtime.record_serving_gap();
                }
                Err(error) if super::is_search_gap(&error, cancellation) => self.runtime.record_serving_gap(),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    async fn merge_logical(&self, index: &LogicalIndex, columns: &[String], snapshot: &Snapshot, inputs: [&SearchRun; 2],
        cancellation: &CancellationToken) -> Result<Option<PreparedLogicalRun>> {
        let entry = snapshot.table_entry(&index.table).ok_or_else(|| fault("Logical merge has no table slug"))?;
        let mut blocks = inputs.iter().flat_map(|run| run.blocks.iter().copied()).collect::<Vec<_>>();
        blocks.sort_unstable();
        let source_rows = inputs.iter().try_fold(0i64, |count, run| count.checked_add(run.source_rows()))
            .ok_or_else(|| fault("Search merge source row count overflow"))?;
        if let [Some(prior), None] | [None, Some(prior)] = inputs.map(|run| run.source_receipt.as_ref()) {
            let Some(recovered) = self.recover_from_history(index, columns, snapshot, &blocks, None, cancellation).await? else { return Ok(None); };
            let has_erases = inputs.iter().any(|run| run.has_erases());
            let recovered_has_erases = !recovered.contents.erases.is_empty();
            if recovered.receipt.source_rows() != source_rows || !recovered.receipt.extends(prior) || has_erases != recovered_has_erases {
                return Ok(None);
            }
            return self.store_documents(index, columns, &entry.slug, blocks, 0, source_rows, recovered.contents, recovered.charge,
                Some(recovered.receipt), cancellation).await.map(Some);
        }
        let mut contents = RunContents::default();
        let bytes = inputs.iter().map(|run| run.file_size()).sum::<i64>().saturating_mul(4);
        let indexing = self.memory.reserve(indexing_allowance(bytes))?;
        let mut files = Vec::with_capacity(inputs.len());
        for run in inputs {
            if cancellation.is_cancelled() { return Err(crate::accelerate::search_cancelled()); }
            let file = self.runtime.logical_file(&self.storage, &entry.slug, run, columns).await?;
            if run.has_erases() {
                let (erases, _charge) = file.erases(&self.memory).await?;
                extend_erases(&mut contents.erases, erases);
            }
            files.push(file);
        }
        let mut documents = BTreeMap::new();
        for file in files {
            file.visit_document_pages(|page| keep_merge_page(&mut documents, &contents.erases, page, cancellation)).await?;
        }
        contents.documents = documents.into_values().collect();
        let receipt = inputs[0].source_receipt.clone().map(|mut receipt| {
            receipt.blocks.clone_from(&blocks);
            receipt.source_rows = Some(source_rows);
            receipt
        });
        self.store_documents(index, columns, &entry.slug, blocks, 0, source_rows, contents, indexing, receipt, cancellation).await.map(Some)
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
            if id(a) == id(b) { require_matching_revision(a, b)?; }
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
        let (locator, digest, summaries) = tokio::task::spawn_blocking(move || {
            let mut output = FileDigestWriter::new(BufWriter::new(File::create(&file).map_err(fault)?));
            let (_, summaries, tree) = write_group_as(&columns, [[Ok(MergedRows(rows))]], &identity, Some(version), None, &mut output,
                Some(indexing), LOGICAL_FORMAT_VERSION, BuildMode::Postings)?;
            let tree = tree.ok_or_else(|| fault("Logical search run has no written root"))?;
            Ok::<_, rtdb_types::Anomaly>((tree.encode(false), output.into_digest(), summaries))
        }).await.map_err(fault)??;
        let run = self.publish_run(index, slug, &output, blocks, rebuild, source_rows, has_erases, locator, digest, allocation, source_receipt, cancellation).await?;
        Ok(PreparedLogicalRun { run, summaries })
    }
}

fn compatible_merge_inputs(left: &SearchRun, right: &SearchRun, has_history: bool) -> bool {
    match (&left.source_receipt, &right.source_receipt) {
        (None, None) => true,
        (Some(left), Some(right)) => left.suffix_through == right.suffix_through && left.extends(right),
        _ => has_history,
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
    fn row(&mut self, row: usize, columns: &[String]) -> Result<IndexedRow<'_>> {
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
    assert_eq!(text_revision(rtdb_text::Analyzer::Simple, "Quarterly Audit—REPORT"), text_revision(rtdb_text::Analyzer::Simple, "quarterly audit report"));
    assert_ne!(text_revision(rtdb_text::Analyzer::Simple, "report audit"), text_revision(rtdb_text::Analyzer::Simple, "audit report"));
    assert_ne!(text_revision(rtdb_text::Analyzer::Simple, "audit report"), text_revision(rtdb_text::Analyzer::Simple, "audit audit report"));
    assert_ne!(text_revision(rtdb_text::Analyzer::Simple, "a bc"), text_revision(rtdb_text::Analyzer::Simple, "ab c"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use rtdb_format::{block::proto::Block, catalog::{BlockCatalog, TableBlock}, data::{EventBatch, StoredEvent, StoredOperation, build_event_batch}, log::proto::ArtefactsRemoved};
    use rtdb_index::{CatalogTrie, PartitionState, TableCatalog, TrieCatalog};
    use rtdb_maintenance::{HistoryVisitor, TableHistorySource};
    use rtdb_storage::{LocalStore, MemoryStore, ObjectStore, cache::BufferPool, object_store::StoredObject};
    use rtdb_types::{LogicalType, MemoryBudget, ReplicaPosition, ScalarType, SourcePosition, StructFields, Value};

    const REPLAY_THROUGH: ReplicaPosition = ReplicaPosition(17);
    const TEST_STORAGE: (StorageVersion, StorageEpoch) = (StorageVersion(6), StorageEpoch(0));

    #[test]
    fn merge_keeps_latest_matching_witness_after_the_erase() -> Result<()> {
        let iid = Iid([7; 16]);
        let revision = [4; 32];
        let doc = |time, text| LogicalDocument {
            iid,
            sql_id: Some(9),
            field: 0,
            revision,
            tokens: arrow_array::StringArray::from(vec![text]),
            position: (Timestamp(time), None, 0),
        };
        let erases = BTreeMap::from([(iid, (Timestamp(2), None, 0))]);
        let mut documents = BTreeMap::new();
        let _ = keep_merge_document(&mut documents, &erases, doc(1, "erased"))?;
        assert!(documents.is_empty());
        let _ = keep_merge_document(&mut documents, &erases, doc(2, "survives"))?;
        let _ = keep_merge_document(&mut documents, &erases, doc(4, "survives"))?;
        let _ = keep_merge_document(&mut documents, &erases, doc(3, "survives"))?;
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[&(iid, 0, revision)].position.0, Timestamp(4));
        assert!(keep_merge_document(&mut documents, &erases, doc(5, "conflict")).is_err());
        assert_eq!(documents[&(iid, 0, revision)].position.0, Timestamp(4));
        Ok(())
    }

    #[test]
    fn sparse_merge_detaches_tokens_and_dense_merge_retains_the_page() -> Result<()> {
        let large = "x".repeat(16_384);
        let tokens = arrow_array::StringArray::from(vec!["kept", large.as_str()]);
        let source = tokens.values().as_ptr();
        let page = || (0..2).map(|row| {
            let id = row as u8 + 1;
            Ok(LogicalDocument {
                iid: Iid([id; 16]), sql_id: None, field: 0, revision: [id; 32],
                position: (Timestamp(1), None, 0), tokens: tokens.slice(row, 1),
            })
        });
        let token = CancellationToken::new();
        let mut documents = BTreeMap::new();
        let erases = BTreeMap::from([(Iid([2; 16]), (Timestamp(2), None, 0))]);
        keep_merge_page(&mut documents, &erases, &mut page(), &token)?;
        assert_eq!(documents.len(), 1);
        let selected = &documents[&(Iid([1; 16]), 0, [1; 32])].tokens;
        assert_ne!(selected.values().as_ptr(), source);
        assert_eq!(selected.value(0), "kept");
        documents.clear();
        keep_merge_page(&mut documents, &BTreeMap::new(), &mut page(), &token)?;
        assert_eq!(documents.len(), 2);
        assert!(documents.values().all(|document| document.tokens.values().as_ptr() == source));
        Ok(())
    }

    struct TableReplay(Vec<EventBatch>);

    #[async_trait::async_trait]
    impl TableHistorySource for TableReplay {
        async fn visit_table(&self, read: HistoryRead<'_>, visit: &mut HistoryVisitor<'_>) -> Result<Option<HistoryCoverage>> {
            for (block, batch) in self.0.iter().enumerate() {
                let block = BlockIndex(block as i64);
                let history = if read.blocks.binary_search(&block.0).is_ok() {
                    HistoryRow::Stored { batch, row: 0 }
                } else {
                    HistoryRow::Header { header: batch.header(0)?, origin: batch.origin(0)?, page_row: 0 }
                };
                visit(block, history)?;
            }
            Ok(Some(HistoryCoverage::Native { lineage: "native/journal".into(), through: REPLAY_THROUGH }))
        }
    }

    struct CatalogPublisher<'a> {
        partition: &'a PartitionState,
        reject_once: parking_lot::Mutex<Option<TableRef>>,
    }

    #[async_trait::async_trait]
    impl ArtefactPublisher for CatalogPublisher<'_> {
        async fn publish(&self, message: ArtefactsAdded) -> Result<SourcePosition> {
            let table = rtdb_format::catalog::legacy_table_ref(message.table_name())?;
            let reject = {
                let mut target = self.reject_once.lock();
                target.as_ref() == Some(&table) && target.take().is_some()
            };
            let mut entries = Vec::new();
            for entry in message.entries {
                let key = entry.trie_key().parse()?;
                let mut record = entry.record.unwrap();
                if reject {
                    let mut change = SearchRunChange::from_artefact(&record)?;
                    assert!(change.term_directory.is_some());
                    change.replaces_directory.as_mut().unwrap()[0] ^= 0xff;
                    record = change.to_artefact()?;
                }
                entries.push((key, record));
            }
            self.partition.add_artefacts(&table, entries);
            Ok(SourcePosition(1))
        }

        async fn remove(&self, _: ArtefactsRemoved) -> Result<SourcePosition> {
            unreachable!()
        }
    }

    fn replay_builder(events: &[StoredOperation]) -> SearchBuilder {
        let batches = events.iter().enumerate().map(|(block, operation)| {
            let fields = matches!(operation, StoredOperation::Put(_))
                .then(|| StructFields::from([("body".into(), LogicalType::Scalar(ScalarType::Utf8))]));
            let event = StoredEvent { position: None, origin: None, commit_position: 1, entity_key: None,
                iid: Iid([block as u8; 16]), system_from: Timestamp(block as i64 + 1), valid_time: Interval::ALL,
                recency: None, operation: operation.clone() };
            EventBatch::new(build_event_batch(&[event], fields, None).unwrap()).unwrap()
        }).collect();
        builder(Arc::new(MemoryStore::default())).with_history(Some(Arc::new(TableReplay(batches))))
    }

    fn replay_partition(runs: &[(TableRef, SearchRun)], block_index: i64) -> PartitionState {
        let mut tries = TrieCatalog::default();
        let mut tables = BTreeMap::new();
        for (table, run) in runs {
            let mut trie = CatalogTrie::notification(TrieKey::l0(BlockIndex(run.blocks[0])).unwrap(), 1, None).unwrap();
            trie.artefacts.push(SearchRunChange { run: Some(run.clone()), ..Default::default() }.to_artefact().unwrap());
            tries = tries.with_tries(table.clone(), [trie], Timestamp(1)).unwrap();
            tables.insert(table.clone(), TableBlock::from_parts(BTreeMap::new(), &StructFields::default(), block_index + 1, Vec::new()).unwrap());
        }
        let block = BlockCatalog::from_proto(Block { block_index: Some(block_index), boundary_replica_msg_id: Some(REPLAY_THROUGH.0),
            table_names: tables.keys().map(TableRef::schema_and_table).collect(), ..Default::default() }).unwrap();
        let tables = TableCatalog::load(Some(block), &tables).unwrap();
        PartitionState::new(PartitionValue::loaded(tries, tables, Default::default()).unwrap())
    }

    fn recorded_run(index: &LogicalIndex, block: i64) -> SearchRun {
        let mut digest = vec![0; 32];
        digest[..8].copy_from_slice(&block.to_be_bytes());
        SearchRun { definition_id: Some(index.id.clone()), definition_version: Some(index.version),
            blocks: vec![block], digest: Some(digest), file_size: Some(1), locator: Some(vec![1]),
            source_rows: Some(1), ..Default::default() }
    }

    #[tokio::test]
    async fn rejected_search_repair_stays_suspect_while_another_definition_repairs() {
        let builder = replay_builder(&[StoredOperation::Erase]);
        let definitions = ["a_docs", "z_docs"].map(|name| SearchDefinition { name: name.into(), table: TableRef::new("public", name),
            columns: vec!["body".into()], analyzer: rtdb_text::Analyzer::Simple, version: 1, history: true });
        let inputs = definitions.each_ref().map(|definition| {
            let index = LogicalIndex::of(definition);
            let run = SearchRun { has_erases: Some(true), ..recorded_run(&index, 0) };
            (index.table, run)
        });
        let partition = replay_partition(&inputs, 0);
        let [(first_table, first_run), (second_table, second_run)] = &inputs;
        let publisher = CatalogPublisher { partition: &partition, reject_once: parking_lot::Mutex::new(Some(first_table.clone())) };
        let cancellation = CancellationToken::new();
        let gaps = builder.runtime.serving_gaps();

        builder.repair_logical(&partition, &publisher, TEST_STORAGE, &definitions, &cancellation).await.unwrap();

        let value = partition.value();
        let catalog = value.trie_catalog();
        assert_eq!(catalog.search_runs(first_table).next(), Some(first_run));
        assert_eq!(catalog.retired_search_runs(first_table).count(), 1);
        assert_ne!(catalog.search_runs(second_table).next(), Some(second_run));
        assert_eq!(builder.runtime.serving_gaps(), gaps + 1);
        assert_eq!(builder.runtime.suspect_runs("xtdb"), [(first_table.clone(), first_run.identity().unwrap())]);

        builder.repair_logical(&partition, &publisher, TEST_STORAGE, &definitions, &cancellation).await.unwrap();

        assert!(builder.runtime.suspect_runs("xtdb").is_empty());
        assert_ne!(partition.value().trie_catalog().search_runs(first_table).next(), Some(first_run));
    }

    #[tokio::test]
    async fn mixed_search_run_merge_replays_and_receipts_the_complete_union() {
        let events = ["repaired text", "sealed text"].map(|body| StoredOperation::Put(StructFields::from([("body".into(), Value::Utf8(body.into()))])));
        let builder = replay_builder(&events);
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let prior = SourceReceipt { lineage: Some("native/journal".into()), checkpoint_boundary: Some(-1), suffix_through: Some(5),
            table: Some(index.table.schema_and_table()), shard_range: Some(rtdb_format::search_manifest::FULL_SHARD_RANGE.to_vec()),
            source_rows: Some(1), erase_frontier_digest: Some(Sha256::digest([]).to_vec()), blocks: vec![0], ..Default::default() };
        let repaired = SearchRun { source_receipt: Some(prior.clone()), ..recorded_run(&index, 0) };
        let sealed = recorded_run(&index, 1);
        let partition = replay_partition(&[(index.table.clone(), repaired), (index.table.clone(), sealed)], 1);
        let publisher = CatalogPublisher { partition: &partition, reject_once: parking_lot::Mutex::default() };
        let columns = ["body".into()];

        builder.merge_logical_runs(&index, &columns, &partition, &publisher, TEST_STORAGE, &CancellationToken::new()).await.unwrap();

        let snapshot = partition.value().snapshot();
        let runs = index.records(&snapshot);
        assert_eq!(runs.len(), 1);
        let receipt = runs[0].source_receipt.as_ref().unwrap();
        assert!(receipt.extends(&prior));
        assert_eq!(receipt.suffix_through(), REPLAY_THROUGH.0);
        assert_eq!(receipt.blocks, [0, 1]);
        assert_eq!(receipt.source_rows(), 2);
        assert_eq!(index.term_directory(&snapshot).unwrap().runs(), 1);
        let slug = &snapshot.table_entry(&index.table).unwrap().slug;
        let file = builder.runtime.logical_file(&builder.storage, slug, runs[0], &columns).await.unwrap();
        assert_eq!(file.documents().await.unwrap().into_iter().map(|doc| doc.revision).collect::<Vec<_>>(),
            [text_revision(rtdb_text::Analyzer::Simple, "repaired text"), text_revision(rtdb_text::Analyzer::Simple, "sealed text")]);
    }

    struct RecordedReplay(TableReplay, parking_lot::Mutex<Vec<Vec<i64>>>);

    #[async_trait::async_trait]
    impl TableHistorySource for RecordedReplay {
        async fn visit_table(&self, read: HistoryRead<'_>, visit: &mut HistoryVisitor<'_>) -> Result<Option<HistoryCoverage>> {
            self.1.lock().push(read.blocks.to_vec());
            self.0.visit_table(read, visit).await
        }
    }

    #[tokio::test]
    async fn unmergeable_mixed_pair_replays_once_while_active() {
        let events = ["repaired text", "sealed text"].map(|body| StoredOperation::Put(StructFields::from([("body".into(), Value::Utf8(body.into()))])));
        let source = Arc::new(RecordedReplay(TableReplay(events.iter().enumerate().map(|(block, operation)| {
            let fields = Some(StructFields::from([("body".into(), LogicalType::Scalar(ScalarType::Utf8))]));
            let event = StoredEvent { position: None, origin: None, commit_position: 1, entity_key: None,
                iid: Iid([block as u8; 16]), system_from: Timestamp(block as i64 + 1), valid_time: Interval::ALL,
                recency: None, operation: operation.clone() };
            EventBatch::new(build_event_batch(&[event], fields, None).unwrap()).unwrap()
        }).collect()), Default::default()));
        let builder = builder(Arc::new(MemoryStore::default())).with_history(Some(source.clone()));
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let prior = SourceReceipt { lineage: Some("native/another-journal".into()), checkpoint_boundary: Some(-1), suffix_through: Some(5),
            table: Some(index.table.schema_and_table()), shard_range: Some(rtdb_format::search_manifest::FULL_SHARD_RANGE.to_vec()),
            source_rows: Some(1), erase_frontier_digest: Some(Sha256::digest([]).to_vec()), blocks: vec![0], ..Default::default() };
        let repaired = SearchRun { source_receipt: Some(prior), ..recorded_run(&index, 0) };
        let partition = replay_partition(&[(index.table.clone(), repaired), (index.table.clone(), recorded_run(&index, 1))], 1);
        let publisher = CatalogPublisher { partition: &partition, reject_once: parking_lot::Mutex::default() };
        let columns = ["body".into()];
        for _ in 0..2 {
            builder.merge_logical_runs(&index, &columns, &partition, &publisher, TEST_STORAGE, &CancellationToken::new()).await.unwrap();
        }
        assert_eq!(index.records(&partition.value().snapshot()).len(), 2);
        assert_eq!(*source.1.lock(), [vec![0, 1]]);
        assert_eq!(builder.unmergeable.lock().len(), 1);

        use rtdb_maintenance::ArtefactBuilder;
        builder.maintain_independent(&partition, &publisher, TEST_STORAGE, &CancellationToken::new()).await.unwrap();

        assert!(index.records(&partition.value().snapshot()).is_empty());
        assert_eq!(builder.unmergeable.lock().len(), 0);
    }

    #[tokio::test]
    async fn unmergeable_overflow_rotates_replays_across_ordered_sweeps() {
        let source = Arc::new(RecordedReplay(TableReplay(Vec::new()), Default::default()));
        let builder = builder(Arc::new(MemoryStore::default())).with_history(Some(source.clone()));
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let last = MAX_UNMERGEABLE_PAIRS as i64 + 1;
        let inputs = (0..=last).map(|block| {
            let mut run = recorded_run(&index, block);
            if block % 2 == 0 {
                run.source_receipt = Some(SourceReceipt { lineage: Some("native/journal".into()), checkpoint_boundary: Some(-1),
                    suffix_through: Some(5), table: Some(index.table.schema_and_table()),
                    shard_range: Some(rtdb_format::search_manifest::FULL_SHARD_RANGE.to_vec()), source_rows: Some(1),
                    erase_frontier_digest: Some(Sha256::digest([]).to_vec()), blocks: vec![block], ..Default::default() });
            }
            (index.table.clone(), run)
        }).collect::<Vec<_>>();
        let partition = replay_partition(&inputs, last);
        let publisher = CatalogPublisher { partition: &partition, reject_once: parking_lot::Mutex::default() };
        let columns = ["body".into()];
        let cancellation = CancellationToken::new();

        builder.merge_logical_runs(&index, &columns, &partition, &publisher, TEST_STORAGE, &cancellation).await.unwrap();
        assert_eq!(source.1.lock().len(), last as usize);

        for _ in 0..2 {
            builder.merge_logical_runs(&index, &columns, &partition, &publisher, TEST_STORAGE, &cancellation).await.unwrap();
        }

        assert_eq!(source.1.lock()[last as usize..], [vec![last - 2, last - 1], vec![last - 3, last - 2]]);
    }

    #[tokio::test]
    async fn unmergeable_pair_is_removed_when_either_input_retires() {
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let runs = [0, 1].map(|block| recorded_run(&index, block));
        let pair = runs.each_ref().map(|run| run.identity().unwrap());
        let inputs = runs.clone().map(|run| (index.table.clone(), run));

        for (retired, surviving) in [(0, 1), (1, 0)] {
            let partition = replay_partition(&inputs, 1);
            let publisher = CatalogPublisher { partition: &partition, reject_once: parking_lot::Mutex::default() };
            let mut memo = UnmergeablePairs::default();
            memo.insert(&index, pair);
            memo.retain_active(partition.value().trie_catalog());
            assert!(memo.touch(&index, pair));

            publish_change(&publisher, TEST_STORAGE, &index, SearchRunChange {
                run: Some(runs[retired].clone()), retire_only: Some(true), ..Default::default()
            }).await.unwrap();

            let snapshot = partition.value().snapshot();
            assert_eq!(index.records(&snapshot), [&runs[surviving]]);
            memo.retain_active(snapshot.trie_catalog());
            assert_eq!(memo.len(), 0);
        }
    }

    #[test]
    fn unmergeable_pairs_belong_to_one_definition_generation() {
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let inputs = [0, 1].map(|block| (index.table.clone(), recorded_run(&index, block)));
        let pair = inputs.each_ref().map(|(_, run)| run.identity().unwrap());
        let partition = replay_partition(&inputs, 1);
        let mut memo = UnmergeablePairs::default();
        memo.insert(&index, pair);

        let other_generations = [
            LogicalIndex { table: TableRef::new("public", "other"), ..index.clone() },
            LogicalIndex { id: "otherf16".into(), ..index.clone() },
            LogicalIndex { version: 2, ..index.clone() },
        ];
        for other in &other_generations {
            assert!(!memo.touch(other, pair));
            memo.insert(other, pair);
        }

        memo.retain_active(partition.value().trie_catalog());
        assert_eq!(memo.len(), 1);
        assert!(memo.touch(&index, pair));
    }

    #[test]
    fn unmergeable_pairs_are_bounded_across_generations() {
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let second_index = LogicalIndex { version: 2, ..index.clone() };
        let pair = |value: usize| {
            let mut right = [0; 32];
            right[..8].copy_from_slice(&(value as u64).to_be_bytes());
            [[0; 32], right]
        };
        let mut memo = UnmergeablePairs::default();
        for value in 1..=MAX_UNMERGEABLE_PAIRS {
            memo.insert(&index, pair(value));
            assert!(memo.len() <= MAX_UNMERGEABLE_PAIRS);
        }

        memo.insert(&second_index, pair(0));

        assert_eq!(memo.len(), MAX_UNMERGEABLE_PAIRS);
        assert!(memo.touch(&second_index, pair(0)));
        assert!(!memo.touch(&index, pair(MAX_UNMERGEABLE_PAIRS)));
        memo.insert(&index, pair(MAX_UNMERGEABLE_PAIRS));
        assert_eq!(memo.len(), MAX_UNMERGEABLE_PAIRS);
        assert!(!memo.touch(&second_index, pair(0)));
        assert!(memo.touch(&index, pair(MAX_UNMERGEABLE_PAIRS)));
        assert!(memo.touch(&index, pair(1)));
    }

    #[derive(Default)]
    struct PublicationRequired(MemoryStore);

    #[async_trait::async_trait]
    impl ObjectStore for PublicationRequired {
        fn requires_search_publication(&self) -> bool { true }
        async fn search_staging_owner(&self) -> Result<Option<rtdb_format::search_manifest::SearchStagingOwner>> { self.0.search_staging_owner().await }
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.0.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.0.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> { self.0.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.0.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, file: &Path) -> rtdb_storage::Result<()> { self.0.put_file(key, file).await }
        async fn list(&self, prefix: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.0.list(prefix).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.0.delete(key).await }
    }

    async fn upload_for_adoption(store: Arc<dyn ObjectStore>, index: &LogicalIndex, slug: &TableSlug) -> Result<SearchRun> {
        let cache = Arc::new(BufferPool::new(store.clone(), 1 << 20));
        let memory = MemoryBudget::new(64 << 20);
        let builder = SearchBuilder::new("xtdb".into(), store, cache, memory.clone(), Arc::new(|| Timestamp(0).into()),
            crate::SearchRuntime::with_logical_search(1 << 20));
        let document = LogicalDocument { iid: Iid([1; 16]), sql_id: None, field: 0, revision: text_revision(rtdb_text::Analyzer::Simple, "body"),
            tokens: rtdb_text::Analyzer::Simple.analyze("body"), position: (Timestamp(1), None, 1) };
        let contents = RunContents { documents: vec![document], erases: Erases::new() };
        builder.store_documents(index, &["body".into()], slug, vec![0], 0, 1, contents,
            memory.reserve(32 << 20).unwrap(), None, &CancellationToken::new()).await.map(|prepared| prepared.run)
    }

    #[tokio::test]
    async fn adoption_fails_when_a_required_provider_publication_is_missing() {
        use rtdb_format::search_manifest::{pending_prefix, run_prefix};
        let store = Arc::new(PublicationRequired::default());
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "bodyf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let slug = TableSlug::for_unrecorded_table(&index.table).unwrap();
        assert_eq!(upload_for_adoption(store.clone(), &index, &slug).await.unwrap_err().message.as_deref(),
            Some("Search upload returned no immutable provider publication"));
        assert_eq!(store.list(Some(&ObjectKey::new(pending_prefix(&slug)).unwrap())).await.unwrap().len(), 1);
        assert_eq!(store.list(Some(&ObjectKey::new(run_prefix(&slug)).unwrap())).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn adoption_accepts_a_native_local_publication_without_a_provider_version() {
        let files = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalStore::open(files.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "bodyf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let slug = TableSlug::for_unrecorded_table(&index.table).unwrap();
        let run = upload_for_adoption(store.clone(), &index, &slug).await.unwrap();
        assert!(run.publication.is_none());
        assert!(store.head(&ObjectKey::new(run.object_key(&slug).unwrap()).unwrap()).await.is_ok());

        let record = SearchRunChange { run: Some(run.clone()), ..Default::default() }.to_artefact().unwrap();
        let mut trie = rtdb_index::CatalogTrie::notification(TrieKey::l0(BlockIndex(0)).unwrap(), 1, None).unwrap();
        trie.artefacts.push(record);
        let catalog = rtdb_index::TrieCatalog::default().with_tries(index.table.clone(), [trie], Timestamp(1)).unwrap();
        assert_eq!(catalog.search_runs(&index.table).next(), Some(&run));
    }

    use object_store::{ClientOptions, StaticCredentialProvider, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
        client::{HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpService},
        gcp::{GcpCredential, GoogleCloudStorageBuilder}};
    use rtdb_format::search_manifest::{SearchStagingOwner, StagedSearchRun};
    use rtdb_storage::{disk_cache::DiskCache, remote::RemoteStore};

    #[derive(Clone, Copy, Debug)]
    struct NoProviderRequests;

    impl HttpConnector for NoProviderRequests {
        fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> { Ok(HttpClient::new(*self)) }
    }

    #[async_trait::async_trait]
    impl HttpService for NoProviderRequests {
        async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
            panic!("Unsupported search staging sent a provider request: {}", request.method());
        }
    }

    fn builder(store: Arc<dyn ObjectStore>) -> SearchBuilder {
        let budget = rtdb_types::MemoryBudget::new(64 << 20);
        SearchBuilder::new("xtdb".into(), store.clone(), Arc::new(BufferPool::new(store, 1 << 20)), budget,
            Arc::new(|| Timestamp(0).into()), crate::SearchRuntime::with_logical_search(1 << 20))
    }

    async fn write_run(builder: &SearchBuilder, slug: &TableSlug) -> Result<SearchRun> {
        let index = LogicalIndex { table: TableRef::new("public", "docs"), id: "docsf16".into(), version: 1, analyzer: rtdb_text::Analyzer::Simple };
        let document = LogicalDocument { iid: Iid([1; 16]), field: 0, revision: text_revision(rtdb_text::Analyzer::Simple, "test"),
            sql_id: Some(1), tokens: rtdb_text::Analyzer::Simple.analyze("test"), position: (Timestamp(1), None, 1) };
        let contents = RunContents { documents: vec![document], ..Default::default() };
        builder.store_documents(&index, &["body".into()], slug, vec![0], 0, 1, contents,
            builder.memory.reserve(indexing_allowance(0))?, None, &CancellationToken::new()).await.map(|prepared| prepared.run)
    }

    #[tokio::test]
    async fn logical_search_staging_refuses_classic_cloud_stores_before_provider_io() {
        let provider = NoProviderRequests;
        let clients: [(&str, Arc<dyn object_store::ObjectStore>); 3] = [
            ("S3", Arc::new(AmazonS3Builder::new().with_bucket_name("search-fixture").with_region("us-east-1")
                .with_skip_signature(true).with_http_connector(provider).build().unwrap())),
            ("GCS", Arc::new(GoogleCloudStorageBuilder::new().with_bucket_name("search-fixture")
                .with_credentials(Arc::new(StaticCredentialProvider::new(GcpCredential { bearer: String::new() })))
                .with_http_connector(provider).build().unwrap())),
            ("Azure", Arc::new(MicrosoftAzureBuilder::new().with_account("searchfixture").with_container_name("search-fixture")
                .with_skip_signature(true).with_http_connector(provider).build().unwrap())),
        ];
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = DiskCache::open(cache_dir.path(), 1 << 20, rtdb_types::MemoryBudget::new(1 << 20)).unwrap();
        let slug = TableSlug::recorded("public$docs").unwrap();
        for (backend, client) in clients {
            let prefix = ObjectKey::new("v06").unwrap();
            let remote = RemoteStore::new_in(client, Some(prefix), IoRegion::new()).with_catalog_ledger().unwrap();
            let store = cache.scope(ObjectKey::new(backend).unwrap(), Arc::new(remote)).unwrap();
            let builder = builder(store);
            assert!(!builder.can_stage_logical().await.unwrap(), "{backend}");
            assert_eq!(write_run(&builder, &slug).await.unwrap_err().code, "rtdb.storage/search-staging-unsupported", "{backend}");
        }
    }

    #[tokio::test]
    async fn logical_search_staging_retains_local_and_volatile_writers() {
        let directory = tempfile::tempdir().unwrap();
        let local = LocalStore::open(directory.path()).unwrap();
        assert!(!builder(Arc::new(local)).can_stage_logical().await.unwrap());
        let local = LocalStore::open(directory.path()).unwrap().with_catalog_ledger(StorageEpoch(0));
        let local = builder(Arc::new(local));
        let memory = builder(Arc::new(MemoryStore::default()));
        let slug = TableSlug::recorded("public$docs").unwrap();
        for (builder, durable) in [(local, true), (memory, false)] {
            assert!(builder.can_stage_logical().await.unwrap());
            let run = write_run(&builder, &slug).await.unwrap();
            let marker = ObjectKey::new(run.pending_key(&slug).unwrap()).unwrap();
            let staged = StagedSearchRun::decode(&builder.store.get(&marker).await.unwrap()).unwrap();
            if durable {
                let Some(owner @ SearchStagingOwner::Local { .. }) = staged.owner else { panic!("missing local witness"); };
                assert!(!builder.store.search_staging_drained(owner).await.unwrap());
            } else {
                assert_eq!(staged.owner, None);
            }
            let body = ObjectKey::new(run.object_key(&slug).unwrap()).unwrap();
            assert_eq!(builder.store.head(&body).await.unwrap().size, run.file_size() as u64);
            let bytes = builder.store.get(&body).await.unwrap();
            let digest: [u8; 32] = Sha256::digest(bytes.as_ref()).into();
            assert_eq!(run.identity().unwrap(), digest);
        }
    }

    #[test]
    fn arrow_build_forward_witnesses_keep_the_standalone_bytes() -> Result<()> {
        use rtdb_format::{data::{EventBatch, StoredEvent, StoredOperation, build_event_batch}, entity_history::HistoryWriter};
        use rtdb_types::{EventOrigin, LogicalType, ScalarType, StructFields, Value};

        let memory = rtdb_columnar::MemoryBudget::new(8 << 20);
        let columns = ["body".to_owned(), "absent".to_owned()];
        let text = "AΣ İ audit audit";
        let rows = [StoredEvent { position: None, origin: Some(EventOrigin::at([7; 16], 1, BlockIndex(0))), commit_position: 0,
            entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(2), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([("body".into(), Value::Utf8(text.into()))])) }];
        let fields = StructFields::from([("body".into(), LogicalType::Scalar(ScalarType::Utf8))]);
        let batch = build_event_batch(&rows, Some(fields), None)?;
        let events = EventBatch::new(batch.clone())?;
        let page = ArrowSearchPage { events: EventBatch::new(batch.clone())?, analyzer: rtdb_text::Analyzer::Simple,
            _owned: rtdb_columnar::OwnedBatch::new(batch, &memory)?, with_identity: false,
            uuid_iids: None, seal: None, mode: BuildMode::Postings };
        let owner = [9; 32];
        let forward = RefCell::new(Some(ForwardOutput::new(owner, &memory)?));
        let mut posting = PostingPage { page, number: 3, columns: columns.len(), forward: &forward,
            revisions: Vec::new(), _scratch: Some(memory.reserve(4096)?) };
        let indexed = posting.row(0, &columns)?;
        assert_eq!(indexed.revisions, [Some(revision_of(rtdb_text::Analyzer::Simple, indexed.values[0].as_ref().unwrap())), None]);
        posting.complete();
        let merged = forward.into_inner().unwrap().finish()?;

        let mut previous = HistoryWriter::new(Vec::new(), owner, &memory)?;
        previous.append_forward(3, &events, &columns, |text| Ok(match text {
            rtdb_format::data::PutText::Utf8(text) => Some(revision_of(rtdb_text::Analyzer::Simple, &rtdb_text::Analyzer::Simple.analyze(text))),
            _ => None,
        }))?;
        let (previous, root) = previous.finish()?;
        assert_eq!(merged.root_bytes(), root.encode());
        assert_eq!(std::fs::read(merged.path()).map_err(fault)?, previous);
        Ok(())
    }

    #[test]
    fn an_erase_scrubs_older_text_and_keeps_later_reuse_at_the_same_system_time() {
        let iid = Iid([1; 16]);
        let document = |text: &str, position| LogicalDocument {
            iid, sql_id: None, field: 0, revision: text_revision(rtdb_text::Analyzer::Simple, text), tokens: rtdb_text::Analyzer::Simple.analyze(text), position
        };
        let mut documents = vec![document("shared text", (Timestamp(1), None, 0)),
            document("old text", (Timestamp(2), None, 2)), document("shared text", (Timestamp(2), None, 4))];
        assert!(scrub_documents(&mut documents, &Erases::from([(iid, (Timestamp(2), None, 3))])));
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].revision, text_revision(rtdb_text::Analyzer::Simple, "shared text"));
        assert_eq!(documents[0].position, (Timestamp(2), None, 4));
    }
}
