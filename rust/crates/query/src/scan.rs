use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_builder_bytes, from_field};
use rtdb_format::{ipc::{ArrowFile, inspect_file_reserved}, metadata::{MetadataFile, MetadataNode}};
use rtdb_index::{CatalogTrie, IndexBatch, LiveTable, LocatedRows, RowSource, Snapshot, TrieState};
use rtdb_plan::{Expr, Scan, Schema, TemporalFilter, UnaryOp};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_temporal::{HistoryResolver, ResolvedRow};
use rtdb_trie::{PageSummary, PruningOptions, TemporalSummary, select_pages, select_pages_with_scratch};
use rtdb_types::{Anomaly, EventHeader, Iid, Interval, Result, TemporalBounds, Timestamp, Value};

use crate::{context::Execution, pages::{OwnedPage,PageBuilder,PageCursor}, scan_tasks::{PageRange,ScanTask,ScanTaskPlan}, invalid};

/// A data file read whole from the buffer pool, or, where the pool no longer holds it but its layout is known, a page at a time.
enum DataState { Unopened, Open { reader: ArrowFile, _reservation: Arc<MemoryReservation> }, Ranged { reader: ArrowFile, file: rtdb_format::ipc::SparseFile, _reservation: Arc<MemoryReservation> } }
struct DataFile { storage: Arc<BufferPool>, key: ObjectKey, state: DataState }

#[derive(Default)]
struct FileDescriptors {
    entries: Vec<DataFile>,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl FileDescriptors {
    fn admit(&mut self, count: usize, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self._reservation.is_some() { return Ok(()); }
        let bytes = count.checked_mul(std::mem::size_of::<DataFile>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid("scan-file-size", "Scan file descriptor capacity exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        self.entries.try_reserve_exact(count).map_err(|error| invalid("scan-file-size", error))?;
        self._reservation = Some(reservation);
        Ok(())
    }
}

enum PageSource { Live { table: Arc<LiveTable>, rows: Arc<LocatedRows> }, File { file: usize, page: usize } }
struct Page { lower: Iid, upper: Iid, priority: usize, source: PageSource, summary: PageSummary, content_matches: bool }

#[derive(Default)]
struct PageDescriptors {
    entries: Vec<Page>,
    capacity: usize,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl PageDescriptors {
    fn push(&mut self, page: Page, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.entries.len() == self.capacity {
            let size_error = || invalid("scan-page-size", "Scan page descriptor capacity exceeds addressable memory");
            let capacity = self.capacity.checked_mul(2).ok_or_else(size_error)?.max(1);
            let bytes = capacity.checked_mul(std::mem::size_of::<Page>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let reservation = memory.reserve(bytes)?;
            self.entries.try_reserve_exact(capacity - self.entries.len()).map_err(|error| invalid("scan-page-size", error))?;
            self.capacity = capacity;
            self._reservation = Some(reservation);
        }
        self.entries.push(page);
        Ok(())
    }
}

const MAX_IID_DEPTH: usize = 64;

#[derive(Clone, Copy)]
struct MetadataVisit { node: usize, path: [u8; MAX_IID_DEPTH], depth: usize, recency: Timestamp }

#[derive(Default)]
struct MetadataStack {
    entries: Vec<MetadataVisit>,
    capacity: usize,
    _reservation: Option<Arc<MemoryReservation>>,
}

impl MetadataStack {
    fn push(&mut self, visit: MetadataVisit, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.entries.len() == self.capacity {
            let size_error = || invalid("scan-metadata-size", "Scan metadata traversal capacity exceeds addressable memory");
            let capacity = self.capacity.checked_mul(2).ok_or_else(size_error)?.max(1);
            let bytes = capacity.checked_mul(std::mem::size_of::<MetadataVisit>())
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(size_error)?;
            let reservation = memory.reserve(bytes)?;
            self.entries.try_reserve_exact(capacity - self.entries.len()).map_err(|error| invalid("scan-metadata-size", error))?;
            self.capacity = capacity;
            self._reservation = Some(reservation);
        }
        self.entries.push(visit);
        Ok(())
    }
}

fn summary_bytes(count: usize) -> Result<usize> {
    count.checked_mul(std::mem::size_of::<PageSummary>()).and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(|| invalid("scan-summary-size", "Scan summary capacity exceeds addressable memory"))
}

/// A loaded page's rows.
enum ActiveRows { Live { table: Arc<LiveTable>, rows: Arc<LocatedRows> }, File(Arc<IndexBatch>) }
struct LoadedPage { rows: ActiveRows, _reservation: Arc<MemoryReservation>, _charge: Option<Arc<MemoryReservation>> }
impl ActiveRows {
    fn len(&self) -> usize { match self { Self::Live { rows, .. } => rows.located.len(), Self::File(batch) => batch.len() } }
    fn row(&self, index: usize) -> Result<RowSource> {
        match self { Self::Live { table, rows } => table.row_at(rows.located[index]), Self::File(batch) => Ok(RowSource { batch: Arc::clone(batch), row: index }) }
    }
    /// The batch holding row `index`, the slot that names it within this page, and the row's position in it.
    fn batch(&self, index: usize) -> Result<(usize, &Arc<IndexBatch>, u32)> {
        match self {
            Self::Live { table, rows } => { let at = rows.located[index]; let (batch, row) = table.batch_at(at)?; Ok((at.0 as usize, batch, row)) }
            Self::File(batch) => Ok((0, batch, u32::try_from(index).map_err(|_| invalid("page-size", "A page row exceeds u32 addressing"))?)),
        }
    }
    fn header(&self, index: usize) -> Result<EventHeader> {
        match self { Self::Live { rows, .. } => Ok(rows.headers[index]), Self::File(batch) => batch.header(index) }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Head { header: EventHeader, active: usize, priority: usize, ordinal: usize }
impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        other.header.iid.cmp(&self.header.iid).then_with(|| self.header.system_from.cmp(&other.header.system_from))
            .then_with(|| other.priority.cmp(&self.priority)).then_with(|| other.ordinal.cmp(&self.ordinal))
            .then_with(|| other.active.cmp(&self.active))
    }
}
impl PartialOrd for Head { fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) } }

struct PartitionScan {
    snapshot: Snapshot,
    storage: Arc<BufferPool>,
    scan: Scan,
    execution: Execution,
    bounds: TemporalBounds,
    initialized: bool,
    files: FileDescriptors,
    pages: PageDescriptors,
    tasks: Option<ScanTaskPlan>,
    loaded: BTreeMap<usize, LoadedPage>,
    output: VecDeque<crate::columnar::ColumnBatch>,
    output_reservation: Option<Arc<MemoryReservation>>,
    counts: ScanCounts,
    pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
    canonical_entity_iids: bool,
}

use crate::analysis::ScanCounts;

/// The IIDs an `_id` predicate selects, as the reference's scan narrows to them: `_id = v`, or an OR of such, with every
/// `v` a literal or parameter of an ID type. `None` where some `v` can't be proven to name one IID — `_id = 7.0` still
/// matches the integer ID 7 — so the scan reads every IID; the predicate itself is applied either way.
fn id_selection(scan: &Scan, parameters: &[Value]) -> Option<std::collections::BTreeSet<Iid>> {
    let id = scan.columns.iter().find(|field| field.name == "_id")?.id;
    let eid = |expr: &Expr| -> Option<Iid> {
        let value = match expr { Expr::Literal(value) => value, Expr::Parameter { index, .. } => parameters.get(*index)?, _ => return None };
        value.entity_id().ok().map(|entity| entity.iid())
    };
    let equality = |expr: &Expr| -> Option<Iid> {
        let Expr::Binary { op: rtdb_plan::BinaryOp::Eq, left, right } = expr else { return None };
        match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id: column, .. }, value) | (value, Expr::Column { id: column, .. }) if *column == id => eid(value),
            _ => None,
        }
    };
    fn disjuncts<'e>(expr: &'e Expr, out: &mut Vec<&'e Expr>) {
        match expr { Expr::Binary { op: rtdb_plan::BinaryOp::Or, left, right } => { disjuncts(left, out); disjuncts(right, out); } other => out.push(other) }
    }
    scan.predicates.iter().filter(|predicate| predicate.referenced_columns() == std::collections::BTreeSet::from([id])).find_map(|predicate| {
        let mut clauses = Vec::new();
        disjuncts(predicate, &mut clauses);
        clauses.into_iter().map(equality).collect::<Option<std::collections::BTreeSet<_>>>()
    })
}
pub(crate) struct ScanCursor { partitions: VecDeque<PartitionScan>, completed: ScanCounts, database: String, source: String, memory: Arc<MemoryBudget>, _reservation: Arc<MemoryReservation> }
impl ScanCursor {
    pub async fn new(mut scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> Result<Self> {
        if scan.iids.is_none() { scan.iids = id_selection(&scan, execution.parameters.values()); }
        let bytes = scan.table.name.database.len().checked_add(scan.table.name.table.schema.len()).and_then(|bytes| bytes.checked_add(scan.table.name.table.table.len())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("scan-size", "Scan labels exceed allocation range"))?;
        let reservation = execution.context.memory.reserve(bytes)?;
        let database = execution.context.catalog.database(&scan.table.name.database)?;
        let canonical_entity_iids = scan.iids.is_none()
            && pushdowns.as_ref().is_some_and(|filters| filters.needs_entity_iid_proof(&scan.columns))
            && database.partitions.iter().all(|snapshot| snapshot.has_canonical_entity_iids(&scan.table.name.table));
        if let (Some(analysis), Some(pushdowns)) = (&execution.analysis, &pushdowns) {
            analysis.set_pushdowns(pushdowns, &scan.columns, canonical_entity_iids, &execution.context.memory)?;
        }
        let name = scan.table.name.database.clone();
        let source = format!("{}.{}", scan.table.name.table.schema, scan.table.name.table.table);
        let mut partitions = VecDeque::new();
        for snapshot in &database.partitions {
            let bounds = bounds(&scan, snapshot, &execution).await?;
            partitions.push_back(PartitionScan { snapshot: snapshot.clone(), storage: Arc::clone(&database.storage), scan: scan.clone(), execution: execution.clone(), bounds,
                initialized: false, files: FileDescriptors::default(), pages: PageDescriptors::default(), tasks: None, loaded: BTreeMap::new(),
                output: VecDeque::new(), output_reservation: None, counts: ScanCounts::default(), pushdowns: pushdowns.clone(), canonical_entity_iids });
        }
        Ok(Self { partitions, completed: ScanCounts::default(), database: name, source, memory: Arc::clone(&execution.context.memory), _reservation: reservation })
    }
}

impl ScanCursor {
    pub(crate) async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        while let Some(partition) = self.partitions.front_mut() {
            if let Some(batch) = partition.next_batch().await? { return Ok(Some(batch)); }
            self.completed.add(partition.counts);
            self.partitions.pop_front();
        }
        Ok(None)
    }
}

#[async_trait]
impl PageCursor for ScanCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(batch) = self.next_batch().await? else { return Ok(None) };
        batch.rows(&self.memory).map(Some)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) {
        let mut counts = self.completed; for partition in &self.partitions { counts.add(partition.counts); }
        span.attribute("scan_db", rtdb_plan::TraceValue::Text(&self.database));
        span.attribute("scan_source", rtdb_plan::TraceValue::Text(&self.source));
        for (name, value) in [("scan_files_pruned", counts.files_pruned), ("scan_files_used", counts.files_used), ("scan_pages_pruned", counts.pages_pruned), ("scan_pages_used", counts.pages_used), ("scan_rows_read", counts.rows_read)] { span.attribute(name, rtdb_plan::TraceValue::Unsigned(value)); }
    }
    fn scan_counts(&self) -> Option<ScanCounts> {
        let mut counts = self.completed; for partition in &self.partitions { counts.add(partition.counts); }
        Some(counts)
    }
}

impl PartitionScan {
    fn pruning(&self) -> PruningOptions {
        PruningOptions { bounds: self.bounds, projects_temporal_columns: self.scan.columns.iter().any(|field| matches!(field.name.as_str(), "_valid_from" | "_valid_to" | "_system_from" | "_system_to")),
            clamp_valid_time: self.scan.clamp_valid_time, bypass_temporal_pruning: self.execution.context.options.bypass_temporal_pruning,
            iid: self.scan.iids.as_ref().filter(|iids| iids.len() == 1).and_then(|iids| iids.first().copied()) }
    }
    fn path_matches(&self, lower: Iid, upper: Iid) -> bool {
        match &self.scan.iids {
            Some(iids) => iids.range(lower..=upper).next().is_some(),
            None => self.pushdowns.as_ref().is_none_or(|filters| filters.path_matches(&self.scan.columns, lower, upper, self.canonical_entity_iids)),
        }
    }
    async fn initialize(&mut self) -> Result<()> {
        self.execution.context.check_cancelled()?;
        if self.scan.iids.as_ref().is_some_and(|iids| iids.is_empty()) || self.bounds.valid_time.is_empty() || self.bounds.system_time.is_empty() {
            self.initialized = true;
            return Ok(());
        }
        let page_test = crate::page_metadata::PageTest::for_scan(&self.scan, &self.execution).await?;
        let table = &self.scan.table.name.table;
        let catalog = self.snapshot.trie_catalog();
        let current_tries = || catalog.iter_tries(table).filter(|trie| trie.state == TrieState::Live);
        let trie_count = current_tries().count();
        let candidate_reservation = if trie_count == 0 { None } else {
            let bytes = trie_count.checked_mul(std::mem::size_of::<&CatalogTrie>()).and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(|| invalid("scan-candidate-size", "Scan trie candidate capacity exceeds addressable memory"))?;
            Some(self.execution.context.memory.reserve(bytes)?)
        };
        let mut tries = Vec::new();
        tries.try_reserve_exact(trie_count).map_err(|error| invalid("scan-candidate-size", error))?;
        tries.extend(current_tries());
        tries.sort_unstable_by(|left, right| right.key.block_index().cmp(&left.key.block_index()).then_with(|| left.key.cmp(&right.key)));
        let memory = &self.execution.context.memory;
        let mut summary_reservation = if tries.is_empty() { None } else { Some(memory.reserve(summary_bytes(tries.len())?)?) };
        let mut combined = Vec::new();
        combined.try_reserve_exact(tries.len()).map_err(|error| invalid("scan-summary-size", error))?;
        for (index, trie) in tries.iter().enumerate() {
            let temporal = trie.metadata.as_ref().and_then(|metadata| metadata.temporal_metadata.as_ref()).map(|metadata| TemporalSummary {
                min_valid_from: Timestamp(metadata.min_valid_from.unwrap_or(i64::MIN)), max_valid_to: Timestamp(metadata.max_valid_to.unwrap_or(i64::MAX)),
                min_system_from: Timestamp(metadata.min_system_from.unwrap_or(i64::MIN)), max_system_from: Timestamp(metadata.max_system_from.unwrap_or(i64::MAX)),
            });
            combined.push(PageSummary { page_index: u32::try_from(index).map_err(|error| invalid("file-count", error))?, temporal,
                recency: Some(recency(trie.key.recency())?), iid_range: Some(path_range(trie.key.part())?) });
        }
        let slug = self.snapshot.table_entry(table).map(|entry| entry.slug.clone());
        let mut pages = PageDescriptors::default();
        let mut live_segment_count = 0;
        for segment in self.snapshot.iter_segments(table) {
            let priority = live_segment_count;
            live_segment_count += 1;
            segment.trie().try_visit_leaves(|leaf| {
                let (lower, upper) = path_range(leaf.path().as_slice())?;
                if !self.path_matches(lower, upper) { return Ok(()); }
                let rows = segment.located_rows(leaf)?;
                let temporal = summarize_headers(rows.headers.iter().copied());
                pages.push(Page { lower, upper, priority, source: PageSource::Live { table: Arc::clone(segment), rows }, content_matches: true,
                    summary: PageSummary { page_index: 0, temporal, recency: Some(Timestamp::MAX), iid_range: Some((lower, upper)) } }, &self.execution.context.memory)
            })?;
        }
        let live_count = pages.entries.len();
        if live_count != 0 {
            let count = tries.len().checked_add(live_count).ok_or_else(|| invalid("page-count", "Candidate count overflow"))?;
            let reservation = memory.reserve(summary_bytes(count)?)?;
            combined.try_reserve_exact(live_count).map_err(|error| invalid("scan-summary-size", error))?;
            summary_reservation = Some(reservation);
        }
        for (index, page) in pages.entries.iter().enumerate() {
            let mut summary = page.summary.clone();
            summary.page_index = u32::try_from(tries.len().checked_add(index).ok_or_else(|| invalid("page-count", "Candidate count overflow"))?).map_err(|error| invalid("page-count", error))?;
            combined.push(summary);
        }
        let (selected, selected_reservation) = {
            let count = combined.len();
            let _selector_reservation = if count == 0 { None } else {
                let bytes = count.checked_mul(std::mem::size_of::<&PageSummary>()).and_then(|bytes| bytes.checked_add(256))
                    .and_then(|bytes| bytes.checked_mul(3))
                    .ok_or_else(|| invalid("scan-selector-size", "Scan selector capacity exceeds addressable memory"))?;
                Some(memory.reserve(bytes)?)
            };
            let references = || -> Result<_> {
                let mut entries = Vec::new();
                entries.try_reserve_exact(count).map_err(|error| invalid("scan-selector-size", error))?;
                Ok(entries)
            };
            let mut eligible = references()?;
            let mut emitted = references()?;
            let mut dependent = references()?;
            select_pages_with_scratch(&combined, self.pruning(), |_| true, &mut eligible, &mut emitted, &mut dependent);
            emitted.retain(|summary| summary.iid_range.is_none_or(|(lower, upper)| self.path_matches(lower, upper))
                && (summary.page_index as usize) < tries.len());
            let count = emitted.len();
            let selected_reservation = if count == 0 { None } else {
                let bytes = count.checked_mul(std::mem::size_of::<usize>()).and_then(|bytes| bytes.checked_add(256))
                    .ok_or_else(|| invalid("scan-selected-file-size", "Selected scan index capacity exceeds addressable memory"))?;
                Some(memory.reserve(bytes)?)
            };
            let mut selected = Vec::new();
            selected.try_reserve_exact(count).map_err(|error| invalid("scan-selected-file-size", error))?;
            selected.extend(emitted.iter().map(|summary| summary.page_index as usize));
            (selected, selected_reservation)
        };
        drop(combined);
        drop(summary_reservation);
        let file_count = selected.len();
        self.counts.files_pruned = (tries.len() - file_count) as u64;
        self.counts.files_used = file_count as u64;
        let mut files = FileDescriptors::default();
        for index in selected {
            let trie = &tries[index];
            let (lower, upper) = path_range(trie.key.part())?;
            if !self.path_matches(lower, upper) { continue; }
            let slug = slug.as_ref().ok_or_else(|| invalid("missing-table-slug", "Persisted trie has no recorded table registry entry"))?;
            let key = ObjectKey::new(slug.metadata_key(&trie.key))?;
            let (metadata, _reservation) = match metadata_cache::get(&self.storage, &key) {
                Some((metadata, bound)) => (metadata, self.execution.context.memory.reserve(bound)?),
                None => {
                    let lease = self.storage.get(&key).await?;
                    let mut cursor = Cursor::new(lease);
                    let layout = inspect_file_reserved(&mut cursor, |bytes| self.execution.context.memory.reserve(bytes))?;
                    let bound = layout.full_allocation_bound()?;
                    let reservation = self.execution.context.memory.reserve(bound)?;
                    let metadata = Arc::new(MetadataFile::read_reader(cursor)?);
                    metadata_cache::insert(&self.storage, &key, Arc::clone(&metadata), bound, metadata.stored_bytes());
                    (metadata, reservation)
                }
            };
            files.admit(file_count, &self.execution.context.memory)?;
            let file = files.entries.len();
            files.entries.push(DataFile { storage: Arc::clone(&self.storage), key: ObjectKey::new(slug.data_key(&trie.key))?, state: DataState::Unopened });
            let trie_recency = recency(trie.key.recency())?;
            let memory = &self.execution.context.memory;
            let mut pending = MetadataStack::default();
            if let Some(node) = metadata.root_index() {
                pending.push(MetadataVisit { node, path: [0; MAX_IID_DEPTH], depth: 0, recency: trie_recency }, memory)?;
            }
            while let Some(visit) = pending.entries.pop() {
                self.execution.context.check_cancelled()?;
                let path = visit.path.get(..visit.depth).ok_or_else(invalid_iid_path)?;
                let (lower, upper) = path_range(path)?;
                if !self.path_matches(lower, upper) { continue; }
                match &metadata.nodes()[visit.node] {
                    MetadataNode::Nil => {}
                    MetadataNode::IidBranch(children) => for (bucket, child) in children.iter().enumerate().rev() {
                        if let Some(node) = child {
                            let mut next = MetadataVisit { node: *node, depth: visit.depth + 1, ..visit };
                            if let Some(slot) = next.path.get_mut(visit.depth) { *slot = bucket as u8; }
                            pending.push(next, memory)?;
                        }
                    },
                    MetadataNode::RecencyBranch(children) => for (upper, node) in children.iter().rev() {
                        pending.push(MetadataVisit { node: *node, recency: visit.recency.min(*upper), ..visit }, memory)?;
                    },
                    MetadataNode::Leaf { data_page_index, columns } => { let columns = &*columns.values()?; pages.push(Page { lower, upper, priority: live_segment_count + index,
                        content_matches: self.pushdowns.as_ref().map(|filters| filters.page_matches(&self.scan.columns, columns, &self.execution.context.memory)).transpose()?.unwrap_or(true)
                            && may_satisfy_not_null(&self.scan, columns) && page_test.matches(columns)?,
                        source: PageSource::File { file, page: *data_page_index }, summary: PageSummary { page_index: 0, temporal: page_temporal(columns),
                            recency: Some(visit.recency), iid_range: Some((lower, upper)) } }, &self.execution.context.memory)? }
                }
            }
        }
        drop(selected_reservation);
        drop(tries);
        drop(candidate_reservation);
        for (index, page) in pages.entries.iter_mut().enumerate() { page.summary.page_index = u32::try_from(index).map_err(|error| invalid("page-count", error))?; }
        self.tasks = Some(ScanTaskPlan::new(pages.entries.iter().map(|page|PageRange { lower: page.lower,upper: page.upper }),&self.execution.context.memory)?);
        self.files = files;
        self.pages = pages;
        self.initialized = true;
        Ok(())
    }
    async fn load(&mut self, index: usize) -> Result<()> {
        self.execution.context.check_cancelled()?;
        if self.loaded.contains_key(&index) { return Ok(()); }
        let reservation = self.execution.context.memory.reserve(std::mem::size_of::<LoadedPage>() + 256)?;
        let page = &self.pages.entries[index];
        let mut charge = None;
        let mut cached = false;
        let mut uncached: Option<(Arc<IndexBatch>, usize)> = None;
        let hit = match &page.source {
            PageSource::File { file, page: page_index } => metadata_cache::page(&self.files.entries[*file].storage, &self.files.entries[*file].key, *page_index),
            PageSource::Live { .. } => None,
        };
        let rows = match (&page.source, hit) {
            (PageSource::Live { table, rows }, _) => ActiveRows::Live { table: Arc::clone(table), rows: Arc::clone(rows) },
            (PageSource::File { .. }, Some((batch, bound))) => {
                charge = Some(self.execution.context.memory.reserve(bound)?);
                cached = true;
                ActiveRows::File(batch)
            }
            (PageSource::File { file, page: page_index }, None) => {
                let data = &mut self.files.entries[*file];
                if matches!(&data.state, DataState::Unopened) && let Some(layout) = metadata_cache::layout(&data.storage, &data.key) && !data.storage.holds(&data.key) {
                    let length = data.storage.object_store().head(&data.key).await?.size;
                    let footer = length.checked_sub(u64::try_from(layout.footer_encoded_bytes).map_err(|_| invalid("file-size", "Arrow footer exceeds its file"))?)
                        .ok_or_else(|| invalid("file-size", "Arrow footer exceeds its file"))?;
                    let file = rtdb_format::ipc::SparseFile::new(length);
                    file.add(footer, Arc::new(data.storage.get_range(&data.key, footer..length).await?));
                    let reservation = self.execution.context.memory.reserve(layout.opening_allocation_bound()?)?;
                    data.state = DataState::Ranged { reader: ArrowFile::from_inspected_reader(file.clone(), None, layout)?, file, _reservation: reservation };
                }
                if matches!(&data.state, DataState::Unopened) {
                    let lease = data.storage.get(&data.key).await?;
                    let mut cursor = Cursor::new(lease);
                    let layout = match metadata_cache::layout(&data.storage, &data.key) {
                        Some(layout) => layout,
                        None => {
                            let layout = inspect_file_reserved(&mut cursor, |bytes| self.execution.context.memory.reserve(bytes))?;
                            metadata_cache::insert_layout(&data.storage, &data.key, layout.clone());
                            layout
                        }
                    };
                    let reservation = self.execution.context.memory.reserve(layout.opening_allocation_bound()?)?;
                    data.state = DataState::Open { reader: ArrowFile::from_inspected_reader(cursor, None, layout)?, _reservation: reservation };
                }
                let reader = match &mut data.state {
                    DataState::Open { reader, .. } => reader,
                    DataState::Ranged { reader, file, .. } => {
                        let range = reader.page_range(*page_index)?;
                        let bytes = data.storage.get_range(&data.key, range.clone()).await?;
                        file.retain_from(0);
                        file.add(range.start, Arc::new(bytes));
                        reader
                    }
                    DataState::Unopened => return Err(invalid("file-state", "Data file failed to open")),
                };
                let mut bound = reader.page_allocation_bound(*page_index)?;
                for field in reader.schema().fields() {
                    bound = bound.checked_add(estimate_builder_bytes(&from_field(field)?, &[])?).ok_or_else(|| invalid("page-size", "Arrow schema allocation bound overflow"))?;
                }
                let charged = self.execution.context.memory.reserve(bound)?;
                let page = reader.read_page(*page_index)?;
                let headers = IndexBatch::header_bytes(page.num_rows())?;
                let batch = match metadata_cache::page_reservation(bound, headers) {
                    Some((reservation, headers)) => {
                        let batch = IndexBatch::from_owned_with_headers(OwnedBatch::with_reservation(page, reservation)?, headers)?;
                        uncached = Some((Arc::clone(&batch), bound));
                        charge = Some(charged);
                        batch
                    }
                    None => IndexBatch::from_owned(OwnedBatch::with_reservation(page, charged)?)?,
                };
                ActiveRows::File(batch)
            }
        };
        let mut previous: Option<EventHeader> = None;
        // A live page is a slice of this process's own trie, ordered as it was built; a file page is checked when first decoded.
        let validated = if matches!(rows, ActiveRows::Live { .. }) || cached { 0 } else { rows.len() };
        for index in 0..validated {
            let source = rows.row(index)?;
            let header = source.header()?;
            if header.iid < page.lower || header.iid > page.upper { return Err(invalid("page-path", "Data page contains an IID outside its metadata path")); }
            if previous.is_some_and(|previous| previous.iid > header.iid || (previous.iid == header.iid && previous.system_from < header.system_from)) {
                return Err(invalid("page-order", "Data page is not in IID and descending system-time order"));
            }
            previous = Some(header);
        }
        if let (Some((batch, bound)), PageSource::File { file, page: page_index }) = (uncached, &page.source) {
            let data = &self.files.entries[*file];
            metadata_cache::insert_page(&data.storage, &data.key, *page_index, batch, bound);
        }
        self.loaded.insert(index,LoadedPage { rows,_reservation: reservation,_charge: charge });
        Ok(())
    }
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        if !self.initialized { self.initialize().await?; }
        loop {
            self.execution.context.check_cancelled()?;
            if let Some(page) = self.output.pop_front() { return Ok(Some(page)); }
            self.output_reservation = None;
            let task = self.tasks.as_mut().map(ScanTaskPlan::next).transpose()?.flatten();
            let Some(task) = task else { self.loaded.clear();return Ok(None); };
            if self.path_matches(task.range.lower,task.range.upper) { self.buffer_task(task).await?; }
        }
    }
    async fn buffer_task(&mut self,task: ScanTask) -> Result<()> {
        let bound = task.pages.len().checked_mul(std::mem::size_of::<PageSummary>() + std::mem::size_of::<Head>() + std::mem::size_of::<PageBuilder>() + std::mem::size_of::<OwnedPage>() + 256).and_then(|bytes|bytes.checked_add(4096)).ok_or_else(||invalid("scan-task-size","Scan task scratch exceeds addressable memory"))?;
        let reservation = self.execution.context.memory.reserve(bound)?;
        let summaries: Vec<_> = task.pages.iter().map(|index|self.pages.entries[*index].summary.clone()).collect();
        let mut selected: Vec<_> = select_pages(&summaries,self.pruning(),|summary|self.pages.entries[summary.page_index as usize].content_matches).into_iter().map(|summary|summary.page_index as usize).collect();
        self.counts.pages_pruned = self.counts.pages_pruned.saturating_add((task.pages.len() - selected.len()) as u64);
        self.counts.pages_used = self.counts.pages_used.saturating_add(selected.len() as u64);
        selected.sort_by_key(|index| (matches!(self.pages.entries[*index].source,PageSource::Live { .. }),self.pages.entries[*index].priority,*index));
        let mut groups = Vec::with_capacity(selected.len());
        let mut group_reservations = Vec::with_capacity(selected.len());
        let mut heads = BinaryHeap::with_capacity(selected.len());
        for (group,index) in selected.iter().copied().enumerate() {
            self.load(index).await?;
            let rows = &self.loaded[&index].rows;
            self.counts.rows_read = self.counts.rows_read.saturating_add(rows.len() as u64);
            groups.push(Vec::new());
            group_reservations.push(self.execution.context.memory.reserve(256)?);
            let (mut low, mut high) = (0, rows.len());
            while low < high {
                let middle = (low + high) / 2;
                if rows.header(middle)?.iid < task.range.lower { low = middle + 1; } else { high = middle; }
            }
            if low < rows.len() {
                let header = rows.header(low)?;
                if header.iid <= task.range.upper { heads.push(Head { header,active: group,priority: self.pages.entries[index].priority,ordinal: low }); }
            }
        }
        let mut resolver = HistoryResolver::new_in(self.bounds, self.scan.clamp_valid_time, Arc::clone(&self.execution.context.memory));
        let mut visited = 0usize;
        let active: Vec<&ActiveRows> = selected.iter().map(|index| &self.loaded[index].rows).collect();
        // Resolved rows name their batch by its place here, so a row costs no handle on its batch.
        let mut batches: Vec<Arc<IndexBatch>> = Vec::new();
        let mut handles: Vec<Vec<u32>> = vec![Vec::new(); active.len()];
        let mut next = heads.pop();
        while let Some(head) = next.take() {
            self.execution.context.check_cancelled_at(visited)?;
            visited += 1;
            let rows = active[head.active];
            let ordinal = head.ordinal + 1;
            let following = if ordinal < rows.len() {
                let header = rows.header(ordinal)?;
                (header.iid <= task.range.upper).then_some(Head { header,ordinal,..head })
            } else { None };
            // A page's next row that still precedes every other page's head is taken without passing through the heap.
            next = match following {
                Some(following) if heads.peek().is_none_or(|top| following > *top) => Some(following),
                Some(following) => { heads.push(following); heads.pop() }
                None => heads.pop(),
            };
            if self.scan.iids.as_ref().is_some_and(|iids| !iids.contains(&head.header.iid)) { continue; }
            if self.scan.iids.is_none() && self.pushdowns.as_ref().is_some_and(|filters| !filters.iid_matches(&self.scan.columns,head.header.iid,self.canonical_entity_iids)) { continue; }
            let (slot, batch, row) = rows.batch(head.ordinal)?;
            let page = &mut handles[head.active];
            if page.len() <= slot { page.resize(slot + 1, u32::MAX); }
            if page[slot] == u32::MAX { batches.push(Arc::clone(batch)); page[slot] = u32::try_from(batches.len() - 1).map_err(|_| invalid("scan-row-size", "A task reads more batches than u32 names"))?; }
            let source = (page[slot], row);
            let group: &mut Vec<_> = &mut groups[head.active];
            let memory = &self.execution.context.memory;
            resolver.resolve_fragments(&head.header, |valid_time, system_time| {
                if group.len() == group.capacity() {
                    let capacity = group.capacity().saturating_mul(2).max(64);
                    let bytes = capacity.checked_mul(std::mem::size_of::<ResolvedRow<(u32, u32)>>()).ok_or_else(|| invalid("scan-row-size", "Scan rows exceed addressable memory"))?;
                    group_reservations.push(memory.reserve(bytes)?);
                    group.reserve_exact(capacity - group.len());
                }
                group.push(ResolvedRow { source, valid_time, system_time });
                Ok(())
            })?;
        }
        self.loaded.retain(|index,_| self.pages.entries[*index].upper > task.range.upper);
        self.output = groups.iter().filter(|rows| !rows.is_empty())
            .map(|rows| crate::columnar::gather(rows, &batches, &self.scan.columns, &self.execution.context.memory)).collect::<Result<_>>()?;
        drop(group_reservations);
        self.output_reservation = Some(reservation);
        Ok(())
    }
}
async fn bounds(scan: &Scan, snapshot: &Snapshot, execution: &Execution) -> Result<TemporalBounds> {
    let valid_time = dimension(&scan.valid_time, execution.context.current_time, execution).await?;
    let basis = snapshot.system_time_cap();
    let system = if basis.is_none() && matches!(scan.system_time, TemporalFilter::Default) { Interval::ALL }
        else { dimension(&scan.system_time, basis.map(Into::into).unwrap_or(execution.context.current_time), execution).await? };
    let cap = basis.map(|basis| basis.0.saturating_add(1)).map(Timestamp).unwrap_or(Timestamp::MAX);
    // A point after the basis reads as the basis: what is known at the basis is all that can be known then.
    let system = match (&scan.system_time, basis) {
        (TemporalFilter::At(_), Some(basis)) if system.lower() > basis => Interval::new(basis, cap)?,
        _ => system,
    };
    let upper = cap.min(system.upper());
    let system_time = Interval::new(system.lower().min(upper), upper)?;
    Ok(TemporalBounds { valid_time, system_time })
}
async fn dimension(filter: &TemporalFilter, default: rtdb_types::Instant, execution: &Execution) -> Result<Interval> {
    let scope = execution.scope()?;
    async fn value(expression: &rtdb_plan::Expr, fallback: rtdb_types::Instant, execution: &Execution, scope: &rtdb_expr::EvaluationScope) -> Result<Timestamp> {
        let value = execution.evaluate(scope, expression, &Schema::default(), &[]).await?;
        if matches!(value, Value::Null | Value::Undefined) { fallback.as_micros() } else { rtdb_expr::timestamp_micros(&value, &execution.context.timezone) }
    }
    let point = |time: Timestamp| Interval::new(time, Timestamp(time.0.saturating_add(1)));
    match filter {
        TemporalFilter::Default => point(default.as_micros()?), TemporalFilter::All => Ok(Interval::ALL),
        TemporalFilter::At(at) => point(value(at, execution.context.current_time, execution, &scope).await?),
        TemporalFilter::FromTo { from, to } => {
            let from = value(from, Timestamp::MIN.into(), execution, &scope).await?;
            Interval::new(from, value(to, Timestamp::MAX.into(), execution, &scope).await?.max(from))
        }
        TemporalFilter::Between { from, to } => {
            let from = value(from, Timestamp::MIN.into(), execution, &scope).await?;
            let to = value(to, Timestamp::MAX.into(), execution, &scope).await?;
            if to < from { Interval::new(from, from) } else { Interval::between(from, to) }
        }
    }
}
/// Decoded trie metadata, shared by every scan of a storage: a trie file never changes once written, and a point
/// lookup otherwise spends most of its time decoding the same files again. A query still admits the decoded size of
/// every file it reads. Bounded by file count and decoded size.
mod metadata_cache {
    use std::{collections::HashMap, sync::{Arc, Mutex, OnceLock, Weak}};
    use rtdb_format::{ipc::FileLayout, metadata::MetadataFile};
    use rtdb_columnar::MemoryBudget;
    use rtdb_index::IndexBatch;
    use rtdb_storage::{ObjectKey, cache::BufferPool};

    const FILES: usize = 1024;
    const BYTES: usize = 256 << 20;
    /// `bound` is what a query admits for the file, as it would to decode it; `bytes` is what the cache retains for it.
    struct Entry { storage: Weak<BufferPool>, metadata: Arc<MetadataFile>, bound: usize, bytes: usize, used: u64 }
    #[derive(Default)]
    struct Cache { entries: HashMap<(usize, String), Entry>, bytes: usize, clock: u64 }
    fn cache() -> &'static Mutex<Cache> { static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new(); CACHE.get_or_init(Default::default) }

    /// The decoded file and the bound a query admits for it, as it would to decode it.
    pub(super) fn get(storage: &Arc<BufferPool>, key: &ObjectKey) -> Option<(Arc<MetadataFile>, usize)> {
        let mut cache = cache().lock().ok()?;
        cache.clock += 1;
        let clock = cache.clock;
        let entry = cache.entries.get_mut(&(Arc::as_ptr(storage) as usize, key.as_str().to_owned()))?;
        if !entry.storage.upgrade().is_some_and(|live| Arc::ptr_eq(&live, storage)) { return None; }
        entry.used = clock;
        Some((Arc::clone(&entry.metadata), entry.bound))
    }
    /// A data file's inspected IPC layout, which a scan otherwise re-derives from the footer on every open.
    pub(super) fn layout(storage: &Arc<BufferPool>, key: &ObjectKey) -> Option<FileLayout> {
        let layouts = layouts().lock().ok()?;
        let (live, layout) = layouts.get(&(Arc::as_ptr(storage) as usize, key.as_str().to_owned()))?;
        live.upgrade().is_some_and(|live| Arc::ptr_eq(&live, storage)).then(|| layout.clone())
    }
    pub(super) fn insert_layout(storage: &Arc<BufferPool>, key: &ObjectKey, layout: FileLayout) {
        let Ok(mut layouts) = layouts().lock() else { return };
        if layouts.len() >= LAYOUTS { layouts.retain(|_, (live, _)| live.strong_count() > 0); }
        if layouts.len() >= LAYOUTS { layouts.clear(); }
        layouts.insert((Arc::as_ptr(storage) as usize, key.as_str().to_owned()), (Arc::downgrade(storage), layout));
    }
    const LAYOUTS: usize = 4096;
    fn layouts() -> &'static Mutex<HashMap<(usize, String), (Weak<BufferPool>, FileLayout)>> { static LAYOUTS: OnceLock<Mutex<HashMap<(usize, String), (Weak<BufferPool>, FileLayout)>>> = OnceLock::new(); LAYOUTS.get_or_init(Default::default) }

    /// Decoded data pages, held under the cache's own budget so no query's charge outlives it.
    const PAGE_BYTES: usize = 512 << 20;
    struct Page { storage: Weak<BufferPool>, batch: Arc<IndexBatch>, bound: usize, used: u64 }
    #[derive(Default)]
    struct Pages { entries: HashMap<(usize, String, usize), Page>, clock: u64 }
    fn pages() -> &'static Mutex<Pages> { static PAGES: OnceLock<Mutex<Pages>> = OnceLock::new(); PAGES.get_or_init(Default::default) }
    fn page_budget() -> &'static Arc<MemoryBudget> { static BUDGET: OnceLock<Arc<MemoryBudget>> = OnceLock::new(); BUDGET.get_or_init(|| MemoryBudget::new(PAGE_BYTES)) }

    pub(super) fn page(storage: &Arc<BufferPool>, key: &ObjectKey, page: usize) -> Option<(Arc<IndexBatch>, usize)> {
        let mut pages = pages().lock().ok()?;
        pages.clock += 1;
        let clock = pages.clock;
        let entry = pages.entries.get_mut(&(Arc::as_ptr(storage) as usize, key.as_str().to_owned(), page))?;
        if !entry.storage.upgrade().is_some_and(|live| Arc::ptr_eq(&live, storage)) { return None; }
        entry.used = clock;
        Some((Arc::clone(&entry.batch), entry.bound))
    }
    /// Admits a page of `bound` bytes to the cache's budget, evicting the least recently used pages to make room.
    /// Reservations for a page and its decoded headers from the cache's own budget, evicting until both fit.
    pub(super) fn page_reservation(bound: usize, headers: usize) -> Option<(Arc<rtdb_columnar::MemoryReservation>, Arc<rtdb_columnar::MemoryReservation>)> {
        if bound.checked_add(headers)? > PAGE_BYTES / 8 { return None; }
        let mut pages = pages().lock().ok()?;
        loop {
            if let Ok(reservation) = page_budget().reserve(bound) {
                if let Ok(headers) = page_budget().reserve(headers) { return Some((reservation, headers)); }
            }
            let oldest = pages.entries.iter().min_by_key(|(_, entry)| entry.used).map(|(key, _)| key.clone())?;
            pages.entries.remove(&oldest);
        }
    }
    pub(super) fn insert_page(storage: &Arc<BufferPool>, key: &ObjectKey, page: usize, batch: Arc<IndexBatch>, bound: usize) {
        let Ok(mut pages) = pages().lock() else { return };
        pages.clock += 1;
        let used = pages.clock;
        pages.entries.retain(|_, entry| entry.storage.strong_count() > 0);
        pages.entries.insert((Arc::as_ptr(storage) as usize, key.as_str().to_owned(), page), Page { storage: Arc::downgrade(storage), batch, bound, used });
    }

    pub(super) fn insert(storage: &Arc<BufferPool>, key: &ObjectKey, metadata: Arc<MetadataFile>, bound: usize, bytes: usize) {
        if bytes > BYTES / 4 { return; }
        let Ok(mut cache) = cache().lock() else { return };
        cache.clock += 1;
        let used = cache.clock;
        cache.entries.retain(|_, entry| entry.storage.strong_count() > 0);
        if let Some(previous) = cache.entries.insert((Arc::as_ptr(storage) as usize, key.as_str().to_owned()), Entry { storage: Arc::downgrade(storage), metadata, bound, bytes, used }) {
            cache.bytes -= previous.bytes;
        }
        cache.bytes += bytes;
        while cache.entries.len() > FILES || cache.bytes > BYTES {
            let Some(oldest) = cache.entries.iter().min_by_key(|(_, entry)| entry.used).map(|(key, _)| key.clone()) else { break };
            if let Some(evicted) = cache.entries.remove(&oldest) { cache.bytes -= evicted.bytes; }
        }
    }
}

fn invalid_iid_path() -> Anomaly { invalid("iid-path", "Trie path exceeds 64 base-four buckets") }
fn path_range(path: &[u8]) -> Result<(Iid, Iid)> {
    if path.len() > MAX_IID_DEPTH || path.iter().any(|bucket| *bucket > 3) { return Err(invalid_iid_path()); }
    let mut lower = [0u8; 16];
    let mut upper = [u8::MAX; 16];
    for (depth, bucket) in path.iter().enumerate() {
        let shift = 6 - 2 * (depth % 4);
        lower[depth / 4] |= bucket << shift;
        upper[depth / 4] = (upper[depth / 4] & !(3 << shift)) | (bucket << shift);
    }
    Ok((Iid(lower), Iid(upper)))
}
fn recency(date: Option<rtdb_types::CivilDate>) -> Result<Timestamp> {
    date.map(|date| date.epoch_day().checked_mul(86_400_000_000).map(Timestamp).ok_or_else(|| invalid("recency", "Trie recency exceeds microsecond range"))).unwrap_or(Ok(Timestamp::MAX))
}
fn summarize_headers(headers: impl IntoIterator<Item = EventHeader>) -> Option<TemporalSummary> {
    let mut result: Option<TemporalSummary> = None;
    for event in headers {
        let summary = result.get_or_insert(TemporalSummary { min_valid_from: event.valid_time.lower(), max_valid_to: event.valid_time.upper(), min_system_from: event.system_from, max_system_from: event.system_from });
        summary.min_valid_from = summary.min_valid_from.min(event.valid_time.lower()); summary.max_valid_to = summary.max_valid_to.max(event.valid_time.upper());
        summary.min_system_from = summary.min_system_from.min(event.system_from); summary.max_system_from = summary.max_system_from.max(event.system_from);
    }
    result
}
fn may_satisfy_not_null(scan: &Scan, columns: &[Value]) -> bool {
    let [Expr::Unary { op: UnaryOp::IsNotNull, expr }] = scan.predicates.as_slice() else {
        return true;
    };
    let Expr::Column { id, .. } = expr.as_ref() else {
        return true;
    };
    let mut fields = scan.columns.iter().filter(|field| field.id == *id);
    let (Some(field), None) = (fields.next(), fields.next()) else {
        return true;
    };
    if matches!(
        field.name.as_str(),
        "_iid" | "_valid_from" | "_valid_to" | "_system_from" | "_system_to" | "_valid_time" | "_system_time" | "_recency"
    ) {
        return true;
    }
    let mut matching = columns.iter().filter(|column| {
        matches!(column.field("col-name"), Some(Value::Utf8(name)) if name == &field.name)
            && !matches!(column.field("root-col?"), Some(Value::Bool(false)))
    });
    let (Some(column), None) = (matching.next(), matching.next()) else {
        return true;
    };
    !matches!(
        (column.field("root-col?"), column.field("count")),
        (Some(Value::Bool(true)), Some(Value::I64(0)))
    )
}

fn page_temporal(columns: &[Value]) -> Option<TemporalSummary> {
    let bound = |name: &str, key: &str, lower: bool| -> Option<Timestamp> {
        let column = columns.iter().find(|column| column.field("col-name") == Some(&Value::Utf8(name.to_owned())) && column.field("root-col?") == Some(&Value::Bool(true)))?;
        let Value::F64(seconds) = column.field("date-times")?.field(key)? else { return None; };
        if !seconds.is_finite() { return None; }
        let epsilon = seconds.abs() * f64::EPSILON * 4.0 + 0.000001;
        let micros = (seconds + if lower { -epsilon } else { epsilon }) * 1_000_000.0;
        Some(Timestamp(if lower { micros.floor() as i64 } else { micros.ceil() as i64 }))
    };
    Some(TemporalSummary { min_valid_from: bound("_valid_from", "min", true)?, max_valid_to: bound("_valid_to", "max", false)?,
        min_system_from: bound("_system_from", "min", true)?, max_system_from: bound("_system_from", "max", false)? })
}

#[cfg(test)]
mod tests {
    use rtdb_types::StructFields;
    use super::*;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, context::OuterRows, parameters::BoundParameters};
    use rtdb_format::{block::proto as pb, catalog::BlockCatalog, data::{StoredEvent, StoredOperation, build_event_batch},
        ipc::write_file, keys::{TrieKey, block_key}, metadata::MetadataBuilder};
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_plan::{Catalog, ColumnId, Field, QualifiedTable};
    use rtdb_storage::{MemoryStore, ObjectStore};
    use rtdb_trie::TrieConfig;
    use rtdb_types::{EntityId, LogicalType, ScalarType, TableRef, TimeUnit, TransactionKey, TxId};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn an_id_equality_or_disjunction_narrows_the_scan_only_when_every_value_names_one_iid() {
        let id = Field { id: ColumnId(0), name: "_id".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
        let table = rtdb_plan::TableBinding { name: QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "docs") }, oid: None, columns: Default::default() };
        let scan = |predicate: Expr| Scan { table: table.clone(), columns: vec![id.clone()], valid_time: TemporalFilter::Default, system_time: TemporalFilter::Default,
            clamp_valid_time: false, iids: None, predicates: vec![predicate] };
        let equals = |value: Expr| Expr::binary(rtdb_plan::BinaryOp::Eq, id.expression(), value);
        let parameter = Expr::Parameter { index: 0, data_type: LogicalType::Scalar(ScalarType::Utf8) };
        let iid = |value: Value| value.entity_id().unwrap().iid();
        assert_eq!(id_selection(&scan(equals(Expr::Literal(Value::I64(7)))), &[]), Some([iid(Value::I64(7))].into()));
        let either = Expr::binary(rtdb_plan::BinaryOp::Or, equals(Expr::Literal(Value::I64(7))), Expr::binary(rtdb_plan::BinaryOp::Eq, parameter.clone(), id.expression()));
        assert_eq!(id_selection(&scan(either.clone()), &[Value::Utf8("seven".into())]), Some([iid(Value::I64(7)), iid(Value::Utf8("seven".into()))].into()));
        assert_eq!(id_selection(&scan(equals(Expr::Literal(Value::F64(7.0)))), &[]), None, "7.0 still matches the integer ID 7");
        assert_eq!(id_selection(&scan(either), &[Value::Null]), None);
        let range = Expr::binary(rtdb_plan::BinaryOp::Lt, id.expression(), Expr::Literal(Value::I64(7)));
        assert_eq!(id_selection(&scan(range), &[]), None);
    }

    fn live_table() -> (PartitionState, TableRef, Vec<StoredEvent>) {
        let state = PartitionState::empty(TrieConfig::new(1, 1).unwrap()).unwrap();
        let table = TableRef::new("public", "docs");
        let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
        let memory = MemoryBudget::new(1 << 20);
        let mut write = TransactionBuffer::new(state.value().snapshot(), key, memory).unwrap();
        let events = [0x00, 0x40, 0x80].into_iter().enumerate().map(|(index, prefix)| {
            let value = Value::I64(index as i64 + 1);
            StoredEvent {
                iid: Iid([prefix; 16]),
                system_from: key.system_time,
                valid_time: Interval::ALL,
                operation: StoredOperation::Put(StructFields::from([("_id".into(), value.clone()), ("v".into(), value)])),
                recency: None,
            }
        }).collect::<Vec<_>>();
        for event in &events {
            write.append(table.clone(), event.clone()).unwrap();
        }
        state.apply(&write.stage().unwrap()).unwrap();
        (state, table, events)
    }

    fn descriptor_scan(snapshot: Snapshot, table: TableRef, storage: Arc<BufferPool>) -> (Scan, Execution, Arc<MemoryBudget>) {
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let binding = catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
        let field = Field { id: ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: binding.columns["v"].clone() };
        let scan = Scan {
            table: binding, columns: vec![field], valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates: Vec::new(),
        };
        let memory = MemoryBudget::new(1 << 24);
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(100), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
        };
        (scan, execution, memory)
    }

    async fn assert_descriptor_lifetime(snapshot: Snapshot, table: TableRef, store: Arc<MemoryStore>, persisted: Option<(usize, usize)>) {
        let (metadata_size, metadata_charge) = persisted.unwrap_or_default();
        let files_used = usize::from(persisted.is_some());
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(snapshot, table, Arc::clone(&storage));
        let before_cursor = memory.used_bytes();
        let mut cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
        let cursor_open = memory.used_bytes();
        let candidate_extent = if files_used == 0 { 0 } else { std::mem::size_of::<&CatalogTrie>() + 256 };
        let selected_extent = if files_used == 0 { 0 } else { std::mem::size_of::<usize>() + 256 };
        let file_extent = if files_used == 0 { 0 } else { std::mem::size_of::<DataFile>() + 256 };
        let stack_extent = if files_used == 0 { 0 } else { 4 * std::mem::size_of::<MetadataVisit>() + 256 };
        if files_used != 0 {
            let pressure = memory.reserve(memory.limit_bytes() - cursor_open - (candidate_extent - 1)).unwrap();
            let retained = cursor_open + pressure.bytes();
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {candidate_extent} bytes from budget {} with {retained} retained", memory.limit_bytes());
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            let partition = cursor.partitions.front().unwrap();
            assert!(!partition.initialized);
            assert!(partition.files.entries.is_empty());
            assert!(partition.pages.entries.is_empty());
            assert!(partition.tasks.is_none());
            assert!(partition.loaded.is_empty());
            assert!(partition.output.is_empty());
            assert_eq!(storage.retained_bytes(), 0);
            assert_eq!(memory.used_bytes(), retained);
            drop(pressure);
            assert_eq!(memory.used_bytes(), cursor_open);

            let summary_extent = std::mem::size_of::<PageSummary>() + 256;
            let selector_extent = 3 * (std::mem::size_of::<&PageSummary>() + 256);
            for (requested, earlier) in [
                (summary_extent, candidate_extent),
                (selector_extent, candidate_extent + summary_extent),
                (selected_extent, candidate_extent + summary_extent + selector_extent),
            ] {
                let pressure = memory.reserve(memory.limit_bytes() - cursor_open - earlier - requested + 1).unwrap();
                let retained = cursor_open + pressure.bytes() + earlier;
                let error = cursor.next_page().await.err().unwrap();
                let expected = format!("Cannot reserve {requested} bytes from budget {} with {retained} retained", memory.limit_bytes());
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                assert_eq!(error.message.as_deref(), Some(expected.as_str()));
                let partition = cursor.partitions.front().unwrap();
                assert!(!partition.initialized);
                assert!(partition.files.entries.is_empty());
                assert!(partition.pages.entries.is_empty());
                assert!(partition.tasks.is_none());
                assert_eq!(storage.retained_bytes(), 0);
                assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
            }
            assert_eq!(memory.used_bytes(), cursor_open);

            let headroom = candidate_extent + selected_extent + metadata_charge + file_extent - 1;
            let pressure = memory.reserve(memory.limit_bytes() - cursor_open - headroom).unwrap();
            let retained = cursor_open + pressure.bytes() + candidate_extent + selected_extent + metadata_charge;
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {file_extent} bytes from budget {} with {retained} retained", memory.limit_bytes());
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            let partition = cursor.partitions.front().unwrap();
            assert!(partition.files.entries.is_empty());
            assert!(partition.files._reservation.is_none());
            assert!(partition.tasks.is_none());
            assert!(partition.loaded.is_empty());
            assert_eq!(storage.retained_bytes(), metadata_size);
            assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
            drop((cursor, pressure));
            assert_eq!(memory.used_bytes(), before_cursor);
            cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
            assert_eq!(memory.used_bytes(), cursor_open);

            let previous_stack = 2 * std::mem::size_of::<MetadataVisit>() + 256;
            let headroom = candidate_extent + selected_extent + metadata_charge + file_extent + previous_stack + stack_extent - 1;
            let pressure = memory.reserve(memory.limit_bytes() - cursor_open - headroom).unwrap();
            let retained = cursor_open + pressure.bytes() + candidate_extent + selected_extent + metadata_charge + file_extent + previous_stack;
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {stack_extent} bytes from budget {} with {retained} retained", memory.limit_bytes());
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            let partition = cursor.partitions.front().unwrap();
            assert!(!partition.initialized);
            assert!(partition.files.entries.is_empty());
            assert!(partition.pages.entries.is_empty());
            assert!(partition.tasks.is_none());
            assert!(partition.loaded.is_empty());
            assert!(partition.output.is_empty());
            assert_eq!(storage.retained_bytes(), metadata_size);
            assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
            drop(pressure);
            assert_eq!(memory.used_bytes(), cursor_open);
        }
        let old_extent = 2 * std::mem::size_of::<Page>() + 256;
        let replacement_extent = 4 * std::mem::size_of::<Page>() + 256;
        let headroom = candidate_extent + selected_extent + metadata_charge + file_extent + stack_extent + old_extent + replacement_extent - 1;
        let pressure = memory.reserve(memory.limit_bytes() - cursor_open - headroom).unwrap();
        let retained_at_growth = cursor_open + pressure.bytes() + candidate_extent + selected_extent + metadata_charge + file_extent + stack_extent + old_extent;
        let error = cursor.next_page().await.err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        let expected = format!("Cannot reserve {replacement_extent} bytes from budget {} with {retained_at_growth} retained", memory.limit_bytes());
        assert_eq!(error.message.as_deref(), Some(expected.as_str()));
        let partition = cursor.partitions.front().unwrap();
        assert!(partition.pages.entries.is_empty());
        assert!(partition.pages._reservation.is_none());
        assert!(partition.tasks.is_none());
        assert!(partition.loaded.is_empty());
        assert!(partition.files.entries.is_empty());
        assert!(partition.files._reservation.is_none());
        assert_eq!(storage.retained_bytes(), metadata_size);
        assert_eq!(memory.used_bytes(), cursor_open + pressure.bytes());
        drop((cursor, pressure));
        assert_eq!(memory.used_bytes(), before_cursor);

        if files_used != 0 {
            let mut cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
            let partition = cursor.partitions.front_mut().unwrap();
            partition.initialize().await.unwrap();
            let plan_memory = MemoryBudget::new(memory.limit_bytes());
            let plan = ScanTaskPlan::new(partition.pages.entries.iter().map(|page| PageRange { lower: page.lower, upper: page.upper }), &plan_memory).unwrap();
            assert_eq!(memory.used_bytes(), cursor_open + file_extent + replacement_extent + plan_memory.used_bytes());
            drop(plan);
            drop(partition.tasks.take().unwrap());
            assert_eq!(memory.used_bytes(), cursor_open + file_extent + replacement_extent);
            drop(cursor);
            assert_eq!(memory.used_bytes(), before_cursor);
        }

        let mut cursor = ScanCursor::new(scan, execution.clone(), None).await.unwrap();
        assert_eq!(memory.used_bytes(), cursor_open);
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            let partition = cursor.partitions.front().unwrap();
            assert_eq!(partition.files.entries.len(), files_used);
            assert_eq!(partition.files._reservation.as_ref().map_or(0, |lease| lease.bytes()), file_extent);
            let descriptors = &partition.pages;
            assert_eq!(descriptors.entries.len(), 3);
            assert_eq!(descriptors._reservation.as_ref().unwrap().bytes(), replacement_extent);
            rows.extend(page.rows().iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(rows, [Value::I64(1), Value::I64(2), Value::I64(3)]);
        let counts = cursor.scan_counts().unwrap();
        assert_eq!(counts.files_used, files_used as u64);
        assert_eq!((counts.pages_used, counts.rows_read), (3, 3));
        assert!(cursor.partitions.is_empty());
        assert_eq!(memory.used_bytes(), cursor_open);
        drop(cursor);
        assert_eq!(memory.used_bytes(), before_cursor);
        drop(execution);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn live_page_descriptors_admit_growth_and_release_on_partition_close() {
        let (state, table, _) = live_table();
        let snapshot = state.value().snapshot();
        let segments = snapshot.segments(&table);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].trie().leaves().len(), 3);
        assert!(snapshot.trie_entries(&table).is_empty());
        assert_descriptor_lifetime(snapshot, table, Arc::new(MemoryStore::default()), None).await;
    }

    async fn persisted_page_fixture() -> (PartitionState, TableRef, Vec<StoredEvent>, Arc<MemoryStore>, (usize, usize)) {
        persisted_pages(false).await
    }

    /// Persists one page per event; `misplaced` writes the second event's page in the first one's place too.
    async fn persisted_pages(misplaced: bool) -> (PartitionState, TableRef, Vec<StoredEvent>, Arc<MemoryStore>, (usize, usize)) {
        let (state, table, events) = live_table();
        let captured = state.value();
        let table_entry = captured.table_catalog().entry(&table).unwrap();
        let trie = TrieKey::l0(captured.live().block_index()).unwrap();
        let put_fields = captured.live().tables()[&table].column_types().clone();
        let mut builder = MetadataBuilder::new(false, false);
        let mut children = [None; 4];
        let mut pages = Vec::new();
        for (slot, event) in children.iter_mut().zip(&events) {
            let page = build_event_batch(std::slice::from_ref(event), Some(put_fields.clone()), None).unwrap();
            *slot = Some(builder.write_leaf(&page).unwrap());
            pages.push(page);
        }
        builder.write_iid_branch(children).unwrap();
        let (metadata, statistics) = builder.finish().unwrap();
        let meta = metadata.write().unwrap();
        if misplaced { pages[0] = pages[1].clone(); }
        let data = write_file(pages[0].schema().as_ref(), &pages).unwrap();
        let metadata_size = meta.len();
        let probe = MemoryBudget::new(1 << 24);
        let layout = inspect_file_reserved(&mut Cursor::new(meta.as_slice()), |bytes| probe.reserve(bytes)).unwrap();
        let metadata_charge = layout.full_allocation_bound().unwrap();
        assert!(probe.peak_bytes() <= metadata_charge);
        assert_eq!(probe.used_bytes(), 0);

        let store = Arc::new(MemoryStore::default());
        let addition = CatalogTrie::notification(trie.clone(), data.len() as i64, Some(statistics)).unwrap();
        let nascent = CatalogTrie::notification("l01-r20200101-b00".parse().unwrap(), 0, None).unwrap();
        store.put(&ObjectKey::new(table_entry.slug.metadata_key(&trie)).unwrap(), meta.into()).await.unwrap();
        store.put(&ObjectKey::new(table_entry.slug.data_key(&trie)).unwrap(), data.into()).await.unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![addition, nascent])]), Timestamp(200)).unwrap();
        (state, table, events, store, (metadata_size, metadata_charge))
    }

    #[tokio::test]
    async fn a_data_page_outside_its_metadata_path_fails_every_scan_that_reads_it() {
        let (state, table, _, store, _) = persisted_pages(true).await;
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        for scan_number in 0..2 {
            let (scan, execution, _) = descriptor_scan(state.value().snapshot(), table.clone(), Arc::clone(&storage));
            let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
            let error = loop {
                match cursor.next_page().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => panic!("scan {scan_number} served the misplaced page"),
                    Err(error) => break error,
                }
            };
            assert!(error.code.ends_with("page-path"), "scan {scan_number}: {error:?}");
        }
    }

    #[tokio::test]
    async fn persisted_page_descriptors_admit_growth_before_opening_the_data_file() {
        let (state, table, _, store, metadata) = persisted_page_fixture().await;
        let snapshot = state.value().snapshot();
        assert_eq!(snapshot.trie_catalog().iter_tries(&table).count(), 2);
        assert_eq!(snapshot.trie_entries(&table).len(), 1);
        assert!(snapshot.segments(&table).is_empty());
        assert_descriptor_lifetime(snapshot.clone(), table.clone(), Arc::clone(&store), Some(metadata)).await;
        assert_combined_summary_replacement(snapshot, table, store).await;
    }

    #[tokio::test]
    async fn live_segment_priority_counts_pruned_segments_before_persisted_pages() {
        let (state, table, events, store, _) = persisted_page_fixture().await;
        let prepared = state.prepare_block().unwrap();
        let catalog = prepared.table_catalog().unwrap();
        let basis = prepared.value().live().latest_completed_tx().unwrap();
        let block = BlockCatalog::from_proto(pb::Block {
            block_index: Some(prepared.block_index().0),
            latest_completed_tx: Some(pb::TxKey { tx_id: Some(basis.tx_id.0), system_time: Some(basis.system_time.0) }),
            tables: catalog.entries().map(|entry| entry.to_proto()).collect(),
            ..pb::Block::default()
        }).unwrap();
        let additions = BTreeMap::new();
        for (table, table_block) in prepared.table_blocks(&additions, Timestamp(200)).unwrap() {
            let key = catalog.entry(&table).unwrap().slug.block_key(prepared.block_index()).unwrap();
            store.put(&ObjectKey::new(key).unwrap(), table_block.encode().into()).await.unwrap();
        }
        let key = block_key(prepared.block_index()).unwrap();
        store.put(&ObjectKey::new(key).unwrap(), block.encode().into()).await.unwrap();
        state.adopt_block(prepared, block, &additions, Timestamp(200)).unwrap();

        let memory = MemoryBudget::new(1 << 20);
        let key = TransactionKey { tx_id: TxId(1), system_time: basis.system_time };
        let mut applied = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
        for event in [&events[0], &events[2]] { applied.append(table.clone(), event.clone()).unwrap(); }
        state.apply(&applied.stage().unwrap()).unwrap();

        let put_overlay = |snapshot, tx_id, value| {
            let key = TransactionKey { tx_id: TxId(tx_id), system_time: basis.system_time };
            let mut write = TransactionBuffer::new(snapshot, key, Arc::clone(&memory)).unwrap();
            let mut event = events[1].clone();
            event.operation = StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(2)), ("v".into(), Value::I64(value))]));
            write.append(table.clone(), event).unwrap();
            write.snapshot().unwrap()
        };
        let older = put_overlay(state.value().snapshot(), 2, 4);
        let mixed = put_overlay(older, 3, 5);
        let segments = mixed.segments(&table);
        assert_eq!(segments.len(), 3);
        let paths: Vec<_> = segments[2].trie().leaves().into_iter().map(|leaf| leaf.path().as_slice()).collect();
        assert_eq!(paths.as_slice(), &[&[0u8][..], &[2u8][..]]);

        let (mut scan, execution, _) = descriptor_scan(mixed, table, Arc::new(BufferPool::new(store, 1 << 20)));
        scan.iids = Some([events[1].iid].into_iter().collect());
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let partition = cursor.partitions.front_mut().unwrap();
        partition.initialize().await.unwrap();
        let pages = &partition.pages.entries;
        assert_eq!(pages.iter().map(|page| page.priority).collect::<Vec<_>>(), [0, 1, 3]);
        assert!(matches!(&pages[0].source, PageSource::Live { .. }));
        assert!(matches!(&pages[1].source, PageSource::Live { .. }));
        assert!(matches!(&pages[2].source, PageSource::File { file: 0, page: 1 }));

        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(rows, [Value::I64(5)]);
    }

    async fn assert_combined_summary_replacement(snapshot: Snapshot, table: TableRef, store: Arc<MemoryStore>) {
        let key = TransactionKey { tx_id: TxId(1), system_time: Timestamp(110) };
        let mut write = TransactionBuffer::new(snapshot, key, MemoryBudget::new(1 << 20)).unwrap();
        write.append(table.clone(), StoredEvent {
            iid: Iid([0xc0; 16]),
            system_from: key.system_time,
            valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(4)), ("v".into(), Value::I64(4))])),
            recency: None,
        }).unwrap();
        let mixed = write.snapshot().unwrap();
        assert_eq!(mixed.trie_entries(&table).len(), 1);
        assert_eq!(mixed.segments(&table).len(), 1);
        assert_eq!(mixed.segments(&table)[0].trie().leaves().len(), 1);
        let storage = Arc::new(BufferPool::new(store, 1 << 20));
        let (scan, execution, memory) = descriptor_scan(mixed, table, Arc::clone(&storage));
        let mut cursor = ScanCursor::new(scan, execution, None).await.unwrap();
        let baseline = memory.used_bytes();
        let candidate = std::mem::size_of::<&CatalogTrie>() + 256;
        let old_summary = std::mem::size_of::<PageSummary>() + 256;
        let new_summary = 2 * std::mem::size_of::<PageSummary>() + 256;
        let live_page = std::mem::size_of::<Page>() + 256;
        let headroom = candidate + old_summary + live_page + new_summary - 1;
        let pressure = memory.reserve(memory.limit_bytes() - baseline - headroom).unwrap();
        let retained = baseline + pressure.bytes() + candidate + old_summary + live_page;
        let error = cursor.next_page().await.err().unwrap();
        let expected = format!("Cannot reserve {new_summary} bytes from budget {} with {retained} retained", memory.limit_bytes());
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(error.message.as_deref(), Some(expected.as_str()));
        let partition = cursor.partitions.front().unwrap();
        assert!(!partition.initialized);
        assert!(partition.pages.entries.is_empty());
        assert!(partition.tasks.is_none());
        assert_eq!(storage.retained_bytes(), 0);
        assert_eq!(memory.used_bytes(), baseline + pressure.bytes());
        drop(pressure);
        assert_eq!(memory.used_bytes(), baseline);

        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values[0].clone()));
        }
        assert_eq!(rows, [Value::I64(1), Value::I64(2), Value::I64(3), Value::I64(4)]);
        assert_eq!(cursor.scan_counts().unwrap().files_used, 1);
        drop(cursor);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn live_scan_admits_resolved_history_before_projecting_fragments() {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        let table = TableRef::new("public", "docs");
        let write_memory = MemoryBudget::new(1 << 20);
        let put = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
        let mut write = TransactionBuffer::new(state.value().snapshot(), put, Arc::clone(&write_memory)).unwrap();
        write.put(table.clone(), StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(9))]), Interval::from_micros(0, 30).unwrap()).unwrap();
        state.apply(&write.stage().unwrap()).unwrap();
        let delete = TransactionKey { tx_id: TxId(1), system_time: Timestamp(200) };
        let mut write = TransactionBuffer::new(state.value().snapshot(), delete, write_memory).unwrap();
        write.delete(table.clone(), &EntityId::Integer(1), Interval::from_micros(10, 20).unwrap()).unwrap();
        state.apply(&write.stage().unwrap()).unwrap();

        let snapshot = state.value().snapshot();
        assert_eq!(snapshot.system_time_cap(), Some(delete.system_time));
        let segments = snapshot.segments(&table);
        assert_eq!(segments.len(), 1);
        let leaves = segments[0].trie().leaves();
        assert_eq!(leaves.len(), 1);
        assert!(leaves[0].path().as_slice().is_empty());
        let storage = Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20));
        let database = DatabaseSnapshot { partitions: vec![snapshot], storage };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let binding = catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
        let time = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
        let columns = [
            ("v", binding.columns["v"].clone()), ("_valid_from", time.clone()), ("_valid_to", time.clone()),
            ("_system_from", time.clone()), ("_system_to", time.maybe()),
        ].into_iter().enumerate().map(|(index, (name, data_type))| Field {
            id: ColumnId(index as u32), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type,
        }).collect();
        let scan = Scan {
            table: binding, columns, valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates: Vec::new(),
        };
        let root = MemoryBudget::new(1 << 24);
        let memory = root.scope();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(200), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let execution = Execution {
            parameters: Arc::new(BoundParameters::infer(Vec::new(), &memory).unwrap()),
            outer: OuterRows::empty(&memory).unwrap(), context, bindings: None, analysis: None,
        };
        let baseline = root.used_bytes();
        let scratch = std::mem::size_of::<PageSummary>() + std::mem::size_of::<Head>() + std::mem::size_of::<PageBuilder>()
            + std::mem::size_of::<OwnedPage>() + 256 + 4096 + 256;
        let timestamp_bytes = std::mem::size_of::<Timestamp>();
        let previous_ceiling = (2 * 3 + 1) * timestamp_bytes + 256;
        let replacement_ceiling = (2 * 6 + 1) * timestamp_bytes + 256;
        let history = 64 * std::mem::size_of::<ResolvedRow<(u32, u32)>>();
        for (requested, charged) in [
            (replacement_ceiling, scratch + previous_ceiling),
            (history, scratch + previous_ceiling + replacement_ceiling),
        ] {
            let mut cursor = ScanCursor::new(scan.clone(), execution.clone(), None).await.unwrap();
            let partition = cursor.partitions.front_mut().unwrap();
            partition.initialize().await.unwrap();
            assert_eq!(partition.pages.entries.len(), 1);
            partition.load(0).await.unwrap();
            let limit = root.limit_bytes();
            let pressure = root.reserve(limit - root.used_bytes() - charged - requested + 1).unwrap();
            let retained = root.used_bytes() + charged;
            let error = cursor.next_page().await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            let expected = format!("Cannot reserve {requested} bytes from budget {limit} with {retained} retained");
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            assert!(cursor.partitions.front().unwrap().output.is_empty());
            drop((cursor, pressure));
            assert_eq!(root.used_bytes(), baseline);
            assert_eq!(memory.used_bytes(), baseline);
        }

        let mut cursor = ScanCursor::new(scan, execution.clone(), None).await.unwrap();
        let cursor_open = root.used_bytes();
        let mut rows = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() {
            rows.extend(page.rows().iter().map(|row| row.values.clone()));
        }
        let timestamp = |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
        assert_eq!(rows, [
            vec![Value::I64(9), timestamp(0), timestamp(10), timestamp(100), Value::Null],
            vec![Value::I64(9), timestamp(10), timestamp(20), timestamp(100), timestamp(200)],
            vec![Value::I64(9), timestamp(20), timestamp(30), timestamp(100), Value::Null],
        ]);
        assert!(cursor.partitions.is_empty());
        assert_eq!(root.used_bytes(), cursor_open);
        drop((cursor, execution));
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(root.used_bytes(), 0);
    }
}
