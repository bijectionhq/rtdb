use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, StructArray, UnionArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, estimate_builder_bytes, from_field};
use rtdb_format::{ipc::{ArrowFile, inspect_file_reserved}, metadata::{MetadataFile, MetadataNode}};
use rtdb_index::{CatalogTrie, IndexBatch, LiveTable, RowSource, Snapshot, TrieState};
use rtdb_plan::{Expr, Scan, Schema, TemporalFilter, UnaryOp};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_temporal::{HistoryResolver, ResolvedRow};
use rtdb_trie::{PageSummary, PruningOptions, TemporalSummary, select_pages, select_pages_with_scratch};
use rtdb_types::{Anomaly, EventHeader, Iid, Interval, Result, TemporalBounds, TimeUnit, Timestamp, Value};

use crate::{context::Execution, cursor::OwnedRow, pages::{OwnedPage,PageBuilder,PageCursor}, scan_tasks::{PageRange,ScanTask,ScanTaskPlan}, invalid};

enum DataState { Unopened, Open { reader: ArrowFile, _reservation: Arc<MemoryReservation> } }
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

enum PageSource { Live { table: Arc<LiveTable>, rows: Arc<[u32]> }, File { file: usize, page: usize } }
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

enum ActiveRows { Live { table: Arc<LiveTable>, rows: Arc<[u32]> }, File(Arc<IndexBatch>) }
struct LoadedPage { rows: ActiveRows, _reservation: Arc<MemoryReservation> }
impl ActiveRows {
    fn len(&self) -> usize { match self { Self::Live { rows, .. } => rows.len(), Self::File(batch) => batch.len() } }
    fn row(&self, index: usize) -> Result<RowSource> {
        match self { Self::Live { table, rows } => table.row(rows[index]), Self::File(batch) => Ok(RowSource { batch: Arc::clone(batch), row: index }) }
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
    output: VecDeque<OwnedPage>,
    output_reservation: Option<Arc<MemoryReservation>>,
    counts: ScanCounts,
    pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
    canonical_entity_iids: bool,
    matches: Vec<EvaluatedMatch>,
    /// Parallel to the scan's columns: the match each evaluates, if any.
    slots: Vec<Option<usize>>,
}

/// A MATCH the scan evaluates: on its column, for its query (`None` where the query is null, so MATCH is too), with an accelerator's answers for some of the partition's data files.
struct EvaluatedMatch { column: String, query: Option<rtdb_text::Query>, answers: Option<crate::MatchAnswers> }

use crate::analysis::ScanCounts;
pub(crate) struct ScanCursor { partitions: VecDeque<PartitionScan>, completed: ScanCounts, database: String, source: String, _reservation: Arc<MemoryReservation> }
impl ScanCursor {
    pub async fn new(scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> Result<Self> {
        let context = Arc::clone(&execution.context);
        let database = context.catalog.database(&scan.table.name.database)?;
        Self::over(scan, execution, pushdowns, &database.partitions, &database.storage).await
    }
    /// A scan of the given partitions of its database, which share `storage`.
    async fn over(scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, database_partitions: &[Snapshot], storage: &Arc<BufferPool>) -> Result<Self> {
        let bytes = scan.table.name.database.len().checked_add(scan.table.name.table.schema.len()).and_then(|bytes| bytes.checked_add(scan.table.name.table.table.len())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("scan-size", "Scan labels exceed allocation range"))?;
        let reservation = execution.context.memory.reserve(bytes)?;
        let canonical_entity_iids = scan.iids.is_none()
            && pushdowns.as_ref().is_some_and(|filters| filters.needs_entity_iid_proof(&scan.columns))
            && database_partitions.iter().all(|snapshot| snapshot.has_canonical_entity_iids(&scan.table.name.table));
        if let (Some(analysis), Some(pushdowns)) = (&execution.analysis, &pushdowns) {
            analysis.set_pushdowns(pushdowns, &scan.columns, canonical_entity_iids, &execution.context.memory)?;
        }
        let name = scan.table.name.database.clone();
        let source = format!("{}.{}", scan.table.name.table.schema, scan.table.name.table.table);
        let mut queries = Vec::with_capacity(scan.matches.len());
        for evaluated in &scan.matches {
            let constants = crate::accelerate::ExecutionConstants(&execution);
            queries.push(match crate::ConstantEvaluator::constant(&constants, &evaluated.query).await? { Some(Value::Utf8(text)) => Some(rtdb_text::Query::parse(&text)), _ => None });
        }
        let slots: Vec<Option<usize>> = scan.columns.iter().map(|field| scan.matches.iter().position(|evaluated| evaluated.id == field.id)).collect();
        let mut partitions = VecDeque::new();
        for snapshot in database_partitions {
            let bounds = bounds(&scan, snapshot, &execution).await?;
            let mut narrowed = scan.clone();
            let verifier = ScanVerifier { scan: &scan, execution: &execution, pushdowns: pushdowns.clone(), partition: snapshot, storage };
            for accelerator in execution.context.accelerators.iter().filter(|_| scan.iids.is_none()) {
                let request = crate::ScanRequest { database: &name, scan: &narrowed, snapshot, storage, bounds, memory: &execution.context.memory,
                    current_time: execution.context.current_time, cancellation: &execution.context.cancellation, constants: &crate::accelerate::ExecutionConstants(&execution),
                    corpora: execution.context.corpora.as_ref(), verifier: &verifier, partitions: database_partitions };
                if let Some(mut candidates) = accelerator.candidate_iids(&request).await? {
                    // `path_matches` stops consulting pushdowns once `iids` is set, so they are applied to the candidates here.
                    if let Some(filters) = &pushdowns { candidates.retain(|iid| filters.iid_matches(&scan.columns, *iid, canonical_entity_iids)); }
                    let iids = match narrowed.iids.take() { Some(existing) => existing.intersection(&candidates).copied().collect(), None => candidates };
                    narrowed.iids = Some(iids);
                }
            }
            let mut matches = Vec::with_capacity(scan.matches.len());
            for (evaluated, query) in scan.matches.iter().zip(&queries) {
                let mut answers = None;
                if let Some(query) = query {
                    let request = crate::ScanRequest { database: &name, scan: &narrowed, snapshot, storage, bounds, memory: &execution.context.memory,
                        current_time: execution.context.current_time, cancellation: &execution.context.cancellation, constants: &crate::accelerate::ExecutionConstants(&execution),
                        corpora: execution.context.corpora.as_ref(), verifier: &verifier, partitions: database_partitions };
                    for accelerator in execution.context.accelerators.iter() {
                        if let Some(found) = accelerator.match_answers(&request, &evaluated.column, query).await? { answers = Some(found); break; }
                    }
                }
                matches.push(EvaluatedMatch { column: evaluated.column.clone(), query: query.clone(), answers });
            }
            partitions.push_back(PartitionScan { snapshot: snapshot.clone(), storage: Arc::clone(storage), matches, slots: slots.clone(), scan: narrowed, execution: execution.clone(), bounds,
                initialized: false, files: FileDescriptors::default(), pages: PageDescriptors::default(), tasks: None, loaded: BTreeMap::new(),
                output: VecDeque::new(), output_reservation: None, counts: ScanCounts::default(), pushdowns: pushdowns.clone(), canonical_entity_iids });
        }
        Ok(Self { partitions, completed: ScanCounts::default(), database: name, source, _reservation: reservation })
    }
}

/// Runs the scan itself over one partition, restricted to a batch of IIDs, its predicates applied as the scan's filter applies them; narrowing is skipped because the IIDs are fixed.
struct ScanVerifier<'a> { scan: &'a Scan, execution: &'a Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>, partition: &'a Snapshot, storage: &'a Arc<BufferPool> }

#[async_trait]
impl crate::CandidateVerifier for ScanVerifier<'_> {
    async fn visible(&self, iids: std::collections::BTreeSet<Iid>, column: rtdb_plan::ColumnId) -> Result<Vec<(Iid, Value)>> {
        if iids.is_empty() { return Ok(Vec::new()); }
        let mut scan = self.scan.clone();
        scan.top_k = None;
        scan.iids = Some(iids);
        if !scan.columns.iter().any(|field| field.name == "_iid") {
            let id = rtdb_plan::ColumnId(scan.columns.iter().map(|field| field.id.0).max().unwrap_or(0) + 1);
            scan.columns.push(rtdb_plan::Field { id, name: "_iid".into(), qualifier: None, qualified_only: false, unnamed: true, data_type: rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::FixedSizeBinary(16)) });
        }
        let position = |found: &dyn Fn(&rtdb_plan::Field) -> bool| scan.columns.iter().position(|field| found(field)).ok_or_else(|| invalid("verifier-column", "Verified column is not projected by the scan"));
        let (iid_at, column_at) = (position(&|field| field.name == "_iid")?, position(&|field| field.id == column)?);
        let schema = Schema::new(scan.columns.clone())?;
        let predicate = scan.predicates.iter().cloned().reduce(|left, right| Expr::binary(rtdb_plan::BinaryOp::And, left, right));
        let mut cursor = ScanCursor::over(scan, self.execution.clone(), self.pushdowns.clone(), std::slice::from_ref(self.partition), self.storage).await?;
        let metadata = self.execution.scope()?;
        let invocation = predicate.as_ref().map(|predicate| self.execution.prepare_expression(predicate, &schema, &metadata)).transpose()?;
        let mut visible = Vec::new();
        while let Some(mut page) = cursor.next_page().await? {
            while let Some(row) = page.next() {
                if let Some(invocation) = &invocation {
                    let scope = self.execution.scope()?;
                    if !rtdb_expr::is_true(&self.execution.evaluate_invocation(&scope, invocation, &row.values).await?)? { continue; }
                }
                let Value::FixedSizeBinary(iid) = &row.values[iid_at] else { return Err(invalid("verifier-iid", "Verified row has no IID")) };
                visible.push((Iid::from_slice(iid)?, row.values[column_at].clone()));
            }
        }
        Ok(visible)
    }
}

#[async_trait]
impl PageCursor for ScanCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        while let Some(partition) = self.partitions.front_mut() {
            if let Some(page) = partition.next_page().await? { return Ok(Some(page)); }
            self.completed.add(partition.counts);
            self.partitions.pop_front();
        }
        Ok(None)
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
                let rows = leaf.sorted_rows(segment.as_ref())?;
                let temporal = summarize_rows(rows.iter().map(|row| segment.row(*row)))?;
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
            let lease = self.storage.get(&key).await?;
            let mut cursor = Cursor::new(lease);
            let layout = inspect_file_reserved(&mut cursor, |bytes| self.execution.context.memory.reserve(bytes))?;
            let _reservation = self.execution.context.memory.reserve(layout.full_allocation_bound()?)?;
            let metadata = MetadataFile::read_reader(cursor)?;
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
                    MetadataNode::Leaf { data_page_index, columns } => pages.push(Page { lower, upper, priority: live_segment_count + index,
                        content_matches: self.pushdowns.as_ref().map(|filters| filters.page_matches(&self.scan.columns, columns, &self.execution.context.memory)).transpose()?.unwrap_or(true)
                            && may_satisfy_not_null(&self.scan, columns),
                        source: PageSource::File { file, page: *data_page_index }, summary: PageSummary { page_index: 0, temporal: page_temporal(columns),
                            recency: Some(visit.recency), iid_range: Some((lower, upper)) } }, &self.execution.context.memory)?,
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
        let rows = match &page.source {
            PageSource::Live { table, rows } => ActiveRows::Live { table: Arc::clone(table),rows: Arc::clone(rows) },
            PageSource::File { file, page: page_index } => {
                let data = &mut self.files.entries[*file];
                if matches!(&data.state, DataState::Unopened) {
                    let lease = data.storage.get(&data.key).await?;
                    let mut cursor = Cursor::new(lease);
                    let layout = inspect_file_reserved(&mut cursor, |bytes| self.execution.context.memory.reserve(bytes))?;
                    let reservation = self.execution.context.memory.reserve(layout.opening_allocation_bound()?)?;
                    data.state = DataState::Open { reader: ArrowFile::from_inspected_reader(cursor, None, layout)?, _reservation: reservation };
                }
                let DataState::Open { reader, .. } = &mut data.state else { return Err(invalid("file-state", "Data file failed to open")); };
                let mut bound = reader.page_allocation_bound(*page_index)?;
                for field in reader.schema().fields() {
                    bound = bound.checked_add(estimate_builder_bytes(&from_field(field)?, &[])?).ok_or_else(|| invalid("page-size", "Arrow schema allocation bound overflow"))?;
                }
                let reservation = self.execution.context.memory.reserve(bound)?;
                let batch = IndexBatch::from_owned(OwnedBatch::with_reservation(reader.read_page(*page_index)?, reservation)?)?;
                ActiveRows::File(batch)
            }
        };
        let mut previous: Option<EventHeader> = None;
        for index in 0..rows.len() {
            let source = rows.row(index)?;
            let header = source.header()?;
            if header.iid < page.lower || header.iid > page.upper { return Err(invalid("page-path", "Data page contains an IID outside its metadata path")); }
            if previous.is_some_and(|previous| previous.iid > header.iid || (previous.iid == header.iid && previous.system_from < header.system_from)) {
                return Err(invalid("page-order", "Data page is not in IID and descending system-time order"));
            }
            previous = Some(header);
        }
        self.loaded.insert(index,LoadedPage { rows,_reservation: reservation });
        Ok(())
    }
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
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
        let mut heads = BinaryHeap::with_capacity(selected.len());
        for (group,index) in selected.iter().copied().enumerate() {
            self.load(index).await?;
            let rows = &self.loaded[&index].rows;
            self.counts.rows_read = self.counts.rows_read.saturating_add(rows.len() as u64);
            groups.push(PageBuilder::new(0,&self.execution.context.memory)?);
            for ordinal in 0..rows.len() {
                let header = rows.row(ordinal)?.header()?;
                if header.iid > task.range.upper { break; }
                if header.iid >= task.range.lower { heads.push(Head { header,active: group,priority: self.pages.entries[index].priority,ordinal });break; }
            }
        }
        let sources: Vec<(Option<Arc<IndexBatch>>, Option<(usize, usize)>)> = selected.iter().map(|index| match (&self.loaded[index].rows, &self.pages.entries[*index].source) {
            (ActiveRows::File(batch), PageSource::File { file, page }) => (Some(Arc::clone(batch)), Some((*file, *page))),
            _ => (None, None),
        }).collect();
        let mut resolver = HistoryResolver::new_in(self.bounds, self.scan.clamp_valid_time, Arc::clone(&self.execution.context.memory));
        while let Some(head) = heads.pop() {
            self.execution.context.check_cancelled()?;
            let rows = &self.loaded[&selected[head.active]].rows;
            let source = rows.row(head.ordinal)?;
            let ordinal = head.ordinal + 1;
            if ordinal < rows.len() {
                let header = rows.row(ordinal)?.header()?;
                if header.iid <= task.range.upper { heads.push(Head { header,ordinal,..head }); }
            }
            if self.scan.iids.as_ref().is_some_and(|iids| !iids.contains(&head.header.iid)) { continue; }
            if self.scan.iids.is_none() && self.pushdowns.as_ref().is_some_and(|filters| !filters.iid_matches(&self.scan.columns,head.header.iid,self.canonical_entity_iids)) { continue; }
            let history = resolver.resolve_in(&head.header, source, &self.execution.context.memory)?;
            for row in history.as_slice() {
                let bound = self.scan.columns.iter().try_fold(256usize,|bound,field|bound.checked_add(project_bound(row,&field.name)?).ok_or_else(||invalid("scan-row-size","Scan row allocation exceeds addressable memory")))?;
                let reservation = self.execution.context.memory.reserve(bound)?;
                let mut values = Vec::with_capacity(self.scan.columns.len());
                for (field, slot) in self.scan.columns.iter().zip(&self.slots) {
                    values.push(match slot { Some(evaluated) => self.match_value(row, *evaluated, &sources)?, None => project(row,&field.name)? });
                }
                groups[head.active].push_growing(OwnedRow::with_reservation(values,reservation)?,&self.execution.context.memory)?;
            }
        }
        self.loaded.retain(|index,_| self.pages.entries[*index].upper > task.range.upper);
        self.output = groups.into_iter().filter(|page|page.len() > 0).map(PageBuilder::finish).collect();
        self.output_reservation = Some(reservation);
        Ok(())
    }
}
impl PartitionScan {
    /// The scan's MATCH on a resolved row: from the accelerator's answers where they cover the row's data file, and from the row's own value otherwise.
    fn match_value(&self, row: &ResolvedRow<RowSource>, evaluated: usize, sources: &[(Option<Arc<IndexBatch>>, Option<(usize, usize)>)]) -> Result<Value> {
        let state = &self.matches[evaluated];
        let Some(query) = &state.query else { return Ok(Value::Null) };
        if let Some(answers) = &state.answers {
            let source = sources.iter().find(|(batch, _)| batch.as_ref().is_some_and(|batch| Arc::ptr_eq(batch, &row.source.batch))).and_then(|(_, source)| *source);
            if let Some((file, page)) = source {
                // An answer the file cannot give for this row is left to the row itself: an index decides cost, never results.
                if let Some(Ok(value)) = answers.file(&self.files.entries[file].key).map(|matches| matches.value(page, row.source.row)) {
                    return Ok(value.map_or(Value::Null, Value::Bool));
                }
            }
        }
        Ok(rtdb_expr::match_value(&project(row, &state.column)?, query))
    }
}
fn project_bound(row: &ResolvedRow<RowSource>,name: &str) -> Result<usize> {
    if matches!(name,"_valid_from" | "_valid_to" | "_system_from" | "_system_to" | "_iid") { return Ok(std::mem::size_of::<Value>() + 128); }
    let batch = row.source.batch.arrow();
    let schema = batch.schema();
    let op_index = schema.index_of("op").map_err(|error|invalid("event-schema",error))?;
    let op = batch.column(op_index).as_any().downcast_ref::<UnionArray>().ok_or_else(||invalid("event-schema","Operation is not a union"))?;
    let put = op.child(op.type_id(row.source.row)).as_any().downcast_ref::<StructArray>().ok_or_else(||invalid("event-schema","Visible event is not a put struct"))?;
    let DataType::Struct(fields) = put.data_type() else { return Err(invalid("event-schema","Put has no struct fields")); };
    let Some(index) = fields.iter().position(|field|field.name() == name) else { return Ok(std::mem::size_of::<Value>()); };
    let (field, column, offset) = (&fields[index], put.column(index).as_ref(), op.value_offset(row.source.row));
    if row.source.batch.put_field_type(index).is_some() { rtdb_columnar::estimate_read_cell_bytes(field, column, offset) }
    else { rtdb_columnar::estimate_read_value_bytes(field, column, offset) }
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
        TemporalFilter::FromTo { from, to } => Interval::new(value(from, Timestamp::MIN.into(), execution, &scope).await?, value(to, Timestamp::MAX.into(), execution, &scope).await?),
        TemporalFilter::Between { from, to } => Interval::between(value(from, Timestamp::MIN.into(), execution, &scope).await?, value(to, Timestamp::MAX.into(), execution, &scope).await?),
    }
}
fn project(row: &ResolvedRow<RowSource>, name: &str) -> Result<Value> {
    let time = |time: Timestamp, upper: bool| if upper && time == Timestamp::MAX { Value::Null }
        else { Value::Timestamp { value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } };
    match name {
        "_valid_from" => return Ok(time(row.valid_time.lower(), false)), "_valid_to" => return Ok(time(row.valid_time.upper(), true)),
        "_system_from" => return Ok(time(row.system_time.lower(), false)), "_system_to" => return Ok(time(row.system_time.upper(), true)),
        "_iid" => return Ok(Value::FixedSizeBinary(row.source.header()?.iid.0.to_vec())), _ => {}
    }
    let batch = row.source.batch.arrow();
    let schema = batch.schema();
    let op_index = schema.index_of("op").map_err(|error| invalid("event-schema", error))?;
    let op = batch.column(op_index).as_any().downcast_ref::<UnionArray>().ok_or_else(|| invalid("event-schema", "Operation is not a union"))?;
    let id = op.type_id(row.source.row);
    let put = op.child(id).as_any().downcast_ref::<StructArray>().ok_or_else(|| invalid("event-schema", "Visible event is not a put struct"))?;
    let DataType::Struct(fields) = put.data_type() else { return Err(invalid("event-schema", "Put has no struct fields")); };
    let Some(index) = fields.iter().position(|field| field.name() == name) else { return Ok(Value::Null); };
    let value = match row.source.batch.put_field_type(index) {
        Some(logical) => rtdb_columnar::read_execution_value_as(logical, &fields[index], put.column(index).as_ref(), op.value_offset(row.source.row)),
        None => rtdb_columnar::read_execution_value(&fields[index], put.column(index).as_ref(), op.value_offset(row.source.row)),
    };
    value.map(|value| if matches!(value, Value::Undefined) { Value::Null } else { value })
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
fn summarize_rows(rows: impl IntoIterator<Item = Result<RowSource>>) -> Result<Option<TemporalSummary>> {
    let mut result: Option<TemporalSummary> = None;
    for row in rows {
        let event = row?.header()?;
        let summary = result.get_or_insert(TemporalSummary { min_valid_from: event.valid_time.lower(), max_valid_to: event.valid_time.upper(), min_system_from: event.system_from, max_system_from: event.system_from });
        summary.min_valid_from = summary.min_valid_from.min(event.valid_time.lower()); summary.max_valid_to = summary.max_valid_to.max(event.valid_time.upper());
        summary.min_system_from = summary.min_system_from.min(event.system_from); summary.max_system_from = summary.max_system_from.max(event.system_from);
    }
    Ok(result)
}
fn may_satisfy_not_null(scan: &Scan, columns: &[Value]) -> bool {
    let [Expr::Unary { op: UnaryOp::IsNotNull, expr }] = scan.predicates.as_slice() else {
        return true;
    };
    let Expr::Column { id, .. } = expr.as_ref() else {
        return true;
    };
    if scan.matches.iter().any(|evaluated| evaluated.id == *id) { return true; }
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
    use super::*;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions, context::OuterRows, parameters::BoundParameters};
    use rtdb_format::{block::proto as pb, catalog::BlockCatalog, data::{StoredEvent, StoredOperation, build_event_batch},
        ipc::write_file, keys::{TrieKey, block_key}, metadata::MetadataBuilder};
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_plan::{Catalog, ColumnId, Field, QualifiedTable};
    use rtdb_storage::{MemoryStore, ObjectStore};
    use rtdb_trie::TrieConfig;
    use rtdb_types::{EntityId, LogicalType, ScalarType, TableRef, TransactionKey, TxId};
    use tokio_util::sync::CancellationToken;

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
                operation: StoredOperation::Put(BTreeMap::from([("_id".into(), value.clone()), ("v".into(), value)])),
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
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, matches: Vec::new(),
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
            event.operation = StoredOperation::Put(BTreeMap::from([("_id".into(), Value::I64(2)), ("v".into(), Value::I64(value))]));
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
            operation: StoredOperation::Put(BTreeMap::from([("_id".into(), Value::I64(4)), ("v".into(), Value::I64(4))])),
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
        write.put(table.clone(), BTreeMap::from([("_id".into(), Value::I64(1)), ("v".into(), Value::I64(9))]), Interval::from_micros(0, 30).unwrap()).unwrap();
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
            clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, matches: Vec::new(),
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
        let slot = std::mem::size_of::<ResolvedRow<RowSource>>();
        let previous_history = 2 * slot + 256;
        let replacement_history = 4 * slot + 256;
        for (requested, charged) in [
            (replacement_ceiling, scratch + previous_ceiling),
            (replacement_history, scratch + previous_ceiling + replacement_ceiling + previous_history),
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
