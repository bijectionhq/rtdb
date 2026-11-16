use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, StructArray, UnionArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use rtdb_columnar::{MemoryReservation, OwnedBatch, estimate_builder_bytes, from_field};
use rtdb_format::{ipc::{ArrowFile, inspect_file_reserved}, metadata::{MetadataFile, MetadataNode}};
use rtdb_index::{IndexBatch, LiveTable, RowSource, Snapshot};
use rtdb_plan::{Scan, Schema, TemporalFilter};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_temporal::{HistoryResolver, ResolvedRow};
use rtdb_trie::{PageSummary, PruningOptions, TemporalSummary, select_pages};
use rtdb_types::{Anomaly, EventHeader, Iid, Interval, Result, TemporalBounds, TimeUnit, Timestamp, Value};

use crate::{context::Execution, cursor::OwnedRow, pages::{OwnedPage,PageBuilder,PageCursor}, scan_tasks::{PageRange,ScanTask,ScanTaskPlan}, invalid};

enum DataState { Unopened, Open { reader: ArrowFile, _reservation: Arc<MemoryReservation> } }
struct DataFile { storage: Arc<BufferPool>, key: ObjectKey, state: DataState }

enum PageSource { Live { table: Arc<LiveTable>, rows: Arc<[u32]> }, File { file: usize, page: usize } }
struct Page { lower: Iid, upper: Iid, priority: usize, source: PageSource, summary: PageSummary, content_matches: bool }
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
    files: Vec<DataFile>,
    pages: Vec<Page>,
    tasks: Option<ScanTaskPlan>,
    loaded: BTreeMap<usize, LoadedPage>,
    output: VecDeque<OwnedPage>,
    output_reservation: Option<Arc<MemoryReservation>>,
    counts: ScanCounts,
    pushdowns: Option<Arc<crate::pushdown::Pushdowns>>,
}

use crate::analysis::ScanCounts;
pub(crate) struct ScanCursor { partitions: VecDeque<PartitionScan>, completed: ScanCounts, database: String, source: String, _reservation: Arc<MemoryReservation> }
impl ScanCursor {
    pub async fn new(scan: Scan, execution: Execution, pushdowns: Option<Arc<crate::pushdown::Pushdowns>>) -> Result<Self> {
        let bytes = scan.table.name.database.len().checked_add(scan.table.name.table.schema.len()).and_then(|bytes| bytes.checked_add(scan.table.name.table.table.len())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(|| invalid("scan-size", "Scan labels exceed allocation range"))?;
        let reservation = execution.context.memory.reserve(bytes)?;
        if let (Some(analysis), Some(pushdowns)) = (&execution.analysis, &pushdowns) {
            analysis.set_pushdowns(pushdowns, &scan.columns, &execution.context.memory)?;
        }
        let name = scan.table.name.database.clone();
        let source = format!("{}.{}", scan.table.name.table.schema, scan.table.name.table.table);
        let database = execution.context.catalog.database(&scan.table.name.database)?;
        let mut partitions = VecDeque::new();
        for snapshot in &database.partitions {
            let bounds = bounds(&scan, snapshot, &execution).await?;
            partitions.push_back(PartitionScan { snapshot: snapshot.clone(), storage: Arc::clone(&database.storage), scan: scan.clone(), execution: execution.clone(), bounds,
                initialized: false, files: Vec::new(), pages: Vec::new(), tasks: None, loaded: BTreeMap::new(),
                output: VecDeque::new(), output_reservation: None, counts: ScanCounts::default(), pushdowns: pushdowns.clone() });
        }
        Ok(Self { partitions, completed: ScanCounts::default(), database: name, source, _reservation: reservation })
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
            None => self.pushdowns.as_ref().is_none_or(|filters| filters.path_matches(&self.scan.columns, lower, upper)),
        }
    }
    async fn initialize(&mut self) -> Result<()> {
        self.execution.context.check_cancelled()?;
        if self.scan.iids.as_ref().is_some_and(|iids| iids.is_empty()) || self.bounds.valid_time.is_empty() || self.bounds.system_time.is_empty() {
            self.initialized = true;
            return Ok(());
        }
        let table = &self.scan.table.name.table;
        let mut tries: Vec<_> = self.snapshot.trie_entries(table).into_iter().cloned().collect();
        tries.sort_by(|left, right| right.key.block_index().cmp(&left.key.block_index()).then_with(|| left.key.cmp(&right.key)));
        let summaries: Vec<_> = tries.iter().enumerate().map(|(index, trie)| {
            let temporal = trie.metadata.as_ref().and_then(|metadata| metadata.temporal_metadata.as_ref()).map(|metadata| TemporalSummary {
                min_valid_from: Timestamp(metadata.min_valid_from.unwrap_or(i64::MIN)), max_valid_to: Timestamp(metadata.max_valid_to.unwrap_or(i64::MAX)),
                min_system_from: Timestamp(metadata.min_system_from.unwrap_or(i64::MIN)), max_system_from: Timestamp(metadata.max_system_from.unwrap_or(i64::MAX)),
            });
            Ok(PageSummary { page_index: u32::try_from(index).map_err(|error| invalid("file-count", error))?, temporal,
                recency: Some(recency(trie.key.recency())?), iid_range: Some(path_range(trie.key.part())?) })
        }).collect::<Result<_>>()?;
        let slug = self.snapshot.table_entry(table).map(|entry| entry.slug.clone());
        let mut pages = Vec::new();
        let live = self.snapshot.segments(table);
        for (priority, segment) in live.iter().enumerate() {
            for leaf in segment.trie().leaves() {
                let (lower, upper) = path_range(leaf.path().as_slice())?;
                if !self.path_matches(lower, upper) { continue; }
                let rows = leaf.sorted_rows(segment.as_ref())?;
                let temporal = summarize_rows(rows.iter().map(|row| segment.row(*row)))?;
                pages.push(Page { lower, upper, priority, source: PageSource::Live { table: Arc::clone(segment), rows }, content_matches: true,
                    summary: PageSummary { page_index: 0, temporal, recency: Some(Timestamp::MAX), iid_range: Some((lower, upper)) } });
            }
        }
        let mut combined = summaries;
        for (index, page) in pages.iter().enumerate() {
            let mut summary = page.summary.clone();
            summary.page_index = u32::try_from(tries.len().checked_add(index).ok_or_else(|| invalid("page-count", "Candidate count overflow"))?).map_err(|error| invalid("page-count", error))?;
            combined.push(summary);
        }
        let selected: Vec<_> = select_pages(&combined, self.pruning(), |_| true).iter().filter(|summary| summary.iid_range.is_none_or(|(lower, upper)| self.path_matches(lower, upper))).map(|summary| summary.page_index as usize).filter(|index| *index < tries.len()).collect();
        self.counts.files_pruned = (tries.len() - selected.len()) as u64;
        self.counts.files_used = selected.len() as u64;
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
            let file = self.files.len();
            self.files.push(DataFile { storage: Arc::clone(&self.storage), key: ObjectKey::new(slug.data_key(&trie.key))?, state: DataState::Unopened });
            let trie_recency = recency(trie.key.recency())?;
            let mut pending = metadata.root_index().map(|root| vec![(root, Vec::new(), trie_recency)]).unwrap_or_default();
            while let Some((node, path, recency)) = pending.pop() {
                self.execution.context.check_cancelled()?;
                let (lower, upper) = path_range(&path)?;
                if !self.path_matches(lower, upper) { continue; }
                match &metadata.nodes()[node] {
                    MetadataNode::Nil => {}
                    MetadataNode::IidBranch(children) => for (bucket, child) in children.iter().enumerate().rev() {
                        if let Some(child) = child { let mut path = path.clone(); path.push(bucket as u8); pending.push((*child, path, recency)); }
                    },
                    MetadataNode::RecencyBranch(children) => for (upper, child) in children.iter().rev() { pending.push((*child, path.clone(), recency.min(*upper))); },
                    MetadataNode::Leaf { data_page_index, columns } => pages.push(Page { lower, upper, priority: live.len() + index,
                        content_matches: self.pushdowns.as_ref().map(|filters| filters.page_matches(&self.scan.columns, columns, &self.execution.context.memory)).transpose()?.unwrap_or(true),
                        source: PageSource::File { file, page: *data_page_index }, summary: PageSummary { page_index: 0, temporal: page_temporal(columns),
                            recency: Some(recency), iid_range: Some((lower, upper)) } }),
                }
            }
        }
        for (index, page) in pages.iter_mut().enumerate() { page.summary.page_index = u32::try_from(index).map_err(|error| invalid("page-count", error))?; }
        self.tasks = Some(ScanTaskPlan::new(pages.iter().map(|page|PageRange { lower: page.lower,upper: page.upper }),&self.execution.context.memory)?);
        self.pages = pages;
        self.initialized = true;
        Ok(())
    }
    async fn load(&mut self, index: usize) -> Result<()> {
        self.execution.context.check_cancelled()?;
        if self.loaded.contains_key(&index) { return Ok(()); }
        let reservation = self.execution.context.memory.reserve(std::mem::size_of::<LoadedPage>() + 256)?;
        let page = &self.pages[index];
        let rows = match &page.source {
            PageSource::Live { table, rows } => ActiveRows::Live { table: Arc::clone(table),rows: Arc::clone(rows) },
            PageSource::File { file, page: page_index } => {
                let data = &mut self.files[*file];
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
        let summaries: Vec<_> = task.pages.iter().map(|index|self.pages[*index].summary.clone()).collect();
        let mut selected: Vec<_> = select_pages(&summaries,self.pruning(),|summary|self.pages[summary.page_index as usize].content_matches).into_iter().map(|summary|summary.page_index as usize).collect();
        self.counts.pages_pruned = self.counts.pages_pruned.saturating_add((task.pages.len() - selected.len()) as u64);
        self.counts.pages_used = self.counts.pages_used.saturating_add(selected.len() as u64);
        selected.sort_by_key(|index| (matches!(self.pages[*index].source,PageSource::Live { .. }),self.pages[*index].priority,*index));
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
                if header.iid >= task.range.lower { heads.push(Head { header,active: group,priority: self.pages[index].priority,ordinal });break; }
            }
        }
        let mut resolver = HistoryResolver::new(self.bounds,self.scan.clamp_valid_time);
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
            if self.scan.iids.is_none() && self.pushdowns.as_ref().is_some_and(|filters| !filters.iid_matches(&self.scan.columns,head.header.iid)) { continue; }
            for row in resolver.resolve(&head.header,source)? {
                let bound = self.scan.columns.iter().try_fold(256usize,|bound,field|bound.checked_add(project_bound(&row,&field.name)?).ok_or_else(||invalid("scan-row-size","Scan row allocation exceeds addressable memory")))?;
                let reservation = self.execution.context.memory.reserve(bound)?;
                let values = self.scan.columns.iter().map(|field|project(&row,&field.name)).collect::<Result<_>>()?;
                groups[head.active].push_growing(OwnedRow::with_reservation(values,reservation)?,&self.execution.context.memory)?;
            }
        }
        self.loaded.retain(|index,_| self.pages[*index].upper > task.range.upper);
        self.output = groups.into_iter().filter(|page|page.len() > 0).map(PageBuilder::finish).collect();
        self.output_reservation = Some(reservation);
        Ok(())
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
    rtdb_columnar::estimate_read_value_bytes(&fields[index],put.column(index).as_ref(),op.value_offset(row.source.row))
}

async fn bounds(scan: &Scan, snapshot: &Snapshot, execution: &Execution) -> Result<TemporalBounds> {
    let valid_time = dimension(&scan.valid_time, execution.context.current_time, execution).await?;
    let basis = snapshot.system_time_cap();
    let system = if basis.is_none() && matches!(scan.system_time, TemporalFilter::Default) { Interval::ALL }
        else { dimension(&scan.system_time, basis.map(Into::into).unwrap_or(execution.context.current_time), execution).await? };
    let upper = basis.map(|basis| basis.0.saturating_add(1)).map(Timestamp).unwrap_or(Timestamp::MAX).min(system.upper());
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
    rtdb_columnar::read_execution_value(&fields[index], put.column(index).as_ref(), op.value_offset(row.source.row)).map(|value| if matches!(value, Value::Undefined) { Value::Null } else { value })
}
fn path_range(path: &[u8]) -> Result<(Iid, Iid)> {
    if path.len() > 64 || path.iter().any(|bucket| *bucket > 3) { return Err(invalid("iid-path", "Trie path exceeds 64 base-four buckets")); }
    let mut lower = [0u8; 16];
    let mut upper = [u8::MAX; 16];
    for (depth, bucket) in path.iter().enumerate() {
        let shift = 6 - 2 * (depth % 4);
        lower[depth / 4] |= bucket << shift;
        upper[depth / 4] = (upper[depth / 4] & !(3 << shift)) | (bucket << shift);
    }
    Ok((Iid(lower), Iid(upper)))
}
fn recency(date: Option<rtdb_types::calendar::CivilDate>) -> Result<Timestamp> {
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
