use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{Array, StructArray, UnionArray};
use arrow_schema::DataType;
use async_trait::async_trait;
use rtdb_columnar::{MemoryReservation, OwnedBatch, estimate_builder_bytes, from_field, read_value};
use rtdb_format::{ipc::{ArrowFile, inspect_file}, metadata::{MetadataFile, MetadataNode}};
use rtdb_index::{IndexBatch, LiveTable, RowSource, Snapshot};
use rtdb_plan::{Scan, Schema, TemporalFilter};
use rtdb_storage::{ObjectKey, cache::BufferPool};
use rtdb_temporal::{HistoryResolver, ResolvedRow};
use rtdb_trie::{PageSummary, PruningOptions, TemporalSummary, select_pages};
use rtdb_types::{Anomaly, EventHeader, Iid, Interval, Result, TemporalBounds, TimeUnit, Timestamp, Value};

use crate::{context::Execution, cursor::{OwnedRow, RowCursor}, invalid};

enum DataState { Unopened, Open { reader: ArrowFile, _reservation: Arc<MemoryReservation> } }
struct DataFile { storage: Arc<BufferPool>, key: ObjectKey, state: DataState }

enum PageSource { Live { table: Arc<LiveTable>, rows: Arc<[u32]> }, File { file: usize, page: usize } }
struct Page { lower: Iid, upper: Iid, priority: usize, source: PageSource, summary: PageSummary }
enum ActiveRows { Live { table: Arc<LiveTable>, rows: Arc<[u32]> }, File(Arc<IndexBatch>) }
impl ActiveRows {
    fn len(&self) -> usize { match self { Self::Live { rows, .. } => rows.len(), Self::File(batch) => batch.len() } }
    fn row(&self, index: usize) -> Result<RowSource> {
        match self { Self::Live { table, rows } => table.row(rows[index]), Self::File(batch) => Ok(RowSource { batch: Arc::clone(batch), row: index }) }
    }
}
struct ActivePage { rows: ActiveRows, next: usize }

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
    pages: VecDeque<Page>,
    active: BTreeMap<usize, ActivePage>,
    next_active: usize,
    heads: BinaryHeap<Head>,
    resolver: HistoryResolver,
    output: VecDeque<ResolvedRow<RowSource>>,
}

pub(crate) struct ScanCursor { partitions: VecDeque<PartitionScan> }
impl ScanCursor {
    pub fn new(scan: Scan, execution: Execution) -> Result<Self> {
        let database = execution.context.catalog.database(&scan.table.name.database)?;
        let partitions = database.partitions.iter().map(|snapshot| {
            let bounds = bounds(&scan, snapshot, &execution)?;
            Ok(PartitionScan { snapshot: snapshot.clone(), storage: Arc::clone(&database.storage), scan: scan.clone(), execution: execution.clone(), bounds,
                initialized: false, files: Vec::new(), pages: VecDeque::new(), active: BTreeMap::new(), next_active: 0, heads: BinaryHeap::new(),
                resolver: HistoryResolver::new(bounds, scan.clamp_valid_time), output: VecDeque::new() })
        }).collect::<Result<_>>()?;
        Ok(Self { partitions })
    }
}

#[async_trait]
impl RowCursor for ScanCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        while let Some(partition) = self.partitions.front_mut() {
            if let Some(row) = partition.next().await? { return Ok(Some(row)); }
            self.partitions.pop_front();
        }
        Ok(None)
    }
}

impl PartitionScan {
    fn pruning(&self) -> PruningOptions {
        PruningOptions { bounds: self.bounds, projects_temporal_columns: self.scan.columns.iter().any(|field| matches!(field.name.as_str(), "_valid_from" | "_valid_to" | "_system_from" | "_system_to")),
            clamp_valid_time: self.scan.clamp_valid_time, bypass_temporal_pruning: self.execution.context.options.bypass_temporal_pruning,
            iid: self.scan.iids.as_ref().filter(|iids| iids.len() == 1).and_then(|iids| iids.first().copied()) }
    }
    fn path_matches(&self, lower: Iid, upper: Iid) -> bool {
        self.scan.iids.as_ref().is_none_or(|iids| iids.range(lower..=upper).next().is_some())
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
                pages.push(Page { lower, upper, priority, source: PageSource::Live { table: Arc::clone(segment), rows },
                    summary: PageSummary { page_index: 0, temporal, recency: Some(Timestamp::MAX), iid_range: Some((lower, upper)) } });
            }
        }
        let mut combined = summaries;
        for (index, page) in pages.iter().enumerate() {
            let mut summary = page.summary.clone();
            summary.page_index = u32::try_from(tries.len().checked_add(index).ok_or_else(|| invalid("page-count", "Candidate count overflow"))?).map_err(|error| invalid("page-count", error))?;
            combined.push(summary);
        }
        let selected: Vec<_> = select_pages(&combined, self.pruning(), |_| true).iter().map(|summary| summary.page_index as usize).filter(|index| *index < tries.len()).collect();
        for index in selected {
            let trie = &tries[index];
            let (lower, upper) = path_range(trie.key.part())?;
            if !self.path_matches(lower, upper) { continue; }
            let slug = slug.as_ref().ok_or_else(|| invalid("missing-table-slug", "Persisted trie has no recorded table registry entry"))?;
            let key = ObjectKey::new(slug.metadata_key(&trie.key))?;
            let lease = self.storage.get(&key).await?;
            let bound = lease.as_slice().len().checked_mul(64).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("metadata-size", "Metadata allocation bound overflow"))?;
            let _reservation = self.execution.context.memory.reserve(bound)?;
            let mut cursor = Cursor::new(lease);
            if !inspect_file(&mut cursor)?.uncompressed { return Err(Anomaly::unsupported("rtdb.query/compressed-trie", "JVM trie storage uses uncompressed Arrow pages")); }
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
                        source: PageSource::File { file, page: *data_page_index }, summary: PageSummary { page_index: 0, temporal: page_temporal(columns),
                            recency: Some(recency), iid_range: Some((lower, upper)) } }),
                }
            }
        }
        for (index, page) in pages.iter_mut().enumerate() { page.summary.page_index = u32::try_from(index).map_err(|error| invalid("page-count", error))?; }
        let summaries: Vec<_> = pages.iter().map(|page| page.summary.clone()).collect();
        let selected: std::collections::BTreeSet<_> = select_pages(&summaries, self.pruning(), |_| true).iter().map(|summary| summary.page_index).collect();
        pages.retain(|page| selected.contains(&page.summary.page_index));
        pages.sort_by(|left, right| left.lower.cmp(&right.lower).then_with(|| left.priority.cmp(&right.priority)));
        self.pages = pages.into();
        self.initialized = true;
        Ok(())
    }
    async fn activate(&mut self, page: Page) -> Result<()> {
        self.execution.context.check_cancelled()?;
        let rows = match page.source {
            PageSource::Live { table, rows } => ActiveRows::Live { table, rows },
            PageSource::File { file, page: page_index } => {
                let data = &mut self.files[file];
                if matches!(&data.state, DataState::Unopened) {
                    let lease = data.storage.get(&data.key).await?;
                    let mut cursor = Cursor::new(lease);
                    let layout = inspect_file(&mut cursor)?;
                    if !layout.uncompressed { return Err(Anomaly::unsupported("rtdb.query/compressed-trie", "JVM trie storage uses uncompressed Arrow pages")); }
                    let bound = layout.dictionary_encoded_bytes.checked_mul(4).and_then(|bytes| layout.footer_encoded_bytes.checked_mul(64).and_then(|footer| bytes.checked_add(footer)))
                        .and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("file-size", "Arrow file allocation bound overflow"))?;
                    let reservation = self.execution.context.memory.reserve(bound)?;
                    data.state = DataState::Open { reader: ArrowFile::from_reader(cursor, None)?, _reservation: reservation };
                }
                let DataState::Open { reader, .. } = &mut data.state else { return Err(invalid("file-state", "Data file failed to open")); };
                let mut bound = reader.encoded_page_size(page_index)?.checked_mul(4).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| invalid("page-size", "Arrow page allocation bound overflow"))?;
                for field in reader.schema().fields() {
                    bound = bound.checked_add(estimate_builder_bytes(&from_field(field)?, &[])?).ok_or_else(|| invalid("page-size", "Arrow schema allocation bound overflow"))?;
                }
                let reservation = self.execution.context.memory.reserve(bound)?;
                let batch = IndexBatch::from_owned(OwnedBatch::with_reservation(reader.read_page(page_index)?, reservation)?)?;
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
        if rows.len() > 0 {
            let first = rows.row(0)?;
            let active = self.next_active;
            self.next_active = self.next_active.checked_add(1).ok_or_else(|| invalid("page-count", "Active page index overflow"))?;
            self.heads.push(Head { header: first.header()?, active, priority: page.priority, ordinal: 0 });
            self.active.insert(active, ActivePage { rows, next: 0 });
        }
        Ok(())
    }
    async fn next_event(&mut self) -> Result<Option<(EventHeader, RowSource)>> {
        loop {
            let activate = match (self.pages.front(), self.heads.peek()) { (Some(page), Some(head)) => page.lower <= head.header.iid, (Some(_), None) => true, _ => false };
            if activate { if let Some(page) = self.pages.pop_front() { self.activate(page).await?; } continue; }
            let Some(head) = self.heads.pop() else { return Ok(None); };
            let active = self.active.get_mut(&head.active).ok_or_else(|| invalid("merge-state", "Event heap lost its active page"))?;
            let source = active.rows.row(active.next)?;
            active.next += 1;
            if active.next < active.rows.len() {
                let next = active.rows.row(active.next)?;
                self.heads.push(Head { header: next.header()?, ordinal: active.next, ..head });
            } else { self.active.remove(&head.active); }
            if self.scan.iids.as_ref().is_some_and(|iids| !iids.contains(&head.header.iid)) { continue; }
            return Ok(Some((head.header, source)));
        }
    }
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        if !self.initialized { self.initialize().await?; }
        loop {
            self.execution.context.check_cancelled()?;
            if let Some(row) = self.output.pop_front() {
                let values = self.scan.columns.iter().map(|field| project(&row, &field.name)).collect::<Result<_>>()?;
                return OwnedRow::new(values, &self.execution.context.memory).map(Some);
            }
            let Some((header, source)) = self.next_event().await? else { return Ok(None); };
            self.output.extend(self.resolver.resolve(&header, source)?);
        }
    }
}

fn bounds(scan: &Scan, snapshot: &Snapshot, execution: &Execution) -> Result<TemporalBounds> {
    let valid_time = dimension(&scan.valid_time, execution.context.current_time, execution)?;
    let basis = snapshot.system_time_cap();
    let default = basis.unwrap_or(execution.context.current_time);
    let system = dimension(&scan.system_time, default, execution)?;
    let upper = basis.map(|basis| basis.0.saturating_add(1)).map(Timestamp).unwrap_or(Timestamp::MIN).min(system.upper());
    let system_time = Interval::new(system.lower().min(upper), upper)?;
    Ok(TemporalBounds { valid_time, system_time })
}
fn dimension(filter: &TemporalFilter, default: Timestamp, execution: &Execution) -> Result<Interval> {
    let value = |expression: &rtdb_plan::Expr, fallback: Timestamp| -> Result<Timestamp> {
        let value = execution.evaluate(expression, &Schema::default(), &[])?;
        if matches!(value, Value::Null | Value::Undefined) { Ok(fallback) } else { rtdb_expr::timestamp_micros(&value, &execution.context.timezone) }
    };
    let point = |time: Timestamp| Interval::new(time, Timestamp(time.0.saturating_add(1)));
    match filter {
        TemporalFilter::Default => point(default), TemporalFilter::All => Ok(Interval::ALL),
        TemporalFilter::At(at) => point(value(at, execution.context.current_time)?),
        TemporalFilter::FromTo { from, to } => Interval::new(value(from, Timestamp::MIN)?, value(to, Timestamp::MAX)?),
        TemporalFilter::Between { from, to } => Interval::between(value(from, Timestamp::MIN)?, value(to, Timestamp::MAX)?),
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
    read_value(&fields[index], put.column(index).as_ref(), op.value_offset(row.source.row)).map(|value| if matches!(value, Value::Undefined) { Value::Null } else { value })
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
fn recency(date: Option<chrono::NaiveDate>) -> Result<Timestamp> {
    date.map(|date| date.and_hms_opt(0, 0, 0).map(|time| Timestamp(time.and_utc().timestamp_micros())).ok_or_else(|| invalid("recency", "Invalid trie recency date"))).unwrap_or(Ok(Timestamp::MAX))
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
