use rtdb_types::StructFields;
use std::{sync::{Arc, Mutex}, time::{Duration, Instant}};
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{Relation, RelationNode};
use rtdb_types::{Keyword, Result, TimeUnit, Value};
use crate::{QueryContext, invalid, pages::{BoxPageCursor, OwnedPage, PageCursor}};

#[derive(Clone, Copy, Default)]
pub(crate) struct ScanCounts {
    pub files_pruned: u64, pub files_used: u64, pub pages_pruned: u64, pub pages_used: u64, pub rows_read: u64,
    pub join_iid_headers_checked: u64, pub join_iid_headers_rejected: u64,
}
impl ScanCounts {
    pub fn add(&mut self, other: Self) {
        self.files_pruned = self.files_pruned.saturating_add(other.files_pruned); self.files_used = self.files_used.saturating_add(other.files_used);
        self.pages_pruned = self.pages_pruned.saturating_add(other.pages_pruned); self.pages_used = self.pages_used.saturating_add(other.pages_used);
        self.rows_read = self.rows_read.saturating_add(other.rows_read);
        self.join_iid_headers_checked = self.join_iid_headers_checked.saturating_add(other.join_iid_headers_checked);
        self.join_iid_headers_rejected = self.join_iid_headers_rejected.saturating_add(other.join_iid_headers_rejected);
    }
}

#[derive(Default)]
struct Counts { peak_memory: usize, total: Duration, first: Option<Duration>, pages: i32, rows: i64, scan: ScanCounts, pushdowns: Option<MeasuredPushdowns> }
struct MeasuredPushdowns { value: Value, _reservation: Arc<MemoryReservation> }
enum Attributes { None, Scan { database: String, source: String } }

pub(crate) struct AnalysisNode {
    op: &'static str, attributes: Attributes, counts: Mutex<Counts>, children: Vec<Arc<Self>>,
    _reservation: Arc<MemoryReservation>,
}
impl AnalysisNode {
    fn observe(&self, elapsed: Duration, rows: Option<usize>, scan: Option<ScanCounts>, peak_memory: usize) -> Result<()> {
        let mut counts = self.counts.lock().map_err(|_| invalid("analysis-state", "Analysis counters were poisoned"))?;
        counts.peak_memory = counts.peak_memory.max(peak_memory);
        counts.total = counts.total.saturating_add(elapsed);
        if let Some(rows) = rows {
            counts.first.get_or_insert(elapsed);
            counts.pages = counts.pages.wrapping_add(1);
            counts.rows = counts.rows.wrapping_add(rows as i64);
        }
        if let Some(scan) = scan { counts.scan = scan; }
        Ok(())
    }

    /// Records one call of a filter fused into the operator above it. A fused stage holds no memory of its own apart
    /// from its operator's, so it reports no peak rather than the query's.
    pub fn observe_stage(&self, elapsed: Duration, rows: Option<usize>) -> Result<()> {
        self.observe(elapsed, rows, None, 0)
    }

    pub fn new(relation: &Relation, context: &Arc<QueryContext>) -> Result<Arc<Self>> {
        let reservation = context.memory.reserve(bound(relation)?)?;
        Self::build(relation, &reservation)
    }
    fn build(relation: &Relation, reservation: &Arc<MemoryReservation>) -> Result<Arc<Self>> {
        let attributes = match relation.node() {
            RelationNode::Scan(scan) if !crate::inspection::is_inspection(&scan.table.name.table) && crate::system_schema::definition(&scan.table.name.table).is_none() => Attributes::Scan { database: scan.table.name.database.clone(), source: format!("{}.{}", scan.table.name.table.schema, scan.table.name.table.table) },
            _ => Attributes::None,
        };
        let children = relation.children().iter().map(|child| Self::build(child, reservation)).collect::<Result<_>>()?;
        Ok(Arc::new(Self { op: operator(relation), attributes, counts: Mutex::new(Counts::default()), children, _reservation: Arc::clone(reservation) }))
    }
    pub fn child(&self, index: usize) -> Option<Arc<Self>> { self.children.get(index).cloned() }
    pub fn set_pushdowns(&self, pushdowns: &crate::pushdown::Pushdowns, fields: &[rtdb_plan::Field], canonical_entity_iids: bool, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<()> {
        let reservation = memory.reserve(pushdowns.description_bound(fields)?)?;
        let value = pushdowns.describe(fields, canonical_entity_iids)?;
        self.counts.lock().map_err(|_| invalid("analysis-state", "Analysis counters were poisoned"))?.pushdowns = Some(MeasuredPushdowns { value, _reservation: reservation });
        Ok(())
    }
    pub fn rows(&self, peak: usize) -> Result<Vec<Vec<Value>>> {
        let mut rows = Vec::new(); self.append_rows(0, peak, &mut rows)?; Ok(rows)
    }
    fn append_rows(&self, depth: usize, peak: usize, rows: &mut Vec<Vec<Value>>) -> Result<()> {
        let mut counts = self.counts.lock().map_err(|_| invalid("analysis-state", "Analysis counters were poisoned"))?;
        let child_time = self.children.iter().try_fold(0i64, |sum, child| {
            let counts = child.counts.lock().map_err(|_| invalid("analysis-state", "Analysis counters were poisoned"))?;
            Ok::<_, rtdb_types::Anomaly>(sum.saturating_add(micros(counts.total)))
        })?;
        let attributes = match &self.attributes {
            Attributes::None => Value::Null,
            Attributes::Scan { database, source } => Value::Struct(StructFields::from([
                ("scan_db".into(), Value::Utf8(database.clone())), ("scan_source".into(), Value::Utf8(source.clone())),
                ("scan_files_pruned".into(), integer(counts.scan.files_pruned)), ("scan_files_used".into(), integer(counts.scan.files_used)),
                ("scan_pages_pruned".into(), integer(counts.scan.pages_pruned)), ("scan_pages_used".into(), integer(counts.scan.pages_used)),
                ("scan_rows_read".into(), integer(counts.scan.rows_read)),
            ])),
        };
        rows.push(vec![Value::Utf8(format!("{}->", "  ".repeat(depth))), Value::Keyword(Keyword::from_name(self.op)), attributes,
            duration(micros(counts.total)), duration(micros(counts.total).saturating_sub(child_time)), counts.first.map_or(Value::Null, |time| duration(micros(time))),
            Value::I64(i64::from(counts.pages)), Value::I64(counts.rows), counts.pushdowns.take().map_or(Value::Null, |pushdowns| pushdowns.value), integer(counts.peak_memory as u64), integer(peak as u64)]);
        drop(counts);
        for child in &self.children { child.append_rows(depth + 1, peak, rows)?; }
        Ok(())
    }
    pub fn output_bound(&self) -> Result<usize> {
        self.output_bound_at(0)
    }
    fn output_bound_at(&self, depth: usize) -> Result<usize> {
        let own = match &self.attributes { Attributes::None => 8192, Attributes::Scan { database, source } => 8192usize.checked_add(database.len()).and_then(|n| n.checked_add(source.len())).ok_or_else(size_error)? };
        let pushdowns = self.counts.lock().map_err(|_| invalid("analysis-state", "Analysis counters were poisoned"))?.pushdowns.as_ref().map(|value| value.value.retained_bytes()).transpose()?.unwrap_or(0);
        let own = own.checked_add(depth.checked_mul(4).ok_or_else(size_error)?).and_then(|bytes| bytes.checked_add(pushdowns)).ok_or_else(size_error)?;
        self.children.iter().try_fold(own, |bytes, child| bytes.checked_add(child.output_bound_at(depth.checked_add(1).ok_or_else(size_error)?)?).ok_or_else(size_error))
    }
}
fn bound(relation: &Relation) -> Result<usize> {
    let own = match relation.node() { RelationNode::Scan(scan) => 4096usize.checked_add(scan.table.name.database.len()).and_then(|n| n.checked_add(scan.table.name.table.schema.len())).and_then(|n| n.checked_add(scan.table.name.table.table.len())).ok_or_else(size_error)?, _ => 4096 };
    relation.children().iter().try_fold(own, |bytes, child| bytes.checked_add(bound(child)?).ok_or_else(size_error))
}
fn size_error() -> rtdb_types::Anomaly { invalid("analysis-size", "Analysis metadata exceeds addressable memory") }
fn micros(value: Duration) -> i64 { value.as_micros().min(i64::MAX as u128) as i64 }
fn integer(value: u64) -> Value { Value::I64(value.min(i64::MAX as u64) as i64) }
fn duration(value: i64) -> Value { Value::Duration { value, unit: TimeUnit::Microsecond } }

pub(crate) fn operator(relation: &Relation) -> &'static str {
    match relation.node() {
        RelationNode::Values { .. } | RelationNode::ColumnTable { .. } => "table", RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } => "pages", RelationNode::Scan(_) => "scan", RelationNode::Project { .. } | RelationNode::Extend { .. } => "project",
        RelationNode::Rename { .. } => "rename", RelationNode::Let { .. } => "let", RelationNode::Reference { .. } => "relation",
        RelationNode::Filter { .. } => "select", RelationNode::Limit { .. } => "top", RelationNode::Distinct { .. } => "distinct",
        RelationNode::Sort { .. } => "order-by", RelationNode::SortLimit { .. } => "sort", RelationNode::Join { kind, .. } => match kind { rtdb_plan::JoinKind::Inner => "inner-join", rtdb_plan::JoinKind::Cross => "cross-join", rtdb_plan::JoinKind::Left | rtdb_plan::JoinKind::Right => "left-join", rtdb_plan::JoinKind::Full => "full-join", rtdb_plan::JoinKind::Semi => "semi-join", rtdb_plan::JoinKind::Anti => "anti-join", rtdb_plan::JoinKind::Single => "single-join" }, RelationNode::Aggregate { .. } => "group-by", RelationNode::Window { .. } => "window",
        RelationNode::Set { operator, .. } => match operator { rtdb_plan::SetOperator::Union => "union-all", rtdb_plan::SetOperator::Intersect => "intersection", rtdb_plan::SetOperator::Except => "difference" }, RelationNode::Unnest { .. } => "unnest", RelationNode::List { .. } => "list", RelationNode::Ordinality { .. } => "map",
        RelationNode::Mark { arguments, .. } => if arguments.is_empty() { "mark-join" } else { "apply-mark-join" },
        RelationNode::Apply { kind, .. } => match kind { rtdb_plan::ApplyKind::Inner => "apply-cross-join", rtdb_plan::ApplyKind::Left => "apply-left-join", rtdb_plan::ApplyKind::Semi => "apply-semi-join", rtdb_plan::ApplyKind::Anti => "apply-anti-join", rtdb_plan::ApplyKind::Exists | rtdb_plan::ApplyKind::Mark => "apply-mark-join", rtdb_plan::ApplyKind::Single | rtdb_plan::ApplyKind::Scalar | rtdb_plan::ApplyKind::Pull | rtdb_plan::ApplyKind::PullMany => "apply-single-join" },
        RelationNode::PatchGaps { .. } => "patch-gaps",
    }
}
pub(crate) fn wrap(input: BoxPageCursor, node: Option<Arc<AnalysisNode>>, memory: &Arc<rtdb_columnar::MemoryBudget>) -> BoxPageCursor {
    match node { Some(node) => Box::new(Measured { input, node, memory: Arc::clone(memory) }), None => input }
}
struct Measured { input: BoxPageCursor, node: Arc<AnalysisNode>, memory: Arc<rtdb_columnar::MemoryBudget> }
#[async_trait]
impl PageCursor for Measured {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let started = Instant::now();
        let result = self.input.next_page().await;
        self.node.observe(started.elapsed(), result.as_ref().ok().and_then(|page| page.as_ref()).map(OwnedPage::len), self.input.scan_counts(), self.memory.peak_bytes())?;
        result
    }
    fn scan_counts(&self) -> Option<ScanCounts> { self.input.scan_counts() }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
}

pub(crate) fn wrap_batches(input: crate::columnar::BoxBatchCursor, node: Option<Arc<AnalysisNode>>, memory: &Arc<rtdb_columnar::MemoryBudget>) -> crate::columnar::BoxBatchCursor {
    match node { Some(node) => Box::new(MeasuredBatches { input, node, memory: Arc::clone(memory) }), None => input }
}

struct MeasuredBatches { input: crate::columnar::BoxBatchCursor, node: Arc<AnalysisNode>, memory: Arc<rtdb_columnar::MemoryBudget> }

#[async_trait]
impl crate::columnar::BatchCursor for MeasuredBatches {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        let started = Instant::now();
        let result = self.input.next_batch().await;
        self.node.observe(started.elapsed(), result.as_ref().ok().and_then(|batch| batch.as_ref()).map(|batch| batch.len), self.input.scan_counts(), self.memory.peak_bytes())?;
        result
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> {
        self.input.skip_complete_pages(rows)
    }
    fn scan_counts(&self) -> Option<ScanCounts> { self.input.scan_counts() }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
}
