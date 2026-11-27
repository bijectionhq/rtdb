use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{Relation, RelationNode, SpanContext, TraceSpan, TraceValue, Tracer};
use rtdb_types::Result;
use crate::{QueryContext, pages::{BoxPageCursor, OwnedPage, PageCursor}};

#[allow(dead_code, reason = "Operator producer changes land independently of the trace contract")]
pub(crate) enum OperatorCounter {
    PeakMemoryBytes,
    InnerExecutions,
    PredicateScalarFallbacks,
    ProjectionScalarFallbacks,
    ScanDecodedBytes,
    ScanGatheredBytes,
    ScanSelectedRows,
    JoinFilterBuildDistinctKeys,
    JoinFilterBuildDistinctKeysComplete,
    JoinFilterAdmittedIids,
    JoinFilterCheckedHeaders,
    JoinFilterRejectedHeaders,
    SpillBytesWritten,
    WindowInputOrderReused,
}

impl OperatorCounter {
    pub(crate) fn record(self, span: &mut dyn TraceSpan, value: u64) {
        let name = match self {
            Self::PeakMemoryBytes => "cursor.peak_memory_bytes",
            Self::InnerExecutions => "cursor.apply.inner_executions",
            Self::PredicateScalarFallbacks => "cursor.predicate.scalar_fallbacks",
            Self::ProjectionScalarFallbacks => "cursor.projection.scalar_fallbacks",
            Self::ScanDecodedBytes => "cursor.scan.decoded_bytes",
            Self::ScanGatheredBytes => "cursor.scan.gathered_bytes",
            Self::ScanSelectedRows => "cursor.scan.selected_rows",
            Self::JoinFilterBuildDistinctKeys => "cursor.join_filter.build_distinct_keys",
            Self::JoinFilterBuildDistinctKeysComplete => "cursor.join_filter.build_distinct_keys_complete",
            Self::JoinFilterAdmittedIids => "cursor.join_filter.admitted_iids",
            Self::JoinFilterCheckedHeaders => "cursor.join_filter.checked_headers",
            Self::JoinFilterRejectedHeaders => "cursor.join_filter.rejected_headers",
            Self::SpillBytesWritten => "cursor.spill.bytes_written",
            Self::WindowInputOrderReused => "cursor.window.input_order_reused",
        };
        span.attribute(name, TraceValue::Unsigned(value));
    }
}

pub(crate) fn operator_evidence_enabled(context: &QueryContext) -> bool {
    context.trace_parent.is_some() && context.tracer.as_ref().is_some_and(|tracer| tracer.query_enabled())
}

pub(crate) struct QueryTrace {
    span: Box<dyn TraceSpan>,
    memory: Arc<rtdb_columnar::MemoryBudget>,
    spill: Arc<crate::spill::SpillRegion>,
}
impl QueryTrace {
    pub fn start(context: &Arc<QueryContext>, text: Option<&str>) -> Option<Self> {
        let tracer = context.tracer.as_ref().filter(|tracer| tracer.query_enabled())?;
        let mut span = tracer.start(context.trace_parent, "xtdb.query", &[])?;
        if let Some(text) = text { span.attribute("query.text", TraceValue::Text(text)); }
        if let Some((name, _)) = &context.application_name { span.attribute("application.name", TraceValue::Text(name)); }
        Some(Self { span, memory: Arc::clone(&context.memory), spill: Arc::clone(&context.spill) })
    }
    pub fn context(&self) -> SpanContext { self.span.context() }
}
impl Drop for QueryTrace {
    fn drop(&mut self) {
        self.span.attribute("query.peak_memory_bytes", TraceValue::Unsigned(self.memory.peak_bytes() as u64));
        self.span.attribute("query.spill_region_peak_bytes", TraceValue::Unsigned(self.spill.peak_bytes()));
    }
}

pub(crate) struct CursorTrace {
    tracer: Arc<dyn Tracer>, parent: SpanContext, name: String, kind: &'static str,
    span: Option<Box<dyn TraceSpan>>, started: bool, total: Duration, first: Option<Duration>,
    rows: u64, pulls: u64, end: SystemTime, pages: bool, operator_memory: Option<Arc<rtdb_columnar::MemoryBudget>>, _reservation: Arc<MemoryReservation>,
}
impl CursorTrace {
    pub fn new(context: &Arc<QueryContext>, kind: &'static str, name: &str, pages: bool) -> Option<Self> {
        let tracer = context.tracer.as_ref().filter(|tracer| tracer.query_enabled())?.clone();
        let parent = context.trace_parent?;
        let reservation = context.memory.reserve(name.len().checked_add(2048)?).ok()?;
        Some(Self { tracer, parent, name: name.into(), kind, span: None, started: false, total: Duration::ZERO,
            first: None, rows: 0, pulls: 0, end: SystemTime::now(), pages,
            operator_memory: context.operator_root.as_ref().map(|_| Arc::clone(&context.memory)), _reservation: reservation })
    }
    pub fn start(&mut self) {
        if !self.started {
            self.started = true;
            self.span = self.tracer.start(Some(self.parent), &self.name, &[("cursor.type", TraceValue::Text(self.kind))]);
            self.end = SystemTime::now();
        }
    }
    pub fn completed(&mut self, elapsed: Duration, rows: Option<usize>) {
        self.total = self.total.saturating_add(elapsed); self.end = SystemTime::now();
        if let Some(rows) = rows { self.first.get_or_insert(elapsed); self.rows = self.rows.saturating_add(rows as u64); self.pulls = self.pulls.saturating_add(1); }
    }
}
impl Drop for CursorTrace {
    fn drop(&mut self) {
        if let Some(span) = &mut self.span {
            span.attribute("cursor.total_time_ms", TraceValue::Unsigned(self.total.as_millis().min(u64::MAX as u128) as u64));
            if let Some(first) = self.first { span.attribute(if self.pages { "cursor.time_to_first_page_ms" } else { "cursor.time_to_first_pull_ms" }, TraceValue::Unsigned(first.as_millis().min(u64::MAX as u128) as u64)); }
            span.attribute(if self.pages { "cursor.page_count" } else { "cursor.pull_count" }, TraceValue::Unsigned(self.pulls));
            span.attribute("cursor.row_count", TraceValue::Unsigned(self.rows));
            if let Some(memory) = &self.operator_memory {
                OperatorCounter::PeakMemoryBytes.record(span.as_mut(), memory.peak_bytes() as u64);
            }
            span.end_at(self.end);
        }
    }
}
pub(crate) fn operator_trace(relation: &Relation, context: &Arc<QueryContext>) -> Option<CursorTrace> {
    if !operator_evidence_enabled(context) { return None; }
    let kind = crate::analysis::operator(relation);
    let extra = match relation.node() { RelationNode::Scan(scan) => scan.table.name.table.schema.len().saturating_add(scan.table.name.table.table.len()), _ => 0 };
    let _name = context.memory.reserve(extra.saturating_add(1024)).ok()?;
    let name = match relation.node() { RelationNode::Scan(scan) => format!("query.cursor.scan.{}.{}", scan.table.name.table.schema, scan.table.name.table.table), _ => format!("query.cursor.{kind}") };
    CursorTrace::new(context, kind, &name, true)
}
pub(crate) fn wrap(input: BoxPageCursor, relation: &Relation, context: &Arc<QueryContext>) -> BoxPageCursor {
    match operator_trace(relation, context) { Some(trace) => Box::new(TracedCursor { input: Some(input), trace }), None => input }
}

pub(crate) fn wrap_batches(input: crate::columnar::BoxBatchCursor, relation: &Relation, context: &Arc<QueryContext>) -> crate::columnar::BoxBatchCursor {
    match operator_trace(relation, context) { Some(trace) => Box::new(TracedBatches { input, trace }), None => input }
}
struct TracedBatches { input: crate::columnar::BoxBatchCursor, trace: CursorTrace }

#[async_trait]
impl crate::columnar::BatchCursor for TracedBatches {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        self.trace.start();
        let started = Instant::now();
        let result = self.input.next_batch().await;
        if let Ok(batch) = &result { self.trace.completed(started.elapsed(), batch.as_ref().map(|batch| batch.len)); }
        result
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> { self.input.skip_complete_pages(rows) }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}
impl Drop for TracedBatches {
    fn drop(&mut self) {
        if let Some(span) = &mut self.trace.span { self.input.trace_attributes(span.as_mut()); }
    }
}
struct TracedCursor { input: Option<BoxPageCursor>, trace: CursorTrace }
#[async_trait]
impl PageCursor for TracedCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.trace.start(); let started = Instant::now();
        let result = self.input.as_mut().expect("open traced cursor owns input").next_page().await;
        if let Ok(page) = &result { self.trace.completed(started.elapsed(), page.as_ref().map(OwnedPage::len)); }
        result
    }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.as_ref().and_then(|input| input.scan_counts()) }
}
impl Drop for TracedCursor {
    fn drop(&mut self) {
        if let (Some(input), Some(span)) = (&self.input, &mut self.trace.span) { input.trace_attributes(span.as_mut()); }
        self.input = None;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;
    use rtdb_columnar::MemoryBudget;
    use rtdb_plan::{ColumnId, Expr, Field, Relation, RelationNode, Schema, SpanContext, Tracer};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{LogicalType, ScalarType, Timestamp, Value};
    use tokio_util::sync::CancellationToken;
    use crate::{BoundParameters, CatalogSnapshot, DatabaseSnapshot, QueryContext, QueryOptions};
    use crate::columnar::BoxBatchCursor;
    use crate::context::{Execution, OuterRows};
    use crate::operators::build_column_pages;
    use crate::runtime_schema::EmittedRelation;
    use super::{CursorTrace, QueryTrace, operator_evidence_enabled, operator_trace};

    fn query_context() -> Arc<QueryContext> {
        let memory = MemoryBudget::new(1 << 24);
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) })]), false).unwrap();
        QueryContext::new(catalog, memory, Timestamp(0), "UTC", QueryOptions { list_batch_rows: 3, ..Default::default() }, &CancellationToken::new()).unwrap()
    }

    async fn open(relation: Arc<Relation>, context: Arc<QueryContext>) -> BoxBatchCursor {
        let memory = &context.memory;
        let parameters = Arc::new(BoundParameters::infer(vec![], memory).unwrap());
        let runtime = EmittedRelation::derive(relation, &context.catalog, parameters.types(), memory).unwrap();
        let execution = Execution { parameters, outer: OuterRows::empty(memory).unwrap(), context, bindings: None, analysis: None };
        build_column_pages(runtime, execution).await.unwrap().unwrap()
    }

    fn field(id: u32, name: &str, scalar: ScalarType) -> Field {
        Field { id: ColumnId(id), name: name.into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(scalar) }
    }

    #[tokio::test]
    async fn tracing_cursor_test_accumulates_only_supplied_pull_durations() {
        let context = query_context();
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 4, true, true).unwrap();
        let parent = tracer.start(None, "xtdb.query", &[]).unwrap();
        let context = context.with_tracer(Some(tracer.clone()), Some(parent.context()));
        let mut trace = CursorTrace::new(&context, "output", "query.cursor.output", true).unwrap();
        trace.start(); trace.completed(Duration::from_millis(7), Some(3)); trace.completed(Duration::from_millis(5), None); drop(trace); drop(parent); tracer.close();
        let span = receiver.next().await.unwrap();
        let attributes: BTreeMap<_, _> = span.attributes().collect();
        assert_eq!(attributes["cursor.total_time_ms"], "12");
        assert_eq!(attributes["cursor.time_to_first_page_ms"], "7");
        assert_eq!(attributes["cursor.page_count"], "1"); assert_eq!(attributes["cursor.row_count"], "3");
    }

    #[test]
    fn query_tracing_disabled_with_a_parent_leaves_reservation_peak_at_zero() {
        let parent = SpanContext { trace_id: [1; 16], span_id: [1; 8] };
        let (tracer, _receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 4, false, true).unwrap();
        let context = query_context().with_tracer(Some(tracer), Some(parent));

        assert!(!operator_evidence_enabled(&context));
        assert!(operator_trace(&Relation::unit(), &context).is_none());
        assert_eq!(context.memory.peak_bytes(), 0);
    }

    #[tokio::test]
    async fn trace_preserves_skipping_whole_series_pages() {
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 8, true, false).unwrap();
        let context = query_context().with_tracer(Some(tracer.clone()), None);
        let query = QueryTrace::start(&context, None).unwrap();
        let context = context.with_tracer(Some(tracer.clone()), Some(query.context()));
        let int = |value| Expr::Literal(Value::I64(value));
        let args = vec![int(1), int(10), int(1), Expr::Literal(Value::Bool(false))];
        let expr = Expr::Call { name: "generate_series".into(), return_type: rtdb_plan::function_type_with_args("generate_series", &args).unwrap(), args };
        let element = field(0, "n", ScalarType::I64);
        let list = Arc::new(Relation::new(Schema::new(vec![element.clone()]).unwrap(), RelationNode::List { expr, element, page_rows: None }).unwrap());
        let limit = Arc::new(list.limit(Some(int(4)), Some(int(1))).unwrap());
        let mut cursor = open(limit, context).await;

        let batch = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(batch.len, 1);
        assert_eq!(batch.columns[0].value(0).unwrap(), Value::I64(5));
        drop((cursor, query));
        tracer.close();

        let mut pulled_pages = None;
        while let Some(span) = receiver.next().await {
            if span.name == "query.cursor.list" {
                pulled_pages = span.attributes().find(|(name, _)| *name == "cursor.page_count").map(|(_, value)| value.parse::<u64>().unwrap());
            }
        }
        assert_eq!(pulled_pages, Some(1));
    }

    #[tokio::test]
    async fn query_trace_reports_spill_region_peak_after_files_close() {
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(MemoryBudget::new(1 << 20), 4, true, false).unwrap();
        let context = query_context().with_tracer(Some(tracer.clone()), None);
        let query = QueryTrace::start(&context, None).unwrap();
        let mut writer = context.spill.create().unwrap();
        writer.write_frame(b"spill").unwrap();
        let bytes = writer.position().unwrap();
        drop(writer.finish().unwrap());
        assert_eq!(context.spill_bytes(), 0);
        drop(query);
        tracer.close();

        let span = receiver.next().await.unwrap();
        let attributes: BTreeMap<_, _> = span.attributes().collect();
        assert!(bytes > 0);
        assert_eq!(attributes["query.spill_region_peak_bytes"].parse::<u64>().unwrap(), bytes);
    }
}
