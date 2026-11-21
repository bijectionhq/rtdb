use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use async_trait::async_trait;
use rtdb_columnar::MemoryReservation;
use rtdb_plan::{Relation, RelationNode, SpanContext, TraceSpan, TraceValue, Tracer};
use rtdb_types::Result;
use crate::{QueryContext, pages::{BoxPageCursor, OwnedPage, PageCursor}};

pub(crate) struct QueryTrace { span: Box<dyn TraceSpan>, memory: Arc<rtdb_columnar::MemoryBudget> }
impl QueryTrace {
    pub fn start(context: &Arc<QueryContext>, text: Option<&str>) -> Option<Self> {
        let tracer = context.tracer.as_ref().filter(|tracer| tracer.query_enabled())?;
        let span = match text { Some(text) => tracer.start(context.trace_parent, "xtdb.query", &[("query.text", TraceValue::Text(text))]), None => tracer.start(context.trace_parent, "xtdb.query", &[]) }?;
        Some(Self { span, memory: Arc::clone(&context.memory) })
    }
    pub fn context(&self) -> SpanContext { self.span.context() }
}
impl Drop for QueryTrace { fn drop(&mut self) { self.span.attribute("query.peak_memory_bytes", TraceValue::Unsigned(self.memory.peak_bytes() as u64)); } }

pub(crate) struct CursorTrace {
    tracer: Arc<dyn Tracer>, parent: SpanContext, name: String, kind: &'static str,
    span: Option<Box<dyn TraceSpan>>, started: bool, total: Duration, first: Option<Duration>,
    rows: u64, pulls: u64, end: SystemTime, pages: bool, _reservation: Arc<MemoryReservation>,
}
impl CursorTrace {
    pub fn new(context: &Arc<QueryContext>, kind: &'static str, name: &str, pages: bool) -> Option<Self> {
        let tracer = context.tracer.as_ref().filter(|tracer| tracer.query_enabled())?.clone();
        let parent = context.trace_parent?;
        let reservation = context.memory.reserve(name.len().checked_add(2048)?).ok()?;
        Some(Self { tracer, parent, name: name.into(), kind, span: None, started: false, total: Duration::ZERO,
            first: None, rows: 0, pulls: 0, end: SystemTime::now(), pages, _reservation: reservation })
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
            span.end_at(self.end);
        }
    }
}
fn operator_trace(relation: &Relation, context: &Arc<QueryContext>) -> Option<CursorTrace> {
    context.trace_parent?;
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
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
    fn trace_attributes(&self, span: &mut dyn TraceSpan) { self.input.trace_attributes(span); }
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
    fn trace_attributes(&self, span: &mut dyn TraceSpan) { if let Some(input) = &self.input { input.trace_attributes(span); } }
}
impl Drop for TracedCursor {
    fn drop(&mut self) {
        if let (Some(input), Some(span)) = (&self.input, &mut self.trace.span) { input.trace_attributes(span.as_mut()); }
        self.input = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use rtdb_columnar::MemoryBudget;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::Timestamp;
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn tracing_cursor_test_accumulates_only_supplied_pull_durations() {
        let memory = MemoryBudget::new(1 << 24);
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) })]), false).unwrap();
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&memory), 4, true, true).unwrap();
        let parent = tracer.start(None, "xtdb.query", &[]).unwrap();
        let context = QueryContext::new(catalog, memory, Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap().with_tracer(Some(tracer.clone()), Some(parent.context()));
        let mut trace = CursorTrace::new(&context, "output", "query.cursor.output", true).unwrap();
        trace.start(); trace.completed(Duration::from_millis(7), Some(3)); trace.completed(Duration::from_millis(5), None); drop(trace); drop(parent); tracer.close();
        let span = receiver.next().await.unwrap();
        let attributes: BTreeMap<_, _> = span.attributes().collect();
        assert_eq!(attributes["cursor.total_time_ms"], "12");
        assert_eq!(attributes["cursor.time_to_first_page_ms"], "7");
        assert_eq!(attributes["cursor.page_count"], "1"); assert_eq!(attributes["cursor.row_count"], "3");
    }
}
