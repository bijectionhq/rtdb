use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use crate::{QueryContext, cursor::{BoxCursor, OwnedRow}, invalid};

pub(crate) struct OwnedPage { rows: std::vec::IntoIter<OwnedRow>, _reservation: Arc<MemoryReservation> }
impl OwnedPage {
    pub fn len(&self) -> usize { self.rows.len() }
    pub fn rows(&self) -> &[OwnedRow] { self.rows.as_slice() }
    pub fn next(&mut self) -> Option<OwnedRow> { self.rows.next() }
}

pub(crate) struct PageBuilder { rows: Vec<OwnedRow>, limit: usize, reservation: Arc<MemoryReservation> }
impl PageBuilder {
    pub fn new(limit: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = limit.checked_mul(std::mem::size_of::<OwnedRow>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid("page-size", "Page row container exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { rows: Vec::with_capacity(limit), limit, reservation })
    }
    pub fn push(&mut self, row: OwnedRow) -> Result<()> {
        if self.rows.len() == self.limit { return Err(invalid("page-size", "Page exceeded its reserved row count")); }
        self.rows.push(row);
        Ok(())
    }
    pub fn len(&self) -> usize { self.rows.len() }
    pub fn finish(self) -> OwnedPage { OwnedPage { rows: self.rows.into_iter(), _reservation: self.reservation } }
}

#[async_trait]
pub(crate) trait PageCursor: Send {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>>;
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { None }
}
pub(crate) type BoxPageCursor = Box<dyn PageCursor>;

pub(crate) fn from_rows(input: BoxCursor, context: Arc<QueryContext>) -> BoxPageCursor {
    Box::new(RowPageCursor { input, context, exhausted: false })
}
pub(crate) fn retain_page_state(input: BoxPageCursor, reservation: Arc<MemoryReservation>) -> BoxPageCursor { Box::new(RetainedPageState { input, _reservation: reservation }) }
struct RetainedPageState { input: BoxPageCursor, _reservation: Arc<MemoryReservation> }
#[async_trait]
impl PageCursor for RetainedPageState {
    /// Each page spends cooperative budget, so a pipeline whose operators never wait on I/O still returns to the
    /// scheduler, which only then polls the I/O driver - a cancel request's connection included.
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        tokio::task::consume_budget().await;
        self.input.next_page().await
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

struct RowPageCursor { input: BoxCursor, context: Arc<QueryContext>, exhausted: bool }
#[async_trait]
impl PageCursor for RowPageCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        if self.exhausted { return Ok(None); }
        let bytes = self.context.options.batch_rows.checked_mul(std::mem::size_of::<OwnedRow>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| invalid("page-size", "Page row container exceeds addressable memory"))?;
        let reservation = self.context.memory.reserve(bytes)?;
        let mut rows = Vec::with_capacity(self.context.options.batch_rows);
        while rows.len() < self.context.options.batch_rows {
            self.context.check_cancelled()?;
            match self.input.next().await? {
                Some(row) => rows.push(row),
                None => { self.exhausted = true; break; },
            }
        }
        if rows.is_empty() { return Ok(None); }
        Ok(Some(OwnedPage { rows: rows.into_iter(), _reservation: reservation }))
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursor::RowCursor;
    use std::{collections::BTreeMap, sync::atomic::{AtomicBool, AtomicUsize, Ordering}};
    use rtdb_columnar::MemoryBudget;
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{Anomaly, Timestamp, Value};
    use tokio_util::sync::CancellationToken;
    use crate::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};

    struct Input { next: usize, end: usize, fail_at: Option<usize>, polls: Arc<AtomicUsize>, closed: Arc<AtomicBool>, memory: Arc<MemoryBudget> }
    #[async_trait]
    impl RowCursor for Input {
        async fn next(&mut self) -> Result<Option<OwnedRow>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.fail_at == Some(self.next) { return Err(Anomaly::fault("test/input", "input failed")); }
            if self.next == self.end { return Ok(None); }
            let value = self.next; self.next += 1;
            OwnedRow::new(vec![Value::I64(value as i64)], &self.memory).map(Some)
        }
    }
    impl Drop for Input { fn drop(&mut self) { self.closed.store(true, Ordering::SeqCst); } }
    fn context(batch_rows: usize) -> Arc<QueryContext> {
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), DatabaseSnapshot { partitions: vec![], storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) })]), false).unwrap();
        QueryContext::new(catalog, MemoryBudget::new(1 << 20), Timestamp(0), "UTC", QueryOptions { batch_rows, ..Default::default() }, &CancellationToken::new()).unwrap()
    }
    fn input(context: &Arc<QueryContext>, end: usize, fail_at: Option<usize>) -> (BoxCursor, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let polls = Arc::new(AtomicUsize::new(0)); let closed = Arc::new(AtomicBool::new(false));
        (Box::new(Input { next: 0, end, fail_at, polls: Arc::clone(&polls), closed: Arc::clone(&closed), memory: Arc::clone(&context.memory) }), polls, closed)
    }

    #[tokio::test]
    async fn page_boundary_is_bounded_and_returned_rows_outlive_the_cursor() {
        let context = context(2);
        let (input, polls, closed) = input(&context, 5, None);
        let mut cursor = from_rows(input, Arc::clone(&context));
        let mut first = cursor.next_page().await.unwrap().unwrap();
        assert_eq!(first.len(), 2); assert_eq!(polls.load(Ordering::SeqCst), 2);
        let retained = first.next().unwrap();
        assert_eq!(retained.values, [Value::I64(0)]);
        drop(first);
        assert_eq!(cursor.next_page().await.unwrap().unwrap().len(), 2);
        assert_eq!(cursor.next_page().await.unwrap().unwrap().len(), 1);
        assert!(cursor.next_page().await.unwrap().is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 6);
        drop(cursor); assert!(closed.load(Ordering::SeqCst));
        assert_eq!(context.memory.used_bytes(), retained.bytes());
        drop(retained); assert_eq!(context.memory.used_bytes(), 0);
    }


    #[tokio::test]
    async fn cancellation_rejects_the_next_catalog_page_without_advancing_its_rows() {
        let context = context(2);
        let (input, polls, closed) = input(&context, 5, None);
        let mut cursor = from_rows(input, Arc::clone(&context));
        let page = cursor.next_page().await.unwrap().unwrap();
        context.cancellation.cancel();
        assert_eq!(cursor.next_page().await.err().unwrap().code, "xtdb/query-cancelled");
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        drop(cursor);
        assert!(closed.load(Ordering::SeqCst));
        assert_eq!(page.rows()[1].values, [Value::I64(1)]);
        drop(page);
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[tokio::test]
    async fn partial_page_failure_releases_rows_and_oversized_container_never_polls() {
        let context = context(4);
        let (input, polls, _) = input(&context, 5, Some(2));
        let mut cursor = from_rows(input, Arc::clone(&context));
        assert_eq!(cursor.next_page().await.err().unwrap().code, "test/input");
        assert_eq!(polls.load(Ordering::SeqCst), 3);
        assert_eq!(context.memory.used_bytes(), 0);
        drop(cursor);
        let context = super::tests::context(usize::MAX);
        let (input, polls, _) = super::tests::input(&context, 1, None);
        let mut cursor = from_rows(input, context);
        assert_eq!(cursor.next_page().await.err().unwrap().code, "rtdb.query/page-size");
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }


}
