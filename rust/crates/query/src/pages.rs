use std::sync::Arc;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{Expr,Schema};
use rtdb_types::Result;
use crate::{QueryContext, cursor::{BoxCursor, OwnedRow, RowCursor}, invalid};

pub(crate) struct OwnedPage { rows: std::vec::IntoIter<OwnedRow>, _reservation: Arc<MemoryReservation> }
impl OwnedPage {
    pub fn len(&self) -> usize { self.rows.len() }
    pub fn rows(&self) -> &[OwnedRow] { self.rows.as_slice() }
    pub fn retained_bytes(&self) -> usize {
        self.rows().iter().fold(self._reservation.bytes(), |bytes, row| bytes.saturating_add(row.bytes()))
    }
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
    pub fn rows_mut(&mut self) -> &mut [OwnedRow] { &mut self.rows }
    pub fn push_growing(&mut self, row: OwnedRow, memory: &Arc<MemoryBudget>) -> Result<()> {
        if self.rows.len() == self.limit {
            let limit = self.limit.checked_mul(2).map(|limit|limit.max(1)).ok_or_else(|| invalid("page-size", "Page row capacity overflow"))?;
            let bytes = limit.checked_mul(std::mem::size_of::<OwnedRow>()).and_then(|bytes|bytes.checked_add(256)).ok_or_else(||invalid("page-size","Page row capacity overflow"))?;
            let reservation = memory.reserve(bytes)?;
            self.rows.reserve_exact(limit - self.rows.len());
            self.limit = limit;
            self.reservation = reservation;
        }
        self.push(row)
    }
    pub fn finish(self) -> OwnedPage { OwnedPage { rows: self.rows.into_iter(), _reservation: self.reservation } }
}

pub(crate) async fn project<'a>(page: &OwnedPage,schema: &Schema,expressions: impl ExactSizeIterator<Item=&'a Expr> + Clone + Send,execution: &crate::context::Execution) -> Result<OwnedPage> {
    let mut output = PageBuilder::new(page.len(),&execution.context.memory)?;
    let positions: Option<Vec<usize>> = expressions.clone().map(|expression| match expression { Expr::Column { id, .. } => schema.position(*id), _ => None }).collect();
    if let Some(positions) = positions {
        let mut slab = crate::cursor::RowSlab::new(&execution.context.memory);
        for row in page.rows() {
            execution.context.check_cancelled()?;
            let mut values = Vec::with_capacity(positions.len());
            for &position in &positions {
                let mut value = row.values.get(position).cloned().ok_or_else(|| invalid("row-width", "Projected column is outside its row"))?;
                value.admit_expression_dates()?;
                values.push(value);
            }
            output.push(OwnedRow::in_slab(values, &mut slab)?)?;
        }
        return Ok(output.finish());
    }
    for _ in 0..page.len() { output.push(OwnedRow::copy_parts(&[],0,expressions.len(),&execution.context.memory)?)?; }
    evaluate_columns(page, &mut output, 0, schema, expressions, execution).await?;
    Ok(output.finish())
}

pub(crate) async fn extend<'a>(page: &OwnedPage, schema: &Schema, expressions: impl ExactSizeIterator<Item = &'a Expr> + Send, execution: &crate::context::Execution) -> Result<OwnedPage> {
    let memory = &execution.context.memory;
    let mut output = PageBuilder::new(page.len(), memory)?;
    for row in page.rows() {
        execution.context.check_cancelled()?;
        output.push(OwnedRow::copy_parts(&[&row.values], 0, expressions.len(), memory)?)?;
    }
    evaluate_columns(page, &mut output, schema.len(), schema, expressions, execution).await?;
    Ok(output.finish())
}

async fn evaluate_columns<'a>(page: &OwnedPage, output: &mut PageBuilder, first: usize, schema: &Schema, expressions: impl Iterator<Item = &'a Expr> + Send, execution: &crate::context::Execution) -> Result<()> {
    for (column,expression) in (first..).zip(expressions) {
        if let Expr::Column { id, .. } = expression && let Some(position) = schema.position(*id) {
            for (input,row) in page.rows().iter().zip(output.rows_mut()) {
                let mut value = input.values.get(position).cloned().ok_or_else(|| invalid("row-width", "Projected column is outside its row"))?;
                value.admit_expression_dates()?;
                row.replace_value(column,value,&execution.context.memory)?;
            }
            continue;
        }
        let metadata_scope = execution.scope()?;
        let invocation = execution.prepare_expression(expression,schema,&metadata_scope)?;
        let scope = execution.scope()?;
        for (input,row) in page.rows().iter().zip(output.rows_mut()) {
            let value = execution.evaluate_invocation(&scope,&invocation,&input.values).await?;
            row.replace_value(column,value,&execution.context.memory)?;
            scope.reset()?;
        }
    }
    Ok(())
}

pub(crate) fn coalesce(input: BoxPageCursor, context: Arc<QueryContext>) -> BoxPageCursor { Box::new(CoalescingPages { input,context,seen: 0 }) }
struct CoalescingPages { input: BoxPageCursor,context: Arc<QueryContext>,seen: usize }
#[async_trait]
impl PageCursor for CoalescingPages {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let mut output: Option<PageBuilder> = None;
        loop {
            self.context.check_cancelled()?;
            let Some(mut page) = self.input.next_page().await? else { return Ok(output.filter(|page|page.len() > 0).map(PageBuilder::finish)); };
            if self.seen < 100 { self.seen = self.seen.saturating_add(page.len());return Ok(Some(page)); }
            if page.len() >= 100 && output.is_none() { return Ok(Some(page)); }
            if output.is_none() { output = Some(PageBuilder::new(100,&self.context.memory)?); }
            let rows = output.as_mut().ok_or_else(||invalid("page-state","Coalescing page builder is absent"))?;
            while let Some(row) = page.next() { rows.push_growing(row,&self.context.memory)?; }
            if rows.len() >= 100 { return Ok(output.map(PageBuilder::finish)); }
        }
    }
    fn trace_attributes(&self,span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

#[async_trait]
pub(crate) trait PageCursor: Send {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>>;
    /// Advances an available prefix of complete pages within the row budget.
    /// Any remainder must still be consumed with `next_page`.
    fn skip_complete_pages(&mut self, _rows: u64) -> Result<u64> { Ok(0) }
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { None }
}
pub(crate) type BoxPageCursor = Box<dyn PageCursor>;

pub(crate) fn single_page(page: Option<OwnedPage>, context: Arc<QueryContext>) -> BoxPageCursor {
    Box::new(SinglePage { page, context })
}

struct SinglePage { page: Option<OwnedPage>, context: Arc<QueryContext> }
#[async_trait]
impl PageCursor for SinglePage {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        Ok(self.page.take())
    }
}

pub(crate) fn from_rows(input: BoxCursor, context: Arc<QueryContext>) -> BoxPageCursor {
    Box::new(RowPageCursor { input, context, exhausted: false })
}
pub(crate) fn rows(input: BoxPageCursor, context: Arc<QueryContext>) -> BoxCursor { Box::new(PageRows { input, page: None, context }) }
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
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> { self.input.skip_complete_pages(rows) }
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

struct PageRows { input: BoxPageCursor, page: Option<OwnedPage>, context: Arc<QueryContext> }
#[async_trait]
impl RowCursor for PageRows {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.context.check_cancelled()?;
        loop {
            if let Some(row) = self.page.as_mut().and_then(OwnedPage::next) { return Ok(Some(row)); }
            self.page = self.input.next_page().await?;
            if self.page.is_none() { return Ok(None); }
        }
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    async fn cancellation_rejects_buffered_rows_and_drop_releases_the_full_page() {
        let context = context(2);
        let (input, polls, closed) = input(&context, 5, None);
        let mut cursor = rows(from_rows(input, Arc::clone(&context)), Arc::clone(&context));
        drop(cursor.next().await.unwrap().unwrap());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(context.memory.used_bytes() > 0);
        context.cancellation.cancel();
        assert_eq!(cursor.next().await.err().unwrap().code, "xtdb/query-cancelled");
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        drop(cursor); assert!(closed.load(Ordering::SeqCst));
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

    #[tokio::test]
    async fn coalescing_passes_first_hundred_rows_then_accumulates_whole_small_pages() {
        let context = context(60);
        let (input,_,closed) = input(&context,300,None);
        let mut cursor = coalesce(from_rows(input,Arc::clone(&context)),Arc::clone(&context));
        let mut sizes = Vec::new();
        while let Some(page) = cursor.next_page().await.unwrap() { sizes.push(page.len()); }
        assert_eq!(sizes,[60,60,120,60]);
        drop(cursor);assert!(closed.load(Ordering::SeqCst));assert_eq!(context.memory.used_bytes(),0);
    }

    #[tokio::test]
    async fn coalescing_failure_drops_accumulated_rows_and_preserves_input_error() {
        let context = context(60);
        let (input,_,closed) = input(&context,300,Some(200));
        let mut cursor = coalesce(from_rows(input,Arc::clone(&context)),Arc::clone(&context));
        drop(cursor.next_page().await.unwrap());drop(cursor.next_page().await.unwrap());
        assert_eq!(cursor.next_page().await.err().unwrap().code,"test/input");
        drop(cursor);assert!(closed.load(Ordering::SeqCst));assert_eq!(context.memory.used_bytes(),0);
    }
}
