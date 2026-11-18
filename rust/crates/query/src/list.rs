use std::sync::Arc;
use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use rtdb_expr::{IndexedListBatches, ListBatch};
use rtdb_types::{Result, Value};
use crate::{QueryContext, context::cancelled, cursor::OwnedRow, invalid, pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor}};

pub(crate) enum ListEvent { Opened(Option<IndexedListBatches>), Batch(ListBatch) }

enum ListPages {
    Indexed(IndexedListBatches),
    Stream(BoxStream<'static, Result<ListEvent>>),
}

pub(crate) struct ListCursor { pages: ListPages, context: Arc<QueryContext> }
impl ListCursor {
    pub async fn open(mut stream: BoxStream<'static, Result<ListEvent>>, context: Arc<QueryContext>) -> Result<Self> {
        let opened = context.owner_cancellation.run_until_cancelled(context.cancellation.run_until_cancelled(stream.next())).await.flatten().ok_or_else(cancelled)?;
        match opened.transpose()? {
            Some(ListEvent::Opened(opened)) => {
                let pages = match opened {
                    Some(indexed) => ListPages::Indexed(indexed),
                    None => ListPages::Stream(stream),
                };
                Ok(Self { pages, context })
            }
            _ => Err(invalid("list-state", "List cursor did not open")),
        }
    }
}
#[async_trait]
impl PageCursor for ListCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        let batch = match &mut self.pages {
            ListPages::Indexed(batches) => batches.next_batch()?,
            ListPages::Stream(stream) => {
                let next = self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(stream.next())).await.flatten().ok_or_else(cancelled)?;
                match next.transpose()? {
                    Some(ListEvent::Batch(batch)) => Some(batch),
                    Some(ListEvent::Opened(_)) => return Err(invalid("list-state", "List cursor opened twice")),
                    None => None,
                }
            }
        };
        let Some(batch) = batch else { return Ok(None); };
        let mut page = PageBuilder::new(batch.values().len(), &self.context.memory)?;
        for value in batch.values() {
            self.context.check_cancelled()?;
            page.push(OwnedRow::copy_parts(&[std::slice::from_ref(value)], 0, 0, &self.context.memory)?)?;
        }
        Ok(Some(page.finish()))
    }

    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> {
        self.context.check_cancelled()?;
        match &mut self.pages {
            ListPages::Indexed(batches) => Ok(batches.skip_complete_batches(rows)),
            ListPages::Stream(_) => Ok(0),
        }
    }
}

pub(crate) struct PageOrdinality { input: BoxPageCursor, context: Arc<QueryContext> }
impl PageOrdinality {
    pub fn new(input: BoxPageCursor, context: Arc<QueryContext>) -> Self { Self { input, context } }
}
#[async_trait]
impl PageCursor for PageOrdinality {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        let Some(mut page) = self.input.next_page().await? else { return Ok(None); };
        let mut output = PageBuilder::new(page.len(), &self.context.memory)?;
        let mut ordinal = 1i64;
        while let Some(row) = page.next() {
            self.context.check_cancelled()?;
            output.push(OwnedRow::copy_parts(&[&row.values, &[Value::I64(ordinal)]], 0, 0, &self.context.memory)?)?;
            ordinal = ordinal.checked_add(1).ok_or_else(|| invalid("ordinality-overflow", "Page ordinality exceeds signed64 bits"))?;
        }
        Ok(Some(output.finish()))
    }

    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> { self.input.skip_complete_pages(rows) }
}
