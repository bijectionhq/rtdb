use std::sync::Arc;
use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use rtdb_types::{Result, Value};
use crate::{QueryContext, context::cancelled, cursor::OwnedRow, invalid, pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor}};

pub(crate) enum ListEvent { Opened, Page(OwnedPage) }

pub(crate) struct ListCursor { stream: BoxStream<'static, Result<ListEvent>>, context: Arc<QueryContext> }
impl ListCursor {
    pub async fn open(mut stream: BoxStream<'static, Result<ListEvent>>, context: Arc<QueryContext>) -> Result<Self> {
        let opened = context.owner_cancellation.run_until_cancelled(context.cancellation.run_until_cancelled(stream.next())).await.flatten().ok_or_else(cancelled)?;
        match opened.transpose()? {
            Some(ListEvent::Opened) => Ok(Self { stream, context }),
            _ => Err(invalid("list-state", "List cursor did not open")),
        }
    }
}
#[async_trait]
impl PageCursor for ListCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        let page = self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(self.stream.next())).await.flatten().ok_or_else(cancelled)?;
        match page.transpose()? {
            Some(ListEvent::Page(page)) => Ok(Some(page)),
            Some(ListEvent::Opened) => Err(invalid("list-state", "List cursor opened twice")),
            None => Ok(None),
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
}
