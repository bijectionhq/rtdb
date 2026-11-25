use std::sync::Arc;
use async_trait::async_trait;
use futures::{StreamExt, stream::BoxStream};
use rtdb_expr::{IndexedListBatches, ListBatch};
use rtdb_plan::OrdinalityScope;
use rtdb_types::Result;
use crate::{QueryContext, context::cancelled, invalid};

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
    async fn next_list_batch(&mut self) -> Result<Option<ListBatch>> {
        self.context.check_cancelled()?;
        Ok(match &mut self.pages {
            ListPages::Indexed(batches) => batches.next_batch()?,
            ListPages::Stream(stream) => {
                let next = self.context.owner_cancellation.run_until_cancelled(self.context.cancellation.run_until_cancelled(stream.next())).await.flatten().ok_or_else(cancelled)?;
                match next.transpose()? {
                    Some(ListEvent::Batch(batch)) => Some(batch),
                    Some(ListEvent::Opened(_)) => return Err(invalid("list-state", "List cursor opened twice")),
                    None => None,
                }
            }
        })
    }
}

#[async_trait]
impl crate::columnar::BatchCursor for ListCursor {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        let Some(batch) = self.next_list_batch().await? else { return Ok(None); };
        if batch.values().array().is_some() {
            let backing = batch.backing().cloned();
            let (field, array, reservation) = batch.into_packed_column()?;
            let output = crate::columnar::ColumnBatch::from_packed_list(array, field, reservation, backing)?;
            return Ok(Some(output));
        }
        let memory = &self.context.memory;
        let values = batch.values();
        let mut output = crate::columnar::ColumnBatch::settled(Vec::new(), values.len(), memory.reserve(256)?)?;
        let (column, charge) = crate::columnar::pack_evaluated_column(values.scalar_values().expect("unpacked list"), memory)?;
        output.append_column(column, charge, memory)?;
        Ok(Some(output))
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> {
        self.context.check_cancelled()?;
        match &mut self.pages {
            ListPages::Indexed(batches) => Ok(batches.skip_complete_batches(rows)),
            ListPages::Stream(_) => Ok(0),
        }
    }
}

fn ordinality_overflow() -> rtdb_types::Anomaly { invalid("ordinality-overflow", "Row ordinality exceeds the signed 64-bit range") }

pub(crate) struct BatchOrdinality {
    input: crate::columnar::BoxBatchCursor,
    context: Arc<QueryContext>,
    name: String,
    relation_rows: Option<i64>,
}

impl BatchOrdinality {
    pub fn new(input: crate::columnar::BoxBatchCursor, name: String, scope: OrdinalityScope, context: Arc<QueryContext>) -> Self {
        Self { input, context, name, relation_rows: (scope == OrdinalityScope::Relation).then_some(0) }
    }
}

#[async_trait]
impl crate::columnar::BatchCursor for BatchOrdinality {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        self.context.check_cancelled()?;
        let Some(mut batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? else { return Ok(None); };
        let length = i64::try_from(batch.len).map_err(|_| ordinality_overflow())?;
        let previous = self.relation_rows.unwrap_or(0);
        let end = previous.checked_add(length).ok_or_else(ordinality_overflow)?;
        let start = if length == 0 { end } else { previous + 1 };
        let (column, reservation) = crate::columnar::ordinal_column(batch.len, start, &self.name, &self.context)?;
        batch.append_column(column, reservation, &self.context.memory)?;
        if let Some(rows) = &mut self.relation_rows { *rows = end; }
        Ok(Some(batch))
    }
    fn skip_complete_pages(&mut self, rows: u64) -> Result<u64> {
        let skipped = self.input.skip_complete_pages(rows)?;
        if let Some(rows) = &mut self.relation_rows {
            let skipped = i64::try_from(skipped).map_err(|_| ordinality_overflow())?;
            *rows = rows.checked_add(skipped).ok_or_else(ordinality_overflow)?;
        }
        Ok(skipped)
    }
}
