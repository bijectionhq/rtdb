use crate::{
    QueryContext,
    columnar::{BatchCursor, BoxBatchCursor, ColumnBatch},
    context::Execution,
    pages::{BoxPageCursor, PageBuilder, PageCursor, single_page},
};
use async_trait::async_trait;
use rtdb_plan::Expr;
use rtdb_types::Result;
use std::sync::Arc;

pub(crate) async fn open(execution: &Execution, expression: Expr) -> Result<BoxPageCursor> {
    let mut input = execution.open_list(expression,None).await?;
    let mut output = PageBuilder::new(0, &execution.context.memory)?;
    while let Some(mut page) = input.next_page().await? {
        execution.context.check_cancelled()?;
        while let Some(row) = page.next() {
            output.push_growing(row, &execution.context.memory)?;
        }
        tokio::task::yield_now().await;
    }
    execution.context.check_cancelled()?;
    Ok(single_page(Some(output.finish()), Arc::clone(&execution.context)))
}

pub(crate) fn open_arrow(execution: &Execution, position: usize) -> Result<Option<BoxBatchCursor>> {
    let Some((input, row)) = execution.parameters.arrow_input(position) else { return Ok(None); };
    let output = ColumnBatch::from_list_parameter(
        input, position, row, &execution.parameters.types()[position], &execution.context.memory,
    )?;
    Ok(output.map(|output| Box::new(ArrowParameterTable {
        output: Some(output), context: Arc::clone(&execution.context),
    }) as BoxBatchCursor))
}

struct ArrowParameterTable { output: Option<ColumnBatch>, context: Arc<QueryContext> }

#[async_trait]
impl BatchCursor for ArrowParameterTable {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.context.check_cancelled()?;
        Ok(self.output.take())
    }
}
