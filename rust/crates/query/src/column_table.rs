use crate::{
    columnar::{BoxBatchCursor, ColumnBatch, single_batch},
    context::Execution,
    pages::{BoxPageCursor, PageBuilder, PageCursor, single_page},
};
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
    Ok(output.map(|output| single_batch(Some(output), Arc::clone(&execution.context))))
}
