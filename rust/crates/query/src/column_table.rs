use crate::{
    columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_column_rows, single_batch},
    context::Execution,
    invalid,
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

pub(crate) async fn open_columns(execution: &Execution, expression: Expr) -> Result<BoxBatchCursor> {
    if let Expr::Parameter { index, .. } = expression
        && let Some(input) = open_arrow(execution, index)?
    {
        return Ok(input);
    }
    let mut input = execution.open_list(expression, None).await?;
    let memory = &execution.context.memory;
    let overflow = || invalid("column-table-size", "Column table exceeds addressable memory");
    let mut pages = Vec::new();
    let mut rows = 0usize;
    let mut inventory = memory.reserve(4096)?;
    while let Some(batch) = input.next_batch().await? {
        rows = rows.checked_add(batch.len).ok_or_else(overflow)?;
        let lease = Arc::get_mut(&mut inventory).expect("column table owns its page inventory");
        lease.grow_to(lease.bytes().checked_add(256).ok_or_else(overflow)?)?;
        pages.push(batch);
        tokio::task::yield_now().await;
    }
    drop(input);
    let mut result = ColumnBatch::settled(Vec::new(), rows, memory.reserve(256)?)?;
    let (column, charge) = if rows == 0 {
        (Column::Values(Vec::new()), memory.reserve(256)?)
    } else {
        let _selection = memory.reserve(rows.checked_mul(std::mem::size_of::<(usize, usize)>()).ok_or_else(overflow)?)?;
        let positions = pages.iter().enumerate().flat_map(|(page, batch)| (0..batch.len).map(move |row| (page, row))).collect::<Vec<_>>();
        gather_column_rows(|index| &pages[index], &positions, 0, memory)?
    };
    result.append_column(column, charge, memory)?;
    Ok(single_batch(Some(result), Arc::clone(&execution.context)))
}
