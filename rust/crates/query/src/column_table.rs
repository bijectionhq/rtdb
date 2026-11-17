use crate::{
    QueryContext,
    context::Execution,
    pages::{OwnedPage, PageBuilder, PageCursor},
};
use async_trait::async_trait;
use rtdb_plan::Expr;
use rtdb_types::Result;
use std::sync::Arc;

pub(crate) struct ColumnTableCursor {
    page: Option<OwnedPage>,
    context: Arc<QueryContext>,
}

impl ColumnTableCursor {
    pub async fn open(execution: &Execution, expression: Expr) -> Result<Self> {
        let mut input = execution.open_list(expression).await?;
        let mut output = PageBuilder::new(0, &execution.context.memory)?;
        while let Some(mut page) = input.next_page().await? {
            execution.context.check_cancelled()?;
            while let Some(row) = page.next() {
                output.push_growing(row, &execution.context.memory)?;
            }
            tokio::task::yield_now().await;
        }
        execution.context.check_cancelled()?;
        Ok(Self {
            page: Some(output.finish()),
            context: Arc::clone(&execution.context),
        })
    }
}

#[async_trait]
impl PageCursor for ColumnTableCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.context.check_cancelled()?;
        Ok(self.page.take())
    }
}
