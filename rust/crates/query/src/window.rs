use std::sync::Arc;
use async_trait::async_trait;
use rtdb_plan::{Binder, ColumnId, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use crate::{context::Execution, cursor::OwnedRow, invalid, operators::rows_equal, pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor}, sort::SortCursor, spill::SpillWriter};

pub(crate) struct WindowCursor {
    input: Option<BoxPageCursor>, runtime: Arc<crate::runtime_schema::RuntimeRelation>, schema: Schema, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr>, execution: Execution,
}

impl WindowCursor {
    pub fn new(input: BoxPageCursor, runtime: Arc<crate::runtime_schema::RuntimeRelation>, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr>, execution: Execution) -> Self {
        Self { input: Some(input), schema: runtime.schema().clone(), runtime, partition_by, order_by, expressions, execution }
    }

    async fn sorted_input(&mut self, input: BoxPageCursor) -> Result<OwnedPage> {
        let mut binder = Binder::after(&self.schema)?;
        let partition = binder.field("", None, LogicalType::Scalar(ScalarType::U64))?;
        let schema = self.schema.concat(&Schema::new(vec![partition.clone()])?)?;
        let positions = self.partition_by.iter().map(|column| self.schema.position(*column).ok_or_else(|| invalid("window-column", "Window partition column is absent"))).collect::<Result<Vec<_>>>()?;
        let partitioned = PartitionPages { input, positions, runtime: Arc::clone(&self.runtime), keys: self.execution.context.spill.create()?, count: 0, execution: self.execution.clone() };
        let mut order = vec![SortExpr { expr: partition.expression(), descending: false, nulls_first: false }];
        order.extend(self.order_by.clone());
        let mut sorted = SortCursor::from_pages(Box::new(partitioned), schema, order, self.execution.clone());
        let mut rows = PageBuilder::new(0,&self.execution.context.memory)?;
        while let Some(mut page) = sorted.next_page().await? {
            while let Some(row) = page.next() {
                self.execution.context.check_cancelled()?;
                rows.push_growing(row,&self.execution.context.memory)?;
            }
        }
        Ok(rows.finish())
    }
}

#[async_trait]
impl PageCursor for WindowCursor {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let Some(input) = self.input.take() else { return Ok(None); };
        let input = self.sorted_input(input).await?;
        let mut output = PageBuilder::new(input.len(),&self.execution.context.memory)?;
        for row in input.rows() {
            self.execution.context.check_cancelled()?;
            output.push(OwnedRow::copy_parts(&[&row.values[..self.schema.len()]],0,self.expressions.len(),&self.execution.context.memory)?)?;
        }
        for (position, expression) in self.expressions.iter().enumerate() {
            let mut partition = None;
            let mut row_number = 0i64;
            for (index,(row,result)) in input.rows().iter().zip(output.rows_mut()).enumerate() {
                self.execution.context.check_cancelled()?;
                let group = partition_id(&row.values)?;
                if partition != Some(group) { partition = Some(group);row_number = 0; }
                row_number = row_number.checked_add(1).ok_or_else(||invalid("window-count","Window row number overflow"))?;
                let counter = Value::I64(row_number);
                let value = match expression.function {
                    WindowFunction::RowNumber => &counter,
                    WindowFunction::Lead { column,offset } | WindowFunction::Lag { column,offset } => {
                        let index = u64::try_from(index).map_err(|_|invalid("window-count","Window row index exceeds signed64 bits"))?;
                        let target = if matches!(expression.function,WindowFunction::Lead {..}) {
                            Some(index.checked_add(offset).filter(|index|*index <= i64::MAX as u64).ok_or_else(||invalid("window-offset","Window row offset overflow"))?)
                        } else { index.checked_sub(offset) };
                        let column = self.schema.position(column).ok_or_else(||invalid("window-column","Window value column is absent"))?;
                        match target.and_then(|target|usize::try_from(target).ok()).and_then(|target|input.rows().get(target)) {
                            Some(target) if partition_id(&target.values)? == group => target.values.get(column).ok_or_else(||invalid("window-width","Shifted window row lacks its value"))?,
                            _ => &Value::Null,
                        }
                    }
                };
                let _copy = self.execution.context.memory.reserve(value.retained_bytes()?)?;
                result.replace_value(self.schema.len() + position,value.clone(),&self.execution.context.memory)?;
            }
        }
        Ok((output.len() > 0).then(||output.finish()))
    }
}

struct PartitionPages { input: BoxPageCursor, positions: Vec<usize>, runtime: Arc<crate::runtime_schema::RuntimeRelation>, keys: SpillWriter, count: u64, execution: Execution }
#[async_trait]
impl PageCursor for PartitionPages {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        let Some(mut page) = self.input.next_page().await? else { return Ok(None); };
        self.execution.context.check_cancelled()?;
        for position in &self.positions { let ty = &self.runtime.schema().fields()[*position].data_type; rtdb_plan::validate_equality_types(ty, ty)?; }
        let mut output = PageBuilder::new(page.len(),&self.execution.context.memory)?;
        while let Some(row) = page.next() {
            self.execution.context.check_cancelled()?;
            let bound = self.positions.iter().try_fold(4096usize, |bytes, position| {
                bytes.checked_add(row.values.get(*position).ok_or_else(|| invalid("window-width", "Window row lacks a partition column"))?.retained_bytes()?).ok_or_else(|| invalid("window-size", "Partition key size overflow"))
            })?;
            let charge = self.execution.context.memory.reserve(bound)?;
            let values = self.positions.iter().map(|position| row.values[*position].clone()).collect::<Vec<_>>();
            let key = OwnedRow::with_reservation(values, charge)?;
            let hash = crate::join_storage::hash_values(&key.values)?;
            let mut reader = self.keys.reader(Arc::clone(&self.execution.context.memory))?;
            let mut found = None;
            while let Some(previous) = reader.next()? {
                self.execution.context.check_cancelled()?;
                if hash == crate::join_storage::hash_values(&previous.values[..previous.values.len() - 1])?
                    && rows_equal(&key.values, &previous.values[..previous.values.len() - 1], &self.execution.context.timezone, &self.execution.context.memory)? { found = Some(partition_id(&previous.values)?); break; }
            }
            let group = match found {
                Some(group) => group,
                None => {
                    let group = self.count;
                    self.count = self.count.checked_add(1).ok_or_else(|| invalid("window-count", "Window partition count overflow"))?;
                    let key = copy_row(&key.values, &[Value::U64(group)], &self.execution)?;
                    self.keys.write(&key.values, &self.execution.context.memory)?;
                    group
                }
            };
            output.push(copy_row(&row.values, &[Value::U64(group)], &self.execution)?)?;
        }
        Ok(Some(output.finish()))
    }
}

fn partition_id(values: &[Value]) -> Result<u64> {
    match values.last() { Some(Value::U64(group)) => Ok(*group), _ => Err(invalid("window-partition", "Window row lacks its partition identifier")) }
}

fn copy_row(input: &[Value], suffix: &[Value], execution: &Execution) -> Result<OwnedRow> {
    let bytes = input.iter().chain(suffix).try_fold(4096usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("window-size", "Window row size overflow")))?;
    let reservation = execution.context.memory.reserve(bytes)?;
    OwnedRow::with_reservation(input.iter().chain(suffix).cloned().collect(), reservation)
}

#[cfg(test)]
mod page_tests;
