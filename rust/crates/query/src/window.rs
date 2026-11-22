use std::{collections::VecDeque, sync::Arc};
use async_trait::async_trait;
use rtdb_plan::{Binder, ColumnId, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use crate::{context::Execution, cursor::OwnedRow, invalid, operators::rows_equal, pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor}, sort::SortCursor};
use crate::columnar::{BatchCursor, BoxBatchCursor, ColumnBatch, gather_nullable_column, ordinal_column, poll_nested_batch};

pub(crate) struct BatchWindow {
    input: Option<BoxBatchCursor>,
    schema: Schema,
    expressions: Vec<WindowExpr>,
    ready: Option<(VecDeque<ColumnBatch>, Arc<rtdb_columnar::MemoryReservation>)>,
    execution: Execution,
}

impl BatchWindow {
    pub fn supports(schema: &Schema, partition_by: &[ColumnId], order_by: &[SortExpr], expressions: &[WindowExpr]) -> bool {
        partition_by.is_empty() && !expressions.is_empty()
            && (order_by.is_empty() || crate::sort::batches::BatchSort::supports(schema, order_by))
    }

    pub fn new(input: BoxBatchCursor, schema: Schema, order_by: Vec<SortExpr>, expressions: &[WindowExpr],
        projected: bool, execution: Execution) -> Result<Self> {
        let input = if order_by.is_empty() { input } else {
            Box::new(crate::sort::batches::BatchSort::new(input, schema.clone(), order_by, execution.clone(), projected, 0, i64::MAX as u64)?)
        };
        Ok(Self { input: Some(input), schema, expressions: expressions.to_vec(), ready: None, execution })
    }

    async fn evaluate(&mut self, mut input: BoxBatchCursor) -> Result<(VecDeque<ColumnBatch>, Arc<rtdb_columnar::MemoryReservation>)> {
        use std::mem::size_of;
        let context = &self.execution.context;
        let memory = &context.memory;
        let mut batches = Vec::new();
        let mut offsets = Vec::new();
        let mut total = 0usize;
        let mut inventory = memory.reserve(256)?;
        while let Some(batch) = poll_nested_batch(input.as_mut()).await? {
            if batch.len == 0 { continue; }
            let entry = 3 * size_of::<ColumnBatch>() + 2 * size_of::<usize>();
            let storage = batches.len().checked_add(1usize).and_then(|count| count.checked_mul(entry))
                .and_then(|bytes| bytes.checked_add(256)).ok_or_else(window_size)?;
            Arc::get_mut(&mut inventory).expect("Window owns the batch inventory reservation").grow_to(storage)?;
            offsets.push(total);
            total = total.checked_add(batch.len).filter(|total| i64::try_from(*total).is_ok())
                .ok_or_else(|| invalid("window-count", "Window row number overflow"))?;
            batches.push(batch);
        }

        for expression in &self.expressions {
            let _suffixes = memory.reserve(batches.len().checked_mul(size_of::<(crate::columnar::Column, Arc<rtdb_columnar::MemoryReservation>)>())
                .ok_or_else(window_size)?)?;
            let mut suffixes = Vec::with_capacity(batches.len());
            for (batch, &start) in batches.iter().zip(&offsets) {
                let suffix = match expression.function {
                    WindowFunction::RowNumber => ordinal_column(batch.len, start as i64 + 1, &expression.field.name, context)?,
                    WindowFunction::Lead { column, offset } | WindowFunction::Lag { column, offset } => {
                        let position = self.schema.position(column).ok_or_else(|| invalid("window-column", "Window value column is absent"))?;
                        let _picks = memory.reserve(batch.len.checked_mul(size_of::<Option<(usize, usize)>>()).ok_or_else(window_size)?)?;
                        let mut cells = Vec::with_capacity(batch.len);
                        for row in 0..batch.len {
                            context.check_cancelled_at(row)?;
                            let index = (start + row) as u64;
                            let target = shifted_index(expression.function, index, offset)?;
                            let cell = target.and_then(|target| usize::try_from(target).ok()).filter(|&target| target < total).map(|target| {
                                let batch = offsets.partition_point(|&start| start <= target) - 1;
                                (batch, target - offsets[batch])
                            });
                            cells.push(cell);
                        }
                        gather_nullable_column(|index| &batches[index], &cells, position, memory)?
                    }
                };
                suffixes.push(suffix);
            }
            batches = batches.into_iter().zip(suffixes).map(|(batch, (suffix, _scratch))| batch.append_column(suffix, memory))
                .collect::<Result<_>>()?;
        }
        Ok((batches.into(), inventory))
    }
}

fn window_size() -> rtdb_types::Anomaly { invalid("window-size", "Window batch inventory exceeds addressable memory") }

fn shifted_index(function: WindowFunction, index: u64, offset: u64) -> Result<Option<u64>> {
    if matches!(function, WindowFunction::Lead { .. }) {
        index.checked_add(offset).filter(|&index| index <= i64::MAX as u64).map(Some)
            .ok_or_else(|| invalid("window-offset", "Window row offset overflow"))
    } else { Ok(index.checked_sub(offset)) }
}

#[async_trait]
impl BatchCursor for BatchWindow {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        self.execution.context.check_cancelled()?;
        if let Some(input) = self.input.take() {
            self.ready = Some(self.evaluate(input).await?);
        }
        let batch = self.ready.as_mut().and_then(|(ready, _)| ready.pop_front());
        if batch.is_none() { self.ready = None; }
        Ok(batch)
    }
}

pub(crate) struct WindowCursor {
    input: Option<BoxPageCursor>, runtime: Arc<crate::runtime_schema::EmittedRelation>, schema: Schema, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr>, execution: Execution,
}

impl WindowCursor {
    pub fn new(input: BoxPageCursor, runtime: Arc<crate::runtime_schema::EmittedRelation>, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr>, execution: Execution) -> Self {
        Self { input: Some(input), schema: runtime.schema().clone(), runtime, partition_by, order_by, expressions, execution }
    }

    async fn sorted_input(&mut self, input: BoxPageCursor) -> Result<OwnedPage> {
        let mut binder = Binder::after(&self.schema)?;
        let partition = binder.field("", None, LogicalType::Scalar(ScalarType::U64))?;
        let schema = self.schema.concat(&Schema::new(vec![partition.clone()])?)?;
        let positions = self.partition_by.iter().map(|column| self.schema.position(*column).ok_or_else(|| invalid("window-column", "Window partition column is absent"))).collect::<Result<Vec<_>>>()?;
        let partitioned = PartitionPages { input, positions, runtime: Arc::clone(&self.runtime), keys: Vec::new(), index: Default::default(), count: 0, execution: self.execution.clone() };
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
                        let target = shifted_index(expression.function, index, offset)?;
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

/// Numbers each row's partition in first-seen order, finding a key among those with its hash.
struct PartitionPages { input: BoxPageCursor, positions: Vec<usize>, runtime: Arc<crate::runtime_schema::EmittedRelation>, keys: Vec<OwnedRow>, index: std::collections::HashMap<u32, Vec<usize>>, count: u64, execution: Execution }
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
            let mut found = None;
            for &candidate in self.index.get(&hash).map(Vec::as_slice).unwrap_or_default() {
                let previous = &self.keys[candidate].values;
                if rows_equal(&key.values, &previous[..previous.len() - 1], &self.execution.context.timezone, &self.execution.context.memory)? { found = Some(partition_id(previous)?); break; }
            }
            let group = match found {
                Some(group) => group,
                None => {
                    let group = self.count;
                    self.count = self.count.checked_add(1).ok_or_else(|| invalid("window-count", "Window partition count overflow"))?;
                    self.index.entry(hash).or_default().push(self.keys.len());
                    self.keys.push(copy_row(&key.values, &[Value::U64(group)], &self.execution)?);
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
