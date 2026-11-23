use std::{collections::VecDeque, sync::Arc};
use async_trait::async_trait;
use rtdb_plan::{Binder, ColumnId, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use crate::{context::Execution, cursor::OwnedRow, invalid, operators::rows_equal, pages::{BoxPageCursor,OwnedPage,PageBuilder,PageCursor}, sort::SortCursor};
use crate::columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_nullable_column, ordinal_column_by, poll_nested_batch};
use crate::{scalar_key::ScalarKey, set_keys::BatchKeyIndex};

pub(crate) struct BatchWindow {
    input: Option<BoxBatchCursor>,
    schema: Schema,
    partition_column: Option<usize>,
    expressions: Vec<WindowExpr>,
    ready: Option<(VecDeque<ColumnBatch>, Arc<rtdb_columnar::MemoryReservation>)>,
    execution: Execution,
}

impl BatchWindow {
    pub fn supports(schema: &Schema, partition_by: &[ColumnId], order_by: &[SortExpr], _expressions: &[WindowExpr]) -> bool {
        partition_by.iter().all(|column| schema.position(*column).is_some())
            && (order_by.is_empty() || crate::sort::batches::BatchSort::supports(schema, order_by))
    }

    pub fn new(input: BoxBatchCursor, schema: Schema, partition_by: &[ColumnId], mut order_by: Vec<SortExpr>, expressions: &[WindowExpr],
        projected: bool, execution: Execution) -> Result<Self> {
        let mut input: BoxBatchCursor = Box::new(WindowInput { input, context: Arc::clone(&execution.context) });
        let mut sort_schema = schema.clone();
        let partition_column = if partition_by.is_empty() { None } else {
            let logical = LogicalType::Scalar(ScalarType::I64);
            let mut binder = Binder::after(&schema)?;
            let field = binder.field("", None, logical.clone())?;
            order_by.insert(0, SortExpr { expr: field.expression(), descending: false, nulls_first: false });
            let arrow = Arc::new(rtdb_columnar::to_field(&field.name, &logical)?);
            let positions = partition_by.iter().map(|&column| schema.position(column).ok_or_else(|| invalid("window-column", "Window partition column is absent")))
                .collect::<Result<Vec<_>>>()?;
            let keys = Schema::new(positions.iter().enumerate().map(|(index, &position)| {
                let mut field = schema.fields()[position].clone();
                field.id = ColumnId(index as u32);
                field
            }).collect())?;
            let context = Arc::clone(&execution.context);
            let index = BatchKeyIndex::vector_hashing(&context)?;
            input = Box::new(PartitionBatches { input, context, positions, keys, index, arrow, logical, projected });
            sort_schema = schema.concat(&Schema::new(vec![field])?)?;
            Some(schema.len())
        };
        let input = if order_by.is_empty() { input } else {
            Box::new(crate::sort::batches::BatchSort::new(input, sort_schema, order_by, execution.clone(), projected, 0, i64::MAX as u64)?)
        };
        Ok(Self { input: Some(input), schema, partition_column, expressions: expressions.to_vec(), ready: None, execution })
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

        let mut partitions = Vec::new();
        let mut partition_storage = memory.reserve(0)?;
        if let Some(position) = self.partition_column {
            let mut previous = None;
            for (batch, &start) in batches.iter_mut().zip(&offsets) {
                let column = batch.columns.remove(position);
                for row in 0..batch.len {
                    context.check_cancelled_at(row)?;
                    let Some(ScalarKey::Signed(group)) = ScalarKey::read(&column, row) else {
                        return Err(invalid("window-partition", "Window batch lacks its partition identifier"));
                    };
                    if previous == Some(group) { continue; }
                    let bound = partitions.len().checked_add(1).and_then(|len| len.checked_mul(4 * size_of::<usize>()))
                        .and_then(|bytes| bytes.checked_add(256)).ok_or_else(window_size)?;
                    Arc::get_mut(&mut partition_storage).expect("Window owns the partition boundaries").grow_to(bound)?;
                    partitions.push(start + row);
                    previous = Some(group);
                }
            }
        }
        let group = |index: usize| {
            let next = partitions.partition_point(|&start| start <= index);
            let lower = if next == 0 { 0 } else { partitions[next - 1] };
            (lower, partitions.get(next).copied().unwrap_or(total))
        };

        for expression in &self.expressions {
            let _suffixes = memory.reserve(batches.len().checked_mul(size_of::<(crate::columnar::Column, Arc<rtdb_columnar::MemoryReservation>)>())
                .ok_or_else(window_size)?)?;
            let mut suffixes = Vec::with_capacity(batches.len());
            for (batch, &start) in batches.iter().zip(&offsets) {
                let suffix = match expression.function {
                    WindowFunction::RowNumber => ordinal_column_by(batch.len, &expression.field.name, context, |row| {
                        let index = start + row;
                        (index - group(index).0) as i64 + 1
                    })?,
                    WindowFunction::Lead { column, offset } | WindowFunction::Lag { column, offset } => {
                        let position = self.schema.position(column).ok_or_else(|| invalid("window-column", "Window value column is absent"))?;
                        let _picks = memory.reserve(batch.len.checked_mul(size_of::<Option<(usize, usize)>>()).ok_or_else(window_size)?)?;
                        let mut cells = Vec::with_capacity(batch.len);
                        for row in 0..batch.len {
                            context.check_cancelled_at(row)?;
                            let index = start + row;
                            let (lower, upper) = group(index);
                            let target = shifted_index(expression.function, index as u64, offset)?;
                            let cell = target.and_then(|target| usize::try_from(target).ok()).filter(|&target| lower <= target && target < upper).map(|target| {
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
            for (batch, (suffix, reservation)) in batches.iter_mut().zip(suffixes) {
                batch.append_column(suffix, reservation, memory)?;
            }
        }
        Ok((batches.into(), inventory))
    }
}

struct PartitionBatches {
    input: BoxBatchCursor,
    context: Arc<crate::QueryContext>,
    positions: Vec<usize>,
    keys: Schema,
    index: BatchKeyIndex,
    arrow: Arc<arrow_schema::Field>,
    logical: LogicalType,
    projected: bool,
}

#[async_trait]
impl BatchCursor for PartitionBatches {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let memory = &self.context.memory;
        while let Some(mut batch) = poll_nested_batch(self.input.as_mut()).await? {
            crate::operators::validate_distinct_types(&self.keys)?;
            if batch.len == 0 { continue; }
            let bytes = batch.len.checked_mul(std::mem::size_of::<i64>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(window_size)?;
            let reservation = memory.reserve(bytes)?;
            let keys = batch.key_projection(&self.positions, memory)?;
            let mut groups = Vec::with_capacity(batch.len);
            self.index.identify(&keys, self.projected, &self.context, &self.keys, |_, group, _| {
                groups.push(i64::try_from(group).map_err(|_| invalid("window-count", "Window partition count overflow"))?);
                Ok(())
            })?;
            let array = Arc::new(arrow_array::Int64Array::from(groups));
            let group = Column::Arrow { array, field: Arc::clone(&self.arrow), logical: Some(self.logical.clone()) };
            batch.append_column(group, reservation, memory)?;
            return Ok(Some(batch));
        }
        Ok(None)
    }
}

struct WindowInput { input: BoxBatchCursor, context: Arc<crate::QueryContext> }

#[async_trait]
impl BatchCursor for WindowInput {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        let batch = poll_nested_batch(self.input.as_mut()).await?;
        if let Some(batch) = &batch { batch.validate_readable(&self.context)?; }
        Ok(batch)
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
