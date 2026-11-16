use std::sync::Arc;
use async_trait::async_trait;
use rtdb_plan::{Binder, ColumnId, Schema, SortExpr, WindowExpr, WindowFunction};
use rtdb_types::{LogicalType, Result, ScalarType, Value};
use crate::{context::Execution, cursor::{BoxCursor, OwnedRow, RowCursor}, invalid, operators::rows_equal, sort::SortCursor, spill::{SpillReader, SpillWriter}};

pub(crate) struct WindowCursor {
    state: State, schema: Schema, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr>, execution: Execution,
}

enum State {
    Pending(BoxCursor),
    Ready { rows: SpillReader, shifted: Vec<Option<ShiftedReader>>, index: u64, partition: Option<u64>, row_number: i64 },
    Done,
}

impl WindowCursor {
    pub fn new(input: BoxCursor, schema: Schema, partition_by: Vec<ColumnId>, order_by: Vec<SortExpr>, expressions: Vec<WindowExpr>, execution: Execution) -> Self {
        Self { state: State::Pending(input), schema, partition_by, order_by, expressions, execution }
    }

    async fn initialize(&mut self, input: BoxCursor) -> Result<State> {
        let mut binder = Binder::after(&self.schema)?;
        let partition = binder.field("", None, LogicalType::Scalar(ScalarType::U64))?;
        let schema = self.schema.concat(&Schema::new(vec![partition.clone()])?)?;
        let positions = self.partition_by.iter().map(|column| self.schema.position(*column).ok_or_else(|| invalid("window-column", "Window partition column is absent"))).collect::<Result<Vec<_>>>()?;
        let partitioned = PartitionRows { input, positions, keys: self.execution.context.spill.create()?, count: 0, execution: self.execution.clone() };
        let mut order = vec![SortExpr { expr: partition.expression(), descending: false, nulls_first: false }];
        order.extend(self.order_by.clone());
        let mut sorted = SortCursor::new(Box::new(partitioned), schema, order, self.execution.clone());
        let mut writer = self.execution.context.spill.create()?;
        let mut count = 0u64;
        while let Some(row) = sorted.next().await? {
            self.execution.context.check_cancelled()?;
            writer.write(&row.values, &self.execution.context.memory)?;
            count = count.checked_add(1).ok_or_else(|| invalid("window-count", "Window row count overflow"))?;
        }
        if let Some(last) = count.checked_sub(1) {
            for expression in &self.expressions {
                if let WindowFunction::Lead { offset, .. } = expression.function {
                    if last.checked_add(offset).is_none_or(|index| index > i64::MAX as u64) { return Err(invalid("window-offset", "Window row offset overflow")); }
                }
            }
        }
        let file = writer.finish()?;
        let shifted = self.expressions.iter().map(|expression| match expression.function {
            WindowFunction::RowNumber => Ok(None),
            _ => Ok(Some(ShiftedReader { rows: file.reader(Arc::clone(&self.execution.context.memory))?, next_index: 0, current: None })),
        }).collect::<Result<Vec<_>>>()?;
        Ok(State::Ready { rows: file.reader(Arc::clone(&self.execution.context.memory))?, shifted, index: 0, partition: None, row_number: 0 })
    }
}

#[async_trait]
impl RowCursor for WindowCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if matches!(self.state, State::Pending(_)) {
            let State::Pending(input) = std::mem::replace(&mut self.state, State::Done) else { return Err(invalid("window-state", "Window input is unavailable")); };
            self.state = self.initialize(input).await?;
        }
        let State::Ready { rows, shifted, index, partition, row_number } = &mut self.state else { return Ok(None); };
        let Some(row) = rows.next()? else { self.state = State::Done; return Ok(None); };
        let group = partition_id(&row.values)?;
        if *partition != Some(group) { *partition = Some(group); *row_number = 0; }
        *row_number = row_number.checked_add(1).ok_or_else(|| invalid("window-count", "Window row number overflow"))?;
        let mut results = Vec::with_capacity(self.expressions.len());
        let mut charges = Vec::with_capacity(self.expressions.len());
        for (position, expression) in self.expressions.iter().enumerate() {
            self.execution.context.check_cancelled()?;
            let counter = Value::I64(*row_number);
            let value = match expression.function {
                WindowFunction::RowNumber => &counter,
                WindowFunction::Lead { column, offset } | WindowFunction::Lag { column, offset } => {
                    let target = if matches!(expression.function, WindowFunction::Lead { .. }) { index.checked_add(offset) } else { index.checked_sub(offset) };
                    let column = self.schema.position(column).ok_or_else(|| invalid("window-column", "Window value column is absent"))?;
                    match target {
                        Some(target) => {
                            let reader = shifted[position].as_mut().ok_or_else(|| invalid("window-state", "Shifted window reader is absent"))?;
                            match reader.at(target, &self.execution)? {
                                Some(target) if partition_id(&target.values)? == group => target.values.get(column).ok_or_else(|| invalid("window-width", "Shifted window row lacks its value"))?,
                                _ => &Value::Null,
                            }
                        }
                        None => &Value::Null,
                    }
                }
            };
            charges.push(self.execution.context.memory.reserve(value.retained_bytes()?.checked_add(256).ok_or_else(|| invalid("window-size", "Window result size overflow"))?)?);
            results.push(value.clone());
        }
        *index = index.checked_add(1).ok_or_else(|| invalid("window-count", "Window row count overflow"))?;
        copy_row(&row.values[..row.values.len() - 1], &results, &self.execution).map(Some)
    }
}

struct ShiftedReader { rows: SpillReader, next_index: u64, current: Option<OwnedRow> }
impl ShiftedReader {
    fn at(&mut self, target: u64, execution: &Execution) -> Result<Option<&OwnedRow>> {
        while self.next_index <= target {
            execution.context.check_cancelled()?;
            self.current = self.rows.next()?;
            if self.current.is_none() { return Ok(None); }
            self.next_index = self.next_index.checked_add(1).ok_or_else(|| invalid("window-count", "Window reader row count overflow"))?;
        }
        Ok(self.current.as_ref())
    }
}

struct PartitionRows { input: BoxCursor, positions: Vec<usize>, keys: SpillWriter, count: u64, execution: Execution }
#[async_trait]
impl RowCursor for PartitionRows {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        let Some(row) = self.input.next().await? else { return Ok(None); };
        self.execution.context.check_cancelled()?;
        let bound = self.positions.iter().try_fold(4096usize, |bytes, position| {
            bytes.checked_add(row.values.get(*position).ok_or_else(|| invalid("window-width", "Window row lacks a partition column"))?.retained_bytes()?).ok_or_else(|| invalid("window-size", "Partition key size overflow"))
        })?;
        let charge = self.execution.context.memory.reserve(bound)?;
        let values = self.positions.iter().map(|position| row.values[*position].clone()).collect::<Vec<_>>();
        let key = OwnedRow::with_reservation(values, charge)?;
        let mut reader = self.keys.reader(Arc::clone(&self.execution.context.memory))?;
        let mut found = None;
        while let Some(previous) = reader.next()? {
            self.execution.context.check_cancelled()?;
            if rows_equal(&key.values, &previous.values[..previous.values.len() - 1])? { found = Some(partition_id(&previous.values)?); break; }
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
        copy_row(&row.values, &[Value::U64(group)], &self.execution).map(Some)
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
