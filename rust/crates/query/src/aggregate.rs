use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::{arithmetic, cast, compare, is_true, number, sql_eq};
use rtdb_plan::{AggregateExpr, AggregateFunction, BinaryOp, NamedExpr, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow, RowCursor};
use crate::operators::rows_equal;
use crate::spill::{SpillFile, SpillReader, SpillWriter};
use crate::{invalid, sort::SortCursor};

pub(crate) struct AggregateCursor {
    input: Option<BoxCursor>, schema: Schema, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>,
    execution: Execution, keys: Option<SpillReader>, rows: Option<Arc<SpillFile>>,
}
impl AggregateCursor {
    pub fn new(input: BoxCursor, schema: Schema, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr>, execution: Execution) -> Self {
        Self { input: Some(input), schema, groups, aggregates, execution, keys: None, rows: None }
    }
    async fn initialize(&mut self, mut input: BoxCursor) -> Result<()> {
        let mut keys = self.execution.context.spill.create()?;
        let mut rows = self.execution.context.spill.create()?;
        let mut group_count = 0u64;
        if self.groups.is_empty() { keys.write(&[Value::U64(0)], &self.execution.context.memory)?; group_count = 1; }
        while let Some(row) = input.next().await? {
            self.execution.context.check_cancelled()?;
            let group = OwnedRow::new(self.groups.iter().map(|group| self.execution.evaluate(&group.expr, &self.schema, &row.values)).collect::<Result<_>>()?, &self.execution.context.memory)?;
            let mut reader = keys.reader(Arc::clone(&self.execution.context.memory))?;
            let mut id = None;
            while let Some(previous) = reader.next()? {
                self.execution.context.check_cancelled()?;
                if rows_equal(&group.values, &previous.values[1..])? { id = Some(group_id(&previous.values)?); break; }
            }
            let id = match id {
                Some(id) => id,
                None => {
                    let id = group_count;
                    group_count = group_count.checked_add(1).ok_or_else(|| invalid("group-count", "Aggregate group count overflow"))?;
                    let key = std::iter::once(Value::U64(id)).chain(group.values.iter().cloned()).collect::<Vec<_>>();
                    keys.write(&key, &self.execution.context.memory)?;
                    id
                }
            };
            let record = std::iter::once(Value::U64(id)).chain(row.values.iter().cloned()).collect::<Vec<_>>();
            rows.write(&record, &self.execution.context.memory)?;
        }
        self.keys = Some(keys.finish()?.reader(Arc::clone(&self.execution.context.memory))?);
        self.rows = Some(rows.finish()?);
        Ok(())
    }
    async fn aggregate(&mut self, group: u64, aggregate: &AggregateExpr) -> Result<Value> {
        let rows = self.rows.as_ref().ok_or_else(|| invalid("aggregate-state", "Aggregate input is not spooled"))?;
        let selected: BoxCursor = Box::new(AggregateRows { reader: rows.reader(Arc::clone(&self.execution.context.memory))?, group,
            schema: self.schema.clone(), aggregate: aggregate.clone(), seen: if aggregate.distinct { Some(self.execution.context.spill.create()?) } else { None }, execution: self.execution.clone() });
        let mut selected: BoxCursor = if aggregate.order_by.is_empty() || aggregate.function != AggregateFunction::ArrayAgg { selected }
            else { Box::new(SortCursor::new(selected, self.schema.clone(), aggregate.order_by.clone(), self.execution.clone())) };
        let mut count = 0i64;
        let mut accumulator: Option<OwnedRow> = None;
        let mut sum = 0.0f64;
        let mut squares = 0.0f64;
        let mut array = Vec::new();
        let mut array_reservations = Vec::new();
        while let Some(row) = selected.next().await? {
            self.execution.context.check_cancelled()?;
            let value = aggregate.argument.as_ref().map(|argument| self.execution.evaluate(argument, &self.schema, &row.values)).transpose()?.unwrap_or(Value::Null);
            if aggregate.function == AggregateFunction::CountStar {
                count = count.checked_add(1).ok_or_else(|| invalid("row-count", "Aggregate count overflow"))?;
                continue;
            }
            if aggregate.function == AggregateFunction::ArrayAgg {
                array_reservations.push(self.execution.context.memory.reserve(value.retained_bytes()?)?);
                array.push(if value == Value::Undefined { Value::Null } else { value });
                continue;
            }
            if matches!(value, Value::Null | Value::Undefined) {
                if aggregate.function == AggregateFunction::Sum && decimal_null_contributes_zero(aggregate) && accumulator.is_none() {
                    let zero = cast(&Value::I64(0), &aggregate.field.data_type, &self.execution.context.timezone)?;
                    accumulator = Some(OwnedRow::new(vec![zero], &self.execution.context.memory)?);
                }
                continue;
            }
            count = count.checked_add(1).ok_or_else(|| invalid("row-count", "Aggregate count overflow"))?;
            use AggregateFunction as A;
            let previous = accumulator.as_ref().map(|row| &row.values[0]);
            let next = match aggregate.function {
                A::Count | A::CountStar => None,
                A::Sum => {
                    let value = cast(&value, &aggregate.field.data_type, &self.execution.context.timezone)?;
                    if matches!(value, Value::I8(_) | Value::I16(_)) { return Err(rtdb_types::Anomaly::unsupported("rtdb.query/small-integer-sum", "Reference SUM does not implement byte or short accumulators")); }
                    let sum = match previous { Some(previous) => sum_value(previous, &value)?, None => value };
                    Some(cast(&sum, &aggregate.field.data_type, &self.execution.context.timezone)?)
                }
                A::Average => {
                    if aggregate.field.data_type.unencoded().legs().iter().any(|ty| matches!(ty, LogicalType::Scalar(ScalarType::Duration(_)))) {
                        let value = cast(&value, &aggregate.field.data_type, &self.execution.context.timezone)?;
                        Some(match previous { Some(previous) => sum_value(previous, &value)?, None => value })
                    } else {
                        sum += number(&value).ok_or_else(|| invalid("invalid-aggregate-type", "AVG requires numeric or duration values"))?;
                        None
                    }
                }
                A::Min | A::Max => {
                    let value = cast(&value, &aggregate.field.data_type, &self.execution.context.timezone)?;
                    let replace = match previous { None => true, Some(previous) => compare(&value, previous, false)? == if aggregate.function == A::Min { Ordering::Less } else { Ordering::Greater } };
                    if replace { Some(value) } else { None }
                }
                A::Every | A::Any => {
                    let Value::Bool(value) = value else { return Err(invalid("invalid-aggregate-type", "Boolean aggregate requires boolean values")); };
                    let previous = previous.and_then(|value| if let Value::Bool(value) = value { Some(*value) } else { None });
                    Some(Value::Bool(previous.map_or(value, |previous| if aggregate.function == A::Every { previous && value } else { previous || value })))
                }
                A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => {
                    let value = number(&value).ok_or_else(|| invalid("invalid-aggregate-type", "Variance requires numeric values"))?;
                    sum += value;
                    squares += value * value;
                    None
                }
                A::ArrayAgg => unreachable!(),
            };
            if let Some(next) = next { accumulator = Some(OwnedRow::new(vec![next], &self.execution.context.memory)?); }
        }
        use AggregateFunction as A;
        let value = match aggregate.function {
            A::Count | A::CountStar => Value::I64(count),
            A::ArrayAgg => if array.is_empty() { Value::Null } else { Value::List(array) },
            A::Average if count == 0 => Value::Null,
            A::Average => match accumulator.as_ref().map(|row| &row.values[0]) {
                Some(Value::Duration { value, unit }) => Value::Duration { value: *value / count, unit: *unit },
                _ => Value::F64(sum / count as f64),
            },
            A::VariancePopulation | A::VarianceSample | A::StddevPopulation | A::StddevSample => {
                let sample = matches!(aggregate.function, A::VarianceSample | A::StddevSample);
                if count < if sample { 2 } else { 1 } { Value::Null }
                else {
                    let variance = (squares - sum * sum / count as f64) / (count - i64::from(sample)) as f64;
                    Value::F64(if matches!(aggregate.function, A::StddevPopulation | A::StddevSample) { variance.sqrt() } else { variance })
                }
            }
            _ => accumulator.as_ref().map_or(Value::Null, |row| row.values[0].clone()),
        };
        Ok(value)
    }
}
#[async_trait]
impl RowCursor for AggregateCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if let Some(input) = self.input.take() { self.initialize(input).await?; }
        let Some(keys) = &mut self.keys else { return Ok(None); };
        let Some(key) = keys.next()? else { self.keys = None; self.rows = None; return Ok(None); };
        let group = group_id(&key.values)?;
        let mut values = key.values[1..].to_vec();
        let mut reservations = Vec::new();
        for index in 0..self.aggregates.len() {
            let aggregate = self.aggregates[index].clone();
            let value = self.aggregate(group, &aggregate).await?;
            reservations.push(self.execution.context.memory.reserve(value.retained_bytes()?)?);
            values.push(value);
        }
        OwnedRow::new(values, &self.execution.context.memory).map(Some)
    }
}

struct AggregateRows { reader: SpillReader, group: u64, schema: Schema, aggregate: AggregateExpr, seen: Option<SpillWriter>, execution: Execution }
#[async_trait]
impl RowCursor for AggregateRows {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        while let Some(mut row) = self.reader.next()? {
            self.execution.context.check_cancelled()?;
            if group_id(&row.values)? != self.group { continue; }
            row.values.remove(0);
            if let Some(filter) = &self.aggregate.filter {
                if !is_true(&self.execution.evaluate(filter, &self.schema, &row.values)?)? { continue; }
            }
            if let Some(seen) = &mut self.seen {
                let value = self.aggregate.argument.as_ref().map(|argument| self.execution.evaluate(argument, &self.schema, &row.values)).transpose()?.unwrap_or(Value::Null);
                let mut reader = seen.reader(Arc::clone(&self.execution.context.memory))?;
                let mut found = false;
                while let Some(previous) = reader.next()? {
                    self.execution.context.check_cancelled()?;
                    if sql_eq(&value, &previous.values[0])? == Some(true) { found = true; break; }
                }
                if found { continue; }
                seen.write(&[value], &self.execution.context.memory)?;
            }
            return Ok(Some(row));
        }
        Ok(None)
    }
}
fn group_id(row: &[Value]) -> Result<u64> {
    match row.first() { Some(Value::U64(id)) => Ok(*id), _ => Err(invalid("invalid-spill", "Aggregate record lacks a group ID")) }
}
fn decimal_null_contributes_zero(aggregate: &AggregateExpr) -> bool {
    let Some(argument) = &aggregate.argument else { return false; };
    let legs = argument.data_type().legs();
    legs.iter().any(|ty| matches!(ty, LogicalType::Scalar(ScalarType::Decimal { .. })))
        && legs.iter().all(|ty| matches!(ty, LogicalType::Null | LogicalType::Scalar(ScalarType::Decimal { .. })))
}
fn sum_value(left: &Value, right: &Value) -> Result<Value> {
    Ok(match (left, right) {
        (Value::I8(_), _) | (Value::I16(_), _) => return Err(rtdb_types::Anomaly::unsupported("rtdb.query/small-integer-sum", "Reference SUM does not implement byte or short accumulators")),
        (Value::I32(left), Value::I32(right)) => Value::I32(left.wrapping_add(*right)),
        (Value::I64(left), Value::I64(right)) => Value::I64(left.wrapping_add(*right)),
        (Value::Duration { value: left, unit }, Value::Duration { value: right, unit: other }) if unit == other => Value::Duration { value: left.wrapping_add(*right), unit: *unit },
        _ => arithmetic(BinaryOp::Add, left, right)?,
    })
}
