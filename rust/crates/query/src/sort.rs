use std::{cmp::Ordering, sync::Arc};
use rtdb_columnar::MemoryReservation;
use rtdb_expr::{ordered_compare_in_timezone, validate_order_type};
use rtdb_plan::{Schema, SortExpr};
use rtdb_types::{LogicalType, Result, Value};
use crate::{context::Execution, invalid};

pub(crate) mod batches;
mod timsort;
mod legacy;
mod native_keys;

const BLOCK_ROWS: usize = 102_400;
const MERGE_FAN_IN: usize = 4;

struct SortPlan { schema: Schema, order: Vec<SortExpr>, limit: u64, execution: Execution }
impl SortPlan {
    fn compare(&self, left: &[Value], right: &[Value]) -> Result<Ordering> {
        for (index, order) in self.order.iter().enumerate() {
            let left = left.get(index).ok_or_else(|| invalid("invalid-spill", "Sort key is absent"))?;
            let right = right.get(index).ok_or_else(|| invalid("invalid-spill", "Sort key is absent"))?;
            let mut comparison = ordered_compare_in_timezone(left, right, order.nulls_first, &self.execution.context.timezone)?;
            if order.descending && !matches!(left, Value::Null | Value::Undefined) && !matches!(right, Value::Null | Value::Undefined) { comparison = comparison.reverse(); }
            if comparison != Ordering::Equal { return Ok(comparison); }
        }
        Ok(Ordering::Equal)
    }
}

pub(crate) struct Indices { pub(crate) values: Vec<usize>, _reservation: Arc<MemoryReservation> }
pub(crate) fn sort_ordinals(len: usize, context: &crate::QueryContext, mut compare: impl FnMut(usize, usize) -> Result<Ordering>) -> Result<Indices> {
    let scratch = match context.options.object_sort {
        rtdb_types::ObjectSort::TimSort => timsort::scratch_bytes(len)?,
        rtdb_types::ObjectSort::LegacyMergeSort => legacy::scratch_bytes(len)?,
    };
    let bytes = len.checked_mul(std::mem::size_of::<usize>()).and_then(|size|size.checked_add(scratch)).and_then(|size|size.checked_add(256)).ok_or_else(size_error)?;
    let reservation = context.memory.reserve(bytes)?;
    let mut indices: Vec<_> = (0..len).collect();
    let compare = |left: usize,right: usize| {
        context.check_cancelled()?;
        compare(left,right)
    };
    match context.options.object_sort {
        rtdb_types::ObjectSort::TimSort => timsort::sort(&mut indices,compare)?,
        rtdb_types::ObjectSort::LegacyMergeSort => legacy::sort(&mut indices,compare)?,
    }
    Ok(Indices { values: indices,_reservation: reservation })
}
fn size_error() -> rtdb_types::Anomaly { invalid("sort-size", "Sort state exceeds addressable memory") }

/// The declared key types joined with every key observed; `seen` holds key-type tuples already joined, which change nothing.
struct KeyTypes { values: Vec<LogicalType>, seen: Vec<Vec<LogicalType>>, _reservation: Arc<MemoryReservation> }
impl KeyTypes {
    fn declared(plan: &SortPlan) -> Result<Self> {
        let mut bound = crate::runtime_schema::schema_bound(&plan.schema)?;
        for order in &plan.order { bound = bound.checked_add(crate::runtime_schema::expression_bound(&order.expr)?).ok_or_else(size_error)?; }
        for ty in plan.execution.parameters.types() { bound = bound.checked_add(ty.retained_bytes()?).ok_or_else(size_error)?; }
        for schema in plan.execution.outer.schemas() { bound = bound.checked_add(crate::runtime_schema::schema_bound(schema)?).ok_or_else(size_error)?; }
        let scope = plan.execution.scope()?;
        scope.reserve(bound.checked_mul(16).and_then(|n| n.checked_add(4096)).ok_or_else(size_error)?)?;
        let values = plan.order.iter().map(|order| plan.execution.expression_type(&scope, &order.expr, &plan.schema)).collect::<Result<Vec<_>>>()?;
        Self::new(values, plan)
    }
    fn new(values: Vec<LogicalType>, plan: &SortPlan) -> Result<Self> {
        let reservation = plan.execution.context.memory.reserve(type_bytes(&values)?.saturating_mul(17))?;
        Ok(Self { values, seen: Vec::new(), _reservation: reservation })
    }
    fn observe(&mut self, values: &[Value], plan: &SortPlan) -> Result<()> {
        if values.len() != self.values.len() { return Err(invalid("sort-state", "Sort key metadata width changed")); }
        let types: Vec<LogicalType> = values.iter().map(Value::logical_type).collect();
        if self.seen.contains(&types) { return Ok(()); }
        let bound = values.iter().try_fold(type_bytes(&self.values)?, |sum, value| sum.checked_add(value.retained_bytes()?).ok_or_else(size_error))?;
        let _temporary = plan.execution.context.memory.reserve(bound.checked_mul(16).and_then(|n| n.checked_add(4096)).ok_or_else(size_error)?)?;
        let values = self.values.iter().zip(&types).map(|(ty, value)| ty.join(value)).collect();
        let mut seen = std::mem::take(&mut self.seen);
        *self = Self::new(values, plan)?;
        if seen.len() == 16 { seen.remove(0); }
        seen.push(types);
        self.seen = seen;
        Ok(())
    }
    fn validate(&self) -> Result<()> { for ty in &self.values { validate_order_type(ty)?; } Ok(()) }
}
fn type_bytes(types: &Vec<LogicalType>) -> Result<usize> {
    let base = types.capacity().checked_mul(std::mem::size_of::<LogicalType>()).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?;
    types.iter().try_fold(base, |sum, ty| sum.checked_add(ty.retained_bytes()?).ok_or_else(size_error))
}
#[cfg(test)]
mod object_sort_tests;
