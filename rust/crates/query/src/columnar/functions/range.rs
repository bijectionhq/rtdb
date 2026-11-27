use std::{cmp::Ordering, sync::Arc};

use arrow_array::BooleanArray;
use arrow_buffer::BooleanBufferBuilder;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::Parameters;
use rtdb_plan::{BinaryOp, ColumnId, Expr, Schema};
use rtdb_types::{LogicalType, Result, Value};

use super::super::{ColumnBatch, PredicateSelection, size_error};
use super::predicate::{self, Cell, CellKind, Cells};

#[derive(Clone, Debug)]
pub(super) struct RangePredicate {
    position: usize,
    first: Bound,
    second: Bound,
}

#[derive(Clone, Copy, Debug)]
struct Bound {
    side: Side,
    inclusive: bool,
    // A bound written first can fail even when the column is null.
    bound_first: bool,
    value: BoundValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Lower,
    Upper,
}

#[derive(Clone, Copy, Debug)]
enum BoundValue {
    Constant(Option<Cell<'static>>),
    Parameter(usize),
}

impl RangePredicate {
    pub(super) fn bind(expr: &Expr, schema: &Schema) -> Option<Self> {
        let (column, first, second) = match expr {
            Expr::Binary { op: BinaryOp::And, left, right } => {
                let (column, first) = Bound::comparison(left)?;
                let (other, second) = Bound::comparison(right)?;
                if column != other || first.side == second.side {
                    return None;
                }
                (column, first, second)
            }
            Expr::Between { operand, low, high, symmetric: false } => {
                let Expr::Column { id, .. } = operand.as_ref() else { return None };
                (*id, Bound::new(BinaryOp::GtEq, low, false)?, Bound::new(BinaryOp::LtEq, high, false)?)
            }
            _ => return None,
        };
        let position = schema.position(column)?;
        numeric_layout(&schema.fields()[position].data_type).then_some(Self { position, first, second })
    }

    pub(super) fn selection(
        &self, batch: &ColumnBatch, parameters: Parameters<'_>, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<PredicateSelection>> {
        let Some(cells) = Cells::of(&batch.columns[self.position], true) else { return Ok(None) };
        let reservation = memory.reserve(PredicateSelection::bitmap_bound(batch.len))?;
        let mut bits = BooleanBufferBuilder::new(batch.len);
        let mut count = 0;
        if !self.visit(&cells, batch.len, parameters, |truth| {
            let keep = truth == Some(true);
            bits.append(keep);
            count += usize::from(keep);
        })? {
            return Ok(None);
        }
        PredicateSelection::settled(BooleanArray::new(bits.finish(), None), count, reservation).map(Some)
    }

    pub(super) fn truth(
        &self, batch: &ColumnBatch, parameters: Parameters<'_>,
    ) -> Result<Option<Vec<Option<bool>>>> {
        let Some(cells) = Cells::of(&batch.columns[self.position], true) else { return Ok(None) };
        let _scratch = batch.reservation.budget().reserve(
            batch.len.checked_mul(size_of::<Option<bool>>()).ok_or_else(size_error)?,
        )?;
        let mut truths = Vec::with_capacity(batch.len);
        Ok(self.visit(&cells, batch.len, parameters, |truth| truths.push(truth))?.then_some(truths))
    }

    /// A layout or value outside the kernel leaves the predicate to the scalar evaluator.
    fn visit(
        &self, cells: &Cells<'_>, len: usize, parameters: Parameters<'_>, mut emit: impl FnMut(Option<bool>),
    ) -> Result<bool> {
        let (mut first, mut second) = (self.first, self.second);
        let mut row = |value| -> Result<bool> {
            let Some(left) = first.test(value, parameters)? else { return Ok(false) };
            if left == Some(false) {
                emit(left);
            } else {
                // An unknown comparison still reaches the second bound. False decides the conjunction.
                let Some(right) = second.test(value, parameters)? else { return Ok(false) };
                emit(match (left, right) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                });
            }
            Ok(true)
        };
        if let Cells::Int(array) = cells {
            if array.len() != len { return Ok(false); }
            for value in array.iter() {
                if !row(value.map(Cell::Int))? { return Ok(false); }
            }
        } else {
            for index in 0..len {
                let Some(value) = cells.get(index).ok().and_then(numeric_cell) else { return Ok(false) };
                if !row(value)? { return Ok(false); }
            }
        }
        Ok(true)
    }
}

impl Bound {
    fn comparison(expr: &Expr) -> Option<(ColumnId, Self)> {
        let Expr::Binary { op, left, right } = expr else { return None };
        match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id, .. }, bound) => Some((*id, Self::new(*op, bound, false)?)),
            (bound, Expr::Column { id, .. }) => Some((*id, Self::new(*op, bound, true)?)),
            _ => None,
        }
    }

    fn new(op: BinaryOp, expr: &Expr, bound_first: bool) -> Option<Self> {
        use BinaryOp::{Gt, GtEq, Lt, LtEq};
        let side = match (op, bound_first) {
            (Gt | GtEq, false) | (Lt | LtEq, true) => Side::Lower,
            (Lt | LtEq, false) | (Gt | GtEq, true) => Side::Upper,
            _ => return None,
        };
        let value = match expr {
            Expr::Literal(value) => BoundValue::Constant(numeric_value(value)?),
            Expr::Parameter { index, data_type } if numeric_layout(data_type) => BoundValue::Parameter(*index),
            _ => return None,
        };
        Some(Self { side, inclusive: matches!(op, GtEq | LtEq), bound_first, value })
    }

    /// Strict comparison in operand order. Once reached, a parameter is resolved for the rest of the batch.
    fn test(&mut self, column: Option<Cell<'static>>, parameters: Parameters<'_>) -> Result<Option<Option<bool>>> {
        if column.is_none() && !self.bound_first {
            return Ok(Some(None));
        }
        let bound = match self.value {
            BoundValue::Constant(value) => value,
            BoundValue::Parameter(index) => {
                let Some(value) = parameters.get(index)?.and_then(numeric_value) else { return Ok(None) };
                self.value = BoundValue::Constant(value);
                value
            }
        };
        let (Some(column), Some(bound)) = (column, bound) else { return Ok(Some(None)) };
        let Ok(order) = predicate::order(column, bound, false) else { return Ok(None) };
        Ok(Some(Some(match order {
            Some(Ordering::Greater) => self.side == Side::Lower,
            Some(Ordering::Less) => self.side == Side::Upper,
            Some(Ordering::Equal) => self.inclusive,
            None => false,
        })))
    }
}

fn numeric_layout(ty: &LogicalType) -> bool {
    predicate::cell_kinds(ty, true).is_some_and(|kinds| kinds.iter().all(|kind| matches!(kind, CellKind::Int | CellKind::Float)))
}

fn numeric_value(value: &Value) -> Option<Option<Cell<'static>>> {
    numeric_cell(predicate::scalar(value, true).ok()?)
}

fn numeric_cell(cell: Option<Cell<'_>>) -> Option<Option<Cell<'static>>> {
    match cell {
        None => Some(None),
        Some(Cell::Int(value)) => Some(Some(Cell::Int(value))),
        Some(Cell::Float(value)) => Some(Some(Cell::Float(value))),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
