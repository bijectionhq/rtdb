use std::{mem::size_of, sync::Arc};

use arrow_array::{Array, BinaryArray, builder::BinaryBuilder};
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_plan::{Expr, Schema, UnaryOp};
use rtdb_types::{AnomalyKind, LogicalType, Result, ScalarType, Value};

use super::{
    Column, ColumnBatch, observed_scalar_column, owned_projection_bytes, plain_scalar, size_error,
};
use crate::context::Execution;

enum BinaryInput<'a> {
    Column(&'a Column),
    Parameter(ArrowCell<'a>),
    Literal(Option<&'a [u8]>),
}

impl<'a> BinaryInput<'a> {
    fn bind(
        expr: &'a Expr,
        schema: &Schema,
        batch: &'a ColumnBatch,
        execution: &'a Execution,
    ) -> Option<Self> {
        Some(match expr {
            Expr::Column { id, .. } => {
                let position = schema.position(*id)?;
                if plain_scalar(&schema.fields()[position].data_type) != Some(&ScalarType::Binary) {
                    return None;
                }
                Self::Column(&batch.columns[position])
            }
            Expr::Parameter { index, .. } => {
                if plain_scalar(execution.parameters.types().get(*index)?)
                    != Some(&ScalarType::Binary)
                {
                    return None;
                }
                Self::Parameter(execution.parameters.evaluator().arrow_cell(*index)?)
            }
            Expr::Literal(Value::Binary(bytes)) => Self::Literal(Some(bytes)),
            Expr::Literal(Value::Null) => Self::Literal(None),
            _ => return None,
        })
    }

    fn cell(&self, row: usize) -> Option<Option<&'a [u8]>> {
        let cell = match *self {
            Self::Column(column) => {
                let Some((array, field, logical, row)) = column.arrow_cell(row)? else {
                    return Some(None);
                };
                if logical.is_some_and(|logical| !super::stable_read_layout(logical, field)) {
                    return None;
                }
                ArrowCell { array, field, row }
            }
            Self::Parameter(cell) => cell,
            Self::Literal(value) => return Some(value),
        };
        let ArrowCell { array, field, row } = cell;
        if !field.metadata().is_empty() || field.data_type() != array.data_type() {
            return None;
        }
        if field.data_type() == &DataType::Null && field.is_nullable() {
            return Some(None);
        }
        let binary = array.as_any().downcast_ref::<BinaryArray>()?;
        Some(binary.is_valid(row).then(|| binary.value(row)))
    }
}

pub(super) fn column(
    expr: &Expr,
    output: &LogicalType,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    match substring(expr, output, schema, batch, execution) {
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => Ok(None),
        result => result,
    }
}

fn substring(
    expr: &Expr,
    output: &LogicalType,
    schema: &Schema,
    batch: &ColumnBatch,
    execution: &Execution,
) -> Result<Option<(Column, Arc<MemoryReservation>)>> {
    let Expr::Call { name, args, .. } = expr else {
        return Ok(None);
    };
    if !name.eq_ignore_ascii_case("substring") || plain_scalar(output) != Some(&ScalarType::Binary)
    {
        return Ok(None);
    }
    let (input, position, length) = match args.as_slice() {
        [input, position] => (input, position, None),
        [input, position, length] => (input, position, Some(length)),
        _ => return Ok(None),
    };
    let Some(position) = signed_literal(position) else {
        return Ok(None);
    };
    let length = match length {
        Some(length) => match signed_literal(length) {
            Some(length) => Some(length),
            None => return Ok(None),
        },
        None => None,
    };
    let Some(input) = BinaryInput::bind(input, schema, batch, execution) else {
        return Ok(None);
    };
    let len = batch.len;
    let context = &execution.context;
    let slots = len
        .checked_mul(size_of::<Option<&[u8]>>())
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or_else(size_error)?;
    let _slots = context.memory.reserve(slots)?;
    let mut slices = Vec::with_capacity(len);
    let mut payload = 0usize;
    for row in 0..len {
        context.check_cancelled_at(row)?;
        let Some(cell) = input.cell(row) else {
            return Ok(None);
        };
        let value = cell
            .map(|cell| rtdb_expr::kernels::binary_substring(cell, position, length))
            .transpose()?;
        payload = payload
            .checked_add(value.map_or(0, <[u8]>::len))
            .ok_or_else(size_error)?;
        if i32::try_from(payload).is_err() {
            return Ok(None);
        }
        slices.push(value);
    }

    let bound = len
        .checked_add(1)
        .and_then(|offsets| offsets.checked_mul(size_of::<i32>()))
        .and_then(|bytes| bytes.checked_add(payload))
        .and_then(|bytes| bytes.checked_add(len.div_ceil(8) + 1024))
        .ok_or_else(size_error)?;
    let mut charge = context.memory.reserve(bound)?;
    let mut builder = BinaryBuilder::with_capacity(len, payload);
    for (row, slice) in slices.into_iter().enumerate() {
        context.check_cancelled_at(row)?;
        builder.append_option(slice);
    }
    let column = observed_scalar_column(builder.finish())?;
    let bytes = owned_projection_bytes(&column)?;
    let reservation = Arc::get_mut(&mut charge).expect("The Binary writer owns its reservation");
    if bytes <= reservation.bytes() {
        reservation.shrink_to(bytes)?;
    } else {
        reservation.grow_to(bytes)?;
    }
    Ok(Some((column, charge)))
}

fn signed_literal(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(Value::I8(value)) => Some(i64::from(*value)),
        Expr::Literal(Value::I16(value)) => Some(i64::from(*value)),
        Expr::Literal(Value::I32(value)) => Some(i64::from(*value)),
        Expr::Literal(Value::I64(value)) => Some(*value),
        Expr::Unary {
            op: UnaryOp::Negate,
            expr,
        } => match expr.as_ref() {
            Expr::Literal(Value::I64(value)) => value.checked_neg(),
            _ => None,
        },
        _ => None,
    }
}
