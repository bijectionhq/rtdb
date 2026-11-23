use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, FixedSizeListArray, Float16Array, Float32Array, Float64Array, ListArray, UnionArray,
    builder::Float64Builder,
};
use arrow_schema::{DataType, Field};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_expr::CosineVector;
use rtdb_plan::{ColumnId, Expr, Schema};
use rtdb_types::{ListValue, ListView, LogicalType, Result, ScalarType, Value};

use crate::columnar::{Column, ColumnBatch};

fn inputs(expr: &Expr) -> Option<(ColumnId, &Expr)> {
    let Expr::Call { name, args, .. } = expr else { return None; };
    if !name.eq_ignore_ascii_case("cosine_distance") { return None; }
    let (column, reference) = match args.as_slice() {
        [Expr::Column { id, .. }, other] | [other, Expr::Column { id, .. }] => (*id, other),
        _ => return None,
    };
    match reference {
        Expr::Literal(_) | Expr::Parameter { .. } => Some((column, reference)),
        Expr::List(items) if items.iter().all(|item| matches!(item, Expr::Literal(_))) => Some((column, reference)),
        _ => None,
    }
}

pub(crate) struct Projection {
    position: usize,
    query: CosineVector,
    _memory: Arc<MemoryReservation>,
}

impl Projection {
    pub(crate) fn bind(
        expr: &Expr, schema: &Schema, parameters: &crate::parameters::BoundParameters, memory: &Arc<MemoryBudget>,
    ) -> Result<Option<Self>> {
        let Some((id, reference)) = inputs(expr) else { return Ok(None); };
        let Some(position) = schema.position(id) else { return Ok(None); };
        let prepared = match reference {
            Expr::Literal(value) => prepare_value(value, memory),
            Expr::Parameter { index, .. } => {
                if let Some((batch, row)) = parameters.arrow_input(*index) {
                    let batch = batch.batch();
                    prepare_arrow(ArrowCell {
                        field: batch.schema_ref().field(*index), array: batch.column(*index).as_ref(), row,
                    }, memory)
                } else {
                    match parameters.get(*index).ok().flatten() {
                        Some(value) => prepare_value(value, memory),
                        None => Ok(None),
                    }
                }
            }
            Expr::List(items) => prepare(items.len(), memory, || {
                CosineVector::prepare(items.iter().filter_map(|item| match item {
                    Expr::Literal(value) => Some(value),
                    _ => None,
                }), items.len())
            }),
            _ => Ok(None),
        }?;
        Ok(prepared.map(|(query, _memory)| Self { position, query, _memory }))
    }

    pub(crate) fn column(&self, batch: &ColumnBatch) -> Option<Column> {
        let column = batch.columns.get(self.position)?;
        let mut output = Float64Builder::with_capacity(batch.len);
        for row in 0..batch.len {
            let Some((array, field, _, row)) = column.arrow_cell(row)? else {
                output.append_null();
                continue;
            };
            let cell = ArrowCell { array, field, row };
            if cell.is_null().ok()? {
                output.append_null();
                continue;
            }
            let (array, components) = span(cell)?;
            let score = if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
                self.query.score(values.values()[components].iter().map(|&value| f64::from(value)))?
            } else if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
                self.query.score(values.values()[components].iter().copied())?
            } else if let Some(values) = array.as_any().downcast_ref::<Float16Array>() {
                self.query.score(values.values()[components].iter().map(|value| value.to_f64()))?
            } else {
                let values = ListView::Union(array.as_any().downcast_ref::<UnionArray>()?);
                if components.clone().any(|i| values.float_component(i).is_none()) { return None; }
                self.query.score(components.map(|i| values.float_component(i).expect("component verified")) )?
            };
            output.append_value(score);
        }
        Some(Column::Arrow {
            array: Arc::new(output.finish()),
            field: Arc::new(Field::new("cosine_distance", DataType::Float64, true)),
            logical: Some(LogicalType::Scalar(ScalarType::F64).maybe()),
        })
    }
}

fn span(cell: ArrowCell<'_>) -> Option<(&dyn Array, Range<usize>)> {
    let cell = cell.physical().ok()?;
    if cell.array.is_null(cell.row) || !cell.field.metadata().is_empty() { return None; }
    let (item, array, range) = match cell.field.data_type() {
        DataType::FixedSizeList(item, width) if *width > 0 => {
            let values = cell.array.as_any().downcast_ref::<FixedSizeListArray>()?;
            let start = usize::try_from(values.value_offset(cell.row)).ok()?;
            (item, values.values().as_ref(), start..start.checked_add(usize::try_from(*width).ok()?)?)
        }
        DataType::List(item) => {
            let values = cell.array.as_any().downcast_ref::<ListArray>()?;
            let offsets = values.value_offsets();
            let start = usize::try_from(*offsets.get(cell.row)?).ok()?;
            let end = usize::try_from(*offsets.get(cell.row + 1)?).ok()?;
            (item, values.values().as_ref(), start..end)
        }
        _ => return None,
    };
    if range.end > array.len() || !item.metadata().is_empty()
        || !(matches!(array.data_type(), DataType::Float16 | DataType::Float32 | DataType::Float64)
            || ListValue::supports_float_union(array.data_type()))
        || array.nulls().is_some_and(|nulls| range.clone().any(|i| nulls.is_null(i)))
    {
        return None;
    }
    Some((array, range))
}

type PreparedVector = (CosineVector, Arc<MemoryReservation>);

fn prepare(dimensions: usize, memory: &Arc<MemoryBudget>, build: impl FnOnce() -> Result<CosineVector>) -> Result<Option<PreparedVector>> {
    let bytes = dimensions.checked_mul(8).and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(|| crate::invalid("vector-size", "Prepared vector exceeds addressable memory"))?;
    let Ok(reservation) = memory.reserve(bytes) else { return Ok(None); };
    Ok(build().ok().map(|vector| (vector, reservation)))
}

fn prepare_value(value: &Value, memory: &Arc<MemoryBudget>) -> Result<Option<PreparedVector>> {
    let Some(items) = value.list_view() else { return Ok(None); };
    prepare(items.len(), memory, || CosineVector::prepare_value(value))
}

fn prepare_arrow(cell: ArrowCell<'_>, memory: &Arc<MemoryBudget>) -> Result<Option<PreparedVector>> {
    let Some((array, range)) = span(cell) else { return Ok(None); };
    let dimensions = range.len();
    if let Some(values) = array.as_any().downcast_ref::<Float32Array>() {
        return prepare(dimensions, memory, || CosineVector::prepare_components(
            values.values()[range].iter().map(|&value| Ok(f64::from(value))), dimensions,
        ));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float64Array>() {
        return prepare(dimensions, memory, || CosineVector::prepare_components(
            values.values()[range].iter().copied().map(Ok), dimensions,
        ));
    }
    if let Some(values) = array.as_any().downcast_ref::<Float16Array>() {
        return prepare(dimensions, memory, || CosineVector::prepare_components(
            values.values()[range].iter().map(|value| Ok(value.to_f64())), dimensions,
        ));
    }
    let Some(values) = array.as_any().downcast_ref::<UnionArray>() else { return Ok(None); };
    let view = ListView::Union(values);
    if range.clone().any(|i| view.float_component(i).is_none()) { return Ok(None); }
    prepare(dimensions, memory, || CosineVector::prepare_components(
        range.map(|i| Ok(view.float_component(i).expect("component verified"))), dimensions,
    ))
}
