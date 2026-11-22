use std::sync::Arc;

use arrow_array::{Array, ArrowPrimitiveType, FixedSizeListArray, Float64Array, PrimitiveArray, builder::Float64Builder, types::{Float32Type, Float64Type}};
use arrow_schema::{DataType, Field};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::CosineVector;
use rtdb_plan::{ColumnId, Expr, Schema};
use rtdb_types::{LogicalType, Result, ScalarType, Value};

use crate::columnar::{Column, ColumnBatch};

fn inputs(expr: &Expr) -> Option<(ColumnId, &Expr)> {
    let Expr::Call { name, args, .. } = expr else { return None };
    if !name.eq_ignore_ascii_case("cosine_distance") { return None; }
    let (id, other) = match args.as_slice() {
        [Expr::Column { id, .. }, other] | [other, Expr::Column { id, .. }] => (*id, other),
        _ => return None,
    };
    match other {
        Expr::Literal(_) | Expr::Parameter { .. } => Some((id, other)),
        Expr::List(items) if items.iter().all(|item| matches!(item, Expr::Literal(_))) => Some((id, other)),
        _ => None,
    }
}

pub(crate) fn shaped(expr: &Expr) -> bool { inputs(expr).is_some() }

fn components(value: &Value) -> Option<&[Value]> {
    match value {
        Value::List(values) | Value::FixedSizeList(values) => Some(values.as_slice()),
        _ => None,
    }
}

pub(crate) struct Projection {
    position: usize,
    query: CosineVector,
    _memory: Arc<MemoryReservation>,
}

impl Projection {
    pub(crate) fn bind(expr: &Expr, schema: &Schema, parameters: &[Value], memory: &Arc<MemoryBudget>) -> Result<Option<Self>> {
        let Some((id, other)) = inputs(expr) else { return Ok(None) };
        let Some(position) = schema.position(id) else { return Ok(None) };
        let prepared = match other {
            Expr::Literal(value) => Self::prepare(components(value).into_iter().flatten(), memory),
            Expr::Parameter { index, .. } => Self::prepare(parameters.get(*index).and_then(components).into_iter().flatten(), memory),
            Expr::List(items) => Self::prepare(items.iter().filter_map(|item| match item { Expr::Literal(value) => Some(value), _ => None }), memory),
            _ => return Ok(None),
        }?;
        Ok(prepared.map(|(query, _memory)| Self { position, query, _memory }))
    }

    fn prepare<'a>(values: impl Iterator<Item = &'a Value> + Clone, memory: &Arc<MemoryBudget>) -> Result<Option<(CosineVector, Arc<MemoryReservation>)>> {
        let len = values.clone().count();
        if len == 0 { return Ok(None); }
        let bytes = len.checked_mul(8).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(|| crate::invalid("vector-size", "Prepared vector exceeds addressable memory"))?;
        let Ok(reservation) = memory.reserve(bytes) else { return Ok(None) };
        Ok(CosineVector::prepare(values, len).ok().map(|query| (query, reservation)))
    }

    pub(crate) fn column(&self, batch: &ColumnBatch) -> Option<Column> {
        let Column::Arrow { array, .. } = batch.columns.get(self.position)? else { return None };
        let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
        let width = usize::try_from(list.value_length()).ok()?;
        if self.query.dimensions() != width { return None; }
        let children = list.values();
        let output = match children.data_type() {
            DataType::Float32 => score_rows::<Float32Type>(list, &self.query, width)?,
            DataType::Float64 => score_rows::<Float64Type>(list, &self.query, width)?,
            _ => return None,
        };
        Some(Column::Arrow { array: Arc::new(output), field: Arc::new(Field::new("cosine_distance", DataType::Float64, true)),
            logical: Some(LogicalType::Scalar(ScalarType::F64).maybe()) })
    }
}

fn score_rows<T: ArrowPrimitiveType>(list: &FixedSizeListArray, query: &CosineVector, width: usize) -> Option<Float64Array>
where T::Native: Into<f64>,
{
    let values = list.values().as_any().downcast_ref::<PrimitiveArray<T>>()?;
    let mut builder = Float64Builder::with_capacity(list.len());
    let nulls = values.nulls();
    for row in 0..list.len() {
        if list.is_null(row) {
            builder.append_null();
            continue;
        }
        let start = row * width;
        let end = start + width;
        if nulls.is_some_and(|nulls| (start..end).any(|component| nulls.is_null(component))) {
            return None;
        }
        let vector = values.values().get(start..end)?;
        builder.append_value(query.score(vector.iter().copied().map(Into::into))?);
    }
    Some(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{FixedSizeListBuilder, Float32Builder};
    use rtdb_plan::Binder;
    use rtdb_types::ListKind;

    #[test]
    fn projects_sliced_arrow_vectors_and_parent_nulls() {
        let memory = MemoryBudget::new(1 << 20);
        let mut lists = FixedSizeListBuilder::new(Float32Builder::new(), 2);
        for (value, valid) in [(Some([9.0, 9.0]), true), (Some([1.0, 0.0]), true), (None, false), (Some([0.0, 1.0]), true)] {
            if let Some(value) = value {
                lists.values().append_slice(&value);
            } else {
                lists.values().append_null();
                lists.values().append_null();
            }
            lists.append(valid);
        }
        let array = lists.finish().slice(1, 3);
        let vector_type = LogicalType::List { kind: ListKind::FixedSizeList(2), element: Box::new(LogicalType::Scalar(ScalarType::F32).maybe()) }.maybe();
        let field = Binder::default().field("embedding", None, vector_type.clone()).unwrap();
        let expr = Expr::Call {
            name: "cosine_distance".into(),
            args: vec![field.expression(), Expr::Parameter { index: 0, data_type: vector_type.clone() }],
            return_type: LogicalType::Scalar(ScalarType::F64).maybe(),
        };
        let schema = Schema::new(vec![field]).unwrap();
        let batch = ColumnBatch::settled(vec![Column::Arrow {
            field: Arc::new(Field::new("embedding", array.data_type().clone(), true)), array: Arc::new(array), logical: Some(vector_type),
        }], 3, memory.reserve(16384).unwrap()).unwrap();
        let parameters = [Value::List(vec![Value::F32(1.0), Value::F32(0.0)])];
        let projection = Projection::bind(&expr, &schema, &parameters, &memory).unwrap().unwrap();
        let Some(Column::Arrow { array, .. }) = projection.column(&batch) else { panic!("Expected Arrow distances") };
        let distance = array.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(distance.iter().collect::<Vec<_>>(), vec![Some(0.0), None, Some(1.0)]);
    }
}
